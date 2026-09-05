//go:build integration_pg

// Real-PostgreSQL coverage for the batch stall detector.
//
// The sqlmock suite in batch_sentinel_test.go proves the control flow: which
// statements run, in what order, and — via a deliberately unfulfilled
// expectation — that the resolver does NOT delete the issue progressTick just
// raised. What it structurally cannot prove is that any of that SQL is
// executable. sqlmock matches statements as strings and never type-checks them,
// which is exactly how the #723 `text = uuid` comparison shipped green: a query
// PostgreSQL refuses to run passed a mock that only ever compared regexes.
//
// This file closes that gap for the stall path. It runs the real detector
// against a real database with the real migrations applied, so the assertions
// cover both halves at once: the SQL executes, AND the issue survives.
//
// Not part of the default suite — needs a live server:
//
//	docker run -d --name sentinel-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=pipeline_db -p 55439:5432 postgres:16
//	for m in api-gateway/migrations/*.sql; do
//	    docker exec -i sentinel-pg psql -U postgres -d pipeline_db -v ON_ERROR_STOP=1 -q < "$m"
//	done
//	SENTINEL_PG_DSN='postgres://postgres:verify@localhost:55439/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/agents/sentinel/ -run PG -v
package sentinel

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/rsync-ai/shared/pgdriver"
)

// Fixed IDs so a crashed run leaves nothing to collide with: every test deletes
// this set before seeding rather than relying on cleanup having happened.
const (
	pgTestUserID      = "aaaaaaaa-0000-4000-8000-000000000001"
	pgTestWorkspaceID = "aaaaaaaa-0000-4000-8000-000000000002"
	pgTestPipelineID  = "aaaaaaaa-0000-4000-8000-000000000003"
	pgTestExecutionID = "aaaaaaaa-0000-4000-8000-000000000004"
)

func pgTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("SENTINEL_PG_DSN")
	if dsn == "" {
		t.Skip("SENTINEL_PG_DSN not set — see the file header for the two commands that provide one")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping %s: %v", dsn, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func pgPurge(t *testing.T, db *sql.DB) {
	t.Helper()
	// pipelines cascades to executions and pipeline_progress; issues are not FK'd
	// to the pipeline, so they need their own delete.
	for _, q := range []string{
		`DELETE FROM sentinel_active_issues WHERE component_id = $1`,
		`DELETE FROM pipelines WHERE id = $1::uuid`,
	} {
		if _, err := db.Exec(q, pgTestPipelineID); err != nil {
			t.Fatalf("purge %q: %v", q, err)
		}
	}
	if _, err := db.Exec(`DELETE FROM workspaces WHERE id = $1::uuid`, pgTestWorkspaceID); err != nil {
		t.Fatalf("purge workspace: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM users WHERE id = $1::uuid`, pgTestUserID); err != nil {
		t.Fatalf("purge user: %v", err)
	}
}

// pgSeedBatchRun creates a batch pipeline whose only running execution last moved
// `quietFor` ago. progressStatus feeds the pp.status the query filters on.
func pgSeedBatchRun(t *testing.T, db *sql.DB, quietFor time.Duration, progressStatus string) {
	t.Helper()
	pgPurge(t, db)
	t.Cleanup(func() { pgPurge(t, db) })

	mustExec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed %.60q: %v", q, err)
		}
	}
	mustExec(`INSERT INTO users (id, email, password_hash) VALUES ($1::uuid, $2, 'x')`,
		pgTestUserID, "batch-stall-verify@example.invalid")
	mustExec(`INSERT INTO workspaces (id, name, slug, owner_id)
	          VALUES ($1::uuid, 'batch-stall-verify', 'batch-stall-verify', $2::uuid)`,
		pgTestWorkspaceID, pgTestUserID)
	// sync_mode 'batch' + cdc_mode NULL is what puts this run in scope: the query
	// excludes CDC pipelines, which have their own sentinel.
	mustExec(`INSERT INTO pipelines
	          (id, name, natural_language_request, workspace_id, status, sync_mode, cdc_mode, created_by)
	          VALUES ($1::uuid, 'batch stall verify', 'verify', $2::uuid, 'running', 'batch', NULL, $3::uuid)`,
		pgTestPipelineID, pgTestWorkspaceID, pgTestUserID)
	mustExec(`INSERT INTO executions (id, pipeline_id, status, start_time, end_time)
	          VALUES ($1::uuid, $2::uuid, 'running', NOW() - $3::interval, NULL)`,
		pgTestExecutionID, pgTestPipelineID, quietFor.String())
	// updated_at is `timestamp WITHOUT time zone` here while executions.start_time
	// is timestamptz — the mixed-type GREATEST the query has to survive.
	mustExec(`INSERT INTO pipeline_progress (pipeline_id, execution_id, status, current_stage, updated_at)
	          VALUES ($1::uuid, $2::uuid, $3, 'extract', (NOW() - $4::interval)::timestamp)`,
		pgTestPipelineID, pgTestExecutionID, progressStatus, quietFor.String())
}

func pgStallIssue(t *testing.T, db *sql.DB) (exists bool, occurrences int) {
	t.Helper()
	err := db.QueryRow(`SELECT occurrence_count FROM sentinel_active_issues WHERE id = $1`,
		stallIssueID(pgTestPipelineID)).Scan(&occurrences)
	switch err {
	case nil:
		return true, occurrences
	case sql.ErrNoRows:
		return false, 0
	default:
		t.Fatalf("read issue: %v", err)
		return false, 0
	}
}

func pgSentinel(db *sql.DB) *BatchSentinel {
	// kafkaManager nil: emitBatchIssue returns after the INSERT, so the DB is the
	// only observable effect and the assertions below are unambiguous.
	return &BatchSentinel{db: db, stallThreshold: 20 * time.Minute}
}

// TestPGStallIssueSurvivesTheFollowingTick is the assertion prod could not give.
//
// Under the bug this fails on the very first tick: progressTick raised the issue
// under `batch-stall-<pid>` but marked it open under the bare `<pid>`, so the
// resolve pass at the end of the same tick found the row unaccounted for and
// deleted it. The occurrence_count check on the second tick is the sharper half —
// a count of 1 would mean the row we see is a fresh insert after a delete, not a
// survivor.
func TestPGStallIssueSurvivesTheFollowingTick(t *testing.T) {
	db := pgTestDB(t)
	pgSeedBatchRun(t, db, 90*time.Minute, "extracting")
	s := pgSentinel(db)

	s.progressTick(context.Background())
	exists, count := pgStallIssue(t, db)
	if !exists {
		t.Fatal("no stall issue after the first tick: either the query did not match the " +
			"seeded run, or resolveStaleIssues deleted the issue progressTick had just raised")
	}
	if count != 1 {
		t.Fatalf("occurrence_count = %d after one tick, want 1", count)
	}

	s.progressTick(context.Background())
	exists, count = pgStallIssue(t, db)
	if !exists {
		t.Fatal("stall issue vanished on the second tick — the map key and the row id have " +
			"drifted apart again; both must come from stallIssueID")
	}
	if count != 2 {
		t.Fatalf("occurrence_count = %d after two ticks, want 2 — a reset to 1 means the row "+
			"was deleted and re-inserted rather than re-raised in place", count)
	}
}

// TestPGBareKeyStillDeletes keeps the test above from being vacuous.
//
// It drives resolveStaleIssues with the pre-fix key by hand and shows the delete
// really does fire, so a green result above means the fix works — not that this
// harness is incapable of observing the failure.
func TestPGBareKeyStillDeletes(t *testing.T) {
	db := pgTestDB(t)
	pgSeedBatchRun(t, db, 90*time.Minute, "extracting")
	s := pgSentinel(db)
	ctx := context.Background()
	id := stallIssueID(pgTestPipelineID)

	raise := func() {
		s.emitBatchIssue(ctx, id, IssueTypeStalledRun, IssueSeverityWarning,
			pgTestPipelineID, "batch stall verify", pgTestExecutionID, "seeded", nil)
		if exists, _ := pgStallIssue(t, db); !exists {
			t.Fatal("emitBatchIssue did not persist the issue")
		}
	}

	raise()
	s.resolveStaleIssues(ctx, batchStallIssuePrefix, map[string]bool{pgTestPipelineID: true})
	if exists, _ := pgStallIssue(t, db); exists {
		t.Fatal("the bare pipelineID key no longer deletes the issue — this test can no longer " +
			"detect the regression it exists to detect, so fix it before trusting the suite")
	}

	raise()
	s.resolveStaleIssues(ctx, batchStallIssuePrefix, map[string]bool{id: true})
	if exists, _ := pgStallIssue(t, db); !exists {
		t.Fatal("the prefixed key deleted the issue it marked open")
	}
}

// TestPGResolveIsPrefixScoped runs the `id LIKE $2 || '%'` scoping against a real
// planner. A stall sweep that also cleared write-rejections would silently drop a
// live, differently-caused problem on the same pipeline.
func TestPGResolveIsPrefixScoped(t *testing.T) {
	db := pgTestDB(t)
	pgSeedBatchRun(t, db, 90*time.Minute, "extracting")
	s := pgSentinel(db)
	ctx := context.Background()
	ackID := ackIssueID(pgTestPipelineID, "orders")

	s.emitBatchIssue(ctx, ackID, IssueTypeSinkWriteRejected, IssueSeverityCritical,
		pgTestPipelineID, "batch stall verify", pgTestExecutionID, "seeded", nil)

	ackPresent := func() bool {
		var n int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM sentinel_active_issues WHERE id = $1`, ackID).Scan(&n); err != nil {
			t.Fatalf("count ack issue: %v", err)
		}
		return n == 1
	}

	// A stall sweep that re-observed nothing must leave the ack issue alone.
	s.resolveStaleIssues(ctx, batchStallIssuePrefix, map[string]bool{})
	if !ackPresent() {
		t.Fatal("the stall sweep deleted a sink write-rejection issue — the two classes must stay independent")
	}

	s.resolveStaleIssues(ctx, batchAckIssuePrefix, map[string]bool{})
	if ackPresent() {
		t.Fatal("the ack sweep did not clear its own unobserved issue")
	}
}

// TestPGQueryScope pins the two exclusions that decide whether a run is even a
// stall candidate — both expressed in SQL, so only a real planner can check them.
func TestPGQueryScope(t *testing.T) {
	db := pgTestDB(t)
	ctx := context.Background()

	t.Run("recent movement is not a stall", func(t *testing.T) {
		pgSeedBatchRun(t, db, 2*time.Minute, "extracting")
		pgSentinel(db).progressTick(ctx)
		if exists, _ := pgStallIssue(t, db); exists {
			t.Fatal("a run that moved 2 minutes ago was reported stalled against a 20 minute threshold")
		}
	})

	t.Run("waiting_for_user is not a stall", func(t *testing.T) {
		// The table-selection HITL wedge parks runs here indefinitely and on
		// purpose; reporting it as a stall would bury the real signal in noise.
		pgSeedBatchRun(t, db, 90*time.Minute, "waiting_for_user")
		pgSentinel(db).progressTick(ctx)
		if exists, _ := pgStallIssue(t, db); exists {
			t.Fatal("a run parked at a user prompt was reported as a stall")
		}
	})
}

// ---------------------------------------------------------------------------
// Sink-presence coverage: the manifest LATERAL must be correlated to the run.
//
// The sqlmock tests for this predicate assert on the query STRING, because
// sqlmock never executes SQL. That is enough to stop the regression, and not
// enough to prove the predicate does what the string suggests: a correlated
// reference to `e.id` from inside a second LATERAL either resolves or is a
// planner error, and a query that errors at runtime disarms the probe SILENTLY
// (probeBatchSinks logs and returns). A disarmed probe looks exactly like a
// fixed one. Only a real server can tell those apart.
// ---------------------------------------------------------------------------

// The prior run's id. Separate from pgTestExecutionID so a manifest row can point
// at a run that is over while a different run is live.
const pgPriorExecutionID = "aaaaaaaa-0000-4000-8000-000000000005"

// The two identifier shapes, taken from the 08-18 prod instance: the stale row
// predates #789 (07450ce8) and carries the bare name, while anything this binary
// registers now goes through kafkaclient.Group and carries the "rsync." prefix.
// Keeping them distinct is what makes the probe assertions discriminating — in
// prod both rows would often carry the SAME string, since a batch consumer group
// is execution-independent, and then only the row's existence distinguishes them.
const (
	pgStaleSinkGroup = "sink-aaaaaaaa-batch"
	pgLiveSinkGroup  = "rsync.sink-aaaaaaaa-batch"
)

// pgSeedStartupWindow reproduces the exact shape of the 08-18 false positive: a
// finished run that left a kafka_sink_worker row behind, and a live run that has
// started but not yet reached executor.go's upsertDependency call.
func pgSeedStartupWindow(t *testing.T, db *sql.DB) {
	t.Helper()
	pgPurge(t, db)
	t.Cleanup(func() { pgPurge(t, db) })

	mustExec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed %.60q: %v", q, err)
		}
	}
	mustExec(`INSERT INTO users (id, email, password_hash) VALUES ($1::uuid, $2, 'x')`,
		pgTestUserID, "batch-sink-presence-verify@example.invalid")
	mustExec(`INSERT INTO workspaces (id, name, slug, owner_id)
	          VALUES ($1::uuid, 'batch-sink-verify', 'batch-sink-verify', $2::uuid)`,
		pgTestWorkspaceID, pgTestUserID)
	mustExec(`INSERT INTO pipelines
	          (id, name, natural_language_request, workspace_id, status, sync_mode, cdc_mode, created_by)
	          VALUES ($1::uuid, 'batch sink verify', 'verify', $2::uuid, 'running', 'batch', NULL, $3::uuid)`,
		pgTestPipelineID, pgTestWorkspaceID, pgTestUserID)
	// The run that is over. Its row is the one that must stop being probed.
	mustExec(`INSERT INTO executions (id, pipeline_id, status, start_time, end_time)
	          VALUES ($1::uuid, $2::uuid, 'completed', NOW() - '4 days'::interval,
	                  NOW() - '4 days'::interval + '1 hour'::interval)`,
		pgPriorExecutionID, pgTestPipelineID)
	// The live run, mid-startup: api-gateway already wrote it as 'running'.
	mustExec(`INSERT INTO executions (id, pipeline_id, status, start_time, end_time)
	          VALUES ($1::uuid, $2::uuid, 'running', NOW() - '40 seconds'::interval, NULL)`,
		pgTestExecutionID, pgTestPipelineID)
	mustExec(`INSERT INTO pipeline_dependencies
	          (pipeline_id, execution_id, kind, identifier, created_at)
	          VALUES ($1::uuid, $2::uuid, 'kafka_sink_worker', $3, NOW() - '4 days'::interval)`,
		pgTestPipelineID, pgPriorExecutionID, pgStaleSinkGroup)
}

func pgSinkTargets(t *testing.T, db *sql.DB) [][2]string {
	t.Helper()
	rows, err := db.Query(runningBatchSinksQuery)
	if err != nil {
		// The failure mode this whole file exists for: #723 shipped a `text = uuid`
		// comparison green through sqlmock. A correlated LATERAL is the same class.
		t.Fatalf("runningBatchSinksQuery does not execute against real PostgreSQL: %v\n"+
			"probeBatchSinks logs this and returns, so the probe would be silently "+
			"disarmed in prod while every string-matching test stayed green", err)
	}
	defer rows.Close()

	var out [][2]string
	for rows.Next() {
		var pid, pname, execID, group string
		if err := rows.Scan(&pid, &pname, &execID, &group); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, [2]string{execID, group})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func pgSinkAbsentIssue(t *testing.T, db *sql.DB) bool {
	t.Helper()
	var n int
	err := db.QueryRow(`SELECT count(*) FROM sentinel_active_issues WHERE id = $1`,
		batchSinkAbsentIssueID(pgTestPipelineID)).Scan(&n)
	if err != nil {
		t.Fatalf("read sink-absent issue: %v", err)
	}
	return n > 0
}

// TestPGSinkProbeIgnoresAPriorRunsManifestRow is the assertion prod gave us the
// hard way on 2026-08-18: a CRITICAL "nothing is writing to the destination" on a
// run that was 40s old and perfectly healthy.
//
// Pre-fix this returns one row pairing the LIVE execution id with the PRIOR run's
// consumer group. That cross-execution pairing is the defect itself, and no mock
// can observe it — the mock is handed whatever rows the test author imagined.
func TestPGSinkProbeIgnoresAPriorRunsManifestRow(t *testing.T) {
	db := pgTestDB(t)
	pgSeedStartupWindow(t, db)

	if got := pgSinkTargets(t, db); len(got) != 0 {
		t.Fatalf("query returned %d row(s) during the startup window, want 0: %v\n"+
			"the manifest LATERAL paired execution %q with consumer group %q — a group "+
			"registered by a DIFFERENT run. This run has registered nothing yet, so there "+
			"is nothing to probe and no alarm to raise.",
			len(got), got, got[0][0], got[0][1])
	}

	s := pgSentinel(db)
	probe := staticSinkProbe(sinkStatusResp(sinkStatusNotFound), nil)
	s.probeBatchSinks(context.Background(), probe)

	if len(probe.gotGroups) != 0 {
		t.Errorf("probed %v during the startup window, want no probe at all", probe.gotGroups)
	}
	if pgSinkAbsentIssue(t, db) {
		t.Error("raised a CRITICAL sink_worker_absent issue against a healthy 40s-old run — " +
			"this is KI-BATCHSENTINEL-SINK-ABSENT-FALSE-POSITIVE reproducing")
	}
}

// TestPGSinkProbeStillProbesTheLiveRunsOwnRow is the positive denominator.
//
// Without it, "zero rows" above also passes for a broken seed, a wrong sync_mode,
// an over-eager purge, or a predicate that matches nothing at all — i.e. for a
// probe that has been disarmed rather than corrected. TestPGBareKeyStillDeletes
// exists for the same reason on the stall path.
func TestPGSinkProbeStillProbesTheLiveRunsOwnRow(t *testing.T) {
	db := pgTestDB(t)
	pgSeedStartupWindow(t, db)

	// The live run reaches upsertDependency. Note the stale row is still present:
	// the fix must prefer this one, not merely tolerate it.
	if _, err := db.Exec(`INSERT INTO pipeline_dependencies
	        (pipeline_id, execution_id, kind, identifier, created_at)
	        VALUES ($1::uuid, $2::uuid, 'kafka_sink_worker', $3, NOW())`,
		pgTestPipelineID, pgTestExecutionID, pgLiveSinkGroup); err != nil {
		t.Fatalf("seed live manifest row: %v", err)
	}

	got := pgSinkTargets(t, db)
	if len(got) != 1 || got[0][0] != pgTestExecutionID || got[0][1] != pgLiveSinkGroup {
		t.Fatalf("query = %v, want one row [%s %s] — the live run's own registration",
			got, pgTestExecutionID, pgLiveSinkGroup)
	}

	s := pgSentinel(db)
	probe := staticSinkProbe(sinkStatusResp(sinkStatusNotFound), nil)
	s.probeBatchSinks(context.Background(), probe)

	if len(probe.gotGroups) != 1 || probe.gotGroups[0] != pgLiveSinkGroup {
		t.Fatalf("probed %v, want [%s]", probe.gotGroups, pgLiveSinkGroup)
	}
	if !pgSinkAbsentIssue(t, db) {
		t.Error("the container answered not_found for the live run's own consumer group and " +
			"no issue was raised — the detector is disarmed, which is the failure this fix " +
			"must NOT introduce")
	}
}

// TestPGSinkProbeStillReadsUnscopedManifestRows pins a deliberate decision.
//
// migration 049:16 documents `execution_id UUID, -- NULL = applies to all
// executions`, and upsertDependency writes NULL whenever it is called with an
// empty execution id. For such a pipeline that row is the ONLY registration there
// is, so scoping it out would disarm the probe rather than narrow it. A later
// "tightening" of the LATERAL to `dep.execution_id = e.id` alone must fail here
// loudly instead of going quiet in prod.
func TestPGSinkProbeStillReadsUnscopedManifestRows(t *testing.T) {
	db := pgTestDB(t)
	pgSeedStartupWindow(t, db)

	if _, err := db.Exec(`DELETE FROM pipeline_dependencies WHERE pipeline_id = $1::uuid`,
		pgTestPipelineID); err != nil {
		t.Fatalf("clear manifest: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO pipeline_dependencies
	        (pipeline_id, execution_id, kind, identifier, created_at)
	        VALUES ($1::uuid, NULL, 'kafka_sink_worker', $2, NOW())`,
		pgTestPipelineID, pgLiveSinkGroup); err != nil {
		t.Fatalf("seed NULL-execution manifest row: %v", err)
	}

	got := pgSinkTargets(t, db)
	if len(got) != 1 || got[0][1] != pgLiveSinkGroup {
		t.Fatalf("query = %v, want the NULL-execution row [%s %s] — an unscoped row applies "+
			"to every execution and is the only registration such a pipeline has",
			got, pgTestExecutionID, pgLiveSinkGroup)
	}
}
