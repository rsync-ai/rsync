//go:build integration_pg

// Real-PostgreSQL coverage for the single zombie sweep statement.
//
// The sqlmock suite proves the control flow and asserts the load-bearing
// predicates against the query TEXT. Neither is proof that the SQL runs: sqlmock
// matches statements as strings and never type-checks them, which is how the
// #723 `text = uuid` comparison shipped green.
//
// That gap is live here. Collapsing the two sweepers changed the statement's
// final SELECT from three scalar counts to `SELECT s.id, s.pipeline_id, (…), (…)
// FROM swept s` — a different result shape, mixing per-row columns with
// aggregate subqueries over sibling data-modifying CTEs. sqlmock will happily
// hand back whatever rows the test declares no matter what PostgreSQL would
// actually do with that. Only a real planner settles it.
//
// Not part of the default suite — needs a live server:
//
//	docker run -d --name heal-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=pipeline_db -p 55440:5432 postgres:16
//	for m in api-gateway/migrations/*.sql; do
//	    docker exec -i heal-pg psql -U postgres -d pipeline_db -v ON_ERROR_STOP=1 -q < "$m"
//	done
//	SENTINEL_PG_DSN='postgres://postgres:verify@localhost:55440/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/agents/heal/ -run PGZombie -v
package heal

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/rsync-ai/shared/pgdriver"
)

// Fixed IDs so a crashed run leaves nothing to collide with.
const (
	pgZombieUserID      = "cccccccc-0000-4000-8000-000000000001"
	pgZombieWorkspaceID = "cccccccc-0000-4000-8000-000000000002"
	pgZombieBatchPipeID = "cccccccc-0000-4000-8000-000000000003"
	pgZombieCDCPipeID   = "cccccccc-0000-4000-8000-000000000004"
	pgZombieBatchExecID = "cccccccc-0000-4000-8000-000000000005"
	pgZombieCDCExecID   = "cccccccc-0000-4000-8000-000000000006"
)

func pgZombiePurge(t *testing.T, db *sql.DB) {
	t.Helper()
	// pipelines cascades to executions, pipeline_progress and pipeline_run_events.
	for _, id := range []string{pgZombieBatchPipeID, pgZombieCDCPipeID} {
		if _, err := db.Exec(`DELETE FROM pipelines WHERE id = $1::uuid`, id); err != nil {
			t.Fatalf("purge pipeline %s: %v", id, err)
		}
	}
	if _, err := db.Exec(`DELETE FROM workspaces WHERE id = $1::uuid`, pgZombieWorkspaceID); err != nil {
		t.Fatalf("purge workspace: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM users WHERE id = $1::uuid`, pgZombieUserID); err != nil {
		t.Fatalf("purge user: %v", err)
	}
}

// pgSeedZombies creates two pipelines, each with one execution that has been
// status='running' with a NULL end_time for 10 hours — well past ZombieTimeout.
//
// One is a plain batch pipeline: nothing is streaming for it, so both its
// execution AND its pipelines row must be closed. The other is CDC: its
// execution is closed but the pipelines row must survive, because a CDC pipeline
// can legitimately keep streaming past its initial execution row.
func pgSeedZombies(t *testing.T, db *sql.DB) {
	t.Helper()
	pgZombiePurge(t, db)
	t.Cleanup(func() { pgZombiePurge(t, db) })

	mustExec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed %.60q: %v", q, err)
		}
	}
	mustExec(`INSERT INTO users (id, email, password_hash) VALUES ($1::uuid, $2, 'x')`,
		pgZombieUserID, "zombie-sweep-verify@example.invalid")
	mustExec(`INSERT INTO workspaces (id, name, slug, owner_id)
	          VALUES ($1::uuid, 'zombie-sweep-verify', 'zombie-sweep-verify', $2::uuid)`,
		pgZombieWorkspaceID, pgZombieUserID)

	mustExec(`INSERT INTO pipelines
	          (id, name, natural_language_request, workspace_id, status, sync_mode, created_by)
	          VALUES ($1::uuid, 'zombie batch', 'verify', $2::uuid, 'running', 'batch', $3::uuid)`,
		pgZombieBatchPipeID, pgZombieWorkspaceID, pgZombieUserID)
	mustExec(`INSERT INTO pipelines
	          (id, name, natural_language_request, workspace_id, status, sync_mode, cdc_mode, created_by)
	          VALUES ($1::uuid, 'zombie cdc', 'verify', $2::uuid, 'running', 'cdc', 'streaming_only', $3::uuid)`,
		pgZombieCDCPipeID, pgZombieWorkspaceID, pgZombieUserID)

	for _, e := range []struct{ execID, pipeID string }{
		{pgZombieBatchExecID, pgZombieBatchPipeID},
		{pgZombieCDCExecID, pgZombieCDCPipeID},
	} {
		mustExec(`INSERT INTO executions (id, pipeline_id, status, start_time, end_time)
		          VALUES ($1::uuid, $2::uuid, 'running', NOW() - INTERVAL '10 hours', NULL)`,
			e.execID, e.pipeID)
		mustExec(`INSERT INTO pipeline_progress (pipeline_id, execution_id, status, current_stage)
		          VALUES ($1::uuid, $2::uuid, 'running', 'execution')`,
			e.pipeID, e.execID)
	}
}

func pgZombiePipelineStatus(t *testing.T, db *sql.DB, pipelineID string) string {
	t.Helper()
	var status string
	if err := db.QueryRow(`SELECT status FROM pipelines WHERE id = $1::uuid`, pipelineID).
		Scan(&status); err != nil {
		t.Fatalf("read pipeline status: %v", err)
	}
	return status
}

func pgZombieExecStatus(t *testing.T, db *sql.DB, execID string) string {
	t.Helper()
	var status string
	if err := db.QueryRow(`SELECT status FROM executions WHERE id = $1::uuid`, execID).
		Scan(&status); err != nil {
		t.Fatalf("read execution status: %v", err)
	}
	return status
}

// TestPGZombieSweepReturnsOneRowPerReapWithCountsAttached is the shape proof.
//
// The reshaped final SELECT is the part sqlmock structurally cannot check. Two
// executions are seeded, so the statement must come back with two rows carrying
// the right (id, pipeline_id) pairs, and the aggregate subqueries over the
// sibling CTEs must evaluate on every one of them.
func TestPGZombieSweepReturnsOneRowPerReapWithCountsAttached(t *testing.T) {
	db := pgHealDB(t)
	pgSeedZombies(t, db)

	rows, err := db.Query(sweepZombiesQuery, ZombieTimeout.String(), AbandonedParkTimeout.String())
	if err != nil {
		t.Fatalf("sweepZombiesQuery did not execute against PostgreSQL: %v\n\n"+
			"The sqlmock suite cannot catch this — it matches SQL as a string and never "+
			"submits it to a planner.", err)
	}
	defer rows.Close()

	got := map[string]string{} // execID -> pipelineID
	var reconciled, closed int
	for rows.Next() {
		var execID, pipelineID string
		if err := rows.Scan(&execID, &pipelineID, &reconciled, &closed); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[execID] = pipelineID
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("row iteration: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("swept %d executions, want 2 (%v)", len(got), got)
	}
	if got[pgZombieBatchExecID] != pgZombieBatchPipeID {
		t.Errorf("batch reap carries pipeline_id %q, want %q — the heal event would be "+
			"attributed to the wrong pipeline's timeline",
			got[pgZombieBatchExecID], pgZombieBatchPipeID)
	}
	if got[pgZombieCDCExecID] != pgZombieCDCPipeID {
		t.Errorf("cdc reap carries pipeline_id %q, want %q",
			got[pgZombieCDCExecID], pgZombieCDCPipeID)
	}
	if reconciled != 2 {
		t.Errorf("reconciled = %d, want 2 — both pipeline_progress rows were 'running'", reconciled)
	}
	if closed != 1 {
		t.Errorf("pipelines_closed = %d, want 1 — the batch pipeline closes, the CDC one is "+
			"carved out", closed)
	}
}

// TestPGZombieSweepFromTheHourlySweeperClosesTheBatchPipelineRow is the fix
// itself, run end-to-end through the caller that used to get it wrong.
//
// ZombieExecutionSweeper.Sweep is the hourly ticker. It used to run its own
// hand-written near-copy of the statement with no pipelines_closed CTE, so every
// row it won before the 60s sweep saw it had its execution closed and its
// pipelines row left at status='running' with nothing left to close it — KI-3
// verbatim. Reverting Sweep to that copy turns the first assertion below red.
func TestPGZombieSweepFromTheHourlySweeperClosesTheBatchPipelineRow(t *testing.T) {
	db := pgHealDB(t)
	pgSeedZombies(t, db)

	s := &ZombieExecutionSweeper{DB: db}
	if err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if got := pgZombiePipelineStatus(t, db, pgZombieBatchPipeID); got != "failed" {
		t.Errorf("batch pipeline status = %q after the hourly sweep, want \"failed\".\n\n"+
			"Its only execution was just reaped and nothing is streaming for a batch run, "+
			"so no second writer is coming to close it: the pipeline sits 'running' forever "+
			"while its execution says 'failed'.", got)
	}
	if got := pgZombiePipelineStatus(t, db, pgZombieCDCPipeID); got != "running" {
		t.Errorf("cdc pipeline status = %q, want \"running\" — the CDC carve-out is gone and "+
			"a live stream was just closed out from under itself", got)
	}
	if got := pgZombieExecStatus(t, db, pgZombieCDCExecID); got != "failed" {
		t.Errorf("cdc execution status = %q, want \"failed\" — the carve-out protects the "+
			"pipelines row only, not the stranded execution row", got)
	}
}

// TestPGZombieSweepWritesAHealEventPerReap runs the heal-event write against a
// real schema. writeHealEvent passes pipeline_id/execution_id as Go strings into
// uuid columns, which is precisely the class of mismatch sqlmock waves through.
func TestPGZombieSweepWritesAHealEventPerReap(t *testing.T) {
	db := pgHealDB(t)
	pgSeedZombies(t, db)

	if err := (&ZombieExecutionSweeper{DB: db}).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	for _, c := range []struct{ pipeID, execID string }{
		{pgZombieBatchPipeID, pgZombieBatchExecID},
		{pgZombieCDCPipeID, pgZombieCDCExecID},
	} {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pipeline_run_events
		                       WHERE pipeline_id = $1::uuid AND execution_id = $2::uuid
		                         AND event_type = 'healer_zombie_swept'`,
			c.pipeID, c.execID).Scan(&n); err != nil {
			t.Fatalf("count heal events: %v", err)
		}
		if n != 1 {
			t.Errorf("healer_zombie_swept events for execution %s = %d, want 1 — the reap is "+
				"invisible on the run timeline", c.execID, n)
		}
	}
}

// TestPGZombieSweepIsIdempotentAcrossBothCallers pins the concurrency claim in
// Sweep's doc comment. The 60s sweep and the hourly ticker now run the SAME
// statement in the same process; its UPDATE guards on status='running', so the
// second pass must find nothing rather than double-reaping or re-stamping.
func TestPGZombieSweepIsIdempotentAcrossBothCallers(t *testing.T) {
	db := pgHealDB(t)
	pgSeedZombies(t, db)
	ctx := context.Background()

	(&HealWorker{DB: db}).sweepZombies(ctx)

	var completedAt sql.NullTime
	if err := db.QueryRow(`SELECT completed_at FROM pipelines WHERE id = $1::uuid`,
		pgZombieBatchPipeID).Scan(&completedAt); err != nil {
		t.Fatalf("read completed_at: %v", err)
	}
	if !completedAt.Valid {
		t.Fatal("completed_at was not stamped by the first sweep")
	}

	if err := (&ZombieExecutionSweeper{DB: db}).Sweep(ctx); err != nil {
		t.Fatalf("second Sweep: %v", err)
	}

	rows, err := db.Query(sweepZombiesQuery, ZombieTimeout.String(), AbandonedParkTimeout.String())
	if err != nil {
		t.Fatalf("third pass: %v", err)
	}
	swept := rows.Next()
	rows.Close()
	if swept {
		t.Error("a later pass reaped a row the first pass already closed — the status='running' " +
			"guard is gone and the sweep is no longer safe to run from two tickers")
	}

	var after sql.NullTime
	if err := db.QueryRow(`SELECT completed_at FROM pipelines WHERE id = $1::uuid`,
		pgZombieBatchPipeID).Scan(&after); err != nil {
		t.Fatalf("re-read completed_at: %v", err)
	}
	if !after.Time.Equal(completedAt.Time) {
		t.Errorf("completed_at moved from %s to %s on a re-run — the COALESCE guard is gone",
			completedAt.Time, after.Time)
	}
}

// TestPGZombieSweepBoundsTheHITLPark is the fix for the 382-hour pipeline, proved
// against a real planner.
//
// Both fixtures are byte-identical except for one column: how long ago the
// pipeline_progress row was last touched. Both are parked at
// status='waiting_for_user' with executions.status='running' and a NULL end_time,
// and both are well past ZombieTimeout — so under the old unbounded guard the sweep
// skipped BOTH, forever, and the abandoned one showed "running" until a human went
// looking.
//
// The query-text assertion in worker_zombie_sweep_test.go cannot establish this.
// sqlmock matches SQL as a string and never runs it, which is exactly how #723's
// `text = uuid` shipped green. Only PostgreSQL settles whether the interval
// comparison actually partitions these two rows.
//
// Asserting BOTH directions is what makes the test meaningful: reaping the stale
// park is the fix, and NOT reaping the fresh one is the guard the fix must not
// break. A test that only checked the first would pass just as well if the guard
// had been deleted outright — which would fail live runs mid-park, the far worse
// bug this guard was added to prevent.
func TestPGZombieSweepBoundsTheHITLPark(t *testing.T) {
	db := pgHealDB(t)
	pgSeedZombies(t, db)
	ctx := context.Background()

	mustExec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("park fixture %.60q: %v", q, err)
		}
	}

	// pipeline_progress carries a BEFORE UPDATE trigger
	// (update_pipeline_progress_updated_at) that stamps updated_at = NOW() on every
	// write, so a backdated value cannot be set while it is armed — the first draft
	// of this test failed for exactly that reason, and only against a real server:
	// sqlmock has no triggers, so it would have reported success.
	//
	// That trigger is also why the guard keys off updated_at and not a hand-written
	// park timestamp. It makes updated_at a "last touched, by anyone" stamp that no
	// writer can forget to maintain, which turns the predicate into: this row has
	// gone AbandonedParkTimeout without a single write while claiming to wait for a
	// person. A park that some live component is still tending keeps its row warm
	// and stays protected; only a genuinely untended one ages out. Disabling the
	// trigger here fabricates that age directly, which is the one thing a test
	// cannot do by waiting.
	mustExec(`ALTER TABLE pipeline_progress DISABLE TRIGGER update_pipeline_progress_updated_at`)
	defer mustExec(`ALTER TABLE pipeline_progress ENABLE TRIGGER update_pipeline_progress_updated_at`)

	// Park both seeded runs. The batch one parked an hour ago (a real user is
	// plausibly still deciding); the CDC one parked well past the ceiling, which
	// no live workflow can do — awaitHITLSignal would have failed it at 24h.
	mustExec(`UPDATE pipeline_progress SET status = 'waiting_for_user', updated_at = NOW() - INTERVAL '1 hour'
	          WHERE execution_id = $1::uuid`, pgZombieBatchExecID)
	mustExec(`UPDATE pipeline_progress SET status = 'waiting_for_user', updated_at = NOW() - $2::interval
	          WHERE execution_id = $1::uuid`, pgZombieCDCExecID, (AbandonedParkTimeout + time.Hour).String())

	(&HealWorker{DB: db}).sweepZombies(ctx)

	if got := pgZombieExecStatus(t, db, pgZombieBatchExecID); got != "running" {
		t.Errorf("a run parked 1h ago was swept to %q — the HITL guard no longer protects live "+
			"parks, so any user who takes a moment to pick tables gets their run failed underneath them", got)
	}
	if got := pgZombieExecStatus(t, db, pgZombieCDCExecID); got != "failed" {
		t.Errorf("a run parked past AbandonedParkTimeout (%s) is still %q — its Temporal workflow "+
			"cannot fire a timer, and this sweep is the only thing that would ever close it, so it "+
			"shows 'running' indefinitely (prod: 382h)", AbandonedParkTimeout, got)
	}
}
