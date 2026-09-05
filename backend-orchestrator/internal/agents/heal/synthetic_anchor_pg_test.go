//go:build integration_pg

// Real-PostgreSQL proof that the healer leaves the sink's synthetic CDC audit row alone.
//
// The unit suite (synthetic_anchor_test.go) proves the predicate is in the SQL. It cannot
// prove the predicate PARTITIONS anything: sqlmock returns whatever rows the test declares,
// so a query that excludes nothing looks identical to one that excludes exactly the anchor.
// Only a planner over real rows settles that, and the distinction here is one row against
// another in the same table on the same pipeline.
//
// Same server as zombie_sweep_pg_test.go — see that file's header for the two commands:
//
//	SENTINEL_PG_DSN='postgres://postgres:verify@localhost:55440/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/agents/heal/ -run PGSynthetic -v
package heal

import (
	"context"
	"database/sql"
	"testing"
)

const (
	pgAnchorUserID      = "cccccccc-0000-4000-8000-000000000011"
	pgAnchorWorkspaceID = "cccccccc-0000-4000-8000-000000000012"
	// The anchor's execution id IS its pipeline id — that identity is the whole
	// discriminator, so the pipeline id doubles as the synthetic execution id.
	pgAnchorPipeID     = "cccccccc-0000-4000-8000-000000000013"
	pgAnchorRealExecID = "cccccccc-0000-4000-8000-000000000014"
)

func pgAnchorPurge(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`DELETE FROM pipelines WHERE id = $1::uuid`, pgAnchorPipeID); err != nil {
		t.Fatalf("purge pipeline: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM workspaces WHERE id = $1::uuid`, pgAnchorWorkspaceID); err != nil {
		t.Fatalf("purge workspace: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM users WHERE id = $1::uuid`, pgAnchorUserID); err != nil {
		t.Fatalf("purge user: %v", err)
	}
}

// pgSeedAnchor builds the two-sided fixture: ONE CDC pipeline carrying both an
// anchor and a genuine run, aged identically past ZombieTimeout, differing in
// nothing the sweep looks at except id = pipeline_id.
//
// The pairing is the point. A fixture with only the anchor would pass just as well
// if the sweep had been disabled outright, and disabling it would strand every real
// stuck run — the far worse bug. The real execution is the control that says the
// sweep still works.
func pgSeedAnchor(t *testing.T, db *sql.DB) {
	t.Helper()
	pgAnchorPurge(t, db)
	t.Cleanup(func() { pgAnchorPurge(t, db) })

	mustExec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed %.60q: %v", q, err)
		}
	}
	mustExec(`INSERT INTO users (id, email, password_hash) VALUES ($1::uuid, $2, 'x')`,
		pgAnchorUserID, "cdc-anchor-verify@example.invalid")
	mustExec(`INSERT INTO workspaces (id, name, slug, owner_id)
	          VALUES ($1::uuid, 'cdc-anchor-verify', 'cdc-anchor-verify', $2::uuid)`,
		pgAnchorWorkspaceID, pgAnchorUserID)
	mustExec(`INSERT INTO pipelines
	          (id, name, natural_language_request, workspace_id, status, sync_mode, cdc_mode, created_by)
	          VALUES ($1::uuid, 'cdc anchor', 'verify', $2::uuid, 'running', 'cdc', 'streaming_only', $3::uuid)`,
		pgAnchorPipeID, pgAnchorWorkspaceID, pgAnchorUserID)

	// The anchor, exactly as ensureExecutionRowForCDCAudit writes it: id = pipeline_id,
	// trigger_source='cdc', 'running' with no end_time, and never closed by anyone.
	mustExec(`INSERT INTO executions (id, pipeline_id, status, trigger_source, start_time, end_time)
	          VALUES ($1::uuid, $1::uuid, 'running', 'cdc', NOW() - INTERVAL '10 hours', NULL)`,
		pgAnchorPipeID)
	// A genuine run on the same pipeline, equally stale. This one IS a zombie.
	mustExec(`INSERT INTO executions (id, pipeline_id, status, trigger_source, start_time, end_time)
	          VALUES ($1::uuid, $2::uuid, 'running', 'manual', NOW() - INTERVAL '10 hours', NULL)`,
		pgAnchorRealExecID, pgAnchorPipeID)
}

// TestPGSyntheticAnchorSurvivesTheZombieSweep is the fix.
//
// Before it, the anchor matched the sweep's WHERE clause by construction — running,
// no end_time, older than ZombieTimeout — and there was nothing in the executions
// UPDATE to stop it. (The CDC carve-out people reach for is in the pipelines_closed
// CTE and guards the `pipelines` row; this fixture proves the two are independent by
// asserting both.) Every healthy CDC stream therefore acquired a permanent, fabricated
// "zombie: execution timed out" failure at the 4h mark, which nothing ever cleared:
// ensureExecutionRowForCDCAudit is ON CONFLICT DO NOTHING, so the sink never reopens it.
func TestPGSyntheticAnchorSurvivesTheZombieSweep(t *testing.T) {
	db := pgHealDB(t)
	pgSeedAnchor(t, db)

	(&HealWorker{DB: db}).sweepZombies(context.Background())

	var status string
	var endTime sql.NullTime
	var errMsg sql.NullString
	if err := db.QueryRow(`SELECT status, end_time, error_message FROM executions WHERE id = $1::uuid`,
		pgAnchorPipeID).Scan(&status, &endTime, &errMsg); err != nil {
		t.Fatalf("read anchor: %v", err)
	}

	if status != "running" {
		t.Errorf("the CDC audit anchor was swept to %q. It is not a run — it is the row the sink "+
			"hangs every ack and transform log off, and it is never closed by design, so reaping it "+
			"stamps a fabricated failure on a stream that is working.", status)
	}
	if endTime.Valid {
		t.Errorf("the anchor was given end_time=%s — a terminal row, which then becomes eligible "+
			"for the candidate sweep and the connector-health rollup", endTime.Time)
	}
	if errMsg.Valid && errMsg.String != "" {
		t.Errorf("the anchor carries error_message %q, which the diagnoser recognises as its own "+
			"handwriting and turns into a HITL request to re-run a healthy pipeline", errMsg.String)
	}

	// The control: the sweep must still do its job on the same pipeline, in the same pass.
	if got := pgZombieExecStatus(t, db, pgAnchorRealExecID); got != "failed" {
		t.Errorf("a genuine 10h-old running execution on the same CDC pipeline is still %q — the "+
			"exclusion is too broad and real stuck runs will never be closed", got)
	}
	// And the pipelines row is untouched either way: that is the pre-existing CDC
	// carve-out doing its own job, independently of this fix.
	if got := pgZombiePipelineStatus(t, db, pgAnchorPipeID); got != "running" {
		t.Errorf("cdc pipeline status = %q, want \"running\"", got)
	}
}

// TestPGSyntheticAnchorIsNotAHealCandidate closes the second door, for the anchors
// that earlier releases already reaped. Those rows are terminal and old, which is
// exactly what sweepCandidates hunts for — so without its own exclusion the healer
// would keep diagnosing them long after the sweep stopped creating them.
func TestPGSyntheticAnchorIsNotAHealCandidate(t *testing.T) {
	db := pgHealDB(t)
	pgSeedAnchor(t, db)

	// Simulate the damage a pre-fix release left behind: both rows already reaped.
	if _, err := db.Exec(`UPDATE executions
	                      SET status = 'failed', end_time = NOW() - INTERVAL '9 hours',
	                          error_message = 'zombie: execution timed out with no end_time (healer cleanup)'
	                      WHERE pipeline_id = $1::uuid`, pgAnchorPipeID); err != nil {
		t.Fatalf("age both rows into candidates: %v", err)
	}

	rows, err := db.Query(sweepCandidatesQuery, GracePeriod.String(), RehealCooldown.String(), BatchSize)
	if err != nil {
		t.Fatalf("sweepCandidatesQuery did not execute against PostgreSQL: %v", err)
	}
	defer rows.Close()

	seen := map[string]bool{}
	for rows.Next() {
		var id, pipelineID, status, errMsg, srcType, dstType string
		var readRows, landedRows, tablesNoLanded, tablesObserved int64
		if err := rows.Scan(&id, &pipelineID, &status, &errMsg, &srcType, &dstType,
			&readRows, &landedRows, &tablesNoLanded, &tablesObserved); err != nil {
			t.Fatalf("scan candidate: %v", err)
		}
		seen[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("row iteration: %v", err)
	}

	if seen[pgAnchorPipeID] {
		t.Error("the CDC audit anchor came back as a heal candidate — the healer will diagnose the " +
			"zombie message it wrote itself and ask an operator to approve re-running a pipeline " +
			"that never failed")
	}
	if !seen[pgAnchorRealExecID] {
		t.Error("the genuine failed execution is no longer a heal candidate — the exclusion is too " +
			"broad and real failures on CDC pipelines will never be diagnosed")
	}
}
