//go:build integration_pg

// Real-PostgreSQL coverage for KI-SILENTDROP-ACK-SUM-SPANS-CHUNKS: the
// destination-landing reconcile of a CHUNKED batch run.
//
// The table-driven suite in landed_reconcile_test.go proves the classification
// arithmetic, but it hands classifyLandedReconcile its numbers directly. The
// defect being fixed here is not in the arithmetic — it is in WHERE THE
// DENOMINATOR COMES FROM: totalRows counts one dispatch, the ack ledger sums the
// whole execution, and a continuation re-dispatch keeps the same execution id. So
// the fix stands or falls on a SQL query (sumDispatchedRows) that a pure unit test
// cannot exercise. sqlmock would not close the gap either: it matches statements
// as strings and never type-checks them, which is exactly how #723's `text = uuid`
// comparison shipped green — and both ledger tables here carry uuid columns
// (migration 043) that the Go code addresses with string parameters.
//
// Not part of the default suite — needs a live server:
//
//	docker run -d --name executor-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=pipeline_db -p 55440:5432 postgres:16
//	for m in api-gateway/migrations/*.sql; do
//	    docker exec -i executor-pg psql -U postgres -d pipeline_db -v ON_ERROR_STOP=1 -q < "$m"
//	done
//	EXECUTOR_PG_DSN='postgres://postgres:verify@localhost:55440/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/agents/executor/ -run PG -v
package executor

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/rsync-ai/shared/pgdriver"
)

// Fixed IDs so a crashed run leaves nothing to collide with: the seed purges this
// set before inserting rather than relying on cleanup having happened.
const (
	pgChunkUserID      = "bbbbbbbb-0000-4000-8000-000000000001"
	pgChunkWorkspaceID = "bbbbbbbb-0000-4000-8000-000000000002"
	pgChunkPipelineID  = "bbbbbbbb-0000-4000-8000-000000000003"
	pgChunkExecutionID = "bbbbbbbb-0000-4000-8000-000000000004"
	// An EARLIER run of the same pipeline. Its ledger rows must be invisible to
	// the run under test — see the scoping assertion in
	// TestChunkedReconcilePG_DenominatorSpansTheWholeExecution.
	pgChunkPriorExecutionID = "bbbbbbbb-0000-4000-8000-000000000005"
)

func pgChunkDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("EXECUTOR_PG_DSN")
	if dsn == "" {
		t.Skip("EXECUTOR_PG_DSN not set — see the file header for the two commands that provide one")
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

func pgChunkPurge(t *testing.T, db *sql.DB) {
	t.Helper()
	// pipelines CASCADEs to executions, pipeline_batch_outbox and
	// pipeline_batch_acks (migration 043 added both FKs), so one delete clears the
	// ledgers too.
	if _, err := db.Exec(`DELETE FROM pipelines WHERE id = $1::uuid`, pgChunkPipelineID); err != nil {
		t.Fatalf("purge pipeline: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM workspaces WHERE id = $1::uuid`, pgChunkWorkspaceID); err != nil {
		t.Fatalf("purge workspace: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM users WHERE id = $1::uuid`, pgChunkUserID); err != nil {
		t.Fatalf("purge user: %v", err)
	}
}

// pgSeedChunkedRun builds the exact shape the KI describes: ONE execution that
// dispatched 150,000 rows across five chunk continuations, of which only the
// first 100,000 were ever received and written by the sink. The final chunk's own
// dispatch was 50,000 rows — the number the pre-fix code compared against.
func pgSeedChunkedRun(t *testing.T, db *sql.DB) {
	t.Helper()
	pgChunkPurge(t, db)
	t.Cleanup(func() { pgChunkPurge(t, db) })

	mustExec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed %.70q: %v", q, err)
		}
	}
	mustExec(`INSERT INTO users (id, email, password_hash) VALUES ($1::uuid, $2, 'x')`,
		pgChunkUserID, "chunked-reconcile-verify@example.invalid")
	mustExec(`INSERT INTO workspaces (id, name, slug, owner_id)
	          VALUES ($1::uuid, 'chunked-reconcile', 'chunked-reconcile', $2::uuid)`,
		pgChunkWorkspaceID, pgChunkUserID)
	mustExec(`INSERT INTO pipelines
	          (id, name, natural_language_request, workspace_id, status, sync_mode, cdc_mode, created_by)
	          VALUES ($1::uuid, 'chunked reconcile verify', 'verify', $2::uuid, 'running', 'batch', NULL, $3::uuid)`,
		pgChunkPipelineID, pgChunkWorkspaceID, pgChunkUserID)
	mustExec(`INSERT INTO executions (id, pipeline_id, status, start_time, end_time)
	          VALUES ($1::uuid, $2::uuid, 'running', NOW() - INTERVAL '20 minutes', NULL)`,
		pgChunkExecutionID, pgChunkPipelineID)

	// A COMPLETED EARLIER RUN of the same pipeline, with its own ledger rows. Both
	// ledgers are keyed (pipeline_id, execution_id) and a pipeline accumulates a row
	// per batch per run forever, so a denominator scoped to the pipeline alone would
	// grow without bound and false-fail every run after the first. Nothing below may
	// count these 77,777 rows or their 3 batches.
	mustExec(`INSERT INTO executions (id, pipeline_id, status, start_time, end_time)
	          VALUES ($1::uuid, $2::uuid, 'completed', NOW() - INTERVAL '3 days', NOW() - INTERVAL '3 days')`,
		pgChunkPriorExecutionID, pgChunkPipelineID)
	for i, rows := range []int64{30000, 30000, 17777} {
		mustExec(`INSERT INTO pipeline_batch_outbox
		          (pipeline_id, execution_id, table_name, batch_offset, row_count, byte_size, storage_type, status)
		          VALUES ($1::uuid, $2::uuid, 'orders', $3, $4, 1024, 'inline', 'acked')`,
			pgChunkPipelineID, pgChunkPriorExecutionID, int64(i)*30000, rows)
		mustExec(`INSERT INTO pipeline_batch_acks
		          (pipeline_id, execution_id, table_name, batch_offset, rows_written, rows_read, storage_type)
		          VALUES ($1::uuid, $2::uuid, 'orders', $3, $4, $4, 'inline')`,
			pgChunkPipelineID, pgChunkPriorExecutionID, int64(i)*30000, rows)
	}

	// PRODUCER OUTBOX — dispatch truth. Chunks 1-4 (batch_offsets 0..99999) plus
	// the final chunk's single 50k batch, all confirmed 'produced'.
	outbox := []struct {
		offset int64
		rows   int64
		status string
	}{
		{0, 25000, "produced"},
		{25000, 25000, "produced"},
		{50000, 25000, "produced"},
		{75000, 25000, "produced"},
		{100000, 50000, "produced"}, // the final chunk — the rows that never landed
		// Noise that must NOT be counted as dispatched: a produce that errored and
		// one still awaiting confirmation. Counting either would inflate the
		// denominator and could turn a healthy run into a reported shortfall.
		{150000, 9999, "failed"},
		{160000, 8888, "pending"},
	}
	for _, o := range outbox {
		mustExec(`INSERT INTO pipeline_batch_outbox
		          (pipeline_id, execution_id, table_name, batch_offset, row_count, byte_size, storage_type, status)
		          VALUES ($1::uuid, $2::uuid, 'orders', $3, $4, 1024, 'inline', $5)`,
			pgChunkPipelineID, pgChunkExecutionID, o.offset, o.rows, o.status)
	}

	// ACK LEDGER — destination truth. Only chunks 1-4 were received and written.
	for _, off := range []int64{0, 25000, 50000, 75000} {
		mustExec(`INSERT INTO pipeline_batch_acks
		          (pipeline_id, execution_id, table_name, batch_offset, rows_written, rows_read, storage_type)
		          VALUES ($1::uuid, $2::uuid, 'orders', $3, 25000, 25000, 'inline')`,
			pgChunkPipelineID, pgChunkExecutionID, off)
	}
}

// TestChunkedReconcilePG_DenominatorSpansTheWholeExecution is the fix's load-bearing
// assertion. It reads BOTH ledgers out of a real database and shows that the two
// denominators disagree about the same run: the whole-execution dispatch total
// (150k, from the producer outbox) exposes the final chunk's loss, while the final
// chunk's own count (50k) is smaller than what earlier chunks already acked, so the
// loss is structurally invisible.
func TestChunkedReconcilePG_DenominatorSpansTheWholeExecution(t *testing.T) {
	db := pgChunkDB(t)
	pgSeedChunkedRun(t, db)
	ctx := context.Background()

	// 1. The new query executes against the real schema (uuid columns, string
	//    params) and counts only confirmed produces.
	dispatched, batches := sumDispatchedRows(ctx, db, pgChunkPipelineID, pgChunkExecutionID)
	if dispatched != 150000 {
		t.Fatalf("sumDispatchedRows = %d rows, want 150000 (the whole execution, excluding the failed and pending batches)", dispatched)
	}
	if batches != 5 {
		t.Fatalf("sumDispatchedRows = %d batches, want 5 ('failed' and 'pending' must not count as dispatched)", batches)
	}

	// 1b. Scoped to the EXECUTION, not the pipeline. The prior run's 77,777 acked
	//     rows share this pipeline_id; counting them would inflate the denominator by
	//     more than half and false-fail a perfectly healthy run — and would get worse
	//     with every run the pipeline ever makes. This is the assertion that fails if
	//     the `execution_id = $2` predicate is ever dropped.
	if prior, priorBatches := sumDispatchedRows(ctx, db, pgChunkPipelineID, pgChunkPriorExecutionID); prior != 77777 || priorBatches != 3 {
		t.Fatalf("prior execution seeded wrong: sumDispatchedRows = (%d rows, %d batches), want (77777, 3) — the scoping assertion above is not actually exercising anything", prior, priorBatches)
	}

	// 2. The ack ledger is unchanged and still sums the whole execution — and is
	//    execution-scoped for the same reason.
	landed, received, ackRows, sinkErr := sumBatchAcks(ctx, db, pgChunkPipelineID, pgChunkExecutionID)
	if landed != 100000 || received != 100000 || ackRows != 4 {
		t.Fatalf("sumBatchAcks = (landed %d, received %d, acks %d), want (100000, 100000, 4) — the prior run's 3 ack rows must not be counted", landed, received, ackRows)
	}

	// 3. FIXED denominator: the 50k shortfall is visible and the run is no longer
	//    a silent success.
	fixed := classifyLandedReconcile(dispatched, landed, received, ackRows, 5, 0, batches, sinkErr, pgChunkExecutionID)
	if !fixed.UnverifiedCompletion {
		t.Errorf("whole-execution denominator: UnverifiedCompletion = false, want true — 50,000 dispatched rows never reached the destination")
	}
	if fixed.AckEvidencedDrop {
		t.Errorf("whole-execution denominator: AckEvidencedDrop = true, want false — no sink error, so the shortfall is unconfirmed, not proven loss")
	}

	// 4. PRE-FIX denominator: same database, same ledgers, only the final chunk's
	//    own row count. Nothing fires — this is the bug, pinned.
	const finalChunkRows = int64(50000)
	preFix := classifyLandedReconcile(finalChunkRows, landed, received, ackRows, 5, 0, batches, sinkErr, pgChunkExecutionID)
	if preFix.UnverifiedCompletion || preFix.AckEvidencedDrop {
		t.Fatalf("pre-fix denominator unexpectedly flagged the run (%+v) — the paired comparison this test rests on is no longer valid", preFix)
	}
}

// TestChunkedReconcilePG_DrainWaitUsesTheWholeExecution runs the real bound-wait
// against the real ledger. With the whole-execution expectation it must NOT
// short-circuit as "drained" (100k landed of 150k dispatched); with the final
// chunk's expectation it exits immediately, which is precisely how the drop went
// unnoticed. The deadline is tiny so the negative case costs one poll, not 120s.
func TestChunkedReconcilePG_DrainWaitUsesTheWholeExecution(t *testing.T) {
	db := pgChunkDB(t)
	pgSeedChunkedRun(t, db)
	a := &Agent{db: db}
	ctx := context.Background()

	start := time.Now()
	landed, received, ackRows, _ := a.reconcileLandedRows(ctx, pgChunkPipelineID, pgChunkExecutionID, 150000, 100*time.Millisecond)
	waited := time.Since(start)
	if landed != 100000 || received != 100000 || ackRows != 4 {
		t.Fatalf("reconcileLandedRows = (landed %d, received %d, acks %d), want (100000, 100000, 4)", landed, received, ackRows)
	}
	if waited < 100*time.Millisecond {
		t.Errorf("returned after %v with the whole-execution expectation — it treated an undrained run as drained", waited)
	}

	// The pre-fix expectation is already satisfied by chunks 1-4, so the wait ends
	// on the first poll and the executor concludes the sink drained.
	start = time.Now()
	if _, _, _, _ = a.reconcileLandedRows(ctx, pgChunkPipelineID, pgChunkExecutionID, 50000, 100*time.Millisecond); time.Since(start) >= 100*time.Millisecond {
		t.Fatalf("the final-chunk expectation no longer short-circuits — the contrast this test rests on is gone")
	}
}
