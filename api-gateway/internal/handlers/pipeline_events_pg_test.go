//go:build integration_pg

// Real-PostgreSQL coverage for the run-event page query.
//
// Row ORDER is the entire contract of GET /pipelines/:id/events — the endpoint
// returns a *window* (default 60–100 rows) over an append-only stream, so
// whichever rows sort last are not "later", they are invisible. sqlmock cannot
// test that: it matches statements as strings and never executes them, so a
// test written against it would assert the ORDER BY *text* and pass for any
// ordering, correct or not.
//
// The bug these tests were written for: 343 of 1614 prod events (21%) carry
// seq IS NULL — every SENTINEL_ALERT and DATA_PLANE_METRICS (the projector only
// assigns a seq when the event has an execution_id, event_projector.go:598) and
// every healer-written row (heal/worker.go:901, :942 and heal/executors.go:325
// insert with no seq column at all). `ORDER BY e.seq DESC NULLS LAST` sorts all
// of them below all 1,271 numbered rows, so the alerts and the healer's own
// verdicts were unreachable in a 60-row window that reached back only to the
// most recent numbered events.
//
// Not part of the default suite — needs a live server:
//
//	docker run -d --name evt-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=pipeline_db -p 55441:5432 postgres:16
//	for m in api-gateway/migrations/*.sql; do
//	    docker exec -i evt-pg psql -U postgres -d pipeline_db -v ON_ERROR_STOP=1 -q < "$m"
//	done
//	EVENTS_PG_DSN='postgres://postgres:verify@localhost:55441/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run PG_PipelineEvents -v
package handlers

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/rsync-ai/shared/pgdriver"
)

// Fixed IDs so a crashed run leaves nothing to collide with: each test deletes
// this set before seeding rather than relying on cleanup having happened.
const (
	pgEvtWorkspaceID = "cccccccc-0000-4000-8000-000000000001"
	pgEvtPipelineID  = "cccccccc-0000-4000-8000-000000000002"
	pgEvtExecutionID = "cccccccc-0000-4000-8000-000000000003"
	pgEvtUserID      = "cccccccc-0000-4000-8000-000000000004"
)

func pgEvtDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("EVENTS_PG_DSN")
	if dsn == "" {
		t.Skip("EVENTS_PG_DSN not set — see the file header for the two commands that provide one")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func pgEvtSeedPipeline(t *testing.T, db *sql.DB) {
	t.Helper()
	// Order matters: the child rows carry an ON DELETE CASCADE FK to pipelines,
	// but deleting the parent first is the only way to guarantee no stranded
	// child survives a schema change that drops the FK.
	if _, err := db.Exec(`DELETE FROM pipeline_run_events WHERE pipeline_id = $1`, pgEvtPipelineID); err != nil {
		t.Fatalf("clean events: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM pipelines WHERE id = $1`, pgEvtPipelineID); err != nil {
		t.Fatalf("clean pipeline: %v", err)
	}
	// pipelines.workspace_id → workspaces.id → users.id, both enforced.
	if _, err := db.Exec(`
		INSERT INTO users (id, email, password_hash, name)
		VALUES ($1, 'pg-events-probe@example.invalid', 'x', 'pg events probe')
		ON CONFLICT (id) DO NOTHING
	`, pgEvtUserID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO workspaces (id, name, slug, owner_id)
		VALUES ($1, 'pg events probe', 'pg-events-probe', $2)
		ON CONFLICT (id) DO NOTHING
	`, pgEvtWorkspaceID, pgEvtUserID); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO pipelines (id, name, natural_language_request, status, workspace_id)
		VALUES ($1, 'pg-events-order-probe', 'probe', 'active', $2)
	`, pgEvtPipelineID, pgEvtWorkspaceID); err != nil {
		t.Fatalf("seed pipeline: %v", err)
	}
	// pipeline_run_events.execution_id → executions.id is enforced, so a stage
	// event cannot be seeded without a real run to hang it on.
	if _, err := db.Exec(`
		INSERT INTO executions (id, pipeline_id, status)
		VALUES ($1, $2, 'completed')
		ON CONFLICT (id) DO NOTHING
	`, pgEvtExecutionID, pgEvtPipelineID); err != nil {
		t.Fatalf("seed execution: %v", err)
	}
}

// pgEvtInsert writes one event. seq nil ⇒ the NULL-seq shape; occurredAt nil ⇒
// the healer shape, which supplies neither seq nor occurred_at and leans on
// received_at DEFAULT NOW() (heal/worker.go:901).
func pgEvtInsert(t *testing.T, db *sql.DB, eventID string, seq *int64, eventType string, occurredAt *time.Time, receivedAt time.Time) {
	t.Helper()
	if _, err := db.Exec(`
		INSERT INTO pipeline_run_events
		    (pipeline_id, execution_id, event_id, seq, event_type, occurred_at, received_at, payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7, '{}'::jsonb)
	`, pgEvtPipelineID, pgEvtExecutionID, eventID, seq, eventType, occurredAt, receivedAt); err != nil {
		t.Fatalf("insert %s: %v", eventID, err)
	}
}

// pgEvtQueryIDs runs a built query and returns the event ids in row order plus
// the cursor that points just past the last row — the same derivation the
// handler performs, so a cursor bug shows up here rather than only in the UI.
func pgEvtQueryIDs(t *testing.T, db *sql.DB, query string, args []any) ([]string, pipelineEventsCursor) {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("query: %v\nSQL: %s", err, query)
	}
	defer rows.Close()

	var ids []string
	var last pipelineEventsCursor
	for rows.Next() {
		var (
			pid, eventID, eventType     string
			execID, stageID, stageGroup sql.NullString
			severity, traceID           sql.NullString
			seq                         sql.NullInt64
			occurredAt                  sql.NullTime
			receivedAt                  time.Time
			payload                     []byte
		)
		if err := rows.Scan(&pid, &execID, &eventID, &seq, &eventType, &stageID,
			&stageGroup, &severity, &traceID, &occurredAt, &receivedAt, &payload); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, eventID)
		last = cursorFromRow(eventID, seq, occurredAt, receivedAt)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return ids, last
}

func pgEvtRun(t *testing.T, db *sql.DB, cur pipelineEventsCursor, limit int) []string {
	t.Helper()
	query, args := buildPipelineEventsQuery(pgEvtPipelineID, pipelineEventsFilter{}, cur, limit)
	ids, _ := pgEvtQueryIDs(t, db, query, args)
	return ids
}

// TestPG_PipelineEventsNullSeqRowIsReachable is the reproducer for the prod
// defect. One sentinel alert — the single most important row in the stream,
// because it is the only thing that says something is wrong — is the NEWEST
// event by time and carries no seq. A 60-row window must show it first.
func TestPG_PipelineEventsNullSeqRowIsReachable(t *testing.T) {
	db := pgEvtDB(t)
	pgEvtSeedPipeline(t, db)

	base := time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC)

	// 60 ordinary numbered stage events, oldest → newest.
	for i := 1; i <= 60; i++ {
		s := int64(i)
		ts := base.Add(time.Duration(i) * time.Second)
		pgEvtInsert(t, db, fmt.Sprintf("stage-%02d", i), &s, "STAGE_PROGRESS", &ts, ts)
	}

	// The alert: newest by wall-clock, no seq — exactly what the projector
	// writes for a pipeline-scoped event (no execution_id ⇒ no seq assigned).
	alertTS := base.Add(2 * time.Hour)
	pgEvtInsert(t, db, "sentinel-alert-1", nil, "SENTINEL_ALERT", &alertTS, alertTS)

	// And the healer shape: no seq AND no occurred_at, only received_at.
	healTS := base.Add(3 * time.Hour)
	pgEvtInsert(t, db, "healer-decision-1", nil, "healer_decision", nil, healTS)

	ids := pgEvtRun(t, db, pipelineEventsCursor{}, 60)

	if len(ids) != 60 {
		t.Fatalf("expected a full 60-row window, got %d", len(ids))
	}
	// Positive control: the window is not empty and does contain the numbered
	// rows, so a miss below is a miss, not an empty result set.
	if ids[len(ids)-1] == "" {
		t.Fatalf("empty event id in window — seeding did not take")
	}

	if ids[0] != "healer-decision-1" {
		t.Errorf("newest row should be the healer event (received_at %s); window starts with %q",
			healTS.Format(time.RFC3339), ids[0])
	}
	if len(ids) > 1 && ids[1] != "sentinel-alert-1" {
		t.Errorf("second row should be the sentinel alert (occurred_at %s); got %q",
			alertTS.Format(time.RFC3339), ids[1])
	}
}

// TestPG_PipelineEventsCursorIsTotalAndTerminates walks the whole stream one
// page at a time. Two properties, both broken by a seq-only cursor over a
// stream that contains NULL seqs: every row appears exactly once, and the walk
// ends. `AND (e.seq IS NULL OR e.seq < $n)` never excludes a NULL-seq row, so
// under the old cursor those rows are re-served on every page forever.
func TestPG_PipelineEventsCursorIsTotalAndTerminates(t *testing.T) {
	db := pgEvtDB(t)
	pgEvtSeedPipeline(t, db)

	base := time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC)
	const total = 25
	want := make(map[string]bool, total)
	for i := 1; i <= total; i++ {
		ts := base.Add(time.Duration(i) * time.Second)
		id := fmt.Sprintf("evt-%02d", i)
		want[id] = true
		if i%3 == 0 {
			// Every third row is NULL-seq — the mix that exists in prod.
			pgEvtInsert(t, db, id, nil, "SENTINEL_ALERT", &ts, ts)
		} else {
			s := int64(i)
			pgEvtInsert(t, db, id, &s, "STAGE_PROGRESS", &ts, ts)
		}
	}

	const pageSize = 10
	seen := map[string]int{}
	var cur pipelineEventsCursor

	// 25 rows at 10/page ⇒ 3 pages. The loop bound is a runaway guard, and
	// tripping it is itself the failure the old seq-only cursor produces.
	for page := 0; ; page++ {
		if page >= 10 {
			t.Fatalf("cursor never terminated: still serving pages after %d distinct rows", len(seen))
		}
		query, args := buildPipelineEventsQuery(pgEvtPipelineID, pipelineEventsFilter{}, cur, pageSize)
		ids, last := pgEvtQueryIDs(t, db, query, args)
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			seen[id]++
		}
		if len(ids) < pageSize {
			break
		}
		cur = last
	}

	for id := range want {
		switch seen[id] {
		case 1:
			// exactly once — correct
		case 0:
			t.Errorf("row %s was never returned by any page", id)
		default:
			t.Errorf("row %s was returned %d times (duplicate page content)", id, seen[id])
		}
	}
	for id := range seen {
		if !want[id] {
			t.Errorf("unexpected row %s in results", id)
		}
	}
}

// pgEvtExecution upserts a run with a fixed window. end nil ⇒ still running.
func pgEvtExecution(t *testing.T, db *sql.DB, id string, start time.Time, end *time.Time) {
	t.Helper()
	if _, err := db.Exec(`
		INSERT INTO executions (id, pipeline_id, status, start_time, end_time)
		VALUES ($1, $2, 'completed', $3, $4)
		ON CONFLICT (id) DO UPDATE SET start_time = EXCLUDED.start_time, end_time = EXCLUDED.end_time
	`, id, pgEvtPipelineID, start, end); err != nil {
		t.Fatalf("seed execution %s: %v", id, err)
	}
}

// pgEvtInsertRow writes an event with the fields the log filters read.
// execID "" ⇒ NULL (the sentinel/healer shape); severity "" ⇒ NULL.
func pgEvtInsertRow(t *testing.T, db *sql.DB, eventID, execID, eventType, severity, payload string, at time.Time) {
	t.Helper()
	var exec, sev any
	if execID != "" {
		exec = execID
	}
	if severity != "" {
		sev = severity
	}
	if payload == "" {
		payload = "{}"
	}
	if _, err := db.Exec(`
		INSERT INTO pipeline_run_events
		    (pipeline_id, execution_id, event_id, event_type, severity, occurred_at, received_at, payload)
		VALUES ($1, $2, $3, $4, $5, $6, $6, $7::jsonb)
	`, pgEvtPipelineID, exec, eventID, eventType, sev, at, payload); err != nil {
		t.Fatalf("insert %s: %v", eventID, err)
	}
}

func pgEvtFilterIDs(t *testing.T, db *sql.DB, f pipelineEventsFilter) map[string]bool {
	t.Helper()
	query, args := buildPipelineEventsQuery(pgEvtPipelineID, f, pipelineEventsCursor{}, 500)
	ids, _ := pgEvtQueryIDs(t, db, query, args)
	got := make(map[string]bool, len(ids))
	for _, id := range ids {
		got[id] = true
	}
	return got
}

func pgEvtAssertSet(t *testing.T, label string, got map[string]bool, want ...string) {
	t.Helper()
	wantSet := make(map[string]bool, len(want))
	for _, id := range want {
		wantSet[id] = true
		if !got[id] {
			t.Errorf("%s: missing %s", label, id)
		}
	}
	for id := range got {
		if !wantSet[id] {
			t.Errorf("%s: unexpected %s", label, id)
		}
	}
}

// TestPG_PipelineEventsRunScope is the query behind a run's "View Logs". A
// sentinel alert or a healer verdict raised during the run carries no
// execution_id, so the strict filter hides exactly the rows that explain why a
// run failed. scope=run adds pipeline-scoped rows that fall inside the run's
// start..end window — and nothing from before it, after it, or another run.
func TestPG_PipelineEventsRunScope(t *testing.T) {
	db := pgEvtDB(t)
	pgEvtSeedPipeline(t, db)

	const otherExecID = "cccccccc-0000-4000-8000-000000000005"
	base := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	end := base.Add(time.Hour)
	pgEvtExecution(t, db, pgEvtExecutionID, base, &end)
	pgEvtExecution(t, db, otherExecID, base.Add(2*time.Hour), nil)
	// A CDC run's id is its pipeline's id — the shape of rows stamped that way.
	pgEvtExecution(t, db, pgEvtPipelineID, base.Add(-24*time.Hour), nil)

	pgEvtInsertRow(t, db, "own-stage", pgEvtExecutionID, "STAGE_COMPLETED", "info", "", base.Add(5*time.Minute))
	pgEvtInsertRow(t, db, "alert-in-window", "", "SENTINEL_ALERT", "error", "", base.Add(10*time.Minute))
	pgEvtInsertRow(t, db, "pipeline-stamped-in-window", pgEvtPipelineID, "STAGE_PROGRESS", "info", "", base.Add(15*time.Minute))
	pgEvtInsertRow(t, db, "alert-before-run", "", "SENTINEL_ALERT", "error", "", base.Add(-10*time.Minute))
	pgEvtInsertRow(t, db, "alert-after-run", "", "SENTINEL_ALERT", "error", "", base.Add(90*time.Minute))
	pgEvtInsertRow(t, db, "other-run-in-window", otherExecID, "STAGE_FAILED", "error", "", base.Add(30*time.Minute))
	pgEvtInsertRow(t, db, "alert-during-other-run", "", "SENTINEL_ALERT", "warn", "", base.Add(3*time.Hour))
	// Healer shape: no occurred_at, only received_at — inside the window.
	pgEvtInsert(t, db, "healer-in-window", nil, "healer_decision", nil, base.Add(20*time.Minute))
	if _, err := db.Exec(`UPDATE pipeline_run_events SET execution_id = NULL WHERE event_id = 'healer-in-window'`); err != nil {
		t.Fatalf("null healer execution_id: %v", err)
	}

	// Control: the strict filter sees only the run's own stamped row.
	pgEvtAssertSet(t, "strict", pgEvtFilterIDs(t, db, pipelineEventsFilter{ExecutionID: pgEvtExecutionID}), "own-stage")

	pgEvtAssertSet(t, "run scope",
		pgEvtFilterIDs(t, db, pipelineEventsFilter{ExecutionID: pgEvtExecutionID, RunScope: true}),
		"own-stage", "alert-in-window", "pipeline-stamped-in-window", "healer-in-window")

	// A still-running run has no end: everything since it started is its own.
	pgEvtAssertSet(t, "open run",
		pgEvtFilterIDs(t, db, pipelineEventsFilter{ExecutionID: otherExecID, RunScope: true}),
		"other-run-in-window", "alert-during-other-run")

	// RunScope without an execution means nothing — the whole pipeline stream.
	if got := pgEvtFilterIDs(t, db, pipelineEventsFilter{RunScope: true}); len(got) != 8 {
		t.Errorf("RunScope without ExecutionID should not filter: got %d rows, want 8", len(got))
	}
}

// TestPG_PipelineEventsExcludeRoutine drops the timer ticks that bury a run's
// log (a CDC stream writes a DATA_PLANE_METRICS row every few seconds) — but
// never a routine-typed row that is itself a warning or an error.
func TestPG_PipelineEventsExcludeRoutine(t *testing.T) {
	db := pgEvtDB(t)
	pgEvtSeedPipeline(t, db)

	base := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	at := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }
	e := pgEvtExecutionID
	heartbeat := `{"metadata":{"heartbeat":true}}`

	pgEvtInsertRow(t, db, "stage-done", e, "STAGE_COMPLETED", "info", "", at(1))
	pgEvtInsertRow(t, db, "table-stats-info", e, "TABLE_STATS", "info", "", at(2))
	pgEvtInsertRow(t, db, "metrics-no-severity", "", "DATA_PLANE_METRICS", "", "", at(3))
	pgEvtInsertRow(t, db, "table-stats-warn", e, "TABLE_STATS", "WARNING", "", at(4))
	pgEvtInsertRow(t, db, "heartbeat-info", e, "STAGE_PROGRESS", "info", heartbeat, at(5))
	pgEvtInsertRow(t, db, "heartbeat-error", e, "STAGE_PROGRESS", "error", heartbeat, at(6))
	pgEvtInsertRow(t, db, "heartbeat-false", e, "STAGE_PROGRESS", "info", `{"metadata":{"heartbeat":false}}`, at(7))

	// Control: unfiltered, every row is there.
	if got := pgEvtFilterIDs(t, db, pipelineEventsFilter{}); len(got) != 7 {
		t.Fatalf("unfiltered: got %d rows, want 7 — seeding did not take", len(got))
	}

	pgEvtAssertSet(t, "exclude routine",
		pgEvtFilterIDs(t, db, pipelineEventsFilter{ExcludeRoutine: true}),
		"stage-done", "table-stats-warn", "heartbeat-error", "heartbeat-false")

	// Both filters compose: an explicit type list still narrows the result.
	pgEvtAssertSet(t, "exclude routine + types",
		pgEvtFilterIDs(t, db, pipelineEventsFilter{ExcludeRoutine: true, EventTypes: []string{"TABLE_STATS"}}),
		"table-stats-warn")
}
