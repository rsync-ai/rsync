//go:build integration_pg

// Real-PostgreSQL coverage for the initial-load statements (migration 118).
//
// sqlmock matches statements as strings and never runs them, so it cannot see a
// placeholder bound to the wrong parameter (initialBlockedWhere once compared
// updated_at with connector_name inside InsertInitial), a NOT EXISTS guard that
// never fires, or the partial unique index rejecting a second open load. Only a
// real server can.
//
// Not part of the default suite — needs a live server:
//
//	docker run -d --name cdcsnap-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=pipeline_db -p 127.0.0.1:55441:5432 postgres:16-alpine
//	for m in api-gateway/migrations/*.sql; do
//	    docker exec -i cdcsnap-pg psql -U postgres -d pipeline_db -v ON_ERROR_STOP=1 -q < "$m"
//	done
//	CDCSNAPSHOT_PG_DSN='postgres://postgres:verify@localhost:55441/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/cdcsnapshot/ -run PG -v
//
// (115 fails under psql — it reads the runner's schema_migrations table — and is
// irrelevant here.)
package cdcsnapshot

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/rsync-ai/shared/pgdriver"
)

const (
	pgSnapUserID      = "5e1d0000-0000-4000-8000-000000000118"
	pgSnapWorkspaceID = "5e1d0000-0000-4000-8000-000000000218"
	pgSnapPipelineID  = "5e1d0000-0000-4000-8000-000000000318"
)

func pgSnapDB(t *testing.T) (*sql.DB, *Store) {
	t.Helper()
	dsn := os.Getenv("CDCSNAPSHOT_PG_DSN")
	if dsn == "" {
		t.Skip("CDCSNAPSHOT_PG_DSN not set — see the file header")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	purge := func() {
		// pipelines cascades to cdc_snapshot_requests.
		for _, q := range []string{
			`DELETE FROM pipelines WHERE id = $1::uuid`,
			`DELETE FROM workspaces WHERE id = $1::uuid`,
			`DELETE FROM users WHERE id = $1::uuid`,
		} {
			id := map[string]string{
				`DELETE FROM pipelines WHERE id = $1::uuid`:  pgSnapPipelineID,
				`DELETE FROM workspaces WHERE id = $1::uuid`: pgSnapWorkspaceID,
				`DELETE FROM users WHERE id = $1::uuid`:      pgSnapUserID,
			}[q]
			if _, err := db.Exec(q, id); err != nil {
				t.Fatalf("purge %q: %v", q, err)
			}
		}
	}
	purge()
	t.Cleanup(purge)
	for _, s := range []struct {
		q    string
		args []interface{}
	}{
		{`INSERT INTO users (id, email, password_hash) VALUES ($1::uuid, 'cdcsnap-verify@example.invalid', 'x')`, []interface{}{pgSnapUserID}},
		{`INSERT INTO workspaces (id, name, slug, owner_id) VALUES ($1::uuid, 'cdcsnap-verify', 'cdcsnap-verify', $2::uuid)`, []interface{}{pgSnapWorkspaceID, pgSnapUserID}},
		{`INSERT INTO pipelines (id, name, natural_language_request, workspace_id, status, sync_mode, created_by)
		  VALUES ($1::uuid, 'cdcsnap verify', 'verify', $2::uuid, 'running', 'cdc', $3::uuid)`, []interface{}{pgSnapPipelineID, pgSnapWorkspaceID, pgSnapUserID}},
	} {
		if _, err := db.Exec(s.q, s.args...); err != nil {
			t.Fatalf("seed %.50q: %v", s.q, err)
		}
	}
	return db, NewStore(db)
}

func pgInitial(first time.Time, status string) Request {
	f := first
	r := Request{
		PipelineID:    pgSnapPipelineID,
		ConnectorName: "cdc-5e1d0000",
		Mode:          "blocking",
		Tables:        []string{"public.orders"},
		Source:        SourceInitial,
		Status:        status,
		SentAt:        &f,
	}
	if status == StatusStarted {
		r.StartedAt, r.LastProgressAt = &f, &f
	}
	return r
}

func pgInitialRows(t *testing.T, db *sql.DB) map[string]int {
	t.Helper()
	rows, err := db.Query(`SELECT status, count(*) FROM cdc_snapshot_requests
		WHERE pipeline_id = $1::uuid AND source = 'initial' GROUP BY status`, pgSnapPipelineID)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			t.Fatal(err)
		}
		out[s] = n
	}
	return out
}

// The dispatcher's whole write path for a Debezium initial load: record once,
// never twice, move it with SaveProgress, then refuse its tail rows.
func TestPGInitialLoadRecordedOnceAndMoved(t *testing.T) {
	db, st := pgSnapDB(t)
	ctx := context.Background()
	first := time.Now().Add(-2 * time.Minute).UTC().Truncate(time.Microsecond)

	if blocked, err := st.InitialBlocked(ctx, pgSnapPipelineID, first); err != nil || blocked {
		t.Fatalf("an empty pipeline must not be blocked: %v %v", blocked, err)
	}
	ok, err := st.InsertInitial(ctx, pgInitial(first, StatusStarted))
	if err != nil || !ok {
		t.Fatalf("InsertInitial = %v, %v; want recorded", ok, err)
	}
	// A second orchestrator that saw the same rows.
	if ok, err := st.InsertInitial(ctx, pgInitial(first, StatusStarted)); err != nil || ok {
		t.Fatalf("second InsertInitial = %v, %v; want refused, no error", ok, err)
	}
	if blocked, err := st.InitialBlocked(ctx, pgSnapPipelineID, first); err != nil || !blocked {
		t.Fatalf("an open load must block a new one: %v %v", blocked, err)
	}
	if got := pgInitialRows(t, db); got[StatusStarted] != 1 || len(got) != 1 {
		t.Fatalf("rows = %v, want one started", got)
	}

	reqs, err := st.ListRecent(ctx, pgSnapPipelineID, time.Now().Add(-time.Hour))
	if err != nil || len(reqs) != 1 {
		t.Fatalf("ListRecent = %v, %v; want the open load", reqs, err)
	}
	r := reqs[0]
	if r.Source != SourceInitial || r.SentAt == nil || !r.SentAt.Equal(first) || r.RequestedAt.IsZero() {
		t.Fatalf("read back %+v", r)
	}

	// Rows for a second table arrive, then Debezium's "last".
	now := time.Now().UTC()
	r.Tables = append(r.Tables, "public.customers")
	r.CompletedTables = []string{"public.orders", "public.customers"}
	r.Status, r.LastProgressAt, r.CompletedAt = StatusCompleted, &now, &now
	if err := st.SaveProgress(ctx, r); err != nil {
		t.Fatalf("SaveProgress: %v", err)
	}
	reqs, _ = st.ListRecent(ctx, pgSnapPipelineID, time.Now().Add(-time.Hour))
	if len(reqs) != 1 || reqs[0].Status != StatusCompleted || len(reqs[0].Tables) != 2 || len(reqs[0].CompletedTables) != 2 {
		t.Fatalf("after SaveProgress: %+v", reqs)
	}
	// A late flush never reopens it.
	r.Status = StatusStarted
	if err := st.SaveProgress(ctx, r); err != nil {
		t.Fatalf("late SaveProgress: %v", err)
	}
	if got := pgInitialRows(t, db); got[StatusCompleted] != 1 || len(got) != 1 {
		t.Fatalf("a late flush reopened the load: %v", got)
	}

	// Its tail rows (read before it closed) are refused; a load first seen after
	// it closed is a new one.
	if blocked, _ := st.InitialBlocked(ctx, pgSnapPipelineID, first); !blocked {
		t.Fatal("rows read before the load closed must not start another")
	}
	if ok, err := st.InsertInitial(ctx, pgInitial(first, StatusStarted)); err != nil || ok {
		t.Fatalf("tail-row InsertInitial = %v, %v; want refused", ok, err)
	}
	later := time.Now().Add(time.Minute).UTC()
	if blocked, err := st.InitialBlocked(ctx, pgSnapPipelineID, later); err != nil || blocked {
		t.Fatalf("a load first seen after the last closed must be recordable: %v %v", blocked, err)
	}

	// ListRecent drops a finished request once it is older than the window.
	if _, err := db.Exec(`UPDATE cdc_snapshot_requests SET updated_at = NOW() - interval '2 hours' WHERE pipeline_id = $1::uuid`, pgSnapPipelineID); err != nil {
		t.Fatal(err)
	}
	if reqs, err := st.ListRecent(ctx, pgSnapPipelineID, time.Now().Add(-time.Hour)); err != nil || len(reqs) != 0 {
		t.Fatalf("ListRecent kept an old finished request: %+v %v", reqs, err)
	}
}

// The partial unique index is what stops two replicas racing past NOT EXISTS.
func TestPGOneOpenInitialLoadPerPipeline(t *testing.T) {
	db, st := pgSnapDB(t)
	ctx := context.Background()
	if ok, err := st.InsertInitial(ctx, pgInitial(time.Now().UTC(), StatusSent)); err != nil || !ok {
		t.Fatalf("InsertInitial: %v %v", ok, err)
	}
	_, err := db.Exec(`INSERT INTO cdc_snapshot_requests (pipeline_id, connector_name, mode, source, status)
		VALUES ($1::uuid, 'c', 'blocking', 'initial', 'started')`, pgSnapPipelineID)
	if err == nil {
		t.Fatal("a second open initial load was accepted; migration 118's unique index is missing")
	}
	// Finished ones are history and may repeat.
	if _, err := db.Exec(`UPDATE cdc_snapshot_requests SET status = 'completed' WHERE pipeline_id = $1::uuid`, pgSnapPipelineID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO cdc_snapshot_requests (pipeline_id, connector_name, mode, source, status)
		VALUES ($1::uuid, 'c', 'blocking', 'initial', 'completed')`, pgSnapPipelineID); err != nil {
		t.Fatalf("a second finished initial load was refused: %v", err)
	}
}

// The hybrid executor's path: BeginInitial closes a load a dead run left open,
// then Finish closes its own.
func TestPGBeginInitialSupersedesAnOpenLoad(t *testing.T) {
	db, st := pgSnapDB(t)
	ctx := context.Background()
	if ok, err := st.InsertInitial(ctx, pgInitial(time.Now().Add(-time.Hour).UTC(), StatusStarted)); err != nil || !ok {
		t.Fatalf("seed: %v %v", ok, err)
	}
	r, err := st.BeginInitial(ctx, Request{PipelineID: pgSnapPipelineID, ConnectorName: "cdc-5e1d0000",
		Mode: "blocking", Tables: []string{"public.orders", "public.customers"}})
	if err != nil {
		t.Fatalf("BeginInitial: %v", err)
	}
	if r.ID == "" || r.Status != StatusStarted || r.Source != SourceInitial || r.StartedAt == nil || r.LastProgressAt != nil || len(r.Tables) != 2 {
		t.Fatalf("BeginInitial returned %+v", r)
	}
	var lastErr string
	if err := db.QueryRow(`SELECT last_error FROM cdc_snapshot_requests
		WHERE pipeline_id = $1::uuid AND status = 'unconfirmed'`, pgSnapPipelineID).Scan(&lastErr); err != nil || lastErr != SupersededMessage {
		t.Fatalf("the old load was not closed as superseded: %q %v", lastErr, err)
	}
	if ok, err := st.Finish(ctx, r, StatusCompleted, ""); err != nil || !ok {
		t.Fatalf("Finish: %v %v", ok, err)
	}
	if got := pgInitialRows(t, db); got[StatusCompleted] != 1 || got[StatusUnconfirmed] != 1 || len(got) != 2 {
		t.Fatalf("rows = %v", got)
	}
}

// Two runs of one pipeline racing BeginInitial: the second's supersede cannot
// see the first's uncommitted load, so both reach the INSERT and the partial
// unique index lets one through. The loser gets ErrInitialLoadOpen, not a
// unique violation, and the winner's load is the one left open.
func TestPGBeginInitialLosesARaceWithoutAnError(t *testing.T) {
	db, st := pgSnapDB(t)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO cdc_snapshot_requests
			(pipeline_id, connector_name, mode, tables, source, status,
			 not_before, requested_at, sent_at, last_sent_at, started_at)
		VALUES ($1::uuid, 'first-run', 'blocking', '[]'::jsonb, 'initial', 'started', NOW(), NOW(), NOW(), NOW(), NOW())`,
		pgSnapPipelineID); err != nil {
		t.Fatalf("the first run's load: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := st.BeginInitial(ctx, Request{PipelineID: pgSnapPipelineID, ConnectorName: "second-run", Mode: "blocking"})
		done <- err
	}()
	// It is only a race once the second run is waiting on the first's row.
	waiting := false
	for deadline := time.Now().Add(5 * time.Second); !waiting && time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		waiting = n > 0
	}
	if !waiting {
		t.Fatal("the second run never waited on the first: the race was not reproduced")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrInitialLoadOpen) {
			t.Fatalf("losing the race returned %v, want ErrInitialLoadOpen", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("BeginInitial never returned")
	}
	var open []string
	rows, err := db.Query(`SELECT connector_name FROM cdc_snapshot_requests
		WHERE pipeline_id = $1::uuid AND source = 'initial' AND status IN ('sent', 'started')`, pgSnapPipelineID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		open = append(open, c)
	}
	if len(open) != 1 || open[0] != "first-run" {
		t.Fatalf("open initial loads = %v, want only the first run's", open)
	}
}

// A gateway older than migration 118 rejects source 'initial' with its CHECK.
// wrap must still read that as "not recorded yet" through the real driver,
// which is what the SQLSTATE test in isOldSourceCheck relies on. The older
// constraint is put back inside a transaction that is rolled back.
func TestPGOlderSchemaRejectingAnInitialLoadIsUnavailable(t *testing.T) {
	db, _ := pgSnapDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`ALTER TABLE cdc_snapshot_requests
		DROP CONSTRAINT cdc_snapshot_requests_source_check,
		ADD CONSTRAINT cdc_snapshot_requests_source_check
			CHECK (source IN ('resnapshot', 'table_edit', 'auto_pickup'))`); err != nil {
		t.Fatalf("restore the pre-118 check: %v", err)
	}
	_, err = tx.Exec(`INSERT INTO cdc_snapshot_requests (pipeline_id, connector_name, mode, tables, source, status, not_before)
		VALUES ($1::uuid, 'c', 'blocking', '[]'::jsonb, 'initial', 'started', NOW())`, pgSnapPipelineID)
	if err == nil {
		t.Fatal("the pre-118 check accepted source 'initial'")
	}
	if !errors.Is(wrap(err), ErrUnavailable) {
		t.Fatalf("wrap(%v) is not ErrUnavailable", err)
	}
}
