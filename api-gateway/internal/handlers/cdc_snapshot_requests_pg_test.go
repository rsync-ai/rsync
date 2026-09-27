//go:build integration_pg

// The CDC sweep's gateway SQL against a real, fully migrated Postgres (113 + 114).
// sqlmock pins bindings and statement text but cannot evaluate jsonb: the one-statement
// merge in updateCDCStreamingOnlyTables, the stats-row lookup and the snapshot-request
// listing are only proven here. Run with a disposable database:
//
//	docker run -d --name cdc-sweep-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=pipeline_db -p 127.0.0.1:55447:5432 postgres:16-alpine
//	until docker exec cdc-sweep-pg pg_isready -h 127.0.0.1 -U postgres; do sleep 1; done
//	for m in api-gateway/migrations/*.sql; do
//	    docker exec -i cdc-sweep-pg psql -h 127.0.0.1 -U postgres -d pipeline_db -v ON_ERROR_STOP=1 -q < "$m"
//	done
//	CDC_SWEEP_PG_DSN='postgres://postgres:verify@127.0.0.1:55447/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run TestPG_CDCSweep -count=1 -v
//	docker rm -f cdc-sweep-pg

package handlers

import (
	"database/sql"
	"os"
	"reflect"
	"testing"

	_ "github.com/rsync-ai/shared/pgdriver"
)

const (
	pgSweepUserID      = "d0000000-0000-4000-8000-000000000001"
	pgSweepWorkspaceID = "d0000000-0000-4000-8000-000000000002"
	pgSweepPipelineID  = "d0000000-0000-4000-8000-000000000003"
)

func pgSweepDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("CDC_SWEEP_PG_DSN")
	if dsn == "" {
		t.Skip("set CDC_SWEEP_PG_DSN to a disposable, migrated Postgres — see the file header")
	}
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(q, args...); err != nil {
			t.Fatalf("exec failed: %v\nSQL: %s", err, q)
		}
	}
	// Stats rows and snapshot requests CASCADE from pipelines.
	mustExec(`DELETE FROM pipelines WHERE id = $1`, pgSweepPipelineID)
	mustExec(`
		INSERT INTO users (id, email, password_hash, name)
		VALUES ($1, 'pg-cdc-sweep-probe@example.invalid', 'x', 'pg cdc sweep probe')
		ON CONFLICT (id) DO NOTHING`, pgSweepUserID)
	mustExec(`
		INSERT INTO workspaces (id, name, slug, owner_id)
		VALUES ($1, 'pg cdc sweep probe', 'pg-cdc-sweep-probe', $2)
		ON CONFLICT (id) DO NOTHING`, pgSweepWorkspaceID, pgSweepUserID)
	mustExec(`
		INSERT INTO pipelines (id, name, natural_language_request, status, workspace_id, config)
		VALUES ($1, 'pg cdc sweep probe', 'probe', 'active', $2, '{"selected_tables":["db.users"]}'::jsonb)`,
		pgSweepPipelineID, pgSweepWorkspaceID)
	t.Cleanup(func() { _, _ = conn.Exec(`DELETE FROM pipelines WHERE id = $1`, pgSweepPipelineID) })
	return conn
}

func pgStreamingOnly(t *testing.T, conn *sql.DB) string {
	t.Helper()
	var v sql.NullString
	if err := conn.QueryRow(`SELECT (config->'cdc_streaming_only_tables')::text FROM pipelines WHERE id = $1`,
		pgSweepPipelineID).Scan(&v); err != nil {
		t.Fatalf("read streaming-only: %v", err)
	}
	return v.String
}

func TestPG_CDCSweep_StreamingOnlyMerge(t *testing.T) {
	conn := pgSweepDB(t)
	step := func(add, drop []string, want string) {
		t.Helper()
		if err := updateCDCStreamingOnlyTables(conn, pgSweepPipelineID, add, drop); err != nil {
			t.Fatalf("update(add=%v, drop=%v): %v", add, drop, err)
		}
		if got := pgStreamingOnly(t, conn); got != want {
			t.Fatalf("after add=%v drop=%v: cdc_streaming_only_tables = %s, want %s", add, drop, got, want)
		}
	}
	step([]string{"db.b", "db.a"}, nil, `["db.a", "db.b"]`)      // created, sorted
	step([]string{"db.c"}, []string{"db.a"}, `["db.b", "db.c"]`) // merged, not overwritten
	step([]string{"db.d"}, []string{"db.d"}, `["db.b", "db.c"]`) // in both lists → out
	step([]string{"db.b"}, nil, `["db.b", "db.c"]`)              // no duplicates
	step(nil, []string{"db.b", "db.c"}, `[]`)                    // emptied → [], never null
	step(nil, nil, `[]`)                                         // both empty → no statement
	if _, err := conn.Exec(`UPDATE pipelines SET config = jsonb_set(config, '{cdc_streaming_only_tables}', '"junk"') WHERE id = $1`, pgSweepPipelineID); err != nil {
		t.Fatalf("seed junk: %v", err)
	}
	step([]string{"db.x"}, nil, `["db.x"]`) // a non-array value reads as empty

	// The other config keys survive, and the table-stats reader sees the list.
	var selected string
	if err := conn.QueryRow(`SELECT (config->'selected_tables')::text FROM pipelines WHERE id = $1`, pgSweepPipelineID).Scan(&selected); err != nil || selected != `["db.users"]` {
		t.Fatalf("selected_tables = %q (%v); the merge must not touch other keys", selected, err)
	}
	if got := getPipelineCDCStreamingOnlyTables(conn, pgSweepPipelineID); !reflect.DeepEqual(got, map[string]bool{"db.x": true}) {
		t.Fatalf("getPipelineCDCStreamingOnlyTables = %v, want {db.x}", got)
	}
}

func TestPG_CDCSweep_TablesWithStatsRowsAndPaused(t *testing.T) {
	conn := pgSweepDB(t)
	if _, err := conn.Exec(`
		INSERT INTO pipeline_run_table_stats (pipeline_id, execution_id, table_name, qualified_name, mode, status)
		VALUES ($1, $1, 'orders', 'db.orders', 'cdc', 'running')`, pgSweepPipelineID); err != nil {
		t.Fatalf("seed stats row: %v", err)
	}
	if got := cdcTablesWithStatsRows(conn, pgSweepPipelineID, []string{"db.users", "db.orders"}); !reflect.DeepEqual(got, []string{"db.orders"}) {
		t.Fatalf("cdcTablesWithStatsRows = %v, want [db.orders]", got)
	}
	if got := cdcTablesWithStatsRows(conn, pgSweepPipelineID, []string{"db.users"}); len(got) != 0 {
		t.Fatalf("cdcTablesWithStatsRows = %v for a never-streamed table, want []", got)
	}

	if pipelineIsPaused(conn, pgSweepPipelineID) {
		t.Fatal("an active pipeline read as paused")
	}
	if _, err := conn.Exec(`UPDATE pipelines SET status = 'paused' WHERE id = $1`, pgSweepPipelineID); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if !pipelineIsPaused(conn, pgSweepPipelineID) {
		t.Fatal("a paused pipeline read as not paused")
	}
}

func TestPG_CDCSweep_ListSnapshotRequests(t *testing.T) {
	conn := pgSweepDB(t)
	if _, err := conn.Exec(`
		INSERT INTO cdc_snapshot_requests (id, pipeline_id, connector_name, mode, tables, source, status, requested_at)
		VALUES ('e0000000-0000-4000-8000-000000000001', $1, 'cdc-probe', 'blocking', '["db.users"]', 'resnapshot', 'failed',
		        '2026-09-24T09:00:00Z')`, pgSweepPipelineID); err != nil {
		t.Fatalf("seed older request: %v", err)
	}
	if _, err := conn.Exec(`
		INSERT INTO cdc_snapshot_requests (id, pipeline_id, connector_name, mode, tables, source, status, attempts,
		                                   completed_tables, requested_at, sent_at)
		VALUES ('e0000000-0000-4000-8000-000000000002', $1, 'cdc-probe', 'incremental', '["db.orders","db.users"]',
		        'table_edit', 'started', 1, '["db.orders"]', '2026-09-24T10:00:00Z', '2026-09-24T10:00:05Z')`, pgSweepPipelineID); err != nil {
		t.Fatalf("seed newer request: %v", err)
	}

	got, err := listCDCSnapshotRequests(conn, pgSweepPipelineID, cdcSnapshotRequestsLimit)
	if err != nil {
		t.Fatalf("listCDCSnapshotRequests: %v", err)
	}
	if len(got) != 2 || got[0].ID != "e0000000-0000-4000-8000-000000000002" {
		t.Fatalf("requests = %+v; want 2, newest first", got)
	}
	newest := got[0]
	if !reflect.DeepEqual(newest.Tables, []string{"db.orders", "db.users"}) || !reflect.DeepEqual(newest.CompletedTables, []string{"db.orders"}) ||
		newest.Source != "table_edit" || newest.Attempts != 1 ||
		newest.RequestedAt != "2026-09-24T10:00:00Z" || newest.SentAt != "2026-09-24T10:00:05Z" || newest.CompletedAt != "" {
		t.Fatalf("newest = %+v", newest)
	}
	if got[1].CompletedTables == nil || len(got[1].CompletedTables) != 0 || got[1].SentAt != "" {
		t.Fatalf("older = %+v; want completed_tables [] and no sent_at", got[1])
	}
	if one, err := listCDCSnapshotRequests(conn, pgSweepPipelineID, 1); err != nil || len(one) != 1 {
		t.Fatalf("limit 1 = %v (%v)", one, err)
	}
}
