//go:build integration

// Integration test for migration 119 (a model run no longer moves its saved query's
// updated_at). Trigger behaviour needs a real, disposable PostgreSQL. Same gate and
// helpers as migration_069_integration_test.go; applyMigration is the runner's own.
//
//	docker run -d --rm --name pg119 -e POSTGRES_PASSWORD=pg -p 55434:5432 postgres:16-alpine
//	MIGRATION_TEST_DSN='postgres://postgres:pg@localhost:55434/postgres?sslmode=disable' \
//	  go test -tags=integration -run TestMigration119 ./internal/db/ -v
//	docker rm -f pg119
package db

import (
	"database/sql"
	"os"
	"sort"
	"strings"
	"testing"
)

// seedSavedQuery119 inserts a saved query whose updated_at is 2026-01-01, so any
// statement that moves it shows.
func seedSavedQuery119(t *testing.T, conn *sql.DB, name string) string {
	t.Helper()
	user := qStr(t, conn, `INSERT INTO users (email, password_hash) VALUES ($1,'x') RETURNING id`, name+"@acme.test")
	ws := qStr(t, conn, `INSERT INTO workspaces (name, slug, owner_id) VALUES ($1,$1,$2) RETURNING id`, name+"-ws", user)
	connID := qStr(t, conn, `INSERT INTO connections (user_id, workspace_id, name, type, connector_type, config)
		VALUES ($1, $2, 'pg', 'source', 'postgresql', '{}') RETURNING id`, user, ws)
	return qStr(t, conn, `INSERT INTO saved_queries (workspace_id, connection_id, name, sql_text, created_by, updated_at)
		VALUES ($1, $2, $3, 'SELECT 1', $4, '2026-01-01T00:00:00Z') RETURNING id`, ws, connID, name, user)
}

func updatedDay119(t *testing.T, conn *sql.DB, id string) string {
	t.Helper()
	return qStr(t, conn, `SELECT to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') FROM saved_queries WHERE id = $1`, id)
}

func exec119(t *testing.T, conn *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// The run path's three writes, as the handlers send them.
const (
	stampOutcome119 = `UPDATE saved_queries SET last_run_at = NOW(), last_run_status = $2, last_run_error = NULLIF($3, '') WHERE id = $1`
	stampFailure119 = `UPDATE saved_queries SET last_run_at = NOW(), last_run_status = 'failed', last_run_error = NULLIF($2, '') WHERE id = $1`
	claimTarget119  = `UPDATE saved_queries SET target_owned = TRUE WHERE id = $1`
)

// TestMigration119_RunKeepsUpdatedAtAnEditMovesIt: on the full schema, the run
// stamps leave updated_at alone and still store what they wrote; each kind of edit
// moves it.
func TestMigration119_RunKeepsUpdatedAtAnEditMovesIt(t *testing.T) {
	conn, cleanup := freshSchema(t, testDSN(t))
	defer cleanup()
	if err := Migrate(migrationsDir); err != nil {
		t.Fatalf("full Migrate() including 119 failed on a fresh DB: %v", err)
	}

	id := seedSavedQuery119(t, conn, "run119")
	exec119(t, conn, stampOutcome119, id, "succeeded", "")
	exec119(t, conn, claimTarget119, id)
	exec119(t, conn, stampFailure119, id, "relation does not exist")
	if got := updatedDay119(t, conn, id); got != "2026-01-01" {
		t.Errorf("a model run moved updated_at to %s, want 2026-01-01", got)
	}
	if !qBool(t, conn, `SELECT last_run_at IS NOT NULL AND last_run_status = 'failed'
		AND last_run_error = 'relation does not exist' AND target_owned FROM saved_queries WHERE id = $1`, id) {
		t.Errorf("the run stamps themselves were not stored")
	}

	// Each edit the handlers make, on its own row so one cannot mask another.
	for name, edit := range map[string]string{
		"sql":         `UPDATE saved_queries SET sql_text = 'SELECT 2', statement_class = 'select', updated_by = created_by WHERE id = $1`,
		"rename":      `UPDATE saved_queries SET name = 'renamed', description = NULLIF('', ''), visibility = 'private', updated_by = created_by WHERE id = $1`,
		"freshness":   `UPDATE saved_queries SET freshness_deadline_seconds = 3600, updated_at = NOW() WHERE id = $1`,
		"materialize": `UPDATE saved_queries SET materialization = 'table', target_table = 'analytics.t', updated_at = NOW() WHERE id = $1`,
		"with-a-run":  `UPDATE saved_queries SET sql_text = 'SELECT 3', last_run_status = 'succeeded' WHERE id = $1`,
	} {
		e := seedSavedQuery119(t, conn, "edit119-"+name)
		exec119(t, conn, edit, e)
		if !qBool(t, conn, `SELECT updated_at > NOW() - interval '1 minute' FROM saved_queries WHERE id = $1`, e) {
			t.Errorf("edit %q left updated_at at %s; it must stamp NOW()", name, updatedDay119(t, conn, e))
		}
	}

	// Other tables keep the shared function, untouched: the schedule table beside this
	// one (085) still runs it, and it still stamps unconditionally.
	if got := qStr(t, conn, `SELECT p.proname FROM pg_trigger g JOIN pg_proc p ON p.oid = g.tgfoid
		WHERE g.tgname = 'update_saved_query_schedules_updated_at'`); got != "update_updated_at_column" {
		t.Errorf("saved_query_schedules' trigger runs %s, want update_updated_at_column", got)
	}
	if src := qStr(t, conn, `SELECT prosrc FROM pg_proc WHERE proname = 'update_updated_at_column'`); strings.Contains(strings.ToUpper(src), " IF ") {
		t.Errorf("update_updated_at_column() became conditional: %s", src)
	}
}

// TestMigration119_BeforeAndIdempotent: the control, on the schema as 118 left it,
// where a run stamp does move updated_at (the bug); then 119 applied on top of the
// same rows, twice.
func TestMigration119_BeforeAndIdempotent(t *testing.T) {
	conn, cleanup := freshSchema(t, testDSN(t))
	defer cleanup()
	exec119(t, conn, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version VARCHAR(255) PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`)
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	var files []string
	var file119 string
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".sql") {
			continue
		}
		if strings.HasPrefix(n, "119_") {
			file119 = n
		}
		if n < "119" {
			files = append(files, n)
		}
	}
	if file119 == "" {
		t.Fatal("no 119_*.sql migration")
	}
	sort.Strings(files)
	for _, f := range files {
		if err := applyMigration(conn, migrationsDir, f); err != nil {
			t.Fatalf("apply %s: %v", f, err)
		}
	}

	before := seedSavedQuery119(t, conn, "before119")
	exec119(t, conn, stampOutcome119, before, "succeeded", "")
	if got := updatedDay119(t, conn, before); got == "2026-01-01" {
		t.Fatalf("control: before 119 a run stamp left updated_at at 2026-01-01; the test cannot tell 119 apart")
	}

	body, err := os.ReadFile(migrationsDir + "/" + file119)
	if err != nil {
		t.Fatalf("read %s: %v", file119, err)
	}
	for i := 0; i < 2; i++ {
		exec119(t, conn, string(body))
	}
	after := seedSavedQuery119(t, conn, "after119")
	exec119(t, conn, stampOutcome119, after, "failed", "boom")
	if got := updatedDay119(t, conn, after); got != "2026-01-01" {
		t.Errorf("after 119 applied twice, a run stamp moved updated_at to %s, want 2026-01-01", got)
	}
	if n := qInt(t, conn, `SELECT COUNT(*) FROM pg_trigger WHERE tgrelid = 'saved_queries'::regclass AND NOT tgisinternal`); n != 1 {
		t.Errorf("saved_queries has %d user triggers after re-applying 119, want 1", n)
	}
}
