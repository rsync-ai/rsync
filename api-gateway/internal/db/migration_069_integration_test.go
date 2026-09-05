//go:build integration

// Integration tests for migration 069 (workspace activation, clean-slate).
//
// These run against a REAL PostgreSQL — sqlmock cannot verify the things that
// matter here: FK CASCADE/NO-ACTION behaviour on `DELETE`/`TRUNCATE`, NOT NULL
// enforcement, and DO-block backfill correctness are engine semantics, not query
// strings. The default `go test ./...` unit lane has no Postgres, so the whole
// file is gated behind the `integration` build tag AND skips unless a disposable
// DSN is provided via MIGRATION_TEST_DSN (or TEST_DATABASE_URL).
//
// Run locally against a throwaway container:
//
//	docker run -d --rm --name pg069 -e POSTGRES_PASSWORD=pg -p 55432:5432 postgres:16
//	MIGRATION_TEST_DSN='postgres://postgres:pg@localhost:55432/postgres?sslmode=disable' \
//	  go test -tags=integration -run TestMigration069 ./internal/db/ -v
//	docker rm -f pg069
package db

import (
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	_ "github.com/rsync-ai/shared/pgdriver"
)

const migrationsDir = "../../migrations"

// testDSN returns the disposable-Postgres DSN or skips the test.
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("MIGRATION_TEST_DSN")
	if dsn == "" {
		dsn = os.Getenv("TEST_DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("set MIGRATION_TEST_DSN to a disposable Postgres to run migration integration tests")
	}
	return dsn
}

// freshSchema connects, nukes `public`, and returns a clean handle. The global
// db.DB is swapped to this connection for the duration of the test (restored by
// the returned cleanup) because the runner reads db.GetDB().
func freshSchema(t *testing.T, dsn string) (*sql.DB, func()) {
	t.Helper()
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := conn.Ping(); err != nil {
		t.Fatalf("ping disposable Postgres: %v", err)
	}
	if _, err := conn.Exec(`DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;`); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	prev := DB
	DB = conn
	cleanup := func() {
		DB = prev
		_ = conn.Close()
	}
	return conn, cleanup
}

// migrationFilesBelow069 lists every existing migration except 069+ so a test can
// reproduce the exact pre-069 schema, seed it, then apply 069 in isolation.
func migrationFilesBelow069(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	var files []string
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".sql") {
			continue
		}
		if n >= "069" { // 069_* and anything later
			continue
		}
		files = append(files, n)
	}
	sort.Strings(files)
	return files
}

// applyPre069 builds the schema_migrations bookkeeping table and applies every
// migration strictly before 069 using the production applyMigration() path.
func applyPre069(t *testing.T, conn *sql.DB) {
	t.Helper()
	if _, err := conn.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version VARCHAR(255) PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`); err != nil {
		t.Fatalf("create schema_migrations: %v", err)
	}
	for _, f := range migrationFilesBelow069(t) {
		if err := applyMigration(conn, migrationsDir, f); err != nil {
			t.Fatalf("apply %s: %v", f, err)
		}
	}
}

// exec069 finds the single 069_*.sql file and executes its body directly (the
// real runner is bypassed so the test controls the seed-then-apply ordering).
// A missing 069 file is the RED signal before the migration is authored.
func exec069(t *testing.T, conn *sql.DB) {
	t.Helper()
	body, err := os.ReadFile(must069Path(t))
	if err != nil {
		t.Fatalf("read 069: %v", err)
	}
	if _, err := conn.Exec(string(body)); err != nil {
		t.Fatalf("exec 069: %v", err)
	}
}

// --- tiny scan helpers ------------------------------------------------------

func qInt(t *testing.T, conn *sql.DB, q string, args ...any) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

func qStr(t *testing.T, conn *sql.DB, q string, args ...any) string {
	t.Helper()
	var s sql.NullString
	if err := conn.QueryRow(q, args...).Scan(&s); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return s.String
}

func qBool(t *testing.T, conn *sql.DB, q string, args ...any) bool {
	t.Helper()
	var b bool
	if err := conn.QueryRow(q, args...).Scan(&b); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return b
}

// seedFixtures plants the clean-slate scenario:
//   - alice, bob: post-047 signups with NO workspace (069 must provision one).
//   - carol: already owns a workspace (069 must adopt+flag it, never duplicate).
//   - alice owns a connection (workspace_id NULL) and a pipeline with a child
//     execution row (executions has a NO-ACTION FK to pipelines — the row that
//     would block a plain DELETE FROM pipelines).
//
// Returns alice/carol ids and the connection id for assertions.
func seedFixtures(t *testing.T, conn *sql.DB) (alice, carol, connID string) {
	t.Helper()
	alice = qStr(t, conn, `INSERT INTO users (email, password_hash) VALUES ('alice@acme.test','x') RETURNING id`)
	_ = qStr(t, conn, `INSERT INTO users (email, password_hash) VALUES ('bob@acme.test','x') RETURNING id`)
	carol = qStr(t, conn, `INSERT INTO users (email, password_hash) VALUES ('carol@acme.test','x') RETURNING id`)

	// Carol already has a workspace + owner membership (is_personal not yet set).
	carolWS := qStr(t, conn,
		`INSERT INTO workspaces (name, slug, owner_id) VALUES ('carol ws','carol-ws',$1) RETURNING id`, carol)
	if _, err := conn.Exec(
		`INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1,$2,'owner')`, carolWS, carol); err != nil {
		t.Fatalf("seed carol membership: %v", err)
	}

	// Alice's NULL-workspace connection.
	connID = qStr(t, conn,
		`INSERT INTO connections (user_id, name, type, connector_type, config)
		 VALUES ($1,'pg-src','source','postgresql','{}') RETURNING id`, alice)

	// Alice's pipeline + a child execution (NO-ACTION FK -> would block DELETE).
	pipeID := qStr(t, conn,
		`INSERT INTO pipelines (name, natural_language_request, created_by)
		 VALUES ('p1','copy a->b',$1) RETURNING id`, alice)
	if _, err := conn.Exec(`INSERT INTO executions (pipeline_id) VALUES ($1)`, pipeID); err != nil {
		t.Fatalf("seed execution: %v", err)
	}
	return alice, carol, connID
}

// TestMigration069_CleanSlate is the headline test: seed the pre-069 world,
// apply 069, and assert every clean-slate guarantee.
func TestMigration069_CleanSlate(t *testing.T) {
	dsn := testDSN(t)
	conn, cleanup := freshSchema(t, dsn)
	defer cleanup()

	applyPre069(t, conn)
	alice, carol, connID := seedFixtures(t, conn)

	exec069(t, conn) // GREEN requires 069 to exist and satisfy the asserts below.

	// 1. Pipelines emptied, and the NO-ACTION child (executions) emptied with it.
	if n := qInt(t, conn, `SELECT count(*) FROM pipelines`); n != 0 {
		t.Errorf("expected 0 pipelines after 069, got %d", n)
	}
	if n := qInt(t, conn, `SELECT count(*) FROM executions`); n != 0 {
		t.Errorf("expected 0 executions (TRUNCATE CASCADE), got %d", n)
	}

	// 2. Every user has exactly one is_personal workspace with an owner membership.
	for _, u := range []string{alice, carol} {
		if n := qInt(t, conn, `SELECT count(*) FROM workspaces WHERE owner_id=$1 AND is_personal`, u); n != 1 {
			t.Errorf("user %s: expected exactly 1 personal workspace, got %d", u, n)
		}
		if n := qInt(t, conn, `SELECT count(*) FROM workspace_members wm
			JOIN workspaces w ON w.id=wm.workspace_id
			WHERE w.owner_id=$1 AND w.is_personal AND wm.user_id=$1 AND wm.role='owner'`, u); n != 1 {
			t.Errorf("user %s: expected owner membership on personal workspace, got %d", u, n)
		}
	}
	// Bob (orphan) too.
	if n := qInt(t, conn, `SELECT count(*) FROM workspaces w JOIN users u ON u.id=w.owner_id
		WHERE u.email='bob@acme.test' AND w.is_personal`); n != 1 {
		t.Errorf("bob: expected exactly 1 personal workspace, got %d", n)
	}

	// 3. Carol's pre-existing workspace was ADOPTED, not duplicated.
	if n := qInt(t, conn, `SELECT count(*) FROM workspaces WHERE owner_id=$1`, carol); n != 1 {
		t.Errorf("carol: expected her single workspace adopted (count 1), got %d", n)
	}

	// 4. Alice's connection backfilled to HER personal workspace.
	wantWS := qStr(t, conn, `SELECT id FROM workspaces WHERE owner_id=$1 AND is_personal`, alice)
	gotWS := qStr(t, conn, `SELECT workspace_id FROM connections WHERE id=$1`, connID)
	if gotWS == "" || gotWS != wantWS {
		t.Errorf("connection backfill: want workspace %s, got %q", wantWS, gotWS)
	}

	// 5. NOT NULL is enforced immediately on both columns.
	if nn := qStr(t, conn, `SELECT is_nullable FROM information_schema.columns
		WHERE table_name='connections' AND column_name='workspace_id'`); nn != "NO" {
		t.Errorf("connections.workspace_id should be NOT NULL, is_nullable=%q", nn)
	}
	if nn := qStr(t, conn, `SELECT is_nullable FROM information_schema.columns
		WHERE table_name='pipelines' AND column_name='workspace_id'`); nn != "NO" {
		t.Errorf("pipelines.workspace_id should be NOT NULL, is_nullable=%q", nn)
	}

	// 6. New objects exist.
	if !qBool(t, conn, `SELECT to_regclass('public.workspace_invites') IS NOT NULL`) {
		t.Error("workspace_invites table missing")
	}
	if !qBool(t, conn, `SELECT to_regclass('public.uq_connections_ws_name') IS NOT NULL`) {
		t.Error("uq_connections_ws_name index missing")
	}
	if !qBool(t, conn, `SELECT EXISTS(SELECT 1 FROM information_schema.columns
		WHERE table_name='oauth_states' AND column_name='workspace_id')`) {
		t.Error("oauth_states.workspace_id column missing")
	}
	if !qBool(t, conn, `SELECT EXISTS(SELECT 1 FROM information_schema.columns
		WHERE table_name='workspaces' AND column_name='is_personal')`) {
		t.Error("workspaces.is_personal column missing")
	}

	// 7. Idempotent: re-running the 069 body must not error or change counts.
	body, _ := os.ReadFile(must069Path(t))
	if _, err := conn.Exec(string(body)); err != nil {
		t.Fatalf("069 is not idempotent: re-run failed: %v", err)
	}
	if n := qInt(t, conn, `SELECT count(*) FROM workspaces WHERE owner_id=$1`, carol); n != 1 {
		t.Errorf("idempotency: carol workspace duplicated on re-run, count=%d", n)
	}
	if n := qInt(t, conn, `SELECT count(*) FROM workspaces WHERE owner_id=$1 AND is_personal`, alice); n != 1 {
		t.Errorf("idempotency: alice personal workspace duplicated, count=%d", n)
	}
}

// TestMigration069_FreshBoot proves the full runner (001..069) applies cleanly on
// an empty database — zero users (provisioning no-op), empty pipelines (TRUNCATE
// no-op), and SET NOT NULL holds.
func TestMigration069_FreshBoot(t *testing.T) {
	dsn := testDSN(t)
	conn, cleanup := freshSchema(t, dsn)
	defer cleanup()

	if err := Migrate(migrationsDir); err != nil {
		t.Fatalf("full Migrate() including 069 failed on fresh DB: %v", err)
	}
	if !qBool(t, conn, `SELECT to_regclass('public.workspace_invites') IS NOT NULL`) {
		t.Error("workspace_invites table missing after fresh boot")
	}
	if nn := qStr(t, conn, `SELECT is_nullable FROM information_schema.columns
		WHERE table_name='connections' AND column_name='workspace_id'`); nn != "NO" {
		t.Errorf("connections.workspace_id should be NOT NULL after fresh boot, is_nullable=%q", nn)
	}
}

// must069Path returns the single 069_*.sql path, failing as the RED signal when
// the migration has not been authored yet.
func must069Path(t *testing.T) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(migrationsDir, "069_*.sql"))
	if err != nil {
		t.Fatalf("glob 069: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("RED: no 069_*.sql migration present yet")
	}
	if len(matches) > 1 {
		t.Fatalf("expected exactly one 069 migration, found %v", matches)
	}
	return matches[0]
}
