//go:build integration

// Integration tests for migration 106 (object-storage layout v2 schema).
//
// Defaults, CHECK constraints, unique keys and FK cascades are engine semantics, so
// these run against a REAL, disposable PostgreSQL. Same gate as the 069 tests: the
// `integration` build tag plus MIGRATION_TEST_DSN (or TEST_DATABASE_URL); without a
// DSN every test skips. The helpers (testDSN, freshSchema, qInt, qStr, qBool,
// migrationsDir) live in migration_069_integration_test.go.
//
//	docker run -d --rm --name pg106 -e POSTGRES_PASSWORD=pg -p 55434:5432 postgres:16-alpine
//	MIGRATION_TEST_DSN='postgres://postgres:pg@localhost:55434/postgres?sslmode=disable' \
//	  go test -tags=integration -run TestMigration106 ./internal/db/ -v
//	docker rm -f pg106
package db

import (
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// applyBefore106 applies every migration strictly before 106 through the runner's
// own applyMigration path.
func applyBefore106(t *testing.T, conn *sql.DB) {
	t.Helper()
	if _, err := conn.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version VARCHAR(255) PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`); err != nil {
		t.Fatalf("create schema_migrations: %v", err)
	}
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	var files []string
	for _, e := range entries {
		n := e.Name()
		if !e.IsDir() && strings.HasSuffix(n, ".sql") && n < "106" {
			files = append(files, n)
		}
	}
	sort.Strings(files)
	for _, f := range files {
		if err := applyMigration(conn, migrationsDir, f); err != nil {
			t.Fatalf("apply %s: %v", f, err)
		}
	}
}

func exec106(t *testing.T, conn *sql.DB) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(migrationsDir, "106_*.sql"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected exactly one 106_*.sql migration, found %v (err %v)", matches, err)
	}
	body, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read 106: %v", err)
	}
	if _, err := conn.Exec(string(body)); err != nil {
		t.Fatalf("exec 106: %v", err)
	}
}

func seedPipeline106(t *testing.T, conn *sql.DB, name string) string {
	t.Helper()
	user := qStr(t, conn, `INSERT INTO users (email, password_hash) VALUES ($1,'x') RETURNING id`, name+"@acme.test")
	ws := qStr(t, conn, `INSERT INTO workspaces (name, slug, owner_id) VALUES ($1,$1,$2) RETURNING id`, name+"-ws", user)
	return qStr(t, conn, `INSERT INTO pipelines (name, natural_language_request, created_by, workspace_id)
		VALUES ($1,'copy a->b',$2,$3) RETURNING id`, name, user, ws)
}

// TestMigration106_FreshBoot: the full runner applies every migration on an empty
// database, and a pipeline inserted without the column lands on layout 0 (108).
func TestMigration106_FreshBoot(t *testing.T) {
	conn, cleanup := freshSchema(t, testDSN(t))
	defer cleanup()

	if err := Migrate(migrationsDir); err != nil {
		t.Fatalf("full Migrate() including 106 failed on a fresh DB: %v", err)
	}
	for _, tbl := range []string{"object_load_counters", "object_load_reservations"} {
		if !qBool(t, conn, `SELECT to_regclass('public.`+tbl+`') IS NOT NULL`) {
			t.Errorf("%s missing after fresh boot", tbl)
		}
	}
	// 108 makes a new pipeline start undecided (0); the orchestrator picks 1 or 2.
	p := seedPipeline106(t, conn, "fresh")
	if v := qInt(t, conn, `SELECT storage_layout_version FROM pipelines WHERE id=$1`, p); v != 0 {
		t.Errorf("new pipeline storage_layout_version = %d, want 0 after the full runner (108)", v)
	}
}

// TestMigration106_ExistingRows: rows that predate 106 read 1, 3 is rejected, 2 is
// accepted, and re-applying the body changes nothing.
func TestMigration106_ExistingRows(t *testing.T) {
	conn, cleanup := freshSchema(t, testDSN(t))
	defer cleanup()

	applyBefore106(t, conn)
	old := seedPipeline106(t, conn, "old")
	exec106(t, conn)

	if v := qInt(t, conn, `SELECT storage_layout_version FROM pipelines WHERE id=$1`, old); v != 1 {
		t.Errorf("pre-106 pipeline storage_layout_version = %d, want 1", v)
	}
	if _, err := conn.Exec(`UPDATE pipelines SET storage_layout_version=3 WHERE id=$1`, old); err == nil {
		t.Errorf("storage_layout_version=3 must be rejected by the CHECK")
	}
	if _, err := conn.Exec(`UPDATE pipelines SET storage_layout_version=2 WHERE id=$1`, old); err != nil {
		t.Errorf("storage_layout_version=2 must be accepted: %v", err)
	}
	if _, err := conn.Exec(`INSERT INTO object_load_counters (pipeline_id, table_key) VALUES ($1,'db/public/t')`, old); err != nil {
		t.Fatalf("insert counter: %v", err)
	}

	exec106(t, conn) // re-apply must be a no-op
	if v := qInt(t, conn, `SELECT storage_layout_version FROM pipelines WHERE id=$1`, old); v != 2 {
		t.Errorf("re-applying 106 changed storage_layout_version to %d", v)
	}
	if n := qInt(t, conn, `SELECT count(*) FROM object_load_counters`); n != 1 {
		t.Errorf("re-applying 106 changed object_load_counters (count %d)", n)
	}
	if n := qInt(t, conn, `SELECT count(*) FROM pg_constraint WHERE conname='pipelines_storage_layout_version_check'`); n != 1 {
		t.Errorf("expected exactly one storage_layout_version CHECK after re-apply, found %d", n)
	}
	if got := qStr(t, conn, `SELECT next_seq::text || '/' || generation::text || '/' || cleaned_generation::text
		FROM object_load_counters WHERE pipeline_id=$1`, old); got != "1/0/-1" {
		t.Errorf("counter defaults next_seq/generation/cleaned_generation = %s, want 1/0/-1", got)
	}
}

// TestMigration106_ReservationUniqueKeysIncludeGeneration: a key or a LOAD number
// may repeat across generations but never within one, and rows cascade with the
// pipeline.
func TestMigration106_ReservationUniqueKeysIncludeGeneration(t *testing.T) {
	conn, cleanup := freshSchema(t, testDSN(t))
	defer cleanup()

	if err := Migrate(migrationsDir); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	p := seedPipeline106(t, conn, "resv")
	ins := func(gen int, key string, seq int) error {
		_, err := conn.Exec(`INSERT INTO object_load_reservations
			(pipeline_id, table_key, generation, reservation_key, load_seq, execution_id, dest_key)
			VALUES ($1,'db/public/t',$2,$3,$4,'exec-1','prefix/db/public/t/dt=2026-09-17/LOAD00000001.parquet')`,
			p, gen, key, seq)
		return err
	}
	if err := ins(0, "s|topic|0|100", 1); err != nil {
		t.Fatalf("first reservation: %v", err)
	}
	if err := ins(0, "s|topic|0|100", 2); err == nil {
		t.Errorf("same reservation_key in the same generation must be rejected")
	}
	if err := ins(0, "s|topic|0|200", 1); err == nil {
		t.Errorf("same load_seq in the same generation must be rejected")
	}
	if err := ins(1, "s|topic|0|100", 1); err != nil {
		t.Errorf("same key and load_seq in a new generation must be accepted: %v", err)
	}
	if err := ins(0, "s|topic|0|300", 100000001); err == nil {
		t.Errorf("load_seq above 100000000 must be rejected")
	}
	if _, err := conn.Exec(`INSERT INTO object_load_counters (pipeline_id, table_key, next_seq) VALUES ($1,'db/public/u',0)`, p); err == nil {
		t.Errorf("next_seq 0 must be rejected")
	}
	if _, err := conn.Exec(`INSERT INTO object_load_counters (pipeline_id, table_key) VALUES ($1,'db/public/t')`, p); err != nil {
		t.Fatalf("insert counter: %v", err)
	}

	if _, err := conn.Exec(`DELETE FROM pipelines WHERE id=$1`, p); err != nil {
		t.Fatalf("delete pipeline: %v", err)
	}
	if n := qInt(t, conn, `SELECT (SELECT count(*) FROM object_load_reservations) + (SELECT count(*) FROM object_load_counters)`); n != 0 {
		t.Errorf("reservations and counters must cascade with the pipeline, %d rows left", n)
	}
}
