//go:build integration

// Integration test for migration 108 (layout 0 = undecided for new pipelines).
// Same gate and helpers as migration_106_integration_test.go.
//
//	docker run -d --rm --name pg108 -e POSTGRES_PASSWORD=pg -p 55434:5432 postgres:16-alpine
//	MIGRATION_TEST_DSN='postgres://postgres:pg@localhost:55434/postgres?sslmode=disable' \
//	  go test -tags=integration -run 'TestMigration10[68]' ./internal/db/ -v
//	docker rm -f pg108
package db

import (
	"os"
	"path/filepath"
	"testing"
)

// TestMigration108_ExistingRowsStayOnLayoutOne: a pipeline created before 108 keeps
// layout 1, a pipeline created after it starts at 0, 3 is still rejected, and
// re-applying the body changes nothing.
func TestMigration108_ExistingRowsStayOnLayoutOne(t *testing.T) {
	conn, cleanup := freshSchema(t, testDSN(t))
	defer cleanup()

	applyBefore106(t, conn)
	exec106(t, conn)
	old := seedPipeline106(t, conn, "old")

	matches, err := filepath.Glob(filepath.Join(migrationsDir, "108_*.sql"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected exactly one 108_*.sql migration, found %v (err %v)", matches, err)
	}
	body, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read 108: %v", err)
	}
	for i := 0; i < 2; i++ { // the second run proves it is re-runnable
		if _, err := conn.Exec(string(body)); err != nil {
			t.Fatalf("exec 108 (run %d): %v", i+1, err)
		}
	}

	if v := qInt(t, conn, `SELECT storage_layout_version FROM pipelines WHERE id=$1`, old); v != 1 {
		t.Errorf("pre-108 pipeline storage_layout_version = %d, want 1", v)
	}
	fresh := seedPipeline106(t, conn, "fresh")
	if v := qInt(t, conn, `SELECT storage_layout_version FROM pipelines WHERE id=$1`, fresh); v != 0 {
		t.Errorf("post-108 pipeline storage_layout_version = %d, want 0", v)
	}
	if _, err := conn.Exec(`UPDATE pipelines SET storage_layout_version=3 WHERE id=$1`, fresh); err == nil {
		t.Errorf("storage_layout_version=3 must be rejected by the CHECK")
	}
	if _, err := conn.Exec(`UPDATE pipelines SET storage_layout_version=2 WHERE id=$1`, fresh); err != nil {
		t.Errorf("storage_layout_version=2 must be accepted: %v", err)
	}
	if n := qInt(t, conn, `SELECT count(*) FROM pg_constraint WHERE conname='pipelines_storage_layout_version_check'`); n != 1 {
		t.Errorf("expected exactly one storage_layout_version CHECK, found %d", n)
	}
}
