//go:build integration

// Integration test for migration 120 (connector_catalog row for sample-data). Same
// gate and helpers as migration_069_integration_test.go.
//
//	docker run -d --rm --name pg120 -e POSTGRES_PASSWORD=pg -p 55435:5432 postgres:16-alpine
//	MIGRATION_TEST_DSN='postgres://postgres:pg@localhost:55435/postgres?sslmode=disable' \
//	  go test -tags=integration -run TestMigration120 ./internal/db/ -v
//	docker rm -f pg120
package db

import (
	"os"
	"path/filepath"
	"testing"
)

// isKnownConnector120 is the chat handler's exact query (chat_nl_pipeline.go
// isKnownConnector), so a pass here means the handler would count the row.
const isKnownConnector120 = `SELECT COUNT(*) FROM connector_catalog WHERE name = $1 AND status = 'active'`

// TestMigration120_SampleDataIsKnownAndReapplySafe: after a full Migrate() the
// chat's query counts one sample-data row; with it deleted, applying 120's body
// alone brings it back; a disabled row is re-activated by a second apply; and the
// version row stays single.
func TestMigration120_SampleDataIsKnownAndReapplySafe(t *testing.T) {
	conn, cleanup := freshSchema(t, testDSN(t))
	defer cleanup()
	if err := Migrate(migrationsDir); err != nil {
		t.Fatalf("full Migrate() including 120 failed on a fresh DB: %v", err)
	}
	if n := qInt(t, conn, isKnownConnector120, "sample-data"); n != 1 {
		t.Fatalf("after 120 the chat's catalog query counts %d sample-data rows, want 1", n)
	}
	if img := qStr(t, conn, `SELECT v.docker_image FROM connector_versions v
		JOIN connector_catalog c ON c.id = v.connector_id WHERE c.name = 'sample-data'`); img != "mcp-sample-data:v1.0.0" {
		t.Errorf("sample-data version row image = %q, want mcp-sample-data:v1.0.0", img)
	}

	// Delete the row so the re-apply below has to recreate it: that, not the count
	// after Migrate(), proves the row is 120's doing and not an earlier migration's.
	matches, err := filepath.Glob(filepath.Join(migrationsDir, "120_*.sql"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected exactly one 120_*.sql, found %v (err %v)", matches, err)
	}
	body, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read %s: %v", matches[0], err)
	}
	if _, err := conn.Exec(`DELETE FROM connector_catalog WHERE name = 'sample-data'`); err != nil {
		t.Fatalf("delete control row: %v", err)
	}
	if n := qInt(t, conn, isKnownConnector120, "sample-data"); n != 0 {
		t.Fatalf("with the row deleted the query still counts %d", n)
	}

	// Re-apply twice, the second time over a disabled row.
	if _, err := conn.Exec(string(body)); err != nil {
		t.Fatalf("re-apply 120: %v", err)
	}
	if _, err := conn.Exec(`UPDATE connector_catalog SET status = 'disabled' WHERE name = 'sample-data'`); err != nil {
		t.Fatalf("disable row: %v", err)
	}
	if _, err := conn.Exec(string(body)); err != nil {
		t.Fatalf("second re-apply of 120: %v", err)
	}
	if n := qInt(t, conn, isKnownConnector120, "sample-data"); n != 1 {
		t.Errorf("after re-applying 120 over a disabled row the query counts %d, want 1", n)
	}
	if n := qInt(t, conn, `SELECT COUNT(*) FROM connector_versions v
		JOIN connector_catalog c ON c.id = v.connector_id WHERE c.name = 'sample-data'`); n != 1 {
		t.Errorf("sample-data has %d connector_versions rows after re-applying 120, want 1", n)
	}
}
