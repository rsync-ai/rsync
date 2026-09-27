package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Static guards for migration 120 (connector_catalog row for sample-data). The chat
// NL handler's isKnownConnector reads exactly `name = $1 AND status = 'active'`, so
// the row must carry that name and that status or the zero-credential demo falls
// through to the LLM again. Real-PostgreSQL behaviour (the row lands, a second run
// is a no-op) is migration_120_integration_test.go (integration tag).

func readMigration120(t *testing.T) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join("..", "..", "migrations", "120_*.sql"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected exactly one 120_*.sql migration, found %v (err %v)", matches, err)
	}
	b, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read %s: %v", matches[0], err)
	}
	return normSQL(stripSQLComments(string(b)))
}

func TestMigration120_SeedsSampleDataActive(t *testing.T) {
	body := readMigration120(t)
	// The name is the connector_type the demo seeds its connection with
	// (demo.go demoSourceConnector) and the id chat.NormalizeConnectorName
	// returns; 'active' is the only status isKnownConnector counts.
	for _, want := range []string{
		"insert into connector_catalog",
		"('sample-data', 'sample data (demo)',",
		"'none', 'active')",
		"'mcp-' || name || ':v1.0.0'",
		"where name = 'sample-data'",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("migration 120 lost %q", want)
		}
	}
}

func TestMigration120_IsIdempotent(t *testing.T) {
	body := readMigration120(t)
	// The migrator keys schema_migrations on the filename, so a rename re-applies
	// the file; both inserts must tolerate the row already being there, and the
	// catalog upsert must restore status = 'active' if someone disabled it.
	for _, want := range []string{
		"on conflict (name) do update set",
		"status = excluded.status",
		"on conflict (connector_id, version) do nothing",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("migration 120 is not idempotent: missing %q", want)
		}
	}
	if strings.Count(body, "insert into") != strings.Count(body, "on conflict") {
		t.Errorf("every INSERT in migration 120 needs an ON CONFLICT clause")
	}
	// The runner wraps a file in a transaction unless it holds BEGIN;.
	if strings.Contains(body, "begin;") {
		t.Error("migration 120 must run inside the runner's transaction")
	}
}
