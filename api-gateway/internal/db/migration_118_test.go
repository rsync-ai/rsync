package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Static guards for migration 118 (cdc_snapshot_requests records the initial
// load). Its statements are exercised against a real PostgreSQL by
// backend-orchestrator/internal/cdcsnapshot/store_pg_test.go (integration_pg
// tag, not built by CI); these pin, in every run, what the orchestrator relies on.

func readMigration118(t *testing.T) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join("..", "..", "migrations", "118_*.sql"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected exactly one 118_*.sql migration, found %v (err %v)", matches, err)
	}
	b, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read %s: %v", matches[0], err)
	}
	return normSQL(stripSQLComments(string(b)))
}

func TestMigration118_AllowsInitialAndKeepsEveryOtherSource(t *testing.T) {
	body := readMigration118(t)
	// Dropping the CHECK without re-adding it would accept any source; re-adding
	// it without a caller's source would fail every Re-snapshot insert.
	for _, want := range []string{
		"drop constraint if exists cdc_snapshot_requests_source_check",
		"add constraint cdc_snapshot_requests_source_check check (source in ('resnapshot', 'table_edit', 'auto_pickup', 'initial'))",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("migration 118 lost %q", want)
		}
	}
}

func TestMigration118_OneOpenInitialLoadPerPipeline(t *testing.T) {
	body := readMigration118(t)
	// InsertInitial is ON CONFLICT DO NOTHING: without this index two
	// orchestrators that see the same snapshot rows both record the load.
	want := "create unique index if not exists uq_cdc_snapshot_requests_open_initial on cdc_snapshot_requests (pipeline_id) where source = 'initial' and status in ('sent', 'started')"
	if !strings.Contains(body, want) {
		t.Errorf("migration 118 lost its partial unique index %q", want)
	}
	// The runner wraps a file in a transaction unless it holds BEGIN; — and
	// CONCURRENTLY cannot run inside one.
	if strings.Contains(body, "concurrently") || strings.Contains(body, "begin;") {
		t.Error("migration 118 must run inside the runner's transaction")
	}
}
