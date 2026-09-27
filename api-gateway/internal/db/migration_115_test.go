package db

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Static guards for migration 115 (snapshot reads a pre-114 producer counted as
// inserts). The data behaviour is pinned against a real PostgreSQL in
// migration_115_integration_test.go, which CI does not build (integration tag);
// these pin, in every run, what the migration must never do.

func readMigration115(t *testing.T) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join("..", "..", "migrations", "115_*.sql"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected exactly one 115_*.sql migration, found %v (err %v)", matches, err)
	}
	b, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read %s: %v", matches[0], err)
	}
	return normSQL(stripSQLComments(string(b)))
}

func TestMigration115_NeverLowersWhatOthersSeedOrSum(t *testing.T) {
	body := readMigration115(t)
	// The stats consumer seeds from `inserts` and would write the old value back;
	// the backlog is total_events - applied_total_events; inserted_rows feeds
	// records_processed. Reads are events: none of these move.
	for _, col := range []string{"inserts", "total_events", "applied_total_events", "inserted_rows"} {
		if regexp.MustCompile(`(^|[\s,(])` + col + ` =`).MatchString(body) {
			t.Errorf("migration 115 assigns %s; it must only record legacy_snapshot_reads for the captured side", col)
		}
	}
}

func TestMigration115_TouchesOnlyPre114RowsOnce(t *testing.T) {
	body := readMigration115(t)
	for _, want := range []string{
		"s.legacy_snapshot_reads is null",
		"s.created_at < ( select applied_at from schema_migrations where version = '114_table_stats_snapshot_rows.sql' )",
		"storage_type = 'cdc' and last_error is null",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("migration 115 lost its guard %q", want)
		}
	}
	// The version it compares against must be the file the runner recorded.
	if _, err := os.Stat(filepath.Join("..", "..", "migrations", "114_table_stats_snapshot_rows.sql")); err != nil {
		t.Errorf("migration 115 compares against 114_table_stats_snapshot_rows.sql, which is gone: %v", err)
	}
}
