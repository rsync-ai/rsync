package db

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Static guards for migration 106 (object-storage layout v2 schema).
//
// The real-database checks live in migration_106_integration_test.go behind the
// `integration` tag, which CI does not pass. These read the .sql file instead, so
// the properties later layout-v2 PRs depend on are pinned in the default suite:
//
//   - every pipeline stays on layout 1 (the default is 1, not 2);
//   - only 1 and 2 are accepted;
//   - a LOAD reservation is unique per generation, both by key and by number;
//   - the migration is additive and safe to re-run.

func readMigration106(t *testing.T) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join("..", "..", "migrations", "106_*.sql"))
	if err != nil {
		t.Fatalf("glob 106: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected exactly one 106_*.sql migration, found %v", matches)
	}
	b, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read %s: %v", matches[0], err)
	}
	return stripSQLComments(string(b))
}

var reWS = regexp.MustCompile(`\s+`)

// normSQL collapses whitespace and lower-cases, so the asserts below match the
// statement, not its formatting.
func normSQL(s string) string {
	return strings.TrimSpace(reWS.ReplaceAllString(strings.ToLower(s), " "))
}

// tableBody returns the column/constraint list of `CREATE TABLE IF NOT EXISTS <name> (...)`.
func tableBody(t *testing.T, sqlText, name string) string {
	t.Helper()
	re := regexp.MustCompile(`(?is)create\s+table\s+if\s+not\s+exists\s+` + name + `\s*\((.*?)\)\s*;`)
	m := re.FindStringSubmatch(sqlText)
	if m == nil {
		t.Fatalf("migration 106 does not CREATE TABLE IF NOT EXISTS %s", name)
	}
	return normSQL(m[1])
}

func TestMigration106_LayoutVersionDefaultsToOneAndAcceptsOnlyOneOrTwo(t *testing.T) {
	body := normSQL(readMigration106(t))

	col := regexp.MustCompile(`alter table pipelines add column if not exists storage_layout_version smallint not null default (\d+)`)
	m := col.FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("migration 106 must add pipelines.storage_layout_version SMALLINT NOT NULL with a default, idempotently")
	}
	if m[1] != "1" {
		t.Fatalf("storage_layout_version default is %s; it must stay 1 until the layout flip migration", m[1])
	}
	if !strings.Contains(body, "check (storage_layout_version in (1, 2))") {
		t.Fatalf("storage_layout_version must be constrained to CHECK (storage_layout_version IN (1, 2))")
	}
	if strings.Contains(body, "set default 2") {
		t.Fatalf("migration 106 must not flip the layout default to 2")
	}
}

func TestMigration106_ReservationUniqueKeysIncludeGenerationInSQL(t *testing.T) {
	body := tableBody(t, readMigration106(t), "object_load_reservations")

	uniques := regexp.MustCompile(`unique \(([^)]*)\)`).FindAllStringSubmatch(body, -1)
	got := map[string]bool{}
	for _, u := range uniques {
		got[normSQL(u[1])] = true
	}
	for _, want := range []string{
		"pipeline_id, table_key, generation, reservation_key",
		"pipeline_id, table_key, generation, load_seq",
	} {
		if !got[want] {
			t.Errorf("object_load_reservations lacks UNIQUE (%s); found %v", want, got)
		}
	}
	if len(uniques) != 2 {
		t.Errorf("expected exactly 2 UNIQUE constraints on object_load_reservations, found %d", len(uniques))
	}
	for _, want := range []string{
		"pipeline_id uuid not null references pipelines(id) on delete cascade",
		"generation bigint not null",
		"reservation_key text not null",
		"load_seq bigint not null",
		"check (load_seq between 1 and 100000000)",
		"execution_id text",
		"dest_key varchar(1024)",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("object_load_reservations is missing %q", want)
		}
	}
}

func TestMigration106_CountersArePerPipelineTableAndStartUncleaned(t *testing.T) {
	body := tableBody(t, readMigration106(t), "object_load_counters")
	for _, want := range []string{
		"pipeline_id uuid not null references pipelines(id) on delete cascade",
		"table_key text not null",
		"generation bigint not null default 0",
		"next_seq bigint not null default 1",
		"check (next_seq between 1 and 100000000)",
		"cleaned_generation bigint not null default -1",
		"primary key (pipeline_id, table_key)",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("object_load_counters is missing %q", want)
		}
	}
}

func TestMigration106_IsAdditiveAndReRunnable(t *testing.T) {
	body := normSQL(readMigration106(t))

	for _, forbidden := range []string{"drop ", "delete from", "update ", "truncate", "rename "} {
		if strings.Contains(body, forbidden) {
			t.Errorf("migration 106 must be additive; found %q", forbidden)
		}
	}
	creates := regexp.MustCompile(`create table (if not exists )?`).FindAllStringSubmatch(body, -1)
	if len(creates) != 2 {
		t.Fatalf("expected 2 CREATE TABLE statements, found %d", len(creates))
	}
	for _, c := range creates {
		if c[1] == "" {
			t.Errorf("every CREATE TABLE in migration 106 must use IF NOT EXISTS")
		}
	}
	if n := strings.Count(body, "add column "); n != strings.Count(body, "add column if not exists ") {
		t.Errorf("every ADD COLUMN in migration 106 must use IF NOT EXISTS")
	}
	// A file containing "BEGIN;" is run outside the runner's transaction, which
	// would record the version separately from the schema change.
	if strings.Contains(body, "begin;") {
		t.Errorf("migration 106 must run inside the runner's transaction (no BEGIN;)")
	}
}
