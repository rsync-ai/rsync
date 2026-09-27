package db

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Static guards for migration 119 (a model run no longer moves its saved query's
// updated_at). The trigger's behaviour is pinned against a real PostgreSQL in
// migration_119_integration_test.go, which CI does not build (integration tag);
// these pin, in every run, the two lists the trigger depends on staying true: which
// columns it skips, and which columns the handlers write.

// runPathFiles are the handler files whose saved_queries UPDATEs are a model run
// stamping its outcome, not anyone editing the query.
var runPathFiles = map[string]bool{
	"saved_query_models.go":      true, // stampLastRunOutcome, recordTargetOwnership
	"saved_query_run_failure.go": true, // stampLastRunFailureIfLatest
}

func readMigration119(t *testing.T) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join("..", "..", "migrations", "119_*.sql"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected exactly one 119_*.sql migration, found %v (err %v)", matches, err)
	}
	b, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read %s: %v", matches[0], err)
	}
	return normSQL(stripSQLComments(string(b)))
}

// skippedColumns119 returns the columns the trigger subtracts before comparing NEW
// with OLD: the run bookkeeping whose change alone is not an edit.
func skippedColumns119(t *testing.T, body string) []string {
	t.Helper()
	decl := regexp.MustCompile(`run_only_columns text\[\] := array\[([^\]]*)\];`).FindStringSubmatch(body)
	if decl == nil {
		t.Fatal("migration 119 no longer declares run_only_columns text[] := ARRAY[...]")
	}
	// Both sides of the comparison subtract that one list, so they cannot disagree.
	if !strings.Contains(body, "if (to_jsonb(new) - run_only_columns) is not distinct from (to_jsonb(old) - run_only_columns) then new.updated_at = old.updated_at; else new.updated_at = now(); end if;") {
		t.Fatal("migration 119's trigger no longer keeps OLD.updated_at when only run_only_columns changed, else NOW()")
	}
	var cols []string
	for _, c := range strings.Split(decl[1], ",") {
		cols = append(cols, strings.Trim(strings.TrimSpace(c), "'"))
	}
	return cols
}

// savedQueriesColumns returns every column saved_queries has been given: 084's
// CREATE TABLE plus each later ADD COLUMN.
func savedQueriesColumns(t *testing.T) map[string]bool {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "migrations", "084_saved_queries.sql"))
	if err != nil {
		t.Fatalf("read 084: %v", err)
	}
	create := regexp.MustCompile(`(?is)create\s+table\s+if\s+not\s+exists\s+saved_queries\s*\((.*?)\n\);`).
		FindStringSubmatch(stripSQLComments(string(b)))
	if create == nil {
		t.Fatal("084 no longer has CREATE TABLE IF NOT EXISTS saved_queries (...);")
	}
	// One column per line, lower-case name then its type; CHECK lines are upper-case.
	reCol := regexp.MustCompile(`^\s*([a-z_]+)\s+[A-Z]`)
	cols := map[string]bool{}
	for _, line := range strings.Split(create[1], "\n") {
		if m := reCol.FindStringSubmatch(line); m != nil {
			cols[m[1]] = true
		}
	}
	files, _ := filepath.Glob(filepath.Join("..", "..", "migrations", "*.sql"))
	reAlter := regexp.MustCompile(`(?is)alter\s+table\s+(?:if\s+exists\s+)?saved_queries\b(.*?);`)
	reAdd := regexp.MustCompile(`(?i)add\s+column\s+(?:if\s+not\s+exists\s+)?([a-z_]+)`)
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		for _, alter := range reAlter.FindAllStringSubmatch(stripSQLComments(string(b)), -1) {
			for _, add := range reAdd.FindAllStringSubmatch(alter[1], -1) {
				cols[strings.ToLower(add[1])] = true
			}
		}
	}
	if len(cols) < 15 || !cols["sql_text"] || !cols["last_run_status"] {
		t.Fatalf("read only %d saved_queries columns (%v); the parser is broken", len(cols), cols)
	}
	return cols
}

type savedQueryUpdate struct {
	file string
	cols []string
}

// savedQueryUpdates returns every `UPDATE saved_queries SET ... WHERE` a handler
// sends, with the columns it assigns. A mention in prose or an error message has a
// quote before its WHERE and is not matched.
func savedQueryUpdates(t *testing.T) []savedQueryUpdate {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "handlers", "*.go"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no handler files found (err %v)", err)
	}
	reUpdate := regexp.MustCompile("(?is)update\\s+saved_queries\\s+set\\s+([^`\"]*?)\\s+where\\b")
	reAssign := regexp.MustCompile(`(?:^|,)\s*([a-z_]+)\s*=`)
	var out []savedQueryUpdate
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		for _, m := range reUpdate.FindAllStringSubmatch(string(b), -1) {
			var cols []string
			for _, a := range reAssign.FindAllStringSubmatch(strings.ToLower(m[1]), -1) {
				cols = append(cols, a[1])
			}
			out = append(out, savedQueryUpdate{file: filepath.Base(f), cols: cols})
		}
	}
	return out
}

func TestMigration119_TriggerRunsTheNewFunction(t *testing.T) {
	body := readMigration119(t)
	for _, want := range []string{
		"create or replace function saved_queries_touch_updated_at() returns trigger",
		"drop trigger if exists update_saved_queries_updated_at on saved_queries;",
		"create trigger update_saved_queries_updated_at before update on saved_queries for each row execute function saved_queries_touch_updated_at();",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("migration 119 lost %q", want)
		}
	}
	// The shared function serves a dozen other tables; this change is saved_queries only.
	if strings.Contains(body, "function update_updated_at_column") {
		t.Error("migration 119 redefines update_updated_at_column(), which every other table's trigger uses")
	}
}

func TestMigration119_SkipsOnlyRealBookkeepingColumns(t *testing.T) {
	skipped := skippedColumns119(t, readMigration119(t))
	cols := savedQueriesColumns(t)
	for _, c := range skipped {
		// A misspelt name subtracts nothing, so the column it meant still moves updated_at.
		if !cols[c] {
			t.Errorf("migration 119 skips %q, which is not a saved_queries column", c)
		}
	}
	got := append([]string(nil), skipped...)
	sort.Strings(got)
	want := []string{"last_run_at", "last_run_error", "last_run_status", "target_owned", "updated_at"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("migration 119 skips %v, want %v; a column added here stops moving updated_at, "+
			"which the editor's stale-write guard relies on", got, want)
	}
}

// Every handler write either is a run stamping its outcome, and touches only skipped
// columns, or is an edit, and touches at least one column that moves updated_at.
// This is what keeps the editor's concurrency token sound: a write that someone's
// open edit could overwrite always moves it.
func TestMigration119_HandlerWritesMatchTheSkipList(t *testing.T) {
	skip := map[string]bool{}
	for _, c := range skippedColumns119(t, readMigration119(t)) {
		skip[c] = true
	}
	updates := savedQueryUpdates(t)
	var runs, edits int
	for _, u := range updates {
		if len(u.cols) == 0 {
			t.Errorf("%s: an UPDATE saved_queries whose SET clause yielded no columns; the parser is broken", u.file)
			continue
		}
		var moving []string
		for _, c := range u.cols {
			if !skip[c] {
				moving = append(moving, c)
			}
		}
		if runPathFiles[u.file] {
			runs++
			if len(moving) > 0 {
				t.Errorf("%s: a run-path UPDATE writes %v, which migration 119 does not skip, so every run "+
					"moves updated_at again; add the column to 119's list (and this test's want) "+
					"or move the statement out of the run path", u.file, moving)
			}
			continue
		}
		edits++
		if len(moving) == 0 {
			t.Errorf("%s: an UPDATE writes only %v, all skipped by migration 119, so it never moves "+
				"updated_at and an open edit can silently overwrite it", u.file, u.cols)
		}
	}
	// Three run stamps and six edits today. Fewer means the scan stopped matching.
	if runs < 3 || edits < 5 {
		t.Fatalf("found %d run-path and %d other saved_queries UPDATEs; the scan is broken, not the code clean", runs, edits)
	}
}
