package db

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// A migration may not touch a table an earlier migration dropped.
//
// Migrate() applies every *.sql in sort.Strings order and RETURNS on the first
// failure, so one file referencing a table that is not there stops the sequence
// dead: markSchemaReady() is never reached, /ready answers 503
// schema_not_migrated, and -- because main.go logs the migration error and
// starts serving anyway -- the container looks up while the schema is frozen at
// whatever the previous file left behind.
//
// That is not hypothetical. `100_sentinel_config_backend_neutral_keys.sql`
// shipped renaming three rows in `sentinel_config`, a table
// `013_cleanup_unused_tables.sql` drops and nothing recreates. Replaying all
// 105 files against postgres:16 in the runner's order: 104 applied, 100 failed
// with `relation "sentinel_config" does not exist` (SQLSTATE 42P01). It broke
// every install, fresh and upgrade alike, and it reached a live host.
//
// Nothing else in CI could catch it. The Go suite compiles code and never reads
// a .sql file; the real-database tests are behind `//go:build integration_pg`
// and CI runs `go test ./...` with no `-tags`, so they do not run at all. A
// migration is otherwise only exercised by starting the product.
//
// This scan is static on purpose: it needs no database, so it runs in the
// DEFAULT suite of a job the `go` paths-filter already points at
// `api-gateway/**`. The filter covers the subject; the guard is in the suite
// that filter runs.
//
// The exemption is the repo's own convention, not an invention here: 077 and
// 078 touch other tables 013 dropped and wrap each in
// `IF to_regclass('public.<table>') IS NOT NULL THEN ... END IF;` inside a DO
// block, which is why those two apply cleanly on a schema where the tables are
// absent. A reference to a dropped table is a finding UNLESS the same file
// names that table in a to_regclass guard.

var (
	sqlIdent = `(?:"?[a-zA-Z_][a-zA-Z0-9_]*"?\.)?"?([a-zA-Z_][a-zA-Z0-9_]*)"?`

	reCreateTable = regexp.MustCompile(`(?i)\bCREATE\s+(?:UNLOGGED\s+|TEMP\s+|TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + sqlIdent)
	reDropTable   = regexp.MustCompile(`(?i)\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?` + sqlIdent)
	reRenameTable = regexp.MustCompile(`(?i)\bALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?` + sqlIdent + `\s+RENAME\s+TO\b`)

	reLineComment  = regexp.MustCompile(`--[^\n]*`)
	reBlockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)

	// Every way this corpus names a table it reads or writes. `ALTER TABLE`
	// carries its own optional IF EXISTS, which is itself a guard, so it is
	// captured as group 1 and the reference skipped when present.
	tableRefs = []struct {
		kind string
		re   *regexp.Regexp
	}{
		{"FROM", regexp.MustCompile(`(?i)\bFROM\s+(?:ONLY\s+)?` + sqlIdent)},
		{"JOIN", regexp.MustCompile(`(?i)\bJOIN\s+` + sqlIdent)},
		{"UPDATE", regexp.MustCompile(`(?i)\bUPDATE\s+(?:ONLY\s+)?` + sqlIdent)},
		{"INSERT INTO", regexp.MustCompile(`(?i)\bINSERT\s+INTO\s+` + sqlIdent)},
		{"DELETE FROM", regexp.MustCompile(`(?i)\bDELETE\s+FROM\s+(?:ONLY\s+)?` + sqlIdent)},
	}
	reAlterTable = regexp.MustCompile(`(?i)\bALTER\s+TABLE\s+(IF\s+EXISTS\s+)?(?:ONLY\s+)?` + sqlIdent)
)

type migrationFile struct {
	name string
	body string
}

type deadRef struct {
	file  string
	kind  string
	table string
}

type lifecycleScan struct {
	findings []deadRef
	// Denominators. A scan that read nothing reports no findings and looks
	// exactly like a clean corpus, so every assertion below is floored.
	files       int
	everCreated int
	liveAtEnd   int
	refsChecked int
	renames     int
}

func stripSQLComments(s string) string {
	return reBlockComment.ReplaceAllString(reLineComment.ReplaceAllString(s, " "), " ")
}

// guardsTable reports whether the file wraps work on this table in the
// to_regclass existence check 077 and 078 established.
func guardsTable(body, table string) bool {
	lowered := strings.ToLower(body)
	for _, form := range []string{
		fmt.Sprintf("to_regclass('public.%s')", table),
		fmt.Sprintf("to_regclass('%s')", table),
	} {
		if strings.Contains(lowered, form) {
			return true
		}
	}
	return false
}

// scanMigrations walks the sequence in apply order, tracking which tables are
// live, and reports every reference to a table the sequence itself created and
// then dropped. Tables it never saw created (system catalogs, CTE aliases, a
// function name after EXTRACT ... FROM) are not its business and are ignored.
func scanMigrations(files []migrationFile) lifecycleScan {
	var scan lifecycleScan
	live := map[string]bool{}
	known := map[string]bool{}
	seen := map[deadRef]bool{}

	for _, f := range files {
		body := stripSQLComments(f.body)
		scan.files++
		scan.renames += len(reRenameTable.FindAllStringSubmatch(body, -1))

		// References are judged against the schema as it stands BEFORE this
		// file runs -- which is what Postgres sees when the file executes.
		check := func(kind, table string) {
			table = strings.ToLower(table)
			if !known[table] {
				return
			}
			scan.refsChecked++
			if live[table] || guardsTable(body, table) {
				return
			}
			d := deadRef{file: f.name, kind: kind, table: table}
			if !seen[d] {
				seen[d] = true
				scan.findings = append(scan.findings, d)
			}
		}
		for _, r := range tableRefs {
			for _, m := range r.re.FindAllStringSubmatch(body, -1) {
				check(r.kind, m[1])
			}
		}
		for _, m := range reAlterTable.FindAllStringSubmatch(body, -1) {
			if strings.TrimSpace(m[1]) != "" {
				continue // ALTER TABLE IF EXISTS is its own guard
			}
			check("ALTER TABLE", m[2])
		}

		for _, m := range reCreateTable.FindAllStringSubmatch(body, -1) {
			t := strings.ToLower(m[1])
			live[t] = true
			known[t] = true
		}
		for _, m := range reDropTable.FindAllStringSubmatch(body, -1) {
			delete(live, strings.ToLower(m[1]))
		}
	}
	scan.everCreated = len(known)
	scan.liveAtEnd = len(live)
	return scan
}

func loadMigrations(t *testing.T) []migrationFile {
	t.Helper()
	dir := filepath.Join("..", "..", "migrations")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("cannot read migrations directory %s: %v", dir, err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // the order Migrate() applies them in
	files := make([]migrationFile, 0, len(names))
	for _, n := range names {
		b, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			t.Fatalf("cannot read migration %s: %v", n, err)
		}
		files = append(files, migrationFile{name: n, body: string(b)})
	}
	return files
}

func TestNoMigrationTouchesATableAnEarlierMigrationDropped(t *testing.T) {
	scan := scanMigrations(loadMigrations(t))

	// Floors, so an empty or failed enumeration cannot report a clean tree.
	// Measured 2026-09-09: 105 files, 79 tables ever created, 68 live at the
	// end. These are well under that -- they catch a scan of nothing, not
	// growth.
	if scan.files < 90 {
		t.Fatalf("scanned only %d migrations; the enumeration is broken, not the corpus clean", scan.files)
	}
	if scan.everCreated < 50 {
		t.Fatalf("saw only %d CREATE TABLE statements across %d files; the parser is broken", scan.everCreated, scan.files)
	}
	if scan.refsChecked < 100 {
		t.Fatalf("checked only %d references to known tables; the reference patterns match nothing", scan.refsChecked)
	}

	if len(scan.findings) > 0 {
		var lines []string
		for _, d := range scan.findings {
			lines = append(lines, fmt.Sprintf("  %s: %s %s", d.file, d.kind, d.table))
		}
		t.Fatalf("these migrations touch a table an earlier migration dropped and never guard it "+
			"with to_regclass, so the runner stops there and the schema never becomes ready:\n%s\n"+
			"Fix it the way 077 and 078 do: wrap the statements in "+
			"`DO $$ BEGIN IF to_regclass('public.<table>') IS NOT NULL THEN ... END IF; END $$;`.",
			strings.Join(lines, "\n"))
	}
}

// The scan has no way to express a renamed table, and the corpus has no
// ALTER TABLE ... RENAME TO. If one appears, the tracking above silently keeps
// the old name live and loses the new one -- a hole, not a pass. Fail here so
// the guard gets taught the shape before it starts lying.
func TestTheCorpusStillContainsNoTableRename(t *testing.T) {
	scan := scanMigrations(loadMigrations(t))
	if scan.renames != 0 {
		t.Fatalf("found %d ALTER TABLE ... RENAME TO statements; scanMigrations does not track renames, "+
			"so teach it before relying on this suite again", scan.renames)
	}
}

// Positive control. Without it, a scan whose reference patterns matched nothing
// would pass the test above on a genuinely broken corpus.
func TestTheScanDetectsAnUnguardedReferenceToADroppedTable(t *testing.T) {
	corpus := []migrationFile{
		{"001_create.sql", "CREATE TABLE widgets (id int); CREATE TABLE gadgets (id int);"},
		{"002_drop.sql", "DROP TABLE IF EXISTS widgets CASCADE;"},
		{"003_touch.sql", "UPDATE widgets SET id = 1; UPDATE gadgets SET id = 2;"},
	}
	scan := scanMigrations(corpus)
	if len(scan.findings) != 1 {
		t.Fatalf("expected exactly the widgets reference, got %v", scan.findings)
	}
	got := scan.findings[0]
	if got.file != "003_touch.sql" || got.table != "widgets" {
		t.Fatalf("wrong finding: %+v", got)
	}
}

// The other half of the control: the exemption must actually exempt, or the
// test above would fail on 077 and 078 and someone would delete the guard.
func TestTheScanAcceptsATo_regclassGuardedReference(t *testing.T) {
	corpus := []migrationFile{
		{"001_create.sql", "CREATE TABLE widgets (id int);"},
		{"002_drop.sql", "DROP TABLE IF EXISTS widgets CASCADE;"},
		{"003_touch.sql", "DO $$ BEGIN IF to_regclass('public.widgets') IS NOT NULL THEN UPDATE widgets SET id = 1; END IF; END $$;"},
	}
	if scan := scanMigrations(corpus); len(scan.findings) != 0 {
		t.Fatalf("a to_regclass-guarded reference must be accepted, got %v", scan.findings)
	}
}
