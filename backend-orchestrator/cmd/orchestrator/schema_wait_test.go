package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// expectBootSchemaRound queues one missingBootSchema pass; present[i] answers
// bootSchema[i].
func expectBootSchemaRound(mock sqlmock.Sqlmock, present ...bool) {
	for i, c := range bootSchema {
		row := sqlmock.NewRows([]string{"ok"}).AddRow(present[i])
		if c.column == "" {
			mock.ExpectQuery(`to_regclass`).WithArgs(c.table).WillReturnRows(row)
		} else {
			mock.ExpectQuery(`information_schema\.columns`).WithArgs(c.table, c.column).WillReturnRows(row)
		}
	}
}

func allPresent(v bool) []bool {
	out := make([]bool, len(bootSchema))
	for i := range out {
		out[i] = v
	}
	return out
}

func TestWaitForBootSchemaReturnsAtOnceWhenMigrated(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectBootSchemaRound(mock, allPresent(true)...)

	slept := 0
	missing := waitForBootSchema(context.Background(), db, time.Minute, time.Second, func(time.Duration) { slept++ })
	if missing != nil || slept != 0 {
		t.Fatalf("missing=%v slept=%d, want nil and 0", missing, slept)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// The fresh-install case: the gateway's migrations land while the orchestrator
// waits, and the workers start only after the second check sees them.
func TestWaitForBootSchemaWaitsForTheMigrations(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectBootSchemaRound(mock, allPresent(false)...)
	expectBootSchemaRound(mock, allPresent(true)...)

	slept := 0
	missing := waitForBootSchema(context.Background(), db, time.Minute, time.Second, func(time.Duration) { slept++ })
	if missing != nil || slept != 1 {
		t.Fatalf("missing=%v slept=%d, want nil and 1", missing, slept)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// An orchestrator whose database nothing migrates must still start: the wait
// gives up at the deadline and reports exactly what it was waiting for.
func TestWaitForBootSchemaGivesUpAndNamesWhatIsMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	present := allPresent(true)
	present[1] = false // connections.connector_version
	expectBootSchemaRound(mock, present...)

	missing := waitForBootSchema(context.Background(), db, 0, time.Second, func(time.Duration) {
		t.Fatal("a zero timeout must not sleep")
	})
	if len(missing) != 1 || missing[0] != "connections.connector_version" {
		t.Fatalf("missing=%v, want [connections.connector_version]", missing)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBootSchemaWaitTimeoutStaysUnderTheChartLivenessWindow(t *testing.T) {
	t.Setenv("ORCHESTRATOR_SCHEMA_WAIT_TIMEOUT", "")
	// deploy/helm/rsync-ai orchestrator livenessProbe: initialDelaySeconds 90,
	// periodSeconds 30, failureThreshold 6.
	if got, limit := bootSchemaWaitTimeout(), 90*time.Second+6*30*time.Second; got >= limit {
		t.Fatalf("default wait %s would outlast the liveness window %s", got, limit)
	}
	t.Setenv("ORCHESTRATOR_SCHEMA_WAIT_TIMEOUT", "5s")
	if got := bootSchemaWaitTimeout(); got != 5*time.Second {
		t.Fatalf("override: got %s, want 5s", got)
	}
	t.Setenv("ORCHESTRATOR_SCHEMA_WAIT_TIMEOUT", "soon")
	if got := bootSchemaWaitTimeout(); got != defaultBootSchemaWait {
		t.Fatalf("garbage: got %s, want the default", got)
	}
}

// The bug this guards: main() started the dependency probe, the sentinels and
// the cdc-stats agent straight after db.Ping(), before api-gateway had migrated
// a fresh database, and each first tick logged "relation ... does not exist".
// The wait has to sit between the ping and the first worker that reads the DB.
func TestMainWaitsForTheSchemaBeforeStartingDBWorkers(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	wait := strings.Index(body, "waitForBootSchema(")
	if wait < 0 {
		t.Fatal("main.go never calls waitForBootSchema")
	}
	if ping := strings.Index(body, "db.Ping()"); ping < 0 || wait < ping {
		t.Fatalf("waitForBootSchema must run after db.Ping() (wait@%d ping@%d)", wait, ping)
	}
	for _, worker := range []string{
		"dependencyProbe.Start(", "cdcReconciler.Start(", "sentinelAgent.Start(",
		"cdcSentinel.Start(", "batchSentinel.Start(", "cdcStatsAgent.Start(", "watchdog.Start(",
	} {
		at := strings.Index(body, worker)
		if at < 0 {
			t.Fatalf("%s not found in main.go; update this list", worker)
		}
		if at < wait {
			t.Errorf("%s starts before waitForBootSchema", worker)
		}
	}
}

// Every entry must be something a gateway migration creates, or the wait could
// never succeed and every boot would sit out the full timeout.
func TestBootSchemaIsCreatedByAGatewayMigration(t *testing.T) {
	files, err := filepath.Glob("../../../api-gateway/migrations/*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("no gateway migrations found (err=%v)", err)
	}
	var all strings.Builder
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		all.Write(b)
		all.WriteByte('\n')
	}
	sqlText := all.String()
	for _, c := range bootSchema {
		var re *regexp.Regexp
		if c.column == "" {
			re = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?` + c.table + `\b`)
		} else {
			re = regexp.MustCompile(`(?is)ALTER\s+TABLE\s+(IF\s+EXISTS\s+)?` + c.table +
				`\b[^;]*ADD\s+COLUMN\s+(IF\s+NOT\s+EXISTS\s+)?` + c.column + `\b`)
		}
		if !re.MatchString(sqlText) {
			t.Errorf("no api-gateway migration creates %s %s", c.table, c.column)
		}
	}
}

// bootWorkerSources are the files behind the workers main() starts after the
// wait. Whole packages where the package IS the worker; single files where the
// package also holds request-path code.
var bootWorkerSources = []string{
	"../../internal/workers/dependency_probe.go",
	"../../internal/workers/cdc_reconciler.go",
	"../../internal/agents/sentinel",
	"../../internal/agents/cdcstats",
	"../../internal/cdcsnapshot",
	"../../internal/agents/heal",
	"../../internal/agents/healthwatch",
	"../../internal/agents/retention",
}

var migrationNumber = regexp.MustCompile(`^(\d+)`)

type migrationObject struct {
	file          string
	num           int
	table, column string
}

// gatewayMigrationObjects lists every table and added column the gateway's
// migrations create, with the migration's number.
func gatewayMigrationObjects(t *testing.T) []migrationObject {
	t.Helper()
	files, err := filepath.Glob("../../../api-gateway/migrations/*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("no gateway migrations found (err=%v)", err)
	}
	comment := regexp.MustCompile(`--[^\n]*`)
	createTable := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:public\.)?"?(\w+)"?`)
	alterTable := regexp.MustCompile(`(?is)ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?(?:public\.)?"?(\w+)"?([^;]*);`)
	addColumn := regexp.MustCompile(`(?i)ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?"?(\w+)"?`)
	var out []migrationObject
	for _, f := range files {
		m := migrationNumber.FindStringSubmatch(filepath.Base(f))
		if m == nil {
			continue
		}
		num, _ := strconv.Atoi(m[1])
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		sqlText := comment.ReplaceAllString(string(b), "")
		for _, c := range createTable.FindAllStringSubmatch(sqlText, -1) {
			out = append(out, migrationObject{filepath.Base(f), num, c[1], ""})
		}
		for _, a := range alterTable.FindAllStringSubmatch(sqlText, -1) {
			for _, c := range addColumn.FindAllStringSubmatch(a[2], -1) {
				out = append(out, migrationObject{filepath.Base(f), num, a[1], c[1]})
			}
		}
	}
	return out
}

// The class this guards: a boot worker queries a table or column that a gateway
// migration NEWER than every bootSchema entry creates. The wait then returns
// before that migration lands and the worker's first tick fails -- the heal
// worker did exactly that on a fresh 0.1.7-rc1 install ("column
// heal_attempted_at does not exist", migrations 053/083) while bootSchema
// stopped at 049. The gateway applies migrations in file order and stops at the
// first failure, so an entry from migration N proves 1..N are all applied; the
// newest entry only has to be at least as new as the newest object any boot
// worker names.
func TestBootSchemaIsAsNewAsWhatTheBootWorkersRead(t *testing.T) {
	objs := gatewayMigrationObjects(t)

	created := map[string]int{} // "table" or "table.column" -> migration number
	for _, o := range objs {
		key := o.table
		if o.column != "" {
			key += "." + o.column
		}
		if _, ok := created[key]; !ok {
			created[key] = o.num
		}
	}
	covered := 0
	for _, c := range bootSchema {
		key := c.table
		if c.column != "" {
			key += "." + c.column
		}
		n, ok := created[key]
		if !ok {
			t.Fatalf("bootSchema entry %s is not created by any gateway migration", key)
		}
		if n > covered {
			covered = n
		}
	}

	var sources []string
	for _, p := range bootWorkerSources {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatalf("boot worker source %s: %v (update bootWorkerSources)", p, err)
		}
		if !st.IsDir() {
			sources = append(sources, p)
			continue
		}
		matches, _ := filepath.Glob(filepath.Join(p, "*.go"))
		for _, m := range matches {
			if !strings.HasSuffix(m, "_test.go") {
				sources = append(sources, m)
			}
		}
	}

	identifier := regexp.MustCompile(`\w+`)
	needed, neededBy := 0, ""
	for _, src := range sources {
		b, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		names := map[string]bool{}
		for _, w := range identifier.FindAllString(string(b), -1) {
			names[w] = true
		}
		for _, o := range objs {
			// A column counts only where its table is named in the same file, so a
			// common name (workspace_id) does not tie a worker to an unrelated table.
			if !names[o.table] || (o.column != "" && !names[o.column]) {
				continue
			}
			if o.num > needed {
				needed = o.num
				what := o.table
				if o.column != "" {
					what += "." + o.column
				}
				neededBy = fmt.Sprintf("%s (%s) read by %s", what, o.file, src)
			}
		}
	}
	if needed == 0 {
		t.Fatal("found no migration objects in the boot worker sources; the scan is broken")
	}
	if covered < needed {
		t.Errorf("bootSchema's newest entry comes from migration %03d, but a boot worker reads %s -- add that object to bootSchema",
			covered, neededBy)
	}
}
