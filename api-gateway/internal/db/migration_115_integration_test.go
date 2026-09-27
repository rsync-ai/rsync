//go:build integration

// Integration test for migration 115 (snapshot reads a pre-114 producer counted as
// inserts). The counts come from a join against the delivery ledger, so this runs
// against a real, disposable PostgreSQL. Same gate and helpers as
// migration_069_integration_test.go; seedPipeline106 is in
// migration_106_integration_test.go.
//
//	docker run -d --rm --name pg115 -e POSTGRES_PASSWORD=pg -p 55434:5432 postgres:16-alpine
//	MIGRATION_TEST_DSN='postgres://postgres:pg@localhost:55434/postgres?sslmode=disable' \
//	  go test -tags=integration -run TestMigration115 ./internal/db/ -v
//	docker rm -f pg115
package db

import (
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func applyBefore115(t *testing.T, conn *sql.DB) {
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
		if !e.IsDir() && strings.HasSuffix(n, ".sql") && n < "115" {
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

// statsRow115 is one CDC stats row as a pre-114 producer left it (created before
// 114 ran) or as a post-114 one wrote it (old=false).
type statsRow115 struct {
	table                                 string
	old                                   bool
	inserts, snapshot                     sql.NullInt64
	appliedInserts, appliedSnapshot       sql.NullInt64
	ledgerReads, ledgerCreates, ledgerBad int // ledgerBad: failed 'r' acks, never counted
}

func n115(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }

// readerSQL115 returns a string constant from handlers/table_stats.go.
func readerSQL115(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "handlers", "table_stats.go"))
	if err != nil {
		t.Fatalf("read table_stats.go: %v", err)
	}
	m := regexp.MustCompile(`(?m)^\s*` + name + `\s*=\s*"([^"]+)"`).FindSubmatch(b)
	if m == nil {
		t.Fatalf("constant %s not found in table_stats.go", name)
	}
	return string(m[1])
}

// TestMigration115_MovesLegacyReadsOnce: reads leave the captured inserts through
// legacy_snapshot_reads and the applied inserts in place, only on pre-114 rows the
// ledger has reads for, and a second run changes nothing.
func TestMigration115_MovesLegacyReadsOnce(t *testing.T) {
	conn, cleanup := freshSchema(t, testDSN(t))
	defer cleanup()
	applyBefore115(t, conn)

	pid := seedPipeline106(t, conn, "p115")
	// CDC acks and stats use execution_id = pipeline_id; the ledger's FK wants the row.
	if _, err := conn.Exec(`INSERT INTO executions (id, pipeline_id, status, start_time)
		VALUES ($1, $1, 'running', NOW())`, pid); err != nil {
		t.Fatalf("seed execution: %v", err)
	}

	rows := []statsRow115{
		// Every row a snapshot read, all counted as inserts on both sides.
		{table: "public.all_reads", old: true, inserts: n115(40), appliedInserts: n115(40), ledgerReads: 40, ledgerBad: 3},
		// Reads and real inserts together (prod 18fd9faa messages).
		{table: "public.mixed", old: true, inserts: n115(45), appliedInserts: n115(45), ledgerReads: 40, ledgerCreates: 5},
		// A later re-snapshot already counted apart on both sides (prod ae6e0deb
		// subscriptions): the applied side must not be split a second time.
		{table: "public.half_split", old: true, inserts: n115(30), snapshot: n115(10),
			appliedInserts: n115(0), appliedSnapshot: n115(40), ledgerReads: 40},
		// Counted after 114: already right, must not move.
		{table: "public.after_114", old: false, inserts: n115(7), snapshot: n115(3),
			appliedInserts: n115(7), appliedSnapshot: n115(3), ledgerReads: 3, ledgerCreates: 7},
		// Pre-114 but no read in the ledger: nothing to move.
		{table: "public.no_reads", old: true, inserts: n115(9), appliedInserts: n115(9), ledgerCreates: 9},
	}
	offset := 0
	for _, r := range rows {
		created := "NOW()"
		if r.old {
			created = "TIMESTAMPTZ '2026-01-01 00:00:00+00'"
		}
		if _, err := conn.Exec(`INSERT INTO pipeline_run_table_stats (pipeline_id, execution_id, table_name,
			qualified_name, mode, status, inserts, updates, deletes, total_events, inserted_rows,
			snapshot_rows, applied_inserts, applied_snapshot_rows, applied_total_events, created_at)
			VALUES ($1, $1, $2, $2, 'cdc', 'running', $3, 0, 0, $3, $3, $4, $5, $6, $5, `+created+`)`,
			pid, r.table, r.inserts, r.snapshot, r.appliedInserts, r.appliedSnapshot); err != nil {
			t.Fatalf("seed stats %s: %v", r.table, err)
		}
		for _, op := range []struct {
			op, lastError string
			n             int
		}{{"r", "", r.ledgerReads}, {"c", "", r.ledgerCreates}, {"r", "sink write failed", r.ledgerBad}} {
			for i := 0; i < op.n; i++ {
				offset++
				var lastError any
				if op.lastError != "" {
					lastError = op.lastError
				}
				if _, err := conn.Exec(`INSERT INTO pipeline_batch_acks (pipeline_id, execution_id, table_name,
					batch_offset, storage_type, kafka_topic, kafka_partition, kafka_offset, cdc_op, last_error)
					VALUES ($1, $1, $2, $3, 'cdc', 't115', 0, $3, $4, $5)`,
					pid, r.table, offset, op.op, lastError); err != nil {
					t.Fatalf("seed ack %s: %v", r.table, err)
				}
			}
		}
	}

	matches, err := filepath.Glob(filepath.Join(migrationsDir, "115_*.sql"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected exactly one 115_*.sql migration, found %v (err %v)", matches, err)
	}
	body, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read 115: %v", err)
	}
	// What readers show, through the expressions table_stats.go ships (read from its
	// source: package handlers imports this one). -1 stands for NULL.
	capturedInserts, capturedSnapshot := readerSQL115(t, "capturedInsertsSQL"), readerSQL115(t, "capturedSnapshotSQL")
	shown := func(table string) (ins, snap, appliedIns, appliedSnap, total, appliedTotal, inserted int) {
		t.Helper()
		if err := conn.QueryRow(`SELECT
				COALESCE(`+capturedInserts+`, -1),
				COALESCE(`+capturedSnapshot+`, -1),
				COALESCE(applied_inserts, -1), COALESCE(applied_snapshot_rows, -1),
				total_events, applied_total_events, inserted_rows
			FROM pipeline_run_table_stats WHERE pipeline_id = $1 AND qualified_name = $2`, pid, table).
			Scan(&ins, &snap, &appliedIns, &appliedSnap, &total, &appliedTotal, &inserted); err != nil {
			t.Fatalf("read %s: %v", table, err)
		}
		return
	}
	want := map[string][7]int{
		// inserts, snapshot, applied inserts, applied snapshot, then the three
		// columns 115 must leave alone: total_events, applied_total_events, inserted_rows.
		"public.all_reads":  {0, 40, 0, 40, 40, 40, 40},
		"public.mixed":      {5, 40, 5, 40, 45, 45, 45},
		"public.half_split": {0, 40, 0, 40, 30, 0, 30},
		"public.after_114":  {7, 3, 7, 3, 7, 7, 7},
		"public.no_reads":   {9, -1, 9, -1, 9, 9, 9},
	}
	for run := 1; run <= 2; run++ { // the second run proves it is re-runnable and moves nothing twice
		if _, err := conn.Exec(string(body)); err != nil {
			t.Fatalf("exec 115 (run %d): %v", run, err)
		}
		for table, w := range want {
			ins, snap, ai, as, te, ate, ir := shown(table)
			if got := [7]int{ins, snap, ai, as, te, ate, ir}; got != w {
				t.Errorf("run %d %s: got %v, want %v (inserts, snapshot, applied inserts, applied snapshot, total_events, applied_total_events, inserted_rows)",
					run, table, got, w)
			}
		}
	}
	if !qBool(t, conn, `SELECT legacy_snapshot_reads IS NULL FROM pipeline_run_table_stats
		WHERE pipeline_id = $1 AND qualified_name = 'public.after_114'`, pid) {
		t.Errorf("a row counted after 114 got legacy_snapshot_reads")
	}
	// The stored captured inserts are never lowered: the stats consumer seeds from them.
	if v := qInt(t, conn, `SELECT inserts FROM pipeline_run_table_stats
		WHERE pipeline_id = $1 AND qualified_name = 'public.all_reads'`, pid); v != 40 {
		t.Errorf("stored inserts = %d, want 40 (unchanged)", v)
	}
}
