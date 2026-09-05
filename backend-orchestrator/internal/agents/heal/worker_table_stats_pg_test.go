//go:build integration_pg

// Real-PostgreSQL coverage for the row-count half of the heal sweep.
//
// Pins KI-HEAL-SWEEP-SAMPLES-ONE-TABLE.
//
// sweepCandidatesQuery feeds diagnose.Signal.SourceRowCount / WrittenRows, and
// those two numbers are the entire input to the silent-drop rule
// (diagnose.go: `SourceRowCount > 0 && WrittenRows == 0` → regenerate_connector).
// They were read from a LATERAL that ends `ORDER BY id ASC LIMIT 1` — the FIRST
// pipeline_run_table_stats row for the execution, one table out of however many
// the pipeline syncs.
//
// A pipeline syncing one table is unaffected. Every pipeline syncing more than
// one is diagnosed from an arbitrary sample of itself:
//
//   - lowest-id table landed fine, the rest dropped everything → the rule sees
//     rows written, misses the drop, and answers "silent drop detected without
//     clear cause" (escalate/0.55) for a total loss it had the numbers to name.
//   - lowest-id table happened to be empty → SourceRowCount 0, same fall-through.
//   - lowest-id table dropped everything, the rest were fine → the rule
//     regenerates the whole connector over one table.
//
// This cannot be caught by sqlmock: sqlmock matches statements as strings and
// hands back whatever rows the test declares, so the LATERAL is never executed
// and `LIMIT 1` versus `SUM(...)` are indistinguishable to it. Only a real
// planner over real fixture rows can tell them apart.
//
// Not part of the default suite — needs a live server; see the header of
// issue_sweep_pg_test.go for the two commands that provide one.
package heal

import (
	"database/sql"
	"testing"

	_ "github.com/rsync-ai/shared/pgdriver"
)

const (
	pgStatsUserID      = "bbbbbbbb-0000-4000-8000-000000000011"
	pgStatsWorkspaceID = "bbbbbbbb-0000-4000-8000-000000000012"
	pgStatsPipelineID  = "bbbbbbbb-0000-4000-8000-000000000013"
	pgStatsExecutionID = "bbbbbbbb-0000-4000-8000-000000000014"
)

func pgStatsPurge(t *testing.T, db *sql.DB) {
	t.Helper()
	// pipelines cascades to executions and pipeline_run_table_stats.
	for _, q := range []string{
		`DELETE FROM pipelines WHERE id = $1::uuid`,
	} {
		if _, err := db.Exec(q, pgStatsPipelineID); err != nil {
			t.Fatalf("purge %.40q: %v", q, err)
		}
	}
	if _, err := db.Exec(`DELETE FROM workspaces WHERE id = $1::uuid`, pgStatsWorkspaceID); err != nil {
		t.Fatalf("purge workspace: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM users WHERE id = $1::uuid`, pgStatsUserID); err != nil {
		t.Fatalf("purge user: %v", err)
	}
}

// pgSeedMultiTableDrop builds the shape production actually produces and the old
// LATERAL could not see: one failed execution, three tables, and the table with
// the LOWEST id — the only one `ORDER BY id ASC LIMIT 1` ever returned — is the
// one that worked.
//
//	orders     read 100    landed 100   <- lowest id, healthy
//	line_items read 5000   landed 0     <- everything dropped
//	shipments  read 3000   landed 0     <- everything dropped
//
// Totals: read 8100, landed 100, 2 of 3 tables read rows and landed none.
func pgSeedMultiTableDrop(t *testing.T, db *sql.DB) {
	t.Helper()
	pgStatsPurge(t, db)
	t.Cleanup(func() { pgStatsPurge(t, db) })

	mustExec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed %.60q: %v", q, err)
		}
	}
	mustExec(`INSERT INTO users (id, email, password_hash) VALUES ($1::uuid, $2, 'x')`,
		pgStatsUserID, "table-stats-verify@example.invalid")
	mustExec(`INSERT INTO workspaces (id, name, slug, owner_id)
	          VALUES ($1::uuid, 'table-stats-verify', 'table-stats-verify', $2::uuid)`,
		pgStatsWorkspaceID, pgStatsUserID)
	mustExec(`INSERT INTO pipelines
	          (id, name, natural_language_request, workspace_id, status, sync_mode, created_by)
	          VALUES ($1::uuid, 'table stats verify', 'verify', $2::uuid, 'failed', 'batch', $3::uuid)`,
		pgStatsPipelineID, pgStatsWorkspaceID, pgStatsUserID)
	// end_time well past GracePeriod so the sweep's terminality guards admit it.
	mustExec(`INSERT INTO executions (id, pipeline_id, status, start_time, end_time, error_message)
	          VALUES ($1::uuid, $2::uuid, 'silent_partial_drop_detected',
	                  NOW() - INTERVAL '2 hours', NOW() - INTERVAL '1 hour',
	                  'destination acknowledged fewer rows than the source emitted')`,
		pgStatsExecutionID, pgStatsPipelineID)

	// Inserted in id order: the healthy table first, so it is the one the old
	// LIMIT 1 picks. That ordering is the fixture's whole point.
	for _, tbl := range []struct {
		name   string
		read   int64
		landed int64
	}{
		{"orders", 100, 100},
		{"line_items", 5000, 0},
		{"shipments", 3000, 0},
	} {
		mustExec(`INSERT INTO pipeline_run_table_stats
		          (pipeline_id, execution_id, schema_name, table_name, qualified_name,
		           mode, status, read_rows, inserted_rows)
		          VALUES ($1::uuid, $2::uuid, 'public', $3, 'public.' || $3,
		                  'batch', 'completed', $4, $5)`,
			pgStatsPipelineID, pgStatsExecutionID, tbl.name, tbl.read, tbl.landed)
	}
}

// pgSweepRow runs the production query and returns the seeded execution's row.
// Scanned into exactly the columns sweep() scans, so this test tracks the real
// contract rather than a copy of it.
type pgSweepRow struct {
	execID, pipelineID, status, errMsg, srcType, dstType string
	readRows, landedRows                                 int64
	tablesNoLanded, tablesObserved                       int64
}

// scanWithTableCounts reads the six identity/type columns, the two totals, and
// the two per-table counters the silent-drop rule needs in order to tell a total
// loss from a partial one.
//
// Restoring the sampling LATERAL fails here twice over: the two counters
// disappear from the SELECT list (Scan reports "expected 8 destination arguments
// in Scan, not 10") and the totals revert to one table's numbers.
func (r *pgSweepRow) scanWithTableCounts(rows *sql.Rows) error {
	return rows.Scan(&r.execID, &r.pipelineID, &r.status, &r.errMsg,
		&r.srcType, &r.dstType, &r.readRows, &r.landedRows,
		&r.tablesNoLanded, &r.tablesObserved)
}

func pgRunSweepQuery(t *testing.T, db *sql.DB, scan func(*pgSweepRow, *sql.Rows) error) pgSweepRow {
	t.Helper()
	rows, err := db.Query(sweepCandidatesQuery,
		GracePeriod.String(), RehealCooldown.String(), BatchSize)
	if err != nil {
		t.Fatalf("sweepCandidatesQuery: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var r pgSweepRow
		if err := scan(&r, rows); err != nil {
			t.Fatalf("scan: %v\n\n"+
				"If this reads \"expected 8 destination arguments\", the sweep query does not "+
				"return the per-table counters yet — that IS the gap under test.", err)
		}
		if r.execID == pgStatsExecutionID {
			return r
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	t.Fatal("the seeded execution did not appear in the sweep at all — the fixture no longer " +
		"satisfies the eligibility clause, so nothing below asserts anything")
	return pgSweepRow{}
}

// TestPGSweepAggregatesEveryTableNotJustTheLowestID is the regression.
//
// Under `ORDER BY id ASC LIMIT 1` this returns read=100, landed=100: the healthy
// table, reported as if it were the run. 8000 rows are missing from the numbers
// the diagnoser is about to reason over.
func TestPGSweepAggregatesEveryTableNotJustTheLowestID(t *testing.T) {
	db := pgHealDB(t)
	pgSeedMultiTableDrop(t, db)

	got := pgRunSweepQuery(t, db, (*pgSweepRow).scanWithTableCounts)

	if got.readRows != 8100 {
		t.Errorf("read_rows = %d, want 8100 (100 + 5000 + 3000).\n\n"+
			"The LATERAL samples ONE pipeline_run_table_stats row per execution, so every "+
			"multi-table pipeline is diagnosed from an arbitrary slice of itself.", got.readRows)
	}
	if got.landedRows != 100 {
		t.Errorf("landed_rows = %d, want 100 (only 'orders' landed anything)", got.landedRows)
	}
}

// TestPGSweepReportsHowManyTablesLandedNothing — the aggregate alone is not
// enough. Summed, this run reads 8100 and lands 100, so `SourceRowCount > 0 &&
// WrittenRows == 0` is false and the rule still falls through to "silent drop
// detected without clear cause". The cause is perfectly clear: two named tables
// read thousands of rows and landed none. That fact only survives aggregation if
// it is counted before the sum erases it.
func TestPGSweepReportsHowManyTablesLandedNothing(t *testing.T) {
	db := pgHealDB(t)
	pgSeedMultiTableDrop(t, db)

	got := pgRunSweepQuery(t, db, (*pgSweepRow).scanWithTableCounts)

	if got.tablesNoLanded != 2 {
		t.Errorf("tables that read rows and landed none = %d, want 2 (line_items, shipments)",
			got.tablesNoLanded)
	}
	if got.tablesObserved != 3 {
		t.Errorf("tables observed = %d, want 3 — without the denominator the count above "+
			"cannot be read: 2-of-3 and 2-of-2000 are very different runs", got.tablesObserved)
	}
}

// TestPGSweepSurvivesAnExecutionWithNoTableStats guards the other direction. The
// old LATERAL returned no row for such an execution and the COALESCEs supplied
// zeros; an aggregate subquery returns one row of NULLs instead, and an
// un-COALESCEd SUM would fail the int64 scan rather than yielding 0.
func TestPGSweepSurvivesAnExecutionWithNoTableStats(t *testing.T) {
	db := pgHealDB(t)
	pgSeedMultiTableDrop(t, db)
	if _, err := db.Exec(`DELETE FROM pipeline_run_table_stats WHERE execution_id = $1::uuid`,
		pgStatsExecutionID); err != nil {
		t.Fatalf("clear stats: %v", err)
	}

	got := pgRunSweepQuery(t, db, (*pgSweepRow).scanWithTableCounts)

	if got.readRows != 0 || got.landedRows != 0 {
		t.Errorf("read=%d landed=%d for an execution with no table stats, want 0/0",
			got.readRows, got.landedRows)
	}
	if got.tablesNoLanded != 0 || got.tablesObserved != 0 {
		t.Errorf("tablesNoLanded=%d tablesObserved=%d with no stats rows, want 0/0",
			got.tablesNoLanded, got.tablesObserved)
	}
}
