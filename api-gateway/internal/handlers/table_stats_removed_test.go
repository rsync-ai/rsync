package handlers

// BUG #19/#20, #7 and #5/#9, table-stats half.
//
//   - A stats row whose table was unselected used to vanish from the list, and its
//     counts from every total, so "Total events" FELL when a table was removed. It
//     now stays as status "removed": out of total_tables, in the event/row totals.
//   - snapshot_rows / applied_snapshot_rows (migration 114) reach the response, nil
//     when no producer counted them — never a fabricated 0.
//   - load_mode "streaming_only" marks a table added without loading its rows.

import (
	"encoding/json"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// addCDCStatsRowWithSnapshot is addCDCStatsRow with the migration-114 columns set.
func addCDCStatsRowWithSnapshot(rows *sqlmock.Rows, qn, table, status string, n int64, snap, appliedSnap interface{}, now time.Time) *sqlmock.Rows {
	return rows.AddRow("pipeline_test", table, qn, "cdc", status,
		snap, appliedSnap,
		n, n, n, 3*n, now,
		n, n, n, 3*n, now,
		int64(0),
		nil, nil, nil,
		now, nil, now)
}

func buildCDCStatsWithStreamingOnly(t *testing.T, rows *sqlmock.Rows, selected []string, streamingOnly map[string]bool, sortBy string) ([]TableStat, TableStatsSummary) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	// Naming the columns makes this a real assertion on the statement.
	mock.ExpectQuery(regexp.QuoteMeta("snapshot_rows, applied_snapshot_rows")).WillReturnRows(rows)
	stats, summary, _, err := buildCDCTableStatsResponse(db, "p1", "e1", selected, streamingOnly, "", sortBy, 50, 0, "")
	if err != nil {
		t.Fatalf("buildCDCTableStatsResponse: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
	return stats, summary
}

func TestCDCTableStats_UnselectedTableStaysAsRemoved(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(cdcStatsCols)
	addCDCStatsRow(rows, "pipeline_test.demo_products", "demo_products", "running", 3, now)
	addCDCStatsRow(rows, "pipeline_test.demo_dropped", "demo_dropped", "running", 10, now)

	stats, summary := buildCDCStatsForTest(t, rows, []string{
		"pipeline_test.demo_products",
		"pipeline_test.demo_new", // selected, no row yet
	}, "")

	byName := map[string]TableStat{}
	for _, s := range stats {
		byName[s.QualifiedName] = s
	}
	gone, ok := byName["pipeline_test.demo_dropped"]
	if !ok {
		t.Fatalf("the unselected table's stats row vanished; want it listed as removed: %+v", stats)
	}
	if gone.Status != tableStatusRemoved {
		t.Fatalf("unselected table status = %q, want %q", gone.Status, tableStatusRemoved)
	}
	if gone.Inserts == nil || *gone.Inserts != 10 {
		t.Fatalf("a removed table keeps its counters, got inserts %v", gone.Inserts)
	}
	if byName["pipeline_test.demo_products"].Status != "running" {
		t.Fatalf("a selected table must keep its own status, got %q", byName["pipeline_test.demo_products"].Status)
	}

	// total_tables counts the selection (products + new), not the removed one…
	if summary.TotalTables != 2 || summary.TablesRemoved != 1 {
		t.Fatalf("total_tables/tables_removed = %d/%d, want 2/1", summary.TotalTables, summary.TablesRemoved)
	}
	if summary.TablesRunning != 1 || summary.TablesWaitingForData != 1 {
		t.Fatalf("running/waiting = %d/%d, want 1/1", summary.TablesRunning, summary.TablesWaitingForData)
	}
	// …but the totals keep what the removed table moved: 3*3 + 3*10 events.
	if summary.TotalCDCEvents == nil || *summary.TotalCDCEvents != 39 {
		t.Fatalf("total_cdc_events = %v, want 39 (removed table included)", summary.TotalCDCEvents)
	}
	if summary.TotalAppliedInserts == nil || *summary.TotalAppliedInserts != 13 {
		t.Fatalf("total_applied_inserts = %v, want 13 (removed table included)", summary.TotalAppliedInserts)
	}
}

// The control: with every stats row still selected nothing is "removed".
func TestCDCTableStats_NoRemovedWhenAllSelected(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(cdcStatsCols)
	addCDCStatsRow(rows, "pipeline_test.demo_products", "demo_products", "running", 3, now)

	stats, summary := buildCDCStatsForTest(t, rows, []string{" pipeline_test.demo_products "}, "")
	if len(stats) != 1 || stats[0].Status != "running" {
		t.Fatalf("stats = %+v, want the one selected table, running", stats)
	}
	if summary.TablesRemoved != 0 || summary.TotalTables != 1 {
		t.Fatalf("tables_removed/total_tables = %d/%d, want 0/1", summary.TablesRemoved, summary.TotalTables)
	}
}

// A stats row named differently from the selection (case, or a database prefix
// the stats consumer keeps) is that table, not a removed one plus a table
// waiting for data. Two candidates are left alone rather than guessed.
func TestCDCTableStats_DifferentlyNamedRowIsTheSelectedTable(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(cdcStatsCols)
	addCDCStatsRow(rows, "inventory.dbo.Orders", "Orders", "running", 4, now)
	addCDCStatsRow(rows, "Public.Users", "Users", "running", 2, now)
	addCDCStatsRow(rows, "a.items", "items", "running", 1, now)
	addCDCStatsRow(rows, "b.items", "items", "running", 1, now)

	stats, summary := buildCDCStatsForTest(t, rows, []string{"dbo.orders", "public.users", "items"}, "")
	byName := map[string]TableStat{}
	for _, s := range stats {
		byName[s.QualifiedName] = s
	}
	for _, qn := range []string{"dbo.orders", "public.users"} {
		st := byName[qn]
		if st.Status != "running" || st.Inserts == nil {
			t.Fatalf("%s = %+v, want its stats row (running, counted)", qn, st)
		}
	}
	if byName["items"].Status != tableStatusWaitingForData {
		t.Fatalf("items matched one of two candidates: %+v", byName["items"])
	}
	if summary.TablesRemoved != 2 || summary.TotalTables != 3 {
		t.Fatalf("tables_removed/total_tables = %d/%d, want 2/3 (only the ambiguous a.items and b.items)",
			summary.TablesRemoved, summary.TotalTables)
	}
}

// Removed tables sort after every live status, so they never push a failing table down.
func TestCDCTableStats_RemovedSortsLast(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(cdcStatsCols)
	addCDCStatsRow(rows, "pipeline_test.a_dropped", "a_dropped", "failed", 1, now)
	addCDCStatsRow(rows, "pipeline_test.b_done", "b_done", "completed", 1, now)

	stats, _ := buildCDCStatsForTest(t, rows, []string{"pipeline_test.b_done"}, "status")
	if len(stats) != 2 || stats[0].QualifiedName != "pipeline_test.b_done" || stats[1].Status != tableStatusRemoved {
		t.Fatalf("status sort = %+v; want the completed table, then the removed one", stats)
	}
}

func TestCDCTableStats_SnapshotRowsAndLoadMode(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(cdcStatsCols)
	addCDCStatsRowWithSnapshot(rows, "pipeline_test.loaded", "loaded", "running", 2, int64(100), int64(90), now)
	addCDCStatsRowWithSnapshot(rows, "pipeline_test.streamed", "streamed", "running", 2, nil, nil, now)
	addCDCStatsRowWithSnapshot(rows, "pipeline_test.gone", "gone", "running", 2, int64(5), int64(5), now)

	stats, summary := buildCDCStatsWithStreamingOnly(t, rows,
		[]string{"pipeline_test.loaded", "pipeline_test.streamed", "pipeline_test.fresh"},
		map[string]bool{"pipeline_test.streamed": true, "pipeline_test.fresh": true, "pipeline_test.gone": true}, "")

	byName := map[string]TableStat{}
	for _, s := range stats {
		byName[s.QualifiedName] = s
	}
	loaded := byName["pipeline_test.loaded"]
	if loaded.SnapshotRows == nil || *loaded.SnapshotRows != 100 || loaded.AppliedSnapshotRows == nil || *loaded.AppliedSnapshotRows != 90 {
		t.Fatalf("loaded snapshot/applied = %v/%v, want 100/90", loaded.SnapshotRows, loaded.AppliedSnapshotRows)
	}
	if loaded.LoadMode != "" {
		t.Fatalf("a loaded table must carry no load_mode, got %q", loaded.LoadMode)
	}
	streamed := byName["pipeline_test.streamed"]
	if streamed.SnapshotRows != nil || streamed.AppliedSnapshotRows != nil {
		t.Fatalf("NULL snapshot columns must stay nil, got %v/%v", streamed.SnapshotRows, streamed.AppliedSnapshotRows)
	}
	if streamed.LoadMode != cdcLoadModeStreamingOnly || byName["pipeline_test.fresh"].LoadMode != cdcLoadModeStreamingOnly {
		t.Fatalf("load_mode = %q / %q (placeholder), want streaming_only for both",
			streamed.LoadMode, byName["pipeline_test.fresh"].LoadMode)
	}
	// A removed table is not "streaming" anything, whatever a stale list says.
	if byName["pipeline_test.gone"].LoadMode != "" {
		t.Fatalf("removed table load_mode = %q, want none", byName["pipeline_test.gone"].LoadMode)
	}

	// Totals: loaded + removed; the NULL row adds nothing.
	if summary.TotalSnapshotRows == nil || *summary.TotalSnapshotRows != 105 {
		t.Fatalf("total_snapshot_rows = %v, want 105", summary.TotalSnapshotRows)
	}
	if summary.TotalAppliedSnapshotRows == nil || *summary.TotalAppliedSnapshotRows != 95 {
		t.Fatalf("total_applied_snapshot_rows = %v, want 95", summary.TotalAppliedSnapshotRows)
	}

	b, _ := json.Marshal(streamed)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if m["load_mode"] != "streaming_only" {
		t.Fatalf("JSON load_mode = %v, want streaming_only", m["load_mode"])
	}
	for _, k := range []string{"snapshot_rows", "applied_snapshot_rows", "read_rows", "inserted_rows"} {
		if _, has := m[k]; has {
			t.Fatalf("JSON carries %q for a CDC row that has none: %s", k, b)
		}
	}
}

// No producer counts snapshot rows yet: the totals are absent, not a measured 0.
func TestCDCTableStats_SnapshotTotalsAbsentWhenUncounted(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(cdcStatsCols)
	addCDCStatsRow(rows, "pipeline_test.demo_products", "demo_products", "running", 3, now)

	_, summary := buildCDCStatsForTest(t, rows, []string{"pipeline_test.demo_products"}, "")
	if summary.TotalSnapshotRows != nil || summary.TotalAppliedSnapshotRows != nil {
		t.Fatalf("snapshot totals = %v/%v, want nil/nil", summary.TotalSnapshotRows, summary.TotalAppliedSnapshotRows)
	}
	b, _ := json.Marshal(summary)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if _, has := m["total_snapshot_rows"]; has {
		t.Fatalf("summary JSON carries total_snapshot_rows with nothing counted: %s", b)
	}
	if m["tables_removed"] != float64(0) {
		t.Fatalf("summary JSON tables_removed = %v, want 0", m["tables_removed"])
	}
}
