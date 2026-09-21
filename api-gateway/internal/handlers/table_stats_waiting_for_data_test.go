package handlers

// Issue #21: the CDC table-stats list showed a selected table that had never
// moved a row as "Running" with every counter at 0. buildCDCTableStatsResponse
// fabricates a row for each selected table the stats projector has not written
// yet; that row claimed a live status and a measured zero, neither of which
// anything had observed.
//
// The placeholder must now say waiting_for_data, carry no counters (so the JSON
// omits them and the UI cannot print 0), and stay out of tables_running — while a
// REAL stats row keeps its status and its counters, zeros included.

import (
	"encoding/json"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

var cdcStatsCols = []string{
	"schema_name", "table_name", "qualified_name", "mode", "status",
	"read_rows", "inserted_rows",
	"inserts", "updates", "deletes", "total_events", "last_event_ts",
	"applied_inserts", "applied_updates", "applied_deletes", "applied_total_events", "last_applied_ts",
	"dlq_rows",
	"destination_schema", "destination_qualified_name", "orchestration_execution_id",
	"started_at", "completed_at", "updated_at",
}

// addCDCStatsRow appends a stats row whose captured and applied counters all equal n.
func addCDCStatsRow(rows *sqlmock.Rows, qn, table, status string, n int64, now time.Time) *sqlmock.Rows {
	return rows.AddRow("pipeline_test", table, qn, "cdc", status,
		nil, nil,
		n, n, n, 3*n, now,
		n, n, n, 3*n, now,
		int64(0),
		nil, nil, nil,
		now, nil, now)
}

func buildCDCStatsForTest(t *testing.T, rows *sqlmock.Rows, selected []string, sortBy string) ([]TableStat, TableStatsSummary) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_run_table_stats")).WillReturnRows(rows)

	stats, summary, _, err := buildCDCTableStatsResponse(db, "p1", "e1", selected, "", sortBy, 50, 0, "")
	if err != nil {
		t.Fatalf("buildCDCTableStatsResponse: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
	return stats, summary
}

func TestCDCTableStats_SelectedTableWithNoStatsRowWaitsForData(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(cdcStatsCols)
	addCDCStatsRow(rows, "pipeline_test.demo_products", "demo_products", "running", 3, now)
	// THE BOUND: a real row that has counted nothing is a measurement, not a placeholder.
	addCDCStatsRow(rows, "pipeline_test.demo_quiet", "demo_quiet", "running", 0, now)

	stats, summary := buildCDCStatsForTest(t, rows, []string{
		"pipeline_test.demo_products",
		"pipeline_test.demo_quiet",
		"pipeline_test.demo_orders", // selected, no stats row
	}, "")

	byName := map[string]TableStat{}
	for _, s := range stats {
		byName[s.QualifiedName] = s
	}
	if len(byName) != 3 {
		t.Fatalf("got %d tables, want 3: %+v", len(byName), stats)
	}

	placeholder := byName["pipeline_test.demo_orders"]
	if placeholder.Status != "waiting_for_data" {
		t.Errorf("placeholder status = %q, want waiting_for_data", placeholder.Status)
	}
	if placeholder.Mode != "cdc" || placeholder.TableName != "demo_orders" || placeholder.SchemaName != "pipeline_test" {
		t.Errorf("placeholder identity = (%q, %q, %q)", placeholder.Mode, placeholder.SchemaName, placeholder.TableName)
	}
	counters := map[string]*int64{
		"inserts": placeholder.Inserts, "updates": placeholder.Updates, "deletes": placeholder.Deletes,
		"total_events": placeholder.TotalEvents, "applied_inserts": placeholder.AppliedInserts,
		"applied_updates": placeholder.AppliedUpdates, "applied_deletes": placeholder.AppliedDeletes,
		"applied_total_events": placeholder.AppliedTotalEvents,
	}
	for name, v := range counters {
		if v != nil {
			t.Errorf("placeholder %s = %d, want nil (nothing was measured)", name, *v)
		}
	}

	// JSON is what the UI reads: the counters must be absent, not 0.
	b, err := json.Marshal(placeholder)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for name := range counters {
		if v, present := out[name]; present {
			t.Errorf("placeholder JSON carries %s = %#v; it must be omitted", name, v)
		}
	}

	// Real rows are untouched.
	if s := byName["pipeline_test.demo_products"]; s.Status != "running" || s.Inserts == nil || *s.Inserts != 3 {
		t.Errorf("real row changed: status=%q inserts=%v", s.Status, s.Inserts)
	}
	if s := byName["pipeline_test.demo_quiet"]; s.Status != "running" || s.AppliedTotalEvents == nil || *s.AppliedTotalEvents != 0 {
		t.Errorf("a real zero must stay a non-nil 0: status=%q applied_total_events=%v", s.Status, s.AppliedTotalEvents)
	}

	if summary.TotalTables != 3 {
		t.Errorf("total_tables = %d, want 3", summary.TotalTables)
	}
	if summary.TablesRunning != 2 {
		t.Errorf("tables_running = %d, want 2 (the two real running rows, not the placeholder)", summary.TablesRunning)
	}
	if summary.TablesWaitingForData != 1 {
		t.Errorf("tables_waiting_for_data = %d, want 1", summary.TablesWaitingForData)
	}
	sb, _ := json.Marshal(summary)
	var sOut map[string]any
	_ = json.Unmarshal(sb, &sOut)
	if sOut["tables_waiting_for_data"] != float64(1) {
		t.Errorf("summary JSON tables_waiting_for_data = %#v, want 1", sOut["tables_waiting_for_data"])
	}
}

// Status sort: problems first, then live tables, then tables with no data yet,
// then finished ones.
func TestCDCTableStats_StatusSortPlacesWaitingBetweenRunningAndCompleted(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(cdcStatsCols)
	addCDCStatsRow(rows, "s.a_completed", "a_completed", "completed", 1, now)
	addCDCStatsRow(rows, "s.b_running", "b_running", "running", 1, now)
	addCDCStatsRow(rows, "s.c_failed", "c_failed", "failed", 1, now)

	stats, _ := buildCDCStatsForTest(t, rows, []string{
		"s.a_completed", "s.b_running", "s.c_failed", "s.d_waiting",
	}, "status")

	got := make([]string, 0, len(stats))
	for _, s := range stats {
		got = append(got, s.QualifiedName)
	}
	want := []string{"s.c_failed", "s.b_running", "s.d_waiting", "s.a_completed"}
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}
