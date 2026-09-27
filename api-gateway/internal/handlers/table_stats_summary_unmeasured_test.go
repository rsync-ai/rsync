package handlers

// The summary half of issue #21.
//
// The per-table fix landed first: buildCDCTableStatsResponse's placeholder for a
// selected table with no stats row carries nil counters, so the JSON omits them
// and the UI prints "–" (table_stats_waiting_for_data_test.go).
//
// computeCDCSummary then undid it at the rollup. It summed with derefInt64 (nil
// -> 0) and assigned all eight `*int64 … omitempty` pointers unconditionally, so
// a pipeline whose stats agent had never written a row published eight MEASURED
// zeros. The Data flow tab's Throughput card reads exactly those eight fields and
// renders a wall of "0" (fmtNum prints "–" for a missing value, "0" for a zero),
// which a user reads as "my pipeline moved nothing" — a far stronger claim than
// the data supports, and indistinguishable from a genuinely idle stream.
//
// The bound in the other direction matters just as much: a REAL stats row that
// counted zero is a measurement and must still be reported as 0.

import (
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// addCDCStatsRowSides appends a stats row whose captured and applied counters are
// each either reported (as n) or entirely absent, so the two sides can be exercised
// apart. With ENABLE_CDC_TABLE_STATS off, the sink still reports Applied while
// Captured is genuinely unmeasured — that asymmetry is a live configuration, not a
// hypothetical.
func addCDCStatsRowSides(rows *sqlmock.Rows, qn, table, status string, n int64, captured, applied bool, now time.Time) *sqlmock.Rows {
	var capI, capU, capD, capT interface{}
	if captured {
		capI, capU, capD, capT = n, n, n, 3*n
	}
	var appI, appU, appD, appT interface{}
	if applied {
		appI, appU, appD, appT = n, n, n, 3*n
	}
	return rows.AddRow("pipeline_test", table, qn, "cdc", status,
		nil, nil,
		capI, capU, capD, capT, now,
		appI, appU, appD, appT, now,
		int64(0),
		nil, nil, nil,
		now, nil, now)
}

// summaryCounterJSON marshals the summary the way the UI receives it and reports
// which of the eight CDC counters are present. Asserting on the JSON rather than
// the struct is the point: `omitempty` on a *int64 is the whole mechanism, and a
// struct-level check would pass with a pointer to zero.
func summaryCounterJSON(t *testing.T, summary TableStatsSummary) map[string]any {
	t.Helper()
	b, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("marshal summary: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}
	return out
}

var cdcCapturedTotals = []string{"total_inserts", "total_updates", "total_deletes", "total_cdc_events"}
var cdcAppliedTotals = []string{
	"total_applied_inserts", "total_applied_updates",
	"total_applied_deletes", "total_applied_cdc_events",
}

func TestCDCSummary_NothingMeasuredOmitsTheTotals(t *testing.T) {
	// No stats rows at all: every selected table becomes a waiting_for_data
	// placeholder with nil counters. This is the screenshotted state — four
	// tables "running", every number 0.
	rows := sqlmock.NewRows(cdcStatsCols)

	_, summary := buildCDCStatsForTest(t, rows, []string{
		"pipeline_test.orders",
		"pipeline_test.users",
	}, "")

	out := summaryCounterJSON(t, summary)
	for _, field := range append(append([]string{}, cdcCapturedTotals...), cdcAppliedTotals...) {
		if v, ok := out[field]; ok {
			t.Errorf("%s = %v, want absent (nothing measured it)", field, v)
		}
	}

	// The mode must survive, or the UI falls back to the batch layout and stops
	// offering the CAPTURED/APPLIED split entirely.
	if summary.Mode != "cdc" {
		t.Errorf("mode = %q, want cdc", summary.Mode)
	}
	// The table census is observed (the selection is known), so it still reports.
	if summary.TotalTables != 2 {
		t.Errorf("total_tables = %d, want 2", summary.TotalTables)
	}
	if summary.TablesWaitingForData != 2 {
		t.Errorf("tables_waiting_for_data = %d, want 2", summary.TablesWaitingForData)
	}
}

func TestCDCSummary_AMeasuredZeroIsStillReported(t *testing.T) {
	now := time.Date(2026, 9, 25, 9, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(cdcStatsCols)
	// A real row that counted nothing. "The stream has moved nothing" is a
	// finding; it must not be rounded down to "unmeasured".
	addCDCStatsRow(rows, "pipeline_test.quiet", "quiet", "running", 0, now)

	_, summary := buildCDCStatsForTest(t, rows, []string{"pipeline_test.quiet"}, "")

	out := summaryCounterJSON(t, summary)
	for _, field := range append(append([]string{}, cdcCapturedTotals...), cdcAppliedTotals...) {
		v, ok := out[field]
		if !ok {
			t.Errorf("%s absent, want a reported 0", field)
			continue
		}
		if n, isNum := v.(float64); !isNum || n != 0 {
			t.Errorf("%s = %v, want 0", field, v)
		}
	}
}

func TestCDCSummary_ReportsTheSideThatWasMeasured(t *testing.T) {
	now := time.Date(2026, 9, 25, 9, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(cdcStatsCols)
	// The ENABLE_CDC_TABLE_STATS=false shape: the sink reports Applied, the
	// stats agent never ran, so Captured is unmeasured. Collapsing the two sides
	// into one flag would hide the half that does exist.
	addCDCStatsRowSides(rows, "pipeline_test.orders", "orders", "running", 7, false, true, now)

	_, summary := buildCDCStatsForTest(t, rows, []string{"pipeline_test.orders"}, "")

	out := summaryCounterJSON(t, summary)
	for _, field := range cdcCapturedTotals {
		if v, ok := out[field]; ok {
			t.Errorf("%s = %v, want absent (the stats agent measured nothing)", field, v)
		}
	}
	for _, field := range cdcAppliedTotals {
		if _, ok := out[field]; !ok {
			t.Errorf("%s absent, want it reported (the sink measured this)", field)
		}
	}
	if got := derefInt64(summary.TotalAppliedInserts); got != 7 {
		t.Errorf("total_applied_inserts = %d, want 7", got)
	}
	if got := derefInt64(summary.TotalAppliedCDCEvents); got != 21 {
		t.Errorf("total_applied_cdc_events = %d, want 21", got)
	}
}

func TestCDCSummary_OneMeasuredTableReportsForThePipeline(t *testing.T) {
	now := time.Date(2026, 9, 25, 9, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(cdcStatsCols)
	addCDCStatsRow(rows, "pipeline_test.orders", "orders", "running", 5, now)

	// orders measured; users has no row at all. A partially-measured pipeline
	// reports its totals — withholding them would blank a number the user can
	// act on because one table has not started.
	_, summary := buildCDCStatsForTest(t, rows, []string{
		"pipeline_test.orders",
		"pipeline_test.users",
	}, "")

	out := summaryCounterJSON(t, summary)
	for _, field := range append(append([]string{}, cdcCapturedTotals...), cdcAppliedTotals...) {
		if _, ok := out[field]; !ok {
			t.Errorf("%s absent, want it reported (orders measured this)", field)
		}
	}
	if got := derefInt64(summary.TotalInserts); got != 5 {
		t.Errorf("total_inserts = %d, want 5 (the unmeasured table adds nothing)", got)
	}
}
