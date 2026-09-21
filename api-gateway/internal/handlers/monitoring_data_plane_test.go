package handlers

import (
	"strconv"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// A CDC status poll's rows_processed was the sink's Kafka lag times ten. The Overview's
// "Rows" must not show it, but its lag reading still counts, and real counters beside it
// still win.
func TestFetchDataPlaneSummary_IgnoresStatusPollRowEstimate(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer mockDB.Close()

	now := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{"occurred_at", "payload"}).
		AddRow(now, []byte(`{"metadata":{"source":"cdc_status_poll","rows_processed":12800,"cdc_lag_ms":12800}}`)).
		AddRow(now.Add(-time.Minute), []byte(`{"metadata":{"source":"executor_batch","rows_processed":300,"bytes_processed":4096}}`)).
		AddRow(now.Add(-2*time.Minute), []byte(`{"metadata":{"source":"cdc_status_poll","rows_processed":99990}}`))
	mock.ExpectQuery(`FROM pipeline_run_events`).WillReturnRows(rows)

	got, err := fetchDataPlaneSummary(mockDB, "p1", TimeRange{Since: now.Add(-time.Hour), Until: now})
	if err != nil {
		t.Fatalf("fetchDataPlaneSummary: %v", err)
	}
	if got.TotalRowsProcessed != 300 {
		t.Errorf("TotalRowsProcessed = %d, want 300 (the status polls' lag estimates are not rows)", got.TotalRowsProcessed)
	}
	if got.TotalBytesProcessed != 4096 {
		t.Errorf("TotalBytesProcessed = %d, want 4096", got.TotalBytesProcessed)
	}
	if got.CDCLagMs == nil || *got.CDCLagMs != 12800 {
		t.Errorf("CDCLagMs = %v, want 12800 (the status poll's lag reading must still be read)", got.CDCLagMs)
	}
}

// Rows arrive newest first. The lag the Overview reports must be the newest
// reading, with its message count and the time it was taken: overwriting on every
// row reported the oldest reading in the window as the current lag.
func TestFetchDataPlaneSummary_LagIsTheNewestReading(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer mockDB.Close()

	now := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	newest := now.Add(-30 * time.Second)
	rows := sqlmock.NewRows([]string{"occurred_at", "payload"}).
		AddRow(newest, []byte(`{"metadata":{"source":"cdc_status_poll","cdc_lag_ms":50,"sink_lag_messages":5}}`)).
		AddRow(now.Add(-time.Minute), []byte(`{"metadata":{"source":"executor_batch","rows_processed":300}}`)).
		AddRow(now.Add(-50*time.Minute), []byte(`{"metadata":{"source":"cdc_status_poll","cdc_lag_ms":184000,"sink_lag_messages":18400}}`))
	mock.ExpectQuery(`FROM pipeline_run_events`).WillReturnRows(rows)

	got, err := fetchDataPlaneSummary(mockDB, "p1", TimeRange{Since: now.Add(-time.Hour), Until: now})
	if err != nil {
		t.Fatalf("fetchDataPlaneSummary: %v", err)
	}
	if got.CDCLagMs == nil || *got.CDCLagMs != 50 {
		t.Errorf("CDCLagMs = %s, want 50 (the newest reading, not the 184000 from 50m ago)", fmtInt64Ptr(got.CDCLagMs))
	}
	if got.SinkLagMessages == nil || *got.SinkLagMessages != 5 {
		t.Errorf("SinkLagMessages = %s, want 5", fmtInt64Ptr(got.SinkLagMessages))
	}
	if got.LagMeasuredAt == nil || !got.LagMeasuredAt.Equal(newest) {
		t.Errorf("LagMeasuredAt = %v, want %v", got.LagMeasuredAt, newest)
	}
}

// A window with no lag reading reports no lag rather than zero: "no reading" and
// "caught up" are different answers.
func TestFetchDataPlaneSummary_NoLagReadingReportsNoLag(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer mockDB.Close()

	now := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{"occurred_at", "payload"}).
		AddRow(now, []byte(`{"metadata":{"source":"executor_batch","rows_processed":300}}`))
	mock.ExpectQuery(`FROM pipeline_run_events`).WillReturnRows(rows)

	got, err := fetchDataPlaneSummary(mockDB, "p1", TimeRange{Since: now.Add(-time.Hour), Until: now})
	if err != nil {
		t.Fatalf("fetchDataPlaneSummary: %v", err)
	}
	if got.CDCLagMs != nil || got.SinkLagMessages != nil || got.LagMeasuredAt != nil {
		t.Errorf("lag = %s/%s/%v, want all nil", fmtInt64Ptr(got.CDCLagMs), fmtInt64Ptr(got.SinkLagMessages), got.LagMeasuredAt)
	}
}

func fmtInt64Ptr(p *int64) string {
	if p == nil {
		return "nil"
	}
	return strconv.FormatInt(*p, 10)
}
