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
	if got.CDCLagMs != nil {
		t.Errorf("CDCLagMs = %s, want nil (a status poll's cdc_lag_ms was messages x 10, not a time)", fmtInt64Ptr(got.CDCLagMs))
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
	if got.SinkLagMessages == nil || *got.SinkLagMessages != 5 {
		t.Errorf("SinkLagMessages = %s, want 5 (the newest reading, not the 18400 from 50m ago)", fmtInt64Ptr(got.SinkLagMessages))
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

// KI-OVERVIEW-CDC-LAG-AND-ROWS-NOT-MEASURED. The status poll now sends only
// sink_lag_messages (no fabricated cdc_lag_ms). The newest reading must be found by
// its message count: keying it on cdc_lag_ms skipped every post-fix poll and reported
// an old pre-fix reading as the current lag.
func TestFetchDataPlaneSummary_NewestLagReadingIsKeyedOnMessages(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer mockDB.Close()

	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	newest := now.Add(-20 * time.Second)
	rows := sqlmock.NewRows([]string{"occurred_at", "payload"}).
		AddRow(newest, []byte(`{"metadata":{"source":"cdc_status_poll","sink_lag_messages":7}}`)).
		AddRow(now.Add(-40*time.Minute), []byte(`{"metadata":{"source":"cdc_status_poll","cdc_lag_ms":12800,"sink_lag_messages":1280}}`))
	mock.ExpectQuery(`FROM pipeline_run_events`).WillReturnRows(rows)
	mock.ExpectQuery(`FROM pipeline_run_table_stats`).WillReturnRows(sqlmock.NewRows([]string{"applied", "tables"}).AddRow(0, 0))

	got, err := fetchDataPlaneSummary(mockDB, "p1", TimeRange{Since: now.Add(-time.Hour), Until: now})
	if err != nil {
		t.Fatalf("fetchDataPlaneSummary: %v", err)
	}
	if got.SinkLagMessages == nil || *got.SinkLagMessages != 7 {
		t.Errorf("SinkLagMessages = %s, want 7 (the newest poll, not the 1280 from 40m ago)", fmtInt64Ptr(got.SinkLagMessages))
	}
	if got.LagMeasuredAt == nil || !got.LagMeasuredAt.Equal(newest) {
		t.Errorf("LagMeasuredAt = %v, want %v", got.LagMeasuredAt, newest)
	}
	if got.CDCLagMs != nil {
		t.Errorf("CDCLagMs = %s, want nil (a status poll's cdc_lag_ms was messages x 10, not a time)", fmtInt64Ptr(got.CDCLagMs))
	}
}

// The sink emits no DATA_PLANE_METRICS, so a CDC pipeline's Overview "Rows" had no
// source at all. It now reads the same destination-side counters Table stats shows
// (pipeline_run_table_stats applied_*, under the stable CDC key execution_id =
// pipeline_id), snapshot rows included.
func TestFetchDataPlaneSummary_CDCRowsComeFromAppliedTableStats(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer mockDB.Close()

	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{"occurred_at", "payload"}).
		AddRow(now, []byte(`{"metadata":{"source":"cdc_status_poll","rows_processed":12800,"sink_lag_messages":1280}}`))
	mock.ExpectQuery(`FROM pipeline_run_events`).WillReturnRows(rows)
	mock.ExpectQuery(`(?s)applied_snapshot_rows.*FROM pipeline_run_table_stats.*execution_id = \$2::uuid.*mode = 'cdc'`).
		WithArgs("p1", "p1").
		WillReturnRows(sqlmock.NewRows([]string{"applied", "tables"}).AddRow(72670, 3))

	got, err := fetchDataPlaneSummary(mockDB, "p1", TimeRange{Since: now.Add(-time.Hour), Until: now})
	if err != nil {
		t.Fatalf("fetchDataPlaneSummary: %v", err)
	}
	if got.CDCRowsApplied == nil || *got.CDCRowsApplied != 72670 {
		t.Errorf("CDCRowsApplied = %s, want 72670 (the applied counters Table stats shows)", fmtInt64Ptr(got.CDCRowsApplied))
	}
	if got.TotalRowsProcessed != 72670 {
		t.Errorf("TotalRowsProcessed = %d, want 72670 (CDC rows, not 0 and not the poll's 12800)", got.TotalRowsProcessed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("queries: %v", err)
	}
}

// A pipeline with no CDC table-stats rows (batch, or CDC before its first event)
// reports no CDC row count rather than a measured zero.
func TestFetchDataPlaneSummary_NoCDCTableStatsReportsNoCDCRows(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer mockDB.Close()

	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{"occurred_at", "payload"}).
		AddRow(now, []byte(`{"metadata":{"source":"executor_batch","rows_processed":300}}`))
	mock.ExpectQuery(`FROM pipeline_run_events`).WillReturnRows(rows)
	mock.ExpectQuery(`FROM pipeline_run_table_stats`).WillReturnRows(sqlmock.NewRows([]string{"applied", "tables"}).AddRow(0, 0))

	got, err := fetchDataPlaneSummary(mockDB, "p1", TimeRange{Since: now.Add(-time.Hour), Until: now})
	if err != nil {
		t.Fatalf("fetchDataPlaneSummary: %v", err)
	}
	if got.CDCRowsApplied != nil {
		t.Errorf("CDCRowsApplied = %s, want nil", fmtInt64Ptr(got.CDCRowsApplied))
	}
	if got.TotalRowsProcessed != 300 {
		t.Errorf("TotalRowsProcessed = %d, want 300", got.TotalRowsProcessed)
	}
}
