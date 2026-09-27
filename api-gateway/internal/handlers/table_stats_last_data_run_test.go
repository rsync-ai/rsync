package handlers

// A batch Resume with no source change finishes with every table at 0 read /
// 0 written. The Table statistics tab showed only that run, so a pipeline that
// had just moved 75,230 rows read as "moved nothing" (prod pipeline 9a094389,
// runs 05ec379f then e0afbc01). The response now names the last batch run that
// moved rows, and the page shows it.

import (
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

const lastDataRunQuery = `(?s)SELECT execution_id::text.*FROM pipeline_run_table_stats.*mode = 'batch'.*` +
	`HAVING SUM\(COALESCE\(read_rows, 0\)\) > 0 OR SUM\(COALESCE\(inserted_rows, 0\)\) > 0.*ORDER BY .* DESC.*LIMIT 1`

func TestLastBatchRunThatMovedRows_ReturnsTheRun(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	finished := time.Date(2026, 9, 26, 19, 55, 5, 0, time.UTC)
	mock.ExpectQuery(lastDataRunQuery).
		WithArgs("9a094389-c5a8-4179-8880-2cb42b460d4a").
		WillReturnRows(sqlmock.NewRows([]string{"execution_id", "count", "read", "written", "finished"}).
			AddRow("05ec379f-0000-0000-0000-000000000000", 6, int64(75230), int64(75230), finished))

	run := lastBatchRunThatMovedRows(db, "9a094389-c5a8-4179-8880-2cb42b460d4a")
	if run == nil {
		t.Fatal("expected a run")
	}
	if run.ExecutionID != "05ec379f-0000-0000-0000-000000000000" || run.Tables != 6 ||
		run.RowsRead != 75230 || run.RowsWritten != 75230 || run.FinishedAt == nil || !run.FinishedAt.Equal(finished) {
		t.Fatalf("unexpected run: %+v", run)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLastBatchRunThatMovedRows_NilWhenNoRunMovedRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(lastDataRunQuery).
		WillReturnRows(sqlmock.NewRows([]string{"execution_id", "count", "read", "written", "finished"}))
	if run := lastBatchRunThatMovedRows(db, "p"); run != nil {
		t.Fatalf("expected nil, got %+v", run)
	}
}

func TestLastBatchRunThatMovedRows_NilOnError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(lastDataRunQuery).WillReturnError(errors.New("boom"))
	if run := lastBatchRunThatMovedRows(db, "p"); run != nil {
		t.Fatalf("expected nil, got %+v", run)
	}
}
