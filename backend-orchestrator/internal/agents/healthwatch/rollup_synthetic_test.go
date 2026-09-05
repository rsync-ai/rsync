package healthwatch

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestQueryRollup_SkipsSyntheticCDCAnchor pins the last consumer of the reaped anchor.
//
// The rollup scores every terminal executions row in the last 24h as a success or a
// failure for its source connector's (type, version), and that score drives the
// regression alert. A CDC audit anchor swept to 'failed' is a row the connector never
// produced — it lands in the failure column for a connector that did not fail, and with
// MinSampleSize defaulting to 10 a low-traffic connector can be pushed past the
// regression threshold by anchors alone.
//
// sqlmock matches the expectation as a regexp against the SQL actually sent to the
// driver, so this fails if the predicate is dropped from the query text.
func TestQueryRollup_SkipsSyntheticCDCAnchor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`e\.id <> e\.pipeline_id`).
		WillReturnRows(sqlmock.NewRows([]string{"connector_type", "version", "success_count", "failure_count"}).
			AddRow("postgresql", "1.0.0", 9, 1))

	w := &Watchdog{DB: db}
	rows, err := w.queryRollup(context.Background())
	if err != nil {
		t.Fatalf("queryRollup: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the 24h rollup sent to the driver does not exclude the CDC audit anchor, so a "+
			"reaped anchor counts as a connector failure: %v", err)
	}
	if len(rows) != 1 || rows[0].SuccessRate != 0.9 {
		t.Errorf("rollup scan broke: %+v", rows)
	}
}
