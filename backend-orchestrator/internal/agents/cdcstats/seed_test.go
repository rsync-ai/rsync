package cdcstats

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// Only rows this agent wrote (last_event_ts set) may seed the captured counters;
// the query must say so, and the scan must carry snapshot_rows into Reads.
func TestLoadStoredStats_ReadsOnlyCapturedRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ts := time.Date(2026, 9, 24, 9, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("AND last_event_ts IS NOT NULL")).
		WithArgs("p1").
		WillReturnRows(sqlmock.NewRows([]string{"schema", "table", "qn", "i", "u", "d", "t", "r", "ts"}).
			AddRow("public", "users", "public.users", 10, 2, 1, 113, 100, ts))
	rows, err := loadStoredStats(context.Background(), db, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
	r := rows[0]
	if r.QualifiedName != "public.users" || r.Inserts != 10 || r.Updates != 2 || r.Deletes != 1 ||
		r.TotalEvents != 113 || r.Reads != 100 || !r.LastEventTs.Equal(ts) {
		t.Fatalf("row = %+v", r)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
