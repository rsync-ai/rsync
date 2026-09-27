package handlers

// Snapshot reads a pre-114 producer counted as inserts (migration 115). The
// migration records them in legacy_snapshot_reads without lowering `inserts`, so
// every reader must show inserts through capturedInsertsSQL and snapshot rows
// through capturedSnapshotSQL; a reader of the raw column shows prod's 51,600
// "Captured Inserts" for a change stream with none. What the two expressions
// compute is pinned against a real PostgreSQL in
// internal/db/migration_115_integration_test.go.

import (
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestCDCTableStats_QueryShowsLegacyReadsAsSnapshotRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(cdcStatsCols)
	addCDCStatsRowWithSnapshot(rows, "public.matches", "matches", "running", 2, int64(8000), int64(8000), now)
	mock.ExpectQuery(`(?s)` +
		regexp.QuoteMeta(capturedSnapshotSQL+` AS snapshot_rows, applied_snapshot_rows`) + `.*` +
		regexp.QuoteMeta(capturedInsertsSQL+` AS inserts, updates, deletes`) + `.*` +
		regexp.QuoteMeta(`(COALESCE(`+capturedInsertsSQL+`, 0) + COALESCE(updates, 0) + COALESCE(deletes, 0)) AS total_events`),
	).WillReturnRows(rows)
	if _, _, _, err := buildCDCTableStatsResponse(db, "p1", "e1", []string{"public.matches"}, nil, "", "", 50, 0, ""); err != nil {
		t.Fatalf("buildCDCTableStatsResponse: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the CDC stats query does not read inserts/snapshot rows through the migration-115 expressions: %v", err)
	}
}

func TestTableStatsSummary_SumsCapturedInserts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`(?s)` +
		regexp.QuoteMeta(`SUM(COALESCE(`+capturedInsertsSQL+`, 0)) as sum_inserts`) + `.*` +
		regexp.QuoteMeta(`SUM(COALESCE(`+capturedInsertsSQL+`, 0) + COALESCE(updates, 0) + COALESCE(deletes, 0)) as sum_total_events`),
	).WillReturnError(errors.New("stop here: only the statement is under test"))
	computeTableStatsSummary(db, "p1", "")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the summary does not sum inserts through capturedInsertsSQL: %v", err)
	}
}

// Every read of the captured inserts in the two stats readers goes through the
// adjustment. The patterns are the shapes each reader used before migration 115.
func TestNoStatsReaderShowsRawCapturedInserts(t *testing.T) {
	raw := map[string][]*regexp.Regexp{
		"table_stats.go": {
			regexp.MustCompile(`COALESCE\(inserts,`),
			regexp.MustCompile(`ORDER BY inserts\b`),
			regexp.MustCompile(`(?m)^\s*inserts, updates, deletes,`),
		},
		"usage.go": {
			regexp.MustCompile(`COALESCE\(s\.inserts, 0\)`),
		},
	}
	for file, patterns := range raw {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		src := string(b)
		for _, p := range patterns {
			if loc := p.FindStringIndex(src); loc != nil {
				line := strings.Count(src[:loc[0]], "\n") + 1
				t.Errorf("%s:%d reads the raw captured inserts (%s); use capturedInsertsSQL so migration 115's legacy_snapshot_reads is taken out",
					file, line, p)
			}
		}
	}
	if b, _ := os.ReadFile("usage.go"); !strings.Contains(string(b), "s.inserts - COALESCE(s.legacy_snapshot_reads, 0)") {
		t.Errorf("usage.go cdc_inserts no longer takes legacy_snapshot_reads out of the captured inserts")
	}
}
