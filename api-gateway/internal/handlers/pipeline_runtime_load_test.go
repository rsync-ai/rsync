package handlers

// The pipeline's load status ("Full load in progress · 3 / 5 tables", "Load
// completed, replication ongoing") comes from the initial load the orchestrator
// records in cdc_snapshot_requests (source 'initial', migration 118). A pipeline
// with no such row — one that streamed before the row existed — gets NO load
// block: the page must not claim a load finished that nobody saw.

import (
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

var initialLoadCols = []string{
	"status", "mode", "tables_total", "tables_done", "started_at", "completed_at", "last_error", "reloading_tables",
	"snapshot_rows_waiting",
}

func TestLoadInitialLoad_ReadsTheLatestLoad(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	started := time.Date(2026, 9, 25, 9, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`FROM cdc_snapshot_requests l\s+WHERE l.pipeline_id = \$1::uuid AND l.source = 'initial'`).
		WithArgs("p1").
		WillReturnRows(sqlmock.NewRows(initialLoadCols).
			AddRow("started", "blocking", 5, 3, started, nil, "", 1, 0))

	got := loadInitialLoad(db, "p1")
	if got == nil {
		t.Fatal("no load block for a recorded load")
	}
	if got.Status != "started" || got.Mode != "blocking" || got.TablesTotal != 5 || got.TablesDone != 3 ||
		got.StartedAt == nil || !got.StartedAt.Equal(started) || got.CompletedAt != nil || got.ReloadingTables != 1 {
		t.Fatalf("load = %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Debezium marks no table done in an incremental snapshot and the hybrid batch
// reports none; a finished load has read every table it names.
func TestLoadInitialLoad_ACompletedLoadHasReadEveryTable(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	done := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("FROM cdc_snapshot_requests l")).WithArgs("p1").
		WillReturnRows(sqlmock.NewRows(initialLoadCols).
			AddRow("completed", "incremental", 4, 0, done.Add(-time.Hour), done, "", 0, 0))
	got := loadInitialLoad(db, "p1")
	if got == nil || got.TablesDone != 4 || got.CompletedAt == nil {
		t.Fatalf("load = %+v", got)
	}
}

// Debezium's last snapshot marker completes the load when the SOURCE has been read
// to the end; the sink can still be writing thousands of those rows (a live
// 17,819-row load read "Load completed" while every row sat in Kafka). The block
// carries the snapshot rows captured but not yet written, per table, never
// below zero, so the page can say the load is still being written.
func TestLoadInitialLoad_CountsSnapshotRowsNotYetWritten(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	done := time.Date(2026, 9, 25, 12, 50, 0, 0, time.UTC)
	mock.ExpectQuery(`SUM\(GREATEST\(COALESCE\(\(snapshot_rows \+ COALESCE\(legacy_snapshot_reads, 0\)\), 0\) - COALESCE\(s\.applied_snapshot_rows, 0\), 0\)\)` +
		`[\s\S]*FROM pipeline_run_table_stats s\s+WHERE s\.pipeline_id = l\.pipeline_id AND s\.mode = 'cdc'`).
		WithArgs("p1").
		WillReturnRows(sqlmock.NewRows(initialLoadCols).
			AddRow("completed", "blocking", 6, 6, done.Add(-time.Minute), done, "", 0, 17819))
	got := loadInitialLoad(db, "p1")
	if got == nil || got.Status != "completed" || got.SnapshotRowsWaiting != 17819 {
		t.Fatalf("load = %+v, want a completed load with 17819 rows waiting", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(RuntimeLoad{Status: "completed"})
	var m map[string]interface{}
	_ = json.Unmarshal(b, &m)
	if m["snapshot_rows_waiting"] != float64(0) {
		t.Fatalf("zero rows waiting must be sent, not omitted: %s", b)
	}
}

func TestLoadInitialLoad_NoRecordedLoadIsNoBlock(t *testing.T) {
	for _, tc := range []struct {
		name string
		arm  func(sqlmock.Sqlmock)
	}{
		{"never recorded", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(regexp.QuoteMeta("FROM cdc_snapshot_requests l")).WillReturnError(sql.ErrNoRows)
		}},
		{"queue table missing", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(regexp.QuoteMeta("FROM cdc_snapshot_requests l")).
				WillReturnError(errors.New(`pq: relation "cdc_snapshot_requests" does not exist`))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			defer db.Close()
			tc.arm(mock)
			if got := loadInitialLoad(db, "p1"); got != nil {
				t.Fatalf("got %+v, want no load block", got)
			}
		})
	}
}

// The block is omitted from the JSON when absent, so the page tells "no load
// recorded" from "a load with zero tables".
func TestRuntimeLoad_OmittedWhenAbsent(t *testing.T) {
	b, _ := json.Marshal(PipelineRuntime{PipelineID: "p1"})
	var m map[string]interface{}
	_ = json.Unmarshal(b, &m)
	if _, ok := m["load"]; ok {
		t.Fatalf("load present without a recorded load: %s", b)
	}
	b, _ = json.Marshal(PipelineRuntime{PipelineID: "p1", Load: &RuntimeLoad{Status: "started", Mode: "blocking"}})
	_ = json.Unmarshal(b, &m)
	load, ok := m["load"].(map[string]interface{})
	if !ok || load["tables_total"] != float64(0) || load["reloading_tables"] != float64(0) {
		t.Fatalf("load block = %s", b)
	}
}
