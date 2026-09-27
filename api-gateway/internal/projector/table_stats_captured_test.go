package projector

// BUG #10 and BUG #7, projector half.
//
// Two producers upsert one CDC stats row: cdcstats writes the CAPTURED counters
// (metadata.ops + last_event_ts) and the sink writes the APPLIED ones (metadata.counts).
// The sink's counts used to be copied into the captured columns on every sink event, so
// with cdcstats running captured silently became a copy of applied. The copy is now
// flagged ($31) and the ON CONFLICT clause keeps the stored captured values whenever
// last_event_ts — written only by cdcstats — proves a captured-side writer owns the row.
//
// sqlmock cannot evaluate SQL, so these tests pin the bindings and the statement text;
// table_stats_captured_pg_test.go runs the same sequence against a real PostgreSQL.

import (
	"database/sql/driver"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// statsBinding is what one upsert bound for the arguments under test.
type statsBinding struct {
	inserts, snapshotRows, appliedSnapshotRows, capturedFromApplied driver.Value
}

// projectCapturedArgs runs one event through the real upsert and returns $12 (inserts),
// $29 (snapshot_rows), $30 (applied_snapshot_rows) and $31 (capturedFromApplied).
// stmt, when non-empty, must appear verbatim in the statement.
func projectCapturedArgs(t *testing.T, ev map[string]interface{}, stmt string) statsBinding {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	inserts, snap, appliedSnap, fromApplied := &capturedArg{}, &capturedArg{}, &capturedArg{}, &capturedArg{}
	args := make([]driver.Value, 0, 31)
	for i := 1; i <= 28; i++ {
		if i == 12 {
			args = append(args, inserts)
			continue
		}
		args = append(args, sqlmock.AnyArg())
	}
	args = append(args, snap, appliedSnap, fromApplied)

	if stmt == "" {
		stmt = "INSERT INTO pipeline_run_table_stats"
	}
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(bytes_committed, 0)")).
		WillReturnRows(sqlmock.NewRows([]string{"bytes_committed"}).AddRow(int64(0)))
	mock.ExpectExec(regexp.QuoteMeta(stmt)).
		WithArgs(args...).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	p := &EventProjector{db: db, lastSeq: map[string]int64{}, gapSeen: map[string]bool{}}
	if err := p.upsertTableStats(ev); err != nil {
		t.Fatalf("upsertTableStats: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
	if !fromApplied.seen {
		t.Fatal("$31 was never bound — the statement no longer carries capturedFromApplied")
	}
	return statsBinding{inserts.got, snap.got, appliedSnap.got, fromApplied.got}
}

func statsConsumerEvent(ops map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"event_type":   "TABLE_STATS",
		"pipeline_id":  testStatsPipeline,
		"execution_id": testStatsPipeline,
		"metadata": map[string]interface{}{
			"source":        "cdc_stats_consumer",
			"mode":          "cdc",
			"status":        "running",
			"last_event_ts": "2026-09-24T10:00:00Z",
			"table":         statsTable(),
			"ops":           ops,
		},
	}
}

// The sink's event is flagged as a copy, and the statement carries the guard that makes a
// flagged copy lose to a stored cdcstats value. Dropping either half reintroduces BUG #10.
func TestSinkEventAfterStatsConsumerDoesNotOverwriteCaptured(t *testing.T) {
	got := projectCapturedArgs(t, cdcTableStatsEvent(statsTable(), map[string]interface{}{
		"source": "kafka_mcp_sink",
	}), "inserts = CASE WHEN $31::boolean AND pipeline_run_table_stats.last_event_ts IS NOT NULL\n"+
		"\t\t\t\tTHEN pipeline_run_table_stats.inserts")
	if got.capturedFromApplied != true {
		t.Fatalf("capturedFromApplied bound as %#v for a sink event, want true", got.capturedFromApplied)
	}
	// Still copied for the no-cdcstats deployment (a row cdcstats never wrote).
	if got.inserts != int64(3) {
		t.Errorf("inserts bound as %#v, want the applied copy 3", got.inserts)
	}
}

// The control: cdcstats' own event is never a copy, so its captured values always merge.
func TestStatsConsumerEventIsNotACopy(t *testing.T) {
	got := projectCapturedArgs(t, statsConsumerEvent(map[string]interface{}{
		"inserts": float64(5), "updates": float64(0), "deletes": float64(0), "total": float64(5), "reads": float64(100),
	}), "")
	if got.capturedFromApplied != false {
		t.Fatalf("capturedFromApplied bound as %#v for a cdcstats event, want false", got.capturedFromApplied)
	}
	if got.inserts != int64(5) {
		t.Errorf("inserts bound as %#v, want 5", got.inserts)
	}
	if got.snapshotRows != int64(100) {
		t.Errorf("snapshot_rows bound as %#v, want ops.reads = 100", got.snapshotRows)
	}
	if got.appliedSnapshotRows != nil {
		t.Errorf("applied_snapshot_rows bound as %#v, want NULL (cdcstats does not apply)", got.appliedSnapshotRows)
	}
}

// BUG #7: the sink's counts.snapshot_rows reaches applied_snapshot_rows (and the captured
// copy), and the statement merges both monotonically without inventing a zero.
func TestSnapshotRowsFromTheSink(t *testing.T) {
	ev := cdcTableStatsEvent(statsTable(), map[string]interface{}{"source": "kafka_mcp_sink"})
	ev["metadata"].(map[string]interface{})["counts"].(map[string]interface{})["snapshot_rows"] = float64(90)
	got := projectCapturedArgs(t, ev,
		"applied_snapshot_rows = CASE\n"+
			"\t\t\t\tWHEN EXCLUDED.applied_snapshot_rows IS NULL THEN pipeline_run_table_stats.applied_snapshot_rows\n"+
			"\t\t\t\tELSE GREATEST(COALESCE(pipeline_run_table_stats.applied_snapshot_rows, 0), EXCLUDED.applied_snapshot_rows)")
	if got.appliedSnapshotRows != int64(90) {
		t.Errorf("applied_snapshot_rows bound as %#v, want 90", got.appliedSnapshotRows)
	}
	if got.snapshotRows != int64(90) {
		t.Errorf("snapshot_rows bound as %#v, want the applied copy 90", got.snapshotRows)
	}
}

// An event without the new fields (an older sink or cdcstats) binds NULL, so the CASE
// keeps the stored value instead of writing a zero over it.
func TestSnapshotRowsAbsentBindNull(t *testing.T) {
	got := projectCapturedArgs(t, cdcTableStatsEvent(statsTable(), map[string]interface{}{"source": "kafka_mcp_sink"}), "")
	if got.snapshotRows != nil || got.appliedSnapshotRows != nil {
		t.Errorf("snapshot_rows/applied_snapshot_rows bound as %#v/%#v, want NULL/NULL",
			got.snapshotRows, got.appliedSnapshotRows)
	}
	got = projectCapturedArgs(t, statsConsumerEvent(map[string]interface{}{"inserts": float64(1), "total": float64(1)}), "")
	if got.snapshotRows != nil {
		t.Errorf("snapshot_rows bound as %#v for ops without reads, want NULL", got.snapshotRows)
	}
}
