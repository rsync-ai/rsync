package projector

// KI-CDC-SYNTHETIC-EXECUTION-NEVER-CLOSED, correlation half, projector side.
//
// Same two-producer setup as table_stats_destination_test.go: the kafka-mcp-sink and the
// orchestrator's cdcstats agent both upsert the row keyed by
// (pipeline_id, execution_id, qualified_name) (migration 034), and cdcstats runs on prod
// (ENABLE_CDC_TABLE_STATS=true in docker-compose.prod.yml) so its ticks land between the
// sink's continuously. Only the sink knows the orchestration execution id, so the ON
// CONFLICT clause must COALESCE it — a plain assignment would let every cdcstats tick blank
// the id the sink had just recorded, on exactly the CDC pipelines migration 090 exists for.
//
// Two more things are pinned here that the destination column does not need. The column is
// UUID, so a malformed label must be dropped in Go rather than cast in SQL: a cast error
// aborts the transaction and takes the real counters down with it. And execution_id itself
// must keep its CDC normalization to pipeline_id — this id is ADDED, never substituted.

import (
	"database/sql/driver"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// projectOrchestrationID runs one event through the real upsert against a mock driver and
// returns what it bound for $2 (execution_id) and $28 (orchestration_execution_id).
func projectOrchestrationID(t *testing.T, ev map[string]interface{}) (driver.Value, driver.Value) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	execID := &capturedArg{}
	orchID := &capturedArg{}

	args := make([]driver.Value, 0, 28)
	args = append(args, sqlmock.AnyArg(), execID)
	for i := 0; i < 25; i++ {
		args = append(args, sqlmock.AnyArg())
	}
	args = append(args, orchID)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(bytes_committed, 0)")).
		WillReturnRows(sqlmock.NewRows([]string{"bytes_committed"}).AddRow(int64(0)))
	// Matched as a regexp against the statement text, so requiring the COALESCE form is a
	// real assertion: rewriting the clause as a plain assignment stops matching and the
	// upsert fails here rather than silently in production.
	mock.ExpectExec(regexp.QuoteMeta(
		"orchestration_execution_id = COALESCE(EXCLUDED.orchestration_execution_id, pipeline_run_table_stats.orchestration_execution_id)")).
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
	if !execID.seen || !orchID.seen {
		t.Fatal("the execution-id arguments were never bound — the statement no longer carries them")
	}
	return execID.got, orchID.got
}

func statsTable() map[string]interface{} {
	return map[string]interface{}{
		"schema":         "pipeline_test",
		"name":           "demo_products",
		"qualified_name": "pipeline_test.demo_products",
	}
}

// The sink's id reaches the database — and does so WITHOUT displacing the normalized
// execution_id, which is the conflict key two producers depend on colliding.
func TestUpsertTableStatsRecordsTheOrchestrationExecutionID(t *testing.T) {
	const orch = "7f3d9c21-0000-4000-8000-0000000000aa"

	gotExec, gotOrch := projectOrchestrationID(t, cdcTableStatsEvent(statsTable(),
		map[string]interface{}{"orchestration_execution_id": orch}))

	if gotOrch != orch {
		t.Errorf("orchestration_execution_id bound as %#v, want %q — without it nothing joins "+
			"a CDC sink log line to the stats row it produced", gotOrch, orch)
	}
	// The whole reason this is a second column: execution_id must stay normalized.
	if gotExec != testStatsPipeline {
		t.Errorf("execution_id bound as %#v, want the pipeline id %q — this id is ADDED "+
			"alongside, never substituted, or the two producers stop sharing a row",
			gotExec, testStatsPipeline)
	}
}

// A cdcstats tick knows nothing about this id, and its silence must arrive as NULL so the
// COALESCE leaves the sink's answer alone. Anything non-NULL satisfies COALESCE and erases it.
func TestUpsertTableStatsSendsNullWhenTheProducerHasNoOrchestrationID(t *testing.T) {
	_, gotOrch := projectOrchestrationID(t, cdcTableStatsEvent(statsTable(),
		map[string]interface{}{"ops": map[string]interface{}{"c": float64(3)}}))

	if gotOrch != nil {
		t.Errorf("orchestration_execution_id bound as %#v, want NULL — a non-NULL value here "+
			"rides the COALESCE and erases the id the sink recorded", gotOrch)
	}
}

// The column is UUID. Validating in Go rather than casting in SQL is the point: this is a
// LABEL, and a malformed one must never abort the transaction carrying the real counters.
func TestUpsertTableStatsDropsAnUnusableOrchestrationID(t *testing.T) {
	for name, v := range map[string]interface{}{
		"blank":       "",
		"whitespace":  "   ",
		"not a uuid":  "e1",
		"truncated":   "7f3d9c21-0000-4000-8000",
		"wrong type":  float64(42),
		"sql literal": "'); DROP TABLE pipeline_run_table_stats; --",
	} {
		t.Run(name, func(t *testing.T) {
			// Reaching the assertion at all proves the upsert still ran: a SQL-side cast
			// would have failed the Exec and failed this test in projectOrchestrationID.
			if _, gotOrch := projectOrchestrationID(t, cdcTableStatsEvent(statsTable(),
				map[string]interface{}{"orchestration_execution_id": v})); gotOrch != nil {
				t.Errorf("bound %#v for input %#v, want NULL", gotOrch, v)
			}
		})
	}
}

// Whitespace around a real id is still that id — trimmed, not discarded.
func TestUpsertTableStatsTrimsTheOrchestrationID(t *testing.T) {
	if _, gotOrch := projectOrchestrationID(t, cdcTableStatsEvent(statsTable(),
		map[string]interface{}{"orchestration_execution_id": "  7f3d9c21-0000-4000-8000-0000000000aa  "},
	)); gotOrch != "7f3d9c21-0000-4000-8000-0000000000aa" {
		t.Errorf("bound %#v, want the trimmed id", gotOrch)
	}
}
