package projector

// KI-CDC-TABLE-STATS-SOURCE-SCHEMA, projector half.
//
// pipeline_run_table_stats rows for a CDC table have TWO producers upserting on the same
// (pipeline_id, execution_id, qualified_name) key (migration 034): the kafka-mcp-sink
// writes the applied counters and — as of migration 089 — the destination namespace, and
// the orchestrator's cdcstats agent writes the captured counters and knows nothing about
// any destination. cdcstats runs on prod (ENABLE_CDC_TABLE_STATS=true in
// docker-compose.prod.yml), so its ticks land between the sink's continuously.
//
// That makes the ON CONFLICT clause load-bearing in a way no other column here is: a
// plain `destination_schema = EXCLUDED.destination_schema` would let every cdcstats tick
// blank the value the sink had just recorded, and the column would read NULL forever on
// exactly the CDC pipelines it was added for. These tests pin both halves — the sink's
// value is sent, and the other producer's absence is sent as NULL into a COALESCE.

import (
	"database/sql/driver"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// capturedArg matches any bound value and records it, so a 27-argument statement can be
// asserted on the two arguments under test without restating the other 25.
type capturedArg struct {
	got  driver.Value
	seen bool
}

func (c *capturedArg) Match(v driver.Value) bool {
	c.got = v
	c.seen = true
	return true
}

const (
	testStatsPipeline = "11111111-1111-1111-1111-111111111111"
	testStatsExec     = "22222222-2222-2222-2222-222222222222"
)

// cdcTableStatsEvent builds a TABLE_STATS event with the supplied metadata.table.
// bytes_committed is deliberately absent so the billing-ledger branch stays out of the
// way of what is being tested.
func cdcTableStatsEvent(table map[string]interface{}, extraMeta map[string]interface{}) map[string]interface{} {
	meta := map[string]interface{}{
		"mode":   "cdc",
		"status": "running",
		"table":  table,
		"counts": map[string]interface{}{
			"inserts":      float64(3),
			"updates":      float64(0),
			"deletes":      float64(0),
			"total_events": float64(3),
			"read_rows":    float64(3),
		},
	}
	for k, v := range extraMeta {
		meta[k] = v
	}
	return map[string]interface{}{
		"event_type":   "TABLE_STATS",
		"pipeline_id":  testStatsPipeline,
		"execution_id": testStatsExec,
		"metadata":     meta,
	}
}

// projectTableStats runs one event through the real upsert against a mock driver and
// returns the two destination arguments it bound.
func projectTableStats(t *testing.T, ev map[string]interface{}) (driver.Value, driver.Value) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	destSchema := &capturedArg{}
	destQualified := &capturedArg{}

	args := make([]driver.Value, 0, 28)
	for i := 0; i < 25; i++ {
		args = append(args, sqlmock.AnyArg())
	}
	// $28 is orchestration_execution_id (migration 090), asserted in
	// table_stats_orchestration_id_test.go.
	args = append(args, destSchema, destQualified, sqlmock.AnyArg())

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(bytes_committed, 0)")).
		WillReturnRows(sqlmock.NewRows([]string{"bytes_committed"}).AddRow(int64(0)))
	// The expectation is matched as a regexp against the statement text, so requiring
	// the COALESCE form here is a real assertion: rewriting the clause as a plain
	// assignment stops matching and the upsert fails.
	mock.ExpectExec(regexp.QuoteMeta(
		"destination_schema = COALESCE(EXCLUDED.destination_schema, pipeline_run_table_stats.destination_schema)")).
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
	if !destSchema.seen || !destQualified.seen {
		t.Fatal("the destination arguments were never bound — the statement no longer carries them")
	}
	return destSchema.got, destQualified.got
}

// The sink's answer reaches the database.
func TestUpsertTableStatsPersistsTheSinkDestination(t *testing.T) {
	gotSchema, gotQualified := projectTableStats(t, cdcTableStatsEvent(map[string]interface{}{
		"schema":                     "pipeline_test",
		"name":                       "demo_products",
		"qualified_name":             "pipeline_test.demo_products",
		"destination_schema":         "rsync_verify_cdc",
		"destination_qualified_name": "rsync_verify_cdc.demo_products",
	}, nil))

	if gotSchema != "rsync_verify_cdc" {
		t.Errorf("destination_schema bound as %#v, want %q", gotSchema, "rsync_verify_cdc")
	}
	if gotQualified != "rsync_verify_cdc.demo_products" {
		t.Errorf("destination_qualified_name bound as %#v, want %q", gotQualified, "rsync_verify_cdc.demo_products")
	}
}

// The other producer's silence must reach the database as NULL, because NULL is what the
// COALESCE in the ON CONFLICT clause needs in order to leave the stored value alone. An
// empty string would satisfy COALESCE and overwrite the sink's answer with nothing.
func TestUpsertTableStatsSendsNullWhenTheProducerHasNoDestination(t *testing.T) {
	// Shaped like a cdcstats event: same key, captured counters, metadata.ops present,
	// and no destination fields anywhere.
	gotSchema, gotQualified := projectTableStats(t, cdcTableStatsEvent(map[string]interface{}{
		"schema":         "pipeline_test",
		"name":           "demo_products",
		"qualified_name": "pipeline_test.demo_products",
	}, map[string]interface{}{
		"ops": map[string]interface{}{"c": float64(3)},
	}))

	if gotSchema != nil {
		t.Errorf("destination_schema bound as %#v, want NULL — a non-NULL value here rides "+
			"the COALESCE and erases the destination the sink recorded", gotSchema)
	}
	if gotQualified != nil {
		t.Errorf("destination_qualified_name bound as %#v, want NULL", gotQualified)
	}
}

// Whitespace is not a destination. Same argument as above: it must arrive as NULL.
func TestUpsertTableStatsTreatsBlankDestinationAsUnknown(t *testing.T) {
	gotSchema, gotQualified := projectTableStats(t, cdcTableStatsEvent(map[string]interface{}{
		"schema":                     "pipeline_test",
		"name":                       "demo_products",
		"qualified_name":             "pipeline_test.demo_products",
		"destination_schema":         "   ",
		"destination_qualified_name": "",
	}, nil))

	if gotSchema != nil || gotQualified != nil {
		t.Errorf("blank destination bound as (%#v, %#v), want (NULL, NULL)", gotSchema, gotQualified)
	}
}
