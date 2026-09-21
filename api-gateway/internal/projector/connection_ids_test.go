package projector

// A chat pipeline whose connections the workflow found by connector type ran end to end
// and still listed as "— → —": the workflow kept the ids in its own state and only the
// HITL path wrote them to the pipelines row. The connection_validation STAGE_COMPLETED
// event carries both ids, and maybePersistConnectionIDs records them.
//
// These tests pin which events write and what gets bound. What the statement does in
// Postgres (fill NULLs only, same workspace only, replay is a no-op) is not visible to
// sqlmock; the statement text assertion below keeps the clauses that carry it.

import (
	"encoding/json"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/segmentio/kafka-go"
)

const (
	connTestPipeline = "33333333-3333-3333-3333-333333333333"
	connTestSource   = "44444444-4444-4444-4444-444444444444"
	connTestDest     = "55555555-5555-5555-5555-555555555555"
)

func connectionValidationEvent(eventType, stage string, meta map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"schema_version": float64(2),
		"event_type":     eventType,
		"pipeline_id":    connTestPipeline,
		"execution_id":   "66666666-6666-6666-6666-666666666666",
		"stage":          stage,
		"metadata":       meta,
	}
}

func newConnIDProjector(t *testing.T) (*EventProjector, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return &EventProjector{db: db, lastSeq: map[string]int64{}, gapSeen: map[string]bool{}}, mock
}

func TestConnectionValidationCompletedRecordsBothIDs(t *testing.T) {
	p, mock := newConnIDProjector(t)
	mock.ExpectExec(regexp.QuoteMeta("UPDATE pipelines p")).
		WithArgs(connTestPipeline, connTestSource, connTestDest).
		WillReturnResult(sqlmock.NewResult(0, 1))

	p.maybePersistConnectionIDs(connectionValidationEvent("STAGE_COMPLETED", "connection_validation", map[string]interface{}{
		"source_connection_id":      connTestSource,
		"destination_connection_id": connTestDest,
		"execution_plan":            map[string]interface{}{},
	}))

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the resolved ids were not written: %v", err)
	}
}

// One resolved side must still be recorded; a malformed id binds as NULL instead of
// failing the statement and losing the good side with it.
func TestConnectionValidationRecordsTheSideThatResolved(t *testing.T) {
	p, mock := newConnIDProjector(t)
	mock.ExpectExec(regexp.QuoteMeta("UPDATE pipelines p")).
		WithArgs(connTestPipeline, nil, connTestDest).
		WillReturnResult(sqlmock.NewResult(0, 1))

	p.maybePersistConnectionIDs(connectionValidationEvent("STAGE_COMPLETED", "connection_validation", map[string]interface{}{
		"source_connection_id":      "not-a-uuid",
		"destination_connection_id": connTestDest,
	}))

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the destination id was not written on its own: %v", err)
	}
}

// The clauses that make the write safe live in the statement: a user's pick is never
// overwritten (COALESCE with the current value first), and an id only links when it
// names a connection in the pipeline's own workspace.
func TestPersistConnectionIDsStatementKeepsItsGuards(t *testing.T) {
	for _, clause := range []string{
		"COALESCE(p.source_connection_id,",
		"COALESCE(p.destination_connection_id,",
		"c.id = $2::uuid AND c.workspace_id = p.workspace_id",
		"c.id = $3::uuid AND c.workspace_id = p.workspace_id",
		"p.source_connection_id IS NULL AND EXISTS",
		"p.destination_connection_id IS NULL AND EXISTS",
	} {
		if n := len(regexp.MustCompile(regexp.QuoteMeta(clause)).FindAllString(persistConnectionIDsQuery, -1)); n == 0 {
			t.Errorf("persistConnectionIDsQuery lost %q", clause)
		}
	}
	if n := len(regexp.MustCompile(regexp.QuoteMeta("c.workspace_id = p.workspace_id")).FindAllString(persistConnectionIDsQuery, -1)); n != 4 {
		t.Errorf("workspace guard appears %d times, want 4 (two SET subqueries, two WHERE EXISTS)", n)
	}
}

func TestOtherEventsDoNotWriteConnectionIDs(t *testing.T) {
	ids := map[string]interface{}{
		"source_connection_id":      connTestSource,
		"destination_connection_id": connTestDest,
	}
	cases := map[string]map[string]interface{}{
		"started, not completed": connectionValidationEvent("STAGE_STARTED", "connection_validation", ids),
		"failed, not completed":  connectionValidationEvent("STAGE_FAILED", "connection_validation", ids),
		"another stage":          connectionValidationEvent("STAGE_COMPLETED", "executor", ids),
		"no ids in the metadata": connectionValidationEvent("STAGE_COMPLETED", "connection_validation", map[string]interface{}{}),
		"both ids malformed":     connectionValidationEvent("STAGE_COMPLETED", "connection_validation", map[string]interface{}{"source_connection_id": "", "destination_connection_id": "x"}),
		"metadata missing":       connectionValidationEvent("STAGE_COMPLETED", "connection_validation", nil),
	}
	cases["pipeline id malformed"] = connectionValidationEvent("STAGE_COMPLETED", "connection_validation", ids)
	cases["pipeline id malformed"]["pipeline_id"] = "abc"

	for name, ev := range cases {
		t.Run(name, func(t *testing.T) {
			p, mock := newConnIDProjector(t)
			// Arm the write and require it to stay unused. An unexpected statement would
			// not do: sqlmock returns it an error, the best-effort hook swallows that, and
			// ExpectationsWereMet reports only expectations that were never consumed.
			mock.ExpectExec(regexp.QuoteMeta("UPDATE pipelines p")).WillReturnResult(sqlmock.NewResult(0, 1))
			p.maybePersistConnectionIDs(ev)
			if mock.ExpectationsWereMet() == nil {
				t.Fatal("the connection ids were written for an event that must not write them")
			}
		})
	}

	// Control: the same harness consumes the armed write for the event that should.
	p, mock := newConnIDProjector(t)
	mock.ExpectExec(regexp.QuoteMeta("UPDATE pipelines p")).WillReturnResult(sqlmock.NewResult(0, 1))
	p.maybePersistConnectionIDs(connectionValidationEvent("STAGE_COMPLETED", "connection_validation", ids))
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("control: the harness did not see the write it should have: %v", err)
	}
}

// The hook only helps if the consume path calls it.
func TestProjectEventRecordsConnectionIDs(t *testing.T) {
	p, mock := newConnIDProjector(t)
	// The run-event store and progress projection issue their own statements first;
	// they are not under test, so match out of order and let them fail unexpected.
	mock.MatchExpectationsInOrder(false)
	mock.ExpectExec(regexp.QuoteMeta("UPDATE pipelines p")).
		WithArgs(connTestPipeline, connTestSource, connTestDest).
		WillReturnResult(sqlmock.NewResult(0, 1))

	body, err := json.Marshal(connectionValidationEvent("STAGE_COMPLETED", "connection_validation", map[string]interface{}{
		"source_connection_id":      connTestSource,
		"destination_connection_id": connTestDest,
	}))
	if err != nil {
		t.Fatal(err)
	}
	_ = p.projectEvent(kafka.Message{Value: body})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("projectEvent did not record the resolved connection ids: %v", err)
	}
}
