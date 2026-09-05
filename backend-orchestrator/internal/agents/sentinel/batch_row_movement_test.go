package sentinel

// batch_row_movement_test.go — the gap: every batch liveness signal this system has is a
// TIMESTAMP, and timestamps are the one thing a stuck run keeps producing.
//
// stalledBatchRunsQuery takes GREATEST() over three surfaces — execution start,
// pipeline_progress.updated_at, and MAX(pipeline_run_table_stats.updated_at) — and alarms
// when all three go quiet. But pipeline_run_table_stats is written by an upsert that
// stamps updated_at on EVERY write, including one whose counters are identical to the row
// already there (event_projector.go:918 — read_rows is GREATEST-merged, updated_at is not
// conditional). So a run retrying the same chunk forever, or a stats emitter heartbeating
// a table that is not moving, refreshes updated_at on every pass and the stall detector
// never fires. The run is dead and "moving" by the only measure being taken.
//
// The counters themselves are the measure that cannot be faked: read_rows only rises when
// rows are actually read. This adds the second detector — frozen counters while the
// clocks tick — under its own issue class, so it never stomps or auto-resolves a stall.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func batchRowCountRows(pipelineID, name, execID string, total int64) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "name", "execution_id", "total_rows"}).
		AddRow(pipelineID, name, execID, total)
}

func newRowMovementSentinel(t *testing.T) (*BatchSentinel, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	s := &BatchSentinel{
		db:                  db,
		stallThreshold:      DefaultBatchStallThreshold,
		noProgressThreshold: DefaultBatchNoProgressThreshold,
		rowMovement:         map[string]*rowMovementState{},
	}
	return s, mock, func() { _ = db.Close() }
}

// The first time a run is seen there is nothing to compare it against. Alarming here would
// mean every orchestrator restart raises an alarm on every healthy in-flight run.
func TestDecideRowMovement_FirstSightingIsNotEvidence(t *testing.T) {
	now := time.Now()
	next, alarm := decideRowMovement(nil,
		rowMovementObservation{executionID: "e1", total: 500, now: now},
		time.Hour)

	if alarm {
		t.Error("first sighting raised an alarm; it is a baseline, not evidence")
	}
	if next.total != 500 || next.executionID != "e1" || !next.frozenSince.Equal(now) {
		t.Errorf("baseline not stamped: %+v", next)
	}
}

// Any change in the total means rows are moving. The clock restarts from the change, not
// from when the pipeline was first seen.
func TestDecideRowMovement_MovementResetsTheClock(t *testing.T) {
	now := time.Now()
	prev := &rowMovementState{executionID: "e1", total: 500, frozenSince: now.Add(-8 * time.Hour)}

	next, alarm := decideRowMovement(prev,
		rowMovementObservation{executionID: "e1", total: 501, now: now},
		time.Hour)

	if alarm {
		t.Error("counters moved by one row and it still alarmed")
	}
	if !next.frozenSince.Equal(now) {
		t.Errorf("frozenSince = %v, want %v — movement must restart the clock", next.frozenSince, now)
	}
	if next.total != 501 {
		t.Errorf("total = %d, want 501", next.total)
	}
}

func TestDecideRowMovement_FrozenPastTheThresholdAlarms(t *testing.T) {
	now := time.Now()
	prev := &rowMovementState{executionID: "e1", total: 500, frozenSince: now.Add(-90 * time.Minute)}

	next, alarm := decideRowMovement(prev,
		rowMovementObservation{executionID: "e1", total: 500, now: now},
		time.Hour)

	if !alarm {
		t.Error("counters frozen for 90m against a 60m threshold did not alarm")
	}
	// The clock must NOT be restarted by the alarm — otherwise the issue would be raised,
	// then immediately look fresh again, and the description's "frozen for X" would reset
	// to zero on every tick.
	if !next.frozenSince.Equal(prev.frozenSince) {
		t.Errorf("frozenSince moved on alarm: %v, want %v", next.frozenSince, prev.frozenSince)
	}
}

func TestDecideRowMovement_FrozenUnderTheThresholdIsQuiet(t *testing.T) {
	now := time.Now()
	prev := &rowMovementState{executionID: "e1", total: 500, frozenSince: now.Add(-30 * time.Minute)}

	if _, alarm := decideRowMovement(prev,
		rowMovementObservation{executionID: "e1", total: 500, now: now},
		time.Hour); alarm {
		t.Error("30m of frozen counters alarmed against a 60m threshold")
	}
}

// The single most dangerous case: a new run reusing a pipeline whose previous run ended
// frozen. Keying the state on the pipeline alone would let the fresh run inherit the dead
// run's clock and alarm on its first tick — before it has had a chance to read anything.
func TestDecideRowMovement_ANewExecutionStartsItsOwnClock(t *testing.T) {
	now := time.Now()
	prev := &rowMovementState{executionID: "e1", total: 500, frozenSince: now.Add(-8 * time.Hour)}

	next, alarm := decideRowMovement(prev,
		rowMovementObservation{executionID: "e2", total: 500, now: now},
		time.Hour)

	if alarm {
		t.Error("a new execution inherited the previous run's frozen clock and alarmed immediately")
	}
	if next.executionID != "e2" || !next.frozenSince.Equal(now) {
		t.Errorf("new execution did not rebaseline: %+v", next)
	}
}

// A total that goes DOWN is still something writing. Retention pruning a stats row, or a
// re-created table, must not be read as progress-free — but it must not alarm either.
// Treating any change as movement is the fail-safe direction.
func TestDecideRowMovement_ADecreaseCountsAsMovement(t *testing.T) {
	now := time.Now()
	prev := &rowMovementState{executionID: "e1", total: 500, frozenSince: now.Add(-8 * time.Hour)}

	if _, alarm := decideRowMovement(prev,
		rowMovementObservation{executionID: "e1", total: 400, now: now},
		time.Hour); alarm {
		t.Error("a decreasing total alarmed; any change means something is writing")
	}
}

func TestRowMovementTickRaisesTheFrozenCounterIssue(t *testing.T) {
	s, mock, done := newRowMovementSentinel(t)
	defer done()

	const pid = "aaaabbbb-0000-4000-8000-000000000001"
	s.rowMovement[pid] = &rowMovementState{
		executionID: "exec-1",
		total:       12000,
		frozenSince: time.Now().Add(-2 * DefaultBatchNoProgressThreshold),
	}

	mock.ExpectQuery(`FROM pipelines p`).
		WillReturnRows(batchRowCountRows(pid, "nightly-export", "exec-1", 12000))
	mock.ExpectExec(`INSERT INTO sentinel_active_issues`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	s.rowMovementTick(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("frozen counters did not raise an issue: %v", err)
	}
}

// The #730 shape, guarded directly: resolveStaleIssues DELETEs every open issue in the
// class this tick did not re-mark, and it looks them up by the id they were WRITTEN with.
// #730 shipped green because the still-open map was keyed on the bare pipelineID while the
// row id carried a prefix, so the resolver deleted every issue the same tick had just
// raised. sqlmock cannot prove a DELETE was absent by omission — an unexpected Exec fails
// at the driver and resolveStaleIssues swallows it with `continue`. So the forbidden
// DELETE is EXPECTED here and the assertion is that expectation going UNMET.
func TestRowMovementTickDoesNotDeleteTheIssueItJustRaised(t *testing.T) {
	s, mock, done := newRowMovementSentinel(t)
	defer done()

	const pid = "aaaabbbb-0000-4000-8000-000000000002"
	issueID := noProgressIssueID(pid)
	s.rowMovement[pid] = &rowMovementState{
		executionID: "exec-1",
		total:       7,
		frozenSince: time.Now().Add(-2 * DefaultBatchNoProgressThreshold),
	}

	mock.ExpectQuery(`FROM pipelines p`).
		WillReturnRows(batchRowCountRows(pid, "frozen", "exec-1", 7))
	mock.ExpectExec(`INSERT INTO sentinel_active_issues`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(issueID))
	mock.ExpectExec(`DELETE FROM sentinel_active_issues`).
		WithArgs(issueID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	s.rowMovementTick(context.Background())

	if err := mock.ExpectationsWereMet(); err == nil {
		t.Fatalf("the tick DELETEd %s, the issue it had just raised — "+
			"the still-open map is keyed on something other than the written id (#730)", issueID)
	}
}

// The positive control for the test above: without it, a resolver that never deletes
// anything would pass both.
func TestRowMovementTickClearsTheIssueWhenRowsMoveAgain(t *testing.T) {
	s, mock, done := newRowMovementSentinel(t)
	defer done()

	const pid = "aaaabbbb-0000-4000-8000-000000000003"
	issueID := noProgressIssueID(pid)
	s.rowMovement[pid] = &rowMovementState{
		executionID: "exec-1",
		total:       7,
		frozenSince: time.Now().Add(-2 * DefaultBatchNoProgressThreshold),
	}

	// Same run, higher total: rows moved.
	mock.ExpectQuery(`FROM pipelines p`).
		WillReturnRows(batchRowCountRows(pid, "recovered", "exec-1", 99))
	mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(issueID))
	mock.ExpectExec(`DELETE FROM sentinel_active_issues`).
		WithArgs(issueID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	s.rowMovementTick(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a run whose rows started moving again kept its alarm: %v", err)
	}
}

// The state map is the only unbounded structure in this sentinel. A pipeline that finished
// is never returned by the query again, so nothing else would ever drop its entry.
func TestRowMovementTickForgetsRunsThatEnded(t *testing.T) {
	s, mock, done := newRowMovementSentinel(t)
	defer done()

	const live = "aaaabbbb-0000-4000-8000-000000000004"
	const ended = "aaaabbbb-0000-4000-8000-000000000005"
	s.rowMovement[live] = &rowMovementState{executionID: "exec-1", total: 1, frozenSince: time.Now()}
	s.rowMovement[ended] = &rowMovementState{executionID: "exec-9", total: 1, frozenSince: time.Now()}

	mock.ExpectQuery(`FROM pipelines p`).
		WillReturnRows(batchRowCountRows(live, "still-running", "exec-1", 2))
	mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	s.rowMovementTick(context.Background())

	if _, ok := s.rowMovement[ended]; ok {
		t.Error("state for a finished run was retained; the map grows without bound")
	}
	if _, ok := s.rowMovement[live]; !ok {
		t.Error("state for a running run was pruned")
	}
}

// A query failure must not be read as "no running pipelines" — that would prune every
// baseline and hand every healthy run a fresh clock on each failed tick, permanently
// disarming the detector against a database that is intermittently slow.
func TestRowMovementTickKeepsItsBaselinesWhenTheQueryFails(t *testing.T) {
	s, mock, done := newRowMovementSentinel(t)
	defer done()

	const pid = "aaaabbbb-0000-4000-8000-000000000006"
	frozen := time.Now().Add(-30 * time.Minute)
	s.rowMovement[pid] = &rowMovementState{executionID: "exec-1", total: 1, frozenSince: frozen}

	mock.ExpectQuery(`FROM pipelines p`).WillReturnError(context.DeadlineExceeded)

	s.rowMovementTick(context.Background())

	st, ok := s.rowMovement[pid]
	if !ok {
		t.Fatal("a failed query pruned the baselines")
	}
	if !st.frozenSince.Equal(frozen) {
		t.Errorf("frozenSince = %v, want %v — a failed query rebaselined a run", st.frozenSince, frozen)
	}
}

// The two detectors must be mutually exclusive by construction. A run that is quiet on
// every timestamp is already reported as stalled; reporting it AGAIN as frozen-counters
// gives an operator two issues, two descriptions, and one problem.
func TestRowMovementQueryLeavesTimestampStallsToTheStallDetector(t *testing.T) {
	q := runningBatchRowCountsQuery

	if !strings.Contains(q, "SUM(") || !strings.Contains(q, "read_rows") {
		t.Error("row-movement query does not aggregate read_rows; a single sampled row is not the run's progress")
	}
	// Same CDC carve-out as the stall query: a CDC stream's counters are supposed to sit
	// still between change events.
	if !strings.Contains(q, "sync_mode IS DISTINCT FROM 'cdc'") || !strings.Contains(q, "cdc_mode IS NULL") {
		t.Error("row-movement query no longer excludes CDC pipelines")
	}
	// Same HITL carve-out: a run parked waiting for a human reads zero rows on purpose,
	// for up to 24 hours.
	if !strings.Contains(q, "waiting_for_user") {
		t.Error("row-movement query no longer excludes runs parked at the table-selection HITL")
	}
	// And the exclusion that keeps the two classes disjoint: only runs whose clocks ARE
	// ticking are candidates here.
	if !strings.Contains(q, ">= NOW() - $1::interval") {
		t.Error("row-movement query does not exclude runs the stall detector already owns")
	}
}

func TestNoProgressIssueIDIsItsOwnClass(t *testing.T) {
	id := noProgressIssueID("p1")
	if !strings.HasPrefix(id, batchNoProgressPrefix) {
		t.Fatalf("noProgressIssueID(%q) = %q, want the %q prefix", "p1", id, batchNoProgressPrefix)
	}
	// resolveStaleIssues scopes with `id LIKE prefix || '%'`, so an overlapping prefix
	// would let one class's resolver delete another class's rows.
	others := []string{batchStallIssuePrefix, batchAckIssuePrefix, batchSinkAbsentPrefix}
	for _, other := range others {
		if strings.HasPrefix(batchNoProgressPrefix, other) || strings.HasPrefix(other, batchNoProgressPrefix) {
			t.Errorf("%q and %q overlap as LIKE prefixes", batchNoProgressPrefix, other)
		}
	}
}

func TestRowMovementTickIsInertWithoutADB(t *testing.T) {
	noDB := &BatchSentinel{
		stallThreshold:      DefaultBatchStallThreshold,
		noProgressThreshold: DefaultBatchNoProgressThreshold,
	}
	noDB.rowMovementTick(context.Background()) // must not panic on a nil db or a nil map
}
