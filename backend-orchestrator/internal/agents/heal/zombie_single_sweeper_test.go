package heal

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// The healer shipped TWO zombie sweepers, both live in the same process, both driven from
// HealWorker.Start:
//
//   - HealWorker.sweepZombies (worker.go), every 60s, running sweepZombiesQuery — which
//     closes the executions row, the pipeline_progress row, AND the pipelines row.
//   - ZombieExecutionSweeper.Sweep (auto_executors.go), hourly, running its own hand-written
//     near-copy that closed the executions and pipeline_progress rows and left the pipelines
//     row alone.
//
// Whichever statement reaches a row first wins it permanently: both guard on
// executions.status = 'running', so once one flips the row to 'failed' the other's predicate
// can never match it again. When the hourly copy won, the pipeline stayed pinned at
// status='running' with nothing left to close it — exactly KI-3, the bug the pipelines_closed
// CTE was written to fix, reintroduced by the duplicate that was never taught about it.
//
// The fix is not to add the missing CTE to the copy. Two statements answering one question is
// the defect; a second correct statement is still a second statement, and the next change to
// the sweep semantics would have to find both. There must be exactly one.
func TestZombieSweeperRunsTheOneCanonicalStatement(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	// QueryMatcherEqual, so this passes only if the SQL is sweepZombiesQuery itself —
	// not a near-copy, not a reformatted equivalent.
	mock.ExpectQuery(sweepZombiesQuery).
		WillReturnRows(sqlmock.NewRows([]string{"id", "pipeline_id", "reconciled", "pipelines_closed"}))

	s := &ZombieExecutionSweeper{DB: db}
	if err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the hourly zombie sweeper did not run sweepZombiesQuery — a second, divergent "+
			"zombie statement is back, and the one that omits pipelines_closed leaves every "+
			"pipeline it wins pinned at status='running' forever: %v", err)
	}
}

// The per-execution heal event is what puts the reap on the pipeline's timeline; without it
// a run goes from Running to Failed with no explanation of who did it or why.
//
// It only ever existed on the hourly copy. The 60s sweep — which wins essentially every race,
// running 60x more often — wrote nothing, so in practice the event almost never appeared. The
// single surviving statement has to carry it.
func TestSweepZombiesWritesAHealEventPerReapedExecution(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(`WITH swept AS`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "pipeline_id", "reconciled", "pipelines_closed"}).
			AddRow("exec-1", "pipe-1", 1, 1).
			AddRow("exec-2", "pipe-2", 1, 1))
	mock.ExpectExec(`INSERT INTO pipeline_run_events`).
		WithArgs("pipe-1", "exec-1", sqlmock.AnyArg(), "healer_zombie_swept", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO pipeline_run_events`).
		WithArgs("pipe-2", "exec-2", sqlmock.AnyArg(), "healer_zombie_swept", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := &HealWorker{DB: db}
	w.sweepZombies(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the 60s zombie sweep reaped executions without writing a heal event for them, "+
			"so the reap is invisible on the run timeline: %v", err)
	}
}

// The old copy stamped a different error prefix on the row it reaped ('healer_zombie_sweep: ')
// than the canonical statement does ('zombie: '), so the same condition reached the diagnoser
// as two different strings depending on which sweeper happened to win. One statement means one
// string; this pins it so a future edit cannot quietly fork it again.
func TestOnlyOneZombieErrorPrefixExists(t *testing.T) {
	if !strings.Contains(sweepZombiesQuery, "'zombie: execution timed out with no end_time (healer cleanup)'") {
		t.Error("sweepZombiesQuery no longer stamps the canonical 'zombie: ' error prefix")
	}
	if strings.Contains(sweepZombiesQuery, "healer_zombie_sweep:") {
		t.Error("the duplicate sweeper's 'healer_zombie_sweep: ' prefix is back in the canonical statement")
	}
}
