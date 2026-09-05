package heal

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// pipelinesClosedCTE returns just the body of the pipelines_closed CTE, so the assertions
// below cannot be accidentally satisfied by an identical-looking predicate in a sibling CTE.
func pipelinesClosedCTE(t *testing.T) string {
	t.Helper()
	start := strings.Index(sweepZombiesQuery, "pipelines_closed AS (")
	if start < 0 {
		t.Fatal("sweepZombiesQuery no longer has a pipelines_closed CTE — batch pipelines will sit " +
			"at status='running' forever after their only execution is swept to 'failed'")
	}
	end := strings.Index(sweepZombiesQuery[start:], "RETURNING p.id")
	if end < 0 {
		t.Fatal("pipelines_closed CTE has no `RETURNING p.id` — the count in the final SELECT will be wrong")
	}
	return sweepZombiesQuery[start : start+end]
}

// TestSweepZombiesQuery_ClosesPipelineRowForNonCDC pins finding 4.
//
// sweepZombies used to close only the executions row and the pipeline_progress row. The
// pipelines row was left alone for EVERY pipeline because of a CDC carve-out — but the carve-out
// only makes sense for CDC. Nothing is streaming for a batch run, so no second writer was ever
// coming to close it, and the pipeline sat at status='running' indefinitely while its latest
// execution said 'failed'. Two code paths answering "is this pipeline running?" and disagreeing.
func TestSweepZombiesQuery_ClosesPipelineRowForNonCDC(t *testing.T) {
	cte := pipelinesClosedCTE(t)

	for _, want := range []struct{ frag, why string }{
		{"UPDATE pipelines p", "the CTE must actually write the pipelines row"},
		{"status        = 'failed'", "a swept-to-failed execution means the pipeline failed"},
		{"completed_at", "migration 020 added completed_at so the list stops showing 'running'"},
	} {
		if !strings.Contains(cte, want.frag) {
			t.Errorf("pipelines_closed CTE is missing %q — %s", want.frag, want.why)
		}
	}

	if !strings.Contains(sweepZombiesQuery, "(SELECT count(*) FROM pipelines_closed)") {
		t.Error("the final SELECT does not count pipelines_closed, so the log line cannot report it")
	}
}

// TestSweepZombiesQuery_KeepsCDCCarveOut asserts the carve-out survives. A CDC pipeline can
// legitimately keep streaming even though its initial execution row was never closed, and the
// sweep cannot signal its workflow (there is no workflow_id on executions), so closing it here
// would falsely kill a live stream. Both columns must be checked: sync_mode is nullable
// (migration 025 adds it with no default), and cdc_mode alone can mark a pipeline as CDC.
func TestSweepZombiesQuery_KeepsCDCCarveOut(t *testing.T) {
	cte := pipelinesClosedCTE(t)

	if !strings.Contains(cte, "p.sync_mode IS DISTINCT FROM 'cdc'") {
		t.Error("pipelines_closed lost the sync_mode CDC carve-out. Note it must be IS DISTINCT FROM, " +
			"not <>: sync_mode is nullable, and NULL <> 'cdc' is NULL, which would exclude every " +
			"legacy batch pipeline from being closed")
	}
	if !strings.Contains(cte, "p.cdc_mode IS NULL") {
		t.Error("pipelines_closed lost the cdc_mode carve-out — a pipeline marked CDC via cdc_mode " +
			"alone (sync_mode still 'batch') would have its live stream closed out from under it")
	}
}

// TestSweepZombiesQuery_ExcludesSweptRowsFromTheOpenExecutionGuard is the subtle one, and the
// reason this test file exists at all.
//
// The guard "don't close a pipeline that has another execution still in flight" is written as
// NOT EXISTS over executions. But every CTE in a single statement reads the SAME snapshot, so
// that subquery still sees the rows `swept` just closed as status='running' AND end_time IS NULL.
// Without explicitly excluding the swept ids the NOT EXISTS is false for every candidate and the
// whole CTE silently updates nothing — no error, no log, just a fix that does not fire.
//
// Verified empirically against PostgreSQL 16: dropping the exclusion took pipelines_closed from
// 3 to 0 on an otherwise-identical 9-fixture matrix.
func TestSweepZombiesQuery_ExcludesSweptRowsFromTheOpenExecutionGuard(t *testing.T) {
	cte := pipelinesClosedCTE(t)

	guard := regexp.MustCompile(`NOT EXISTS\s*\(\s*SELECT 1 FROM swept s WHERE s\.id = e2\.id\s*\)`)
	if !guard.MatchString(cte) {
		t.Fatal("the in-flight-execution guard no longer excludes the rows `swept` just closed.\n" +
			"CTEs share one snapshot, so e2 still sees those rows as running with a NULL end_time; " +
			"the NOT EXISTS is therefore false for every candidate and pipelines_closed updates NOTHING, " +
			"silently. Restore: AND NOT EXISTS (SELECT 1 FROM swept s WHERE s.id = e2.id)")
	}

	if !strings.Contains(cte, "e2.pipeline_id = p.id") {
		t.Error("the in-flight guard is not scoped to this pipeline — any open execution anywhere " +
			"would block every pipeline from closing")
	}
}

// TestSweepZombiesQuery_NeverClobbersNonRunningOrExistingState covers the write-safety guards:
// only a pipeline still claiming to run is touched (never a user's explicit paused/stopped, nor
// an already-terminal row), and pre-existing completed_at/error_message are preserved.
func TestSweepZombiesQuery_NeverClobbersNonRunningOrExistingState(t *testing.T) {
	cte := pipelinesClosedCTE(t)

	if !strings.Contains(cte, "p.status = 'running'") {
		t.Error("pipelines_closed does not restrict to status='running' — it would overwrite a " +
			"user's explicit paused/stopped state, or re-stamp an already-terminal pipeline")
	}
	if !strings.Contains(cte, "COALESCE(p.completed_at, NOW())") {
		t.Error("completed_at is not COALESCEd — a re-run of the sweep would move an existing timestamp")
	}
	if !strings.Contains(cte, "NULLIF(p.error_message, '')") {
		t.Error("error_message is not preserved via COALESCE/NULLIF — the healer's generic message " +
			"would overwrite a real diagnostic the operator or executor already wrote")
	}
}

// TestSweepZombiesQuery_HITLGuardStillGuardsTheSweep re-pins guard 1 alongside the new CTE: a run
// parked at a human-in-the-loop step holds status='running' with a NULL end_time ON PURPOSE, for
// up to 24h. It must never be swept — and because pipelines_closed keys off swept, protecting the
// execution transitively protects the pipeline row too.
func TestSweepZombiesQuery_HITLGuardStillGuardsTheSweep(t *testing.T) {
	if !strings.Contains(sweepZombiesQuery, "pp.status = 'waiting_for_user'") {
		t.Fatal("the waiting_for_user guard is gone — the sweep will falsely fail runs that are " +
			"legitimately parked at a HITL step while their Temporal workflow is still alive")
	}
}

// TestSweepZombiesQuery_HITLGuardIsAgeBounded pins the OTHER half of guard 1, which is
// the half that was missing.
//
// "For up to 24h" was only ever a sentence in a comment. The guard itself matched any
// waiting_for_user row regardless of age, so it shielded a parked run from the sweep
// FOREVER — and the sweep is the only thing that ever closes a run whose Temporal
// workflow is gone.
//
// A live workflow no longer needs that shield indefinitely: every one of the six park
// sites now waits on awaitHITLSignal with TimeoutAt = park + 24h, so it fails itself,
// with an accurate message, long before this ceiling. The unbounded guard therefore
// protected exactly one population — parks whose workflow is DEAD and can never fire a
// timer. That is a pipeline showing "running" until someone notices by hand; prod had
// one at 382 hours.
//
// The ceiling has to sit clear of the 24h workflow timeout so the workflow always wins
// the race and writes the specific error; the sweep is a backstop, not a competitor.
func TestSweepZombiesQuery_HITLGuardIsAgeBounded(t *testing.T) {
	if !strings.Contains(sweepZombiesQuery, "pp.updated_at > NOW() - $2::interval") {
		t.Fatal("the waiting_for_user guard has no age ceiling — a run whose Temporal workflow " +
			"died while parked is shielded from the sweep forever and shows 'running' until a " +
			"human notices (prod had one at 382h)")
	}
	if AbandonedParkTimeout <= 24*time.Hour {
		t.Errorf("AbandonedParkTimeout = %s, but the HITL park timeout is 24h — the sweep would "+
			"race the workflow's own timer and stamp the generic 'zombie:' message over the "+
			"accurate one", AbandonedParkTimeout)
	}
}

// TestSweepZombies_ScansRowColumnsInOrder is the plumbing check: the query returns one row per
// swept execution (id, pipeline_id) plus the two repair counts, and those four columns must land
// in the four destinations the caller uses. A transposition of the first two would attribute the
// reap to the wrong pipeline on the timeline; of the last two, misreport how many pipelines were
// closed.
//
// Asserting the heal-event arguments is what makes this test unable to pass on a scan failure:
// a mismatched row shape yields zero reaped rows, so no INSERT is issued and the expectation
// goes unmet. Checking only that the SELECT ran would stay green through exactly that bug.
func TestSweepZombies_ScansRowColumnsInOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("WITH swept AS").
		WithArgs(ZombieTimeout.String(), AbandonedParkTimeout.String()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "pipeline_id", "reconciled", "pipelines_closed"}).
			AddRow("exec-7", "pipe-7", 2, 3))
	mock.ExpectExec(`INSERT INTO pipeline_run_events`).
		WithArgs("pipe-7", "exec-7", sqlmock.AnyArg(), "healer_zombie_swept", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := &HealWorker{DB: db}
	w.sweepZombies(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sweepZombies did not run the expected statements: %v", err)
	}
}

// TestSweepZombies_QueryFailureDoesNotPanic — the sweep runs on a ticker in a background
// goroutine; a DB blip must degrade to a logged warning, not take the healer down.
func TestSweepZombies_QueryFailureDoesNotPanic(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("WITH swept AS").WillReturnError(context.DeadlineExceeded)

	w := &HealWorker{DB: db}
	w.sweepZombies(context.Background()) // must not panic
}
