package heal

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// staleCTE returns just the body of the `stale` CTE — the SELECT that decides
// which pipelines get written. Scoping the assertions to it keeps them from being
// accidentally satisfied by a lookalike predicate in the UPDATE below it.
func staleCTE(t *testing.T) string {
	t.Helper()
	start := strings.Index(reconcileStaleParksQuery, "WITH stale AS (")
	if start < 0 {
		t.Fatal("reconcileStaleParksQuery no longer has a `stale` CTE — pipelines whose run ended " +
			"while parked at a HITL step will sit at status='running' forever")
	}
	end := strings.Index(reconcileStaleParksQuery, "UPDATE pipelines p")
	if end < 0 || end < start {
		t.Fatal("reconcileStaleParksQuery no longer ends its CTE with `UPDATE pipelines p`")
	}
	return reconcileStaleParksQuery[start:end]
}

// TestStaleParks_MirrorsTheReaderPredicate is the whole point of this fix.
//
// api-gateway staleParkTerminalStatus (internal/handlers/pipeline_state.go)
// already computes this exact verdict — but only to rewrite what ONE endpoint
// renders. The pipelines row it read from keeps saying 'running', so the list
// page and every other reader disagree with the detail badge. This test pins the
// writer to the reader clause for clause. If someone changes one, this fails and
// points at the other.
func TestStaleParks_MirrorsTheReaderPredicate(t *testing.T) {
	cte := staleCTE(t)

	for _, want := range []struct{ frag, why string }{
		{
			"lower(btrim(pp.status)) IN ('processing','waiting_for_user')",
			"the reader only rewrites these two park states; a writer that covered more would " +
				"close pipelines the detail page still shows as live",
		},
		{
			"e.end_time IS NOT NULL",
			"the reader treats a NULL end_time as 'not evidence of anything' — an execution " +
				"without one may still be running",
		},
		{
			"lower(btrim(e.status)) IN ('failed','cancelled','canceled','stopped')",
			"the reader's terminal set, including the 'canceled' spelling. Dropping either " +
				"spelling silently strands every row written by the other code path",
		},
	} {
		if !strings.Contains(cte, want.frag) {
			t.Errorf("the `stale` CTE no longer contains %q.\nwhy it matters: %s", want.frag, want.why)
		}
	}
}

// TestStaleParks_SuccessIsNotTerminal guards the one exclusion that looks like an
// oversight and is not. A CDC run closes its execution row at the
// backfill→streaming handoff while the feed goes on streaming. If success or
// completed were treated as "the run ended", this sweep would mark every healthy
// CDC pipeline failed the moment it started streaming — the exact inversion of
// the bug it fixes.
func TestStaleParks_SuccessIsNotTerminal(t *testing.T) {
	cte := staleCTE(t)

	terminalSet := regexp.MustCompile(`lower\(btrim\(e\.status\)\) IN \(([^)]*)\)`)
	m := terminalSet.FindStringSubmatch(cte)
	if m == nil {
		t.Fatal("could not find the terminal-status set in the `stale` CTE")
	}
	for _, forbidden := range []string{"'success'", "'completed'"} {
		if strings.Contains(m[1], forbidden) {
			t.Errorf("%s is in the terminal set (%s). A CDC pipeline closes its execution row at "+
				"the backfill→streaming handoff and keeps streaming; this would mark every healthy "+
				"CDC pipeline failed.", forbidden, m[1])
		}
	}
}

// TestStaleParks_NeverWritesProgress is the guard that protects the healer from
// itself. RegenerateConnectorExecutor and RequestUserConfigExecutor create
// 'waiting_for_user' parks pointing at failed executions ON PURPOSE — that park
// is the healer's own HITL request, and the operator's prompt lives in it. A
// sweep that also "reconciled" pipeline_progress would delete the healer's asks
// as fast as it made them.
func TestStaleParks_NeverWritesProgress(t *testing.T) {
	writes := regexp.MustCompile(`(?i)UPDATE\s+(\w+)`).FindAllStringSubmatch(reconcileStaleParksQuery, -1)
	if len(writes) == 0 {
		t.Fatal("reconcileStaleParksQuery contains no UPDATE at all — it cannot fix anything")
	}
	for _, w := range writes {
		if strings.EqualFold(w[1], "pipeline_progress") {
			t.Error("this sweep writes pipeline_progress. It must write pipelines.status and " +
				"nothing else: the 'waiting_for_user' parks it would clear include the healer's " +
				"own HITL requests, whose prompts exist nowhere else.")
		}
		if !strings.EqualFold(w[1], "pipelines") {
			t.Errorf("this sweep writes an unexpected table %q — its contract is pipelines.status only", w[1])
		}
	}
}

// TestStaleParks_DoesNotClobberAnInFlightRun covers the two ways a live run could
// be destroyed: a newer execution actually running, and a pipeline whose status
// the user set deliberately.
func TestStaleParks_DoesNotClobberAnInFlightRun(t *testing.T) {
	cte := staleCTE(t)

	guard := regexp.MustCompile(`NOT EXISTS\s*\(\s*SELECT 1 FROM executions e2\s+WHERE e2\.pipeline_id = p\.id`)
	if !guard.MatchString(cte) {
		t.Error("the in-flight-execution guard is gone or is not scoped to this pipeline. A newer " +
			"run's 'running' is real and must never be overwritten by a verdict about an older one.")
	}
	if !strings.Contains(cte, "e2.end_time IS NULL") || !strings.Contains(cte, "e2.status = 'running'") {
		t.Error("the in-flight guard no longer identifies an open execution as running-with-no-end_time")
	}
	if !strings.Contains(cte, "p.status = 'running'") {
		t.Error("the sweep is not restricted to pipelines still claiming to run — it would overwrite " +
			"a user's explicit paused/stopped state or re-stamp an already-terminal row")
	}
	if !strings.Contains(cte, "e.end_time < NOW() - $1::interval") {
		t.Error("the grace period is gone. The projector and the workflow-completion path own this " +
			"transition; without the delay the healer races them instead of backstopping them.")
	}
}

// TestStaleParks_CDCGuardIsEvidenceBasedNotModeBased pins the deliberate
// difference from sweepZombies.
//
// sweepZombies excludes every CDC pipeline, because an execution still open there
// might belong to a live stream. Here the execution is closed as failed/cancelled
// — not the handoff shape — so a blanket mode exclusion would strand real CDC
// breakage forever (it stranded one on production). But a LATER failed re-run of
// an ALREADY-streaming pipeline is a genuine hazard, so the guard proves the
// handoff never happened rather than assuming it did: no success/completed
// execution has ever existed for this pipeline.
func TestStaleParks_CDCGuardIsEvidenceBasedNotModeBased(t *testing.T) {
	cte := staleCTE(t)

	if !strings.Contains(cte, "p.sync_mode IS DISTINCT FROM 'cdc'") {
		t.Fatal("the CDC branch is gone. It must be IS DISTINCT FROM, not <>: sync_mode is nullable " +
			"and NULL <> 'cdc' is NULL, which would exclude every legacy batch pipeline from the fix.")
	}

	handoffGuard := regexp.MustCompile(
		`NOT EXISTS\s*\(\s*SELECT 1 FROM executions e3\s+WHERE e3\.pipeline_id = p\.id\s+AND lower\(btrim\(e3\.status\)\) IN \('success','completed'\)`)
	if !handoffGuard.MatchString(cte) {
		t.Error("the CDC handoff-never-happened guard is missing. Without it this sweep would mark a " +
			"live streaming pipeline 'failed' on the strength of one failed re-run. With a blanket " +
			"mode exclusion instead, a CDC pipeline whose only run ever failed stays stuck at " +
			"'running' forever — that is the production row this fix exists for.")
	}

	// The two must be alternatives, not both-required: a batch pipeline that HAS
	// succeeded before must still be reconcilable.
	if !regexp.MustCompile(`p\.sync_mode IS DISTINCT FROM 'cdc'\s+OR`).MatchString(cte) {
		t.Error("the CDC branch is ANDed rather than ORed with the handoff guard — a batch pipeline " +
			"with any past successful run would be excluded from the fix for no reason")
	}
}

// TestStaleParks_PicksTheNewestRun — a pipeline accumulates one pipeline_progress
// row per execution. Joining them all and taking whichever the planner happens to
// emit first would let an ancient failed run decide the verdict for a pipeline
// whose latest run told a different story.
func TestStaleParks_PicksTheNewestRun(t *testing.T) {
	cte := staleCTE(t)

	if !strings.Contains(cte, "DISTINCT ON (p.id)") {
		t.Error("the CTE can emit more than one row per pipeline; the UPDATE would then apply an " +
			"arbitrary one of several verdicts")
	}
	if !strings.Contains(cte, "ORDER BY p.id, e.start_time DESC") {
		t.Error("DISTINCT ON without a matching ORDER BY picks an arbitrary row. It must order by " +
			"e.start_time DESC so the newest run — the one every surface is showing — wins.")
	}
}

// TestStaleParks_PreservesExistingDiagnostics — the sweep's generic message is a
// last resort. A real error already on the pipeline, or on the execution that
// ended it, is far more useful to whoever opens the row next.
func TestStaleParks_PreservesExistingDiagnostics(t *testing.T) {
	if !strings.Contains(reconcileStaleParksQuery, "NULLIF(p.error_message, '')") {
		t.Error("error_message is not preserved — the healer's generic string would overwrite a real " +
			"diagnostic the executor or the operator already wrote")
	}
	if !strings.Contains(reconcileStaleParksQuery, "NULLIF(stale.error_message, '')") {
		t.Error("the ending execution's error_message is not carried onto the pipeline row, so the " +
			"list page would show a failure with no reason attached")
	}
	if !strings.Contains(reconcileStaleParksQuery, "COALESCE(p.completed_at, NOW())") {
		t.Error("completed_at is not COALESCEd — a re-run of the sweep would move an existing timestamp")
	}
}

// TestStaleParks_MapsTerminalStatusesTheWayTheReaderDoes — failed stays 'failed';
// every other terminal state the reader recognises collapses to 'stopped'.
func TestStaleParks_MapsTerminalStatusesTheWayTheReaderDoes(t *testing.T) {
	cte := staleCTE(t)

	mapping := regexp.MustCompile(`CASE lower\(btrim\(e\.status\)\)\s+WHEN 'failed' THEN 'failed'\s+ELSE 'stopped'`)
	if !mapping.MatchString(cte) {
		t.Error("the status mapping no longer matches staleParkTerminalStatus, which returns \"failed\" " +
			"for failed and \"stopped\" for cancelled/canceled/stopped. A pipeline reported one way by " +
			"the detail endpoint and another by its own row is the bug this fix removes.")
	}
}

// TestReconcileStaleParks_PassesTheGraceIntervalAndCountsRows is the plumbing
// check: the right argument reaches the query and every returned row is counted.
func TestReconcileStaleParks_PassesTheGraceIntervalAndCountsRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("WITH stale AS").
		WithArgs(StaleParkGrace.String()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "new_status"}).
			AddRow("p-1", "failed").
			AddRow("p-2", "stopped"))

	w := &HealWorker{DB: db}
	if got := w.reconcileStaleParks(context.Background()); got != 2 {
		t.Errorf("reconciled %d pipelines, want 2", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("reconcileStaleParks did not run the expected query: %v", err)
	}
}

// TestReconcileStaleParks_QueryFailureDoesNotPanic — this runs on a ticker in a
// background goroutine; a DB blip must degrade to a logged warning, not take the
// healer down.
func TestReconcileStaleParks_QueryFailureDoesNotPanic(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("WITH stale AS").WillReturnError(context.DeadlineExceeded)

	w := &HealWorker{DB: db}
	if got := w.reconcileStaleParks(context.Background()); got != 0 {
		t.Errorf("reconciled %d on a failed query, want 0", got)
	}
}

// TestReconcileStaleParks_NilDBIsSafe — HealWorker is constructed in several
// places and the sweep loop must not assume a DB is wired.
func TestReconcileStaleParks_NilDBIsSafe(t *testing.T) {
	w := &HealWorker{}
	if got := w.reconcileStaleParks(context.Background()); got != 0 {
		t.Errorf("reconciled %d with a nil DB, want 0", got)
	}
}
