package heal

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

const (
	supersededQuery = `SELECT COUNT\(\*\) FROM heal_attempts`
	successorQuery  = `FROM executions e`
	subjectQuery    = `SELECT e\.status, e\.end_time`
)

func newVerifier(t *testing.T) (*ExecutionOutcomeVerifier, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	return &ExecutionOutcomeVerifier{DB: db}, mock, func() { _ = db.Close() }
}

func noNewerAttempts(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(supersededQuery).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
}

// subjectUnchanged declares that the subject execution row is still sitting at the
// end_time it had when the heal fired — i.e. nothing happened in place either. Any
// test that drives the no-successor path has to say something about this query now,
// because that path consults the subject row before concluding nothing ran.
func subjectUnchanged(mock sqlmock.Sqlmock, a Attempt) {
	mock.ExpectQuery(subjectQuery).WillReturnRows(
		sqlmock.NewRows([]string{"status", "end_time"}).
			AddRow("failed", a.CreatedAt.Add(-time.Hour)))
}

// attemptAged is the ordinary path: the healer picked an action and RAN it. That is
// stated explicitly because the zero value silently answers a question these tests do
// not mean to ask — Verify refuses to credit an attempt that executed nothing, so a
// fixture defaulting to ActionExecuted:false would make every verdict below
// self_resolved for a reason unrelated to what the test is named for. For the
// un-executed case see attemptAgedUnexecuted and the attribution tests.
func attemptAged(d time.Duration) Attempt {
	return Attempt{
		ID: 1, PipelineID: "p1", ExecutionID: "e1",
		FailureSignature: "sig-a",
		CreatedAt:        time.Now().Add(-d),
		Action:           "restart_sink",
		Outcome:          "auto_executed",
		ActionExecuted:   true,
	}
}

// attemptAgedUnexecuted is production's escalate-to-human shape: a decision was recorded,
// a human was notified, and the healer itself changed nothing.
func attemptAgedUnexecuted(d time.Duration) Attempt {
	a := attemptAged(d)
	a.Action = "escalate_to_human"
	a.Outcome = "escalated"
	a.ActionExecuted = false
	return a
}

func TestClassifyExecutionStatus(t *testing.T) {
	tests := []struct {
		status       string
		wantVerdict  Verdict
		wantTerminal bool
	}{
		{"completed", VerdictHealed, true},
		{"succeeded", VerdictHealed, true},
		{"success", VerdictHealed, true},
		{"failed", VerdictFailedAgain, true},
		{"error", VerdictFailedAgain, true},
		// The postflight guard's own downgrades: a run that "completed" while
		// dropping data must count as a failure, or the healer would learn that
		// its action works from runs that silently lost rows.
		{"silent_drop_detected", VerdictFailedAgain, true},
		{"silent_partial_drop_detected", VerdictFailedAgain, true},
		{"credential_check_failed", VerdictFailedAgain, true},
		// A user stopping the run says nothing about whether the heal would have
		// worked; counting it as failed_again would feed the give-up streak with
		// evidence that isn't evidence.
		{"cancelled", VerdictInconclusive, true},
		{"stopped", VerdictInconclusive, true},
		// Not terminal — the verifier must wait rather than guess.
		{"running", "", false},
		{"pending", "", false},
		{"streaming", "", false},
		{"", "", false},
		{"some_future_status", "", false},
	}
	for _, tt := range tests {
		gotVerdict, gotTerminal := classifyExecutionStatus(tt.status)
		if gotVerdict != tt.wantVerdict || gotTerminal != tt.wantTerminal {
			t.Errorf("classifyExecutionStatus(%q) = (%q, %v), want (%q, %v)",
				tt.status, gotVerdict, gotTerminal, tt.wantVerdict, tt.wantTerminal)
		}
	}
}

func TestVerifySuccessorCompletedIsHealed(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnRows(
		sqlmock.NewRows([]string{"id", "status"}).AddRow("e2", "completed"))

	verdict, succ, decided := v.Verify(context.Background(), attemptAged(5*time.Minute))
	if !decided || verdict != VerdictHealed || succ != "e2" {
		t.Fatalf("Verify = (%q, %q, %v), want (healed, e2, true)", verdict, succ, decided)
	}
}

// A recovery the healer did not cause is not a heal. Production graded exactly this
// shape 'healed': action escalate_to_human, nothing executed, and the successor row it
// took credit for was the very execution that had raised the alert. Left uncorrected it
// corrupts two downstream readers — RecallBestAction learns that escalating "works", and
// the operator dashboard reports a self-healing system that has never healed anything.
//
// This test lives in the default suite deliberately. The real-database proof is in
// verifier_attribution_pg_test.go, but CI runs `go test ./...` with no build tags, so
// that file never runs there; without this test the rule has no CI protection at all.
func TestVerifyUnexecutedAttemptIsNotCreditedWithTheRecovery(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnRows(
		sqlmock.NewRows([]string{"id", "status"}).AddRow("e2", "completed"))

	verdict, succ, decided := v.Verify(context.Background(), attemptAgedUnexecuted(5*time.Minute))

	if !decided {
		t.Fatalf("Verify undecided; the successor is terminal and the attempt is still resolvable")
	}
	if verdict != VerdictSelfResolved {
		t.Errorf("Verify = %q, want %q — the run recovered, but this attempt executed nothing", verdict, VerdictSelfResolved)
	}
	// The successor is still reported: what changed is the credit, not the fact that a
	// later run succeeded. Losing the id would break the audit trail.
	if succ != "e2" {
		t.Errorf("successor = %q, want e2 — the downgrade must not discard which run recovered", succ)
	}
}

// The control for the test above. A fix that stopped the false credit by never returning
// healed would satisfy every assertion there and quietly destroy the verifier.
func TestVerifyExecutedAttemptStillEarnsHealed(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnRows(
		sqlmock.NewRows([]string{"id", "status"}).AddRow("e2", "completed"))

	verdict, _, decided := v.Verify(context.Background(), attemptAged(5*time.Minute))
	if !decided || verdict != VerdictHealed {
		t.Fatalf("Verify = (%q, %v), want (healed, true) — an executed action that fixed the run must still be credited", verdict, decided)
	}
}

// failed_again is deliberately NOT downgraded: it claims no credit, so rewriting it would
// be a second fabrication in the opposite direction — and it must keep counting toward the
// give-up streak so a genuinely stuck pipeline still stops being retried.
func TestVerifyUnexecutedAttemptStillReportsFailure(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnRows(
		sqlmock.NewRows([]string{"id", "status"}).AddRow("e2", "failed"))

	verdict, _, decided := v.Verify(context.Background(), attemptAgedUnexecuted(5*time.Minute))
	if !decided || verdict != VerdictFailedAgain {
		t.Fatalf("Verify = (%q, %v), want (failed_again, true)", verdict, decided)
	}
}

func TestVerifySuccessorFailedIsFailedAgain(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnRows(
		sqlmock.NewRows([]string{"id", "status"}).AddRow("e2", "failed"))

	verdict, succ, decided := v.Verify(context.Background(), attemptAged(5*time.Minute))
	if !decided || verdict != VerdictFailedAgain || succ != "e2" {
		t.Fatalf("Verify = (%q, %q, %v), want (failed_again, e2, true)", verdict, succ, decided)
	}
}

// A newer attempt on the same signature owns whatever happens next. Judging this
// attempt on that run would credit or blame the wrong action.
func TestVerifyNewerAttemptSupersedes(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	mock.ExpectQuery(supersededQuery).WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(1))
	// No successor lookup should follow — the answer is already final.

	verdict, _, decided := v.Verify(context.Background(), attemptAged(5*time.Minute))
	if !decided || verdict != VerdictSuperseded {
		t.Fatalf("Verify = (%q, %v), want (superseded, true)", verdict, decided)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The successor run is created asynchronously (executor POSTs → api-gateway enqueues
// → Temporal mints the execution row). Concluding "inconclusive" before that lands
// would misfile every single successful retry.
func TestVerifyWaitsForAnAsyncSuccessor(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	a := attemptAged(4 * time.Minute)
	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnError(sql.ErrNoRows)
	subjectUnchanged(mock, a)

	verdict, _, decided := v.Verify(context.Background(), a)
	if decided {
		t.Fatalf("Verify decided = true (verdict %q) before the no-successor timeout", verdict)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// Escalations and HITL prompts produce no run by design. Once the window closes,
// that is a final answer — inconclusive, not a failure.
func TestVerifyNoSuccessorAfterTimeoutIsInconclusive(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	a := attemptAged(VerifyNoSuccessorTimeout + time.Minute)
	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnError(sql.ErrNoRows)
	subjectUnchanged(mock, a)

	verdict, succ, decided := v.Verify(context.Background(), a)
	if !decided || verdict != VerdictInconclusive || succ != "" {
		t.Fatalf("Verify = (%q, %q, %v), want (inconclusive, \"\", true)", verdict, succ, decided)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A full backfill legitimately runs for tens of minutes. Giving up early would file
// a slow success as inconclusive and teach the healer nothing.
func TestVerifyWaitsWhileSuccessorIsStillRunning(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnRows(
		sqlmock.NewRows([]string{"id", "status"}).AddRow("e2", "running"))

	verdict, succ, decided := v.Verify(context.Background(), attemptAged(20*time.Minute))
	if decided {
		t.Fatalf("Verify decided = true (verdict %q) while the successor was still running", verdict)
	}
	// The successor id is still returned so the caller can record what it is watching.
	if succ != "e2" {
		t.Errorf("successor = %q, want e2 even while undecided", succ)
	}
}

// A run stuck in flight past the terminal timeout belongs to the zombie sweeper, not
// the verifier — which must stop holding the attempt open forever.
func TestVerifyStuckSuccessorEventuallyResolvesInconclusive(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnRows(
		sqlmock.NewRows([]string{"id", "status"}).AddRow("e2", "running"))

	v.TerminalTimeout = time.Minute
	verdict, succ, decided := v.Verify(context.Background(), attemptAged(90*time.Second))
	if !decided || verdict != VerdictInconclusive || succ != "e2" {
		t.Fatalf("Verify = (%q, %q, %v), want (inconclusive, e2, true)", verdict, succ, decided)
	}
}

// Fail-soft: a database error is not a verdict. Leaving the attempt unverified lets
// the next pass retry, whereas guessing would write a permanent wrong answer into
// the memory that later decisions are built on.
func TestVerifyFailsSoftOnQueryError(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnError(errors.New("connection reset by peer"))

	verdict, _, decided := v.Verify(context.Background(), attemptAged(time.Hour))
	if decided {
		t.Fatalf("Verify decided = true (verdict %q) on a query error", verdict)
	}
}

func TestVerifyNilReceiverAndNilDB(t *testing.T) {
	var nilV *ExecutionOutcomeVerifier
	if _, _, decided := nilV.Verify(context.Background(), attemptAged(time.Hour)); decided {
		t.Error("nil verifier decided a verdict")
	}
	empty := &ExecutionOutcomeVerifier{}
	if _, _, decided := empty.Verify(context.Background(), attemptAged(time.Hour)); decided {
		t.Error("verifier with no DB decided a verdict")
	}
}

// ── verifySubjectInPlace: grading a pipeline that never mints a second row ──
//
// Measured on production: batch pipelines hold 28 execution rows across 19
// pipelines (up to 4 for one), while all 3 CDC pipelines hold exactly 1 each —
// one of them re-stamped from 2026-07-16 to 2026-07-30 on a frozen start_time.
// The successor lookup is therefore structurally blind to CDC recovery, and every
// CDC heal used to age out at VerifyNoSuccessorTimeout as "inconclusive".

// subjectRow is the state of the subject execution row as the verifier finds it.
func subjectRow(mock sqlmock.Sqlmock, status string, endTime interface{}) {
	mock.ExpectQuery(subjectQuery).WillReturnRows(
		sqlmock.NewRows([]string{"status", "end_time"}).AddRow(status, endTime))
}

// The CDC recovery path end to end: a restarted stream reaches streaming_active,
// and UpdatePipelineStatusActivity closes the row as completed/NOW()
// (backend-temporal-adapter/internal/workflows/pipeline_status_activity.go:45-50).
// That is the only "it worked" signal CDC ever emits.
func TestVerifySubjectRowCompletedAfterHealIsHealed(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	a := attemptAged(10 * time.Minute)
	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnError(sql.ErrNoRows)
	subjectRow(mock, "completed", a.CreatedAt.Add(2*time.Minute))

	verdict, succ, decided := v.Verify(context.Background(), a)
	if !decided || verdict != VerdictHealed {
		t.Fatalf("Verify = (%q, %v), want (healed, true) — a CDC stream that recovered in "+
			"place must grade as healed, not age out as inconclusive", verdict, decided)
	}
	// The subject row IS the evidence, so it is what the attempt should point at.
	if succ != a.ExecutionID {
		t.Errorf("successor_execution_id = %q, want the subject row %q", succ, a.ExecutionID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The other half: a stream that failed again after the heal must be graded a
// failure. Without this the give-up streak never advances for CDC and the healer
// retries the same losing action forever.
func TestVerifySubjectRowFailedAfterHealIsFailedAgain(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	a := attemptAged(10 * time.Minute)
	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnError(sql.ErrNoRows)
	subjectRow(mock, "failed", a.CreatedAt.Add(90*time.Second))

	verdict, _, decided := v.Verify(context.Background(), a)
	if !decided || verdict != VerdictFailedAgain {
		t.Fatalf("Verify = (%q, %v), want (failed_again, true)", verdict, decided)
	}
}

// The discriminator, isolated: end_time at or BEFORE the attempt is the row's
// pre-existing failure — the very failure that triggered the heal. Grading it
// would mark every heal failed the instant it was recorded.
func TestVerifySubjectRowIsNotGradedFromItsOwnPreHealFailure(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	a := attemptAged(10 * time.Minute)
	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnError(sql.ErrNoRows)
	subjectRow(mock, "failed", a.CreatedAt.Add(-time.Second))

	verdict, _, decided := v.Verify(context.Background(), a)
	if decided {
		t.Fatalf("Verify = (%q, true) from an end_time that predates the heal — the healer "+
			"would grade its own trigger as the outcome", verdict)
	}
}

// Batch regression. A closed batch row is never rewritten, so this path must stay
// inert for batch and leave the successor lookup as the only source of truth.
func TestVerifySubjectRowInertForBatch(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	a := attemptAged(VerifyNoSuccessorTimeout + time.Minute)
	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnError(sql.ErrNoRows)
	subjectUnchanged(mock, a) // frozen at its pre-heal end_time, as batch rows are

	verdict, succ, decided := v.Verify(context.Background(), a)
	if !decided || verdict != VerdictInconclusive || succ != "" {
		t.Fatalf("Verify = (%q, %q, %v), want (inconclusive, \"\", true) — batch behaviour "+
			"must be unchanged", verdict, succ, decided)
	}
}

// RefreshAuthExecutor parks the SUBJECT row at waiting_for_credential_reauth with
// end_time = NULL (executors.go). That is a pipeline sitting still waiting for a
// human, not a recovery; crediting it would record "fixed" for something nobody
// has fixed yet. An open row decides nothing, whatever its status.
func TestVerifySubjectRowReopenedDecidesNothing(t *testing.T) {
	for _, status := range []string{"waiting_for_credential_reauth", "running", "streaming", "pending"} {
		t.Run(status, func(t *testing.T) {
			v, mock, done := newVerifier(t)
			defer done()

			a := attemptAged(10 * time.Minute)
			noNewerAttempts(mock)
			mock.ExpectQuery(successorQuery).WillReturnError(sql.ErrNoRows)
			subjectRow(mock, status, nil) // end_time NULL

			verdict, _, decided := v.Verify(context.Background(), a)
			if decided {
				t.Fatalf("Verify = (%q, true) for an open row in %q — an open row is not "+
					"evidence of anything yet", verdict, status)
			}
		})
	}
}

// An attempt with no execution_id has no subject row to consult; skip straight to
// the timeout rather than issuing a query with an empty uuid (which errors).
func TestVerifySubjectSkippedWhenAttemptHasNoExecution(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	a := attemptAged(VerifyNoSuccessorTimeout + time.Minute)
	a.ExecutionID = ""
	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnError(sql.ErrNoRows)

	verdict, _, decided := v.Verify(context.Background(), a)
	if !decided || verdict != VerdictInconclusive {
		t.Fatalf("Verify = (%q, %v), want (inconclusive, true)", verdict, decided)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A subject-row lookup that errors must degrade to the old behaviour, not decide.
func TestVerifySubjectLookupFailureFallsThrough(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	a := attemptAged(VerifyNoSuccessorTimeout + time.Minute)
	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(subjectQuery).WillReturnError(errors.New("connection reset"))

	verdict, _, decided := v.Verify(context.Background(), a)
	if !decided || verdict != VerdictInconclusive {
		t.Fatalf("Verify = (%q, %v), want (inconclusive, true) — a lookup failure must "+
			"fall back to the timeout, not swallow the verdict", verdict, decided)
	}
}

// A successor run, when one exists, still wins: the in-place path is a fallback
// for the no-successor case only and must never pre-empt real successor evidence.
func TestVerifySuccessorTakesPrecedenceOverSubjectRow(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	noNewerAttempts(mock)
	mock.ExpectQuery(successorQuery).WillReturnRows(
		sqlmock.NewRows([]string{"id", "status"}).AddRow("e2", "completed"))
	// No subject-row expectation: reaching it at all would be the bug.

	verdict, succ, decided := v.Verify(context.Background(), attemptAged(10*time.Minute))
	if !decided || verdict != VerdictHealed || succ != "e2" {
		t.Fatalf("Verify = (%q, %q, %v), want (healed, e2, true)", verdict, succ, decided)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
