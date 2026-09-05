package heal

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

const (
	// The two heal_attempts reads are distinguished by their SELECT list.
	streakQuery = `SELECT verdict`
	recallQuery = `SELECT action, COUNT`
)

func newAttemptStore(t *testing.T) (*AttemptStore, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	return &AttemptStore{DB: db}, mock, func() { _ = db.Close() }
}

func verdictRows(verdicts ...string) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"verdict"})
	for _, v := range verdicts {
		r.AddRow(v)
	}
	return r
}

// The rule the existing retry cap cannot express: the cap bounds retry VOLUME while
// staying blind to whether retries accomplish anything. Two verified failures of the
// same action on the same signature is evidence the action is wrong, not unlucky.
func TestApplyMemoryGivesUpAfterConsecutiveFailures(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	mock.ExpectQuery(streakQuery).WillReturnRows(verdictRows("failed_again", "failed_again"))

	in := diagnose.Diagnosis{
		Category:        diagnose.CategoryNetwork,
		SuggestedAction: diagnose.ActionBackoffRetry,
		Confidence:      0.95,
		Rationale:       "transient network error",
	}
	got, note := store.ApplyMemory(context.Background(), "sig-a", in)

	if got.SuggestedAction != diagnose.ActionEscalate {
		t.Errorf("action = %q, want %q", got.SuggestedAction, diagnose.ActionEscalate)
	}
	if got.Confidence != 0.3 {
		t.Errorf("confidence = %v, want 0.3 (escalation is recoverable; a retry loop is not)", got.Confidence)
	}
	if note == "" || !strings.Contains(note, "in a row") {
		t.Errorf("note = %q, want an explanation of the streak", note)
	}
	// The original rationale must survive — the operator needs both the rules'
	// reasoning and the reason it was overridden.
	if !strings.Contains(got.Rationale, "transient network error") {
		t.Errorf("rationale = %q, want the original reasoning preserved", got.Rationale)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// One failure is not a pattern. Giving up here would strand recoverable incidents.
func TestApplyMemoryDoesNotGiveUpOnASingleFailure(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	mock.ExpectQuery(streakQuery).WillReturnRows(verdictRows("failed_again"))
	mock.ExpectQuery(recallQuery).WillReturnError(sql.ErrNoRows)

	in := diagnose.Diagnosis{SuggestedAction: diagnose.ActionBackoffRetry, Confidence: 0.9}
	got, note := store.ApplyMemory(context.Background(), "sig-a", in)

	if got.SuggestedAction != diagnose.ActionBackoffRetry {
		t.Errorf("action = %q, want the rules' action left alone", got.SuggestedAction)
	}
	if note != "" {
		t.Errorf("note = %q, want empty when memory has nothing to say", note)
	}
}

// A heal in the middle of the history breaks the streak: the action demonstrably
// works sometimes, so the healer must not conclude it never does.
func TestApplyMemoryStreakStopsAtFirstNonFailure(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	// Newest first: one failure, then a heal, then older failures that must not count.
	mock.ExpectQuery(streakQuery).WillReturnRows(
		verdictRows("failed_again", "healed", "failed_again", "failed_again"))
	mock.ExpectQuery(recallQuery).WillReturnError(sql.ErrNoRows)

	in := diagnose.Diagnosis{SuggestedAction: diagnose.ActionBackoffRetry, Confidence: 0.9}
	got, _ := store.ApplyMemory(context.Background(), "sig-a", in)
	if got.SuggestedAction != diagnose.ActionEscalate && got.SuggestedAction != diagnose.ActionBackoffRetry {
		t.Fatalf("unexpected action %q", got.SuggestedAction)
	}
	if got.SuggestedAction == diagnose.ActionEscalate {
		t.Error("streak counted past a 'healed' verdict — the streak must be CONSECUTIVE failures")
	}
}

// The streak is CONSECUTIVE: it stops at the first verdict that is not a failure.
// 'inconclusive' is the normal outcome of an escalation or a HITL prompt, so it must
// break the run rather than extend it.
func TestSignatureFailureStreakCountsOnlyLeadingFailures(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	mock.ExpectQuery(streakQuery).WillReturnRows(
		verdictRows("failed_again", "failed_again", "inconclusive", "failed_again"))

	if got := store.SignatureFailureStreak(context.Background(), "sig-a"); got != 2 {
		t.Errorf("streak = %d, want 2 (stops at the first non-failure)", got)
	}
}

// The regression that made give-up unreachable in production.
//
// Verify runs on a 5-minute tick; a failing pipeline earns its next heal attempt in ~3
// minutes. So attempt N is almost always superseded by N+1 before the verifier looks at
// it, and a long chain of identical failures records ONLY 'superseded'. The streak query
// used to exclude that verdict, so the chain scored 1 against giveUpStreak=2 and the
// healer would repeat a losing action indefinitely — the exact behaviour the ledger was
// built to end.
//
// A newer attempt exists only because the pipeline failed the SAME way again, so
// supersession is evidence the action did not hold.
func TestSupersededChainStillReachesGiveUp(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	// A realistic fast-failure chain: nothing was ever slow enough to be graded.
	mock.ExpectQuery(streakQuery).WillReturnRows(
		verdictRows("superseded", "superseded", "superseded"))

	if got := store.SignatureFailureStreak(context.Background(), "sig-a"); got < giveUpStreak {
		t.Fatalf("streak = %d, want >= %d — a supersede chain must reach the give-up threshold",
			got, giveUpStreak)
	}
}

// ...and the streak must not care whether the chain is graded, superseded, or mixed.
func TestSignatureFailureStreakCountsMixedFailureAndSupersede(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	mock.ExpectQuery(streakQuery).WillReturnRows(
		verdictRows("superseded", "failed_again", "superseded", "healed", "failed_again"))

	if got := store.SignatureFailureStreak(context.Background(), "sig-a"); got != 3 {
		t.Errorf("streak = %d, want 3 (three failures, then a 'healed' that breaks it)", got)
	}
}

// The streak must not be reachable by verdicts that mean "we could not tell" or
// "it worked". Table-driven so a verdict added later fails here until someone decides.
func TestOnlyFailureVerdictsAdvanceTheStreak(t *testing.T) {
	cases := map[Verdict]bool{
		VerdictFailedAgain:  true,
		VerdictSuperseded:   true,
		VerdictHealed:       false,
		VerdictInconclusive: false,
		Verdict("something_nobody_has_written_yet"): false,
	}
	for v, want := range cases {
		if got := countsAsStreakFailure(v); got != want {
			t.Errorf("countsAsStreakFailure(%q) = %v, want %v", v, got, want)
		}
	}
}

// The give-up rule must actually fire end-to-end off a supersede-only chain, not just
// count correctly in isolation.
func TestApplyMemoryGivesUpOnSupersededChain(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	mock.ExpectQuery(streakQuery).WillReturnRows(verdictRows("superseded", "superseded"))

	in := diagnose.Diagnosis{SuggestedAction: diagnose.ActionBackoffRetry, Confidence: 0.9}
	got, note := store.ApplyMemory(context.Background(), "sig-a", in)

	if got.SuggestedAction != diagnose.ActionEscalate {
		t.Errorf("action = %q, want %q — two supersessions is a losing move",
			got.SuggestedAction, diagnose.ActionEscalate)
	}
	if got.Confidence != 0.3 {
		t.Errorf("confidence = %v, want 0.3", got.Confidence)
	}
	if note == "" {
		t.Error("give-up produced no decision note")
	}
}

// Memory's job is to change WHICH action is taken. It must not change how much
// autonomy the healer is granted to take it — an inflated confidence would promote a
// remembered action into a decision band the rules engine never earned.
func TestApplyMemoryRecallDoesNotInflateConfidence(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	mock.ExpectQuery(streakQuery).WillReturnRows(verdictRows())
	mock.ExpectQuery(recallQuery).WillReturnRows(
		sqlmock.NewRows([]string{"action", "healed"}).
			AddRow(string(diagnose.ActionRefreshAuth), 4))

	in := diagnose.Diagnosis{SuggestedAction: diagnose.ActionBackoffRetry, Confidence: 0.62}
	got, note := store.ApplyMemory(context.Background(), "sig-a", in)

	if got.SuggestedAction != diagnose.ActionRefreshAuth {
		t.Errorf("action = %q, want the remembered %q", got.SuggestedAction, diagnose.ActionRefreshAuth)
	}
	if got.Confidence != 0.62 {
		t.Errorf("confidence = %v, want it unchanged at 0.62", got.Confidence)
	}
	if !strings.Contains(note, "healed this failure signature 4 times") {
		t.Errorf("note = %q, want the evidence count", note)
	}
}

// A single success is indistinguishable from a coincidence — an unrelated retry, a
// dependency recovering on its own. Promoting it would launder luck into policy.
func TestApplyMemoryRecallRequiresTwoConfirmedHeals(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	mock.ExpectQuery(streakQuery).WillReturnRows(verdictRows())
	mock.ExpectQuery(recallQuery).WillReturnRows(
		sqlmock.NewRows([]string{"action", "healed"}).
			AddRow(string(diagnose.ActionRefreshAuth), 1))

	in := diagnose.Diagnosis{SuggestedAction: diagnose.ActionBackoffRetry, Confidence: 0.9}
	got, note := store.ApplyMemory(context.Background(), "sig-a", in)

	if got.SuggestedAction != diagnose.ActionBackoffRetry {
		t.Errorf("action = %q, want the rules' action on a single unconfirmed heal", got.SuggestedAction)
	}
	if note != "" {
		t.Errorf("note = %q, want empty", note)
	}
}

// When memory agrees with the rules there is nothing to report; a note here would be
// noise in every decision event.
func TestApplyMemoryQuietWhenRecallAgreesWithRules(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	mock.ExpectQuery(streakQuery).WillReturnRows(verdictRows())
	mock.ExpectQuery(recallQuery).WillReturnRows(
		sqlmock.NewRows([]string{"action", "healed"}).
			AddRow(string(diagnose.ActionBackoffRetry), 5))

	in := diagnose.Diagnosis{SuggestedAction: diagnose.ActionBackoffRetry, Confidence: 0.9}
	got, note := store.ApplyMemory(context.Background(), "sig-a", in)
	if got.SuggestedAction != diagnose.ActionBackoffRetry || note != "" {
		t.Errorf("action=%q note=%q, want the diagnosis untouched and no note", got.SuggestedAction, note)
	}
}

// Already escalating on a losing streak: nothing to change, and re-stamping
// confidence at 0.3 would silently demote a high-confidence escalation.
func TestApplyMemoryLeavesAnExistingEscalationAlone(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	mock.ExpectQuery(streakQuery).WillReturnRows(verdictRows("failed_again", "failed_again"))

	in := diagnose.Diagnosis{SuggestedAction: diagnose.ActionEscalate, Confidence: 0.8, Rationale: "cdc provisioning"}
	got, note := store.ApplyMemory(context.Background(), "sig-a", in)

	if got != in {
		t.Errorf("diagnosis = %+v, want it returned unchanged", got)
	}
	if note != "" {
		t.Errorf("note = %q, want empty", note)
	}
}

// The ledger is an improvement to decision quality, not a precondition for healing.
// A missing table (migration 081 not yet applied) must leave the healer working
// exactly as it did before.
func TestApplyMemoryFailsSoftWhenLedgerIsUnavailable(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	boom := errors.New(`pq: relation "heal_attempts" does not exist`)
	mock.ExpectQuery(streakQuery).WillReturnError(boom)
	mock.ExpectQuery(recallQuery).WillReturnError(boom)

	in := diagnose.Diagnosis{SuggestedAction: diagnose.ActionBackoffRetry, Confidence: 0.91, Rationale: "r"}
	got, note := store.ApplyMemory(context.Background(), "sig-a", in)

	if got != in {
		t.Errorf("diagnosis = %+v, want it returned unchanged on ledger failure", got)
	}
	if note != "" {
		t.Errorf("note = %q, want empty", note)
	}
}

func TestApplyMemoryNoOpWithoutStoreOrSignature(t *testing.T) {
	in := diagnose.Diagnosis{SuggestedAction: diagnose.ActionBackoffRetry, Confidence: 0.9}

	var nilStore *AttemptStore
	if got, note := nilStore.ApplyMemory(context.Background(), "sig-a", in); got != in || note != "" {
		t.Errorf("nil store: got %+v note %q, want the diagnosis unchanged", got, note)
	}

	store, _, done := newAttemptStore(t)
	defer done()
	// An empty signature means the incident has no identity to recall against;
	// querying on "" would match every other identity-less row.
	if got, note := store.ApplyMemory(context.Background(), "", in); got != in || note != "" {
		t.Errorf("empty signature: got %+v note %q, want the diagnosis unchanged", got, note)
	}
}

func TestRecordReturnsInsertedID(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	mock.ExpectQuery(`INSERT INTO heal_attempts`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(77)))

	got := store.Record(context.Background(),
		diagnose.Signal{PipelineID: "p1", ExecutionID: "e1"},
		diagnose.Diagnosis{Category: diagnose.CategoryNetwork, SuggestedAction: diagnose.ActionBackoffRetry, Confidence: 0.9},
		HealResult{Outcome: OutcomeAutoExecuted, ActionExecuted: true},
		"sig-a")

	if got != 77 {
		t.Errorf("Record = %d, want 77", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// Escalations and failures are the most important rows in the ledger: a signature
// that only ever escalates is the pattern an operator most needs to see. Recording
// only successful auto-executions would make it invisible.
func TestRecordPersistsNonExecutedOutcomes(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	for _, outcome := range []Outcome{OutcomeEscalated, OutcomeHITLRequested, OutcomeActionFailed, OutcomeNoActionDefined} {
		mock.ExpectQuery(`INSERT INTO heal_attempts`).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)))
		if got := store.Record(context.Background(),
			diagnose.Signal{PipelineID: "p1"},
			diagnose.Diagnosis{SuggestedAction: diagnose.ActionEscalate},
			HealResult{Outcome: outcome, Error: errors.New("boom")},
			"sig-a"); got == 0 {
			t.Errorf("outcome %q was not recorded", outcome)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// heal_attempts.pipeline_id is NOT NULL with an FK; recording without one would
// throw on every sweep of a signal we cannot attribute.
func TestRecordSkipsSignalWithoutPipeline(t *testing.T) {
	store, _, done := newAttemptStore(t)
	defer done()

	// No INSERT is expected on the mock, so if Record reached the database at all it
	// would get sqlmock's "call was not expected" error — which it reports the same
	// way as a real insert failure, as 0.
	if got := store.Record(context.Background(), diagnose.Signal{},
		diagnose.Diagnosis{}, HealResult{}, "sig-a"); got != 0 {
		t.Errorf("Record = %d, want 0", got)
	}
}

func TestRecordFailsSoftOnInsertError(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	mock.ExpectQuery(`INSERT INTO heal_attempts`).
		WillReturnError(errors.New(`pq: relation "heal_attempts" does not exist`))

	if got := store.Record(context.Background(),
		diagnose.Signal{PipelineID: "p1"},
		diagnose.Diagnosis{SuggestedAction: diagnose.ActionEscalate},
		HealResult{Outcome: OutcomeEscalated}, "sig-a"); got != 0 {
		t.Errorf("Record = %d, want 0 on insert failure", got)
	}
}

func TestRecallBestActionReturnsNoOpinionWhenEmpty(t *testing.T) {
	store, mock, done := newAttemptStore(t)
	defer done()

	mock.ExpectQuery(recallQuery).WillReturnError(sql.ErrNoRows)

	action, n := store.RecallBestAction(context.Background(), "sig-a")
	if action != "" || n != 0 {
		t.Errorf("RecallBestAction = (%q, %d), want no opinion", action, n)
	}
}
