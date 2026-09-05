package heal

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// capturePayload runs writeDecisionEvent against a mocked DB and hands back the
// decoded JSONB payload it tried to insert. Everything in this file asserts on
// that map, because that map IS the healer's contract with the UI: the
// self-healing panel renders nothing it cannot find in here.
func capturePayload(
	t *testing.T,
	sig diagnose.Signal, dx diagnose.Diagnosis, result HealResult,
	signature, memoryNote string, attemptID int64,
) map[string]interface{} {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	var raw []byte
	mock.ExpectExec("INSERT INTO pipeline_run_events").
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			payloadCaptor{dst: &raw},
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := &HealWorker{DB: db}
	w.writeDecisionEvent(context.Background(), sig, dx, result, signature, memoryNote, attemptID)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("writeDecisionEvent did not insert the event: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("decision event was inserted with an empty payload")
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("payload is not valid JSON (%v): %s", err, raw)
	}
	return payload
}

// payloadCaptor matches any argument and keeps a copy of it. sqlmock compares
// []byte args by value, which would force every test to spell out the exact
// marshalled JSON — brittle for a map whose key order is not stable.
type payloadCaptor struct{ dst *[]byte }

func (p payloadCaptor) Match(v driver.Value) bool {
	switch b := v.(type) {
	case []byte:
		*p.dst = append([]byte(nil), b...)
	case string:
		*p.dst = []byte(b)
	}
	return true
}

func baseInputs() (diagnose.Signal, diagnose.Diagnosis, HealResult) {
	sig := diagnose.Signal{
		PipelineID:     "11111111-1111-1111-1111-111111111111",
		ExecutionID:    "22222222-2222-2222-2222-222222222222",
		ExecutorStatus: "workflow_gone",
		ErrorMessage:   "pipeline run is no longer active (workflow not found)",
	}
	dx := diagnose.Diagnosis{
		Category:        diagnose.CategoryOrchestration,
		SuggestedAction: diagnose.ActionBackoffRetry,
		Confidence:      0.8,
		Rationale:       "the workflow backing this run is gone",
	}
	result := HealResult{
		Outcome:    OutcomeHITLRequested,
		HITLPrompt: "Re-run this pipeline?",
	}
	return sig, dx, result
}

// TestDecisionEvent_CarriesTheFailureItReactedTo is the fix for the gap the
// production pass found: eight healer_decision rows, every one of them saying
// "no rule matched", none of them saying what had failed to match. The verdict
// without the evidence is unactionable — an operator cannot tell a correct
// escalation from a diagnoser blind spot.
func TestDecisionEvent_CarriesTheFailureItReactedTo(t *testing.T) {
	sig, dx, result := baseInputs()
	payload := capturePayload(t, sig, dx, result, "sig-abc", "", 42)

	for _, want := range []struct{ key, val, why string }{
		{"error_message", sig.ErrorMessage,
			"the raw failure text; without it a wrong classification is invisible from the UI"},
		{"failure_signature", "sig-abc",
			"the normalised identity that groups repeat occurrences of the same failure"},
		{"executor_status", "workflow_gone",
			"several rules key off status rather than error text — the UI must show which input matched"},
	} {
		got, ok := payload[want.key]
		if !ok {
			t.Errorf("payload is missing %q — %s", want.key, want.why)
			continue
		}
		if got != want.val {
			t.Errorf("payload[%q] = %v, want %q — %s", want.key, got, want.val, want.why)
		}
	}
}

// TestDecisionEvent_CarriesTheAttemptIDThatJoinsItToItsVerdict pins the one
// field that makes the timeline coherent. The verify loop writes healer_verified
// minutes later carrying attempt_id; without the same id on the decision the UI
// shows "the healer decided to retry" and, separately and unrelatedly, "an
// attempt failed again".
func TestDecisionEvent_CarriesTheAttemptIDThatJoinsItToItsVerdict(t *testing.T) {
	sig, dx, result := baseInputs()
	payload := capturePayload(t, sig, dx, result, "sig-abc", "", 4242)

	got, ok := payload["attempt_id"]
	if !ok {
		t.Fatal("payload is missing attempt_id — nothing joins this decision to its verdict")
	}
	// JSON numbers decode as float64.
	if n, _ := got.(float64); int64(n) != 4242 {
		t.Fatalf("payload[attempt_id] = %v, want 4242", got)
	}
}

// TestSweep_RecordsTheAttemptBeforeWritingTheEvent guards the ordering that
// makes the previous test mean anything at runtime.
//
// writeDecisionEvent can only carry a real attempt id if Attempts.Record has
// already returned one. Swap the two lines back and every decision event on
// production silently carries attempt_id: 0 — still valid JSON, still a green
// build, and the join to the verdict is gone again. Asserted on the source
// because the alternative is standing up the whole sweep loop to observe two
// calls happening in order.
func TestSweep_RecordsTheAttemptBeforeWritingTheEvent(t *testing.T) {
	src, err := os.ReadFile("worker.go")
	if err != nil {
		t.Fatalf("read worker.go: %v", err)
	}
	body := string(src)

	rec := strings.Index(body, "w.Attempts.Record(ctx, sig, dx, result, signature)")
	if rec < 0 {
		t.Fatal("sweep no longer calls Attempts.Record — the heal ledger is not being written")
	}
	evt := strings.Index(body, "w.writeDecisionEvent(ctx, sig, dx, result, signature")
	if evt < 0 {
		t.Fatal("sweep no longer calls writeDecisionEvent — the UI loses the healer timeline")
	}
	if rec > evt {
		t.Fatal("writeDecisionEvent now runs before Attempts.Record, so it can only pass " +
			"attempt_id: 0 — the decision event no longer joins to its healer_verified verdict")
	}
}

// TestDecisionEvent_MemoryNoteExplainsAConfidenceThatDisagreesWithTheRule.
// ApplyMemory downgrades an action the ledger says has already failed against
// this signature, which makes the recorded confidence differ from the rule's
// own. Without the note the UI would show a number no rule in the codebase
// produces.
func TestDecisionEvent_MemoryNoteExplainsAConfidenceThatDisagreesWithTheRule(t *testing.T) {
	sig, dx, result := baseInputs()
	payload := capturePayload(t, sig, dx, result, "sig-abc", "tried twice, failed twice", 1)

	if payload["memory_note"] != "tried twice, failed twice" {
		t.Fatalf("payload[memory_note] = %v, want the ledger's note", payload["memory_note"])
	}
}

// TestDecisionEvent_KeepsTheExecutorErrorSeparateFromTheDiagnosedOne.
// `error` is the executor's own failure and is empty for every outcome except
// action_failed; `error_message` is the failure being diagnosed and is present
// on all of them. Collapsing the two would make an escalation look like a
// crashed executor.
func TestDecisionEvent_KeepsTheExecutorErrorSeparateFromTheDiagnosedOne(t *testing.T) {
	sig, dx, result := baseInputs()

	escalated := capturePayload(t, sig, dx, HealResult{Outcome: OutcomeEscalated}, "s", "", 1)
	if escalated["error"] != "" {
		t.Errorf("escalation recorded error=%v; no executor ran, so it has no error",
			escalated["error"])
	}
	if escalated["error_message"] != sig.ErrorMessage {
		t.Errorf("escalation lost the failure it escalated: %v", escalated["error_message"])
	}

	result.Outcome = OutcomeActionFailed
	result.Error = errors.New("retry endpoint returned 503")
	failed := capturePayload(t, sig, dx, result, "s", "", 1)
	if failed["error"] != "retry endpoint returned 503" {
		t.Errorf("action_failed dropped the executor error: %v", failed["error"])
	}
	if failed["error_message"] != sig.ErrorMessage {
		t.Errorf("action_failed lost the original failure: %v", failed["error_message"])
	}
}

// TestDecisionEvent_TruncatesAnEnormousError. The run-detail page fetches these
// 200 at a time; an unbounded stack trace in each is a page-weight problem. The
// distinguishing part of an error is always at the front.
func TestDecisionEvent_TruncatesAnEnormousError(t *testing.T) {
	sig, dx, result := baseInputs()
	sig.ErrorMessage = "head:" + strings.Repeat("x", 50_000)

	payload := capturePayload(t, sig, dx, result, "s", "", 1)
	got, _ := payload["error_message"].(string)

	if len([]rune(got)) > maxEventErrorLen+1 {
		t.Fatalf("error_message is %d runes; expected it capped near %d",
			len([]rune(got)), maxEventErrorLen)
	}
	if !strings.HasPrefix(got, "head:") {
		t.Fatalf("truncation dropped the front of the error, which is the useful half: %q", got)
	}
	if !strings.HasSuffix(got, "…") {
		t.Fatal("truncated error is not marked as truncated, so it reads as the whole message")
	}
}

// TestTruncateForEvent_LeavesNormalErrorsAlone — the cap must not add an
// ellipsis to messages that were never truncated.
func TestTruncateForEvent_LeavesNormalErrorsAlone(t *testing.T) {
	const msg = "connection refused"
	if got := truncateForEvent(msg); got != msg {
		t.Fatalf("truncateForEvent(%q) = %q, want it unchanged", msg, got)
	}
	if got := truncateForEvent(""); got != "" {
		t.Fatalf("truncateForEvent(\"\") = %q, want empty", got)
	}
}

// TestDecisionEvent_StillRecordsOutcomesWhereNothingExecuted. Escalations and
// no-ops are the outcomes an operator most needs to see, because they are the
// ones where the healer decided NOT to act.
func TestDecisionEvent_StillRecordsOutcomesWhereNothingExecuted(t *testing.T) {
	sig, dx, _ := baseInputs()

	for _, outcome := range []Outcome{OutcomeEscalated, OutcomeNoActionDefined, OutcomeHITLRequested} {
		payload := capturePayload(t, sig, dx,
			HealResult{Outcome: outcome, ActionExecuted: false}, "s", "", 1)

		if payload["outcome"] != string(outcome) {
			t.Errorf("outcome %s was recorded as %v", outcome, payload["outcome"])
		}
		if payload["action_executed"] != false {
			t.Errorf("outcome %s recorded action_executed=%v, want false",
				outcome, payload["action_executed"])
		}
		if payload["error_message"] != sig.ErrorMessage {
			t.Errorf("outcome %s lost the failure text", outcome)
		}
	}
}

// TestDecisionEvent_NoDBIsSafe — the worker runs with a nil DB in tests and in
// any deployment where the ledger is not wired.
func TestDecisionEvent_NoDBIsSafe(t *testing.T) {
	sig, dx, result := baseInputs()
	w := &HealWorker{}
	w.writeDecisionEvent(context.Background(), sig, dx, result, "s", "", 1)
}

// TestDecisionEvent_SkipsSignalsWithoutAPipeline — pipeline_id is NOT NULL and
// is the only column the UI can query these by.
func TestDecisionEvent_SkipsSignalsWithoutAPipeline(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	sig, dx, result := baseInputs()
	sig.PipelineID = ""

	w := &HealWorker{DB: db}
	w.writeDecisionEvent(context.Background(), sig, dx, result, "s", "", 1)

	// No ExpectExec was registered; any insert would be an unexpected call.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected no insert for a pipeline-less signal: %v", err)
	}
}
