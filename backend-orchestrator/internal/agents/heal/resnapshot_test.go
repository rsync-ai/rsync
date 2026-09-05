package heal

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// Pins KI-HEAL-RESNAPSHOT-HAS-NO-EXECUTOR.
//
// diagnose.go emits ActionReSnapshot at confidence 0.8 for the one failure class
// a retry provably cannot fix: the CDC stream position is GONE. A MongoDB resume
// token that aged out of the oplog, an Oracle SCN past redo/archive retention.
// The diagnoser gets this exactly right, and then nothing is registered to
// receive it — NewHealWorkerWithHooks registers seven executors and none of them
// claims that action.
//
// So Heal falls through its registry lookup and returns OutcomeNoActionDefined
// with `no executor registered for action "re_snapshot"`. That string is the
// healer telling an operator about its own wiring. The pipeline is stopped, the
// cause is known and named, the remedy is known — and what surfaces is a
// developer's error message with no next step in it.
//
// Registering an executor changes that to OutcomeHITLRequested carrying the
// diagnosis, which is what the self-healing panel renders.

// resnapshotDiagnosis is the diagnosis diagnose.go actually produces for this
// class, copied field-for-field so these tests break if it drifts.
func resnapshotDiagnosis() diagnose.Diagnosis {
	return diagnose.Diagnosis{
		Category:        diagnose.CategoryUserConfig,
		SuggestedAction: diagnose.ActionReSnapshot,
		Confidence:      0.8,
		Rationale:       "CDC stream position is no longer available",
	}
}

func TestReSnapshotReachesAnOperatorInsteadOfAWiringError(t *testing.T) {
	// The production registry, built the way main.go builds it.
	w := NewHealWorker(nil, "")

	res := w.Healer.Heal(context.Background(),
		diagnose.Signal{PipelineID: "p1", SourceType: "mongodb"},
		resnapshotDiagnosis())

	if res.Outcome == OutcomeNoActionDefined {
		t.Fatalf("ActionReSnapshot still has no executor: outcome=%q err=%v\n\n"+
			"The diagnoser identified an unrecoverable CDC position and the healer "+
			"answered with a message about its own registry.", res.Outcome, res.Error)
	}
	if res.Outcome != OutcomeHITLRequested {
		t.Fatalf("outcome = %q, want %q — a re-snapshot re-reads the entire source, so "+
			"it must be offered to a human, never applied silently",
			res.Outcome, OutcomeHITLRequested)
	}
	if res.HITLPrompt == "" {
		t.Error("HITLPrompt is empty — the panel would render an empty recommendation")
	}
	if res.ActionExecuted {
		t.Error("ActionExecuted is true — the healer re-snapshotted without approval")
	}
}

// A re-snapshot re-reads the whole source. It is in the same class as
// BackoffRetry starting a run, and must never fire unattended.
func TestReSnapshotIsNotHITLSafe(t *testing.T) {
	if (&ReSnapshotExecutor{}).HITLSafe() {
		t.Fatal("ReSnapshotExecutor claims to be HITL-safe; Heal would then run it " +
			"unattended anywhere in the 0.50–0.85 band")
	}
}

// The HITLSafe guard only covers the middle band — Heal's `Confidence >= AutoBand`
// branch calls Run without consulting it. So the property that actually keeps a
// full re-snapshot off the unattended path is that no rule emits this action at
// or above the band. Asserted against the real diagnoser, over the error strings
// that route here, because that is where it is decided.
func TestNoRuleEmitsReSnapshotInsideTheAutoBand(t *testing.T) {
	d := diagnose.New()

	// Every one of these must reach re_snapshot. Counted rather than assumed:
	// the loop skips actions that route elsewhere, so without this the test
	// would pass trivially if the rule stopped matching altogether.
	matched := 0

	for _, errText := range []string{
		"resume of change stream was not possible, as the resume point may no longer be in the oplog",
		"change stream history lost",
		"InvalidResumeToken: cannot resume stream",
		"ORA-01555: snapshot too old: rollback segment number 3 too small",
		"the starting SCN is no longer available in the archive logs",
	} {
		dx := d.Diagnose(diagnose.Signal{PipelineID: "p1", ErrorMessage: errText})
		if dx.SuggestedAction != diagnose.ActionReSnapshot {
			// InvalidResumeToken used to land here: the pattern list carried
			// "invalidateresumetoken", which matches nothing MongoDB emits, so a
			// lost-position failure fell through to escalate/0.3 — diagnosed as
			// "uncertain" when the cause was known exactly.
			t.Errorf("%q → %s at %v, want re_snapshot — a lost stream position is not "+
				"something a retry or an operator can resolve",
				errText, dx.SuggestedAction, dx.Confidence)
			continue
		}
		matched++
		if dx.Confidence >= AutoBand {
			t.Errorf("%q → re_snapshot at confidence %v, at or above AutoBand %v — Heal's "+
				"auto branch does not check HITLSafe, so this would re-snapshot the "+
				"entire source with nobody asked", errText, dx.Confidence, AutoBand)
		}
	}

	if matched == 0 {
		t.Fatal("no error string routed to re_snapshot — the band assertion above " +
			"asserted nothing")
	}
}

// Run is reached once a human approves (Healer.ApproveHITL). It must go through
// the guarded recovery endpoint rather than re-implementing re-provisioning:
// that endpoint already enforces the CDC_RECOVERY_ENABLED flag, the
// connector-must-be-FAILED precondition, and the workspace-role check.
func TestReSnapshotRunGoesThroughTheGuardedRecoveryEndpoint(t *testing.T) {
	var gotPath, gotMethod, gotMode string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		body, _ := io.ReadAll(r.Body)
		var parsed map[string]interface{}
		_ = json.Unmarshal(body, &parsed)
		gotMode, _ = parsed["snapshot_mode"].(string)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer srv.Close()

	e := &ReSnapshotExecutor{OrchestratorURL: srv.URL}
	if err := e.Run(context.Background(), diagnose.Signal{PipelineID: "p1"}); err != nil {
		t.Fatalf("Run returned an error against a healthy endpoint: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if !strings.Contains(gotPath, "/cdc/pipelines/p1/recover") {
		t.Errorf("path = %q, want the guarded recovery route for pipeline p1", gotPath)
	}
	// "recovery" is the mode that re-establishes a position without a full
	// re-read of history; "initial" would re-snapshot everything.
	if gotMode != "recovery" {
		t.Errorf("snapshot_mode = %q, want %q", gotMode, "recovery")
	}
}

// The recovery endpoint is flag-gated and returns 403 when CDC_RECOVERY_ENABLED
// is not set — which is its state on production. That has to surface as a
// failure, not as a re-snapshot that happened.
func TestReSnapshotRunSurfacesARefusalFromTheEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"success":false,"error":"cdc_recovery_disabled"}`))
	}))
	defer srv.Close()

	e := &ReSnapshotExecutor{OrchestratorURL: srv.URL}
	err := e.Run(context.Background(), diagnose.Signal{PipelineID: "p1"})
	if err == nil {
		t.Fatal("a 403 from the recovery endpoint was reported as a successful re-snapshot")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Errorf("the refusal did not survive into the error: %v", err)
	}
}

func TestReSnapshotRequiresAPipelineID(t *testing.T) {
	if err := (&ReSnapshotExecutor{}).Run(context.Background(), diagnose.Signal{}); err == nil {
		t.Fatal("expected an error when PipelineID is empty")
	}
}
