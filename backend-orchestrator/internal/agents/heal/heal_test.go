package heal

import (
	"context"
	"errors"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// fakeExecutor lets tests assert which action got dispatched and
// simulate failure/success without touching real systems.
type fakeExecutor struct {
	action    diagnose.Action
	calls     int
	err       error
	hitlSafe  bool
}

func (f *fakeExecutor) Action() diagnose.Action { return f.action }
func (f *fakeExecutor) HITLSafe() bool           { return f.hitlSafe }
func (f *fakeExecutor) Run(_ context.Context, _ diagnose.Signal) error {
	f.calls++
	return f.err
}

func TestHeal_HighConfidence_AutoExecutes(t *testing.T) {
	h := New()
	exec := &fakeExecutor{action: diagnose.ActionRefreshAuth}
	h.Register(exec)

	res := h.Heal(context.Background(), diagnose.Signal{}, diagnose.Diagnosis{
		Category:        diagnose.CategoryAuthExpired,
		SuggestedAction: diagnose.ActionRefreshAuth,
		Confidence:      0.95,
	})
	if res.Outcome != OutcomeAutoExecuted {
		t.Errorf("outcome: want auto_executed, got %s", res.Outcome)
	}
	if !res.ActionExecuted {
		t.Error("ActionExecuted should be true")
	}
	if exec.calls != 1 {
		t.Errorf("executor should be called once, got %d", exec.calls)
	}
}

func TestHeal_MediumConfidence_AsksFirst(t *testing.T) {
	h := New()
	exec := &fakeExecutor{action: diagnose.ActionRegenerateConnector}
	h.Register(exec)

	res := h.Heal(context.Background(), diagnose.Signal{}, diagnose.Diagnosis{
		Category:        diagnose.CategoryConnectorBug,
		SuggestedAction: diagnose.ActionRegenerateConnector,
		Confidence:      0.70,
		Rationale:       "source has rows but destination wrote zero",
	})
	if res.Outcome != OutcomeHITLRequested {
		t.Errorf("outcome: want hitl_requested, got %s", res.Outcome)
	}
	if res.ActionExecuted {
		t.Error("ActionExecuted must be false until user approves")
	}
	if exec.calls != 0 {
		t.Errorf("executor should NOT be called, got %d calls", exec.calls)
	}
	if res.HITLPrompt == "" {
		t.Error("HITLPrompt must be populated")
	}
}

func TestHeal_LowConfidence_Escalates(t *testing.T) {
	h := New()
	exec := &fakeExecutor{action: diagnose.ActionBackoffRetry}
	h.Register(exec)

	res := h.Heal(context.Background(), diagnose.Signal{}, diagnose.Diagnosis{
		Category:        diagnose.CategoryUnknown,
		SuggestedAction: diagnose.ActionBackoffRetry,
		Confidence:      0.3,
	})
	if res.Outcome != OutcomeEscalated {
		t.Errorf("outcome: want escalated, got %s", res.Outcome)
	}
	if exec.calls != 0 {
		t.Error("executor must not be called on low confidence")
	}
}

func TestHeal_ExplicitEscalateAction_AlwaysEscalates(t *testing.T) {
	// Even at high confidence, ActionEscalate skips dispatch.
	h := New()
	res := h.Heal(context.Background(), diagnose.Signal{}, diagnose.Diagnosis{
		Category:        diagnose.CategoryDestCapacity,
		SuggestedAction: diagnose.ActionEscalate,
		Confidence:      1.0,
	})
	if res.Outcome != OutcomeEscalated {
		t.Errorf("outcome: want escalated, got %s", res.Outcome)
	}
}

func TestHeal_ActionWithNoExecutor_Escalates(t *testing.T) {
	// If the registry doesn't have an executor for the suggested action,
	// fail safe rather than silent no-op.
	h := New()
	res := h.Heal(context.Background(), diagnose.Signal{}, diagnose.Diagnosis{
		SuggestedAction: diagnose.ActionRefreshAuth,
		Confidence:      0.95,
	})
	if res.Outcome != OutcomeNoActionDefined {
		t.Errorf("outcome: want no_action_defined, got %s", res.Outcome)
	}
	if res.Error == nil {
		t.Error("Error should explain the missing executor")
	}
}

func TestHeal_ExecutorFailure_PropagatesError(t *testing.T) {
	h := New()
	exec := &fakeExecutor{action: diagnose.ActionBackoffRetry, err: errors.New("backoff failed: no retry budget")}
	h.Register(exec)

	res := h.Heal(context.Background(), diagnose.Signal{}, diagnose.Diagnosis{
		SuggestedAction: diagnose.ActionBackoffRetry,
		Confidence:      0.95,
	})
	if res.Outcome != OutcomeActionFailed {
		t.Errorf("outcome: want action_failed, got %s", res.Outcome)
	}
	if res.Error == nil {
		t.Error("Error must be set")
	}
}

func TestHeal_NoOpAction(t *testing.T) {
	h := New()
	res := h.Heal(context.Background(), diagnose.Signal{}, diagnose.Diagnosis{
		SuggestedAction: diagnose.ActionNoOp,
		Confidence:      0.95,
	})
	if res.Outcome != OutcomeNoActionDefined {
		t.Errorf("outcome: want no_action_defined for noop, got %s", res.Outcome)
	}
}

func TestApproveHITL_RunsAction(t *testing.T) {
	// Simulates the post-HITL approval flow.
	h := New()
	exec := &fakeExecutor{action: diagnose.ActionRegenerateConnector}
	h.Register(exec)

	dx := diagnose.Diagnosis{
		SuggestedAction: diagnose.ActionRegenerateConnector,
		Confidence:      0.7,
	}
	// First call: medium confidence -> HITL.
	res := h.Heal(context.Background(), diagnose.Signal{}, dx)
	if res.Outcome != OutcomeHITLRequested {
		t.Fatalf("expected hitl_requested, got %s", res.Outcome)
	}

	// Operator approves -> action runs.
	approved := h.ApproveHITL(context.Background(), diagnose.Signal{}, dx)
	if approved.Outcome != OutcomeAutoExecuted {
		t.Errorf("post-approval outcome: want auto_executed, got %s", approved.Outcome)
	}
	if exec.calls != 1 {
		t.Errorf("executor should be called once after approval, got %d", exec.calls)
	}
}

func TestBands_AreOverridable(t *testing.T) {
	// Per-pipeline risk tuning: tests with stricter bands.
	h := New()
	h.AutoBand = 0.99
	h.HITLBand = 0.80
	exec := &fakeExecutor{action: diagnose.ActionRefreshAuth}
	h.Register(exec)

	// 0.95 confidence — high enough by default, NOT by the strict bands.
	res := h.Heal(context.Background(), diagnose.Signal{}, diagnose.Diagnosis{
		SuggestedAction: diagnose.ActionRefreshAuth,
		Confidence:      0.95,
	})
	if res.Outcome != OutcomeHITLRequested {
		t.Errorf("with strict bands 0.95 should ask, got %s", res.Outcome)
	}
}
