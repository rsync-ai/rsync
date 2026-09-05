package heal

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// Pins KI-HEAL-NIL-HOOK-REPORTS-SUCCESS.
//
// CleanupCDCResourcesExecutor and RepairOwnershipRowExecutor both do their real
// work through an injected hook, and both returned nil when that hook was
// absent — and again when the hook itself failed. `return nil` from Run is not
// "skipped"; it is the success signal. Heal reads it as one:
//
//	if err := exec.Run(ctx, sig); err != nil { ...OutcomeActionFailed... }
//	res.Outcome = OutcomeAutoExecuted
//	res.ActionExecuted = true
//
// So the healer recorded an auto-executed fix, set ActionExecuted, and put a
// success on the pipeline timeline for an action that did not happen. The
// attempt ledger — the thing ApplyMemory later consults to decide whether an
// action is worth trying again — was being fed fabricated successes.
//
// Production hits the nil branch on every deployment: main.go:798 calls
// NewHealWorker, which forwards an empty AutoHealHooks{} (worker.go:111), so
// CleanupFn and RepairFn are nil in every running orchestrator.
//
// The justification in the tests this file replaces was that erroring would
// "crash the healer worker". It would not, and that is checkable in the caller
// above: Run's error is captured into the HealResult and returned. Nothing
// panics, nothing stops the sweep, and the next tick proceeds normally. The
// only thing an error changes is whether the ledger tells the truth.
//
// These assert through Heal rather than on Run's return value directly,
// because the outcome is what is persisted and what an operator sees.

// healResultFor registers exec on a fresh Healer and drives one diagnosis
// through it at the given confidence.
func healResultFor(t *testing.T, exec Executor, confidence float64) HealResult {
	t.Helper()
	h := New()
	h.Register(exec)
	return h.Heal(context.Background(),
		diagnose.Signal{PipelineID: "p1"},
		diagnose.Diagnosis{
			SuggestedAction: exec.Action(),
			Confidence:      confidence,
			Rationale:       "test",
		})
}

func assertNotReportedAsDone(t *testing.T, res HealResult, what string) {
	t.Helper()
	if res.Outcome == OutcomeAutoExecuted {
		t.Errorf("%s: outcome is %q — the healer recorded an auto-executed fix for work "+
			"that never happened", what, res.Outcome)
	}
	if res.ActionExecuted {
		t.Errorf("%s: ActionExecuted is true — the timeline and the attempt ledger will "+
			"both claim this pipeline was acted on", what)
	}
	if res.Error == nil {
		t.Errorf("%s: HealResult carries no error, so nothing downstream can tell this "+
			"apart from a real success", what)
	}
}

func TestCleanupCDCWithNoHookIsNotRecordedAsAFix(t *testing.T) {
	res := healResultFor(t, &CleanupCDCResourcesExecutor{DB: nil, CleanupFn: nil}, 0.95)
	assertNotReportedAsDone(t, res, "CleanupCDCResources with nil CleanupFn")
}

func TestRepairOwnershipWithNoHookIsNotRecordedAsAFix(t *testing.T) {
	res := healResultFor(t, &RepairOwnershipRowExecutor{DB: nil, RepairFn: nil}, 0.95)
	assertNotReportedAsDone(t, res, "RepairOwnershipRow with nil RepairFn")
}

// A failing hook is the worse half of the pair: the executor learned, from the
// hook itself, that the fix did not work — and reported success anyway.
func TestCleanupCDCHookFailureIsNotRecordedAsAFix(t *testing.T) {
	res := healResultFor(t, &CleanupCDCResourcesExecutor{
		DB: nil,
		CleanupFn: func(_ context.Context, _ string) error {
			return errors.New("slot is in use by an active subscriber")
		},
	}, 0.95)
	assertNotReportedAsDone(t, res, "CleanupCDCResources with a failing CleanupFn")

	// The reason has to survive to the ledger. "action failed" alone sends an
	// operator back to the logs to find out what failed; the slot being held by
	// a live subscriber is the whole diagnosis.
	if res.Error != nil && !strings.Contains(res.Error.Error(), "active subscriber") {
		t.Errorf("the hook's own error was dropped on the way out: %v", res.Error)
	}
}

func TestRepairOwnershipHookFailureIsNotRecordedAsAFix(t *testing.T) {
	res := healResultFor(t, &RepairOwnershipRowExecutor{
		DB: nil,
		RepairFn: func(_ context.Context, _ string) error {
			return errors.New("destination connector unreachable")
		},
	}, 0.95)
	assertNotReportedAsDone(t, res, "RepairOwnershipRow with a failing RepairFn")

	if res.Error != nil && !strings.Contains(res.Error.Error(), "destination connector unreachable") {
		t.Errorf("the hook's own error was dropped on the way out: %v", res.Error)
	}
}

// The success path must stay exactly as it was. A fix that made every call
// error would pass every assertion above and be strictly worse than the bug.
func TestCleanupCDCStillSucceedsWhenTheHookSucceeds(t *testing.T) {
	called := false
	res := healResultFor(t, &CleanupCDCResourcesExecutor{
		DB: nil,
		CleanupFn: func(_ context.Context, pid string) error {
			called = true
			if pid != "p1" {
				t.Errorf("wrong pipeline id passed to the hook: %q", pid)
			}
			return nil
		},
	}, 0.95)

	if !called {
		t.Fatal("CleanupFn was never invoked")
	}
	if res.Outcome != OutcomeAutoExecuted || !res.ActionExecuted || res.Error != nil {
		t.Fatalf("a successful cleanup no longer reports success: outcome=%q executed=%v err=%v",
			res.Outcome, res.ActionExecuted, res.Error)
	}
}

func TestRepairOwnershipStillSucceedsWhenTheHookSucceeds(t *testing.T) {
	called := false
	res := healResultFor(t, &RepairOwnershipRowExecutor{
		DB: nil,
		RepairFn: func(_ context.Context, _ string) error {
			called = true
			return nil
		},
	}, 0.95)

	if !called {
		t.Fatal("RepairFn was never invoked")
	}
	if res.Outcome != OutcomeAutoExecuted || !res.ActionExecuted || res.Error != nil {
		t.Fatalf("a successful repair no longer reports success: outcome=%q executed=%v err=%v",
			res.Outcome, res.ActionExecuted, res.Error)
	}
}
