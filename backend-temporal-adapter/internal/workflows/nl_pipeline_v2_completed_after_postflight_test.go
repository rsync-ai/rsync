package workflows

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// KI-SILENTDROP-COMPLETED-EVENT. The postflight silent-drop guard lives in the
// terminal status write, and that write used to run from the workflow's deferred
// updater — after PIPELINE_COMPLETED had gone out. A run the guard failed first
// told the event stream it completed (and the projector's PIPELINE_COMPLETED hook
// fired its downstream model refreshes). These tests drive the REAL
// NLPipelineWorkflowV2 through a successful executor call and record, in one
// ordered log, every domain event, state write and status write.

// finalizeVia adapts a harness's UpdatePipelineStatusActivity fake into a
// FinalizeCompletedRunActivity fake: the same write, with status "completed",
// returning the outcome the real activity would when the postflight passes.
func finalizeVia(pipelineStatus func(context.Context, string, string, string, string) error) func(context.Context, string, string) (runOutcome, error) {
	return func(ctx context.Context, pipelineID, executionID string) (runOutcome, error) {
		if err := pipelineStatus(ctx, pipelineID, executionID, "completed", ""); err != nil {
			return runOutcome{}, err
		}
		return runOutcome{Status: "completed"}, nil
	}
}

type postflightOrderHarness struct {
	mu  sync.Mutex
	log []string // "event:<type>", "state:<event type>", "status:<status>", "finalize"

	failedMessage string // message of the PIPELINE_FAILED event, if one was emitted

	// finalize decides what FinalizeCompletedRunActivity returns.
	finalize func() (runOutcome, error)
}

func (h *postflightOrderHarness) add(s string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.log = append(h.log, s)
}

func (h *postflightOrderHarness) executor(_ context.Context, _ map[string]interface{}) (ExecutorNativeResult, error) {
	return ExecutorNativeResult{Status: "success", Output: map[string]interface{}{"rows": 0}}, nil
}

func (h *postflightOrderHarness) stateUpdate(_ context.Context, in StateUpdateInput) error {
	h.add("state:" + in.EventType)
	return nil
}

func (h *postflightOrderHarness) pipelineStatus(_ context.Context, _, _, status, _ string) error {
	h.add("status:" + status)
	return nil
}

func (h *postflightOrderHarness) finalizeRun(_ context.Context, _, _ string) (runOutcome, error) {
	h.add("finalize")
	return h.finalize()
}

func (h *postflightOrderHarness) domainEvent(_ context.Context, ev map[string]interface{}) error {
	et, _ := ev["event_type"].(string)
	if et == "PIPELINE_FAILED" {
		h.mu.Lock()
		h.failedMessage, _ = ev["message"].(string)
		h.mu.Unlock()
	}
	h.add("event:" + et)
	return nil
}

func (h *postflightOrderHarness) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.log...)
}

func indexOf(list []string, want string) int {
	for i, s := range list {
		if s == want {
			return i
		}
	}
	return -1
}

func countOf(list []string, want string) int {
	n := 0
	for _, s := range list {
		if s == want {
			n++
		}
	}
	return n
}

func runPostflightOrder(t *testing.T, h *postflightOrderHarness, setup func(*testsuite.TestWorkflowEnvironment)) []string {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetStartTime(time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC))
	env.RegisterWorkflowWithOptions(NLPipelineWorkflowV2, workflow.RegisterOptions{Name: NLPipelineWorkflowV2Name})
	env.RegisterActivityWithOptions(h.executor, activity.RegisterOptions{Name: "ExecutorNativeActivity"})
	env.RegisterActivityWithOptions(h.stateUpdate, activity.RegisterOptions{Name: "StateUpdateActivity"})
	env.RegisterActivityWithOptions(h.pipelineStatus, activity.RegisterOptions{Name: "UpdatePipelineStatusActivity"})
	env.RegisterActivityWithOptions(h.finalizeRun, activity.RegisterOptions{Name: "FinalizeCompletedRunActivity"})
	env.RegisterActivityWithOptions(h.domainEvent, activity.RegisterOptions{Name: "EmitDomainEventActivity"})
	if setup != nil {
		setup(env)
	}

	env.ExecuteWorkflow(NLPipelineWorkflowV2, NLPipelineWorkflowV2Input{
		PipelineID:              "pipe-postflight-1",
		ExecutionID:             "exec-postflight-1",
		UserID:                  "user-1",
		Message:                 "copy orders",
		SourceConnectionID:      "src-1",
		DestinationConnectionID: "dst-1",
		SelectedTables:          []string{"orders"},
		ExecutorDispatch:        ExecutorDispatchTemporal,
	})
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned an error: %v", err)
	}
	log := h.snapshot()
	// Vacuity floor: the executor ran to success, or nothing below means anything.
	if indexOf(log, "state:STAGE_COMPLETED") < 0 {
		t.Fatalf("the run never reached the executor's STAGE_COMPLETED (log %v); the test proves nothing", log)
	}
	return log
}

// The bug: the postflight fails the run, and PIPELINE_COMPLETED must not have gone out.
func TestNLPipelineV2_PostflightFailureNeverEmitsPipelineCompleted(t *testing.T) {
	const reason = "silent_drop_detected: public.orders: selected but never reported (no stats row)"
	h := &postflightOrderHarness{finalize: func() (runOutcome, error) {
		return runOutcome{Status: "failed", ErrorMessage: reason}, nil
	}}
	log := runPostflightOrder(t, h, nil)

	if n := countOf(log, "event:PIPELINE_COMPLETED"); n != 0 {
		t.Fatalf("a run the postflight failed emitted PIPELINE_COMPLETED %d time(s); log %v", n, log)
	}
	if n := countOf(log, "state:PIPELINE_COMPLETED"); n != 0 {
		t.Fatalf("a run the postflight failed wrote a PIPELINE_COMPLETED state %d time(s); log %v", n, log)
	}
	if countOf(log, "event:PIPELINE_FAILED") != 1 {
		t.Fatalf("expected exactly one PIPELINE_FAILED event; log %v", log)
	}
	if indexOf(log, "finalize") > indexOf(log, "event:PIPELINE_FAILED") {
		t.Fatalf("PIPELINE_FAILED went out before the postflight ran; log %v", log)
	}
	if h.failedMessage != reason {
		t.Fatalf("PIPELINE_FAILED must carry the postflight's reason, got %q", h.failedMessage)
	}
	// The status is already written; the deferred updater must not write it again.
	if n := countOf(log, "finalize"); n != 1 {
		t.Fatalf("expected one finalize write, got %d; log %v", n, log)
	}
	for _, s := range log {
		if len(s) > len("status:") && s[:len("status:")] == "status:" {
			t.Fatalf("the deferred updater wrote the status again (%s); log %v", s, log)
		}
	}
}

// A healthy run: the status is written (and judged) first, then PIPELINE_COMPLETED,
// and only once.
func TestNLPipelineV2_CompletedRunIsJudgedBeforePipelineCompleted(t *testing.T) {
	h := &postflightOrderHarness{finalize: func() (runOutcome, error) {
		return runOutcome{Status: "completed"}, nil
	}}
	log := runPostflightOrder(t, h, nil)

	fin, done := indexOf(log, "finalize"), indexOf(log, "event:PIPELINE_COMPLETED")
	if fin < 0 || done < 0 {
		t.Fatalf("expected both a finalize write and PIPELINE_COMPLETED; log %v", log)
	}
	if fin > done {
		t.Fatalf("PIPELINE_COMPLETED went out before the postflight judged the run; log %v", log)
	}
	if countOf(log, "finalize") != 1 || countOf(log, "status:completed") != 0 {
		t.Fatalf("expected exactly one terminal write (finalize), no deferred rewrite; log %v", log)
	}
	if countOf(log, "event:PIPELINE_FAILED") != 0 {
		t.Fatalf("a healthy run emitted PIPELINE_FAILED; log %v", log)
	}
}

// Replay safety: a history recorded before the marker keeps the old command
// sequence — emit first, then the deferred status write — and never calls the new
// activity, or replay would hit a nondeterminism error.
func TestNLPipelineV2_CompletedEventOrderOnDefaultVersionIsUnchanged(t *testing.T) {
	h := &postflightOrderHarness{finalize: func() (runOutcome, error) {
		return runOutcome{}, errors.New("must not be called on an old history")
	}}
	log := runPostflightOrder(t, h, func(env *testsuite.TestWorkflowEnvironment) {
		env.OnGetVersion(completedEventAfterPostflightVersion, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
	})

	if countOf(log, "finalize") != 0 {
		t.Fatalf("an old history called FinalizeCompletedRunActivity; log %v", log)
	}
	done, status := indexOf(log, "event:PIPELINE_COMPLETED"), indexOf(log, "status:completed")
	if done < 0 || status < 0 || done > status {
		t.Fatalf("expected PIPELINE_COMPLETED then the deferred completed write; log %v", log)
	}
}

// The finalize activity itself failing (DB down past its retries) must not lose
// the status write: the run falls back to the old path.
func TestNLPipelineV2_FinalizeFailureFallsBackToDeferredWrite(t *testing.T) {
	h := &postflightOrderHarness{finalize: func() (runOutcome, error) {
		return runOutcome{}, temporal.NewNonRetryableApplicationError("db down", "test", fmt.Errorf("db down"))
	}}
	log := runPostflightOrder(t, h, nil)

	if countOf(log, "finalize") != 1 {
		t.Fatalf("expected one (failed) finalize attempt; log %v", log)
	}
	if countOf(log, "status:completed") != 1 || countOf(log, "event:PIPELINE_COMPLETED") != 1 {
		t.Fatalf("expected the fallback: PIPELINE_COMPLETED plus one deferred completed write; log %v", log)
	}
}
