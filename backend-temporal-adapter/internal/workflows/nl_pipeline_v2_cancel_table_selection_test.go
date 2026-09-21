package workflows

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// These tests drive the REAL NLPipelineWorkflowV2 into the table-selection park
// and then send the same "cancel" signal StopPipeline sends
// (api-gateway pipelines.go StopPipeline → signalPipelineWorkflowWithFallback).
//
// Before hitl-wait-honours-cancel the park listened only for tables_selected and
// its deadline timer, so a cancelled setup sat there for the full 24h. When the
// timer finally fired, the timeout branch read state.WaitReason — which the cancel
// goroutine's Transition(StateCancelled) had already set to nil — and the workflow
// task panicked on every retry. The chat that asked for the cancel was left
// looking at a pipeline that never finished.
//
// The harness uses the fast-rerun input shape (connections + a saved selection)
// only to skip the agentic phases; the executor fake then asks for table
// selection, which parks the workflow at exactly the same code as a first run.

type tableSelectionHarness struct {
	mu            sync.Mutex
	executorTasks []map[string]interface{}
	stateEvents   []string
	statusWrites  []string

	// onWaiting, when set, runs once inside the StateUpdateActivity that announces
	// the park, before that activity returns.
	onWaiting func()
}

func (h *tableSelectionHarness) executor(_ context.Context, task map[string]interface{}) (ExecutorNativeResult, error) {
	h.mu.Lock()
	h.executorTasks = append(h.executorTasks, task)
	n := len(h.executorTasks)
	h.mu.Unlock()
	if n == 1 {
		return ExecutorNativeResult{
			Status: "waiting_for_table_selection",
			Error:  "select tables to continue",
			Output: map[string]interface{}{"available_tables": []interface{}{"orders", "customers"}},
		}, nil
	}
	return ExecutorNativeResult{Status: "success", Output: map[string]interface{}{"rows": 10}}, nil
}

func (h *tableSelectionHarness) stateUpdate(_ context.Context, in StateUpdateInput) error {
	h.mu.Lock()
	h.stateEvents = append(h.stateEvents, in.EventType)
	var hook func()
	if in.EventType == "PIPELINE_WAITING" && h.onWaiting != nil {
		hook, h.onWaiting = h.onWaiting, nil
	}
	h.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

func (h *tableSelectionHarness) pipelineStatus(_ context.Context, _ string, _ string, status string, _ string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.statusWrites = append(h.statusWrites, status)
	return nil
}

func (h *tableSelectionHarness) domainEvent(_ context.Context, _ map[string]interface{}) error {
	return nil
}

func (h *tableSelectionHarness) snapshot() (tasks []map[string]interface{}, events []string, statuses []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]map[string]interface{}(nil), h.executorTasks...),
		append([]string(nil), h.stateEvents...),
		append([]string(nil), h.statusWrites...)
}

var tableSelectionStart = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

func newTableSelectionEnv(h *tableSelectionHarness) *testsuite.TestWorkflowEnvironment {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetStartTime(tableSelectionStart)
	env.RegisterWorkflowWithOptions(NLPipelineWorkflowV2, workflow.RegisterOptions{Name: NLPipelineWorkflowV2Name})
	env.RegisterActivityWithOptions(h.executor, activity.RegisterOptions{Name: "ExecutorNativeActivity"})
	env.RegisterActivityWithOptions(h.stateUpdate, activity.RegisterOptions{Name: "StateUpdateActivity"})
	env.RegisterActivityWithOptions(h.pipelineStatus, activity.RegisterOptions{Name: "UpdatePipelineStatusActivity"})
	env.RegisterActivityWithOptions(h.domainEvent, activity.RegisterOptions{Name: "EmitDomainEventActivity"})
	return env
}

func tableSelectionInput() NLPipelineWorkflowV2Input {
	return NLPipelineWorkflowV2Input{
		PipelineID:              "pipe-cancel-1",
		ExecutionID:             "exec-cancel-1",
		UserID:                  "user-1",
		Message:                 "move my collections",
		SourceConnectionID:      "src-1",
		DestinationConnectionID: "dst-1",
		SelectedTables:          []string{"orders"},
		ExecutorDispatch:        ExecutorDispatchTemporal,
	}
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// The fix: a cancel during table selection ends the run within the same workflow
// task, recorded as stopped (the DB's cancelled outcome), never failed.
func TestNLPipelineV2_CancelDuringTableSelectionEndsRunAsStopped(t *testing.T) {
	h := &tableSelectionHarness{}
	env := newTableSelectionEnv(h)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCancel, map[string]interface{}{"action": "stop"})
	}, time.Hour)
	env.ExecuteWorkflow(NLPipelineWorkflowV2, tableSelectionInput())

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("a user cancel must not end the run with an error, got: %v", err)
	}

	tasks, events, statuses := h.snapshot()
	if !containsString(events, "PIPELINE_WAITING") {
		t.Fatalf("the harness never reached the table-selection park (state events %v) — the test proves nothing", events)
	}
	if len(tasks) != 1 {
		t.Fatalf("executor must not run again after a cancel, ran %d times", len(tasks))
	}
	if len(statuses) != 1 || statuses[0] != "stopped" {
		t.Fatalf("expected exactly one terminal status write of \"stopped\", got %v", statuses)
	}

	// Promptly: the run ends at the cancel (1h in), not at the 24h park deadline.
	elapsed := env.Now().Sub(tableSelectionStart)
	if elapsed >= 2*time.Hour {
		t.Fatalf("run ended %s after start; a cancel at 1h must not wait for the 24h deadline", elapsed)
	}
}

// The cancel lands while the park is still being announced (the user saw the
// picker from the state write and pressed Cancel before that activity's completion
// was recorded). The cancel handler has already cleared state.WaitReason by the
// time the workflow reaches the wait, so the call itself must not read it.
func TestNLPipelineV2_CancelWhileAnnouncingTableSelectionEndsRunAsStopped(t *testing.T) {
	h := &tableSelectionHarness{}
	env := newTableSelectionEnv(h)
	// SignalWorkflow only queues a callback, and it is queued before this activity's
	// completion, so the workflow sees the cancel first.
	h.onWaiting = func() {
		env.SignalWorkflow(SignalCancel, map[string]interface{}{"action": "stop"})
	}

	env.ExecuteWorkflow(NLPipelineWorkflowV2, tableSelectionInput())

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("a user cancel must not end the run with an error, got: %v", err)
	}
	tasks, events, statuses := h.snapshot()
	if !containsString(events, "PIPELINE_WAITING") {
		t.Fatalf("the harness never reached the table-selection park (state events %v)", events)
	}
	if len(tasks) != 1 {
		t.Fatalf("executor must not run again after a cancel, ran %d times", len(tasks))
	}
	if len(statuses) != 1 || statuses[0] != "stopped" {
		t.Fatalf("expected exactly one terminal status write of \"stopped\", got %v", statuses)
	}
	if elapsed := env.Now().Sub(tableSelectionStart); elapsed >= time.Hour {
		t.Fatalf("run ended %s after start; it must end at the cancel", elapsed)
	}
}

// Control: a normal selection still resumes execution with the chosen tables and
// completes, and a cancel listener that is never fired changes nothing.
func TestNLPipelineV2_TableSelectionStillProceeds(t *testing.T) {
	h := &tableSelectionHarness{}
	env := newTableSelectionEnv(h)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalTablesSelected, TablesSelectedPayload{
			PipelineID:     "pipe-cancel-1",
			SelectedTables: []string{"orders", "customers"},
		})
	}, time.Hour)
	env.ExecuteWorkflow(NLPipelineWorkflowV2, tableSelectionInput())

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned an error: %v", err)
	}

	tasks, events, statuses := h.snapshot()
	if !containsString(events, "PIPELINE_WAITING") {
		t.Fatalf("the harness never reached the table-selection park (state events %v)", events)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected the executor to run again after selection (2 calls), got %d", len(tasks))
	}
	sel, ok := tasks[1]["selected_tables"].([]interface{})
	if !ok || len(sel) != 2 {
		t.Fatalf("second executor call must carry the 2 selected tables, got %#v", tasks[1]["selected_tables"])
	}
	if len(statuses) != 1 || statuses[0] != "completed" {
		t.Fatalf("expected exactly one terminal status write of \"completed\", got %v", statuses)
	}
}

// Replay safety: a history recorded before hitl-wait-honours-cancel has no marker
// for it, so GetVersion returns DefaultVersion and the park must keep ignoring the
// cancel until its deadline — anything else would emit commands the old history
// never recorded. What changes for such a run is only what happens AFTER the
// deadline: it now ends as stopped instead of panicking on the nil WaitReason.
func TestNLPipelineV2_CancelDuringTableSelectionOnDefaultVersionWaitsForDeadline(t *testing.T) {
	h := &tableSelectionHarness{}
	env := newTableSelectionEnv(h)
	env.OnGetVersion(hitlWaitHonoursCancelVersion, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCancel, map[string]interface{}{"action": "stop"})
	}, time.Hour)
	env.ExecuteWorkflow(NLPipelineWorkflowV2, tableSelectionInput())

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("a cancelled run reaching its park deadline must end cleanly, got: %v", err)
	}

	tasks, events, statuses := h.snapshot()
	if !containsString(events, "PIPELINE_WAITING") {
		t.Fatalf("the harness never reached the table-selection park (state events %v)", events)
	}
	if len(tasks) != 1 {
		t.Fatalf("executor must not run again after a cancel, ran %d times", len(tasks))
	}
	if len(statuses) != 1 || statuses[0] != "stopped" {
		t.Fatalf("expected exactly one terminal status write of \"stopped\", got %v", statuses)
	}
	elapsed := env.Now().Sub(tableSelectionStart)
	if elapsed < 24*time.Hour {
		t.Fatalf("on DefaultVersion the park must ignore the cancel until its 24h deadline, but the run ended after %s", elapsed)
	}
}
