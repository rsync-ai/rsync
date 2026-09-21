package workflows

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// Model refresh loop tests.
//
// The behaviour worth guarding is not "the rebuild happens" — that is the clock
// path's activity, already exercised where it is defined. It is what the loop does
// with MORE completions than it has rebuilds to give them: two upstream completions
// inside one rebuild window must produce one further rebuild, and must produce it at
// all. The goroutine path produced none — the model's advisory run lock refused the
// second rebuild and nothing kept the completion. That is the whole reason this
// workflow exists rather than a second goroutine.
//
// Each test replaces RunModelActivity with a closure registered under the same name,
// so nothing here reaches an api-gateway, an HTTP client or a database.

// modelRefreshProbe records what the loop asked the activity to do.
type modelRefreshProbe struct {
	mu    sync.Mutex
	calls []ScheduledModelRunInput
	// onCall runs inside the activity, before it returns. It is how a test makes a
	// completion arrive *while a rebuild is in flight*, which is the only moment
	// coalescing can be observed.
	onCall func(n int)
	// err, when set, is returned by every call so the retry policy is exercised.
	err error
}

func (p *modelRefreshProbe) run(_ context.Context, in ScheduledModelRunInput) (modelRunActivityResult, error) {
	p.mu.Lock()
	p.calls = append(p.calls, in)
	n := len(p.calls)
	onCall := p.onCall
	err := p.err
	p.mu.Unlock()

	if onCall != nil {
		onCall(n)
	}
	if err != nil {
		return modelRunActivityResult{}, err
	}
	return modelRunActivityResult{Status: "succeeded", TargetTable: "public.orders_daily"}, nil
}

func (p *modelRefreshProbe) recorded() []ScheduledModelRunInput {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ScheduledModelRunInput(nil), p.calls...)
}

// newModelRefreshEnv wires the probe in as RunModelActivity.
func newModelRefreshEnv(probe *modelRefreshProbe) *testsuite.TestWorkflowEnvironment {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ModelRefreshWorkflow)
	env.RegisterActivityWithOptions(probe.run, activity.RegisterOptions{Name: "RunModelActivity"})
	return env
}

func signalAt(env *testsuite.TestWorkflowEnvironment, d time.Duration, req ModelRefreshRequest) {
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ModelRefreshSignalName, req)
	}, d)
}

// TestModelRefreshWorkflow_OneCompletionRebuildsOnce is also the guard on the
// argument: the workflow is started WITH a saved_query_id and receives ONE signal,
// so a second rebuild here would mean the argument was treated as work too — which
// is exactly what signal-with-start would make happen on every first completion.
func TestModelRefreshWorkflow_OneCompletionRebuildsOnce(t *testing.T) {
	probe := &modelRefreshProbe{}
	env := newModelRefreshEnv(probe)

	signalAt(env, time.Millisecond, ModelRefreshRequest{
		ScheduleID: "sched-1", UpstreamKind: "pipeline", UpstreamID: "pipe-1", ExecutionID: "exec-1",
	})
	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed — the idle window did not end the loop")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned an error: %v", err)
	}

	calls := probe.recorded()
	if len(calls) != 1 {
		t.Fatalf("one completion produced %d rebuilds, want exactly 1: %+v", len(calls), calls)
	}
	if calls[0].SavedQueryID != "model-1" || calls[0].ScheduleID != "sched-1" {
		t.Errorf("rebuild ran with %+v, want the model and schedule the signal named", calls[0])
	}
	// Without this the run is recorded as a clock tick and the gateway serves it from
	// the wrong door, which cannot find an after_upstream schedule at all.
	if calls[0].Trigger != ModelRefreshTrigger {
		t.Errorf("rebuild used trigger %q, want %q", calls[0].Trigger, ModelRefreshTrigger)
	}
}

// TestModelRefreshWorkflow_CompletionsDuringARebuildCoalesce is the load-bearing
// one. Two completions land while the first rebuild is running; they must become
// one further rebuild, not two.
func TestModelRefreshWorkflow_CompletionsDuringARebuildCoalesce(t *testing.T) {
	probe := &modelRefreshProbe{}
	env := newModelRefreshEnv(probe)
	probe.onCall = func(n int) {
		if n != 1 {
			return
		}
		env.SignalWorkflow(ModelRefreshSignalName, ModelRefreshRequest{
			ScheduleID: "sched-a", UpstreamKind: "pipeline", UpstreamID: "pipe-1", ExecutionID: "exec-2",
		})
		env.SignalWorkflow(ModelRefreshSignalName, ModelRefreshRequest{
			ScheduleID: "sched-b", UpstreamKind: "pipeline", UpstreamID: "pipe-1", ExecutionID: "exec-3",
		})
	}

	signalAt(env, time.Millisecond, ModelRefreshRequest{
		ScheduleID: "sched-1", UpstreamKind: "pipeline", UpstreamID: "pipe-1", ExecutionID: "exec-1",
	})
	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned an error: %v", err)
	}

	calls := probe.recorded()
	if len(calls) != 2 {
		t.Fatalf("three completions produced %d rebuilds, want 2 (one, then one for the pair): %+v",
			len(calls), calls)
	}
	if calls[1].ScheduleID != "sched-b" {
		t.Errorf("the coalesced rebuild used schedule %q, want the last completion's", calls[1].ScheduleID)
	}
}

// TestModelRefreshWorkflow_CarriedRequestsSurviveContinueAsNew: the SDK does not
// move unhandled signals across continue-as-new, so anything buffered at the
// history bound is passed in the argument. It has to be executed, and it has to be
// executed WITHOUT another signal arriving — otherwise a busy model silently loses
// exactly the completions that arrived at its busiest moment.
func TestModelRefreshWorkflow_CarriedRequestsSurviveContinueAsNew(t *testing.T) {
	probe := &modelRefreshProbe{}
	env := newModelRefreshEnv(probe)

	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{
		SavedQueryID: "model-1",
		Carried: []ModelRefreshRequest{
			{ScheduleID: "sched-a", UpstreamKind: "pipeline", UpstreamID: "pipe-1", ExecutionID: "exec-1"},
			{ScheduleID: "sched-b", UpstreamKind: "pipeline", UpstreamID: "pipe-1", ExecutionID: "exec-2"},
		},
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned an error: %v", err)
	}

	calls := probe.recorded()
	if len(calls) != 1 {
		t.Fatalf("two carried requests produced %d rebuilds, want 1 coalesced: %+v", len(calls), calls)
	}
	if calls[0].SavedQueryID != "model-1" {
		t.Errorf("carried rebuild ran for %q, want model-1", calls[0].SavedQueryID)
	}
	// The batch is the whole queue, not its head: a rebuild reads current state, so
	// the most recent request is the one that describes it. Asserting on the LAST
	// schedule id is what makes a silently truncated batch visible at all.
	if calls[0].ScheduleID != "sched-b" {
		t.Errorf("carried rebuild used schedule %q, want the most recent one", calls[0].ScheduleID)
	}
}

// TestModelRefreshWorkflow_HistoryIsBounded: a model wired to a minutely pipeline
// never goes idle, so the loop must hand over rather than grow one run forever.
func TestModelRefreshWorkflow_HistoryIsBounded(t *testing.T) {
	probe := &modelRefreshProbe{}
	env := newModelRefreshEnv(probe)
	// Every rebuild is immediately followed by another completion, so the idle window
	// never closes and only the bound can end this run.
	probe.onCall = func(n int) {
		if n > modelRefreshesPerRun {
			return
		}
		env.SignalWorkflow(ModelRefreshSignalName, ModelRefreshRequest{
			ScheduleID: "sched-1", UpstreamKind: "pipeline", UpstreamID: "pipe-1", ExecutionID: "exec-next",
			// Non-zero so the handover below can show depth surviving the serialization
			// round trip. A depth that reset at continue-as-new would let a ring run
			// forever in hops of modelRefreshesPerRun.
			Depth: 4,
		})
	}

	signalAt(env, time.Millisecond, ModelRefreshRequest{
		ScheduleID: "sched-1", UpstreamKind: "pipeline", UpstreamID: "pipe-1", ExecutionID: "exec-1",
	})
	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	err := env.GetWorkflowError()
	if !workflow.IsContinueAsNewError(err) {
		t.Fatalf("a never-idle loop ended with %v, want a continue-as-new", err)
	}
	if got := len(probe.recorded()); got != modelRefreshesPerRun {
		t.Errorf("handed over after %d rebuilds, want %d", got, modelRefreshesPerRun)
	}

	// The queue does not survive continue-as-new on its own: the SDK does not move
	// unhandled signals to the new run. The completion that landed during the last
	// rebuild has to be handed over in the argument, or it is dropped at precisely
	// the busiest moment this loop ever has.
	var canErr *workflow.ContinueAsNewError
	if !errors.As(err, &canErr) {
		t.Fatalf("continue-as-new error has type %T, cannot read the handover", err)
	}
	var next ModelRefreshInput
	if derr := converter.GetDefaultDataConverter().FromPayloads(canErr.Input, &next); derr != nil {
		t.Fatalf("could not decode the handover argument: %v", derr)
	}
	if next.SavedQueryID != "model-1" {
		t.Errorf("handover names model %q, want model-1", next.SavedQueryID)
	}
	if len(next.Carried) != 1 {
		t.Fatalf("handover carried %d completions, want the 1 that arrived during the last rebuild",
			len(next.Carried))
	}
	if next.Carried[0].Depth != 4 {
		t.Errorf("the carried completion handed over depth %d, want 4 — the chain bound is only "+
			"worth having if it survives the handover", next.Carried[0].Depth)
	}
}

// TestModelRefreshWorkflow_ExhaustedRetriesDoNotFailTheWorkflow: failing here would
// take every buffered request down with the run. The next upstream completion is a
// better retry than a replay of this one — it carries fresher data.
func TestModelRefreshWorkflow_ExhaustedRetriesDoNotFailTheWorkflow(t *testing.T) {
	probe := &modelRefreshProbe{err: errors.New("api-gateway unreachable")}
	env := newModelRefreshEnv(probe)

	signalAt(env, time.Millisecond, ModelRefreshRequest{
		ScheduleID: "sched-1", UpstreamKind: "pipeline", UpstreamID: "pipe-1", ExecutionID: "exec-1",
	})
	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("a failed rebuild failed the whole workflow: %v", err)
	}
	// Bounded, too: an endpoint that is down must not be hammered until the run
	// timeout there is none of.
	if got := len(probe.recorded()); got != 3 {
		t.Errorf("activity attempted %d times, want the policy's 3", got)
	}
}

// ============================================================================
// Model-to-model chains (migration 100)
// ============================================================================

// ModelRefreshTrigger is a wire contract, not a constant: the gateway selects the event
// door by string-comparing it against its own scheduleAfterUpstream, and the two modules
// share no types. Renaming it on one side leaves a workflow that rebuilds nothing and a
// gateway that answers 404 for every event run, with nothing failing to compile.
func TestModelRefreshTriggerIsTheGatewaysScheduleTypeVerbatim(t *testing.T) {
	if ModelRefreshTrigger != "after_upstream" {
		t.Fatalf("ModelRefreshTrigger = %q, want %q — it is compared as a string against the "+
			"gateway's scheduleAfterUpstream and against the schedule_type CHECK in migration 100",
			ModelRefreshTrigger, "after_upstream")
	}
}

// The rule coalescing must not break. A batch can mix a shallow chain with a deep one —
// two producers of the same model reached by routes of different length — and the batch
// collapses to one rebuild. Taking the LATEST request's depth would launder a chain that
// is already seven hops deep into one that reports two, and the gateway's bound would
// then never stop the ring it exists to stop.
func TestModelRefreshWorkflow_CoalescingKeepsTheDeepestChain(t *testing.T) {
	probe := &modelRefreshProbe{}
	env := newModelRefreshEnv(probe)
	probe.onCall = func(n int) {
		if n != 1 {
			return
		}
		// Deep first, then shallow: the last one to arrive is the one a naive
		// implementation keeps, so this ordering is what makes the bug visible.
		env.SignalWorkflow(ModelRefreshSignalName, ModelRefreshRequest{
			ScheduleID: "sched-a", UpstreamKind: "model", UpstreamID: "model-0", ExecutionID: "exec-2", Depth: 7,
		})
		env.SignalWorkflow(ModelRefreshSignalName, ModelRefreshRequest{
			ScheduleID: "sched-b", UpstreamKind: "pipeline", UpstreamID: "pipe-1", ExecutionID: "exec-3", Depth: 2,
		})
	}

	signalAt(env, time.Millisecond, ModelRefreshRequest{
		ScheduleID: "sched-1", UpstreamKind: "model", UpstreamID: "model-0", ExecutionID: "exec-1", Depth: 5,
	})
	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned an error: %v", err)
	}

	calls := probe.recorded()
	if len(calls) != 2 {
		t.Fatalf("three completions produced %d rebuilds, want 2: %+v", len(calls), calls)
	}
	// The first rebuild is the one signal it saw, so this is the plain propagation check:
	// depth reaches the activity at all, unchanged.
	if calls[0].Depth != 5 {
		t.Errorf("the first rebuild ran at depth %d, want the 5 its signal carried", calls[0].Depth)
	}
	if calls[1].Depth != 7 {
		t.Errorf("the coalesced rebuild ran at depth %d, want 7 — the deepest request in the "+
			"batch, not the last one (which carried 2)", calls[1].Depth)
	}
}

// A signal that was already in flight when the gateway was upgraded past migration 100
// carries pipeline_id, not upstream_kind/upstream_id. The upstream fields are history
// only — the rebuild reads whatever the tables now hold — so the old payload must still
// rebuild rather than fail. Failing it would drop exactly the completions that arrived
// during a deploy.
func TestModelRefreshWorkflow_APreMigration100PayloadStillRebuilds(t *testing.T) {
	probe := &modelRefreshProbe{}
	env := newModelRefreshEnv(probe)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ModelRefreshSignalName, map[string]any{
			"schedule_id":  "sched-1",
			"pipeline_id":  "pipe-1",
			"execution_id": "exec-1",
		})
	}, time.Millisecond)
	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("an old-shaped signal failed the workflow: %v", err)
	}

	calls := probe.recorded()
	if len(calls) != 1 {
		t.Fatalf("an old-shaped signal produced %d rebuilds, want 1: %+v", len(calls), calls)
	}
	if calls[0].ScheduleID != "sched-1" || calls[0].SavedQueryID != "model-1" {
		t.Errorf("rebuild ran with %+v, want the model and schedule the old payload named", calls[0])
	}
	// The dropped pipeline_id must not be read back as a depth from somewhere: an
	// unknown-shaped signal that arrived at depth 0 is a chain root, which is the
	// safe reading and the one the gateway's bound assumes.
	if calls[0].Depth != 0 {
		t.Errorf("an old-shaped signal rebuilt at depth %d, want 0", calls[0].Depth)
	}
}

// TestModelRefreshWorkflow_TheRebuildCarriesWhatWokeItAndHowManyItAbsorbed: the run row
// the gateway writes can only say which upstream run woke a rebuild, and how many
// completions it absorbed, if the activity input carries them. The coalesced rebuild
// names the LAST completion and counts both.
func TestModelRefreshWorkflow_TheRebuildCarriesWhatWokeItAndHowManyItAbsorbed(t *testing.T) {
	probe := &modelRefreshProbe{}
	env := newModelRefreshEnv(probe)
	probe.onCall = func(n int) {
		if n != 1 {
			return
		}
		env.SignalWorkflow(ModelRefreshSignalName, ModelRefreshRequest{
			ScheduleID: "sched-1", UpstreamKind: "model", UpstreamID: "up-a", UpstreamRunID: "run-a", Depth: 2,
		})
		env.SignalWorkflow(ModelRefreshSignalName, ModelRefreshRequest{
			ScheduleID: "sched-1", UpstreamKind: "model", UpstreamID: "up-b", UpstreamRunID: "run-b", ExecutionID: "exec-9", Depth: 2,
		})
	}
	signalAt(env, time.Millisecond, ModelRefreshRequest{
		ScheduleID: "sched-1", UpstreamKind: "pipeline", UpstreamID: "pipe-1", ExecutionID: "exec-1",
	})
	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-1"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned an error: %v", err)
	}

	calls := probe.recorded()
	if len(calls) != 2 {
		t.Fatalf("got %d rebuilds, want 2: %+v", len(calls), calls)
	}
	first := calls[0]
	if first.UpstreamKind != "pipeline" || first.UpstreamID != "pipe-1" || first.ExecutionID != "exec-1" || first.Coalesced != 1 {
		t.Errorf("first rebuild carried %+v, want pipeline/pipe-1 exec-1 coalesced 1", first)
	}
	second := calls[1]
	if second.UpstreamKind != "model" || second.UpstreamID != "up-b" || second.UpstreamRunID != "run-b" ||
		second.ExecutionID != "exec-9" || second.Coalesced != 2 {
		t.Errorf("coalesced rebuild carried %+v, want model/up-b run-b exec-9 coalesced 2", second)
	}
}
