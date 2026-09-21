package workflows

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// Upstream fan-out tests.
//
// What is worth guarding here is not "a model gets rebuilt" — ModelRefreshWorkflow owns
// that and is tested where it lives. It is the arithmetic of the fan-out: a completion
// with five downstream models must produce five dispatches, exactly once each, carrying
// the depth that bounds the chain, and one model that cannot be reached must not take the
// other four with it. Every one of those failures is silent — the fan-out reports nothing
// and the models simply keep serving yesterday's data.

// fanOutProbe records what each dispatch child was handed.
type fanOutProbe struct {
	mu    sync.Mutex
	calls []ModelRefreshDispatchInput
	// enteredAt is the workflow clock as each child began. Children started in parallel
	// all see the same instant; children awaited one at a time do not.
	enteredAt []time.Time
	// failFor makes one model's dispatch fail the way an unreachable one would.
	failFor string
	// hold is how long each child occupies before returning.
	hold time.Duration
}

func (p *fanOutProbe) child(ctx workflow.Context, in ModelRefreshDispatchInput) error {
	p.mu.Lock()
	p.calls = append(p.calls, in)
	p.enteredAt = append(p.enteredAt, workflow.Now(ctx))
	fail := p.failFor == in.SavedQueryID
	hold := p.hold
	p.mu.Unlock()

	if hold > 0 {
		_ = workflow.Sleep(ctx, hold)
	}
	if fail {
		return errors.New("could not signal the model")
	}
	return nil
}

func (p *fanOutProbe) recorded() []ModelRefreshDispatchInput {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ModelRefreshDispatchInput(nil), p.calls...)
}

// newFanOutEnv wires the probe in as the dispatch child, so nothing here reaches a
// Temporal client or a signal.
func newFanOutEnv(probe *fanOutProbe) *testsuite.TestWorkflowEnvironment {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(UpstreamFanOutWorkflow)
	env.RegisterWorkflowWithOptions(probe.child, workflow.RegisterOptions{Name: "ModelRefreshDispatchWorkflow"})
	return env
}

func pipelineFanOut(targets ...UpstreamFanOutTarget) UpstreamFanOutInput {
	return UpstreamFanOutInput{
		UpstreamKind: "pipeline",
		UpstreamID:   "p1",
		ExecutionID:  "e1",
		Targets:      targets,
	}
}

func TestUpstreamFanOutWorkflow_EveryModelGetsItsOwnDispatch(t *testing.T) {
	// The count is the whole point. The gateway used to issue these one at a time from a
	// goroutine that died with the process, so a restart partway through left the rest of
	// the models owed a rebuild with nothing recording that they were owed one.
	probe := &fanOutProbe{}
	env := newFanOutEnv(probe)
	env.ExecuteWorkflow(UpstreamFanOutWorkflow, pipelineFanOut(
		UpstreamFanOutTarget{ScheduleID: "s1", SavedQueryID: "m1"},
		UpstreamFanOutTarget{ScheduleID: "s2", SavedQueryID: "m2"},
		UpstreamFanOutTarget{ScheduleID: "s3", SavedQueryID: "m3"},
	))

	if !env.IsWorkflowCompleted() {
		t.Fatal("fan-out did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("fan-out failed: %v", err)
	}
	got := probe.recorded()
	if len(got) != 3 {
		t.Fatalf("%d models were dispatched, want 3", len(got))
	}
	seen := map[string]string{}
	for _, in := range got {
		seen[in.SavedQueryID] = in.ScheduleID
	}
	for model, schedule := range map[string]string{"m1": "s1", "m2": "s2", "m3": "s3"} {
		if seen[model] != schedule {
			t.Errorf("model %s was dispatched under schedule %q, want %q", model, seen[model], schedule)
		}
	}
}

func TestUpstreamFanOutWorkflow_TheCompletionAndItsDepthReachEveryChild(t *testing.T) {
	// Depth is the only thing that stops a rebuild ring, and it has to survive this hop as
	// well as the signal. A child that lost it would report a chain eight hops deep as
	// depth zero, and the bound would never fire.
	probe := &fanOutProbe{}
	env := newFanOutEnv(probe)
	env.ExecuteWorkflow(UpstreamFanOutWorkflow, UpstreamFanOutInput{
		UpstreamKind:  "model",
		UpstreamID:    "m0",
		UpstreamRunID: "r0",
		ExecutionID:   "e9",
		Depth:         4,
		Targets: []UpstreamFanOutTarget{
			{ScheduleID: "s1", SavedQueryID: "m1"},
			{ScheduleID: "s2", SavedQueryID: "m2"},
		},
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("fan-out failed: %v", err)
	}
	for _, in := range probe.recorded() {
		if in.UpstreamKind != "model" || in.UpstreamID != "m0" || in.ExecutionID != "e9" {
			t.Errorf("child for %s got upstream %s/%s exec %q, want model/m0 exec e9",
				in.SavedQueryID, in.UpstreamKind, in.UpstreamID, in.ExecutionID)
		}
		// The upstream's run row is what lets the child's own run name the run that woke
		// it; a hop that drops it leaves the history unable to walk the chain back.
		if in.UpstreamRunID != "r0" {
			t.Errorf("child for %s got upstream run %q, want r0", in.SavedQueryID, in.UpstreamRunID)
		}
		if in.Depth != 4 {
			t.Errorf("child for %s got depth %d, want 4; a hop that loses the depth unbounds the chain",
				in.SavedQueryID, in.Depth)
		}
	}
}

func TestUpstreamFanOutWorkflow_ChildrenRunInParallel(t *testing.T) {
	// Started in one pass and awaited in a second. Awaiting inside the start loop would
	// make a fan-out to thirty models thirty round trips deep, and a slow first model
	// would delay every model behind it.
	probe := &fanOutProbe{hold: time.Minute}
	env := newFanOutEnv(probe)
	env.ExecuteWorkflow(UpstreamFanOutWorkflow, pipelineFanOut(
		UpstreamFanOutTarget{ScheduleID: "s1", SavedQueryID: "m1"},
		UpstreamFanOutTarget{ScheduleID: "s2", SavedQueryID: "m2"},
		UpstreamFanOutTarget{ScheduleID: "s3", SavedQueryID: "m3"},
	))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("fan-out failed: %v", err)
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if len(probe.enteredAt) != 3 {
		t.Fatalf("%d children ran, want 3", len(probe.enteredAt))
	}
	for i, at := range probe.enteredAt {
		if !at.Equal(probe.enteredAt[0]) {
			// Serialized starts show up as each child entering a full hold later than the
			// one before it.
			t.Errorf("child %d started %v after the first, want every child started at once",
				i, at.Sub(probe.enteredAt[0]))
		}
	}
}

func TestUpstreamFanOutWorkflow_AModelListedTwiceIsDispatchedOnce(t *testing.T) {
	// Two targets naming one model would collide on the child workflow id, and the second
	// start would be refused — failing a fan-out that had in fact delivered everything.
	// Collapsing is also plainly correct: one completion owes a model one rebuild however
	// many rows say so.
	probe := &fanOutProbe{}
	env := newFanOutEnv(probe)
	env.ExecuteWorkflow(UpstreamFanOutWorkflow, pipelineFanOut(
		UpstreamFanOutTarget{ScheduleID: "s1", SavedQueryID: "m1"},
		UpstreamFanOutTarget{ScheduleID: "s2", SavedQueryID: "m1"},
		UpstreamFanOutTarget{ScheduleID: "s3", SavedQueryID: "m2"},
	))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("fan-out failed: %v", err)
	}
	got := probe.recorded()
	if len(got) != 2 {
		t.Fatalf("%d dispatches for 3 targets naming 2 models, want 2: %+v", len(got), got)
	}
}

func TestUpstreamFanOutWorkflow_ATargetWithNoModelIsSkippedNotFatal(t *testing.T) {
	// A target with nothing to address cannot be dispatched. Failing the fan-out over it
	// would cost every other model its rebuild for a row that was never actionable.
	probe := &fanOutProbe{}
	env := newFanOutEnv(probe)
	env.ExecuteWorkflow(UpstreamFanOutWorkflow, pipelineFanOut(
		UpstreamFanOutTarget{ScheduleID: "s1", SavedQueryID: ""},
		UpstreamFanOutTarget{ScheduleID: "s2", SavedQueryID: "m2"},
	))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("fan-out failed over an unaddressable target: %v", err)
	}
	got := probe.recorded()
	if len(got) != 1 || got[0].SavedQueryID != "m2" {
		t.Fatalf("dispatches = %+v, want only the addressable model", got)
	}
}

func TestUpstreamFanOutWorkflow_NoTargetsIsANoOp(t *testing.T) {
	probe := &fanOutProbe{}
	env := newFanOutEnv(probe)
	env.ExecuteWorkflow(UpstreamFanOutWorkflow, pipelineFanOut())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("an empty fan-out failed: %v", err)
	}
	if got := probe.recorded(); len(got) != 0 {
		t.Fatalf("%d dispatches for no targets", len(got))
	}
}

func TestUpstreamFanOutWorkflow_OneUnreachableModelDoesNotCostTheOthers(t *testing.T) {
	// The children are already started by the time any of them is awaited, so one that
	// cannot be delivered is collected and reported rather than abandoning the rest. The
	// failure still has to surface: a fan-out that swallowed it would report a completion
	// as fully delivered while one model quietly stopped updating.
	probe := &fanOutProbe{failFor: "m2"}
	env := newFanOutEnv(probe)
	env.ExecuteWorkflow(UpstreamFanOutWorkflow, pipelineFanOut(
		UpstreamFanOutTarget{ScheduleID: "s1", SavedQueryID: "m1"},
		UpstreamFanOutTarget{ScheduleID: "s2", SavedQueryID: "m2"},
		UpstreamFanOutTarget{ScheduleID: "s3", SavedQueryID: "m3"},
	))

	if got := probe.recorded(); len(got) != 3 {
		t.Fatalf("%d models were dispatched, want all 3 despite one failing", len(got))
	}
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("a fan-out that could not reach a model reported success")
	}
	if !strings.Contains(err.Error(), "m2") {
		t.Errorf("the failure does not name the model that was missed: %v", err)
	}
	if strings.Contains(err.Error(), "m1") || strings.Contains(err.Error(), "m3") {
		t.Errorf("the failure names models that were delivered: %v", err)
	}
}

// ============================================================================
// One child: delivering one completion to one model
// ============================================================================

// dispatchProbe stands in for SignalModelRefreshActivity.
type dispatchProbe struct {
	mu    sync.Mutex
	calls int
	// failFirst makes the first n attempts fail the way a rate-limited frontend would.
	failFirst int
}

func (p *dispatchProbe) run(context.Context, ModelRefreshDispatchInput) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls <= p.failFirst {
		return errors.New("temporal frontend is busy")
	}
	return nil
}

func newDispatchEnv(probe *dispatchProbe) *testsuite.TestWorkflowEnvironment {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ModelRefreshDispatchWorkflow)
	env.RegisterActivityWithOptions(probe.run, activity.RegisterOptions{Name: "SignalModelRefreshActivity"})
	return env
}

func TestModelRefreshDispatchWorkflow_ATransientFailureIsRetried(t *testing.T) {
	// This retry is the durability the child exists for. On the goroutine path a signal
	// that failed was one log line and a model that never rebuilt.
	probe := &dispatchProbe{failFirst: 2}
	env := newDispatchEnv(probe)
	env.ExecuteWorkflow(ModelRefreshDispatchWorkflow, ModelRefreshDispatchInput{SavedQueryID: "m1", ScheduleID: "s1"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("dispatch gave up on a transient failure: %v", err)
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if probe.calls != 3 {
		t.Errorf("dispatch made %d attempts, want 3 (two failures then the delivery)", probe.calls)
	}
}

func TestModelRefreshDispatchWorkflow_APermanentFailureSurfaces(t *testing.T) {
	// The other half. A dispatch that never lands has to end as a failed child, because
	// that child execution is the only record that this model was owed a rebuild.
	probe := &dispatchProbe{failFirst: 1 << 30}
	env := newDispatchEnv(probe)
	env.ExecuteWorkflow(ModelRefreshDispatchWorkflow, ModelRefreshDispatchInput{SavedQueryID: "m1"})

	if env.GetWorkflowError() == nil {
		t.Fatal("a dispatch that never delivered reported success")
	}
}

func TestSignalModelRefreshActivity_ADispatchWithNoModelIsNotRetried(t *testing.T) {
	// There is no model to signal and no amount of retrying invents one. Left retryable it
	// would occupy the child for the full horizon before failing anyway.
	err := SignalModelRefreshActivity(context.Background(), ModelRefreshDispatchInput{ScheduleID: "s1"})
	if err == nil {
		t.Fatal("a dispatch with no saved_query_id was accepted")
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Fatalf("error is %T (%v), want a non-retryable application error", err, err)
	}
}

func TestSignalModelRefreshActivity_NoClientFailsRatherThanSilentlySucceeding(t *testing.T) {
	// A worker that came up without SetTemporalClient must not accept dispatches and drop
	// them. Returning a retryable error is what makes the fan-out visibly stuck instead of
	// invisibly empty.
	prev := activityCtx
	t.Cleanup(func() { activityCtx = prev })
	activityCtx = &ActivityContext{}

	err := SignalModelRefreshActivity(context.Background(), ModelRefreshDispatchInput{SavedQueryID: "m1"})
	if err == nil {
		t.Fatal("a dispatch with no Temporal client reported success")
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.NonRetryable() {
		t.Fatal("a missing client was reported as permanent; a worker that boots late would drop every dispatch")
	}
	if !strings.Contains(err.Error(), ModelRefreshWorkflowID("m1")) {
		t.Errorf("the error does not name the refresh loop it could not reach: %v", err)
	}
}

// ============================================================================
// The two ids the fan-out is built on
// ============================================================================

func TestModelRefreshWorkflowID_IsKeyedOnTheModel(t *testing.T) {
	// The id is what makes two completions of the same pipeline coalesce instead of the
	// second one losing the model's run lock and being dropped, so it must depend on the
	// model and nothing else.
	a := ModelRefreshWorkflowID("m1")
	if a != ModelRefreshWorkflowID("m1") {
		t.Error("the same model produced two different workflow ids")
	}
	if a == ModelRefreshWorkflowID("m2") {
		t.Error("two different models share one workflow id, so one would coalesce into the other")
	}
	if !strings.Contains(a, "m1") {
		t.Errorf("workflow id %q does not name its model", a)
	}
}

func TestModelRefreshDispatchWorkflowID_TwoCompletionsGiveOneModelTwoChildren(t *testing.T) {
	// The child id is derived from the parent's, which already names the completion. If it
	// were keyed on the model alone, the second completion's child would be refused as a
	// duplicate and that model would silently miss the rebuild.
	first := modelRefreshDispatchWorkflowID("upstream-fanout:pipeline:p1:e1", "m1")
	second := modelRefreshDispatchWorkflowID("upstream-fanout:pipeline:p1:e2", "m1")
	if first == second {
		t.Errorf("two completions produced one child id %q", first)
	}
	if a := modelRefreshDispatchWorkflowID("upstream-fanout:pipeline:p1:e1", "m2"); a == first {
		t.Error("two models under one completion share a child id, so the second start would be refused")
	}
	if !strings.Contains(first, "m1") {
		t.Errorf("child id %q does not name its model", first)
	}
}
