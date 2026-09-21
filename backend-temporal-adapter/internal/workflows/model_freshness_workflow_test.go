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

// Freshness sweep tests.
//
// What is worth guarding here is not that a sweep happens — the sweep's own judgement is
// tested in the gateway, where it is pure. It is that the loop CANNOT STOP. This
// workflow is the only thing in the product that notices an absence, so every way it
// could quietly end is a way the whole feature silently turns off: a gateway restart, an
// eight-hour history bound, a boot ordering that spends its first retries on connection
// refusals. None of those produce an error anywhere. They produce a workspace where
// nothing is ever reported stale, which is indistinguishable from a workspace that is
// fine.
//
// The probe replaces SweepModelFreshnessActivity under the same name, so nothing here
// reaches an api-gateway or a database.

type freshnessSweepProbe struct {
	mu    sync.Mutex
	calls int
	// err, when set, is returned by every call.
	err error
}

func (p *freshnessSweepProbe) sweep(context.Context) (ModelFreshnessSweepResult, error) {
	p.mu.Lock()
	p.calls++
	err := p.err
	p.mu.Unlock()

	if err != nil {
		return ModelFreshnessSweepResult{}, err
	}
	return ModelFreshnessSweepResult{Scanned: 3, Opened: 1}, nil
}

func (p *freshnessSweepProbe) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func newFreshnessEnv(probe *freshnessSweepProbe) *testsuite.TestWorkflowEnvironment {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ModelFreshnessWorkflow)
	env.RegisterActivityWithOptions(probe.sweep, activity.RegisterOptions{Name: "SweepModelFreshnessActivity"})
	return env
}

// The sweep must wait out one interval before its first call. The adapter and the
// gateway come up together, so an immediate sweep spends all three of its retries on
// connection refusals against a gateway that is still binding its port — and then the
// first real sweep is an interval later anyway, having logged a failure that means
// nothing.
func TestFreshnessSweepWaitsOneIntervalBeforeTheFirstSweep(t *testing.T) {
	probe := &freshnessSweepProbe{}
	env := newFreshnessEnv(probe)

	var atAlmostOneInterval, atJustPastOneInterval int
	env.RegisterDelayedCallback(func() {
		atAlmostOneInterval = probe.count()
	}, ModelFreshnessSweepInterval-time.Second)
	env.RegisterDelayedCallback(func() {
		atJustPastOneInterval = probe.count()
	}, ModelFreshnessSweepInterval+time.Second)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, ModelFreshnessSweepInterval+2*time.Second)

	env.ExecuteWorkflow(ModelFreshnessWorkflow, ModelFreshnessInput{})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if atAlmostOneInterval != 0 {
		t.Fatalf("%d sweeps had run one second before the first interval elapsed, want 0 — the loop swept before sleeping", atAlmostOneInterval)
	}
	if atJustPastOneInterval != 1 {
		t.Fatalf("%d sweeps had run one second after the first interval, want 1", atJustPastOneInterval)
	}
}

// A workflow that ends is a monitor that is off, and nothing reports a monitor being
// off. A gateway that is down or restarting must therefore cost detection latency and
// nothing else.
func TestAFailingGatewayDoesNotEndTheSweep(t *testing.T) {
	probe := &freshnessSweepProbe{err: errors.New("freshness sweep endpoint returned http 502")}
	env := newFreshnessEnv(probe)

	env.ExecuteWorkflow(ModelFreshnessWorkflow, ModelFreshnessInput{})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	err := env.GetWorkflowError()
	var can *workflow.ContinueAsNewError
	if !errors.As(err, &can) {
		t.Fatalf("workflow ended with %v, want a continue-as-new — a failing gateway must not stop the only thing that notices absences", err)
	}
	if got := probe.count(); got < modelFreshnessTicksPerRun {
		t.Fatalf("the sweep was attempted on %d ticks, want at least %d — it gave up before the history bound", got, modelFreshnessTicksPerRun)
	}
}

// The singleton never ends on its own, so its history never stops growing on its own
// either. Without the bound the failure arrives months after the deploy, as a workflow
// too large to make progress.
func TestTheSweepContinuesAsNewAtItsHistoryBound(t *testing.T) {
	probe := &freshnessSweepProbe{}
	env := newFreshnessEnv(probe)

	env.ExecuteWorkflow(ModelFreshnessWorkflow, ModelFreshnessInput{})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	var can *workflow.ContinueAsNewError
	if !errors.As(env.GetWorkflowError(), &can) {
		t.Fatalf("workflow ended with %v, want a continue-as-new", env.GetWorkflowError())
	}
	if got := probe.count(); got != modelFreshnessTicksPerRun {
		t.Fatalf("continued as new after %d sweeps, want exactly %d", got, modelFreshnessTicksPerRun)
	}
}

// A non-default interval must survive continue-as-new. If it does not, an operator who
// set a five-minute interval gets one for about eight hours and then silently gets sixty
// seconds forever — a drift with a delay long enough that nobody connects the two.
func TestANonDefaultIntervalIsCarriedAcrossContinueAsNew(t *testing.T) {
	probe := &freshnessSweepProbe{}
	env := newFreshnessEnv(probe)

	const custom = 300
	env.ExecuteWorkflow(ModelFreshnessWorkflow, ModelFreshnessInput{IntervalSeconds: custom})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	var can *workflow.ContinueAsNewError
	if !errors.As(env.GetWorkflowError(), &can) {
		t.Fatalf("workflow ended with %v, want a continue-as-new", env.GetWorkflowError())
	}

	var next ModelFreshnessInput
	if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &next); err != nil {
		t.Fatalf("could not decode the continue-as-new argument: %v", err)
	}
	if next.IntervalSeconds != custom {
		t.Fatalf("the next run is started with interval_seconds=%d, want %d — the operator's interval was dropped at the history bound",
			next.IntervalSeconds, custom)
	}
	if next.Ticks != 0 {
		t.Fatalf("the next run is started with ticks=%d, want 0 — a carried-over count would make the next run continue as new immediately, forever", next.Ticks)
	}
}

// The interval is honoured, and it is honoured from the ARGUMENT rather than from the
// environment. Workflow code that read os.Getenv would take a different path on replay
// than it took originally, which is a non-determinism error rather than a wrong number —
// and it would surface on a worker restart, not on the deploy that introduced it.
func TestTheIntervalOverrideChangesHowOftenTheSweepRuns(t *testing.T) {
	probe := &freshnessSweepProbe{}
	env := newFreshnessEnv(probe)

	const custom = 600 // ten minutes, ten times the default

	var atFiveDefaultIntervals int
	env.RegisterDelayedCallback(func() {
		atFiveDefaultIntervals = probe.count()
	}, 5*ModelFreshnessSweepInterval)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 21*time.Minute)

	env.ExecuteWorkflow(ModelFreshnessWorkflow, ModelFreshnessInput{IntervalSeconds: custom})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if atFiveDefaultIntervals != 0 {
		t.Fatalf("%d sweeps had run after five minutes under a ten-minute interval, want 0 — the override was ignored", atFiveDefaultIntervals)
	}
	if got := probe.count(); got != 2 {
		t.Fatalf("%d sweeps ran in 21 minutes under a ten-minute interval, want 2", got)
	}
}
