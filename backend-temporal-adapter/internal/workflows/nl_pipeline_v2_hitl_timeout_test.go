package workflows

import (
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// HITL wait-state deadline tests.
//
// Every HITL park in the V2 workflow computes a WaitReason.TimeoutAt and, before
// this change, enforced it nowhere: all six park sites were a bare
// `<chan>.Receive(ctx, &payload)` with no selector and no timer. A pipeline whose
// signal never arrived — nobody ever picks the tables, the required connection
// pairing does not exist in the workspace — blocked forever. The zombie sweeper
// could not rescue it either: sweepZombiesQuery explicitly skips rows with
// pipeline_progress.status='waiting_for_user'
// (backend-orchestrator/internal/agents/heal/worker.go:543-547).
//
// TestAwaitHITLSignalTimesOut is the load-bearing one: under the old code it does
// not fail, it DEADLOCKS — the test environment fires every timer it knows about
// and the workflow is still parked, which is precisely the prod symptom.

const hitlProbeSignal = "hitl-probe-signal"

type hitlProbePayload struct {
	Value string `json:"value"`
}

// hitlProbeWorkflow parks on awaitHITLSignal and reports which way the park ended.
// waitFor is relative so a negative value can express an already-expired deadline.
func hitlProbeWorkflow(ctx workflow.Context, waitFor time.Duration) (string, error) {
	ch := workflow.GetSignalChannel(ctx, hitlProbeSignal)

	var payload hitlProbePayload
	if !awaitHITLSignal(ctx, ch, &payload, workflow.Now(ctx).Add(waitFor)) {
		return "timeout", nil
	}
	return "received:" + payload.Value, nil
}

func runHITLProbe(t *testing.T, waitFor time.Duration, beforeExec func(env *testsuite.TestWorkflowEnvironment)) string {
	t.Helper()

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(hitlProbeWorkflow)
	if beforeExec != nil {
		beforeExec(env)
	}

	env.ExecuteWorkflow(hitlProbeWorkflow, waitFor)

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow did not complete — the park never resolved")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned an error: %v", err)
	}

	var got string
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatalf("could not read workflow result: %v", err)
	}
	return got
}

// TestAwaitHITLSignalTimesOut: the signal never arrives, so the park must end at
// the deadline instead of blocking for the life of the workflow.
func TestAwaitHITLSignalTimesOut(t *testing.T) {
	if got := runHITLProbe(t, 24*time.Hour, nil); got != "timeout" {
		t.Fatalf("park should have timed out, got %q", got)
	}
}

// TestAwaitHITLSignalReceivesBeforeDeadline: the fix must not cost us the normal
// path — a signal that arrives inside the window still wins, payload intact.
func TestAwaitHITLSignalReceivesBeforeDeadline(t *testing.T) {
	got := runHITLProbe(t, 24*time.Hour, func(env *testsuite.TestWorkflowEnvironment) {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(hitlProbeSignal, hitlProbePayload{Value: "tables-picked"})
		}, time.Hour)
	})

	if got != "received:tables-picked" {
		t.Fatalf("signal inside the window should win, got %q", got)
	}
}

// TestAwaitHITLSignalExpiredDeadlineDoesNotBlock: a resumed workflow can re-enter
// a park whose deadline already passed. That must return immediately rather than
// arming a negative-duration timer.
func TestAwaitHITLSignalExpiredDeadlineDoesNotBlock(t *testing.T) {
	if got := runHITLProbe(t, -time.Second, nil); got != "timeout" {
		t.Fatalf("already-expired deadline should not block, got %q", got)
	}
}

// TestNoBareHITLChannelReceives is a structural guard, not a behavioral one.
//
// Fixing the six known park sites does not stop a seventh from being added as a
// bare Receive — and a new unbounded park is invisible in testing precisely
// because "waits forever" looks identical to "waiting legitimately". So assert
// the property at the source level: every HITL signal channel must be consumed
// through awaitHITLSignal, which is the only place TimeoutAt is enforced.
func TestNoBareHITLChannelReceives(t *testing.T) {
	const src = "nl_pipeline_v2_workflow.go"

	body, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}

	// Matches `somethingCh.Receive(ctx` — the bare blocking form. Receives made
	// inside awaitHITLSignal use the selector callback's channel parameter (`c`),
	// so they do not match this pattern.
	bare := regexp.MustCompile(`(\w*Ch)\.Receive\(ctx`)

	var offenders []string
	for i, line := range strings.Split(string(body), "\n") {
		if m := bare.FindStringSubmatch(line); m != nil {
			offenders = append(offenders, strings.TrimSpace(line)+"  ("+src+":"+itoa(i+1)+")")
		}
	}

	if len(offenders) > 0 {
		t.Fatalf("HITL park(s) bypass awaitHITLSignal, so WaitReason.TimeoutAt is unenforced there:\n  %s",
			strings.Join(offenders, "\n  "))
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
