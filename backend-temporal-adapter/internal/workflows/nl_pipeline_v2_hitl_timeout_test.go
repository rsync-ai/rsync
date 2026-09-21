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
const hitlProbeCancelSignal = "hitl-probe-cancel"

type hitlProbePayload struct {
	Value string `json:"value"`
}

// hitlProbeWorkflow parks on awaitHITLSignal and reports which way the park ended.
// waitFor is relative so a negative value can express an already-expired deadline.
func hitlProbeWorkflow(ctx workflow.Context, waitFor time.Duration) (string, error) {
	ch := workflow.GetSignalChannel(ctx, hitlProbeSignal)

	// Mirrors the real workflow: the cancel signal cancels a child context that the
	// park is handed, and the caller reads its own flag on a false return.
	cancelCtx, cancel := workflow.WithCancel(ctx)
	defer cancel()
	cancelled := false
	workflow.Go(ctx, func(gCtx workflow.Context) {
		workflow.GetSignalChannel(gCtx, hitlProbeCancelSignal).Receive(gCtx, nil)
		cancelled = true
		cancel()
	})

	var payload hitlProbePayload
	if !awaitHITLSignal(ctx, cancelCtx, ch, &payload, workflow.Now(ctx).Add(waitFor)) {
		if cancelled {
			return "cancelled", nil
		}
		return "timeout", nil
	}
	return "received:" + payload.Value, nil
}

func runHITLProbe(t *testing.T, waitFor time.Duration, beforeExec func(env *testsuite.TestWorkflowEnvironment)) string {
	t.Helper()
	got, _ := runHITLProbeTimed(t, waitFor, beforeExec)
	return got
}

var hitlProbeStart = time.Date(2026, 9, 1, 9, 0, 0, 0, time.UTC)

// runHITLProbeTimed also reports how much workflow time the park took: a run that
// was cancelled reads "cancelled" whether the park returned on the cancel or only
// at its deadline, so elapsed time is what tells those apart.
func runHITLProbeTimed(t *testing.T, waitFor time.Duration, beforeExec func(env *testsuite.TestWorkflowEnvironment)) (string, time.Duration) {
	t.Helper()

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetStartTime(hitlProbeStart)
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
	return got, env.Now().Sub(hitlProbeStart)
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

// TestAwaitHITLSignalReturnsOnCancel: the user cancels while the run is parked. The
// park must end at the cancel, not sit there until the 24h deadline.
func TestAwaitHITLSignalReturnsOnCancel(t *testing.T) {
	got, elapsed := runHITLProbeTimed(t, 24*time.Hour, func(env *testsuite.TestWorkflowEnvironment) {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(hitlProbeCancelSignal, nil)
		}, time.Hour)
	})

	if got != "cancelled" {
		t.Fatalf("a cancel inside the window should end the park as cancelled, got %q", got)
	}
	if elapsed >= 2*time.Hour {
		t.Fatalf("park ended %s after start; a cancel at 1h must not wait for the 24h deadline", elapsed)
	}
}

// TestAwaitHITLSignalOnDefaultCancelVersionIgnoresCancel: replay safety. A history
// recorded before hitl-wait-honours-cancel has no marker, so GetVersion returns
// DefaultVersion and the park must behave exactly as it did then: the cancel does
// not end it, and a signal that arrives afterwards is still taken. Same inputs as
// TestAwaitHITLSignalReturnsOnCancel plus a later signal, so the version is the
// only thing that differs.
func TestAwaitHITLSignalOnDefaultCancelVersionIgnoresCancel(t *testing.T) {
	got, elapsed := runHITLProbeTimed(t, 24*time.Hour, func(env *testsuite.TestWorkflowEnvironment) {
		env.OnGetVersion(hitlWaitHonoursCancelVersion, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(hitlProbeCancelSignal, nil)
		}, time.Hour)
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(hitlProbeSignal, hitlProbePayload{Value: "after-cancel"})
		}, 2*time.Hour)
	})

	if got != "received:after-cancel" {
		t.Fatalf("on DefaultVersion the park must ignore the cancel and take the later signal, got %q", got)
	}
	if elapsed < 2*time.Hour {
		t.Fatalf("park ended %s after start, before the 2h signal it should have waited for", elapsed)
	}
}

// hitlProbeCancelledBeforeSelectWorkflow reaches the park with the run already
// cancelled AND a payload already buffered, the one case where selector order
// decides the outcome.
func hitlProbeCancelledBeforeSelectWorkflow(ctx workflow.Context) (string, error) {
	ch := workflow.GetSignalChannel(ctx, hitlProbeSignal)
	cancelCtx, cancel := workflow.WithCancel(ctx)
	defer cancel()
	cancel()

	if err := workflow.Await(ctx, func() bool { return ch.Len() > 0 }); err != nil {
		return "", err
	}
	var payload hitlProbePayload
	if !awaitHITLSignal(ctx, cancelCtx, ch, &payload, workflow.Now(ctx).Add(24*time.Hour)) {
		return "cancelled", nil
	}
	return "received:" + payload.Value, nil
}

// TestAwaitHITLSignalCancelWinsOverPendingSignal: a cancelled run must not resume
// on a payload that happened to be waiting when the park was entered.
func TestAwaitHITLSignalCancelWinsOverPendingSignal(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(hitlProbeCancelledBeforeSelectWorkflow)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(hitlProbeSignal, hitlProbePayload{Value: "late"})
	}, time.Minute)

	env.ExecuteWorkflow(hitlProbeCancelledBeforeSelectWorkflow)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned an error: %v", err)
	}
	var got string
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatalf("could not read workflow result: %v", err)
	}
	if got != "cancelled" {
		t.Fatalf("a cancelled run must not take the pending payload, got %q", got)
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

	// Arming control: the pattern flags the bare park form and leaves the helper's
	// own receives alone, so "no offenders" below means something.
	if !bare.MatchString("tablesSelectedCh.Receive(ctx, &tablePayload)") {
		t.Fatal("pattern does not flag a bare HITL receive; this guard cannot fail")
	}
	for _, helperLine := range []string{"c.Receive(ctx, valuePtr)", "ch.Receive(ctx, valuePtr)"} {
		if bare.MatchString(helperLine) {
			t.Fatalf("pattern flags awaitHITLSignal's own receive %q", helperLine)
		}
	}

	// Denominator: every park records its WaitReason and then waits through
	// awaitHITLSignal. Equal, non-zero counts show the scan saw the parks, and a park
	// that waits some other way (not only a bare Receive) breaks the equality.
	parks := strings.Count(string(body), "state.SetWaitReason(")
	waits := strings.Count(string(body), "awaitHITLSignal(ctx, childCtx, ")
	if parks == 0 || waits != parks {
		t.Fatalf("found %d HITL parks (state.SetWaitReason) but %d awaitHITLSignal waits; every park must wait through the helper", parks, waits)
	}

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
