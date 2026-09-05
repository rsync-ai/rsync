package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Activities/workflows below use unique type names so their series do not
// collide with the names used in metrics_test.go — that lets us assert exact
// counter deltas rather than merely "a series exists".

func okActivity(ctx context.Context) (string, error)   { return "ok", nil }
func boomActivity(ctx context.Context) (string, error) { return "", errors.New("boom") }

func okWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})
	return workflow.ExecuteActivity(ctx, "InterceptedOKActivity").Get(ctx, nil)
}

func workerOpts() worker.Options {
	return worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{NewWorkerInterceptor()},
	}
}

// TestInterceptorRecordsActivitySuccess proves the interceptor — not a manual
// .Inc() — drives the counters when a real activity runs through it.
func TestInterceptorRecordsActivitySuccess(t *testing.T) {
	before := testutil.ToFloat64(ActivityExecutionsTotal.WithLabelValues("InterceptedOKActivity", "success"))
	durBefore := testutil.CollectAndCount(ActivityDurationSeconds)

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.SetWorkerOptions(workerOpts())
	env.RegisterActivityWithOptions(okActivity, activity.RegisterOptions{Name: "InterceptedOKActivity"})

	if _, err := env.ExecuteActivity("InterceptedOKActivity"); err != nil {
		t.Fatalf("ExecuteActivity returned unexpected error: %v", err)
	}

	after := testutil.ToFloat64(ActivityExecutionsTotal.WithLabelValues("InterceptedOKActivity", "success"))
	if after-before != 1 {
		t.Fatalf("success counter delta = %v, want 1", after-before)
	}
	// A duration series for this activity must now exist.
	if durAfter := testutil.CollectAndCount(ActivityDurationSeconds); durAfter <= durBefore {
		t.Fatalf("duration series count did not grow: before=%d after=%d", durBefore, durAfter)
	}
}

// TestInterceptorRecordsActivityFailure proves a failing activity lands on the
// "failure" result label (resultLabel wired through the real Next call).
func TestInterceptorRecordsActivityFailure(t *testing.T) {
	before := testutil.ToFloat64(ActivityExecutionsTotal.WithLabelValues("InterceptedBoomActivity", "failure"))

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.SetWorkerOptions(workerOpts())
	env.RegisterActivityWithOptions(boomActivity, activity.RegisterOptions{Name: "InterceptedBoomActivity"})

	if _, err := env.ExecuteActivity("InterceptedBoomActivity"); err == nil {
		t.Fatal("expected boomActivity to return an error")
	}

	after := testutil.ToFloat64(ActivityExecutionsTotal.WithLabelValues("InterceptedBoomActivity", "failure"))
	if after-before != 1 {
		t.Fatalf("failure counter delta = %v, want 1", after-before)
	}
}

// TestInterceptorRecordsWorkflow proves the workflow interceptor increments
// exactly once per completion (the !IsReplaying guard does not suppress the
// real, non-replay completion in the test environment).
func TestInterceptorRecordsWorkflow(t *testing.T) {
	before := testutil.ToFloat64(WorkflowExecutionsTotal.WithLabelValues("okWorkflow", "success"))

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(workerOpts())
	env.RegisterActivityWithOptions(okActivity, activity.RegisterOptions{Name: "InterceptedOKActivity"})
	env.RegisterWorkflow(okWorkflow)

	env.ExecuteWorkflow(okWorkflow)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned unexpected error: %v", err)
	}

	after := testutil.ToFloat64(WorkflowExecutionsTotal.WithLabelValues("okWorkflow", "success"))
	if after-before != 1 {
		t.Fatalf("workflow success counter delta = %v, want 1 (replay double-count or miss?)", after-before)
	}
}
