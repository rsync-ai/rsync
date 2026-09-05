// Worker interceptor that records Prometheus metrics for every activity
// and workflow execution WITHOUT editing each of the ~27 activity and 2
// workflow functions. The Temporal worker calls InterceptActivity /
// InterceptWorkflow once per execution; we wrap the inbound interceptor
// and record outcome + duration at completion.
//
// Determinism note: activity code is ordinary Go and runs exactly once,
// so timing it with time.Now() is safe. Workflow code is replayed from
// history, so the workflow interceptor records counts ONLY when
// workflow.IsReplaying(ctx) is false — otherwise every replay would
// double-count. We deliberately do NOT measure workflow duration here
// (wall-clock in workflow code is non-deterministic); duration lives in
// activity metrics and in SigNoz traces.
//
// F-Obs-2 (Obs-TemporalAdapter).
package metrics

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
)

// NewWorkerInterceptor returns the metrics worker interceptor to pass via
// worker.Options{Interceptors: []interceptor.WorkerInterceptor{...}}.
func NewWorkerInterceptor() interceptor.WorkerInterceptor {
	return &workerInterceptor{}
}

type workerInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func (w *workerInterceptor) InterceptActivity(
	ctx context.Context,
	next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	i := &activityInbound{}
	i.Next = next
	return i
}

func (w *workerInterceptor) InterceptWorkflow(
	ctx workflow.Context,
	next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	i := &workflowInbound{}
	i.Next = next
	return i
}

type activityInbound struct {
	interceptor.ActivityInboundInterceptorBase
}

func (a *activityInbound) ExecuteActivity(
	ctx context.Context,
	in *interceptor.ExecuteActivityInput,
) (interface{}, error) {
	name := activity.GetInfo(ctx).ActivityType.Name
	start := time.Now()

	out, err := a.Next.ExecuteActivity(ctx, in)

	ActivityDurationSeconds.WithLabelValues(name).Observe(time.Since(start).Seconds())
	ActivityExecutionsTotal.WithLabelValues(name, resultLabel(err)).Inc()
	return out, err
}

type workflowInbound struct {
	interceptor.WorkflowInboundInterceptorBase
}

func (wi *workflowInbound) ExecuteWorkflow(
	ctx workflow.Context,
	in *interceptor.ExecuteWorkflowInput,
) (interface{}, error) {
	name := workflow.GetInfo(ctx).WorkflowType.Name

	out, err := wi.Next.ExecuteWorkflow(ctx, in)

	// Count once per real completion, not on each history replay.
	if !workflow.IsReplaying(ctx) {
		WorkflowExecutionsTotal.WithLabelValues(name, resultLabel(err)).Inc()
	}
	return out, err
}

func resultLabel(err error) string {
	if err != nil {
		return "failure"
	}
	return "success"
}
