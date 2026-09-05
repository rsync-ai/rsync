// Package temporalworker hosts a native Temporal worker inside the orchestrator
// process so the executor stage can run as a Temporal activity on a dedicated
// task queue instead of via the Redis correlation hop.
//
// This is Phase 3 of removing Kafka/Redis from the agent-orchestration dispatch
// path: Temporal task queues natively provide load-balanced dispatch, retries,
// worker redundancy and durable request/reply — the exact job the Kafka+Redis
// layer reimplemented. Only the executor agent is migrated here; the other
// agents stay on Redis correlation for now.
//
// The activity reuses the EXISTING ExecutorWorker.Execute unchanged, so the MCP
// registry, progress emitter, agent.executor.responses UI stream and all
// data-plane behavior are identical to the correlation path.
package temporalworker

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/rsync-ai/backend-orchestrator/internal/workers"
)

// ExecutorTaskQueue MUST match the adapter's workflows.ExecutorTaskQueue.
const ExecutorTaskQueue = "executor-tasks"

// ExecutorNativeActivityName MUST match the string the workflow invokes via
// workflow.ExecuteActivity(ctx, "ExecutorNativeActivity", ...).
const ExecutorNativeActivityName = "ExecutorNativeActivity"

// ExecutorNativeResult is the wire contract returned to the workflow. Its JSON
// shape MUST stay in lockstep with the adapter's workflows.ExecutorNativeResult.
type ExecutorNativeResult struct {
	Status string                 `json:"status"`
	Output map[string]interface{} `json:"output,omitempty"`
	Error  string                 `json:"error,omitempty"`
}

// Executor is the subset of *workers.ExecutorWorker the native activity needs.
// Kept as an interface so the activity is unit-testable without a full worker.
type Executor interface {
	Execute(ctx context.Context, task workers.Task) workers.TaskResult
}

// Worker owns the Temporal client + worker hosting ExecutorNativeActivity.
type Worker struct {
	client client.Client
	worker worker.Worker
}

// New dials Temporal and registers ExecutorNativeActivity on ExecutorTaskQueue.
// The caller is responsible for Start()/Stop(). TEMPORAL_ADDRESS defaults to
// "temporal:7233" to match the adapter.
func New(exec Executor) (*Worker, error) {
	if exec == nil {
		return nil, fmt.Errorf("temporalworker: nil executor")
	}
	addr := os.Getenv("TEMPORAL_ADDRESS")
	if addr == "" {
		addr = "temporal:7233"
	}
	c, err := client.Dial(client.Options{HostPort: addr, Namespace: "default"})
	if err != nil {
		return nil, fmt.Errorf("temporalworker: dial %s: %w", addr, err)
	}

	w := worker.New(c, ExecutorTaskQueue, worker.Options{})
	act := &executorActivity{exec: exec}
	w.RegisterActivityWithOptions(act.run, activity.RegisterOptions{Name: ExecutorNativeActivityName})

	return &Worker{client: c, worker: w}, nil
}

// Start begins polling the executor-tasks queue (non-blocking).
func (x *Worker) Start() error { return x.worker.Start() }

// Stop drains the worker and closes the Temporal client.
func (x *Worker) Stop() {
	if x.worker != nil {
		x.worker.Stop()
	}
	if x.client != nil {
		x.client.Close()
	}
}

type executorActivity struct {
	exec Executor
}

// run is the ExecutorNativeActivity implementation. It receives the same task
// payload the correlation path writes to Redis, calls ExecutorWorker.Execute,
// and returns the raw status/output/error for the workflow to classify (via the
// shared classifyExecutorResponse on the adapter side). Heartbeats keep
// long-running batch/CDC starts alive against the HeartbeatTimeout.
func (a *executorActivity) run(ctx context.Context, payload map[string]interface{}) (ExecutorNativeResult, error) {
	logger := activity.GetLogger(ctx)
	task := taskFromPayload(payload)
	logger.Info("ExecutorNativeActivity starting",
		"pipeline_id", task.PipelineID,
		"execution_id", task.ExecutionID,
	)

	// Heartbeat on a ticker so executions running for minutes (batch loads,
	// CDC startup) are not considered stalled. Stops when run() returns.
	hbCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx, "executing", task.PipelineID)
			}
		}
	}()

	result := a.exec.Execute(ctx, task)

	logger.Info("ExecutorNativeActivity completed",
		"status", result.Status,
		"pipeline_id", task.PipelineID,
	)
	return ExecutorNativeResult{
		Status: result.Status,
		Output: result.Output,
		Error:  result.Error,
	}, nil
}

// taskFromPayload converts the executor task payload (identical to the map the
// correlation path puts in correlation.Request.Task) into a workers.Task. This
// mirrors the conversion in executor_redis_polling.go processCorrelationRequest
// so both dispatch paths feed Execute the same shape.
func taskFromPayload(payload map[string]interface{}) workers.Task {
	get := func(k string) string {
		if v, ok := payload[k]; ok && v != nil {
			return fmt.Sprintf("%v", v)
		}
		return ""
	}

	pipelineID := get("pipeline_id")
	correlationID := get("correlation_id") // empty on the native path

	taskID := "executor-native"
	if len(correlationID) >= 8 {
		taskID = "executor-" + correlationID[:8]
	}

	return workers.Task{
		CorrelationID: correlationID,
		TaskType:      "execute", // matches what the Redis poller sets
		Payload:       payload,
		Context:       payload,
		PipelineID:    pipelineID,
		ExecutionID:   get("execution_id"),
		UserID:        get("user_id"),
		TaskID:        taskID,
		WorkflowID:    pipelineID,
		StepID:        "executor",
		TraceID:       get("trace_id"),
	}
}
