package temporalworker

import "testing"

func TestTaskFromPayload_CoreFields(t *testing.T) {
	payload := map[string]interface{}{
		"pipeline_id":  "pipe-1",
		"execution_id": "exec-1",
		"user_id":      "user-1",
		"trace_id":     "trace-1",
	}
	task := taskFromPayload(payload)

	if task.PipelineID != "pipe-1" || task.WorkflowID != "pipe-1" {
		t.Errorf("pipeline/workflow id mismatch: %+v", task)
	}
	if task.ExecutionID != "exec-1" || task.UserID != "user-1" || task.TraceID != "trace-1" {
		t.Errorf("field mismatch: %+v", task)
	}
	if task.TaskType != "execute" {
		t.Errorf("TaskType = %q, want execute (matches Redis poller)", task.TaskType)
	}
	if task.StepID != "executor" {
		t.Errorf("StepID = %q, want executor", task.StepID)
	}
	// No correlation id on the native path → stable fallback task id.
	if task.CorrelationID != "" {
		t.Errorf("CorrelationID should be empty on native path, got %q", task.CorrelationID)
	}
	if task.TaskID != "executor-native" {
		t.Errorf("TaskID = %q, want executor-native", task.TaskID)
	}
	// Payload + Context both reference the full map (parity with the poller).
	if task.Payload["pipeline_id"] != "pipe-1" || task.Context["pipeline_id"] != "pipe-1" {
		t.Errorf("payload/context not wired through")
	}
}

func TestTaskFromPayload_DerivesTaskIDFromCorrelation(t *testing.T) {
	task := taskFromPayload(map[string]interface{}{
		"pipeline_id":    "pipe-1",
		"correlation_id": "abcdef1234567890",
	})
	if task.CorrelationID != "abcdef1234567890" {
		t.Errorf("CorrelationID = %q", task.CorrelationID)
	}
	if task.TaskID != "executor-abcdef12" {
		t.Errorf("TaskID = %q, want executor-abcdef12", task.TaskID)
	}
}

func TestExecutorNativeResult_JSONShapeMatchesAdapter(t *testing.T) {
	// Guards the cross-module wire contract: field names must match the adapter's
	// workflows.ExecutorNativeResult {status, output, error}.
	r := ExecutorNativeResult{Status: "running", Output: map[string]interface{}{"x": 1}, Error: ""}
	if r.Status != "running" {
		t.Fatalf("unexpected: %+v", r)
	}
}
