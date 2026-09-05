package workflows

import "testing"

func TestClassifyExecutorResponse_SuccessStatuses(t *testing.T) {
	out := map[string]interface{}{"rows": 10}
	for _, status := range []string{"success", "completed", "running"} {
		res, aerr := classifyExecutorResponse(status, "", out, "corr-1", nil)
		if aerr != nil {
			t.Fatalf("status %q: expected no error, got %v", status, aerr)
		}
		if res["rows"] != 10 {
			t.Fatalf("status %q: expected output passthrough, got %v", status, res)
		}
	}
}

func TestClassifyExecutorResponse_TableSelection(t *testing.T) {
	res, aerr := classifyExecutorResponse("waiting_for_table_selection", "pick tables", nil, "corr-1", nil)
	if res != nil {
		t.Fatalf("expected nil result, got %v", res)
	}
	if aerr == nil || aerr.Type != ErrTypePolicy || aerr.Code != PolicyCodeTableSelectionRequired {
		t.Fatalf("expected policy/table-selection error, got %v", aerr)
	}
}

func TestClassifyExecutorResponse_GenericFailureIsDeterministic(t *testing.T) {
	res, aerr := classifyExecutorResponse("failed", "boom", map[string]interface{}{"k": "v"}, "corr-9", nil)
	if res != nil {
		t.Fatalf("expected nil result on failure, got %v", res)
	}
	if aerr == nil || aerr.Type != ErrTypeDeterministic || aerr.Code != "EXECUTION_FAILED" {
		t.Fatalf("expected deterministic EXECUTION_FAILED, got %v", aerr)
	}
	// Failure metadata must carry the agent status/error + correlation id for healing.
	if aerr.Metadata["agent_status"] != "failed" || aerr.Metadata["agent_error"] != "boom" || aerr.Metadata["correlation_id"] != "corr-9" {
		t.Fatalf("missing healing metadata: %v", aerr.Metadata)
	}
}

func TestClassifyExecutorResponse_NeedsContinuation(t *testing.T) {
	// A chunked-continuation signal must surface as a POLICY error so the
	// workflow loop re-dispatches the executor to resume from the durable
	// checkpoint — NOT a deterministic failure, and NOT a success that would
	// prematurely complete the pipeline while a large table still has rows.
	out := map[string]interface{}{"rows": 2000000}
	res, aerr := classifyExecutorResponse("needs_continuation", "chunk budget reached", out, "corr-cont", nil)
	if res != nil {
		t.Fatalf("expected nil result for continuation, got %v", res)
	}
	if aerr == nil || aerr.Type != ErrTypePolicy || aerr.Code != PolicyCodeNeedsContinuation {
		t.Fatalf("expected policy/needs-continuation error, got %v", aerr)
	}
}

func TestBuildExecutorTaskMap_CoreFields(t *testing.T) {
	input := NLPipelineWorkflowV2Input{
		PipelineID:  "pipe-1",
		ExecutionID: "exec-1",
		UserID:      "owner-42",
		Message:     "sync everything",
		RunMode:     "reload",
	}
	state := &WorkflowState{
		SourceConnectionID:      "src-1",
		DestinationConnectionID: "dst-1",
		SelectedTables:          []string{"a", "b"},
	}
	task := buildExecutorTaskMap(input, state, "trace-1", "tp", "ts")

	checks := map[string]interface{}{
		"pipeline_id":  "pipe-1",
		"execution_id": "exec-1",
		// KI-NLCHAT-TENANT-BLIND-FETCH: the pipeline owner MUST be threaded onto the
		// executor task so the orchestrator's getConnectionConfigForTask resolves the
		// connection config via the tenant-scoped GetForUser path (fails closed on a
		// cross-tenant id) instead of the tenant-blind bare-id fallback. Regressing
		// this drops every chat/scheduled run back to the tenant-blind fetch.
		"user_id":                   "owner-42",
		"type":                      "executor",
		"trace_id":                  "trace-1",
		"traceparent":               "tp",
		"tracestate":                "ts",
		"run_mode":                  "reload",
		"source_connection_id":      "src-1",
		"destination_connection_id": "dst-1",
	}
	for k, want := range checks {
		if task[k] != want {
			t.Errorf("task[%q] = %v, want %v", k, task[k], want)
		}
	}
	if got, ok := task["selected_tables"].([]string); !ok || len(got) != 2 {
		t.Errorf("selected_tables not propagated: %v", task["selected_tables"])
	}
	// correlation_id must NOT be embedded — the native path carries no correlation.
	if _, present := task["correlation_id"]; present {
		t.Errorf("task must not embed correlation_id")
	}
}
