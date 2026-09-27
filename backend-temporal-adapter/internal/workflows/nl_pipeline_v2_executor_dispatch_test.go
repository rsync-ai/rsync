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

// The GKE rsync-v016 run a6026f71 (2026-09-27): the sink DLQ'd a batch because it
// could not resolve the MinIO MCP Service, the executor failed the run with that
// sink error embedded, and "no such host" classified it INVALID_CONFIGURATION. The
// workflow then parked on "Configure connections" for 24h with the execution row
// left running, and the batch sentinel re-raised its alert every minute.
func TestClassifyExecutorResponse_SilentDropIsNotReadAsAConnectionProblem(t *testing.T) {
	cases := map[string]struct{ status, err string }{
		"sink dns failure": {"silent_partial_drop_detected",
			`destination partially dropped rows: dispatched 12000, ack ledger confirmed only 2000 landed after the sink DLQ'd at least one batch (execution a6026f71); sink error: minio fetch failed after 5 attempt(s): Post "http://minio-mcp:8000/mcp": dial tcp: lookup minio-mcp on 34.118.224.10:53: no such host`},
		"row count containing 403": {"silent_drop_detected",
			"destination silently dropped all rows: dispatched 14030, ack ledger confirmed 0 landed across 3 ack batches (execution e1)"},
	}
	for name, c := range cases {
		// Control: the same text under a plain failure status still trips the
		// heuristics, so the assertion below is about the status, not the text.
		if _, ctl := classifyExecutorResponse("failed", c.err, nil, "corr", nil); ctl == nil || ctl.Type != ErrTypePolicy {
			t.Fatalf("%s: control expected a POLICY classification for status failed, got %v", name, ctl)
		}
		_, aerr := classifyExecutorResponse(c.status, c.err, nil, "corr", nil)
		if aerr == nil || aerr.Type != ErrTypeDeterministic || aerr.Code != "EXECUTION_FAILED" {
			t.Fatalf("%s: expected deterministic EXECUTION_FAILED, got %v", name, aerr)
		}
		if aerr.Metadata["agent_status"] != c.status {
			t.Fatalf("%s: healing metadata lost the status: %v", name, aerr.Metadata)
		}
	}
}
