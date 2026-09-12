package workflows

import (
	"encoding/json"
	"testing"
)

// TestBuildConnectorResolverTaskMap_CoreFields pins the connector-resolver payload.
//
// The task map is the whole transport: the orchestrator reads it verbatim as
// correlation.Request.Task, which becomes PendingRequest.Payload, which becomes both
// Task.Payload and Task.Context, which becomes the assignment. A key missing here is
// a key missing at the resolver, with no error anywhere in between.
func TestBuildConnectorResolverTaskMap_CoreFields(t *testing.T) {
	input := NLPipelineWorkflowV2Input{
		PipelineID:  "pipe-1",
		ExecutionID: "exec-1",
		UserID:      "owner-42",
		WorkspaceID: "ws-7",
		Message:     "move mongo to gcs",
		BatchSize:   500,
	}
	state := &WorkflowState{
		SourceConnectionID:      "src-1",
		DestinationConnectionID: "dst-1",
	}

	task := buildConnectorResolverTaskMap(input, state, "trace-1", "tp", "ts")

	checks := map[string]interface{}{
		"pipeline_id":  "pipe-1",
		"execution_id": "exec-1",
		"user_id":      "owner-42",
		// The orchestrator's workspace resolver scopes its connection queries by
		// workspace. Without this key it fell back to the user id — a different UUID
		// space — so every query returned zero rows and the run ended blocked.
		"workspace_id":              "ws-7",
		"type":                      "connector_resolver",
		"trace_id":                  "trace-1",
		"traceparent":               "tp",
		"tracestate":                "ts",
		"user_request":              "move mongo to gcs",
		"source_connection_id":      "src-1",
		"destination_connection_id": "dst-1",
		"batch_size":                500,
	}
	for k, want := range checks {
		if task[k] != want {
			t.Errorf("task[%q] = %v, want %v", k, task[k], want)
		}
	}
}

// TestWorkflowInputDecodesWorkspaceID guards the silent-drop mechanism.
//
// Both api-gateway start sites hand Temporal a plain map[string]interface{}; the
// default JSON converter decodes it into NLPipelineWorkflowV2Input and discards any
// key with no matching field, without an error. So a start site can send
// "workspace_id" forever and the workflow still sees the zero value unless the
// struct tag matches exactly. This decodes the literal map the handlers build.
func TestWorkflowInputDecodesWorkspaceID(t *testing.T) {
	wire := map[string]interface{}{
		"pipeline_id":               "pipe-1",
		"execution_id":              "exec-1",
		"message":                   "move mongo to gcs",
		"user_id":                   "owner-42",
		"workspace_id":              "ws-7",
		"source_connection_id":      "src-1",
		"destination_connection_id": "dst-1",
	}

	raw, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("marshal wire map: %v", err)
	}

	var input NLPipelineWorkflowV2Input
	if err := json.Unmarshal(raw, &input); err != nil {
		t.Fatalf("unmarshal into workflow input: %v", err)
	}

	if input.WorkspaceID != "ws-7" {
		t.Errorf("input.WorkspaceID = %q, want ws-7 — the json tag no longer matches the key the handlers send", input.WorkspaceID)
	}
	if input.UserID != "owner-42" {
		t.Errorf("input.UserID = %q, want owner-42", input.UserID)
	}
}
