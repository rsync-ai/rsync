package executor

import (
	"strings"
	"testing"
)

// requireSourceAndDestination is the small nil-guard helper that prevents
// the executor from crashing the whole orchestrator container with a
// nil-pointer dereference when a connection was deleted between pipeline
// create and execution. These tests pin the contract.

func TestRequireSourceAndDestination_BothPresentReturnsNil(t *testing.T) {
	task := ExecutorTask{
		TaskID:      "task-1",
		PipelineID:  "pipe-1",
		Source:      &ConnectorConfig{Type: "shopify-admin-graphql"},
		Destination: &ConnectorConfig{Type: "postgresql"},
	}
	if got := requireSourceAndDestination(task); got != nil {
		t.Fatalf("expected nil when both present, got %+v", got)
	}
}

func TestRequireSourceAndDestination_SourceMissingReturnsFailed(t *testing.T) {
	task := ExecutorTask{
		TaskID:      "task-2",
		PipelineID:  "pipe-2",
		Source:      nil,
		Destination: &ConnectorConfig{Type: "postgresql"},
	}
	resp := requireSourceAndDestination(task)
	if resp == nil {
		t.Fatal("expected non-nil response when source missing")
	}
	if resp.Status != "failed" {
		t.Errorf("status: want failed, got %q", resp.Status)
	}
	if resp.TaskID != "task-2" || resp.PipelineID != "pipe-2" {
		t.Errorf("response must echo task/pipeline IDs: got %+v", resp)
	}
	if !strings.Contains(resp.Error, "source") {
		t.Errorf("error message should mention 'source': %q", resp.Error)
	}
	if strings.Contains(resp.Error, "destination") {
		t.Errorf("error message must NOT mention destination when only source is missing: %q", resp.Error)
	}
}

func TestRequireSourceAndDestination_DestinationMissingReturnsFailed(t *testing.T) {
	task := ExecutorTask{
		TaskID:      "task-3",
		PipelineID:  "pipe-3",
		Source:      &ConnectorConfig{Type: "shopify-admin-graphql"},
		Destination: nil,
	}
	resp := requireSourceAndDestination(task)
	if resp == nil {
		t.Fatal("expected non-nil response when destination missing")
	}
	if resp.Status != "failed" {
		t.Errorf("status: want failed, got %q", resp.Status)
	}
	if !strings.Contains(resp.Error, "destination") {
		t.Errorf("error message should mention 'destination': %q", resp.Error)
	}
	if strings.Contains(resp.Error, "source ") {
		t.Errorf("error message must NOT mention source when only destination is missing: %q", resp.Error)
	}
}

func TestRequireSourceAndDestination_BothMissingReturnsFailed(t *testing.T) {
	task := ExecutorTask{
		TaskID:     "task-4",
		PipelineID: "pipe-4",
	}
	resp := requireSourceAndDestination(task)
	if resp == nil {
		t.Fatal("expected non-nil response when both missing")
	}
	if resp.Status != "failed" {
		t.Errorf("status: want failed, got %q", resp.Status)
	}
	// Both pieces should be named.
	for _, expected := range []string{"source", "destination"} {
		if !strings.Contains(resp.Error, expected) {
			t.Errorf("error message should mention %q: %q", expected, resp.Error)
		}
	}
}

func TestRequireSourceAndDestination_ErrorMessageActionable(t *testing.T) {
	// The error message is what an operator sees; it should suggest the
	// most likely cause (connection deletion mid-flight) so they don't
	// spend an hour digging through logs.
	resp := requireSourceAndDestination(ExecutorTask{Source: nil, Destination: nil})
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
	if !strings.Contains(resp.Error, "deleted") {
		t.Errorf("error should hint at deletion as the likely cause: %q", resp.Error)
	}
}
