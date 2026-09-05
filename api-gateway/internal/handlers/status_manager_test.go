package handlers

import (
	"testing"

	"api-gateway/internal/kafka"
)

// mapAgentStatusToPipelineStatus is the function that decides what
// pipeline status to write to the DB when an agent publishes a response.
//
// The bug surfaced by the Shopify→Postgres E2E test was that ANY executor
// response was mapped to "completed", regardless of whether the executor
// actually succeeded or was reporting an interim state. These tests pin
// the corrected mapping so the bug can't regress.

func TestMapAgentStatus_ExecutorSuccessIsCompleted(t *testing.T) {
	sm := NewStatusManager()
	cases := []string{"success", "completed", "succeeded"}
	for _, s := range cases {
		got := sm.mapAgentStatusToPipelineStatus(kafka.AgentResponse{Agent: "executor", Status: s})
		if got != "completed" {
			t.Errorf("executor status=%q should map to completed, got %q", s, got)
		}
	}
}

func TestMapAgentStatus_ExecutorFailedIsFailed(t *testing.T) {
	sm := NewStatusManager()
	// Both "failed" and "error" must map to "failed". The original bug
	// was that response.Status="failed" got the executor->completed path
	// because the global "error" check didn't match.
	for _, s := range []string{"failed", "error"} {
		got := sm.mapAgentStatusToPipelineStatus(kafka.AgentResponse{Agent: "executor", Status: s})
		if got != "failed" {
			t.Errorf("executor status=%q should map to failed, got %q", s, got)
		}
	}
}

func TestMapAgentStatus_ExecutorCancelledIsCancelled(t *testing.T) {
	sm := NewStatusManager()
	for _, s := range []string{"cancelled", "canceled"} {
		got := sm.mapAgentStatusToPipelineStatus(kafka.AgentResponse{Agent: "executor", Status: s})
		if got != "cancelled" {
			t.Errorf("executor status=%q should map to cancelled, got %q", s, got)
		}
	}
}

func TestMapAgentStatus_ExecutorWaitingForHITLIsWaitingForUser(t *testing.T) {
	sm := NewStatusManager()
	// HITL pauses surface as waiting_for_table_selection / waiting_for_node_input /
	// waiting_for_approval etc. They must map to the canonical waiting_for_user
	// (the value the Temporal projector, /state, and the list projections agree
	// on) — NOT "completed", NOT bare "waiting" (which no list/badge surface
	// recognizes, so it silently degrades to "pending").
	cases := []string{
		"waiting_for_table_selection",
		"waiting_for_node_input",
		"waiting_for_connection_config",
		"waiting_for_approval",
		"waiting",
		"paused",
	}
	for _, s := range cases {
		got := sm.mapAgentStatusToPipelineStatus(kafka.AgentResponse{Agent: "executor", Status: s})
		if got != "waiting_for_user" {
			t.Errorf("executor status=%q should map to waiting_for_user, got %q", s, got)
		}
	}
}

func TestMapAgentStatus_ExecutorIntermediateIsRunning(t *testing.T) {
	sm := NewStatusManager()
	// Any unrecognized executor status (heartbeats, progress, empty) must
	// keep the pipeline in flight — NOT flip it to completed. This is the
	// specific regression that made the Shopify E2E test see "completed"
	// 30 seconds before the executor actually finished.
	for _, s := range []string{"running", "in_progress", "processing", ""} {
		got := sm.mapAgentStatusToPipelineStatus(kafka.AgentResponse{Agent: "executor", Status: s})
		if got == "completed" {
			t.Errorf("executor status=%q must NOT map to completed (premature success), got %q", s, got)
		}
		if got != "running" {
			t.Errorf("executor status=%q should map to running, got %q", s, got)
		}
	}
}

func TestMapAgentStatus_NonExecutorAgents(t *testing.T) {
	sm := NewStatusManager()
	cases := map[string]string{
		"planner":     "processing",
		"validator":   "validating",
		"decomposer":  "planning",
		"transformer": "transforming",
	}
	for agent, want := range cases {
		got := sm.mapAgentStatusToPipelineStatus(kafka.AgentResponse{Agent: agent, Status: "success"})
		if got != want {
			t.Errorf("agent=%q should map to %q, got %q", agent, want, got)
		}
	}
}

func TestMapAgentStatus_PolicyViolationIsBlocked(t *testing.T) {
	sm := NewStatusManager()
	got := sm.mapAgentStatusToPipelineStatus(kafka.AgentResponse{
		Agent:  "policy",
		Status: "success",
		Result: map[string]interface{}{"violation": true},
	})
	if got != "blocked" {
		t.Errorf("policy violation should map to blocked, got %q", got)
	}
}

func TestMapAgentStatus_PolicyNoViolationIsProcessing(t *testing.T) {
	sm := NewStatusManager()
	got := sm.mapAgentStatusToPipelineStatus(kafka.AgentResponse{
		Agent:  "policy",
		Status: "success",
		Result: map[string]interface{}{"violation": false},
	})
	if got != "processing" {
		t.Errorf("policy clean should map to processing, got %q", got)
	}
}

func TestMapAgentStatus_ExecutorSilentDropIsFailed(t *testing.T) {
	// Phase 1: executor's post-flight verifier flags silent_drop_detected
	// or silent_partial_drop_detected when data went missing without an
	// error being raised. Both must surface as "failed" in the pipeline
	// status — never as "completed".
	sm := NewStatusManager()
	for _, s := range []string{"silent_drop_detected", "silent_partial_drop_detected"} {
		got := sm.mapAgentStatusToPipelineStatus(kafka.AgentResponse{Agent: "executor", Status: s})
		if got != "failed" {
			t.Errorf("executor status=%q should map to failed (silent drop), got %q", s, got)
		}
		if got == "completed" {
			t.Errorf("executor status=%q MUST NOT map to completed", s)
		}
	}
}

func TestMapAgentStatus_TopLevelErrorAlwaysFails(t *testing.T) {
	sm := NewStatusManager()
	// response.Status == "error" trumps the per-agent mapping.
	for _, agent := range []string{"executor", "planner", "validator", "policy"} {
		got := sm.mapAgentStatusToPipelineStatus(kafka.AgentResponse{Agent: agent, Status: "error"})
		if got != "failed" {
			t.Errorf("agent=%q with status=error should map to failed, got %q", agent, got)
		}
	}
}
