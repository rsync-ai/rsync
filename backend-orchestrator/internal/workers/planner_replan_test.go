package workers

import "testing"

// Auto-replan (beta): the orchestrator planner worker forwards a V2 workflow's
// error_context from the inbound task into the outbound planner-service context.
// These tests pin the forwarding behavior and the non-regression guarantee that the
// normal first planning pass (no error_context) is left untouched.

func TestForwardErrorContext_PresentIsForwarded(t *testing.T) {
	ec := map[string]interface{}{"reason": "table missing", "stage": "extract"}
	dst := map[string]interface{}{}
	forwardErrorContext(dst, map[string]interface{}{"error_context": ec})

	got, ok := dst["error_context"]
	if !ok {
		t.Fatalf("error_context not forwarded into planner context")
	}
	if gotMap, ok := got.(map[string]interface{}); !ok || gotMap["reason"] != "table missing" {
		t.Errorf("forwarded error_context mismatch: got %#v", got)
	}
}

func TestForwardErrorContext_AbsentLeavesDstUntouched(t *testing.T) {
	// Normal first pass: no error_context in the task. dst must remain empty so the
	// planner request is byte-identical to the pre-feature behavior.
	dst := map[string]interface{}{}
	forwardErrorContext(dst, map[string]interface{}{"parsed_intent": "sync foo"})

	if _, exists := dst["error_context"]; exists {
		t.Errorf("error_context should not be set when absent from task context")
	}
	if len(dst) != 0 {
		t.Errorf("dst should be untouched on the normal first pass, got %#v", dst)
	}
}

func TestForwardErrorContext_NilTaskContextIsSafe(t *testing.T) {
	dst := map[string]interface{}{}
	forwardErrorContext(dst, nil) // must not panic
	if len(dst) != 0 {
		t.Errorf("dst should be untouched for a nil task context, got %#v", dst)
	}
}

func TestForwardErrorContext_NilValueNotForwarded(t *testing.T) {
	// An explicit nil error_context entry is treated as absent.
	dst := map[string]interface{}{}
	forwardErrorContext(dst, map[string]interface{}{"error_context": nil})
	if _, exists := dst["error_context"]; exists {
		t.Errorf("a nil error_context value should not be forwarded")
	}
}
