package workflows

import "testing"

// Auto-replan (beta) unit tests for the V2 workflow's pure gating + threading helpers.
// These pin (a) the eligibility predicate that bounds corrective re-plans and (b) the
// non-regression guarantee that the normal first planning pass carries no error_context.

func TestExecutorReplanEligible(t *testing.T) {
	const maxReplans = 1
	deterministicExecFail := &ActivityError{Type: ErrTypeDeterministic, Code: "EXECUTION_FAILED", Message: "boom"}

	tests := []struct {
		name     string
		err      *ActivityError
		ok       bool
		attempts int
		want     bool
	}{
		{"eligible: first deterministic exec failure", deterministicExecFail, true, 0, true},
		{"not eligible: budget exhausted", deterministicExecFail, true, maxReplans, false},
		{"not eligible: unwrap failed", deterministicExecFail, false, 0, false},
		{"not eligible: nil error", nil, true, 0, false},
		{"not eligible: transient error", &ActivityError{Type: ErrTypeTransient, Code: "EXECUTION_FAILED"}, true, 0, false},
		{"not eligible: infra error", &ActivityError{Type: ErrTypeInfra, Code: "EXECUTION_FAILED"}, true, 0, false},
		{"not eligible: policy error", &ActivityError{Type: ErrTypePolicy, Code: "EXECUTION_FAILED"}, true, 0, false},
		{"not eligible: wrong code", &ActivityError{Type: ErrTypeDeterministic, Code: "VALIDATION_FAILED"}, true, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := executorReplanEligible(tt.err, tt.ok, tt.attempts, maxReplans); got != tt.want {
				t.Errorf("executorReplanEligible() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAddReplanErrorContext_PresentIsThreaded(t *testing.T) {
	failure := map[string]interface{}{"stage": "executor", "reason": "table missing"}
	state := &WorkflowState{LastFailure: failure}
	task := map[string]interface{}{"type": "planner"}

	addReplanErrorContext(task, state)

	got, ok := task["error_context"]
	if !ok {
		t.Fatalf("error_context not threaded into planner task")
	}
	if gotMap, ok := got.(map[string]interface{}); !ok || gotMap["reason"] != "table missing" {
		t.Errorf("threaded error_context mismatch: got %#v", got)
	}
}

func TestAddReplanErrorContext_AbsentLeavesTaskUntouched(t *testing.T) {
	// Normal first pass: no LastFailure recorded -> task must carry no error_context.
	state := &WorkflowState{}
	task := map[string]interface{}{"type": "planner"}

	addReplanErrorContext(task, state)

	if _, exists := task["error_context"]; exists {
		t.Errorf("error_context should be absent on the normal first planning pass")
	}
	if len(task) != 1 {
		t.Errorf("task should be untouched on the first pass, got %#v", task)
	}
}

func TestAddReplanErrorContext_NilStateIsSafe(t *testing.T) {
	task := map[string]interface{}{"type": "planner"}
	addReplanErrorContext(task, nil) // must not panic
	if _, exists := task["error_context"]; exists {
		t.Errorf("error_context should be absent for a nil state")
	}
}
