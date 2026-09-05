package healer

import "testing"

// TestShouldAutoApply is the safety-gate regression guard for the drift→approve loop.
//
// The brand contract (and the design doc's rollout plan) is: enabling detection
// (RSYNC_SCHEMA_DRIFT_ENABLED) must NEVER silently mutate a customer's destination
// schema. Auto-applying DDL is a SEPARATE opt-in (RSYNC_SCHEMA_DRIFT_AUTOAPPLY).
// With the opt-in off (the default), every detected change — even a "safe" additive
// one the classifier would auto-migrate — must route to the approval queue instead.
//
// This test pins that gate so a future classifier tweak can't re-open silent
// auto-apply on the destination.
func TestShouldAutoApply(t *testing.T) {
	safeAdditive := func(changeType string) *LLMAnalysisResponse {
		return &LLMAnalysisResponse{
			SafeToAutoMigrate: true,
			RequiresApproval:  false,
			Reasoning:         changeType + " is safe",
		}
	}
	needsApproval := func(changeType string) *LLMAnalysisResponse {
		return &LLMAnalysisResponse{
			SafeToAutoMigrate: false,
			RequiresApproval:  true,
			Reasoning:         changeType + " requires approval",
		}
	}

	cases := []struct {
		name             string
		analysis         *LLMAnalysisResponse
		autoApplyEnabled bool
		want             bool
	}{
		// Default posture: opt-in OFF → nothing auto-applies, not even safe additive.
		{"add_column, autoapply off -> approval", safeAdditive("add_column"), false, false},
		{"create_table, autoapply off -> approval", safeAdditive("create_table"), false, false},

		// Opt-in ON → safe additive changes may auto-apply.
		{"add_column, autoapply on -> auto", safeAdditive("add_column"), true, true},
		{"create_table, autoapply on -> auto", safeAdditive("create_table"), true, true},

		// Destructive / approval-required changes NEVER auto-apply, even with opt-in on.
		{"drop_column, autoapply on -> approval", needsApproval("drop_column"), true, false},
		{"drop_table, autoapply on -> approval", needsApproval("drop_table"), true, false},
		{"modify_column, autoapply on -> approval", needsApproval("modify_column"), true, false},
		{"unknown, autoapply on -> approval", needsApproval("unknown"), true, false},

		// Defensive: nil analysis never auto-applies.
		{"nil analysis", nil, true, false},

		// A malformed classification that is both safe AND requires approval must
		// fail closed (approval wins).
		{"safe+requires-approval conflict fails closed", &LLMAnalysisResponse{SafeToAutoMigrate: true, RequiresApproval: true}, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldAutoApply(tc.analysis, tc.autoApplyEnabled); got != tc.want {
				t.Errorf("shouldAutoApply(%+v, autoApply=%v) = %v, want %v",
					tc.analysis, tc.autoApplyEnabled, got, tc.want)
			}
		})
	}
}

// TestSchemaDriftAutoApplyEnabled_DefaultsOff pins that the opt-in is OFF unless the
// env flag is explicitly "true" — so a fresh deploy that only sets
// RSYNC_SCHEMA_DRIFT_ENABLED gets detect+approve, never silent auto-apply.
func TestSchemaDriftAutoApplyEnabled_DefaultsOff(t *testing.T) {
	t.Setenv("RSYNC_SCHEMA_DRIFT_AUTOAPPLY", "")
	if schemaDriftAutoApplyEnabled() {
		t.Error("auto-apply must default OFF when RSYNC_SCHEMA_DRIFT_AUTOAPPLY is unset")
	}

	t.Setenv("RSYNC_SCHEMA_DRIFT_AUTOAPPLY", "false")
	if schemaDriftAutoApplyEnabled() {
		t.Error("auto-apply must be OFF when RSYNC_SCHEMA_DRIFT_AUTOAPPLY=false")
	}

	// Only the exact string "true" opts in (mirrors schemaDriftEnabled semantics).
	t.Setenv("RSYNC_SCHEMA_DRIFT_AUTOAPPLY", "1")
	if schemaDriftAutoApplyEnabled() {
		t.Error("auto-apply must require the literal \"true\", not \"1\"")
	}

	t.Setenv("RSYNC_SCHEMA_DRIFT_AUTOAPPLY", "true")
	if !schemaDriftAutoApplyEnabled() {
		t.Error("auto-apply must be ON when RSYNC_SCHEMA_DRIFT_AUTOAPPLY=true")
	}
}
