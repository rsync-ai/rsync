package workflows

import "testing"

// Prod retest 2026-09-19: infra_preflight fell through to the "planning" default,
// so the Activity tab showed the preflight as a second "Planning" group.
func TestStageGroupForStageGivesInfraPreflightItsOwnGroup(t *testing.T) {
	if got := stageGroupForStage("infra_preflight"); got != "infra_preflight" {
		t.Errorf("stageGroupForStage(infra_preflight) = %q, want infra_preflight", got)
	}
	// Control: the planner still files under planning.
	if got := stageGroupForStage("planner"); got != "planning" {
		t.Errorf("stageGroupForStage(planner) = %q, want planning", got)
	}
}
