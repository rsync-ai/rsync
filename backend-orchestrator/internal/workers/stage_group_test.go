package workers

import "testing"

// Prod retest 2026-09-19: infra_preflight fell through to the "planning" default,
// so the Activity tab showed the preflight as a second "Planning" group.
func TestStageGroupForGivesInfraPreflightItsOwnGroup(t *testing.T) {
	if got := stageGroupFor("infra_preflight"); got != "infra_preflight" {
		t.Errorf("stageGroupFor(infra_preflight) = %q, want infra_preflight", got)
	}
	// Control: the planner still files under planning.
	if got := stageGroupFor("planner"); got != "planning" {
		t.Errorf("stageGroupFor(planner) = %q, want planning", got)
	}
}
