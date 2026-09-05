package security

import "testing"

// WorkspaceRole is a new, orthogonal axis from the global platform roles in
// rbac.go (user/power_user/admin). A global "user" can be a workspace "owner".
func TestWorkspaceRole_Meets(t *testing.T) {
	cases := []struct {
		role WorkspaceRole
		min  WorkspaceRole
		want bool
	}{
		{WSOwner, WSAdmin, true},
		{WSOwner, WSOwner, true},
		{WSAdmin, WSAdmin, true},
		{WSAdmin, WSOwner, false},
		{WSMember, WSAdmin, false},
		{WSMember, WSMember, true},
		{WSMember, WSViewer, true},
		{WSViewer, WSMember, false},
		{WSViewer, WSViewer, true},
		// unknown roles never satisfy any gate, and no role satisfies an
		// unknown minimum (fail closed).
		{"bogus", WSViewer, false},
		{WSOwner, "bogus", false},
		{"", WSViewer, false},
	}
	for _, c := range cases {
		if got := c.role.Meets(c.min); got != c.want {
			t.Errorf("WorkspaceRole(%q).Meets(%q) = %v, want %v", c.role, c.min, got, c.want)
		}
	}
}

func TestWorkspaceRole_Valid(t *testing.T) {
	for _, r := range []WorkspaceRole{WSViewer, WSMember, WSAdmin, WSOwner} {
		if !r.Valid() {
			t.Errorf("%q should be a valid workspace role", r)
		}
	}
	for _, r := range []WorkspaceRole{"", "user", "power_user", "Owner", "OWNER"} {
		if r.Valid() {
			t.Errorf("%q should NOT be a valid workspace role", r)
		}
	}
}
