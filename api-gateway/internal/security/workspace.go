package security

// Workspace roles gate mutations on workspace-scoped resources (connections,
// pipelines, members, invites). This axis is ORTHOGONAL to the global platform
// roles in rbac.go (UserRole = user|power_user|admin): a global "user" can be a
// workspace "owner", and a global "admin" need not be a member of a given
// workspace at all. Do NOT conflate the two — they answer different questions.
type WorkspaceRole string

const (
	WSViewer WorkspaceRole = "viewer"
	WSMember WorkspaceRole = "member"
	WSAdmin  WorkspaceRole = "admin"
	WSOwner  WorkspaceRole = "owner"
)

// wsRoleLevel orders the roles. Higher number = more authority. Roles not in the
// map (unknown / empty) have level 0 and therefore satisfy no gate.
var wsRoleLevel = map[WorkspaceRole]int{
	WSViewer: 1,
	WSMember: 2,
	WSAdmin:  3,
	WSOwner:  4,
}

// Meets reports whether r satisfies the minimum required role. It fails closed:
// an unknown role never meets any gate, and no role meets an unknown minimum.
func (r WorkspaceRole) Meets(min WorkspaceRole) bool {
	rl, ok := wsRoleLevel[r]
	if !ok {
		return false
	}
	ml, ok := wsRoleLevel[min]
	if !ok {
		return false
	}
	return rl >= ml
}

// Valid reports whether r is one of the four known workspace roles.
func (r WorkspaceRole) Valid() bool {
	_, ok := wsRoleLevel[r]
	return ok
}
