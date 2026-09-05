package handlers

import (
	"database/sql"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Tenant-isolation gates for the CDC control handlers.
//
// These endpoints live on the public `/orchestrator` Traefik route and are
// gated by requirePrincipal (cmd/orchestrator/auth_middleware.go), which only
// AUTHENTICATES the caller — it does NOT check that the caller may act on the
// target resource. Without a per-resource gate, any authenticated user who knows
// a connection UUID or pipeline UUID could drive CDC provisioning/teardown
// against another tenant's resource, including using another tenant's decrypted
// connection credentials to connect to their source DB (the ARCH-3 /
// tenant-blind CDC config-fetch gap).
//
// Trusted internal callers (api-gateway proxy / Next.js server, which
// authenticate with X-Internal-Secret and have already applied their own
// workspace-role gate) set auth_internal=true and pass through, so browser
// traffic is unaffected — these gates only bind direct API callers.
//
// The boundary is the WORKSPACE, not the creator. These helpers used to compare
// the caller against `pipelines.created_by` / `connections.user_id`, which is
// workspace-blind in both directions: it let a creator act on a resource after
// they had been removed from its workspace, and it refused a teammate who holds
// a real role on a resource the workspace collectively owns. api-gateway already
// authorizes the same resources by workspace role (pipeline_ownership.go
// requirePipelineWorkspaceRole); this is the same model on the direct route.
// A raw API call carries no "active workspace" signal, so the gate here asks the
// weaker but well-defined question — does the caller hold a sufficient role in
// the workspace that owns this resource — rather than api-gateway's stricter
// "is this resource in your ACTIVE workspace".

// workspaceRoleRank mirrors api-gateway's security.WorkspaceRole ladder
// (viewer < member < admin < owner). It is duplicated rather than imported
// because that package lives in a different Go module.
var workspaceRoleRank = map[string]int{
	"viewer": 1,
	"member": 2,
	"admin":  3,
	"owner":  4,
}

// minMutatingRole is the floor for every caller of these gates: all six call
// sites (provision, cleanup, update-tables, backfill, recover, sink restart)
// mutate CDC infrastructure, so a viewer must not pass.
const minMutatingRole = "member"

// resourceAccess is what the DB says about one resource and one caller.
type resourceAccess struct {
	found       bool
	workspaceID string // "" when the row's workspace_id is NULL
	owner       string // created_by / user_id; "" when NULL
	memberRole  string // the caller's role in workspaceID; "" when not a member
}

// decideResourceAccess is the pure authorization core, split out so the policy
// is unit-testable without a database. `label` names the resource in the error
// ("pipeline" / "connection"). An empty httpStatus/message with allow=true means
// the caller may proceed.
func decideResourceAccess(a resourceAccess, authUser, label string) (allow bool, httpStatus int, message string) {
	if !a.found {
		return false, http.StatusNotFound, label + " not found"
	}
	// Rows that predate workspaces (or were never backfilled) have no workspace
	// to authorize against. Fall back to the legacy creator comparison rather
	// than locking the row out entirely.
	if a.workspaceID == "" {
		if a.owner != "" && a.owner == authUser {
			return true, 0, ""
		}
		return false, http.StatusForbidden, "not authorized for this " + label
	}
	if a.memberRole == "" {
		return false, http.StatusForbidden, "not authorized for this " + label
	}
	if workspaceRoleRank[a.memberRole] < workspaceRoleRank[minMutatingRole] {
		return false, http.StatusForbidden, "insufficient workspace role for this " + label
	}
	return true, 0, ""
}

// cdcPrincipal returns the authenticated user id (empty for internal
// principals) and whether the caller is a trusted internal service. Mirrors
// principalUserID in auth_middleware.go, reading the gin keys that
// requirePrincipal sets.
func cdcPrincipal(c *gin.Context) (userID string, internal bool) {
	if v, ok := c.Get("auth_internal"); ok {
		if b, _ := v.(bool); b {
			return "", true
		}
	}
	return c.GetString("auth_user_id"), false
}

// loadResourceAccess runs the single-round-trip lookup: the resource row plus
// the caller's role in that resource's workspace. `query` must be one of the
// two constants below — it is never built from request data.
func loadResourceAccess(db *sql.DB, query, resourceID, authUser string) (resourceAccess, error) {
	var (
		a         resourceAccess
		workspace sql.NullString
		owner     sql.NullString
		role      sql.NullString
	)
	err := db.QueryRow(query, resourceID, authUser).Scan(&workspace, &owner, &role)
	if err == sql.ErrNoRows {
		return a, nil
	}
	if err != nil {
		return a, err
	}
	a.found = true
	a.workspaceID = workspace.String
	a.owner = owner.String
	a.memberRole = role.String
	return a, nil
}

const connectionAccessQuery = `
	SELECT c.workspace_id::text, c.user_id::text, wm.role
	  FROM connections c
	  LEFT JOIN workspace_members wm
	    ON wm.workspace_id = c.workspace_id AND wm.user_id::text = $2
	 WHERE c.id = $1`

const pipelineAccessQuery = `
	SELECT p.workspace_id::text, p.created_by::text, wm.role
	  FROM pipelines p
	  LEFT JOIN workspace_members wm
	    ON wm.workspace_id = p.workspace_id AND wm.user_id::text = $2
	 WHERE p.id = $1`

// assertResourceWorkspaceRole is the shared body of the two gates below: it
// resolves the principal, runs the access lookup and applies decideResourceAccess,
// writing the response and aborting on any failure (so the handler must
// `return`).
func assertResourceWorkspaceRole(c *gin.Context, db *sql.DB, query, resourceID, label string) bool {
	authUser, internal := cdcPrincipal(c)
	if internal {
		return true
	}
	if authUser == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		c.Abort()
		return false
	}
	if db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database unavailable"})
		c.Abort()
		return false
	}
	access, err := loadResourceAccess(db, query, resourceID, authUser)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "authorization check failed"})
		c.Abort()
		return false
	}
	allow, status, message := decideResourceAccess(access, authUser, label)
	if !allow {
		c.JSON(status, gin.H{"error": message})
		c.Abort()
		return false
	}
	return true
}

// assertConnectionOwner enforces that a user principal holds a sufficient role
// in the workspace that owns the target connection before its decrypted
// credentials are used to provision CDC resources against the source database.
// This closes the tenant-blind config fetch: the CDC managers load
// `SELECT config FROM connections WHERE id = $1` with no tenant predicate, so
// the boundary MUST be enforced here. Trusted internal callers pass through.
func assertConnectionOwner(c *gin.Context, db *sql.DB, connectionID string) bool {
	return assertResourceWorkspaceRole(c, db, connectionAccessQuery, connectionID, "connection")
}

// assertPipelineOwnerForHandlers enforces the same workspace-role gate on the
// target pipeline before a mutating CDC control action (cleanup / update-tables
// / backfill / recover / sink restart) that reaches the per-DB CDC managers via
// the pipeline's source_connection_id. Trusted internal callers pass through.
func assertPipelineOwnerForHandlers(c *gin.Context, db *sql.DB, pipelineID string) bool {
	return assertResourceWorkspaceRole(c, db, pipelineAccessQuery, pipelineID, "pipeline")
}
