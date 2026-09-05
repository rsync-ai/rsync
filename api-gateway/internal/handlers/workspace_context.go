package handlers

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
)

const (
	workspaceHeader  = "X-Workspace-ID"
	workspaceCookie  = "rsync_active_workspace_id"
	ctxWorkspaceID   = "workspace_id"
	ctxWorkspaceRole = "workspace_role"
)

// resourceTables allowlists the tables requireResourceRole may join against.
// The table name is interpolated into SQL (identifiers cannot be bound as query
// parameters), so it MUST come from this map and never from request input.
var resourceTables = map[string]bool{
	"pipelines":     true,
	"connections":   true,
	"saved_queries": true,
}

// WorkspaceContextMiddleware resolves the caller's active workspace and pins it
// into the request context (keys ctxWorkspaceID + ctxWorkspaceRole) after
// re-verifying membership on every request. The X-Workspace-ID header (mirrored
// by the rsync_active_workspace_id cookie) is an untrusted hint: a selection is
// only honored when the caller is actually a member.
//
// Resolution order:
//  1. explicit X-Workspace-ID header (preferred) or its cookie mirror;
//  2. the caller's personal workspace (every user has exactly one post-069).
//
// Failure semantics:
//   - no user_id yet (unauthenticated / public route) → pass through untouched;
//     downstream auth gates reject. The DB is not touched in this case.
//   - DB unavailable → 503.
//   - explicit header naming a workspace the caller cannot access → 404 on
//     non-optional routes (do not confirm existence, do not proceed under it).
//     A stale *cookie* never hard-fails; it falls through to personal.
//   - optional routes (admin, workspace list/create, features) never abort on a
//     bad selection — they resolve best-effort so the user can always recover.
func WorkspaceContextMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.GetString("user_id")
		if userID == "" {
			c.Next()
			return
		}

		database := db.GetDB()
		if database == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
			c.Abort()
			return
		}

		optional := workspaceContextOptional(c)

		requested := strings.TrimSpace(c.GetHeader(workspaceHeader))
		viaHeader := requested != ""
		if requested == "" {
			if ck, err := c.Cookie(workspaceCookie); err == nil {
				requested = strings.TrimSpace(ck)
			}
		}

		if requested != "" {
			role, err := lookupWorkspaceMembership(database, requested, userID)
			switch {
			case err == nil:
				c.Set(ctxWorkspaceID, requested)
				c.Set(ctxWorkspaceRole, string(role))
				c.Next()
				return
			case err == sql.ErrNoRows:
				// Not a member. An explicit header on a non-optional route is a
				// hard 404; a stale cookie (or any selection on an optional
				// route) falls through to the personal-workspace default.
				if viaHeader && !optional {
					c.JSON(http.StatusNotFound, gin.H{"error": "workspace not found"})
					c.Abort()
					return
				}
			default:
				c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
				c.Abort()
				return
			}
		}

		// Default: the caller's personal workspace.
		wsID, role, err := personalWorkspace(database, userID)
		switch {
		case err == nil:
			c.Set(ctxWorkspaceID, wsID)
			c.Set(ctxWorkspaceRole, string(role))
		case err == sql.ErrNoRows:
			// No personal workspace (should not happen post-069). Leave the
			// context unset; resource/role gates fail closed downstream.
		default:
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
			c.Abort()
			return
		}
		c.Next()
	}
}

// workspaceContextOptional reports whether the matched route must keep working
// even when no valid workspace can be selected, so a stale active workspace can
// never lock a user out of the routes they need to recover.
func workspaceContextOptional(c *gin.Context) bool {
	p := c.FullPath()
	if p == "" {
		p = c.Request.URL.Path
	}
	switch {
	case strings.HasPrefix(p, "/api/v1/admin/"):
		return true
	case p == "/api/v1/workspaces":
		return true
	case p == "/api/v1/features":
		return true
	default:
		return false
	}
}

func lookupWorkspaceMembership(database *sql.DB, workspaceID, userID string) (security.WorkspaceRole, error) {
	var role string
	err := database.QueryRow(
		"SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
		workspaceID, userID,
	).Scan(&role)
	return security.WorkspaceRole(role), err
}

func personalWorkspace(database *sql.DB, userID string) (string, security.WorkspaceRole, error) {
	var wsID, role string
	err := database.QueryRow(`
		SELECT w.id, wm.role
		FROM workspaces w
		JOIN workspace_members wm ON wm.workspace_id = w.id AND wm.user_id = $1
		WHERE w.owner_id = $1 AND w.is_personal
		ORDER BY w.created_at ASC
		LIMIT 1
	`, userID).Scan(&wsID, &role)
	return wsID, security.WorkspaceRole(role), err
}

// activeWorkspaceID returns the workspace pinned by the middleware, or "" when
// none is active. Non-writing: callers that surface their own error shape (e.g.
// the chat path, which returns an error tuple rather than an HTTP body) use this
// directly; HTTP handlers use resolveActiveWorkspace.
func activeWorkspaceID(c *gin.Context) string {
	return c.GetString(ctxWorkspaceID)
}

// resolveActiveWorkspace returns the workspace pinned by the middleware, or
// writes 400 and returns false when no workspace is active.
func resolveActiveWorkspace(c *gin.Context) (string, bool) {
	ws := activeWorkspaceID(c)
	if ws == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no active workspace"})
		return "", false
	}
	return ws, true
}

// requireWorkspaceRole gates on the caller's role in the active workspace (as
// resolved by the middleware). Fail-closed: a missing/unknown role meets no gate.
func requireWorkspaceRole(c *gin.Context, min security.WorkspaceRole) (security.WorkspaceRole, bool) {
	role := security.WorkspaceRole(c.GetString(ctxWorkspaceRole))
	if !role.Meets(min) {
		c.JSON(http.StatusForbidden, gin.H{"error": "insufficient workspace role"})
		return "", false
	}
	return role, true
}

// WorkspaceGeneratorMiddleware gates connector-generation routes on workspace
// role >= admin (owner or admin). It replaces the global-role PowerUserOrAdmin
// gate on these routes.
//
// Why the axis changed: connector generation is a TENANT capability — every
// self-serve signup owns their personal workspace — not a platform-staff one.
// Gating it on the global user|power_user|admin axis meant a paying signup (whose
// global role is "user") hit a dead, greyed-out Generate button: the exact
// feature they signed up for, blocked by default. Re-keying to the workspace axis
// lets the workspace owner generate immediately while still refusing members and
// viewers.
//
// The security rationale of the old gate is PRESERVED, not dropped: a member or
// viewer still cannot drive LLM codegen with an attacker-chosen docs_url or plant
// connector code via prompt injection. That capability is now scoped to workspace
// owner/admin instead of the platform-staff tier.
//
// Ordering: these routes live on the /api/v1 group, so AuthRequiredMiddleware and
// WorkspaceContextMiddleware (both group-level .Use) have already run and pinned
// user_id + workspace_role into the context by the time this route middleware
// executes. Fail-closed: a missing or unknown workspace role satisfies no gate
// (requireWorkspaceRole writes 403 and returns false).
func WorkspaceGeneratorMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if _, ok := requireWorkspaceRole(c, security.WSAdmin); !ok {
			c.Abort()
			return
		}
		c.Next()
	}
}

// requireWorkspaceParamRole gates on the caller's role in the workspace named by
// a URL path segment (e.g. /workspaces/:id/...), NOT the active-workspace header.
// It is the IDOR-safe gate for workspace-ADMINISTRATION endpoints (invites,
// members) whose target workspace is the path: the role is proven against the
// path workspace, so a workspace-A admin cannot administer workspace B by sending
// X-Workspace-ID: A. Mirrors the inline checks in UpdateWorkspace /
// ListWorkspaceMembers, consolidated.
//
// Fail-closed. On any failure it writes the response and returns ("", false):
//   - unauthenticated → 401
//   - empty workspaceID / caller not a member → 404 (never confirm existence)
//   - DB down → 503
//   - role below minimum → 403
func requireWorkspaceParamRole(c *gin.Context, workspaceID string, min security.WorkspaceRole) (security.WorkspaceRole, bool) {
	userID, ok := resolveUserID(c)
	if !ok {
		return "", false
	}
	if workspaceID == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "workspace not found"})
		return "", false
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return "", false
	}

	role, err := lookupWorkspaceMembership(database, workspaceID, userID)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "workspace not found"})
		return "", false
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return "", false
	}
	if !role.Meets(min) {
		c.JSON(http.StatusForbidden, gin.H{"error": "insufficient workspace role"})
		return "", false
	}
	return role, true
}

// requireResourceRole is the IDOR-safe gate for a workspace-scoped resource. In
// a single query it proves that (a) the resource exists, (b) it belongs to the
// caller's ACTIVE workspace, and (c) the caller is a member with a sufficient
// role — with NO created_by fallback (clean-slate ownership is by membership).
//
// On any failure it writes the response and returns ("", false):
//   - unauthenticated → 401
//   - table not allowlisted → 500 (programming error; never interpolate input)
//   - no active workspace → 404
//   - DB down → 503
//   - resource missing / not in active workspace / caller not a member → 404
//   - role below minimum → 403
func requireResourceRole(c *gin.Context, table, resourceID string, min security.WorkspaceRole) (security.WorkspaceRole, bool) {
	userID, ok := resolveUserID(c)
	if !ok {
		return "", false
	}
	if !resourceTables[table] {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return "", false
	}
	activeWS := c.GetString(ctxWorkspaceID)
	if activeWS == "" {
		// Cannot authorize without an active workspace. 404, never reveal
		// whether the resource exists.
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return "", false
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return "", false
	}

	// table is allowlisted above; resourceID/userID/activeWS are bound params.
	query := fmt.Sprintf(`
		SELECT wm.role
		FROM %s r
		JOIN workspace_members wm ON wm.workspace_id = r.workspace_id
		WHERE r.id = $1 AND wm.user_id = $2 AND r.workspace_id = $3
	`, table)

	var role string
	err := database.QueryRow(query, resourceID, userID, activeWS).Scan(&role)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return "", false
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return "", false
	}

	wr := security.WorkspaceRole(role)
	if !wr.Meets(min) {
		c.JSON(http.StatusForbidden, gin.H{"error": "insufficient workspace role"})
		return "", false
	}
	return wr, true
}
