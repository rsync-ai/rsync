package handlers

import (
	"context"
	"database/sql"
	"net/http"
	"net/mail"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	db "api-gateway/internal/db"
	"api-gateway/internal/security"
)

// maxWorkspaceNameLen bounds a workspace name to the workspaces.name VARCHAR(255)
// column. Counted in runes (not bytes) to match Postgres VARCHAR semantics, so a
// name made of multibyte/emoji characters is measured the way the column stores it.
// Without this guard an over-long name fell through to the INSERT/UPDATE and
// surfaced as a generic 500 instead of a clear 400.
const maxWorkspaceNameLen = 255

// isValidEmailAddress reports whether s is a single, bare RFC 5322 address (no
// display-name form like "Name <a@b>"). Used to reject obviously-malformed invite
// emails at the API boundary instead of minting a dead pending invite.
func isValidEmailAddress(s string) bool {
	addr, err := mail.ParseAddress(s)
	if err != nil {
		return false
	}
	return addr.Address == s
}

type Workspace struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Slug       string    `json:"slug"`
	OwnerID    string    `json:"owner_id"`
	Plan       string    `json:"plan"`
	Role       string    `json:"role,omitempty"` // caller's role in this workspace
	IsPersonal bool      `json:"is_personal"`    // identity-anchor workspace; can't be deleted or left
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type WorkspaceMember struct {
	ID          string    `json:"id"`
	WorkspaceID string    `json:"workspace_id"`
	UserID      string    `json:"user_id"`
	Email       string    `json:"email,omitempty"`
	Role        string    `json:"role"`
	CreatedAt   time.Time `json:"created_at"`
}

var slugRe = regexp.MustCompile(`[^a-z0-9]+`)

func toSlug(s string) string {
	s = strings.ToLower(s)
	s = slugRe.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if len(s) > 60 {
		s = s[:60]
	}
	return s
}

// ListWorkspaces returns all workspaces the authenticated user belongs to.
// GET /api/v1/workspaces
func ListWorkspaces(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not connected"})
		return
	}
	userID := c.GetString("user_id")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
		return
	}

	rows, err := database.Query(`
		SELECT w.id, w.name, w.slug, w.owner_id, w.plan, w.is_personal, w.created_at, w.updated_at, wm.role
		FROM workspaces w
		JOIN workspace_members wm ON wm.workspace_id = w.id AND wm.user_id = $1
		ORDER BY w.created_at ASC
	`, userID)
	if err != nil {
		log.Errorf("workspace: list for user %s: %v", userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list workspaces"})
		return
	}
	defer rows.Close()

	workspaces := []Workspace{}
	for rows.Next() {
		var w Workspace
		if err := rows.Scan(&w.ID, &w.Name, &w.Slug, &w.OwnerID, &w.Plan, &w.IsPersonal, &w.CreatedAt, &w.UpdatedAt, &w.Role); err != nil {
			continue
		}
		workspaces = append(workspaces, w)
	}
	c.JSON(http.StatusOK, gin.H{"workspaces": workspaces})
}

// GetWorkspace returns a single workspace (must be a member).
// GET /api/v1/workspaces/:id
func GetWorkspace(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not connected"})
		return
	}
	userID := c.GetString("user_id")

	// workspaces.id is a uuid column, so a non-UUID :id makes Postgres raise
	// "invalid input syntax for type uuid" — which is NOT sql.ErrNoRows and so fell
	// through to the generic 500 below. GET /api/v1/workspaces/current hit exactly
	// this: gin binds the literal "current" to :id (there is no /workspaces/current
	// route), and a caller's typo became a server error. A malformed id is a client
	// error; reject it here, before it reaches SQL.
	//
	// 400 leaks nothing the 404-for-non-members rule protects: an id that is not a
	// UUID cannot name any workspace, so the status code still never distinguishes
	// "exists" from "not yours". See workspace_param_gate_consistency_test.go.
	workspaceID, ok := requireUUIDParam(c, "id", "invalid_workspace_id", "Invalid workspace ID format")
	if !ok {
		return
	}

	var w Workspace
	err := database.QueryRow(`
		SELECT w.id, w.name, w.slug, w.owner_id, w.plan, w.is_personal, w.created_at, w.updated_at, wm.role
		FROM workspaces w
		JOIN workspace_members wm ON wm.workspace_id = w.id AND wm.user_id = $1
		WHERE w.id = $2
	`, userID, workspaceID).Scan(&w.ID, &w.Name, &w.Slug, &w.OwnerID, &w.Plan, &w.IsPersonal, &w.CreatedAt, &w.UpdatedAt, &w.Role)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "workspace not found or access denied"})
		return
	}
	if err != nil {
		log.Errorf("workspace: fetch %s for user %s: %v", workspaceID, userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch workspace"})
		return
	}
	c.JSON(http.StatusOK, w)
}

// CreateWorkspace creates a new workspace owned by the authenticated user.
// POST /api/v1/workspaces
func CreateWorkspace(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not connected"})
		return
	}
	userID := c.GetString("user_id")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
		return
	}

	var body struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	body.Name = strings.TrimSpace(body.Name)
	if body.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name cannot be blank"})
		return
	}
	if utf8.RuneCountInString(body.Name) > maxWorkspaceNameLen {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name must be 255 characters or fewer"})
		return
	}

	slug := toSlug(body.Name)
	if slug == "" {
		slug = "workspace"
	}

	// Ensure slug uniqueness. Each retry appends a FRESH random suffix — the same
	// shape personalWorkspaceSlug uses. The previous candidate was clock-derived
	// (minute+second), which is identical on every iteration of a loop that runs in
	// microseconds, so the loop retested one unchanged candidate and then fell back
	// to base+userID[:4] — deterministic per (user, name), and therefore able to
	// reach the INSERT with an already-taken slug and surface the UNIQUE violation
	// as a generic 500 when the same name was used twice.
	base := slug
	for i := 0; i < 10; i++ {
		var exists bool
		if err := database.QueryRow("SELECT EXISTS(SELECT 1 FROM workspaces WHERE slug = $1)", slug).Scan(&exists); err != nil || !exists {
			break
		}
		slug = base + "-" + randomSlugSuffix()
	}

	tx, err := database.Begin()
	if err != nil {
		log.Errorf("workspace: begin tx to create for user %s: %v", userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start transaction"})
		return
	}
	defer tx.Rollback() //nolint:errcheck

	var w Workspace
	err = tx.QueryRow(`
		INSERT INTO workspaces (name, slug, owner_id, plan)
		VALUES ($1, $2, $3, 'free')
		RETURNING id, name, slug, owner_id, plan, is_personal, created_at, updated_at
	`, body.Name, slug, userID).Scan(&w.ID, &w.Name, &w.Slug, &w.OwnerID, &w.Plan, &w.IsPersonal, &w.CreatedAt, &w.UpdatedAt)
	if err != nil {
		log.Errorf("workspace: insert slug %s for user %s: %v", slug, userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create workspace"})
		return
	}

	_, err = tx.Exec(`
		INSERT INTO workspace_members (workspace_id, user_id, role)
		VALUES ($1, $2, 'owner')
	`, w.ID, userID)
	if err != nil {
		log.Errorf("workspace: add owner %s to new workspace %s: %v", userID, w.ID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to add owner to workspace"})
		return
	}

	if err := tx.Commit(); err != nil {
		log.Errorf("workspace: commit create of %s for user %s: %v", w.ID, userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to commit"})
		return
	}

	w.Role = "owner"
	c.JSON(http.StatusCreated, w)
}

// requireNonPersonalWorkspace fails closed (409) when the workspace named by the
// :id param is a user's auto-provisioned PERSONAL workspace, which is immutable:
// it cannot be renamed or gain members (it always holds exactly its one owner).
// Mirrors the is_personal guard DeleteWorkspace already enforces. Returns false
// (after writing the response) when the workspace is personal or unreadable.
func requireNonPersonalWorkspace(c *gin.Context, database *sql.DB, workspaceID string) bool {
	var isPersonal bool
	if err := database.QueryRow("SELECT is_personal FROM workspaces WHERE id = $1", workspaceID).Scan(&isPersonal); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "workspace not found"})
		return false
	}
	if isPersonal {
		c.JSON(http.StatusConflict, gin.H{"error": "personal workspaces cannot be modified"})
		return false
	}
	return true
}

// UpdateWorkspace renames a workspace (owner or admin only).
// PATCH /api/v1/workspaces/:id
func UpdateWorkspace(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not connected"})
		return
	}
	workspaceID := c.Param("id")

	// Gate through the shared :id-param helper rather than an inline check: it is
	// the single place the role ordering lives (no second copy of "owner or admin"
	// to drift out of step with security.WorkspaceRole), and it answers 404 — not
	// 403 — to a non-member, so this endpoint stops acting as a membership oracle
	// that distinguishes "exists but you lack the role" from "does not exist".
	callerRole, ok := requireWorkspaceParamRole(c, workspaceID, security.WSAdmin)
	if !ok {
		return
	}
	role := string(callerRole)

	// Personal workspaces are immutable — no rename.
	if !requireNonPersonalWorkspace(c, database, workspaceID) {
		return
	}

	var body struct {
		Name string `json:"name"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || strings.TrimSpace(body.Name) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	newName := strings.TrimSpace(body.Name)
	if utf8.RuneCountInString(newName) > maxWorkspaceNameLen {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name must be 255 characters or fewer"})
		return
	}

	var w Workspace
	err := database.QueryRow(`
		UPDATE workspaces SET name = $1, updated_at = NOW()
		WHERE id = $2
		RETURNING id, name, slug, owner_id, plan, is_personal, created_at, updated_at
	`, newName, workspaceID).Scan(
		&w.ID, &w.Name, &w.Slug, &w.OwnerID, &w.Plan, &w.IsPersonal, &w.CreatedAt, &w.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "workspace not found"})
		return
	}
	if err != nil {
		log.Errorf("workspace: rename %s: %v", workspaceID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update workspace"})
		return
	}
	w.Role = role
	c.JSON(http.StatusOK, w)
}

// ListWorkspaceMembers returns all members of a workspace.
// GET /api/v1/workspaces/:id/members
func ListWorkspaceMembers(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not connected"})
		return
	}
	workspaceID := c.Param("id")

	// Any member (viewer+) may read the roster. Gated through the shared :id-param
	// helper so a non-member gets the same 404 every other workspace-administration
	// endpoint returns, instead of a 403 that confirms the workspace exists.
	if _, ok := requireWorkspaceParamRole(c, workspaceID, security.WSViewer); !ok {
		return
	}

	rows, err := database.Query(`
		SELECT wm.id, wm.workspace_id, wm.user_id, u.email, wm.role, wm.created_at
		FROM workspace_members wm
		JOIN users u ON u.id = wm.user_id
		WHERE wm.workspace_id = $1
		ORDER BY wm.created_at ASC
	`, workspaceID)
	if err != nil {
		log.Errorf("workspace: list members of %s: %v", workspaceID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list members"})
		return
	}
	defer rows.Close()

	members := []WorkspaceMember{}
	for rows.Next() {
		var m WorkspaceMember
		if err := rows.Scan(&m.ID, &m.WorkspaceID, &m.UserID, &m.Email, &m.Role, &m.CreatedAt); err != nil {
			continue
		}
		members = append(members, m)
	}
	c.JSON(http.StatusOK, gin.H{"members": members})
}

// applyOwnerSafeMutation runs an owner-affecting membership mutation (an owner
// demotion or removal) under a transaction that holds a row lock on the workspace,
// after verifying the workspace will retain at least one owner. Locking the
// workspace row serializes concurrent owner demotions/removals for the same
// workspace, so the count-then-mutate sequence is atomic — closing the TOCTOU
// window where two concurrent requests both observe >1 owner and drain the last
// one. It is used ONLY when the target is currently an owner (the only case where
// the >=1-owner invariant is at risk).
//
// mutate performs the actual UPDATE/DELETE on tx and returns the affected-row
// count. On any failure applyOwnerSafeMutation writes the response and returns
// false: 409 if the guard trips (would drop the last owner), 404 if the mutation
// matched no row, 503 on a DB error. On success it commits and returns true (the
// caller writes the 200 body).
func applyOwnerSafeMutation(c *gin.Context, database *sql.DB, workspaceID string, mutate func(tx *sql.Tx) (int64, error)) bool {
	tx, err := database.Begin()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return false
	}
	defer tx.Rollback()

	// Lock the workspace row, then count owners under the lock so concurrent
	// owner-affecting mutations serialize instead of racing the count.
	var lockID string
	if err := tx.QueryRow("SELECT id FROM workspaces WHERE id = $1 FOR UPDATE", workspaceID).Scan(&lockID); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return false
	}
	var owners int
	if err := tx.QueryRow(
		"SELECT COUNT(*) FROM workspace_members WHERE workspace_id = $1 AND role = 'owner'",
		workspaceID,
	).Scan(&owners); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return false
	}
	if owners <= 1 {
		c.JSON(http.StatusConflict, gin.H{"error": "workspace must have at least one owner"})
		return false
	}

	n, err := mutate(tx)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return false
	}
	if n == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "member not found"})
		return false
	}
	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return false
	}
	return true
}

// ChangeMemberRole updates an existing member's role within the workspace named in
// the URL (admin+, gated on the :id param — IDOR-safe). Two guards beyond the
// admin gate:
//   - Ownership is owner-only: only a caller who is themselves an owner may grant
//     the owner role or modify someone who is already an owner. This stops an admin
//     from self-promoting to owner or demoting an owner.
//   - Last-owner guard: demoting the sole owner would leave the workspace
//     ownerless → 409. A workspace always keeps at least one owner.
//
// POST /api/v1/workspaces/:id/members/:userId/role
func ChangeMemberRole(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not connected"})
		return
	}
	workspaceID := c.Param("id")
	callerRole, ok := requireWorkspaceParamRole(c, workspaceID, security.WSAdmin)
	if !ok {
		return
	}
	callerID := c.GetString("user_id")
	targetUserID := c.Param("userId")

	var body struct {
		Role string `json:"role" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "role is required"})
		return
	}
	newRole := strings.TrimSpace(strings.ToLower(body.Role))
	if !security.WorkspaceRole(newRole).Valid() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "role must be one of owner, admin, member, viewer"})
		return
	}
	// Granting ownership is owner-only (fail fast, before any target lookup).
	if newRole == string(security.WSOwner) && !callerRole.Meets(security.WSOwner) {
		c.JSON(http.StatusForbidden, gin.H{"error": "only an owner can grant ownership"})
		return
	}

	// Resolve the target's current role (reuse the caller's when self-targeting).
	var targetRole security.WorkspaceRole
	if targetUserID == callerID {
		targetRole = callerRole
	} else {
		var tr string
		err := database.QueryRow(
			"SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
			workspaceID, targetUserID,
		).Scan(&tr)
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "member not found"})
			return
		}
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
			return
		}
		targetRole = security.WorkspaceRole(tr)
	}

	// Modifying an existing owner is owner-only.
	if targetRole == security.WSOwner && !callerRole.Meets(security.WSOwner) {
		c.JSON(http.StatusForbidden, gin.H{"error": "only an owner can change an owner's role"})
		return
	}

	// Demoting an owner could leave the workspace ownerless, so it runs under the
	// atomic last-owner guard (lock + count + write in one transaction).
	if targetRole == security.WSOwner && newRole != string(security.WSOwner) {
		ok := applyOwnerSafeMutation(c, database, workspaceID, func(tx *sql.Tx) (int64, error) {
			res, err := tx.Exec(
				"UPDATE workspace_members SET role = $1 WHERE workspace_id = $2 AND user_id = $3",
				newRole, workspaceID, targetUserID,
			)
			if err != nil {
				return 0, err
			}
			return res.RowsAffected()
		})
		if !ok {
			return
		}
		c.JSON(http.StatusOK, gin.H{"user_id": targetUserID, "role": newRole, "status": "updated"})
		return
	}

	// Not an owner demotion: the >=1-owner invariant is not at risk, so a plain
	// UPDATE is sufficient.
	res, err := database.Exec(
		"UPDATE workspace_members SET role = $1 WHERE workspace_id = $2 AND user_id = $3",
		newRole, workspaceID, targetUserID,
	)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "member not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": targetUserID, "role": newRole, "status": "updated"})
}

// RemoveMember removes a member from the workspace named in the URL. An admin+ may
// remove a non-owner; removing an owner is owner-only (symmetric with
// ChangeMemberRole). Any member may always remove THEMSELVES (self-leave). The
// atomic last-owner guard prevents removing the sole owner (→ 409). Scheduled
// pipelines are workspace-owned (pipelines.workspace_id), not user-owned, so
// removal neither pauses nor reassigns them — the schedule keeps running under the
// workspace.
//
// DELETE /api/v1/workspaces/:id/members/:userId
func RemoveMember(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not connected"})
		return
	}
	workspaceID := c.Param("id")
	callerRole, ok := requireWorkspaceParamRole(c, workspaceID, security.WSViewer)
	if !ok {
		return
	}
	callerID := c.GetString("user_id")
	targetUserID := c.Param("userId")
	isSelf := targetUserID == callerID
	if !isSelf && !callerRole.Meets(security.WSAdmin) {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin role required to remove other members"})
		return
	}

	// Resolve the target's current role (reuse the caller's when self-leaving).
	var targetRole security.WorkspaceRole
	if isSelf {
		targetRole = callerRole
	} else {
		var tr string
		err := database.QueryRow(
			"SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
			workspaceID, targetUserID,
		).Scan(&tr)
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "member not found"})
			return
		}
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
			return
		}
		targetRole = security.WorkspaceRole(tr)
	}

	// Removing an existing owner is owner-only (symmetric with ChangeMemberRole —
	// removal is the most severe modification of an owner). A member may always
	// remove THEMSELVES (self-leave), which then falls through to the last-owner
	// guard; only a non-owner removing a DIFFERENT owner is rejected here.
	if !isSelf && targetRole == security.WSOwner && !callerRole.Meets(security.WSOwner) {
		c.JSON(http.StatusForbidden, gin.H{"error": "only an owner can remove an owner"})
		return
	}

	// Removing an owner could leave the workspace ownerless, so it runs under the
	// atomic last-owner guard (lock + count + delete in one transaction).
	if targetRole == security.WSOwner {
		ok := applyOwnerSafeMutation(c, database, workspaceID, func(tx *sql.Tx) (int64, error) {
			res, err := tx.Exec(
				"DELETE FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
				workspaceID, targetUserID,
			)
			if err != nil {
				return 0, err
			}
			return res.RowsAffected()
		})
		if !ok {
			return
		}
		c.JSON(http.StatusOK, gin.H{"user_id": targetUserID, "status": "removed"})
		return
	}

	// Non-owner target: invariant not at risk, plain DELETE.
	res, err := database.Exec(
		"DELETE FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
		workspaceID, targetUserID,
	)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "member not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": targetUserID, "status": "removed"})
}

// DeleteWorkspace permanently deletes the workspace named in the URL. Owner-only
// (gated on the :id param — IDOR-safe). Two safety guards beyond the owner gate:
//
//   - Personal workspaces are undeletable: a user's auto-provisioned personal
//     workspace (is_personal = true) is their identity anchor and the backfill
//     target for connections (migration 069). Deleting it would orphan the user
//     from the workspace model → 409.
//   - Block-if-non-empty: pipelines.workspace_id, connections.workspace_id and
//     saved_queries.workspace_id are all ON DELETE CASCADE, so deleting the
//     workspace would silently cascade-delete every pipeline, connection and saved
//     query under it — and, through saved_query_id, every schedule and run record
//     belonging to those queries. We refuse while it still owns any of them → 409,
//     forcing the owner to remove them first so the destructive cascade is an
//     explicit, deliberate sequence.
//
// An empty, non-personal workspace deletes cleanly (204 No Content);
// workspace_members and workspace_invites cascade away with it. The count and the
// delete are not wrapped in one transaction: the only residual race is a pipeline
// or connection added between the count and the delete, which then cascade-deletes
// with the workspace — but that is the OWNER deleting their OWN just-emptied
// workspace, so the lost row is their own and the window is negligible.
//
// DELETE /api/v1/workspaces/:id
func DeleteWorkspace(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not connected"})
		return
	}
	workspaceID := c.Param("id")
	if _, ok := requireWorkspaceParamRole(c, workspaceID, security.WSOwner); !ok {
		return
	}

	// Personal workspaces are never deletable.
	var isPersonal bool
	err := database.QueryRow("SELECT is_personal FROM workspaces WHERE id = $1", workspaceID).Scan(&isPersonal)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "workspace not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}
	if isPersonal {
		c.JSON(http.StatusConflict, gin.H{"error": "personal workspaces cannot be deleted"})
		return
	}

	// Refuse to delete a workspace that still owns pipelines, connections or saved
	// queries — every one of those is ON DELETE CASCADE, so the delete would
	// silently take them with it. saved_queries counts here because its cascade
	// reaches furthest: saved_query_schedules and saved_query_runs hang off
	// saved_query_id, so one uncounted saved query can take live schedules and the
	// entire run history down with it (migrations 084-086).
	var resourceCount int
	err = database.QueryRow(`
		SELECT (SELECT COUNT(*) FROM pipelines     WHERE workspace_id = $1)
		     + (SELECT COUNT(*) FROM connections   WHERE workspace_id = $1)
		     + (SELECT COUNT(*) FROM saved_queries WHERE workspace_id = $1)`,
		workspaceID,
	).Scan(&resourceCount)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}
	if resourceCount > 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "workspace not empty; remove all pipelines, connections and saved queries first"})
		return
	}

	res, err := database.Exec("DELETE FROM workspaces WHERE id = $1", workspaceID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "workspace not found"})
		return
	}
	c.Status(http.StatusNoContent)
}

// WorkspaceInvite is a team-membership invitation. Token is the bearer secret an
// invitee presents to accept; it is surfaced ONLY to the inviter on creation —
// list/read responses omit it so a co-member cannot accept on someone's behalf.
type WorkspaceInvite struct {
	ID          string     `json:"id"`
	WorkspaceID string     `json:"workspace_id"`
	Email       string     `json:"email"`
	Role        string     `json:"role"`
	Status      string     `json:"status"`
	Token       string     `json:"token,omitempty"`
	InvitedBy   string     `json:"invited_by,omitempty"`
	ExpiresAt   time.Time  `json:"expires_at"`
	AcceptedAt  *time.Time `json:"accepted_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	// AcceptURL is the shareable frontend invite-landing link, set ONLY on creation
	// (alongside Token) so the inviter can share it directly when email is off. It is
	// never populated by list/read responses.
	AcceptURL string `json:"accept_url,omitempty"`
}

// CreateWorkspaceInvite mints a pending invitation for an email to join the
// workspace named in the URL. Admin+ only (gated on the :id-param workspace, not
// the active-workspace header). The invited role is never 'owner' — ownership is
// transferred explicitly, never granted by invite.
// POST /api/v1/workspaces/:id/invites
func CreateWorkspaceInvite(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not connected"})
		return
	}
	workspaceID := c.Param("id")
	if _, ok := requireWorkspaceParamRole(c, workspaceID, security.WSAdmin); !ok {
		return
	}

	// Personal workspaces are immutable — they never gain members.
	if !requireNonPersonalWorkspace(c, database, workspaceID) {
		return
	}
	userID := c.GetString("user_id")

	var body struct {
		Email string `json:"email" binding:"required"`
		Role  string `json:"role"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email is required"})
		return
	}
	email := strings.TrimSpace(body.Email)
	if email == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email cannot be blank"})
		return
	}
	if !isValidEmailAddress(email) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "a valid email address is required"})
		return
	}
	role := strings.TrimSpace(strings.ToLower(body.Role))
	if role == "" {
		role = string(security.WSMember)
	}
	if role == string(security.WSOwner) || !security.WorkspaceRole(role).Valid() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "role must be one of admin, member, viewer"})
		return
	}

	// Already a member? Compare case-insensitively on the joined user email.
	var exists int
	err := database.QueryRow(
		`SELECT 1 FROM workspace_members wm JOIN users u ON u.id = wm.user_id
		WHERE wm.workspace_id = $1 AND lower(u.email) = lower($2)`,
		workspaceID, email,
	).Scan(&exists)
	if err == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "user is already a member of this workspace"})
		return
	}
	if err != sql.ErrNoRows {
		log.Errorf("workspace: check membership before invite in %s: %v", workspaceID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to check membership"})
		return
	}

	// Outstanding pending invite for the same email?
	err = database.QueryRow(
		`SELECT 1 FROM workspace_invites
		WHERE workspace_id = $1 AND lower(email) = lower($2) AND status = 'pending'`,
		workspaceID, email,
	).Scan(&exists)
	if err == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "a pending invite already exists for this email"})
		return
	}
	if err != sql.ErrNoRows {
		log.Errorf("workspace: check existing invites in %s: %v", workspaceID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to check existing invites"})
		return
	}

	token, err := generateInviteToken()
	if err != nil {
		// The token itself is a bearer secret and is never logged, only the failure.
		log.Errorf("workspace: generate invite token for %s: %v", workspaceID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate invite token"})
		return
	}

	inv := WorkspaceInvite{WorkspaceID: workspaceID, Email: email, Role: role, Token: token, InvitedBy: userID}
	err = database.QueryRow(
		`INSERT INTO workspace_invites (workspace_id, email, role, token, invited_by, expires_at)
		VALUES ($1, $2, $3, $4, $5, NOW() + INTERVAL '7 days')
		RETURNING id, status, expires_at, created_at`,
		workspaceID, email, role, token, userID,
	).Scan(&inv.ID, &inv.Status, &inv.ExpiresAt, &inv.CreatedAt)
	if err != nil {
		log.Errorf("workspace: create invite in %s by %s: %v", workspaceID, userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create invite"})
		return
	}

	// The accept URL embeds the token and is returned to the admin who created the
	// invite so they can share the link even when email delivery is off; it is never
	// echoed by ListWorkspaceInvites.
	inv.AcceptURL = buildInviteAcceptURL(token)

	// Deliver the invite email asynchronously — best-effort, never blocks or fails
	// the request. Gated on IsConfigured() so self-hosted/air-gapped deployments
	// (no Resend key) skip the goroutine entirely; the accept_url in the response is
	// the fallback delivery channel.
	if emailClient.IsConfigured() {
		go sendWorkspaceInviteEmail(database, emailClient.SendWorkspaceInvite, email, role, inv.AcceptURL, workspaceID)
	}

	c.JSON(http.StatusCreated, inv)
}

// inviteEmailSender matches emailClient.SendWorkspaceInvite. Injecting it lets the
// wiring (workspace-name lookup + argument order) be unit-tested without a live
// Resend client — the handler's IsConfigured gate keeps it from running in tests.
type inviteEmailSender func(ctx context.Context, toEmail, workspaceName, role, acceptURL string) error

// sendWorkspaceInviteEmail looks up the workspace's display name and dispatches the
// invite email via send. Best-effort: a name-lookup miss falls back to a generic
// name (handled in SendWorkspaceInvite) and a send error is logged, never fatal.
// CreateWorkspaceInvite runs this in its own goroutine; the workspace-name lookup
// therefore never touches the request path when email is disabled.
func sendWorkspaceInviteEmail(database *sql.DB, send inviteEmailSender, toEmail, role, acceptURL, workspaceID string) {
	ctx := context.Background()
	wsName := ""
	if database != nil {
		var n string
		if e := database.QueryRow("SELECT name FROM workspaces WHERE id = $1", workspaceID).Scan(&n); e == nil {
			wsName = n
		}
	}
	if e := send(ctx, toEmail, wsName, role, acceptURL); e != nil {
		log.Errorf("workspace: failed to send invite email to %s: %v", toEmail, e)
	}
}

// buildInviteAcceptURL returns the shareable frontend invite-landing link for a
// token: <APP_BASE_URL>/invite/<token> (APP_BASE_URL defaults to
// http://localhost:3000, mirroring the auth email links). A trailing slash on the
// base is trimmed so the path never doubles up.
func buildInviteAcceptURL(token string) string {
	base := strings.TrimRight(strings.TrimSpace(os.Getenv("APP_BASE_URL")), "/")
	if base == "" {
		base = "http://localhost:3000"
	}
	return base + "/invite/" + token
}

// ListWorkspaceInvites returns the workspace's invitations. Any member may view
// the roster of pending/historical invites; the bearer token is never included.
// GET /api/v1/workspaces/:id/invites
func ListWorkspaceInvites(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not connected"})
		return
	}
	workspaceID := c.Param("id")
	if _, ok := requireWorkspaceParamRole(c, workspaceID, security.WSMember); !ok {
		return
	}

	rows, err := database.Query(
		`SELECT id, workspace_id, email, role, status, invited_by, expires_at, accepted_at, created_at
		FROM workspace_invites WHERE workspace_id = $1
		ORDER BY created_at DESC`,
		workspaceID,
	)
	if err != nil {
		log.Errorf("workspace: list invites of %s: %v", workspaceID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list invites"})
		return
	}
	defer rows.Close()

	invites := []WorkspaceInvite{}
	for rows.Next() {
		var inv WorkspaceInvite
		var acceptedAt sql.NullTime
		if err := rows.Scan(&inv.ID, &inv.WorkspaceID, &inv.Email, &inv.Role, &inv.Status,
			&inv.InvitedBy, &inv.ExpiresAt, &acceptedAt, &inv.CreatedAt); err != nil {
			continue
		}
		if acceptedAt.Valid {
			t := acceptedAt.Time
			inv.AcceptedAt = &t
		}
		// Token deliberately left zero (omitempty) — it is a bearer secret.
		invites = append(invites, inv)
	}
	c.JSON(http.StatusOK, gin.H{"invites": invites})
}

// RevokeWorkspaceInvite cancels a pending invitation (admin+). The update is
// scoped by BOTH invite id AND workspace_id so an inviteId belonging to another
// workspace can never be revoked through this path, and only 'pending' invites
// transition — a missed match (already accepted/revoked, wrong workspace, or
// unknown id) is a 404.
// DELETE /api/v1/workspaces/:id/invites/:inviteId
func RevokeWorkspaceInvite(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not connected"})
		return
	}
	workspaceID := c.Param("id")
	if _, ok := requireWorkspaceParamRole(c, workspaceID, security.WSAdmin); !ok {
		return
	}
	inviteID := c.Param("inviteId")

	res, err := database.Exec(
		`UPDATE workspace_invites SET status = 'revoked' WHERE id = $1 AND workspace_id = $2 AND status = 'pending'`,
		inviteID, workspaceID,
	)
	if err != nil {
		log.Errorf("workspace: revoke invite %s in %s: %v", inviteID, workspaceID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to revoke invite"})
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "invite not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "revoked"})
}

// GetWorkspaceInvite returns a public preview of an invitation by its token so an
// invitee can see what they're joining before authenticating. Rate-limited at the
// route to blunt enumeration (the 256-bit token already makes guessing
// infeasible); the token is never echoed back.
// GET /api/v1/workspace-invites/:token
func GetWorkspaceInvite(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not connected"})
		return
	}
	token := c.Param("token")
	if token == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "invite not found"})
		return
	}

	var workspaceID, email, role, status, wsName string
	var expiresAt time.Time
	err := database.QueryRow(
		`SELECT i.workspace_id, i.email, i.role, i.status, i.expires_at, w.name
		FROM workspace_invites i JOIN workspaces w ON w.id = i.workspace_id
		WHERE i.token = $1`,
		token,
	).Scan(&workspaceID, &email, &role, &status, &expiresAt, &wsName)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "invite not found"})
		return
	}
	if err != nil {
		// No identifier logged: the only one in scope is the token, a bearer secret.
		log.Errorf("workspace: load invite by token: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load invite"})
		return
	}

	if status != "pending" {
		c.JSON(http.StatusGone, gin.H{"error": "invite is no longer valid", "status": status})
		return
	}
	if expiresAt.Before(time.Now()) {
		c.JSON(http.StatusGone, gin.H{"error": "invite has expired", "status": "expired"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"workspace_id":   workspaceID,
		"workspace_name": wsName,
		"email":          email,
		"role":           role,
		"status":         status,
		"expires_at":     expiresAt,
	})
}

// AcceptWorkspaceInvite claims a pending invitation for the authenticated caller.
// Security invariants:
//   - email-bind HARD CHECK: the caller's authoritative account email must equal
//     the invite email (case-insensitive). A mismatch rolls the claim back so the
//     invite stays pending for the intended recipient.
//   - single-use: the claim is an atomic UPDATE ... WHERE status='pending' AND
//     expires_at > NOW() RETURNING, so concurrent accepts race on one row and
//     only the winner joins.
//   - expired → 410; already accepted/revoked → 409; unknown token → 404.
//
// The route runs under WorkspaceContextMiddleware, but the handler never reads
// the active workspace — it joins the invite's workspace, which the caller is not
// yet a member of. Send the accept request with no X-Workspace-ID (the personal
// fallback applies); a stale cookie also falls through safely.
// POST /api/v1/workspace-invites/:token/accept
func AcceptWorkspaceInvite(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not connected"})
		return
	}
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	token := c.Param("token")
	if token == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "invite not found"})
		return
	}

	// Authoritative email for the bind check — read from the DB, never trust a
	// header/claim.
	var userEmail string
	if err := database.QueryRow(`SELECT email FROM users WHERE id = $1`, userID).Scan(&userEmail); err != nil {
		log.Errorf("workspace: resolve account %s to accept invite: %v", userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to resolve account"})
		return
	}

	tx, err := database.Begin()
	if err != nil {
		log.Errorf("workspace: begin tx to accept invite for %s: %v", userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start transaction"})
		return
	}
	defer tx.Rollback() //nolint:errcheck

	var wsID, inviteEmail, role string
	err = tx.QueryRow(
		`UPDATE workspace_invites SET status = 'accepted', accepted_at = NOW(), accepted_by = $1
		WHERE token = $2 AND status = 'pending' AND expires_at > NOW()
		RETURNING workspace_id, email, role`,
		userID, token,
	).Scan(&wsID, &inviteEmail, &role)
	if err == sql.ErrNoRows {
		// Nothing claimed — disambiguate for a useful status code.
		var status string
		var expiresAt time.Time
		e2 := tx.QueryRow(`SELECT status, expires_at FROM workspace_invites WHERE token = $1`, token).
			Scan(&status, &expiresAt)
		switch {
		case e2 == sql.ErrNoRows:
			c.JSON(http.StatusNotFound, gin.H{"error": "invite not found"})
		case e2 != nil:
			log.Errorf("workspace: disambiguate unclaimed invite for %s: %v", userID, e2)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load invite"})
		case status != "pending":
			c.JSON(http.StatusConflict, gin.H{"error": "invite has already been used or revoked"})
		default:
			c.JSON(http.StatusGone, gin.H{"error": "invite has expired"})
		}
		return
	}
	if err != nil {
		log.Errorf("workspace: claim invite for %s: %v", userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to accept invite"})
		return
	}

	// Email-bind hard check. Mismatch → return without commit; the deferred
	// rollback leaves the invite pending for the intended recipient.
	if !strings.EqualFold(strings.TrimSpace(userEmail), strings.TrimSpace(inviteEmail)) {
		c.JSON(http.StatusForbidden, gin.H{"error": "this invite was issued to a different email address"})
		return
	}

	if _, err := tx.Exec(
		`INSERT INTO workspace_members (workspace_id, user_id, role)
		VALUES ($1, $2, $3)
		ON CONFLICT (workspace_id, user_id) DO NOTHING`,
		wsID, userID, role,
	); err != nil {
		log.Errorf("workspace: add %s to %s as %s on invite accept: %v", userID, wsID, role, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to add membership"})
		return
	}

	if err := tx.Commit(); err != nil {
		log.Errorf("workspace: commit invite accept of %s by %s: %v", wsID, userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to commit"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"workspace_id": wsID, "role": role, "status": "accepted"})
}
