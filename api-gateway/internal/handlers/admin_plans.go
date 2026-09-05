package handlers

import (
	"database/sql"
	"net/http"
	"strings"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// setWorkspacePlanRequest is the body of POST /admin/workspaces/:id/plan.
type setWorkspacePlanRequest struct {
	Plan string `json:"plan"`
}

// AdminSetWorkspacePlan sets a workspace's billing plan. This is the interim
// manual-upgrade mechanism until Stripe (P2) becomes the authoritative writer of
// workspaces.plan.
//
// POST /api/v1/admin/workspaces/:id/plan  { "plan": "starter" }
//
// It is a PLATFORM-admin action: the route sits under AdminRoleMiddleware
// (users.role='admin'), deliberately NOT a workspace-role gate — a workspace
// admin who could set their own plan to a paid tier would be a free
// self-upgrade. Setting a plan clears plan_expires_at so a manual grant does not
// silently auto-expire; the resolver treats a never-expiring plan as active
// (see plan_quota.go).
func AdminSetWorkspacePlan(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	workspaceID := strings.TrimSpace(c.Param("id"))
	if workspaceID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "workspace id is required"})
		return
	}

	var req setWorkspacePlanRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	plan := strings.TrimSpace(req.Plan)
	if plan == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "plan is required"})
		return
	}

	// workspaces.plan has no FK to plans (it is a free VARCHAR, migration 047),
	// so validate the requested plan against the catalogue ourselves rather than
	// writing a typo'd tier that would silently fail open to unlimited.
	var one int
	switch err := database.QueryRow(`SELECT 1 FROM plans WHERE name = $1`, plan).Scan(&one); err {
	case nil:
		// valid plan
	case sql.ErrNoRows:
		c.JSON(http.StatusBadRequest, gin.H{"error": "unknown plan", "plan": plan})
		return
	default:
		log.WithError(err).Errorf("[AdminSetWorkspacePlan] plan validation failed for %q", plan)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to validate plan"})
		return
	}

	res, err := database.Exec(
		`UPDATE workspaces SET plan = $1, plan_expires_at = NULL, updated_at = NOW() WHERE id = $2`,
		plan, workspaceID,
	)
	if err != nil {
		log.WithError(err).Errorf("[AdminSetWorkspacePlan] update failed for workspace %s", workspaceID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to set plan"})
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "workspace not found", "workspace_id": workspaceID})
		return
	}

	logAudit(c, "workspace.plan.set", "workspace", workspaceID, map[string]interface{}{"plan": plan})

	c.JSON(http.StatusOK, gin.H{
		"success":      true,
		"workspace_id": workspaceID,
		"plan":         plan,
	})
}
