package handlers

import (
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
)

// requirePipelineWorkspaceRole is the gate for every /pipelines/:id* handler.
// Unlike the prior membership-only check (which authorized any workspace member
// regardless of role and re-scoped writes by created_by), this
// authorizes against the caller's ACTIVE workspace via requireResourceRole —
// proving (a) the pipeline exists, (b) it lives in the active workspace, and
// (c) the caller holds at least `min` there — then returns the active workspace
// id so the write can scope by workspace_id. That makes a pipeline a SHARED,
// workspace-owned resource: any member (per `min`) may mutate a teammate's
// pipeline, and the write's tenancy boundary is the workspace, not the creator.
//
// On any failure requireResourceRole has already written the response
// (401/403/404/500/503); the caller simply returns.
func requirePipelineWorkspaceRole(c *gin.Context, pipelineID string, min security.WorkspaceRole) (string, bool) {
	if _, ok := requireResourceRole(c, "pipelines", pipelineID, min); !ok {
		return "", false
	}
	// requireResourceRole guarantees a non-empty active workspace on success.
	return activeWorkspaceID(c), true
}
