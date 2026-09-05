package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type UpdatePipelineTablesRequest struct {
	// Preferred field name for this endpoint
	Tables []string `json:"tables"`
	// Back-compat with HITL payload naming
	SelectedTables []string `json:"selected_tables"`
}

// UpdatePipelineTables persists a pipeline-level table selection (pipelines.config.selected_tables).
// This is used for future runs so users don't have to select tables repeatedly once confirmed.
//
// POST /api/v1/pipelines/:id/tables
func UpdatePipelineTables(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	pipelineID := strings.TrimSpace(c.Param("id"))
	if _, err := uuid.Parse(pipelineID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}

	// RBAC: mutating a pipeline's selected tables requires at least `member` in
	// the active workspace — membership alone is not enough (viewers are
	// read-only).
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}

	var req UpdatePipelineTablesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	raw := req.Tables
	if len(raw) == 0 {
		raw = req.SelectedTables
	}

	tables := normalizeSelectedTables(raw)

	// Expand any "select entire database" ("*") / "select entire namespace"
	// ("<ns>.*") sentinel into an explicit list before persisting — the run path
	// reads pipelines.config.selected_tables verbatim, so a sentinel must never
	// be stored raw.
	if resolved, _, rerr := resolveSelectionForPipeline(c, pipelineSourceConnectionID(pipelineID), tables); rerr != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "table_resolution_failed", "message": rerr.Error()})
		return
	} else {
		tables = resolved
	}

	if len(tables) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no_tables", "message": "Provide at least one table"})
		return
	}

	b, err := json.Marshal(tables)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to encode tables"})
		return
	}

	_, err = database.Exec(`
		UPDATE pipelines
		SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{selected_tables}', $1::jsonb, true),
		    updated_at = NOW()
		WHERE id = $2::uuid
	`, string(b), pipelineID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update pipeline tables"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success":     true,
		"pipeline_id": pipelineID,
		"tables":      tables,
	})
}
