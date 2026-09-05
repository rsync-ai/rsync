package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// GetPipelineCheckpoints returns checkpoints for a pipeline (for UI guidance)
func GetPipelineCheckpoints(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}

	// Gate on the ACTIVE workspace, not on created_by. The old creator check was
	// workspace-blind in both directions: it let the creator read this pipeline's
	// checkpoints (connection ids, source table names, byte/row cursors) while a
	// DIFFERENT workspace was active, and it denied a teammate who legitimately
	// shares the pipeline's workspace but did not create it.
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	// Get checkpoints
	rows, err := database.Query(`
		SELECT id, pipeline_id, connection_id, source_table, position, created_at, updated_at
		FROM pipeline_checkpoints
		WHERE pipeline_id = $1
		ORDER BY source_table
	`, pipelineID)

	if err != nil {
		log.WithError(err).Error("Failed to query checkpoints")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get checkpoints"})
		return
	}
	defer rows.Close()

	type Checkpoint struct {
		ID           string                 `json:"id"`
		PipelineID   string                 `json:"pipeline_id"`
		ConnectionID string                 `json:"connection_id"`
		SourceTable  string                 `json:"source_table"`
		Position     map[string]interface{} `json:"position"`
		CreatedAt    time.Time              `json:"created_at"`
		UpdatedAt    time.Time              `json:"updated_at"`
	}

	checkpoints := []Checkpoint{}
	for rows.Next() {
		var cp Checkpoint
		var positionJSON []byte

		err := rows.Scan(&cp.ID, &cp.PipelineID, &cp.ConnectionID, &cp.SourceTable, &positionJSON, &cp.CreatedAt, &cp.UpdatedAt)
		if err != nil {
			log.WithError(err).Warn("Failed to scan checkpoint")
			continue
		}

		if len(positionJSON) > 0 {
			if err := json.Unmarshal(positionJSON, &cp.Position); err != nil {
				log.WithError(err).Warn("Failed to unmarshal position")
			}
		}

		checkpoints = append(checkpoints, cp)
	}

	c.JSON(http.StatusOK, gin.H{
		"pipeline_id": pipelineID,
		"checkpoints": checkpoints,
		"count":       len(checkpoints),
	})
}
