package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	sharedcrypto "github.com/rsync-ai/shared/crypto"
	"github.com/rsync-ai/shared/transforms"
	log "github.com/sirupsen/logrus"
)

// TransformHandler handles transform-related API endpoints
type TransformHandler struct {
	db            *sql.DB
	llmServiceURL string
}

// NewTransformHandler creates a new transform handler
func NewTransformHandler(db *sql.DB, llmServiceURL string) *TransformHandler {
	if llmServiceURL == "" {
		llmServiceURL = "http://llm-service:5000"
	}
	return &TransformHandler{
		db:            db,
		llmServiceURL: llmServiceURL,
	}
}

// TransformDefinition represents a stored transform definition
type TransformDefinition struct {
	ID              string                 `json:"id"`
	PipelineID      string                 `json:"pipeline_id"`
	TransformType   string                 `json:"transform_type"`
	TransformOrder  int                    `json:"transform_order"`
	TransformConfig map[string]interface{} `json:"transform_config"`
	Enabled         bool                   `json:"enabled"`
	CreatedAt       time.Time              `json:"created_at"`
	UpdatedAt       time.Time              `json:"updated_at"`
}

// TransformPlan represents a complete transformation plan
type TransformPlan struct {
	ID                 string                `json:"id"`
	PipelineID         string                `json:"pipeline_id"`
	ProducerTransforms []TransformDefinition `json:"producer_transforms"`
	ConsumerTransforms []TransformDefinition `json:"consumer_transforms"`
	PIIMaskingRules    []PIIMaskingRule      `json:"pii_masking_rules,omitempty"`
	GeneratedFrom      string                `json:"generated_from"`
	CreatedAt          time.Time             `json:"created_at"`
}

// PIIMaskingRule represents a PII masking rule in a transform plan
type PIIMaskingRule struct {
	TableName    string                 `json:"table_name,omitempty"`
	ColumnName   string                 `json:"column_name"`
	PIIType      string                 `json:"pii_type"`
	Action       string                 `json:"action"`
	HashFunction string                 `json:"hash_function,omitempty"`
	CustomConfig map[string]interface{} `json:"custom_config,omitempty"`
}

// RegisterRoutes registers transform routes
func (h *TransformHandler) RegisterRoutes(r *gin.RouterGroup) {
	transforms := r.Group("/transforms")
	{
		transforms.POST("/parse", h.ParseNaturalLanguage)
		transforms.POST("/preview", h.PreviewTransforms)
		transforms.GET("/pipeline/:pipeline_id", h.GetPipelineTransforms)
		// Read-only monitoring + versioning views (transform_monitoring.go).
		transforms.GET("/pipeline/:pipeline_id/rollup", h.GetPipelineTransformRollup)
		transforms.GET("/pipeline/:pipeline_id/config-history", h.GetPipelineTransformConfigHistory)
		transforms.POST("/pipeline/:pipeline_id", h.SavePipelineTransforms)
		transforms.DELETE("/pipeline/:pipeline_id", h.DeletePipelineTransforms)
		transforms.PUT("/:id", h.UpdateTransform)
		transforms.DELETE("/:id", h.DeleteTransform)
	}
}

// ParseNaturalLanguage parses natural language into transform rules
func (h *TransformHandler) ParseNaturalLanguage(c *gin.Context) {
	var req struct {
		NaturalLanguage string                 `json:"natural_language" binding:"required"`
		Schema          map[string]interface{} `json:"schema,omitempty"`
		PIIColumns      []PIIColumnInfo        `json:"pii_columns,omitempty"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}

	// Parse using rule-based approach (LLM integration can be added later)
	transforms := parseTransformRequest(req.NaturalLanguage, req.PIIColumns)

	c.JSON(http.StatusOK, gin.H{
		"transforms":  transforms,
		"parsed_from": "rule_based",
		"confidence":  0.8,
	})
}

// PIIColumnInfo for transform parsing
type PIIColumnInfo struct {
	TableName  string  `json:"table_name"`
	ColumnName string  `json:"column_name"`
	PIIType    string  `json:"pii_type"`
	Confidence float64 `json:"confidence"`
}

// parseTransformRequest parses NL into transform rules (rule-based)
func parseTransformRequest(nl string, piiColumns []PIIColumnInfo) []TransformDefinition {
	var transforms []TransformDefinition
	nl = strings.ToLower(nl)
	order := 0

	// Detect filter operations
	if strings.Contains(nl, "filter") || strings.Contains(nl, "where") || strings.Contains(nl, "only") {
		// Extract condition (simplified)
		condition := extractCondition(nl)
		if condition != "" {
			transforms = append(transforms, TransformDefinition{
				ID:             uuid.New().String(),
				TransformType:  "producer",
				TransformOrder: order,
				TransformConfig: map[string]interface{}{
					"operation": "filter",
					"condition": condition,
				},
				Enabled: true,
			})
			order++
		}
	}

	// Detect masking operations
	if strings.Contains(nl, "mask") || strings.Contains(nl, "hash") || strings.Contains(nl, "encrypt") {
		for _, col := range piiColumns {
			if col.Confidence >= 0.5 {
				transforms = append(transforms, TransformDefinition{
					ID:             uuid.New().String(),
					TransformType:  "producer",
					TransformOrder: order,
					TransformConfig: map[string]interface{}{
						"operation":     "mask",
						"column":        col.ColumnName,
						"mask_type":     "hash",
						"hash_function": "sha256",
						"pii_type":      col.PIIType,
					},
					Enabled: true,
				})
				order++
			}
		}
	}

	// NOTE: aggregate/join operations are intentionally NOT emitted here. No
	// transform engine can execute them (the Tier-2/SQL engine is a stub), so
	// suggesting them produced config that saved cleanly then hard-failed at
	// pipeline run time (DLQ). Re-add — with extractAggregation/extractJoin —
	// only once a real engine exists.

	return transforms
}

// Helper functions for NL parsing
func extractCondition(nl string) string {
	// Simple extraction - in production, use proper NLP
	patterns := []string{"where ", "filter ", "only "}
	for _, p := range patterns {
		if idx := strings.Index(nl, p); idx != -1 {
			rest := nl[idx+len(p):]
			// Find end of condition
			endPatterns := []string{" and ", " or ", " group", " aggregate", " join", " sort"}
			endIdx := len(rest)
			for _, ep := range endPatterns {
				if eIdx := strings.Index(rest, ep); eIdx != -1 && eIdx < endIdx {
					endIdx = eIdx
				}
			}
			return strings.TrimSpace(rest[:endIdx])
		}
	}
	return ""
}

// PreviewTransforms previews the result of transformations using the real transform engine
func (h *TransformHandler) PreviewTransforms(c *gin.Context) {
	var req struct {
		Transforms         []map[string]interface{} `json:"transforms"`
		SampleData         []map[string]interface{} `json:"sample_data"`
		SourceConnectionID string                   `json:"source_connection_id"`
		Table              string                   `json:"table"`
		Limit              int                      `json:"limit"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}

	warningsOut := make([]string, 0)
	sampleData := req.SampleData

	table := strings.TrimSpace(req.Table)

	// Option B: fetch sample rows server-side.
	if len(sampleData) == 0 && strings.TrimSpace(req.SourceConnectionID) != "" {
		workspaceID, ok := resolveActiveWorkspace(c)
		if !ok {
			return
		}
		if table == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "table is required when using source_connection_id"})
			return
		}
		limit := req.Limit
		if limit <= 0 {
			limit = 10
		}
		if limit > 100 {
			limit = 100
		}
		if h.db == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
			return
		}

		var connectorType, configEncrypted string
		err := h.db.QueryRow(`
			SELECT connector_type, config
			FROM connections
			WHERE id = $1 AND workspace_id = $2
		`, strings.TrimSpace(req.SourceConnectionID), workspaceID).Scan(&connectorType, &configEncrypted)
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load connection"})
			return
		}

		configJSON, err := sharedcrypto.DecryptString(configEncrypted)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decrypt config"})
			return
		}

		var config map[string]interface{}
		_ = json.Unmarshal([]byte(configJSON), &config)

		logConnectionAccess(c, strings.TrimSpace(req.SourceConnectionID), "transforms_preview_sample", true, "")

		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		res, err := sampleDataFromConnection(ctx, connectorType, config, table, limit)
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				c.JSON(http.StatusRequestTimeout, gin.H{
					"error": "Sampling timed out",
					"hint":  "The query took longer than 5 seconds. Try a smaller table or lower limit.",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to sample data: %v", err)})
			return
		}
		sampleData = res.Rows
		warningsOut = append(warningsOut, res.Warnings...)
	}

	if len(sampleData) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "either sample_data or source_connection_id is required for preview"})
		return
	}

	// Normalize + validate transforms (best-effort in preview mode).
	canonical, normWarnings, _ := transforms.NormalizeAndValidate(req.Transforms, table, transforms.NormalizeModePreview)
	warningsOut = append(warningsOut, normWarnings...)

	transformList := make([]transforms.Transform, 0, len(canonical))
	for _, t := range canonical {
		transformList = append(transformList, t.EngineTransform())
	}

	// Create transform engine and coordinator
	tier1 := transforms.NewSimpleTransformEngine()
	tier2 := transforms.NewDuckDBTransformEngine() // Stubbed for Phase 2
	coordinator := transforms.NewTransformCoordinator(tier1, tier2)
	executor := transforms.NewPreviewExecutor(coordinator)

	// Convert sample data to transforms.Row format
	sampleRows := make([]transforms.Row, len(sampleData))
	for i, row := range sampleData {
		sampleRows[i] = transforms.Row(row)
	}

	// Execute preview with 3 second timeout
	result, warnings, err := executor.Preview(sampleRows, transformList, 3*time.Second)
	if err != nil {
		log.WithError(err).Error("Transform preview failed")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Transform preview failed: %v", err),
		})
		return
	}

	// Convert result back to []map[string]interface{}
	preview := make([]map[string]interface{}, len(result))
	for i, row := range result {
		preview[i] = map[string]interface{}(row)
	}

	c.JSON(http.StatusOK, gin.H{
		"preview":            preview,
		"original_rows":      sampleData, // include originals so UI can show before/after diff
		"transforms_applied": len(transformList),
		"sample_size":        len(preview),
		// Merge engine warnings + normalization warnings
		"warnings": append(warningsOut, warnings...),
	})
}

// GetPipelineTransforms gets transforms for a pipeline
func (h *TransformHandler) GetPipelineTransforms(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "pipeline_id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}

	// Tenant-isolation gate (IDOR): transform_definitions has no workspace_id, so
	// prove the pipeline lives in the caller's ACTIVE workspace (>= viewer) before
	// leaking its transform_config (filter conditions, column lists, PII-masking
	// rules). 404 when the pipeline is not in the active workspace — never reveal
	// its existence.
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	query := `
		SELECT id, pipeline_id::text, transform_type, transform_order, transform_config, enabled, created_at, updated_at
		FROM transform_definitions
		WHERE pipeline_id = $1
		ORDER BY transform_order
	`

	rows, err := h.db.Query(query, pipelineID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch transforms"})
		return
	}
	defer rows.Close()

	var producerTransforms []TransformDefinition
	var consumerTransforms []TransformDefinition

	for rows.Next() {
		var t TransformDefinition
		var configJSON []byte

		err := rows.Scan(
			&t.ID, &t.PipelineID, &t.TransformType, &t.TransformOrder,
			&configJSON, &t.Enabled, &t.CreatedAt, &t.UpdatedAt,
		)
		if err != nil {
			continue
		}

		json.Unmarshal(configJSON, &t.TransformConfig)

		if t.TransformType == "producer" {
			producerTransforms = append(producerTransforms, t)
		} else {
			consumerTransforms = append(consumerTransforms, t)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"pipeline_id":         pipelineID,
		"producer_transforms": producerTransforms,
		"consumer_transforms": consumerTransforms,
	})
}

// SavePipelineTransforms saves transforms for a pipeline
func (h *TransformHandler) SavePipelineTransforms(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "pipeline_id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}

	// Tenant-isolation gate (IDOR): a member of the ACTIVE workspace (>= member)
	// may overwrite the pipeline's transforms; 404 otherwise. Without this an
	// attacker in workspace A could DELETE+replace workspace B's transforms —
	// including the mask_pii transforms the NL gate materialized — causing B's
	// next run to copy PII to the destination UNMASKED.
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}

	var req struct {
		Transforms []TransformDefinition `json:"transforms" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}

	// Start transaction
	tx, err := h.db.BeginTx(c.Request.Context(), &sql.TxOptions{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to start transaction"})
		return
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Delete existing transforms
	_, err = tx.Exec("DELETE FROM transform_definitions WHERE pipeline_id = $1", pipelineID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete existing transforms"})
		return
	}

	// Insert new transforms
	now := time.Now()
	for i, t := range req.Transforms {
		if t.ID == "" {
			t.ID = uuid.New().String()
		}

		configJSON, mErr := json.Marshal(t.TransformConfig)
		if mErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid transform config"})
			return
		}

		_, err = tx.Exec(`
			INSERT INTO transform_definitions (id, pipeline_id, transform_type, transform_order, transform_config, enabled, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
		`, t.ID, pipelineID, t.TransformType, i, configJSON, t.Enabled, now)

		if err != nil {
			log.WithError(err).Error("Failed to insert transform")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save transform"})
			return
		}
	}

	if err := tx.Commit(); err != nil {
		// Best-effort: commit errors can be ambiguous. If expected rows are present, treat as success.
		var cnt int
		if verr := h.db.QueryRow(`SELECT COUNT(*) FROM transform_definitions WHERE pipeline_id = $1`, pipelineID).Scan(&cnt); verr == nil && cnt == len(req.Transforms) {
			committed = true
			c.JSON(http.StatusOK, gin.H{
				"message": "Transforms saved",
				"count":   len(req.Transforms),
			})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to commit transaction"})
		return
	}
	committed = true

	c.JSON(http.StatusOK, gin.H{
		"message": "Transforms saved",
		"count":   len(req.Transforms),
	})
}

// DeletePipelineTransforms deletes all transforms for a pipeline
func (h *TransformHandler) DeletePipelineTransforms(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "pipeline_id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}

	// Tenant-isolation gate (IDOR): only a member of the pipeline's ACTIVE
	// workspace (>= member) may delete its transforms; 404 otherwise.
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}

	result, err := h.db.Exec("DELETE FROM transform_definitions WHERE pipeline_id = $1", pipelineID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete transforms"})
		return
	}

	rows, _ := result.RowsAffected()
	c.JSON(http.StatusOK, gin.H{
		"message": "Transforms deleted",
		"count":   rows,
	})
}

// requireTransformWorkspaceRole is the tenant-isolation gate for the id-keyed
// transform handlers (Update/Delete). transform_definitions has no workspace_id
// column — only a pipeline_id FK — so we resolve the parent pipeline, then prove
// the caller holds >= min in that pipeline's workspace (their active workspace).
// This mirrors the pipeline_schedules pattern (getSchedule → requirePipelineWorkspaceRole).
//
// On any failure it has already written the response and returns ("", false):
//   - transform missing / orphaned (NULL pipeline_id) → 404 (never reveal existence)
//   - pipeline not in the caller's active workspace / caller not a member → 404
//   - role below min → 403 · DB down → 503
func (h *TransformHandler) requireTransformWorkspaceRole(c *gin.Context, transformID string, min security.WorkspaceRole) (string, bool) {
	if h.db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return "", false
	}
	var pipelineID sql.NullString
	err := h.db.QueryRow(
		`SELECT pipeline_id::text FROM transform_definitions WHERE id = $1`,
		transformID,
	).Scan(&pipelineID)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Transform not found"})
		return "", false
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return "", false
	}
	if !pipelineID.Valid || pipelineID.String == "" {
		// pipeline_id is nullable; an orphaned transform has no provable tenancy.
		c.JSON(http.StatusNotFound, gin.H{"error": "Transform not found"})
		return "", false
	}
	return requirePipelineWorkspaceRole(c, pipelineID.String, min)
}

// UpdateTransform updates a single transform
func (h *TransformHandler) UpdateTransform(c *gin.Context) {
	transformID, ok := requireUUIDParam(c, "id", "invalid_transform_id", "Invalid transform ID format")
	if !ok {
		return
	}

	// Tenant-isolation gate (IDOR): join transform_definitions -> pipelines and
	// require >= member in the pipeline's active workspace; 404 otherwise.
	if _, ok := h.requireTransformWorkspaceRole(c, transformID, security.WSMember); !ok {
		return
	}

	var req struct {
		TransformConfig map[string]interface{} `json:"transform_config"`
		TransformOrder  *int                   `json:"transform_order"`
		Enabled         *bool                  `json:"enabled"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}

	query := "UPDATE transform_definitions SET updated_at = $1"
	args := []interface{}{time.Now()}
	argNum := 2

	if req.TransformConfig != nil {
		configJSON, _ := json.Marshal(req.TransformConfig)
		query += fmt.Sprintf(", transform_config = $%d", argNum)
		args = append(args, configJSON)
		argNum++
	}
	if req.TransformOrder != nil {
		query += fmt.Sprintf(", transform_order = $%d", argNum)
		args = append(args, *req.TransformOrder)
		argNum++
	}
	if req.Enabled != nil {
		query += fmt.Sprintf(", enabled = $%d", argNum)
		args = append(args, *req.Enabled)
		argNum++
	}

	query += fmt.Sprintf(" WHERE id = $%d", argNum)
	args = append(args, transformID)

	result, err := h.db.Exec(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update transform"})
		return
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Transform not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Transform updated"})
}

// DeleteTransform deletes a single transform
func (h *TransformHandler) DeleteTransform(c *gin.Context) {
	transformID, ok := requireUUIDParam(c, "id", "invalid_transform_id", "Invalid transform ID format")
	if !ok {
		return
	}

	// Tenant-isolation gate (IDOR): join transform_definitions -> pipelines and
	// require >= member in the pipeline's active workspace; 404 otherwise.
	if _, ok := h.requireTransformWorkspaceRole(c, transformID, security.WSMember); !ok {
		return
	}

	result, err := h.db.Exec("DELETE FROM transform_definitions WHERE id = $1", transformID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete transform"})
		return
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Transform not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Transform deleted"})
}
