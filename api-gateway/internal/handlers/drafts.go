package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// ============================================================================
// Pipeline Drafts Handler
// ============================================================================
// Manages pipeline drafts for the right-side panel agentic chat flow.
// Supports: create, get, update, delete with optimistic concurrency control.

// PipelineDraft represents a pipeline draft
type PipelineDraft struct {
	ID             string          `json:"id"`
	UserID         string          `json:"user_id"`
	State          string          `json:"state"`        // intent|connections|tables|config|review
	Version        int             `json:"version"`      // Optimistic concurrency control
	Status         string          `json:"status"`       // active|archived (promote-to-pipeline is not yet implemented; ARCH-1)
	TemplateID     *string         `json:"template_id"`  // Template slug or null
	ChatHistory    json.RawMessage `json:"chat_history"` // Array of messages
	Config         json.RawMessage `json:"config"`       // Pipeline config being built
	LastActivityAt time.Time       `json:"last_activity_at"`
	ExpiresAt      time.Time       `json:"expires_at"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// draftPayload is stored inside pipeline_drafts.draft (jsonb).
// We keep these fields in JSON so we can support the older pipeline_drafts table schema
// (migration 027) while still exposing a stable API contract to the frontend.
type draftPayload struct {
	State       string          `json:"state"`
	Version     int             `json:"version"`
	TemplateID  *string         `json:"template_id,omitempty"`
	ChatHistory json.RawMessage `json:"chat_history"`
	Config      json.RawMessage `json:"config"`
}

// CreateDraftRequest represents a request to create a draft
type CreateDraftRequest struct {
	TemplateID  *string         `json:"template_id"`
	State       string          `json:"state"`        // Default: intent
	ChatHistory json.RawMessage `json:"chat_history"` // Optional initial messages
	Config      json.RawMessage `json:"config"`       // Optional initial config
}

// UpdateDraftRequest represents a request to update a draft
type UpdateDraftRequest struct {
	Version     int             `json:"version" binding:"required"` // Must match current version
	State       *string         `json:"state"`
	ChatHistory json.RawMessage `json:"chat_history"`
	Config      json.RawMessage `json:"config"`
}

// getDraftTTL returns the draft TTL duration from env var (default 24h)
func getDraftTTL() time.Duration {
	ttlHours := 24
	if envStr := strings.TrimSpace(os.Getenv("DRAFT_TTL_HOURS")); envStr != "" {
		if v, err := strconv.Atoi(envStr); err == nil && v > 0 {
			ttlHours = v
		}
	}
	return time.Duration(ttlHours) * time.Hour
}

// CreateDraft creates a new pipeline draft
// POST /api/v1/drafts
func CreateDraft(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req CreateDraftRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	// Defaults
	if req.State == "" {
		req.State = "intent"
	}
	if len(req.ChatHistory) == 0 {
		req.ChatHistory = json.RawMessage("[]")
	}
	if len(req.Config) == 0 {
		req.Config = json.RawMessage("{}")
	}

	payloadBytes, err := json.Marshal(draftPayload{
		State:       req.State,
		Version:     1,
		TemplateID:  req.TemplateID,
		ChatHistory: req.ChatHistory,
		Config:      req.Config,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to encode draft payload"})
		return
	}

	draftID := uuid.New().String()
	now := time.Now()
	expiresAt := now.Add(getDraftTTL())

	// NOTE: The current DB schema uses the older `pipeline_drafts` table (migration 027 + 028):
	// - user_id TEXT
	// - name TEXT
	// - draft JSONB
	// - status TEXT, expires_at, last_activity_at, state
	//
	// We store our agentic draft payload inside `draft` JSONB.
	_, err = database.Exec(`
		INSERT INTO pipeline_drafts
		  (id, user_id, name, description, draft, status, last_opened_at, expires_at, last_activity_at, state)
		VALUES
		  ($1, $2, $3, $4, $5, 'active', $6, $7, $8, $9)
	`, draftID, userID, "New pipeline draft", nil, payloadBytes, now, expiresAt, now, req.State)

	if err != nil {
		log.Errorf("Failed to create draft: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create draft"})
		return
	}

	// Fetch the created draft
	draft, err := getDraft(database, draftID, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Draft created but failed to fetch"})
		return
	}

	log.WithFields(log.Fields{
		"draft_id": draftID,
		"user_id":  userID,
		"state":    req.State,
	}).Info("✅ Created pipeline draft")

	c.JSON(http.StatusCreated, draft)
}

// GetDraft retrieves a draft by ID
// GET /api/v1/drafts/:id
func GetDraft(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	draftID, ok := requireUUIDParam(c, "id", "invalid_draft_id", "Invalid draft ID format")
	if !ok {
		return
	}
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	draft, err := getDraft(database, draftID, userID)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Draft not found or access denied"})
		return
	}
	if err != nil {
		log.Errorf("Failed to get draft: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve draft"})
		return
	}

	c.JSON(http.StatusOK, draft)
}

// UpdateDraft updates a draft with optimistic concurrency control
// PUT /api/v1/drafts/:id
func UpdateDraft(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	draftID, ok := requireUUIDParam(c, "id", "invalid_draft_id", "Invalid draft ID format")
	if !ok {
		return
	}
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req UpdateDraftRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	// Check current version
	current, err := getDraft(database, draftID, userID)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Draft not found or access denied"})
		return
	}
	if err != nil {
		log.Errorf("Failed to get draft: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve draft"})
		return
	}

	// Optimistic concurrency check
	if current.Version != req.Version {
		c.JSON(http.StatusConflict, gin.H{
			"error":               "draft_conflict",
			"message":             "Draft was modified elsewhere",
			"latest_version":      current.Version,
			"latest_config":       current.Config,
			"latest_chat_history": current.ChatHistory,
		})
		return
	}

	// Merge updates into payload stored in pipeline_drafts.draft
	nextState := current.State
	if req.State != nil && strings.TrimSpace(*req.State) != "" {
		nextState = strings.TrimSpace(*req.State)
	}
	nextChatHistory := current.ChatHistory
	if len(req.ChatHistory) > 0 {
		nextChatHistory = req.ChatHistory
	}
	nextConfig := current.Config
	if len(req.Config) > 0 {
		nextConfig = req.Config
	}
	nextVersion := current.Version + 1
	if nextVersion <= 0 {
		nextVersion = 1
	}

	payloadBytes, err := json.Marshal(draftPayload{
		State:       nextState,
		Version:     nextVersion,
		TemplateID:  current.TemplateID,
		ChatHistory: nextChatHistory,
		Config:      nextConfig,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to encode draft payload"})
		return
	}

	now := time.Now()
	expiresAt := now.Add(getDraftTTL())

	result, err := database.Exec(`
		UPDATE pipeline_drafts
		SET draft = $1,
		    last_activity_at = $2,
		    expires_at = $3,
		    state = $4
		WHERE id = $5 AND user_id = $6 AND status = 'active'
	`, payloadBytes, now, expiresAt, nextState, draftID, userID)
	if err != nil {
		log.Errorf("Failed to update draft: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update draft"})
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		// Concurrent update detected (version mismatch) or draft not active
		latest, _ := getDraft(database, draftID, userID)
		if latest != nil && latest.Version != req.Version {
			c.JSON(http.StatusConflict, gin.H{
				"error":               "draft_conflict",
				"message":             "Draft was modified elsewhere",
				"latest_version":      latest.Version,
				"latest_config":       latest.Config,
				"latest_chat_history": latest.ChatHistory,
			})
			return
		}
		c.JSON(http.StatusNotFound, gin.H{"error": "Draft not found or not editable"})
		return
	}

	// Fetch updated draft
	updated, err := getDraft(database, draftID, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Draft updated but failed to fetch"})
		return
	}

	log.WithFields(log.Fields{
		"draft_id": draftID,
		"version":  updated.Version,
	}).Info("✅ Updated pipeline draft")

	c.JSON(http.StatusOK, updated)
}

// DeleteDraft archives a draft (soft delete)
// DELETE /api/v1/drafts/:id
func DeleteDraft(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	draftID, ok := requireUUIDParam(c, "id", "invalid_draft_id", "Invalid draft ID format")
	if !ok {
		return
	}
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	result, err := database.Exec(`
		UPDATE pipeline_drafts 
		SET status = 'archived'
		WHERE id = $1 AND user_id = $2 AND status = 'active'
	`, draftID, userID)

	if err != nil {
		log.Errorf("Failed to delete draft: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete draft"})
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Draft not found or already deleted"})
		return
	}

	log.WithFields(log.Fields{
		"draft_id": draftID,
		"user_id":  userID,
	}).Info("✅ Deleted pipeline draft")

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Draft deleted",
	})
}

// getDraft is a helper to fetch a draft by ID with user scoping
func getDraft(database *sql.DB, draftID string, userID string) (*PipelineDraft, error) {
	var (
		out          PipelineDraft
		rawDraft     []byte
		stateCol     sql.NullString
		lastActivity sql.NullTime
	)

	query := `
		SELECT
		  id::text,
		  user_id,
		  status,
		  draft,
		  state,
		  last_activity_at,
		  expires_at,
		  created_at,
		  updated_at
		FROM pipeline_drafts
		WHERE id = $1
	`
	args := []any{draftID}
	if userID != "" {
		query += " AND user_id = $2"
		args = append(args, userID)
	}

	err := database.QueryRow(query, args...).Scan(
		&out.ID,
		&out.UserID,
		&out.Status,
		&rawDraft,
		&stateCol,
		&lastActivity,
		&out.ExpiresAt,
		&out.CreatedAt,
		&out.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	// Parse payload stored in JSONB
	payload := draftPayload{
		State:       "",
		Version:     1,
		ChatHistory: json.RawMessage("[]"),
		Config:      json.RawMessage("{}"),
	}
	if len(rawDraft) > 0 {
		if uerr := json.Unmarshal(rawDraft, &payload); uerr != nil {
			// Best-effort: keep defaults if schema differs.
			log.Warnf("Failed to decode pipeline_drafts.draft JSON for id=%s: %v", out.ID, uerr)
		}
	}

	out.State = strings.TrimSpace(payload.State)
	if out.State == "" && stateCol.Valid {
		out.State = strings.TrimSpace(stateCol.String)
	}
	if out.State == "" {
		out.State = "intent"
	}

	out.Version = payload.Version
	if out.Version <= 0 {
		out.Version = 1
	}

	out.TemplateID = payload.TemplateID
	out.ChatHistory = payload.ChatHistory
	if len(out.ChatHistory) == 0 {
		out.ChatHistory = json.RawMessage("[]")
	}
	out.Config = payload.Config
	if len(out.Config) == 0 {
		out.Config = json.RawMessage("{}")
	}

	if lastActivity.Valid {
		out.LastActivityAt = lastActivity.Time
	} else {
		out.LastActivityAt = out.UpdatedAt
	}

	return &out, nil
}
