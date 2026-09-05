package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/rsync-ai/shared/pgdriver"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/robfig/cron"
	log "github.com/sirupsen/logrus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// ============================================================================
// Pipeline Schedules Handler
// ============================================================================
// Manages durable pipeline schedules using Temporal Schedules API
// Implements: create, list, pause, resume, update, trigger, delete

// ScheduleSpec represents the schedule configuration
type ScheduleSpec struct {
	Cron         string `json:"cron,omitempty"`          // Cron expression (e.g., "0 * * * *")
	EverySeconds int    `json:"every_seconds,omitempty"` // Interval in seconds
	Timezone     string `json:"timezone,omitempty"`      // Timezone (default: UTC)
}

// Schedule represents a pipeline schedule
type Schedule struct {
	ScheduleID         string       `json:"schedule_id"`
	PipelineID         string       `json:"pipeline_id"`
	ScheduleType       string       `json:"schedule_type"` // "cron" or "interval"
	ScheduleSpec       ScheduleSpec `json:"schedule_spec"`
	TemporalScheduleID string       `json:"temporal_schedule_id"`
	Status             string       `json:"status"` // "active", "paused", "deleted"
	CreatedBy          string       `json:"created_by"`
	CreatedAt          time.Time    `json:"created_at"`
	UpdatedAt          time.Time    `json:"updated_at"`
	PausedAt           *time.Time   `json:"paused_at,omitempty"`
	PausedReason       string       `json:"paused_reason,omitempty"`

	// Blocked is computed per-request, not stored: an `active` schedule whose
	// pipeline is parked on a human decision keeps firing, and the wrapper
	// workflow's overlap guard skips every tick as a "success". Nothing else
	// tells the user their schedule stopped producing runs, so say it here.
	Blocked       bool   `json:"blocked,omitempty"`
	BlockedReason string `json:"blocked_reason,omitempty"`
}

// CreateScheduleRequest represents a request to create a schedule
type CreateScheduleRequest struct {
	ScheduleType string       `json:"schedule_type" binding:"required"` // "cron" or "interval"
	ScheduleSpec ScheduleSpec `json:"schedule_spec" binding:"required"`
}

// UpdateScheduleRequest represents a request to update a schedule
type UpdateScheduleRequest struct {
	ScheduleType string       `json:"schedule_type" binding:"required"`
	ScheduleSpec ScheduleSpec `json:"schedule_spec" binding:"required"`
}

// PauseScheduleRequest represents a request to pause a schedule
type PauseScheduleRequest struct {
	Reason string `json:"reason,omitempty"`
}

// ============================================================================
// Handlers
// ============================================================================

// CreatePipelineSchedule creates a new schedule for a pipeline
// POST /api/v1/pipelines/:id/schedules
func CreatePipelineSchedule(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	pipelineID := c.Param("id")
	if _, err := uuid.Parse(pipelineID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid pipeline ID"})
		return
	}

	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	// RBAC: creating a schedule is a MUTATION — require at least `member` in the
	// active workspace (membership alone is not enough; viewers are read-only).
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}

	// Fail fast if Temporal client is not available — checked AFTER authorization
	// so an unauthorized caller never learns the scheduling service's state.
	if temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Scheduling service not available"})
		return
	}

	// Schedules are only supported for batch pipelines.
	// CDC pipelines are continuous by design and must not be scheduled.
	if ok, mode, err := pipelineAllowsScheduling(database, pipelineID); err != nil {
		log.Errorf("Failed to check pipeline sync_mode for schedules: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to validate pipeline schedule eligibility"})
		return
	} else if !ok {
		msg := "CDC pipelines run continuously and cannot be scheduled."
		c.JSON(http.StatusBadRequest, gin.H{
			// Keep `error` as user-facing message (frontend extractErrorMessage prefers it).
			"error":      msg,
			"message":    msg,
			"error_code": "schedules_not_supported_for_cdc",
			"sync_mode": mode,
		})
		return
	}

	// A schedule on a pipeline with no saved table selection is dead on arrival:
	// its first run parks at the table_selection HITL and the overlap guard then
	// skips every later tick as a "successful" no-op.
	if !requireTableSelection(c, database, pipelineID, "create") {
		return
	}

	var req CreateScheduleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	// Default interval from pipelines.polling_interval_seconds when the
	// caller didn't specify one. Migration 050 added this column expecting
	// it to feed the schedule interval; before this hook the column was
	// orphan (writable but never read). For api-polling pipelines the
	// pipeline-level default is the right fallback.
	if req.ScheduleType == "interval" && req.ScheduleSpec.EverySeconds <= 0 {
		var defaultInterval sql.NullInt64
		_ = database.QueryRow(
			`SELECT polling_interval_seconds FROM pipelines WHERE id = $1`,
			pipelineID,
		).Scan(&defaultInterval)
		if defaultInterval.Valid && defaultInterval.Int64 >= 60 {
			req.ScheduleSpec.EverySeconds = int(defaultInterval.Int64)
		}
	}

	// Validate schedule spec
	if err := validateScheduleSpec(req.ScheduleType, req.ScheduleSpec); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid schedule: %s", err)})
		return
	}
	// Normalize timezone default (validateScheduleSpec doesn't mutate the struct)
	if req.ScheduleSpec.Timezone == "" {
		req.ScheduleSpec.Timezone = "UTC"
	}

	// Check if schedule already exists (uniqueness constraint)
	var existingCount int
	err := database.QueryRow(`
		SELECT COUNT(*) 
		FROM pipeline_schedules 
		WHERE pipeline_id = $1 AND status != 'deleted'
	`, pipelineID).Scan(&existingCount)

	if err != nil {
		log.Errorf("Failed to check existing schedules: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check existing schedules"})
		return
	}

	if existingCount > 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "Pipeline already has an active schedule. Delete or pause the existing schedule first."})
		return
	}

	// Begin transaction
	tx, err := database.BeginTx(c.Request.Context(), &sql.TxOptions{})
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

	// Create schedule record
	scheduleID := uuid.New().String()
	specJSON, err := json.Marshal(req.ScheduleSpec)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid schedule spec"})
		return
	}

	_, err = tx.Exec(`
		INSERT INTO pipeline_schedules (schedule_id, pipeline_id, schedule_type, schedule_spec, temporal_schedule_id, created_by)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, scheduleID, pipelineID, req.ScheduleType, specJSON, scheduleID, userID)

	if err != nil {
		// Race guard: another request may have created the schedule after our pre-check.
		if pgdriver.SQLState(err) == "23505" {
			c.JSON(http.StatusConflict, gin.H{
				"error":   "schedule_already_exists",
				"message": "Pipeline already has an active schedule. Delete or pause the existing schedule first.",
			})
			return
		}
		log.Errorf("Failed to insert schedule: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create schedule"})
		return
	}

	// Create Temporal schedule
	err = createTemporalSchedule(c.Request.Context(), scheduleID, pipelineID, req.ScheduleType, req.ScheduleSpec)
	if err != nil {
		log.Errorf("Failed to create Temporal schedule: %v", err)
		respondError(c, http.StatusInternalServerError, "temporal_schedule_create_failed", "Failed to create schedule in Temporal", err)
		return
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		// Commit failures can be ambiguous (e.g., network issues). Only cleanup Temporal if we
		// successfully rolled back the DB transaction (i.e., we know the row is not persisted).
		if rerr := tx.Rollback(); rerr == nil {
			_ = deleteTemporalSchedule(c.Request.Context(), scheduleID)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to commit schedule"})
			return
		}

		// Best-effort: if the schedule row exists, treat as success (avoid spurious retries)
		if schedule, ferr := getSchedule(database, scheduleID); ferr == nil {
			committed = true
			c.JSON(http.StatusCreated, schedule)
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to commit schedule"})
		return
	}
	committed = true

	// Fetch created schedule
	schedule, err := getSchedule(database, scheduleID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Schedule created but failed to fetch"})
		return
	}

	log.WithFields(log.Fields{
		"schedule_id": scheduleID,
		"pipeline_id": pipelineID,
		"type":        req.ScheduleType,
	}).Info("✅ Created pipeline schedule")

	c.JSON(http.StatusCreated, schedule)
}

// ListPipelineSchedules lists all schedules for a pipeline
// GET /api/v1/pipelines/:id/schedules
func ListPipelineSchedules(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	pipelineID := c.Param("id")
	if _, err := uuid.Parse(pipelineID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid pipeline ID"})
		return
	}

	// RBAC: read gate scoped to the caller's ACTIVE workspace — the same gate the
	// six mutating schedule handlers already use. The query below filters only by
	// pipeline_id, so this is the sole thing keeping another workspace's schedules
	// off the response.
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	rows, err := database.Query(`
		SELECT schedule_id, pipeline_id, schedule_type, schedule_spec, temporal_schedule_id, 
		       status, created_by, created_at, updated_at, paused_at, paused_reason
		FROM pipeline_schedules
		WHERE pipeline_id = $1 AND status != 'deleted'
		ORDER BY created_at DESC
	`, pipelineID)

	if err != nil {
		log.Errorf("Failed to fetch schedules: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch schedules"})
		return
	}
	defer rows.Close()

	schedules := []Schedule{}
	for rows.Next() {
		var s Schedule
		var specJSON []byte
		var pausedAt sql.NullTime
		var pausedReason sql.NullString

		err := rows.Scan(&s.ScheduleID, &s.PipelineID, &s.ScheduleType, &specJSON, &s.TemporalScheduleID,
			&s.Status, &s.CreatedBy, &s.CreatedAt, &s.UpdatedAt, &pausedAt, &pausedReason)
		if err != nil {
			log.Warnf("Failed to scan schedule row: %v", err)
			continue
		}

		json.Unmarshal(specJSON, &s.ScheduleSpec)
		if pausedAt.Valid {
			s.PausedAt = &pausedAt.Time
		}
		if pausedReason.Valid {
			s.PausedReason = pausedReason.String
		}

		schedules = append(schedules, s)
	}

	// One check per response, not per row: every schedule here belongs to the
	// same pipeline (the table carries a unique index on pipeline_id for
	// non-deleted rows). Only `active` schedules are affected — a paused one
	// already explains itself via paused_reason.
	if blockedReason := scheduleBlockedReason(database, pipelineID); blockedReason != "" {
		for i := range schedules {
			if schedules[i].Status == "active" {
				schedules[i].Blocked = true
				schedules[i].BlockedReason = blockedReason
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"schedules": schedules,
		"total":     len(schedules),
	})
}

// PausePipelineSchedule pauses a schedule
// PATCH /api/v1/pipelines/:id/schedules/:schedule_id/pause
func PausePipelineSchedule(c *gin.Context) {
	if temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Scheduling service not available"})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	scheduleID := c.Param("schedule_id")
	if _, ok := resolveUserID(c); !ok {
		return
	}

	var req PauseScheduleRequest
	c.ShouldBindJSON(&req)

	// Check ownership
	schedule, err := getSchedule(database, scheduleID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Schedule not found"})
		return
	}

	// RBAC: schedule mutations require at least `member` in the active workspace
	// (membership alone is not enough; viewers are read-only).
	if _, ok := requirePipelineWorkspaceRole(c, schedule.PipelineID, security.WSMember); !ok {
		return
	}

	if ok, mode, err := pipelineAllowsScheduling(database, schedule.PipelineID); err == nil && !ok {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":     "schedules_not_supported_for_cdc",
			"message":   "CDC pipelines run continuously and cannot be scheduled.",
			"sync_mode": mode,
		})
		return
	}

	// Pause in Temporal
	err = pauseTemporalSchedule(c.Request.Context(), schedule.TemporalScheduleID, req.Reason)
	if err != nil {
		log.Errorf("Failed to pause Temporal schedule: %v", err)
		respondError(c, http.StatusInternalServerError, "schedule_pause_failed", "Failed to pause schedule", err)
		return
	}

	// Update DB
	_, err = database.Exec(`
		UPDATE pipeline_schedules 
		SET status = 'paused', paused_at = NOW(), paused_reason = $1, updated_at = NOW()
		WHERE schedule_id = $2
	`, req.Reason, scheduleID)

	if err != nil {
		log.Errorf("Failed to update schedule status: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update schedule"})
		return
	}

	log.WithFields(log.Fields{
		"schedule_id": scheduleID,
		"pipeline_id": schedule.PipelineID,
	}).Info("✅ Paused pipeline schedule")

	c.JSON(http.StatusOK, gin.H{"status": "paused"})
}

// ResumePipelineSchedule resumes a paused schedule
// PATCH /api/v1/pipelines/:id/schedules/:schedule_id/resume
func ResumePipelineSchedule(c *gin.Context) {
	if temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Scheduling service not available"})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	scheduleID := c.Param("schedule_id")
	if _, ok := resolveUserID(c); !ok {
		return
	}

	// Check ownership
	schedule, err := getSchedule(database, scheduleID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Schedule not found"})
		return
	}

	// RBAC: schedule mutations require at least `member` in the active workspace
	// (membership alone is not enough; viewers are read-only).
	if _, ok := requirePipelineWorkspaceRole(c, schedule.PipelineID, security.WSMember); !ok {
		return
	}

	if ok, mode, err := pipelineAllowsScheduling(database, schedule.PipelineID); err == nil && !ok {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":     "schedules_not_supported_for_cdc",
			"message":   "CDC pipelines run continuously and cannot be scheduled.",
			"sync_mode": mode,
		})
		return
	}

	if !requireTableSelection(c, database, schedule.PipelineID, "resume") {
		return
	}

	// Resume in Temporal
	err = resumeTemporalSchedule(c.Request.Context(), schedule.TemporalScheduleID)
	if err != nil {
		log.Errorf("Failed to resume Temporal schedule: %v", err)
		respondError(c, http.StatusInternalServerError, "schedule_resume_failed", "Failed to resume schedule", err)
		return
	}

	// Update DB
	_, err = database.Exec(`
		UPDATE pipeline_schedules 
		SET status = 'active', paused_at = NULL, paused_reason = NULL, updated_at = NOW()
		WHERE schedule_id = $1
	`, scheduleID)

	if err != nil {
		log.Errorf("Failed to update schedule status: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update schedule"})
		return
	}

	log.WithFields(log.Fields{
		"schedule_id": scheduleID,
		"pipeline_id": schedule.PipelineID,
	}).Info("✅ Resumed pipeline schedule")

	c.JSON(http.StatusOK, gin.H{"status": "active"})
}

// TriggerPipelineSchedule triggers an immediate execution
// POST /api/v1/pipelines/:id/schedules/:schedule_id/trigger
func TriggerPipelineSchedule(c *gin.Context) {
	if temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Scheduling service not available"})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	scheduleID := c.Param("schedule_id")
	if _, ok := resolveUserID(c); !ok {
		return
	}

	// Check ownership
	schedule, err := getSchedule(database, scheduleID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Schedule not found"})
		return
	}

	// RBAC: schedule mutations require at least `member` in the active workspace
	// (membership alone is not enough; viewers are read-only).
	if _, ok := requirePipelineWorkspaceRole(c, schedule.PipelineID, security.WSMember); !ok {
		return
	}

	if ok, mode, err := pipelineAllowsScheduling(database, schedule.PipelineID); err == nil && !ok {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":     "schedules_not_supported_for_cdc",
			"message":   "CDC pipelines run continuously and cannot be scheduled.",
			"sync_mode": mode,
		})
		return
	}

	if !requireTableSelection(c, database, schedule.PipelineID, "trigger") {
		return
	}

	// Trigger immediate execution in Temporal
	err = triggerTemporalSchedule(c.Request.Context(), schedule.TemporalScheduleID)
	if err != nil {
		log.Errorf("Failed to trigger Temporal schedule: %v", err)
		respondError(c, http.StatusInternalServerError, "schedule_trigger_failed", "Failed to trigger schedule", err)
		return
	}

	log.WithFields(log.Fields{
		"schedule_id": scheduleID,
		"pipeline_id": schedule.PipelineID,
	}).Info("✅ Triggered pipeline schedule")

	c.JSON(http.StatusOK, gin.H{"status": "triggered"})
}

// DeletePipelineSchedule deletes a schedule
// DELETE /api/v1/pipelines/:id/schedules/:schedule_id
func DeletePipelineSchedule(c *gin.Context) {
	if temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Scheduling service not available"})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	scheduleID := c.Param("schedule_id")
	if _, ok := resolveUserID(c); !ok {
		return
	}

	// Check ownership
	schedule, err := getSchedule(database, scheduleID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Schedule not found"})
		return
	}

	// RBAC: schedule mutations require at least `member` in the active workspace
	// (membership alone is not enough; viewers are read-only).
	if _, ok := requirePipelineWorkspaceRole(c, schedule.PipelineID, security.WSMember); !ok {
		return
	}

	// Delete from Temporal
	err = deleteTemporalSchedule(c.Request.Context(), schedule.TemporalScheduleID)
	if err != nil {
		log.Errorf("Failed to delete Temporal schedule: %v", err)
		respondError(c, http.StatusInternalServerError, "schedule_delete_failed", "Failed to delete schedule", err)
		return
	}

	// Soft delete in DB
	_, err = database.Exec(`
		UPDATE pipeline_schedules 
		SET status = 'deleted', updated_at = NOW()
		WHERE schedule_id = $1
	`, scheduleID)

	if err != nil {
		log.Errorf("Failed to delete schedule: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete schedule"})
		return
	}

	log.WithFields(log.Fields{
		"schedule_id": scheduleID,
		"pipeline_id": schedule.PipelineID,
	}).Info("✅ Deleted pipeline schedule")

	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

// UpdatePipelineSchedule updates a schedule's spec
// PATCH /api/v1/pipelines/:id/schedules/:schedule_id
func UpdatePipelineSchedule(c *gin.Context) {
	if temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Scheduling service not available"})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	scheduleID := c.Param("schedule_id")
	if _, ok := resolveUserID(c); !ok {
		return
	}

	var req UpdateScheduleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	// Validate schedule spec
	if err := validateScheduleSpec(req.ScheduleType, req.ScheduleSpec); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid schedule: %s", err)})
		return
	}
	// Normalize timezone default
	if req.ScheduleSpec.Timezone == "" {
		req.ScheduleSpec.Timezone = "UTC"
	}

	// Check ownership
	schedule, err := getSchedule(database, scheduleID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Schedule not found"})
		return
	}

	// RBAC: schedule mutations require at least `member` in the active workspace
	// (membership alone is not enough; viewers are read-only).
	if _, ok := requirePipelineWorkspaceRole(c, schedule.PipelineID, security.WSMember); !ok {
		return
	}

	if ok, mode, err := pipelineAllowsScheduling(database, schedule.PipelineID); err == nil && !ok {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":     "schedules_not_supported_for_cdc",
			"message":   "CDC pipelines run continuously and cannot be scheduled.",
			"sync_mode": mode,
		})
		return
	}

	// Update Temporal schedule (try Update first, fallback to delete+create)
	err = updateTemporalSchedule(c.Request.Context(), schedule.TemporalScheduleID, schedule.PipelineID, req.ScheduleType, req.ScheduleSpec)
	if err != nil {
		log.Errorf("Failed to update Temporal schedule: %v", err)
		respondError(c, http.StatusInternalServerError, "schedule_update_failed", "Failed to update schedule", err)
		return
	}

	// Update DB
	specJSON, _ := json.Marshal(req.ScheduleSpec)
	_, err = database.Exec(`
		UPDATE pipeline_schedules 
		SET schedule_type = $1, schedule_spec = $2, updated_at = NOW()
		WHERE schedule_id = $3
	`, req.ScheduleType, specJSON, scheduleID)

	if err != nil {
		log.Errorf("Failed to update schedule: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update schedule"})
		return
	}

	log.WithFields(log.Fields{
		"schedule_id": scheduleID,
		"pipeline_id": schedule.PipelineID,
	}).Info("✅ Updated pipeline schedule")

	// Fetch updated schedule
	updatedSchedule, err := getSchedule(database, scheduleID)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"status": "updated"})
		return
	}

	c.JSON(http.StatusOK, updatedSchedule)
}

// ============================================================================
// Helper Functions
// ============================================================================

// pipelineAllowsScheduling returns false if the pipeline is CDC (pipeline-level override or source-connection default).
// We treat schedules as a batch-only feature.
func pipelineAllowsScheduling(database *sql.DB, pipelineID string) (bool, string, error) {
	var syncMode sql.NullString
	var cdcMode sql.NullString
	var sourceSyncMode sql.NullString

	err := database.QueryRow(`
		SELECT p.sync_mode, p.cdc_mode, sc.sync_mode
		FROM pipelines p
		LEFT JOIN connections sc ON p.source_connection_id = sc.id
		WHERE p.id = $1
	`, pipelineID).Scan(&syncMode, &cdcMode, &sourceSyncMode)
	if err != nil {
		return false, "", err
	}

	// If pipeline explicitly indicates CDC, treat as CDC.
	if (syncMode.Valid && strings.EqualFold(strings.TrimSpace(syncMode.String), "cdc")) || (cdcMode.Valid && strings.TrimSpace(cdcMode.String) != "") {
		return false, "cdc", nil
	}

	// Otherwise fall back to source connection mode, if present.
	if sourceSyncMode.Valid && strings.EqualFold(strings.TrimSpace(sourceSyncMode.String), "cdc") {
		return false, "cdc", nil
	}

	// Default: batch
	mode := "batch"
	if syncMode.Valid && strings.TrimSpace(syncMode.String) != "" {
		mode = strings.ToLower(strings.TrimSpace(syncMode.String))
	}
	return true, mode, nil
}

// pipelineHasTableSelection reports whether `pipelines.config->'selected_tables'`
// holds at least one non-empty entry.
//
// This is a hard precondition for scheduling, not a nicety. The executor's batch
// path refuses to infer a table from the prompt, the plan, or `config.table`
// (executor.go, "HITL POLICY: batch must not infer a table"), so a run with no
// persisted selection parks at the table_selection HITL. The wrapper workflow's
// overlap guard then reads that park as an active run and skips every subsequent
// tick — forever, and reporting success each time. Refusing up front is the only
// point in the chain where the user is still present to fix it.
// requireTableSelection aborts the request with 400 when the pipeline has no
// persisted table selection, and reports whether the caller may continue.
//
// Applied only to the handlers that arm or fire work — create, resume, trigger.
// Deliberately NOT applied to pause, update, or delete: a schedule on a
// table-less pipeline is exactly the one a user most needs to be able to pause
// or remove, and gating those would replace a silent brick with a loud one.
func requireTableSelection(c *gin.Context, database *sql.DB, pipelineID, action string) bool {
	hasTables, err := pipelineHasTableSelection(database, pipelineID)
	if err != nil {
		// Unknown state must not block a legitimate request; the run itself
		// still parks safely if the selection really is missing.
		log.Warnf("schedule %s: table-selection precheck failed for %s: %v", action, pipelineID, err)
		return true
	}
	if hasTables {
		return true
	}
	msg := "Select the tables to sync before scheduling this pipeline. Without a saved selection every scheduled run stops at table selection instead of moving data."
	c.JSON(http.StatusBadRequest, gin.H{
		// `error` is the user-facing string (frontend extractErrorMessage prefers it).
		"error":      msg,
		"message":    msg,
		"error_code": "schedule_requires_table_selection",
	})
	return false
}

func pipelineHasTableSelection(database *sql.DB, pipelineID string) (bool, error) {
	var configJSON []byte
	err := database.QueryRow(`SELECT config FROM pipelines WHERE id = $1`, pipelineID).Scan(&configJSON)
	if err != nil {
		return false, err
	}
	return hasSelectedTables(configJSON), nil
}

// hasSelectedTables reports whether a raw `pipelines.config` document carries at
// least one usable entry under `selected_tables`.
//
// Mirrors FetchPipelineRunContextActivity's sticky-selection parsing: the column
// is JSONB, so entries arrive as []interface{}, but []string is tolerated in
// case a writer ever hands us a pre-typed value. Anything unparseable counts as
// "no selection" — the same answer the run itself would reach.
func hasSelectedTables(configJSON []byte) bool {
	if len(configJSON) == 0 {
		return false
	}
	var cfg map[string]interface{}
	if err := json.Unmarshal(configJSON, &cfg); err != nil {
		return false
	}
	switch arr := cfg["selected_tables"].(type) {
	case []interface{}:
		for _, v := range arr {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return true
			}
		}
	case []string:
		for _, s := range arr {
			if strings.TrimSpace(s) != "" {
				return true
			}
		}
	}
	return false
}

// scheduleBlockedReason returns a user-facing explanation of why an otherwise
// `active` schedule is not producing runs, or "" when nothing is blocking it.
//
// Read-side only: it derives the answer from live pipeline state rather than
// storing a flag, so it can never drift from reality the way a cached column
// would. Errors resolve to "" — an unknown state must not manufacture a scary
// banner on a schedule that is actually fine.
func scheduleBlockedReason(database *sql.DB, pipelineID string) string {
	var status sql.NullString
	var reasonType sql.NullString
	var updatedAt sql.NullTime
	err := database.QueryRow(`
		SELECT status, blocking_reason_type, updated_at
		FROM pipeline_progress
		WHERE pipeline_id = $1
	`, pipelineID).Scan(&status, &reasonType, &updatedAt)

	if err == nil && strings.TrimSpace(status.String) == "waiting_for_user" {
		what := "input"
		if t := strings.TrimSpace(reasonType.String); t != "" {
			what = strings.ReplaceAll(t, "_", " ")
		}
		since := ""
		if updatedAt.Valid {
			since = fmt.Sprintf(" (waiting since %s)", updatedAt.Time.UTC().Format(time.RFC3339))
		}
		return fmt.Sprintf(
			"This pipeline is waiting for %s%s. Scheduled runs are skipped while a run is parked — answer the prompt on the pipeline page to resume them.",
			what, since,
		)
	}
	if err != nil && err != sql.ErrNoRows {
		log.Warnf("schedule block check: failed to read pipeline_progress for %s: %v", pipelineID, err)
		return ""
	}

	// No park right now, but a missing table selection means the NEXT run parks.
	if hasTables, tErr := pipelineHasTableSelection(database, pipelineID); tErr == nil && !hasTables {
		return "No tables are selected for this pipeline. Scheduled runs will stop and wait for a table selection instead of moving data."
	}
	return ""
}

func getSchedule(database *sql.DB, scheduleID string) (*Schedule, error) {
	var s Schedule
	var specJSON []byte
	var pausedAt sql.NullTime
	var pausedReason sql.NullString

	err := database.QueryRow(`
		SELECT schedule_id, pipeline_id, schedule_type, schedule_spec, temporal_schedule_id,
		       status, created_by, created_at, updated_at, paused_at, paused_reason
		FROM pipeline_schedules
		WHERE schedule_id = $1
	`, scheduleID).Scan(&s.ScheduleID, &s.PipelineID, &s.ScheduleType, &specJSON, &s.TemporalScheduleID,
		&s.Status, &s.CreatedBy, &s.CreatedAt, &s.UpdatedAt, &pausedAt, &pausedReason)

	if err != nil {
		return nil, err
	}

	json.Unmarshal(specJSON, &s.ScheduleSpec)
	if pausedAt.Valid {
		s.PausedAt = &pausedAt.Time
	}
	if pausedReason.Valid {
		s.PausedReason = pausedReason.String
	}
	if s.Status == "active" {
		if blockedReason := scheduleBlockedReason(database, s.PipelineID); blockedReason != "" {
			s.Blocked = true
			s.BlockedReason = blockedReason
		}
	}

	return &s, nil
}

func validateScheduleSpec(scheduleType string, spec ScheduleSpec) error {
	if scheduleType != "cron" && scheduleType != "interval" {
		return fmt.Errorf("schedule_type must be 'cron' or 'interval'")
	}

	if scheduleType == "cron" {
		if spec.Cron == "" {
			return fmt.Errorf("cron expression is required for cron schedules")
		}
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		if _, err := parser.Parse(spec.Cron); err != nil {
			return fmt.Errorf("invalid cron expression %q: %w", spec.Cron, err)
		}
	}

	if scheduleType == "interval" {
		if spec.EverySeconds <= 0 {
			return fmt.Errorf("every_seconds must be positive for interval schedules")
		}
		// 60s is a validation bound, NOT a guaranteed cadence. The Temporal
		// schedule uses Overlap: SKIP, so a tick arriving while the previous run
		// is still going is dropped rather than queued: a pipeline whose run
		// takes longer than the interval settles at half the requested rate or
		// slower. That is correct engine behavior, so don't "fix" it by raising
		// this bound — the create dialog warns about it below 300s instead.
		if spec.EverySeconds < 60 {
			return fmt.Errorf("minimum interval is 60 seconds")
		}
	}

	return nil
}

// ============================================================================
// Temporal Schedule Operations
// ============================================================================

func createTemporalSchedule(ctx context.Context, scheduleID, pipelineID, scheduleType string, spec ScheduleSpec) error {
	scheduleSpec := client.ScheduleSpec{}

	if scheduleType == "cron" {
		scheduleSpec.CronExpressions = []string{spec.Cron}
		// Temporal supports IANA time zones via ScheduleSpec.TimeZoneName
		if spec.Timezone != "" {
			scheduleSpec.TimeZoneName = spec.Timezone
		}
	} else if scheduleType == "interval" {
		scheduleSpec.Intervals = []client.ScheduleIntervalSpec{
			{Every: time.Duration(spec.EverySeconds) * time.Second},
		}
	}

	// Prepare workflow input
	input := map[string]interface{}{
		"schedule_id": scheduleID,
		"pipeline_id": pipelineID,
	}

	_, err := temporalClient.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID:   scheduleID,
		Spec: scheduleSpec,
		Action: &client.ScheduleWorkflowAction{
			Workflow:           "ScheduledPipelineRunWorkflow",
			TaskQueue:          "pipeline-workflows",
			Args:               []interface{}{input},
			WorkflowRunTimeout: 5 * time.Minute,
		},
		Overlap:       enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		CatchupWindow: 1 * time.Minute,
		Paused:        false,
	})

	return err
}

// attachScheduleForChat creates a pipeline_schedules row + a Temporal schedule
// for a chat-created (NL) pipeline. It is the lean, non-HTTP sibling of
// CreatePipelineSchedule: the chat handler has already resolved the active
// workspace + owner and gated CDC, so this skips the RBAC/HTTP layer and just
// does the durable work. Idempotency: callers create at most one schedule per
// chat pipeline (fresh pipeline id), so no pre-existence check is needed.
func attachScheduleForChat(ctx context.Context, database *sql.DB, pipelineID, userID, scheduleType string, spec ScheduleSpec) error {
	if database == nil {
		return fmt.Errorf("database not connected")
	}
	if temporalClient == nil {
		return fmt.Errorf("scheduling service not available")
	}
	if err := validateScheduleSpec(scheduleType, spec); err != nil {
		return err
	}
	if spec.Timezone == "" {
		spec.Timezone = "UTC"
	}
	scheduleID := uuid.New().String()
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	if _, err := database.ExecContext(ctx, `
		INSERT INTO pipeline_schedules (schedule_id, pipeline_id, schedule_type, schedule_spec, temporal_schedule_id, created_by)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, scheduleID, pipelineID, scheduleType, specJSON, scheduleID, userID); err != nil {
		return err
	}
	if err := createTemporalSchedule(ctx, scheduleID, pipelineID, scheduleType, spec); err != nil {
		// Roll back the row so a retry can re-create cleanly (mirrors the
		// commit-failure cleanup in CreatePipelineSchedule).
		_, _ = database.ExecContext(ctx, `DELETE FROM pipeline_schedules WHERE schedule_id = $1`, scheduleID)
		return err
	}
	log.WithFields(log.Fields{
		"schedule_id": scheduleID,
		"pipeline_id": pipelineID,
		"type":        scheduleType,
	}).Info("✅ Attached schedule to chat pipeline")
	return nil
}

func pauseTemporalSchedule(ctx context.Context, scheduleID, reason string) error {
	handle := temporalClient.ScheduleClient().GetHandle(ctx, scheduleID)
	return handle.Pause(ctx, client.SchedulePauseOptions{
		Note: reason,
	})
}

func resumeTemporalSchedule(ctx context.Context, scheduleID string) error {
	handle := temporalClient.ScheduleClient().GetHandle(ctx, scheduleID)
	return handle.Unpause(ctx, client.ScheduleUnpauseOptions{
		Note: "Resumed by user",
	})
}

func triggerTemporalSchedule(ctx context.Context, scheduleID string) error {
	handle := temporalClient.ScheduleClient().GetHandle(ctx, scheduleID)
	return handle.Trigger(ctx, client.ScheduleTriggerOptions{
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	})
}

func deleteTemporalSchedule(ctx context.Context, scheduleID string) error {
	handle := temporalClient.ScheduleClient().GetHandle(ctx, scheduleID)
	return handle.Delete(ctx)
}

func updateTemporalSchedule(ctx context.Context, scheduleID, pipelineID, scheduleType string, spec ScheduleSpec) error {
	// Try to update in-place using ScheduleHandle.Update
	handle := temporalClient.ScheduleClient().GetHandle(ctx, scheduleID)

	return handle.Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			scheduleSpec := client.ScheduleSpec{}

			if scheduleType == "cron" {
				scheduleSpec.CronExpressions = []string{spec.Cron}
				if spec.Timezone != "" {
					scheduleSpec.TimeZoneName = spec.Timezone
				}
			} else if scheduleType == "interval" {
				scheduleSpec.Intervals = []client.ScheduleIntervalSpec{
					{Every: time.Duration(spec.EverySeconds) * time.Second},
				}
			}

			return &client.ScheduleUpdate{
				Schedule: &client.Schedule{
					Spec:   &scheduleSpec,
					Action: input.Description.Schedule.Action, // Keep same action
					Policy: input.Description.Schedule.Policy, // Keep same policy
					State:  input.Description.Schedule.State,  // Keep same state
				},
			}, nil
		},
	})
}
