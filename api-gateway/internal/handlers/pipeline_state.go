package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// PipelineState represents the authoritative state of a pipeline execution
type PipelineState struct {
	SchemaVersion   int             `json:"schema_version"`
	PipelineID      string          `json:"pipeline_id"`
	ExecutionID     string          `json:"execution_id,omitempty"`
	Status          string          `json:"status"` // processing, waiting_for_user, completed, failed
	CurrentStage    string          `json:"current_stage,omitempty"`
	StageGroup      string          `json:"stage_group,omitempty"`
	State           string          `json:"state,omitempty"` // queued|running|succeeded|failed|skipped|waiting
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	LastHeartbeatAt *time.Time      `json:"last_heartbeat_at,omitempty"`
	DurationMs      int64           `json:"duration_ms,omitempty"`
	Attempt         int             `json:"attempt,omitempty"`
	MaxAttempts     int             `json:"max_attempts,omitempty"`
	Summary         string          `json:"summary,omitempty"`
	Progress        Progress        `json:"progress"`
	BlockingReason  *BlockingReason `json:"blocking_reason,omitempty"`
	Message         string          `json:"message"`
	ExecutionPlan   json.RawMessage `json:"execution_plan,omitempty"`
	Metadata        json.RawMessage `json:"metadata,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	// New fields for staleness detection
	IsStale     bool   `json:"is_stale,omitempty"`
	StaleReason string `json:"stale_reason,omitempty"`
	// StaleElapsedSeconds is server-computed seconds since last heartbeat (helps UI avoid Date.now()).
	StaleElapsedSeconds int64 `json:"stale_elapsed_seconds,omitempty"`
	// CancelRecommended is server-computed guidance for when to offer a cancel action (e.g. stale > 5m).
	CancelRecommended bool `json:"cancel_recommended,omitempty"`
	// New field for explicit error message
	ErrorMessage string `json:"error_message,omitempty"`
}

// Progress represents actual pipeline progress
type Progress struct {
	Percent     int    `json:"percent"`      // 0-100, calculated by backend
	CurrentStep int    `json:"current_step"` // e.g., 3
	TotalSteps  int    `json:"total_steps"`  // e.g., 7
	Stage       string `json:"stage,omitempty"`
}

// BlockingReason explains why a pipeline is slow or waiting.
type BlockingReason struct {
	Type             string `json:"type"` // connector_generation, large_table_scan, etc.
	Description      string `json:"description"`
	EstimatedSeconds *int   `json:"estimated_seconds,omitempty"`
	// Details mirrors the V2 websocket/domain event shape so UI can render generically.
	// For table selection, this is where `available_tables` lives today.
	Details map[string]interface{} `json:"details,omitempty"`
	// AvailableTables is a convenience alias for table-selection flows so
	// callers don't have to dig into details. It mirrors
	// details.available_tables — populated by enrichBlockingReasonWithSchema
	// for backwards compatibility with existing UIs. Future clients should
	// prefer details.available_tables; this field will go away in v2 of
	// the API.
	AvailableTables []map[string]interface{} `json:"available_tables,omitempty"`
	// ResumeEndpoint tells the client which HITL signal endpoint to POST
	// to in order to unblock the pipeline. Without it, the UI had to
	// hardcode the path-per-type mapping; with it, the API is
	// self-describing. Populated by enrichBlockingReasonResume.
	ResumeEndpoint string `json:"resume_endpoint,omitempty"`
}

// GetPipelineState returns the authoritative current state of a pipeline
// This is the source of truth for UI reconciliation
// GET /api/v1/pipelines/:id/state
func GetPipelineState(c *gin.Context) {
	pipelineID := c.Param("id")

	// Guard: ensure pipeline_id is a UUID. This keeps error semantics clean (404 vs 500).
	if _, err := uuid.Parse(pipelineID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	var state PipelineState
	var executionID, currentStage, stageGroup, stageState, stageSummary, blockingReasonType, blockingReasonDesc sql.NullString
	var blockingReasonSec sql.NullInt32
	var stageStartedAt, stageHeartbeatAt sql.NullTime
	var stageDurationMs sql.NullInt64
	var stageAttempt, stageMaxAttempts sql.NullInt32
	var metadata sql.NullString

	err := database.QueryRow(`
		SELECT 
			pipeline_id, 
			execution_id, 
			status, 
			current_stage,
			schema_version,
			stage_group,
			stage_state,
			stage_started_at,
			stage_last_heartbeat_at,
			stage_duration_ms,
			stage_attempt,
			stage_max_attempts,
			stage_summary,
			progress_percent,
			progress_current_step,
			progress_total_steps,
			blocking_reason_type,
			blocking_reason_description,
			blocking_reason_estimated_seconds,
			message,
			metadata,
			created_at,
			updated_at
		FROM pipeline_progress 
		WHERE pipeline_id = $1
	`, pipelineID).Scan(
		&state.PipelineID,
		&executionID,
		&state.Status,
		&currentStage,
		&state.SchemaVersion,
		&stageGroup,
		&stageState,
		&stageStartedAt,
		&stageHeartbeatAt,
		&stageDurationMs,
		&stageAttempt,
		&stageMaxAttempts,
		&stageSummary,
		&state.Progress.Percent,
		&state.Progress.CurrentStep,
		&state.Progress.TotalSteps,
		&blockingReasonType,
		&blockingReasonDesc,
		&blockingReasonSec,
		&state.Message,
		&metadata,
		&state.CreatedAt,
		&state.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		// No pipeline_progress row yet.
		//
		// This can happen if:
		// - the pipeline was created but never run
		// - the workflow failed to start (Temporal down) before emitting any StateUpdateActivity
		// - legacy direct-Kafka fallback was used (no authoritative progress events)
		//
		// In these cases, derive a best-effort state from pipelines + latest execution so the UI
		// does not get stuck showing "pending" forever.

		var pStatus string
		var pCreatedAt, pUpdatedAt time.Time
		if err2 := database.QueryRow(`
			SELECT status, created_at, updated_at
			FROM pipelines
			WHERE id = $1
		`, pipelineID).Scan(&pStatus, &pCreatedAt, &pUpdatedAt); err2 != nil {
			if err2 == sql.ErrNoRows {
				c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
				return
			}
			log.Errorf("Failed to query pipelines for state fallback: %v", err2)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve pipeline state"})
			return
		}

		// Latest execution (best-effort)
		var execID, execStatus sql.NullString
		var execError sql.NullString
		_ = database.QueryRow(`
			SELECT id, status, error_message
			FROM executions
			WHERE pipeline_id = $1
			ORDER BY start_time DESC
			LIMIT 1
		`, pipelineID).Scan(&execID, &execStatus, &execError)

		// Map pipeline status into the UI-facing state.status.
		// Keep "pending" as-is; treat "running" as "processing" for the state API.
		stateStatus := strings.TrimSpace(pStatus)
		if strings.EqualFold(stateStatus, "running") {
			stateStatus = "processing"
		}

		msg := "Pipeline created."
		if stateStatus == "processing" {
			msg = "Pipeline starting..."
		}
		if stateStatus == "failed" {
			msg = "Pipeline failed."
		}
		if stateStatus == "completed" {
			msg = "Pipeline completed."
		}
		if execError.Valid && strings.TrimSpace(execError.String) != "" {
			// Prefer execution error as the message for failed pipelines.
			if stateStatus == "failed" {
				msg = execError.String
			}
		}

		resp := PipelineState{
			SchemaVersion: 1,
			PipelineID:    pipelineID,
			Status:        stateStatus,
			Progress: Progress{
				Percent:     0,
				CurrentStep: 0,
				TotalSteps:  7,
			},
			Message:   msg,
			CreatedAt: pCreatedAt,
			UpdatedAt: pUpdatedAt,
		}
		if execID.Valid {
			resp.ExecutionID = execID.String
		}
		if execError.Valid && strings.TrimSpace(execError.String) != "" {
			resp.ErrorMessage = execError.String
		}
		c.JSON(http.StatusOK, resp)
		return
	}

	if err != nil {
		log.Errorf("Failed to query pipeline state: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve pipeline state"})
		return
	}

	// Reconcile against pipeline record status.
	// A user can stop a pipeline while the last progress heartbeat still says "processing" (e.g. executor hung).
	// In that case, the UI should reflect the explicit stop immediately.
	var pipelineStatus sql.NullString
	var pipelineUpdatedAt sql.NullTime
	_ = database.QueryRow(`
		SELECT status, updated_at
		FROM pipelines
		WHERE id = $1
	`, pipelineID).Scan(&pipelineStatus, &pipelineUpdatedAt)

	// If the Phase 4 Healer (or any background process) marked pipeline_progress
	// as waiting_for_user with a blocking_reason, that takes precedence over
	// the terminal pipelines.status. Without this guard, a healer-triggered
	// HITL request gets silently masked by the older "completed" pipeline row,
	// and the UI renders "Pipeline completed successfully" while the user is
	// actually expected to act.
	//
	// BUG-6/7: the HITL guard must NOT mask a *terminal failure* of the
	// authoritative pipelines.status column. A "completed"/"running" parent row
	// is benign to mask, but if the workflow has actually failed or been stopped
	// the waiting_for_user progress row is stale — keeping it leaves the list
	// stuck on a "Needs input" badge with no Run button. Reconcile against the
	// pipeline_status DB column: a terminal failed/stopped parent always wins.
	pipelineStatusLower := ""
	if pipelineStatus.Valid {
		pipelineStatusLower = strings.ToLower(strings.TrimSpace(pipelineStatus.String))
	}
	pipelineTerminallyFailed := pipelineStatusLower == "failed" || pipelineStatusLower == "stopped"

	// ...and `pipelines.status` is not the only way a park can be dead. A run
	// parked on an EXECUTION that has already closed failed/cancelled is just as
	// unanswerable: nothing is left alive to receive the signal the banner is
	// asking the user for. Prod carries four such rows (oldest 2026-07-05), every
	// one of them at pipelines.status='running' — so the check above sees nothing
	// wrong with them.
	//
	// The list query (pipelines.go derived_status, first CASE branch) and
	// GET /pipelines/:id (pipelines.go:1670) already resolve that contradiction in
	// favour of the execution. This endpoint did not, and this endpoint is the one
	// that drives the detail page's badge — so the list card said "Failed:
	// zombie: execution timed out" while the pipeline's own page offered a "Select
	// table(s) to sync" prompt that could never be acted on. Same question, three
	// handlers, two answers.
	//
	// Deliberately limited to failed/cancelled/stopped. A CDC pipeline's execution
	// row is closed as 'success' at the backfill→streaming handoff
	// (pipeline_status_activity.go) while the feed keeps running, so treating a
	// closed *success* as terminal here would mask a live HITL prompt on a live
	// stream.
	deadParkStatus := staleParkTerminalStatus(database, state.Status, executionID)

	healerHITL := state.Status == "waiting_for_user" && blockingReasonType.Valid && blockingReasonType.String != "" && !pipelineTerminallyFailed && deadParkStatus == ""

	pipelineStatusNormalized := ""
	if pipelineStatus.Valid && !healerHITL {
		pipelineStatusNormalized = strings.ToLower(strings.TrimSpace(pipelineStatus.String))
		switch pipelineStatusNormalized {
		case "paused":
			// Explicit pause should override last known progress state immediately (UI needs Resume).
			state.Status = "paused"
			state.Summary = "Paused"
			state.Message = "Pipeline paused"
			if pipelineUpdatedAt.Valid {
				state.UpdatedAt = pipelineUpdatedAt.Time
			}
		case "stopped":
			// Explicit stop overrides last known progress state.
			state.Status = "stopped"
			state.Summary = "Stopped"
			state.Message = "Pipeline stopped"
			state.State = "stopped"
			if pipelineUpdatedAt.Valid {
				state.UpdatedAt = pipelineUpdatedAt.Time
			}
		case "failed":
			// Ensure we surface failure even if progress projection didn't update.
			if state.Status != "failed" {
				state.Status = "failed"
				state.Summary = "Failed"
				state.Message = "Pipeline failed"
			}
			state.State = "failed"
			if pipelineUpdatedAt.Valid {
				state.UpdatedAt = pipelineUpdatedAt.Time
			}
		case "completed":
			if state.Status != "completed" {
				state.Status = "completed"
				state.Summary = "Completed"
				state.Message = "Pipeline completed"
			}
			state.State = "succeeded"
			if pipelineUpdatedAt.Valid {
				state.UpdatedAt = pipelineUpdatedAt.Time
			}
		}
	}

	// Map nullable fields
	if executionID.Valid {
		state.ExecutionID = executionID.String
	}
	if currentStage.Valid {
		state.CurrentStage = currentStage.String
		state.Progress.Stage = currentStage.String
	}
	if stageGroup.Valid {
		state.StageGroup = stageGroup.String
	}
	if stageState.Valid {
		state.State = stageState.String
	}
	if stageStartedAt.Valid {
		t := stageStartedAt.Time
		state.StartedAt = &t
	}
	if stageHeartbeatAt.Valid {
		t := stageHeartbeatAt.Time
		state.LastHeartbeatAt = &t
	}
	if stageDurationMs.Valid {
		state.DurationMs = stageDurationMs.Int64
	}
	if stageAttempt.Valid {
		state.Attempt = int(stageAttempt.Int32)
	}
	if stageMaxAttempts.Valid {
		state.MaxAttempts = int(stageMaxAttempts.Int32)
	}
	if stageSummary.Valid {
		state.Summary = stageSummary.String
	}
	if metadata.Valid {
		state.Metadata = json.RawMessage(metadata.String)
	}

	// Phase 4 healer-HITL: if the pipeline progress has transitioned to
	// waiting_for_user with a blocking_reason, the stale stage_summary /
	// pipelines.status="completed" message from the previous workflow run
	// must NOT leak into the response. The user sees this string under the
	// HITL banner; "Pipeline completed successfully" while blocking is
	// actively misleading.
	if healerHITL {
		if state.Summary == "" || strings.Contains(strings.ToLower(state.Summary), "complet") {
			state.Summary = "Action required"
		}
		if blockingReasonDesc.Valid && blockingReasonDesc.String != "" {
			state.Message = blockingReasonDesc.String
		} else if state.Message == "" || strings.Contains(strings.ToLower(state.Message), "complet") {
			state.Message = "Pipeline is waiting for your input"
		}
	}

	// Ensure explicit control-plane pipeline status wins over stage_state projections.
	// (The projector may continue to heartbeat "running" briefly after a pause signal.)
	if pipelineStatusNormalized == "paused" {
		state.State = "paused"
	}
	if pipelineStatusNormalized == "stopped" {
		state.State = "stopped"
	}
	if pipelineStatusNormalized == "failed" {
		state.State = "failed"
	}
	if pipelineStatusNormalized == "completed" {
		state.State = "succeeded"
	}

	// A park on an already-dead execution reports that execution's outcome. This
	// runs last so the stage_* projections mapped above (which still describe the
	// moment the run parked) cannot paint over it, and before the metadata strip
	// and the blocking-reason build below — both of which key off state.Status and
	// will now correctly drop the HITL banner instead of re-attaching it.
	if deadParkStatus != "" {
		state.Status = deadParkStatus
		if deadParkStatus == "failed" {
			state.Summary = "Failed"
			state.Message = "Pipeline failed"
			state.State = "failed"
		} else {
			state.Summary = "Stopped"
			state.Message = "Pipeline stopped"
			state.State = "stopped"
		}
	}

	// Lift metadata.execution_plan to top-level execution_plan for data-driven UI.
	// This keeps backward compatibility (metadata still includes it) while making it easy for clients.
	var metaObj map[string]interface{}
	if len(state.Metadata) > 0 {
		if err := json.Unmarshal(state.Metadata, &metaObj); err == nil {
			if plan, ok := metaObj["execution_plan"]; ok {
				if b, err := json.Marshal(plan); err == nil {
					state.ExecutionPlan = json.RawMessage(b)
				}
				// Optional: derive current_stage from plan if missing.
				if state.CurrentStage == "" {
					if derived := deriveCurrentStageFromPlan(plan); derived != "" {
						state.CurrentStage = derived
						state.Progress.Stage = derived
					}
				}
			}
		}
	}

	// ----------------------------------------------------------------------------
	// Reconciliation & sanitation for non-waiting states
	//
	// We observed that some projector/heartbeat updates can advance `status`/`current_stage`
	// without replacing metadata.execution_plan, leaving it stale (e.g., executor stage shows
	// "waiting" even while status="processing"). This confuses the UI which uses the plan
	// to render the current card.
	//
	// If the pipeline is NOT waiting_for_user, strip HITL-only metadata keys and reconcile
	// the execution_plan stage status for current_stage.
	// ----------------------------------------------------------------------------
	if metaObj != nil && state.Status != "waiting_for_user" {
		// Strip HITL-only keys that should not persist once we resume.
		delete(metaObj, "available_tables")
		delete(metaObj, aiSuggestionsMetaKey)
		delete(metaObj, "request_type")
		delete(metaObj, "action_needed")
		delete(metaObj, "button_text")
		delete(metaObj, "blocking_reason_type")
		delete(metaObj, "reason")
		delete(metaObj, "missing_connections")
		delete(metaObj, "missing_connectors")

		// Reconcile execution_plan current stage status if present.
		if planAny, ok := metaObj["execution_plan"]; ok && state.CurrentStage != "" {
			if planObj, ok := planAny.(map[string]interface{}); ok && planObj != nil {
				if stagesRaw, ok := planObj["stages"].([]interface{}); ok && stagesRaw != nil {
					for _, s := range stagesRaw {
						stageObj, ok := s.(map[string]interface{})
						if !ok || stageObj == nil {
							continue
						}
						id, _ := stageObj["id"].(string)
						if id != state.CurrentStage {
							continue
						}
						switch state.Status {
						case "processing":
							stageObj["status"] = "running"
						case "completed":
							stageObj["status"] = "complete"
							stageObj["progress"] = 100
						case "failed":
							stageObj["status"] = "failed"
						}
					}
					planObj["stages"] = stagesRaw
				}

				// Re-marshal plan to top-level ExecutionPlan
				if b, err := json.Marshal(planObj); err == nil {
					state.ExecutionPlan = json.RawMessage(b)
				}
				// Keep metadata.execution_plan consistent with top-level output
				metaObj["execution_plan"] = planObj
			}
		}

		// Re-marshal sanitized metadata back into response (response-only; DB remains unchanged).
		if b, err := json.Marshal(metaObj); err == nil {
			state.Metadata = json.RawMessage(b)
		}
	}

	// Build blocking reason if present.
	//
	// BUG-6/7: only surface a blocking reason while the pipeline is genuinely
	// waiting_for_user. If we reconciled a stale waiting_for_user row down to a
	// terminal failed/stopped status above, the blocking_reason columns are
	// stale too — attaching them here would re-introduce the "Needs input"
	// banner and suppress the Run button we just enabled.
	if blockingReasonType.Valid && blockingReasonDesc.Valid && state.Status == "waiting_for_user" {
		state.BlockingReason = &BlockingReason{
			Type:        blockingReasonType.String,
			Description: blockingReasonDesc.String,
		}
		if blockingReasonSec.Valid {
			seconds := int(blockingReasonSec.Int32)
			state.BlockingReason.EstimatedSeconds = &seconds
		}

		// Tell the client which HITL endpoint to POST to in order to
		// resume the workflow. Without this the UI had to hardcode the
		// type→path mapping; a self-describing API is friendlier.
		state.BlockingReason.ResumeEndpoint = hitlResumeEndpointFor(state.BlockingReason.Type, pipelineID)

		// Include details from metadata for generic UI flows (e.g. table selection, connection config).
		if metaObj != nil {
			state.BlockingReason.Details = metaObj

			// Inject JSON Schema for known HITL types (future-proofing for generic form rendering)
			enrichBlockingReasonWithSchema(state.BlockingReason, metaObj)

			// Convenience alias for table selection so clients can use blocking_reason.available_tables.
			if raw, ok := metaObj["available_tables"]; ok {
				state.BlockingReason.AvailableTables = normalizeAvailableTables(raw)
			}

			// AI table suggestions for the table-selection park (P0). Entirely
			// best-effort: on any error blocking_reason is exactly as above.
			if isTableSelectionType(state.BlockingReason.Type) {
				attachTableSuggestions(c.Request.Context(), database, pipelineID, state.BlockingReason)
			}
		}
	}

	// Compute staleness if pipeline is processing (server-computed so UI can stay pure/idempotent).
	if state.Status == "processing" && state.LastHeartbeatAt != nil {
		isStale, reason := computeStaleness(state)
		state.IsStale = isStale
		state.StaleReason = reason
		state.StaleElapsedSeconds = int64(time.Since(*state.LastHeartbeatAt).Seconds())
		// Offer cancel after 5 minutes of staleness (additive, UI may ignore).
		state.CancelRecommended = isStale && state.StaleElapsedSeconds > 300
	}

	// Normalize progress to ensure consistency (force 100% when completed, clamp values, etc.)
	normalizeProgress(&state)

	// Extract error message with explicit fallback chain. If the
	// extractor returns nothing useful AND the pipeline is in a
	// failed/terminal state, fall back to the latest executions row's
	// error_message. The Driver E2E run showed users were getting the
	// generic "Pipeline failed. No error details available." line while
	// the actual root cause (HTTP 404 from the source connector) was
	// sitting in executions.error_message.
	state.ErrorMessage = extractErrorMessage(state, metaObj)
	if (state.ErrorMessage == "" || strings.Contains(state.ErrorMessage, "No error details available")) &&
		(state.Status == "failed" || state.Status == "error") {
		var execErr sql.NullString
		_ = database.QueryRow(`
			SELECT error_message
			FROM executions
			WHERE pipeline_id = $1
			ORDER BY start_time DESC
			LIMIT 1
		`, pipelineID).Scan(&execErr)
		if execErr.Valid && strings.TrimSpace(execErr.String) != "" {
			state.ErrorMessage = execErr.String
		}
	}

	c.JSON(http.StatusOK, state)
}

func deriveCurrentStageFromPlan(plan interface{}) string {
	planObj, ok := plan.(map[string]interface{})
	if !ok || planObj == nil {
		return ""
	}
	stagesRaw, ok := planObj["stages"].([]interface{})
	if !ok {
		return ""
	}

	// Prefer running/waiting stage with highest order.
	bestID := ""
	bestOrder := -1

	for _, s := range stagesRaw {
		stage, ok := s.(map[string]interface{})
		if !ok || stage == nil {
			continue
		}
		status, _ := stage["status"].(string)
		if status != "running" && status != "waiting" {
			continue
		}
		id, _ := stage["id"].(string)
		if id == "" {
			continue
		}
		order := 0
		if v, ok := stage["order"].(float64); ok {
			order = int(v)
		}
		if order >= bestOrder {
			bestOrder = order
			bestID = id
		}
	}

	return bestID
}

// hitlResumeEndpointFor maps a BlockingReason.Type to the HTTP endpoint a
// client should POST to in order to unblock the pipeline. Returns "" when
// the type isn't a known HITL resume signal (in which case the UI keeps
// polling /state until the workflow advances on its own). Keep this list
// in sync with the routes registered in api-gateway/cmd/server/main.go.
func hitlResumeEndpointFor(blockingType, pipelineID string) string {
	if pipelineID == "" {
		return ""
	}
	switch blockingType {
	case "needs_connection", "needs_connections", "connection_missing":
		return "/api/v1/pipelines/" + pipelineID + "/hitl/connections"
	case "needs_connectors", "connector_missing":
		return "/api/v1/pipelines/" + pipelineID + "/hitl/connectors"
	case "needs_tables", "table_selection", "needs_table_selection":
		return "/api/v1/pipelines/" + pipelineID + "/hitl/tables"
	case "needs_node_input", "node_input_required":
		return "/api/v1/pipelines/" + pipelineID + "/hitl/node-input"
	default:
		return ""
	}
}

// enrichBlockingReasonWithSchema injects JSON Schema for known HITL types to enable generic form rendering
func enrichBlockingReasonWithSchema(br *BlockingReason, details map[string]interface{}) {
	if br == nil || details == nil {
		return
	}

	// Only inject schema if not already present (backward compatibility)
	if _, exists := details["input_schema"]; exists {
		return
	}

	switch br.Type {
	case "table_selection":
		// Schema for table selection: array of table names
		details["input_schema"] = map[string]interface{}{
			"type": "object",
			"required": []string{"selected_tables"},
			"properties": map[string]interface{}{
				"selected_tables": map[string]interface{}{
					"type": "array",
					"items": map[string]interface{}{
						"type": "string",
					},
					"minItems": 1,
					"description": "Select one or more tables to sync",
				},
			},
		}
		details["ui_hints"] = map[string]interface{}{
			"widget": "table_selector",
			"data_source": "available_tables",
		}

	case "connection_config":
		// Schema for connection configuration
		details["input_schema"] = map[string]interface{}{
			"type": "object",
			"required": []string{"connections"},
			"properties": map[string]interface{}{
				"connections": map[string]interface{}{
					"type": "object",
					"description": "Connection configurations",
					"additionalProperties": map[string]interface{}{
						"type": "object",
					},
				},
			},
		}
		details["ui_hints"] = map[string]interface{}{
			"widget": "connection_configurator",
			"required_connections": details["required_connections"],
		}

	case "connector_generation":
		// Schema for connector generation approval
		details["input_schema"] = map[string]interface{}{
			"type": "object",
			"required": []string{"approved"},
			"properties": map[string]interface{}{
				"approved": map[string]interface{}{
					"type": "boolean",
					"description": "Approve connector generation",
				},
				"connectors": map[string]interface{}{
					"type": "array",
					"items": map[string]interface{}{
						"type": "string",
					},
					"description": "List of connectors to generate",
				},
			},
		}
		details["ui_hints"] = map[string]interface{}{
			"widget": "connector_generator",
			"missing_connectors": details["missing_connectors"],
		}
	}
}

func normalizeAvailableTables(input interface{}) []map[string]interface{} {
	arr, ok := input.([]interface{})
	if !ok || arr == nil {
		return nil
	}

	out := make([]map[string]interface{}, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok || m == nil {
			continue
		}

		// Ensure both `name` and `table` are present as aliases (UI/backends vary).
		if name, ok := m["name"].(string); ok && name != "" {
			if _, hasTable := m["table"]; !hasTable {
				m["table"] = name
			}
		}
		if tbl, ok := m["table"].(string); ok && tbl != "" {
			if _, hasName := m["name"]; !hasName {
				m["name"] = tbl
			}
		}

		out = append(out, m)
	}

	return out
}

// ----------------------------------------------------------------------------
// AI table suggestions for the table-selection HITL park (P0)
//
// When a pipeline parks at table_selection, rank the discovered tables against
// the user's intent via the existing metadata-only LLM ranker (rankTablesByLLM,
// connections.go) and surface the top picks in
// blocking_reason.details.suggested_tables so the UI can pre-select them.
// Suggestions are computed at most ONCE per park: concurrent /state polls race
// on an atomic jsonb_set claim (mirroring the hitl_tables_selected claim in
// pipeline_hitl.go), and the winner computes off the request path. The whole
// path is best-effort — any failure leaves blocking_reason exactly as today.
// ----------------------------------------------------------------------------

// aiSuggestionsMetaKey is the pipeline_progress.metadata key persisting the
// suggestion claim/result across polls. Internal bookkeeping — stripped from
// client-facing details.
const aiSuggestionsMetaKey = "ai_suggested_tables"

const maxSuggestedTables = 10

// isTableSelectionType mirrors the table-selection aliases accepted by
// hitlResumeEndpointFor.
func isTableSelectionType(blockingType string) bool {
	switch blockingType {
	case "needs_tables", "table_selection", "needs_table_selection":
		return true
	default:
		return false
	}
}

// buildTableMetadataFromAvailable converts the executor's park-time
// available_tables entries ({name, schema, row_count, columns:<count>}) into
// the []TableMetadata shape the LLM ranker consumes. Column NAMES are not
// available at park time (the executor only records a count), so Columns
// stays empty — rankTablesByLLM tolerates that.
func buildTableMetadataFromAvailable(raw interface{}) []TableMetadata {
	arr, ok := raw.([]interface{})
	if !ok || len(arr) == 0 {
		return nil
	}
	out := make([]TableMetadata, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok || m == nil {
			continue
		}
		name, _ := m["name"].(string)
		if strings.TrimSpace(name) == "" {
			// normalizeAvailableTables aliases name<->table; accept either.
			name, _ = m["table"].(string)
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		// Never feed rsync's own bookkeeping/staging tables (`_rsync_*`, `flat_*`)
		// to the LLM ranker: they are not user data and must not be suggested.
		// Covers pipelines that parked before the executor-side filter shipped,
		// whose persisted available_tables still contains them.
		if isInternalExplorerTable(name) {
			continue
		}
		schema, _ := m["schema"].(string)
		out = append(out, TableMetadata{
			Name:     name,
			Schema:   strings.TrimSpace(schema),
			RowCount: asInt64(m["row_count"]),
		})
	}
	return out
}

// asInt64 handles the numeric types a jsonb round-trip can produce.
func asInt64(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i
		}
	}
	return 0
}

// suggestedEntriesFromRecommendations shapes ranker output for
// blocking_reason.details.suggested_tables: {name, schema, reason, confidence},
// capped at limit.
func suggestedEntriesFromRecommendations(recs []TableRecommendation, limit int) []map[string]interface{} {
	if limit <= 0 || len(recs) == 0 {
		return nil
	}
	out := make([]map[string]interface{}, 0, limit)
	for _, r := range recs {
		if len(out) >= limit {
			break
		}
		name := strings.TrimSpace(r.Name)
		if name == "" {
			continue
		}
		entry := map[string]interface{}{
			"name":       name,
			"reason":     r.Reason,
			"confidence": r.Confidence,
		}
		if s := strings.TrimSpace(r.Schema); s != "" {
			entry["schema"] = s
		}
		out = append(out, entry)
	}
	return out
}

// parsePersistedSuggestions reads a previously persisted claim/result from
// pipeline_progress metadata. claimed=false → no claim yet (compute may
// proceed). entries is non-empty only for status "ready".
func parsePersistedSuggestions(metaObj map[string]interface{}) (entries []map[string]interface{}, status string, claimed bool) {
	raw, exists := metaObj[aiSuggestionsMetaKey]
	if !exists || raw == nil {
		return nil, "", false
	}
	m, isMap := raw.(map[string]interface{})
	if !isMap || m == nil {
		// Malformed claim: treat as claimed so we never recompute in a loop.
		return nil, "", true
	}
	status, _ = m["status"].(string)
	if status != "ready" {
		return nil, status, true
	}
	arr, _ := m["suggestions"].([]interface{})
	for _, it := range arr {
		if len(entries) >= maxSuggestedTables {
			break
		}
		if e, isEntry := it.(map[string]interface{}); isEntry && e != nil {
			entries = append(entries, e)
		}
	}
	return entries, status, true
}

// extractIntentText pulls a human-readable intent string out of
// pipeline_states.intent_data (migration 012). Best-effort across known shapes.
func extractIntentText(intentData map[string]interface{}) string {
	if intentData == nil {
		return ""
	}
	for _, key := range []string{"intent", "user_request", "raw_request", "original_request", "description"} {
		if s, ok := intentData[key].(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	// Nested {"intent": {...}} (adapter CachedIntent shape).
	if nested, ok := intentData["intent"].(map[string]interface{}); ok {
		for _, key := range []string{"user_request", "raw_request", "description"} {
			if s, ok := nested[key].(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

// lookupPipelineIntent resolves the user's intent for ranking: parsed
// intent_data first, then the pipeline's own NL request / name / description
// (all user text — never row values). Empty result → caller skips suggestions
// rather than hallucinating a use case.
func lookupPipelineIntent(ctx context.Context, database *sql.DB, pipelineID string) string {
	var intentRaw sql.NullString
	if err := database.QueryRowContext(ctx,
		`SELECT intent_data FROM pipeline_states WHERE pipeline_id = $1`, pipelineID,
	).Scan(&intentRaw); err == nil && intentRaw.Valid && strings.TrimSpace(intentRaw.String) != "" {
		var intentData map[string]interface{}
		if json.Unmarshal([]byte(intentRaw.String), &intentData) == nil {
			if intent := extractIntentText(intentData); intent != "" {
				return intent
			}
		}
	}

	var name, desc, nlRequest sql.NullString
	if err := database.QueryRowContext(ctx,
		`SELECT name, description, natural_language_request FROM pipelines WHERE id = $1`, pipelineID,
	).Scan(&name, &desc, &nlRequest); err != nil {
		return ""
	}
	if nlRequest.Valid && strings.TrimSpace(nlRequest.String) != "" {
		return strings.TrimSpace(nlRequest.String)
	}
	parts := make([]string, 0, 2)
	if name.Valid && strings.TrimSpace(name.String) != "" {
		parts = append(parts, strings.TrimSpace(name.String))
	}
	if desc.Valid && strings.TrimSpace(desc.String) != "" {
		parts = append(parts, strings.TrimSpace(desc.String))
	}
	return strings.Join(parts, " — ")
}

// attachTableSuggestions injects suggested_tables + suggestions_source into
// blocking_reason.details for a table-selection park. Fail-soft by contract.
func attachTableSuggestions(ctx context.Context, database *sql.DB, pipelineID string, br *BlockingReason) {
	if br == nil || br.Details == nil {
		return
	}
	details := br.Details

	// Reuse the persisted result — suggestions are computed once per park.
	// (No DB needed on this path: the result rode in with the metadata.)
	if entries, status, claimed := parsePersistedSuggestions(details); claimed {
		delete(details, aiSuggestionsMetaKey) // internal bookkeeping, not for clients
		if status == "ready" && len(entries) > 0 {
			details["suggested_tables"] = entries
			details["suggestions_source"] = "llm"
		}
		return
	}

	if database == nil {
		return
	}
	tables := buildTableMetadataFromAvailable(details["available_tables"])
	if len(tables) == 0 {
		return // manual-entry park (discovery failed/empty) — nothing to rank
	}
	intent := lookupPipelineIntent(ctx, database, pipelineID)
	if intent == "" {
		return
	}

	// Atomic idempotent claim (mirrors the hitl_tables_selected claim in
	// pipeline_hitl.go): concurrent polls race to a single UPDATE; only the
	// winner computes, everyone else answers without suggestions until the
	// result lands in metadata.
	claimPayload, err := json.Marshal(map[string]interface{}{
		"status":     "pending",
		"claimed_at": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return
	}
	res, err := database.ExecContext(ctx, `
		UPDATE pipeline_progress
		SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{ai_suggested_tables}', $1::jsonb, true),
		    updated_at = NOW()
		WHERE pipeline_id = $2
		  AND COALESCE(metadata->'ai_suggested_tables', 'null'::jsonb) = 'null'::jsonb
	`, string(claimPayload), pipelineID)
	if err != nil {
		log.WithField("pipeline_id", pipelineID).WithError(err).Debug("table suggestions: claim failed")
		return
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return // another poll won the claim; its result shows up on a later poll
	}

	// Compute + persist off the request path so /state stays fast; the next
	// poll (~3s) picks the persisted result up.
	go computeAndPersistTableSuggestions(database, pipelineID, tables, intent)
}

// computeAndPersistTableSuggestions runs the LLM ranking (metadata + user
// intent only — no row values, no credentials) and persists the outcome.
// A failed ranking is persisted as status "failed" and never retried: at most
// one LLM call per park, by design.
func computeAndPersistTableSuggestions(database *sql.DB, pipelineID string, tables []TableMetadata, intent string) {
	// Detached from the HTTP request: the poll that won the claim has likely
	// already returned by the time the LLM answers.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result := map[string]interface{}{
		"status":      "failed",
		"computed_at": time.Now().UTC().Format(time.RFC3339),
	}
	recs, err := rankTablesByLLM(ctx, tables, intent, "", maxSuggestedTables)
	if err == nil {
		if entries := suggestedEntriesFromRecommendations(recs, maxSuggestedTables); len(entries) > 0 {
			result["status"] = "ready"
			result["source"] = "llm"
			result["suggestions"] = entries
		}
	} else {
		log.WithField("pipeline_id", pipelineID).WithError(err).Debug("table suggestions: LLM ranking unavailable")
	}

	payload, err := json.Marshal(result)
	if err != nil {
		return
	}
	if _, err := database.ExecContext(ctx, `
		UPDATE pipeline_progress
		SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{ai_suggested_tables}', $1::jsonb, true),
		    updated_at = NOW()
		WHERE pipeline_id = $2
	`, string(payload), pipelineID); err != nil {
		log.WithField("pipeline_id", pipelineID).WithError(err).Debug("table suggestions: persist failed")
	}
}

// getStalenessThreshold returns the expected heartbeat threshold based on stage characteristics
func getStalenessThreshold(stageGroup, stageID string, executionPlan json.RawMessage) time.Duration {
	// Check environment overrides first
	if envThreshold := os.Getenv("PIPELINE_STALENESS_THRESHOLD_" + strings.ToUpper(stageGroup)); envThreshold != "" {
		if seconds, err := strconv.Atoi(envThreshold); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}

	// Try to get group from execution_plan if not set
	if stageGroup == "" && len(executionPlan) > 0 {
		var plan map[string]interface{}
		if err := json.Unmarshal(executionPlan, &plan); err == nil {
			if stages, ok := plan["stages"].([]interface{}); ok {
				for _, s := range stages {
					if stage, ok := s.(map[string]interface{}); ok {
						if id, _ := stage["id"].(string); id == stageID {
							stageGroup, _ = stage["group"].(string)
							break
						}
					}
				}
			}
		}
	}

	// Default thresholds based on stage group
	switch stageGroup {
	case "planning":
		return getEnvDuration("PIPELINE_STALENESS_PLANNING", 60)
	case "connecting":
		return getEnvDuration("PIPELINE_STALENESS_CONNECTING", 120)
	case "executing":
		return getEnvDuration("PIPELINE_STALENESS_EXECUTING", 180)
	default:
		// Fallback: check stage ID patterns
		lowerID := strings.ToLower(stageID)
		if strings.Contains(lowerID, "intent") || strings.Contains(lowerID, "capability") ||
			strings.Contains(lowerID, "planner") || strings.Contains(lowerID, "validator") {
			return getEnvDuration("PIPELINE_STALENESS_PLANNING", 60)
		}
		if strings.Contains(lowerID, "connector") || strings.Contains(lowerID, "connection") {
			return getEnvDuration("PIPELINE_STALENESS_CONNECTING", 120)
		}
		if strings.Contains(lowerID, "executor") || strings.Contains(lowerID, "transfer") {
			return getEnvDuration("PIPELINE_STALENESS_EXECUTING", 180)
		}
		return getEnvDuration("PIPELINE_STALENESS_DEFAULT", 90)
	}
}

// getEnvDuration gets duration from environment or returns default in seconds
func getEnvDuration(envKey string, defaultSeconds int) time.Duration {
	if val := os.Getenv(envKey); val != "" {
		if seconds, err := strconv.Atoi(val); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Duration(defaultSeconds) * time.Second
}

// staleParkTerminalStatus answers one question: is this progress row waiting on an
// execution that has already ended for good? It returns the status the pipeline
// should report instead ("failed" or "stopped"), or "" when the park is live and
// must be left alone.
//
// Only an in-flight progress status can be stale this way — a row that already says
// completed/failed/cancelled is not claiming to be waiting on anything.
//
// The `success`/`completed` outcomes are deliberately NOT terminal here: CDC closes
// its execution row at the backfill→streaming handoff while the feed keeps running,
// so a closed-success execution does not mean the pipeline is over. Only an
// execution that failed or was cancelled proves nothing is coming back to answer
// the park.
func staleParkTerminalStatus(database *sql.DB, progressStatus string, executionID sql.NullString) string {
	if database == nil || !executionID.Valid {
		return ""
	}
	execID := strings.TrimSpace(executionID.String)
	if execID == "" {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(progressStatus)) {
	case "processing", "waiting_for_user":
	default:
		return ""
	}

	var execStatus sql.NullString
	var execEnd sql.NullTime
	if err := database.QueryRow(`
		SELECT status, end_time FROM executions WHERE id = $1
	`, execID).Scan(&execStatus, &execEnd); err != nil {
		// Unreadable or missing execution is not evidence of anything. Leave the
		// park exactly as the projector wrote it rather than failing a live run.
		return ""
	}
	// end_time is what "closed" means everywhere else in this codebase (the list's
	// le.completed_at, the zombie sweeper's guard). A terminal status without one is
	// a row mid-write; wait for the next poll.
	if !execEnd.Valid || !execStatus.Valid {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(execStatus.String)) {
	case "failed":
		return "failed"
	case "cancelled", "canceled", "stopped":
		return "stopped"
	}
	return ""
}

// computeStaleness determines if a pipeline stage is stale based on last heartbeat
func computeStaleness(state PipelineState) (bool, string) {
	// Only compute for processing state with valid heartbeat
	if state.Status != "processing" || state.LastHeartbeatAt == nil {
		return false, ""
	}

	// Don't consider stale if waiting for user input
	if state.Status == "waiting_for_user" {
		return false, ""
	}

	threshold := getStalenessThreshold(state.StageGroup, state.CurrentStage, state.ExecutionPlan)
	elapsed := time.Since(*state.LastHeartbeatAt)

	if elapsed > threshold {
		return true, formatStaleReason(elapsed, threshold)
	}

	return false, ""
}

// formatStaleReason creates a human-readable staleness message
func formatStaleReason(elapsed, threshold time.Duration) string {
	elapsedStr := formatDuration(elapsed)
	thresholdStr := formatDuration(threshold)
	return "No update for " + elapsedStr + " (expected heartbeat every " + thresholdStr + ")"
}

// formatDuration formats a duration in a human-readable way
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return strconv.Itoa(int(d.Seconds())) + "s"
	}
	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) - (minutes * 60)
	if seconds > 0 {
		return strconv.Itoa(minutes) + "m " + strconv.Itoa(seconds) + "s"
	}
	return strconv.Itoa(minutes) + "m"
}

// normalizeProgress ensures progress values are consistent with pipeline status
func normalizeProgress(state *PipelineState) {
	// Force 100% for completed pipelines
	if state.Status == "completed" {
		if state.Progress.Percent != 100 {
			log.Warnf(
				"[Pipeline %s] Status='completed' but progress=%d%%. Normalizing to 100%%.",
				state.PipelineID,
				state.Progress.Percent,
			)
			state.Progress.Percent = 100
		}

		// Also normalize step counts
		if state.Progress.TotalSteps > 0 && state.Progress.CurrentStep != state.Progress.TotalSteps {
			log.Warnf(
				"[Pipeline %s] Status='completed' but CurrentStep=%d < TotalSteps=%d. Normalizing.",
				state.PipelineID,
				state.Progress.CurrentStep,
				state.Progress.TotalSteps,
			)
			state.Progress.CurrentStep = state.Progress.TotalSteps
		}
	}

	// Clamp progress to valid range [0, 100]
	if state.Progress.Percent < 0 {
		log.Errorf("[Pipeline %s] Negative progress: %d%%. Clamping to 0%%.", state.PipelineID, state.Progress.Percent)
		state.Progress.Percent = 0
	}
	if state.Progress.Percent > 100 {
		log.Errorf("[Pipeline %s] Progress exceeds 100%%: %d%%. Clamping to 100%%.", state.PipelineID, state.Progress.Percent)
		state.Progress.Percent = 100
	}

	// If ExecutionPlan exists but TotalSteps is wrong, derive it
	if len(state.ExecutionPlan) > 0 {
		var plan map[string]interface{}
		if err := json.Unmarshal(state.ExecutionPlan, &plan); err == nil {
			if stages, ok := plan["stages"].([]interface{}); ok {
				expectedTotal := len(stages)

				if state.Progress.TotalSteps == 0 {
					log.Debugf("[Pipeline %s] TotalSteps was 0, deriving from ExecutionPlan (%d stages).", state.PipelineID, expectedTotal)
					state.Progress.TotalSteps = expectedTotal
				}

				// Validate CurrentStep doesn't exceed TotalSteps
				if state.Progress.CurrentStep > state.Progress.TotalSteps {
					log.Warnf(
						"[Pipeline %s] CurrentStep (%d) > TotalSteps (%d). Clamping.",
						state.PipelineID,
						state.Progress.CurrentStep,
						state.Progress.TotalSteps,
					)
					state.Progress.CurrentStep = state.Progress.TotalSteps
				}
			}
		}
	}
}

// extractErrorMessage extracts error message with explicit fallback chain
func extractErrorMessage(state PipelineState, metaObj map[string]interface{}) string {
	// Priority 1: Check metadata for error_message
	if metaObj != nil {
		if errMsg, ok := metaObj["error_message"].(string); ok && errMsg != "" {
			return errMsg
		}
	}

	// Priority 2: Check execution_plan for failed stage error
	if len(state.ExecutionPlan) > 0 {
		var plan map[string]interface{}
		if err := json.Unmarshal(state.ExecutionPlan, &plan); err == nil {
			if stages, ok := plan["stages"].([]interface{}); ok {
				for _, s := range stages {
					if stage, ok := s.(map[string]interface{}); ok {
						status, _ := stage["status"].(string)
						if status == "failed" {
							if errMsg, ok := stage["error_message"].(string); ok && errMsg != "" {
								return errMsg
							}
						}
					}
				}
			}
		}
	}

	// Priority 3: Generic fallback for failed state
	if state.Status == "failed" {
		return "Pipeline failed. No error details available."
	}

	return ""
}
