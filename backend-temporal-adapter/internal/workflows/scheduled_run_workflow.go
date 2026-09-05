package workflows

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ============================================================================
// Scheduled Pipeline Run Workflow
// ============================================================================
// This workflow is triggered by Temporal Schedules for recurring pipeline runs.
// It creates an execution record, prevents overlaps, and starts the main
// NLPipelineWorkflowV2 as a child workflow with fire-and-forget semantics.

// ScheduledRunInput is the input to the scheduled run wrapper workflow
type ScheduledRunInput struct {
	ScheduleID  string `json:"schedule_id"`
	PipelineID  string `json:"pipeline_id"`
	ScheduledAt int64  `json:"scheduled_at"` // Filled by workflow for determinism (UnixNano)
}

// ScheduledPipelineRunWorkflow is the wrapper workflow for scheduled executions
// Pattern: Create execution → Check overlap → Start child → Return immediately
func ScheduledPipelineRunWorkflow(ctx workflow.Context, input ScheduledRunInput) error {
	logger := workflow.GetLogger(ctx)

	// Important: DO NOT rely on a timestamp passed in via Schedule args (it would be constant across runs).
	// Temporal's workflow clock is deterministic; use it to derive a stable "scheduled_at" per invocation.
	input.ScheduledAt = workflow.Now(ctx).UnixNano()

	logger.Info("🕐 ScheduledPipelineRunWorkflow started",
		"schedule_id", input.ScheduleID,
		"pipeline_id", input.PipelineID,
		"scheduled_at", input.ScheduledAt)

	// Setup activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		// Keep HeartbeatTimeout >= StartToCloseTimeout since these wrapper activities
		// are short-lived and generally don't emit heartbeats.
		HeartbeatTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Check for active runs (overlap guard)
	var hasActiveRun bool
	err := workflow.ExecuteActivity(ctx, CheckActiveRunActivity, input.PipelineID).Get(ctx, &hasActiveRun)
	if err != nil {
		logger.Error("Failed to check for active runs", "error", err)
		return fmt.Errorf("overlap guard check failed: %w", err)
	}

	if hasActiveRun {
		logger.Info("⏭️  Skipping scheduled execution (pipeline has active run)",
			"pipeline_id", input.PipelineID)
		return nil // Successful skip (not an error)
	}

	// Step 2: Create execution record (deterministic + idempotent)
	var executionID string
	err = workflow.ExecuteActivity(ctx, CreateScheduledExecutionActivity, input).Get(ctx, &executionID)
	if err != nil {
		logger.Error("Failed to create execution record", "error", err)
		return fmt.Errorf("create execution failed: %w", err)
	}

	// If the pipeline was deleted but its Temporal Schedule still exists, treat as a no-op.
	// Returning an empty executionID avoids noisy "Activity error" logs.
	if executionID == "" {
		logger.Info("⏭️  Skipping scheduled execution (pipeline not found; likely deleted)",
			"pipeline_id", input.PipelineID,
			"schedule_id", input.ScheduleID)
		return nil
	}

	logger.Info("✅ Created execution record", "execution_id", executionID)

	// Step 3: Update pipeline status to 'running' (and mark scheduled execution row as running)
	err = workflow.ExecuteActivity(ctx, UpdatePipelineStatusActivity, input.PipelineID, executionID, "running", "").Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to update pipeline status", "error", err)
		// Mark execution as failed
		_ = workflow.ExecuteActivity(ctx, MarkExecutionFailedActivity, executionID, err.Error()).Get(ctx, nil)
		return fmt.Errorf("update status failed: %w", err)
	}

	// Step 4: Fetch pipeline run context (live lookup)
	var pipelineInput NLPipelineWorkflowV2Input
	err = workflow.ExecuteActivity(ctx, FetchPipelineRunContextActivity, input.PipelineID, executionID).Get(ctx, &pipelineInput)
	if err != nil {
		logger.Error("Failed to fetch pipeline context", "error", err)
		// Mark execution as failed
		_ = workflow.ExecuteActivity(ctx, MarkExecutionFailedActivity, executionID, err.Error()).Get(ctx, nil)
		return fmt.Errorf("fetch context failed: %w", err)
	}

	// Step 5: Start child workflow (fire-and-forget)
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:        executionID,
		TaskQueue:         "pipeline-workflows",
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON, // Key: child continues independently
	})

	childFuture := workflow.ExecuteChildWorkflow(childCtx, NLPipelineWorkflowV2, pipelineInput)

	// Wait only for child to START (not complete)
	childExec := childFuture.GetChildWorkflowExecution()
	if err := childExec.Get(ctx, nil); err != nil {
		logger.Error("Failed to start child workflow", "error", err)
		// Mark execution as failed
		_ = workflow.ExecuteActivity(ctx, MarkExecutionFailedActivity, executionID, err.Error()).Get(ctx, nil)
		return fmt.Errorf("failed to start child workflow: %w", err)
	}

	logger.Info("✅ Started child workflow",
		"execution_id", executionID)

	// Success - wrapper completes, child continues independently
	return nil
}

// ============================================================================
// Activities
// ============================================================================

// CheckActiveRunActivity checks if the pipeline has an active execution
// Returns true if there's an active run (prevent overlap)
func CheckActiveRunActivity(ctx context.Context, pipelineID string) (bool, error) {
	db := activityCtx.DB
	if db == nil {
		return false, fmt.Errorf("database not available")
	}

	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return false, fmt.Errorf("database type assertion failed")
	}

	// Check pipeline_progress for authoritative state
	var status string
	var blockingReason sql.NullString
	var progressUpdatedAt sql.NullTime
	err := sqlDB.QueryRow(`
		SELECT status, blocking_reason_type, updated_at
		FROM pipeline_progress
		WHERE pipeline_id = $1
	`, pipelineID).Scan(&status, &blockingReason, &progressUpdatedAt)

	if err == sql.ErrNoRows {
		// No active progress record - check executions as fallback
		var runningCount int
		err = sqlDB.QueryRow(`
			SELECT COUNT(*) 
			FROM executions 
			WHERE pipeline_id = $1 AND status = 'running'
		`, pipelineID).Scan(&runningCount)

		if err != nil {
			// Fail CLOSED: if we cannot verify whether a run is active, do not proceed.
			// Returning the error lets Temporal retry (RetryPolicy: 3 attempts); if it
			// still fails the wrapper workflow aborts this scheduled run rather than risk
			// a concurrent duplicate run double-writing the destination.
			log.Warnf("overlap guard: failed to check executions for active run: %v", err)
			return false, fmt.Errorf("overlap guard: failed to check executions: %w", err)
		}

		return runningCount > 0, nil
	}

	if err != nil {
		// Fail CLOSED (see the executions fallback above): unverifiable overlap
		// state must not allow a scheduled run to proceed.
		log.Warnf("overlap guard: failed to check pipeline_progress for active run: %v", err)
		return false, fmt.Errorf("overlap guard: failed to check pipeline_progress: %w", err)
	}

	// Check if status indicates active run.
	//
	// `waiting_for_user` counts as active on purpose: a parked run still owns the
	// destination, so starting a second one would double-write. But a park is not
	// progress — it lasts until a human answers, which may be never. The guard
	// therefore keeps skipping, and this skip is the ONLY trace the scheduled run
	// leaves anywhere (the wrapper returns nil before any execution row is
	// created). Log a park at WARN, with how long it has lasted, so a bricked
	// schedule is greppable in SigNoz instead of looking like a healthy no-op.
	// The user-facing counterpart is `blocked_reason` on the schedules API.
	isActive := status == "processing" || status == "waiting_for_user"
	if status == "waiting_for_user" {
		fields := log.Fields{
			"pipeline_id":     pipelineID,
			"status":          status,
			"blocking_reason": strings.TrimSpace(blockingReason.String),
		}
		if progressUpdatedAt.Valid {
			fields["parked_since"] = progressUpdatedAt.Time.UTC().Format(time.RFC3339)
			fields["parked_for"] = time.Since(progressUpdatedAt.Time).Truncate(time.Second).String()
		}
		log.WithFields(fields).Warn("⏸️  Scheduled execution skipped: pipeline is parked on user input — this schedule produces no runs until it is answered")
	} else if isActive {
		log.WithFields(log.Fields{
			"pipeline_id": pipelineID,
			"status":      status,
		}).Info("⏭️  Active run detected, skipping scheduled execution")
	}

	return isActive, nil
}

// CreateScheduledExecutionActivity creates a deterministic execution record
// Uses schedule_id + scheduled_at for idempotent execution ID generation
func CreateScheduledExecutionActivity(ctx context.Context, input ScheduledRunInput) (string, error) {
	db := activityCtx.DB
	if db == nil {
		return "", fmt.Errorf("database not available")
	}

	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return "", fmt.Errorf("database type assertion failed")
	}

	// Generate deterministic execution ID (idempotent across retries)
	deterministicSeed := fmt.Sprintf("%s_%d", input.ScheduleID, input.ScheduledAt)
	executionID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(deterministicSeed)).String()

	scheduledTime := time.Unix(0, input.ScheduledAt).UTC()

	// If the pipeline no longer exists (deleted/archived), do NOT keep failing the schedule.
	// This prevents log spam and repeated retries for orphaned Temporal schedules.
	var exists bool
	if err := sqlDB.QueryRow(`SELECT EXISTS(SELECT 1 FROM pipelines WHERE id = $1)`, input.PipelineID).Scan(&exists); err == nil {
		if !exists {
			log.WithFields(log.Fields{
				"pipeline_id": input.PipelineID,
				"schedule_id": input.ScheduleID,
			}).Warn("Skipping scheduled execution: pipeline not found (orphaned schedule)")
			return "", nil
		}
	} else {
		// Fail open if existence check fails; we'll attempt insert and let DB enforce constraints.
		log.WithError(err).Warn("Failed to check pipeline existence; proceeding with insert")
	}

	// Idempotent insert
	_, err := sqlDB.Exec(`
		INSERT INTO executions (id, pipeline_id, trigger_source, schedule_id, scheduled_time, status, start_time)
		VALUES ($1, $2, 'scheduled', $3, $4, 'pending', NOW())
		ON CONFLICT (id) DO NOTHING
	`, executionID, input.PipelineID, input.ScheduleID, scheduledTime)

	if err != nil {
		log.WithFields(log.Fields{
			"execution_id": executionID,
			"pipeline_id":  input.PipelineID,
			"schedule_id":  input.ScheduleID,
			"error":        err.Error(),
		}).Error("Failed to create scheduled execution record")
		return "", fmt.Errorf("failed to insert execution: %w", err)
	}

	log.WithFields(log.Fields{
		"execution_id":   executionID,
		"pipeline_id":    input.PipelineID,
		"schedule_id":    input.ScheduleID,
		"scheduled_time": scheduledTime.Format(time.RFC3339),
	}).Info("✅ Created scheduled execution record")

	return executionID, nil
}

// FetchPipelineRunContextActivity fetches the current pipeline configuration
// This implements "live lookup" strategy - use current settings, not frozen
func FetchPipelineRunContextActivity(ctx context.Context, pipelineID, executionID string) (NLPipelineWorkflowV2Input, error) {
	db := activityCtx.DB
	if db == nil {
		return NLPipelineWorkflowV2Input{}, fmt.Errorf("database not available")
	}

	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return NLPipelineWorkflowV2Input{}, fmt.Errorf("database type assertion failed")
	}

	var nlRequest, userID, sourceConnID, destConnID string
	var sourceConnIDNull, destConnIDNull sql.NullString
	var defaultRunMode, dataset, pipelineName sql.NullString
	var configJSON, srcSnapshotJSON, destSnapshotJSON []byte

	// Read the same column set the manual /run path reads from
	// api-gateway/pipelines.go RunPipeline + the snapshot persistence
	// at lines 2117-2124. Without these fields, a scheduled run
	// silently differs from the manual run of the same pipeline —
	// `default_run_mode=reload` becomes a `resume`, and connector
	// versions track "latest" instead of the pinned snapshot. Both
	// gaps were independently confirmed by the ARCH-1 and ARCH-2
	// audit agents.
	err := sqlDB.QueryRow(`
		SELECT
			COALESCE(natural_language_request, description, name) as nl_request,
			name,
			created_by,
			source_connection_id,
			destination_connection_id,
			default_run_mode,
			dataset,
			config,
			source_connector_snapshot,
			destination_connector_snapshot
		FROM pipelines
		WHERE id = $1
	`, pipelineID).Scan(
		&nlRequest,
		&pipelineName,
		&userID,
		&sourceConnIDNull,
		&destConnIDNull,
		&defaultRunMode,
		&dataset,
		&configJSON,
		&srcSnapshotJSON,
		&destSnapshotJSON,
	)

	if err != nil {
		return NLPipelineWorkflowV2Input{}, fmt.Errorf("failed to fetch pipeline: %w", err)
	}

	if sourceConnIDNull.Valid {
		sourceConnID = sourceConnIDNull.String
	}
	if destConnIDNull.Valid {
		destConnID = destConnIDNull.String
	}

	input := NLPipelineWorkflowV2Input{
		PipelineID:              pipelineID,
		ExecutionID:             executionID,
		UserID:                  userID,
		Message:                 nlRequest,
		SourceConnectionID:      sourceConnID,
		DestinationConnectionID: destConnID,
	}

	// Honor pipeline-level default_run_mode + dataset so scheduled runs
	// match manual /run semantics. Manual /run defaults RunMode to
	// "resume" when nothing's set; we keep the empty string here and
	// let executor's loadPipelineModes + run-mode fallback at
	// executor.go's resolution handle the default (avoids two
	// defaulting layers diverging).
	if defaultRunMode.Valid && strings.TrimSpace(defaultRunMode.String) != "" {
		input.RunMode = strings.TrimSpace(defaultRunMode.String)
	}
	if dataset.Valid && strings.TrimSpace(dataset.String) != "" {
		input.Dataset = strings.TrimSpace(dataset.String)
	}
	if pipelineName.Valid {
		input.PipelineName = strings.TrimSpace(pipelineName.String)
	}

	// Hydrate the pinned connector snapshots persisted by manual /run.
	// Without these, scheduled runs route to "latest" image while
	// manual runs route to the pinned image — same code path,
	// different destination data shape if "latest" has drift.
	if len(srcSnapshotJSON) > 0 {
		var snap ConnectorSnapshot
		if uErr := json.Unmarshal(srcSnapshotJSON, &snap); uErr == nil && strings.TrimSpace(snap.Type) != "" {
			input.SourceConnectorSnapshot = &snap
		}
	}
	if len(destSnapshotJSON) > 0 {
		var snap ConnectorSnapshot
		if uErr := json.Unmarshal(destSnapshotJSON, &snap); uErr == nil && strings.TrimSpace(snap.Type) != "" {
			input.DestinationConnectorSnapshot = &snap
		}
	}

	// Sticky table selection: if pipeline has persisted selected_tables, pass to workflow input
	// so the scheduled run does not block on table_selection HITL.
	if len(configJSON) > 0 {
		var cfg map[string]interface{}
		if err := json.Unmarshal(configJSON, &cfg); err == nil {
			if iarr, ok := cfg["selected_tables"].([]interface{}); ok && len(iarr) > 0 {
				tables := make([]string, 0, len(iarr))
				for _, v := range iarr {
					if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
						tables = append(tables, strings.TrimSpace(s))
					}
				}
				if len(tables) > 0 {
					input.SelectedTables = tables
				}
			} else if arr, ok := cfg["selected_tables"].([]string); ok && len(arr) > 0 {
				tables := make([]string, 0, len(arr))
				for _, v := range arr {
					if s := strings.TrimSpace(v); s != "" {
						tables = append(tables, s)
					}
				}
				if len(tables) > 0 {
					input.SelectedTables = tables
				}
			}
		}
	}

	log.WithFields(log.Fields{
		"pipeline_id":  pipelineID,
		"execution_id": executionID,
		"user_id":      userID,
	}).Info("✅ Fetched pipeline run context")

	return input, nil
}

// MarkExecutionFailedActivity marks an execution as failed with an error message
func MarkExecutionFailedActivity(ctx context.Context, executionID, errorMessage string) error {
	db := activityCtx.DB
	if db == nil {
		return fmt.Errorf("database not available")
	}

	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return fmt.Errorf("database type assertion failed")
	}

	_, err := sqlDB.Exec(`
		UPDATE executions 
		SET status = 'failed', end_time = NOW(), error_message = $1
		WHERE id = $2
	`, errorMessage, executionID)

	if err != nil {
		log.WithFields(log.Fields{
			"execution_id": executionID,
			"error":        err.Error(),
		}).Error("Failed to mark execution as failed")
		return fmt.Errorf("failed to update execution: %w", err)
	}

	log.WithFields(log.Fields{
		"execution_id":  executionID,
		"error_message": errorMessage,
	}).Info("Marked execution as failed")

	return nil
}
