package workflows

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/rsync-ai/shared/kafkaclient"
	"math"
	"strings"
	"time"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
)

// ==============================================================================
// TEMPORAL ACTIVITIES - KAFKA BRIDGE
// ==============================================================================
// Activities emit commands to Kafka and emit domain events.
// Results are received via Temporal signals from the Kafka adapter.
//
// Pattern:
// 1. Activity emits command to agent.control.commands
// 2. Agent consumes command, executes task
// 3. Agent emits result to agent.control.results
// 4. Kafka adapter consumes result and signals workflow
// 5. Workflow resumes with result
// ==============================================================================

// KafkaProducer interface for producing messages
type KafkaProducer interface {
	SendMessage(*sarama.ProducerMessage) (partition int32, offset int64, err error)
}

// ActivityContext holds shared dependencies for activities
type ActivityContext struct {
	KafkaProducer KafkaProducer
	DB            interface{} // *sql.DB, stored as interface{} to avoid import cycles
}

var activityCtx *ActivityContext

// InitActivityContext initializes the global activity context
func InitActivityContext(producer KafkaProducer) {
	activityCtx = &ActivityContext{
		KafkaProducer: producer,
	}
}

// SetDB sets the database connection for activities
func SetDB(db interface{}) {
	if activityCtx != nil {
		activityCtx.DB = db
	}
}

// ==============================================================================
// SHARED ACTIVITIES (Used by all workflows)
// ==============================================================================

// EmitDomainEventActivity emits a domain event to Kafka
func EmitDomainEventActivity(ctx context.Context, event map[string]interface{}) error {
	return emitDomainEventActivity(ctx, event)
}

// SendToPipelineDLQ sends a failed pipeline to the DLQ
func SendToPipelineDLQ(ctx context.Context, failure map[string]interface{}) error {
	return sendToPipelineDLQ(ctx, failure)
}

// SendToAgentDLQ sends a failed activity to the DLQ
func SendToAgentDLQ(ctx context.Context, failure map[string]interface{}) error {
	return sendToAgentDLQ(ctx, failure)
}

// StateUpdateActivity updates the authoritative pipeline state in DB (Architecture Phase 1)
// This is called by Temporal at major state transitions to write the "source of truth".
// The Event Projector handles best-effort telemetry updates.
func StateUpdateActivity(ctx context.Context, update StateUpdateInput) error {
	return stateUpdateActivity(ctx, update)
}

// ==============================================================================
// DOMAIN EVENT ACTIVITIES
// ==============================================================================

// emitDomainEventActivity emits a domain event to Kafka
func emitDomainEventActivity(ctx context.Context, event map[string]interface{}) error {
	// Ensure every domain event carries a stable trace_id for correlation (SigNoz).
	// We don't assume OTEL is configured in this binary yet, so we fall back to execution_id.
	if event != nil {
		if _, ok := event["trace_id"]; !ok {
			if execID, ok := event["execution_id"].(string); ok && execID != "" {
				event["trace_id"] = execID
			} else if pipelineID, ok := event["pipeline_id"].(string); ok && pipelineID != "" {
				event["trace_id"] = pipelineID
			}
		}
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal domain event: %w", err)
	}

	pipelineID, ok := event["pipeline_id"].(string)
	if !ok {
		return fmt.Errorf("domain event missing pipeline_id")
	}

	traceID, _ := event["trace_id"].(string)
	traceparent, _ := event["traceparent"].(string)
	tracestate, _ := event["tracestate"].(string)

	headers := []sarama.RecordHeader{
		{
			Key:   []byte("trace_id"),
			Value: []byte(traceID),
		},
	}
	if traceID != "" {
		headers = append(headers, sarama.RecordHeader{Key: []byte("X-Trace-ID"), Value: []byte(traceID)})
	}
	if traceparent != "" {
		headers = append(headers, sarama.RecordHeader{Key: []byte("traceparent"), Value: []byte(traceparent)})
	}
	if tracestate != "" {
		headers = append(headers, sarama.RecordHeader{Key: []byte("tracestate"), Value: []byte(tracestate)})
	}

	msg := &sarama.ProducerMessage{
		Topic:   kafkaclient.Topic("pipeline.domain.events"),
		Key:     sarama.StringEncoder(pipelineID),
		Value:   sarama.ByteEncoder(eventJSON),
		Headers: headers,
	}

	_, _, err = activityCtx.KafkaProducer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to emit domain event: %w", err)
	}

	log.WithFields(log.Fields{
		"event_type":  event["event_type"],
		"pipeline_id": pipelineID,
	}).Info("📡 Emitted domain event to Kafka")

	return nil
}

// ==============================================================================
// DLQ ACTIVITIES
// ==============================================================================

// sendToPipelineDLQ sends a failed pipeline to the DLQ
func sendToPipelineDLQ(ctx context.Context, failure map[string]interface{}) error {
	failureJSON, err := json.Marshal(failure)
	if err != nil {
		return fmt.Errorf("failed to marshal pipeline failure: %w", err)
	}

	pipelineID, ok := failure["pipeline_id"].(string)
	if !ok {
		return fmt.Errorf("pipeline failure missing pipeline_id")
	}

	msg := &sarama.ProducerMessage{
		Topic: kafkaclient.Topic("pipeline.failed.dlq"),
		Key:   sarama.StringEncoder(pipelineID),
		Value: sarama.ByteEncoder(failureJSON),
	}

	_, _, err = activityCtx.KafkaProducer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send to pipeline DLQ: %w", err)
	}

	log.WithFields(log.Fields{
		"pipeline_id": pipelineID,
		"stage":       failure["stage"],
	}).Warn("⚠️  Sent failed pipeline to DLQ")

	return nil
}

// sendToAgentDLQ sends a failed activity to the DLQ
func sendToAgentDLQ(ctx context.Context, failure map[string]interface{}) error {
	failureJSON, err := json.Marshal(failure)
	if err != nil {
		return fmt.Errorf("failed to marshal agent failure: %w", err)
	}

	workflowID, ok := failure["workflow_id"].(string)
	if !ok {
		return fmt.Errorf("agent failure missing workflow_id")
	}

	msg := &sarama.ProducerMessage{
		Topic: kafkaclient.Topic("agent.failed.dlq"),
		Key:   sarama.StringEncoder(workflowID),
		Value: sarama.ByteEncoder(failureJSON),
	}

	_, _, err = activityCtx.KafkaProducer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send to agent DLQ: %w", err)
	}

	log.WithFields(log.Fields{
		"workflow_id": workflowID,
		"agent_type":  failure["agent_type"],
	}).Warn("⚠️  Sent failed activity to DLQ")

	return nil
}

// ==============================================================================
// STATE UPDATE ACTIVITY (Architecture Phase 1)
// ==============================================================================
// This activity is called by Temporal to write authoritative state transitions.
// It uses optimistic locking to ensure consistency and overrides best-effort
// projector updates when there are conflicts.

// stateUpdateActivity writes authoritative pipeline state to the database
func stateUpdateActivity(ctx context.Context, update StateUpdateInput) error {
	if activityCtx == nil || activityCtx.DB == nil {
		log.Warn("⚠️  StateUpdateActivity: DB not available, skipping state write")
		return nil
	}

	db, ok := activityCtx.DB.(*sql.DB)
	if !ok || db == nil {
		log.Warn("⚠️  StateUpdateActivity: DB has unexpected type, skipping state write")
		return nil
	}

	// Retry loop for optimistic locking
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Read current version
		var currentVersion int
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(version, 1) FROM pipeline_progress WHERE pipeline_id = $1
		`, update.PipelineID).Scan(&currentVersion)

		newVersion := 1
		if err == nil {
			newVersion = currentVersion + 1
		} else if err != sql.ErrNoRows {
			// DB is unhealthy or schema mismatch; treat as non-fatal.
			log.WithError(err).Warn("⚠️  StateUpdateActivity: Failed to read current version, skipping")
			return nil
		}

		// Prepare metadata JSON
		metadataJSON := "{}"
		if update.Metadata != nil {
			if metaBytes, err := json.Marshal(update.Metadata); err == nil {
				metadataJSON = string(metaBytes)
			}
		}

		// Derive progress from execution_plan when present (drives fully dynamic UI).
		progressPercent, progressStep, progressTotal := deriveProgressFromExecutionPlan(update.Metadata)
		if progressTotal == 0 {
			progressTotal = 7
		}
		progressPercent, progressStep = normalizeTerminalProgress(update.Status, progressPercent, progressStep, progressTotal)

		// Persist blocking_reason_* columns for wait states so /api/v1/pipelines/:id/state
		// can return a consistent, query-friendly blocking_reason (in addition to metadata JSON).
		var blockingType sql.NullString
		var blockingDesc sql.NullString
		var blockingSec sql.NullInt32
		isWaiting := update.EventType == "PIPELINE_WAITING" || update.Status == "waiting_for_user"
		if isWaiting {
			bt := ""
			if update.Metadata != nil {
				if v, ok := update.Metadata["blocking_reason_type"].(string); ok && v != "" {
					bt = v
				} else if v, ok := update.Metadata["request_type"].(string); ok && v != "" {
					bt = v
				} else if v, ok := update.Metadata["action_needed"].(string); ok && v != "" {
					bt = v
				}

				// Optional estimate (best-effort)
				switch v := update.Metadata["estimated_seconds"].(type) {
				case int:
					blockingSec = sql.NullInt32{Int32: int32(v), Valid: true}
				case int32:
					blockingSec = sql.NullInt32{Int32: v, Valid: true}
				case int64:
					blockingSec = sql.NullInt32{Int32: int32(v), Valid: true}
				case float64:
					blockingSec = sql.NullInt32{Int32: int32(v), Valid: true}
				}
			}

			if bt == "" {
				bt = "user_input_required"
			}

			blockingType = sql.NullString{String: bt, Valid: true}
			if update.Summary != "" {
				blockingDesc = sql.NullString{String: update.Summary, Valid: true}
			}
		}

		// Stage timing fields (best-effort; authoritative per-stage timing is stored in execution_plan).
		stageStartedAt := (*time.Time)(nil)
		if update.EventType == "STAGE_STARTED" {
			now := time.Now().UTC()
			stageStartedAt = &now
		}

		// Execute update with version check (optimistic lock)
		// This is an AUTHORITATIVE update - it should override projector updates
		result, err := db.ExecContext(ctx, `
			INSERT INTO pipeline_progress (
				pipeline_id, execution_id, status, current_stage, 
				schema_version,
				progress_percent, progress_current_step, progress_total_steps,
				blocking_reason_type, blocking_reason_description, blocking_reason_estimated_seconds,
				stage_group, stage_state, stage_summary,
				stage_started_at, stage_last_heartbeat_at,
				message, metadata, version, updated_at
			) VALUES (
				$1, $2, $3, $4,
				2,
				$5, $6, $7,
				$8, $9, $10,
				$11, $12, $13,
				$14, NOW(),
				$15, $16::jsonb, $17, NOW()
			)
			ON CONFLICT (pipeline_id) DO UPDATE SET
				execution_id = EXCLUDED.execution_id,
				status = EXCLUDED.status,
				current_stage = EXCLUDED.current_stage,
				schema_version = EXCLUDED.schema_version,
				progress_percent = EXCLUDED.progress_percent,
				progress_current_step = EXCLUDED.progress_current_step,
				progress_total_steps = EXCLUDED.progress_total_steps,
				blocking_reason_type = EXCLUDED.blocking_reason_type,
				blocking_reason_description = EXCLUDED.blocking_reason_description,
				blocking_reason_estimated_seconds = EXCLUDED.blocking_reason_estimated_seconds,
				stage_group = EXCLUDED.stage_group,
				stage_state = EXCLUDED.stage_state,
				stage_summary = EXCLUDED.stage_summary,
				stage_started_at = COALESCE(EXCLUDED.stage_started_at, pipeline_progress.stage_started_at),
				stage_last_heartbeat_at = EXCLUDED.stage_last_heartbeat_at,
				message = EXCLUDED.message,
				metadata = EXCLUDED.metadata,
				version = EXCLUDED.version,
				updated_at = NOW()
			WHERE pipeline_progress.version = $17 - 1
		`, update.PipelineID, update.ExecutionID, update.Status, update.Stage,
			progressPercent, progressStep, progressTotal,
			blockingType, blockingDesc, blockingSec,
			update.StageGroup, stateForEventType(update.EventType), update.Summary,
			stageStartedAt,
			update.Summary, metadataJSON, newVersion)

		if err != nil {
			// Non-fatal for workflow correctness; UI can fall back to websocket events.
			log.WithError(err).Warn("⚠️  StateUpdateActivity: DB write failed, skipping")
			return nil
		}

		// Check if update succeeded (rows affected)
		if rows, err := result.RowsAffected(); err == nil && rows > 0 {
			log.WithFields(log.Fields{
				"pipeline_id":  update.PipelineID,
				"execution_id": update.ExecutionID,
				"stage":        update.Stage,
				"status":       update.Status,
				"event_type":   update.EventType,
				"version":      newVersion,
			}).Info("📝 StateUpdateActivity: Wrote authoritative state")
			return nil
		}

		// Version conflict - retry
		log.WithFields(log.Fields{
			"pipeline_id": update.PipelineID,
			"attempt":     attempt + 1,
			"max_retries": maxRetries,
		}).Warn("⚠️  StateUpdateActivity: Version conflict, retrying...")

		time.Sleep(time.Millisecond * 100) // Brief backoff
	}

	// Best-effort: don't fail workflow on version conflicts.
	log.WithFields(log.Fields{
		"pipeline_id": update.PipelineID,
	}).Warn("⚠️  StateUpdateActivity: Failed after retries (version conflicts), skipping")
	return nil
}

func stateForEventType(eventType string) string {
	switch eventType {
	case "STAGE_STARTED", "STAGE_PROGRESS":
		return "running"
	case "STAGE_COMPLETED":
		return "succeeded"
	case "STAGE_FAILED":
		return "failed"
	case "PIPELINE_WAITING":
		return "waiting"
	case "PIPELINE_COMPLETED":
		return "succeeded"
	default:
		return "running"
	}
}

// normalizeTerminalProgress lands a finished progress bar on a terminal COMPLETED
// status, returning the percent and current step to persist.
//
// deriveProgressFromExecutionPlan reports the plan as it stood when the event was
// emitted, which for the final event is routinely one step short: a prod run persisted
// `completed | 88% | 7 of 8` and stayed there, because 7/8 rounds to 88 and nothing ever
// revisited the row. api-gateway's event projector already normalizes this, but that
// writer is explicitly best-effort and loses: StateUpdateActivity is the AUTHORITATIVE
// writer and its ON CONFLICT copies EXCLUDED.progress_* verbatim, overwriting the
// projector's corrected values. So the clamp has to live here too.
//
// Only "completed" is normalized — a failed or cancelled run should keep the partial
// progress it actually reached, since that is the diagnostic.
func normalizeTerminalProgress(status string, percent, currentStep, totalSteps int) (int, int) {
	if !strings.EqualFold(strings.TrimSpace(status), "completed") {
		return percent, currentStep
	}
	if totalSteps > 0 {
		currentStep = totalSteps
	}
	return 100, currentStep
}

func deriveProgressFromExecutionPlan(meta map[string]interface{}) (percent int, currentStep int, totalSteps int) {
	if meta == nil {
		return 0, 0, 7
	}

	raw, ok := meta["execution_plan"].(map[string]interface{})
	if !ok || raw == nil {
		return 0, 0, 7
	}

	stagesRaw, ok := raw["stages"].([]interface{})
	if !ok || stagesRaw == nil {
		return 0, 0, 7
	}

	totalSteps = len(stagesRaw)
	if totalSteps == 0 {
		return 0, 0, 0
	}

	sumProgress := 0
	completed := 0
	runningOrWaitingSeen := false

	for _, s := range stagesRaw {
		stage, ok := s.(map[string]interface{})
		if !ok || stage == nil {
			continue
		}

		// progress
		switch v := stage["progress"].(type) {
		case float64:
			sumProgress += int(v)
		case int:
			sumProgress += v
		case int32:
			sumProgress += int(v)
		case int64:
			sumProgress += int(v)
		}

		// status
		status, _ := stage["status"].(string)
		switch status {
		case "complete", "completed":
			completed++
		case "running", "waiting":
			runningOrWaitingSeen = true
		}
	}

	avg := float64(sumProgress) / float64(totalSteps)
	percent = int(math.Round(avg))
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}

	if runningOrWaitingSeen {
		// best-effort: assume we're on the next step after completed ones.
		currentStep = completed + 1
	} else {
		currentStep = completed
	}

	if currentStep < 0 {
		currentStep = 0
	}
	if currentStep > totalSteps {
		currentStep = totalSteps
	}

	return percent, currentStep, totalSteps
}
