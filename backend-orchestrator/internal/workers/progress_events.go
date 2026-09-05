package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/telemetry"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	log "github.com/sirupsen/logrus"
)

// ProgressEvent represents a pipeline progress update event
// This is the standardized event schema for UI updates
type ProgressEvent struct {
	// Versioned, canonical contract (UX hardening)
	SchemaVersion int `json:"schema_version"` // v1

	// Core identifiers
	EventType   string `json:"event_type"` // STAGE_STARTED, STAGE_PROGRESS, STAGE_COMPLETED, STAGE_FAILED, PIPELINE_WAITING, PIPELINE_COMPLETED
	PipelineID  string `json:"pipeline_id"`
	ExecutionID string `json:"execution_id,omitempty"`
	TraceID     string `json:"trace_id,omitempty"`

	// Event envelope (for ordering + replay + correlation)
	EventID    string `json:"event_id,omitempty"`
	Seq        int64  `json:"seq,omitempty"`
	OccurredAt string `json:"occurred_at,omitempty"`
	ReceivedAt string `json:"received_at,omitempty"`
	Severity   string `json:"severity,omitempty"` // info|warn|error

	// Canonical stage contract fields
	Stage           string `json:"stage,omitempty"`       // canonical stage id (intent/resolver/discovery/planner/validator/executor)
	StageGroup      string `json:"stage_group,omitempty"` // UI lane (understanding/connecting/discovering/planning/validating/executing)
	State           string `json:"state,omitempty"`       // queued|running|succeeded|failed|skipped|waiting
	StartedAt       string `json:"started_at,omitempty"`
	LastHeartbeatAt string `json:"last_heartbeat_at,omitempty"`
	DurationMs      int64  `json:"duration_ms,omitempty"`
	Attempt         int    `json:"attempt,omitempty"`
	MaxAttempts     int    `json:"max_attempts,omitempty"`
	Summary         string `json:"summary,omitempty"` // 1-line summary for header/timeline

	// Existing/compat fields (keep for backward compatibility)
	Timestamp      string                 `json:"timestamp"`
	Progress       ProgressInfo           `json:"progress"`
	Status         string                 `json:"status"` // processing, waiting_for_user, completed, failed
	Message        string                 `json:"message"`
	BlockingReason *BlockingReason        `json:"blocking_reason,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

// ProgressInfo contains actual progress data (not calculated)
type ProgressInfo struct {
	Percent     int    `json:"percent"`      // 0-100, backend calculates this
	CurrentStep int    `json:"current_step"` // e.g., 3
	TotalSteps  int    `json:"total_steps"`  // e.g., 7
	Stage       string `json:"stage"`        // intent, discovery, etc.
}

// BlockingReason explains why execution is slow or waiting
type BlockingReason struct {
	Type             string                 `json:"type"` // connector_generation, large_table_scan, external_api_latency, user_input_required
	Description      string                 `json:"description"`
	EstimatedSeconds *int                   `json:"estimated_seconds,omitempty"`
	Details          map[string]interface{} `json:"details,omitempty"`
}

// ProgressEmitter is a helper for workers to emit standardized progress events
type ProgressEmitter struct {
	kafkaManager *kafka.Manager
	db           *sql.DB
}

// NewProgressEmitter creates a new progress emitter
func NewProgressEmitter(kafkaManager *kafka.Manager, db *sql.DB) *ProgressEmitter {
	return &ProgressEmitter{
		kafkaManager: kafkaManager,
		db:           db,
	}
}

// StartStageHeartbeat emits lightweight STAGE_PROGRESS heartbeats at a fixed interval
// until the returned stop() function is called.
//
// Heartbeats are designed to be:
// - best-effort (non-fatal on failure)
// - noise-controlled in UI (metadata.heartbeat=true)
func (e *ProgressEmitter) StartStageHeartbeat(ctx context.Context, base ProgressEvent, interval time.Duration) (stop func()) {
	if interval <= 0 {
		interval = 7 * time.Second
	}

	hbCtx, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(interval)

	// Ensure heartbeat flag exists (UI can compress)
	if base.Metadata == nil {
		base.Metadata = map[string]interface{}{}
	}
	base.Metadata["heartbeat"] = true

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				hb := base
				hb.EventType = "STAGE_PROGRESS"
				hb.Timestamp = "" // let emitter set current timestamp
				hb.Status = "processing"
				// Keep progress percent stable unless caller updates base externally.
				_ = e.EmitProgress(hbCtx, hb)
			}
		}
	}()

	return func() {
		cancel()
	}
}

// normalizeProgressEvent fills in everything a caller may legitimately leave off:
// the envelope (schema_version, timestamp, occurred_at, trace_id, seq, event_id),
// the derived severity/state, and the stage-field normalisations.
//
// Split out of EmitProgress so the defaults can be asserted without a Kafka broker.
// The envelope half is the part worth guarding: seq and event_id are what the
// api-gateway projector orders and dedupes this topic by, and when a producer
// leaves them off the projector silently substitutes values from its own
// namespace — see shared/go/kafkaclient/envelope.go.
func normalizeProgressEvent(ctx context.Context, event *ProgressEvent) error {
	// Set version if not provided
	if event.SchemaVersion == 0 {
		event.SchemaVersion = 1
	}

	// Set timestamp if not provided
	if event.Timestamp == "" {
		event.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	// Ensure occurred_at is set for consistent event ordering/replay.
	if event.OccurredAt == "" {
		event.OccurredAt = event.Timestamp
	}

	// Validate required fields
	if event.PipelineID == "" {
		return fmt.Errorf("pipeline_id is required")
	}
	if event.EventType == "" {
		return fmt.Errorf("event_type is required")
	}

	// Ensure trace_id is present for end-to-end correlation (SigNoz).
	// Prefer the active OTel trace from context; fall back to execution_id; then pipeline_id.
	if event.TraceID == "" {
		if tid := telemetry.TraceIDFromContext(ctx); tid != "" {
			event.TraceID = tid
		} else if event.ExecutionID != "" {
			event.TraceID = event.ExecutionID
		} else {
			event.TraceID = event.PipelineID
		}
	}

	// Ensure event_id + seq exist for UI ordering.
	//
	// Both come from shared/go/kafkaclient rather than being spelled out here, because
	// the api-gateway projector interleaves this service's events with the temporal
	// adapter's on the same topic and orders them by (occurred_at, seq). Two producers
	// deriving seq differently is not a cosmetic difference: the projector invents one
	// for whoever omits it, and an invented per-execution counter of 3 sorts below a
	// UnixNano of 1.7e18 on every tie.
	//
	// seq is a tiebreaker, not a gapless sequence — it does not dedupe anything, and
	// nothing should treat it as if it did.
	if event.Seq == 0 {
		event.Seq = kafkaclient.DomainEventSeq(time.Now())
	}
	if event.EventID == "" {
		// Stable enough for debugging; unique at ns resolution.
		event.EventID = kafkaclient.DomainEventID(event.PipelineID, event.Seq)
	}

	// Default severity based on status/error semantics.
	if event.Severity == "" {
		switch event.EventType {
		case "STAGE_FAILED":
			event.Severity = "error"
		case "PIPELINE_WAITING":
			event.Severity = "warn"
		default:
			event.Severity = "info"
		}
	}

	// Normalize stage fields (keep old + new consistent)
	if event.Stage == "" {
		event.Stage = event.Progress.Stage
	}
	if event.Progress.Stage == "" {
		event.Progress.Stage = event.Stage
	}
	if event.StageGroup == "" {
		event.StageGroup = stageGroupFor(event.Stage)
	}
	if event.Summary == "" {
		// Best-effort: summary is a stable 1-line sentence for header/timeline
		event.Summary = event.Message
	}

	// Default stage state based on event type, unless explicitly set
	if event.State == "" {
		event.State = stateForEventType(event.EventType, event.Status)
	}

	// Default stage timestamps for started/progress events
	if event.EventType == "STAGE_STARTED" {
		event.StartedAt = event.Timestamp
		event.LastHeartbeatAt = event.Timestamp
		if event.Attempt == 0 {
			event.Attempt = 1
		}
		if event.MaxAttempts == 0 {
			event.MaxAttempts = 1
		}
	}
	if event.EventType == "STAGE_PROGRESS" && event.LastHeartbeatAt == "" {
		event.LastHeartbeatAt = event.Timestamp
	}

	return nil
}

// EmitProgress emits a progress event to Kafka and updates the state table
func (e *ProgressEmitter) EmitProgress(ctx context.Context, event ProgressEvent) error {
	if err := normalizeProgressEvent(ctx, &event); err != nil {
		return err
	}

	// NOTE: DB enrichment removed (Architecture Phase 1)
	// Workers are now stateless and don't read/write DB directly.
	// Timing data will be managed by the Event Projector and StateUpdateActivity.

	// Marshal to JSON
	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Errorf("Failed to marshal progress event: %v", err)
		return err
	}

	// Emit to pipeline.domain.events (for WebSocket)
	// IMPORTANT: propagate trace context from ctx into Kafka headers.
	err = e.kafkaManager.ProduceWithContext(ctx, "pipeline.domain.events", []byte(event.PipelineID), eventJSON)
	if err != nil {
		log.Errorf("Failed to produce progress event to Kafka: %v", err)
		return err
	}

	log.Infof("📤 Emitted progress event: %s - %s (%d%%)", event.PipelineID, event.EventType, event.Progress.Percent)

	// NOTE: Workers no longer write to DB directly (Architecture Phase 1)
	// State updates are handled by:
	// 1. Temporal's StateUpdateActivity (authoritative state transitions)
	// 2. API Gateway's Event Projector (best-effort telemetry updates)
	// This ensures a single source of truth and prevents race conditions.

	return nil
}

func stageGroupFor(stage string) string {
	switch stage {
	case "intent":
		return "understanding"
	case "capability_resolver", "resolver", "connection_validator":
		return "connecting"
	case "discovery":
		return "discovering"
	case "planner":
		return "planning"
	case "validator", "policy_check", "schema_validation":
		return "validating"
	case "executor":
		return "executing"
	case "completed":
		return "executing"
	default:
		return "planning"
	}
}

func stateForEventType(eventType string, status string) string {
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
		// Best-effort fallback from legacy status
		switch status {
		case "waiting_for_user":
			return "waiting"
		case "failed":
			return "failed"
		case "completed":
			return "succeeded"
		default:
			return "running"
		}
	}
}

// enrichFromDB fills started_at/heartbeat/duration/attempt fields when possible.
// This keeps the event contract deterministic even for completed events.
func (e *ProgressEmitter) enrichFromDB(event *ProgressEvent) {
	if e.db == nil || event == nil || event.PipelineID == "" {
		return
	}

	// These columns are added by migration 015; treat missing columns as non-fatal.
	var (
		dbStartedAt   sql.NullTime
		dbHeartbeatAt sql.NullTime
		dbAttempt     sql.NullInt32
		dbMaxAttempts sql.NullInt32
	)

	err := e.db.QueryRow(`
		SELECT stage_started_at, stage_last_heartbeat_at, stage_attempt, stage_max_attempts
		FROM pipeline_progress
		WHERE pipeline_id = $1
	`, event.PipelineID).Scan(&dbStartedAt, &dbHeartbeatAt, &dbAttempt, &dbMaxAttempts)
	if err != nil {
		return
	}

	if event.StartedAt == "" && dbStartedAt.Valid {
		event.StartedAt = dbStartedAt.Time.UTC().Format(time.RFC3339)
	}
	if event.LastHeartbeatAt == "" && dbHeartbeatAt.Valid {
		event.LastHeartbeatAt = dbHeartbeatAt.Time.UTC().Format(time.RFC3339)
	}
	if event.Attempt == 0 && dbAttempt.Valid {
		event.Attempt = int(dbAttempt.Int32)
	}
	if event.MaxAttempts == 0 && dbMaxAttempts.Valid {
		event.MaxAttempts = int(dbMaxAttempts.Int32)
	}

	// Compute duration_ms when we have started_at
	if event.DurationMs == 0 && event.StartedAt != "" {
		if started, err := time.Parse(time.RFC3339, event.StartedAt); err == nil {
			if now, err := time.Parse(time.RFC3339, event.Timestamp); err == nil {
				if now.After(started) {
					event.DurationMs = now.Sub(started).Milliseconds()
				}
			}
		}
	}
}

// updateStateTable updates the pipeline_states table
func (e *ProgressEmitter) updateStateTable(event ProgressEvent) error {
	// Build update map
	update := map[string]interface{}{
		"execution_id":  event.ExecutionID,
		"status":        event.Status,
		"current_stage": event.Progress.Stage,
		"message":       event.Message,
		"progress": map[string]interface{}{
			"percent":      event.Progress.Percent,
			"current_step": event.Progress.CurrentStep,
			"total_steps":  event.Progress.TotalSteps,
		},
	}

	if event.BlockingReason != nil {
		update["blocking_reason"] = map[string]interface{}{
			"type":              event.BlockingReason.Type,
			"description":       event.BlockingReason.Description,
			"estimated_seconds": event.BlockingReason.EstimatedSeconds,
		}
	}

	if event.Metadata != nil {
		update["metadata"] = event.Metadata
	}

	// Merge metadata (artifacts/metrics/stage data must persist across events)
	mergedMeta := map[string]interface{}{}
	if e.db != nil {
		var existing sql.NullString
		// Non-fatal if the row doesn't exist yet (first event)
		_ = e.db.QueryRow(`SELECT metadata FROM pipeline_progress WHERE pipeline_id = $1`, event.PipelineID).Scan(&existing)
		if existing.Valid && existing.String != "" {
			_ = json.Unmarshal([]byte(existing.String), &mergedMeta)
		}
	}
	if event.Metadata != nil {
		mergedMeta = deepMergeMaps(mergedMeta, event.Metadata)
	}

	metadataJSON := "{}"
	if metaBytes, err := json.Marshal(mergedMeta); err == nil {
		metadataJSON = string(metaBytes)
	}

	var blockingType, blockingDesc sql.NullString
	var blockingSec sql.NullInt32
	if event.BlockingReason != nil {
		blockingType = sql.NullString{String: event.BlockingReason.Type, Valid: true}
		blockingDesc = sql.NullString{String: event.BlockingReason.Description, Valid: true}
		if event.BlockingReason.EstimatedSeconds != nil {
			blockingSec = sql.NullInt32{Int32: int32(*event.BlockingReason.EstimatedSeconds), Valid: true}
		}
	}

	// Upsert
	_, err := e.db.Exec(`
		INSERT INTO pipeline_progress (
			pipeline_id, execution_id, status, current_stage, schema_version,
			stage_group, stage_state, stage_started_at, stage_last_heartbeat_at,
			stage_duration_ms, stage_attempt, stage_max_attempts, stage_summary,
			progress_percent, progress_current_step, progress_total_steps,
			blocking_reason_type, blocking_reason_description, blocking_reason_estimated_seconds,
			message, metadata, updated_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, NULLIF($8,'')::timestamp, NULLIF($9,'')::timestamp,
			$10, $11, $12, $13,
			$14, $15, $16, $17, $18, $19, $20, $21::jsonb, NOW()
		)
		ON CONFLICT (pipeline_id) DO UPDATE SET
			execution_id = COALESCE(EXCLUDED.execution_id, pipeline_progress.execution_id),
			status = EXCLUDED.status,
			current_stage = EXCLUDED.current_stage,
			schema_version = EXCLUDED.schema_version,
			stage_group = EXCLUDED.stage_group,
			stage_state = EXCLUDED.stage_state,
			stage_started_at = COALESCE(EXCLUDED.stage_started_at, pipeline_progress.stage_started_at),
			stage_last_heartbeat_at = COALESCE(EXCLUDED.stage_last_heartbeat_at, pipeline_progress.stage_last_heartbeat_at),
			stage_duration_ms = EXCLUDED.stage_duration_ms,
			stage_attempt = EXCLUDED.stage_attempt,
			stage_max_attempts = EXCLUDED.stage_max_attempts,
			stage_summary = EXCLUDED.stage_summary,
			progress_percent = EXCLUDED.progress_percent,
			progress_current_step = EXCLUDED.progress_current_step,
			progress_total_steps = EXCLUDED.progress_total_steps,
			blocking_reason_type = EXCLUDED.blocking_reason_type,
			blocking_reason_description = EXCLUDED.blocking_reason_description,
			blocking_reason_estimated_seconds = EXCLUDED.blocking_reason_estimated_seconds,
			message = EXCLUDED.message,
			metadata = EXCLUDED.metadata,
			updated_at = NOW()
	`, event.PipelineID, event.ExecutionID, event.Status, event.Progress.Stage, event.SchemaVersion,
		event.StageGroup, event.State, event.StartedAt, event.LastHeartbeatAt,
		event.DurationMs, event.Attempt, event.MaxAttempts, event.Summary,
		event.Progress.Percent, event.Progress.CurrentStep, event.Progress.TotalSteps,
		blockingType, blockingDesc, blockingSec,
		event.Message, metadataJSON)

	return err
}

func deepMergeMaps(dst map[string]interface{}, src map[string]interface{}) map[string]interface{} {
	if dst == nil {
		dst = map[string]interface{}{}
	}
	for k, v := range src {
		if v == nil {
			// Treat explicit nil as "set", but keep existing if present.
			if _, exists := dst[k]; !exists {
				dst[k] = nil
			}
			continue
		}

		// If both are objects, merge recursively
		if dstMap, ok := dst[k].(map[string]interface{}); ok {
			if srcMap, ok := v.(map[string]interface{}); ok {
				dst[k] = deepMergeMaps(dstMap, srcMap)
				continue
			}
		}

		// Otherwise replace
		dst[k] = v
	}
	return dst
}

// Helper function to create a pointer to int
func ptr(i int) *int {
	return &i
}
