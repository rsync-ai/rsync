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

// progressEventTimeLayout is RFC3339 with milliseconds, the precision the
// temporal adapter stamps its copy of the same stage transition with. Whole
// seconds put this producer's STAGE_COMPLETED of a sub-second stage BEFORE the
// adapter's STAGE_STARTED of it (14:58:25Z vs 14:58:25.368Z), and the Overview
// read the second start as a retry: "Retry 2/2" on five stages that ran once.
// Fixed width, so the strings still sort as the times do; time.RFC3339 parses it.
const progressEventTimeLayout = "2006-01-02T15:04:05.000Z07:00"

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
		event.Timestamp = time.Now().UTC().Format(progressEventTimeLayout)
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

	// Ensure trace_id is present for end-to-end correlation.
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
	case "infra_preflight":
		// Its own lane: under the "planning" default it read as a second Planning.
		return "infra_preflight"
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
