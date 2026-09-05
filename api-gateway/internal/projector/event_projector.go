package projector

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/rsync-ai/shared/kafkaclient"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"sync"
	"time"

	rsynckafka "api-gateway/internal/kafka"
	"api-gateway/internal/security"

	"github.com/segmentio/kafka-go"
)

// EventProjector consumes pipeline.domain.events and projects them to pipeline_progress table
// This is the "best-effort" projector for UI telemetry updates.
// Temporal's StateUpdateActivity writes authoritative state transitions.
type EventProjector struct {
	db      *sql.DB
	brokers []string
	reader  *kafka.Reader
	ctx     context.Context
	cancel  context.CancelFunc

	seqMu   sync.Mutex
	lastSeq map[string]int64 // execution_id -> last assigned seq (projector-local best-effort)

	// OnPipelineCompleted is called once per pipeline completion, for the downstream
	// models that are scheduled on "after this pipeline runs" rather than on a clock.
	//
	// A hook field rather than a direct call because the implementation lives in
	// package handlers, which already imports far too much to be importable from here
	// without a cycle. main.go is the composition root and assigns it; nil is a valid
	// value and means nothing is listening, which is what every test and every
	// projector-only binary gets.
	//
	// Deliberately NOT part of the offset-commit contract: a failure here must not
	// redeliver the event, because redelivery would re-run every model attached to the
	// pipeline. See the call site.
	OnPipelineCompleted func(ctx context.Context, pipelineID, executionID string)

	// One log line per (producer, event_type, field) the first time this projector
	// has to invent an envelope field. See noteEnvelopeGap.
	gapMu   sync.Mutex
	gapSeen map[string]bool
}

// ProgressEvent represents a pipeline progress event from Kafka
type ProgressEvent struct {
	SchemaVersion   int                    `json:"schema_version"`
	EventType       string                 `json:"event_type"`
	PipelineID      string                 `json:"pipeline_id"`
	ExecutionID     string                 `json:"execution_id"`
	Stage           string                 `json:"stage"`
	StageGroup      string                 `json:"stage_group"`
	State           string                 `json:"state"`
	StartedAt       string                 `json:"started_at"`
	LastHeartbeatAt string                 `json:"last_heartbeat_at"`
	DurationMs      int64                  `json:"duration_ms"`
	Attempt         int                    `json:"attempt"`
	MaxAttempts     int                    `json:"max_attempts"`
	Summary         string                 `json:"summary"`
	Timestamp       string                 `json:"timestamp"`
	Progress        ProgressInfo           `json:"progress"`
	Status          string                 `json:"status"`
	Message         string                 `json:"message"`
	BlockingReason  *BlockingReason        `json:"blocking_reason"`
	Metadata        map[string]interface{} `json:"metadata"`
}

type ProgressInfo struct {
	Percent     int    `json:"percent"`
	CurrentStep int    `json:"current_step"`
	TotalSteps  int    `json:"total_steps"`
	Stage       string `json:"stage"`
}

type BlockingReason struct {
	Type             string                 `json:"type"`
	Description      string                 `json:"description"`
	EstimatedSeconds *int                   `json:"estimated_seconds"`
	Details          map[string]interface{} `json:"details"`
}

// NewEventProjector creates a new event projector
func NewEventProjector(db *sql.DB, brokers []string) *EventProjector {
	ctx, cancel := context.WithCancel(context.Background())
	return &EventProjector{
		db:      db,
		brokers: brokers,
		ctx:     ctx,
		cancel:  cancel,
		lastSeq: make(map[string]int64),
	}
}

// Start begins consuming and projecting events
func (p *EventProjector) Start() error {
	// Namespaced under the same KAFKA_TOPIC_PREFIX as the topic it reads, so a
	// customer-managed cluster can cover both with one PREFIXED grant.
	groupID := kafkaclient.Group("api-gateway-projector")

	p.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        p.brokers,
		Dialer:         rsynckafka.Dialer(p.brokers),
		Topic:          kafkaclient.Topic("pipeline.domain.events"),
		GroupID:        groupID,
		MinBytes:       1e3,  // 1KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        3 * time.Second,
		StartOffset:    kafka.FirstOffset, // Start from beginning to catch up
		CommitInterval: 0,                 // synchronous: we commit explicitly AFTER projecting (see consumeLoop)
	})

	log.Println("📊 Event Projector: Starting (consuming pipeline.domain.events)")

	go p.consumeLoop()

	return nil
}

// Stop stops the projector
func (p *EventProjector) Stop() error {
	p.cancel()
	if p.reader != nil {
		return p.reader.Close()
	}
	return nil
}

// consumeLoop consumes events and projects them to the database
func (p *EventProjector) consumeLoop() {
	for {
		select {
		case <-p.ctx.Done():
			log.Println("Event Projector: Stopping")
			return
		default:
			// FetchMessage (not ReadMessage) does NOT auto-commit the offset on read.
			// We commit explicitly only AFTER projectEvent has run, so if the process
			// crashes mid-projection the message is redelivered on restart instead of
			// being silently skipped (the offset-committed-before-write loss window the
			// old ReadMessage + CommitInterval auto-commit had).
			msg, err := p.reader.FetchMessage(p.ctx)
			if err != nil {
				if p.ctx.Err() != nil {
					return // Context cancelled
				}
				log.Printf("Event Projector: Error reading message: %v", err)
				time.Sleep(time.Second)
				continue
			}

			if err := p.projectEvent(msg); err != nil {
				log.Printf("Event Projector: Failed to project event: %v", err)
				// Continue: these are best-effort telemetry events. We still commit below
				// (a malformed/poison event must not block the partition forever); the
				// crash-during-projection window is what the explicit commit protects.
			}

			if err := p.reader.CommitMessages(p.ctx, msg); err != nil {
				if p.ctx.Err() != nil {
					return
				}
				log.Printf("Event Projector: Failed to commit offset: %v", err)
			}
		}
	}
}

// projectEvent projects a single event to the database
func (p *EventProjector) projectEvent(msg kafka.Message) error {
	// Parse raw payload first so we can store ALL events (including AGENT_*).
	var raw map[string]interface{}
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		return err
	}

	// Store the run event for replayable monitoring UX (redacted).
	stored, err := p.storeRunEvent(msg, raw)
	if err != nil {
		// Best-effort: don't fail projection if storage fails.
		log.Printf("Event Projector: Failed to store run event: %v", err)
	}

	// Upsert per-table statistics (DMS-like) for TABLE_STATS events.
	eventType, _ := raw["event_type"].(string)

	// Downstream models that rebuild after this pipeline. Gated on `stored` — the
	// INSERT above is ON CONFLICT DO NOTHING on (pipeline_id, event_id), so a true
	// here means this is the first time this projector has ever seen THIS event, and
	// the same row that dedupes the event store dedupes the rebuild for free.
	//
	// That matters more than it looks. Offsets are committed after projection, so a
	// crash between the two redelivers the whole message; a partition reassignment can
	// replay a batch; a projector started with StartOffset FirstOffset replays the
	// entire topic. Without this guard every one of those would fire every downstream
	// model again — a DROP/CREATE of a user's table, on a replay of an event from
	// weeks ago.
	//
	// Errors are swallowed inside the hook rather than returned, for the same reason:
	// a returned error skips the offset commit, and the redelivery would arrive with
	// `stored` false and fire nothing anyway. The hook owns its own retry story.
	if stored && eventType == "PIPELINE_COMPLETED" && p.OnPipelineCompleted != nil {
		pipelineID, _ := raw["pipeline_id"].(string)
		executionID, _ := raw["execution_id"].(string)
		if strings.TrimSpace(pipelineID) != "" {
			p.OnPipelineCompleted(p.ctx, pipelineID, executionID)
		}
	}
	if eventType == "TABLE_STATS" {
		if err := p.upsertTableStats(raw); err != nil {
			log.Printf("Event Projector: Failed to upsert table stats: %v", err)
		}
	}

	// Best-effort: infer CDC mode for "streaming" runs and persist onto the pipeline record.
	// This unblocks CDC telemetry agents (cdcstats/sentinel) that rely on pipelines.sync_mode.
	p.maybePersistStreamingSyncMode(raw)

	// Then parse into the progress struct for pipeline_progress projection (subset of events).
	var event ProgressEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return err
	}

	// Only project lifecycle/progress events into pipeline_progress.
	// Agent-specific events (AGENT_*, LLM_USAGE, etc.) are intended for the run-event store
	// and should not mutate UI stage/progress state.
	switch event.EventType {
	case "PIPELINE_CREATED",
		"PIPELINE_STARTED",
		"PIPELINE_WAITING",
		"PIPELINE_COMPLETED",
		"PIPELINE_FAILED",
		"STAGE_STARTED",
		"STAGE_PROGRESS",
		"STAGE_COMPLETED",
		"STAGE_FAILED":
		// allow
	default:
		return nil
	}

	// Skip events without pipeline_id
	if event.PipelineID == "" {
		return nil
	}

	// ----------------------------------------------------------------------------
	// Backfill missing fields for legacy/minimal domain events
	// ----------------------------------------------------------------------------
	// Some producers emit only top-level `stage` without `progress.stage`.
	if event.Progress.Stage == "" && event.Stage != "" {
		event.Progress.Stage = event.Stage
	}

	// Some producers emit PIPELINE_WAITING without top-level status/message/metadata
	// but include details nested under blocking_reason.details.
	if event.Status == "" {
		switch event.EventType {
		case "PIPELINE_WAITING":
			event.Status = "waiting_for_user"
		case "PIPELINE_COMPLETED":
			event.Status = "completed"
		case "PIPELINE_FAILED":
			event.Status = "failed"
		default:
			event.Status = "processing"
		}
	}

	if event.Message == "" {
		if event.Summary != "" {
			event.Message = event.Summary
		} else if event.BlockingReason != nil && event.BlockingReason.Description != "" {
			event.Message = event.BlockingReason.Description
		}
	}

	if len(event.Metadata) == 0 && event.BlockingReason != nil && len(event.BlockingReason.Details) > 0 {
		event.Metadata = event.BlockingReason.Details
	}

	if event.Progress.TotalSteps == 0 {
		event.Progress.TotalSteps = 7
	}

	// Merge metadata (preserve existing artifacts)
	metadataJSON := "{}"
	if event.Metadata != nil {
		if metaBytes, err := json.Marshal(event.Metadata); err == nil {
			metadataJSON = string(metaBytes)
		}
	}

	// Prepare blocking reason fields
	var blockingType, blockingDesc sql.NullString
	var blockingSec sql.NullInt32
	if event.BlockingReason != nil {
		blockingType = sql.NullString{String: event.BlockingReason.Type, Valid: true}
		blockingDesc = sql.NullString{String: event.BlockingReason.Description, Valid: true}
		if event.BlockingReason.EstimatedSeconds != nil {
			blockingSec = sql.NullInt32{Int32: int32(*event.BlockingReason.EstimatedSeconds), Valid: true}
		}
	}

	// Use optimistic locking with version column
	// First, read current version + existing terminal state (if any).
	var currentVersion int
	var existingStatus sql.NullString
	var existingExecID sql.NullString
	err = p.db.QueryRow(`
		SELECT COALESCE(version, 1), status, execution_id
		FROM pipeline_progress
		WHERE pipeline_id = $1
	`, event.PipelineID).Scan(&currentVersion, &existingStatus, &existingExecID)

	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// Guard: don't let late heartbeats/progress events overwrite a terminal status for the same execution.
	// This can happen when a stage heartbeat tick races with a terminal event.
	if err == nil && existingStatus.Valid && existingExecID.Valid {
		es := strings.ToLower(strings.TrimSpace(existingStatus.String))
		ee := strings.TrimSpace(existingExecID.String)
		ie := strings.TrimSpace(event.ExecutionID)
		isTerminal := es == "completed" || es == "failed" || es == "cancelled"
		incomingTerminal := strings.EqualFold(strings.TrimSpace(event.Status), "completed") ||
			strings.EqualFold(strings.TrimSpace(event.Status), "failed") ||
			strings.EqualFold(strings.TrimSpace(event.Status), "cancelled")
		if isTerminal && ee != "" && ie != "" && ee == ie && !incomingTerminal {
			// Best-effort: ignore non-terminal events for a finished execution.
			return nil
		}
	}

	newVersion := currentVersion + 1

	// Upsert with version check
	// This is a "best-effort" update - Temporal's StateUpdateActivity has priority
	query := `
		INSERT INTO pipeline_progress (
			pipeline_id, execution_id, status, current_stage, schema_version,
			stage_group, stage_state, stage_started_at, stage_last_heartbeat_at,
			stage_duration_ms, stage_attempt, stage_max_attempts, stage_summary,
			progress_percent, progress_current_step, progress_total_steps,
			blocking_reason_type, blocking_reason_description, blocking_reason_estimated_seconds,
			message, metadata, version, updated_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, NULLIF($8,'')::timestamp, NULLIF($9,'')::timestamp,
			$10, $11, $12, $13,
			$14, $15, $16, $17, $18, $19, $20, $21::jsonb, $22, NOW()
		)
		ON CONFLICT (pipeline_id) DO UPDATE SET
			execution_id = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN COALESCE(EXCLUDED.execution_id, pipeline_progress.execution_id)
				ELSE pipeline_progress.execution_id
			END,
			status = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN EXCLUDED.status
				ELSE pipeline_progress.status
			END,
			current_stage = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN EXCLUDED.current_stage
				ELSE pipeline_progress.current_stage
			END,
			schema_version = EXCLUDED.schema_version,
			stage_group = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN EXCLUDED.stage_group
				ELSE pipeline_progress.stage_group
			END,
			stage_state = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN EXCLUDED.stage_state
				ELSE pipeline_progress.stage_state
			END,
			stage_started_at = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN COALESCE(EXCLUDED.stage_started_at, pipeline_progress.stage_started_at)
				ELSE pipeline_progress.stage_started_at
			END,
			stage_last_heartbeat_at = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN COALESCE(EXCLUDED.stage_last_heartbeat_at, pipeline_progress.stage_last_heartbeat_at)
				ELSE pipeline_progress.stage_last_heartbeat_at
			END,
			stage_duration_ms = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN EXCLUDED.stage_duration_ms
				ELSE pipeline_progress.stage_duration_ms
			END,
			stage_attempt = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN EXCLUDED.stage_attempt
				ELSE pipeline_progress.stage_attempt
			END,
			stage_max_attempts = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN EXCLUDED.stage_max_attempts
				ELSE pipeline_progress.stage_max_attempts
			END,
			stage_summary = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN EXCLUDED.stage_summary
				ELSE pipeline_progress.stage_summary
			END,
			-- Monotonicity: hardcoded Percent values in different stage
			-- emitters can disagree (e.g. validator emits 75, then
			-- infra_preflight emits 74 — the progress bar visibly
			-- regresses). GREATEST clamps the persisted value so once
			-- the pipeline has crossed N%, it never goes below N% again
			-- even if a later emit forgets to bump it. Same rule applies
			-- to current_step (an inserted stage with a low CurrentStep
			-- otherwise scrolls the UI backward).
			progress_percent = CASE
				WHEN pipeline_progress.version = $22 - 1 THEN GREATEST(EXCLUDED.progress_percent, pipeline_progress.progress_percent)
				ELSE pipeline_progress.progress_percent
			END,
			-- On "completed", land the step counter on the total. The terminal
			-- event often carries total_steps = 0 (only the stage emitters set
			-- it), so the Go-side normalization can't do this one — the real
			-- total lives in the row we're updating. Without it a finished run
			-- renders "Step 2/10" beside its own "completed".
			progress_current_step = CASE
				WHEN pipeline_progress.version = $22 - 1 AND LOWER(TRIM(EXCLUDED.status)) = 'completed'
					THEN GREATEST(EXCLUDED.progress_current_step, pipeline_progress.progress_current_step,
					              EXCLUDED.progress_total_steps, pipeline_progress.progress_total_steps)
				WHEN pipeline_progress.version = $22 - 1 THEN GREATEST(EXCLUDED.progress_current_step, pipeline_progress.progress_current_step)
				ELSE pipeline_progress.progress_current_step
			END,
			progress_total_steps = CASE
				WHEN pipeline_progress.version = $22 - 1 THEN GREATEST(EXCLUDED.progress_total_steps, pipeline_progress.progress_total_steps)
				ELSE pipeline_progress.progress_total_steps
			END,
			blocking_reason_type = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN EXCLUDED.blocking_reason_type
				ELSE pipeline_progress.blocking_reason_type
			END,
			blocking_reason_description = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN EXCLUDED.blocking_reason_description
				ELSE pipeline_progress.blocking_reason_description
			END,
			blocking_reason_estimated_seconds = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN EXCLUDED.blocking_reason_estimated_seconds
				ELSE pipeline_progress.blocking_reason_estimated_seconds
			END,
			message = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN EXCLUDED.message
				ELSE pipeline_progress.message
			END,
			metadata = CASE 
				-- IMPORTANT: never clobber existing metadata (e.g. execution_plan) with empty metadata from
				-- later events (common when v2 workflow emits plan, then v1 worker emits heartbeat/progress).
				-- jsonb concat keeps existing keys unless EXCLUDED provides a value for that key.
				WHEN pipeline_progress.version = $22 - 1 THEN COALESCE(pipeline_progress.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb)
				ELSE pipeline_progress.metadata
			END,
			version = CASE 
				WHEN pipeline_progress.version = $22 - 1 THEN $22
				ELSE pipeline_progress.version
			END,
			updated_at = NOW()
	`

	// A run that reached "completed" is 100% done by definition, but the percent
	// comes from whichever stage emitter fired last and those are hardcoded — the
	// terminal event routinely carries the previous stage's number (a completed
	// run sat at 88% / "Step 2/10" while its own status said completed). GREATEST
	// above only stops the bar going backwards; it can't finish it. So finish it
	// here, and only for "completed": a failed or cancelled run genuinely stopped
	// partway and its percent is the useful fact.
	percent, currentStep, totalSteps := event.Progress.Percent, event.Progress.CurrentStep, event.Progress.TotalSteps
	if strings.EqualFold(strings.TrimSpace(event.Status), "completed") {
		percent = 100
		if totalSteps > 0 {
			currentStep = totalSteps
		}
	}

	_, err = p.db.Exec(query,
		event.PipelineID, event.ExecutionID, event.Status, event.Progress.Stage, event.SchemaVersion,
		event.StageGroup, event.State, event.StartedAt, event.LastHeartbeatAt,
		event.DurationMs, event.Attempt, event.MaxAttempts, event.Summary,
		percent, currentStep, totalSteps,
		blockingType, blockingDesc, blockingSec,
		event.Message, metadataJSON, newVersion)

	if err != nil {
		return err
	}

	// Log projections for important events only (not heartbeats)
	if event.EventType != "STAGE_PROGRESS" {
		displayPipeline := event.PipelineID
		if len(displayPipeline) > 8 {
			displayPipeline = displayPipeline[:8]
		}
		log.Printf("📊 Projected: %s - %s (stage=%s, pipeline=%s)",
			event.EventType, event.Summary, event.Stage, displayPipeline)
	}

	return nil
}

// maybePersistStreamingSyncMode updates pipelines.sync_mode/cdc_mode when the workflow indicates a streaming run.
// Some NL-created pipelines don't persist sync_mode at creation time, which prevents CDC telemetry agents from attaching.
func (p *EventProjector) maybePersistStreamingSyncMode(raw map[string]interface{}) {
	if p.db == nil || raw == nil {
		return
	}

	pipelineID, _ := raw["pipeline_id"].(string)
	if strings.TrimSpace(pipelineID) == "" {
		return
	}

	eventType, _ := raw["event_type"].(string)
	if strings.TrimSpace(eventType) == "" {
		return
	}

	// Only infer from lifecycle/progress events (avoid agent/tool noise).
	switch eventType {
	case "PIPELINE_STARTED", "STAGE_STARTED", "STAGE_COMPLETED", "STAGE_PROGRESS", "PIPELINE_COMPLETED", "PIPELINE_FAILED":
		// allow
	default:
		return
	}

	stage, _ := raw["stage"].(string)
	if stage == "" {
		if v, ok := raw["current_stage"].(string); ok && v != "" {
			stage = v
		} else if prog, ok := raw["progress"].(map[string]interface{}); ok && prog != nil {
			if v, ok := prog["stage"].(string); ok {
				stage = v
			}
		}
	}
	stage = strings.ToLower(strings.TrimSpace(stage))
	if stage != "executor" {
		return
	}

	message, _ := raw["message"].(string)
	summary, _ := raw["summary"].(string)
	hay := strings.ToLower(strings.TrimSpace(message + " " + summary))
	if !strings.Contains(hay, "streaming") {
		return
	}

	// NOTE: We only set CDC fields when they are missing or different, to avoid clobbering user_manual settings.
	if _, err := p.db.Exec(`
		UPDATE pipelines
		SET
			sync_mode = 'cdc',
			cdc_mode = COALESCE(cdc_mode, 'streaming_only'),
			sync_mode_source = COALESCE(sync_mode_source, 'pipeline_override'),
			updated_at = NOW()
		WHERE id = $1::uuid
		  AND (sync_mode IS DISTINCT FROM 'cdc' OR cdc_mode IS NULL OR sync_mode_source IS NULL)
	`, pipelineID); err != nil {
		log.Printf("⚠️ [EventProjector] failed to backfill CDC mode fields (ignored) pipeline_id=%s: %v", pipelineID, err)
	}
}

// storeRunEvent appends one event to the replayable run-event store.
//
// The bool reports whether this call actually inserted a row: false for a duplicate
// (ON CONFLICT), for an event whose pipeline no longer exists (the WHERE EXISTS
// guard), and for every early return below. Callers use it as a first-sighting
// signal — see the OnPipelineCompleted call site, which must not act twice on one
// event.
func (p *EventProjector) storeRunEvent(msg kafka.Message, raw map[string]interface{}) (bool, error) {
	if p.db == nil {
		return false, nil
	}
	if raw == nil {
		return false, nil
	}

	pipelineID, _ := raw["pipeline_id"].(string)
	if pipelineID == "" && len(msg.Key) > 0 {
		pipelineID = string(msg.Key)
	}
	if pipelineID == "" {
		return false, nil
	}

	executionID, _ := raw["execution_id"].(string)
	eventType, _ := raw["event_type"].(string)
	stageID, _ := raw["stage"].(string)
	if stageID == "" {
		// Some payloads use current_stage or progress.stage
		if v, ok := raw["current_stage"].(string); ok && v != "" {
			stageID = v
		} else if prog, ok := raw["progress"].(map[string]interface{}); ok && prog != nil {
			if v, ok := prog["stage"].(string); ok {
				stageID = v
			}
		}
	}
	stageGroup, _ := raw["stage_group"].(string)
	severity, _ := raw["severity"].(string)
	traceID, _ := raw["trace_id"].(string)

	// occurred_at: prefer payload.occurred_at, else payload.timestamp, else msg.Time, else now.
	occurredAt := time.Time{}
	if s, ok := raw["occurred_at"].(string); ok && s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			occurredAt = t
		}
	}
	if occurredAt.IsZero() {
		if s, ok := raw["timestamp"].(string); ok && s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				occurredAt = t
			}
		}
	}
	if occurredAt.IsZero() && !msg.Time.IsZero() {
		occurredAt = msg.Time
	}
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	receivedAt := time.Now().UTC()

	// event_id: prefer payload.event_id; else stable hash of raw bytes.
	eventID, _ := raw["event_id"].(string)
	if strings.TrimSpace(eventID) == "" {
		sum := sha256.Sum256(msg.Value)
		eventID = "sha256:" + hex.EncodeToString(sum[:])
		p.noteEnvelopeGap(raw, "event_id")
	}

	// seq: prefer payload.seq; else assign projector-local increment per execution_id.
	var seq int64
	if v, ok := raw["seq"]; ok && v != nil {
		switch tv := v.(type) {
		case float64:
			seq = int64(tv)
		case int64:
			seq = tv
		case int:
			seq = int64(tv)
		case string:
			if n, err := strconv.ParseInt(tv, 10, 64); err == nil {
				seq = n
			}
		}
	}
	if seq == 0 && executionID != "" {
		p.seqMu.Lock()
		last := p.lastSeq[executionID]
		next := last + 1
		p.lastSeq[executionID] = next
		p.seqMu.Unlock()
		seq = next
		p.noteEnvelopeGap(raw, "seq")
	}

	// Redact the entire event before storing.
	redactedPayload := security.RedactMap(raw)
	payloadJSON, err := json.Marshal(redactedPayload)
	if err != nil {
		// If marshal fails, fall back to empty payload.
		payloadJSON = []byte("{}")
	}

	// Best-effort insert. We cast UUID strings at the DB layer.
	// Dedupe by (pipeline_id, event_id). The WHERE EXISTS guard drops events for a
	// pipeline_id that no longer exists (deleted/orphan) instead of throwing the
	// pipeline_run_events_pipeline_id_fkey FK violation that was previously logged on
	// every poll for such events (and, with commit-after-process below, dropped them).
	// The FK stays strict; we simply don't attempt the doomed insert.
	res, err := p.db.Exec(`
		INSERT INTO pipeline_run_events (
			pipeline_id, execution_id,
			event_id, seq, event_type, stage_id, stage_group, severity, trace_id,
			occurred_at, received_at,
			payload, redacted
		)
		SELECT
			$1::uuid, NULLIF($2,'')::uuid,
			$3, NULLIF($4::bigint,0), NULLIF($5,''), NULLIF($6,''), NULLIF($7,''), NULLIF($8,''), NULLIF($9,''),
			$10::timestamptz, $11::timestamptz,
			$12::jsonb, TRUE
		WHERE EXISTS (SELECT 1 FROM pipelines WHERE id = $1::uuid)
		ON CONFLICT (pipeline_id, event_id) DO NOTHING
	`,
		pipelineID, executionID,
		eventID, seq, eventType, stageID, stageGroup, severity, traceID,
		occurredAt, receivedAt,
		string(payloadJSON),
	)
	if err != nil {
		return false, err
	}
	// Both no-op paths above (unknown pipeline, duplicate event_id) land here as 0.
	// RowsAffected can itself fail on drivers that do not track it; pgx does, but
	// treat an error as "not the first sighting" rather than as a first one — the cost
	// of a missed rebuild is a stale table until the next event, and the cost of a
	// wrong true is re-running someone's DDL on a replay.
	n, err := res.RowsAffected()
	if err != nil {
		log.Printf("Event Projector: could not read rows affected for run event %s: %v", eventID, err)
		return false, nil
	}
	return n == 1, nil
}

// noteEnvelopeGap records that a producer left an envelope field off an event and
// this projector had to invent one.
//
// The invented values are fine in isolation — the content hash is unique, the
// per-execution counter increases — which is exactly why this went unnoticed for so
// long: a row with an invented seq is indistinguishable from a row whose producer
// supplied one, and an invented seq of 3 sorts a mile below a supplied
// seq of 1.7e18 on every ORDER BY that uses it as a tiebreaker. The gap was only
// ever visible by reading both producers side by side.
//
// One line per (schema_version, event_type, field): enough to name which producer
// is missing what, without a line per event on a topic that carries every stage
// transition of every run.
func (p *EventProjector) noteEnvelopeGap(raw map[string]interface{}, field string) {
	eventType, _ := raw["event_type"].(string)
	schemaVersion := "?"
	if v, ok := raw["schema_version"]; ok && v != nil {
		schemaVersion = fmt.Sprintf("%v", v)
	}
	key := schemaVersion + "|" + eventType + "|" + field

	p.gapMu.Lock()
	if p.gapSeen == nil {
		p.gapSeen = map[string]bool{}
	}
	if p.gapSeen[key] {
		p.gapMu.Unlock()
		return
	}
	p.gapSeen[key] = true
	p.gapMu.Unlock()

	log.Printf("⚠️ [EventProjector] producer omitted %q — substituting a projector-local value "+
		"(schema_version=%s event_type=%s). Projector-assigned values are not comparable with "+
		"other producers' on this topic; logged once per event_type.",
		field, schemaVersion, eventType)
}

// upsertTableStats projects TABLE_STATS domain events into pipeline_run_table_stats.
// Counters are cumulative; we use GREATEST() to handle out-of-order delivery.
func (p *EventProjector) upsertTableStats(raw map[string]interface{}) error {
	pipelineID, _ := raw["pipeline_id"].(string)
	executionID, _ := raw["execution_id"].(string)
	if pipelineID == "" {
		return nil // skip events with no pipeline_id
	}

	meta, _ := raw["metadata"].(map[string]interface{})
	if meta == nil {
		return nil
	}

	mode, _ := meta["mode"].(string)
	if mode != "batch" && mode != "cdc" {
		return nil // skip unsupported modes
	}
	// For CDC table stats we normalize execution_id to pipeline_id so the DB can enforce
	// uniqueness and ON CONFLICT can work with a regular unique index.
	if mode == "cdc" {
		// CDC pipelines are long-running/streaming; treat pipeline_id as the stable execution key
		// so "captured" (source) and "applied" (sink) counters upsert into the same row.
		executionID = pipelineID
	}

	// The execution id the orchestrator actually minted for this run, which the line
	// above has just discarded on the CDC lane (the sink discards it the same way, in
	// parseCDCMessage). Recorded alongside — never in place of — execution_id, because
	// execution_id is the key this upsert conflicts on and rewriting it would split
	// every CDC table into two half-filled rows instead of correcting a label. It is
	// the id every sink log line carries, so without it nothing joins a CDC log back to
	// the numbers it produced. See migration 090.
	//
	// Parsed here rather than cast in SQL: this column is a label, and a malformed one
	// must never abort the transaction that carries the real counters. Unparseable or
	// absent (batch lane, cdcstats, a pre-090 sink) → NULL, which the COALESCE in the
	// ON CONFLICT clause below leaves alone.
	var orchestrationExecutionID sql.NullString
	if v, _ := meta["orchestration_execution_id"].(string); strings.TrimSpace(v) != "" {
		if parsed, err := uuid.Parse(strings.TrimSpace(v)); err == nil {
			orchestrationExecutionID = sql.NullString{String: parsed.String(), Valid: true}
		}
	}

	tableObj, _ := meta["table"].(map[string]interface{})
	if tableObj == nil {
		return nil
	}

	schemaName, _ := tableObj["schema"].(string)
	tableName, _ := tableObj["name"].(string)
	qualifiedName, _ := tableObj["qualified_name"].(string)
	if qualifiedName == "" {
		qualifiedName = tableName
	}
	if tableName == "" {
		return nil
	}

	// Where the rows LANDED, which schema_name/qualified_name above do not say: for CDC
	// they carry the SOURCE-side name (the sink derives them from the Debezium envelope),
	// so a MySQL->Postgres pipeline reports the MySQL database as its schema. The sink
	// adds these two alongside rather than correcting the originals, because
	// qualified_name is the key this upsert conflicts on and the orchestrator's cdcstats
	// agent writes the captured-side counters into the same row under the same
	// source-derived name — see migration 089.
	//
	// NULL, not "", when absent: cdcstats never emits them, an object-storage destination
	// has no schema to name, and a pre-089 sink does not know about them. All three must
	// leave whatever the sink already stored intact, which is what the COALESCE in the
	// ON CONFLICT clause below does. Writing "" here would have each cdcstats event blank
	// the destination the sink had just recorded.
	var destSchema, destQualifiedName sql.NullString
	if v, _ := tableObj["destination_schema"].(string); strings.TrimSpace(v) != "" {
		destSchema = sql.NullString{String: strings.TrimSpace(v), Valid: true}
	}
	if v, _ := tableObj["destination_qualified_name"].(string); strings.TrimSpace(v) != "" {
		destQualifiedName = sql.NullString{String: strings.TrimSpace(v), Valid: true}
	}

	status, _ := meta["status"].(string)
	if status == "" {
		status = "running"
	}

	// Parse timestamps
	startedAt, _ := meta["started_at"].(string)
	completedAt, _ := meta["completed_at"].(string)

	// Batch counters
	var insertedRows, readRows, bytesRead, filesWritten sql.NullInt64
	// Destination-committed bytes (both modes) — feeds the display column + billing ledger.
	var bytesCommitted sql.NullInt64
	if mode == "batch" {
		if counts, ok := meta["counts"].(map[string]interface{}); ok {
			if v, ok := counts["read_rows"].(float64); ok {
				readRows = sql.NullInt64{Int64: int64(v), Valid: true}
			} else if v, ok := counts["rows_read"].(float64); ok {
				readRows = sql.NullInt64{Int64: int64(v), Valid: true}
			}
			if v, ok := counts["inserted_rows"].(float64); ok {
				insertedRows = sql.NullInt64{Int64: int64(v), Valid: true}
			} else if v, ok := counts["written_rows"].(float64); ok {
				insertedRows = sql.NullInt64{Int64: int64(v), Valid: true}
			}
			if v, ok := counts["bytes_read"].(float64); ok {
				bytesRead = sql.NullInt64{Int64: int64(v), Valid: true}
			}
			if v, ok := counts["files_written"].(float64); ok {
				filesWritten = sql.NullInt64{Int64: int64(v), Valid: true}
			}
		}
	}

	// CDC counters
	var inserts, updates, deletes, totalEvents sql.NullInt64
	var lastEventTs sql.NullString
	// CDC applied counters (destination-truth)
	var appliedInserts, appliedUpdates, appliedDeletes, appliedTotalEvents sql.NullInt64
	var lastAppliedTs sql.NullString
	if mode == "cdc" {
		source, _ := meta["source"].(string)
		source = strings.ToLower(strings.TrimSpace(source))

		// 1) Captured CDC stats (source-of-truth): emitted by cdcstats agent as metadata.ops + last_event_ts.
		if ops, ok := meta["ops"].(map[string]interface{}); ok && ops != nil {
			if v, ok := ops["inserts"].(float64); ok {
				inserts = sql.NullInt64{Int64: int64(v), Valid: true}
			}
			if v, ok := ops["updates"].(float64); ok {
				updates = sql.NullInt64{Int64: int64(v), Valid: true}
			}
			if v, ok := ops["deletes"].(float64); ok {
				deletes = sql.NullInt64{Int64: int64(v), Valid: true}
			}
			if v, ok := ops["total"].(float64); ok {
				totalEvents = sql.NullInt64{Int64: int64(v), Valid: true}
			}
		}
		if ts, ok := meta["last_event_ts"].(string); ok && ts != "" {
			lastEventTs = sql.NullString{String: ts, Valid: true}
		}

		// 2) Applied CDC stats (destination-truth): emitted by kafka-mcp-sink as metadata.counts.*.
		// These are monotonic counters that only advance after a successful destination write.
		// We record them into applied_* columns so UI can show "incoming vs applied" like DMS.
		// (If both producers emit the same field names, `source` disambiguates; fallback to presence of counts keys.)
		if counts, ok := meta["counts"].(map[string]interface{}); ok && counts != nil {
			// Heuristic: treat kafka-mcp-sink as applied stats producer.
			isAppliedProducer := source == "kafka_mcp_sink"
			// Also accept applied counts when counts include CDC keys and ops is absent.
			if !isAppliedProducer {
				_, hasInserts := counts["inserts"]
				_, hasUpdates := counts["updates"]
				_, hasDeletes := counts["deletes"]
				if (hasInserts || hasUpdates || hasDeletes) && (meta["ops"] == nil) {
					isAppliedProducer = true
				}
			}
			if isAppliedProducer {
				if v, ok := counts["inserts"].(float64); ok {
					appliedInserts = sql.NullInt64{Int64: int64(v), Valid: true}
				}
				if v, ok := counts["updates"].(float64); ok {
					appliedUpdates = sql.NullInt64{Int64: int64(v), Valid: true}
				}
				if v, ok := counts["deletes"].(float64); ok {
					appliedDeletes = sql.NullInt64{Int64: int64(v), Valid: true}
				}
				if v, ok := counts["total_events"].(float64); ok {
					appliedTotalEvents = sql.NullInt64{Int64: int64(v), Valid: true}
				} else if v, ok := counts["total"].(float64); ok {
					appliedTotalEvents = sql.NullInt64{Int64: int64(v), Valid: true}
				}

				// Applied "rows added/modified" for CDC. The sink emits
				// counts.inserted_rows = inserts + updates; the batch branch reads
				// it, but the CDC branch historically did NOT, so inserted_rows
				// stayed 0 for EVERY CDC pipeline — making table-stats and the
				// LLM Diagnose evidence falsely report "streaming but 0 rows
				// inserted" even while rows were landing. Populate it so both modes
				// agree and the diagnosis reflects reality.
				if v, ok := counts["inserted_rows"].(float64); ok {
					insertedRows = sql.NullInt64{Int64: int64(v), Valid: true}
				}

				// Applied "rows read (source)" for CDC. The sink emits
				// counts.read_rows = total change events (kafka-mcp-sink
				// emitCDCTableStats, "for compatibility with batch stats"). Same
				// omission as inserted_rows above: the batch branch reads read_rows
				// but the CDC branch did NOT, so read_rows stayed NULL for EVERY CDC
				// pipeline — Usage "Rows read (source)" showed 0 while rows written
				// advanced. Populate it so both modes agree.
				if v, ok := counts["read_rows"].(float64); ok {
					readRows = sql.NullInt64{Int64: int64(v), Valid: true}
				}

				// When cdcstats agent is disabled (ENABLE_CDC_TABLE_STATS != true), no events
				// with metadata.ops are emitted, so "captured" stays 0 while "applied" advances.
				// Backfill captured from applied so the UI shows consistent numbers (captured = applied).
				if meta["ops"] == nil {
					if v, ok := counts["inserts"].(float64); ok {
						inserts = sql.NullInt64{Int64: int64(v), Valid: true}
					}
					if v, ok := counts["updates"].(float64); ok {
						updates = sql.NullInt64{Int64: int64(v), Valid: true}
					}
					if v, ok := counts["deletes"].(float64); ok {
						deletes = sql.NullInt64{Int64: int64(v), Valid: true}
					}
					if v, ok := counts["total_events"].(float64); ok {
						totalEvents = sql.NullInt64{Int64: int64(v), Valid: true}
					} else if v, ok := counts["total"].(float64); ok {
						totalEvents = sql.NullInt64{Int64: int64(v), Valid: true}
					}
				}

				// Best-effort applied timestamp: prefer event timestamp.
				if ts, ok := raw["timestamp"].(string); ok && ts != "" {
					lastAppliedTs = sql.NullString{String: ts, Valid: true}
				}
			}
		}
	}

	// Destination-committed byte volume (emitted by the kafka-mcp-sink in BOTH batch
	// and CDC TABLE_STATS as a cumulative counts.bytes_committed). Feeds the display
	// column below AND the data-transfer billing ledger. Absent on older sinks /
	// non-sink emitters → stays NULL and no ledger row is written (back-compatible).
	if counts, ok := meta["counts"].(map[string]interface{}); ok && counts != nil {
		if v, ok := counts["bytes_committed"].(float64); ok {
			bytesCommitted = sql.NullInt64{Int64: int64(v), Valid: true}
		}
	}

	// Rows the destination will never receive (parked in the sink's DLQ after
	// exhausting retries). Cumulative per table, emitted by kafka-mcp-sink in both
	// modes. This is the ONLY signal that a record was lost: a DLQ'd row is never
	// counted as captured, so read_rows/inserted_rows and the captured-vs-applied
	// pair reconcile perfectly across the gap. Absent on older sinks → stays NULL,
	// GREATEST keeps whatever is already stored.
	var dlqRows sql.NullInt64
	if counts, ok := meta["counts"].(map[string]interface{}); ok && counts != nil {
		if v, ok := counts["dlq_rows"].(float64); ok {
			dlqRows = sql.NullInt64{Int64: int64(v), Valid: true}
		}
	}
	// A table that is shedding rows is not healthy, whatever the emitter called it.
	// The sink already sends status=degraded in this case; this is the fail-safe for
	// any other emitter that reports dlq_rows without downgrading its own status.
	if dlqRows.Valid && dlqRows.Int64 > 0 && status != "failed" {
		status = "degraded"
	}

	// Upsert with GREATEST() for cumulative counters.
	// Status precedence (with recovery): failed > completed (if reconciled) > degraded > running.
	// WHERE EXISTS guard: drop TABLE_STATS for a deleted/orphan pipeline_id rather than
	// throwing the pipeline_run_table_stats_pipeline_id_fkey FK violation (same class as
	// the run-events guard above). FK stays strict.
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Committed-byte delta for the billing ledger: read the prior cumulative BEFORE
	// the GREATEST-upsert below. Same-pipeline TABLE_STATS are Kafka-keyed by
	// pipeline_id → processed in order, so this is the true previous high-water mark.
	var oldBytes int64
	_ = tx.QueryRow(
		`SELECT COALESCE(bytes_committed, 0) FROM pipeline_run_table_stats
		 WHERE pipeline_id = $1::uuid AND execution_id = $2::uuid AND qualified_name = $3`,
		pipelineID, executionID, qualifiedName,
	).Scan(&oldBytes)

	if _, err := tx.Exec(`
		INSERT INTO pipeline_run_table_stats (
			pipeline_id, execution_id, schema_name, table_name, qualified_name,
			mode, status,
			read_rows, inserted_rows, bytes_read, files_written,
			inserts, updates, deletes, total_events, last_event_ts,
			applied_inserts, applied_updates, applied_deletes, applied_total_events, last_applied_ts,
			started_at, completed_at, bytes_committed, dlq_rows,
			destination_schema, destination_qualified_name, orchestration_execution_id, updated_at
		)
		SELECT
			$1::uuid, $2::uuid, NULLIF($3,''), $4, $5,
			$6, $7,
			$8, $9, $10, $11,
			$12, $13, $14, $15, NULLIF($16,'')::timestamptz,
			$17, $18, $19, $20, NULLIF($21,'')::timestamptz,
			NULLIF($22,'')::timestamptz, NULLIF($23,'')::timestamptz, $24, COALESCE($25, 0),
			$26, $27, $28::uuid, NOW()
		WHERE EXISTS (SELECT 1 FROM pipelines WHERE id = $1::uuid)
		ON CONFLICT (pipeline_id, execution_id, qualified_name)
		DO UPDATE SET
			status = CASE
				WHEN EXCLUDED.status = 'failed' THEN 'failed'
				WHEN pipeline_run_table_stats.status = 'failed' THEN 'failed'
				-- Rows were shed to the DLQ: never 'completed', in either mode. The
				-- read==written recovery below is exactly the reconciliation a DLQ'd row
				-- slips past (it is never counted as read), so it must not run here.
				WHEN GREATEST(COALESCE(pipeline_run_table_stats.dlq_rows, 0), COALESCE(EXCLUDED.dlq_rows, 0)) > 0
				THEN 'degraded'
				-- Recovery: if we have a terminal completion marker AND read==written for batch, mark completed
				WHEN EXCLUDED.mode = 'batch'
				     AND (EXCLUDED.status = 'completed' OR EXCLUDED.completed_at IS NOT NULL OR pipeline_run_table_stats.completed_at IS NOT NULL)
				     AND GREATEST(COALESCE(pipeline_run_table_stats.read_rows, 0), COALESCE(EXCLUDED.read_rows, 0))
				         = GREATEST(COALESCE(pipeline_run_table_stats.inserted_rows, 0), COALESCE(EXCLUDED.inserted_rows, 0))
				THEN 'completed'
				WHEN EXCLUDED.status = 'degraded' THEN 'degraded'
				WHEN pipeline_run_table_stats.status = 'degraded' THEN 'degraded'
				WHEN EXCLUDED.status = 'running' THEN 'running'
				ELSE EXCLUDED.status
			END,
			read_rows = GREATEST(COALESCE(pipeline_run_table_stats.read_rows, 0), COALESCE(EXCLUDED.read_rows, 0)),
			inserted_rows = GREATEST(COALESCE(pipeline_run_table_stats.inserted_rows, 0), COALESCE(EXCLUDED.inserted_rows, 0)),
			bytes_read = GREATEST(COALESCE(pipeline_run_table_stats.bytes_read, 0), COALESCE(EXCLUDED.bytes_read, 0)),
			files_written = GREATEST(COALESCE(pipeline_run_table_stats.files_written, 0), COALESCE(EXCLUDED.files_written, 0)),
			bytes_committed = GREATEST(COALESCE(pipeline_run_table_stats.bytes_committed, 0), COALESCE(EXCLUDED.bytes_committed, 0)),
			dlq_rows = GREATEST(COALESCE(pipeline_run_table_stats.dlq_rows, 0), COALESCE(EXCLUDED.dlq_rows, 0)),
			inserts = GREATEST(COALESCE(pipeline_run_table_stats.inserts, 0), COALESCE(EXCLUDED.inserts, 0)),
			updates = GREATEST(COALESCE(pipeline_run_table_stats.updates, 0), COALESCE(EXCLUDED.updates, 0)),
			deletes = GREATEST(COALESCE(pipeline_run_table_stats.deletes, 0), COALESCE(EXCLUDED.deletes, 0)),
			total_events = GREATEST(COALESCE(pipeline_run_table_stats.total_events, 0), COALESCE(EXCLUDED.total_events, 0)),
			applied_inserts = GREATEST(COALESCE(pipeline_run_table_stats.applied_inserts, 0), COALESCE(EXCLUDED.applied_inserts, 0)),
			applied_updates = GREATEST(COALESCE(pipeline_run_table_stats.applied_updates, 0), COALESCE(EXCLUDED.applied_updates, 0)),
			applied_deletes = GREATEST(COALESCE(pipeline_run_table_stats.applied_deletes, 0), COALESCE(EXCLUDED.applied_deletes, 0)),
			applied_total_events = GREATEST(COALESCE(pipeline_run_table_stats.applied_total_events, 0), COALESCE(EXCLUDED.applied_total_events, 0)),
			last_event_ts = CASE
				WHEN EXCLUDED.last_event_ts IS NOT NULL AND 
				     (pipeline_run_table_stats.last_event_ts IS NULL OR EXCLUDED.last_event_ts > pipeline_run_table_stats.last_event_ts)
				THEN EXCLUDED.last_event_ts
				ELSE pipeline_run_table_stats.last_event_ts
			END,
			last_applied_ts = CASE
				WHEN EXCLUDED.last_applied_ts IS NOT NULL AND
				     (pipeline_run_table_stats.last_applied_ts IS NULL OR EXCLUDED.last_applied_ts > pipeline_run_table_stats.last_applied_ts)
				THEN EXCLUDED.last_applied_ts
				ELSE pipeline_run_table_stats.last_applied_ts
			END,
			completed_at = CASE WHEN EXCLUDED.completed_at IS NOT NULL THEN EXCLUDED.completed_at ELSE pipeline_run_table_stats.completed_at END,
			-- COALESCE, not assignment: two producers upsert this row and only one of
			-- them knows the destination. cdcstats (captured counters) sends NULL on
			-- every tick, so an assignment here would erase the sink's answer seconds
			-- after it arrived and leave the column permanently NULL on exactly the CDC
			-- pipelines it exists for.
			destination_schema = COALESCE(EXCLUDED.destination_schema, pipeline_run_table_stats.destination_schema),
			destination_qualified_name = COALESCE(EXCLUDED.destination_qualified_name, pipeline_run_table_stats.destination_qualified_name),
			-- COALESCE for the same reason as the two above: only the sink knows this id,
			-- and cdcstats upserts this row on every tick without it.
			orchestration_execution_id = COALESCE(EXCLUDED.orchestration_execution_id, pipeline_run_table_stats.orchestration_execution_id),
			updated_at = NOW()
	`,
		pipelineID, executionID, schemaName, tableName, qualifiedName,
		mode, status,
		readRows, insertedRows, bytesRead, filesWritten,
		inserts, updates, deletes, totalEvents, lastEventTs,
		appliedInserts, appliedUpdates, appliedDeletes, appliedTotalEvents, lastAppliedTs,
		startedAt, completedAt, bytesCommitted, dlqRows,
		destSchema, destQualifiedName, orchestrationExecutionID,
	); err != nil {
		return err
	}

	// Append the positive committed-byte delta to the append-only billing ledger and
	// accrue it to the workspace's monthly counter — atomically with the stats upsert.
	// Fail-open: an unattributable pipeline (NULL workspace_id, or a deleted pipeline)
	// selects nothing and is simply not charged.
	if bytesCommitted.Valid && bytesCommitted.Int64 > oldBytes {
		if _, err := tx.Exec(`
			WITH ledger AS (
				INSERT INTO data_transfer_usage_events
					(workspace_id, pipeline_id, execution_id, qualified_name, mode, bytes)
				SELECT p.workspace_id, $1::uuid, $2::uuid, $3, $4, $5
				  FROM pipelines p
				 WHERE p.id = $1::uuid AND p.workspace_id IS NOT NULL
				RETURNING workspace_id, bytes
			)
			SELECT charge_workspace_bytes(workspace_id, bytes) FROM ledger
		`, pipelineID, executionID, qualifiedName, mode, bytesCommitted.Int64-oldBytes); err != nil {
			return err
		}
	}

	return tx.Commit()
}
