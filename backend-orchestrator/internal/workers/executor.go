package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/executor"
	"github.com/rsync-ai/backend-orchestrator/internal/agents/healer"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	appmetrics "github.com/rsync-ai/backend-orchestrator/internal/metrics"
	"github.com/rsync-ai/shared/correlation"
)

// ExecutorWorker executes pipeline plans (GENERIC for ANY MCP connector)
// This worker wraps the real executor agent to integrate with Temporal workflow pattern
type ExecutorWorker struct {
	kafkaManager      *kafka.Manager
	db                *sql.DB
	toolsDir          string
	executorAgent     *executor.Agent // Real executor with MCP integration
	progressEmitter   *ProgressEmitter
	ctx               context.Context
	cancel            context.CancelFunc
	tracer            trace.Tracer
	correlationClient *correlation.Client
	workerID          string
	healerAgent       *healer.Agent // PHASE 3: Healer integration
}

// NewExecutorWorkerWithAgent lets the caller pass an existing executor.Agent
// so the worker shares its MCP server registry with other components (e.g.
// the orchestrator's HTTP discover-schema endpoint, the dependency liveness
// probe). Without sharing, each constructor spawns its own stdio subprocess
// per connector — which silently diverges (one path returns the right
// tables, the other returns 0). Pass agent=nil to get the legacy behavior
// of constructing a fresh agent.
func NewExecutorWorkerWithAgent(kafkaManager *kafka.Manager, db *sql.DB, toolsDir string, sharedAgent *executor.Agent) *ExecutorWorker {
	ctx, cancel := context.WithCancel(context.Background())

	// Reuse the shared agent when one is provided; otherwise construct one.
	executorAgent := sharedAgent
	if executorAgent == nil {
		executorAgent = executor.NewAgent(kafkaManager, db, toolsDir)
	}

	// PHASE 3: Initialize healer agent for recovery. toolsDir lets the healer
	// apply approved additive DDL via the destination connector's ensure_table.
	healerAgent := healer.NewAgent(kafkaManager, db, toolsDir)

	// Initialize correlation client for V2 workflows
	redisAddr := os.Getenv("REDIS_ADDRESS")
	if redisAddr == "" {
		redisAddr = os.Getenv("REDIS_ADDR")
	}
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	redisPassword := os.Getenv("REDIS_PASSWORD")
	correlationClient, err := correlation.NewClient(redisAddr, redisPassword)
	if err != nil {
		log.WithError(err).Warn("Failed to initialize correlation client for Executor worker")
	}

	return &ExecutorWorker{
		kafkaManager:      kafkaManager,
		db:                db,
		toolsDir:          toolsDir,
		executorAgent:     executorAgent,
		healerAgent:       healerAgent, // PHASE 3: Add healer
		progressEmitter:   NewProgressEmitter(kafkaManager, db),
		ctx:               ctx,
		cancel:            cancel,
		tracer:            otel.Tracer("executor-worker"),
		correlationClient: correlationClient,
		workerID:          fmt.Sprintf("executor-worker-%s", uuid.New().String()[:8]),
	}
}

func (w *ExecutorWorker) GetWorkerType() string {
	return "executor"
}

func (w *ExecutorWorker) Execute(ctx context.Context, task Task) TaskResult {
	ctx, span := w.tracer.Start(ctx, "executor.execute",
		trace.WithAttributes(
			attribute.String("task_id", task.TaskID),
			attribute.String("pipeline_id", task.PipelineID),
		),
	)
	defer span.End()

	// F-Obs-2: instrument pipeline-run duration + terminal status.
	// The metric labels are intentionally low-cardinality (status +
	// sync_mode); per-pipeline labels would explode Prometheus memory.
	runStart := time.Now()
	var resolvedSyncModeForMetric string
	var finalResult *TaskResult
	defer func() {
		if finalResult == nil {
			return
		}
		status := finalResult.Status
		if status == "" {
			status = "unknown"
		}
		appmetrics.PipelineRunsTotal.WithLabelValues(status).Inc()
		appmetrics.PipelineSyncDurationSeconds.WithLabelValues(resolvedSyncModeForMetric).Observe(time.Since(runStart).Seconds())
	}()

	log.WithFields(log.Fields{
		"task_id":     task.TaskID,
		"pipeline_id": task.PipelineID,
		"task_type":   task.TaskType,
	}).Info("⚡ Executor Worker: Processing task")

	// Convert worker Task to executor.ExecutorTask early so infra preflight
	// has access to resolved source/destination types and sync_mode.
	executorTask := w.convertToExecutorTask(task)

	// Load sync_mode from DB so infra preflight can correctly identify CDC pipelines.
	// The executor agent loads this later via loadPipelineModes, but preflight runs first.
	var resolvedSyncMode string
	if task.PipelineID != "" && w.db != nil {
		if err := w.db.QueryRowContext(ctx,
			"SELECT COALESCE(sync_mode,'batch') FROM pipelines WHERE id = $1",
			task.PipelineID).Scan(&resolvedSyncMode); err == nil && resolvedSyncMode != "" {
			if executorTask.Params == nil {
				executorTask.Params = make(map[string]interface{})
			}
			executorTask.Params["sync_mode"] = resolvedSyncMode
		}
	}
	resolvedSyncModeForMetric = resolvedSyncMode
	if resolvedSyncModeForMetric == "" {
		resolvedSyncModeForMetric = "batch"
	}

	// Delegate api_polling pipelines to the dedicated APIPollingWorker.
	// This worker manages cursor persistence and incremental export→import cycles.
	if resolvedSyncMode == "api_polling" {
		pollingWorker := NewAPIPollingWorker(w.kafkaManager, w.db, w.toolsDir, w.executorAgent)
		pollResult := pollingWorker.Execute(ctx, task)
		finalResult = &pollResult
		return pollResult
	}

	// ──────────────────────────────────────────────────────────────────────
	// INFRA PRE-FLIGHT STAGE
	// Checks and auto-starts all required infrastructure (MCP containers,
	// kafka-connect, debezium-mcp, kafka-mcp-sink, minio-mcp) before data
	// movement begins. Purely agentic — no manual user action required.
	// ──────────────────────────────────────────────────────────────────────
	preflight := newInfraPreflightStage(w.executorAgent, w.progressEmitter)
	if err := preflight.Run(ctx, task, executorTask); err != nil {
		stopHeartbeat := w.progressEmitter.StartStageHeartbeat(ctx, ProgressEvent{
			PipelineID:  task.PipelineID,
			ExecutionID: getExecutionID(task),
			Status:      "processing",
			Message:     "Infrastructure check failed",
			Summary:     "Infrastructure check failed",
			// Keep monotonic with validator's final 75% emit; the projector
			// also clamps with GREATEST but we shouldn't rely on it.
			Progress: ProgressInfo{Percent: 78, CurrentStep: 7, TotalSteps: 8, Stage: "infra_preflight"},
		}, 60*time.Second)
		stopHeartbeat()
		failResult := TaskResult{
			TaskID:     task.TaskID,
			WorkflowID: task.WorkflowID,
			StepID:     task.StepID,
			Status:     "failed",
			Error:      err.Error(),
		}
		finalResult = &failResult
		return failResult
	}

	// Emit STAGE_STARTED for executor (step 7/8 now that infra_preflight is step 6).
	//
	// Use a phase-neutral "Preparing pipeline" message. The executor agent
	// hasn't decided yet whether it needs HITL for table selection, so ANY
	// transfer-flavored wording ("Starting data transfer" / "Transferring
	// data" / even "Preparing data transfer") over-claims during the entire
	// pre-HITL window — the UI showed a data-transfer label before the user
	// had even picked tables (table selection is a sub-phase of this stage,
	// so there is a poll-lag window where the stage is running but the
	// table-selection block has not hydrated). Keep the message truthful
	// whether the next step is a table-selection HITL park or the batch
	// loop. The frontend executor fallback subtitle is kept in lockstep
	// ("Preparing…", PipelineAccordionView.tsx).
	//
	// Once the executor agent enters the actual batch loop it emits
	// row-count progress events that update the data plane; the user
	// learns about real movement from those, not from the stage
	// header.
	w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_STARTED",
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Preparing pipeline",
		Summary:     "Executing pipeline",
		Progress: ProgressInfo{
			Percent:     80,
			CurrentStep: 7,
			TotalSteps:  8,
			Stage:       "executor",
		},
	})

	// Heartbeats while execution is running (5–10s). Phrased neutrally
	// for the same reason — heartbeats fire continuously through HITL
	// and through the actual transfer, so the message has to be
	// truthful in both phases.
	stopHeartbeat := w.progressEmitter.StartStageHeartbeat(ctx, ProgressEvent{
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Executor working…",
		Summary:     "Executing pipeline",
		Progress: ProgressInfo{
			Percent:     80,
			CurrentStep: 7,
			TotalSteps:  8,
			Stage:       "executor",
		},
	}, 7*time.Second)
	defer stopHeartbeat()

	log.WithFields(log.Fields{
		"task_id":     task.TaskID,
		"pipeline_id": task.PipelineID,
	}).Debug("Calling executor agent")

	// PHASE 3: Execute with healer integration for recovery
	response := w.executeWithHealer(ctx, executorTask, task.RetryCount)

	// Convert executor response to worker TaskResult
	result := w.convertToTaskResult(task, response)

	if result.Status == "success" {
		// Best-effort bytes processed extraction (used for UI metrics).
		getInt64 := func(v interface{}) (int64, bool) {
			switch t := v.(type) {
			case int:
				return int64(t), true
			case int32:
				return int64(t), true
			case int64:
				return t, true
			case float64:
				return int64(t), true
			case float32:
				return int64(t), true
			case json.Number:
				if n, err := t.Int64(); err == nil {
					return n, true
				}
				return 0, false
			default:
				return 0, false
			}
		}

		var bytesProcessed int64 = 0
		if response.Result != nil {
			if v, ok := response.Result["bytes_processed"]; ok {
				if n, ok := getInt64(v); ok {
					bytesProcessed = n
				}
			} else if v, ok := response.Result["total_bytes"]; ok {
				if n, ok := getInt64(v); ok {
					bytesProcessed = n
				}
			}
			// Some paths nest import_result with bytes_written
			if bytesProcessed == 0 {
				if ir, ok := response.Result["import_result"].(map[string]interface{}); ok && ir != nil {
					if v, ok := ir["bytes_written"]; ok {
						if n, ok := getInt64(v); ok {
							bytesProcessed = n
						}
					}
				}
			}
		}

		log.WithFields(log.Fields{
			"task_id":        task.TaskID,
			"rows_processed": response.RowsProcessed,
		}).Info("✅ Executor Worker: Task completed successfully")

		// Stop heartbeats immediately on success to avoid late STAGE_PROGRESS overwriting newer stages.
		stopHeartbeat()

		// Emit STAGE_COMPLETED event
		completedMetadata := map[string]interface{}{
			"rows_transferred": response.RowsProcessed,
			"bytes_processed":  bytesProcessed,
			"metrics": map[string]interface{}{
				"records_read": response.RowsProcessed,
				"bytes_read":   bytesProcessed,
				"errors":       0,
				"skipped":      0,
				"dlq":          0,
			},
			"artifacts": map[string]interface{}{
				"execution_summary": map[string]interface{}{
					"version":      1,
					"generated_by": "executor",
					"timestamp":    time.Now().UTC().Format(time.RFC3339),
					"data": map[string]interface{}{
						"rows_processed":  response.RowsProcessed,
						"bytes_processed": bytesProcessed,
						"pipeline_type":   response.PipelineType,
						"kafka_topic":     response.KafkaTopic,
						"connector_name":  response.ConnectorName,
					},
				},
			},
		}
		// Surface column schema when the MCP connector returns it
		if schema := extractOutputSchema(response.Result); schema != nil {
			completedMetadata["output_schema"] = schema
		}

		// KI-BATCH-RESUME-MSG-UNDERCOUNT: response.RowsProcessed covers only THIS
		// executor dispatch. The V2 workflow re-dispatches the executor (chunked
		// continuation / large-table resume, HITL repair, activity retry), each
		// dispatch emits its own STAGE_COMPLETED, and every projection of `message`
		// is last-write-wins — so a tail dispatch that found no new rows reported
		// "Processed 0 rows" for a run that moved 42k. Report the run total instead.
		// The per-task metrics below intentionally stay per-task.
		reportedRows := runRowsProcessed(ctx, w.db, task.PipelineID, getExecutionID(task), response.PipelineType, response.RowsProcessed)

		// Status is the PIPELINE status, not the stage's — the projector writes it
		// verbatim into pipeline_progress.status (event_projector.go:320). This is a
		// mid-run stage event (step 7 of 8) and the workflow still has to run its
		// postflight, so claiming "completed" here reported the whole run finished
		// while executions.status was still 'running' with end_time NULL. Only
		// PIPELINE_COMPLETED may carry a run-terminal status; every sibling
		// STAGE_COMPLETED in this package says "processing" for the same reason.
		w.progressEmitter.EmitProgress(ctx, ProgressEvent{
			EventType:   "STAGE_COMPLETED",
			PipelineID:  task.PipelineID,
			ExecutionID: getExecutionID(task),
			Status:      "processing",
			Message:     fmt.Sprintf("Data transfer complete! Processed %d rows", reportedRows),
			Summary:     "Execution complete",
			Progress: ProgressInfo{
				Percent:     100,
				CurrentStep: 7,
				TotalSteps:  8,
				Stage:       "executor",
			},
			Metadata: completedMetadata,
		})

		// Emit DATA_PLANE_METRICS as a separate event so UI can update counters without parsing stage metadata.
		// (Projector stores this in pipeline_run_events; pipeline_progress projection will ignore it.)
		w.progressEmitter.EmitProgress(ctx, ProgressEvent{
			EventType:   "DATA_PLANE_METRICS",
			PipelineID:  task.PipelineID,
			ExecutionID: getExecutionID(task),
			Status:      "processing",
			Stage:       "executor",
			StageGroup:  "executing",
			Message:     "Execution metrics update",
			Summary:     "Data-plane metrics",
			Metadata: map[string]interface{}{
				"source":         "executor_batch",
				"metrics_schema": "v2", // Standardized schema version
				"metrics": map[string]interface{}{
					"records_read": response.RowsProcessed,
					"bytes_read":   bytesProcessed,
					"errors":       0,
					"skipped":      0,
					"dlq":          0,
				},
				// Legacy fields for backward compatibility
				"rows_processed":  response.RowsProcessed,
				"bytes_processed": bytesProcessed,
			},
		})
	} else if result.Status == "waiting_for_table_selection" {
		// HITL: User needs to select table(s) to sync
		log.WithFields(log.Fields{
			"task_id":          task.TaskID,
			"available_tables": response.Result["available_tables"],
		}).Info("⏸️  Executor Worker: Waiting for table selection")

		// Include connection IDs when available so the UI can perform post-selection
		// schema fetches (e.g. AI suggestions) without re-running earlier stages.
		sourceConnID := getStringFromMap(task.Payload, "source_connection_id")
		destConnID := getStringFromMap(task.Payload, "destination_connection_id")

		// Stop heartbeats
		stopHeartbeat()

		// Emit PIPELINE_WAITING event with table options
		details := map[string]interface{}{
			"request_type":     "table_selection",
			"available_tables": response.Result["available_tables"],
			"source_type":      response.Result["source_type"],
			"action_needed":    "table_selection",
			"button_text":      "Select Tables",
		}
		if sourceConnID != "" {
			details["source_connection_id"] = sourceConnID
		}
		if destConnID != "" {
			details["destination_connection_id"] = destConnID
		}

		w.progressEmitter.EmitProgress(ctx, ProgressEvent{
			EventType:   "PIPELINE_WAITING",
			PipelineID:  task.PipelineID,
			ExecutionID: getExecutionID(task),
			Status:      "waiting_for_user",
			Message:     response.Error, // e.g. "Found 5 tables. Please select which table(s) to sync."
			Summary:     "Table selection required",
			Progress: ProgressInfo{
				Percent:     80,
				CurrentStep: 7,
				TotalSteps:  8,
				Stage:       "executor",
			},
			BlockingReason: &BlockingReason{
				// Use a specific HITL type so API Gateway can enrich schema/ui_hints and UI can render reliably.
				Type:        "table_selection",
				Description: response.Result["reason"].(string),
				Details:     details,
			},
		})
	} else if result.Status == "running" {
		// Streaming/CDC pipelines return "running" from the executor agent.
		// This is NOT a failure: it means the long-running pipeline has been started successfully
		// (Debezium/Kafka Connect + kafka-mcp-sink worker).
		log.WithFields(log.Fields{
			"task_id":        task.TaskID,
			"pipeline_id":    task.PipelineID,
			"pipeline_type":  response.PipelineType,
			"kafka_topic":    response.KafkaTopic,
			"connector_name": response.ConnectorName,
		}).Info("✅ Executor Worker: Streaming pipeline started")

		// Stop heartbeats immediately so we don't overwrite the streaming monitor stage.
		stopHeartbeat()

		// Emit STAGE_COMPLETED to indicate executor successfully started the streaming pipeline.
		// The workflow will proceed to monitor_streaming next.
		w.progressEmitter.EmitProgress(ctx, ProgressEvent{
			EventType:   "STAGE_COMPLETED",
			PipelineID:  task.PipelineID,
			ExecutionID: getExecutionID(task),
			Status:      "running",
			Message:     "Streaming pipeline started",
			Summary:     "Execution started (streaming)",
			Progress: ProgressInfo{
				Percent:     100,
				CurrentStep: 7,
				TotalSteps:  8,
				Stage:       "executor",
			},
			Metadata: map[string]interface{}{
				"pipeline_type":  response.PipelineType,
				"kafka_topic":    response.KafkaTopic,
				"connector_name": response.ConnectorName,
			},
		})
	} else if result.Status == "needs_continuation" {
		// CHUNKED CONTINUATION (large-table resume) — NOT a failure.
		// The executor exported a bounded chunk of a large table (durably
		// checkpointing every batch) and signalled that more rows remain. The
		// correlated response carries "needs_continuation" back to the Temporal
		// workflow, which re-dispatches THIS SAME executor stage to resume from
		// the durable checkpoint (convertToTaskResult already set
		// NextAction="continue"; the healer bypass leaves the status untouched).
		// Without this branch the status fell through to the else below and
		// emitted STAGE_FAILED — flipping a healthy in-flight transfer to
		// "failed" on every chunk boundary even though data kept moving. Keep
		// the user-visible status truthful: the transfer is still processing.
		log.WithFields(log.Fields{
			"task_id":     task.TaskID,
			"pipeline_id": task.PipelineID,
		}).Info("📦 Executor Worker: chunk complete, more data remains — re-dispatching to resume from checkpoint")

		// Stop heartbeats so the explicit processing event below is the last
		// word for this dispatch; the next re-dispatch re-emits its own
		// STAGE_STARTED when the resumed chunk begins.
		stopHeartbeat()

		w.progressEmitter.EmitProgress(ctx, ProgressEvent{
			EventType:   "STAGE_STARTED",
			PipelineID:  task.PipelineID,
			ExecutionID: getExecutionID(task),
			Status:      "processing",
			Message:     "Transferring large table — chunk complete, resuming from checkpoint",
			Summary:     "Executing pipeline",
			Progress: ProgressInfo{
				Percent:     80,
				CurrentStep: 7,
				TotalSteps:  8,
				Stage:       "executor",
			},
		})
	} else {
		log.WithFields(log.Fields{
			"task_id": task.TaskID,
			"error":   result.Error,
		}).Error("❌ Executor Worker: Task failed")

		// Stop heartbeats immediately on failure to avoid late STAGE_PROGRESS overwriting newer stages.
		stopHeartbeat()

		// Emit STAGE_FAILED event
		w.progressEmitter.EmitProgress(ctx, ProgressEvent{
			EventType:   "STAGE_FAILED",
			PipelineID:  task.PipelineID,
			ExecutionID: getExecutionID(task),
			Status:      "failed",
			Message:     fmt.Sprintf("Data transfer failed: %s", result.Error),
			Progress: ProgressInfo{
				Percent:     80,
				CurrentStep: 7,
				TotalSteps:  8,
				Stage:       "executor",
			},
		})
	}

	finalResult = &result
	return result
}

// convertToExecutorTask converts worker Task to executor.ExecutorTask
func (w *ExecutorWorker) convertToExecutorTask(task Task) executor.ExecutorTask {
	execTask := executor.ExecutorTask{
		TaskID:     task.TaskID,
		PipelineID: task.PipelineID,
		Payload:    task.Payload,
		Params:     make(map[string]interface{}),
	}

	// Determine operation from task type or payload
	if op, ok := task.Payload["operation"].(string); ok {
		execTask.Operation = op
	} else {
		// Default to generic plan execution
		execTask.Operation = "execute"
	}

	// PHASE 2.4 FIX: Extract connection IDs from payload if present
	sourceConnID := getStringFromMap(task.Payload, "source_connection_id")
	destConnID := getStringFromMap(task.Payload, "destination_connection_id")
	log.WithFields(log.Fields{
		"task_id":                   task.TaskID,
		"pipeline_id":               task.PipelineID,
		"source_connection_id":      sourceConnID,
		"destination_connection_id": destConnID,
	}).Debug("convertToExecutorTask extracted connection IDs")

	if sourceConnID != "" && sourceConnID != "<nil>" {
		execTask.Params["source_connection_id"] = sourceConnID
	}
	if destConnID != "" && destConnID != "<nil>" {
		execTask.Params["destination_connection_id"] = destConnID
	}

	// Extract source/destination configs if present
	if sourceConfig, ok := task.Context["source_config"].(map[string]interface{}); ok {
		execTask.Source = &executor.ConnectorConfig{
			Type:   getStringFromMap(task.Context, "source_type"),
			Config: convertToStringMap(sourceConfig),
		}
	}

	if destConfig, ok := task.Context["destination_config"].(map[string]interface{}); ok {
		execTask.Destination = &executor.ConnectorConfig{
			Type:   getStringFromMap(task.Context, "destination_type"),
			Config: convertToStringMap(destConfig),
		}
	}

	// Copy all context as params for generic plan execution
	for k, v := range task.Context {
		execTask.Params[k] = v
	}

	// Merge payload into params
	for k, v := range task.Payload {
		// IMPORTANT: don't allow empty payload fields to wipe out authoritative values
		// propagated via task.ExecutionID / task.Context (this previously caused executor events
		// to be emitted with execution_id="", breaking per-execution monitoring like table-stats).
		if k == "execution_id" {
			if s, ok := v.(string); ok && strings.TrimSpace(s) == "" {
				continue
			}
		}
		execTask.Params[k] = v
	}

	// Re-apply execution_id last (authoritative), after merges, so it cannot be overwritten.
	if execID := strings.TrimSpace(getExecutionID(task)); execID != "" {
		execTask.Params["execution_id"] = execID
	}

	return execTask
}

// convertToTaskResult converts executor.ExecutorResponse to worker TaskResult
func (w *ExecutorWorker) convertToTaskResult(task Task, response executor.ExecutorResponse) TaskResult {
	result := TaskResult{
		TaskID:      task.TaskID,
		WorkflowID:  task.WorkflowID,
		StepID:      task.StepID,
		Status:      response.Status,
		CompletedAt: time.Now(),
		TraceID:     task.TraceID,
		Output:      response.Result,
	}

	if response.Error != "" {
		result.Error = response.Error
	}

	// Add execution metadata
	if result.Output == nil {
		result.Output = make(map[string]interface{})
	}
	result.Output["rows_processed"] = response.RowsProcessed
	result.Output["pipeline_type"] = response.PipelineType
	if response.KafkaTopic != "" {
		result.Output["kafka_topic"] = response.KafkaTopic
	}
	if response.ConnectorName != "" {
		result.Output["connector_name"] = response.ConnectorName
	}

	// Set next action
	if response.Status == "success" {
		result.NextAction = "completed"
	} else if response.Status == "running" {
		result.NextAction = "monitor_streaming"
	} else if response.Status == "needs_continuation" {
		// Chunk done, more data exists — the workflow re-dispatches to resume
		// from the durable checkpoint. Not a terminal or failed state.
		result.NextAction = "continue"
	} else if response.Status == "waiting_for_table_selection" {
		result.NextAction = "needs_input"
		result.Status = "waiting_for_table_selection"
	} else {
		result.NextAction = "failed"
	}

	return result
}

// extractOutputSchema attempts to pull column schema from an MCP connector result.
// Connectors may surface schema under various key names; we check common ones.
func extractOutputSchema(result map[string]interface{}) []map[string]interface{} {
	if result == nil {
		return nil
	}
	for _, key := range []string{"output_schema", "schema", "columns", "fields", "column_definitions"} {
		v, ok := result[key]
		if !ok {
			continue
		}
		arr, ok := v.([]interface{})
		if !ok {
			continue
		}
		schema := make([]map[string]interface{}, 0, len(arr))
		for _, item := range arr {
			if m, ok := item.(map[string]interface{}); ok {
				schema = append(schema, m)
			}
		}
		if len(schema) > 0 {
			return schema
		}
	}
	return nil
}

// runRowsProcessed returns the row count to report in a run's terminal message.
// response.RowsProcessed is scoped to ONE executor dispatch, but the V2 workflow
// re-dispatches the executor for chunked continuation / large-table resume
// (nl_pipeline_v2_workflow.go, PolicyCodeNeedsContinuation) and on activity retry,
// and each dispatch emits its own STAGE_COMPLETED whose message overwrites the
// previous one (progress_events.go `message = EXCLUDED.message`; the api-gateway
// projector and the temporal adapter's authoritative write do the same). A tail
// dispatch that finds no new rows therefore reported "Processed 0 rows" for a run
// that moved 42k rows (KI-BATCH-RESUME-MSG-UNDERCOUNT).
//
// pipeline_batch_acks is the run-cumulative destination-truth ledger the executor
// already trusts for silent-drop reconciliation (agents/executor/silent_drop_check.go
// sumBatchAcks): one row per acked batch, idempotent on the Kafka coordinates, and
// scoped by an execution_id minted fresh per run (handlers/pipelines.go RunPipeline)
// that the workflow never mutates across re-dispatches. SUM(rows_written) therefore
// spans every dispatch of THIS run and nothing else.
//
// Report the LARGER of the two, never the ledger alone: paths that do not use the
// ledger (plan-based/direct-only runs, and CDC whose ledger is empty) keep their own
// count, a lagging ledger can never shrink a number the executor already reported,
// and a single-dispatch run keeps its exact pre-existing value.
func runRowsProcessed(ctx context.Context, db *sql.DB, pipelineID, executionID, pipelineType string, taskRows int) int64 {
	rows := int64(taskRows)
	if db == nil || pipelineType != "batch" || strings.TrimSpace(pipelineID) == "" || strings.TrimSpace(executionID) == "" {
		return rows
	}
	var landed sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(rows_written), 0)
		FROM pipeline_batch_acks
		WHERE pipeline_id = $1 AND execution_id = $2
	`, pipelineID, executionID).Scan(&landed); err != nil {
		log.WithError(err).Debug("run-total row count unavailable; reporting this dispatch's count")
		return rows
	}
	if landed.Valid && landed.Int64 > rows {
		return landed.Int64
	}
	return rows
}

// Helper functions
func getStringFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func convertToStringMap(m map[string]interface{}) map[string]string {
	result := make(map[string]string)
	for k, v := range m {
		result[k] = fmt.Sprintf("%v", v)
	}
	return result
}

func (w *ExecutorWorker) Start() error {
	log.Info("🚀 Starting Executor Worker (with REAL MCP integration)")

	// Phase 3: dispatch path is selected PER-WORKFLOW via NLPipelineWorkflowV2Input.
	// ExecutorDispatch (resolved at workflow-start, never here), so the Redis poller and
	// the Kafka consumer ALWAYS run. A "temporal"-dispatched workflow invokes the native
	// ExecutorNativeActivity (executor-tasks queue) and never writes its executor task to
	// Redis or Kafka — so the legacy paths simply idle for those runs; no double-execution
	// (guaranteed by the deterministic per-workflow branch + the CorrelationID guard in
	// handleTask + Phase 2 not publishing V2 commands). Keeping both consumers up also
	// preserves the V1 (empty-CorrelationID) Kafka fallback and removes the rolling-deploy
	// strand window that a global EXECUTOR_DISPATCH gate here would create.

	// Start Redis poller for V2 workflows (correlation pattern)
	if w.correlationClient != nil {
		go w.startRedisPoller()
		log.Info("✅ ExecutorWorker: Redis poller started for V2 correlation requests")
	} else {
		log.Warn("⚠️  ExecutorWorker: Correlation client not initialized - V2 workflows will not work")
	}

	// Self-healing schema drift (P0): activate the dormant healer's schema-change
	// + approved-change consumers. RSYNC_SCHEMA_DRIFT_ENABLED gates the whole path;
	// off (default) → never called → zero behavior change. StartSchemaOnly subscribes
	// ONLY the two schema topics — NOT the executor/planner DLQs the full Start()
	// registers — so it never double-reacts to execution failures already handled by
	// executeWithHealer. Non-fatal: a consumer-start failure must not stop the worker.
	// Teardown is symmetric via Stop() → w.healerAgent.Stop().
	if os.Getenv("RSYNC_SCHEMA_DRIFT_ENABLED") == "true" && w.healerAgent != nil {
		if err := w.healerAgent.StartSchemaOnly(); err != nil {
			log.WithError(err).Warn("⚠️  ExecutorWorker: healer schema-drift consumer failed to start (non-fatal)")
		} else {
			log.Info("✅ ExecutorWorker: schema-drift consumers started (RSYNC_SCHEMA_DRIFT_ENABLED=true)")
		}
	}

	// Consume from dedicated topic (no consumer group = no rebalancing)
	return w.kafkaManager.ConsumeWithContext("agent.control.commands.executor", w.handleTask)
}

func (w *ExecutorWorker) handleTask(ctx context.Context, msg *sarama.ConsumerMessage) error {
	log.WithFields(log.Fields{
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
	}).Info("🔍 Executor Worker: Received message")

	var task Task
	if err := json.Unmarshal(msg.Value, &task); err != nil {
		log.WithError(err).Error("❌ Failed to unmarshal task")
		return err
	}

	// V2 tasks (carrying a CorrelationID) are dispatched via the Redis correlation
	// poller (startRedisPoller → processCorrelationRequest). The Kafka command path is
	// V1-only. Without this guard the same task executes twice — once here, once from
	// the Redis poller — which is the root of the hybrid-CDC double-dispatch (KI-HYBRID-1).
	if task.CorrelationID != "" {
		log.WithField("correlation_id", task.CorrelationID).
			Debug("⏭️  Skipping Kafka path for V2 task (handled by Redis correlation poller)")
		return nil
	}

	log.WithFields(log.Fields{
		"task_id":     task.TaskID,
		"task_type":   task.TaskType,
		"pipeline_id": task.PipelineID,
	}).Info("✅ Task unmarshaled successfully")

	// Filter: only process execution tasks
	if task.TaskType != "execute_pipeline" && task.TaskType != "execute_plan" {
		log.WithFields(log.Fields{
			"received_type": task.TaskType,
			"expected_type": "execute_pipeline or execute_plan",
		}).Warn("⏭️  Task type mismatch, skipping")
		return nil
	}

	// Execute the task
	result := w.Execute(ctx, task)

	// Route result to correlation store (V2) or Kafka (V1)
	if err := RouteResult(ctx, task, result, w.kafkaManager); err != nil {
		log.WithError(err).WithFields(log.Fields{
			"task_id":        task.TaskID,
			"correlation_id": task.CorrelationID,
		}).Error("Failed to route result")
		return err
	}

	log.WithFields(log.Fields{
		"task_id":     task.TaskID,
		"pipeline_id": task.PipelineID,
		"status":      result.Status,
	}).Info("📤 Sent result to orchestrator")

	return nil
}

func (w *ExecutorWorker) Stop() error {
	log.Info("🛑 Stopping Executor Worker")
	w.cancel()

	// Stop the executor agent
	if w.executorAgent != nil {
		w.executorAgent.Stop()
	}

	// Stop healer agent
	if w.healerAgent != nil {
		w.healerAgent.Stop()
	}

	return nil
}

// nonFailureStatuses are the ExecutorResponse.Status values that executeWithHealer
// must return unchanged. They are not all successes — they are all *not failures*,
// which is the distinction that matters here, because the healer's only response
// to a failure is to re-run the entire ExecuteTask.
//
// An allowlist, never a denylist: an unrecognised status is a failure, so a new
// status added upstream defaults to being healed rather than to being ignored.
var nonFailureStatuses = map[string]struct{}{
	// The run finished.
	"success": {},
	// The table hit its per-dispatch chunk budget and the durable checkpoint is
	// saved; the Temporal workflow re-dispatches until natural EOF.
	"needs_continuation": {},
	// A CDC / streaming pipeline that started correctly and is now streaming.
	// This one re-ran a full CDC start on prod (executor-9089fb7c): Debezium
	// start_sync → resolveDestinationNamespace → Starting Kafka-MCP-Sink →
	// start_sink, all a second time.
	"running": {},
	// The table-selection HITL park. The user has been asked a question and the
	// run is waiting for the answer — re-running it repeats full schema
	// discovery against the customer's source database.
	"waiting_for_table_selection": {},
	// Stopped on purpose, by a user or an operator.
	"cancelled": {},
	"stopped":   {},
	// A CDC connector reported PAUSED by Kafka Connect (executor.go:6779 in
	// agents/executor, via pipelineStatus). Somebody paused it deliberately;
	// re-running restarts it. Same wrong as re-running "stopped".
	"paused": {},
	// Everything was dispatched and only the ACK could not be confirmed inside
	// the reconcile deadline. The emitting site states the contract itself —
	// "ambiguous, never a hard fail (the sink retries the ack forever)"
	// (agents/executor/blob_lane.go:318-321). It carries Error:"", so
	// suggestRecoveryAction matched nothing, fell through to the default, and
	// returned retry_with_backoff — re-dispatching the whole transfer and, on a
	// destination with no upsert key, duplicating the customer's rows to
	// "recover" a run that had in all likelihood already succeeded.
	"unverified_completion": {},
}

// isNonFailureStatus reports whether a status means "do not consult the healer".
func isNonFailureStatus(status string) bool {
	_, ok := nonFailureStatuses[status]
	return ok
}

// PHASE 3: executeWithHealer wraps execution with healer recovery logic
func (w *ExecutorWorker) executeWithHealer(
	ctx context.Context,
	execTask executor.ExecutorTask,
	attemptCount int,
) executor.ExecutorResponse {
	// Execute the task
	response := w.executorAgent.ExecuteTask(ctx, execTask)

	// Return immediately for any status that is not a failure — see
	// isNonFailureStatus for the list and why each one is on it.
	if isNonFailureStatus(response.Status) {
		return response
	}

	// On failure, consult healer for recovery strategy
	log.WithFields(log.Fields{
		"task_id": execTask.TaskID,
		"error":   response.Error,
		"attempt": attemptCount,
	}).Warn("⚠️  Execution failed, consulting healer")

	// Create error context for healer
	errorContext := map[string]interface{}{
		"error_message": response.Error,
		"task_id":       execTask.TaskID,
		"pipeline_id":   execTask.PipelineID,
		"attempt_count": attemptCount,
		"task_payload":  execTask.Payload,
	}

	// Ask healer for recovery action (using its error analysis)
	recoveryAction := w.suggestRecoveryAction(ctx, errorContext)

	log.WithFields(log.Fields{
		"recovery_action": recoveryAction,
		"reasoning":       errorContext["reasoning"],
	}).Info("🩹 Healer suggested recovery action")

	// Apply recovery strategy
	switch recoveryAction {
	case "retry_smaller_batch":
		// Reduce batch size and retry
		if batchSize, ok := execTask.Params["batch_size"].(int); ok && batchSize > 100 {
			newBatchSize := batchSize / 2
			log.WithField("new_batch_size", newBatchSize).Info("🔄 Retrying with smaller batch")
			execTask.Params["batch_size"] = newBatchSize
			return w.executorAgent.ExecuteTask(ctx, execTask)
		}

	case "skip_and_continue":
		// Mark as partial success
		log.Warn("⏭️  Healer suggests skipping problematic data")
		response.Status = "partial_success"
		return response

	case "retry_with_backoff":
		// Simple backoff retry
		if attemptCount < 3 {
			backoffDuration := time.Duration(attemptCount*2) * time.Second
			log.WithField("backoff_seconds", backoffDuration.Seconds()).Info("⏰ Backing off before retry")
			time.Sleep(backoffDuration)
			return w.executorAgent.ExecuteTask(ctx, execTask)
		}

	case "fail":
		// Unrecoverable error
		log.Error("❌ Healer determined error is unrecoverable")
	}

	return response
}

// suggestRecoveryAction analyzes error and suggests recovery
func (w *ExecutorWorker) suggestRecoveryAction(
	ctx context.Context,
	errorContext map[string]interface{},
) string {
	errorMsg := fmt.Sprintf("%v", errorContext["error_message"])
	errorLower := strings.ToLower(errorMsg)

	// Rule-based recovery suggestions
	if strings.Contains(errorLower, "timeout") ||
		strings.Contains(errorLower, "connection reset") ||
		strings.Contains(errorLower, "temporarily unavailable") {
		errorContext["reasoning"] = "Transient network error"
		return "retry_with_backoff"
	}

	if strings.Contains(errorLower, "too large") ||
		strings.Contains(errorLower, "out of memory") ||
		strings.Contains(errorLower, "batch size") {
		errorContext["reasoning"] = "Batch size too large"
		return "retry_smaller_batch"
	}

	if strings.Contains(errorLower, "invalid data") ||
		strings.Contains(errorLower, "malformed") ||
		strings.Contains(errorLower, "constraint violation") {
		errorContext["reasoning"] = "Data quality issue"
		return "skip_and_continue"
	}

	if strings.Contains(errorLower, "permission denied") ||
		strings.Contains(errorLower, "unauthorized") ||
		strings.Contains(errorLower, "access denied") {
		errorContext["reasoning"] = "Authentication/authorization error"
		return "fail"
	}

	// Connector contract failures: caller asked for an op the connector
	// doesn't implement. Deterministic — retrying produces the same error
	// and burns ~70s in backoff before the postflight silent_drop guard
	// fires. Fail fast so the planner can surface a HITL prompt.
	if strings.Contains(errorLower, "unknown operation") ||
		strings.Contains(errorLower, "no such operation") ||
		strings.Contains(errorLower, "operation not found") {
		errorContext["reasoning"] = "Connector reports operation not implemented (non-retryable)"
		log.Warnf("⚠️  Non-retryable connector contract error: %s", errorMsg)
		return "fail"
	}

	// CDC source-config prerequisite failures (binlog_format/binlog_row_image,
	// wal_level, replication grants/privilege). These come from the executor's
	// pre-flight ValidatePrerequisites check and are server-side configuration
	// problems: retrying is futile (the server parameter cannot change between
	// attempts) and just loops in backoff. Fail fast so the user fixes the
	// source server. The message carries the exact remediation action.
	if strings.Contains(errorLower, "not configured for change data capture") ||
		strings.Contains(errorLower, "binlog_row_image") ||
		strings.Contains(errorLower, "binlog_format") ||
		strings.Contains(errorLower, "wal_level") ||
		strings.Contains(errorLower, "replication client") ||
		strings.Contains(errorLower, "replication slave") ||
		strings.Contains(errorLower, "replication privilege") {
		errorContext["reasoning"] = "CDC source server is not configured for change data capture (non-retryable; requires source server config change)"
		log.Warnf("⚠️  Non-retryable CDC source prerequisite error: %s", errorMsg)
		return "fail"
	}

	// CDC *provisioning* failures — the resources CDC needs on the source don't
	// exist and can't come into existence by waiting: a table with no PRIMARY KEY,
	// a table that isn't in the source at all, a missing publication/replication
	// slot, a capture instance that was never enabled, ARCHIVELOG/supplemental
	// logging that is off, a standalone mongod with no oplog to stream. All are
	// operator/DBA actions on the source database.
	//
	// CLAUDE.md already states the policy — "CDC provisioning errors (publication
	// missing, slot, WAL level, PK missing, table not found) → ActionEscalate" —
	// and pkg/diagnose (RuleBasedDiagnoser, diagnose.go:242) implements it for the
	// asynchronous healer. This synchronous classifier never did, so the exact same
	// error escalated on one path and fell through to the "unrecognized shape"
	// default on the other, burning two backoff retries before failing with the
	// identical message. Keep this list in step with diagnose.go's CDC-provisioning
	// block. (Connection failures during PK validation are deliberately NOT here —
	// an unreachable source really can recover on retry.)
	if strings.Contains(errorLower, "cdc requires primary key") ||
		strings.Contains(errorLower, "missing pk on") ||
		strings.Contains(errorLower, "table not found in source") ||
		strings.Contains(errorLower, "publication does not exist") ||
		strings.Contains(errorLower, "replication slot") ||
		strings.Contains(errorLower, "slot already exists") ||
		// SQL Server: the capture-instance equivalent of publication + slot.
		strings.Contains(errorLower, "sp_cdc_enable_db") ||
		strings.Contains(errorLower, "sp_cdc_enable_table") ||
		strings.Contains(errorLower, "cannot enable cdc") ||
		strings.Contains(errorLower, "capture instance") ||
		strings.Contains(errorLower, "sql server agent is not running") ||
		// Oracle: ARCHIVELOG + supplemental logging + LogMiner grants are DBA-only
		// (ARCHIVELOG even needs an instance restart).
		strings.Contains(errorLower, "archivelog") ||
		strings.Contains(errorLower, "supplemental log") ||
		strings.Contains(errorLower, "logminer") ||
		// MongoDB: change streams need a replica set / sharded cluster.
		strings.Contains(errorLower, "not a replica set") ||
		strings.Contains(errorLower, "is not a member of a replica set") {
		errorContext["reasoning"] = "CDC provisioning failure (non-retryable; requires an operator change on the source — PK, publication/slot, capture instance, supplemental logging, or replica set)"
		log.Warnf("⚠️  Non-retryable CDC provisioning error: %s", errorMsg)
		return "fail"
	}

	// Data-loss guard: the executor's offset-ignored / stuck-cursor guard refused
	// to silently truncate a table whose source connector exposed no usable keyset
	// cursor (e.g. an INVISIBLE generated primary key / GIPK is the only PK). This
	// is a deterministic data-integrity failure — retrying re-runs the same export,
	// re-trips the guard, re-stages the first page, and burns the retry budget
	// before failing anyway. Fail fast so the operator fixes the connector or
	// nominates a key column. (Mirrors the connector-contract + CDC-prereq
	// fail-fast above; aligns with the healer rule "retry loop is not recoverable".)
	if strings.Contains(errorLower, "data-loss guard") ||
		strings.Contains(errorLower, "exposed no usable keyset cursor") {
		errorContext["reasoning"] = "Data-loss guard tripped — source exposed no usable cursor (non-retryable; fix the connector or nominate a key column)"
		log.Warnf("⚠️  Non-retryable data-loss guard error: %s", errorMsg)
		return "fail"
	}

	// Partial silent drop. Both shapes that produce it are deterministic and
	// both leave the landed rows landed, so a blind re-run re-transfers every
	// table that already succeeded in order to re-hit the same cause:
	//
	//   - source permission denial (agents/executor/executor.go:5185) — the
	//     source will deny the same scopes again until a human grants them;
	//   - sink DLQ poison (agents/executor/silent_drop_check.go:88-91) — the
	//     destination rejected that batch and will reject it identically.
	//
	// Neither matched any rule above (the messages say "source denied access"
	// and "DLQ'd", not "access denied" or "permission denied"), so both landed
	// on the default branch and were retried.
	if strings.Contains(errorLower, "silent_partial_drop_detected") ||
		strings.Contains(errorLower, "dlq'd at least one batch") {
		errorContext["reasoning"] = "Partial silent drop — cause is deterministic and the landed rows stay landed (non-retryable; needs a human)"
		log.Warnf("⚠️  Non-retryable partial silent drop: %s", errorMsg)
		return "fail"
	}

	// Default: fail after attempts
	attemptCount := 0
	if val, ok := errorContext["attempt_count"].(int); ok {
		attemptCount = val
	}

	if attemptCount < 2 {
		errorContext["reasoning"] = "Unknown error, retrying with backoff"
		log.Warnf("⚠️  Unrecognized error code/shape, falling back to retry_with_backoff: %s", errorMsg)
		return "retry_with_backoff"
	}

	errorContext["reasoning"] = "Max retries exceeded"
	return "fail"
}
