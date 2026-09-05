package workers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/telemetry"
	"github.com/rsync-ai/shared/correlation"
)

// PlannerWorker creates execution plans (GENERIC for ANY pipeline pattern)
// Calls the Python LLM service to generate intelligent execution plans
type PlannerWorker struct {
	kafkaManager      *kafka.Manager
	plannerURL        string
	httpClient        *http.Client
	progressEmitter   *ProgressEmitter
	db                *sql.DB
	ctx               context.Context
	cancel            context.CancelFunc
	tracer            trace.Tracer
	correlationClient *correlation.Client
	workerID          string
	modeSelector      *ModeSelector // PHASE 3: Auto mode selection
}

func NewPlannerWorker(kafkaManager *kafka.Manager, db *sql.DB) *PlannerWorker {
	ctx, cancel := context.WithCancel(context.Background())

	// Get planner service URL from environment
	plannerURL := os.Getenv("PLANNER_URL")
	if plannerURL == "" {
		plannerURL = "http://planner:5011"
	}

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
		log.WithError(err).Warn("Failed to initialize correlation client for Planner worker")
	}

	return &PlannerWorker{
		kafkaManager:      kafkaManager,
		plannerURL:        plannerURL,
		progressEmitter:   NewProgressEmitter(kafkaManager, db),
		db:                db,
		correlationClient: correlationClient,
		workerID:          fmt.Sprintf("planner-worker-%s", uuid.New().String()[:8]),
		httpClient: &http.Client{
			Timeout: 60 * time.Second, // Planning can take longer
		},
		ctx:          ctx,
		cancel:       cancel,
		tracer:       otel.Tracer("planner-worker"),
		modeSelector: NewModeSelector(), // PHASE 3: Auto mode selection
	}
}

func (w *PlannerWorker) GetWorkerType() string {
	return "planner"
}

func (w *PlannerWorker) Execute(ctx context.Context, task Task) TaskResult {
	ctx, span := w.tracer.Start(ctx, "planner.execute",
		trace.WithAttributes(
			attribute.String("task_id", task.TaskID),
			attribute.String("pipeline_id", task.PipelineID),
		),
	)
	defer span.End()

	log.WithFields(log.Fields{
		"task_id":     task.TaskID,
		"pipeline_id": task.PipelineID,
	}).Info("📋 Planner Worker: Processing task")

	// Emit STAGE_STARTED event
	w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_STARTED",
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Creating an optimal execution plan for your data transfer",
		Summary:     "Planning pipeline",
		Progress: ProgressInfo{
			Percent:     50,
			CurrentStep: 4,
			TotalSteps:  7,
			Stage:       "planner",
		},
	})

	// Heartbeats while planning is running (5–10s)
	stopHeartbeat := w.progressEmitter.StartStageHeartbeat(ctx, ProgressEvent{
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Still planning…",
		Summary:     "Planning pipeline",
		Progress: ProgressInfo{
			Percent:     50,
			CurrentStep: 4,
			TotalSteps:  7,
			Stage:       "planner",
		},
	}, 7*time.Second)
	defer stopHeartbeat()

	// Emit telemetry for LLM planning
	w.emitTelemetry(ctx, TelemetryEvent{
		TelemetryType: TelemetryProgressUpdate,
		PipelineID:    task.PipelineID,
		Agent:         "planner",
		Timestamp:     time.Now(),
		Data: map[string]interface{}{
			"message": "Generating execution plan with LLM",
		},
		TraceID: task.TraceID,
	})

	// PHASE 3: Auto-select mode (batch vs CDC) before planning
	modeDecision, modeErr := w.selectOptimalMode(ctx, task)
	if modeErr != nil {
		log.WithError(modeErr).Warn("⚠️  Mode selection failed, proceeding with default")
	} else {
		log.WithFields(log.Fields{
			"selected_mode": modeDecision.Mode,
			"reasoning":     modeDecision.Reasoning,
			"confidence":    modeDecision.Confidence,
		}).Info("🎯 Mode auto-selected")
	}

	// Call LLM service to generate plan
	plan, err := w.generatePlan(ctx, task, modeDecision)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.WithError(err).Error("❌ Planner Worker: Failed to generate plan")
		stopHeartbeat()
		return TaskResult{
			TaskID:      task.TaskID,
			WorkflowID:  task.WorkflowID,
			StepID:      task.StepID,
			Status:      "failed",
			Error:       fmt.Sprintf("failed to generate plan: %v", err),
			CompletedAt: time.Now(),
			TraceID:     task.TraceID,
		}
	}

	span.SetStatus(codes.Ok, "Plan generated successfully")
	log.WithFields(log.Fields{
		"task_id":    task.TaskID,
		"plan_steps": len(plan["steps"].([]interface{})),
	}).Info("✅ Planner Worker: Plan generated successfully")

	// Stop heartbeats immediately on success to avoid late STAGE_PROGRESS overwriting newer stages.
	stopHeartbeat()

	// Emit STAGE_COMPLETED event
	w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_COMPLETED",
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Execution plan created successfully",
		Summary:     "Plan generated",
		Progress: ProgressInfo{
			Percent:     65,
			CurrentStep: 4,
			TotalSteps:  7,
			Stage:       "planner",
		},
		Metadata: map[string]interface{}{
			"plan_steps": len(plan),
			"artifacts": map[string]interface{}{
				"plan": map[string]interface{}{
					"version":      1,
					"generated_by": "planner",
					"timestamp":    time.Now().UTC().Format(time.RFC3339),
					"data": map[string]interface{}{
						"plan": plan,
					},
				},
			},
		},
	})

	return TaskResult{
		TaskID:     task.TaskID,
		WorkflowID: task.WorkflowID,
		StepID:     task.StepID,
		Status:     "success",
		Output: map[string]interface{}{
			"plan": plan,
		},
		NextAction:  "validate_plan",
		CompletedAt: time.Now(),
		TraceID:     task.TraceID,
	}
}

// forwardErrorContext copies the auto-replan failure snapshot from the inbound task
// context into the outbound planner context, when present. A nil error_context (the
// normal first planning pass) leaves dst untouched, so the existing path is unchanged.
func forwardErrorContext(dst, taskContext map[string]interface{}) {
	if taskContext == nil {
		return
	}
	if ec := taskContext["error_context"]; ec != nil {
		dst["error_context"] = ec
	}
}

// generatePlan calls the Python planner service to generate an execution plan
func (w *PlannerWorker) generatePlan(ctx context.Context, task Task, modeDecision *ModeDecision) (map[string]interface{}, error) {
	ctx, span := w.tracer.Start(ctx, "planner.llm_call",
		trace.WithAttributes(
			attribute.String("trace_id", task.TraceID),
		),
	)
	defer span.End()

	// PHASE 2.4 FIX: Extract connection IDs from task payload
	sourceConnectionID := getStringFromMap(task.Payload, "source_connection_id")
	destConnectionID := getStringFromMap(task.Payload, "destination_connection_id")

	log.WithFields(log.Fields{
		"source_connection_id": sourceConnectionID,
		"dest_connection_id":   destConnectionID,
	}).Info("📋 Planner received connection IDs")

	// Prepare planner request - match planner service's expected schema
	// Planner expects: { "natural_language": string, "context": {...}, "pipeline_id": string }
	var userRequest interface{}

	// Try multiple field names in order of preference
	if val := task.Payload["user_request"]; val != nil {
		userRequest = val
	} else if val := task.Payload["message"]; val != nil {
		userRequest = val
	} else if val := task.Payload["request"]; val != nil {
		// API Gateway pipeline creation uses `request` as the natural language field.
		userRequest = val
	} else if val := task.Context["user_request"]; val != nil {
		userRequest = val
	} else if val := task.Context["message"]; val != nil {
		userRequest = val
	} else if val := task.Context["request"]; val != nil {
		userRequest = val
	} else if parsedIntent := task.Context["parsed_intent"]; parsedIntent != nil {
		// Last resort: generate a natural language request from parsed intent
		if intentMap, ok := parsedIntent.(map[string]interface{}); ok {
			source := intentMap["source_category"]
			dest := intentMap["destination_category"]
			userRequest = fmt.Sprintf("migrate data from %v to %v", source, dest)
		}
	}

	// Final fallback
	if userRequest == nil {
		userRequest = "migrate data"
	}

	// If we still ended up with a generic fallback request, use the persisted pipeline request.
	// This is critical for CDC: the planner needs source/destination hints in NL to detect tools/tables.
	if w.db != nil && task.PipelineID != "" {
		urStr := strings.TrimSpace(fmt.Sprintf("%v", userRequest))
		if urStr == "" || strings.EqualFold(urStr, "migrate data") {
			var nl string
			if err := w.db.QueryRow("SELECT natural_language_request FROM pipelines WHERE id = $1", task.PipelineID).Scan(&nl); err == nil {
				if strings.TrimSpace(nl) != "" {
					userRequest = nl
				}
			}
		}
	}

	log.WithFields(log.Fields{
		"user_request":      userRequest,
		"has_parsed_intent": task.Context["parsed_intent"] != nil,
	}).Info("📋 Planner preparing request")

	// PHASE 3: Include mode decision in context
	plannerContext := map[string]interface{}{
		"parsed_intent":             task.Context["parsed_intent"],
		"source_connector":          task.Context["source_connector"],
		"destination_connector":     task.Context["destination_connector"],
		"source_connection":         task.Context["source_connection"],
		"destination_connection":    task.Context["destination_connection"],
		"source_connection_id":      sourceConnectionID,
		"destination_connection_id": destConnectionID,
		"discovery_result":          task.Context["discovery_result"],
		"schema":                    task.Context["schema"],
	}

	// Auto-replan (beta): a V2 workflow that re-invokes the planner after a deterministic
	// executor failure passes the sanitized failure as error_context. Forward it to the
	// planner service so it can produce a corrected plan instead of regenerating the failing
	// one. Absent on the normal first planning pass, so the existing path is unchanged.
	forwardErrorContext(plannerContext, task.Context)

	// Add mode decision if available
	if modeDecision != nil {
		plannerContext["selected_mode"] = string(modeDecision.Mode)
		plannerContext["mode_reasoning"] = modeDecision.Reasoning
		plannerContext["mode_parameters"] = modeDecision.Parameters
		plannerContext["mode_confidence"] = modeDecision.Confidence

		// Planner service expects `sync_mode` and uses it to decide streaming vs batch.
		// Keep it aligned with the mode decision so CDC plans actually get generated.
		if modeDecision.Mode == ModeCDC {
			plannerContext["sync_mode"] = "cdc"
			plannerContext["is_streaming"] = true
		} else {
			plannerContext["sync_mode"] = "batch"
			plannerContext["is_streaming"] = false
		}
	}

	plannerRequest := map[string]interface{}{
		"natural_language": userRequest,
		"context":          plannerContext,
		"pipeline_id":      task.PipelineID,
	}

	reqBody, err := json.Marshal(plannerRequest)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to marshal planner request: %w", err)
	}

	// Call planner service - correct endpoint is /plan (not /api/v1/plan/generate)
	endpoint := fmt.Sprintf("%s/plan", w.plannerURL)
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(reqBody))
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range telemetry.InjectTraceToHeaders(ctx) {
		req.Header.Set(k, v)
	}

	startTime := time.Now()
	resp, err := w.httpClient.Do(req)
	latencyMs := time.Since(startTime).Milliseconds()

	if err != nil {
		span.RecordError(err)
		w.emitTelemetry(ctx, TelemetryEvent{
			TelemetryType: TelemetryLLMCall,
			PipelineID:    task.PipelineID,
			Agent:         "planner",
			Timestamp:     time.Now(),
			Data: map[string]interface{}{
				"error":      err.Error(),
				"latency_ms": latencyMs,
			},
			TraceID: task.TraceID,
		})
		return nil, fmt.Errorf("planner request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		err := fmt.Errorf("planner returned status %d: %s", resp.StatusCode, string(body))
		span.RecordError(err)
		return nil, err
	}

	// Parse planner response
	var plannerResponse map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&plannerResponse); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to decode planner response: %w", err)
	}

	// Extract plan from response
	plan, ok := plannerResponse["plan"].(map[string]interface{})
	if !ok {
		err := fmt.Errorf("invalid planner response: missing 'plan' field")
		span.RecordError(err)
		return nil, err
	}

	// PHASE 2.4 FIX: Inject connection IDs into plan steps if missing
	sourceConnectionID = getStringFromMap(task.Payload, "source_connection_id")
	destConnectionID = getStringFromMap(task.Payload, "destination_connection_id")

	if sourceConnectionID != "" || destConnectionID != "" {
		if steps, ok := plan["steps"].([]interface{}); ok {
			for i, stepInterface := range steps {
				if stepMap, ok := stepInterface.(map[string]interface{}); ok {
					// If the planner returned placeholder/mock connection IDs, override them.
					existingConnID := ""
					if v, ok := stepMap["connection_id"]; ok && v != nil {
						existingConnID = fmt.Sprintf("%v", v)
					}
					needsConnID := existingConnID == "" ||
						strings.HasPrefix(existingConnID, "mock-") ||
						strings.Contains(strings.ToLower(existingConnID), "mock") ||
						existingConnID == "<nil>"

					// Check if step has connection_id (and it is not a placeholder)
					if _, hasConnID := stepMap["connection_id"]; !hasConnID || needsConnID {
						// Infer connection ID based on step type
						tool, _ := stepMap["tool"].(string)
						method, _ := stepMap["method"].(string)

						// Export/read operations use source connection
						if method == "export" || method == "read" || method == "query" || strings.Contains(method, "export") || strings.Contains(method, "read") {
							if sourceConnectionID != "" {
								stepMap["connection_id"] = sourceConnectionID
								log.WithFields(log.Fields{
									"step_id":       stepMap["id"],
									"method":        method,
									"connection_id": sourceConnectionID,
								}).Info("📌 Planner injected source connection ID into step")
							}
						}
						// Import/write operations use destination connection
						if method == "import" || method == "write" || method == "upload" || method == "import_data" || strings.Contains(method, "import") || strings.Contains(method, "upload") || strings.Contains(method, "write") {
							if destConnectionID != "" {
								stepMap["connection_id"] = destConnectionID
								log.WithFields(log.Fields{
									"step_id":       stepMap["id"],
									"method":        method,
									"connection_id": destConnectionID,
								}).Info("📌 Planner injected destination connection ID into step")
							}
						}

						// Fallback: if tool name matches connector type, try to match
						if sourceConnectionID != "" && (tool == "mysql" || tool == "postgresql") {
							if _, hasConnID := stepMap["connection_id"]; !hasConnID {
								stepMap["connection_id"] = sourceConnectionID
								log.WithField("step", i).Info("📌 Planner fallback: assigned source connection")
							}
						}
						if destConnectionID != "" && (tool == "minio" || tool == "s3" || tool == "aws-s3") {
							if _, hasConnID := stepMap["connection_id"]; !hasConnID {
								stepMap["connection_id"] = destConnectionID
								log.WithField("step", i).Info("📌 Planner fallback: assigned dest connection")
							}
						}
					}

					// Update the step in the array
					steps[i] = stepMap
				}
			}
			// Update plan with modified steps
			plan["steps"] = steps
		}
	}

	// Emit telemetry for successful LLM call
	w.emitTelemetry(ctx, TelemetryEvent{
		TelemetryType: TelemetryLLMCall,
		PipelineID:    task.PipelineID,
		Agent:         "planner",
		Timestamp:     time.Now(),
		Data: map[string]interface{}{
			"latency_ms": latencyMs,
			"plan_steps": len(plan["steps"].([]interface{})),
		},
		TraceID: task.TraceID,
	})

	span.SetStatus(codes.Ok, "Plan generated successfully")
	return plan, nil
}

// emitTelemetry sends telemetry to pipeline.agent.telemetry topic
func (w *PlannerWorker) emitTelemetry(ctx context.Context, event TelemetryEvent) {
	telemetryJSON, err := json.Marshal(event)
	if err != nil {
		log.WithError(err).Warn("Failed to marshal telemetry event")
		return
	}

	// Fire and forget - telemetry should not block task processing
	go func() {
		err := w.kafkaManager.Produce("pipeline.agent.telemetry", []byte(event.PipelineID), telemetryJSON)
		if err != nil {
			log.WithError(err).Warn("Failed to emit telemetry")
		}
	}()
}

func (w *PlannerWorker) Start() error {
	log.Info("🚀 Starting Planner Worker (with REAL LLM integration)")
	log.Infof("   Planner Service: %s", w.plannerURL)

	// Start Redis poller for V2 workflows (correlation pattern)
	if w.correlationClient != nil {
		go w.startRedisPoller()
		log.Info("✅ PlannerWorker: Redis poller started for V2 correlation requests")
	} else {
		log.Warn("⚠️  PlannerWorker: Correlation client not initialized - V2 workflows will not work")
	}

	// Consume from dedicated topic (no consumer group = no rebalancing)
	return w.kafkaManager.ConsumeWithContext("agent.control.commands.planner", w.handleTask)
}

func (w *PlannerWorker) handleTask(ctx context.Context, msg *sarama.ConsumerMessage) error {
	log.WithFields(log.Fields{
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
	}).Info("🔍 Planner Worker: Received message")

	var task Task
	if err := json.Unmarshal(msg.Value, &task); err != nil {
		log.WithError(err).Error("❌ Failed to unmarshal task")
		return err
	}

	// V2 tasks are handled by the Redis correlation poller; the Kafka path is V1-only.
	// Skipping here prevents double-execution (see KI-HYBRID-1).
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

	// Filter: only process planning tasks
	if task.TaskType != "create_plan" && task.TaskType != "plan_pipeline" {
		log.WithFields(log.Fields{
			"received_type": task.TaskType,
			"expected_type": "create_plan or plan_pipeline",
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

func (w *PlannerWorker) Stop() error {
	log.Info("🛑 Stopping Planner Worker")
	w.cancel()
	return nil
}

// PHASE 3: selectOptimalMode uses AI to choose between batch and CDC
func (w *PlannerWorker) selectOptimalMode(ctx context.Context, task Task) (*ModeDecision, error) {
	// Extract necessary information
	sourceType := GetStringFromContext(task.Context, "source_type")
	if sourceType == "" {
		if connector, ok := task.Context["source_connector"].(map[string]interface{}); ok {
			sourceType = fmt.Sprintf("%v", connector["type"])
		}
	}

	destType := GetStringFromContext(task.Context, "destination_type")
	if destType == "" {
		if connector, ok := task.Context["destination_connector"].(map[string]interface{}); ok {
			destType = fmt.Sprintf("%v", connector["type"])
		}
	}

	// Get user intent
	intent := ""
	if val, ok := task.Payload["user_request"].(string); ok {
		intent = val
	} else if val, ok := task.Payload["message"].(string); ok {
		intent = val
	} else if val, ok := task.Payload["request"].(string); ok {
		intent = val
	}

	// If the workflow didn't propagate the NL request to this stage, fall back to the
	// persisted pipeline request in the database (pipelines.natural_language_request).
	if strings.TrimSpace(intent) == "" && w.db != nil && task.PipelineID != "" {
		var nl string
		if err := w.db.QueryRow("SELECT natural_language_request FROM pipelines WHERE id = $1", task.PipelineID).Scan(&nl); err == nil {
			intent = nl
		}
	}

	// Priority 1: Check pipeline overrides first, then fall back to connection defaults
	// This allows per-pipeline customization of sync mode even when connection has a default
	sourceConnID := getStringFromMap(task.Payload, "source_connection_id")
	if sourceConnID == "" {
		sourceConnID = GetStringFromContext(task.Context, "source_connection_id")
	}

	var finalSyncMode, finalCDCMode, syncModeSource string
	var connType sql.NullString

	if sourceConnID != "" && w.db != nil {
		// First, try to get pipeline overrides
		var pipelineSyncMode, pipelineCDCMode, pipelineSyncModeSource sql.NullString
		var connSyncMode, connCDCMode sql.NullString

		// Use pipeline's persisted source_connection_id as the authoritative join key.
		// This prevents mismatches if task payload/sourceConnID diverges from the stored pipeline config.
		var pipelineSourceConnID sql.NullString
		if err := w.db.QueryRow(`
			SELECT 
				p.sync_mode, p.cdc_mode, p.sync_mode_source,
				c.sync_mode, c.cdc_mode, c.connector_type,
				p.source_connection_id
			FROM pipelines p
			JOIN connections c ON c.id = p.source_connection_id
			WHERE p.id = $1
		`, task.PipelineID).Scan(
			&pipelineSyncMode, &pipelineCDCMode, &pipelineSyncModeSource,
			&connSyncMode, &connCDCMode, &connType,
			&pipelineSourceConnID,
		); err == nil {
			if pipelineSourceConnID.Valid && sourceConnID != "" && pipelineSourceConnID.String != sourceConnID {
				log.WithFields(log.Fields{
					"pipeline_id":            task.PipelineID,
					"payload_source_conn_id": sourceConnID,
					"stored_source_conn_id":  pipelineSourceConnID.String,
				}).Warn("Source connection ID mismatch between task payload and pipelines table; using stored source_connection_id")
			}
			// Pipeline override takes precedence
			if pipelineSyncMode.Valid {
				finalSyncMode = pipelineSyncMode.String
				if pipelineCDCMode.Valid {
					finalCDCMode = pipelineCDCMode.String
				}
				if pipelineSyncModeSource.Valid {
					syncModeSource = pipelineSyncModeSource.String
				} else {
					syncModeSource = "pipeline_override"
				}
			} else if connSyncMode.Valid {
				// Fall back to connection default
				finalSyncMode = connSyncMode.String
				if connCDCMode.Valid {
					finalCDCMode = connCDCMode.String
				}
				syncModeSource = "connection_default"
			}

			if sourceType == "" && connType.Valid {
				sourceType = connType.String
			}
		} else if err := w.db.QueryRow(
			"SELECT sync_mode, cdc_mode, connector_type FROM connections WHERE id = $1",
			sourceConnID,
		).Scan(&connSyncMode, &connCDCMode, &connType); err == nil {
			// Fallback: just connection (no pipeline found or join failed)
			if connSyncMode.Valid {
				finalSyncMode = connSyncMode.String
				if connCDCMode.Valid {
					finalCDCMode = connCDCMode.String
				}
				syncModeSource = "connection_default"
			}
			if sourceType == "" && connType.Valid {
				sourceType = connType.String
			}
		}

		// Validation and auto-fix for CDC edge cases
		if strings.EqualFold(finalSyncMode, "cdc") {
			// Validation: CDC requires source to support it (assume true if connector type is recognized)
			// TODO: Add explicit supports_cdc check from connector metadata

			// "Changes only" (streaming_only) must NOT backfill historical rows, even on the
			// first run when no checkpoint exists yet. The Debezium MCP maps streaming_only ->
			// snapshot.mode="no_data" (Debezium 3.x): schema only, ZERO data, then stream — and
			// that is safe on a fresh MySQL connector (it builds schema history without a
			// snapshot). Forcing "initial" here (the old behaviour) snapshotted the whole table
			// and violated the "no historical backfill" contract, so we leave finalCDCMode as-is.

			// Default to initial if not specified
			if finalCDCMode == "" {
				finalCDCMode = "initial"
			}

			snapshotMode := "initial"
			if strings.EqualFold(finalCDCMode, "streaming_only") {
				// Debezium 3.x: "no_data" = schema-only snapshot (no historical rows), then stream.
				// (The Debezium MCP also normalises streaming_only -> no_data; keep them in sync.)
				snapshotMode = "no_data"
			}

			reasoning := "Source connection is configured for CDC"
			if syncModeSource == "pipeline_override" {
				reasoning = "Pipeline override configured for CDC"
			}

			return &ModeDecision{
				Mode:       ModeCDC,
				Reasoning:  reasoning,
				Confidence: 0.95,
				Parameters: map[string]interface{}{
					"offset_storage":   "kafka",
					"snapshot_mode":    snapshotMode,
					"sync_mode_source": syncModeSource,
					"cdc_mode":         finalCDCMode,
				},
			}, nil
		} else if strings.EqualFold(finalSyncMode, "batch") {
			return &ModeDecision{
				Mode:       ModeBatch,
				Reasoning:  "Configured for batch mode",
				Confidence: 0.95,
				Parameters: map[string]interface{}{
					"sync_mode_source": syncModeSource,
				},
			}, nil
		}
	}

	// Extract source metadata
	metadata := ExtractSourceMetadata(task)

	// Build request
	modeRequest := ModeSelectionRequest{
		Intent:              intent,
		SourceConnectorType: sourceType,
		DestConnectorType:   destType,
		SourceMetadata:      metadata,
		Context:             task.Context,
	}

	// Call mode selector
	return w.modeSelector.SelectMode(ctx, modeRequest)
}
