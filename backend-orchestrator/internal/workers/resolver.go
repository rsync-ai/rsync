package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/shared/correlation"
)

// ==============================================================================
// RESOLVER WORKER - PURELY AGENTIC & GENERIC
// ==============================================================================

// ResolverWorker finds or creates connections for source/destination
type ResolverWorker struct {
	kafkaManager      *kafka.Manager
	db                *sql.DB
	progressEmitter   *ProgressEmitter
	ctx               context.Context
	cancel            context.CancelFunc
	tracer            trace.Tracer
	correlationClient *correlation.Client
	workerID          string
}

// NewResolverWorker creates a new resolver worker
func NewResolverWorker(kafkaManager *kafka.Manager, db *sql.DB) *ResolverWorker {
	ctx, cancel := context.WithCancel(context.Background())

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
		log.WithError(err).Warn("Failed to initialize correlation client for Resolver worker")
	}

	return &ResolverWorker{
		kafkaManager:      kafkaManager,
		db:                db,
		progressEmitter:   NewProgressEmitter(kafkaManager, db),
		ctx:               ctx,
		cancel:            cancel,
		tracer:            otel.Tracer("resolver-worker"),
		correlationClient: correlationClient,
		workerID:          fmt.Sprintf("resolver-worker-%s", uuid.New().String()[:8]),
	}
}

// GetWorkerType returns the worker type
func (w *ResolverWorker) GetWorkerType() string {
	return "resolver"
}

// Execute processes a task and returns a result
func (w *ResolverWorker) Execute(ctx context.Context, task Task) TaskResult {
	ctx, span := w.tracer.Start(ctx, "resolver.execute",
		trace.WithAttributes(
			attribute.String("task_id", task.TaskID),
			attribute.String("pipeline_id", task.PipelineID),
		),
	)
	defer span.End()

	log.WithFields(log.Fields{
		"task_id":     task.TaskID,
		"pipeline_id": task.PipelineID,
	}).Info("🔍 Resolver Worker: Processing task")

	// Emit STAGE_STARTED event
	w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_STARTED",
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Finding your data connections",
		Summary:     "Resolving connections",
		Progress: ProgressInfo{
			Percent:     20,
			CurrentStep: 2,
			TotalSteps:  7,
			Stage:       "resolver",
		},
	})

	// Heartbeats while resolving is running (5–10s)
	stopHeartbeat := w.progressEmitter.StartStageHeartbeat(ctx, ProgressEvent{
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Still resolving connections…",
		Summary:     "Resolving connections",
		Progress: ProgressInfo{
			Percent:     20,
			CurrentStep: 2,
			TotalSteps:  7,
			Stage:       "resolver",
		},
	}, 7*time.Second)
	defer stopHeartbeat()

	// Extract intent from task context
	intentData, ok := task.Context["intent"].(map[string]interface{})
	if !ok {
		err := fmt.Errorf("missing intent in task context")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		stopHeartbeat()
		return TaskResult{
			TaskID:      task.TaskID,
			WorkflowID:  task.WorkflowID,
			StepID:      task.StepID,
			Status:      "failed",
			Error:       err.Error(),
			CompletedAt: time.Now(),
			TraceID:     task.TraceID,
		}
	}

	sourceCategory, _ := intentData["source_category"].(string)
	destCategory, _ := intentData["destination_category"].(string)

	// Emit telemetry
	w.emitTelemetry(ctx, TelemetryEvent{
		TelemetryType: TelemetryProgressUpdate,
		PipelineID:    task.PipelineID,
		Agent:         "resolver",
		Timestamp:     time.Now(),
		Data: map[string]interface{}{
			"message":         "Resolving connections",
			"source_category": sourceCategory,
			"dest_category":   destCategory,
		},
		TraceID: task.TraceID,
	})

	// AUTONOMOUS DECISION: Find or create connections
	sourceConn, err := w.resolveConnection(ctx, sourceCategory, task.PipelineID, task.TraceID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		// Check if needs user input
		if err.Error() == "needs_connection" {
			stopHeartbeat()
			return TaskResult{
				TaskID:     task.TaskID,
				WorkflowID: task.WorkflowID,
				StepID:     task.StepID,
				Status:     "needs_input",
				Output: map[string]interface{}{
					"reason":         "missing_connection",
					"connector_type": sourceCategory,
					"direction":      "source",
				},
				CompletedAt: time.Now(),
				TraceID:     task.TraceID,
			}
		}

		stopHeartbeat()
		return TaskResult{
			TaskID:      task.TaskID,
			WorkflowID:  task.WorkflowID,
			StepID:      task.StepID,
			Status:      "failed",
			Error:       fmt.Sprintf("failed to resolve source connection: %v", err),
			CompletedAt: time.Now(),
			TraceID:     task.TraceID,
		}
	}

	destConn, err := w.resolveConnection(ctx, destCategory, task.PipelineID, task.TraceID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		if err.Error() == "needs_connection" {
			stopHeartbeat()
			return TaskResult{
				TaskID:     task.TaskID,
				WorkflowID: task.WorkflowID,
				StepID:     task.StepID,
				Status:     "needs_input",
				Output: map[string]interface{}{
					"reason":         "missing_connection",
					"connector_type": destCategory,
					"direction":      "destination",
				},
				CompletedAt: time.Now(),
				TraceID:     task.TraceID,
			}
		}

		stopHeartbeat()
		return TaskResult{
			TaskID:      task.TaskID,
			WorkflowID:  task.WorkflowID,
			StepID:      task.StepID,
			Status:      "failed",
			Error:       fmt.Sprintf("failed to resolve destination connection: %v", err),
			CompletedAt: time.Now(),
			TraceID:     task.TraceID,
		}
	}

	// Return structured result
	span.SetStatus(codes.Ok, "Connections resolved")
	log.WithFields(log.Fields{
		"task_id":          task.TaskID,
		"source_conn":      sourceConn,
		"destination_conn": destConn,
	}).Info("✅ Resolver Worker: Successfully resolved connections")

	// Stop heartbeats immediately on success to avoid late STAGE_PROGRESS overwriting newer stages.
	stopHeartbeat()

	// Emit STAGE_COMPLETED event
	w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_COMPLETED",
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     fmt.Sprintf("Connected to %s and %s", sourceCategory, destCategory),
		Progress: ProgressInfo{
			Percent:     25,
			CurrentStep: 2,
			TotalSteps:  7,
			Stage:       "resolver",
		},
		Metadata: map[string]interface{}{
			"source_type":      sourceCategory,
			"destination_type": destCategory,
		},
	})

	return TaskResult{
		TaskID:     task.TaskID,
		WorkflowID: task.WorkflowID,
		StepID:     task.StepID,
		Status:     "success",
		Output: map[string]interface{}{
			"source_connection_id":       sourceConn,
			"destination_connection_id":  destConn,
			"source_connector_type":      sourceCategory,
			"destination_connector_type": destCategory,
		},
		NextAction:  "discovery",
		CompletedAt: time.Now(),
		TraceID:     task.TraceID,
	}
}

// resolveConnection finds or creates a connection (GENERIC for ANY connector)
func (w *ResolverWorker) resolveConnection(ctx context.Context, connectorType, pipelineID, traceID string) (string, error) {
	ctx, span := w.tracer.Start(ctx, "resolver.resolve_connection",
		trace.WithAttributes(
			attribute.String("connector_type", connectorType),
		),
	)
	defer span.End()

	// Check if connection exists in database (GENERIC query)
	var connectionID string
	err := w.db.QueryRowContext(ctx, `
		SELECT id FROM connections 
		WHERE connector_type = $1 
		AND status = 'active' 
		LIMIT 1
	`, connectorType).Scan(&connectionID)

	if err == sql.ErrNoRows {
		// No connection found - needs user input
		return "", fmt.Errorf("needs_connection")
	}

	if err != nil {
		span.RecordError(err)
		return "", fmt.Errorf("database error: %w", err)
	}

	span.SetStatus(codes.Ok, "Connection found")
	return connectionID, nil
}

// emitTelemetry sends telemetry (optional)
func (w *ResolverWorker) emitTelemetry(ctx context.Context, event TelemetryEvent) {
	telemetryJSON, err := json.Marshal(event)
	if err != nil {
		log.WithError(err).Warn("Failed to marshal telemetry event")
		return
	}

	go func() {
		err := w.kafkaManager.Produce("pipeline.agent.telemetry", []byte(event.PipelineID), telemetryJSON)
		if err != nil {
			log.WithError(err).Warn("Failed to emit telemetry")
		}
	}()
}

// Start begins consuming tasks
func (w *ResolverWorker) Start() error {
	log.Info("🚀 Starting Resolver Worker")

	// Use shared consumer group for load balancing
	// Consume from dedicated topic (no consumer group = no rebalancing)
	err := w.kafkaManager.ConsumeWithContext("agent.control.commands.resolver", w.handleTask)
	if err != nil {
		return fmt.Errorf("failed to consume agent.control.commands: %w", err)
	}

	log.Info("✅ Resolver Worker started")
	return nil
}

// handleTask is the Kafka message handler
func (w *ResolverWorker) handleTask(ctx context.Context, msg *sarama.ConsumerMessage) error {
	// ctx is already provided by the Kafka consumer

	var task Task
	if err := json.Unmarshal(msg.Value, &task); err != nil {
		log.WithError(err).Error("Failed to unmarshal task")
		return err
	}

	// Filter: Only process resolver tasks
	if task.TaskType != "resolve_connections" {
		return nil
	}

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
	}).Info("📤 Resolver Worker: Sent result")

	return nil
}

// Stop gracefully shuts down the worker
func (w *ResolverWorker) Stop() error {
	log.Info("🛑 Stopping Resolver Worker")
	w.cancel()
	return nil
}
