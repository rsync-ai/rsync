package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/executor"
	"github.com/rsync-ai/backend-orchestrator/internal/idempotency"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// DiscoveryWorker discovers schema from data sources (GENERIC for ANY connector)
// Uses the executor agent's DiscoverSchema method which talks to MCP connectors
type DiscoveryWorker struct {
	kafkaManager    *kafka.Manager
	db              *sql.DB
	toolsDir        string
	executorAgent   *executor.Agent // Use executor's discovery capabilities
	progressEmitter *ProgressEmitter
	idempotencyMgr  *idempotency.Manager // NEW: Idempotency manager (Phase 2.2)
	ctx             context.Context
	cancel          context.CancelFunc
	tracer          trace.Tracer
}

func NewDiscoveryWorker(kafkaManager *kafka.Manager, db *sql.DB, toolsDir string) *DiscoveryWorker {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize executor agent for MCP access
	executorAgent := executor.NewAgent(kafkaManager, db, toolsDir)

	return &DiscoveryWorker{
		kafkaManager:    kafkaManager,
		db:              db,
		toolsDir:        toolsDir,
		executorAgent:   executorAgent,
		progressEmitter: NewProgressEmitter(kafkaManager, db),
		idempotencyMgr:  idempotency.NewManager(db), // NEW: Initialize idempotency manager
		ctx:             ctx,
		cancel:          cancel,
		tracer:          otel.Tracer("discovery-worker"),
	}
}

func (w *DiscoveryWorker) GetWorkerType() string {
	return "discovery"
}

func (w *DiscoveryWorker) Execute(ctx context.Context, task Task) TaskResult {
	ctx, span := w.tracer.Start(ctx, "discovery.execute",
		trace.WithAttributes(
			attribute.String("task_id", task.TaskID),
			attribute.String("pipeline_id", task.PipelineID),
		),
	)
	defer span.End()

	log.WithFields(log.Fields{
		"task_id":     task.TaskID,
		"pipeline_id": task.PipelineID,
	}).Info("🔎 Discovery Worker: Processing task")

	// PHASE 2.2: Check idempotency before processing
	idempotencyKey := idempotency.Key{
		PipelineID:  task.PipelineID,
		ExecutionID: task.ExecutionID,
		StepID:      task.StepID,
		ChunkID:     task.ChunkID,
	}

	completed, cachedResult, err := w.idempotencyMgr.CheckOrStart(ctx, idempotencyKey, idempotency.OpTypeDiscovery)
	if err != nil {
		log.WithError(err).Error("Failed to check idempotency")
		// Continue anyway, idempotency is a safety feature
	}

	if completed && cachedResult != nil {
		log.WithField("idempotency_key", idempotencyKey.String()).Info("⏭️  Returning cached discovery result")
		return TaskResult{
			TaskID:      task.TaskID,
			WorkflowID:  task.WorkflowID,
			StepID:      task.StepID,
			Status:      "success",
			Output:      cachedResult,
			CompletedAt: time.Now(),
			TraceID:     task.TraceID,
		}
	}

	// Emit STAGE_STARTED event
	w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_STARTED",
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Discovering tables and schemas from your data source",
		Summary:     "Discovering schema",
		Progress: ProgressInfo{
			Percent:     30,
			CurrentStep: 3,
			TotalSteps:  7,
			Stage:       "discovery",
		},
	})

	// Heartbeats while discovery is running (5–10s)
	stopHeartbeat := w.progressEmitter.StartStageHeartbeat(ctx, ProgressEvent{
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Still discovering schema…",
		Summary:     "Discovering schema",
		Progress: ProgressInfo{
			Percent:     30,
			CurrentStep: 3,
			TotalSteps:  7,
			Stage:       "discovery",
		},
	}, 7*time.Second)
	defer stopHeartbeat()

	// Extract connection info from task context
	sourceConnectorType, ok := task.Context["source_connector"].(string)
	if !ok || sourceConnectorType == "" {
		err := fmt.Errorf("missing source_connector in task context")
		span.RecordError(err)
		// PHASE 2.2: Record failure for idempotency
		_ = w.idempotencyMgr.Fail(ctx, idempotencyKey, err.Error())
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

	// Get connection config
	sourceConfig, ok := task.Context["source_config"].(map[string]interface{})
	if !ok {
		// Try from source_connection
		if sourceConn, ok := task.Context["source_connection"].(map[string]interface{}); ok {
			if config, ok := sourceConn["config"].(map[string]interface{}); ok {
				sourceConfig = config
			}
		}
	}

	if sourceConfig == nil {
		err := fmt.Errorf("missing source_config in task context")
		span.RecordError(err)
		stopHeartbeat()
		// PHASE 2.2: Record failure for idempotency
		_ = w.idempotencyMgr.Fail(ctx, idempotencyKey, err.Error())
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

	log.WithFields(log.Fields{
		"connector_type": sourceConnectorType,
		"config_keys":    len(sourceConfig),
	}).Info("🔍 Calling MCP connector for schema discovery")

	// Call the REAL MCP connector via executor agent
	tables, err := w.executorAgent.DiscoverSchema(ctx, sourceConnectorType, sourceConfig)
	if err != nil {
		span.RecordError(err)
		log.WithError(err).Error("❌ Schema discovery failed")
		stopHeartbeat()
		// PHASE 2.2: Record failure for idempotency
		_ = w.idempotencyMgr.Fail(ctx, idempotencyKey, fmt.Sprintf("schema discovery failed: %v", err))
		return TaskResult{
			TaskID:      task.TaskID,
			WorkflowID:  task.WorkflowID,
			StepID:      task.StepID,
			Status:      "failed",
			Error:       fmt.Sprintf("schema discovery failed: %v", err),
			CompletedAt: time.Now(),
			TraceID:     task.TraceID,
		}
	}

	log.WithFields(log.Fields{
		"task_id":        task.TaskID,
		"tables_found":   len(tables),
		"connector_type": sourceConnectorType,
	}).Info("✅ Discovery Worker: Schema discovered successfully")

	// Stop heartbeats immediately on success to avoid late STAGE_PROGRESS overwriting newer stages.
	stopHeartbeat()

	// Convert tables to output format
	tablesOutput := make([]map[string]interface{}, len(tables))
	for i, table := range tables {
		columnsOutput := make([]map[string]interface{}, len(table.Columns))
		for j, col := range table.Columns {
			columnsOutput[j] = map[string]interface{}{
				"name":     col.Name,
				"type":     col.Type,
				"nullable": col.Nullable,
			}
		}

		tablesOutput[i] = map[string]interface{}{
			"name":      table.Name,
			"schema":    table.Schema,
			"columns":   columnsOutput,
			"row_count": table.RowCount,
		}
	}

	// Emit STAGE_COMPLETED event
	w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_COMPLETED",
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     fmt.Sprintf("Found %d tables in your database", len(tablesOutput)),
		Summary:     "Schema discovered",
		Progress: ProgressInfo{
			Percent:     45,
			CurrentStep: 3,
			TotalSteps:  7,
			Stage:       "discovery",
		},
		Metadata: map[string]interface{}{
			"tables_found": len(tablesOutput),
			"metrics": map[string]interface{}{
				"entities_discovered": len(tablesOutput),
			},
			"artifacts": map[string]interface{}{
				"discovery": map[string]interface{}{
					"version":      1,
					"generated_by": "discovery",
					"timestamp":    time.Now().UTC().Format(time.RFC3339),
					"data": map[string]interface{}{
						"tables_found":   len(tablesOutput),
						"connector_type": sourceConnectorType,
						"tables_sample":  tablesOutput[:minInt(len(tablesOutput), 20)],
						"discovery_time": time.Now().UTC().Format(time.RFC3339),
					},
				},
			},
		},
	})

	// PHASE 2.2: Store result for idempotency
	result := map[string]interface{}{
		"schema": map[string]interface{}{
			"tables":         tablesOutput,
			"connector_type": sourceConnectorType,
			"discovery_time": time.Now().UTC().Format(time.RFC3339),
		},
		"tables": tablesOutput, // Also at root level for easy access
	}

	if err := w.idempotencyMgr.Complete(ctx, idempotencyKey, result); err != nil {
		log.WithError(err).Error("Failed to store idempotency record")
		// Non-fatal, continue
	}

	return TaskResult{
		TaskID:      task.TaskID,
		WorkflowID:  task.WorkflowID,
		StepID:      task.StepID,
		Status:      "success",
		Output:      result,
		NextAction:  "plan_pipeline",
		CompletedAt: time.Now(),
		TraceID:     task.TraceID,
	}
}

func (w *DiscoveryWorker) Start() error {
	log.Info("🚀 Starting Discovery Worker (with REAL MCP integration)")
	// Consume from dedicated topic (no consumer group = no rebalancing)
	return w.kafkaManager.ConsumeWithContext("agent.control.commands.discovery", w.handleTask)
}

func (w *DiscoveryWorker) handleTask(ctx context.Context, msg *sarama.ConsumerMessage) error {
	log.WithFields(log.Fields{
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
	}).Info("🔍 Discovery Worker: Received message")

	var task Task
	if err := json.Unmarshal(msg.Value, &task); err != nil {
		log.WithError(err).Error("❌ Failed to unmarshal task")
		return err
	}

	log.WithFields(log.Fields{
		"task_id":     task.TaskID,
		"task_type":   task.TaskType,
		"pipeline_id": task.PipelineID,
	}).Info("✅ Task unmarshaled successfully")

	// Filter: only process discovery tasks
	if task.TaskType != "discover_schema" && task.TaskType != "schema_discovery" {
		log.WithFields(log.Fields{
			"received_type": task.TaskType,
			"expected_type": "discover_schema or schema_discovery",
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (w *DiscoveryWorker) Stop() error {
	log.Info("🛑 Stopping Discovery Worker")
	w.cancel()

	// Stop the executor agent
	if w.executorAgent != nil {
		w.executorAgent.Stop()
	}

	return nil
}
