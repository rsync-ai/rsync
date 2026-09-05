package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/shared/correlation"
)

// ==============================================================================
// COST ESTIMATOR WORKER
// ==============================================================================
// This worker estimates:
// 1. Data volume (rows, bytes)
// 2. Cloud costs (compute, storage, network)
// 3. Execution time
// 4. Resource requirements
//
// Enterprise requirement: Production systems cannot execute blind.
// ==============================================================================

// CostEstimatorWorker estimates costs for pipeline execution
type CostEstimatorWorker struct {
	kafkaManager *kafka.Manager
	ctx          context.Context
	cancel       context.CancelFunc
	tracer       trace.Tracer

	// V2 (Temporal correlation) dispatch. The workflow writes cost_estimator
	// requests to Redis (no Kafka publish), so without this poller the request
	// is never consumed and the activity blocks for its full 5-minute timeout.
	correlationClient *correlation.Client
	workerID          string
}

// NewCostEstimatorWorker creates a new cost estimator worker
func NewCostEstimatorWorker(kafkaManager *kafka.Manager) *CostEstimatorWorker {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize correlation client for V2 workflows (mirrors the other agents).
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
		log.WithError(err).Warn("Failed to initialize correlation client for Cost Estimator worker")
	}

	return &CostEstimatorWorker{
		kafkaManager:      kafkaManager,
		ctx:               ctx,
		cancel:            cancel,
		tracer:            otel.Tracer("cost-estimator-worker"),
		correlationClient: correlationClient,
		workerID:          fmt.Sprintf("cost-estimator-worker-%s", uuid.New().String()[:8]),
	}
}

// AgentName returns the worker name
func (w *CostEstimatorWorker) AgentName() string {
	return "cost_estimator"
}

// Start begins consuming tasks
func (w *CostEstimatorWorker) Start() error {
	log.Info("🚀 Starting Cost Estimator Worker")

	// Start Redis poller for V2 workflows (correlation pattern). Without this the
	// V2 cost-estimation request sits unclaimed in Redis and every pipeline stalls
	// ~5 minutes between planning and validation waiting for the activity timeout.
	if w.correlationClient != nil {
		go w.startRedisPoller()
		log.Info("✅ CostEstimatorWorker: Redis poller started for V2 correlation requests")
	} else {
		log.Warn("⚠️  CostEstimatorWorker: Correlation client not initialized - V2 workflows will not work")
	}

	// Consume from dedicated topic (no consumer group = no rebalancing)
	err := w.kafkaManager.ConsumeWithContext("agent.control.commands.cost_estimator", w.handleTask)
	if err != nil {
		return fmt.Errorf("failed to consume agent.control.commands: %w", err)
	}

	log.Info("✅ Cost Estimator Worker started")
	return nil
}

// Stop stops the worker
func (w *CostEstimatorWorker) Stop() {
	log.Info("🛑 Stopping Cost Estimator Worker")
	w.cancel()
	// Kafka consumer will be stopped via context cancellation
}

// handleTask processes incoming task assignments
func (w *CostEstimatorWorker) handleTask(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var assignment Task
	if err := json.Unmarshal(msg.Value, &assignment); err != nil {
		log.WithError(err).Error("Failed to unmarshal task assignment")
		return err
	}

	// Filter: only process cost estimation tasks
	if assignment.TaskType != "estimate_cost" {
		return nil // Not for us
	}

	log.WithFields(log.Fields{
		"pipeline_id": assignment.PipelineID,
		"task_id":     assignment.TaskID,
	}).Info("📊 Processing cost estimation task")

	result, err := w.ProcessTask(ctx, assignment)
	if err != nil {
		log.WithError(err).Error("Cost estimation failed")
		result.Status = "failed"
		result.Error = err.Error()
	}

	// Route result to correlation store (V2) or Kafka (V1)
	return RouteResult(ctx, assignment, result, w.kafkaManager)
}

// ProcessTask implements the cost estimation logic
func (w *CostEstimatorWorker) ProcessTask(ctx context.Context, assignment Task) (TaskResult, error) {
	ctx, span := w.tracer.Start(ctx, "cost_estimator.process_task")
	defer span.End()

	// Extract pipeline context
	sourceConnector := assignment.Context["source_connector"]
	destinationConnector := assignment.Context["destination_connector"]
	schemaInfo := assignment.Context["schema"]

	// Estimate data volume
	estimatedRows, estimatedBytes := w.estimateDataVolume(schemaInfo)

	// Estimate cloud costs
	estimatedCost := w.estimateCloudCosts(destinationConnector, estimatedRows, estimatedBytes)

	// Estimate execution time
	estimatedDuration := w.estimateExecutionTime(sourceConnector, destinationConnector, estimatedRows)

	// Determine risk level
	riskLevel := w.assessRiskLevel(destinationConnector, estimatedCost, estimatedRows)

	log.WithFields(log.Fields{
		"pipeline_id":        assignment.PipelineID,
		"estimated_rows":     estimatedRows,
		"estimated_bytes":    estimatedBytes,
		"estimated_cost":     estimatedCost,
		"estimated_duration": estimatedDuration,
		"risk_level":         riskLevel,
	}).Info("💰 Cost estimation completed")

	return TaskResult{
		TaskID:     assignment.TaskID,
		WorkflowID: assignment.WorkflowID,
		StepID:     assignment.StepID,
		Status:     "success",
		Output: map[string]interface{}{
			"estimated_rows":     estimatedRows,
			"estimated_bytes":    estimatedBytes,
			"estimated_cost":     estimatedCost,
			"estimated_duration": estimatedDuration,
			"risk_level":         riskLevel,
			"breakdown": map[string]interface{}{
				"compute_cost":  estimatedCost * 0.4, // 40% compute
				"storage_cost":  estimatedCost * 0.3, // 30% storage
				"network_cost":  estimatedCost * 0.2, // 20% network
				"overhead_cost": estimatedCost * 0.1, // 10% overhead
			},
		},
		NextAction:  "check_policy", // Next: policy validation
		CompletedAt: time.Now(),
		TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
	}, nil
}

// estimateDataVolume estimates rows and bytes to be processed
func (w *CostEstimatorWorker) estimateDataVolume(schemaInfo interface{}) (rows int64, bytes int64) {
	// In production, this would:
	// 1. Query source connector for table/collection stats
	// 2. Use EXPLAIN/ANALYZE for SQL sources
	// 3. Check incremental settings (full vs incremental load)
	// 4. Apply sampling if available

	// Simplified estimation for demo
	schema, ok := schemaInfo.(map[string]interface{})
	if !ok {
		// Default estimates if schema info not available
		return 100000, 100 * 1024 * 1024 // 100K rows, 100MB
	}

	// Estimate based on table count and avg row size
	tableCount := len(schema)
	if tableCount == 0 {
		tableCount = 1
	}

	estimatedRowsPerTable := int64(50000) // Default 50K rows per table
	estimatedBytesPerRow := int64(1024)   // Default 1KB per row

	rows = estimatedRowsPerTable * int64(tableCount)
	bytes = rows * estimatedBytesPerRow

	return rows, bytes
}

// estimateCloudCosts estimates cloud provider costs
func (w *CostEstimatorWorker) estimateCloudCosts(destinationConnector interface{}, rows, bytes int64) float64 {
	// In production, this would use cloud pricing APIs:
	// - AWS Cost Explorer API
	// - Google Cloud Billing API
	// - Azure Cost Management API

	// Simplified estimation
	dest, ok := destinationConnector.(map[string]interface{})
	if !ok {
		return 0.50 // Default $0.50
	}

	destType, _ := dest["type"].(string)

	// Cost per million rows (rough estimates)
	costPerMillionRows := map[string]float64{
		"aws_s3":     0.10, // S3 is cheap
		"bigquery":   0.50, // BigQuery moderate
		"snowflake":  1.00, // Snowflake higher
		"redshift":   0.80, // Redshift moderate-high
		"databricks": 1.20, // Databricks higher
		"postgresql": 0.20, // Self-hosted cheap
		"mysql":      0.20, // Self-hosted cheap
	}

	costRate, exists := costPerMillionRows[destType]
	if !exists {
		costRate = 0.50 // Default rate
	}

	// Calculate cost
	millionRows := float64(rows) / 1000000.0
	estimatedCost := millionRows * costRate

	// Add data transfer cost (rough estimate: $0.01 per GB)
	gigabytes := float64(bytes) / (1024 * 1024 * 1024)
	transferCost := gigabytes * 0.01

	return estimatedCost + transferCost
}

// estimateExecutionTime estimates how long the pipeline will take
func (w *CostEstimatorWorker) estimateExecutionTime(sourceConnector, destinationConnector interface{}, rows int64) string {
	// Rough estimate: 10,000 rows per second for typical pipelines
	rowsPerSecond := 10000.0
	estimatedSeconds := float64(rows) / rowsPerSecond

	// Add overhead (connection, transformation, etc.)
	estimatedSeconds += 30 // 30 seconds baseline overhead

	// Format as human-readable duration
	duration := time.Duration(estimatedSeconds) * time.Second

	if duration < time.Minute {
		return fmt.Sprintf("%ds", int(duration.Seconds()))
	} else if duration < time.Hour {
		return fmt.Sprintf("%dm %ds", int(duration.Minutes()), int(duration.Seconds())%60)
	} else {
		return fmt.Sprintf("%dh %dm", int(duration.Hours()), int(duration.Minutes())%60)
	}
}

// assessRiskLevel determines the risk level of the pipeline
func (w *CostEstimatorWorker) assessRiskLevel(destinationConnector interface{}, estimatedCost float64, estimatedRows int64) string {
	dest, ok := destinationConnector.(map[string]interface{})
	if !ok {
		return "medium"
	}

	environment, _ := dest["environment"].(string)

	// High risk criteria
	if environment == "production" {
		return "high" // Production always high risk
	}

	if estimatedCost > 10.0 {
		return "high" // Expensive operations are high risk
	}

	if estimatedRows > 10000000 { // 10M rows
		return "high" // Large data volumes are high risk
	}

	// Medium risk criteria
	if estimatedCost > 1.0 || estimatedRows > 1000000 {
		return "medium"
	}

	// Low risk
	return "low"
}
