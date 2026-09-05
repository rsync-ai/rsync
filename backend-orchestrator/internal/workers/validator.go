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
	"go.opentelemetry.io/otel/trace"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/shared/correlation"
)

// ValidatorWorker is the pipeline's validation STAGE: it emits stage progress and
// hands the task on to the executor. It runs no checks of its own.
//
// This comment used to read "validates policy compliance (GENERIC policy engine)",
// and Execute emitted `"policy_approved": true` -- into the STAGE_COMPLETED artifact
// the UI stores AND into the TaskResult the correlation store forwards -- directly
// under a comment reading "AUTONOMOUS: Check policy (access control, cost limits,
// compliance, safeguards)" with no code beneath it. Nothing ever read the key, so
// nothing broke; the cost was a machine-readable compliance assertion the product
// cannot support, in a repository about to be published.
//
// It is deleted rather than satisfied. A check that returns true unconditionally is
// worse than a missing one, because it answers the question and stops anyone asking.
// The masking that DOES run is the transform engine's (`mask_pii`,
// shared/go/transforms/engine.go), configured per pipeline, not here.
type ValidatorWorker struct {
	kafkaManager      *kafka.Manager
	progressEmitter   *ProgressEmitter
	db                *sql.DB
	ctx               context.Context
	cancel            context.CancelFunc
	tracer            trace.Tracer
	correlationClient *correlation.Client
	workerID          string
}

func NewValidatorWorker(kafkaManager *kafka.Manager, db *sql.DB) *ValidatorWorker {
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
		log.WithError(err).Warn("Failed to initialize correlation client for Validator worker")
	}
	
	return &ValidatorWorker{
		kafkaManager:      kafkaManager,
		progressEmitter:   NewProgressEmitter(kafkaManager, db),
		db:                db,
		ctx:               ctx,
		cancel:            cancel,
		tracer:            otel.Tracer("validator-worker"),
		correlationClient: correlationClient,
		workerID:          fmt.Sprintf("validator-worker-%s", uuid.New().String()[:8]),
	}
}

func (w *ValidatorWorker) GetWorkerType() string {
	return "validator"
}

func (w *ValidatorWorker) Execute(ctx context.Context, task Task) TaskResult {
	log.WithField("task_id", task.TaskID).Info("✅ Validator Worker: Processing task")

	// Emit STAGE_STARTED event
	w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_STARTED",
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Validating your pipeline plan",
		Summary:     "Validating plan",
		Progress: ProgressInfo{
			Percent:     70,
			CurrentStep: 5,
			TotalSteps:  7,
			Stage:       "validator",
		},
	})

	// Heartbeats while validation is running (5–10s)
	stopHeartbeat := w.progressEmitter.StartStageHeartbeat(ctx, ProgressEvent{
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Still validating…",
		Summary:     "Validating plan",
		Progress: ProgressInfo{
			Percent:     70,
			CurrentStep: 5,
			TotalSteps:  7,
			Stage:       "validator",
		},
	}, 7*time.Second)
	defer stopHeartbeat()

	// No policy engine runs here -- see the type comment. This stage must not
	// assert an approval it never computed.

	// Stop heartbeats immediately on success to avoid late STAGE_PROGRESS overwriting newer stages.
	stopHeartbeat()

	// Emit STAGE_COMPLETED event
	w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_COMPLETED",
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Plan check complete",
		Summary:     "Plan check complete",
		Progress: ProgressInfo{
			Percent:     75,
			CurrentStep: 5,
			TotalSteps:  7,
			Stage:       "validator",
		},
		Metadata: map[string]interface{}{
			"artifacts": map[string]interface{}{
				"validation": map[string]interface{}{
					"version":      1,
					"generated_by": "validator",
					"timestamp":    time.Now().UTC().Format(time.RFC3339),
					// Deliberately carries no verdict: an artifact key is what the UI
					// and any future audit read, so an empty result is honest and a
					// hardcoded approval is not.
					"data": map[string]interface{}{},
				},
			},
		},
	})

	return TaskResult{
		TaskID:      task.TaskID,
		WorkflowID:  task.WorkflowID,
		StepID:      task.StepID,
		Status:      "success",
		Output:      map[string]interface{}{},
		NextAction:  "executor",
		CompletedAt: time.Now(),
		TraceID:     task.TraceID,
	}
}

func (w *ValidatorWorker) Start() error {
	log.Info("🚀 Starting Validator Worker")
	
	// Start Redis poller for V2 workflows (correlation pattern)
	if w.correlationClient != nil {
		go w.startRedisPoller()
		log.Info("✅ ValidatorWorker: Redis poller started for V2 correlation requests")
	} else {
		log.Warn("⚠️  ValidatorWorker: Correlation client not initialized - V2 workflows will not work")
	}
	
	// Consume from dedicated topic (no consumer group = no rebalancing)
	return w.kafkaManager.ConsumeWithContext("agent.control.commands.validator", w.handleTask)
}

func (w *ValidatorWorker) handleTask(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var task Task
	if err := json.Unmarshal(msg.Value, &task); err != nil {
		return err
	}
	// V2 tasks are handled by the Redis correlation poller; the Kafka path is V1-only.
	// Skipping here prevents double-execution (see KI-HYBRID-1).
	if task.CorrelationID != "" {
		return nil
	}
	if task.TaskType != "validate_policy" && task.TaskType != "validate_schema" {
		return nil
	}
	result := w.Execute(context.Background(), task)
	// Route result to correlation store (V2) or Kafka (V1)
	return RouteResult(context.Background(), task, result, w.kafkaManager)
}

func (w *ValidatorWorker) Stop() error {
	w.cancel()
	return nil
}
