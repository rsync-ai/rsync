package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/shared/correlation"
	log "github.com/sirupsen/logrus"
)

// ==============================================================================
// CORRELATION ROUTER (V2 Request/Reply Pattern)
// ==============================================================================
// This routes worker results to either:
// - Correlation store (V2 workflows) - Activities wait on Redis
// - Kafka topic (V1 workflows) - KafkaAdapter signals workflows
//
// Decision: If CorrelationID present → Redis, else → Kafka

var (
	correlationClient *correlation.Client
	correlationOnce   sync.Once
	correlationErr    error
)

// InitCorrelationClient initializes the global correlation client
// Call this once during orchestrator startup
func InitCorrelationClient() error {
	correlationOnce.Do(func() {
		redisAddr := os.Getenv("REDIS_ADDRESS")
		if redisAddr == "" {
			redisAddr = os.Getenv("REDIS_ADDR")
		}
		if redisAddr == "" {
			redisAddr = "redis:6379"
		}
		redisPassword := os.Getenv("REDIS_PASSWORD")

		log.WithField("redis_addr", redisAddr).Info("🔌 Initializing correlation client for V2 workflows")

		correlationClient, correlationErr = correlation.NewClient(redisAddr, redisPassword)
		if correlationErr != nil {
			log.WithError(correlationErr).Error("❌ Failed to initialize correlation client")
			return
		}

		log.Info("✅ Correlation client initialized for V2 workflows")
	})

	return correlationErr
}

// RouteResult routes task result to correlation store (V2) or Kafka (V1)
// This is the ONLY function workers should call to return results
func RouteResult(
	ctx context.Context,
	task Task,
	result TaskResult,
	kafkaManager *kafka.Manager,
) error {
	// V2 Path: Write to correlation store if correlation_id present
	if task.CorrelationID != "" {
		if correlationClient == nil {
			return fmt.Errorf("correlation client not initialized (V2 workflow but no Redis)")
		}

		log.WithFields(log.Fields{
			"correlation_id": task.CorrelationID,
			"task_id":        task.TaskID,
			"status":         result.Status,
		}).Debug("📝 Routing result to correlation store (V2)")

		return correlationClient.WriteResponse(ctx, correlation.WorkerResponse{
			CorrelationID: task.CorrelationID,
			Status:        result.Status,
			Output:        result.Output,
			Error:         result.Error,
		})
	}

	// V1 Path: Publish to Kafka topic (backward compatibility)
	log.WithFields(log.Fields{
		"task_id": task.TaskID,
		"status":  result.Status,
	}).Debug("📝 Routing result to Kafka (V1)")

	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}

	return kafkaManager.Produce(
		"agent.control.results",
		[]byte(task.PipelineID),
		resultJSON,
	)
}
