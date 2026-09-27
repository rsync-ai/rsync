package workers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/rsync-ai/shared/correlation"
	log "github.com/sirupsen/logrus"
)

// ==============================================================================
// CORRELATION ROUTER (V2 Request/Reply Pattern)
// ==============================================================================
// Workers claim requests from the Redis correlation store (each worker's
// startRedisPoller) and write their result back to it; the Temporal activity
// waiting on that correlation ID picks it up. There is no Kafka reply path: a
// task without a CorrelationID has nowhere to be delivered, so RouteResult
// rejects it (ErrNoCorrelationID) rather than dropping it silently.

// ErrNoCorrelationID is returned by RouteResult for a task that carries no
// CorrelationID. Every live dispatch path (the Redis pollers) sets one.
var ErrNoCorrelationID = errors.New("task has no correlation_id: results are delivered only through the correlation store")

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

// RouteResult writes a task result to the correlation store under the task's
// CorrelationID. This is the ONLY function workers should call to return results.
func RouteResult(ctx context.Context, task Task, result TaskResult) error {
	if task.CorrelationID == "" {
		return fmt.Errorf("route result for task %q: %w", task.TaskID, ErrNoCorrelationID)
	}
	if correlationClient == nil {
		return fmt.Errorf("correlation client not initialized (V2 workflow but no Redis)")
	}

	log.WithFields(log.Fields{
		"correlation_id": task.CorrelationID,
		"task_id":        task.TaskID,
		"status":         result.Status,
	}).Debug("📝 Routing result to correlation store")

	return correlationClient.WriteResponse(ctx, correlation.WorkerResponse{
		CorrelationID: task.CorrelationID,
		Status:        result.Status,
		Output:        result.Output,
		Error:         result.Error,
	})
}
