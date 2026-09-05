package workers

import (
	"context"
	"fmt"
	"time"

	"github.com/rsync-ai/shared/correlation"
	log "github.com/sirupsen/logrus"
)

// ==============================================================================
// REDIS POLLING FOR V2 WORKFLOWS (Correlation Pattern)
// ==============================================================================
// This file contains Redis polling logic for the Cost Estimator Worker to
// process requests from V2 Temporal workflows via the correlation store.
//
// Background: when Kafka was removed from the Temporal→agent hop, every agent
// was migrated to Redis polling EXCEPT cost_estimator, which was left consuming
// only the Kafka topic. The V2 workflow dispatches the cost-estimation request
// to Redis (no Kafka publish), so the request was never claimed and the activity
// blocked for its full 5-minute timeout on every run. This poller completes the
// migration and removes that stall.
//
// Flow:
// 1. Temporal CostEstimatorActivityV2 writes request to Redis
// 2. Worker polls Redis for pending requests
// 3. Worker processes request using existing ProcessTask logic
// 4. Worker writes response back to Redis via RouteResult
// 5. Temporal Activity receives response and continues
// ==============================================================================

// startRedisPoller polls Redis for pending cost_estimator requests from V2 workflows
func (w *CostEstimatorWorker) startRedisPoller() {
	logger := log.WithField("component", "redis_poller").WithField("worker", "cost_estimator")
	logger.Info("🔄 Starting Redis poller for cost_estimator requests (V2 workflows)")

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			logger.Info("Redis poller stopped")
			return

		case <-ticker.C:
			if err := w.pollAndProcessRequests(); err != nil {
				logger.WithError(err).Debug("Poll error")
			}
		}
	}
}

func (w *CostEstimatorWorker) pollAndProcessRequests() error {
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()

	// Poll for pending cost_estimator requests
	requests, err := w.correlationClient.PollPendingRequests(ctx, "cost_estimator")
	if err != nil {
		return fmt.Errorf("poll failed: %w", err)
	}

	if len(requests) == 0 {
		return nil // No pending requests
	}

	log.WithField("count", len(requests)).Debug("📋 Found pending cost_estimator requests")

	for _, req := range requests {
		// Try to claim this request atomically
		claimed, err := w.correlationClient.ClaimRequest(ctx, req.CorrelationID, w.workerID)
		if err != nil || !claimed {
			continue // Another worker claimed it or error
		}

		// Process the request asynchronously
		go w.processCorrelationRequest(req)
	}

	return nil
}

func (w *CostEstimatorWorker) processCorrelationRequest(req *correlation.PendingRequest) {
	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()

	logger := log.WithFields(log.Fields{
		"correlation_id": req.CorrelationID,
		"request_type":   req.RequestType,
		"worker_id":      w.workerID,
	})

	logger.Info("📥 Processing cost_estimator request from Redis (V2 workflow)")

	// Extract pipeline info from payload
	pipelineID := fmt.Sprintf("%v", req.Payload["pipeline_id"])
	executionID := fmt.Sprintf("%v", req.Payload["execution_id"])

	// The activity nests connector/plan/schema context under payload["context"];
	// ProcessTask reads assignment.Context["source_connector"] etc., so map it through.
	taskContext := map[string]interface{}{}
	if c, ok := req.Payload["context"].(map[string]interface{}); ok {
		taskContext = c
	}

	// Convert correlation request to Task format
	task := Task{
		CorrelationID: req.CorrelationID,
		TaskType:      "estimate_cost", // Match what ProcessTask expects
		Payload:       req.Payload,
		Context:       taskContext,
		PipelineID:    pipelineID,
		ExecutionID:   executionID,
		TaskID:        fmt.Sprintf("cost-estimator-%s", req.CorrelationID[:8]),
		WorkflowID:    pipelineID,
		StepID:        "cost_estimator",
	}

	// Reuse the existing cost-estimation logic.
	result, err := w.ProcessTask(ctx, task)
	if err != nil {
		logger.WithError(err).Warn("Cost estimation failed (advisory)")
		result.Status = "failed"
		result.Error = err.Error()
	}

	// Write response to Redis
	if routeErr := RouteResult(ctx, task, result, w.kafkaManager); routeErr != nil {
		logger.WithError(routeErr).Error("Failed to route response")
	}

	// Delete request from Redis after processing
	if delErr := w.correlationClient.DeleteRequest(ctx, req.CorrelationID, "cost_estimator"); delErr != nil {
		logger.WithError(delErr).Warn("Failed to delete request from Redis")
	}

	logger.Info("📤 Cost estimator request processed and response sent to Redis")
}
