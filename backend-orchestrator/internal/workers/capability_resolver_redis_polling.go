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
// This file contains Redis polling logic for Capability_resolver Worker to process
// requests from V2 Temporal workflows via correlation store.
//
// Flow:
// 1. Temporal Activity writes request to Redis
// 2. Worker polls Redis for pending requests
// 3. Worker processes request using existing logic
// 4. Worker writes response back to Redis
// 5. Temporal Activity receives response and continues
// ==============================================================================

// startRedisPollerCapability_resolver polls Redis for pending connector_resolver requests from V2 workflows
func (w *CapabilityResolverWorker) startRedisPollerCapability_resolver() {
	logger := log.WithField("component", "redis_poller").WithField("worker", "connector_resolver")
	logger.Info("🔄 Starting Redis poller for connector_resolver requests (V2 workflows)")

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

func (w *CapabilityResolverWorker) pollAndProcessRequests() error {
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()

	// Poll for pending connector_resolver requests
	requests, err := w.correlationClient.PollPendingRequests(ctx, "connector_resolver")
	if err != nil {
		return fmt.Errorf("poll failed: %w", err)
	}

	if len(requests) == 0 {
		return nil // No pending requests
	}

	log.WithField("count", len(requests)).Debug("📋 Found pending connector_resolver requests")

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

func (w *CapabilityResolverWorker) processCorrelationRequest(req *correlation.PendingRequest) {
	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()

	logger := log.WithFields(log.Fields{
		"correlation_id": req.CorrelationID,
		"request_type":   req.RequestType,
		"worker_id":      w.workerID,
	})

	logger.Info("📥 Processing connector_resolver request from Redis (V2 workflow)")

	// Extract pipeline info from payload
	pipelineID := fmt.Sprintf("%v", req.Payload["pipeline_id"])
	executionID := fmt.Sprintf("%v", req.Payload["execution_id"])
	userID := fmt.Sprintf("%v", req.Payload["user_id"])

	// Convert correlation request to Task format with full context
	task := Task{
		CorrelationID: req.CorrelationID,
		TaskType:      "resolve_capability", // Must match what ProcessTask expects
		Payload:       req.Payload,          // Pass full payload
		Context:       req.Payload,          // Also set as context
		PipelineID:    pipelineID,
		ExecutionID:   executionID,
		UserID:        userID,
		TaskID:        fmt.Sprintf("connector_resolver-%s", req.CorrelationID[:8]),
		WorkflowID:    pipelineID,
		StepID:        "connector_resolver",
	}

	// PHASE 2.4 FIX: Use main ProcessTask method which has connection ID resolution logic
	result, err := w.ProcessTask(ctx, task)

	if err != nil {
		logger.WithError(err).Error("❌ Task processing failed")

		// Write error response to Redis
		errorResult := TaskResult{
			TaskID:      task.TaskID,
			WorkflowID:  task.WorkflowID,
			StepID:      task.StepID,
			Status:      "failure",
			Error:       err.Error(),
			Output:      map[string]interface{}{"error": err.Error()},
			CompletedAt: time.Now(),
			TraceID:     task.TraceID,
		}

		if routeErr := RouteResult(ctx, task, errorResult, w.kafkaManager); routeErr != nil {
			logger.WithError(routeErr).Error("Failed to route error response")
		}
	} else {
		logger.Info("✅ Task processing succeeded")

		// Write success response to Redis
		if routeErr := RouteResult(ctx, task, result, w.kafkaManager); routeErr != nil {
			logger.WithError(routeErr).Error("Failed to route success response")
		}
	}

	// Delete request from Redis after processing
	if delErr := w.correlationClient.DeleteRequest(ctx, req.CorrelationID, "connector_resolver"); delErr != nil {
		logger.WithError(delErr).Warn("Failed to delete request from Redis")
	}

	logger.Info("📤 Capability_resolver request processed and response sent to Redis")
}
