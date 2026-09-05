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
// This file contains Redis polling logic for Intent Worker to process
// requests from V2 Temporal workflows via correlation store.
//
// Flow:
// 1. Temporal Activity writes request to Redis
// 2. Worker polls Redis for pending requests
// 3. Worker processes request using existing logic
// 4. Worker writes response back to Redis
// 5. Temporal Activity receives response and continues
// ==============================================================================

// startRedisPoller polls Redis for pending intent requests from V2 workflows
func (w *IntentWorker) startRedisPoller() {
	logger := log.WithField("component", "redis_poller").WithField("worker", "intent")
	logger.Info("🔄 Starting Redis poller for intent requests (V2 workflows)")

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

func (w *IntentWorker) pollAndProcessRequests() error {
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()

	// Poll for pending intent requests
	requests, err := w.correlationClient.PollPendingRequests(ctx, "intent")
	if err != nil {
		return fmt.Errorf("poll failed: %w", err)
	}

	if len(requests) == 0 {
		return nil // No pending requests
	}

	log.WithField("count", len(requests)).Debug("📋 Found pending intent requests")

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

func (w *IntentWorker) processCorrelationRequest(req *correlation.PendingRequest) {
	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()

	logger := log.WithFields(log.Fields{
		"correlation_id": req.CorrelationID,
		"request_type":   req.RequestType,
		"worker_id":      w.workerID,
	})

	logger.Info("📥 Processing intent request from Redis (V2 workflow)")

	// Extract pipeline info from payload
	pipelineID := fmt.Sprintf("%v", req.Payload["pipeline_id"])
	executionID := fmt.Sprintf("%v", req.Payload["execution_id"])
	userID := fmt.Sprintf("%v", req.Payload["user_id"])

	// Convert correlation request to Task format with full payload
	task := Task{
		CorrelationID: req.CorrelationID,
		TaskType:      "intent",
		Payload:       req.Payload,
		Context:       req.Payload,
		PipelineID:    pipelineID,
		ExecutionID:   executionID,
		UserID:        userID,
		TaskID:        fmt.Sprintf("intent-%s", req.CorrelationID[:8]),
		WorkflowID:    pipelineID,
		StepID:        "intent",
	}

	// PHASE 2.4 FIX: Use main Execute() method from intent.go
	result := w.Execute(ctx, task)

	logger.Info("✅ Task processing completed")

	// Write response to Redis
	if routeErr := RouteResult(ctx, task, result, w.kafkaManager); routeErr != nil {
		logger.WithError(routeErr).Error("Failed to route response")
	}

	// Delete request from Redis after processing
	if delErr := w.correlationClient.DeleteRequest(ctx, req.CorrelationID, "intent"); delErr != nil {
		logger.WithError(delErr).Warn("Failed to delete request from Redis")
	}

	logger.Info("📤 Intent request processed and response sent to Redis")
}
