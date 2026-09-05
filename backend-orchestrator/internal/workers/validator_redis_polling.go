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
// This file contains Redis polling logic for Validator Worker to process
// requests from V2 Temporal workflows via correlation store.
//
// Flow:
// 1. Temporal Activity writes request to Redis
// 2. Worker polls Redis for pending requests
// 3. Worker processes request using existing logic
// 4. Worker writes response back to Redis
// 5. Temporal Activity receives response and continues
// ==============================================================================

// startRedisPoller polls Redis for pending validator requests from V2 workflows
func (w *ValidatorWorker) startRedisPoller() {
	logger := log.WithField("component", "redis_poller").WithField("worker", "validator")
	logger.Info("🔄 Starting Redis poller for validator requests (V2 workflows)")

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

func (w *ValidatorWorker) pollAndProcessRequests() error {
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()

	// Poll for pending validator requests
	requests, err := w.correlationClient.PollPendingRequests(ctx, "validator")
	if err != nil {
		return fmt.Errorf("poll failed: %w", err)
	}

	if len(requests) == 0 {
		return nil // No pending requests
	}

	log.WithField("count", len(requests)).Debug("📋 Found pending validator requests")

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

func (w *ValidatorWorker) processCorrelationRequest(req *correlation.PendingRequest) {
	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()

	logger := log.WithFields(log.Fields{
		"correlation_id": req.CorrelationID,
		"request_type":   req.RequestType,
		"worker_id":      w.workerID,
	})

	logger.Info("📥 Processing validator request from Redis (V2 workflow)")

	// Extract pipeline info from payload
	pipelineID := fmt.Sprintf("%v", req.Payload["pipeline_id"])
	executionID := fmt.Sprintf("%v", req.Payload["execution_id"])
	userID := fmt.Sprintf("%v", req.Payload["user_id"])

	// Convert correlation request to Task format with full payload
	task := Task{
		CorrelationID: req.CorrelationID,
		TaskType:      "validate",
		Payload:       req.Payload,
		Context:       req.Payload,
		PipelineID:    pipelineID,
		ExecutionID:   executionID,
		UserID:        userID,
		TaskID:        fmt.Sprintf("validator-%s", req.CorrelationID[:8]),
		WorkflowID:    pipelineID,
		StepID:        "validator",
	}

	// PHASE 2.4 FIX: Use main Execute() method from validator.go
	result := w.Execute(ctx, task)

	logger.Info("✅ Task processing completed")

	// Write response to Redis
	if routeErr := RouteResult(ctx, task, result, w.kafkaManager); routeErr != nil {
		logger.WithError(routeErr).Error("Failed to route response")
	}

	// Delete request from Redis after processing
	if delErr := w.correlationClient.DeleteRequest(ctx, req.CorrelationID, "validator"); delErr != nil {
		logger.WithError(delErr).Warn("Failed to delete request from Redis")
	}

	logger.Info("📤 Validator request processed and response sent to Redis")
}

// processValidatorTask is the core validator processing logic extracted for reuse
func (w *ValidatorWorker) processValidatorTask(ctx context.Context, task *Task) (TaskResult, error) {
	// Extract request from payload
	request, ok := task.Payload["request"].(string)
	if !ok {
		return TaskResult{}, fmt.Errorf("missing 'request' in payload")
	}

	// Call existing validator analysis logic
	// This calls the LLM service to parse natural language into structured validator
	validatorResult, err := w.callLLMForValidator(ctx, request, task.PipelineID)
	if err != nil {
		return TaskResult{}, fmt.Errorf("LLM service error: %w", err)
	}

	// Build result
	result := TaskResult{
		TaskID:      task.TaskID,
		WorkflowID:  task.WorkflowID,
		StepID:      task.StepID,
		Status:      "success",
		Output:      validatorResult,
		CompletedAt: time.Now(),
		TraceID:     task.TraceID,
	}

	return result, nil
}

// callLLMForValidator calls the LLM service for validator analysis
// This method should already exist in validator.go - this is just a reference
func (w *ValidatorWorker) callLLMForValidator(ctx context.Context, request string, pipelineID string) (map[string]interface{}, error) {
	// This delegates to the existing LLM call logic in the worker
	// The actual implementation should call w.llmServiceURL
	// For now, we'll return a placeholder that needs to be replaced with actual logic
	return map[string]interface{}{
		"validator":   "data_transfer",
		"source":      "auto",
		"destination": "auto",
		"query":       request,
	}, nil
}
