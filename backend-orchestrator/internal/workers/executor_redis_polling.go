package workers

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/rsync-ai/shared/correlation"
	log "github.com/sirupsen/logrus"
)

// ==============================================================================
// REDIS POLLING FOR V2 WORKFLOWS (Correlation Pattern)
// ==============================================================================
// This file contains Redis polling logic for Executor Worker to process
// requests from V2 Temporal workflows via correlation store.
//
// Flow:
// 1. Temporal Activity writes request to Redis
// 2. Worker polls Redis for pending requests
// 3. Worker processes request using existing logic
// 4. Worker writes response back to Redis
// 5. Temporal Activity receives response and continues
// ==============================================================================

// startRedisPoller polls Redis for pending executor requests from V2 workflows
func (w *ExecutorWorker) startRedisPoller() {
	logger := log.WithField("component", "redis_poller").WithField("worker", "executor")
	logger.Info("🔄 Starting Redis poller for executor requests (V2 workflows)")

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

func (w *ExecutorWorker) pollAndProcessRequests() error {
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()

	// Poll for pending executor requests
	requests, err := w.correlationClient.PollPendingRequests(ctx, "executor")
	if err != nil {
		return fmt.Errorf("poll failed: %w", err)
	}

	if len(requests) == 0 {
		return nil // No pending requests
	}

	log.WithField("count", len(requests)).Debug("📋 Found pending executor requests")

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

func (w *ExecutorWorker) processCorrelationRequest(req *correlation.PendingRequest) {
	logger := log.WithFields(log.Fields{
		"correlation_id": req.CorrelationID,
		"request_type":   req.RequestType,
		"worker_id":      w.workerID,
	})

	logger.Info("📥 Processing executor request from Redis (V2 workflow)")

	// Extract pipeline info from payload
	pipelineID := fmt.Sprintf("%v", req.Payload["pipeline_id"])
	executionID := fmt.Sprintf("%v", req.Payload["execution_id"])
	userID := fmt.Sprintf("%v", req.Payload["user_id"])
	sourceConnID := fmt.Sprintf("%v", req.Payload["source_connection_id"])
	destConnID := fmt.Sprintf("%v", req.Payload["destination_connection_id"])
	logger.WithFields(log.Fields{
		"pipeline_id":               pipelineID,
		"execution_id":              executionID,
		"user_id":                   userID,
		"source_connection_id":      sourceConnID,
		"destination_connection_id": destConnID,
	}).Debug("Received executor correlation request")

	// Convert correlation request to Task format with full payload
	task := Task{
		CorrelationID: req.CorrelationID,
		TaskType:      "execute", // Match what processTask in executor.go expects
		Payload:       req.Payload,
		Context:       req.Payload,
		PipelineID:    pipelineID,
		ExecutionID:   executionID,
		UserID:        userID,
		TaskID:        fmt.Sprintf("executor-%s", req.CorrelationID[:8]),
		WorkflowID:    pipelineID,
		StepID:        "executor",
	}

	// IMPORTANT: Executor tasks can be long running. A 30s context will cancel real transfers.
	// Keep this aligned with Temporal ExecutorActivity StartToClose timeout (tens of minutes).
	execTimeout := 45 * time.Minute
	if raw := os.Getenv("EXECUTOR_REQUEST_TIMEOUT"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			execTimeout = d
		}
	}
	ctxExec, cancelExec := context.WithTimeout(w.ctx, execTimeout)
	defer cancelExec()

	// Renew the claim while processing so long-running executions don't get double-claimed,
	// but still recover quickly if this worker crashes.
	stopRenew := startClaimRenewer(ctxExec, w.correlationClient, req.CorrelationID, w.workerID)
	defer stopRenew()

	// PHASE 2.4 FIX: Use main Execute() method from executor.go
	// This will call the executor agent with the real MCP connectors
	result := w.Execute(ctxExec, task)

	logger.Info("✅ Task processing completed")

	// Write response to Redis (success or failure)
	ctxCleanup, cancelCleanup := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelCleanup()

	if routeErr := RouteResult(ctxCleanup, task, result, w.kafkaManager); routeErr != nil {
		logger.WithError(routeErr).Error("Failed to route response")
	}

	// Delete request from Redis after processing
	if delErr := w.correlationClient.DeleteRequest(ctxCleanup, req.CorrelationID, "executor"); delErr != nil {
		logger.WithError(delErr).Warn("Failed to delete request from Redis")
	}

	logger.Info("📤 Executor request processed and response sent to Redis")
}
