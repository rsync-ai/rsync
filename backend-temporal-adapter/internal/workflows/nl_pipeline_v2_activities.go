package workflows

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rsync-ai/backend-temporal-adapter/internal/correlation"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// ==============================================================================
// V2 ACTIVITIES (Phase D - Request/Reply Pattern)
// ==============================================================================
// These activities use Redis-based correlation instead of KafkaAdapter signals
// Each activity:
// 1. Generates a correlation ID
// 2. Writes request to correlation store
// 3. Publishes command to Kafka (for worker pickup)
// 4. Blocks waiting for response in correlation store
// 5. Returns typed errors for workflow branching

var correlationStore *correlation.Store

const v2ActivityErrorTemporalType = "V2_ACTIVITY_ERROR"

// waitForResponseWithHeartbeats waits on the correlation store while emitting Temporal activity heartbeats.
//
// IMPORTANT: Our correlation store uses Redis BRPOP which can block longer than Temporal's HeartbeatTimeout.
// We therefore poll in short windows and record heartbeats between polls to prevent false Heartbeat timeouts.
func waitForResponseWithHeartbeats(
	ctx context.Context,
	agentType string,
	correlationID string,
	totalTimeout time.Duration,
) (*correlation.Response, error) {
	start := time.Now()
	pollWindow := 10 * time.Second // must be < workflow HeartbeatTimeout (30s)
	for {
		elapsed := time.Since(start)
		if elapsed >= totalTimeout {
			return nil, fmt.Errorf("timeout waiting for response: correlation_id=%s", correlationID)
		}
		remaining := totalTimeout - elapsed
		if remaining < pollWindow {
			pollWindow = remaining
		}

		resp, err := correlationStore.WaitForResponse(ctx, correlationID, pollWindow)
		if err != nil {
			// Treat correlation store timeouts as "poll ticks" (not a hard failure).
			// WaitForResponse formats errors as: "timeout waiting for response: correlation_id=..."
			if strings.Contains(err.Error(), "timeout waiting for response") {
				activity.RecordHeartbeat(ctx, map[string]interface{}{
					"agent_type":     agentType,
					"correlation_id": correlationID,
					"waited_ms":      elapsed.Milliseconds(),
					"poll_window_ms": pollWindow.Milliseconds(),
				})
				continue
			}
			return nil, err
		}

		// Successful response
		activity.RecordHeartbeat(ctx, map[string]interface{}{
			"agent_type":     agentType,
			"correlation_id": correlationID,
			"status":         "response_received",
			"waited_ms":      elapsed.Milliseconds(),
		})
		return resp, nil
	}
}

// toTemporalError wraps our typed ActivityError so workflows can deterministically branch on it.
// Policy / deterministic / infra errors are marked non-retryable (so we don't burn activity retries).
// Transient errors remain retryable (Temporal will retry them per ActivityOptions).
func toTemporalError(e *ActivityError) error {
	if e == nil {
		return nil
	}
	switch e.Type {
	case ErrTypeTransient:
		return temporal.NewApplicationError(e.Error(), v2ActivityErrorTemporalType, e)
	default:
		// IMPORTANT: NewNonRetryableApplicationError signature is (message, type, cause, details...).
		// We want our structured ActivityError to be stored as DETAILS (so the workflow can decode Metadata),
		// not as the CAUSE.
		return temporal.NewNonRetryableApplicationError(e.Error(), v2ActivityErrorTemporalType, nil, e)
	}
}

// InitCorrelationStore initializes the global correlation store
func InitCorrelationStore(redisAddr, redisPassword string) error {
	store, err := correlation.NewStore(redisAddr, redisPassword)
	if err != nil {
		return fmt.Errorf("failed to initialize correlation store: %w", err)
	}
	correlationStore = store
	log.Info("✅ Correlation store initialized for V2 activities")
	return nil
}

func traceContextFromInput(input NLPipelineWorkflowV2Input) (traceID string, traceparent string, tracestate string) {
	// Prefer upstream distributed trace id when available (32-char OTel hex).
	traceID = input.TraceID
	if traceID == "" {
		// Fallback: stable business identifier
		traceID = input.ExecutionID
	}
	return traceID, input.Traceparent, input.Tracestate
}

// IntentActivityV2 executes intent resolution with request/reply
func IntentActivityV2(ctx context.Context, input NLPipelineWorkflowV2Input) (map[string]interface{}, error) {
	logger := activity.GetLogger(ctx)
	correlationID := uuid.New().String()
	traceID, traceparent, tracestate := traceContextFromInput(input)

	logger.Info("IntentActivityV2 starting", "correlation_id", correlationID, "pipeline_id", input.PipelineID)

	// Build task for intent agent
	task := map[string]interface{}{
		"pipeline_id":  input.PipelineID,
		"execution_id": input.ExecutionID,
		"user_id":      input.UserID,
		"type":         "intent",
		"trace_id":     traceID,
		"traceparent":  traceparent,
		"tracestate":   tracestate,
		// Propagate connection IDs early so downstream workers can avoid DB lookups.
		"source_connection_id":      input.SourceConnectionID,
		"destination_connection_id": input.DestinationConnectionID,
		"batch_size":                input.BatchSize,
		"user_request":              input.Message, // orchestrator IntentWorker expects this
		"request":                   input.Message, // backward-compatible alias
		"message":                   input.Message, // legacy field kept for debugging
	}

	// Write request to correlation store
	req := correlation.Request{
		CorrelationID: correlationID,
		AgentType:     "intent",
		Task:          task,
		Timeout:       2 * time.Minute,
		SentAt:        time.Now(),
	}

	if err := correlationStore.WriteRequest(ctx, req); err != nil {
		return nil, toTemporalError(NewInfraError("REDIS_WRITE_FAILED", fmt.Sprintf("Failed to write correlation request: %v", err), nil))
	}

	// V2 dispatch is Redis-correlation-only: the worker's Redis poller (claimed via
	// SET NX) handles the request written above. We no longer also publish to Kafka —
	// that second path caused double-execution (KI-HYBRID-1). V1 still uses Kafka.

	// Wait for response (blocking)
	response, err := waitForResponseWithHeartbeats(ctx, "intent", correlationID, 2*time.Minute)
	if err != nil {
		return nil, toTemporalError(NewTransientError("RESPONSE_TIMEOUT", fmt.Sprintf("Timeout waiting for intent response: %v", err), nil))
	}

	// Handle response status
	if response.Status != "success" && response.Status != "completed" {
		return nil, toTemporalError(NewDeterministicError("INTENT_FAILED", response.Error, nil))
	}

	logger.Info("✅ IntentActivityV2 completed", "output", response.Output)
	return response.Output, nil
}

// ConnectorResolverActivityV2 executes connector resolution with request/reply
func ConnectorResolverActivityV2(ctx context.Context, input NLPipelineWorkflowV2Input, state *WorkflowState) (map[string]interface{}, error) {
	logger := activity.GetLogger(ctx)
	correlationID := uuid.New().String()
	traceID, traceparent, tracestate := traceContextFromInput(input)

	logger.Info("🔌 ConnectorResolverActivityV2 starting", "correlation_id", correlationID)

	// Build task for connector resolver
	task := map[string]interface{}{
		"pipeline_id":               input.PipelineID,
		"execution_id":              input.ExecutionID,
		"user_id":                   input.UserID,
		"type":                      "connector_resolver",
		"trace_id":                  traceID,
		"traceparent":               traceparent,
		"tracestate":                tracestate,
		"user_request":              input.Message,
		"source_connection_id":      state.SourceConnectionID,
		"destination_connection_id": state.DestinationConnectionID,
		"batch_size":                input.BatchSize,
		"parsed_intent":             state.CachedIntent, // orchestrator CapabilityResolverWorker expects this in Context
		"intent":                    state.CachedIntent, // legacy alias (kept for debugging)
	}

	// Write request to correlation store
	req := correlation.Request{
		CorrelationID: correlationID,
		AgentType:     "connector_resolver",
		Task:          task,
		Timeout:       3 * time.Minute,
		SentAt:        time.Now(),
	}

	if err := correlationStore.WriteRequest(ctx, req); err != nil {
		return nil, toTemporalError(NewInfraError("REDIS_WRITE_FAILED", fmt.Sprintf("Failed to write correlation request: %v", err), nil))
	}

	// V2 dispatch is Redis-correlation-only (see IntentActivityV2); no Kafka publish.

	// Wait for response (blocking)
	response, err := waitForResponseWithHeartbeats(ctx, "connector_resolver", correlationID, 3*time.Minute)
	if err != nil {
		return nil, toTemporalError(NewTransientError("RESPONSE_TIMEOUT", fmt.Sprintf("Timeout waiting for connector response: %v", err), nil))
	}

	// Handle response based on v2 contract (Phase F)
	if response.Status != "success" && response.Status != "completed" {
		// Check if it's a policy error
		if errCode, ok := response.Output["error_code"].(string); ok {
			if errCode == PolicyCodeMissingConnectors {
				return nil, toTemporalError(NewPolicyError(PolicyCodeMissingConnectors, "Connectors need to be generated", response.Output))
			} else if errCode == PolicyCodeMissingConnections {
				return nil, toTemporalError(NewPolicyError(PolicyCodeMissingConnections, "Connections need to be configured", response.Output))
			}
		}
		return nil, toTemporalError(NewDeterministicError("CONNECTOR_RESOLUTION_FAILED", response.Error, nil))
	}

	logger.Info("✅ ConnectorResolverActivityV2 completed", "output", response.Output)
	return response.Output, nil
}

// ConnectionValidatorActivityV2 validates user-provided connections
func ConnectionValidatorActivityV2(ctx context.Context, input NLPipelineWorkflowV2Input, state *WorkflowState) error {
	logger := activity.GetLogger(ctx)
	correlationID := uuid.New().String()
	traceID, traceparent, tracestate := traceContextFromInput(input)

	logger.Info("🔍 ConnectionValidatorActivityV2 starting", "correlation_id", correlationID)

	// Build task for connection validator
	task := map[string]interface{}{
		"pipeline_id":               input.PipelineID,
		"execution_id":              input.ExecutionID,
		"user_id":                   input.UserID,
		"type":                      "connection_validator",
		"trace_id":                  traceID,
		"traceparent":               traceparent,
		"tracestate":                tracestate,
		"source_connection_id":      state.SourceConnectionID,
		"destination_connection_id": state.DestinationConnectionID,
	}

	// Write request to correlation store
	req := correlation.Request{
		CorrelationID: correlationID,
		AgentType:     "connection_validator",
		Task:          task,
		Timeout:       1 * time.Minute,
		SentAt:        time.Now(),
	}

	if err := correlationStore.WriteRequest(ctx, req); err != nil {
		return toTemporalError(NewInfraError("REDIS_WRITE_FAILED", fmt.Sprintf("Failed to write correlation request: %v", err), nil))
	}

	// V2 dispatch is Redis-correlation-only (see IntentActivityV2); no Kafka publish.

	// Wait for response (blocking)
	response, err := waitForResponseWithHeartbeats(ctx, "connection_validator", correlationID, 1*time.Minute)
	if err != nil {
		return toTemporalError(NewTransientError("RESPONSE_TIMEOUT", fmt.Sprintf("Timeout waiting for validation response: %v", err), nil))
	}

	// Handle response
	if response.Status != "success" && response.Status != "completed" {
		// Invalid connections - return policy error to stay in wait state
		return toTemporalError(NewPolicyError(PolicyCodeInvalidConfiguration, response.Error, response.Output))
	}

	logger.Info("✅ ConnectionValidatorActivityV2 completed")
	return nil
}

// addReplanErrorContext threads the workflow's recorded last failure into the planner task as
// error_context. Auto-replan (beta): present only when a prior executor failure triggered a
// re-plan; on the normal first pass state.LastFailure is nil and the task is left unchanged.
// Pure helper so the threading can be unit-tested without an activity environment.
func addReplanErrorContext(task map[string]interface{}, state *WorkflowState) {
	if state != nil && state.LastFailure != nil {
		task["error_context"] = state.LastFailure
	}
}

// PlannerActivityV2 executes planning with request/reply
func PlannerActivityV2(ctx context.Context, input NLPipelineWorkflowV2Input, state *WorkflowState) (map[string]interface{}, error) {
	logger := activity.GetLogger(ctx)
	correlationID := uuid.New().String()
	traceID, traceparent, tracestate := traceContextFromInput(input)

	logger.Info("📋 PlannerActivityV2 starting", "correlation_id", correlationID)

	// Build task for planner
	task := map[string]interface{}{
		"pipeline_id":               input.PipelineID,
		"execution_id":              input.ExecutionID,
		"type":                      "planner",
		"trace_id":                  traceID,
		"traceparent":               traceparent,
		"tracestate":                tracestate,
		"intent":                    state.CachedIntent,
		"connectors":                state.CachedConnectorResolution,
		"source_connection_id":      state.SourceConnectionID,
		"destination_connection_id": state.DestinationConnectionID,
	}

	// Error-context-aware replan: when the executor failed and the workflow re-invokes the
	// planner, pass the last failure so the planner can produce a corrected plan instead of
	// regenerating the same failing one. Absent on the first (normal) planning pass, so the
	// existing planning path is unchanged.
	addReplanErrorContext(task, state)

	// Write request to correlation store
	req := correlation.Request{
		CorrelationID: correlationID,
		AgentType:     "planner",
		Task:          task,
		Timeout:       3 * time.Minute,
		SentAt:        time.Now(),
	}

	if err := correlationStore.WriteRequest(ctx, req); err != nil {
		return nil, toTemporalError(NewInfraError("REDIS_WRITE_FAILED", fmt.Sprintf("Failed to write correlation request: %v", err), nil))
	}

	// V2 dispatch is Redis-correlation-only (see IntentActivityV2); no Kafka publish.

	// Wait for response (blocking)
	response, err := waitForResponseWithHeartbeats(ctx, "planner", correlationID, 3*time.Minute)
	if err != nil {
		return nil, toTemporalError(NewTransientError("RESPONSE_TIMEOUT", fmt.Sprintf("Timeout waiting for planner response: %v", err), nil))
	}

	// Handle response
	if response.Status != "success" && response.Status != "completed" {
		return nil, toTemporalError(NewDeterministicError("PLANNING_FAILED", response.Error, nil))
	}

	logger.Info("✅ PlannerActivityV2 completed", "output", response.Output)
	return response.Output, nil
}

// ValidatorActivityV2 executes validation with request/reply
func ValidatorActivityV2(ctx context.Context, input NLPipelineWorkflowV2Input, state *WorkflowState) (map[string]interface{}, error) {
	logger := activity.GetLogger(ctx)
	correlationID := uuid.New().String()
	traceID, traceparent, tracestate := traceContextFromInput(input)

	logger.Info("✓ ValidatorActivityV2 starting", "correlation_id", correlationID)

	// Build task for validator
	task := map[string]interface{}{
		"pipeline_id":               input.PipelineID,
		"execution_id":              input.ExecutionID,
		"type":                      "validator",
		"trace_id":                  traceID,
		"traceparent":               traceparent,
		"tracestate":                tracestate,
		"plan":                      state.CachedPlan,
		"source_connection_id":      state.SourceConnectionID,
		"destination_connection_id": state.DestinationConnectionID,
		"batch_size":                input.BatchSize,
	}

	// Write request to correlation store
	req := correlation.Request{
		CorrelationID: correlationID,
		AgentType:     "validator",
		Task:          task,
		Timeout:       2 * time.Minute,
		SentAt:        time.Now(),
	}

	if err := correlationStore.WriteRequest(ctx, req); err != nil {
		return nil, toTemporalError(NewInfraError("REDIS_WRITE_FAILED", fmt.Sprintf("Failed to write correlation request: %v", err), nil))
	}

	// V2 dispatch is Redis-correlation-only (see IntentActivityV2); no Kafka publish.

	// Wait for response (blocking)
	response, err := waitForResponseWithHeartbeats(ctx, "validator", correlationID, 2*time.Minute)
	if err != nil {
		return nil, toTemporalError(NewTransientError("RESPONSE_TIMEOUT", fmt.Sprintf("Timeout waiting for validator response: %v", err), nil))
	}

	// Handle response
	if response.Status != "success" && response.Status != "completed" {
		return nil, toTemporalError(NewDeterministicError("VALIDATION_FAILED", response.Error, nil))
	}

	logger.Info("✅ ValidatorActivityV2 completed", "output", response.Output)
	return response.Output, nil
}

// CostEstimatorActivityV2 sends a cost-estimation task to the CostEstimatorWorker and returns
// the estimate.  The result is advisory — callers should log but not fail on estimation errors.
func CostEstimatorActivityV2(ctx context.Context, input NLPipelineWorkflowV2Input, state *WorkflowState) (map[string]interface{}, error) {
	logger := activity.GetLogger(ctx)
	correlationID := uuid.New().String()
	traceID, traceparent, tracestate := traceContextFromInput(input)

	task := map[string]interface{}{
		"pipeline_id":               input.PipelineID,
		"execution_id":              input.ExecutionID,
		"task_type":                 "estimate_cost",
		"type":                      "estimate_cost",
		"trace_id":                  traceID,
		"traceparent":               traceparent,
		"tracestate":                tracestate,
		"source_connection_id":      state.SourceConnectionID,
		"destination_connection_id": state.DestinationConnectionID,
		"selected_tables":           state.SelectedTables,
		"context": map[string]interface{}{
			"source_connector":      state.SourceConnectorSnapshot,
			"destination_connector": state.DestinationConnectorSnapshot,
			"plan":                  state.CachedPlan,
		},
	}
	task["correlation_id"] = correlationID

	// Cost estimation is advisory and the cost_estimator worker now answers via the
	// Redis poller within milliseconds. Cap the wait at 30s so that if the worker is
	// ever unavailable the pipeline loses 30s here instead of stalling 5 minutes
	// between planning and validation (the original regression).
	req := correlation.Request{
		CorrelationID: correlationID,
		AgentType:     "cost_estimator",
		Task:          task,
		Timeout:       30 * time.Second,
		SentAt:        time.Now(),
	}
	if err := correlationStore.WriteRequest(ctx, req); err != nil {
		return nil, toTemporalError(NewInfraError("REDIS_WRITE_FAILED", fmt.Sprintf("cost estimator: failed to write correlation request: %v", err), nil))
	}

	// V2 dispatch is Redis-correlation-only (see IntentActivityV2); no Kafka publish.

	response, err := waitForResponseWithHeartbeats(ctx, "cost_estimator", correlationID, 30*time.Second)
	if err != nil {
		logger.Warn("CostEstimatorActivityV2: timeout or error (non-fatal, continuing)", "error", err)
		return nil, nil // Advisory — don't block the pipeline
	}

	logger.Info("CostEstimatorActivityV2 completed",
		"pipeline_id", input.PipelineID,
		"risk_level", response.Output["risk_level"],
		"estimated_rows", response.Output["estimated_rows"],
	)
	return response.Output, nil
}

// ExecutorActivityV2 executes the pipeline with request/reply
func ExecutorActivityV2(ctx context.Context, input NLPipelineWorkflowV2Input, state *WorkflowState) (map[string]interface{}, error) {
	logger := activity.GetLogger(ctx)
	correlationID := uuid.New().String()
	traceID, traceparent, tracestate := traceContextFromInput(input)

	logger.Info(
		"ExecutorActivityV2 starting",
		"correlation_id", correlationID,
		"pipeline_id", input.PipelineID,
		"source_connection_id", state.SourceConnectionID,
		"destination_connection_id", state.DestinationConnectionID,
	)

	// Build task for executor (shared with the Temporal-native dispatch path).
	task := buildExecutorTaskMap(input, state, traceID, traceparent, tracestate)

	// Write request to correlation store
	req := correlation.Request{
		CorrelationID: correlationID,
		AgentType:     "executor",
		Task:          task,
		Timeout:       30 * time.Minute, // Longer timeout for execution
		SentAt:        time.Now(),
	}

	if err := correlationStore.WriteRequest(ctx, req); err != nil {
		return nil, toTemporalError(NewInfraError("REDIS_WRITE_FAILED", fmt.Sprintf("Failed to write correlation request: %v", err), nil))
	}

	// V2 dispatch is Redis-correlation-only (see IntentActivityV2); no Kafka publish.

	// Wait for response (blocking)
	response, err := waitForResponseWithHeartbeats(ctx, "executor", correlationID, 30*time.Minute)
	if err != nil {
		// DETERMINISTIC, not TRANSIENT — this one is deliberately different from every
		// other activity's RESPONSE_TIMEOUT, and the difference is load-bearing.
		//
		// The timeout ladder is inverted for the executor: this wait is 30m, but the
		// workflow gives ExecutorActivityV2 StartToCloseTimeout 45m with
		// MaximumAttempts 3 (nl_pipeline_v2_workflow.go), and the orchestrator-side
		// executor context is also 45m. A TRANSIENT error here is retryable, so a batch
		// run that legitimately takes between 30 and 45 minutes had its activity fail at
		// minute 30 and Temporal immediately dispatched a SECOND executor — with a fresh
		// correlation ID — while the first one was still producing. Two executors then
		// wrote the same pipeline_checkpoints rows: duplicate rows at an append-only
		// destination, and interleaved checkpoint writes that skip ranges at an upsert
		// destination.
		//
		// A retry cannot help here in any case: the request is already in the
		// correlation store and the executor either picked it up (and is still working,
		// so re-dispatching duplicates it) or the executor is down (and re-dispatching
		// from the same workflow will time out identically). Failing deterministically
		// hands the run to the heal sweep, which retries ONCE, in resume mode, under the
		// 3-per-24h cap — i.e. exactly the bounded retry this uncapped path was faking.
		//
		// Do NOT "fix" this by raising the wait above StartToClose: StartToClose expiry
		// is itself retryable, so the double-dispatch just moves to minute 45.
		return nil, toTemporalError(NewDeterministicError(
			"EXECUTOR_RESPONSE_TIMEOUT",
			fmt.Sprintf("Timeout waiting for executor response (no retry — a retry would double-dispatch onto shared checkpoints): %v", err),
			map[string]interface{}{
				"correlation_id": correlationID,
				"pipeline_id":    input.PipelineID,
				"execution_id":   input.ExecutionID,
			},
		))
	}

	// Classify the response (shared with the Temporal-native dispatch path).
	// NOTE: status "running" is a SUCCESS for streaming/CDC pipelines.
	result, aerr := classifyExecutorResponse(response.Status, response.Error, response.Output, correlationID, state)
	if aerr != nil {
		return nil, toTemporalError(aerr)
	}

	logger.Info("✅ ExecutorActivityV2 completed", "output", response.Output)
	return result, nil
}

func looksLikeAuthError(errMsg string, meta map[string]interface{}) bool {
	msg := strings.ToLower(strings.TrimSpace(errMsg))
	if msg == "" {
		return false
	}
	// Common signals: HTTP 401/403, token expired, oauth wording.
	if strings.Contains(msg, "401") || strings.Contains(msg, "403") {
		return true
	}
	if strings.Contains(msg, "token expired") || strings.Contains(msg, "expired token") {
		return true
	}
	if strings.Contains(msg, "oauth") && (strings.Contains(msg, "unauthorized") || strings.Contains(msg, "invalid") || strings.Contains(msg, "expired")) {
		return true
	}
	// Some producers put a machine-readable hint in metadata
	if meta != nil {
		if v, ok := meta["error_category"].(string); ok && strings.EqualFold(strings.TrimSpace(v), "auth") {
			return true
		}
	}
	return false
}

func looksLikeInvalidConfig(errMsg string, meta map[string]interface{}) bool {
	msg := strings.ToLower(strings.TrimSpace(errMsg))
	if msg == "" {
		return false
	}
	// Common executor-side validation failures
	if strings.Contains(msg, "not validated") || strings.Contains(msg, "invalid configuration") {
		return true
	}
	if strings.Contains(msg, "failed to fetch") && strings.Contains(msg, "connection") {
		return true
	}
	if strings.Contains(msg, "missing required") || strings.Contains(msg, "required field") {
		return true
	}
	// DNS / host resolution errors are almost always configuration issues (bad host, wrong network).
	if strings.Contains(msg, "unknown mysql server host") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "name or service not known") ||
		strings.Contains(msg, "getaddrinfo") {
		return true
	}
	if meta != nil {
		if v, ok := meta["validation_errors"]; ok && v != nil {
			return true
		}
	}
	return false
}

func looksLikeSchemaDrift(errMsg string, meta map[string]interface{}) bool {
	msg := strings.ToLower(strings.TrimSpace(errMsg))
	if msg == "" {
		return false
	}
	// Conservative: only match strong signals.
	if strings.Contains(msg, "column") && strings.Contains(msg, "does not exist") {
		return true
	}
	if strings.Contains(msg, "unknown column") || strings.Contains(msg, "undefinedcolumn") {
		return true
	}
	if strings.Contains(msg, "schema mismatch") || strings.Contains(msg, "schema drift") {
		return true
	}
	if meta != nil {
		if v, ok := meta["error_category"].(string); ok && strings.EqualFold(strings.TrimSpace(v), "schema") {
			return true
		}
	}
	return false
}

// ==============================================================================
// OAuth support activities (used by healing loop)
// ==============================================================================

var (
	errInvalidEncryptionKey = errors.New("invalid encryption key")
	errInvalidCiphertext    = errors.New("invalid ciphertext")
)

func getEncryptionKeyForDecrypt() ([]byte, error) {
	key := strings.TrimSpace(os.Getenv("ENCRYPTION_KEY"))
	isDev := os.Getenv("ENVIRONMENT") == "development" || os.Getenv("ENVIRONMENT") == "dev"
	if key == "" {
		if !isDev {
			return nil, errInvalidEncryptionKey
		}
		key = "dev-only-key-please-change-me!!!" // 32 chars (matches shared/crypto dev default)
	}
	kb := []byte(key)
	if len(kb) < 32 {
		return nil, errInvalidEncryptionKey
	}
	return kb[:32], nil
}

func decryptString(ciphertextBase64 string) (string, error) {
	key, err := getEncryptionKeyForDecrypt()
	if err != nil {
		return "", err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextBase64)
	if err != nil {
		return "", errInvalidCiphertext
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return "", errInvalidCiphertext
	}
	nonce, ct := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", errInvalidCiphertext
	}
	return string(plaintext), nil
}

// fetchDecryptedConnectionConfigForUser loads + decrypts the connection
// config from DB, scoped to a single tenant (mirrors backend-orchestrator's
// connections.Manager.GetForUser).
//
// ⚠️ STATUS: currently UNWIRED — zero call sites. This is the *intended*
// tenant-aware path, written ahead of the migration but not yet adopted; the
// live activities all still call the tenant-blind fetchDecryptedConnectionConfig
// below. Do NOT assume connection loads are tenant-scoped today. To finish the
// migration, route the live callers (FetchConnectionOAuthTokenIDActivity,
// PlanSchemaDriftRepairActivity, ApplySchemaDDLActivity) through here with
// userID = state.UserID. Tracked in BACKLOG (temporal-adapter tenant scoping).
func fetchDecryptedConnectionConfigForUser(ctx context.Context, userID, connectionID string) (map[string]string, error) {
	return fetchConnectionConfigInternal(ctx, userID, connectionID)
}

// fetchDecryptedConnectionConfig loads + decrypts the connection config from DB,
// tenant-blind (no user_id filter).
//
// ⚠️ STATUS: this is the ACTUALLY-LIVE path — it has the real callers
// (FetchConnectionOAuthTokenIDActivity, PlanSchemaDriftRepairActivity,
// ApplySchemaDDLActivity). It is NOT slated for removal; the tenant-aware
// fetchDecryptedConnectionConfigForUser above is the future replacement once
// those callers are migrated. Until then this is correct-but-tenant-blind; it
// logs a note (below) on each call so the remaining sites are findable.
// IMPORTANT: Never log the decrypted config (contains secrets).
func fetchDecryptedConnectionConfig(ctx context.Context, connectionID string) (map[string]string, error) {
	return fetchConnectionConfigInternal(ctx, "", connectionID)
}

// fetchConnectionConfigInternal is the shared implementation. When
// userID is non-empty, the SQL filter includes `AND user_id = $2`.
func fetchConnectionConfigInternal(ctx context.Context, userID, connectionID string) (map[string]string, error) {
	if activityCtx == nil || activityCtx.DB == nil {
		return nil, fmt.Errorf("database not available")
	}
	db, ok := activityCtx.DB.(*sql.DB)
	if !ok || db == nil {
		return nil, fmt.Errorf("database type assertion failed")
	}

	if strings.TrimSpace(userID) == "" {
		log.Debugf("⚠️  fetchDecryptedConnectionConfig: tenant-blind call for connection %s — migrate to fetchDecryptedConnectionConfigForUser", connectionID)
	}

	var connType, connectorType, connectorVersion, encryptedConfig string
	var err error
	if userID != "" {
		err = db.QueryRowContext(ctx,
			`SELECT type, connector_type, connector_version, config FROM connections WHERE id = $1 AND user_id = $2`,
			connectionID, userID,
		).Scan(&connType, &connectorType, &connectorVersion, &encryptedConfig)
	} else {
		err = db.QueryRowContext(ctx,
			`SELECT type, connector_type, connector_version, config FROM connections WHERE id = $1`,
			connectionID,
		).Scan(&connType, &connectorType, &connectorVersion, &encryptedConfig)
	}
	if err != nil {
		return nil, err
	}

	decryptedJSON, err := decryptString(encryptedConfig)
	if err != nil {
		return nil, err
	}

	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(decryptedJSON), &raw); err != nil {
		return nil, err
	}

	out := make(map[string]string, len(raw)+4)
	for k, v := range raw {
		out[k] = fmt.Sprintf("%v", v)
	}
	// Keep keys stable and lowercase (compat with executor).
	out["type"] = strings.ToLower(strings.TrimSpace(connectorType))
	out["connector_type"] = strings.ToLower(strings.TrimSpace(connectorType))
	out["connection_type"] = strings.ToLower(strings.TrimSpace(connType)) // source|destination
	if strings.TrimSpace(connectorVersion) != "" {
		out["connector_version"] = strings.TrimSpace(connectorVersion)
	}
	return out, nil
}

// FetchConnectionOAuthTokenIDActivity fetches oauth_token_id for a given connection (best-effort).
func FetchConnectionOAuthTokenIDActivity(ctx context.Context, connectionID string) (string, error) {
	cfg, err := fetchDecryptedConnectionConfig(ctx, connectionID)
	if err != nil || cfg == nil {
		// Best-effort: don't break workflow healing due to missing token id.
		return "", nil
	}
	return strings.TrimSpace(cfg["oauth_token_id"]), nil
}

// RefreshOAuthTokenActivity refreshes an OAuth token via the API Gateway refresh endpoint.
func RefreshOAuthTokenActivity(ctx context.Context, tokenID string) (map[string]interface{}, error) {
	if strings.TrimSpace(tokenID) == "" {
		return nil, toTemporalError(NewPolicyError(PolicyCodeAuthReauthRequired, "Missing oauth token id; re-authorization required", nil))
	}
	apiGatewayURL := strings.TrimSpace(os.Getenv("API_GATEWAY_URL"))
	if apiGatewayURL == "" {
		apiGatewayURL = "http://api-gateway:5001"
	}
	refreshURL := fmt.Sprintf("%s/api/v1/oauth/tokens/%s/refresh", apiGatewayURL, tokenID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, refreshURL, nil)
	if err != nil {
		return nil, toTemporalError(NewInfraError("OAUTH_REFRESH_REQUEST_FAILED", fmt.Sprintf("failed to build refresh request: %v", err), nil))
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, toTemporalError(NewTransientError("OAUTH_REFRESH_FAILED", fmt.Sprintf("refresh request failed: %v", err), nil))
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, toTemporalError(NewPolicyError(PolicyCodeAuthRefreshFailed, fmt.Sprintf("oauth refresh failed (http %d): %s", resp.StatusCode, string(body)), map[string]interface{}{
			"token_id": tokenID,
		}))
	}
	var out map[string]interface{}
	_ = json.Unmarshal(body, &out)
	return out, nil
}

// ==============================================================================
// Schema drift repair activities (beta)
// ==============================================================================

type pgColumn struct {
	Name string
	Type string // formatted SQL type for ADD COLUMN
}

type SchemaRepairChange struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Type   string `json:"type"`
}

type SchemaRepairPlan struct {
	Safe             bool                 `json:"safe"`
	RequiresApproval bool                 `json:"requires_approval"`
	Reason           string               `json:"reason"`
	DDL              []string             `json:"ddl"`
	Changes          []SchemaRepairChange `json:"changes,omitempty"`

	SourceConnectionID      string `json:"source_connection_id,omitempty"`
	DestinationConnectionID string `json:"destination_connection_id,omitempty"`
	SourceConnector         string `json:"source_connector,omitempty"`
	DestinationConnector    string `json:"destination_connector,omitempty"`
}

type SchemaRepairValidation struct {
	Valid        bool             `json:"valid"`
	Reason       string           `json:"reason"`
	RemainingDDL []string         `json:"remaining_ddl,omitempty"`
	RepairPlan   SchemaRepairPlan `json:"repair_plan,omitempty"`
}

func isPostgresConnector(cfg map[string]string) bool {
	if cfg == nil {
		return false
	}
	ct := strings.ToLower(strings.TrimSpace(cfg["connector_type"]))
	if ct == "" {
		ct = strings.ToLower(strings.TrimSpace(cfg["type"]))
	}
	return strings.Contains(ct, "postgres")
}

func postgresDSNFromConfig(cfg map[string]string) (string, error) {
	host := strings.TrimSpace(cfg["host"])
	port := strings.TrimSpace(cfg["port"])
	user := strings.TrimSpace(cfg["user"])
	if user == "" {
		user = strings.TrimSpace(cfg["username"])
	}
	pass := strings.TrimSpace(cfg["password"])
	dbname := strings.TrimSpace(cfg["database"])
	if dbname == "" {
		dbname = strings.TrimSpace(cfg["db_name"])
	}
	sslmode := strings.TrimSpace(cfg["sslmode"])
	if sslmode == "" {
		sslmode = "disable"
	}
	if host == "" || user == "" || dbname == "" {
		return "", fmt.Errorf("missing required postgres connection fields (host/user/database)")
	}
	if port == "" {
		port = "5432"
	}

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, pass),
		Host:   fmt.Sprintf("%s:%s", host, port),
		Path:   "/" + dbname,
	}
	q := url.Values{}
	q.Set("sslmode", sslmode)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// splitSchemaTable parses "schema.table" or bare "table".
//
// The caller MUST pass defaultSchema (typically srcCfg["schema"] for the
// source connection's configured schema). Only fall back to "public" if
// the caller has no better default — never assume "public" silently, as
// many PostgreSQL deployments use schemas like "myapp", "analytics",
// "tenant_42", etc., and a hardcoded "public" silently breaks them.
func splitSchemaTable(t, defaultSchema string) (schema string, table string) {
	defaultSchema = strings.TrimSpace(defaultSchema)
	if defaultSchema == "" {
		defaultSchema = "public"
	}
	t = strings.TrimSpace(t)
	if t == "" {
		return defaultSchema, ""
	}
	if strings.Contains(t, ".") {
		parts := strings.SplitN(t, ".", 2)
		schema = strings.Trim(parts[0], `"`)
		table = strings.Trim(parts[1], `"`)
		if schema == "" {
			schema = defaultSchema
		}
		return schema, table
	}
	return defaultSchema, strings.Trim(t, `"`)
}

func formatPostgresType(dataType string, charMax sql.NullInt64, numPrec sql.NullInt64, numScale sql.NullInt64) string {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dt {
	case "character varying":
		if charMax.Valid && charMax.Int64 > 0 {
			return fmt.Sprintf("varchar(%d)", charMax.Int64)
		}
		return "varchar"
	case "character":
		if charMax.Valid && charMax.Int64 > 0 {
			return fmt.Sprintf("char(%d)", charMax.Int64)
		}
		return "char"
	case "numeric", "decimal":
		if numPrec.Valid && numPrec.Int64 > 0 {
			if numScale.Valid && numScale.Int64 >= 0 {
				return fmt.Sprintf("numeric(%d,%d)", numPrec.Int64, numScale.Int64)
			}
			return fmt.Sprintf("numeric(%d)", numPrec.Int64)
		}
		return "numeric"
	default:
		// information_schema.data_type is already a usable Postgres type string for most primitives
		return dt
	}
}

func fetchPostgresColumns(ctx context.Context, db *sql.DB, schema, table string) (map[string]pgColumn, error) {
	rows, err := db.QueryContext(ctx, `
SELECT
  column_name,
  data_type,
  character_maximum_length,
  numeric_precision,
  numeric_scale
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2
ORDER BY ordinal_position
`, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]pgColumn{}
	for rows.Next() {
		var name, dataType string
		var charMax, numPrec, numScale sql.NullInt64
		if err := rows.Scan(&name, &dataType, &charMax, &numPrec, &numScale); err != nil {
			return nil, err
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		out[strings.ToLower(name)] = pgColumn{
			Name: name,
			Type: formatPostgresType(dataType, charMax, numPrec, numScale),
		}
	}
	return out, nil
}

// PlanSchemaDriftRepairActivity computes a conservative set of ADD COLUMN statements for Postgres->Postgres.
// It only proposes "ADD COLUMN IF NOT EXISTS <nullable>" changes. Anything else should go through manual approval.
func PlanSchemaDriftRepairActivity(ctx context.Context, sourceConnectionID, destinationConnectionID string, tables []string) (SchemaRepairPlan, error) {
	if strings.TrimSpace(sourceConnectionID) == "" || strings.TrimSpace(destinationConnectionID) == "" {
		return SchemaRepairPlan{}, toTemporalError(NewPolicyError(PolicyCodeSchemaApprovalRequired, "Missing connection IDs for schema repair", nil))
	}
	if len(tables) == 0 {
		return SchemaRepairPlan{
			Safe:                    false,
			RequiresApproval:        true,
			Reason:                  "No selected tables available for schema diff; manual repair required",
			DDL:                     []string{},
			SourceConnectionID:      sourceConnectionID,
			DestinationConnectionID: destinationConnectionID,
		}, nil
	}

	srcCfg, err := fetchDecryptedConnectionConfig(ctx, sourceConnectionID)
	if err != nil {
		return SchemaRepairPlan{}, err
	}
	dstCfg, err := fetchDecryptedConnectionConfig(ctx, destinationConnectionID)
	if err != nil {
		return SchemaRepairPlan{}, err
	}
	if !isPostgresConnector(srcCfg) || !isPostgresConnector(dstCfg) {
		return SchemaRepairPlan{
			Safe:                    false,
			RequiresApproval:        true,
			Reason:                  "Schema auto-repair is only supported for Postgres→Postgres in beta",
			DDL:                     []string{},
			SourceConnectionID:      sourceConnectionID,
			DestinationConnectionID: destinationConnectionID,
			SourceConnector:         srcCfg["connector_type"],
			DestinationConnector:    dstCfg["connector_type"],
		}, nil
	}

	srcDSN, err := postgresDSNFromConfig(srcCfg)
	if err != nil {
		return SchemaRepairPlan{}, err
	}
	dstDSN, err := postgresDSNFromConfig(dstCfg)
	if err != nil {
		return SchemaRepairPlan{}, err
	}

	srcDB, err := sql.Open("postgres", srcDSN)
	if err != nil {
		return SchemaRepairPlan{}, err
	}
	defer srcDB.Close()
	dstDB, err := sql.Open("postgres", dstDSN)
	if err != nil {
		return SchemaRepairPlan{}, err
	}
	defer dstDB.Close()

	// Limit tables to keep activity bounded.
	if len(tables) > 25 {
		tables = tables[:25]
	}

	changes := []SchemaRepairChange{}
	ddl := []string{}

	// Use the source connection's configured schema as the default when a
	// table is passed unqualified. Fall back to "public" only if the user
	// didn't set a schema on their connection.
	srcDefaultSchema := strings.TrimSpace(srcCfg["schema"])
	if srcDefaultSchema == "" {
		srcDefaultSchema = "public"
	}

	for _, t := range tables {
		schema, table := splitSchemaTable(t, srcDefaultSchema)
		if table == "" {
			continue
		}

		srcCols, err := fetchPostgresColumns(ctx, srcDB, schema, table)
		if err != nil {
			// Fall back to approval on introspection failure.
			return SchemaRepairPlan{
				Safe:                    false,
				RequiresApproval:        true,
				Reason:                  fmt.Sprintf("Failed to read source schema for %s.%s: %v", schema, table, err),
				DDL:                     []string{},
				SourceConnectionID:      sourceConnectionID,
				DestinationConnectionID: destinationConnectionID,
			}, nil
		}
		dstCols, err := fetchPostgresColumns(ctx, dstDB, schema, table)
		if err != nil {
			return SchemaRepairPlan{
				Safe:                    false,
				RequiresApproval:        true,
				Reason:                  fmt.Sprintf("Failed to read destination schema for %s.%s: %v", schema, table, err),
				DDL:                     []string{},
				SourceConnectionID:      sourceConnectionID,
				DestinationConnectionID: destinationConnectionID,
			}, nil
		}

		for _, col := range srcCols {
			if _, exists := dstCols[strings.ToLower(col.Name)]; exists {
				continue
			}
			typ := strings.TrimSpace(col.Type)
			if typ == "" || typ == "user-defined" || typ == "array" {
				return SchemaRepairPlan{
					Safe:                    false,
					RequiresApproval:        true,
					Reason:                  fmt.Sprintf("Unsupported column type for auto-repair: %s (%s.%s.%s)", typ, schema, table, col.Name),
					DDL:                     []string{},
					SourceConnectionID:      sourceConnectionID,
					DestinationConnectionID: destinationConnectionID,
				}, nil
			}

			changes = append(changes, SchemaRepairChange{Table: fmt.Sprintf("%s.%s", schema, table), Column: col.Name, Type: typ})
			ddl = append(ddl, fmt.Sprintf(`ALTER TABLE "%s"."%s" ADD COLUMN IF NOT EXISTS "%s" %s`, schema, table, col.Name, typ))
		}
	}

	// Safe if we only need a small number of additive changes.
	safe := len(ddl) > 0 && len(ddl) <= 10

	return SchemaRepairPlan{
		Safe:                    safe,
		RequiresApproval:        !safe,
		Reason:                  "Additive columns detected",
		DDL:                     ddl,
		Changes:                 changes,
		SourceConnectionID:      sourceConnectionID,
		DestinationConnectionID: destinationConnectionID,
		SourceConnector:         srcCfg["connector_type"],
		DestinationConnector:    dstCfg["connector_type"],
	}, nil
}

// ApplySchemaDDLActivity applies a list of DDL statements to the destination (Postgres-only in beta).
func ApplySchemaDDLActivity(ctx context.Context, destinationConnectionID string, ddl []string) error {
	if strings.TrimSpace(destinationConnectionID) == "" {
		return toTemporalError(NewPolicyError(PolicyCodeSchemaApprovalRequired, "Missing destination connection for DDL apply", nil))
	}
	if len(ddl) == 0 {
		return nil
	}

	dstCfg, err := fetchDecryptedConnectionConfig(ctx, destinationConnectionID)
	if err != nil {
		return err
	}
	if !isPostgresConnector(dstCfg) {
		return toTemporalError(NewPolicyError(PolicyCodeSchemaApprovalRequired, "DDL apply is only supported for Postgres destinations in beta", map[string]interface{}{
			"destination_connector": dstCfg["connector_type"],
		}))
	}
	dstDSN, err := postgresDSNFromConfig(dstCfg)
	if err != nil {
		return err
	}
	db, err := sql.Open("postgres", dstDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	for _, stmt := range ddl {
		s := strings.TrimSpace(stmt)
		if s == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, s); err != nil {
			return toTemporalError(NewDeterministicError("DDL_APPLY_FAILED", fmt.Sprintf("failed to apply ddl: %v", err), map[string]interface{}{
				"statement": s,
			}))
		}
	}
	return nil
}

// ValidateSchemaRepairActivity re-diffs and returns remaining missing columns (empty means repaired).
func ValidateSchemaRepairActivity(ctx context.Context, sourceConnectionID, destinationConnectionID string, tables []string) (SchemaRepairValidation, error) {
	plan, err := PlanSchemaDriftRepairActivity(ctx, sourceConnectionID, destinationConnectionID, tables)
	if err != nil {
		return SchemaRepairValidation{}, err
	}
	if len(plan.DDL) == 0 {
		return SchemaRepairValidation{
			Valid:  true,
			Reason: "No remaining schema drift detected for selected tables",
		}, nil
	}
	return SchemaRepairValidation{
		Valid:        false,
		Reason:       "Schema drift still present after attempted repair",
		RemainingDDL: plan.DDL,
		RepairPlan:   plan,
	}, nil
}

// NOTE: publishAgentCommand (per-agent Kafka command publisher) was removed in Phase 2
// of the Kafka-removal work. V2 agent dispatch is now Redis-correlation-only; the Kafka
// command topics (agent.control.commands.*) are no longer written by the V2 path. V1
// dispatch (if any remains) still produces to those topics from its own code path.

// ==============================================================================
// DAG NODE EXECUTION ACTIVITY
// ==============================================================================

// ExecuteGraphNodeActivityV2 executes a single node in a DAG workflow
// It uses the same correlation pattern as ExecutorActivityV2 but operates on
// a single node at a time.
func ExecuteGraphNodeActivityV2(ctx context.Context, input ExecuteGraphNodeInput) (map[string]interface{}, error) {
	logger := log.WithFields(log.Fields{
		"activity":    "ExecuteGraphNodeActivityV2",
		"pipeline_id": input.PipelineID,
		"node_id":     input.NodeID,
		"node_kind":   input.NodeKind,
	})
	logger.Info("🔀 ExecuteGraphNodeActivityV2 started")

	// Record heartbeat
	activity.RecordHeartbeat(ctx, map[string]interface{}{
		"node_id":   input.NodeID,
		"node_kind": input.NodeKind,
		"status":    "starting",
	})

	// Generate correlation ID for this node execution
	correlationID := fmt.Sprintf("node-%s-%s", input.NodeID, uuid.New().String()[:8])

	// Build task based on node kind
	task := map[string]interface{}{
		"type":                      "node_execution",
		"correlation_id":            correlationID,
		"pipeline_id":               input.PipelineID,
		"execution_id":              input.ExecutionID,
		"node_id":                   input.NodeID,
		"node_kind":                 input.NodeKind,
		"node_config":               input.NodeConfig,
		"node_inputs":               input.NodeInputs,
		"source_connection_id":      input.SourceConnectionID,
		"destination_connection_id": input.DestinationConnectionID,
	}

	// Optional override: allow planners to set an explicit executor operation for a node.
	// Example: destination node can set operation="data_transfer" for full transfer.
	if input.NodeConfig != nil {
		if op, ok := input.NodeConfig["operation"].(string); ok && strings.TrimSpace(op) != "" {
			task["operation"] = strings.TrimSpace(op)
		}
	}

	// If node config already contains table selection, surface it in the canonical
	// fields the executor understands (selected_tables / tables).
	if input.NodeConfig != nil {
		if v, ok := input.NodeConfig["selected_tables"]; ok && v != nil {
			task["selected_tables"] = v
		} else if v, ok := input.NodeConfig["tables"]; ok && v != nil {
			task["selected_tables"] = v
		}
	}

	// Best-effort: propagate metadata.transforms into the task payload.
	// The executor's data_transfer path applies transforms from Params.metadata.transforms
	// or Payload.metadata.transforms.
	//
	// In DAG mode, transforms can come from multiple upstream transform nodes; we must
	// aggregate them deterministically (map iteration order is random).
	coerceTransforms := func(raw interface{}) ([]map[string]interface{}, bool) {
		if raw == nil {
			return nil, false
		}
		out := make([]map[string]interface{}, 0, 4)
		switch vv := raw.(type) {
		case []interface{}:
			for _, it := range vv {
				if m, ok := it.(map[string]interface{}); ok && m != nil {
					out = append(out, m)
				}
			}
		case []map[string]interface{}:
			out = append(out, vv...)
		default:
			// ignore unsupported shapes
		}
		if len(out) == 0 {
			return nil, false
		}
		return out, true
	}

	extractTransforms := func(val interface{}) ([]map[string]interface{}, bool) {
		m, ok := val.(map[string]interface{})
		if !ok || m == nil {
			return nil, false
		}
		// Shape A: { "metadata": { "transforms": [...] } }
		if mdRaw, ok := m["metadata"]; ok && mdRaw != nil {
			if md, ok := mdRaw.(map[string]interface{}); ok && md != nil {
				if tRaw, ok := md["transforms"]; ok && tRaw != nil {
					return coerceTransforms(tRaw)
				}
			}
		}
		// Shape B: { "transforms": [...] }
		if tRaw, ok := m["transforms"]; ok && tRaw != nil {
			return coerceTransforms(tRaw)
		}
		return nil, false
	}

	inferTransformTypeFromKey := func(k string) string {
		k = strings.TrimSpace(k)
		if strings.HasPrefix(k, "transform_") {
			return strings.TrimSpace(strings.TrimPrefix(k, "transform_"))
		}
		return ""
	}

	ensureType := func(t map[string]interface{}, inferred string) {
		if t == nil || strings.TrimSpace(inferred) == "" {
			return
		}
		if v, ok := t["type"]; ok && strings.TrimSpace(fmt.Sprint(v)) != "" {
			return
		}
		t["type"] = inferred
	}

	agg := make([]map[string]interface{}, 0, 8)

	// 1) Upstream transforms from node inputs (merge all).
	if input.NodeInputs != nil && len(input.NodeInputs) > 0 {
		keys := make([]string, 0, len(input.NodeInputs))
		for k := range input.NodeInputs {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			v := input.NodeInputs[k]
			list, ok := extractTransforms(v)
			if !ok || len(list) == 0 {
				continue
			}
			inferred := inferTransformTypeFromKey(k)
			for _, t := range list {
				ensureType(t, inferred)
				agg = append(agg, t)
			}
		}
	}

	// 2) Allow UI-provided transform suggestions to be attached via node_config.metadata.transforms
	// (e.g. via DAG node_input HITL endpoint).
	if input.NodeConfig != nil {
		if mdRaw, ok := input.NodeConfig["metadata"]; ok && mdRaw != nil {
			if md, ok := mdRaw.(map[string]interface{}); ok && md != nil {
				if tRaw, ok := md["transforms"]; ok && tRaw != nil {
					if list, ok := coerceTransforms(tRaw); ok {
						for _, t := range list {
							agg = append(agg, t)
						}
					}
				}
			}
		}
		// Back-compat: allow node_config.transforms too.
		if tRaw, ok := input.NodeConfig["transforms"]; ok && tRaw != nil {
			if list, ok := coerceTransforms(tRaw); ok {
				for _, t := range list {
					agg = append(agg, t)
				}
			}
		}
	}

	if len(agg) > 0 {
		task["metadata"] = map[string]interface{}{"transforms": agg}
	}

	// Route to appropriate agent based on node kind
	agentType := nodeKindToAgentType(input.NodeKind)
	logger.Info("📡 Routing node to agent", "agent_type", agentType)

	// Build node-specific task payload. Every branch on input.NodeKind lives in
	// applyNodeKindRouting (node_kind_routing.go) so the conformance census can
	// call the real routing instead of parsing a switch out of this file.
	applyNodeKindRouting(task, input.NodeKind, input.NodeConfig)

	// Write request to correlation store
	req := correlation.Request{
		CorrelationID: correlationID,
		AgentType:     agentType,
		Task:          task,
		Timeout:       15 * time.Minute,
		SentAt:        time.Now(),
	}

	if err := correlationStore.WriteRequest(ctx, req); err != nil {
		return nil, toTemporalError(NewInfraError("REDIS_WRITE_FAILED", fmt.Sprintf("Failed to write correlation request: %v", err), nil))
	}

	// V2 dispatch is Redis-correlation-only (see IntentActivityV2); no Kafka publish.

	logger.Info("📤 Node execution request written, waiting for response...")

	// Wait for response
	response, err := waitForResponseWithHeartbeats(ctx, agentType, correlationID, 15*time.Minute)
	if err != nil {
		return nil, toTemporalError(NewTransientError("RESPONSE_TIMEOUT", fmt.Sprintf("Timeout waiting for node execution response: %v", err), nil))
	}

	// Handle response
	if response.Status == "waiting_for_table_selection" {
		// For DAG node execution, table selection is a node-level HITL interaction.
		// Convert it into the DAG-specific policy code so the scheduler can wait.
		return nil, toTemporalError(NewPolicyError(PolicyCodeNodeInputRequired, response.Error, response.Output))
	}

	if response.Status == "waiting_for_input" || response.Status == "input_required" {
		// Node needs HITL input
		return nil, toTemporalError(NewPolicyError(PolicyCodeNodeInputRequired, response.Error, response.Output))
	}

	if response.Status != "success" && response.Status != "completed" {
		return nil, toTemporalError(NewDeterministicError("NODE_EXECUTION_FAILED", response.Error, response.Output))
	}

	logger.Info("✅ Node execution completed", "node_id", input.NodeID, "output_keys", getMapKeys(response.Output))
	return response.Output, nil
}

// nodeKindToAgentType maps node kinds to agent types
func nodeKindToAgentType(nodeKind string) string {
	switch nodeKind {
	case "source", "destination":
		return "executor"
	case "transform":
		return "executor"
	case "notification":
		return "executor"
	case "api_call":
		return "executor"
	case "llm":
		return "executor"
	case "condition":
		return "executor"
	default:
		return "executor"
	}
}

// getMapKeys returns the keys of a map for logging
func getMapKeys(m map[string]interface{}) []string {
	if m == nil {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// ==============================================================================
// HITL NODE INPUT INTERPRETATION ACTIVITY
// ==============================================================================

// InterpretNodeInputInput is the input for InterpretNodeInputActivity
type InterpretNodeInputInput struct {
	PipelineID   string                 `json:"pipeline_id"`
	ExecutionID  string                 `json:"execution_id"`
	NodeID       string                 `json:"node_id"`
	NodeKind     string                 `json:"node_kind"`
	HITLPrompt   string                 `json:"hitl_prompt"`
	UserResponse string                 `json:"user_response"`
	NodeContext  map[string]interface{} `json:"node_context,omitempty"`
}

// InterpretNodeInputOutput is the output from InterpretNodeInputActivity
type InterpretNodeInputOutput struct {
	Success             bool                   `json:"success"`
	ConfigPatch         map[string]interface{} `json:"config_patch,omitempty"`
	Error               string                 `json:"error,omitempty"`
	NeedsClarification  bool                   `json:"needs_clarification,omitempty"`
	ClarificationPrompt string                 `json:"clarification_prompt,omitempty"`
	Confidence          float64                `json:"confidence,omitempty"`
}

// InterpretNodeInputActivity interprets a user's NL response into structured config updates
// This calls the llm-service's HITL interpreter endpoint
func InterpretNodeInputActivity(ctx context.Context, input InterpretNodeInputInput) (*InterpretNodeInputOutput, error) {
	logger := log.WithFields(log.Fields{
		"activity":    "InterpretNodeInputActivity",
		"pipeline_id": input.PipelineID,
		"node_id":     input.NodeID,
	})
	logger.Info("🎯 InterpretNodeInputActivity started")

	// Record heartbeat
	activity.RecordHeartbeat(ctx, map[string]interface{}{
		"node_id":      input.NodeID,
		"status":       "interpreting",
		"response_len": len(input.UserResponse),
	})

	// Call llm-service HITL interpreter via HTTP
	llmServiceURL := os.Getenv("LLM_SERVICE_URL")
	if llmServiceURL == "" {
		llmServiceURL = "http://llm-service:5000"
	}

	requestBody := map[string]interface{}{
		"node_id":       input.NodeID,
		"node_kind":     input.NodeKind,
		"hitl_prompt":   input.HITLPrompt,
		"user_response": input.UserResponse,
		"node_context":  input.NodeContext,
	}

	bodyJSON, err := json.Marshal(requestBody)
	if err != nil {
		return nil, toTemporalError(NewInfraError("JSON_MARSHAL_FAILED", fmt.Sprintf("Failed to marshal request: %v", err), nil))
	}

	// Make HTTP request to llm-service
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(
		fmt.Sprintf("%s/v1/hitl/interpret", llmServiceURL),
		"application/json",
		bytes.NewReader(bodyJSON),
	)
	if err != nil {
		logger.Warn("LLM service call failed, using heuristic interpretation", "error", err)
		// Fallback to simple heuristic interpretation
		return heuristicInterpretation(input), nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		logger.Warn("LLM service returned non-200, using heuristic", "status", resp.StatusCode, "body", string(bodyBytes))
		return heuristicInterpretation(input), nil
	}

	var result InterpretNodeInputOutput
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		logger.Warn("Failed to decode LLM response, using heuristic", "error", err)
		return heuristicInterpretation(input), nil
	}

	logger.Info("✅ Node input interpreted", "success", result.Success, "confidence", result.Confidence)
	return &result, nil
}

// heuristicInterpretation provides simple fallback interpretation without LLM
func heuristicInterpretation(input InterpretNodeInputInput) *InterpretNodeInputOutput {
	response := strings.TrimSpace(input.UserResponse)
	responseLower := strings.ToLower(response)

	// Pattern 1: Yes/No confirmation
	if strings.Contains(strings.ToLower(input.HITLPrompt), "confirm") ||
		strings.Contains(strings.ToLower(input.HITLPrompt), "proceed") {
		if containsAny(responseLower, []string{"yes", "yeah", "yep", "confirm", "ok", "sure", "proceed"}) {
			return &InterpretNodeInputOutput{
				Success:     true,
				ConfigPatch: map[string]interface{}{"confirmed": true},
				Confidence:  0.9,
			}
		}
		if containsAny(responseLower, []string{"no", "nope", "cancel", "stop"}) {
			return &InterpretNodeInputOutput{
				Success:     true,
				ConfigPatch: map[string]interface{}{"confirmed": false},
				Confidence:  0.9,
			}
		}
	}

	// Pattern 2: Table selection
	if strings.Contains(strings.ToLower(input.HITLPrompt), "table") {
		tables := extractTableNames(response)
		if len(tables) > 0 {
			return &InterpretNodeInputOutput{
				Success:     true,
				ConfigPatch: map[string]interface{}{"tables": tables},
				Confidence:  0.85,
			}
		}
	}

	// Pattern 3: Path/location
	if strings.Contains(strings.ToLower(input.HITLPrompt), "path") ||
		strings.Contains(strings.ToLower(input.HITLPrompt), "location") {
		// Look for path-like strings
		if strings.Contains(response, "/") || strings.Contains(response, "s3://") {
			return &InterpretNodeInputOutput{
				Success:     true,
				ConfigPatch: map[string]interface{}{"path": response},
				Confidence:  0.8,
			}
		}
	}

	// Fallback: treat as raw input
	return &InterpretNodeInputOutput{
		Success:     true,
		ConfigPatch: map[string]interface{}{"user_input": response},
		Confidence:  0.5,
	}
}

// containsAny checks if s contains any of the substrings
func containsAny(s string, substrs []string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// extractTableNames extracts table names from user response
func extractTableNames(response string) []string {
	// Simple extraction - split by common separators
	response = strings.ToLower(response)
	// Remove noise words
	noise := []string{"the", "table", "tables", "sync", "select", "from", "and", "or", ","}

	words := strings.Fields(response)
	var tables []string

	for _, word := range words {
		word = strings.Trim(word, ",.;:\"'")
		if word == "" {
			continue
		}
		isNoise := false
		for _, n := range noise {
			if word == n {
				isNoise = true
				break
			}
		}
		if !isNoise && len(word) > 1 {
			// Basic table name validation (alphanumeric + underscore)
			valid := true
			for _, c := range word {
				if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
					valid = false
					break
				}
			}
			if valid {
				tables = append(tables, word)
			}
		}
	}

	return tables
}

// CleanupPartialDataActivityV2 is called as a best-effort compensation step when a pipeline
// execution fails after the executor has already started writing to the destination.
// It flags the execution as requiring cleanup so an operator / background job can verify and
// roll back or tombstone any partial rows written during the failed run.  Errors are non-fatal —
// callers should log but not fail the workflow on this activity's failure.
//
// NOTE: this previously also published a "cleanup" command to the executor's Kafka topic, but no
// worker ever consumed it (the executor filters to execute_pipeline/execute_plan task types), so
// that path was dead. The actionable signal is the cleanup_required DB flag set below.
func CleanupPartialDataActivityV2(ctx context.Context, pipelineID, executionID, destinationConnectionID string, tables []string) error {
	logger := activity.GetLogger(ctx)
	_ = destinationConnectionID // retained for signature stability / future executor-side cleanup
	_ = tables

	// Mark execution as requiring cleanup in the DB so operators / a background job can verify.
	if db := getDBFromActivityContext(); db != nil {
		if _, err := db.ExecContext(ctx,
			`UPDATE executions SET cleanup_required = true WHERE id = $1`,
			executionID,
		); err != nil {
			logger.Warn("CleanupPartialDataActivityV2: failed to set cleanup_required flag", "error", err, "execution_id", executionID)
		}
	}

	logger.Info("CleanupPartialDataActivityV2: cleanup initiated", "pipeline_id", pipelineID, "execution_id", executionID)
	return nil
}
