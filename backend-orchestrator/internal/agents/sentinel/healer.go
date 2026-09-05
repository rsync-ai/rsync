package sentinel

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/saramaauth"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// Healer executes healing actions for detected issues
type Healer struct {
	kafkaManager *kafka.Manager
	db           *sql.DB
	config       *SentinelConfig
	logger       *AuditLogger
	agentManager *AgentManager
	
	// Healing state tracking
	healingAttempts map[string]int
	lastAttempt     map[string]time.Time
	circuitBreakers map[string]*CircuitBreaker
	mu              sync.RWMutex
	
	// Control
	ctx    context.Context
	cancel context.CancelFunc
}

// CircuitBreaker tracks repeated failures and trips to prevent thrashing
type CircuitBreaker struct {
	FailureCount int
	LastFailure  time.Time
	Tripped      bool
	TrippedAt    time.Time
}

// NewHealer creates a new healer
func NewHealer(kafkaManager *kafka.Manager, db *sql.DB, config *SentinelConfig, logger *AuditLogger, agentManager *AgentManager) *Healer {
	return &Healer{
		kafkaManager:    kafkaManager,
		db:              db,
		config:          config,
		logger:          logger,
		agentManager:    agentManager,
		healingAttempts: make(map[string]int),
		lastAttempt:     make(map[string]time.Time),
		circuitBreakers: make(map[string]*CircuitBreaker),
	}
}

// Start starts the healer
func (h *Healer) Start(ctx context.Context) error {
	h.ctx, h.cancel = context.WithCancel(ctx)
	
	// Start circuit breaker reset goroutine
	go h.resetCircuitBreakers()
	
	return nil
}

// Stop stops the healer
func (h *Healer) Stop() {
	if h.cancel != nil {
		h.cancel()
	}
}

// DetermineAction determines the appropriate healing action for an issue
func (h *Healer) DetermineAction(issue *Issue) HealingAction {
	switch issue.Type {
	case IssueTypeMissingHeartbeat:
		return HealingActionRestartAgent
	case IssueTypeHighLag:
		return HealingActionScaleUp
	case IssueTypeProtocolMismatch:
		return HealingActionFixProtocol
	case IssueTypeDLQGrowth:
		return HealingActionReplayMessages
	case IssueTypeConnectorDown:
		return HealingActionRestartConsumer
	case IssueTypeInfrastructureDown:
		return HealingActionAlert // Critical infrastructure needs manual intervention
	case IssueTypeRepeatedFailure:
		return HealingActionCircuitBreak
	default:
		return "" // No action
	}
}

// ExecuteHealing executes a healing action
func (h *Healer) ExecuteHealing(ctx context.Context, issue *Issue, action HealingAction) *HealingResult {
	ctx, span := sentinelTracer.Start(ctx, "execute_healing")
	defer span.End()
	
	span.SetAttributes(
		attribute.String("issue_id", issue.ID),
		attribute.String("action", string(action)),
		attribute.String("component_id", issue.ComponentID),
	)
	
	startTime := time.Now()
	
	// Check circuit breaker
	if h.isCircuitBroken(issue.ComponentID) {
		log.WithFields(log.Fields{
			"component_id": issue.ComponentID,
			"action":       action,
		}).Warn("Circuit breaker tripped, skipping healing action")
		
		return &HealingResult{
			IssueID:       issue.ID,
			Action:        action,
			ComponentID:   issue.ComponentID,
			ComponentType: issue.ComponentType,
			Success:       false,
			Error:         "Circuit breaker tripped - too many consecutive failures",
			DurationMs:    time.Since(startTime).Milliseconds(),
			Timestamp:     time.Now(),
		}
	}
	
	// Check max restart attempts
	if h.exceedsMaxAttempts(issue.ComponentID) {
		log.WithFields(log.Fields{
			"component_id": issue.ComponentID,
			"action":       action,
			"attempts":     h.healingAttempts[issue.ComponentID],
		}).Warn("Max healing attempts exceeded")
		
		h.tripCircuitBreaker(issue.ComponentID)
		
		return &HealingResult{
			IssueID:       issue.ID,
			Action:        HealingActionCircuitBreak,
			ComponentID:   issue.ComponentID,
			ComponentType: issue.ComponentType,
			Success:       false,
			Error:         fmt.Sprintf("Max attempts (%d) exceeded", h.config.MaxRestartAttempts),
			DurationMs:    time.Since(startTime).Milliseconds(),
			Timestamp:     time.Now(),
		}
	}
	
	// Apply exponential backoff
	if !h.canAttemptHealing(issue.ComponentID) {
		backoffDuration := h.calculateBackoff(issue.ComponentID)
		log.WithFields(log.Fields{
			"component_id": issue.ComponentID,
			"backoff":      backoffDuration,
		}).Debug("Backoff period active, delaying healing")
		
		return &HealingResult{
			IssueID:       issue.ID,
			Action:        action,
			ComponentID:   issue.ComponentID,
			ComponentType: issue.ComponentType,
			Success:       false,
			Error:         fmt.Sprintf("Backoff period active: %s remaining", backoffDuration),
			DurationMs:    time.Since(startTime).Milliseconds(),
			Timestamp:     time.Now(),
		}
	}
	
	// Execute the healing action
	var err error
	var details map[string]interface{}
	
	switch action {
	case HealingActionRestartAgent:
		err, details = h.restartAgent(ctx, issue.ComponentID)
	case HealingActionRestartConsumer:
		err, details = h.restartConsumer(ctx, issue.ComponentID)
	case HealingActionFixProtocol:
		err, details = h.fixProtocol(ctx, issue)
	case HealingActionReplayMessages:
		err, details = h.replayMessages(ctx, issue)
	case HealingActionScaleUp:
		err, details = h.scaleUpConsumers(ctx, issue)
	case HealingActionAlert:
		err, details = h.sendAlert(ctx, issue)
	default:
		err = fmt.Errorf("unknown healing action: %s", action)
		details = make(map[string]interface{})
	}
	
	// A skip is not an attempt. Grading it with `err == nil` would clear the circuit
	// breaker and the backoff as though a repair had happened; recording it as a failure
	// would count a no-op toward MaxRestartAttempts. Neither is true of an action that
	// was never tried, so a skip records nothing here.
	skipped := errors.Is(err, ErrHealingSkipped)
	if !skipped {
		h.recordAttempt(issue.ComponentID, err == nil)
	}
	
	result := &HealingResult{
		IssueID:       issue.ID,
		Action:        action,
		ComponentID:   issue.ComponentID,
		ComponentType: issue.ComponentType,
		Success:       err == nil,
		Skipped:       skipped,
		DurationMs:    time.Since(startTime).Milliseconds(),
		Timestamp:     time.Now(),
		Details:       details,
	}
	
	if err != nil {
		result.Error = err.Error()
	}
	
	return result
}

// restartAgent restarts a failed agent
func (h *Healer) restartAgent(ctx context.Context, componentID string) (error, map[string]interface{}) {
	log.WithField("component_id", componentID).Info("🔄 Attempting to restart agent")
	
	details := map[string]interface{}{
		"component_id": componentID,
		"method":       "restart_agent",
	}
	
	// Check if AgentManager is available
	if h.agentManager == nil {
		log.Error("❌ AgentManager not available for agent restart")
		return fmt.Errorf("agent manager not initialized"), details
	}
	
	// Extract agent name from component ID (format: "agent:name")
	// Examples: "agent:intent", "agent:resolver", "agent:discovery"
	agentName := componentID
	if len(componentID) > 6 && componentID[:6] == "agent:" {
		agentName = componentID[6:] // Remove "agent:" prefix
	}
	
	// Add agent name to details
	details["agent_name"] = agentName
	
	// Restart the agent using AgentManager
	if err := h.agentManager.RestartAgent(ctx, agentName); err != nil {
		log.WithFields(log.Fields{
			"component_id": componentID,
			"agent_name":   agentName,
			"error":        err,
		}).Error("❌ Failed to restart agent")
		
		details["error"] = err.Error()
		return err, details
	}
	
	// Get restart count for details
	if agent, exists := h.agentManager.GetAgent(agentName); exists {
		agent.mu.RLock()
		details["restart_count"] = agent.RestartCount
		details["last_restart"] = agent.LastRestart.Format(time.RFC3339)
		agent.mu.RUnlock()
	}
	
	log.WithFields(log.Fields{
		"component_id": componentID,
		"agent_name":   agentName,
	}).Info("✅ Agent restarted successfully")
	
	return nil, details
}

// ErrHealingSkipped marks a healing action the healer DECLINED TO ATTEMPT, as distinct
// from one it attempted and failed. It is returned wrapped, so callers test it with
// errors.Is rather than by string match.
//
// It exists because `nil` was being used to mean two incompatible things: "repaired it"
// and "there was nothing I could do here". ExecuteHealing grades `Success: err == nil`,
// and Agent.triggerHealing (sentinel.go) deletes the issue whenever a result is
// successful — so a component the healer has no restart path for had its issue closed
// and logged as "✅ Healing action successful", resolved by nobody.
var ErrHealingSkipped = errors.New("healing action skipped")

// restartConsumer restarts a Kafka consumer group for one of the new-architecture topics.
//
// There is NO MCP connector branch. This comment used to claim one ("restarts a Kafka
// consumer or MCP connector"), which is how a reader concludes MCP healing is covered
// when nothing of the sort exists. Every component this function does not recognise —
// MCP connectors included — is skipped, and the skip is reported as ErrHealingSkipped so
// it can never be mistaken for a repair.
func (h *Healer) restartConsumer(ctx context.Context, componentID string) (error, map[string]interface{}) {
	log.WithField("component_id", componentID).Info("🔄 Attempting to restart consumer group")
	
	details := map[string]interface{}{
		"component_id": componentID,
		"method":       "restart_consumer",
		"timestamp":    time.Now(),
	}
	
	// ComponentID format: "task.assignments", "task.results", etc.
	var topic string
	details["architecture"] = "unknown"
	
	// Check if it's a new architecture topic
	newArchTopics := []string{"task.assignments", "task.results", "pipeline.domain.events", "pipeline.agent.telemetry"}
	isNewArch := false
	for _, newTopic := range newArchTopics {
		if componentID == newTopic {
			topic = componentID
			isNewArch = true
			break
		}
	}
	
	// For new architecture, workers are stateless and self-healing via Kafka rebalancing
	if isNewArch {
		details["topic"] = topic
		details["architecture"] = "new"
		log.WithField("topic", topic).Info("🔄 New architecture - workers self-heal via Kafka rebalancing")
		// Kafka consumer groups automatically rebalance, no manual restart needed
		details["action"] = "kafka_auto_rebalance"
		details["success"] = true
		return nil, details
	}
	
	// Unknown component
	log.WithField("component_id", componentID).Warn("⚠️  Unknown component ID, skipping restart")
	details["action"] = "skipped"
	details["reason"] = "unknown_component"
	// Deliberately not nil: nothing was restarted. `nil` here graded the no-op as a
	// successful heal, and the sentinel then deleted the issue.
	return fmt.Errorf("%w: no restart path for component %q", ErrHealingSkipped, componentID), details
}

// fixProtocol attempts to fix protocol mismatches (Avro/JSON)
func (h *Healer) fixProtocol(ctx context.Context, issue *Issue) (error, map[string]interface{}) {
	log.WithFields(log.Fields{
		"issue_id":     issue.ID,
		"component_id": issue.ComponentID,
	}).Info("🔧 Attempting to fix protocol mismatch")
	
	topic, _ := issue.Metadata["topic"].(string)
	
	details := map[string]interface{}{
		"component_id": issue.ComponentID,
		"topic":        topic,
		"method":       "fix_protocol",
	}
	
	// TODO: Implement protocol conversion
	// Best-effort implementation:
	// Many of our "Avro" messages use Confluent wire-format header (0x00 + schema id)
	// followed by a JSON payload. JSON-only consumers will fail to deserialize those.
	// We convert DLQ messages by stripping the 5-byte header when present, then replay.

	if h.kafkaManager == nil {
		return fmt.Errorf("kafka manager not initialized"), details
	}

	targetTopic := strings.TrimSpace(topic)
	if targetTopic == "" {
		// fall back to component id if detector didn't provide topic
		targetTopic = strings.TrimSpace(issue.ComponentID)
	}
	if targetTopic == "" {
		return fmt.Errorf("missing target topic"), details
	}

	dlqTopic := targetTopic + ".dlq"
	if issue.Metadata != nil {
		if v, ok := issue.Metadata["dlq_topic"].(string); ok && strings.TrimSpace(v) != "" {
			dlqTopic = strings.TrimSpace(v)
		}
	}

	details["topic"] = targetTopic
	details["dlq_topic"] = dlqTopic

	// Reuse the DLQ replay path but apply a transform to strip the Confluent header.
	maxReplay := 100
	if v := strings.TrimSpace(os.Getenv("SENTINEL_PROTOCOL_FIX_MAX_MESSAGES")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxReplay = n
		}
	}
	details["max_replay"] = maxReplay

	// Brokers and SASL/TLS both come from the manager's resolved config. This
	// replaces a local split loop whose fallback hardcoded "kafka:29092" — a
	// hostname that exists only inside the bundled compose stack, so against a
	// customer-managed cluster it dialed the wrong place instead of erroring.
	security := h.kafkaSecurity()

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V3_3_0_0
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	saramaCfg.Consumer.Group.Session.Timeout = 30 * time.Second
	saramaCfg.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	groupID := stableGroupID("sentinel-protocol-fix", dlqTopic)
	if err := saramaauth.Apply(saramaCfg, security); err != nil {
		return fmt.Errorf("invalid Kafka security configuration: %w", err), details
	}
	cg, err := sarama.NewConsumerGroup(security.Brokers, groupID, saramaCfg)
	if err != nil {
		return fmt.Errorf("failed to create protocol-fix consumer group: %w", err), details
	}
	defer func() { _ = cg.Close() }()

	consumeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	var replayed int64
	var failed int64
	var stripped int64
	var lastErr atomic.Value

	handler := &protocolFixHandler{
		kafkaManager:  h.kafkaManager,
		targetTopic:   targetTopic,
		maxMessages:   int64(maxReplay),
		cancel:        cancel,
		replayed:      &replayed,
		failed:        &failed,
		stripped:      &stripped,
		lastErr:       &lastErr,
	}

	if err := cg.Consume(consumeCtx, []string{dlqTopic}, handler); err != nil && !errorsIsContextDone(err) {
		details["consume_error"] = err.Error()
	}

	details["replayed"] = atomic.LoadInt64(&replayed)
	details["failed"] = atomic.LoadInt64(&failed)
	details["stripped_headers"] = atomic.LoadInt64(&stripped)
	if v := lastErr.Load(); v != nil {
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			details["last_error"] = s
		}
	}

	if atomic.LoadInt64(&replayed) == 0 {
		return fmt.Errorf("no dlq messages replayed for protocol fix"), details
	}
	if atomic.LoadInt64(&failed) > 0 {
		return fmt.Errorf("protocol fix completed with failures"), details
	}

	return nil, details
}

// replayMessages replays messages from a DLQ
func (h *Healer) replayMessages(ctx context.Context, issue *Issue) (error, map[string]interface{}) {
	log.WithFields(log.Fields{
		"issue_id":     issue.ID,
		"component_id": issue.ComponentID,
	}).Info("📤 Attempting to replay DLQ messages")
	
	details := map[string]interface{}{
		"component_id": issue.ComponentID,
		"method":       "replay_messages",
	}
	
	if h.kafkaManager == nil {
		return fmt.Errorf("kafka manager not initialized"), details
	}

	dlqTopic := strings.TrimSpace(issue.ComponentID)
	if issue.Metadata != nil {
		if v, ok := issue.Metadata["dlq_topic"].(string); ok && strings.TrimSpace(v) != "" {
			dlqTopic = strings.TrimSpace(v)
		} else if v, ok := issue.Metadata["topic"].(string); ok && strings.TrimSpace(v) != "" {
			// Best-effort fallback: sometimes detectors store topic here.
			dlqTopic = strings.TrimSpace(v)
		}
	}
	if dlqTopic == "" {
		return fmt.Errorf("missing dlq topic"), details
	}

	derivedOriginal := ""
	if strings.HasSuffix(dlqTopic, ".dlq") {
		derivedOriginal = strings.TrimSuffix(dlqTopic, ".dlq")
	}

	maxReplay := 100
	if v := strings.TrimSpace(os.Getenv("SENTINEL_DLQ_REPLAY_MAX_MESSAGES")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxReplay = n
		}
	}
	if issue.Metadata != nil {
		// If the detector provided a count, cap to that (so we don't block on an empty DLQ).
		if raw, ok := issue.Metadata["dlq_message_count"]; ok && raw != nil {
			switch tv := raw.(type) {
			case int64:
				if tv > 0 && int(tv) < maxReplay {
					maxReplay = int(tv)
				}
			case float64:
				if tv > 0 && int(tv) < maxReplay {
					maxReplay = int(tv)
				}
			}
		}
	}
	details["dlq_topic"] = dlqTopic
	if derivedOriginal != "" {
		details["derived_original_topic"] = derivedOriginal
	}
	details["max_replay"] = maxReplay

	// Brokers and SASL/TLS both come from the manager's resolved config. This
	// replaces a local split loop whose fallback hardcoded "kafka:29092" — a
	// hostname that exists only inside the bundled compose stack, so against a
	// customer-managed cluster it dialed the wrong place instead of erroring.
	security := h.kafkaSecurity()

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V3_3_0_0
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	saramaCfg.Consumer.Group.Session.Timeout = 30 * time.Second
	saramaCfg.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	groupID := stableGroupID("sentinel-dlq-replay", dlqTopic)
	if err := saramaauth.Apply(saramaCfg, security); err != nil {
		return fmt.Errorf("invalid Kafka security configuration: %w", err), details
	}
	cg, err := sarama.NewConsumerGroup(security.Brokers, groupID, saramaCfg)
	if err != nil {
		return fmt.Errorf("failed to create dlq replay consumer group: %w", err), details
	}
	defer func() { _ = cg.Close() }()

	consumeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	var replayed int64
	var failed int64
	var lastErr atomic.Value

	handler := &dlqReplayHandler{
		kafkaManager:     h.kafkaManager,
		defaultTarget:    derivedOriginal,
		maxMessages:      int64(maxReplay),
		cancel:           cancel,
		replayed:         &replayed,
		failed:           &failed,
		lastErr:          &lastErr,
	}

	// Consume once; handler cancels when maxReplay is reached or timeout elapses.
	if err := cg.Consume(consumeCtx, []string{dlqTopic}, handler); err != nil && !errorsIsContextDone(err) {
		details["consume_error"] = err.Error()
	}

	details["replayed"] = atomic.LoadInt64(&replayed)
	details["failed"] = atomic.LoadInt64(&failed)
	if v := lastErr.Load(); v != nil {
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			details["last_error"] = s
		}
	}

	if atomic.LoadInt64(&replayed) == 0 {
		return fmt.Errorf("no dlq messages replayed"), details
	}
	if atomic.LoadInt64(&failed) > 0 {
		return fmt.Errorf("dlq replay completed with failures"), details
	}

	return nil, details
}

// scaleUpConsumers scales up Kafka consumers for a topic
func (h *Healer) scaleUpConsumers(ctx context.Context, issue *Issue) (error, map[string]interface{}) {
	log.WithFields(log.Fields{
		"issue_id":     issue.ID,
		"component_id": issue.ComponentID,
	}).Info("📈 Attempting to scale up consumers")
	
	details := map[string]interface{}{
		"component_id": issue.ComponentID,
		"method":       "scale_up",
	}
	
	// Use the existing Consumer Registry API exposed by backend-orchestrator
	// (registered at /api/v1/consumers) to apply scaling decisions.
	topic := strings.TrimSpace(issue.ComponentID)
	if issue.Metadata != nil {
		if v, ok := issue.Metadata["topic"].(string); ok && strings.TrimSpace(v) != "" {
			topic = strings.TrimSpace(v)
		}
	}
	if topic == "" {
		return fmt.Errorf("missing topic for scaling"), details
	}
	details["topic"] = topic

	escaped := url.PathEscape(topic)
	u := fmt.Sprintf("http://localhost:8080/api/v1/consumers/scaling/%s/apply", escaped)

	req, _ := http.NewRequestWithContext(ctx, "POST", u, nil)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		details["error"] = err.Error()
		return fmt.Errorf("failed to call consumer scaling api: %w", err), details
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	details["http_status"] = resp.StatusCode
	if len(body) > 0 {
		details["response_body"] = string(body)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("consumer scaling api returned status %d", resp.StatusCode), details
	}

	// Best-effort parse for "applied" flag and decision info
	var parsed map[string]interface{}
	if json.Unmarshal(body, &parsed) == nil {
		if applied, ok := parsed["applied"].(bool); ok {
			details["applied"] = applied
			if !applied {
				return fmt.Errorf("scaling decision resulted in no action"), details
			}
		}
	}

	return nil, details
}

// sendAlert sends an alert for critical issues
func (h *Healer) sendAlert(ctx context.Context, issue *Issue) (error, map[string]interface{}) {
	log.WithFields(log.Fields{
		"issue_id":     issue.ID,
		"severity":     issue.Severity,
		"component_id": issue.ComponentID,
		"description":  issue.Description,
	}).Error("🚨 CRITICAL ALERT: Manual intervention required")
	
	details := map[string]interface{}{
		"component_id": issue.ComponentID,
		"severity":     issue.Severity,
		"description":  issue.Description,
		"method":       "alert",
	}
	
	webhookURL := strings.TrimSpace(os.Getenv("SENTINEL_ALERT_WEBHOOK_URL"))
	if webhookURL == "" {
		// Fail closed: if alerting is configured as an action, it must have a real sink.
		return fmt.Errorf("alert webhook not configured (SENTINEL_ALERT_WEBHOOK_URL)"), details
	}

	payload := map[string]interface{}{
		"event_type":  "sentinel_alert",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"issue":       issue,
		"component_id": issue.ComponentID,
		"severity":    issue.Severity,
		"type":        issue.Type,
		"description": issue.Description,
		"metadata":    issue.Metadata,
	}
	b, _ := json.Marshal(payload)

	req, _ := http.NewRequestWithContext(ctx, "POST", webhookURL, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		details["error"] = err.Error()
		return fmt.Errorf("alert webhook request failed: %w", err), details
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	details["http_status"] = resp.StatusCode
	if len(respBody) > 0 {
		details["response_body"] = string(respBody)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("alert webhook returned status %d", resp.StatusCode), details
	}
	
	return nil, details
}

type dlqReplayHandler struct {
	kafkaManager  *kafka.Manager
	defaultTarget string
	maxMessages   int64
	cancel        context.CancelFunc
	replayed      *int64
	failed        *int64
	lastErr       *atomic.Value
}

func (h *dlqReplayHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *dlqReplayHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *dlqReplayHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// Stop when we've replayed enough
		if atomic.LoadInt64(h.replayed) >= h.maxMessages {
			if h.cancel != nil {
				h.cancel()
			}
			return nil
		}

		targetTopic := strings.TrimSpace(h.defaultTarget)
		headers := map[string]string{}
		for _, hh := range msg.Headers {
			if hh == nil {
				continue
			}
			k := strings.TrimSpace(string(hh.Key))
			v := ""
			if hh.Value != nil {
				v = string(hh.Value)
			}
			// Preserve trace context and metadata, but do not carry DLQ routing headers back.
			if k == "dlq_original_topic" {
				if strings.TrimSpace(v) != "" {
					targetTopic = strings.TrimSpace(v)
				}
				continue
			}
			if k == "dlq_error" {
				continue
			}
			if k != "" {
				headers[k] = v
			}
		}

		if strings.TrimSpace(targetTopic) == "" {
			atomic.AddInt64(h.failed, 1)
			if h.lastErr != nil {
				h.lastErr.Store("missing original topic for dlq message")
			}
			// Do not mark message; leave it in DLQ for manual inspection.
			continue
		}

		if h.kafkaManager == nil {
			atomic.AddInt64(h.failed, 1)
			if h.lastErr != nil {
				h.lastErr.Store("kafka manager not initialized")
			}
			continue
		}

		if err := h.kafkaManager.ProduceWithHeaders(targetTopic, msg.Key, msg.Value, headers); err != nil {
			atomic.AddInt64(h.failed, 1)
			if h.lastErr != nil {
				h.lastErr.Store(err.Error())
			}
			// Do NOT `continue`. Sarama offsets are high-water only: marking any
			// later message in this partition would advance the committed offset
			// PAST this failed (unmarked) one, dropping it from both the target
			// topic (produce failed) and the DLQ (offset advanced) — silent loss
			// in the one component whose job is DLQ recovery. Stop the replay run
			// here so the committed offset never passes an un-replayed message;
			// the message stays in the DLQ and the next replay run retries it.
			return nil
		}

		session.MarkMessage(msg, "replayed")
		atomic.AddInt64(h.replayed, 1)

		if atomic.LoadInt64(h.replayed) >= h.maxMessages {
			if h.cancel != nil {
				h.cancel()
			}
			return nil
		}
	}
	return nil
}

// stableGroupID builds a deterministic consumer-group id for a one-shot DLQ
// drain, keyed to the topic being drained.
//
// These group ids used to be suffixed with time.Now().UnixNano(), which broke
// the drain in two ways. A brand-new group has no committed offset, so
// Consumer.Offsets.Initial (OffsetOldest) sent every invocation back to the head
// of the DLQ: run N+1 re-replayed exactly the messages run N had already
// replayed, duplicating them downstream, and any message past maxReplay was
// never reached at all — a DLQ deeper than the cap could not drain no matter how
// many times the healer fired. The handlers' session.MarkMessage calls were
// recording progress into a group nobody would ever read again.
//
// It also registered a new consumer group per invocation on the broker. Against
// a customer-managed (BYO) Kafka that is unbounded metadata growth, retained for
// offsets.retention.minutes and visible in every consumer-group listing.
//
// Keying the group to the topic makes those commits load-bearing: each run
// resumes where the previous one stopped, so repeated healing walks the DLQ
// forward instead of re-treading its first page.
//
// The topic can originate from issue metadata, so it is sanitized to the Kafka
// topic charset and truncated rather than trusted verbatim.
//
// The finished id is qualified through kafkaclient.Group so it lands under the
// same KAFKA_TOPIC_PREFIX namespace as the topics it reads. On a shared cluster
// the customer grants topics and groups in one ACL set; a group left outside the
// namespace is denied at join, and a denied join surfaces as a drain that quietly
// does nothing rather than as an error.
func stableGroupID(prefix, topic string) string {
	const maxTopicLen = 200
	topic = strings.TrimSpace(topic)

	var b strings.Builder
	for i := 0; i < len(topic) && b.Len() < maxTopicLen; i++ {
		c := topic[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '-':
			b.WriteByte(c)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		// No usable topic: fall back to the bare prefix rather than emitting a
		// dangling separator. Still stable, which is the property that matters.
		return kafkaclient.Group(prefix)
	}
	return kafkaclient.Group(prefix + "-" + b.String())
}

func errorsIsContextDone(err error) bool {
	if err == nil {
		return false
	}
	return err == context.Canceled || err == context.DeadlineExceeded
}

type protocolFixHandler struct {
	kafkaManager *kafka.Manager
	targetTopic  string
	maxMessages  int64
	cancel       context.CancelFunc
	replayed     *int64
	failed       *int64
	stripped     *int64
	lastErr      *atomic.Value
}

func (h *protocolFixHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *protocolFixHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *protocolFixHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if atomic.LoadInt64(h.replayed) >= h.maxMessages {
			if h.cancel != nil {
				h.cancel()
			}
			return nil
		}

		// Strip Confluent "Avro" header if present (magic byte 0x00 + 4-byte schema id).
		val := msg.Value
		if len(val) > 5 && val[0] == 0x00 {
			val = val[5:]
			atomic.AddInt64(h.stripped, 1)
		}

		headers := map[string]string{}
		for _, hh := range msg.Headers {
			if hh == nil {
				continue
			}
			k := strings.TrimSpace(string(hh.Key))
			v := ""
			if hh.Value != nil {
				v = string(hh.Value)
			}
			// Do not preserve DLQ routing headers.
			if k == "dlq_original_topic" || k == "dlq_error" {
				continue
			}
			if k != "" {
				headers[k] = v
			}
		}

		if h.kafkaManager == nil {
			atomic.AddInt64(h.failed, 1)
			if h.lastErr != nil {
				h.lastErr.Store("kafka manager not initialized")
			}
			continue
		}

		if err := h.kafkaManager.ProduceWithHeaders(h.targetTopic, msg.Key, val, headers); err != nil {
			atomic.AddInt64(h.failed, 1)
			if h.lastErr != nil {
				h.lastErr.Store(err.Error())
			}
			continue
		}

		session.MarkMessage(msg, "protocol_fixed")
		atomic.AddInt64(h.replayed, 1)

		if atomic.LoadInt64(h.replayed) >= h.maxMessages {
			if h.cancel != nil {
				h.cancel()
			}
			return nil
		}
	}
	return nil
}

// Helper methods for backoff and circuit breaking

func (h *Healer) canAttemptHealing(componentID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	lastAttempt, exists := h.lastAttempt[componentID]
	if !exists {
		return true
	}
	
	backoffDuration := h.calculateBackoff(componentID)
	return time.Since(lastAttempt) >= backoffDuration
}

func (h *Healer) calculateBackoff(componentID string) time.Duration {
	h.mu.RLock()
	attempts := h.healingAttempts[componentID]
	h.mu.RUnlock()
	
	// Exponential backoff: base * 2^attempts
	backoff := time.Duration(float64(h.config.RestartBackoffBase) * math.Pow(2, float64(attempts)))
	
	if backoff > h.config.RestartBackoffMax {
		backoff = h.config.RestartBackoffMax
	}
	
	return backoff
}

func (h *Healer) recordAttempt(componentID string, success bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	h.lastAttempt[componentID] = time.Now()
	
	if success {
		// Reset on success
		delete(h.healingAttempts, componentID)
		delete(h.circuitBreakers, componentID)
	} else {
		// Increment on failure
		h.healingAttempts[componentID]++
		
		// Update circuit breaker
		cb, exists := h.circuitBreakers[componentID]
		if !exists {
			cb = &CircuitBreaker{}
			h.circuitBreakers[componentID] = cb
		}
		cb.FailureCount++
		cb.LastFailure = time.Now()
	}
}

func (h *Healer) exceedsMaxAttempts(componentID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	attempts := h.healingAttempts[componentID]
	return attempts >= h.config.MaxRestartAttempts
}

func (h *Healer) isCircuitBroken(componentID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	cb, exists := h.circuitBreakers[componentID]
	if !exists {
		return false
	}
	
	return cb.Tripped
}

func (h *Healer) tripCircuitBreaker(componentID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	cb, exists := h.circuitBreakers[componentID]
	if !exists {
		cb = &CircuitBreaker{}
		h.circuitBreakers[componentID] = cb
	}
	
	cb.Tripped = true
	cb.TrippedAt = time.Now()
	
	log.WithField("component_id", componentID).Error("🚫 Circuit breaker tripped")
}

func (h *Healer) resetCircuitBreakers() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.mu.Lock()
			now := time.Now()
			for componentID, cb := range h.circuitBreakers {
				if cb.Tripped && now.Sub(cb.TrippedAt) >= h.config.CircuitBreakerCooldown {
					log.WithField("component_id", componentID).Info("🔓 Circuit breaker reset")
					cb.Tripped = false
					cb.FailureCount = 0
				}
			}
			h.mu.Unlock()
		}
	}
}

