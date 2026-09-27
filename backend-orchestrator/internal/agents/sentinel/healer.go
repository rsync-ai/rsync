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
	"strings"
	"sync"
	"time"

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
	
	// ComponentID is a topic name for the one platform topic listed below.
	var topic string
	details["architecture"] = "unknown"
	
	// Check if it's a new architecture topic
	newArchTopics := []string{"pipeline.domain.events"}
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

