package sentinel

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

var sentinelTracer = otel.Tracer("sentinel-agent")

const (
	HeartbeatTopic = "rsync.agents.heartbeat"
	AuditTopic     = "rsync.sentinel.audit"
)

// Agent is the main Sentinel Agent that monitors and heals the system
type Agent struct {
	config       *SentinelConfig
	kafkaManager *kafka.Manager
	db           *sql.DB
	agentManager *AgentManager

	// Sub-components
	healthMonitor *HealthMonitor
	issueDetector *IssueDetector
	healer        *Healer
	logger        *AuditLogger
	predictor     *AnomalyPredictor

	// State
	components   map[string]*ComponentHealth
	activeIssues map[string]*Issue
	mu           sync.RWMutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Metrics
	startTime      time.Time
	issuesDetected int64
	issuesResolved int64
	healingActions int64
}

// NewAgent creates a new Sentinel Agent
func NewAgent(kafkaManager *kafka.Manager, db *sql.DB, config *SentinelConfig, agentManager *AgentManager) *Agent {
	if config == nil {
		config = DefaultSentinelConfig()
	}

	// Create AgentManager if not provided
	if agentManager == nil {
		log.Warn("⚠️  No AgentManager provided, creating new instance (agent restart will not work)")
		agentManager = NewAgentManager()
	}

	ctx, cancel := context.WithCancel(context.Background())

	agent := &Agent{
		config:       config,
		kafkaManager: kafkaManager,
		db:           db,
		agentManager: agentManager,
		components:   make(map[string]*ComponentHealth),
		activeIssues: make(map[string]*Issue),
		ctx:          ctx,
		cancel:       cancel,
		startTime:    time.Now(),
	}

	// Initialize sub-components
	agent.logger = NewAuditLogger(kafkaManager, db, config)
	agent.healthMonitor = NewHealthMonitor(kafkaManager, db, config, agent.logger)
	agent.issueDetector = NewIssueDetector(config, agent.logger)
	agent.healer = NewHealer(kafkaManager, db, config, agent.logger, agentManager)
	agent.predictor = NewAnomalyPredictor(config, agent.logger)

	return agent
}

// Start starts the Sentinel Agent
func (a *Agent) Start() error {
	log.Info("🛡️  Starting Sentinel Agent (System Health & Auto-Healing)")

	// Start consuming heartbeats
	if err := a.kafkaManager.ConsumeWithContext(HeartbeatTopic, a.handleHeartbeat); err != nil {
		return fmt.Errorf("failed to start heartbeat consumer: %w", err)
	}
	log.Infof("✅ Sentinel listening for heartbeats on %s", HeartbeatTopic)

	// Start health monitor
	if err := a.healthMonitor.Start(a.ctx); err != nil {
		return fmt.Errorf("failed to start health monitor: %w", err)
	}
	log.Info("✅ Health Monitor started")

	// Start issue detector
	if err := a.issueDetector.Start(a.ctx); err != nil {
		return fmt.Errorf("failed to start issue detector: %w", err)
	}
	log.Info("✅ Issue Detector started")

	// Start healer
	if err := a.healer.Start(a.ctx); err != nil {
		return fmt.Errorf("failed to start healer: %w", err)
	}
	log.Info("✅ Auto-Healer started")

	// Start predictor
	if err := a.predictor.Start(a.ctx); err != nil {
		return fmt.Errorf("failed to start anomaly predictor: %w", err)
	}
	log.Info("✅ Anomaly Predictor started")

	// Start logger
	if err := a.logger.Start(a.ctx); err != nil {
		return fmt.Errorf("failed to start audit logger: %w", err)
	}
	log.Info("✅ Audit Logger started")

	// Start background loops
	a.wg.Add(3)
	go a.healthCheckLoop()
	go a.issueDetectionLoop()
	go a.heartbeatPublishLoop()

	log.Info("✅ Sentinel Agent fully operational")

	return nil
}

// Stop stops the Sentinel Agent
func (a *Agent) Stop() {
	log.Info("🛡️  Stopping Sentinel Agent...")

	a.cancel()
	a.wg.Wait()

	// Stop sub-components
	if a.healthMonitor != nil {
		a.healthMonitor.Stop()
	}
	if a.issueDetector != nil {
		a.issueDetector.Stop()
	}
	if a.healer != nil {
		a.healer.Stop()
	}
	if a.predictor != nil {
		a.predictor.Stop()
	}
	if a.logger != nil {
		a.logger.Stop()
	}

	log.Info("✅ Sentinel Agent stopped")
}

// handleHeartbeat processes incoming heartbeat messages from agents
func (a *Agent) handleHeartbeat(ctx context.Context, msg *sarama.ConsumerMessage) error {
	ctx, span := sentinelTracer.Start(ctx, "handle_heartbeat")
	defer span.End()

	// Parse heartbeat using smart deserialization
	var heartbeat AgentHeartbeat
	if err := kafka.SmartDeserialize(msg.Value, &heartbeat); err != nil {
		log.WithError(err).Warn("Failed to deserialize heartbeat message")
		return nil // Don't retry bad messages
	}

	span.SetAttributes(
		attribute.String("agent", heartbeat.Agent),
		attribute.String("status", heartbeat.Status),
	)

	// Update component health
	a.mu.Lock()
	componentID := fmt.Sprintf("agent:%s", heartbeat.Agent)
	health, exists := a.components[componentID]
	if !exists {
		health = &ComponentHealth{
			ComponentID:   componentID,
			ComponentType: ComponentTypeAgent,
			Metadata:      make(map[string]interface{}),
		}
		a.components[componentID] = health
	}

	// Update health from heartbeat
	health.LastHeartbeat = heartbeat.Timestamp
	health.MessagesProcessed = heartbeat.MessagesProcessed
	health.ErrorCount = heartbeat.ErrorCount
	health.ConsumerLag = heartbeat.ConsumerLag
	health.UpdatedAt = time.Now()

	// Map status
	switch heartbeat.Status {
	case "healthy":
		health.Status = HealthStatusHealthy
	case "degraded":
		health.Status = HealthStatusDegraded
	case "unhealthy":
		health.Status = HealthStatusUnhealthy
	default:
		health.Status = HealthStatusUnknown
	}

	a.mu.Unlock()

	log.WithFields(log.Fields{
		"agent":              heartbeat.Agent,
		"status":             heartbeat.Status,
		"messages_processed": heartbeat.MessagesProcessed,
		"error_count":        heartbeat.ErrorCount,
		"consumer_lag":       heartbeat.ConsumerLag,
	}).Debug("Received agent heartbeat")

	// Feed to health monitor for analysis
	a.healthMonitor.RecordHeartbeat(componentID, health)

	return nil
}

// healthCheckLoop periodically checks all components for issues
func (a *Agent) healthCheckLoop() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.config.HeartbeatCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.performHealthCheck()
		}
	}
}

// performHealthCheck checks all monitored components
func (a *Agent) performHealthCheck() {
	_, span := sentinelTracer.Start(a.ctx, "health_check")
	defer span.End()

	// Extract trace context for logging
	traceID := span.SpanContext().TraceID().String()
	spanID := span.SpanContext().SpanID().String()

	a.mu.RLock()
	components := make([]*ComponentHealth, 0, len(a.components))
	for _, c := range a.components {
		components = append(components, c)
	}
	a.mu.RUnlock()

	now := time.Now()
	var staleIDs []string

	for _, component := range components {
		// Evict components that have been dead long enough to be considered garbage
		if component.Status == HealthStatusDead && now.Sub(component.UpdatedAt) > a.config.StaleComponentTTL {
			staleIDs = append(staleIDs, component.ComponentID)
			continue
		}

		// Check for missing heartbeats
		if now.Sub(component.LastHeartbeat) > a.config.HeartbeatTimeout {
			if component.Status != HealthStatusDead {
				log.WithFields(log.Fields{
					"component_id":   component.ComponentID,
					"last_heartbeat": component.LastHeartbeat,
					"timeout":        a.config.HeartbeatTimeout,
					"trace_id":       traceID,
					"span_id":        spanID,
				}).Warn("🚨 Component heartbeat timeout")

				a.mu.Lock()
				component.Status = HealthStatusDead
				component.UpdatedAt = now
				a.mu.Unlock()

				// Report to health monitor with trace context
				a.healthMonitor.RecordHealthChange(component.ComponentID, component)

				// Log status change with trace context
				log.WithFields(log.Fields{
					"component_id": component.ComponentID,
					"status":       "dead",
					"trace_id":     traceID,
					"span_id":      spanID,
				}).Info("Component health changed")
			}
		}

		// Feed metrics to predictor
		a.predictor.RecordMetric(component.ComponentID, "consumer_lag", float64(component.ConsumerLag), now)
		a.predictor.RecordMetric(component.ComponentID, "error_count", float64(component.ErrorCount), now)
	}

	// Evict stale dead components to prevent unbounded map growth
	if len(staleIDs) > 0 {
		a.mu.Lock()
		for _, id := range staleIDs {
			delete(a.components, id)
		}
		a.mu.Unlock()

		log.WithFields(log.Fields{
			"evicted":  len(staleIDs),
			"trace_id": traceID,
		}).Info("Evicted stale dead components from sentinel")

		a.healthMonitor.EvictStaleComponents(staleIDs)
	}

	span.SetAttributes(
		attribute.Int("components_checked", len(components)),
		attribute.String("trace_id", traceID),
	)

	// Log health check summary with trace context
	log.WithFields(log.Fields{
		"components_checked": len(components),
		"trace_id":           traceID,
		"span_id":            spanID,
	}).Debug("Health check completed")
}

// issueDetectionLoop periodically runs issue detection
func (a *Agent) issueDetectionLoop() {
	defer a.wg.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.detectIssues()
		}
	}
}

// detectIssues runs issue detection across all components
func (a *Agent) detectIssues() {
	ctx, span := sentinelTracer.Start(a.ctx, "detect_issues")
	defer span.End()

	// Extract trace context for logging
	traceID := span.SpanContext().TraceID().String()
	spanID := span.SpanContext().SpanID().String()

	a.mu.RLock()
	components := make([]*ComponentHealth, 0, len(a.components))
	for _, c := range a.components {
		components = append(components, c)
	}
	a.mu.RUnlock()

	// Run detection
	issues := a.issueDetector.DetectIssues(ctx, components)

	// Process detected issues
	for _, issue := range issues {
		a.handleDetectedIssue(ctx, issue)
	}

	span.SetAttributes(
		attribute.Int("issues_detected", len(issues)),
		attribute.String("trace_id", traceID),
	)

	log.WithFields(log.Fields{
		"issues_detected": len(issues),
		"trace_id":        traceID,
		"span_id":         spanID,
	}).Debug("Issue detection completed")
}

// handleDetectedIssue processes a newly detected issue
func (a *Agent) handleDetectedIssue(ctx context.Context, issue *Issue) {
	// Extract trace context from the passed context
	span := trace.SpanFromContext(ctx)
	traceID := span.SpanContext().TraceID().String()
	spanID := span.SpanContext().SpanID().String()

	a.mu.Lock()
	existingIssue, exists := a.activeIssues[issue.ID]
	if exists {
		// Update occurrence count
		existingIssue.OccurrenceCount++
		existingIssue.LastOccurrence = time.Now()
		a.mu.Unlock()
		return
	}

	// New issue
	a.activeIssues[issue.ID] = issue
	a.issuesDetected++
	a.mu.Unlock()

	log.WithFields(log.Fields{
		"issue_id":     issue.ID,
		"issue_type":   issue.Type,
		"severity":     issue.Severity,
		"component_id": issue.ComponentID,
		"description":  issue.Description,
		"trace_id":     traceID,
		"span_id":      spanID,
	}).Warn("🚨 New issue detected")

	// Persist issue to database
	a.persistIssueToDB(ctx, issue)

	// Log to audit
	a.logger.LogIssueDetected(ctx, issue)

	// Trigger healing
	go a.triggerHealing(issue)
}

// triggerHealing triggers healing action for an issue
func (a *Agent) triggerHealing(issue *Issue) {
	ctx, span := sentinelTracer.Start(a.ctx, "trigger_healing")
	defer span.End()

	// Extract trace context for logging
	traceID := span.SpanContext().TraceID().String()
	spanID := span.SpanContext().SpanID().String()

	span.SetAttributes(
		attribute.String("issue_id", issue.ID),
		attribute.String("trace_id", traceID),
		attribute.String("issue_type", string(issue.Type)),
		attribute.String("component_id", issue.ComponentID),
	)

	// Determine healing action
	action := a.healer.DetermineAction(issue)
	if action == "" {
		log.WithFields(log.Fields{
			"issue_type": issue.Type,
			"trace_id":   traceID,
			"span_id":    spanID,
		}).Debug("No healing action for issue type")
		return
	}

	// Execute healing
	result := a.healer.ExecuteHealing(ctx, issue, action)

	a.healingActions++

	if result.Skipped {
		// Declined, not repaired: the healer had no path for this component. Nothing was
		// attempted, so nothing is resolved — the issue stays in activeIssues rather than
		// being closed by the attempt not to heal it.
		log.WithFields(log.Fields{
			"issue_id":     issue.ID,
			"action":       action,
			"component_id": issue.ComponentID,
			"reason":       result.Error,
			"trace_id":     traceID,
			"span_id":      spanID,
		}).Warn("⏭️  Healing action skipped — issue left open")
	} else if result.Success {
		log.WithFields(log.Fields{
			"issue_id":     issue.ID,
			"action":       action,
			"component_id": issue.ComponentID,
			"duration_ms":  result.DurationMs,
			"trace_id":     traceID,
			"span_id":      spanID,
		}).Info("✅ Healing action successful")

		// Mark issue as resolved
		a.mu.Lock()
		if activeIssue, exists := a.activeIssues[issue.ID]; exists {
			now := time.Now()
			activeIssue.ResolvedAt = &now
			delete(a.activeIssues, issue.ID)
			a.issuesResolved++
		}
		a.mu.Unlock()
	} else {
		log.WithFields(log.Fields{
			"issue_id":     issue.ID,
			"action":       action,
			"component_id": issue.ComponentID,
			"error":        result.Error,
			"trace_id":     traceID,
			"span_id":      spanID,
		}).Error("❌ Healing action failed")
	}

	// Log result
	a.logger.LogHealingResult(ctx, result)
}

// heartbeatPublishLoop publishes Sentinel's own heartbeat
func (a *Agent) heartbeatPublishLoop() {
	defer a.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.publishSentinelHeartbeat()
		}
	}
}

// publishSentinelHeartbeat publishes Sentinel's own health status
func (a *Agent) publishSentinelHeartbeat() {
	a.mu.RLock()
	activeIssueCount := len(a.activeIssues)
	componentCount := len(a.components)
	a.mu.RUnlock()

	status := "healthy"
	if activeIssueCount > 10 {
		status = "degraded"
	}
	if activeIssueCount > 50 {
		status = "unhealthy"
	}

	// TODO: Publish heartbeat to Kafka topic
	// heartbeat := &AgentHeartbeat{
	// 	Agent:             "sentinel",
	// 	Status:            status,
	// 	LastMessageAt:     time.Now().Format(time.RFC3339),
	// 	MessagesProcessed: a.issuesResolved,
	// 	ErrorCount:        a.issuesDetected - a.issuesResolved,
	// 	ConsumerLag:       int64(activeIssueCount),
	// 	Timestamp:         time.Now(),
	// }
	log.WithFields(log.Fields{
		"status":          status,
		"active_issues":   activeIssueCount,
		"components":      componentCount,
		"issues_detected": a.issuesDetected,
		"issues_resolved": a.issuesResolved,
		"healing_actions": a.healingActions,
	}).Debug("📡 Sentinel heartbeat")
}

// persistIssueToDB persists an issue to the database
func (a *Agent) persistIssueToDB(ctx context.Context, issue *Issue) {
	metadataJSON, _ := json.Marshal(issue.Metadata)

	_, err := a.db.ExecContext(ctx, `
		INSERT INTO sentinel_active_issues (
			id, type, severity, component_id, component_type,
			description, detected_at, occurrence_count, last_occurrence, metadata
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO UPDATE SET
			occurrence_count = sentinel_active_issues.occurrence_count + 1,
			last_occurrence = EXCLUDED.last_occurrence
	`,
		issue.ID, issue.Type, issue.Severity, issue.ComponentID, issue.ComponentType,
		issue.Description, issue.DetectedAt, issue.OccurrenceCount, issue.LastOccurrence,
		metadataJSON,
	)

	if err != nil {
		log.WithError(err).WithField("issue_id", issue.ID).Debug("Failed to persist issue to DB")
	}
}
