package sentinel

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

var sentinelTracer = otel.Tracer("sentinel-agent")

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
		activeIssues: make(map[string]*Issue),
		ctx:          ctx,
		cancel:       cancel,
		startTime:    time.Now(),
	}

	// Initialize sub-components
	agent.logger = NewAuditLogger(db, config)
	agent.healthMonitor = NewHealthMonitor(kafkaManager, db, config, agent.logger)
	// An evicted component's issue is deleted from the table by the monitor; this drops
	// the matching entry from the map below, which is what decides whether a later
	// recurrence is persisted at all (handleDetectedIssue returns early for a repeat).
	agent.healthMonitor.onComponentsEvicted = agent.forgetIssuesForComponents
	// Built here rather than in NewHealthMonitor: the Redis probe holds a client, and a
	// monitor built directly (as the tests do) should not open one it never closes.
	agent.healthMonitor.serviceProbes = serviceProbesFromEnv(os.Getenv)
	agent.issueDetector = NewIssueDetector(config, agent.logger)
	agent.healer = NewHealer(kafkaManager, db, config, agent.logger, agentManager)
	agent.predictor = NewAnomalyPredictor(config, agent.logger)

	return agent
}

// Start starts the Sentinel Agent
func (a *Agent) Start() error {
	log.Info("🛡️  Starting Sentinel Agent (System Health & Auto-Healing)")

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
	a.wg.Add(1)
	go a.issueDetectionLoop()

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

	components := a.snapshotComponents()

	// Run detection
	issues := a.issueDetector.DetectIssues(ctx, components)

	// Process detected issues
	for _, issue := range issues {
		a.handleDetectedIssue(ctx, issue)
	}

	// Close whatever the components that are healthy again had open.
	a.resolveRecoveredIssues(ctx, components)

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

// snapshotComponents returns every component the detector should consider: the
// HealthMonitor's view, copied so the detector can read fields the polling loops are
// still writing.
//
// This used to be the union of that view and a second map, Agent.components, filled from
// agent heartbeats on rsync.agents.heartbeat. Nothing produced that topic — the only
// publisher was the executor agent's, and its Start() was never called — so the second
// map was always empty. Both are gone.
func (a *Agent) snapshotComponents() []*ComponentHealth {
	if a.healthMonitor == nil {
		return nil
	}
	return a.healthMonitor.snapshotComponents()
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

	// Tell a human. Until this call existed, every IssueDetector finding went to
	// the database, the audit log and the healer and to nobody at all: the two
	// publishers in this package (emitCDCIssue, emitBatchIssue) cover the pipeline
	// lane only, and infrastructure_down and missing_heartbeat have no other
	// producer anywhere, so a downed Kafka or a worker that stopped sending
	// heartbeats was silent unless somebody happened to be looking at
	// /admin/health.
	//
	// Safe to put here rather than in detectIssues: this is the only caller, and it
	// has already returned above for a repeat occurrence, so an issue that persists
	// across ticks publishes once. The pipeline lane reaches publishSentinelAlert by
	// its own path and never comes through here, so this cannot double-publish.
	a.publishInstanceIssueAlert(issue)

	// Trigger healing
	go a.triggerHealing(issue)
}

// forgetIssuesForComponents drops active issues whose component has been evicted.
//
// Called by the health monitor after it deletes the rows, so the two stores agree.
// Without it the Agent would still hold the issue, handleDetectedIssue would take its
// "already active" early return on the next detection, and the row would never come
// back — a fault against a component that came back would be invisible in the table
// the UI reads for the rest of the process's life.
func (a *Agent) forgetIssuesForComponents(componentIDs []string) {
	if len(componentIDs) == 0 {
		return
	}
	gone := make(map[string]struct{}, len(componentIDs))
	for _, id := range componentIDs {
		gone[id] = struct{}{}
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	for id, issue := range a.activeIssues {
		if _, ok := gone[issue.ComponentID]; ok {
			delete(a.activeIssues, id)
		}
	}
}

// recoverableIssueTypes are the IssueDetector findings that a component's own health
// row decides: each is filed because that row went unhealthy, so the row going healthy
// again is the end of it.
var recoverableIssueTypes = []IssueType{
	IssueTypeInfrastructureDown,
	IssueTypeConnectorDown,
	IssueTypeMissingHeartbeat,
	IssueTypeConsumerGroupClosed,
}

// resolveRecoveredIssues closes the findings of every component that is healthy again.
//
// Nothing did before. A finding left a.activeIssues only when a heal succeeded or its
// component was evicted, and its sentinel_active_issues row only on eviction — so a
// Redis that came back after ten minutes stayed "down" in the issues table indefinitely,
// and because handleDetectedIssue returns early for an issue still in activeIssues, the
// NEXT outage of the same service was never persisted or alerted at all.
//
// Only HealthStatusHealthy resolves. Degraded is a service between probe misses
// (service_probes.go), unknown is "could not find out", and neither is a recovery.
//
// The row is deleted rather than stamped resolved_at, as the CDC, batch and WAL-watchdog
// lanes resolve theirs: persistIssueToDB's upsert never clears resolved_at, so a stamped
// row would read resolved through the next outage. The delete runs over every candidate
// id each tick, not just those found in memory, so rows a previous process left behind
// are closed too.
func (a *Agent) resolveRecoveredIssues(ctx context.Context, components []*ComponentHealth) {
	var ids []string
	for _, c := range components {
		if c == nil || c.Status != HealthStatusHealthy {
			continue
		}
		for _, t := range recoverableIssueTypes {
			ids = append(ids, generateIssueID(c.ComponentID, t))
		}
	}
	if len(ids) == 0 {
		return
	}

	now := time.Now()
	var resolved []*Issue
	a.mu.Lock()
	for _, id := range ids {
		if issue, ok := a.activeIssues[id]; ok {
			issue.ResolvedAt = &now
			delete(a.activeIssues, id)
			a.issuesResolved++
			resolved = append(resolved, issue)
		}
	}
	a.mu.Unlock()

	for _, issue := range resolved {
		log.WithFields(log.Fields{
			"issue_id":     issue.ID,
			"issue_type":   issue.Type,
			"component_id": issue.ComponentID,
		}).Info("✅ Issue resolved — component is healthy again")
	}

	a.deleteIssueRows(ctx, ids)
}

// deleteIssueRows removes the given issue ids from sentinel_active_issues.
func (a *Agent) deleteIssueRows(ctx context.Context, ids []string) {
	if a.db == nil || len(ids) == 0 {
		return
	}
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	query := fmt.Sprintf(`DELETE FROM sentinel_active_issues WHERE id IN (%s)`, strings.Join(placeholders, ", "))
	if _, err := a.db.ExecContext(ctx, query, args...); err != nil {
		log.WithError(err).Debug("Failed to delete resolved issues")
	}
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
	} else if result.Success && action == HealingActionAlert {
		// Delivered, not repaired. An alert fixes nothing, so the issue stays open until
		// resolveRecoveredIssues sees the component healthy again. Resolving it here
		// dropped it from activeIssues while the service was still down, and the next
		// detection after the cooldown filed it as new and alerted again every five
		// minutes for the length of the outage.
		log.WithFields(log.Fields{
			"issue_id":     issue.ID,
			"component_id": issue.ComponentID,
			"trace_id":     traceID,
			"span_id":      spanID,
		}).Info("📣 Alert delivered — issue left open until the component recovers")
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
