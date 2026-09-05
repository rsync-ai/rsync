package sentinel

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

// IssueDetector detects issues in the system
type IssueDetector struct {
	config *SentinelConfig
	logger *AuditLogger

	// Issue tracking for deduplication
	recentIssues map[string]time.Time
	mu           sync.RWMutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
}

// NewIssueDetector creates a new issue detector
func NewIssueDetector(config *SentinelConfig, logger *AuditLogger) *IssueDetector {
	return &IssueDetector{
		config:       config,
		logger:       logger,
		recentIssues: make(map[string]time.Time),
	}
}

// Start starts the issue detector
func (d *IssueDetector) Start(ctx context.Context) error {
	d.ctx, d.cancel = context.WithCancel(ctx)

	// Start cleanup goroutine for recent issues
	go d.cleanupRecentIssues()

	return nil
}

// Stop stops the issue detector
func (d *IssueDetector) Stop() {
	if d.cancel != nil {
		d.cancel()
	}
}

// DetectIssues analyzes component health and detects issues
func (d *IssueDetector) DetectIssues(ctx context.Context, components []*ComponentHealth) []*Issue {
	ctx, span := sentinelTracer.Start(ctx, "detect_issues")
	defer span.End()

	var issues []*Issue

	for _, component := range components {
		// Check for missing heartbeat
		if issue := d.detectMissingHeartbeat(component); issue != nil {
			if d.shouldReportIssue(issue) {
				issues = append(issues, issue)
			}
		}

		// Check for closed consumer groups
		if issue := d.detectClosedConsumerGroup(component); issue != nil {
			if d.shouldReportIssue(issue) {
				issues = append(issues, issue)
			}
		}

		// Check for high consumer lag
		if issue := d.detectHighConsumerLag(component); issue != nil {
			if d.shouldReportIssue(issue) {
				issues = append(issues, issue)
			}
		}

		// Check for component unhealthy status
		if issue := d.detectUnhealthyStatus(component); issue != nil {
			if d.shouldReportIssue(issue) {
				issues = append(issues, issue)
			}
		}
	}

	span.SetAttributes(attribute.Int("issues_found", len(issues)))

	return issues
}

// detectMissingHeartbeat detects when a component stops sending heartbeats
func (d *IssueDetector) detectMissingHeartbeat(component *ComponentHealth) *Issue {
	if component.ComponentType != ComponentTypeAgent {
		return nil // Only check agents for heartbeats
	}

	timeSinceHeartbeat := time.Since(component.LastHeartbeat)
	if timeSinceHeartbeat > d.config.HeartbeatTimeout {
		return &Issue{
			ID:            generateIssueID(component.ComponentID, IssueTypeMissingHeartbeat),
			Type:          IssueTypeMissingHeartbeat,
			Severity:      IssueSeverityCritical,
			ComponentID:   component.ComponentID,
			ComponentType: component.ComponentType,
			Description: fmt.Sprintf("Component has not sent heartbeat for %s (timeout: %s)",
				timeSinceHeartbeat.Round(time.Second), d.config.HeartbeatTimeout),
			DetectedAt:      time.Now(),
			OccurrenceCount: 1,
			LastOccurrence:  time.Now(),
			Metadata: map[string]interface{}{
				"last_heartbeat":       component.LastHeartbeat,
				"time_since_heartbeat": timeSinceHeartbeat.Seconds(),
				"timeout_seconds":      d.config.HeartbeatTimeout.Seconds(),
			},
		}
	}

	return nil
}

// detectClosedConsumerGroup detects when a Kafka consumer group is closed
func (d *IssueDetector) detectClosedConsumerGroup(component *ComponentHealth) *Issue {
	// Only check Kafka consumer components
	if component.ComponentType != ComponentTypeKafkaConsumer {
		return nil
	}

	// Check if metadata indicates consumer is closed
	if component.Metadata != nil {
		if issueType, ok := component.Metadata["issue_type"].(string); ok && issueType == "consumer_group_closed" {
			return &Issue{
				ID:              generateIssueID(component.ComponentID, IssueTypeConsumerGroupClosed),
				Type:            IssueTypeConsumerGroupClosed,
				Severity:        IssueSeverityCritical,
				ComponentID:     component.ComponentID,
				ComponentType:   component.ComponentType,
				Description:     fmt.Sprintf("Consumer group for %s is closed and not processing messages", component.ComponentID),
				DetectedAt:      time.Now(),
				OccurrenceCount: 1,
				LastOccurrence:  time.Now(),
				Metadata: map[string]interface{}{
					"topic":      component.ComponentID,
					"last_error": component.LastError,
				},
			}
		}
	}

	// Also check if status is unhealthy and last error mentions "closed"
	if component.Status == HealthStatusUnhealthy &&
		(component.LastError == "Consumer group closed" ||
			strings.Contains(strings.ToLower(component.LastError), "closed")) {
		return &Issue{
			ID:              generateIssueID(component.ComponentID, IssueTypeConsumerGroupClosed),
			Type:            IssueTypeConsumerGroupClosed,
			Severity:        IssueSeverityCritical,
			ComponentID:     component.ComponentID,
			ComponentType:   component.ComponentType,
			Description:     fmt.Sprintf("Consumer group for %s is closed: %s", component.ComponentID, component.LastError),
			DetectedAt:      time.Now(),
			OccurrenceCount: 1,
			LastOccurrence:  time.Now(),
			Metadata: map[string]interface{}{
				"topic":      component.ComponentID,
				"last_error": component.LastError,
			},
		}
	}

	return nil
}

// detectHighConsumerLag detects high consumer lag
func (d *IssueDetector) detectHighConsumerLag(component *ComponentHealth) *Issue {
	if component.ConsumerLag == 0 {
		return nil
	}

	var severity IssueSeverity
	if component.ConsumerLag >= d.config.ConsumerLagCriticalThreshold {
		severity = IssueSeverityCritical
	} else if component.ConsumerLag >= d.config.ConsumerLagWarningThreshold {
		severity = IssueSeverityWarning
	} else {
		return nil
	}

	return &Issue{
		ID:            generateIssueID(component.ComponentID, IssueTypeHighLag),
		Type:          IssueTypeHighLag,
		Severity:      severity,
		ComponentID:   component.ComponentID,
		ComponentType: component.ComponentType,
		Description: fmt.Sprintf("High consumer lag detected: %d messages (threshold: %d)",
			component.ConsumerLag, d.config.ConsumerLagWarningThreshold),
		DetectedAt:      time.Now(),
		OccurrenceCount: 1,
		LastOccurrence:  time.Now(),
		Metadata: map[string]interface{}{
			"consumer_lag":       component.ConsumerLag,
			"warning_threshold":  d.config.ConsumerLagWarningThreshold,
			"critical_threshold": d.config.ConsumerLagCriticalThreshold,
		},
	}
}

// detectUnhealthyStatus detects components in unhealthy or dead status
func (d *IssueDetector) detectUnhealthyStatus(component *ComponentHealth) *Issue {
	if component.Status != HealthStatusUnhealthy && component.Status != HealthStatusDead {
		return nil
	}

	var issueType IssueType
	var severity IssueSeverity

	switch component.ComponentType {
	case ComponentTypeMCPConnector:
		issueType = IssueTypeConnectorDown
		severity = IssueSeverityWarning
	case ComponentTypeInfrastructure:
		issueType = IssueTypeInfrastructureDown
		severity = IssueSeverityCritical
	default:
		issueType = IssueTypeMissingHeartbeat
		severity = IssueSeverityCritical
	}

	if component.Status == HealthStatusDead {
		severity = IssueSeverityCritical
	}

	return &Issue{
		ID:              generateIssueID(component.ComponentID, issueType),
		Type:            issueType,
		Severity:        severity,
		ComponentID:     component.ComponentID,
		ComponentType:   component.ComponentType,
		Description:     fmt.Sprintf("Component is %s", component.Status),
		DetectedAt:      time.Now(),
		OccurrenceCount: 1,
		LastOccurrence:  time.Now(),
		Metadata: map[string]interface{}{
			"status":     component.Status,
			"last_error": component.LastError,
			"updated_at": component.UpdatedAt,
		},
	}
}

// shouldReportIssue checks if an issue should be reported (deduplication)
func (d *IssueDetector) shouldReportIssue(issue *Issue) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	lastReported, exists := d.recentIssues[issue.ID]
	if exists && time.Since(lastReported) < d.config.IssueCooldownPeriod {
		// Issue was recently reported, skip to avoid spam
		return false
	}

	// Record this issue
	d.recentIssues[issue.ID] = time.Now()
	return true
}

// cleanupRecentIssues periodically cleans up old issue records
func (d *IssueDetector) cleanupRecentIssues() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			d.mu.Lock()
			now := time.Now()
			for issueID, lastReported := range d.recentIssues {
				if now.Sub(lastReported) > d.config.IssueCooldownPeriod*2 {
					delete(d.recentIssues, issueID)
				}
			}
			d.mu.Unlock()
		}
	}
}

// generateIssueID generates a unique ID for an issue
func generateIssueID(componentID string, issueType IssueType) string {
	// Use deterministic ID so same issue on same component gets same ID
	return fmt.Sprintf("%s:%s", componentID, issueType)
}
