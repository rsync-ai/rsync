package sentinel

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// AuditLogger logs all Sentinel actions for observability
type AuditLogger struct {
	kafkaManager *kafka.Manager
	db           *sql.DB
	config       *SentinelConfig

	// Metrics for SigNoz
	meter              metric.Meter
	issuesDetected     metric.Int64Counter
	issuesResolved     metric.Int64Counter
	healingSuccess     metric.Int64Counter
	healingFailures    metric.Int64Counter
	// Consumer lag per (topic, group). Push-model gauge — Sentinel's health
	// monitor calls RecordConsumerLag() on each polling cycle.
	consumerLagGauge metric.Int64Gauge

	// Control
	ctx    context.Context
	cancel context.CancelFunc
}

// AuditLog represents a structured audit log entry.
//
// Success is a two-state field over a three-state world. Skipped marks the third
// state — an action the healer DECLINED to attempt (HealingResult.Skipped, see
// types.go) — because without it a decline is indistinguishable from a failed
// repair in every downstream surface: same level, same success=false.
type AuditLog struct {
	Timestamp time.Time              `json:"timestamp"`
	Level     string                 `json:"level"`
	Component string                 `json:"component"`
	Action    string                 `json:"action"`
	Target    string                 `json:"target"`
	IssueType string                 `json:"issue_type,omitempty"`
	Details   map[string]interface{} `json:"details,omitempty"`
	Success   bool                   `json:"success"`
	Skipped   bool                   `json:"skipped,omitempty"`
	Error     string                 `json:"error,omitempty"`
	TraceID   string                 `json:"trace_id,omitempty"`
	SpanID    string                 `json:"span_id,omitempty"`
}

// NewAuditLogger creates a new audit logger
func NewAuditLogger(kafkaManager *kafka.Manager, db *sql.DB, config *SentinelConfig) *AuditLogger {
	logger := &AuditLogger{
		kafkaManager: kafkaManager,
		db:           db,
		config:       config,
	}

	// Initialize OpenTelemetry metrics for SigNoz
	if config.EnableSigNozExport {
		logger.initializeMetrics()
	}

	return logger
}

// initializeMetrics initializes OpenTelemetry metrics
func (l *AuditLogger) initializeMetrics() {
	// Get meter from global meter provider
	l.meter = otel.Meter("sentinel-agent")

	var err error

	// Counter metrics
	l.issuesDetected, err = l.meter.Int64Counter(
		"sentinel.issues.detected",
		metric.WithDescription("Total number of issues detected"),
		metric.WithUnit("{issue}"),
	)
	if err != nil {
		log.WithError(err).Error("Failed to create issues_detected metric")
	}

	l.issuesResolved, err = l.meter.Int64Counter(
		"sentinel.issues.resolved",
		metric.WithDescription("Total number of issues resolved"),
		metric.WithUnit("{issue}"),
	)
	if err != nil {
		log.WithError(err).Error("Failed to create issues_resolved metric")
	}

	l.healingSuccess, err = l.meter.Int64Counter(
		"sentinel.healing.success",
		metric.WithDescription("Successful healing actions"),
		metric.WithUnit("{action}"),
	)
	if err != nil {
		log.WithError(err).Error("Failed to create healing_success metric")
	}

	l.healingFailures, err = l.meter.Int64Counter(
		"sentinel.healing.failures",
		metric.WithDescription("Failed healing actions"),
		metric.WithUnit("{action}"),
	)
	if err != nil {
		log.WithError(err).Error("Failed to create healing_failures metric")
	}

	l.consumerLagGauge, err = l.meter.Int64Gauge(
		"sentinel.kafka.consumer_lag",
		metric.WithDescription("Kafka consumer-group lag (messages behind log end)"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		log.WithError(err).Error("Failed to create consumer_lag metric")
	}

	log.Info("✅ Initialized SigNoz metrics for Sentinel")
}

// RecordConsumerLag emits the current Kafka consumer lag for a (topic, group)
// pair. Safe to call when SigNoz export is disabled — becomes a no-op.
//
// Lag is the number of records produced to a partition that the consumer
// group hasn't acknowledged yet. Sustained non-zero lag indicates a slow or
// dead consumer; sudden growth on a previously-zero topic indicates a
// producer/consumer topic-name mismatch (the failure mode that left 2892
// messages stranded on agent.control.commands before the publishAgentCommand
// fix).
func (l *AuditLogger) RecordConsumerLag(ctx context.Context, topic, group string, lag int64) {
	if l.consumerLagGauge == nil {
		return
	}
	l.consumerLagGauge.Record(ctx, lag,
		metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("group", group),
		),
	)
}

// Start starts the audit logger
func (l *AuditLogger) Start(ctx context.Context) error {
	l.ctx, l.cancel = context.WithCancel(ctx)

	// Start metric export loop if SigNoz is enabled
	if l.config.EnableSigNozExport {
		go l.exportMetricsLoop()
	}

	return nil
}

// Stop stops the audit logger
func (l *AuditLogger) Stop() {
	if l.cancel != nil {
		l.cancel()
	}
}

// LogIssueDetected logs when an issue is detected
func (l *AuditLogger) LogIssueDetected(ctx context.Context, issue *Issue) {
	ctx, span := sentinelTracer.Start(ctx, "log_issue_detected")
	defer span.End()

	traceID := span.SpanContext().TraceID().String()
	spanID := span.SpanContext().SpanID().String()

	auditLog := AuditLog{
		Timestamp: time.Now(),
		Level:     mapSeverityToLevel(issue.Severity),
		Component: "sentinel",
		Action:    "issue_detected",
		Target:    issue.ComponentID,
		IssueType: string(issue.Type),
		Details:   l.redactSensitiveData(issue.Metadata),
		Success:   true,
		TraceID:   traceID,
		SpanID:    spanID,
	}

	// Log to structured logger
	l.logToStructuredLogger(auditLog)

	// Publish to Kafka audit topic
	l.publishToKafka(auditLog)

	// Store in database
	l.storeInDatabase(ctx, auditLog, issue.ID, issue.Severity)

	// Record metric
	if l.issuesDetected != nil {
		l.issuesDetected.Add(ctx, 1, metric.WithAttributes(
			attribute.String("issue_type", string(issue.Type)),
			attribute.String("severity", string(issue.Severity)),
			attribute.String("component_type", string(issue.ComponentType)),
		))
	}
}

// healingOutcome is the three-state answer a HealingResult actually carries. Success is
// two-state, so `!Success` collapses "declined to try" into "tried and failed".
type healingOutcome string

const (
	healingOutcomeSkipped healingOutcome = "skipped"
	healingOutcomeSuccess healingOutcome = "success"
	healingOutcomeFailure healingOutcome = "failure"
)

// classifyHealingResult is the single decision the audit level AND the SigNoz counters
// both key off, so the two cannot drift into disagreeing about what a skip is.
func classifyHealingResult(result *HealingResult) healingOutcome {
	switch {
	case result.Skipped:
		return healingOutcomeSkipped
	case result.Success:
		return healingOutcomeSuccess
	default:
		return healingOutcomeFailure
	}
}

// LogHealingResult logs the result of a healing action
func (l *AuditLogger) LogHealingResult(ctx context.Context, result *HealingResult) {
	ctx, span := sentinelTracer.Start(ctx, "log_healing_result")
	defer span.End()

	traceID := span.SpanContext().TraceID().String()
	spanID := span.SpanContext().SpanID().String()

	// This is the layer where "a skip is neither a success nor a failure" is actually
	// kept or broken. Branching on !Success alone filed a DECLINED action at level
	// "error" with the raw error text as the message — an on-call engineer reading the
	// logs or SigNoz saw a repair that failed, for a repair that was never attempted.
	// A skip is a warning: something is unhealthy and the healer has no path for it.
	outcome := classifyHealingResult(result)

	level := "info"
	switch outcome {
	case healingOutcomeSkipped:
		level = "warn"
	case healingOutcomeFailure:
		level = "error"
	}

	auditLog := AuditLog{
		Timestamp: time.Now(),
		Level:     level,
		Component: "sentinel",
		Action:    string(result.Action),
		Target:    result.ComponentID,
		Details:   l.redactSensitiveData(result.Details),
		Success:   result.Success,
		Skipped:   result.Skipped,
		Error:     result.Error,
		TraceID:   traceID,
		SpanID:    spanID,
	}

	// Log to structured logger
	l.logToStructuredLogger(auditLog)

	// Publish to Kafka audit topic
	l.publishToKafka(auditLog)

	// Store in database
	l.storeHealingResultInDatabase(ctx, result)

	// Record metrics. A skip increments NEITHER counter: sentinel.healing.success would
	// count a repair that never happened, and sentinel.healing.failures would invent a
	// failure out of an action the healer declined to attempt. Both make the SigNoz
	// healing rate a fiction for any component with no restart path.
	switch outcome {
	case healingOutcomeSkipped:
		// Deliberately no counter. The skip is visible as level=warn + skipped=true.
	case healingOutcomeSuccess:
		if l.healingSuccess != nil {
			l.healingSuccess.Add(ctx, 1, metric.WithAttributes(
				attribute.String("action", string(result.Action)),
				attribute.String("component_type", string(result.ComponentType)),
			))
		}
		if l.issuesResolved != nil {
			l.issuesResolved.Add(ctx, 1, metric.WithAttributes(
				attribute.String("action", string(result.Action)),
			))
		}
	default:
		if l.healingFailures != nil {
			l.healingFailures.Add(ctx, 1, metric.WithAttributes(
				attribute.String("action", string(result.Action)),
				attribute.String("component_type", string(result.ComponentType)),
			))
		}
	}
}

// logToStructuredLogger logs to the structured logger (logrus)
func (l *AuditLogger) logToStructuredLogger(auditLog AuditLog) {
	fields := log.Fields{
		"component": auditLog.Component,
		"action":    auditLog.Action,
		"target":    auditLog.Target,
		"success":   auditLog.Success,
		"trace_id":  auditLog.TraceID,
		"span_id":   auditLog.SpanID,
	}

	if auditLog.IssueType != "" {
		fields["issue_type"] = auditLog.IssueType
	}

	if len(auditLog.Details) > 0 {
		fields["details"] = auditLog.Details
	}

	// A declined action carries success=false like a failed one does. Without these two
	// fields the only thing separating them in the log is the level, and nothing at all
	// separates them once the line is queried by success.
	if auditLog.Skipped {
		fields["skipped"] = true
		if auditLog.Error != "" {
			fields["skip_reason"] = auditLog.Error
		}
	}

	entry := log.WithFields(fields)

	switch auditLog.Level {
	case "error":
		entry.Error(auditLog.Error)
	case "warn":
		entry.Warn(auditLog.Action)
	default:
		entry.Info(auditLog.Action)
	}
}

// publishToKafka publishes audit log to Kafka for real-time monitoring
func (l *AuditLogger) publishToKafka(auditLog AuditLog) {
	// A nil manager is the unit-test shape (and the pre-wiring startup window), the same
	// state CDCSentinel.emitCDCIssue already guards at cdc_sentinel.go:1288. Without this
	// the audit path panics inside Produce on the nil receiver instead of degrading to the
	// structured log and the database, which both still work.
	if l.kafkaManager == nil {
		return
	}

	logBytes, err := json.Marshal(auditLog)
	if err != nil {
		log.WithError(err).Error("Failed to marshal audit log")
		return
	}

	if err := l.kafkaManager.Produce(AuditTopic, []byte(auditLog.Target), logBytes); err != nil {
		log.WithError(err).Error("Failed to publish audit log to Kafka")
	}
}

// storeInDatabase stores audit log in the database
func (l *AuditLogger) storeInDatabase(ctx context.Context, auditLog AuditLog, issueID string, severity IssueSeverity) {
	detailsJSON, _ := json.Marshal(auditLog.Details)

	_, err := l.db.ExecContext(ctx, `
		INSERT INTO sentinel_audit_logs (
			timestamp, level, component, action, target, issue_type, issue_id, severity,
			details, success, error_message, trace_id, span_id, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
	`,
		auditLog.Timestamp, auditLog.Level, auditLog.Component, auditLog.Action,
		auditLog.Target, auditLog.IssueType, issueID, severity,
		detailsJSON, auditLog.Success, auditLog.Error, auditLog.TraceID, auditLog.SpanID,
	)

	if err != nil {
		log.WithError(err).Error("Failed to store audit log in database")
	}
}

// storeHealingResultInDatabase stores healing result in the database
func (l *AuditLogger) storeHealingResultInDatabase(ctx context.Context, result *HealingResult) {
	detailsJSON, _ := json.Marshal(result.Details)

	_, err := l.db.ExecContext(ctx, `
		INSERT INTO sentinel_healing_results (
			issue_id, action, component_id, component_type,
			success, error_message, duration_ms, details, timestamp, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
	`,
		result.IssueID, result.Action, result.ComponentID, result.ComponentType,
		result.Success, result.Error, result.DurationMs, detailsJSON, result.Timestamp,
	)

	if err != nil {
		log.WithError(err).Error("Failed to store healing result in database")
	}
}

// redactSensitiveData redacts sensitive fields from metadata
func (l *AuditLogger) redactSensitiveData(data map[string]interface{}) map[string]interface{} {
	if data == nil {
		return nil
	}

	sensitiveKeys := []string{
		"password", "secret", "token", "api_key", "apikey",
		"auth", "credential", "private_key", "access_key",
	}

	redacted := make(map[string]interface{})
	for key, value := range data {
		lowerKey := strings.ToLower(key)
		isSensitive := false

		for _, sensitiveKey := range sensitiveKeys {
			if strings.Contains(lowerKey, sensitiveKey) {
				isSensitive = true
				break
			}
		}

		if isSensitive {
			redacted[key] = "[REDACTED]"
		} else {
			redacted[key] = value
		}
	}

	return redacted
}

// exportMetricsLoop periodically exports metrics to SigNoz
func (l *AuditLogger) exportMetricsLoop() {
	ticker := time.NewTicker(l.config.MetricExportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			// Metrics are automatically exported by the OpenTelemetry SDK
			// This loop can be used for custom metric calculations if needed
			log.Debug("📊 Metrics exported to SigNoz")
		}
	}
}

// mapSeverityToLevel maps issue severity to log level
func mapSeverityToLevel(severity IssueSeverity) string {
	switch severity {
	case IssueSeverityCritical:
		return "error"
	case IssueSeverityWarning:
		return "warn"
	case IssueSeverityInfo:
		return "info"
	default:
		return "info"
	}
}
