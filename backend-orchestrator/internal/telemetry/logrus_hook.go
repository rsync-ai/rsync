package telemetry

import (
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

// =============================================================================
// TRACE CONTEXT LOGRUS HOOK
// =============================================================================
// This hook automatically injects trace_id and span_id into every log entry
// when a valid trace context is available. This enables log-trace correlation
// in observability backends like SigNoz.
//
// The sidecar OTEL Collector parses these fields from JSON logs and correlates
// them with spans received via OTLP.

// TraceContextHook is a Logrus hook that injects trace context into log entries
type TraceContextHook struct{}

// NewTraceContextHook creates a new TraceContextHook
func NewTraceContextHook() *TraceContextHook {
	return &TraceContextHook{}
}

// Levels returns all log levels (hook applies to all)
func (h *TraceContextHook) Levels() []log.Level {
	return log.AllLevels
}

// Fire is called for every log entry. It extracts trace context from the
// log entry's context (if present) and adds trace_id and span_id fields.
func (h *TraceContextHook) Fire(entry *log.Entry) error {
	// Check if context is available in the log entry
	if entry.Context == nil {
		return nil
	}

	// Extract span from context
	span := trace.SpanFromContext(entry.Context)
	if !span.SpanContext().IsValid() {
		return nil
	}

	// Inject trace_id and span_id for log-trace correlation
	// These field names are standard and recognized by most OTEL collectors
	entry.Data["trace_id"] = span.SpanContext().TraceID().String()
	entry.Data["span_id"] = span.SpanContext().SpanID().String()

	// Also add trace flags (sampled status)
	if span.SpanContext().IsSampled() {
		entry.Data["trace_sampled"] = true
	}

	return nil
}

// =============================================================================
// INITIALIZATION
// =============================================================================

// InitLogrusWithTraceHook initializes logrus with the trace context hook.
// Call this once during application startup after setting up the log formatter.
func InitLogrusWithTraceHook() {
	log.AddHook(NewTraceContextHook())
	log.Info("✅ Logrus TraceContext hook initialized (log-trace correlation enabled)")
}

