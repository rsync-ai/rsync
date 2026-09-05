package telemetry

import (
	"context"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// tracer is the process-wide tracer handle, set by InitTracerWithConfigOptions
// (logger.go) during startup.
var tracer trace.Tracer

// getEnv returns an environment variable value or the given default.
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// TraceIDFromContext extracts trace ID from context as string
func TraceIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		return span.SpanContext().TraceID().String()
	}
	return ""
}
