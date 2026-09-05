package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials/insecure"
)

var tracer trace.Tracer

// TelemetryConfig holds OpenTelemetry settings (matches config.TelemetryConfig)
type TelemetryConfig struct {
	OTLPEndpoint   string
	ServiceName    string
	ServiceVersion string
	Enabled        bool
	SamplingRate   float64
	Insecure       bool
}

// InitTracerWithConfig initializes OpenTelemetry with typed config
func InitTracerWithConfig(cfg TelemetryConfig) (func(context.Context) error, error) {
	ctx := context.Background()

	// Build exporter options
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
	}

	// Use insecure for sidecar (localhost) connections
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithTLSCredentials(insecure.NewCredentials()))
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	// Create OTLP trace exporter
	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Create resource with service info
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
			semconv.ServiceVersionKey.String(cfg.ServiceVersion),
			attribute.String("environment", getEnv("ENVIRONMENT", "development")),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Configure sampler based on sampling rate
	var sampler sdktrace.Sampler
	if cfg.SamplingRate >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	} else if cfg.SamplingRate <= 0 {
		sampler = sdktrace.NeverSample()
	} else {
		sampler = sdktrace.TraceIDRatioBased(cfg.SamplingRate)
	}

	// Create trace provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	// Set global trace provider
	otel.SetTracerProvider(tp)

	// Set global propagator for trace context propagation
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Initialize the global tracer
	tracer = otel.Tracer(cfg.ServiceName)

	log.Infof("✅ OpenTelemetry initialized for service: %s (endpoint: %s, sampling: %.0f%%)",
		cfg.ServiceName, cfg.OTLPEndpoint, cfg.SamplingRate*100)

	// Return shutdown function
	return tp.Shutdown, nil
}

// GetTracer returns the global tracer
func GetTracer() trace.Tracer {
	if tracer == nil {
		return otel.Tracer("default")
	}
	return tracer
}

// AddSpanAttributes adds attributes to the current span
func AddSpanAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetAttributes(attrs...)
	}
}

// RecordError records an error on the current span
func RecordError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.RecordError(err)
	}
}

// Helper to get environment variable with default
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// TraceIDFromContext extracts trace ID from context as 32-char hex string (OTel standard)
func TraceIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		return span.SpanContext().TraceID().String()
	}
	return ""
}

// =============================================================================
// TRACE ID FORMAT UTILITIES (Ensures consistency across Go/Python/PostgreSQL)
// =============================================================================

// TraceIDToUUID converts a 32-char hex trace_id to UUID format for PostgreSQL storage
// OpenTelemetry: 0af7651916cd43dd8448eb211c80319c
// UUID format:   0af76519-16cd-43dd-8448-eb211c80319c
func TraceIDToUUID(traceID string) string {
	if len(traceID) < 32 {
		return traceID
	}

	// Remove any existing dashes
	clean := strings.ReplaceAll(traceID, "-", "")
	if len(clean) < 32 {
		return traceID
	}

	// Format as UUID: 8-4-4-4-12
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		clean[0:8],
		clean[8:12],
		clean[12:16],
		clean[16:20],
		clean[20:32])
}

// NormalizeTraceID normalizes a trace_id to the specified format
// format: "hex" for 32-char, "uuid" for PostgreSQL UUID format
func NormalizeTraceID(traceID string, format string) string {
	if traceID == "" {
		return traceID
	}

	// First convert to canonical 32-char hex
	cleanID := strings.ToLower(strings.ReplaceAll(traceID, "-", ""))

	if format == "uuid" {
		return TraceIDToUUID(cleanID)
	}
	return cleanID
}

// CreateSpanFromKafkaHeaders creates a new span from Kafka message headers
func CreateSpanFromKafkaHeaders(ctx context.Context, headers map[string]string, spanName string) (context.Context, trace.Span) {
	// Extract trace context from headers
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier(headers)
	ctx = propagator.Extract(ctx, carrier)

	// Start new span as child of extracted context
	return GetTracer().Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindConsumer))
}

// InjectTraceToHeaders injects trace context into headers map
func InjectTraceToHeaders(ctx context.Context) map[string]string {
	headers := make(map[string]string)
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier(headers)
	propagator.Inject(ctx, carrier)
	// Also include X-Trace-ID for backward compatibility with older clients/services.
	if traceID := TraceIDFromContext(ctx); traceID != "" {
		headers["X-Trace-ID"] = traceID
		headers["trace_id"] = traceID
	}
	return headers
}
