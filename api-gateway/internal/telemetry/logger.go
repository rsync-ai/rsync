package telemetry

import (
	"context"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
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

// ServiceFieldHook adds service name to all log entries
type ServiceFieldHook struct {
	ServiceName string
}

func (h *ServiceFieldHook) Levels() []log.Level {
	return log.AllLevels
}

func (h *ServiceFieldHook) Fire(entry *log.Entry) error {
	entry.Data["service"] = h.ServiceName
	return nil
}

// =============================================================================
// INITIALIZATION
// =============================================================================

// InitLogging initializes logrus with JSON format and trace context hook.
// Call this once during application startup.
func InitLogging(serviceName string) {
	// Setup JSON format for production, text for development
	if os.Getenv("LOG_FORMAT") == "json" || os.Getenv("ENVIRONMENT") == "production" {
		log.SetFormatter(&log.JSONFormatter{
			TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
			FieldMap: log.FieldMap{
				log.FieldKeyTime:  "timestamp",
				log.FieldKeyLevel: "level",
				log.FieldKeyMsg:   "message",
			},
		})
	} else {
		log.SetFormatter(&log.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02T15:04:05.000",
		})
	}

	// Set log level
	logLevel := os.Getenv("LOG_LEVEL")
	switch logLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}

	// Override with debug flag
	if os.Getenv("DEBUG") == "true" {
		log.SetLevel(log.DebugLevel)
	}

	// Add service field hook
	log.AddHook(&ServiceFieldHook{ServiceName: serviceName})

	// Add trace context hook for log-trace correlation
	log.AddHook(NewTraceContextHook())

	log.Info("✅ Logrus initialized with TraceContext hook (log-trace correlation enabled)")
}

// =============================================================================
// TRACER CONFIGURATION
// =============================================================================

// TelemetryConfig holds OpenTelemetry settings
type TelemetryConfig struct {
	OTLPEndpoint   string
	ServiceName    string
	ServiceVersion string
	Enabled        bool
	SamplingRate   float64
	Insecure       bool
}

// LoadTelemetryConfig loads telemetry configuration from environment
func LoadTelemetryConfig(serviceName string) TelemetryConfig {
	return TelemetryConfig{
		OTLPEndpoint:   getEnvWithDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		ServiceName:    getEnvWithDefault("OTEL_SERVICE_NAME", serviceName),
		ServiceVersion: getEnvWithDefault("OTEL_SERVICE_VERSION", "1.0.0"),
		Enabled:        getEnvWithDefault("OTEL_ENABLED", "true") == "true",
		SamplingRate:   1.0, // Default to 100% sampling
		Insecure:       getEnvWithDefault("OTEL_INSECURE", "true") == "true",
	}
}

func getEnvWithDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// InitTracerWithConfig initializes OpenTelemetry with typed config
func InitTracerWithConfig(cfg TelemetryConfig) (func(context.Context) error, error) {
	return InitTracerWithConfigOptions(cfg.OTLPEndpoint, cfg.ServiceName, cfg.ServiceVersion, cfg.SamplingRate, cfg.Insecure)
}

// InitTracerWithConfigOptions initializes OpenTelemetry with explicit parameters
func InitTracerWithConfigOptions(endpoint, serviceName, version string, samplingRate float64, insecure bool) (func(context.Context) error, error) {
	ctx := context.Background()

	// Build exporter options
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(endpoint),
	}

	// Use insecure for sidecar (localhost) connections
	if insecure {
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
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String(version),
			attribute.String("environment", getEnv("ENVIRONMENT", "development")),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Configure sampler based on sampling rate
	var sampler sdktrace.Sampler
	if samplingRate >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	} else if samplingRate <= 0 {
		sampler = sdktrace.NeverSample()
	} else {
		sampler = sdktrace.TraceIDRatioBased(samplingRate)
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
	tracer = otel.Tracer(serviceName)

	log.Infof("✅ OpenTelemetry initialized for service: %s (endpoint: %s, sampling: %.0f%%)",
		serviceName, endpoint, samplingRate*100)

	// Return shutdown function
	return tp.Shutdown, nil
}
