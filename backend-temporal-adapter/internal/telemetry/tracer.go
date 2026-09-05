package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TraceContextHook injects trace_id/span_id into every logrus entry.
type TraceContextHook struct{}

func (h *TraceContextHook) Levels() []log.Level { return log.AllLevels }

func (h *TraceContextHook) Fire(entry *log.Entry) error {
	if entry.Context == nil {
		return nil
	}
	span := trace.SpanFromContext(entry.Context)
	if !span.SpanContext().IsValid() {
		return nil
	}
	entry.Data["trace_id"] = span.SpanContext().TraceID().String()
	entry.Data["span_id"] = span.SpanContext().SpanID().String()
	if span.SpanContext().IsSampled() {
		entry.Data["trace_sampled"] = true
	}
	return nil
}

// ServiceFieldHook adds the service name to every log entry.
type ServiceFieldHook struct{ ServiceName string }

func (h *ServiceFieldHook) Levels() []log.Level { return log.AllLevels }
func (h *ServiceFieldHook) Fire(entry *log.Entry) error {
	entry.Data["service"] = h.ServiceName
	return nil
}

// InitLogging sets up JSON logrus with trace-context and service-name hooks.
func InitLogging(serviceName string) {
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
		log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	}

	switch os.Getenv("LOG_LEVEL") {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}

	log.AddHook(&ServiceFieldHook{ServiceName: serviceName})
	log.AddHook(&TraceContextHook{})
}

// defaultOTLPDialTimeout bounds the startup dial. Five seconds is long enough for a
// collector that is up but slow to accept, and short enough that a collector that is
// gone costs one startup pause rather than an outage.
const defaultOTLPDialTimeout = 5 * time.Second

// otlpDialTimeout reads OTEL_EXPORTER_OTLP_DIAL_TIMEOUT (a Go duration, e.g. "2s").
// An unset, unparseable, or non-positive value falls back to the default rather than
// to "no timeout" -- restoring the unbounded wait is the one outcome this must not
// have.
func otlpDialTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_DIAL_TIMEOUT"))
	if raw == "" {
		return defaultOTLPDialTimeout
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		log.WithField("value", raw).Warn("invalid OTEL_EXPORTER_OTLP_DIAL_TIMEOUT; using default")
		return defaultOTLPDialTimeout
	}
	return d
}

// InitTracer initialises the global OTel TracerProvider and returns a shutdown fn.
// If OTEL_EXPORTER_OTLP_ENDPOINT is empty or OTEL_ENABLED != "true" it is a no-op.
func InitTracer(serviceName string) (shutdown func(context.Context) error, err error) {
	noop := func(context.Context) error { return nil }

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" || os.Getenv("OTEL_ENABLED") != "true" {
		log.Info("OTel tracing disabled (set OTEL_ENABLED=true and OTEL_EXPORTER_OTLP_ENDPOINT)")
		return noop, nil
	}

	ctx := context.Background()

	// WithBlock() makes the dial wait for a ready connection, which is what lets a
	// typo'd endpoint surface at startup instead of as silently dropped spans. But
	// a blocking dial needs a deadline to block *against*: on context.Background()
	// it waits forever, so main.go's "OTel tracer init failed (non-fatal)" branch
	// can never be reached and a collector that is merely absent takes the whole
	// adapter down before it logs a single line. The other two Go services never had
	// this problem because they let otlptracegrpc.New connect lazily.
	dialCtx, cancelDial := context.WithTimeout(ctx, otlpDialTimeout())
	defer cancelDial()

	//nolint:staticcheck // DialContext is deprecated in newer grpc but NewClient unavailable in v1.59
	conn, err := grpc.DialContext(dialCtx, endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		// Deliberately not fatal, and deliberately not retried: tracing is
		// diagnostic, and a service that will not start because its telemetry
		// sidecar is missing is a worse failure than one that starts untraced.
		return noop, fmt.Errorf("otlp dial %q: %w", endpoint, err)
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return noop, err
	}

	res, _ := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String(os.Getenv("APP_VERSION")),
			semconv.DeploymentEnvironmentKey.String(os.Getenv("ENVIRONMENT")),
		),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.WithField("endpoint", endpoint).Info("✅ OTel tracer initialised → SigNoz")

	return func(ctx context.Context) error {
		return tp.Shutdown(ctx)
	}, nil
}
