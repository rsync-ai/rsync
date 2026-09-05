package telemetry

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// sensitiveQueryParams lists query parameter names whose values must never appear in traces.
var sensitiveQueryParams = map[string]bool{
	"token": true, "api_key": true, "apikey": true, "access_token": true,
	"secret": true, "password": true, "passwd": true, "key": true,
}

// redactSensitiveQueryParams returns the URL string with sensitive query param values
// replaced by "[redacted]". Prevents session tokens from leaking into OTel spans.
func redactSensitiveQueryParams(u *url.URL) string {
	if u == nil {
		return ""
	}
	q := u.Query()
	redacted := false
	for param := range sensitiveQueryParams {
		if q.Has(param) {
			q.Set(param, "[redacted]")
			redacted = true
		}
	}
	if !redacted {
		return u.String()
	}
	out := *u
	out.RawQuery = q.Encode()
	return out.String()
}

const (
	zeroTraceID = "00000000000000000000000000000000"
	zeroSpanID  = "0000000000000000"
)

func randomTraceID() trace.TraceID {
	var tid trace.TraceID
	_, _ = rand.Read(tid[:])
	// Extremely unlikely, but ensure we don't return the invalid all-zero TraceID.
	if tid.String() == zeroTraceID {
		_, _ = rand.Read(tid[:])
	}
	return tid
}

func randomSpanID() trace.SpanID {
	var sid trace.SpanID
	_, _ = rand.Read(sid[:])
	if sid.String() == zeroSpanID {
		_, _ = rand.Read(sid[:])
	}
	return sid
}

// TracingMiddleware returns a Gin middleware that creates spans for HTTP requests
// Enhanced with X-Trace-ID backward compatibility for clients that don't support W3C traceparent
func TracingMiddleware(serviceName string) gin.HandlerFunc {
	tracer := otel.Tracer(serviceName)
	propagator := otel.GetTextMapPropagator()

	return func(c *gin.Context) {
		// Extract trace context from incoming request headers (W3C traceparent)
		ctx := propagator.Extract(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))

		// Check for legacy X-Trace-ID header (backward compatibility)
		xTraceID := c.GetHeader("X-Trace-ID")

		// Ensure we never run with an "invalid" all-zero trace id when tracing is disabled
		// (e.g., no-op tracer provider). If upstream provided X-Trace-ID, use it as the
		// base trace id; otherwise generate a new one.
		ctxBase := ctx
		if !trace.SpanContextFromContext(ctxBase).IsValid() {
			traceIDHex := ""
			if xTraceID != "" {
				traceIDHex = NormalizeTraceID(xTraceID)
			}
			if traceIDHex == "" || traceIDHex == zeroTraceID || len(traceIDHex) != 32 {
				traceIDHex = NormalizeTraceID(uuid.New().String())
			}
			tid, err := trace.TraceIDFromHex(traceIDHex)
			if err != nil || !tid.IsValid() {
				tid = randomTraceID()
			}
			sc := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    tid,
				SpanID:     randomSpanID(),
				TraceFlags: trace.FlagsSampled,
			})
			ctxBase = trace.ContextWithSpanContext(ctxBase, sc)
		}

		// Start a new span
		spanName := fmt.Sprintf("%s %s", c.Request.Method, c.FullPath())
		if c.FullPath() == "" {
			spanName = fmt.Sprintf("%s %s", c.Request.Method, c.Request.URL.Path)
		}

		ctx, span := tracer.Start(ctxBase, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.method", c.Request.Method),
				// Redact sensitive query params (e.g. ?token= on WebSocket upgrade)
				// before recording the URL in traces — session tokens in OTel spans
				// would be visible to anyone with SigNoz read access.
				attribute.String("http.url", redactSensitiveQueryParams(c.Request.URL)),
				attribute.String("http.route", c.FullPath()),
				attribute.String("http.host", c.Request.Host),
				attribute.String("http.user_agent", c.Request.UserAgent()),
				attribute.String("http.client_ip", c.ClientIP()),
			),
		)
		defer span.End()

		// Prefer the span trace id when valid, otherwise fall back to ctxBase trace id.
		traceID := span.SpanContext().TraceID().String()
		if traceID == "" || traceID == zeroTraceID {
			traceID = trace.SpanContextFromContext(ctxBase).TraceID().String()
		}
		// Final guard: never emit the invalid all-zero trace id.
		if traceID == "" || traceID == zeroTraceID {
			traceID = NormalizeTraceID(xTraceID)
			if traceID == "" || traceID == zeroTraceID {
				traceID = NormalizeTraceID(uuid.New().String())
			}
		}
		if xTraceID != "" {
			span.SetAttributes(attribute.String("legacy.trace_id", xTraceID))
		}

		spanID := span.SpanContext().SpanID().String()
		if spanID == zeroSpanID {
			spanID = ""
		}

		// Use ctx if it carries a valid span context, otherwise propagate ctxBase.
		ctxForPropagation := ctx
		if !trace.SpanContextFromContext(ctxForPropagation).IsValid() {
			ctxForPropagation = ctxBase
		}

		// Store trace ID in context for logging and handlers
		c.Set("trace_id", traceID)
		c.Set("span_id", spanID)
		c.Set("otel_ctx", ctxForPropagation)

		// Add trace ID to response headers for client correlation
		c.Header("X-Trace-ID", traceID)

		// Inject trace context into response headers for downstream propagation
		propagator.Inject(ctxForPropagation, propagation.HeaderCarrier(c.Writer.Header()))

		// Update request context
		c.Request = c.Request.WithContext(ctxForPropagation)

		// Record start time
		startTime := time.Now()

		// Process request
		c.Next()

		// Record response attributes
		duration := time.Since(startTime)
		statusCode := c.Writer.Status()

		span.SetAttributes(
			attribute.Int("http.status_code", statusCode),
			attribute.Int64("http.response_size", int64(c.Writer.Size())),
			attribute.Float64("http.duration_ms", float64(duration.Milliseconds())),
		)

		// Set span status based on HTTP status code
		if statusCode >= 400 {
			span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", statusCode))
		} else {
			span.SetStatus(codes.Ok, "")
		}

		// Record errors if any
		if len(c.Errors) > 0 {
			for _, err := range c.Errors {
				span.RecordError(err.Err)
			}
		}
	}
}

// NormalizeTraceID converts various trace ID formats to 32-char hex
func NormalizeTraceID(traceID string) string {
	if traceID == "" {
		traceID = uuid.New().String()
	}
	// Remove dashes (UUID format to 32-char hex)
	return strings.ToLower(strings.ReplaceAll(traceID, "-", ""))
}

// GetTraceIDFromContext extracts trace ID from Gin context
func GetTraceIDFromContext(c *gin.Context) string {
	if traceID, exists := c.Get("trace_id"); exists {
		return traceID.(string)
	}
	return ""
}

// GetOTelContext retrieves the OpenTelemetry context from Gin context
func GetOTelContext(c *gin.Context) context.Context {
	if ctx, exists := c.Get("otel_ctx"); exists {
		if otelCtx, ok := ctx.(context.Context); ok {
			return otelCtx
		}
	}
	return c.Request.Context()
}

// InjectTraceToHeaders injects trace context into headers map
// Note: Uses TraceIDFromContext from tracer.go
func InjectTraceToHeaders(ctx context.Context) map[string]string {
	headers := make(map[string]string)
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier(headers)
	propagator.Inject(ctx, carrier)

	// Also add X-Trace-ID for backward compatibility
	traceID := TraceIDFromContext(ctx)
	if traceID != "" {
		headers["X-Trace-ID"] = traceID
		headers["trace_id"] = traceID
	}

	return headers
}
