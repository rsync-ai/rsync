package handlers

import (
	"crypto/rand"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/rsync-ai/backend-orchestrator/internal/telemetry"
)

const (
	zeroTraceID = "00000000000000000000000000000000"
	zeroSpanID  = "0000000000000000"
)

var sensitiveQueryParams = map[string]bool{
	"token": true, "api_key": true, "apikey": true, "access_token": true,
	"secret": true, "password": true, "passwd": true, "key": true,
}

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

func randomTraceID() trace.TraceID {
	var tid trace.TraceID
	_, _ = rand.Read(tid[:])
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

// OTelTraceMiddleware provides OpenTelemetry tracing with X-Trace-ID header backward compatibility
// This replaces the old TraceMiddleware and properly integrates with the OTel ecosystem
func OTelTraceMiddleware(serviceName string) gin.HandlerFunc {
	tracer := otel.Tracer(serviceName)
	propagator := otel.GetTextMapPropagator()

	return func(c *gin.Context) {
		// Extract any existing trace context from incoming request headers
		ctx := propagator.Extract(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))

		// Check for legacy X-Trace-ID header (backward compatibility)
		xTraceID := c.GetHeader("X-Trace-ID")

		// Ensure we never emit the invalid all-zero trace id when tracing is effectively disabled
		// (e.g. no-op tracer provider). Prefer X-Trace-ID as a base trace id if supplied.
		ctxBase := ctx
		if !trace.SpanContextFromContext(ctxBase).IsValid() {
			traceIDHex := ""
			if xTraceID != "" {
				traceIDHex = telemetry.NormalizeTraceID(xTraceID, "hex")
			}
			if traceIDHex == "" || traceIDHex == zeroTraceID || len(traceIDHex) != 32 {
				traceIDHex = telemetry.NormalizeTraceID(uuid.New().String(), "hex")
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

		// Start a new span for this request
		spanName := c.Request.Method + " " + c.FullPath()
		if spanName == " " {
			spanName = c.Request.Method + " " + c.Request.URL.Path
		}

		ctx, span := tracer.Start(ctxBase, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.method", c.Request.Method),
				attribute.String("http.url", redactSensitiveQueryParams(c.Request.URL)),
				attribute.String("http.target", c.Request.URL.Path),
				attribute.String("http.host", c.Request.Host),
				attribute.String("http.user_agent", c.Request.UserAgent()),
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
			traceID = telemetry.NormalizeTraceID(xTraceID, "hex")
			if traceID == "" || traceID == zeroTraceID {
				traceID = telemetry.NormalizeTraceID(uuid.New().String(), "hex")
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

		// Store trace info in Gin context for handlers
		c.Set("trace_id", traceID)
		c.Set("span_id", spanID)
		c.Set("otel_ctx", ctxForPropagation)

		// Set trace ID in response header for client correlation
		c.Header("X-Trace-ID", traceID)

		// Inject trace context into response headers for downstream propagation
		propagator.Inject(ctxForPropagation, propagation.HeaderCarrier(c.Writer.Header()))

		// Update request context with span context
		c.Request = c.Request.WithContext(ctxForPropagation)

		// Process request
		c.Next()

		// Record response status on span
		statusCode := c.Writer.Status()
		span.SetAttributes(attribute.Int("http.status_code", statusCode))

		if statusCode >= 400 {
			span.SetAttributes(attribute.Bool("error", true))
		}
	}
}

// getTraceIDFromCtx retrieves trace_id from Gin context (internal helper for logging middleware)
// Note: handlers/connections.go has a public getTraceID that also generates new IDs if missing
func getTraceIDFromCtx(c *gin.Context) string {
	if traceID, exists := c.Get("trace_id"); exists {
		if id, ok := traceID.(string); ok {
			return id
		}
	}
	return ""
}

// LoggingMiddleware logs all requests with trace_id and OTel context
func LoggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery

		// Process request
		c.Next()

		// Get trace_id
		traceID := getTraceIDFromCtx(c)

		// Calculate latency
		latency := time.Since(start)
		statusCode := c.Writer.Status()
		method := c.Request.Method

		if raw != "" {
			path = path + "?" + raw
		}

		logLevel := log.InfoLevel
		if statusCode >= 500 {
			logLevel = log.ErrorLevel
		} else if statusCode >= 400 {
			logLevel = log.WarnLevel
		}

		// Structured logging with trace context
		log.WithFields(log.Fields{
			"trace_id":   traceID,
			"status":     statusCode,
			"method":     method,
			"path":       path,
			"latency":    latency,
			"latency_ms": latency.Milliseconds(),
			"service":    "orchestrator",
		}).Log(logLevel, "HTTP request")
	}
}

// isProductionLikeEnv mirrors the api-gateway polarity (fail-closed).
// Dev mode requires EXPLICIT opt-in via {development, dev, test,
// local}; any other ENVIRONMENT (including "docker", "staging",
// empty) yields production semantics. Keeps the two services in
// lockstep so a misconfigured deployment can't have one service in
// dev mode and another in prod.
func isProductionLikeEnv() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))
	switch v {
	case "development", "dev", "test", "local":
		return false
	default:
		return true
	}
}

// CORSMiddleware handles CORS with trace header exposure
func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Dev default: allow http://localhost:3000
		// Prod: allowlist via RSYNC_CORS_ORIGINS (comma-separated)
		origin := strings.TrimSpace(c.GetHeader("Origin"))

		allowed := map[string]struct{}{}
		raw := strings.TrimSpace(os.Getenv("RSYNC_CORS_ORIGINS"))
		if raw != "" {
			for _, part := range strings.Split(raw, ",") {
				o := strings.TrimSpace(part)
				if o != "" {
					allowed[o] = struct{}{}
				}
			}
		} else if !isProductionLikeEnv() {
			allowed["http://localhost:3000"] = struct{}{}
		}

		if origin != "" {
			if _, ok := allowed[origin]; ok {
				c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
				c.Writer.Header().Set("Vary", "Origin")
			}
		}

		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
		allowHeaders := "Content-Type, Authorization, X-Trace-ID, Origin, Accept, traceparent, tracestate"
		if !isProductionLikeEnv() {
			allowHeaders += ", X-User-ID"
		}
		allowHeaders += ", X-CSRF-Token"
		c.Writer.Header().Set("Access-Control-Allow-Headers", allowHeaders)
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Expose-Headers", "X-Trace-ID, traceparent, tracestate")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}
