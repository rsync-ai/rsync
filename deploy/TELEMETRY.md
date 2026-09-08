# Telemetry Architecture

## Overview

This repo ships **no observability backend**. The supported way to see what a
self-hosted stack is doing is `docker compose logs`; everything below describes
the OpenTelemetry plumbing that is already wired up, and what to point it at if
you run a backend of your own.

Services emit structured JSON logs to stdout and export OTLP traces and metrics
to a single shared OTel Collector. The collector correlates the two and forwards
everything to one OTLP endpoint — `OTLP_BACKEND_ENDPOINT`. Nothing listens on
that endpoint by default, so exports fail and are dropped. That is deliberate:
the instrumentation stays on so a backend is a config change, not a code change.

## Architecture

```
┌──────────────────────┐
│  Application         │   OTLP (traces + metrics)
│  container           │───────────────────────────────┐
│                      │                               │
│  JSON logs → stdout  │                               ▼
└──────────┬───────────┘                    ┌──────────────────────┐
           │ docker log driver              │   OTel Collector     │
           ▼                                │   (shared, one per   │
┌──────────────────────┐   fluentforward    │    stack)            │
│  fluent-bit          │───────────────────▶│                      │
│  (parses trace_id,   │      :8006         │  receivers:          │
│   span_id, level)    │                    │   otlp, fluentforward│
└──────────────────────┘                    │   prometheus (scrape)│
                                            │                      │
                                            │  processors:         │
                                            │   groupbyattrs       │
                                            │   transform (typed   │
                                            │    trace_id/severity)│
                                            │                      │
                                            │  exporters:          │
                                            │   otlp/backend ──────┼──▶ your OTLP
                                            │   debug              │    backend
                                            └──────────────────────┘    (optional)
```

Config: [`otel-collector-config.yaml`](otel-collector-config.yaml). The collector
service itself is defined in the root `docker-compose.yml`.

## Components

### 1. Application layer

Applications are responsible for:

1. **Trace export** — OTLP to `otel-collector:4317`, gated on `OTEL_ENABLED`
2. **Log format** — JSON on stdout, including `trace_id` and `span_id` fields
3. **Context propagation** — W3C `traceparent` headers between services

### 2. Shared OTel Collector

One collector per stack, not a sidecar per service. It:

1. **Receives traces and metrics** over OTLP on 4317/4318
2. **Receives logs** from fluent-bit over fluentforward on 8006
3. **Scrapes** the Prometheus `/metrics` endpoints the Go services expose
4. **Promotes** `trace_id`, `span_id` and `level` from log attributes into the
   typed OTLP LogRecord fields, which is what makes trace↔log correlation work
5. **Exports** everything to `OTLP_BACKEND_ENDPOINT`

## Configuration

### Application environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_ENABLED` | `true` | Master switch. `docker-compose.quickstart.yml` sets it `false` because that bundle ships no collector. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `otel-collector:4317` | Where the SDK exports |
| `OTEL_SERVICE_NAME` | per service | Service name for telemetry |
| `OTEL_SERVICE_VERSION` | `1.0.0` | Service version |
| `OTEL_SAMPLING_RATE` | `1.0` | Trace sampling rate |
| `OTEL_INSECURE` | `true` | Use an insecure connection |
| `LOG_FORMAT` | `text` | `json` for production — required for log correlation |
| `ENVIRONMENT` | `development` | Environment name |

### Collector environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OTLP_BACKEND_ENDPOINT` | `host.docker.internal:4317` | The OTLP backend the collector forwards to. Nothing ships listening here — set it in the root `.env` to your own backend's OTLP endpoint. |

## Log-trace correlation

The key to log-trace correlation is including `trace_id` and `span_id` in every
log entry.

### Go (Logrus)

```go
// The TraceContextHook automatically injects trace_id/span_id
telemetry.InitLogrusWithTraceHook()

// Log with context to include trace information
log.WithContext(ctx).Info("Processing request")

// Or use the helper
telemetry.WithContext(ctx).WithField("user_id", "123").Info("User action")
```

### JSON log output

```json
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "level": "info",
  "message": "Processing request",
  "service": "orchestrator",
  "trace_id": "0af7651916cd43dd8448eb211c80319c",
  "span_id": "b7ad6b7169203331",
  "trace_sampled": true
}
```

## Files

### Backend orchestrator

- `internal/config/config.go` — Viper-based configuration
- `internal/telemetry/tracer.go` — OpenTelemetry tracer initialization
- `internal/telemetry/logrus_hook.go` — trace-id injection hook

### API gateway

- `internal/telemetry/tracer.go` — OpenTelemetry tracer initialization
- `internal/telemetry/logger.go` — logging with trace correlation
- `internal/telemetry/middleware.go` — request tracing and logging

### Deploy

- [`otel-collector-config.yaml`](otel-collector-config.yaml) — collector pipelines
- [`fluent-bit.conf`](fluent-bit.conf) — log shipping and JSON parsing

## Reading the logs

Without a backend, this is the whole story:

```bash
docker compose logs -f api-gateway
docker compose logs --since 15m | grep '"level":"error"'
```

`LOG_FORMAT=json` means every line is a JSON object, so `jq` works:

```bash
docker compose logs --no-log-prefix orchestrator | jq -r 'select(.level=="error") | .message'
```

## Attaching your own backend

1. Run any OTLP-compatible backend and note its OTLP gRPC endpoint.
2. Set `OTLP_BACKEND_ENDPOINT=<host>:<port>` in the root `.env`.
3. `docker compose up -d otel-collector`.
4. Make a request, then look for the trace; its logs carry the same `trace_id`.

## Best practices

1. **Always use context** — pass `context.Context` through your call chain
2. **Log with context** — use `log.WithContext(ctx)` for trace correlation
3. **JSON in production** — set `LOG_FORMAT=json`
4. **Sample appropriately** — adjust `OTEL_SAMPLING_RATE` for high-traffic services
5. **Include meaningful fields** — add business context (user_id, pipeline_id, …)
