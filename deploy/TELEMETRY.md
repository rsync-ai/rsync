# Telemetry Architecture

## Overview

This document describes the telemetry architecture for rsync-ai services using the **OTEL Sidecar Pattern**.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SERVICE POD                                     │
│                                                                              │
│   ┌─────────────────────┐        ┌─────────────────────┐                    │
│   │   Application       │        │   OTEL Collector    │                    │
│   │   Container         │        │   Sidecar           │                    │
│   │                     │        │                     │                    │
│   │  ┌───────────────┐  │ OTLP   │  ┌───────────────┐  │                    │
│   │  │ Traces        │──┼───────▶│  │ OTLP Receiver │  │                    │
│   │  │ (OTLP:4317)   │  │        │  └───────────────┘  │                    │
│   │  └───────────────┘  │        │          │         │                    │
│   │                     │        │          ▼         │                    │
│   │  ┌───────────────┐  │        │  ┌───────────────┐  │                    │
│   │  │ JSON Logs     │──┼───────▶│  │ Filelog       │  │                    │
│   │  │ (stdout)      │  │ File   │  │ Receiver      │  │                    │
│   │  │               │  │        │  └───────────────┘  │                    │
│   │  │ trace_id: xxx │  │        │          │         │                    │
│   │  │ span_id: yyy  │  │        │          ▼         │                    │
│   │  └───────────────┘  │        │  ┌───────────────┐  │                    │
│   │                     │        │  │ Processors    │  │                    │
│   └─────────────────────┘        │  │ - Batch       │  │                    │
│                                   │  │ - Resource    │  │                    │
│                                   │  │ - Transform   │  │                    │
│                                   │  └───────────────┘  │                    │
│                                   │          │         │                    │
│                                   │          ▼         │                    │
│                                   │  ┌───────────────┐  │                    │
│                                   │  │ OTLP Exporter │──┼───▶ SigNoz        │
│                                   │  └───────────────┘  │                    │
│                                   └─────────────────────┘                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Application Layer

Applications are responsible for:

1. **Trace Export**: Export traces via OTLP to `localhost:4317` (the sidecar)
2. **Log Format**: Output JSON logs to stdout with `trace_id` and `span_id` fields
3. **Context Propagation**: Propagate trace context using W3C traceparent headers

### 2. OTEL Collector Sidecar

Each service runs an OTEL Collector sidecar that:

1. **Receives Traces**: OTLP receiver on port 4317
2. **Scrapes Logs**: Filelog receiver reads container stdout
3. **Correlates Logs**: Extracts `trace_id` from JSON logs
4. **Exports All**: Sends traces, logs, and metrics to SigNoz

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | OTLP endpoint (sidecar) |
| `OTEL_SERVICE_NAME` | `orchestrator` | Service name for telemetry |
| `OTEL_SERVICE_VERSION` | `1.0.0` | Service version |
| `OTEL_ENABLED` | `true` | Enable/disable telemetry |
| `OTEL_SAMPLING_RATE` | `1.0` | Trace sampling rate |
| `OTEL_INSECURE` | `true` | Use insecure connection |
| `LOG_FORMAT` | `text` | `json` for production |
| `ENVIRONMENT` | `development` | Environment name |

### Sidecar Environment Variables

| Variable | Description |
|----------|-------------|
| `SERVICE_NAME` | Name of the service being monitored |
| `ENVIRONMENT` | Environment (production, staging, etc.) |
| `SIGNOZ_ENDPOINT` | SigNoz OTEL Collector endpoint |
| `SIGNOZ_INSECURE` | Use insecure connection to SigNoz |
| `SIGNOZ_ACCESS_TOKEN` | SigNoz API token (optional) |

## Log-Trace Correlation

The key to log-trace correlation is including `trace_id` and `span_id` in every log entry.

### Go (Logrus)

```go
// The TraceContextHook automatically injects trace_id/span_id
telemetry.InitLogrusWithTraceHook()

// Log with context to include trace information
log.WithContext(ctx).Info("Processing request")

// Or use the helper
telemetry.WithContext(ctx).WithField("user_id", "123").Info("User action")
```

### JSON Log Output

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

### Backend Orchestrator

- `internal/config/config.go` - Viper-based configuration
- `internal/telemetry/tracer.go` - OpenTelemetry tracer initialization
- `internal/telemetry/logrus_hook.go` - TraceID injection hook

### API Gateway

- `internal/telemetry/tracer.go` - OpenTelemetry tracer initialization
- `internal/telemetry/logger.go` - Logging with trace correlation
- `internal/telemetry/middleware.go` - Request tracing and logging

### Deploy

- `deploy/otel-sidecar-config.yaml` - OTEL Collector configuration
- `deploy/docker-compose.sidecar.yaml` - Docker Compose example

## Usage

### Starting Services with Sidecar

```bash
docker-compose -f deploy/docker-compose.sidecar.yaml up
```

### Verifying Correlation

1. Make a request to any service
2. Check SigNoz for the trace
3. Click on the trace to see correlated logs
4. Logs should appear with the same `trace_id`

## Best Practices

1. **Always use context**: Pass `context.Context` through your call chain
2. **Log with context**: Use `log.WithContext(ctx)` for trace correlation
3. **JSON in production**: Set `LOG_FORMAT=json` in production
4. **Sample appropriately**: Adjust `OTEL_SAMPLING_RATE` for high-traffic services
5. **Include meaningful fields**: Add business context to logs (user_id, pipeline_id, etc.)

