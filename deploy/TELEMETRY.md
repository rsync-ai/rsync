# Telemetry

## Default: `docker logs`, nothing exported

The default stack runs **no log shipper and no OpenTelemetry collector**. Every service writes
JSON logs to stdout through Docker's `json-file` driver with a bounded `max-size` / `max-file`,
so the logs are read where Docker keeps them:

```bash
docker compose logs -f orchestrator          # one service
docker logs --since 10m rsync-ai-api-gateway # one container
```

Export is off: the compose files set `OTEL_ENABLED` to `false` unless you set it, so no
service opens an OTLP connection and nothing retries against a collector that is not there.

## Exporting to your own OTLP backend

The services are instrumented with OpenTelemetry (traces for all Go services and the LLM
service, plus `trace_id` / `span_id` on every JSON log line). To ship that telemetry, run your
own collector or OTLP-compatible backend and point the services at it:

| Variable | Default in compose | Meaning |
|----------|-------------------|---------|
| `OTEL_ENABLED` | `false` | `true` turns on the OTLP exporter |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | a collector name the default stack does not run (unset in the quickstart, so `localhost:4317`) | gRPC endpoint of your collector (`host:port`) |
| `OTEL_SERVICE_NAME` | the service's own name | `service.name` on every span |
| `OTEL_SAMPLING_RATE` | unset (orchestrator's code default is `1.0`) | trace sampling ratio |
| `LOG_FORMAT` | `json` in the compose files | `json` keeps logs machine-parsable |

Turn it on with a compose overlay of your own, listed last, that sets both
`OTEL_ENABLED: "true"` and `OTEL_EXPORTER_OTLP_ENDPOINT` on each service you want traced, then
`docker compose up -d` again. The code-level default for
`OTEL_ENABLED` when the variable is absent is `true`, so a deployment outside these compose
files (for example a Kubernetes manifest you write yourself) must set it explicitly.

## Log-trace correlation

Every JSON log line carries the active span's IDs, so a collector that ingests both can join
them:

```json
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "level": "info",
  "message": "Processing request",
  "service": "orchestrator",
  "trace_id": "0af7651916cd43dd8448eb211c80319c",
  "span_id": "b7ad6b7169203331"
}
```

With export off there is no active span, so these fields are empty. That is expected.

In Go code, log with the request context so the hook can inject the IDs:

```go
log.WithContext(ctx).Info("Processing request")
```

## Where the wiring lives

- `backend-orchestrator/internal/config/config.go` — reads `OTEL_ENABLED` and the endpoint
- `backend-orchestrator/internal/telemetry/tracer.go`, `logrus_hook.go` — tracer and trace-ID hook
- `api-gateway/internal/telemetry/logger.go`, `tracer.go`, `middleware.go` — config, tracer, request tracing
- `backend-temporal-adapter/internal/telemetry/tracer.go` — exports only when `OTEL_ENABLED=true` and an endpoint is set
- `llm-service/src/utils/telemetry.py` — the Python services' tracer and JSON log format
