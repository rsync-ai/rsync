## Observability (Fluent Bit + OTel Collector) — HLD

### Purpose
rsync-ai uses a lightweight observability stack to enable:
- structured JSON logging for all services
- trace-log correlation via OpenTelemetry
- **domain metrics** (RED-style + business counters) scraped from each service
- forwarding to SigNoz (run separately) as the single backend for logs, traces, **and metrics**

SigNoz is the single observability backend. A parallel Prometheus + Grafana +
Alertmanager stack was deliberately rejected — a second pane of glass breaks
trace↔metric correlation. Services expose pull-only Prometheus endpoints; the
OTel Collector scrapes them and forwards over OTLP. No extra containers.

### Components
- **Fluent Bit**
  - receives container logs via `fluentd` logging driver
  - forwards to OTel Collector (fluentforward)
- **OpenTelemetry Collector**
  - receives logs and traces (OTLP)
  - **`prometheus` receiver** scrapes each service's metrics endpoint (15s)
  - exports logs, traces, and metrics to SigNoz / OTLP backends
- **Per-service metrics endpoints** (pull-only, no backend of their own):
  - Go services use `promauto` collectors + `promhttp` at `/metrics`
  - Python (llm-service) uses `prometheus_client` mounted at `/prometheus`
    (NOT `/metrics`, which already serves business JSON)

### Domain metrics by service
- **api-gateway** (`/metrics`, #101): HTTP request count/latency by route+status,
  401/403 auth failures, pipeline-run-trigger accepted/rejected.
- **backend-orchestrator** (`/metrics`, #68 + #101): pipeline runs + sync duration,
  MCP calls + latency, and the previously-dead Kafka/DLQ/healer counters now wired.
- **backend-temporal-adapter** (`/metrics` on `:8082`, #102): workflow/activity
  execution counts + activity duration, recorded by a single `worker.Interceptor`
  (workflow counter guarded by `!IsReplaying` to avoid replay double-count).
- **llm-service** — gateway/tool-generator/planner (`/prometheus`, #102): LLM call
  count/latency/tokens/cost by service+provider+model. Only **direct** provider
  calls are instrumented; callers that reach LLMs through the gateway are not
  re-counted.

All metric labels are bounded enums/type-names — no pipeline_id / connection_id /
run_id / raw URLs.

### Runtime Interface
- Fluent Bit:
  - TCP/UDP `24224` (fluentd receiver)
- OTel Collector:
  - gRPC `4317` mapped to `14317` on host (avoid conflicts)
  - HTTP `4318` mapped to `14318` on host
  - health `13133`
  - fluentforward `8006`


