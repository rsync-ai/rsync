## Observability (Fluent Bit + OTel Collector) — LLD

### Compose Definition
Source: `docker-compose.yml`

Fluent Bit:
- image: `fluent/fluent-bit:2.2`
- config mounted from:
  - `deploy/fluent-bit.conf`
  - `deploy/parsers.conf`

OTel Collector:
- image: `otel/opentelemetry-collector-contrib:0.92.0`
- config mounted from:
  - `deploy/otel-collector-config.yaml`

### Logging Pipeline
1) services log JSON
2) docker logging driver sends logs to Fluent Bit (fluentd)
3) Fluent Bit forwards to OTel Collector
4) OTel Collector exports to backend (SigNoz) and keeps trace context

### Metrics Pipeline
Source: `deploy/otel-collector-config.yaml` (a `prometheus` receiver feeding the
existing `otlp/signoz` metrics pipeline — `receivers: [otlp, prometheus]`).

Six scrape jobs (15s), all on the internal docker network — no host exposure needed:

| Job | Target | Path | Added |
|---|---|---|---|
| `rsync-api-gateway` | `api-gateway:8080` | `/metrics` | #101 |
| `rsync-orchestrator` | `orchestrator:8080` | `/metrics` | #101 |
| `rsync-temporal-adapter` | `temporal-adapter:8082` | `/metrics` | #102 |
| `rsync-llm-gateway` | `llm-service:5000` | `/prometheus` | #102 |
| `rsync-llm-tool-generator` | `tool-generator:5010` | `/prometheus` | #102 |
| `rsync-llm-planner` | `planner:5011` | `/prometheus` | #102 |

Flow: each service exposes a pull-only Prometheus endpoint → OTel Collector
`prometheus` receiver scrapes it → forwarded over OTLP to SigNoz. Counters only
emit a series after their first increment (e.g. `rsync_pipeline_runs_total`
appears after the first pipeline run).

Implementation entry points:
- Go: `internal/metrics/metrics.go` (`promauto` collectors) + `promhttp` handler
  in each service's `cmd/.../main.go`. temporal-adapter additionally uses
  `internal/metrics/interceptor.go` (a `worker.Interceptor`).
- Python: `llm-service/src/utils/metrics.py` (`prometheus_client` collectors +
  `instrument_openai_client` + `mount_metrics(app)` → `/prometheus`).

Importable SigNoz assets (dashboard + threshold alerts) live in `deploy/signoz/`
(see `deploy/signoz/README.md`).


