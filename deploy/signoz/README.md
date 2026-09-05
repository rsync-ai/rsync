# SigNoz observability assets (F-Obs-2)

rsync-ai uses **SigNoz** as its single observability backend (traces, logs, **and metrics**).
There is intentionally **no separate Prometheus/Grafana/Alertmanager stack** — that would be a
second pane of glass and would break trace↔metric correlation.

## Running self-hosted SigNoz (`deploy/signoz/docker`)

The SigNoz stack (ClickHouse + Zookeeper + query/UI + its own otel-collector) is **vendored**
from the official self-host compose, pinned to **v0.127.1** (otel-collector v0.144.5). It runs
as plain Docker containers on its own `signoz-net` network — independent of the rsync-ai stack.

```bash
# bring it up (run from repo root; relative ../common paths resolve correctly)
docker compose -f deploy/signoz/docker/docker-compose.yaml up -d

# tear down (keeps data volumes)
docker compose -f deploy/signoz/docker/docker-compose.yaml down
```

**Ports / wiring (deliberate):**
- SigNoz **UI → host `3301`** (container 8080). Host 8080 is owned by Traefik, so we remap.
  `SIGNOZ_UI_URL=http://localhost:3301` (already the default in the compose files).
- SigNoz **otel-collector → host `4317`/`4318`**. The rsync-ai otel-collector
  (`deploy/otel-collector-config.yaml`) exports `otlp/signoz` to SigNoz's collector. (The
  rsync-ai collector deliberately exposes host `14317`/`14318` to avoid colliding with these.)

> ### ⚠️ macOS vs Linux — the export endpoint differs (this bit us in prod)
> How the rsync-ai collector reaches SigNoz's collector depends on the Docker runtime:
>
> - **Docker Desktop (the staging Mac stack):** `host.docker.internal` is special-cased
>   and routes to the host from any network, so `host.docker.internal:4317` "just works".
> - **Native Linux (the prod VM):** it does **not**. `host-gateway` resolves to a single
>   fixed docker0 IP (`172.17.0.1`) that does not match the app collector's compose bridge
>   (e.g. `172.18.x`), and cross-bridge access to SigNoz's *published* port is refused with
>   `connection refused`. Symptom: the rsync-ai collector logs
>   `dial tcp 172.17.0.1:4317: connect: connection refused` and nothing ingests.
>
> **Fix on Linux — talk container-to-container over SigNoz's network, no host hop:**
> attach the rsync-ai otel-collector to `signoz-net` and point the exporter at the
> container name. See **"SigNoz on a Linux prod VM"** in
> [docs/deployment/self-hosting.md](../../docs/deployment/self-hosting.md#monitoring-optional)
> for the exact steps.

**Resource note:** ClickHouse + the SigNoz stack want ~2–4 GB RAM. On the 8 GB prod VM this is
fine alongside the app stack, but watch memory after first ingest.

**First-run setup is MANDATORY before anything ingests.** Open the UI
(http://localhost:3301) and create the admin account — this creates the **organization**.
Until an org exists, SigNoz's OpAMP server refuses to register the otel-collector
(`cannot create agent without orgId`), so the collector never starts its OTLP receivers
(4317/4318) and **all telemetry is silently dropped**. After creating the org, import
[`dashboard-rsync-overview.json`](dashboard-rsync-overview.json) and the
[`alerts-rsync.json`](alerts-rsync.json) rules (see below).

## How domain metrics reach SigNoz

```
api-gateway:8080/metrics  ─┐
                           ├─►  otel-collector (prometheus receiver, scrape 15s)
orchestrator:8080/metrics ─┘         │  metrics pipeline: [otlp, prometheus] → otlp/signoz
                                     ▼
                                  SigNoz (ClickHouse) ──► dashboards + alerts
```

- The Go services expose Prometheus collectors (`promauto`) on their existing `/metrics`
  endpoints — **pull-only**, so they never travel over OTLP.
- The `prometheus` receiver in [`../otel-collector-config.yaml`](../otel-collector-config.yaml)
  scrapes both endpoints and feeds the existing `otlp/signoz` metrics pipeline.
- No new containers; the collector and SigNoz are already running.

### Metric inventory

| Metric | Service | Labels |
|---|---|---|
| `rsync_gateway_http_requests_total` | api-gateway | method, route, status |
| `rsync_gateway_http_request_duration_seconds` | api-gateway | method, route |
| `rsync_gateway_auth_failures_total` | api-gateway | reason (unauthorized\|forbidden) |
| `rsync_gateway_pipeline_run_triggers_total` | api-gateway | result (accepted\|rejected) |
| `rsync_pipeline_runs_total` | orchestrator | status |
| `rsync_pipeline_sync_duration_seconds` | orchestrator | sync_mode |
| `rsync_mcp_calls_total` / `_duration_seconds` | orchestrator | connector, operation[, status] |
| `rsync_kafka_messages_published_total` | orchestrator | topic, result |
| `rsync_healer_actions_total` | orchestrator | action, outcome |
| `rsync_dlq_messages_total` | orchestrator | source_topic |

All labels are bounded — no `pipeline_id`, `connection_id`, table names, or raw URLs.

## Importing the dashboard

SigNoz UI → **Dashboards → + New dashboard → Import JSON** →
upload [`dashboard-rsync-overview.json`](dashboard-rsync-overview.json).

## Creating the alerts

[`alerts-rsync.json`](alerts-rsync.json) holds five threshold rules (DLQ, gateway 5xx,
auth-failure spike, healer escalations, pipeline failure rate). Create each one via
SigNoz UI → **Alerts → New Alert → (PromQL)**, or POST it to the SigNoz alerts API:

```bash
# one rule at a time (jq extracts each element of .rules)
jq -c '.rules[]' deploy/signoz/alerts-rsync.json | while read -r rule; do
  curl -sS -X POST "$SIGNOZ_URL/api/v1/rules" \
    -H 'Content-Type: application/json' \
    -H "Authorization: Bearer $SIGNOZ_TOKEN" \
    -d "$rule"
done
```

Targets (5xx >5%, auth >5/s, failure >20%) are starting points — tune to real traffic
before enabling in prod. Wire a notification channel (Slack/email) in SigNoz first.

## Verifying the scrape

```bash
docker logs rsync-ai-otel-collector 2>&1 | grep -i prometheus   # receiver started, no scrape errors
curl -s localhost:5001/metrics | grep rsync_gateway_            # gateway domain metrics present
```
Then in SigNoz: **Metrics → Explorer**, search `rsync_` — the series should appear within ~30s.
