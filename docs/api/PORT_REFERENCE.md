# 🔌 Port Reference (Current)
**Updated:** 2026-05-31  
**Source of truth:** `docker-compose.yml` (+ `docker-compose.e2e.yml` for test-only services)

---

## User-facing (host ports)

| Port | Service | What it’s for |
|------|---------|----------------|
| **3000** | Frontend | Next.js UI |
| **5001** | API Gateway | Public REST API + WebSocket (`ws://localhost:5001/ws`) |
| **8233** | Temporal UI | Workflow debugging UI |

---

## Control plane + infra (host ports)

| Port | Service | What it’s for |
|------|---------|----------------|
| **5432** | Postgres | Metadata DB |
| **6379** | Redis | Cache + correlation store |
| **9092** | Kafka | Broker (host access) |
| **9101** | Kafka JMX | Kafka metrics |
| **8083** | Kafka Connect | Debezium connectors (CDC) |
| **8085** | Schema Registry | Schema Registry (mapped from 8081) |
| **7233** | Temporal | Temporal frontend |
| **8082** | Temporal Adapter | Workflow worker/adapter service |
| **8081** | Orchestrator | Worker host (9 workers) + helpers |

---

## AI services (host ports)

| Port | Service | What it’s for |
|------|---------|----------------|
| **5010** | Tool Generator | Connector generation service |
| **5011** | Planner | Planning service |
| **8087** | Context7 MCP | Documentation/capability lookup (MCP) |

**LLM service** runs **internal-only** (no host port in `docker-compose.yml`).

---

## Observability (optional)

| Port | Service | What it’s for |
|------|---------|----------------|
| **24224** | Fluent Bit | Fluent forward receiver |
| **14317** | OTel Collector | OTLP gRPC |
| **14318** | OTel Collector | OTLP HTTP |
| **13133** | OTel Collector | Health check |

---

## Test-only overlay (when using `docker-compose.e2e.yml`)

| Port | Service | What it’s for |
|------|---------|----------------|
| **3307** | mysql-e2e | E2E MySQL (binlog enabled for CDC) |
| **9000** | MinIO | S3-compatible object storage |
| **9001** | MinIO Console | MinIO UI |
| **5433** | postgres-e2e | E2E Postgres |

---

## Notes (important)

- **MCP connector containers** usually listen on **port 8000 inside Docker**, and are **not exposed on the host**.
- **User-scoped APIs**: most API calls should include `X-User-ID` header (see `docs/api/README.md`).
