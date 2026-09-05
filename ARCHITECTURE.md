# ARCHITECTURE.md

The front door to rsync-ai's system design. **What it is, how it's built, and why each technology was chosen.**

> This is the conceptual + rationale overview. For depth, follow the links in [§7](#7-where-to-go-deeper):
> - Component diagram + data-flow sequences → [`docs/architecture/overview.md`](docs/architecture/overview.md)
> - Per-service HLD/LLD → [`docs/services/`](docs/services/INDEX.md)

---

## 1. What rsync-ai is

**An agentic data platform that builds and runs production-grade batch + CDC pipelines from natural language**, with real-time reasoning visibility, human-in-the-loop controls, and governance (audit, redaction, traceability).

**Core thesis:** moving data isn't the hard part — *operating it safely* is. Ambiguity, schema drift, retries/idempotency, pause/resume/stop, "why did it do that?", and PII governance are the real problems. rsync-ai is built around those with an **event-sourced, agentic** architecture.

The guiding runtime pattern: **Temporal thinks, Kafka talks, Agents act.**
- **Temporal** owns durable workflow state and orchestration (the "thinking").
- **Kafka** is the message bus between the orchestrator and stateless agent workers (the "talking").
- **Agent workers** execute one stage each — intent, resolver, discovery, planner, validator, executor (the "acting").

---

## 2. The three planes

| Plane | Responsibility | Outputs |
|-------|---------------|---------|
| **Decision (agentic)** | Interpret intent, plan stages, choose tools, adapt to failures, ask for human confirmation when ambiguous | Stage plan, agent decisions + rationale, tool calls |
| **Data (execution)** | Actually move data — batch export/import and CDC streaming — emitting standardized metrics | Rows/bytes (batch), CDC lag/freshness, execution outcomes |
| **Control (governance + ops)** | Lifecycle controls, event persistence/replay, audit, redaction, RBAC, trace correlation | Pause/resume/stop, replayable event history, redacted-by-default views, `trace_id` across services |

---

## 3. Component map (brief)

```
User → Frontend (Next.js)
     → API Gateway (Go)            public REST + WebSocket, auth/tenant scoping, redaction
     → Temporal (+ Adapter, Go)    durable workflow orchestration, HITL signals
     → Kafka                       control-plane commands/results + domain events
     → Orchestrator (Go, 9 workers) intent→resolver→discovery→planner→validator→executor
     → MCP connectors (Docker)     test_connection / discover_schema / export / import
     → Kafka Connect + Debezium    CDC streaming infrastructure
Data: Postgres (system of record + event store) · Redis (ephemeral/cache) · MinIO (claim-check staging)
AI:   Planner (Python) · LLM Service / Tool Generator (Python) · Context7 MCP
Obs:  OpenTelemetry → SigNoz, trace_id carried end-to-end
```

Full Mermaid diagram + the draft-first sequence diagram: [`docs/architecture/overview.md`](docs/architecture/overview.md).

---

## 4. Stack rationale — why each technology

| Layer | Technology | Why this, not the alternative |
|-------|-----------|-------------------------------|
| **API Gateway, Orchestrator, Temporal Adapter** | **Go** | Low-latency, low-memory, strong concurrency for the always-on stateless services that fan out work and bridge Kafka. These are hot-path I/O coordinators — Go's goroutines + small footprint fit a single-VM deploy far better than a JVM or Python event loop. |
| **Planner, LLM Service, Tool Generator, agents** | **Python** | The AI layer lives where the ecosystem is — OpenAI/Azure SDKs, LangChain-style tooling, fast iteration on prompts and connector codegen. Rewriting this in Go would trade velocity for nothing. |
| **Frontend** | **Next.js (React + SSR)** | Server-side rendering for the monitoring UI + a mature component ecosystem; same-origin API routing through Traefik keeps auth cookies simple. |
| **Workflow orchestration** | **Temporal** | Pipelines are long-running and must survive process restarts, retries, and partial failure deterministically. Temporal gives durable execution, replay, and first-class signals for human-in-the-loop (table selection, pause/resume) — building this on cron + a queue would reinvent it badly. |
| **Message bus** | **Kafka** (Apache 2.0, `apache/kafka:3.7.0`) | The event stream of truth. Decouples the orchestrator from stateless agent workers, and is the durable backbone for domain events (replay, WebSocket streaming, audit) and CDC. Migrated off Confluent images to Apache 2.0 to avoid CCL licensing constraints for self-hosters. |
| **CDC** | **Debezium** (Apache 2.0, `debezium/connect`) on Kafka Connect | Battle-tested log-based change capture across MySQL/Postgres/Mongo/SQL Server/Oracle. Publication-before-slot ordering is enforced (see CAPABILITIES §5). |
| **Connectors** | **MCP servers (dockerized, versioned)** | A single uniform tool interface (`test_connection`, `discover_schema`, `export`, `import`) across every connected system — 17 pre-built connectors today, plus any connector the tool-generator writes on demand — each a versioned container. Lets the platform treat databases and SaaS APIs identically, deploy connectors on demand, and run customer-private connectors in isolation. |
| **System of record** | **PostgreSQL** | Pipelines, executions, ownership, and the `pipeline_run_events` event store. One relational store for state + replay; managed (Azure Flexible Server) in production for backups/PITR with no DBA. |
| **Ephemeral state** | **Redis** | Fast coordination, caching, short-lived state. |
| **Large-payload staging** | **MinIO (S3-compatible)** | Claim-check pattern — batch chunks land in object storage and Kafka carries a reference, keeping the bus lean. |
| **Observability** | **OpenTelemetry → SigNoz** | `trace_id` propagated through every event and service boundary for end-to-end correlation (UI → logs → traces). |
| **Reverse proxy / TLS** | **Traefik** | Single public entrypoint, automatic Let's Encrypt certs (DNS challenge via Cloudflare), routes `/api`→gateway, `/ws`→websocket, everything else→frontend. |

---

## 5. The two data paths

rsync-ai has exactly **two** execution paths (a third, "Path A" direct-transfer, was deleted 2026-05-22 — do not reintroduce it).

- **Path B — Batch** (Kafka sink + MinIO claim-check): executor extracts rows → chunks to Kafka (large payloads via MinIO claim-check) → `kafka-mcp-sink` → destination connector `ensure_table` + `import_data`/`upsert_data`.
- **Path C — CDC streaming** (Debezium): publication → REPLICA IDENTITY → replication slot → Debezium → Kafka topics → sink → destination. Publication MUST be created before the slot (CAPABILITIES §5).

**MCP boundary (control plane ≠ bulk transport):** MCP is the control / invocation / packaging layer — workers call connector tools (`test_connection`, `discover_schema`, `export`, `import_data`/`upsert_data`, `ensure_table`) at the source-export and dest-write **boundaries** only. Sustained bulk does **not** ride MCP: in Path B the batch rows flow over Kafka with large payloads staged in MinIO (claim-check), and in Path C the CDC byte stream rides Debezium → Kafka → `kafka-mcp-sink` and never touches MCP. MCP bounds and packages each batch; it is not the bulk byte transport. This is the single source of truth for the boundary — cross-link here rather than restating it.

---

## 6. Cross-cutting invariants

- **Tenant isolation:** every per-connection / per-pipeline DB read includes `AND user_id = $X`. Canonical primitive: `Manager.GetForUser`.
- **Secrets:** connection configs encrypted at rest (`enc.v1:` prefix); never logged (only key counts).
- **Trace correlation:** `trace_id` flows through Kafka headers and event payloads end-to-end.
- **Redacted by default:** stored events are redacted; raw access is RBAC-gated and audited.
- **Connector contract:** a fix to a hand-curated connector must be propagated to its Jinja template + regenerated (CAPABILITIES §3).

---

## 7. Where to go deeper

| You want to… | Read |
|--------------|------|
| See the full component + sequence diagrams | [`docs/architecture/overview.md`](docs/architecture/overview.md) |
| Understand tenant isolation / workspace roles / IDOR gates | [`docs/architecture/multi-tenancy.md`](docs/architecture/multi-tenancy.md) |
| Understand one service in depth | [`docs/services/<service>/HLD.md` + `LLD.md`](docs/services/INDEX.md) |
| Know the Kafka topic contracts | [`docs/architecture/kafka-topics.md`](docs/architecture/kafka-topics.md) |
| Run it locally | [`docs/getting-started/quickstart.md`](docs/getting-started/quickstart.md) |
| Build a connector | [`docs/connectors/developer-guide.md`](docs/connectors/developer-guide.md) |
| Deploy to production | [`docs/deployment/self-hosting.md`](docs/deployment/self-hosting.md) · [`docs/deployment/kubernetes.md`](docs/deployment/kubernetes.md) |
