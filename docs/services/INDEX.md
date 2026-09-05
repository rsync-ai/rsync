## Services Documentation Index

### Purpose
This directory contains **service-scoped** documentation in two layers:
- **HLD**: architecture, boundaries, interfaces, deployment, NFRs
- **LLD**: code-level structure, key flows, data models, config, failure modes

### First‑party services (owned in this repo)
- **API Gateway**: `api-gateway/`
  - `docs/services/api-gateway/HLD.md`
  - `docs/services/api-gateway/LLD.md`
- **Backend Orchestrator**: `backend-orchestrator/`
  - `docs/services/orchestrator/HLD.md`
  - `docs/services/orchestrator/LLD.md`
- **Temporal Adapter**: `backend-temporal-adapter/`
  - `docs/services/temporal-adapter/HLD.md`
  - `docs/services/temporal-adapter/LLD.md`
- **LLM Service**: `llm-service/` (LLM gateway)
  - `docs/services/llm-service/HLD.md`
  - `docs/services/llm-service/LLD.md`
- **Planner**: `llm-service/` (planner command)
  - `docs/services/planner/HLD.md`
  - `docs/services/planner/LLD.md`
- **Frontend**: `frontend/`
  - `docs/services/frontend/HLD.md`
  - `docs/services/frontend/LLD.md`

### Platform-managed runtime services (built from repo, but “connectors”)
- **Context7 MCP**: `shared/mcp-connectors/internal/context7/`
  - `docs/services/context7-mcp/HLD.md`
  - `docs/services/context7-mcp/LLD.md`
- **Kafka Connect (Debezium)**: `shared/internal/infra/kafka-connect/`
  - `docs/services/kafka-connect/HLD.md`
  - `docs/services/kafka-connect/LLD.md`
- **MCP Connector Runtime (generic)**: `shared/mcp-connectors/<connector>/`
  - `docs/services/mcp-connector-runtime/HLD.md`
  - `docs/services/mcp-connector-runtime/LLD.md`

### Infra dependencies (third‑party images, configured by `docker-compose.yml`)
These are documented as configured in this repo (ports/env/volumes), not as full upstream system docs:
- `docs/services/infra/postgres/HLD.md` + `LLD.md`
- `docs/services/infra/kafka/HLD.md` + `LLD.md`
- `docs/services/infra/redis/HLD.md` + `LLD.md`
- `docs/services/infra/temporal/HLD.md` + `LLD.md` (includes Temporal UI + admin tools)
- `docs/services/infra/schema-registry/HLD.md` + `LLD.md`
- `docs/services/infra/observability/HLD.md` + `LLD.md` (Fluent Bit + OTel Collector)


