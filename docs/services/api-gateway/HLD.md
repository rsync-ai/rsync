## API Gateway — HLD

### Purpose
The API Gateway is the **product-facing REST + WebSocket edge** for rsync-ai. It:
- serves the frontend,
- triggers pipeline creation/execution,
- exposes connector metadata + connection management,
- proxies connector generation to the Tool Generator,
- initiates OAuth flows and stores tokens/config securely.

### Runtime Interface
- **Container**: `rsync-ai-api-gateway`
- **Internal port**: `8080`
- **Host port**: `5001` (dev; avoids macOS conflicts on 5000)
- **Endpoints**
  - `GET /health`
  - `GET /ws` (pipeline progress + agent updates)
  - `POST /api/v1/chat/message` (NL-driven entrypoint)
  - `POST /api/v1/connectors/generate` (proxy to tool-generator)
  - `POST /api/v1/connectors/detect-category` (category hinting)
  - `GET /api/v1/connectors`, `GET /api/v1/connectors/:name` (includes tier/QA metadata)
  - auth + connections + pipelines + CDC endpoints (see API reference)

### Responsibilities (What it owns)
- **HTTP API**: stable surface for UI and external clients.
- **Auth**: login/register/session handling (JWT-based in current implementation).
- **Workflow triggering**:
  - prefers **Temporal** for V2 pipeline execution and progress timeline,
  - falls back to direct Kafka publishing if Temporal is unavailable.
- **Connector lifecycle UX**:
  - name canonicalization and generation proxy,
  - connector list/get by reading `shared/mcp-connectors` metadata,
  - docker status inspection for MCP connectors (via docker socket).
- **OAuth UX**:
  - starts OAuth, receives callback, stores tokens for later connector use.
- **Real-time UI**:
  - Kafka → WebSocket bridge and event projector for progress tables.

### Dependencies
- **Postgres**: main metadata store (pipelines, connections, oauth token rows, etc.)
- **Kafka**: agent messaging + pipeline domain events
- **Temporal**: workflow orchestration (V2 path)
- **Redis**: correlation store for V2 request/reply activities
- **Tool Generator**: connector generation proxy target
- **Shared volume**: read-only access to `shared/mcp-connectors`
- **Docker socket**: read container status for connector cards

### Data Ownership
- Owns API-level CRUD and queries over Postgres tables (via `api-gateway/internal/db` + handlers).
- Does not own connector artifacts; it reads them from `shared/mcp-connectors`.

### Scaling / HA Notes
- Stateless HTTP service; can scale horizontally **if**:
  - Redis/DB/Kafka are shared,
  - websocket fanout is handled (today: single instance hub; multi-instance needs pub/sub).

### Observability
- OpenTelemetry tracing + trace-aware logging.
- Fluent Bit logs → OTel Collector for trace/log correlation.

### Failure Modes
- **DB down**: gateway may run with limited “mock data” behavior (degraded).
- **Temporal down**: falls back to direct Kafka publishing (reduced UI timeline fidelity).
- **Tool generator down**: connector generation endpoints fail; existing connectors still list.
- **Docker socket unavailable**: connector docker status fields degrade (still list metadata).


