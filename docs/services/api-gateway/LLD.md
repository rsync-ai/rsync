## API Gateway — LLD

### Repo Location
- `api-gateway/`

### Entry Point
- `api-gateway/cmd/server/main.go`

### Process Model
Single Go process that:
- initializes DB + migrations,
- initializes Kafka producer + consumer,
- initializes Temporal client (best-effort),
- starts WebSocket hub + Kafka bridge,
- registers Gin routes under `/api/v1/*`.

### Key Packages
- `api-gateway/internal/db`
  - DB connection init and migrations (`migrations/`)
- `api-gateway/internal/kafka`
  - unified producer (JSON/Avro) and consumer wrappers
- `api-gateway/internal/handlers`
  - HTTP handlers for chat, pipelines, connectors, OAuth, connections, schema registry, etc.
  - tool generation proxy and connector name canonicalization
- `api-gateway/internal/websocket`
  - websocket hub + Kafka bridge (real-time UI updates)
- `api-gateway/internal/projector`
  - projects Kafka domain events into DB “progress” tables (best-effort view)
- `api-gateway/internal/telemetry`
  - tracing + trace context middleware + log correlation helpers

### Connector Metadata Path (JIT Integration)
- Reads connector artifacts from:
  - `MCP_CONNECTORS_PATH` or
  - docker default: `/app/shared/mcp-connectors`
- Exposes `quality_tier`, `qa_warnings`, `qa_metadata`, and docker container status in connector APIs.

### Tool Generation Proxy (V2)
- Route: `POST /api/v1/connectors/generate`
- Behavior:
  - canonicalize connector id (kebab-case)
  - if connector exists and is already versioned:
    - short-circuit unless `force_regenerate=true`
  - otherwise proxy to tool-generator `POST /v1/generate`
  - return frontend-friendly payload, including metadata returned by tool-generator

### Category Detection (API Domain Hint)
- Route: `POST /api/v1/connectors/detect-category`
- Implements deterministic category inference with confidence and source:
  - oauth registry match → 0.95
  - known apis config → 0.85
  - partial match → 0.65
  - keyword heuristic → 0.45
  - default api_saas → 0.30

### Configuration (Env Vars)
Common:
- `PORT` (default 8080)
- `DATABASE_URL`
- `KAFKA_BROKERS`
- `REDIS_ADDRESS`
- `TEMPORAL_ADDRESS`
- `ORCHESTRATOR_URL`
- `TOOL_GENERATOR_URL`
- `MCP_CONNECTORS_PATH` / `TOOLS_PATH`
- `JWT_SECRET`
- `ENCRYPTION_KEY`

OAuth:
- `OAUTH_CALLBACK_URL`
- `<PROVIDER>_CLIENT_ID`, `<PROVIDER>_CLIENT_SECRET` (GitHub/Google/HubSpot/Salesforce/Pipedrive)

Observability:
- `OTEL_*`
- `LOG_FORMAT`, `LOG_LEVEL`

### Persistence / Migrations
- Migrations live under `api-gateway/migrations/`.
- Startup runs migrations best-effort; failures are logged.


