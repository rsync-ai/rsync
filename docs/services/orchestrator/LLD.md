## Backend Orchestrator — LLD

### Repo Location
- `backend-orchestrator/`

### Entry Point
- `backend-orchestrator/cmd/orchestrator/main.go`

### Key Internal Modules (high-signal)
- `backend-orchestrator/internal/config`
  - configuration loading (Viper) and env parsing
- `backend-orchestrator/internal/kafka`
  - Kafka manager + producer/consumer, topology manager
- `backend-orchestrator/internal/agents/*`
  - `consumer`: consumes control topics
  - `executor`: MCP execution (legacy used for HTTP endpoints, but core runtime pieces remain)
  - `validator`: config validation (legacy HTTP path)
  - `sentinel`: health + auto-restart hooks
  - `retention`: cleanup policies
- `backend-orchestrator/internal/mcp`
  - MCP client implementation used to call connector servers (HTTP/stdio variants)
- `backend-orchestrator/internal/workers`
  - correlation client init (Redis-based V2 request/reply)
- `backend-orchestrator/internal/telemetry`
  - OpenTelemetry + trace-log correlation hook

### Startup Sequence (simplified)
1) load config + setup logging
2) init telemetry (if enabled)
3) connect to Postgres
4) init Kafka manager and topology manager (best-effort)
5) init correlation client (Redis)
6) initialize agent manager and worker set (staggered startup to reduce Kafka rebalance thrash)
7) start HTTP server (Gin) for debugging/legacy endpoints + health

### MCP Connector Resolution
The orchestrator resolves connector runtime targets using:
- connector id + version
- on-disk `shared/mcp-connectors/<id>/latest.json` and `versions/<v>/metadata.json`

### Configuration (Env Vars)
Database:
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`

Kafka:
- `KAFKA_BROKERS`
- `KAFKA_USE_AVRO`, `SCHEMA_REGISTRY_URL`, `AVRO_AUTO_REGISTER_SCHEMAS`

Connectors:
- `TOOLS_DIR=/app/shared/mcp-connectors`
- `TOOL_GENERATOR_URL` (self-healing deploy)

Security:
- `JWT_SECRET`
- `ENCRYPTION_KEY` (must be stable; decrypts stored configs)

Redis:
- `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`
- `CORRELATION_CLAIM_TTL`

Observability:
- `OTEL_*`
- `LOG_FORMAT`, `LOG_LEVEL`


