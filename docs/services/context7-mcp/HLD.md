## Context7 MCP — HLD

### Purpose
`context7-mcp` is an internal FastAPI service that wraps the Context7 REST API and exposes a minimal interface used by the Tool Generator to fetch documentation.

It exists to:
- normalize Context7 API calling patterns (resolve + docs),
- centralize API key handling and timeouts,
- provide a stable internal dependency for the agentic tool generation pipeline.

### Runtime Interface
- **Container**: `rsync-ai-context7-v1-0-0-mcp`
- **Internal port**: `8080`
- **Host port**: `8087`
- **Endpoints**
  - `GET /health`
  - `POST /resolve` (library name → library id)
  - `POST /docs` (library id → documentation text)

### Dependencies
- External: Context7 public REST API (`CONTEXT7_API_URL`)
- Internal callers:
  - Tool Generator (`DocResearcherAgent`)

### Observability
- JSON logging (service name `context7-mcp`)
- OpenTelemetry enabled via env in docker-compose (where configured)

### Failure Modes
- Context7 upstream down / rate-limited → tool generation quality degrades or fails (depending on fallback behavior).


