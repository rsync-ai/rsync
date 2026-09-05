## Context7 MCP — LLD

### Repo Location
- `shared/mcp-connectors/internal/context7/`

### Entry Point
- `shared/mcp-connectors/internal/context7/server.py`

### API Surface
- `GET /health`
  - returns service status and whether an API key is configured
- `POST /resolve`
  - calls Context7 v2: `GET /v2/search?query=<name>`
  - returns top `library_id`
- `POST /docs`
  - calls Context7 v2:
    - `GET /v2/docs/info/{owner}/{repo}` (or `.../{version}`)
  - returns raw documentation text (topic-filtered when provided)

### Configuration (Env Vars)
- `PORT` (default 8080)
- `CONTEXT7_API_URL` (default `https://context7.com/api`)
- `CONTEXT7_API_KEY` (optional; increases rate limits)
- `CONTEXT7_TIMEOUT` (default 30s)
- `CONTEXT7_DEFAULT_TOKENS` (default 10000)
- `LOG_LEVEL`

### Notes
This service is intentionally small; caching is implemented in tool-generator (`DocResearcherAgent`) rather than here.


