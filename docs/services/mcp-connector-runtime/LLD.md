## MCP Connector Runtime — LLD

### Connector Artifact Layout (Versioned)
See `shared/mcp-connectors/<id>/`:
- `latest.json` → points to `current_version`
- `versions/<version>/connector.py` + `metadata.json` + `spec.json` + Dockerfile

### Deployment Compose
- `docker-compose.mcp.yml`
  - generated from the connectors on disk
  - each service includes labels:
    - `rsync-ai.connector=<id>`
    - `rsync-ai.connector_version=<version>`

### Container Naming
Standard:
- `rsync-ai-<id>-vX-Y-Z-mcp`

### Health
Connectors implement:
- `GET /health` returning status and basic info.

### Discovery and Routing
- API Gateway:
  - reads `metadata.json` and checks docker status (via docker socket)
- Orchestrator/Executor:
  - resolves connector **id + version** and calls the correct container


