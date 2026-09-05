# Context7 MCP Server

MCP (Managed Connector Platform) server for Context7 documentation lookup.

## Purpose

Provides API documentation lookup for the tool-generator service during connector generation. Uses the Context7 API to resolve library names and fetch relevant documentation.

## Endpoints

- `GET /health` - Health check
- `POST /resolve` - Resolve library name to Context7 library ID
- `POST /docs` - Fetch documentation for a library

## Environment Variables

- `PORT` - Server port (default: 8080)
- `CONTEXT7_API_URL` - Context7 API base URL (default: https://api.context7.com)
- `CONTEXT7_API_KEY` - Context7 API key (required)
- `CONTEXT7_TIMEOUT` - Request timeout in seconds (default: 30)
- `CONTEXT7_DEFAULT_TOKENS` - Default token limit for docs (default: 10000)
- `LOG_LEVEL` - Logging level (default: INFO)

## Usage

The tool-generator service calls this MCP server during connector generation to fetch API documentation, which is then used by the DocResearcherAgent to understand the target API's structure and capabilities.

## Docker

Built and run via docker-compose as part of the main stack:

```bash
docker-compose up context7-mcp
```

Available at: http://localhost:8087 (external) or http://context7-mcp:8080 (internal)
