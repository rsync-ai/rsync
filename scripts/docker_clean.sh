#!/usr/bin/env bash
set -euo pipefail

# Clean local docker compose projects used by rsync-ai.
# - MCP connectors: rsync-ai-mcp (docker-compose.mcp.yml sets name: rsync-ai-mcp)
# - E2E dbs:        rsync-ai-e2e (we use -p in scripts)
# - Core stack:     rsync-ai (default)
#
# By default: stops/removes containers + orphans (keeps volumes).
# Pass --volumes to also remove volumes.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

WITH_VOLUMES="false"
if [[ "${1:-}" == "--volumes" ]]; then
  WITH_VOLUMES="true"
fi

down_flags=(--remove-orphans)
if [[ "${WITH_VOLUMES}" == "true" ]]; then
  down_flags+=(-v)
fi

log() { printf "\n==> %s\n" "$*"; }

log "Stopping MCP connectors project (rsync-ai-mcp)…"
docker compose -p rsync-ai-mcp -f docker-compose.mcp.yml down "${down_flags[@]}" >/dev/null 2>&1 || true

log "Stopping E2E project (rsync-ai-e2e)…"
docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml down "${down_flags[@]}" >/dev/null 2>&1 || true

log "Stopping core stack project (rsync-ai)…"
docker compose -p rsync-ai -f docker-compose.yml down "${down_flags[@]}" >/dev/null 2>&1 || true

log "Force removing any lingering fixed-name containers (best-effort)…"
for c in \
  rsync-ai-aws-s3-v1-0-0-mcp \
  rsync-ai-aws-s3-mcp \
  rsync-ai-aws_s3-mcp \
  rsync-ai-debezium-v1-0-0-mcp \
  rsync-ai-debezium-mcp \
  rsync-ai-freshdesk-mcp \
  rsync-ai-github-v1-0-0-mcp \
  rsync-ai-github-mcp \
  rsync-ai-google_sheets-mcp \
  rsync-ai-hubspot-v1-0-0-mcp \
  rsync-ai-hubspot-mcp \
  rsync-ai-kafka-mcp-sink-v1-0-0-mcp \
  rsync-ai-kafka-mcp-sink-mcp \
  rsync-ai-minio-v1-0-0-mcp \
  rsync-ai-mysql-mcp \
  rsync-ai-mysql-v1-0-0-mcp \
  rsync-ai-postgresql-mcp \
  rsync-ai-postgresql-v1-0-0-mcp \
  rsync-ai-segment-mcp \
  rsync-ai-shopify-mcp \
  rsync-ai-slack-mcp \
  rsync-ai-snowflake-mcp \
  rsync-ai-sqlite-mcp \
  rsync-ai-stripe-mcp \
  rsync-ai-twitter-mcp \
  rsync-ai-tool-generator \
  rsync-ai-context7-v1-0-0-mcp \
  rsync-ai-planner \
  rsync-ai-llm-service \
  rsync-ai-api-gateway \
  rsync-ai-orchestrator \
  rsync-ai-frontend \
  rsync-ai-mysql-e2e \
  rsync-ai-postgres-e2e \
  rsync-ai-minio \
  rsync-ai-minio-init
do
  docker rm -f "${c}" >/dev/null 2>&1 || true
done

log "✅ Docker cleanup complete. (volumes removed: ${WITH_VOLUMES})"


