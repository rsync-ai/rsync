#!/usr/bin/env bash
set -euo pipefail

# Reset generated MCP connectors (safe for demos)
#
# What it does:
# - Stops/removes ONLY generated connector containers (label: rsync-ai.mcp=true)
# - Deletes generated connector directories under shared/mcp-connectors (dirs that have latest.json)
# - Regenerates docker-compose.mcp.yml based on what remains on disk
#
# What it does NOT do:
# - Does NOT stop core stack services (api-gateway, tool-generator, llm-service, etc.)
# - Does NOT stop the system Context7 MCP service (context7-mcp) — it's part of rsync-ai core stack.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

log() { printf "\n==> %s\n" "$*"; }

log "Stopping generated connector containers (label rsync-ai.mcp=true)…"
docker ps --filter "label=rsync-ai.mcp=true" --format "{{.Names}}" | xargs -r docker stop >/dev/null 2>&1 || true

log "Removing generated connector containers (label rsync-ai.mcp=true)…"
docker ps -a --filter "label=rsync-ai.mcp=true" --format "{{.Names}}" | xargs -r docker rm >/dev/null 2>&1 || true

log "Deleting generated connector directories (shared/mcp-connectors/* with latest.json)…"
find shared/mcp-connectors -mindepth 1 -maxdepth 1 -type d -exec test -f '{}/latest.json' \; -print0 | xargs -0 -r rm -rf
rm -rf shared/mcp-connectors/__pycache__ shared/mcp-connectors/_failed >/dev/null 2>&1 || true

log "Regenerating docker-compose.mcp.yml…"
python3 scripts/mcp_generate_compose.py >/dev/null 2>&1 || true

log "✅ Generated connector reset complete."


