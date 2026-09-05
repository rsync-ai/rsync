#!/usr/bin/env bash
set -euo pipefail

# Wipes saved app state from the main Postgres (pipeline_db):
# - connections
# - pipelines + executions
# - CDC resources/checkpoints
# - connection_access_logs
#
# This is meant for LOCAL dev only.

log() { printf "\n==> %s\n" "$*"; }
die() { echo "❌ $*" >&2; exit 1; }

PG_CONTAINER="${PG_CONTAINER:-postgres}"
PG_USER="${PG_USER:-user}"
PG_DB="${PG_DB:-pipeline_db}"

log "🔎 Verifying main Postgres container exists…"
docker inspect "${PG_CONTAINER}" >/dev/null 2>&1 || die "Missing Postgres container '${PG_CONTAINER}'. Is the stack running?"

log "🧹 Truncating connections/pipelines state in ${PG_CONTAINER}/${PG_DB}…"
docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -v ON_ERROR_STOP=1 -c "
  -- Order doesn’t matter with CASCADE, but keep it readable.
  TRUNCATE TABLE
    connection_access_logs,
    pipeline_checkpoints,
    cdc_resources,
    executions,
    pipelines,
    connections
  RESTART IDENTITY
  CASCADE;
"

log "✅ Local app connections/pipelines wiped."

