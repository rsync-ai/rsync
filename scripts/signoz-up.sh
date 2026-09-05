#!/usr/bin/env bash
#
# signoz-up.sh — bring the SigNoz observability stack up SAFELY.
#
# Why this exists:
#   SigNoz (ClickHouse + collector + UI) must ALWAYS be launched from the canonical
#   main checkout, never from a `.claude/worktrees/<id>/` worktree. When it was once
#   `docker compose up`'d from a worktree, ClickHouse's bind-mounts pointed into that
#   worktree path; after the worktree was pruned the mount source vanished, so when
#   ClickHouse was later OOM-killed Docker's `restart: unless-stopped` could not bring
#   it back — observability went dark for hours and nobody noticed until a pipeline
#   test "found new issues" that were really just missing telemetry.
#
#   This script resolves the *main* working tree (via the shared git common dir) and
#   runs compose from there with a fixed project name, so the mounts are always stable.
#
# Usage:
#   scripts/signoz-up.sh            # up -d (recreates only what changed)
#   scripts/signoz-up.sh down       # stop the stack (keeps the named data volume)
#   scripts/signoz-up.sh status     # ps + clickhouse ingest-lag check
#
# Override ClickHouse memory bound (prod VMs with more RAM):
#   CLICKHOUSE_MEM_LIMIT=8g scripts/signoz-up.sh
set -euo pipefail

# Resolve the MAIN repo root even when invoked from inside a worktree.
# `git rev-parse --git-common-dir` points at the shared .git (…/<main>/.git),
# whose parent is the canonical main working tree.
GIT_COMMON_DIR="$(git rev-parse --git-common-dir 2>/dev/null || true)"
if [[ -z "${GIT_COMMON_DIR}" ]]; then
  echo "❌ not inside the rsync-ai git repo" >&2
  exit 1
fi
MAIN_ROOT="$(cd "$(dirname "${GIT_COMMON_DIR}")" && pwd)"
COMPOSE_DIR="${MAIN_ROOT}/deploy/signoz/docker"

if [[ ! -f "${COMPOSE_DIR}/docker-compose.yaml" ]]; then
  echo "❌ SigNoz compose not found at ${COMPOSE_DIR}" >&2
  exit 1
fi

# Refuse to run from a worktree path — the whole point of this guard.
case "${MAIN_ROOT}" in
  *"/.claude/worktrees/"*)
    echo "❌ refusing to launch SigNoz from a worktree path: ${MAIN_ROOT}" >&2
    echo "   Launch it from the main checkout instead." >&2
    exit 1
    ;;
esac

cmd="${1:-up}"
cd "${COMPOSE_DIR}"
echo "▶ SigNoz compose dir: ${COMPOSE_DIR}"

case "${cmd}" in
  up)
    docker compose -p signoz up -d
    echo "✅ SigNoz up. UI: http://localhost:3301  ·  ClickHouse HTTP: http://localhost:8123"
    ;;
  down)
    docker compose -p signoz down
    echo "✅ SigNoz stopped (named data volume 'signoz_clickhouse' preserved)."
    ;;
  status)
    docker compose -p signoz ps
    echo "— ClickHouse ingest lag —"
    curl -s "http://localhost:8123/" --data \
      "SELECT dateDiff('second', toDateTime(max(timestamp)/1e9), now()) AS lag_seconds FROM signoz_logs.logs_v2 WHERE timestamp > (toUnixTimestamp(now()-3600)*1e9)" \
      2>/dev/null || echo "(clickhouse not reachable)"
    ;;
  *)
    echo "usage: scripts/signoz-up.sh [up|down|status]" >&2
    exit 2
    ;;
esac
