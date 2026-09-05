#!/usr/bin/env bash
# staging-down.sh — the blessed way to release the staging stack and hand the
# shared self-hosted box back to CI.
#
# Counterpart to scripts/staging-up.sh. staging-up sets a DURABLE "staging owns
# the stack" marker (scripts/_stack_lock.sh: set_staging_hold) so the e2e gate
# fail-closes instead of clobbering the live Azure-wired stack. This script tears
# the staging stack down and CLEARS that marker, so e2e/run_gate.sh may run
# again. Run it when you're finished with a manual staging session.
#
# Usage:  scripts/staging-down.sh
#   STAGING_KEEP_CONTAINERS=1  clear the hold only; leave containers running
#                              (e.g. you want to keep browsing staging but still
#                              let CI take over — NOT usually what you want).
# Exit:   0 = staging released · non-zero = a running e2e gate holds the mutex.

set -euo pipefail

# Resolve the MAIN repo root (not a worktree): the shared stack + .env.staging
# live there. Mirrors scripts/staging-up.sh.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(git -C "$SCRIPT_DIR" rev-parse --git-common-dir)"
case "$COMMON_DIR" in /*) ;; *) COMMON_DIR="$SCRIPT_DIR/$COMMON_DIR" ;; esac
MAIN_ROOT="$(cd "$COMMON_DIR/.." && pwd)"
cd "$MAIN_ROOT"

ENV_FILE="${ENV_FILE:-$MAIN_ROOT/.env.staging}"
COMPOSE=(-f docker-compose.yml -f docker-compose.prod.yml -f docker-compose.staging.yml)
[[ -f "$ENV_FILE" ]] && COMPOSE+=(--env-file "$ENV_FILE")

# shellcheck source=_stack_lock.sh
source "$SCRIPT_DIR/_stack_lock.sh"
trap 'release_stack_lock' EXIT

# Take the mutex so we cannot race a gate's bring-up while tearing down. If a
# gate is actively holding it, wait/bail rather than half-tear-down under it.
acquire_stack_lock "staging-down (PID $$)" \
  || { echo "🛑 an e2e gate is holding the stack — wait for it to finish, then retry." >&2; exit 1; }

if [[ "${STAGING_KEEP_CONTAINERS:-}" == "1" ]]; then
  echo "▶ STAGING_KEEP_CONTAINERS=1 — leaving containers up; clearing the staging hold only."
else
  echo "▶ Tearing down the staging stack (project rsync-ai)…"
  docker compose "${COMPOSE[@]}" down --remove-orphans || true
fi

clear_staging_hold

echo "✅ staging hold cleared — the e2e gate may run again."
echo "   The shared stack is now free; the next CI gate (or a dev bring-up) will populate it."
