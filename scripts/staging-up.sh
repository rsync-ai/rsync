#!/usr/bin/env bash
# staging-up.sh — the ONE correct way to bring up the staging stack.
#
# Wraps the canonical launch (docker-compose.yml + prod + staging, --env-file
# .env.staging) so nobody fat-fingers a bare `docker compose up -d` and silently
# gets the DEV stack (local postgres, wrong ports) — the trap the runbook warns
# about. Then it:
#   1. takes the shared-stack mutex (scripts/_stack_lock.sh) so the bring-up
#      cannot race a running e2e gate's bring-up on the same host, and
#   2. runs Gate 0 (scripts/preflight-staging-runtime.sh --fix) so the shared
#      services are reconciled to Azure staging even if a prior gate left them
#      on base/e2e wiring.
# The lock is released once the stack is up + reconciled (the stack then runs
# detached); the symmetric guards handle whoever takes the stack over next.
#
# Usage:  scripts/staging-up.sh
# Exit:   0 = staging up and wired for Azure · non-zero = bring-up / guard failed.

set -euo pipefail

# Resolve the MAIN repo root (not a worktree): the shared stack + .env.staging
# live there. Mirrors scripts/preflight-staging-runtime.sh.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(git -C "$SCRIPT_DIR" rev-parse --git-common-dir)"
case "$COMMON_DIR" in /*) ;; *) COMMON_DIR="$SCRIPT_DIR/$COMMON_DIR" ;; esac
MAIN_ROOT="$(cd "$COMMON_DIR/.." && pwd)"
cd "$MAIN_ROOT"

ENV_FILE="${ENV_FILE:-$MAIN_ROOT/.env.staging}"
[[ -f "$ENV_FILE" ]] || { echo "🛑 $ENV_FILE not found — run on the staging machine." >&2; exit 2; }

# Belt-and-suspenders: INTERNAL_SERVICE_SECRET must be present or S2S calls 401.
# Gate 0 (preflight-staging-runtime.sh --fix, run below) generates + applies it,
# so just surface the state here — don't duplicate the heal.
if ! grep -qE '^INTERNAL_SERVICE_SECRET=.+' "$ENV_FILE"; then
  echo "ℹ️  INTERNAL_SERVICE_SECRET is empty in $ENV_FILE — Gate 0 will generate + apply it." >&2
fi

# shellcheck source=_stack_lock.sh
source "$SCRIPT_DIR/_stack_lock.sh"
trap 'release_stack_lock' EXIT
acquire_stack_lock "staging-up (PID $$)" \
  || { echo "🛑 an e2e gate is holding the stack — wait for it to finish." >&2; exit 1; }

COMPOSE=(-f docker-compose.yml -f docker-compose.prod.yml -f docker-compose.staging.yml --env-file "$ENV_FILE")

# Clean switch — one stack at a time. Tear the current mode down (typically a
# dev/e2e-wired stack the gate left behind) BEFORE bringing staging up, so every
# service is RECREATED Azure-wired instead of a stale container being reused.
# This is the fix for the "api-gateway still on local pipeline_db" split-brain:
# the shared rsync-ai-api-gateway container is replaced with one wired to
# .env.staging (Azure). --remove-orphans also clears a leftover Traefik/socket-
# proxy from a prior run. The lock is already held, so this cannot race a live
# gate. Opt out (fast additive reconcile, no downtime) with STAGING_NO_DOWN=1.
if [[ "${STAGING_NO_DOWN:-}" != "1" ]]; then
  echo "▶ Clean switch: tearing down the current rsync-ai stack first…"
  docker compose "${COMPOSE[@]}" down --remove-orphans || true
fi

echo "▶ Bringing up staging stack (Azure-wired)…"
docker compose "${COMPOSE[@]}" up -d

echo "▶ Gate 0 — reconciling shared-service wiring for Azure staging"
"$SCRIPT_DIR/preflight-staging-runtime.sh" --fix

# Claim the shared stack for staging PERSISTENTLY. The mkdir mutex (released by
# the EXIT trap below) only serialized this bring-up; it says nothing once we
# exit and the staging stack runs detached. This durable marker tells the e2e
# gate (e2e/run_gate.sh) that staging owns project `rsync-ai`, so a later
# post-merge/PR gate fail-closes instead of reconciling the stack back to local
# wiring and clobbering us. Release it with scripts/staging-down.sh.
set_staging_hold "staging-up (PID $$)"

echo "✅ staging is up and wired for Azure, and marked as owning the shared stack."
echo "   CI's data-pipeline gate will refuse to clobber it until you run: scripts/staging-down.sh"
