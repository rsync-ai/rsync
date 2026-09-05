#!/usr/bin/env bash
# test-staging-faithful.sh — one command that makes a green staging run
# actually mean "this will work on prod".
#
# WHY THIS EXISTS
#   We kept passing on staging and failing on prod. The reason was never a
#   bad config value — it was that staging tests in a WARM + DIRTY state that
#   prod never matches:
#     • WARM   — the destination MCP connector has been up for hours, so the
#                sink's one-shot startup DDL probe always wins. A cold prod
#                start (everything restarts together) made that probe miss and
#                permanently disabled auto-create-table. (Fixed in PR #206; the
#                deterministic guard is TestDDLSupportRetriesTransientStartupMiss.)
#     • DIRTY  — staging writes to a destination that already has the tables
#                from prior runs, so a missing-create-table bug is invisible.
#   This wrapper forces the conditions prod actually hits and verifies the
#   DESTINATION TRUTH (row counts), never the "completed" badge.
#
# THE GATES (fail fast on any)
#   0. Runtime wiring  — the RUNNING shared containers are wired for Azure, not
#                        left on CI/e2e base defaults (scripts/preflight-staging-runtime.sh).
#                        A CI e2e run shares this stack and can leave kafka-mcp-sink
#                        pointed at the local pipeline_db, silently stalling every
#                        batch pipeline at 80%. Fail loud; heal with --fix.
#   1. Config-parity   — no Postgres URL drifts from sslmode=require
#                        (scripts/preflight-prod-config.sh).
#   2. Cold start      — bounce the destination MCP connector + sink so the next
#                        pipeline's worker boots against a cold connector
#                        (best-effort: the race window is narrow — the *reliable*
#                        latch guard is the unit test shipped in PR #206).
#   3. Empty dest      — DROP SCHEMA before the run (done inside e2e-shopify-pg.sh).
#   4. Destination-truth — assert landed rows == source rows
#                        (done inside e2e-shopify-pg.sh).
#
# USAGE (from repo root, on the staging machine):
#   scripts/test-staging-faithful.sh
#
#   # before a prod deploy, also run gate 1 against the real prod env:
#   ENV_FILE=.env.prod scripts/preflight-prod-config.sh
#
# Override anything via env:
#   ENV_FILE=.env.staging \
#   DEST_CONNECTOR_CONTAINER=rsync-ai-postgresql-v1-0-0-mcp \
#   SINK_CONTAINER=rsync-ai-kafka-mcp-sink-v1-0-0-mcp \
#   GATEWAY=http://localhost:5001 \
#   scripts/test-staging-faithful.sh
#
# Exit codes: 0 = all gates passed (safe to trust) · non-zero = a gate failed.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENV_FILE="${ENV_FILE:-.env.staging}"
DEST_CONNECTOR_CONTAINER="${DEST_CONNECTOR_CONTAINER:-rsync-ai-postgresql-v1-0-0-mcp}"
SINK_CONTAINER="${SINK_CONTAINER:-rsync-ai-kafka-mcp-sink-v1-0-0-mcp}"

say()  { printf '\n\033[1m▶ %s\033[0m\n' "$*"; }
ok()   { printf '\033[32m✅ %s\033[0m\n' "$*"; }
die()  { printf '\033[31m🛑 %s\033[0m\n' "$*" >&2; exit 1; }

[[ -f "$ENV_FILE" ]] || die "ENV_FILE '$ENV_FILE' not found — run this on the staging machine, or set ENV_FILE=..."

# ── Gate 0: runtime wiring parity ───────────────────────────────────────────
# The CI e2e gate and staging share one Docker stack (project `rsync-ai`, same
# pinned container_names). A prior CI run can leave kafka-mcp-sink (and friends)
# wired to the LOCAL pipeline_db — which silently stalls every staging batch
# pipeline at executor 80%. Verify the RUNNING containers before trusting anything.
say "Gate 0/4 — runtime wiring parity (shared containers wired for Azure)"
if scripts/preflight-staging-runtime.sh; then
  ok "runtime wiring parity passed"
else
  die "runtime wiring drift — a shared container is on CI/base defaults. Heal it: scripts/preflight-staging-runtime.sh --fix"
fi

# ── Gate 1: config parity ───────────────────────────────────────────────────
say "Gate 1/4 — config parity (no sslmode drift) against $ENV_FILE"
if ENV_FILE="$ENV_FILE" scripts/preflight-prod-config.sh; then
  ok "config parity passed"
else
  die "config parity FAILED — a Postgres URL is not sslmode=require. Fix before deploying."
fi

# ── Gate 2: cold start ──────────────────────────────────────────────────────
# Restart the destination connector + sink so the next pipeline's sink worker
# boots while the connector is (re)starting — the exact cold-start ordering that
# bit prod. Best-effort: the window is narrow, which is precisely why the
# deterministic guard lives in the unit test, not here.
say "Gate 2/4 — cold start (bounce destination connector + sink)"
bounced=0
for c in "$DEST_CONNECTOR_CONTAINER" "$SINK_CONTAINER"; do
  if docker ps --format '{{.Names}}' | grep -qx "$c"; then
    docker restart "$c" >/dev/null && { printf '   restarted %s\n' "$c"; bounced=$((bounced+1)); }
  else
    printf '   ⚠️  %s not running — skipping (set DEST_CONNECTOR_CONTAINER / SINK_CONTAINER if named differently)\n' "$c"
  fi
done
[[ "$bounced" -gt 0 ]] || printf '   ⚠️  nothing bounced — running E2E warm (cold-path still covered by the unit test)\n'
ok "cold-bounce issued — triggering the pipeline immediately (no warm-up)"

# ── Gates 3 + 4: empty destination + destination-truth row verification ─────
# e2e-shopify-pg.sh drops the schema (gate 3) and verifies landed rows == source
# rows (gate 4), exiting non-zero on any mismatch or non-terminal status.
say "Gate 3+4/4 — empty destination + destination-truth E2E (scripts/e2e-shopify-pg.sh)"
if scripts/e2e-shopify-pg.sh; then
  ok "E2E passed: rows landed and counts match the source"
else
  die "E2E FAILED — rows did not land / counts mismatch / run not terminal. Do NOT deploy."
fi

say "ALL GATES PASSED"
ok "staging exercised prod's cold + empty + destination-truth conditions — safe to promote to prod."
echo "   Reminder: before the prod deploy, also run: ENV_FILE=.env.prod scripts/preflight-prod-config.sh"
