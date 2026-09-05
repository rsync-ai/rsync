#!/usr/bin/env bash
# scripts/e2e-shopify-pg.sh
#
# Single-command Shopify → Postgres pipeline test against the live docker
# stack. Runs the journey end-to-end via the api-gateway REST surface so
# every step is reproducible from a terminal and lands in CI as one
# pass/fail signal.
#
# What it does:
#   1. Drops the destination shopify schema in postgres-e2e (clean slate).
#   2. POSTs a chat message that triggers a Shopify → PostgreSQL pipeline.
#   3. Polls /pipelines/:id until the orchestrator parks at the
#      table-selection HITL.
#   4. POSTs /hitl/tables with all 6 Shopify tables — no UI clicks needed.
#   5. Polls /pipelines/:id/state until the pipeline reaches a terminal
#      status (completed / failed / silent_drop) or the timeout fires.
#   6. Verifies row counts in the destination database against the
#      expected counts from the source.
#   7. Exits 0 on success, non-zero on any failure.
#
# Usage:
#   scripts/e2e-shopify-pg.sh                 # against existing pipeline
#   GATEWAY=http://localhost:5001 scripts/e2e-shopify-pg.sh
#   EXPECTED_ROWS=21 scripts/e2e-shopify-pg.sh
#   TIMEOUT_SECS=600 scripts/e2e-shopify-pg.sh
#
# Designed to be the no-touch testing tool the team needs: one command,
# real Shopify data, real Postgres dest, zero browser clicks.

set -euo pipefail

GATEWAY="${GATEWAY:-http://localhost:5001}"
TIMEOUT_SECS="${TIMEOUT_SECS:-600}"
DEST_PG_CONTAINER="${DEST_PG_CONTAINER:-rsync-ai-postgres-e2e}"
DEST_PG_USER="${DEST_PG_USER:-e2e_user}"
DEST_PG_DB="${DEST_PG_DB:-e2e_db}"
DEST_SCHEMA="${DEST_SCHEMA:-shopify}"

EXPECTED_TABLES=(collections customers inventory_items orders products shop)

# ── helpers ────────────────────────────────────────────────────────────────
log()  { printf '[e2e %s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
fail() { log "❌ $*"; exit 1; }
ok()   { log "✅ $*"; }

require() {
  command -v "$1" >/dev/null 2>&1 || fail "missing dependency: $1"
}
require curl
require docker
require jq
require python3

# ── 1. clean slate ────────────────────────────────────────────────────────
log "step 1: drop destination schema ${DEST_SCHEMA} in ${DEST_PG_CONTAINER}"
docker exec "${DEST_PG_CONTAINER}" psql -U "${DEST_PG_USER}" -d "${DEST_PG_DB}" -c \
  "DROP SCHEMA IF EXISTS ${DEST_SCHEMA} CASCADE;" >/dev/null

# Also wipe any prior public.* tables left over from earlier runs that
# wrote without a schema prefix (legacy behaviour pre-F-26). After the
# F-26 fix the executor preserves the source schema so rows land in
# ${DEST_SCHEMA}.* — but this cleanup keeps reruns deterministic even
# across the fix transition.
for t in "${EXPECTED_TABLES[@]}"; do
  docker exec "${DEST_PG_CONTAINER}" psql -U "${DEST_PG_USER}" -d "${DEST_PG_DB}" -c \
    "DROP TABLE IF EXISTS public.${t} CASCADE;" >/dev/null 2>&1 || true
done
ok "destination cleaned (schema ${DEST_SCHEMA} + legacy public.*)"

# ── 2. kick a fresh chat pipeline ─────────────────────────────────────────
log "step 2: POST /api/v1/chat/message to start a Shopify → Postgres pipeline"
CHAT_RESP="$(curl -sS -X POST "${GATEWAY}/api/v1/chat/message" \
  -H 'Content-Type: application/json' \
  -d '{"message":"Sync Shopify to PostgreSQL","autosend":true}' || true)"
PIPELINE_ID="$(printf '%s' "${CHAT_RESP}" | jq -r '.pipeline_id // .metadata.pipeline_id // .data.pipeline_id // empty')"

# A-1: refuse to silently fall back to a pre-existing pipeline. The fallback
# masks F-24 (autosend ignored) by giving the harness a green-looking result
# against an unrelated pipeline — Cell A Reviewer caught this as a
# false-positive risk. After F-24 lands the chat endpoint returns a
# pipeline_id directly; if it doesn't, that's a real regression we want CI
# to surface, not paper over.
[ -n "${PIPELINE_ID}" ] || fail "chat/message did not return pipeline_id (response: ${CHAT_RESP:0:300}) — check F-24 (autosend handling)"
ok "pipeline ${PIPELINE_ID}"

# ── 3. wait for table-selection HITL ──────────────────────────────────────
log "step 3: poll for table-selection HITL (timeout ${TIMEOUT_SECS}s)"
DEADLINE=$(( $(date +%s) + TIMEOUT_SECS ))
HITL_REACHED=""
while [ "$(date +%s)" -lt "${DEADLINE}" ]; do
  STATE="$(curl -sS "${GATEWAY}/api/v1/pipelines/${PIPELINE_ID}/state" 2>/dev/null || true)"
  # Note: `state` (top-level) is a string like "running"/"waiting_for_user",
  # not a nested object. Other relevant fields:
  #   .status         -- "processing" / "completed" / "failed" / "waiting_for_user"
  #   .current_stage  -- e.g. "capability_resolver"
  #   .progress.stage -- duplicate of current_stage
  #   .blocking_reason.type -- "table_selection" etc. when paused
  STAGE="$(printf '%s' "${STATE}" | jq -r '.current_stage // .progress.stage // empty')"
  STATUS="$(printf '%s' "${STATE}" | jq -r '.status // .state // empty')"
  BLOCK_TYPE="$(printf '%s' "${STATE}" | jq -r '.blocking_reason.type // empty')"
  if [ "${BLOCK_TYPE}" = "table_selection" ] || [ "${STATUS}" = "waiting_for_user" ]; then
    HITL_REACHED=1
    break
  fi
  if [ "${STATUS}" = "failed" ]; then
    fail "pipeline ${PIPELINE_ID} failed before reaching HITL (state: $(printf '%s' "${STATE}" | jq -c .))"
  fi
  sleep 3
done
[ -n "${HITL_REACHED}" ] || fail "timed out waiting for table-selection HITL"
ok "table-selection HITL reached (stage=${STAGE:-?}, status=${STATUS:-?})"

# ── 4. resume with all 6 tables ───────────────────────────────────────────
log "step 4: POST /pipelines/${PIPELINE_ID}/hitl/tables with all 6 tables"
SELECTED_TABLES_JSON="$(printf '%s\n' "${EXPECTED_TABLES[@]}" \
  | jq -R 'select(length > 0)' | jq -s --arg s "${DEST_SCHEMA}" 'map($s + "." + .)')"
RESUME_RESP="$(curl -sS -X POST "${GATEWAY}/api/v1/pipelines/${PIPELINE_ID}/hitl/tables" \
  -H 'Content-Type: application/json' \
  -d "{\"selected_tables\": ${SELECTED_TABLES_JSON}}" || true)"
RESUME_OK="$(printf '%s' "${RESUME_RESP}" | jq -r '.success // .ok // empty')"
[ "${RESUME_OK}" = "true" ] || log "resume response (continuing): ${RESUME_RESP:0:300}"
ok "tables submitted"

# ── 5. poll for terminal status ───────────────────────────────────────────
log "step 5: poll for terminal status"
DEADLINE=$(( $(date +%s) + TIMEOUT_SECS ))
TERMINAL=""
while [ "$(date +%s)" -lt "${DEADLINE}" ]; do
  STATUS="$(curl -sS "${GATEWAY}/api/v1/pipelines/${PIPELINE_ID}" | jq -r '.status // empty')"
  case "${STATUS}" in
    completed|failed|cancelled|stopped)
      TERMINAL="${STATUS}"
      break
      ;;
  esac
  sleep 5
done
[ -n "${TERMINAL}" ] || fail "timed out waiting for pipeline to reach terminal status"
log "pipeline reached terminal status: ${TERMINAL}"

# ── 6. row-count verification ─────────────────────────────────────────────
log "step 6: verify rows landed in destination"
FAILED_TABLES=()
for t in "${EXPECTED_TABLES[@]}"; do
  COUNT="$(docker exec "${DEST_PG_CONTAINER}" psql -U "${DEST_PG_USER}" -d "${DEST_PG_DB}" -tAc \
    "SELECT count(*) FROM ${DEST_SCHEMA}.${t}" 2>/dev/null || echo "missing")"
  printf '[e2e]   %-20s rows=%s\n' "${t}" "${COUNT}" >&2
  if [ "${COUNT}" = "missing" ] || [ "${COUNT}" -le 0 ] 2>/dev/null; then
    FAILED_TABLES+=("${t}=${COUNT}")
  fi
done

if [ "${TERMINAL}" != "completed" ]; then
  fail "pipeline finished with status=${TERMINAL}; tables: ${FAILED_TABLES[*]:-(all populated)}"
fi
if [ "${#FAILED_TABLES[@]}" -gt 0 ]; then
  fail "tables with 0/missing rows: ${FAILED_TABLES[*]}"
fi

# Total rows sanity check — known Shopify dev store has 21 rows total.
TOTAL=0
for t in "${EXPECTED_TABLES[@]}"; do
  C="$(docker exec "${DEST_PG_CONTAINER}" psql -U "${DEST_PG_USER}" -d "${DEST_PG_DB}" -tAc \
    "SELECT count(*) FROM ${DEST_SCHEMA}.${t}" 2>/dev/null)"
  TOTAL=$(( TOTAL + C ))
done
ok "all 6 tables populated; total rows=${TOTAL} (expected ~21 against the seeded dev store)"
ok "Shopify → Postgres E2E PASSED (pipeline ${PIPELINE_ID})"
