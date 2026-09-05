#!/usr/bin/env bash
set -euo pipefail

# E2E: Batch pipeline run modes
# - reload: full refresh (truncate destination) + reset checkpoints
# - resume: incremental (keyset cursor) without duplicating / PK conflicts
#
# Requires:
# - main stack up (api-gateway/orchestrator/temporal/adapter/kafka/redis/postgres)
# - demo profile MCP connectors (mysql-mcp, postgresql-mcp, kafka-mcp-sink-mcp, minio-mcp)
# - e2e databases (mysql-e2e, postgres-e2e, minio)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

API_GATEWAY_URL="${API_GATEWAY_URL:-http://localhost:5001}"
API_BASE="${API_GATEWAY_URL%/}/api/v1"
USER_ID="${USER_ID:-00000000-0000-0000-0000-000000000001}"

log() { printf "%s\n" "$*"; }
die() { printf "❌ %s\n" "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }

need curl
need jq
need docker
need python3

BUILD_OPT=""
if [[ "${E2E_BUILD:-}" == "1" ]]; then
  BUILD_OPT="--build"
fi

cleanup_named_container() {
  local name="$1"
  if docker ps -a --format '{{.Names}}' | grep -qx "${name}"; then
    log "🧹 Removing conflicting container: ${name}"
    docker rm -f "${name}" >/dev/null 2>&1 || true
  fi
}

dc_main() {
  docker compose -p "${STACK_PREFIX:-rsync-ai}" -f "${ROOT_DIR}/docker-compose.yml" -f "${ROOT_DIR}/docker-compose.e2e.yml" "$@"
}

dc_e2e() {
  docker compose -p "${STACK_PREFIX:-rsync-ai}-e2e" -f "${ROOT_DIR}/docker-compose.e2e.dbs.yml" "$@"
}

dc_mcp() {
  docker compose -p "${STACK_PREFIX:-rsync-ai}-mcp" -f "${ROOT_DIR}/docker-compose.mcp.yml" "$@"
}

resolve_current_version() {
  local connector_id="$1"
  python3 - "${connector_id}" <<'PY'
import glob, json, sys
cid = sys.argv[1].strip()
root = "shared/mcp-connectors/public"
candidates = []
direct = f"{root}/{cid}/latest.json"
if glob.glob(direct):
    candidates.append(direct)
candidates.extend(glob.glob(f"{root}/**/{cid}/latest.json", recursive=True))
if not candidates:
    raise SystemExit(f"latest.json not found for connector: {cid}")
def score(p: str) -> tuple:
    p2 = p.replace("\\", "/")
    return (1 if "/database/" in p2 else 0, len(p2))
best = sorted(set(candidates), key=score)[0]
latest = json.load(open(best, "r"))
v = (latest.get("current_version") or "").strip()
if not v:
    p = (latest.get("path") or "").strip()
    if p.startswith("versions/"):
        v = p[len("versions/"):].strip()
if not v:
    v = (latest.get("version") or "").strip()
if not v:
    raise SystemExit(f"missing version in {best}")
if not v.startswith("v"):
    v = "v" + v
print(v)
PY
}

to_version_part() {
  local v="$1"
  v="${v#v}"
  echo "${v//./-}"
}

mcp_service_name() {
  local connector_id="$1"
  local v part
  v="$(resolve_current_version "${connector_id}")"
  part="$(to_version_part "${v}")"
  echo "${connector_id}-v${part}-mcp"
}

mcp_container_name() {
  local connector_id="$1"
  local v part
  v="$(resolve_current_version "${connector_id}")"
  part="$(to_version_part "${v}")"
  echo "${STACK_PREFIX:-rsync-ai}-${connector_id}-v${part}-mcp"
}

api_post() {
  local path="$1"
  local body="$2"
  curl -sS -X POST \
    -H "Content-Type: application/json" \
    -H "X-User-ID: ${USER_ID}" \
    "${API_BASE}${path}" \
    -d "${body}"
}

api_get() {
  local path="$1"
  curl -sS -H "X-User-ID: ${USER_ID}" "${API_BASE}${path}"
}

pg_e2e_exec() {
  local sql="$1"
  docker exec "${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}" psql -U e2e_user -d e2e_db -t -A -c "${sql}" | tr -d '\r'
}

mysql_e2e_exec() {
  local sql="$1"
  docker exec "${MYSQL_CONTAINER:-${STACK_PREFIX:-rsync-ai}-mysql-e2e}" mysql -uroot -prootpassword -N -B -e "${sql}" | tr -d '\r'
}

pg_main_exec() {
  local sql="$1"
  docker exec "${ORCH_PG_CONTAINER:-postgres}" psql -U user -d pipeline_db -t -A -c "${sql}" | tr -d '\r'
}

pg_e2e_count() {
  # Row count of a destination table, or empty string if it does not exist yet.
  docker exec "${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}" psql -U e2e_user -d e2e_db -t -A \
    -c "SELECT COUNT(*) FROM public.\"$1\";" 2>/dev/null | tr -d '[:space:]' || true
}

# Drive a pipeline to genuine completion (mirrors the golden test).
#
# The orchestrator runs a pipeline in two phases: a PLANNING phase that emits its
# own transient status="completed" when planning finishes, and a separate
# EXECUTION phase (executor stage) that raises the table-selection HITL. The old
# version of this helper returned on that planning-phase "completed" — BEFORE the
# executor stage ran and BEFORE answering the HITL — so the pipeline wedged at
# table_selection until the 20-min timeout (the quarantine cause). So we NEVER
# treat "completed" as terminal. Terminal = the destination table holds the
# expected row count; the loop stays alive to answer the table HITL and to fail
# fast on "failed".
wait_for_pipeline() {
  local pipeline_id="$1" src_table="$2" dst_table="$3" expected="$4" timeout_s="${5:-${E2E_PIPELINE_TIMEOUT_S:-480}}"
  local start now elapsed last_stage="" signaled="false"
  start="$(date +%s)"
  while true; do
    now="$(date +%s)"; elapsed=$((now - start))
    (( elapsed > timeout_s )) && die "Timed out waiting for pipeline ${pipeline_id} after ${timeout_s}s (last stage=${last_stage})"
    local state status stage br_type
    state="$(api_get "/pipelines/${pipeline_id}/state")" || true
    status="$(echo "${state}" | jq -r '.status // ""' 2>/dev/null || true)"
    stage="$(echo "${state}" | jq -r '.current_stage // .progress.stage // ""' 2>/dev/null || true)"
    br_type="$(echo "${state}" | jq -r '.blocking_reason.type // .blocking_reason.details.action_needed // ""' 2>/dev/null || true)"
    if [[ "${stage}" != "${last_stage}" && -n "${stage}" ]]; then
      log "⏳ pipeline=${pipeline_id} stage=${stage} status=${status} (${elapsed}s)"
      last_stage="${stage}"
    fi
    if [[ "${status}" == "failed" ]]; then
      echo "${state}" | jq . | head -c 4000 || true
      die "Pipeline ${pipeline_id} failed"
    fi
    if [[ "${status}" == "waiting_for_user" || "${status}" == "hitl_required" ]]; then
      if [[ "${br_type}" == *"table"* && "${signaled}" != "true" ]]; then
        log "📋 HITL: selecting source table [${src_table}]"
        api_post "/pipelines/${pipeline_id}/hitl/tables" "$(jq -cn --arg t "${src_table}" '{selected_tables:[$t],metadata:{source:"e2e_test_batch_reload_resume"}}')" >/dev/null
        signaled="true"
      fi
    fi
    local c; c="$(pg_e2e_count "${dst_table}")"
    if [[ -n "${c}" && "${c}" == "${expected}" ]]; then
      log "  destination reached ${c} rows (stage=${last_stage}, ${elapsed}s)"
      return 0
    fi
    sleep 5
  done
}

log "=================================================="
log "🧪 E2E: Batch pipeline reload → resume (incremental)"
log "=================================================="
log "API: ${API_BASE}"

if [[ "${E2E_SKIP_UP:-}" == "1" ]]; then
  log "⏭  E2E_SKIP_UP=1 — reusing the shared gate stack (no bring-up)"
else
log "🚀 Ensuring main stack is up (demo profile)…"
MYSQL_MCP_SERVICE="$(mcp_service_name mysql)"
POSTGRESQL_MCP_SERVICE="$(mcp_service_name postgresql)"
MYSQL_MCP_CONTAINER="$(mcp_container_name mysql)"
POSTGRESQL_MCP_CONTAINER="$(mcp_container_name postgresql)"

cleanup_named_container "${MYSQL_MCP_CONTAINER}"
cleanup_named_container "${POSTGRESQL_MCP_CONTAINER}"
cleanup_named_container "${STACK_PREFIX:-rsync-ai}-kafka-mcp-sink-v1-0-0-mcp"
cleanup_named_container "${STACK_PREFIX:-rsync-ai}-minio-v1-0-0-mcp"
cleanup_named_container "${STACK_PREFIX:-rsync-ai}-debezium-v1-0-0-mcp"
dc_main up -d ${BUILD_OPT} postgres redis kafka api-gateway orchestrator temporal temporal-adapter >/dev/null
# minio is owned by the main `rsync-ai` project (fixed container_name
# rsync-ai-minio). Starting it under the `rsync-ai-e2e` project collides on
# that name, so bring it up via dc_main and run only the bucket-init one-shot
# from the e2e file with --no-deps. Mirrors the passing golden test.
dc_main up -d ${BUILD_OPT} kafka-mcp-sink-mcp minio-mcp debezium-mcp minio >/dev/null
dc_mcp up -d ${BUILD_OPT} "${MYSQL_MCP_SERVICE}" "${POSTGRESQL_MCP_SERVICE}" >/dev/null

log "🚀 Ensuring e2e databases are up…"
dc_e2e up -d mysql-e2e postgres-e2e >/dev/null
dc_e2e up -d --no-deps minio-init >/dev/null
fi

log "⏳ Waiting for API Gateway…"
for i in {1..60}; do
  if curl -fsS "${API_GATEWAY_URL%/}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
curl -fsS "${API_GATEWAY_URL%/}/health" >/dev/null 2>&1 || die "API gateway not healthy"

log "🧹 Preparing destination table (public.big_table_copy)…"
pg_e2e_exec "CREATE TABLE IF NOT EXISTS big_table_copy (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, created_at TIMESTAMP);" >/dev/null || true
pg_e2e_exec "TRUNCATE TABLE big_table_copy;" >/dev/null || true

TS="$(date +%s)"
SRC_NAME="e2e-mysql-batch-${TS}"
DST_NAME="e2e-postgres-batch-${TS}"
PIPE_NAME="e2e-batch-reload-resume-${TS}"

log "🔌 Creating source connection…"
SRC_ID="$(
  api_post "/connections" "$(jq -cn --arg name "${SRC_NAME}" '{
    name: $name,
    connection_type: "source",
    connector_type: "mysql",
    connector_version: "v1.0.0",
    sync_mode: "batch",
    description: "e2e batch source",
    force_save: true,
    config: {
      host: "mysql-e2e",
      port: "3306",
      user: "e2e_user",
      password: "e2e_password",
      database: "e2e_db",
      table: "big_table"
    }
  }')"
)" || die "failed to create source connection"
SRC_ID="$(echo "${SRC_ID}" | jq -r '.id // empty')"
[[ -n "${SRC_ID}" ]] || die "source connection id missing"

log "🔌 Creating destination connection…"
DST_ID="$(
  api_post "/connections" "$(jq -cn --arg name "${DST_NAME}" --arg pghost "${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}" '{
    name: $name,
    connection_type: "destination",
    connector_type: "postgresql",
    connector_version: "v1.0.0",
    sync_mode: "batch",
    description: "e2e batch destination",
    force_save: true,
    config: {
      host: $pghost,
      port: "5432",
      user: "e2e_user",
      password: "e2e_password",
      database: "e2e_db",
      table: "big_table_copy"
    }
  }')"
)" || die "failed to create destination connection"
DST_ID="$(echo "${DST_ID}" | jq -r '.id // empty')"
[[ -n "${DST_ID}" ]] || die "destination connection id missing"

REQ="Batch copy table big_table from MySQL (e2e_db) to PostgreSQL table big_table_copy. Use batch mode (not CDC)."
log "🧠 Creating pipeline…"
PIPE_ID="$(
  api_post "/pipelines?allow_draft=true" "$(jq -cn --arg name "${PIPE_NAME}" --arg req "${REQ}" --arg src "${SRC_ID}" --arg dst "${DST_ID}" '{
    name: $name,
    request: $req,
    source_connection_id: $src,
    destination_connection_id: $dst,
    default_run_mode: "resume"
  }')"
)" || die "failed to create pipeline"
PIPE_ID="$(echo "${PIPE_ID}" | jq -r '.id // empty')"
[[ -n "${PIPE_ID}" ]] || die "pipeline id missing"
log "✅ Pipeline created: ${PIPE_ID}"

SRC_COUNT="$(mysql_e2e_exec "SELECT COUNT(*) FROM e2e_db.big_table;" | tr -d '[:space:]')"
SRC_MAX_ID="$(mysql_e2e_exec "SELECT COALESCE(MAX(id),0) FROM e2e_db.big_table;" | tr -d '[:space:]')"
log "📊 Source rows before reload: ${SRC_COUNT} (max id=${SRC_MAX_ID})"

log "▶️  Run #1 (reload) — full refresh (truncate) + backfill…"
# ack_warnings acknowledges the pre-migration assessment gate so the run proceeds
# (without it the pipeline wedges at the assessment HITL). Mirrors the golden test.
RUN1="$(api_post "/pipelines/${PIPE_ID}/run?run_mode=reload&allow_draft=true" '{"ack_warnings": true}')" || die "failed to run pipeline (reload)"
EXEC1="$(echo "${RUN1}" | jq -r '.execution_id // empty')"
[[ -n "${EXEC1}" ]] || die "execution_id missing (run1)"
wait_for_pipeline "${PIPE_ID}" "big_table" "big_table_copy" "${SRC_COUNT}" "${E2E_PIPELINE_TIMEOUT_S:-480}"

DST_COUNT="$(pg_e2e_exec "SELECT COUNT(*) FROM big_table_copy;" | tr -d '[:space:]')"
log "📊 After reload: source=${SRC_COUNT} dest=${DST_COUNT}"
[[ "${DST_COUNT}" == "${SRC_COUNT}" ]] || die "row count mismatch after reload"

# NB: the orchestrator stores source_table SCHEMA-QUALIFIED (db.table) in
# pipeline_checkpoints — "e2e_db.big_table", not "big_table" (same convention the
# golden test's "e2e_db.golden_types_*" rows use). Querying the bare name returns
# no row and reads as a false "checkpoint missing" — the original quarantine-era bug.
CK1_POS="$(pg_main_exec "SELECT COALESCE(position->>'cursor', position->>'offset', '') FROM pipeline_checkpoints WHERE pipeline_id='${PIPE_ID}' AND source_table='e2e_db.big_table' ORDER BY updated_at DESC NULLS LAST LIMIT 1;")"
log "📍 Checkpoint position after reload: ${CK1_POS} (source max id=${SRC_MAX_ID})"
[[ -n "${CK1_POS}" ]] || die "checkpoint position missing after reload"

log "✍️  Inserting 3 new rows into MySQL source…"
mysql_e2e_exec "INSERT INTO e2e_db.big_table(payload) VALUES ('e2e-new-row-1'),('e2e-new-row-2'),('e2e-new-row-3');" >/dev/null
NEW_SRC_COUNT="$(mysql_e2e_exec "SELECT COUNT(*) FROM e2e_db.big_table;" | tr -d '[:space:]')"
NEW_SRC_MAX_ID="$(mysql_e2e_exec "SELECT COALESCE(MAX(id),0) FROM e2e_db.big_table;" | tr -d '[:space:]')"
log "📊 Source now: ${NEW_SRC_COUNT}"

log "▶️  Run #2 (resume) — incremental (should NOT re-copy old rows)…"
RUN2="$(api_post "/pipelines/${PIPE_ID}/run?run_mode=resume&allow_draft=true" '{"ack_warnings": true}')" || die "failed to run pipeline (resume)"
EXEC2="$(echo "${RUN2}" | jq -r '.execution_id // empty')"
[[ -n "${EXEC2}" ]] || die "execution_id missing (run2)"
wait_for_pipeline "${PIPE_ID}" "big_table" "big_table_copy" "${NEW_SRC_COUNT}" "${E2E_PIPELINE_TIMEOUT_S:-480}"
NEW_DST_COUNT="$(pg_e2e_exec "SELECT COUNT(*) FROM big_table_copy;" | tr -d '[:space:]')"
log "📊 After resume: source=${NEW_SRC_COUNT} dest=${NEW_DST_COUNT}"
[[ "${NEW_DST_COUNT}" == "${NEW_SRC_COUNT}" ]] || die "row count mismatch after resume (expected incremental upsert/append without duplicates)"

CK2_POS="$(pg_main_exec "SELECT COALESCE(position->>'cursor', position->>'offset', '') FROM pipeline_checkpoints WHERE pipeline_id='${PIPE_ID}' AND source_table='e2e_db.big_table' ORDER BY updated_at DESC NULLS LAST LIMIT 1;")"
log "📍 Checkpoint position after resume: ${CK2_POS} (source max id=${NEW_SRC_MAX_ID})"
[[ -n "${CK2_POS}" ]] || die "checkpoint position missing after resume"

log "✅ PASS: Batch reload→resume behaves as full refresh + incremental"

