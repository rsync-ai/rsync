#!/usr/bin/env bash
set -euo pipefail

# E2E: PostgreSQL non-public source schema -> NAMESPACED destination (batch, MULTI-TABLE).
#
# Regression net for the SINK-side silent-drop fix in PR #236
# (resolveDestTableForWrite): when a Postgres SOURCE keeps its tables in a
# NON-public schema (e.g. "rsync_public.customers") AND the pipeline has a REAL
# destination namespace (e.g. "tenant7"), the rows MUST land in
# "<namespace>.<table>" (tenant7.customers) at the destination -- NOT in a
# leaked "rsync_public" schema, and NOT in "public".
#
# Why this exercises #236 specifically: the non-CDC batch write path
# (writeToDestination in the kafka-mcp-sink worker) was the only one of four
# relational write paths that forwarded the namespace param but never stripped
# the source-schema qualifier from the target table. So a qualified table
# ("rsync_public.cust") + a real namespace made the connector ignore the
# namespace (qualified-table contract) and write into the leaked source schema.
# resolveDestTableForWrite now bare-ifies the table when a real namespace is set,
# matching the three CDC write paths.
#
# Why MULTI-TABLE: startKafkaMCPSink (executor.go) DELETES destCfg["table"] for
# multi-table runs (the sink routes per message.table), so the per-table
# destTableName the executor computes -- source-qualified -- is what actually
# reaches writeToDestination. A SINGLE-table batch run pins destCfg["table"] to
# the bare selected table, masking the bug (false green). So we select TWO
# source tables to exercise the real path.
#
# Pre-fix symptom: tenant7.<t> stays empty; the connector writes the rows into a
#                  leaked rsync_public.<t> at the destination instead.
# Post-fix:        tenant7.<t> holds all source rows; public.<t> and a destination
#                  rsync_public.<t> are NOT created.
#
# Source and destination are the SAME e2e Postgres DB (e2e_db); separation is by
# SCHEMA (source=rsync_public, dest=tenant7). Requires the e2e stack up.
# Set E2E_SKIP_UP=1 to skip bring-up when the stack is already running.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

API_GATEWAY_URL="${API_GATEWAY_URL:-http://localhost:5001}"
API_BASE="${API_GATEWAY_URL%/}/api/v1"
USER_ID="${USER_ID:-00000000-0000-0000-0000-000000000001}"
DEST_NS="${DEST_NS:-tenant7}"   # the REAL destination namespace under test

# Isolated-stack awareness: run_gate.sh exports STACK_PREFIX (+ PG_CONTAINER) for
# the CI stack; unset -> default rsync-ai behavior. PG_CONTAINER is also the docker
# network hostname the connectors dial (container name == DNS name on the network).
PG_CONTAINER="${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}"

log() { printf '%s\n' "$*"; }
die() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }

need curl
need jq
need docker

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
print(v if v.startswith("v") else "v" + v)
PY
}

api_post() {
  curl -sS -X POST -H "Content-Type: application/json" -H "X-User-ID: ${USER_ID}" "${API_BASE}$1" -d "$2"
}
api_get() { curl -sS -H "X-User-ID: ${USER_ID}" "${API_BASE}$1"; }

pg_count_in() {
  # pg_count_in <schema> <table> -> row count or "" if table absent.
  docker exec "${PG_CONTAINER}" psql -U e2e_user -d e2e_db -t -A \
    -c "SELECT COUNT(*) FROM \"$1\".\"$2\";" 2>/dev/null | tr -d '[:space:]' || true
}

# Drive a multi-table batch pipeline to completion.
# Terminal = every expected <DEST_NS>.<table> holds its expected count.
# Never treat the planning-phase "completed" as terminal (see golden test).
wait_for_pipeline_multi() {
  local pipeline_id="$1"; shift
  local timeout_s="$1"; shift
  # remaining args: alternating "<sel_table> <bare_table> <expected>" triples
  local -a SEL=() BARE=() EXP=()
  while (( "$#" >= 3 )); do
    SEL+=("$1"); BARE+=("$2"); EXP+=("$3"); shift 3
  done
  local start now elapsed last_stage="" signaled="false"
  start="$(date +%s)"
  local sel_json; sel_json="$(printf '%s\n' "${SEL[@]}" | jq -R . | jq -cs .)"
  while true; do
    now="$(date +%s)"; elapsed=$((now - start))
    (( elapsed > timeout_s )) && die "Timed out waiting for pipeline ${pipeline_id} after ${timeout_s}s (last stage=${last_stage})"
    local state status stage br_type
    state="$(api_get "/pipelines/${pipeline_id}/state")" || true
    status="$(echo "${state}" | jq -r '.status // ""' 2>/dev/null || true)"
    stage="$(echo "${state}" | jq -r '.current_stage // .progress.stage // ""' 2>/dev/null || true)"
    br_type="$(echo "${state}" | jq -r '.blocking_reason.type // .blocking_reason.details.action_needed // ""' 2>/dev/null || true)"
    if [[ "${stage}" != "${last_stage}" && -n "${stage}" ]]; then
      log "  ... pipeline=${pipeline_id} stage=${stage} status=${status} (${elapsed}s)"
      last_stage="${stage}"
    fi
    if [[ "${status}" == "failed" || "${status}" == "silent_drop_detected" ]]; then
      echo "${state}" | jq . | head -c 4000 || true
      die "Pipeline ${pipeline_id} reported status=${status}"
    fi
    if [[ "${status}" == "waiting_for_user" || "${status}" == "hitl_required" ]]; then
      if [[ "${br_type}" == *"table"* && "${signaled}" != "true" ]]; then
        log "  HITL: selecting source tables ${sel_json} (dest namespace=${DEST_NS})"
        api_post "/pipelines/${pipeline_id}/hitl/tables" \
          "$(jq -cn --argjson t "${sel_json}" --arg ns "${DEST_NS}" \
            '{selected_tables:$t,destination_config:{namespace:$ns,namespace_kind:"schema",create_if_not_exists:true},metadata:{source:"e2e_pg_nonpublic_schema_namespaced"}}')" >/dev/null
        signaled="true"
      fi
    fi
    local all_ok="true" i
    for i in "${!BARE[@]}"; do
      local c; c="$(pg_count_in "${DEST_NS}" "${BARE[$i]}")"
      if [[ -z "${c}" || "${c}" != "${EXP[$i]}" ]]; then all_ok="false"; fi
    done
    if [[ "${all_ok}" == "true" ]]; then
      log "  all destination ${DEST_NS}.<table> reached expected counts (stage=${last_stage}, ${elapsed}s)"
      return 0
    fi
    sleep 5
  done
}

log "=================================================="
log "E2E: PG non-public source schema -> ${DEST_NS} namespace (batch, multi-table) [PR #236]"
log "=================================================="

BUILD_OPT=""; [[ "${E2E_BUILD:-}" == "1" ]] && BUILD_OPT="--build"
if [[ "${E2E_SKIP_UP:-}" != "1" ]]; then
  log "Ensuring stack is up..."
  dc_main up -d ${BUILD_OPT} postgres redis kafka api-gateway orchestrator temporal temporal-adapter >/dev/null
  dc_main up -d ${BUILD_OPT} kafka-mcp-sink-mcp minio-mcp minio >/dev/null
  dc_mcp up -d ${BUILD_OPT} "postgresql-v$(resolve_current_version postgresql | sed 's/^v//;s/\./-/g')-mcp" >/dev/null
  dc_e2e up -d postgres-e2e >/dev/null
fi

log "Waiting for API Gateway..."
for _ in {1..60}; do curl -fsS "${API_GATEWAY_URL%/}/health" >/dev/null 2>&1 && break; sleep 2; done
curl -fsS "${API_GATEWAY_URL%/}/health" >/dev/null 2>&1 || die "API gateway not healthy"

log "Waiting for e2e Postgres..."
for _ in {1..60}; do docker exec "${PG_CONTAINER}" pg_isready -U e2e_user -d e2e_db >/dev/null 2>&1 && break; sleep 2; done
docker exec "${PG_CONTAINER}" pg_isready -U e2e_user -d e2e_db >/dev/null 2>&1 || die "e2e postgres not ready"

TS="$(date +%s)"
T1="cust_${TS}"
T2="ord_${TS}"
SRC_SCHEMA="rsync_public"

log "Seeding NON-public source schema ${SRC_SCHEMA} with two tables; pre-creating empty dest namespace ${DEST_NS}..."
docker exec -i "${PG_CONTAINER}" psql -U e2e_user -d e2e_db <<SQL
CREATE SCHEMA IF NOT EXISTS ${SRC_SCHEMA};
CREATE SCHEMA IF NOT EXISTS ${DEST_NS};
DROP TABLE IF EXISTS ${SRC_SCHEMA}.${T1} CASCADE;
DROP TABLE IF EXISTS ${SRC_SCHEMA}.${T2} CASCADE;
CREATE TABLE ${SRC_SCHEMA}.${T1} (id INT PRIMARY KEY, name VARCHAR(64));
CREATE TABLE ${SRC_SCHEMA}.${T2} (id INT PRIMARY KEY, amount NUMERIC(10,2));
INSERT INTO ${SRC_SCHEMA}.${T1} (id,name) VALUES (1,'alice'),(2,'bob'),(3,'carol');
INSERT INTO ${SRC_SCHEMA}.${T2} (id,amount) VALUES (1,10.50),(2,20.25);
-- CRITICAL: drop any leftover destination tables in BOTH the target namespace and
-- public so we prove fresh, namespace-correct landing (and no public leak).
DROP TABLE IF EXISTS ${DEST_NS}.${T1} CASCADE;
DROP TABLE IF EXISTS ${DEST_NS}.${T2} CASCADE;
DROP TABLE IF EXISTS public.${T1} CASCADE;
DROP TABLE IF EXISTS public.${T2} CASCADE;
SQL

C1="$(pg_count_in ${SRC_SCHEMA} ${T1})"
C2="$(pg_count_in ${SRC_SCHEMA} ${T2})"
log "Source rows: ${SRC_SCHEMA}.${T1}=${C1}  ${SRC_SCHEMA}.${T2}=${C2}"
[[ "${C1}" == "3" && "${C2}" == "2" ]] || die "seed failed (got ${C1}/${C2})"

log "Creating connections..."
DST_VER="$(resolve_current_version postgresql)"
log "Using postgresql connector version: ${DST_VER}"

# Source: postgresql pointing at e2e_db (NO table -> discovery lists schema-qualified tables for HITL).
SRC_ID="$(api_post "/connections" "$(jq -cn --arg name "pgns-src-${TS}" --arg v "${DST_VER}" --arg host "${PG_CONTAINER}" '{
  name:$name, connection_type:"source", connector_type:"postgresql", connector_version:$v,
  sync_mode:"batch", description:"PG non-public-schema source", force_save:true,
  config:{host:$host,port:"5432",user:"e2e_user",password:"e2e_password",database:"e2e_db"}
}')")"
SRC_ID="$(echo "${SRC_ID}" | jq -r '.id // empty')"
[[ -n "${SRC_ID}" ]] || die "source connection id missing"

# Destination: postgresql, same DB, NO table (multi-table -> executor deletes destCfg[table];
# sink routes per message.table = executor destTableName, which is source-qualified).
DST_ID="$(api_post "/connections" "$(jq -cn --arg name "pgns-dst-${TS}" --arg v "${DST_VER}" --arg host "${PG_CONTAINER}" '{
  name:$name, connection_type:"destination", connector_type:"postgresql", connector_version:$v,
  sync_mode:"batch", description:"PG destination (namespaced)", force_save:true,
  config:{host:$host,port:"5432",user:"e2e_user",password:"e2e_password",database:"e2e_db"}
}')")"
DST_ID="$(echo "${DST_ID}" | jq -r '.id // empty')"
[[ -n "${DST_ID}" ]] || die "destination connection id missing"

log "Creating pipeline (multi-table batch) WITH destination_namespace=${DEST_NS}..."
REQ="Batch copy tables ${SRC_SCHEMA}.${T1} and ${SRC_SCHEMA}.${T2} from the PostgreSQL source to the PostgreSQL destination, writing into the destination schema ${DEST_NS}. Use batch mode (not CDC)."
PIPE_ID="$(api_post "/pipelines?allow_draft=true" "$(jq -cn --arg name "pgns-${TS}" --arg req "${REQ}" --arg src "${SRC_ID}" --arg dst "${DST_ID}" --arg ns "${DEST_NS}" '{
  name:$name, request:$req, source_connection_id:$src, destination_connection_id:$dst, destination_namespace:$ns, default_run_mode:"reload"
}')")"
PIPE_ID="$(echo "${PIPE_ID}" | jq -r '.id // empty')"
[[ -n "${PIPE_ID}" ]] || die "pipeline id missing"
log "Pipeline created: ${PIPE_ID}"

log "Running reload..."
RUN="$(api_post "/pipelines/${PIPE_ID}/run?run_mode=reload&allow_draft=true" '{"ack_warnings": true}')" || die "failed to run pipeline"
[[ -n "$(echo "${RUN}" | jq -r '.execution_id // empty')" ]] || die "execution_id missing"

wait_for_pipeline_multi "${PIPE_ID}" "${E2E_PIPELINE_TIMEOUT_S:-480}" \
  "${SRC_SCHEMA}.${T1}" "${T1}" "${C1}" \
  "${SRC_SCHEMA}.${T2}" "${T2}" "${C2}"

# --- Assertions ---------------------------------------------------------------
log "Verifying rows landed in DESTINATION ${DEST_NS} schema (the fix)..."
D1="$(pg_count_in ${DEST_NS} ${T1})"; D2="$(pg_count_in ${DEST_NS} ${T2})"
[[ "${D1}" == "${C1}" ]] || die "${DEST_NS}.${T1} count mismatch: src=${C1} dest=${D1}"
[[ "${D2}" == "${C2}" ]] || die "${DEST_NS}.${T2} count mismatch: src=${C2} dest=${D2}"
log "  OK   ${DEST_NS}.${T1}=${D1}  ${DEST_NS}.${T2}=${D2}"

log "Verifying NO public leak at destination (rows did NOT fall back to public)..."
P1="$(pg_count_in public ${T1})"; P2="$(pg_count_in public ${T2})"
[[ -z "${P1}" || "${P1}" == "0" ]] || die "LEAK: public.${T1} unexpectedly has ${P1} rows (namespace ignored)"
[[ -z "${P2}" || "${P2}" == "0" ]] || die "LEAK: public.${T2} unexpectedly has ${P2} rows (namespace ignored)"
log "  OK   public.${T1}=${P1:-<absent>}  public.${T2}=${P2:-<absent>} (no fallback)"

log "Verifying NO source-schema leak at destination (source rows unchanged, not re-written into ${SRC_SCHEMA})..."
S1="$(pg_count_in ${SRC_SCHEMA} ${T1})"; S2="$(pg_count_in ${SRC_SCHEMA} ${T2})"
[[ "${S1}" == "${C1}" && "${S2}" == "${C2}" ]] || die "source ${SRC_SCHEMA} rows changed (${S1}/${S2}) — possible leak/self-write"
log "  OK   ${SRC_SCHEMA}.${T1}=${S1}  ${SRC_SCHEMA}.${T2}=${S2} (unchanged)"

log "PASS: PG non-public source schema landed in destination ${DEST_NS} namespace (batch, multi-table) — #236 sink bare-ify verified"
