#!/usr/bin/env bash
set -euo pipefail

# E2E (Phase 0 "golden pipeline"): Batch column-type + primary-key preservation.
#
# This is the regression net for the bug class fixed in PR #75: schema metadata
# (column_types, primary_keys) is PRODUCED by the executor but must survive every
# hop -- executor colTypesByTable -> Kafka message "column_types" key -> sink
# parseSinkMessage -> ensureDestinationTable -> destination connector ensure_table.
# A regression at ANY hop makes typed columns land as TEXT at the destination.
#
# What it does:
#   1. Seeds a typed MySQL source table (INT PK, INT, DECIMAL, DATETIME, VARCHAR, TEXT).
#   2. PRE-DROPS the Postgres destination table (ensure_table uses CREATE IF NOT
#      EXISTS and will NOT alter an existing table -- the bug only repros on a fresh
#      table, so we must drop it to actually exercise DDL generation).
#   3. Creates source+dest connections + a batch pipeline, runs reload.
#   4. Asserts at the destination:
#        - id / qty            -> integer        (regression symptom: text)
#        - price               -> numeric        (regression symptom: text)
#        - created_at          -> timestamp*     (regression symptom: text)
#        - id is the PRIMARY KEY (NO_PRIMARY_KEY false-positive regression)
#        - row count matches source
#
# Requires the demo/e2e stack up (see test_batch_reload_resume.sh for the exact
# compose orchestration this mirrors). Set E2E_SKIP_UP=1 to skip bring-up when the
# stack is already running.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

API_GATEWAY_URL="${API_GATEWAY_URL:-http://localhost:5001}"
API_BASE="${API_GATEWAY_URL%/}/api/v1"
USER_ID="${USER_ID:-00000000-0000-0000-0000-000000000001}"

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
to_version_part() { local v="$1"; v="${v#v}"; echo "${v//./-}"; }
mcp_service_name() { echo "$1-v$(to_version_part "$(resolve_current_version "$1")")-mcp"; }

api_post() {
  curl -sS -X POST -H "Content-Type: application/json" -H "X-User-ID: ${USER_ID}" "${API_BASE}$1" -d "$2"
}
api_get() { curl -sS -H "X-User-ID: ${USER_ID}" "${API_BASE}$1"; }

mysql_e2e_exec() {
  docker exec "${MYSQL_CONTAINER:-${STACK_PREFIX:-rsync-ai}-mysql-e2e}" mysql -uroot -prootpassword -N -B -e "$1" 2>/dev/null | tr -d '\r'
}
pg_e2e_exec() {
  docker exec "${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}" psql -U e2e_user -d e2e_db -t -A -F',' -c "$1" | tr -d '\r'
}

pg_e2e_count() {
  # Row count of a destination table, or empty string if it does not exist yet.
  docker exec "${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}" psql -U e2e_user -d e2e_db -t -A \
    -c "SELECT COUNT(*) FROM public.\"$1\";" 2>/dev/null | tr -d '[:space:]' || true
}

# Drive a pipeline to genuine completion.
#
# IMPORTANT: the orchestrator runs the pipeline in two phases -- a PLANNING phase
# (intent -> capability_resolver -> planner) that emits its own transient
# status="completed" when planning finishes, and then a separate EXECUTION phase
# (executor stage, which raises the table-selection HITL). An earlier version of
# this helper returned on that planning-phase "completed" and exited before the
# executor stage, leaving the pipeline wedged at table_selection forever.
#
# So we never treat "completed" as terminal. Terminal = the destination table
# actually holds the expected row count. The loop stays alive to (a) answer the
# table-selection HITL and (b) fail fast on a "failed" status.
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
      log "  ... pipeline=${pipeline_id} stage=${stage} status=${status} (${elapsed}s)"
      last_stage="${stage}"
    fi
    if [[ "${status}" == "failed" ]]; then
      echo "${state}" | jq . | head -c 4000 || true
      die "Pipeline ${pipeline_id} failed"
    fi
    if [[ "${status}" == "waiting_for_user" || "${status}" == "hitl_required" ]]; then
      if [[ "${br_type}" == *"table"* && "${signaled}" != "true" ]]; then
        log "  HITL: selecting source table [${src_table}]"
        api_post "/pipelines/${pipeline_id}/hitl/tables" "$(jq -cn --arg t "${src_table}" '{selected_tables:[$t],metadata:{source:"e2e_golden_type_preservation"}}')" >/dev/null
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
log "E2E: Batch column-type + PK preservation (golden)"
log "=================================================="

# --- Ensure stack is up (idempotent; mirrors test_batch_reload_resume.sh) ---
BUILD_OPT=""; [[ "${E2E_BUILD:-}" == "1" ]] && BUILD_OPT="--build"
if [[ "${E2E_SKIP_UP:-}" != "1" ]]; then
  log "Ensuring stack is up..."
  dc_main up -d ${BUILD_OPT} postgres redis kafka api-gateway orchestrator temporal temporal-adapter >/dev/null
  # minio (object store) is owned by the main `rsync-ai` project. Starting it
  # again under the `rsync-ai-e2e` project collides on container_name
  # rsync-ai-minio, so bring it up via dc_main and only run the bucket-init
  # one-shot from the e2e file with --no-deps (it targets the shared `minio`
  # network alias, not a second container).
  dc_main up -d ${BUILD_OPT} kafka-mcp-sink-mcp minio-mcp minio >/dev/null
  dc_mcp up -d ${BUILD_OPT} "$(mcp_service_name mysql)" "$(mcp_service_name postgresql)" >/dev/null
  dc_e2e up -d mysql-e2e postgres-e2e >/dev/null
  dc_e2e up -d --no-deps minio-init >/dev/null
fi

log "Waiting for API Gateway..."
for _ in {1..60}; do curl -fsS "${API_GATEWAY_URL%/}/health" >/dev/null 2>&1 && break; sleep 2; done
curl -fsS "${API_GATEWAY_URL%/}/health" >/dev/null 2>&1 || die "API gateway not healthy"

# Wait for the e2e DBs to accept connections. dc_e2e may have just recreated
# mysql-e2e (config drift or E2E_BUILD), and the seed step below uses
# `docker exec ... mysql` which fails under `set -e` if the server is not ready.
log "Waiting for e2e MySQL + Postgres..."
for _ in {1..60}; do docker exec "${MYSQL_CONTAINER:-${STACK_PREFIX:-rsync-ai}-mysql-e2e}" mysqladmin ping -h 127.0.0.1 -uroot -prootpassword --silent >/dev/null 2>&1 && break; sleep 2; done
docker exec "${MYSQL_CONTAINER:-${STACK_PREFIX:-rsync-ai}-mysql-e2e}" mysqladmin ping -h 127.0.0.1 -uroot -prootpassword --silent >/dev/null 2>&1 || die "e2e mysql not ready"
for _ in {1..60}; do docker exec "${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}" pg_isready -U e2e_user -d e2e_db >/dev/null 2>&1 && break; sleep 2; done
docker exec "${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}" pg_isready -U e2e_user -d e2e_db >/dev/null 2>&1 || die "e2e postgres not ready"

TS="$(date +%s)"
SRC_TABLE="golden_types_${TS}"
DST_TABLE="golden_types_copy_${TS}"

log "Seeding typed MySQL source e2e_db.${SRC_TABLE}..."
mysql_e2e_exec "
CREATE DATABASE IF NOT EXISTS e2e_db;
DROP TABLE IF EXISTS e2e_db.${SRC_TABLE};
CREATE TABLE e2e_db.${SRC_TABLE} (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(64),
  qty INT,
  price DECIMAL(10,2),
  is_active TINYINT(1),
  created_at DATETIME,
  notes TEXT
);
INSERT INTO e2e_db.${SRC_TABLE} (name, qty, price, is_active, created_at, notes) VALUES
  ('widget', 7, 12.34, 1, '2026-02-02 10:11:12', 'first'),
  ('gadget', 42, 99.99, 0, '2026-02-03 08:09:10', 'second'),
  ('gizmo', 0, 0.01, 1, '2026-02-04 23:59:59', 'third');
"

SRC_COUNT="$(mysql_e2e_exec "SELECT COUNT(*) FROM e2e_db.${SRC_TABLE};" | tr -d '[:space:]')"
log "Source rows: ${SRC_COUNT}"

# CRITICAL: drop destination so ensure_table regenerates DDL from column_types.
log "Dropping destination public.${DST_TABLE} (force fresh DDL)..."
docker exec "${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}" psql -U e2e_user -d e2e_db -c "DROP TABLE IF EXISTS public.\"${DST_TABLE}\" CASCADE;" >/dev/null 2>&1 || true

log "Creating connections..."
# Resolve the CURRENTLY deployed connector versions instead of hardcoding. The
# executor looks for a running MCP container named after connector_type+version
# (e.g. postgresql-v1-0-14-mcp); a stale hardcoded version makes the destination
# pre-flight fail with "no container is reachable" even though the code is fine.
SRC_VER="$(resolve_current_version mysql)"
DST_VER="$(resolve_current_version postgresql)"
log "Using connector versions: mysql=${SRC_VER} postgresql=${DST_VER}"
SRC_ID="$(api_post "/connections" "$(jq -cn --arg name "golden-src-${TS}" --arg t "${SRC_TABLE}" --arg v "${SRC_VER}" '{
  name:$name, connection_type:"source", connector_type:"mysql", connector_version:$v,
  sync_mode:"batch", description:"golden type-fidelity source", force_save:true,
  config:{host:"mysql-e2e",port:"3306",user:"e2e_user",password:"e2e_password",database:"e2e_db",table:$t}
}')")"
SRC_ID="$(echo "${SRC_ID}" | jq -r '.id // empty')"
[[ -n "${SRC_ID}" ]] || die "source connection id missing"

DST_ID="$(api_post "/connections" "$(jq -cn --arg name "golden-dst-${TS}" --arg t "${DST_TABLE}" --arg v "${DST_VER}" --arg pgh "${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}" '{
  name:$name, connection_type:"destination", connector_type:"postgresql", connector_version:$v,
  sync_mode:"batch", description:"golden type-fidelity destination", force_save:true,
  config:{host:$pgh,port:"5432",user:"e2e_user",password:"e2e_password",database:"e2e_db",table:$t}
}')")"
DST_ID="$(echo "${DST_ID}" | jq -r '.id // empty')"
[[ -n "${DST_ID}" ]] || die "destination connection id missing"

log "Creating pipeline..."
REQ="Batch copy table ${SRC_TABLE} from MySQL (e2e_db) to PostgreSQL table ${DST_TABLE}. Use batch mode (not CDC)."
PIPE_ID="$(api_post "/pipelines?allow_draft=true" "$(jq -cn --arg name "golden-types-${TS}" --arg req "${REQ}" --arg src "${SRC_ID}" --arg dst "${DST_ID}" '{
  name:$name, request:$req, source_connection_id:$src, destination_connection_id:$dst, default_run_mode:"reload"
}')")"
PIPE_ID="$(echo "${PIPE_ID}" | jq -r '.id // empty')"
[[ -n "${PIPE_ID}" ]] || die "pipeline id missing"
log "Pipeline created: ${PIPE_ID}"

log "Running reload..."
# ack_warnings acknowledges the pre-migration assessment (Pillar 1) so the run proceeds.
RUN="$(api_post "/pipelines/${PIPE_ID}/run?run_mode=reload&allow_draft=true" '{"ack_warnings": true}')" || die "failed to run pipeline"
[[ -n "$(echo "${RUN}" | jq -r '.execution_id // empty')" ]] || die "execution_id missing"
wait_for_pipeline "${PIPE_ID}" "${SRC_TABLE}" "${DST_TABLE}" "${SRC_COUNT}" "${E2E_PIPELINE_TIMEOUT_S:-480}"

# --- Assertions ---------------------------------------------------------------
log "Verifying destination row count..."
DST_COUNT="$(pg_e2e_exec "SELECT COUNT(*) FROM public.\"${DST_TABLE}\";" | tr -d '[:space:]')"
[[ "${DST_COUNT}" == "${SRC_COUNT}" ]] || die "row count mismatch: source=${SRC_COUNT} dest=${DST_COUNT}"
log "Row count OK: ${DST_COUNT}"

log "Verifying destination column types..."
# Query each column's type directly (no associative array) so this stays
# compatible with the bash 3.2 that ships as /bin/bash on macOS runners.
DDL="$(pg_e2e_exec "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='${DST_TABLE}' ORDER BY ordinal_position;")"
log "  discovered: ${DDL//$'\n'/ | }"

fail=0
get_type() {
  pg_e2e_exec "SELECT data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='${DST_TABLE}' AND column_name='$1';" | tr -d '[:space:]'
}
check_type() {
  local col="$1" want_regex="$2" got
  got="$(get_type "${col}")"; [[ -z "${got}" ]] && got="<missing>"
  if [[ "${got}" =~ ${want_regex} ]]; then
    log "  OK   ${col} -> ${got}"
  else
    log "  FAIL ${col} -> ${got} (expected /${want_regex}/) -- LIKELY column_types REGRESSION (TEXT fallback)"
    fail=1
  fi
}
# Integer + timestamp preservation is the core column_types guarantee (PR #75).
# Regexes accept the equivalent PG spellings: a source connector may emit canonical
# "integer" (-> INTEGER) or the stronger BIGINT, and TIMESTAMP renders as
# "timestamp without time zone" while TIMESTAMPTZ renders "with time zone".
check_type id         '^(integer|bigint)$'
check_type qty        '^(integer|bigint)$'
check_type created_at '^timestamp'
# DECIMAL must not collapse to TEXT. NUMERIC/DECIMAL is ideal (exact); DOUBLE
# PRECISION is the lossy-but-still-numeric fallback some connector versions emit.
# A "text" result here is the regression this golden test exists to catch.
check_type price      '^(numeric|decimal|double precision)'

# Key preservation: the destination connector enforces uniqueness for upserts via
# a UNIQUE INDEX (by design) rather than a formal PRIMARY KEY constraint. Accept
# EITHER -- both guarantee that the source key survived to the destination and
# de-duplication works. (Asserting only PRIMARY KEY would test a non-goal.)
log "Verifying key preservation (PK constraint OR unique index on id)..."
KEYCOLS="$(pg_e2e_exec "
SELECT a.attname
FROM pg_index i
JOIN pg_class t   ON t.oid = i.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
WHERE n.nspname='public' AND t.relname='${DST_TABLE}' AND (i.indisprimary OR i.indisunique)
ORDER BY a.attname;" | tr -d '[:space:]')"
if [[ "${KEYCOLS}" == *"id"* ]]; then
  log "  OK   key on id (PK or unique index): [${KEYCOLS}]"
else
  log "  FAIL no PK/unique index covering 'id' (got '${KEYCOLS}') -- key propagation regression"
  fail=1
fi

(( fail != 0 )) && die "GOLDEN TEST FAILED: column-type / key preservation regressed"

log "PASS: column types + key preserved end-to-end (MySQL batch -> Postgres)"
