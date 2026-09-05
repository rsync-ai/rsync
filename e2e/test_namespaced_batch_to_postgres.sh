#!/usr/bin/env bash
set -euo pipefail

# E2E: kafka-mcp-sink BATCH path + a REAL destination namespace (PR #236 regression net).
#
# Regression net for the silent-drop class fixed in PR #236 (resolveDestTableForWrite):
# the non-CDC batch write path (writeToDestination) was the ONLY one of four relational
# write paths that forwarded the per-pipeline namespace param but never stripped the
# source-schema qualifier from the target table. A PG dest with a real namespace
# therefore received e.g. "src_leak.orders" PLUS a namespace param; per the connector
# contract a QUALIFIED table makes the connector IGNORE the namespace and write into the
# leaked source schema -> read>0, landed=0 (the shopify->postgres silent-drop symptom).
#
#   pre-fix  symptom: rows land in   <source_schema>.<table>  (namespace bypassed)
#   post-fix         : rows land in   <namespace>.<table>      (table bare-ified, ns honored)
#
# WHY DRIVE THE SINK DIRECTLY (not via the orchestrator API):
#   The orchestrator's executor pre-qualifies the dest table WITH the namespace
#   (resolveDestTableName -> "<ns>.<bare>", executor.go), so the sink's batch path never
#   receives a *source*-qualified table through that route -- an orchestrator batch test
#   with a namespace would be a NO-OP for this fix (false green: it lands correctly with
#   OR without the fix). The buggy shape (source-qualified table + a DIFFERENT namespace)
#   is what the CDC path produces naturally (Debezium sets table = source) -- which is why
#   the sibling CDC fix + test_mysql_cdc_namespace_to_postgres.sh exist. To exercise the
#   BATCH path's bare-ify deterministically we synthesize exactly that buggy message and
#   feed it to the sink in batch mode -- the same direct-sink-drive pattern that
#   test_mysql_cdc_namespace_to_postgres.sh uses for CDC.
#
# Asserts:
#   1. rows land in        <NS>.<TBL>                       (fix bare-ified the table)
#   2. NO leak: source schema <SRC>.<TBL> never created at the destination
#   3. NO leak: public.<TBL> never created at the destination
#
# Requires kafka + kafka-mcp-sink + postgresql MCP + the e2e Postgres fixture, all on a
# shared network (the test bridges the connector to rsync-ai_default inline; the gate
# also runs preflight-e2e-runtime.sh's connector bridge). Set
# E2E_SKIP_UP=1 to skip bring-up when the stack is already running (the gate sets this).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

log() { printf '\n==> %s\n' "$*"; }
die() { printf '❌ FAIL: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }
need docker; need python3

NETWORK="${NETWORK:-${STACK_PREFIX:-rsync-ai}_default}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-${STACK_PREFIX:-rsync-ai}-kafka}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
PG_CONTAINER="${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}"
PG_USER="${PG_USER:-e2e_user}"
PG_DB="${PG_DB:-e2e_db}"
PG_PASSWORD="${PG_PASSWORD:-e2e_password}"
PG_HOST="${PG_HOST:-${PG_CONTAINER}}"
DEST_CONNECTOR_TYPE="${DEST_CONNECTOR_TYPE:-postgresql}"

# Orchestrator control-plane DB (the sink's durable ACK ledger lives here). The ledger
# has FKs pipeline_batch_acks.{pipeline_id->pipelines, execution_id->executions}; we seed
# minimal rows so the ack commits (else the sink parks the partition retrying forever).
ORCH_PG_CONTAINER="${ORCH_PG_CONTAINER:-postgres}"
ORCH_PG_USER="${ORCH_PG_USER:-user}"
ORCH_PG_DB="${ORCH_PG_DB:-pipeline_db}"

resolve_current_version() {
  local connector_id="$1"
  python3 - "${connector_id}" <<'PY'
import glob, json, sys
cid = sys.argv[1].strip()
root = "shared/mcp-connectors"
cands = glob.glob(f"{root}/**/{cid}/latest.json", recursive=True)
if not cands:
    raise SystemExit(f"latest.json not found for connector: {cid}")
def score(p):
    p2 = p.replace("\\", "/")
    return (1 if "/database/" in p2 else 0, len(p2))
best = sorted(set(cands), key=score)[0]
latest = json.load(open(best))
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

SINK_VERSION="${SINK_VERSION:-$(resolve_current_version "kafka-mcp-sink")}"
SINK_CONTAINER="${SINK_CONTAINER:-${STACK_PREFIX:-rsync-ai}-kafka-mcp-sink-v$(to_version_part "${SINK_VERSION}")-mcp}"
DEST_VERSION="${DEST_VERSION:-$(resolve_current_version "${DEST_CONNECTOR_TYPE}")}"
DEST_CONTAINER="${DEST_CONTAINER:-${STACK_PREFIX:-rsync-ai}-${DEST_CONNECTOR_TYPE}-v$(to_version_part "${DEST_VERSION}")-mcp}"

# Per-run-unique names so the test is fully name-isolated (safe under any parallelism)
# and cleanup is a single targeted DROP SCHEMA -- never nukes a concurrent run.
TS="$(date +%s)"
NS="ns_batch_${TS}"          # destination namespace under test (the schema rows MUST land in)
SRC_SCHEMA="srcleak_${TS}"   # synthetic SOURCE schema: prefix != NS and != dest db -> exercises the leak
TBL="orders_${TS}"
MSG_TABLE="${SRC_SCHEMA}.${TBL}"   # SOURCE-qualified table = the buggy input the fix targets
TOPIC="ns_batch_${TS}"
GROUP="sink-ns-batch-${TS}"
PIPELINE_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"   # ledger keys are UUID-typed
EXECUTION_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
# pipelines.workspace_id is NOT NULL since migration 069 (workspace activation), and a
# fresh control-plane DB has no user/workspace to borrow. Seed a self-contained chain
# (user -> workspace -> pipeline) so the FK is satisfied without depending on pre-existing rows.
WS_OWNER_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
WS_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"

pg_q() { docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -tA -c "$1" 2>/dev/null | tr -d '[:space:]' || true; }
orch_pg() { docker exec "${ORCH_PG_CONTAINER}" psql -U "${ORCH_PG_USER}" -d "${ORCH_PG_DB}" -tA -c "$1" 2>/dev/null | tr -d '[:space:]' || true; }
sink_rpc() {
  # sink_rpc <json-rpc body> -> stdout. Hits the sink MCP endpoint from the shared network.
  docker run --rm --network "${NETWORK}" curlimages/curl:8.7.1 -sS -H 'Content-Type: application/json' \
    -d "$1" "http://${SINK_CONTAINER}:8000/mcp"
}

cleanup() {
  log "🧹 Cleanup (best-effort)…"
  # Build the stop_sink RPC via a heredoc (NOT python -c "...{...}"): an inline -c with a
  # brace dict is brace-expanded by bash and produces invalid JSON.
  local stop_req
  stop_req="$(python3 - "${GROUP}" <<'PY'
import json, sys
print(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
  "params": {"name": "kafka-mcp-sink_stop_sink", "arguments": {"config": {"consumer_group": sys.argv[1]}}}}))
PY
)"
  sink_rpc "${stop_req}" >/dev/null 2>&1 || true
  docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -c \
    "DROP SCHEMA IF EXISTS \"${NS}\" CASCADE; DROP SCHEMA IF EXISTS \"${SRC_SCHEMA}\" CASCADE; DROP TABLE IF EXISTS public.\"${TBL}\";" >/dev/null 2>&1 || true
  # Remove the seeded control-plane rows (ON DELETE CASCADE removes the executions + acks).
  # Delete the pipeline first (it references the workspace), then the self-seeded
  # workspace + owning user (deleting the user cascades the workspace + membership).
  docker exec "${ORCH_PG_CONTAINER}" psql -U "${ORCH_PG_USER}" -d "${ORCH_PG_DB}" -c \
    "DELETE FROM pipelines WHERE id='${PIPELINE_ID}';
     DELETE FROM workspaces WHERE id='${WS_ID}';
     DELETE FROM users WHERE id='${WS_OWNER_ID}';" >/dev/null 2>&1 || true
  docker exec "${KAFKA_CONTAINER}" "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${KAFKA_BOOTSTRAP}" --delete --topic "${TOPIC}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

log "🧪 BATCH namespace E2E (PR #236): sink batch path, source-qualified table + namespace=${NS}"
log "  - sink:        ${SINK_CONTAINER} (network=${NETWORK})"
log "  - dest:        ${DEST_CONNECTOR_TYPE} ${DEST_VERSION} -> ${PG_HOST}/${PG_DB}"
log "  - msg table:   ${MSG_TABLE}  (db_or_schema=${NS})"

# --- optional bring-up (standalone use; the gate sets E2E_SKIP_UP=1) --------
if [[ "${E2E_SKIP_UP:-}" != "1" ]]; then
  BUILD_OPT=""; [[ "${E2E_BUILD:-}" == "1" ]] && BUILD_OPT="--build"
  log "Ensuring stack is up…"
  docker compose -p "${STACK_PREFIX:-rsync-ai}" -f docker-compose.yml -f docker-compose.e2e.yml up -d ${BUILD_OPT} \
    postgres redis kafka kafka-mcp-sink-mcp >/dev/null
  docker compose -p "${STACK_PREFIX:-rsync-ai}-mcp" -f docker-compose.mcp.yml up -d ${BUILD_OPT} \
    "${DEST_CONNECTOR_TYPE}-v$(to_version_part "${DEST_VERSION}")-mcp" >/dev/null
  docker compose -p "${STACK_PREFIX:-rsync-ai}-e2e" -f docker-compose.e2e.dbs.yml up -d postgres-e2e >/dev/null
fi

# Ensure the dest MCP connector can reach the e2e Postgres fixture: the connector lives
# on rsync-ai-mcp, the e2e DB fixtures on rsync-ai_default. Attach the connector to
# rsync-ai_default (idempotent; mirrors preflight-e2e-runtime.sh's
# bridge_connectors_to_e2e_net) so it resolves the fixture — inline so the test is
# self-contained in both the gate and standalone (KI-E2E-GATE-CONNECTOR-NET).
docker network connect "${STACK_PREFIX:-rsync-ai}_default" "${DEST_CONTAINER}" >/dev/null 2>&1 || true

log "⏳ Waiting for kafka-mcp-sink + ${DEST_CONNECTOR_TYPE} MCP on ${NETWORK}…"
for _ in $(seq 1 60); do
  docker run --rm --network "${NETWORK}" curlimages/curl:8.7.1 -fsS "http://${SINK_CONTAINER}:8000/health" >/dev/null 2>&1 && break
  sleep 2
done
docker run --rm --network "${NETWORK}" curlimages/curl:8.7.1 -fsS "http://${SINK_CONTAINER}:8000/health" >/dev/null 2>&1 \
  || die "kafka-mcp-sink not reachable on ${NETWORK}"
for _ in $(seq 1 30); do docker exec "${PG_CONTAINER}" pg_isready -U "${PG_USER}" -d "${PG_DB}" >/dev/null 2>&1 && break; sleep 2; done
docker exec "${PG_CONTAINER}" pg_isready -U "${PG_USER}" -d "${PG_DB}" >/dev/null 2>&1 || die "e2e postgres not ready"

log "🧱 Pre-clean destination schemas (namespace + leaked-source + public)…"
docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -v ON_ERROR_STOP=1 -c \
  "DROP SCHEMA IF EXISTS \"${NS}\" CASCADE; DROP SCHEMA IF EXISTS \"${SRC_SCHEMA}\" CASCADE; DROP TABLE IF EXISTS public.\"${TBL}\";" >/dev/null

log "🌱 Seeding control-plane rows so the sink's ACK ledger FK is satisfied (else it parks the partition)…"
docker exec "${ORCH_PG_CONTAINER}" psql -U "${ORCH_PG_USER}" -d "${ORCH_PG_DB}" -v ON_ERROR_STOP=1 -c \
  "INSERT INTO users (id,email,password_hash) VALUES ('${WS_OWNER_ID}','e2e-ns-batch-${TS}@e2e.local','x') ON CONFLICT (id) DO NOTHING;
   INSERT INTO workspaces (id,name,slug,owner_id) VALUES ('${WS_ID}','e2e-ns-batch','e2e-ns-batch-${TS}','${WS_OWNER_ID}') ON CONFLICT (id) DO NOTHING;
   INSERT INTO pipelines (id,name,natural_language_request,workspace_id) VALUES ('${PIPELINE_ID}','e2e-ns-batch-${TS}','PR#236 direct-sink namespaced-batch regression net','${WS_ID}') ON CONFLICT (id) DO NOTHING;
   INSERT INTO executions (id,pipeline_id) VALUES ('${EXECUTION_ID}','${PIPELINE_ID}') ON CONFLICT (id) DO NOTHING;" >/dev/null \
  || die "could not seed control-plane rows (users/workspaces/pipelines/executions) for the ACK ledger FK"

log "📨 Pre-create topic + produce ONE source-qualified batch message…"
docker exec "${KAFKA_CONTAINER}" "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${KAFKA_BOOTSTRAP}" \
  --create --topic "${TOPIC}" --partitions 1 --replication-factor 1 >/dev/null 2>&1 \
  || die "could not create topic ${TOPIC}"
# The message mirrors the executor's inline batch shape (sendChunkedToKafka) EXCEPT the
# table is SOURCE-qualified (prefix != namespace) -- the exact input PR #236 fixes.
MSG="$(python3 - "$PIPELINE_ID" "$EXECUTION_ID" "$MSG_TABLE" "$NS" <<'PY'
import json, sys
pid, exid, table, ns = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
print(json.dumps({
  "pipeline_id": pid, "execution_id": exid, "trace_id": pid,
  "table": table, "storage_type": "inline", "db_or_schema": ns,
  "primary_keys": ["id"], "key_fields": ["id"], "run_mode": "reload", "row_count": 3,
  "data": [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}, {"id": 3, "name": "carol"}],
  "column_types": {"id": "integer", "name": "text"},
}, separators=(",", ":")))
PY
)"
# Produce from a file INSIDE the kafka container: the console-producer drops the message
# on UNKNOWN_TOPIC_OR_PARTITION if it races topic auto-create, so the topic is pre-created
# above and we feed a complete line via stdin redirection (MSG has no single quotes).
docker exec "${KAFKA_CONTAINER}" bash -lc \
  "printf '%s\n' '${MSG}' > /tmp/${TOPIC}.json && ${KAFKA_BIN}/kafka-console-producer.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${TOPIC} < /tmp/${TOPIC}.json" \
  || die "produce failed"
END_OFFSET="$(docker exec "${KAFKA_CONTAINER}" "${KAFKA_BIN}/kafka-get-offsets.sh" --bootstrap-server "${KAFKA_BOOTSTRAP}" --topic "${TOPIC}" 2>/dev/null | tr -d '[:space:]')"
[[ "${END_OFFSET}" == "${TOPIC}:0:1" ]] || die "message not persisted to topic (end offset=${END_OFFSET:-none}); produce dropped it"
log "  ✅ message persisted (${END_OFFSET})"

log "🛰️  Starting kafka-mcp-sink in BATCH mode (sink_mode=auto, earliest, namespace=${NS})…"
START_REQ="$(python3 - "$PIPELINE_ID" "$EXECUTION_ID" "$TOPIC" "$GROUP" "$NS" "$DEST_CONNECTOR_TYPE" "$DEST_VERSION" "$PG_HOST" "$PG_DB" "$PG_USER" "$PG_PASSWORD" <<'PY'
import json, sys
pid, exid, topic, group, ns, dconn, dver, host, db, user, pw = sys.argv[1:12]
print(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
  "name": "kafka-mcp-sink_start_sink", "arguments": {"config": {
    "pipeline_id": pid, "execution_id": exid, "topics": topic, "consumer_group": group,
    "kafka_bootstrap_servers": "kafka:29092", "sink_mode": "auto", "start_offset": "earliest",
    "destination_connector": dconn, "destination_version": dver, "destination_namespace": ns,
    # NOTE: no "table" in destination_config -> sink routes per message.table (multi-table style),
    # so the per-message source-qualified table is what reaches writeToDestination.
    "destination_config": {"host": host, "port": 5432, "database": db, "user": user, "password": pw},
  }}}}))
PY
)"
START_RESP="$(sink_rpc "${START_REQ}")"
echo "${START_RESP}" | python3 -c 'import json,sys; d=json.load(sys.stdin); r=d.get("result",{}); sys.exit(0 if r.get("success") or r.get("status")=="started" else 1)' \
  || die "start_sink failed: ${START_RESP}"

log "⏳ Waiting for rows to land in ${NS}.${TBL} (up to 60s)…"
landed=""
for _ in $(seq 1 30); do
  landed="$(pg_q "SELECT COUNT(*) FROM \"${NS}\".\"${TBL}\";")"
  [[ "${landed}" == "3" ]] && break
  sleep 2
done

# --- assertions -------------------------------------------------------------
log "🔎 Assertion 1: rows landed in ${NS}.${TBL}"
[[ "${landed}" == "3" ]] || die "expected 3 rows in ${NS}.${TBL}, got '${landed:-0}' — fix did NOT route to the namespace"
log "  ✅ ${NS}.${TBL} = 3"

log "🔎 Assertion 2: NO leak into source schema ${SRC_SCHEMA}"
src_leaked="$(pg_q "SELECT to_regclass('\"${SRC_SCHEMA}\".\"${TBL}\"') IS NOT NULL;")"
[[ "${src_leaked}" == "t" ]] && die "LEAK: ${SRC_SCHEMA}.${TBL} was created at the destination — namespace was bypassed (pre-fix symptom)"
log "  ✅ ${SRC_SCHEMA}.${TBL} absent"

log "🔎 Assertion 3: NO leak into public"
pub_leaked="$(pg_q "SELECT to_regclass('public.\"${TBL}\"') IS NOT NULL;")"
[[ "${pub_leaked}" == "t" ]] && die "LEAK: public.${TBL} was created at the destination — namespace was bypassed"
log "  ✅ public.${TBL} absent"

# Belt-and-suspenders: the ONLY schema holding the table must be the namespace.
schemas="$(pg_q "SELECT string_agg(table_schema,',' ORDER BY table_schema) FROM information_schema.tables WHERE table_name='${TBL}';")"
[[ "${schemas}" == "${NS}" ]] || die "table ${TBL} found in unexpected schema(s): '${schemas}' (expected only ${NS})"

log "🔎 Assertion 4: durable ACK ledger honestly recorded the write (rows_written=3)"
# With the FK rows seeded, the sink commits the ack right after the destination write;
# poll briefly to absorb that lag. A non-3 here means the write was not honestly acked
# (the silent-drop accounting hole), so it is a hard failure.
ACK=""
for _ in $(seq 1 15); do
  ACK="$(orch_pg "SELECT COALESCE(SUM(rows_written),0) FROM pipeline_batch_acks WHERE pipeline_id='${PIPELINE_ID}';")"
  [[ "${ACK}" == "3" ]] && break
  sleep 1
done
[[ "${ACK}" == "3" ]] || die "ACK ledger rows_written=${ACK:-<none>} (expected 3) — write not honestly acked"
log "  ✅ ACK ledger rows_written=3 (honest write, offset committed)"

log "🎉 PASS: BATCH path bare-ified the source-qualified table; rows landed in ${NS}.${TBL}, no source/public leak (PR #236 verified)"
