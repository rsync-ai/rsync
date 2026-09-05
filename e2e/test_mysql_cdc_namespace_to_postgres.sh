#!/usr/bin/env bash
set -euo pipefail

# C2 regression: MySQL (Debezium CDC) -> Kafka -> kafka-mcp-sink -> PostgreSQL
# WITH a user destination namespace.
#
# Proves the CDC destination-namespace fix (C2): when the pipeline has a real
# destination namespace, CDC INSERT/UPDATE/DELETE must land in
# <namespace>.<dest_table> at the destination — NOT the default/source schema
# (the pre-C2 leak). The start_sink call passes `destination_namespace` exactly
# as the orchestrator now injects it (executor.startKafkaMCPSink / RestartCDCSink).
#
# Asserts:
#   1. dest rows land in   <DEST_NAMESPACE>.<DEST_TABLE>  (insert/update/delete)
#   2. NO leak: public.<DEST_TABLE> never receives the rows
#
# Mirrors e2e/test_mysql_cdc_to_postgres.sh (same e2e containers + Connect).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

log() { printf "\n==> %s\n" "$*"; }
die() { echo "❌ $*" >&2; exit 1; }

NETWORK="${NETWORK:-${STACK_PREFIX:-rsync-ai}_default}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SNAPSHOT_MODE="${SNAPSHOT_MODE:-never}"
START_OFFSET="earliest"
if [[ "${SNAPSHOT_MODE}" == "never" || "${SNAPSHOT_MODE}" == "streaming_only" ]]; then
  START_OFFSET="latest"
fi

MYSQL_CONTAINER="${MYSQL_CONTAINER:-${STACK_PREFIX:-rsync-ai}-mysql-e2e}"
MYSQL_ROOT_PW="${MYSQL_ROOT_PW:-rootpassword}"
DB="${DB:-e2e_db}"
TABLE="${TABLE:-cdc_ns_test}"

PG_CONTAINER="${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}"
PG_USER="${PG_USER:-e2e_user}"
PG_DB="${PG_DB:-e2e_db}"
DEST_TABLE="${DEST_TABLE:-cdc_ns_test_dest}"
PG_HOST="${PG_HOST:-${PG_CONTAINER}}"
# The user-selected destination namespace under test.
DEST_NAMESPACE="${DEST_NAMESPACE:-e2e_cdc_ns}"

DEST_CONNECTOR_TYPE="${DEST_CONNECTOR_TYPE:-postgresql}"

resolve_current_version() {
  local connector_dir="$1"
  python3 - <<PY
import json, os, sys
base = os.path.join("${ROOT_DIR}", "shared", "mcp-connectors")
path = os.path.join(base, "${connector_dir}", "latest.json")
if not os.path.exists(path):
  path = os.path.join(base, "internal", "${connector_dir}", "latest.json")
if not os.path.exists(path):
  pub = os.path.join(base, "public")
  for dirpath, dirnames, filenames in os.walk(pub):
    if "versions" in dirnames:
      dirnames.remove("versions")
    if os.path.basename(dirpath) == "${connector_dir}" and "latest.json" in filenames:
      path = os.path.join(dirpath, "latest.json")
      break
data = json.load(open(path))
cv = (data.get("current_version") or "").strip()
if not cv:
  p = (data.get("path") or "").strip()
  if p.startswith("versions/"):
    cv = p[len("versions/"):].strip()
if not cv:
  v = (data.get("version") or "").strip()
  if v:
    cv = "v" + v if not v.startswith("v") else v
if not cv:
  print(""); sys.exit(0)
if not cv.startswith("v"):
  cv = "v" + cv
print(cv)
PY
}

to_version_part() { local v="$1"; v="${v#v}"; echo "${v//./-}"; }

SINK_VERSION="${SINK_VERSION:-$(resolve_current_version "kafka-mcp-sink")}"
SINK_VERSION_PART="$(to_version_part "${SINK_VERSION}")"
SINK_CONTAINER="${SINK_CONTAINER:-${STACK_PREFIX:-rsync-ai}-kafka-mcp-sink-v${SINK_VERSION_PART}-mcp}"

DEST_CONNECTOR_VERSION="${DEST_CONNECTOR_VERSION:-$(resolve_current_version "${DEST_CONNECTOR_TYPE}")}"
DEST_VERSION_PART="$(to_version_part "${DEST_CONNECTOR_VERSION}")"
DEST_CONTAINER="${DEST_CONTAINER:-${STACK_PREFIX:-rsync-ai}-${DEST_CONNECTOR_TYPE}-v${DEST_VERSION_PART}-mcp}"

CONNECTOR_NAME="e2e-mysql-cdc-ns-to-postgres-$(date +%s)"
TOPIC="${CONNECTOR_NAME}.${DB}.${TABLE}"
SINK_GROUP="sink-${CONNECTOR_NAME}"

EVENT_ID="$(python3 -c 'import random; print(random.randint(1000000, 2000000))')"

# Helper to query the destination.
pg_q() { docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -tA -c "$1" 2>/dev/null | tr -d '[:space:]' || true; }

cleanup() {
  log "🧹 Cleanup (best-effort)…"
  docker run --rm --network "${NETWORK}" curlimages/curl:8.7.1 -sS \
    -H "Content-Type: application/json" \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"kafka-mcp-sink_stop_sink\",\"arguments\":{\"config\":{\"consumer_group\":\"${SINK_GROUP}\"}}}}" \
    "http://${SINK_CONTAINER}:8000/mcp" >/dev/null 2>&1 || true
  curl -sS -X DELETE "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

log "🧪 C2 CDC-namespace E2E: MySQL → Postgres (namespace=${DEST_NAMESPACE})"
log "  - Debezium connector: ${CONNECTOR_NAME}"
log "  - Topic: ${TOPIC}"
log "  - Sink: ${SINK_CONTAINER} (network=${NETWORK})"
log "  - Destination: ${DEST_CONNECTOR_TYPE} table=${DEST_TABLE} namespace=${DEST_NAMESPACE}"

log "⏳ Waiting for Kafka Connect…"
for i in {1..60}; do curl -fsS "${CONNECT_URL}/" 2>/dev/null | grep -q "version" && break; sleep 2; done
curl -fsS "${CONNECT_URL}/" 2>/dev/null | grep -q "version" || die "Kafka Connect not healthy at ${CONNECT_URL}"

log "⏳ Waiting for MySQL…"
for i in {1..60}; do docker exec "${MYSQL_CONTAINER}" mysqladmin ping -h 127.0.0.1 -uroot -p"${MYSQL_ROOT_PW}" --silent >/dev/null 2>&1 && break; sleep 2; done
docker exec "${MYSQL_CONTAINER}" mysqladmin ping -h 127.0.0.1 -uroot -p"${MYSQL_ROOT_PW}" --silent >/dev/null 2>&1 || die "MySQL not healthy"

log "⏳ Waiting for Postgres…"
for i in {1..60}; do docker exec "${PG_CONTAINER}" pg_isready -U "${PG_USER}" -d "${PG_DB}" >/dev/null 2>&1 && break; sleep 2; done
docker exec "${PG_CONTAINER}" pg_isready -U "${PG_USER}" -d "${PG_DB}" >/dev/null 2>&1 || die "Postgres not healthy"

log "⏳ Waiting for kafka-mcp-sink + ${DEST_CONNECTOR_TYPE} MCP servers…"
docker run --rm --network "${NETWORK}" curlimages/curl:8.7.1 -fsS "http://${SINK_CONTAINER}:8000/health" >/dev/null || die "kafka-mcp-sink not reachable on ${NETWORK}"
docker run --rm --network "${NETWORK}" curlimages/curl:8.7.1 -fsS "http://${DEST_CONTAINER}:8000/health" >/dev/null || die "${DEST_CONNECTOR_TYPE} MCP not reachable on ${NETWORK}"

log "🧱 Source table + clean destination (both schemas) for id=${EVENT_ID}…"
docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
  CREATE DATABASE IF NOT EXISTS ${DB};
  CREATE TABLE IF NOT EXISTS ${DB}.${TABLE} (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  DELETE FROM ${DB}.${TABLE} WHERE id=${EVENT_ID};
"
# Clean both the target namespace and public so the leak assertion is meaningful.
docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -v ON_ERROR_STOP=1 -c "
  DROP SCHEMA IF EXISTS ${DEST_NAMESPACE} CASCADE;
  DROP TABLE IF EXISTS public.${DEST_TABLE};
"

log "🔌 Creating Debezium MySQL connector: ${CONNECTOR_NAME}"
CREATE_PAYLOAD="$(cat <<JSON
{
  "name": "${CONNECTOR_NAME}",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "${MYSQL_CONTAINER}",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "${MYSQL_ROOT_PW}",
    "database.server.id": "184356",
    "topic.prefix": "${CONNECTOR_NAME}",
    "database.include.list": "${DB}",
    "table.include.list": "${DB}.${TABLE}",
    "snapshot.mode": "${SNAPSHOT_MODE}",
    "include.schema.changes": "false",
    "database.allowPublicKeyRetrieval": "true",
    "database.ssl.mode": "disabled",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
    "schema.history.internal.kafka.topic": "schemahistory.${CONNECTOR_NAME}",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
JSON
)"
resp="$(curl -sS -w $'\n%{http_code}' -X POST "${CONNECT_URL}/connectors" -H "Content-Type: application/json" -d "${CREATE_PAYLOAD}" || true)"
code="$(echo "${resp}" | tail -n 1)"
[[ "${code}" == "200" || "${code}" == "201" || "${code}" == "409" ]] || die "Failed to create Debezium connector (HTTP ${code})"

log "⏳ Waiting for Debezium connector + task RUNNING…"
for i in {1..90}; do
  st="$(curl -sS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || true)"
  cstate="$(echo "${st}" | jq -r '.connector.state // empty' 2>/dev/null || true)"
  tstate="$(echo "${st}" | jq -r '.tasks[0].state // empty' 2>/dev/null || true)"
  [[ "${cstate}" == "RUNNING" && "${tstate}" == "RUNNING" ]] && break
  sleep 2
  [[ $i -eq 90 ]] && { echo "${st}" | jq . || true; die "Debezium connector/task did not become RUNNING"; }
done

log "🛰️  Starting kafka-mcp-sink worker WITH destination_namespace=${DEST_NAMESPACE}…"
DEST_CONFIG_JSON="$(cat <<JSON
{
  "host": "${PG_HOST}",
  "port": 5432,
  "database": "${PG_DB}",
  "user": "${PG_USER}",
  "password": "e2e_password",
  "table": "${DEST_TABLE}",
  "key_fields": ["id"]
}
JSON
)"
# NOTE: destination_namespace is the C2 field under test. This is exactly what
# the orchestrator now injects (executor.startKafkaMCPSink + RestartCDCSink).
START_REQ="$(cat <<JSON
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "kafka-mcp-sink_start_sink",
    "arguments": {
      "config": {
        "pipeline_id": "${CONNECTOR_NAME}",
        "topics": "${TOPIC}",
        "consumer_group": "${SINK_GROUP}",
        "kafka_bootstrap_servers": "kafka:29092",
        "sink_mode": "cdc",
        "start_offset": "${START_OFFSET}",
        "destination_connector": "${DEST_CONNECTOR_TYPE}",
        "destination_version": "${DEST_CONNECTOR_VERSION}",
        "destination_namespace": "${DEST_NAMESPACE}",
        "destination_config": ${DEST_CONFIG_JSON}
      }
    }
  }
}
JSON
)"
START_RESP="$(docker run --rm --network "${NETWORK}" curlimages/curl:8.7.1 -sS -H "Content-Type: application/json" -d "${START_REQ}" "http://${SINK_CONTAINER}:8000/mcp")"
echo "${START_RESP}" | jq -e '.result.success == true' >/dev/null 2>&1 \
  || die "Failed to start sink worker: $(echo "${START_RESP}" | jq -r '.result.error // .error.message // "unknown_error"')"

# Wait for the sink consumer group to be assigned partitions (cold-start race).
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-${STACK_PREFIX:-rsync-ai}-kafka}"
sink_group_assigned() {
  docker exec "${KAFKA_CONTAINER}" bash -lc \
    "${KAFKA_BIN}/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group '${SINK_GROUP}' 2>/dev/null" \
    | awk -v t="${TOPIC}" '$2==t {print $7}' | grep -qvx '-'
}
if [[ "${START_OFFSET}" == "latest" ]]; then
  log "⏳ Waiting for sink consumer group assignment…"
  assigned=0
  # With START_OFFSET=latest the sink only captures events produced AFTER the group
  # is assigned partitions, so the INSERT re-drive below is useless until this
  # confirms. Under the gate's SERIAL_CHAIN + concurrent golden-batch load the group
  # took >30s to confirm → "assignment unconfirmed" → the row never landed (infra
  # timing, not a routing bug). Wait up to 90s; the loop breaks early when assigned,
  # so the happy path pays nothing.
  for i in {1..90}; do sink_group_assigned && { assigned=1; break; }; sleep 1; done
  [[ "${assigned}" == "1" ]] && { log "✅ assigned — settle 2s"; sleep 2; } || { log "⚠️ assignment unconfirmed; sleep 5s"; sleep 5; }
fi

# Re-drive the INSERT until the namespace dest reflects it. Each poll issues a GENUINELY
# NEW change (a fresh probe name) instead of `ON DUPLICATE KEY UPDATE name=VALUES(name)`,
# which MySQL treats as a no-op (0 rows changed -> no binlog event) once the row exists —
# so the old loop could emit only ONE event ever, and if the sink's consumer had not yet
# discovered the just-created topic, that single event was lost and the loop spun to failure.
# A fresh value per poll keeps producing real events until the streaming path is live, then
# the sink upserts by PK and the row appears. Mirrors the #311 fix on the plain CDC test.
# Liveness is confirmed by the row EXISTING; its exact value is asserted by the UPDATE step below.
# NB: the sink auto-creates this dest (no pre-typed DDL) with text columns, so the id checks
# quote the literal (WHERE id='${EVENT_ID}') — that matches whether id is text or integer,
# whereas a bare integer literal errors with "operator does not exist: text = integer".
log "✍️  INSERT into MySQL (id=${EVENT_ID}) — re-driven until the namespace dest reflects it…"
got_insert=0
for i in {1..90}; do
  docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
    INSERT INTO ${DB}.${TABLE} (id, name) VALUES (${EVENT_ID}, 'cdc-insert-${EVENT_ID}')
    ON DUPLICATE KEY UPDATE name='cdc-insert-${EVENT_ID}-probe${i}';
  " 2>/dev/null || true
  got="$(pg_q "SELECT 1 FROM ${DEST_NAMESPACE}.${DEST_TABLE} WHERE id='${EVENT_ID}'")"
  [[ "${got}" == "1" ]] && { got_insert=1; break; }
  sleep 2
done
[[ "${got_insert}" == "1" ]] || die "INSERT never landed in ${DEST_NAMESPACE}.${DEST_TABLE} (C2 namespace routing failed)"
log "✅ INSERT landed in ${DEST_NAMESPACE}.${DEST_TABLE}"

log "🔎 Leak guard: public.${DEST_TABLE} must NOT have received the row…"
leak_exists="$(pg_q "SELECT to_regclass('public.${DEST_TABLE}') IS NOT NULL")"
if [[ "${leak_exists}" == "t" ]]; then
  leak_cnt="$(pg_q "SELECT COUNT(*) FROM public.${DEST_TABLE} WHERE id='${EVENT_ID}'")"
  [[ "${leak_cnt}" == "0" ]] || die "LEAK: public.${DEST_TABLE} received ${leak_cnt} row(s) — namespace was bypassed"
fi
log "✅ No leak into public.${DEST_TABLE}"

log "✍️  UPDATE in MySQL (id=${EVENT_ID})…"
docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "UPDATE ${DB}.${TABLE} SET name='cdc-update-${EVENT_ID}' WHERE id=${EVENT_ID};"
for i in {1..60}; do
  name="$(pg_q "SELECT name FROM ${DEST_NAMESPACE}.${DEST_TABLE} WHERE id='${EVENT_ID}'")"
  [[ "${name}" == "cdc-update-${EVENT_ID}" ]] && break
  sleep 2
  [[ $i -eq 60 ]] && die "UPDATE not reflected in ${DEST_NAMESPACE}.${DEST_TABLE}"
done
log "✅ UPDATE replicated in ${DEST_NAMESPACE}.${DEST_TABLE}"

log "✍️  DELETE in MySQL (id=${EVENT_ID})…"
docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "DELETE FROM ${DB}.${TABLE} WHERE id=${EVENT_ID};"
for i in {1..60}; do
  cnt="$(pg_q "SELECT COUNT(*) FROM ${DEST_NAMESPACE}.${DEST_TABLE} WHERE id='${EVENT_ID}'")"
  [[ "${cnt}" == "0" ]] && break
  sleep 2
  [[ $i -eq 60 ]] && die "DELETE not reflected in ${DEST_NAMESPACE}.${DEST_TABLE}"
done
log "✅ DELETE replicated in ${DEST_NAMESPACE}.${DEST_TABLE}"

log "🎉 C2 CDC-namespace E2E PASSED — CDC routed to ${DEST_NAMESPACE}.${DEST_TABLE}, no public leak"
