#!/usr/bin/env bash
set -euo pipefail

# HYBRID CDC E2E:
# MySQL (Debezium CDC) -> Kafka (dim tables unified topic + txn table per-table topic)
#   -> kafka-mcp-sink -> PostgreSQL
#
# Validates:
# - Debezium RegexRouter SMT routes dim_* into "<topic_prefix>.dimensions"
# - kafka-mcp-sink can consume multiple topics in one worker
# - Destination tables can be auto-created (DDL) and CDC events are applied

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

log() { printf "\n==> %s\n" "$*"; }
die() { echo "❌ $*" >&2; exit 1; }

NETWORK="${NETWORK:-${STACK_PREFIX:-rsync-ai}_default}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SNAPSHOT_MODE="${SNAPSHOT_MODE:-never}" # "initial" | "never" (stream-only)
START_OFFSET="latest"
if [[ "${SNAPSHOT_MODE}" == "initial" ]]; then
  START_OFFSET="earliest"
fi

MYSQL_CONTAINER="${MYSQL_CONTAINER:-${STACK_PREFIX:-rsync-ai}-mysql-e2e}"
MYSQL_ROOT_PW="${MYSQL_ROOT_PW:-rootpassword}"
DB="${DB:-e2e_db}"
DIM_TABLE="${DIM_TABLE:-dim_products}"
TXN_TABLE="${TXN_TABLE:-fact_orders}"

PG_CONTAINER="${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}"
PG_USER="${PG_USER:-e2e_user}"
PG_DB="${PG_DB:-e2e_db}"
PG_HOST="${PG_HOST:-${PG_CONTAINER}}"

SINK_CONTAINER="${SINK_CONTAINER:-${STACK_PREFIX:-rsync-ai}-kafka-mcp-sink-v1-0-0-mcp}"
# Use the versioned container_name, not the bare `rsync-ai-postgresql-mcp` alias:
# that alias is declared only on the `rsync-ai-mcp` network (docker-compose.mcp.yml
# networks: rsync-network → name: rsync-ai-mcp), so it does NOT resolve from the
# gate's NETWORK=rsync-ai_default → the line-113 health check fails with NXDOMAIN.
# The container_name resolves as a DNSName on both networks (matches SINK_CONTAINER above).
DEST_CONTAINER="${DEST_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgresql-v1-0-0-mcp}"
DEST_CONNECTOR_TYPE="${DEST_CONNECTOR_TYPE:-postgresql}"
DEST_CONNECTOR_VERSION="${DEST_CONNECTOR_VERSION:-v1.0.0}"

CONNECTOR_NAME="e2e-hybrid-cdc-$(date +%s)"
TOPIC_PREFIX="${CONNECTOR_NAME}"
UNIFIED_TOPIC="${TOPIC_PREFIX}.dimensions"
TXN_TOPIC="${TOPIC_PREFIX}.${DB}.${TXN_TABLE}"
SINK_GROUP="sink-${CONNECTOR_NAME}"

# Stable ids for this run
PID="$((3000000 + RANDOM))"
OID="$((4000000 + RANDOM))"

cleanup() {
  log "🧹 Cleanup (best-effort)…"

  # Stop sink worker
  docker run --rm --network "${NETWORK}" curlimages/curl:8.7.1 -sS \
    -H "Content-Type: application/json" \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"kafka-mcp-sink_stop_sink\",\"arguments\":{\"config\":{\"consumer_group\":\"${SINK_GROUP}\"}}}}" \
    "http://${SINK_CONTAINER}:8000/mcp" >/dev/null 2>&1 || true

  # Delete Debezium connector
  curl -sS -X DELETE "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" >/dev/null 2>&1 || true
}
KEEP_RESOURCES="${KEEP_RESOURCES:-0}"
if [[ "${KEEP_RESOURCES}" != "1" ]]; then
  trap cleanup EXIT
else
  log "⚠️  KEEP_RESOURCES=1: leaving Debezium connector + sink running for debugging"
fi

log "🧪 HYBRID CDC E2E: MySQL → Kafka → Postgres"
log "  - Debezium connector: ${CONNECTOR_NAME}"
log "  - Unified topic:      ${UNIFIED_TOPIC}"
log "  - Txn topic:          ${TXN_TOPIC}"
log "  - IDs: ${DIM_TABLE}.id=${PID}, ${TXN_TABLE}.order_id=${OID}"

log "⏳ Waiting for Kafka Connect…"
for i in {1..60}; do
  if curl -fsS "${CONNECT_URL}/" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
curl -fsS "${CONNECT_URL}/" >/dev/null 2>&1 || die "Kafka Connect not healthy at ${CONNECT_URL}"

log "⏳ Waiting for MySQL…"
for i in {1..60}; do
  if docker exec "${MYSQL_CONTAINER}" mysqladmin ping -h 127.0.0.1 -uroot -p"${MYSQL_ROOT_PW}" --silent >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker exec "${MYSQL_CONTAINER}" mysqladmin ping -h 127.0.0.1 -uroot -p"${MYSQL_ROOT_PW}" --silent >/dev/null 2>&1 \
  || die "MySQL not healthy"

log "⏳ Waiting for Postgres…"
for i in {1..60}; do
  if docker exec "${PG_CONTAINER}" pg_isready -U "${PG_USER}" -d "${PG_DB}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker exec "${PG_CONTAINER}" pg_isready -U "${PG_USER}" -d "${PG_DB}" >/dev/null 2>&1 \
  || die "Postgres not healthy"

log "⏳ Waiting for kafka-mcp-sink MCP server…"
docker run --rm --network "${NETWORK}" curlimages/curl:8.7.1 -fsS \
  "http://${SINK_CONTAINER}:8000/health" >/dev/null || die "kafka-mcp-sink not reachable on ${NETWORK}"

log "⏳ Waiting for postgresql MCP server…"
docker run --rm --network "${NETWORK}" curlimages/curl:8.7.1 -fsS \
  "http://${DEST_CONTAINER}:8000/health" >/dev/null || die "postgresql MCP not reachable on ${NETWORK}"

log "🧱 Ensuring source tables exist + cleaning rows…"
docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
  CREATE DATABASE IF NOT EXISTS ${DB};
  CREATE TABLE IF NOT EXISTS ${DB}.${DIM_TABLE} (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
  );
  CREATE TABLE IF NOT EXISTS ${DB}.${TXN_TABLE} (
    order_id INT PRIMARY KEY,
    amount INT NOT NULL
  );
  DELETE FROM ${DB}.${DIM_TABLE} WHERE id=${PID};
  DELETE FROM ${DB}.${TXN_TABLE} WHERE order_id=${OID};
"

log "🧱 Dropping destination tables so auto-DDL must create them…"
docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -v ON_ERROR_STOP=1 -c "
  DROP TABLE IF EXISTS public.${DIM_TABLE};
  DROP TABLE IF EXISTS public.${TXN_TABLE};
"

log "🧱 Ensuring Kafka topics exist (avoid UNKNOWN_TOPIC_OR_PARTITION)…"
# apache/kafka image ships the CLI as kafka-topics.sh under /opt/kafka/bin (not on PATH, no bare `kafka-topics`).
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
docker exec "${STACK_PREFIX:-rsync-ai}-kafka" bash -lc "${KAFKA_BIN}/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic '${UNIFIED_TOPIC}' --partitions 1 --replication-factor 1" >/dev/null
docker exec "${STACK_PREFIX:-rsync-ai}-kafka" bash -lc "${KAFKA_BIN}/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic '${TXN_TOPIC}' --partitions 1 --replication-factor 1" >/dev/null

log "🔌 Creating Debezium connector with RegexRouter SMT (dim_* → unified topic)…"
CREATE_PAYLOAD="$(python3 - <<PY
import json
name = "${CONNECTOR_NAME}"
host = "${MYSQL_CONTAINER}"
pw = "${MYSQL_ROOT_PW}"
db = "${DB}"
dim = "${DIM_TABLE}"
txn = "${TXN_TABLE}"
prefix = "${TOPIC_PREFIX}"
unified = "${UNIFIED_TOPIC}"
regex = rf"^{prefix}\.[^.]+\.(dim_|dimension_|lookup_|lkp_|ref_).*$"
print(json.dumps({
  "name": name,
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": host,
    "database.port": "3306",
    "database.user": "root",
    "database.password": pw,
    "database.server.id": "184156",
    "topic.prefix": prefix,
    "database.include.list": db,
    "table.include.list": f"{db}.{dim},{db}.{txn}",
    "snapshot.mode": "${SNAPSHOT_MODE}",
    "include.schema.changes": "false",
    "database.allowPublicKeyRetrieval": "true",
    "database.ssl.mode": "disabled",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
    "schema.history.internal.kafka.topic": f"schemahistory.{name}",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "transforms": "routeDims",
    "transforms.routeDims.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.routeDims.regex": regex,
    "transforms.routeDims.replacement": unified,
  }
}))
PY
)"

resp="$(curl -sS -w $'\n%{http_code}' -X POST "${CONNECT_URL}/connectors" -H "Content-Type: application/json" -d "${CREATE_PAYLOAD}" || true)"
code="$(echo "${resp}" | tail -n 1)"
if [[ "${code}" != "200" && "${code}" != "201" && "${code}" != "409" ]]; then
  die "Failed to create Debezium connector (HTTP ${code}): $(echo "${resp}" | head -n 5)"
fi

log "⏳ Waiting for Debezium connector + task to be RUNNING…"
for i in {1..90}; do
  st="$(curl -sS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || true)"
  cstate="$(echo "${st}" | jq -r '.connector.state // empty' 2>/dev/null || true)"
  tstate="$(echo "${st}" | jq -r '.tasks[0].state // empty' 2>/dev/null || true)"
  if [[ "${cstate}" == "RUNNING" && "${tstate}" == "RUNNING" ]]; then
    break
  fi
  sleep 2
  if [[ $i -eq 90 ]]; then
    echo "${st}" | jq . || true
    die "Debezium connector/task did not become RUNNING"
  fi
done

log "🛰️  Starting kafka-mcp-sink worker (2 topics) → Postgres (no table override)…"
START_REQ="$(python3 - <<PY
import json
print(json.dumps({
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "kafka-mcp-sink_start_sink",
    "arguments": {
      "config": {
        "pipeline_id": "${CONNECTOR_NAME}",
        "topics": ["${UNIFIED_TOPIC}", "${TXN_TOPIC}"],
        "consumer_group": "${SINK_GROUP}",
        "kafka_bootstrap_servers": "kafka:29092",
        "sink_mode": "cdc",
        "start_offset": "${START_OFFSET}",
        "destination_connector": "${DEST_CONNECTOR_TYPE}",
        "destination_version": "${DEST_CONNECTOR_VERSION}",
        "destination_config": {
          "host": "${PG_HOST}",
          "port": 5432,
          "database": "${PG_DB}",
          "user": "${PG_USER}",
          "password": "e2e_password"
        }
      }
    }
  }
}))
PY
)"

START_RESP="$(
  docker run --rm --network "${NETWORK}" curlimages/curl:8.7.1 -sS \
    -H "Content-Type: application/json" \
    -d "${START_REQ}" \
    "http://${SINK_CONTAINER}:8000/mcp"
)"
if ! echo "${START_RESP}" | jq -e '.result.success == true' >/dev/null 2>&1; then
  echo "${START_RESP}" | jq . || true
  die "Failed to start sink worker"
fi

log "⏳ Waiting 5s for sink consumer to subscribe…"
sleep 5

log "✍️  INSERT into MySQL (dim + txn)…"
docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
  INSERT INTO ${DB}.${DIM_TABLE} (id, name) VALUES (${PID}, 'dim-${PID}');
  INSERT INTO ${DB}.${TXN_TABLE} (order_id, amount) VALUES (${OID}, 123);
"

log "📥 Verifying topic routing by consuming 1 message from each topic…"
DIM_MSG="$(docker exec "${STACK_PREFIX:-rsync-ai}-kafka" bash -lc "timeout 20 ${KAFKA_BIN}/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic '${UNIFIED_TOPIC}' --from-beginning --max-messages 1" 2>/dev/null || true)"
TXN_MSG="$(docker exec "${STACK_PREFIX:-rsync-ai}-kafka" bash -lc "timeout 20 ${KAFKA_BIN}/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic '${TXN_TOPIC}' --from-beginning --max-messages 1" 2>/dev/null || true)"

export DIM_MSG
export TXN_MSG
export DIM_TABLE
export TXN_TABLE

python3 - <<'PY'
import json, os, sys

def check(msg: str, expected_table: str) -> None:
    msg = (msg or "").strip()
    if not msg:
        raise SystemExit(f"Missing Kafka message for {expected_table}")
    o = json.loads(msg)
    src = o.get("source") or {}
    got = src.get("table") or ""
    if got != expected_table:
        raise SystemExit(f"Unexpected source.table. expected={expected_table} got={got}")

check(os.environ.get("DIM_MSG"), os.environ.get("DIM_TABLE", "dim_products"))
check(os.environ.get("TXN_MSG"), os.environ.get("TXN_TABLE", "fact_orders"))
print("✅ Kafka routing looks correct (dim event in unified topic, txn event in per-table topic)")
PY

log "⏳ Waiting for Postgres to reflect dim INSERT (auto-DDL + apply)…"
for i in {1..90}; do
  name="$(docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -tA -c "SELECT name FROM public.${DIM_TABLE} WHERE id='${PID}'" 2>/dev/null | tr -d '[:space:]' || true)"
  if [[ "${name}" == "dim-${PID}" ]]; then
    break
  fi
  sleep 2
  if [[ $i -eq 90 ]]; then
    die "Postgres did not receive dim INSERT in time"
  fi
done
log "✅ dim INSERT replicated (public.${DIM_TABLE})"

log "⏳ Waiting for Postgres to reflect txn INSERT (auto-DDL + apply)…"
for i in {1..90}; do
  amt="$(docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -tA -c "SELECT amount FROM public.${TXN_TABLE} WHERE order_id='${OID}'" 2>/dev/null | tr -d '[:space:]' || true)"
  if [[ "${amt}" == "123" ]]; then
    break
  fi
  sleep 2
  if [[ $i -eq 90 ]]; then
    die "Postgres did not receive txn INSERT in time"
  fi
done
log "✅ txn INSERT replicated (public.${TXN_TABLE})"

log "🎉 HYBRID CDC MySQL → Postgres E2E PASSED"

