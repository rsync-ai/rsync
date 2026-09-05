#!/usr/bin/env bash
set -euo pipefail

# MySQL (Debezium CDC) -> Kafka -> kafka-mcp-sink -> PostgreSQL
#
# This is an end-to-end CDC validation for the Demo-2 architecture, using:
# - rsync-ai-e2e MySQL + Postgres containers (mysql-e2e/postgres-e2e)
# - Kafka Connect (Debezium) on http://localhost:8083
# - kafka-mcp-sink MCP connector (internal) running on rsync-ai_default (port 8000)
# - postgresql MCP connector running on rsync-ai_default (port 8000)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

log() { printf "\n==> %s\n" "$*"; }
die() { echo "❌ $*" >&2; exit 1; }

NETWORK="${NETWORK:-${STACK_PREFIX:-rsync-ai}_default}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SNAPSHOT_MODE="${SNAPSHOT_MODE:-never}" # "initial" (snapshot + stream) or "never" (stream-only)
# Always consume THIS run's topic from the beginning. The Debezium server name is the
# timestamped connector name, so both the topic (<connector>.<db>.<table>) and the sink
# consumer group (sink-<connector>) are unique per run — "earliest" therefore reads
# exactly this run's change events from offset 0, with no history to replay. "latest"
# was the root cause of the "Postgres did not receive INSERT in time" flake: it is
# resolved at consumer-join time, so on a cold/loaded CI runner any event produced
# before the group finished joining was silently skipped, and the idempotent re-drive
# (a no-op upsert) produced nothing new to recover it. "earliest" makes a late join
# harmless — the first event is still on the topic to consume whenever the group joins.
START_OFFSET="earliest"

MYSQL_CONTAINER="${MYSQL_CONTAINER:-${STACK_PREFIX:-rsync-ai}-mysql-e2e}"
MYSQL_ROOT_PW="${MYSQL_ROOT_PW:-rootpassword}"
DB="${DB:-e2e_db}"
TABLE="${TABLE:-cdc_test}"

PG_CONTAINER="${PG_CONTAINER:-${STACK_PREFIX:-rsync-ai}-postgres-e2e}"
PG_USER="${PG_USER:-e2e_user}"
PG_DB="${PG_DB:-e2e_db}"
DEST_TABLE="${DEST_TABLE:-cdc_test_dest}"
PG_HOST="${PG_HOST:-${PG_CONTAINER}}"

SINK_CONTAINER="${SINK_CONTAINER:-}"
DEST_CONNECTOR_TYPE="${DEST_CONNECTOR_TYPE:-postgresql}"

# Versioned-only runtime container naming:
#   rsync-ai-<id>-v<MAJOR>-<MINOR>-<PATCH>-mcp
resolve_current_version() {
  local connector_dir="$1"
  python3 - <<PY
import json, os, sys
base = os.path.join("${ROOT_DIR}", "shared", "mcp-connectors")
path = os.path.join(base, "${connector_dir}", "latest.json")
if not os.path.exists(path):
  # Internal connectors are stored under shared/mcp-connectors/internal/<id>/
  path = os.path.join(base, "internal", "${connector_dir}", "latest.json")
if not os.path.exists(path):
  # Public connectors are stored under shared/mcp-connectors/public/<category>/<id>/
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
  print("")
  sys.exit(0)
if not cv.startswith("v"):
  cv = "v" + cv
print(cv)
PY
}

to_version_part() {
  local v="$1"
  v="${v#v}"
  echo "${v//./-}"
}

SINK_VERSION="${SINK_VERSION:-$(resolve_current_version "kafka-mcp-sink")}"
SINK_VERSION_PART="$(to_version_part "${SINK_VERSION}")"
SINK_CONTAINER_DEFAULT="${STACK_PREFIX:-rsync-ai}-kafka-mcp-sink-v${SINK_VERSION_PART}-mcp"
SINK_CONTAINER="${SINK_CONTAINER:-${SINK_CONTAINER_DEFAULT}}"

DEST_CONNECTOR_VERSION="${DEST_CONNECTOR_VERSION:-$(resolve_current_version "${DEST_CONNECTOR_TYPE}")}"
DEST_VERSION_PART="$(to_version_part "${DEST_CONNECTOR_VERSION}")"
DEST_CONTAINER_DEFAULT="${STACK_PREFIX:-rsync-ai}-${DEST_CONNECTOR_TYPE}-v${DEST_VERSION_PART}-mcp"
DEST_CONTAINER="${DEST_CONTAINER:-${DEST_CONTAINER_DEFAULT}}"

CONNECTOR_NAME="e2e-mysql-cdc-to-postgres-$(date +%s)"
TOPIC="${CONNECTOR_NAME}.${DB}.${TABLE}"
SINK_GROUP="sink-${CONNECTOR_NAME}"

EVENT_ID="$(python3 - <<'PY'
import random
print(random.randint(1000000, 2000000))
PY
)"

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
trap cleanup EXIT

log "🧪 CDC E2E: MySQL → Postgres"
log "  - Kafka Connect: ${CONNECT_URL}"
log "  - Debezium connector: ${CONNECTOR_NAME}"
log "  - Topic: ${TOPIC}"
log "  - Sink: ${SINK_CONTAINER} (network=${NETWORK})"
log "  - Destination: ${DEST_CONNECTOR_TYPE} (table=${DEST_TABLE})"

log "⏳ Waiting for Kafka Connect…"
for i in {1..60}; do
  if curl -fsS "${CONNECT_URL}/" | grep -q "version"; then
    break
  fi
  sleep 2
done
curl -fsS "${CONNECT_URL}/" | grep -q "version" || die "Kafka Connect not healthy at ${CONNECT_URL}"

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

log "🧱 Ensuring source/destination tables exist and are clean (id=${EVENT_ID})…"
docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
  CREATE DATABASE IF NOT EXISTS ${DB};
  CREATE TABLE IF NOT EXISTS ${DB}.${TABLE} (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  DELETE FROM ${DB}.${TABLE} WHERE id=${EVENT_ID};
"

docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -v ON_ERROR_STOP=1 -c "
  CREATE TABLE IF NOT EXISTS ${DEST_TABLE} (
    id INT PRIMARY KEY,
    name TEXT,
    created_at TIMESTAMP
  );
  DELETE FROM ${DEST_TABLE} WHERE id=${EVENT_ID};
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
    "database.server.id": "184156",
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
if [[ "${code}" != "200" && "${code}" != "201" && "${code}" != "409" ]]; then
  die "Failed to create Debezium connector (HTTP ${code}): $(echo \"${resp}\" | sed '$d' | head -n 10)"
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

log "🛰️  Starting kafka-mcp-sink worker (topic -> Postgres)…"
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
        "destination_config": ${DEST_CONFIG_JSON}
      }
    }
  }
}
JSON
)"

START_RESP="$(
  docker run --rm --network "${NETWORK}" curlimages/curl:8.7.1 -sS \
    -H "Content-Type: application/json" \
    -d "${START_REQ}" \
    "http://${SINK_CONTAINER}:8000/mcp"
)"
echo "${START_RESP}" | jq . >/dev/null 2>&1 || true
if echo "${START_RESP}" | jq -e '.error' >/dev/null 2>&1; then
  die "Failed to start sink worker: $(echo \"${START_RESP}\" | jq -r '.error.message')"
fi
if ! echo "${START_RESP}" | jq -e '.result.success == true' >/dev/null 2>&1; then
  die "Failed to start sink worker: $(echo \"${START_RESP}\" | jq -r '.result.error // .error.message // \"unknown_error\"')"
fi

# Readiness is handled entirely by the "earliest" offset + the self-healing re-drive
# below — we deliberately do NOT pre-wait for consumer-group assignment. The previous
# approach polled `kafka-consumer-groups.sh --describe` up to 30× and EACH call spawns a
# JVM inside the kafka container; on a memory-starved CI runner that pile-on was both
# unreliable (it logged "could not confirm assignment" and fell through) AND a load
# amplifier that worsened the very starvation it was racing. With "earliest" the join no
# longer has to win a race against the first INSERT, so the poll is unnecessary.

# Re-drive a change for this id until Postgres reflects it. Debezium can report the
# connector/task RUNNING *before* its binlog streaming position is fully established, so
# a single change event produced in that window can be dropped before it reaches Kafka.
# Each poll therefore issues a GENUINELY NEW change (a fresh name value) instead of a
# no-op upsert: the old `ON DUPLICATE KEY UPDATE name=VALUES(name)` re-wrote the same
# value, which MySQL treats as a no-op (0 rows changed → no binlog event), so it could
# NOT actually self-heal a dropped/missed event despite the loop — that was the latent
# bug behind the flake. With "earliest" + a fresh event per poll, once streaming is live
# the sink upserts the row by its PK and the wait clears. We confirm liveness by the row
# appearing at all (its exact value is asserted by the UPDATE step below).
log "✍️  INSERT into MySQL (id=${EVENT_ID}) — re-driven until the streaming path is confirmed live…"
got_insert=0
for i in {1..90}; do
  docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
    INSERT INTO ${DB}.${TABLE} (id, name) VALUES (${EVENT_ID}, 'cdc-insert-${EVENT_ID}')
    ON DUPLICATE KEY UPDATE name='cdc-insert-${EVENT_ID}-probe${i}';
  " 2>/dev/null || true
  got="$(docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -tA -c "SELECT 1 FROM ${DEST_TABLE} WHERE id=${EVENT_ID}" 2>/dev/null | tr -d '[:space:]' || true)"
  if [[ "${got}" == "1" ]]; then
    got_insert=1
    break
  fi
  sleep 2
done
[[ "${got_insert}" == "1" ]] || die "Postgres did not receive INSERT in time (CDC streaming path never confirmed live)"
log "✅ INSERT replicated (streaming path confirmed live)"

log "✍️  UPDATE in MySQL (id=${EVENT_ID})…"
docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "UPDATE ${DB}.${TABLE} SET name='cdc-update-${EVENT_ID}' WHERE id=${EVENT_ID};"

log "⏳ Waiting for Postgres to reflect UPDATE…"
for i in {1..60}; do
  name="$(docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -tA -c "SELECT name FROM ${DEST_TABLE} WHERE id=${EVENT_ID}" 2>/dev/null | tr -d '[:space:]' || true)"
  if [[ "${name}" == "cdc-update-${EVENT_ID}" ]]; then
    break
  fi
  sleep 2
  if [[ $i -eq 60 ]]; then
    die "Postgres did not receive UPDATE in time"
  fi
done
log "✅ UPDATE replicated"

log "✍️  DELETE in MySQL (id=${EVENT_ID})…"
docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "DELETE FROM ${DB}.${TABLE} WHERE id=${EVENT_ID};"

log "⏳ Waiting for Postgres to reflect DELETE…"
for i in {1..60}; do
  cnt="$(docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -tA -c "SELECT COUNT(*) FROM ${DEST_TABLE} WHERE id=${EVENT_ID}" 2>/dev/null | tr -d '[:space:]' || true)"
  if [[ "${cnt}" == "0" ]]; then
    break
  fi
  sleep 2
  if [[ $i -eq 60 ]]; then
    die "Postgres did not receive DELETE in time"
  fi
done
log "✅ DELETE replicated"

log "🎉 CDC MySQL → Postgres E2E PASSED"

