#!/usr/bin/env bash
set -euo pipefail

# Minimal MySQL CDC validation using Debezium (Kafka Connect).
#
# What it does:
# - Starts: kafka, kafka-connect, mysql-e2e
# - Creates a Debezium MySQL connector for table e2e_db.cdc_test
# - Inserts a row into MySQL
# - Verifies at least one CDC event is produced to the expected Kafka topic
# - Cleans up the connector

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CORE_COMPOSE_FILES=(
  "${ROOT_DIR}/docker-compose.yml"
  "${ROOT_DIR}/docker-compose.e2e.yml"
  "${ROOT_DIR}/shared/internal/infra/kafka-connect/docker-compose.kafka-connect.yml"
)

core() {
  docker compose \
    -p rsync-ai \
    -f "${CORE_COMPOSE_FILES[0]}" \
    -f "${CORE_COMPOSE_FILES[1]}" \
    -f "${CORE_COMPOSE_FILES[2]}" \
    "$@"
}

e2e() {
  docker compose \
    -p rsync-ai-e2e \
    -f "${ROOT_DIR}/docker-compose.e2e.dbs.yml" \
    "$@"
}

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
MYSQL_SERVICE="mysql-e2e"
MYSQL_ROOT_PW="rootpassword"
DB="e2e_db"
TABLE="cdc_test"

CONNECTOR_NAME="e2e-mysql-cdc-$(date +%s)"
TOPIC="${CONNECTOR_NAME}.${DB}.${TABLE}"

cleanup() {
  echo ""
  echo "🧹 Cleanup: deleting connector ${CONNECTOR_NAME} (best-effort)"
  curl -sS -X DELETE "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=================================================="
echo "🧪 MySQL CDC Test (Debezium via Kafka Connect)"
echo "=================================================="
echo "Connector: ${CONNECTOR_NAME}"
echo "Topic:     ${TOPIC}"
echo "Connect:   ${CONNECT_URL}"
echo ""

echo "🚀 Starting required services..."
# Build kafka-connect explicitly so plugin install failures are surfaced early.
core build kafka-connect
core up -d kafka kafka-connect
e2e up -d "${MYSQL_SERVICE}"

echo "⏳ Waiting for MySQL..."
for i in {1..60}; do
  if e2e exec -T "${MYSQL_SERVICE}" mysqladmin ping -h 127.0.0.1 -uroot -p"${MYSQL_ROOT_PW}" --silent >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! e2e exec -T "${MYSQL_SERVICE}" mysqladmin ping -h 127.0.0.1 -uroot -p"${MYSQL_ROOT_PW}" --silent >/dev/null 2>&1; then
  echo "❌ MySQL did not become healthy in time"
  exit 1
fi
echo "✅ MySQL is healthy"

echo "⏳ Waiting for Kafka Connect..."
for i in {1..60}; do
  if curl -sS "${CONNECT_URL}/" | grep -q "version"; then
    break
  fi
  sleep 2
done

if ! curl -sS "${CONNECT_URL}/" | grep -q "version"; then
  echo "❌ Kafka Connect did not become healthy at ${CONNECT_URL}"
  exit 1
fi

echo "✅ Kafka Connect is healthy"

echo "🔎 Verifying Debezium MySQL plugin is installed..."
if ! curl -sS "${CONNECT_URL}/connector-plugins" | grep -q "io.debezium.connector.mysql.MySqlConnector"; then
  echo "❌ Debezium MySQL connector plugin not found in Kafka Connect."
  echo "   Check the kafka-connect image build logs and plugin path."
  curl -sS "${CONNECT_URL}/connector-plugins" || true
  exit 1
fi
echo "✅ Debezium MySQL plugin is available"

echo "🧱 Ensuring Kafka topic exists (auto-create may be disabled)..."
# Debezium will fail to produce if the topic doesn't exist (UNKNOWN_TOPIC_OR_PARTITION).
core exec -T kafka bash -lc "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic '${TOPIC}' --partitions 1 --replication-factor 1" >/dev/null
echo "✅ Topic ready: ${TOPIC}"

echo "🛠️  Creating test table and signaling table (if needed)..."
e2e exec -T "${MYSQL_SERVICE}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
  CREATE DATABASE IF NOT EXISTS ${DB};
  -- Make this test idempotent across runs:
  -- other e2e scripts may create ${DB}.${TABLE} with a different schema (e.g., id without AUTO_INCREMENT),
  -- which would cause inserts to fail with: 'Field \"id\" doesn't have a default value'.
  DROP TABLE IF EXISTS ${DB}.${TABLE};
  CREATE TABLE ${DB}.${TABLE} (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  
  -- Signaling table for incremental snapshots
  CREATE TABLE IF NOT EXISTS ${DB}.debezium_signal (
    id VARCHAR(64) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data TEXT
  );
  
  -- Grant permissions for signaling table
  GRANT SELECT, INSERT, UPDATE, DELETE ON ${DB}.debezium_signal TO 'root'@'%';
"

# Verify signaling table created successfully
if ! e2e exec -T "${MYSQL_SERVICE}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "SELECT COUNT(*) FROM ${DB}.debezium_signal" >/dev/null 2>&1; then
  echo "❌ Failed to create signaling table"
  exit 1
fi
echo "✅ Signaling table created"

echo "🔌 Creating Debezium connector..."
CREATE_PAYLOAD="$(cat <<JSON
{
  "name": "${CONNECTOR_NAME}",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",

    "database.hostname": "${MYSQL_SERVICE}",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "${MYSQL_ROOT_PW}",
    "database.server.id": "184055",

    "topic.prefix": "${CONNECTOR_NAME}",
    "database.include.list": "${DB}",
    "table.include.list": "${DB}.${TABLE}",

    "snapshot.mode": "initial",
    "include.schema.changes": "false",

    "database.allowPublicKeyRetrieval": "true",
    "database.ssl.mode": "disabled",

    "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
    "schema.history.internal.kafka.topic": "schemahistory.${CONNECTOR_NAME}",

    "signal.data.collection": "${DB}.debezium_signal",
    "signal.enabled.channels": "source",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
JSON
)"

CREATE_RESP="$(curl -sS -w $'\n%{http_code}' -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d "${CREATE_PAYLOAD}" || true)"
CREATE_BODY="$(echo "${CREATE_RESP}" | sed '$d')"
CREATE_CODE="$(echo "${CREATE_RESP}" | tail -n 1)"
if [[ "${CREATE_CODE}" != "200" && "${CREATE_CODE}" != "201" && "${CREATE_CODE}" != "409" ]]; then
  echo "❌ Failed to create connector (HTTP ${CREATE_CODE})"
  echo "${CREATE_BODY}" | head -n 200
  exit 1
fi
if [[ "${CREATE_CODE}" == "409" ]]; then
  echo "ℹ️  Connector already exists (409). Continuing."
fi

echo "⏳ Waiting for connector to be RUNNING..."
for i in {1..30}; do
  status_json="$(curl -sS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || true)"
  if echo "${status_json}" | tr -d '\n' | grep -q '"connector":{"state":"RUNNING"'; then
    state="RUNNING"
    break
  fi
  sleep 2
done

# Check 1: Connector status
status_json="$(curl -sS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || true)"
connector_state="$(echo "${status_json}" | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")"
if [[ "${connector_state}" != "RUNNING" ]]; then
  echo "❌ Connector not RUNNING: ${connector_state}"
  echo "Connector status:"
  echo "${status_json}" | jq . 2>/dev/null || echo "${status_json}"
  echo ""
  echo "Kafka Connect logs (last 100 lines):"
  core logs kafka-connect --tail 100
  exit 1
fi
echo "✅ Connector is RUNNING"

# Check 2: Task status (tasks may appear a bit after connector reports RUNNING)
echo "⏳ Waiting for task 0 to be RUNNING..."
task_state=""
for i in {1..60}; do
  status_json="$(curl -sS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || true)"
  task_state="$(echo "${status_json}" | jq -r '.tasks[0].state // empty' 2>/dev/null || true)"

  if [[ "${task_state}" == "RUNNING" ]]; then
    break
  fi

  if [[ "${task_state}" == "FAILED" ]]; then
    echo "❌ Task 0 FAILED"
    echo "Full status:"
    echo "${status_json}" | jq . 2>/dev/null || echo "${status_json}"
    echo ""
    echo "Kafka Connect logs (last 200 lines):"
    core logs kafka-connect --tail 200
    exit 1
  fi

  sleep 2
done

status_json="$(curl -sS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || true)"
task_state="$(echo "${status_json}" | jq -r '.tasks[0].state // empty' 2>/dev/null || true)"
if [[ "${task_state}" != "RUNNING" ]]; then
  echo "❌ Task 0 did not reach RUNNING (state=${task_state:-missing})"
  echo "Full status:"
  echo "${status_json}" | jq . 2>/dev/null || echo "${status_json}"
  echo ""
  echo "Kafka Connect logs (last 200 lines):"
  core logs kafka-connect --tail 200
  exit 1
fi
echo "✅ Task 0 is RUNNING"

echo "✍️  Inserting a row into ${DB}.${TABLE}..."
ROW_NAME="hello-$(date +%s)"
e2e exec -T "${MYSQL_SERVICE}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
  INSERT INTO ${DB}.${TABLE}(name) VALUES ('${ROW_NAME}');
"

echo "👂 Consuming CDC events from Kafka topic ${TOPIC} (timeout: 30s)..."
# Run the consumer inside the Kafka service container with timeout.
# (Do the timeout inside the Linux container so this script stays portable on macOS.)
set +e
CDC_MSG="$(core exec -T kafka bash -lc "timeout 30 kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic '${TOPIC}' \
  --from-beginning \
  --max-messages 50" 2>/dev/null)"
CONSUME_EXIT_CODE=$?
set -e

if [[ -z "${CDC_MSG}" ]]; then
  echo "❌ Did not receive any CDC message on topic ${TOPIC}"
  echo ""
  echo "Connector status:"
  curl -sS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | jq . 2>/dev/null || true
  echo ""
  echo "Kafka Connect logs (last 100 lines):"
  core logs kafka-connect --tail 100
  exit 1
fi

echo "✅ Received CDC message(s):"
echo "${CDC_MSG}" | head -n 10

# If the consumer timed out, that's OK *as long as* we actually received messages
# (max-messages might be higher than what's currently available).
if [[ ${CONSUME_EXIT_CODE} -eq 124 ]]; then
  echo "ℹ️  Consumer hit timeout after receiving some output (acceptable for this test)."
fi

# Validate JSON structure (required fields)
REQUIRED_FIELDS=(".after.id" ".after.name" ".source.table" ".op")
echo ""
echo "🔍 Validating JSON structure..."
for field in "${REQUIRED_FIELDS[@]}"; do
  if ! echo "${CDC_MSG}" | jq -e "${field}" >/dev/null 2>&1; then
    echo "❌ CDC event missing field: ${field}"
    echo "Event content:"
    echo "${CDC_MSG}" | jq . 2>/dev/null || echo "${CDC_MSG}"
    exit 1
  fi
done
echo "✅ JSON structure validated (all required fields present)"

if echo "${CDC_MSG}" | grep -q "${ROW_NAME}"; then
  echo "✅ Found inserted row value (${ROW_NAME}) in CDC stream"
else
  echo "❌ Did not find inserted row value (${ROW_NAME}) in the first 20 messages."
  echo "Connector status:"
  curl -sS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" || true
  exit 1
fi

echo ""
echo "🎉 PASS: MySQL CDC is producing events via Debezium to Kafka"


