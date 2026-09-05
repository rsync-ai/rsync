#!/usr/bin/env bash
set -euo pipefail

# DB (MySQL / PostgreSQL) Debezium CDC -> Kafka -> kafka-mcp-sink -> aws-s3
# connector -> a LOCAL external MinIO (NOT real AWS, NOT the internal claim-check
# MinIO). This is the local-only counterpart to test_mysql_cdc_to_real_s3.sh: it
# proves the db -> object-storage CDC DESTINATION path end-to-end without needing
# real AWS credentials, by pointing the aws-s3 connection's endpoint_url at a
# throwaway MinIO on the MCP network.
#
# It is the first green proof that a CDC change event (INSERT/UPDATE/DELETE)
# actually lands in an object store as a "bronze" jsonl object.
#
# Usage:   bash e2e/test_db_cdc_to_local_minio.sh [mysql|postgres|both]   (default: both)
#
# Prereqs (the running staging/e2e stack):
#   - kafka (rsync-ai-kafka), kafka-connect (REST on :8083), debezium plugins
#   - kafka-mcp-sink MCP container (rsync-ai-kafka-mcp-sink-v<ver>-mcp)
#   - aws-s3 MCP container (rsync-ai-aws-s3-v1-0-0-mcp) on network rsync-ai-mcp
#   - source DBs from docker-compose.e2e.dbs.yml (mysql-e2e / postgres-e2e)
#
# THREE GOTCHAS this harness encodes (learned the hard way):
#   1. sink_mode=cdc is MANDATORY. The sink only AUTO-detects CDC for
#      schemas-ENABLED Debezium envelopes (inner "payload"). With
#      value.converter.schemas.enable=false (flat envelope), auto-detect misses
#      it and every event is mis-parsed as a batch row -> DLQ "missing table".
#      We pass schemas.enable=false (smaller) AND sink_mode=cdc explicitly.
#   2. Connector name must NOT start with "cdc-". The orchestrator CDC reconciler
#      (backend-orchestrator/internal/workers/cdc_reconciler.go) deletes any
#      "cdc-*" connector with no live pipeline row within ~1 sweep. We prefix
#      "debug-" so the reconciler ignores our ad-hoc connector.
#   3. endpoint_url must point at a NON-internal MinIO. The aws-s3 connector's
#      INV-1 guard (rsync_protocol/storage_safety.py) rejects the internal
#      "minio"/"rsync-ai-minio" host. We stand up a separate "cdc-dest-minio".

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

ENGINE="${1:-both}"

MCP_NET="${MCP_NET:-rsync-ai-mcp}"
CORE_NET="${CORE_NET:-rsync-ai_default}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"

DEST_MINIO="${DEST_MINIO:-cdc-dest-minio}"
BUCKET="${BUCKET:-cdc-dest}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASS="${MINIO_PASS:-minioadmin}"

DEST_CONNECTOR="aws-s3"
DEST_VERSION="${DEST_VERSION:-v1.0.0}"

MYSQL_CONTAINER="${MYSQL_CONTAINER:-rsync-ai-mysql-e2e}"
MYSQL_ROOT_PW="${MYSQL_ROOT_PW:-rootpassword}"
PG_CONTAINER="${PG_CONTAINER:-rsync-ai-postgres-e2e}"
PG_USER="${PG_USER:-e2e_user}"
PG_PASS="${PG_PASS:-e2e_password}"
PG_DB="${PG_DB:-e2e_db}"

RUN="$(date +%s)"
SINK_CONTAINER=""

log() { printf '%s\n' "$*"; }
die() { printf '❌ %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }
need docker; need curl; need jq

resolve_sink() {
  SINK_CONTAINER="$(docker ps --format '{{.Names}}' | grep -E 'kafka-mcp-sink.*-mcp$' | head -1)"
  [ -n "${SINK_CONTAINER}" ] || die "kafka-mcp-sink container not running"
}

# mc helper: runs mc commands against the external MinIO.
mc() {
  docker run --rm --network "${MCP_NET}" --entrypoint sh minio/mc:latest -c \
    "mc alias set d http://${DEST_MINIO}:9000 ${MINIO_USER} ${MINIO_PASS} >/dev/null 2>&1; $1"
}

sink_rpc() {
  docker run --rm --network "${CORE_NET}" curlimages/curl:8.7.1 -sS \
    -H 'Content-Type: application/json' -d "$1" "http://${SINK_CONTAINER}:8000/mcp"
}

STARTED_GROUPS=()
CREATED_CONNECTORS=()
PG_SLOTS=()
PG_PUBS=()

cleanup() {
  log ""
  log "🧹 Cleanup (best-effort)..."
  for g in "${STARTED_GROUPS[@]:-}"; do
    [ -n "$g" ] || continue
    sink_rpc "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"kafka-mcp-sink_stop_sink\",\"arguments\":{\"config\":{\"consumer_group\":\"${g}\"}}}}" >/dev/null 2>&1 || true
  done
  for c in "${CREATED_CONNECTORS[@]:-}"; do
    [ -n "$c" ] || continue
    curl -sS -X DELETE "${CONNECT_URL}/connectors/${c}" >/dev/null 2>&1 || true
  done
  for s in "${PG_SLOTS[@]:-}"; do
    [ -n "$s" ] || continue
    docker exec -e PGPASSWORD="${PG_PASS}" "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" \
      -c "SELECT pg_drop_replication_slot('${s}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='${s}');" >/dev/null 2>&1 || true
  done
  for p in "${PG_PUBS[@]:-}"; do
    [ -n "$p" ] || continue
    docker exec -e PGPASSWORD="${PG_PASS}" "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" \
      -c "DROP PUBLICATION IF EXISTS ${p};" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

setup_minio() {
  if ! docker ps --format '{{.Names}}' | grep -qx "${DEST_MINIO}"; then
    docker rm -f "${DEST_MINIO}" >/dev/null 2>&1 || true
    docker run -d --name "${DEST_MINIO}" --network "${MCP_NET}" \
      -e "MINIO_ROOT_USER=${MINIO_USER}" -e "MINIO_ROOT_PASSWORD=${MINIO_PASS}" \
      minio/minio:latest server /data >/dev/null
  fi
  local ok=""
  for _ in $(seq 1 30); do
    if mc "mc mb -p d/${BUCKET} >/dev/null 2>&1; mc ls d >/dev/null 2>&1"; then ok=1; break; fi
    sleep 1
  done
  [ -n "${ok}" ] || die "external MinIO ${DEST_MINIO} did not become ready"
  log "✅ external MinIO '${DEST_MINIO}' ready (bucket ${BUCKET})"
}

wait_running() {
  local name="$1"
  for i in $(seq 1 60); do
    local st c t
    st="$(curl -sS "${CONNECT_URL}/connectors/${name}/status" 2>/dev/null || true)"
    c="$(echo "${st}" | jq -r '.connector.state // empty' 2>/dev/null || true)"
    t="$(echo "${st}" | jq -r '.tasks[0].state // empty' 2>/dev/null || true)"
    [ "${c}" = "RUNNING" ] && [ "${t}" = "RUNNING" ] && return 0
    if [ "${c}" = "FAILED" ] || [ "${t}" = "FAILED" ]; then
      echo "${st}" | jq '.tasks' >&2 || true; return 1
    fi
    sleep 2
  done
  return 1
}

start_sink() {
  local pipeline_id="$1" topic="$2" group="$3" prefix="$4"
  STARTED_GROUPS+=("${group}")
  local req
  req="$(cat <<JSON
{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"kafka-mcp-sink_start_sink","arguments":{"config":{
  "pipeline_id":"${pipeline_id}","sink_mode":"cdc","topics":"${topic}","consumer_group":"${group}",
  "kafka_bootstrap_servers":"${KAFKA_BOOTSTRAP}",
  "destination_connector":"${DEST_CONNECTOR}","destination_version":"${DEST_VERSION}",
  "destination_config":{"bucket":"${BUCKET}","endpoint_url":"http://${DEST_MINIO}:9000",
    "access_key_id":"${MINIO_USER}","secret_access_key":"${MINIO_PASS}","region":"us-east-1",
    "path_prefix":"${prefix}","table":"cdc_events"}}}}}
JSON
)"
  local resp; resp="$(sink_rpc "${req}")"
  echo "${resp}" | jq -e '.result.status=="started"' >/dev/null 2>&1 \
    || die "start_sink failed: $(echo "${resp}" | jq -c '.error // .result')"
}

# Assert the bronze object(s) contain I/U/D for the given id.
verify_bronze() {
  local topic="$1" prefix="$2" id="$3"
  # CDC bronze lands under <prefix>/<schema>/<table>/<YYYY-MM-DD>/<timestamp>-<offset>.jsonl
  # (DMS-style layout). Search the whole prefix recursively — mc find locates the jsonl
  # regardless of the intermediate schema/table/date path.
  local pfx="d/${BUCKET}/${prefix}/"
  local content=""
  for _ in $(seq 1 45); do
    content="$(mc "for k in \$(mc find ${pfx} --name '*.jsonl' 2>/dev/null); do mc cat \"\$k\" 2>/dev/null; done")"
    if echo "${content}" | grep -q "insert-${id}"; then break; fi
    sleep 2
  done
  log "----- bronze rows for id=${id} under ${pfx} -----"
  echo "${content}" | grep -E "\"id\": ${id}|\"pk\": \{\"id\": ${id}\}" || true
  echo "${content}" | grep -q "insert-${id}" || die "INSERT (op I) for id=${id} NOT found in bronze"
  echo "${content}" | grep -q "update-${id}" || die "UPDATE (op U) for id=${id} NOT found in bronze"
  echo "${content}" | grep -E "\"op\": \"D\"" | grep -q "${id}" || die "DELETE (op D) for id=${id} NOT found in bronze"
  log "✅ I/U/D all present for id=${id}"
}

run_mysql() {
  log ""; log "=== MySQL CDC -> aws-s3 -> ${DEST_MINIO} ==="
  local conn="debug-mysql-minio-${RUN}"
  local db="e2e_db" tbl="cdc_test" id="17${RUN: -7}"
  local topic="${conn}.${db}.${tbl}" group="sink-${conn}" prefix="cdc-min-mysql-${RUN}"
  local sid=$(( (RUN % 100000) + 190000 ))

  docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
    CREATE DATABASE IF NOT EXISTS ${db};
    CREATE TABLE IF NOT EXISTS ${db}.${tbl} (id BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
    DELETE FROM ${db}.${tbl} WHERE id=${id};" 2>/dev/null

  CREATED_CONNECTORS+=("${conn}")
  local payload
  payload="$(cat <<JSON
{"name":"${conn}","config":{
  "connector.class":"io.debezium.connector.mysql.MySqlConnector","tasks.max":"1",
  "database.hostname":"mysql-e2e","database.port":"3306","database.user":"root","database.password":"${MYSQL_ROOT_PW}",
  "database.server.id":"${sid}","topic.prefix":"${conn}","database.include.list":"${db}","table.include.list":"${db}.${tbl}",
  "snapshot.mode":"never","include.schema.changes":"false","database.allowPublicKeyRetrieval":"true","database.ssl.mode":"disabled",
  "schema.history.internal.kafka.bootstrap.servers":"${KAFKA_BOOTSTRAP}","schema.history.internal.kafka.topic":"schemahistory.${conn}",
  "key.converter":"org.apache.kafka.connect.json.JsonConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable":"false","value.converter.schemas.enable":"false"}}
JSON
)"
  local code; code="$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${CONNECT_URL}/connectors" -H 'Content-Type: application/json' -d "${payload}")"
  case "${code}" in 200|201|409) ;; *) die "MySQL connector create HTTP ${code}";; esac
  wait_running "${conn}" || die "MySQL Debezium did not reach RUNNING"
  log "✅ Debezium MySQL RUNNING (topic ${topic})"

  start_sink "${conn}" "${topic}" "${group}" "${prefix}"
  log "✅ sink started (sink_mode=cdc, group ${group})"
  sleep 4

  docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "INSERT INTO ${db}.${tbl}(id,name) VALUES (${id},'insert-${id}');" 2>/dev/null
  sleep 1
  docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "UPDATE ${db}.${tbl} SET name='update-${id}' WHERE id=${id};" 2>/dev/null
  sleep 1
  docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "DELETE FROM ${db}.${tbl} WHERE id=${id};" 2>/dev/null
  log "✍️  INSERT/UPDATE/DELETE id=${id} applied"

  verify_bronze "${topic}" "${prefix}" "${id}"
  log "🎉 MySQL -> object-storage CDC PASS"
}

run_postgres() {
  log ""; log "=== PostgreSQL CDC -> aws-s3 -> ${DEST_MINIO} ==="
  local conn="debug-pg-minio-${RUN}"
  local tbl="cdc_minio_pg" id="27${RUN: -7}"
  local topic="${conn}.public.${tbl}" group="sink-${conn}" prefix="cdc-min-pg-${RUN}"
  local slot="debug_pg_minio_${RUN}_slot" pub="debug_pg_minio_${RUN}_pub"
  PSQL() { docker exec -e PGPASSWORD="${PG_PASS}" "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -v ON_ERROR_STOP=1 "$@"; }

  PSQL -c "CREATE TABLE IF NOT EXISTS public.${tbl} (id BIGINT PRIMARY KEY, name TEXT NOT NULL);" >/dev/null
  PSQL -c "ALTER TABLE public.${tbl} REPLICA IDENTITY FULL;" >/dev/null
  PSQL -c "DELETE FROM public.${tbl} WHERE id=${id};" >/dev/null 2>&1 || true
  # Publication BEFORE slot (project rule). Debezium creates the slot (autocreate disabled).
  PSQL -c "DROP PUBLICATION IF EXISTS ${pub};" >/dev/null 2>&1 || true
  PG_PUBS+=("${pub}"); PG_SLOTS+=("${slot}")
  PSQL -c "CREATE PUBLICATION ${pub} FOR TABLE public.${tbl};" >/dev/null
  log "✅ publication ${pub} created (before slot)"

  CREATED_CONNECTORS+=("${conn}")
  local payload
  payload="$(cat <<JSON
{"name":"${conn}","config":{
  "connector.class":"io.debezium.connector.postgresql.PostgresConnector","tasks.max":"1",
  "database.hostname":"postgres-e2e","database.port":"5432","database.user":"${PG_USER}","database.password":"${PG_PASS}","database.dbname":"${PG_DB}",
  "topic.prefix":"${conn}","table.include.list":"public.${tbl}",
  "plugin.name":"pgoutput","slot.name":"${slot}","publication.name":"${pub}","publication.autocreate.mode":"disabled","snapshot.mode":"never",
  "key.converter":"org.apache.kafka.connect.json.JsonConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable":"false","value.converter.schemas.enable":"false"}}
JSON
)"
  local code; code="$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${CONNECT_URL}/connectors" -H 'Content-Type: application/json' -d "${payload}")"
  case "${code}" in 200|201|409) ;; *) die "PG connector create HTTP ${code}";; esac
  wait_running "${conn}" || die "PG Debezium did not reach RUNNING"
  log "✅ Debezium PostgreSQL RUNNING (topic ${topic})"

  start_sink "${conn}" "${topic}" "${group}" "${prefix}"
  log "✅ sink started (sink_mode=cdc, group ${group})"
  sleep 4

  PSQL -c "INSERT INTO public.${tbl}(id,name) VALUES (${id},'insert-${id}');" >/dev/null
  sleep 1
  PSQL -c "UPDATE public.${tbl} SET name='update-${id}' WHERE id=${id};" >/dev/null
  sleep 1
  PSQL -c "DELETE FROM public.${tbl} WHERE id=${id};" >/dev/null
  log "✍️  INSERT/UPDATE/DELETE id=${id} applied"

  verify_bronze "${topic}" "${prefix}" "${id}"
  log "🎉 PostgreSQL -> object-storage CDC PASS"
}

log "=================================================="
log "🧪 DB CDC -> kafka-mcp-sink -> aws-s3 -> local MinIO"
log "   engine=${ENGINE}  run=${RUN}"
log "=================================================="
resolve_sink
setup_minio
docker compose -p rsync-ai-e2e -f "${ROOT_DIR}/docker-compose.e2e.dbs.yml" up -d mysql-e2e postgres-e2e >/dev/null 2>&1 || true

case "${ENGINE}" in
  mysql)    run_mysql ;;
  postgres) run_postgres ;;
  both)     run_mysql; run_postgres ;;
  *) die "unknown engine '${ENGINE}' (use: mysql|postgres|both)" ;;
esac

log ""
log "✅✅ ALL CDC -> object-storage checks PASSED (engine=${ENGINE})"
