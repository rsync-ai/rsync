#!/usr/bin/env bash
set -euo pipefail

# MySQL Debezium CDC -> Kafka -> kafka-mcp-sink -> {gcs | azure-blob} connector
# -> a LOCAL emulator (fake-gcs-server / Azurite).
#
# Phase 4b sibling of test_db_cdc_to_local_minio.sh (#309, which proved the same
# path for aws-s3 against a local MinIO). This extends the DB -> object-storage
# CDC DESTINATION proof to the other two object stores, exercising the SAME
# generic sink path: cdcObjectBatcher -> <dest>_import_data -> bronze .jsonl.
# The sink is provider-agnostic (it just calls "<connector>_import_data"); the
# only per-provider difference is bucket-vs-container and the connector's write
# primitive — so a green run here closes the "only aws-s3 is a proven dest" gap.
#
# Usage:   bash e2e/test_db_cdc_to_emulated_gcs_azure.sh [gcs|azure|both]   (default: both)
#
# Prereqs (the running staging/e2e stack):
#   - kafka (rsync-ai-kafka), kafka-connect (REST :8083), debezium plugins
#   - kafka-mcp-sink MCP container (rsync-ai-kafka-mcp-sink-v<ver>-mcp)
#   - gcs + azure-blob MCP containers (rsync-ai-gcs-v1-0-0-mcp / rsync-ai-azure-blob-v1-0-0-mcp)
#     on network rsync-ai-mcp (they also carry the SDKs we use to read bronze back)
#   - source DB from docker-compose.e2e.dbs.yml (mysql-e2e)
#
# Same THREE gotchas as #309 apply: sink_mode=cdc mandatory (flat Debezium),
# connector name must NOT start "cdc-", endpoint_url must be a NON-internal store
# (here a throwaway emulator on the MCP network — never the internal MinIO).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PROVIDER="${1:-both}"

MCP_NET="${MCP_NET:-rsync-ai-mcp}"
CORE_NET="${CORE_NET:-rsync-ai_default}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"

GCS_EMU="${GCS_EMU:-cdc-fake-gcs}"
GCS_MCP="${GCS_MCP:-rsync-ai-gcs-v1-0-0-mcp}"
AZ_EMU="${AZ_EMU:-cdc-azurite}"
AZ_MCP="${AZ_MCP:-rsync-ai-azure-blob-v1-0-0-mcp}"
# Standard, well-known Azurite devstoreaccount1 key (the image's default differs,
# so we force it via AZURITE_ACCOUNTS to get a deterministic shared key).
AZ_ACCT="${AZ_ACCT:-devstoreaccount1}"
AZ_KEY="${AZ_KEY:-Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==}"

BUCKET="${BUCKET:-cdc-dest}"
DEST_VERSION="${DEST_VERSION:-v1.0.0}"

MYSQL_CONTAINER="${MYSQL_CONTAINER:-rsync-ai-mysql-e2e}"
MYSQL_ROOT_PW="${MYSQL_ROOT_PW:-rootpassword}"

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

wait_mysql() {
  for _ in $(seq 1 60); do
    if docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e 'SELECT 1' >/dev/null 2>&1; then
      log "✅ mysql-e2e ready"; return 0
    fi
    sleep 2
  done
  die "mysql-e2e did not become ready (is docker-compose.e2e.dbs.yml mysql-e2e healthy?)"
}

sink_rpc() {
  docker run --rm --network "${CORE_NET}" curlimages/curl:8.7.1 -sS \
    -H 'Content-Type: application/json' -d "$1" "http://${SINK_CONTAINER}:8000/mcp"
}

STARTED_GROUPS=()
CREATED_CONNECTORS=()
STARTED_EMULATORS=()

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
  for e in "${STARTED_EMULATORS[@]:-}"; do
    [ -n "$e" ] || continue
    docker rm -f "$e" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

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

# ---- emulator setup + readback (reuse each MCP container's own SDK) ----------

setup_gcs() {
  docker rm -f "${GCS_EMU}" >/dev/null 2>&1 || true
  docker run -d --name "${GCS_EMU}" --network "${MCP_NET}" \
    fsouza/fake-gcs-server:latest -scheme http -port 4443 -external-url "http://${GCS_EMU}:4443" >/dev/null
  STARTED_EMULATORS+=("${GCS_EMU}")
  local ok=""
  for _ in $(seq 1 30); do
    if docker exec "${GCS_MCP}" python3 -c '
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials
c=storage.Client(project="emulator",credentials=AnonymousCredentials(),client_options={"api_endpoint":"http://'"${GCS_EMU}"':4443"})
try:
    c.create_bucket("'"${BUCKET}"'")
except Exception as e:
    # already-exists is fine; anything else re-raises to fail the readiness probe
    if "exist" not in str(e).lower(): raise
print("ok")
' >/dev/null 2>&1; then ok=1; break; fi
    sleep 1
  done
  [ -n "${ok}" ] || die "fake-gcs-server ${GCS_EMU} did not become ready / bucket create failed"
  log "✅ fake-gcs-server '${GCS_EMU}' ready (bucket ${BUCKET})"
}

read_gcs() {  # $1=prefix -> bronze content on stdout
  docker exec -e PFX="$1" "${GCS_MCP}" python3 -c '
import os
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials
c=storage.Client(project="emulator",credentials=AnonymousCredentials(),client_options={"api_endpoint":"http://'"${GCS_EMU}"':4443"})
pfx=os.environ["PFX"]
print("".join(b.download_as_text() for b in c.list_blobs("'"${BUCKET}"'",prefix=pfx+"/")))
' 2>/dev/null || true
}

setup_azure() {
  docker rm -f "${AZ_EMU}" >/dev/null 2>&1 || true
  docker run -d --name "${AZ_EMU}" --network "${MCP_NET}" \
    -e "AZURITE_ACCOUNTS=${AZ_ACCT}:${AZ_KEY}" \
    mcr.microsoft.com/azure-storage/azurite \
    azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --skipApiVersionCheck --disableProductStyleUrl >/dev/null
  STARTED_EMULATORS+=("${AZ_EMU}")
  local ok=""
  for _ in $(seq 1 30); do
    if docker exec -e AK="${AZ_KEY}" "${AZ_MCP}" python3 -c '
import os
from azure.storage.blob import BlobServiceClient
conn="DefaultEndpointsProtocol=http;AccountName='"${AZ_ACCT}"';AccountKey=%s;BlobEndpoint=http://'"${AZ_EMU}"':10000/'"${AZ_ACCT}"';"%os.environ["AK"]
svc=BlobServiceClient.from_connection_string(conn)
try:
    svc.create_container("'"${BUCKET}"'")
except Exception as e:
    if "exist" not in str(e).lower(): raise
print("ok")
' >/dev/null 2>&1; then ok=1; break; fi
    sleep 1
  done
  [ -n "${ok}" ] || die "Azurite ${AZ_EMU} did not become ready / container create failed"
  log "✅ Azurite '${AZ_EMU}' ready (container ${BUCKET})"
}

read_azure() {  # $1=prefix -> bronze content on stdout
  docker exec -e PFX="$1" -e AK="${AZ_KEY}" "${AZ_MCP}" python3 -c '
import os
from azure.storage.blob import BlobServiceClient
conn="DefaultEndpointsProtocol=http;AccountName='"${AZ_ACCT}"';AccountKey=%s;BlobEndpoint=http://'"${AZ_EMU}"':10000/'"${AZ_ACCT}"';"%os.environ["AK"]
cc=BlobServiceClient.from_connection_string(conn).get_container_client("'"${BUCKET}"'")
pfx=os.environ["PFX"]
print("".join(cc.get_blob_client(b.name).download_blob().readall().decode() for b in cc.list_blobs(name_starts_with=pfx+"/")))
' 2>/dev/null || true
}

# destination_config JSON for start_sink, per provider.
dest_config() {  # $1=provider $2=prefix
  case "$1" in
    gcs)   printf '{"bucket":"%s","endpoint_url":"http://%s:4443","path_prefix":"%s","file_format":"jsonl","table":"cdc_events"}' "${BUCKET}" "${GCS_EMU}" "$2" ;;
    azure) printf '{"container":"%s","bucket":"%s","account_name":"%s","account_key":"%s","endpoint_url":"http://%s:10000/%s","path_prefix":"%s","file_format":"jsonl","table":"cdc_events"}' "${BUCKET}" "${BUCKET}" "${AZ_ACCT}" "${AZ_KEY}" "${AZ_EMU}" "${AZ_ACCT}" "$2" ;;
  esac
}

start_sink() {  # $1=provider $2=pipeline_id $3=topic $4=group $5=prefix
  local provider="$1" pipeline_id="$2" topic="$3" group="$4" prefix="$5"
  local dest_connector="$provider"; [ "$provider" = "azure" ] && dest_connector="azure-blob"
  STARTED_GROUPS+=("${group}")
  local req
  req="$(cat <<JSON
{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"kafka-mcp-sink_start_sink","arguments":{"config":{
  "pipeline_id":"${pipeline_id}","sink_mode":"cdc","topics":"${topic}","consumer_group":"${group}",
  "kafka_bootstrap_servers":"${KAFKA_BOOTSTRAP}",
  "destination_connector":"${dest_connector}","destination_version":"${DEST_VERSION}",
  "destination_config":$(dest_config "${provider}" "${prefix}")}}}}
JSON
)"
  local resp; resp="$(sink_rpc "${req}")"
  echo "${resp}" | jq -e '.result.status=="started"' >/dev/null 2>&1 \
    || die "start_sink failed: $(echo "${resp}" | jq -c '.error // .result')"
}

verify_bronze() {  # $1=provider $2=prefix $3=id
  local provider="$1" prefix="$2" id="$3" content=""
  for _ in $(seq 1 45); do
    if [ "$provider" = "gcs" ]; then content="$(read_gcs "${prefix}")"; else content="$(read_azure "${prefix}")"; fi
    if echo "${content}" | grep -q "insert-${id}"; then break; fi
    sleep 2
  done
  log "----- bronze rows for id=${id} (prefix ${prefix}) -----"
  echo "${content}" | grep -E "\"id\": ${id}|\"pk\": \{\"id\": ${id}\}|insert-${id}|update-${id}" || true
  echo "${content}" | grep -q "insert-${id}"                      || die "INSERT (op I) for id=${id} NOT found in bronze"
  echo "${content}" | grep -q "update-${id}"                      || die "UPDATE (op U) for id=${id} NOT found in bronze"
  echo "${content}" | grep -E "\"op\": \"D\"" | grep -q "${id}"   || die "DELETE (op D) for id=${id} NOT found in bronze"
  log "✅ I/U/D all present for id=${id}"
}

run_provider() {  # $1=gcs|azure
  local provider="$1"
  local dest_connector="$provider"; [ "$provider" = "azure" ] && dest_connector="azure-blob"
  log ""; log "=== MySQL CDC -> ${dest_connector} -> emulator ==="
  case "$provider" in
    gcs)   setup_gcs ;;
    azure) setup_azure ;;
  esac

  local conn="debug-${provider}-cdc-${RUN}"
  local db="e2e_db" tbl="cdc_${provider}" id="3${RUN: -7}"
  local topic="${conn}.${db}.${tbl}" group="sink-${conn}" prefix="cdc-${provider}-${RUN}"
  # Distinct Debezium MySQL server-id per provider (gcs=3, azure=5 chars) to avoid collisions.
  local sid=$(( (RUN % 100000) + 290000 + ${#provider} ))

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
  case "${code}" in 200|201|409) ;; *) die "${provider} connector create HTTP ${code}";; esac
  wait_running "${conn}" || die "${provider}: Debezium MySQL did not reach RUNNING"
  log "✅ Debezium MySQL RUNNING (topic ${topic})"

  start_sink "${provider}" "${conn}" "${topic}" "${group}" "${prefix}"
  log "✅ sink started (sink_mode=cdc, dest=${dest_connector}, group ${group})"
  sleep 4

  docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "INSERT INTO ${db}.${tbl}(id,name) VALUES (${id},'insert-${id}');" 2>/dev/null
  sleep 1
  docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "UPDATE ${db}.${tbl} SET name='update-${id}' WHERE id=${id};" 2>/dev/null
  sleep 1
  docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "DELETE FROM ${db}.${tbl} WHERE id=${id};" 2>/dev/null
  log "✍️  INSERT/UPDATE/DELETE id=${id} applied"

  verify_bronze "${provider}" "${prefix}" "${id}"
  log "🎉 MySQL -> ${dest_connector} CDC PASS"
}

log "=================================================="
log "🧪 DB CDC -> kafka-mcp-sink -> {gcs|azure-blob} -> emulator"
log "   provider=${PROVIDER}  run=${RUN}"
log "=================================================="
resolve_sink
docker compose -p rsync-ai-e2e -f "${ROOT_DIR}/docker-compose.e2e.dbs.yml" up -d mysql-e2e >/dev/null 2>&1 || true
wait_mysql

case "${PROVIDER}" in
  gcs)   run_provider gcs ;;
  azure) run_provider azure ;;
  both)  run_provider gcs; run_provider azure ;;
  *) die "unknown provider '${PROVIDER}' (use: gcs|azure|both)" ;;
esac

log ""
log "✅✅ ALL CDC -> object-storage checks PASSED (provider=${PROVIDER})"
