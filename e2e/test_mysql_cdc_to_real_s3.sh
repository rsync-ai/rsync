#!/usr/bin/env bash
set -euo pipefail

# MySQL (Debezium CDC) -> Kafka -> kafka-mcp-sink -> AWS S3 (REAL)
#
# This test:
# - Uses mysql-e2e as CDC source (Kafka Connect + Debezium)
# - Uses the destination connection ID from the Connections page (DEST_CONNECTION_ID)
#   and decrypts its config from Postgres using the same ENCRYPTION_KEY as the app.
# - Starts a kafka-mcp-sink worker that consumes the Debezium topic and writes 1 object per CDC event
# - Verifies the object exists in REAL AWS S3 by listing + reading it via boto3
#
# Prereqs:
# - Postgres container running (service name: postgres)
# - Destination connection exists with:
#   - id = DEST_CONNECTION_ID
#   - config.endpoint_url is EMPTY/absent (otherwise it's MinIO, not real AWS)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
DEST_CONNECTION_ID="${DEST_CONNECTION_ID:-a216e4b5-b6e0-4846-a173-11908f19ef51}"

# Must match api-gateway ENCRYPTION_KEY (32+ chars). For local dev this is the default.
ENCRYPTION_KEY="${ENCRYPTION_KEY:-dev-encryption-key-32-bytes-long!!}"

MYSQL_SERVICE="mysql-e2e"
MYSQL_ROOT_PW="rootpassword"
DB="e2e_db"
TABLE="cdc_test"

NETWORK="${DOCKER_NETWORK:-rsync-ai_default}"

log() { printf "%s\n" "$*"; }
die() { printf "❌ %s\n" "$*" >&2; exit 1; }

# Versioned-only runtime container naming:
#   rsync-ai-<id>-v<MAJOR>-<MINOR>-<PATCH>-mcp
resolve_connector_base_dir() {
  local connector_dir="$1"
  python3 - <<PY
import os, sys
base = os.path.join("${ROOT_DIR}", "shared", "mcp-connectors")
target = "${connector_dir}"

# Legacy flat
cand = os.path.join(base, target)
if os.path.isdir(cand) and os.path.exists(os.path.join(cand, "metadata.json")):
  print(cand); sys.exit(0)

# Internal
cand = os.path.join(base, "internal", target)
if os.path.isdir(cand) and os.path.exists(os.path.join(cand, "metadata.json")):
  print(cand); sys.exit(0)

# Public nested: public/<category>/<id>
pub = os.path.join(base, "public")
for dirpath, dirnames, filenames in os.walk(pub):
  if "versions" in dirnames:
    dirnames.remove("versions")
  if os.path.basename(dirpath) == target and "metadata.json" in filenames:
    print(dirpath); sys.exit(0)

print("")
sys.exit(0)
PY
}

resolve_current_version() {
  local connector_dir="$1"
  python3 - <<PY
import json, os, sys
base = os.path.join("${ROOT_DIR}", "shared", "mcp-connectors")
target = "${connector_dir}"
path = os.path.join(base, target, "latest.json")
if not os.path.exists(path):
  # Internal connectors are stored under shared/mcp-connectors/internal/<id>/
  path = os.path.join(base, "internal", target, "latest.json")
if not os.path.exists(path):
  # Public connectors are stored under shared/mcp-connectors/public/<category>/<id>/
  pub = os.path.join(base, "public")
  for dirpath, dirnames, filenames in os.walk(pub):
    if "versions" in dirnames:
      dirnames.remove("versions")
    if os.path.basename(dirpath) == target and "latest.json" in filenames:
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
SINK_CONTAINER="${SINK_CONTAINER:-rsync-ai-kafka-mcp-sink-v${SINK_VERSION_PART}-mcp}"

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

need() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

need curl
need jq
need docker
need python3

log "=================================================="
log "🧪 MySQL CDC → REAL AWS S3 (via kafka-mcp-sink)"
log "=================================================="
log "Connect:     ${CONNECT_URL}"
log "Dest conn:   ${DEST_CONNECTION_ID}"
log ""

# ─────────────────────────────────────────────────────────────
# Step 0: Load + decrypt destination connection config from Postgres
# ─────────────────────────────────────────────────────────────
log "🔎 Loading destination connection from Postgres..."

ROW="$(docker exec postgres psql -U user -d pipeline_db -t -A -c "select connector_type, connector_version, config from connections where id='${DEST_CONNECTION_ID}';" | tr -d '\r')"
[[ -n "${ROW}" ]] || die "Connection id not found in Postgres: ${DEST_CONNECTION_ID}"

DEST_CONNECTOR_TYPE_RAW="$(echo "${ROW}" | cut -d'|' -f1)"
DEST_CONNECTOR_VERSION_RAW="$(echo "${ROW}" | cut -d'|' -f2)"
DEST_CONFIG_ENC="$(echo "${ROW}" | cut -d'|' -f3-)"

# Normalize connector type/version
DEST_CONNECTOR_TYPE="$(echo "${DEST_CONNECTOR_TYPE_RAW}" | tr '[:upper:]' '[:lower:]' | tr '_' '-' | tr ' ' '-' | sed -e 's/--*/-/g' -e 's/^-//' -e 's/-$//')"

# Resolve version: if connection has "latest" or empty, use on-disk latest.json
# If connection has a specific version (e.g. v1.0.2), check if it exists; if not, fall back to latest
if [[ -z "${DEST_CONNECTOR_VERSION_RAW}" || "${DEST_CONNECTOR_VERSION_RAW}" == "latest" ]]; then
  DEST_CONNECTOR_VERSION="$(resolve_current_version "${DEST_CONNECTOR_TYPE}")"
else
  candidate_version="${DEST_CONNECTOR_VERSION_RAW}"
  [[ "${candidate_version}" == v* ]] || candidate_version="v${candidate_version}"
  
  # Check if this version exists on disk
  base_dir="$(resolve_connector_base_dir "${DEST_CONNECTOR_TYPE}")"
  version_dir="${base_dir}/versions/${candidate_version}"
  if [[ ! -d "${version_dir}" ]]; then
    log "⚠️  Connector version ${candidate_version} not found on disk, falling back to latest"
    DEST_CONNECTOR_VERSION="$(resolve_current_version "${DEST_CONNECTOR_TYPE}")"
  else
    DEST_CONNECTOR_VERSION="${candidate_version}"
  fi
fi

if [[ -n "${DEST_CONNECTOR_VERSION}" && "${DEST_CONNECTOR_VERSION}" != v* ]]; then
  DEST_CONNECTOR_VERSION="v${DEST_CONNECTOR_VERSION}"
fi


DEST_CONFIG_JSON="$(
python3 - <<PY
import base64, json, sys
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

enc = """${DEST_CONFIG_ENC}""".strip()
key = """${ENCRYPTION_KEY}""".encode("utf-8")[:32]

raw = base64.b64decode(enc)
nonce, ct = raw[:12], raw[12:]
pt = AESGCM(key).decrypt(nonce, ct, None)
sys.stdout.write(pt.decode("utf-8"))
PY
)"

DEST_BUCKET="$(python3 - <<PY
import json
c=json.loads('''${DEST_CONFIG_JSON}''')
print(c.get("bucket",""))
PY
)"
DEST_REGION="$(python3 - <<PY
import json
c=json.loads('''${DEST_CONFIG_JSON}''')
print(c.get("region","us-east-1") or "us-east-1")
PY
)"
DEST_PREFIX="$(python3 - <<PY
import json
c=json.loads('''${DEST_CONFIG_JSON}''')
print((c.get("path_prefix") or c.get("prefix") or "").strip())
PY
)"
DEST_ENDPOINT_URL="$(python3 - <<PY
import json
c=json.loads('''${DEST_CONFIG_JSON}''')
print((c.get("endpoint_url") or c.get("endpoint") or "").strip())
PY
)"

[[ -n "${DEST_BUCKET}" ]] || die "Destination config missing bucket"
if [[ -n "${DEST_ENDPOINT_URL}" ]]; then
  die "Destination config has endpoint_url set (${DEST_ENDPOINT_URL}). This is NOT real AWS S3."
fi

log "✅ Destination loaded"
log "  - connector_type: ${DEST_CONNECTOR_TYPE_RAW} @ ${DEST_CONNECTOR_VERSION}"
log "  - bucket:         ${DEST_BUCKET}"
log "  - prefix:         ${DEST_PREFIX}"
log "  - region:         ${DEST_REGION}"
log ""

# ─────────────────────────────────────────────────────────────
# Step 1: Sanity check AWS creds by listing prefix (no secrets printed)
# ─────────────────────────────────────────────────────────────
log "🔐 Verifying AWS credentials can access bucket (list prefix)..."
python3 - <<PY
import sys, json
import boto3
from botocore.config import Config

cfg = json.loads('''${DEST_CONFIG_JSON}''')
bucket = cfg.get("bucket")
region = cfg.get("region") or "us-east-1"
ak = cfg.get("access_key_id") or cfg.get("aws_access_key_id") or cfg.get("access_key")
sk = cfg.get("secret_access_key") or cfg.get("aws_secret_access_key") or cfg.get("secret_key")
prefix = (cfg.get("path_prefix") or cfg.get("prefix") or "").strip()
if prefix and not prefix.endswith("/"):
    prefix += "/"

if not ak or not sk:
    print("❌ Missing AWS credentials in destination config (access_key_id/secret_access_key).")
    sys.exit(2)

s3 = boto3.client("s3", aws_access_key_id=ak, aws_secret_access_key=sk, region_name=region, config=Config(signature_version="s3v4"))
try:
    # cheap check: list at most 1 object
    s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    print("✅ AWS list_objects_v2 succeeded")
except Exception as e:
    print(f"❌ AWS access failed: {e}")
    sys.exit(3)
PY
log ""

# ─────────────────────────────────────────────────────────────
# Step 1.5: Ensure infra is running
# ─────────────────────────────────────────────────────────────
log "🚀 Ensuring services are running (kafka, kafka-connect, mysql-e2e)..."
core up -d kafka kafka-connect >/dev/null
e2e up -d "${MYSQL_SERVICE}" >/dev/null
log "✅ Services running"
log ""

# ─────────────────────────────────────────────────────────────
# Step 2: Prep test row + create Debezium connector (mysql-e2e -> Kafka)
# ─────────────────────────────────────────────────────────────
CONNECTOR_NAME="cdc-mysql-e2e-s3-$(date +%s)"
TOPIC="${CONNECTOR_NAME}.${DB}.${TABLE}"

# Pick a high random ID to avoid collisions with earlier runs.
EVENT_ID="$(python3 - <<'PY'
import random
print(random.randint(1000000000, 2000000000))
PY
)"
INSERT_NAME="insert-${CONNECTOR_NAME}-${EVENT_ID}"
UPDATE_NAME="update-${CONNECTOR_NAME}-${EVENT_ID}"

log "🧱 Ensuring table exists and test row is absent (id=${EVENT_ID})..."
e2e exec -T "${MYSQL_SERVICE}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
  CREATE DATABASE IF NOT EXISTS ${DB};
  CREATE TABLE IF NOT EXISTS ${DB}.${TABLE} (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  DELETE FROM ${DB}.${TABLE} WHERE id=${EVENT_ID};
"

cleanup() {
  log ""
  log "🧹 Cleanup (best-effort)..."
  # Stop sink worker
  docker run --rm --network "${NETWORK}" curlimages/curl:8.7.1 -sS \
    -H "Content-Type: application/json" \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"kafka-mcp-sink_stop_sink\",\"arguments\":{\"config\":{\"consumer_group\":\"sink-${CONNECTOR_NAME}\"}}}}" \
    "http://${SINK_CONTAINER}:8000/mcp" >/dev/null 2>&1 || true
  # Delete Debezium connector
  curl -sS -X DELETE "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

log "🔌 Creating Debezium connector: ${CONNECTOR_NAME}"

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

    "snapshot.mode": "never",
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
  die "Failed to create connector (HTTP ${code}): $(echo "${resp}" | sed '$d' | head -n 5)"
fi

log "⏳ Waiting for connector + task to be RUNNING..."
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
    die "Connector/task did not become RUNNING"
  fi
done

log "✅ Debezium is RUNNING"
log "  - topic: ${TOPIC}"
log ""

# Give the connector a moment to fully start streaming before we mutate data.
sleep 5

# ─────────────────────────────────────────────────────────────
# Step 3: Start kafka-mcp-sink worker (Kafka -> AWS S3)
# ─────────────────────────────────────────────────────────────
log "🛰️  Starting kafka-mcp-sink worker (topic -> S3)..."

# Use decrypted destination config; add a logical 'table' label for routing.
DEST_CONFIG_JSON_MIN="$(
python3 - <<PY
import json
c=json.loads('''${DEST_CONFIG_JSON}''')
c["table"]="cdc_events"
print(json.dumps(c))
PY
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
        "consumer_group": "sink-${CONNECTOR_NAME}",
        "kafka_bootstrap_servers": "kafka:29092",
        "destination_connector": "${DEST_CONNECTOR_TYPE}",
        "destination_version": "${DEST_CONNECTOR_VERSION}",
        "destination_config": ${DEST_CONFIG_JSON_MIN}
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

if echo "${START_RESP}" | jq -e '.error' >/dev/null 2>&1; then
  die "Failed to start sink worker: $(echo "${START_RESP}" | jq -r '.error.message')"
fi
log "✅ Sink worker started"
echo "${START_RESP}" | jq '.result' || true
log ""

# Give the sink a moment to start consuming.
sleep 2

# ─────────────────────────────────────────────────────────────
# Step 4: INSERT / UPDATE / DELETE and verify objects appear in AWS S3
# ─────────────────────────────────────────────────────────────
EXPECT_PREFIX="${DEST_PREFIX}"
EXPECT_PREFIX="$(echo "${EXPECT_PREFIX}" | sed -e 's#^/##' -e 's#//*#/#g')"
if [[ -n "${EXPECT_PREFIX}" && "${EXPECT_PREFIX}" != */ ]]; then
  EXPECT_PREFIX="${EXPECT_PREFIX}/"
fi
EXPECT_PREFIX="${EXPECT_PREFIX}cdc/${TOPIC}/"

wait_for_s3_event() {
  local expected_op="$1"
  local expected_id="$2"
  local expected_name="$3"     # may be empty for delete checks (we still pass it)
  local expected_name_path="$4" # e.g. after.name or before.name

  python3 - <<PY
import json, time, sys, io, csv, ast
import boto3
from botocore.config import Config

cfg = json.loads('''${DEST_CONFIG_JSON}''')
bucket = cfg.get("bucket")
region = cfg.get("region") or "us-east-1"
ak = cfg.get("access_key_id") or cfg.get("aws_access_key_id") or cfg.get("access_key")
sk = cfg.get("secret_access_key") or cfg.get("aws_secret_access_key") or cfg.get("secret_key")

prefix = "${EXPECT_PREFIX}"
expected_op = "${expected_op}"
expected_id = int("${expected_id}")
expected_name = "${expected_name}"
expected_name_path = "${expected_name_path}"

s3 = boto3.client("s3", aws_access_key_id=ak, aws_secret_access_key=sk, region_name=region, config=Config(signature_version="s3v4"))

def match_evt(evt: dict) -> bool:
    op = evt.get("op")
    if op != expected_op:
        return False
    if expected_op in ("c", "u"):
        after = evt.get("after") or {}
        if after.get("id") != expected_id:
            return False
        if expected_name and after.get("name") != expected_name:
            return False
        return True
    if expected_op == "d":
        before = evt.get("before") or {}
        if before.get("id") != expected_id:
            return False
        if expected_name and before.get("name") != expected_name:
            return False
        return True
    return False

def parse_csv_events(body: str):
    # The AWS S3 connector may emit CSV with dicts stringified using Python repr:
    # after = "{'id': 123, 'name': 'x'}"
    out = []
    reader = csv.DictReader(io.StringIO(body))
    for row in reader:
        evt = {"op": row.get("op") or None}
        after_s = (row.get("after") or "").strip()
        before_s = (row.get("before") or "").strip()
        if after_s:
            try:
                evt["after"] = ast.literal_eval(after_s)
            except Exception:
                evt["after"] = {"_raw": after_s}
        if before_s:
            try:
                evt["before"] = ast.literal_eval(before_s)
            except Exception:
                evt["before"] = {"_raw": before_s}
        out.append(evt)
    return out

deadline = time.time() + 120
seen = set()
while time.time() < deadline:
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=200)
    for item in resp.get("Contents", []) or []:
        key = item.get("Key")
        if not key or key in seen:
            continue
        seen.add(key)
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", errors="replace")
        # Prefer JSON if possible; fall back to CSV parsing.
        matched = False
        if body.lstrip().startswith("{"):
            try:
                evt = json.loads(body)
                if isinstance(evt, dict) and match_evt(evt):
                    matched = True
            except Exception:
                pass
        if not matched:
            try:
                for evt in parse_csv_events(body):
                    if match_evt(evt):
                        matched = True
                        break
            except Exception:
                pass

        if matched:
            print(key)
            sys.exit(0)

    time.sleep(2)

print("TIMEOUT", file=sys.stderr)
sys.exit(4)
PY
}

log "🧱 Ensuring table exists..."
e2e exec -T "${MYSQL_SERVICE}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
  CREATE DATABASE IF NOT EXISTS ${DB};
  CREATE TABLE IF NOT EXISTS ${DB}.${TABLE} (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
"

log "✍️  INSERT: id=${EVENT_ID} name=${INSERT_NAME}"
e2e exec -T "${MYSQL_SERVICE}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
  INSERT INTO ${DB}.${TABLE}(id, name) VALUES (${EVENT_ID}, '${INSERT_NAME}');
"

log "🔎 Waiting for INSERT event object under: s3://${DEST_BUCKET}/${EXPECT_PREFIX}"
INSERT_KEY="$(wait_for_s3_event "c" "${EVENT_ID}" "${INSERT_NAME}" "after.name")" || die "Timed out waiting for INSERT event in S3"
log "✅ INSERT event written: ${INSERT_KEY}"

log ""
log "✍️  UPDATE: id=${EVENT_ID} name=${UPDATE_NAME}"
e2e exec -T "${MYSQL_SERVICE}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
  UPDATE ${DB}.${TABLE} SET name='${UPDATE_NAME}' WHERE id=${EVENT_ID};
"

log "🔎 Waiting for UPDATE event object under: s3://${DEST_BUCKET}/${EXPECT_PREFIX}"
UPDATE_KEY="$(wait_for_s3_event "u" "${EVENT_ID}" "${UPDATE_NAME}" "after.name")" || die "Timed out waiting for UPDATE event in S3"
log "✅ UPDATE event written: ${UPDATE_KEY}"

log ""
log "✍️  DELETE: id=${EVENT_ID}"
e2e exec -T "${MYSQL_SERVICE}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
  DELETE FROM ${DB}.${TABLE} WHERE id=${EVENT_ID};
"

log "🔎 Waiting for DELETE event object under: s3://${DEST_BUCKET}/${EXPECT_PREFIX}"
DELETE_KEY="$(wait_for_s3_event "d" "${EVENT_ID}" "${UPDATE_NAME}" "before.name")" || die "Timed out waiting for DELETE event in S3"
log "✅ DELETE event written: ${DELETE_KEY}"

log ""
log "🎉 PASS: INSERT/UPDATE/DELETE CDC events were written to REAL AWS S3"

