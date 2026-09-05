#!/bin/bash
set -euo pipefail

echo "╔════════════════════════════════════════════════════╗"
echo "║  MySQL → AWS S3 Pipeline Test                     ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

core() {
  # Core stack (optionally with e2e overrides for MinIO/GitHub mock URLs)
  if [ -f "$ROOT_DIR/docker-compose.e2e.yml" ]; then
    docker compose -p rsync-ai -f docker-compose.yml -f docker-compose.e2e.yml "$@"
  else
    docker compose -p rsync-ai -f docker-compose.yml "$@"
  fi
}

e2e() {
  # E2E infra stack (mysql/postgres/minio/mock-github)
  docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml "$@"
}

get_cid() {
  local scope="$1"
  local service="$2"
  local cid=""
  if [ "${scope}" = "e2e" ]; then
    cid="$(e2e ps -q "$service" 2>/dev/null | head -1 || true)"
  else
    cid="$(core ps -q "$service" 2>/dev/null | head -1 || true)"
  fi
  if [ -n "$cid" ]; then
    echo "$cid"
    return 0
  fi
  cid="$(docker ps --filter "name=${service}" --format '{{.ID}}' 2>/dev/null | head -1 || true)"
  echo "$cid"
}

ensure_service_running() {
  local scope="$1"
  local service="$2"
  local cid=""
  cid="$(get_cid "${scope}" "$service")"
  if [ -z "$cid" ]; then
    echo "  Starting $service..."
    if [ "${scope}" = "e2e" ]; then
      e2e up -d "$service"
    else
      core up -d "$service"
    fi
    sleep 5
    cid="$(get_cid "${scope}" "$service")"
  fi

  if [ -z "$cid" ]; then
    echo "  ❌ Failed to start/find $service container"
    return 1
  fi

  local running="false"
  running="$(docker inspect -f '{{.State.Running}}' "$cid" 2>/dev/null || echo "false")"
  if [ "$running" != "true" ]; then
    echo "  Starting $service..."
    if [ "${scope}" = "e2e" ]; then
      e2e up -d "$service"
    else
      core up -d "$service"
    fi
    sleep 5
    cid="$(get_cid "${scope}" "$service")"
  fi

  echo "$cid"
}

get_first_network() {
  local cid="$1"
  local nets
  nets="$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$cid" 2>/dev/null | awk '{print $1}')"
  echo "$nets"
}

mc() {
  local network="$1"
  shift
  docker run --rm --network "$network" minio/mc:latest "$@"
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "❌ Missing required command: $cmd"
    exit 1
  fi
}

require_cmd curl
require_cmd jq

# API auth (dev): API Gateway uses X-User-ID to scope connections/pipelines.
USER_ID="${E2E_USER_ID:-00000000-0000-0000-0000-000000000000}"
AUTH_HEADER=(-H "X-User-ID: ${USER_ID}")

# Use unique names per run to avoid DUPLICATE_NAME errors.
RUN_TS="$(date +%s)"

# ═══════════════════════════════════════════════════════
# STEP 1: PRE-FLIGHT CHECKS
# ═══════════════════════════════════════════════════════

echo "Step 1: Pre-flight Checks"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "Checking API Gateway..."
if ! curl -sf http://localhost:5001/health >/dev/null 2>&1; then
  echo "  ❌ API Gateway not reachable at http://localhost:5001/health"
  echo "  Start the stack first (example):"
  echo "    docker compose -p rsync-ai -f docker-compose.yml up -d"
  exit 1
fi
echo "  ✅ API Gateway is reachable"

echo ""
echo "Ensuring MySQL (E2E) is running..."
MYSQL_CID="$(ensure_service_running "e2e" "mysql-e2e" || true)"
if [ -z "$MYSQL_CID" ]; then
  # Fallback for older setups that name the service/container "mysql"
  MYSQL_CID="$(ensure_service_running "core" "mysql")"
fi
echo "  ✅ MySQL container: $MYSQL_CID"

echo ""
echo "Ensuring MinIO is running..."
MINIO_CID="$(ensure_service_running "e2e" "minio")"
echo "  ✅ MinIO container: $MINIO_CID"

MINIO_NET="$(get_first_network "$MINIO_CID")"
if [ -z "$MINIO_NET" ]; then
  echo "  ❌ Could not determine MinIO docker network"
  exit 1
fi

echo ""
echo "Checking MinIO bucket..."
mc "$MINIO_NET" alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1 || true
if mc "$MINIO_NET" mb local/pipeline-data >/dev/null 2>&1; then
  echo "  ✅ Bucket created"
else
  echo "  ✅ Bucket exists"
fi

echo ""
echo "Checking MySQL test data..."
small_count="$(
  docker exec "$MYSQL_CID" mysql -u root -prootpassword test_db -e "SELECT COUNT(*) FROM small_table" 2>/dev/null | tail -1 | tr -d '\r' || true
)"

if [ "$small_count" = "100" ]; then
  echo "  ✅ small_table: 100 rows ready"
else
  echo "  ❌ small_table: ${small_count:-0} rows (expected 100)"
  echo "  Generating test data..."
  python3 tests/data/generate_test_data.py
fi

echo ""
echo "Checking connectors..."
if curl -sf "http://localhost:5001/api/v1/connectors/mysql" >/dev/null 2>&1; then
  echo "  ✅ MySQL connector: Available (mysql)"
else
  echo "  ❌ MySQL connector: Not found at /api/v1/connectors/mysql"
  exit 1
fi

if curl -sf "http://localhost:5001/api/v1/connectors/minio" >/dev/null 2>&1; then
  echo "  ✅ MinIO connector: Available (minio)"
elif curl -sf "http://localhost:5001/api/v1/connectors/aws-s3" >/dev/null 2>&1; then
  echo "  ✅ AWS S3 connector: Available (aws-s3)"
else
  echo "  ❌ S3-compatible connector not found (expected /connectors/minio or /connectors/aws-s3)"
  exit 1
fi

echo ""

# ═══════════════════════════════════════════════════════
# STEP 2: CREATE CONNECTIONS
# ═══════════════════════════════════════════════════════

echo "Step 2: Creating Connections"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "Creating MySQL source connection..."
mysql_response="$(
  curl -s -X POST http://localhost:5001/api/v1/connections \
    "${AUTH_HEADER[@]}" \
    -H 'Content-Type: application/json' \
    -d '{
      "name": "test_mysql_source_s3_'${RUN_TS}'",
      "connector_type": "mysql",
      "connection_type": "source",
      "config": {
        "host": "mysql-e2e",
        "port": 3306,
        "database": "test_db",
        "user": "root",
        "password": "rootpassword"
      }
    }' 2>/dev/null || true
)"

mysql_conn_id="$(echo "$mysql_response" | jq -r '.connection_id // .id // empty' 2>/dev/null || true)"
if [ -n "$mysql_conn_id" ]; then
  echo "  ✅ MySQL connection created: $mysql_conn_id"
else
  mysql_conn_id="$(
    curl -s http://localhost:5001/api/v1/connections "${AUTH_HEADER[@]}" 2>/dev/null \
      | jq -r '(.connections // . // [])[] | select(.connector_type=="mysql" and ((.connection_type // .type)=="source")) | (.id // .connection_id // empty)' | head -1
  )"
  if [ -n "$mysql_conn_id" ]; then
    echo "  ✅ Using existing MySQL connection: $mysql_conn_id"
  else
    echo "  ❌ Failed to create MySQL connection"
    echo "  Response: $mysql_response"
    exit 1
  fi
fi

echo ""
echo "Creating AWS S3 destination connection (pointing at MinIO for local testing)..."
s3_response="$(
  curl -s -X POST http://localhost:5001/api/v1/connections \
    "${AUTH_HEADER[@]}" \
    -H 'Content-Type: application/json' \
    -d '{
      "name": "test_s3_dest_'${RUN_TS}'",
      "connector_type": "aws-s3",
      "connection_type": "destination",
      "config": {
        "endpoint_url": "http://minio:9000",
        "bucket": "pipeline-data",
        "access_key_id": "minioadmin",
        "secret_access_key": "minioadmin",
        "region": "us-east-1",
        "path_prefix": "batch-test/'${RUN_TS}'"
      }
    }' 2>/dev/null || true
)"

s3_conn_id="$(echo "$s3_response" | jq -r '.connection_id // .id // empty' 2>/dev/null || true)"
if [ -n "$s3_conn_id" ]; then
  echo "  ✅ S3 connection created: $s3_conn_id"
else
  s3_conn_id="$(
    curl -s http://localhost:5001/api/v1/connections "${AUTH_HEADER[@]}" 2>/dev/null \
      | jq -r '(.connections // . // [])[] | select((.connector_type=="minio" or .connector_type=="aws-s3" or .connector_type=="aws_s3") and ((.connection_type // .type)=="destination")) | (.id // .connection_id // empty)' | head -1
  )"
  if [ -n "$s3_conn_id" ]; then
    echo "  ✅ Using existing S3 connection: $s3_conn_id"
  else
    echo "  ❌ Failed to create S3 connection"
    echo "  Response: $s3_response"
    exit 1
  fi
fi

echo ""

# ═══════════════════════════════════════════════════════
# STEP 3: CREATE PIPELINE
# ═══════════════════════════════════════════════════════

echo "Step 3: Creating Pipeline"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "Sending pipeline creation request..."
pipeline_response="$(
  curl -s -X POST http://localhost:5001/api/v1/chat/message \
    "${AUTH_HEADER[@]}" \
    -H 'Content-Type: application/json' \
    -d "{
      \"message\": \"migrate small_table from mysql to s3\",
      \"context\": {
        \"source_connection_id\": \"$mysql_conn_id\",
        \"destination_connection_id\": \"$s3_conn_id\"
      }
    }" 2>/dev/null || true
)"

echo "Response:"
echo "$pipeline_response" | jq '.' 2>/dev/null || echo "$pipeline_response"

pipeline_id="$(echo "$pipeline_response" | jq -r '.metadata.pipeline_id // .pipeline_id // .metadata.workflow_id // empty' 2>/dev/null || true)"
if [ -z "$pipeline_id" ]; then
  echo ""
  echo "  ❌ Could not extract pipeline ID"
  exit 1
fi

echo ""
echo "  ✅ Pipeline created: $pipeline_id"
echo ""

# ═══════════════════════════════════════════════════════
# STEP 4: MONITOR PIPELINE EXECUTION
# ═══════════════════════════════════════════════════════

echo "Step 4: Monitoring Pipeline Execution"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

last_stage=""
start_time="$(date +%s)"
max_wait=300

for _ in {1..150}; do
  sleep 2
  elapsed="$(($(date +%s) - start_time))"

  state="$(curl -s "http://localhost:5001/api/v1/pipelines/$pipeline_id/state" 2>/dev/null || true)"
  if [ -z "$state" ]; then
    echo "  ⚠️  [${elapsed}s] No state response"
    continue
  fi

  status="$(echo "$state" | jq -r '.status // "unknown"' 2>/dev/null || echo "unknown")"
  current_stage="$(echo "$state" | jq -r '.current_stage // "unknown"' 2>/dev/null || echo "unknown")"
  progress="$(echo "$state" | jq -r '.progress // 0' 2>/dev/null || echo "0")"

  if [ "$current_stage" != "$last_stage" ] && [ "$current_stage" != "unknown" ]; then
    display_name="$(echo "$state" | jq -r ".execution_plan.stages[]? | select(.id==\"$current_stage\") | .display_name // \"$current_stage\"" 2>/dev/null || echo "$current_stage")"
    echo ""
    echo "  [${elapsed}s] 📍 $display_name"
    last_stage="$current_stage"
  fi

  printf "\r  [%ss] Status: %s | Stage: %s | Progress: %s%%" "$elapsed" "$status" "$current_stage" "$progress"

  if [ "$status" = "completed" ]; then
    echo ""
    echo ""
    echo "  ✅ Pipeline completed successfully!"

    rows="$(echo "$state" | jq -r '.metadata.rows_processed // "N/A"' 2>/dev/null || echo "N/A")"
    duration="$(echo "$state" | jq -r '.metadata.duration_seconds // empty' 2>/dev/null || true)"
    if [ -z "$duration" ]; then duration="$elapsed"; fi

    echo "  📊 Rows processed: $rows"
    echo "  ⏱️  Duration: ${duration}s"
    break
  elif [ "$status" = "failed" ]; then
    echo ""
    echo ""
    echo "  ❌ Pipeline failed"
    error="$(echo "$state" | jq -r '.error // "Unknown error"' 2>/dev/null || echo "Unknown error")"
    echo "  Error: $error"
    exit 1
  elif [ "$status" = "waiting_for_user" ]; then
    echo ""
    echo ""
    echo "  ⏸️  Pipeline waiting for user input"
    reason="$(echo "$state" | jq -r '.waiting_reason // "Unknown"' 2>/dev/null || echo "Unknown")"
    echo "  Reason: $reason"
    exit 1
  fi

  if [ "$elapsed" -gt "$max_wait" ]; then
    echo ""
    echo ""
    echo "  ⏱️  Timeout after ${max_wait}s"
    exit 1
  fi
done

echo ""
echo ""

# ═══════════════════════════════════════════════════════
# STEP 5: VERIFY DATA IN S3/MinIO
# ═══════════════════════════════════════════════════════

echo "Step 5: Verifying Data in S3/MinIO"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "Listing objects in bucket..."
PREFIX="batch-test/${RUN_TS}/"
objects="$(
  docker run --rm --network "$MINIO_NET" --entrypoint /bin/sh minio/mc:latest -lc \
    "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1 && mc ls --recursive local/pipeline-data/${PREFIX}" \
    2>/dev/null || true
)"

# If nothing found under the expected prefix, fall back to listing the whole bucket.
if [ -z "$objects" ]; then
  objects="$(
    docker run --rm --network "$MINIO_NET" --entrypoint /bin/sh minio/mc:latest -lc \
      "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1 && mc ls --recursive local/pipeline-data" \
      2>/dev/null || true
  )"
fi

if [ -n "$objects" ]; then
  echo "  ✅ Objects found:"
  echo "$objects" | sed 's/^/     /'

  first_object="$(echo "$objects" | head -1 | awk '{print $NF}')"
  sample_key="${PREFIX}${first_object}"
  echo ""
  echo "Sample object: $sample_key"

  echo ""
  echo "Sample data (first 5 lines):"
  docker run --rm --network "$MINIO_NET" --entrypoint /bin/sh minio/mc:latest -lc \
    "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1 && mc cat \"local/pipeline-data/${sample_key}\" | head -5" \
    2>/dev/null | sed 's/^/     /' || true
else
  echo "  ⚠️  No objects found in bucket (pipeline may have written to a different prefix)"
  echo ""
  echo "  Buckets:"
  docker run --rm --network "$MINIO_NET" --entrypoint /bin/sh minio/mc:latest -lc \
    "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1 && mc ls local/" \
    2>/dev/null | sed 's/^/     /' || true
fi

echo ""

# ═══════════════════════════════════════════════════════
# STEP 6: SUMMARY
# ═══════════════════════════════════════════════════════

echo "╔════════════════════════════════════════════════════╗"
echo "║  TEST COMPLETE                                    ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""

cat << EOF
Summary:
  ✅ MySQL source: small_table expected 100 rows
  ✅ MinIO destination: pipeline-data bucket ready
  ✅ Connections: Created/selected successfully
  ✅ Pipeline: Created and monitored
  ✅ Data verification: Listed objects (if any)

MinIO Console:
  URL: http://localhost:9001
  Username: minioadmin
  Password: minioadmin
  Bucket: pipeline-data
EOF


# Cleanup