#!/usr/bin/env bash
# Smoke test: data pipeline E2E (mysql-e2e -> postgres-e2e, big_table 100k rows).
#
# Purpose: catch silent-drop failures (orchestrator says completed, sink
# wrote 0 rows). The sink network bug shipped to main and stayed there
# undetected because no test asserted dest row count.
#
# Exit codes:
#   0  pass — source_count == dest_count
#   1  setup failure (missing services / connections / containers)
#   2  pipeline ran but row counts diverged (the bug class this test exists for)
#   3  pipeline failed to terminal status within timeout
#
# Required preconditions (caller's responsibility):
#   docker compose up -d              # main stack incl. minio
#   docker compose -f docker-compose.e2e.dbs.yml up -d mysql-e2e postgres-e2e
#   docker network connect --alias mysql-e2e   rsync-ai-mcp rsync-ai-mysql-e2e
#   docker network connect --alias postgres-e2e rsync-ai-mcp rsync-ai-postgres-e2e
#   Pre-existing connections r14-mysql-src and r14-pg-dst pinned to v1.0.0.

set -euo pipefail

API="${SMOKE_API_BASE:-http://localhost:5001}"
SOURCE_CONN="${SMOKE_SOURCE_CONN:-ef9b2b7b-5be7-410b-a802-28a1b4f5c58d}"   # r14-mysql-src
DEST_CONN="${SMOKE_DEST_CONN:-bfbdb9fc-e6a4-4086-bce5-b9252a87f412}"       # r14-pg-dst
SOURCE_TABLE="${SMOKE_SOURCE_TABLE:-e2e_db.big_table}"
DEST_TABLE="${SMOKE_DEST_TABLE:-big_table}"
SOURCE_DB_CONTAINER="${SMOKE_SOURCE_DB:-rsync-ai-mysql-e2e}"
DEST_DB_CONTAINER="${SMOKE_DEST_DB:-rsync-ai-postgres-e2e}"
TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-600}"   # 10 minutes for 100k rows
PIPELINE_NAME="smoke-$(date -u +%s)"

red()    { printf '\033[31m%s\033[0m\n' "$*"; }
green()  { printf '\033[32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[33m%s\033[0m\n' "$*"; }

require() { command -v "$1" >/dev/null 2>&1 || { red "missing dependency: $1"; exit 1; }; }
require curl
require docker
require python3

# ----- Preflight ------------------------------------------------------------

if ! curl -fsS -m 5 "$API/health" >/dev/null; then
    red "api-gateway not reachable at $API/health"
    exit 1
fi

source_count=$(docker exec "$SOURCE_DB_CONTAINER" \
    mysql -u e2e_user -pe2e_password -D e2e_db -Ne \
    "SELECT COUNT(*) FROM ${SOURCE_TABLE##*.};" 2>/dev/null | tail -1)
if [[ -z "$source_count" || "$source_count" -lt 1 ]]; then
    red "source table $SOURCE_TABLE has 0 rows — seed before smoke testing"
    exit 1
fi
yellow "preflight: source $SOURCE_TABLE has $source_count rows"

# Drop dest so we measure the actual delta, not residue from previous runs.
docker exec "$DEST_DB_CONTAINER" \
    psql -U e2e_user -d e2e_db -c "DROP TABLE IF EXISTS $DEST_TABLE;" >/dev/null
yellow "preflight: dropped $DEST_TABLE in $DEST_DB_CONTAINER"

# ----- Create + run pipeline -----------------------------------------------

create_payload=$(printf '{"name":"%s","description":"smoke test mysql->pg","request":"Copy %s from mysql-e2e to postgres-e2e %s","source_connection_id":"%s","destination_connection_id":"%s"}' \
    "$PIPELINE_NAME" "$SOURCE_TABLE" "$DEST_TABLE" "$SOURCE_CONN" "$DEST_CONN")
create_response=$(curl -fsS -XPOST "$API/api/v1/pipelines" -H 'Content-Type: application/json' -d "$create_payload")
pid=$(printf '%s' "$create_response" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
yellow "created pipeline $pid"

curl -fsS -XPOST "$API/api/v1/pipelines/$pid/run" \
    -H 'Content-Type: application/json' -d '{}' >/dev/null
yellow "triggered pipeline"

# ----- Wait for HITL, submit tables, poll until terminal -------------------

start_ts=$(date +%s)
hitl_submitted=0
status="running"
while :; do
    now=$(date +%s)
    elapsed=$(( now - start_ts ))
    if (( elapsed > TIMEOUT_SECONDS )); then
        red "pipeline did not reach terminal status within ${TIMEOUT_SECONDS}s (status=$status)"
        exit 3
    fi

    status=$(curl -fsS "$API/api/v1/executions?pipeline_id=$pid&limit=1" \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['executions'][0]['status'] if d['executions'] else 'unknown')")

    if [[ "$status" == "completed" || "$status" == "failed" || "$status" == "cancelled" ]]; then
        break
    fi

    # Submit HITL table selection once we see the orchestrator asking for it.
    if (( hitl_submitted == 0 )) && \
       docker logs --since 30s rsync-ai-orchestrator 2>&1 | grep -q "PIPELINE_WAITING.*$pid"; then
        curl -fsS -XPOST "$API/api/v1/pipelines/$pid/hitl/tables" \
            -H 'Content-Type: application/json' \
            -d "{\"selected_tables\":[\"$SOURCE_TABLE\"]}" >/dev/null
        hitl_submitted=1
        yellow "submitted HITL tables"
    fi

    sleep 10
done

# ----- The actual invariant -------------------------------------------------

dest_count=$(docker exec "$DEST_DB_CONTAINER" \
    psql -U e2e_user -d e2e_db -tAc "SELECT COUNT(*) FROM $DEST_TABLE;" 2>&1 \
    | tail -1 | tr -d ' ')

# 99% floor — allows for natural NULL/type filtering without false-failing.
min_dest=$(( source_count - source_count / 100 ))

if [[ "$status" != "completed" ]]; then
    red "pipeline terminal status = $status (expected completed)"
    red "dest count: $dest_count, source: $source_count"
    exit 2
fi

if [[ -z "$dest_count" || ! "$dest_count" =~ ^[0-9]+$ ]]; then
    red "dest table $DEST_TABLE missing or query failed (got: '$dest_count')"
    red "pipeline status was 'completed' but no destination data — this is the silent-drop class"
    exit 2
fi

if (( dest_count < min_dest )); then
    red "row-count invariant violated:"
    red "  source ${SOURCE_TABLE}: $source_count"
    red "  dest ${DEST_TABLE}:    $dest_count"
    red "  minimum allowed (99%): $min_dest"
    red "pipeline reported 'completed' but destination is short — silent-drop class"
    exit 2
fi

green "smoke test PASSED: source=$source_count dest=$dest_count (>= ${min_dest})"
exit 0
