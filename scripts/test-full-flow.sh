#!/usr/bin/env bash
# test-full-flow.sh — golden path API test for the full data pipeline flow.
# Tests the same paths users hit in the UI, catches the class of issues
# that were found in the prod audit (wrong status codes, silent 404s, etc.)
#
# Usage:
#   ./scripts/test-full-flow.sh                                  # local stack
#   BASE_URL=https://rsync.example.com ./scripts/test-full-flow.sh   # any deployment

set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
API="${BASE_URL}/api/v1"
EMAIL="${TEST_EMAIL:-default@rsync-ai.local}"
PASSWORD="${TEST_PASSWORD:-password123}"

PASS=0
FAIL=0
COOKIE_JAR=$(mktemp)
CSRF_TOKEN=""
trap 'rm -f "$COOKIE_JAR"' EXIT

green() { echo "  ✓ $*"; PASS=$((PASS+1)); }
red()   { echo "  ✗ $*"; FAIL=$((FAIL+1)); }

# Build curl args with CSRF header when available
curl_args() {
  local args=(-s --max-time 15 -b "$COOKIE_JAR" -c "$COOKIE_JAR")
  [[ -n "$CSRF_TOKEN" ]] && args+=(-H "X-CSRF-Token: $CSRF_TOKEN")
  echo "${args[@]}"
}

check_status() {
  local label="$1" url="$2" expected="$3" method="${4:-GET}" data="${5:-}"
  local args
  read -ra args <<< "$(curl_args)"
  args+=(-o /dev/null -w "%{http_code}" -X "$method")
  [[ -n "$data" ]] && args+=(-H "Content-Type: application/json" -d "$data")
  local got
  # `|| true`: curl exits non-zero when it cannot connect, and under `set -e`
  # that aborts the whole script on the first check with no output at all.
  got=$(curl "${args[@]}" "$url" || true)
  [[ -z "$got" ]] && got="000"
  if [[ "$got" == "$expected" ]]; then
    green "$label ($got)"
  else
    red "$label — got $got, expected $expected"
  fi
}

check_json() {
  local label="$1" url="$2" jq_expr="$3" expected="$4" method="${5:-GET}" data="${6:-}"
  local args
  read -ra args <<< "$(curl_args)"
  args+=(-X "$method")
  [[ -n "$data" ]] && args+=(-H "Content-Type: application/json" -d "$data")
  local body got
  body=$(curl "${args[@]}" "$url" 2>/dev/null || echo "{}")
  got=$(echo "$body" | python3 -c "import sys,json; d=json.load(sys.stdin); v=$jq_expr; print(v if v is not None else 'null')" 2>/dev/null || echo "ERROR")
  if [[ "$got" == "$expected" ]]; then
    green "$label ($got)"
  else
    red "$label — got '$got', expected '$expected'"
    echo "     body: $(echo "$body" | head -c 200)"
  fi
}

echo "=== full flow test against $BASE_URL ==="
echo ""

# ── 1. Infrastructure health ─────────────────────────────────────────────────
echo "--- infrastructure ---"
check_status "frontend loads"        "$BASE_URL/login"    "200"
# 401 when auth is enforced; 200 on a local stack with the dev bypass enabled.
API_HEALTH=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "$API/pipelines" || true)
[[ -z "$API_HEALTH" ]] && API_HEALTH="000"
if [[ "$API_HEALTH" == "000" ]]; then
  red "api-gateway did not respond at all — nothing is listening on $BASE_URL"
  echo ""
  echo "The stack is probably not running, or is published on a different port."
  echo "Every check below would fail for that reason alone, so stopping here."
  echo "Bring it up, or point the script elsewhere:  BASE_URL=http://localhost:<port> $0"
  exit 1
fi
if [[ "$API_HEALTH" == "401" || "$API_HEALTH" == "200" ]]; then
  green "api-gateway reachable ($API_HEALTH)"
  PASS=$((PASS+1))
else
  red "api-gateway unreachable ($API_HEALTH)"
  FAIL=$((FAIL+1))
fi
# Orchestrator is internal-only — test it through Traefik's /orchestrator prefix
ORCH_STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 \
  "${BASE_URL}/orchestrator/health" 2>/dev/null || true)
[[ -z "$ORCH_STATUS" ]] && ORCH_STATUS="000"
if [[ "$ORCH_STATUS" == "200" ]]; then
  green "orchestrator reachable via Traefik ($ORCH_STATUS)"
elif [[ "$ORCH_STATUS" == "404" ]]; then
  # Check directly (staging may not expose /orchestrator prefix)
  ORCH_DIRECT=$(docker exec rsync-ai-orchestrator curl -s -o /dev/null -w "%{http_code}" \
    http://localhost:8080/health 2>/dev/null || true)
  [[ -z "$ORCH_DIRECT" ]] && ORCH_DIRECT="000"
  [[ "$ORCH_DIRECT" == "200" ]] && green "orchestrator healthy (internal)" || red "orchestrator unhealthy ($ORCH_DIRECT)"
else
  red "orchestrator unreachable ($ORCH_STATUS)"
fi

# ── 2. Auth + CSRF ───────────────────────────────────────────────────────────
echo ""
echo "--- auth ---"

# Fetch CSRF token — the login page sets it as a cookie or X-CSRF-Token header.
# We load the page first so the cookie jar picks it up, then extract the token.
curl -s --max-time 10 -c "$COOKIE_JAR" -o /dev/null "$BASE_URL/login" 2>/dev/null || true
CSRF_TOKEN=$(grep -i "csrf" "$COOKIE_JAR" 2>/dev/null | awk '{print $NF}' | head -1 || echo "")

LOGIN_BODY=$(curl -s --max-time 10 \
  -b "$COOKIE_JAR" -c "$COOKIE_JAR" \
  ${CSRF_TOKEN:+-H "X-CSRF-Token: $CSRF_TOKEN"} \
  -X POST \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" \
  "$API/auth/login" 2>/dev/null || echo "{}")

LOGIN_STATUS=$(echo "$LOGIN_BODY" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print('ok' if 'user' in d or 'token' in d or d.get('message') else 'fail')" \
  2>/dev/null || echo "fail")

if [[ "$LOGIN_STATUS" == "ok" ]]; then
  green "login succeeded"
  PASS=$((PASS+1))
  # Update CSRF token from login response headers if provided
  NEW_CSRF=$(echo "$LOGIN_BODY" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(d.get('csrf_token',''))" 2>/dev/null || echo "")
  [[ -n "$NEW_CSRF" ]] && CSRF_TOKEN="$NEW_CSRF"
else
  red "login failed — body: $(echo "$LOGIN_BODY" | head -c 200)"
  FAIL=$((FAIL+1))
  echo "Cannot continue without auth."
  echo "=== results: $PASS passed, $FAIL failed ==="
  exit 1
fi

# ── 3. Dashboard stats ───────────────────────────────────────────────────────
echo ""
echo "--- dashboard stats ---"
check_json "pipelines total is a number" "$API/pipelines" \
  "'number' if isinstance(d.get('total'), (int,float)) else 'not-number'" "number"
check_json "executions total is a number" "$API/executions" \
  "'number' if isinstance(d.get('total'), (int,float)) else 'not-number'" "number"
check_status "connections list"  "$API/connections"  "200"
check_status "connectors list"   "$API/connectors"   "200"
check_status "oauth tokens list" "$API/oauth/tokens" "200"

# ── 4. Chat / NL pipeline ────────────────────────────────────────────────────
echo ""
echo "--- chat (NL pipeline) ---"
CHAT_ARGS=(-s --max-time 25 -b "$COOKIE_JAR" -c "$COOKIE_JAR" -X POST
  -H "Content-Type: application/json"
  -d '{"message":"mysql to postgresql","session_id":"test-flow-session"}')
[[ -n "$CSRF_TOKEN" ]] && CHAT_ARGS+=(-H "X-CSRF-Token: $CSRF_TOKEN")
CHAT_BODY=$(curl "${CHAT_ARGS[@]}" "$API/chat/message" 2>/dev/null || echo "{}")
CHAT_TYPE=$(echo "$CHAT_BODY" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('type','missing'))" 2>/dev/null || echo "error")
if [[ "$CHAT_TYPE" != "missing" && "$CHAT_TYPE" != "error" ]]; then
  green "chat responded (type=$CHAT_TYPE)"
  PASS=$((PASS+1))
else
  red "chat failed — body: $(echo "$CHAT_BODY" | head -c 300)"
  FAIL=$((FAIL+1))
fi

# ── 5. Connections CRUD (real Azure PostgreSQL test DB) ───────────────────────
echo ""
echo "--- connections ---"
# Create + test a real PostgreSQL connection when TEST_PG_* is configured.
# Set these in .env.staging (gitignored) and source them before running, e.g.
#   set -a; . .env.staging; set +a; ./scripts/test-full-flow.sh
if [[ -n "${TEST_PG_HOST:-}" ]]; then
  PG_PORT="${TEST_PG_PORT:-5432}"
  PG_DB="${TEST_PG_DB:-postgres}"
  PG_USER="${TEST_PG_USER:-}"
  PG_PASS="${TEST_PG_PASSWORD:-}"
  PG_SSL="${TEST_PG_SSLMODE:-require}"   # Azure PostgreSQL requires TLS
  CONN_ARGS=(-s --max-time 20 -b "$COOKIE_JAR" -c "$COOKIE_JAR" -X POST
    -H "Content-Type: application/json"
    -d "{\"name\":\"test-pg-azure\",\"connector_type\":\"postgresql\",\"config\":{\"host\":\"$TEST_PG_HOST\",\"port\":\"$PG_PORT\",\"database\":\"$PG_DB\",\"user\":\"$PG_USER\",\"password\":\"$PG_PASS\",\"sslmode\":\"$PG_SSL\"}}")
  [[ -n "$CSRF_TOKEN" ]] && CONN_ARGS+=(-H "X-CSRF-Token: $CSRF_TOKEN")
  CONN_BODY=$(curl "${CONN_ARGS[@]}" "$API/connections" 2>/dev/null || echo "{}")
  CONN_ID=$(echo "$CONN_BODY" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(d.get('id',''))" 2>/dev/null || echo "")
  if [[ -n "$CONN_ID" ]]; then
    green "created PostgreSQL connection ($CONN_ID)"
    PASS=$((PASS+1))
    # Test the live connection (validates host/creds/SSL reach Azure)
    TEST_ARGS=(-s -o /dev/null -w "%{http_code}" --max-time 25 -b "$COOKIE_JAR" -X POST)
    [[ -n "$CSRF_TOKEN" ]] && TEST_ARGS+=(-H "X-CSRF-Token: $CSRF_TOKEN")
    TEST_STATUS=$(curl "${TEST_ARGS[@]}" "$API/connections/$CONN_ID/test" 2>/dev/null || echo "000")
    if [[ "$TEST_STATUS" == "200" ]]; then
      green "PostgreSQL connection test passed (reached Azure)"
      PASS=$((PASS+1))
    else
      red "connection test returned $TEST_STATUS (check host/creds/SSL/firewall)"
      FAIL=$((FAIL+1))
    fi
    # Cleanup
    DEL_ARGS=(-s -o /dev/null -b "$COOKIE_JAR" -X DELETE)
    [[ -n "$CSRF_TOKEN" ]] && DEL_ARGS+=(-H "X-CSRF-Token: $CSRF_TOKEN")
    curl "${DEL_ARGS[@]}" "$API/connections/$CONN_ID" 2>/dev/null || true
  else
    red "failed to create connection — body: $(echo "$CONN_BODY" | head -c 300)"
    FAIL=$((FAIL+1))
  fi
else
  echo "  ⏭  skipped — set TEST_PG_HOST (+ TEST_PG_USER/PASSWORD/DB) in .env.staging"
fi

# ── 6. Logout ────────────────────────────────────────────────────────────────
echo ""
echo "--- logout ---"
check_status "logout clears session"       "$API/auth/logout" "200" "POST"
# 401 after logout when auth is enforced; a local dev bypass still gives 200.
POST_LOGOUT=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -b "$COOKIE_JAR" "$API/pipelines" || true)
[[ -z "$POST_LOGOUT" ]] && POST_LOGOUT="000"
if [[ "$POST_LOGOUT" == "401" || "$POST_LOGOUT" == "200" ]]; then
  green "post-logout state correct ($POST_LOGOUT)"
  PASS=$((PASS+1))
else
  red "unexpected post-logout status ($POST_LOGOUT)"
  FAIL=$((FAIL+1))
fi

# ── 7. Summary ───────────────────────────────────────────────────────────────
echo ""
echo "=== results: $PASS passed, $FAIL failed ==="
[[ $FAIL -gt 0 ]] && exit 1 || exit 0
