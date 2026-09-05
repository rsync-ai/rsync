#!/usr/bin/env bash
# smoke-test.sh — quick health check against a running stack.
#
# Defaults to the stack on this machine. Nothing here is specific to any one
# deployment: point it at whatever you are running.
#
#   1. Local stack (the default — no arguments needed):
#        ./scripts/smoke-test.sh
#      Equivalent to BASE_URL=http://localhost:8080.
#
#   2. Any reachable stack, by URL:
#        BASE_URL=https://rsync.example.com ./scripts/smoke-test.sh
#      If a CDN or WAF sits in front, it may answer curl with a 403 or a
#      challenge page; use mode 3 from the host itself to bypass it.
#
#   3. On the server, bypassing whatever is in front of it:
#        DOMAIN=rsync.example.com DIRECT=1 ./scripts/smoke-test.sh
#      Uses --resolve to hit the local reverse proxy's TLS on 127.0.0.1 while
#      keeping SNI/Host = your real domain, so TLS and routing still match.
#      DOMAIN is required in this mode — there is no default domain.

set -euo pipefail

# Browser-like UA so a CDN/WAF's bot heuristics don't 403 us when going via one.
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

DIRECT="${DIRECT:-0}"
DOMAIN="${DOMAIN:-}"

# Extra curl args, set per-mode below.
RESOLVE_ARGS=()

if [[ "$DIRECT" == "1" ]]; then
  # Resolve your domain to localhost so curl hits the reverse proxy directly,
  # but keep SNI/Host = the real domain so TLS + routing still match.
  if [[ -z "$DOMAIN" ]]; then
    echo "DIRECT=1 needs the domain your stack serves, e.g." >&2
    echo "  DOMAIN=rsync.example.com DIRECT=1 $0" >&2
    exit 2
  fi
  BASE_URL="https://${DOMAIN}"
  RESOLVE_ARGS=(--resolve "${DOMAIN}:443:127.0.0.1" -k)
  echo "=== smoke test against $BASE_URL (DIRECT — resolved to 127.0.0.1) ==="
else
  # Default to the stack on this machine. Never guess a remote host: a wrong
  # target fails every check below for reasons that have nothing to do with
  # the operator's stack.
  BASE_URL="${BASE_URL:-http://localhost:8080}"
  echo "=== smoke test against $BASE_URL ==="
fi

# Is auth enforced? A hardened deployment enforces it (expect 401); a local
# dev stack may have the dev bypass on, so 200 is acceptable there too.
IS_LOCAL=0
case "$BASE_URL" in
  http://localhost*|http://127.0.0.1*) IS_LOCAL=1 ;;
esac

PASS=0
FAIL=0
UNREACHABLE=0

# check <label> <path> <expected[,expected2]> [method]
check() {
  local label="$1" path="$2" expected="$3" method="${4:-GET}"
  local status
  # `|| true` matters: curl exits non-zero when it cannot connect at all, and
  # under `set -e` that would abort the script on the first check with no
  # output whatsoever. Keep going and report it as a failed check instead —
  # curl still writes 000 to %{http_code} in that case.
  status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 15 \
    -A "$UA" ${RESOLVE_ARGS[@]+"${RESOLVE_ARGS[@]}"} -X "$method" "${BASE_URL}${path}" || true)
  [[ -z "$status" ]] && status="000"
  # expected may be a comma-separated list of acceptable codes
  if [[ ",$expected," == *",$status,"* ]]; then
    echo "  ✓ $label ($status)"
    PASS=$((PASS+1))
  else
    if [[ "$status" == "000" ]]; then
      echo "  ✗ $label — no response (could not connect)  [${path}]"
      UNREACHABLE=$((UNREACHABLE+1))
    else
      echo "  ✗ $label — got $status, expected $expected  [${path}]"
    fi
    FAIL=$((FAIL+1))
  fi
}

# Auth-protected endpoints: 401 when auth is enforced, 200 also OK locally (dev bypass).
auth_expected="401"
[[ "$IS_LOCAL" == "1" ]] && auth_expected="401,200"

# ── Routing / frontend ────────────────────────────────────────────────────────
check "frontend / (→ redirect)"       "/"                 "200,307,308"
check "login page"                    "/login"            "200"
check "dashboard (→ redirect/auth)"   "/dashboard"        "200,307,308"

# ── API routes reach api-gateway (not 404/403 from proxy) ─────────────────────
check "api-gateway: pipelines"        "/api/v1/pipelines"   "$auth_expected"
check "api-gateway: connections"      "/api/v1/connections" "$auth_expected"
check "api-gateway: connectors"       "/api/v1/connectors"  "$auth_expected"
check "api-gateway: executions"       "/api/v1/executions"  "$auth_expected"

# ── Auth endpoint exists (POST logout always 200; clears cookie) ──────────────
check "logout endpoint"               "/api/v1/auth/logout" "200" "POST"

echo ""
echo "=== results: $PASS passed, $FAIL failed ==="
if [[ $UNREACHABLE -gt 0 && $PASS -eq 0 ]]; then
  echo ""
  echo "Nothing answered at $BASE_URL — the stack is probably not running, or is"
  echo "published on a different port. This is not a failure of the checks above."
  echo "Bring it up (docker compose ... up -d), or point the script somewhere else:"
  echo "  BASE_URL=http://localhost:<port> $0"
fi
if [[ $FAIL -gt 0 ]]; then
  exit 1
fi
