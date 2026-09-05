#!/usr/bin/env bash
# Seed the world for workspace_multiuser_ui.spec.ts against a RUNNING stack.
#
# Creates two email-verified users (owner A + member B), a shared workspace W with
# A as owner and B as a member, plus one pending invite for the /invite landing
# check. Writes the WSUI_* env the spec reads to $WSUI_ENV_OUT.
#
# Requires:
#   BASE_URL       public stack URL          (default http://localhost:8080)
#   DATABASE_URL   app Postgres DSN           (used only to mark users verified)
#   WSUI_ENV_OUT   where to write the env     (default ./wsui.env)
#   WSUI_SHOTS     screenshot output dir      (default ./wsui-shots)
# psql is reached via the postgres:16-alpine image on the host network if a host
# psql is not installed, so only Docker (or psql) is needed — not a local PG.
#
# Usage:
#   set -a; source .env.staging; set +a   # exports DATABASE_URL
#   bash frontend/e2e/fixtures/workspace_multiuser.seed.sh
#   source ./wsui.env
#   (cd frontend && npx playwright test e2e/workspace_multiuser_ui.spec.ts \
#       --config=playwright.e2e-live.config.ts)
set -u
BASE="${BASE_URL:-http://localhost:8080}"
OUT="${WSUI_ENV_OUT:-./wsui.env}"
SHOTS="${WSUI_SHOTS:-./wsui-shots}"
: "${DATABASE_URL:?export DATABASE_URL (e.g. source .env.staging) before seeding}"
TS=$(date +%s)
TMP=$(mktemp -d)
JA="$TMP/jarA"; JB="$TMP/jarB"
trap 'rm -rf "$TMP"' EXIT

psql_x() {
  if command -v psql >/dev/null 2>&1; then psql "$DATABASE_URL" "$@"
  else docker run --rm --network host postgres:16-alpine psql "$DATABASE_URL" "$@"; fi
}
jget() { python3 -c "import sys,json
try:
 d=json.load(sys.stdin)
except Exception:
 print(''); raise SystemExit
print(d.get('$1','') if isinstance(d,dict) else '')"; }

A_EMAIL="wsui-owner@example.com"; B_EMAIL="wsui-member@example.com"; PW="Password123!"
reg() { curl -s -o /dev/null -H 'Content-Type: application/json' -X POST \
  -d "{\"email\":\"$1\",\"password\":\"$PW\",\"name\":\"$2\"}" "$BASE/api/v1/auth/register"; }
reg "$A_EMAIL" "WS Owner"; reg "$B_EMAIL" "WS Member"
psql_x -At -c "UPDATE users SET email_verified=true WHERE email IN ('$A_EMAIL','$B_EMAIL');" >/dev/null

RA=$(curl -s -c "$JA" -H 'Content-Type: application/json' -X POST \
  -d "{\"email\":\"$A_EMAIL\",\"password\":\"$PW\"}" "$BASE/api/v1/auth/login"); AID=$(echo "$RA"|jget user_id)
RB=$(curl -s -c "$JB" -H 'Content-Type: application/json' -X POST \
  -d "{\"email\":\"$B_EMAIL\",\"password\":\"$PW\"}" "$BASE/api/v1/auth/login"); BID=$(echo "$RB"|jget user_id)
[ -n "$AID" ] || { echo "FATAL: owner login failed: $RA" >&2; exit 1; }

# idempotency: drop any prior test workspaces owned by A
psql_x -At -c "DELETE FROM workspaces WHERE owner_id='$AID' AND NOT is_personal AND name LIKE 'WSUITeam%';" >/dev/null

WSNAME="WSUITeam $TS"
WID=$(curl -s -b "$JA" -H 'Content-Type: application/json' -X POST \
  -d "{\"name\":\"$WSNAME\"}" "$BASE/api/v1/workspaces" | jget id)
TOKB=$(curl -s -b "$JA" -H 'Content-Type: application/json' -X POST \
  -d "{\"email\":\"$B_EMAIL\",\"role\":\"member\"}" "$BASE/api/v1/workspaces/$WID/invites" | jget token)
curl -s -o /dev/null -b "$JB" -X POST "$BASE/api/v1/workspace-invites/$TOKB/accept"
TOKP=$(curl -s -b "$JA" -H 'Content-Type: application/json' -X POST \
  -d "{\"email\":\"wsui-pending-$TS@example.com\",\"role\":\"viewer\"}" "$BASE/api/v1/workspaces/$WID/invites" | jget token)

mkdir -p "$SHOTS"
cat > "$OUT" <<EOF
export WSUI_A_EMAIL='$A_EMAIL'
export WSUI_A_PW='$PW'
export WSUI_B_EMAIL='$B_EMAIL'
export WSUI_WID='$WID'
export WSUI_WS_NAME='$WSNAME'
export WSUI_PENDING_TOK='$TOKP'
export WSUI_SHOTS='$SHOTS'
EOF
echo "seeded: WID=$WID name='$WSNAME' member=$B_EMAIL → env at $OUT"
