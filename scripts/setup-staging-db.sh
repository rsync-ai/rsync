#!/usr/bin/env bash
# =============================================================================
# setup-staging-db.sh
#
# One-time setup for staging. staging is a TOPOLOGY MIRROR of prod:
# every database lives on Azure PostgreSQL — there is NO local postgres.
# Isolation from prod is by database name only (all suffixed `_staging`):
#
#   staging            ← app services (api-gateway, orchestrator, …)
#   temporal_staging               ← Temporal default store
#   temporal_visibility_staging    ← Temporal visibility store
#
# This script creates those three empty databases on the SAME Azure server prod
# uses, then patches .env.staging. It never touches prod's pipeline_db /
# temporal / temporal_visibility, and never touches .env.prod.
#
# Usage:
#   ./scripts/setup-staging-db.sh
#
# Prerequisites:
#   - .env.prod must exist with DATABASE_URL + POSTGRES_* pointing to Azure
#   - You must have CREATE DATABASE rights on the Azure server
#   - psql OR docker (the script auto-falls back to a throwaway `docker run
#     --rm postgres:16 psql` CLIENT if psql isn't installed — this is a CLIENT
#     only; it connects to Azure and exits, it is NOT a local database)
#
# App migrations auto-apply on first api-gateway boot; Temporal schema is set up
# by its own auto-setup on first boot (SKIP_DB_CREATE=true, same as prod). No
# manual SQL needed.
#
# After running this script:
#   docker compose \
#     -f docker-compose.yml \
#     -f docker-compose.prod.yml \
#     -f docker-compose.staging.yml \
#     --env-file .env.staging \
#     up -d
# =============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROD_ENV="$REPO_ROOT/.env.prod"
LOCAL_ENV="$REPO_ROOT/.env.staging"

APP_DB_NAME="staging"
TEMPORAL_DB_NAME="temporal_staging"
TEMPORAL_VIS_DB_NAME="temporal_visibility_staging"

# ─── Sanity checks ────────────────────────────────────────────────────────────
if [[ ! -f "$PROD_ENV" ]]; then
  echo "❌  .env.prod not found at $PROD_ENV"
  exit 1
fi
if [[ ! -f "$LOCAL_ENV" ]]; then
  echo "❌  .env.staging not found at $LOCAL_ENV"
  exit 1
fi

# ─── psql shim: native psql if present, else throwaway docker client ──────────
# IMPORTANT: the docker fallback is a CLIENT only. `--rm` means the container is
# deleted the instant the command returns. It runs NO local database — it simply
# borrows the psql binary to talk to Azure (default bridge networking reaches
# the public Azure host fine — no --network host needed, and it breaks on macOS).
if command -v psql >/dev/null 2>&1; then
  echo "ℹ️  Using native psql client"
  psql_run() { psql "$@"; }
else
  echo "ℹ️  psql not installed — using throwaway 'docker run --rm postgres:16 psql' CLIENT (no local DB)"
  psql_run() { docker run --rm postgres:16 psql "$@"; }
fi

# ─── Parse Azure connection from .env.prod ───────────────────────────────────
PROD_DB_URL=$(grep '^DATABASE_URL=' "$PROD_ENV" | head -1 | cut -d= -f2-)
if [[ -z "$PROD_DB_URL" ]]; then
  echo "❌  DATABASE_URL not found in .env.prod"
  exit 1
fi
PROD_DB_NAME=$(grep '^POSTGRES_DB=' "$PROD_ENV" | head -1 | cut -d= -f2-)
PROD_DB_NAME="${PROD_DB_NAME:-pipeline_db}"

AZURE_HOST=$(grep '^POSTGRES_HOST=' "$PROD_ENV" | head -1 | cut -d= -f2-)
AZURE_USER=$(grep '^POSTGRES_USER=' "$PROD_ENV" | head -1 | cut -d= -f2-)
AZURE_PASS=$(grep '^POSTGRES_PASSWORD=' "$PROD_ENV" | head -1 | cut -d= -f2-)
AZURE_PORT=$(grep '^POSTGRES_PORT=' "$PROD_ENV" | head -1 | cut -d= -f2-)
AZURE_PORT="${AZURE_PORT:-5432}"

# DATABASE_URL is the source of truth for credentials. .env.prod often leaves the
# discrete POSTGRES_USER/POSTGRES_PASSWORD lines EMPTY and embeds creds only in
# DATABASE_URL — so derive user/pass/host/port from the URL whenever the discrete
# vars are missing. (Skipping this is what silently wrote an EMPTY POSTGRES_PASSWORD
# into .env.staging and broke Temporal auth — POSTGRES_PWD=${POSTGRES_PASSWORD}.)
# URL form: postgresql://USER:PASS@HOST:PORT/DBNAME?params
URL_BODY="${PROD_DB_URL#*://}"          # USER:PASS@HOST:PORT/DBNAME?params
URL_CREDS="${URL_BODY%%@*}"             # USER:PASS
URL_HOSTPORT="${URL_BODY#*@}"; URL_HOSTPORT="${URL_HOSTPORT%%/*}"  # HOST:PORT
: "${AZURE_USER:=${URL_CREDS%%:*}}"
: "${AZURE_PASS:=${URL_CREDS#*:}}"
: "${AZURE_HOST:=${URL_HOSTPORT%%:*}}"
[[ "$URL_HOSTPORT" == *:* ]] && : "${AZURE_PORT:=${URL_HOSTPORT##*:}}"

if [[ -z "$AZURE_PASS" ]]; then
  echo "❌  Could not determine Azure password from .env.prod (neither POSTGRES_PASSWORD nor DATABASE_URL has one)"
  exit 1
fi

# Admin URL = connect to the built-in `postgres` database to run CREATE DATABASE
ADMIN_URL="${PROD_DB_URL/$PROD_DB_NAME/postgres}"
APP_DB_URL="${PROD_DB_URL/$PROD_DB_NAME/$APP_DB_NAME}"

echo ""
echo "🔧 rsync-ai staging Azure DB setup"
echo "======================================="
echo "  Server   : $AZURE_HOST"
echo "  Creating : $APP_DB_NAME, $TEMPORAL_DB_NAME, $TEMPORAL_VIS_DB_NAME"
echo "  Prod DBs ($PROD_DB_NAME / temporal / temporal_visibility) are NOT touched."
echo ""

# ─── Create a database if missing (idempotent) ───────────────────────────────
create_db() {
  local name="$1"
  local exists
  exists=$(psql_run "$ADMIN_URL" -tAc \
    "SELECT 1 FROM pg_database WHERE datname='$name'" 2>/dev/null | tr -d '[:space:]' || echo "")
  if [[ "$exists" == "1" ]]; then
    echo "   ✅ $name already exists — skipping"
  else
    psql_run "$ADMIN_URL" -c "CREATE DATABASE $name;" >/dev/null
    echo "   ✅ $name created"
  fi
}

echo "📦 Ensuring isolated databases exist on Azure..."
create_db "$APP_DB_NAME"
create_db "$TEMPORAL_DB_NAME"
create_db "$TEMPORAL_VIS_DB_NAME"

# ─── Patch .env.staging ───────────────────────────────────────────────────
echo ""
echo "📝 Patching .env.staging (TLS on, app DB → Azure $APP_DB_NAME)..."

set_kv() {  # set_kv KEY VALUE  — replace existing line or append
  local key="$1" val="$2"
  if grep -q "^$key=" "$LOCAL_ENV"; then
    sed -i '' "s|^$key=.*|$key=$val|" "$LOCAL_ENV"
  else
    printf '%s=%s\n' "$key" "$val" >> "$LOCAL_ENV"
  fi
}

set_kv DATABASE_URL "$APP_DB_URL"
set_kv POSTGRES_HOST "$AZURE_HOST"
set_kv POSTGRES_USER "$AZURE_USER"
set_kv POSTGRES_PASSWORD "$AZURE_PASS"
set_kv POSTGRES_PORT "$AZURE_PORT"
set_kv POSTGRES_DB "$APP_DB_NAME"
set_kv DB_SSLMODE require
set_kv POSTGRES_SSLMODE require
set_kv POSTGRES_TLS_ENABLED true          # Azure requires TLS (app + Temporal)
set_kv POSTGRES_TLS_DISABLE_HOST_VERIFICATION true

# INTERNAL_SERVICE_SECRET authenticates service-to-service calls (api-gateway →
# orchestrator: connection tests, OAuth refresh). Empty ⇒ the orchestrator
# (ENVIRONMENT=production) fails requirePrincipal closed and every internal call
# 401s. Generate once if absent; NEVER overwrite an existing value — rotating it
# would 401 in-flight S2S until all services are recreated together. `.+` matches
# only a non-empty value, so a bare `INTERNAL_SERVICE_SECRET=` is (re)generated.
if grep -qE '^INTERNAL_SERVICE_SECRET=.+' "$LOCAL_ENV"; then
  echo "   ✅ INTERNAL_SERVICE_SECRET → kept existing"
else
  set_kv INTERNAL_SERVICE_SECRET "$(openssl rand -hex 32)"
  echo "   ✅ INTERNAL_SERVICE_SECRET → generated"
fi

echo "   ✅ DATABASE_URL → Azure $APP_DB_NAME"
echo "   ✅ POSTGRES_TLS_ENABLED → true (Temporal connects to Azure over TLS)"
echo "   ✅ Temporal DBs wired via docker-compose.staging.yml (DBNAME / VISIBILITY_DBNAME)"

echo ""
echo "✅ Done! staging is a full Azure mirror — no local postgres."
echo ""
echo "Start the stack:"
echo ""
echo "  docker compose \\"
echo "    -f docker-compose.yml \\"
echo "    -f docker-compose.prod.yml \\"
echo "    -f docker-compose.staging.yml \\"
echo "    --env-file .env.staging \\"
echo "    up -d"
echo ""
echo "Verify app DB (after api-gateway starts):"
echo "  docker run --rm postgres:16 psql \"$APP_DB_URL\" -c '\\dt'"
