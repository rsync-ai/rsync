#!/usr/bin/env bash
set -euo pipefail

log() { printf "\n==> %s\n" "$*"; }
die() { echo "❌ $*" >&2; exit 1; }

MYSQL_CONTAINER="${MYSQL_CONTAINER:-rsync-ai-mysql-e2e}"
PG_CONTAINER="${PG_CONTAINER:-rsync-ai-postgres-e2e}"

# Use MySQL root for fixture loading (needs CREATE DATABASE), then fixtures grant access back to e2e_user.
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-rootpassword}"

PG_USER="${PG_USER:-e2e_user}"
PG_DB="${PG_DB:-e2e_db}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MYSQL_FIXTURE="${ROOT_DIR}/e2e/mysql/fixtures/fintech_logistics.sql"
PG_FIXTURE="${ROOT_DIR}/e2e/postgres/fixtures/warehouse.sql"

docker inspect "${MYSQL_CONTAINER}" >/dev/null 2>&1 || die "Missing MySQL container '${MYSQL_CONTAINER}'"
docker inspect "${PG_CONTAINER}" >/dev/null 2>&1 || die "Missing Postgres container '${PG_CONTAINER}'"

log "🧱 Loading MySQL fintech/logistics fixtures into ${MYSQL_CONTAINER}…"
docker exec -i "${MYSQL_CONTAINER}" mysql -u "${MYSQL_USER}" -p"${MYSQL_PASSWORD}" < "${MYSQL_FIXTURE}"

log "🧱 Loading Postgres warehouse destination tables into ${PG_CONTAINER}/${PG_DB}…"
docker exec -i "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -v ON_ERROR_STOP=1 < "${PG_FIXTURE}"

log "✅ Fixtures loaded."

