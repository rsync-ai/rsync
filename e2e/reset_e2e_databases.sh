#!/usr/bin/env bash
set -euo pipefail

# Wipes E2E MySQL/Postgres data WITHOUT removing volumes.
# This avoids re-running old init SQL that seeds demo tables, and leaves you with a clean DB
# ready for realistic fintech/logistics fixtures.
#
# Targets:
# - mysql-e2e  (rsync-ai-mysql-e2e)    : drops/recreates e2e_db
# - postgres-e2e (rsync-ai-postgres-e2e): drops/recreates e2e_db
#
# Safe-by-default:
# - Only operates on the rsync-ai-e2e compose project containers.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

log() { printf "\n==> %s\n" "$*"; }
die() { echo "❌ $*" >&2; exit 1; }

MYSQL_CONTAINER="${MYSQL_CONTAINER:-rsync-ai-mysql-e2e}"
MYSQL_ROOT_PW="${MYSQL_ROOT_PW:-rootpassword}"
MYSQL_DB="${MYSQL_DB:-e2e_db}"
MYSQL_USER="${MYSQL_USER:-e2e_user}"
MYSQL_PW="${MYSQL_PW:-e2e_password}"

PG_CONTAINER="${PG_CONTAINER:-rsync-ai-postgres-e2e}"
PG_DB="${PG_DB:-e2e_db}"
PG_ADMIN_USER="${PG_ADMIN_USER:-e2e_user}"

log "🔎 Verifying E2E containers exist…"
docker inspect "${MYSQL_CONTAINER}" >/dev/null 2>&1 || die "Missing MySQL container: ${MYSQL_CONTAINER} (run e2e/start.sh first)"
docker inspect "${PG_CONTAINER}" >/dev/null 2>&1 || die "Missing Postgres container: ${PG_CONTAINER} (run e2e/start.sh first)"

log "🧨 Resetting MySQL database '${MYSQL_DB}' (drop + recreate)…"
docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PW}" -e "
  SET sql_notes = 0;
  DROP DATABASE IF EXISTS \`${MYSQL_DB}\`;
  CREATE DATABASE \`${MYSQL_DB}\`;
  -- Ensure app user exists and has privileges (best-effort).
  CREATE USER IF NOT EXISTS '${MYSQL_USER}'@'%' IDENTIFIED BY '${MYSQL_PW}';
  GRANT ALL PRIVILEGES ON \`${MYSQL_DB}\`.* TO '${MYSQL_USER}'@'%';
  -- Debezium snapshotting + binlog streaming privileges (kept for CDC tests).
  GRANT RELOAD, FLUSH_TABLES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '${MYSQL_USER}'@'%';
  FLUSH PRIVILEGES;
  SET sql_notes = 1;
"

log "🧨 Resetting Postgres database '${PG_DB}' (drop + recreate)…"
# Must connect to a different DB to drop target DB.
docker exec "${PG_CONTAINER}" psql -U "${PG_ADMIN_USER}" -d postgres -v ON_ERROR_STOP=1 -c "
  SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
  WHERE datname = '${PG_DB}' AND pid <> pg_backend_pid();
"
docker exec "${PG_CONTAINER}" psql -U "${PG_ADMIN_USER}" -d postgres -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS ${PG_DB};"
docker exec "${PG_CONTAINER}" psql -U "${PG_ADMIN_USER}" -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE ${PG_DB};"

log "✅ E2E databases wiped. Next step: load fintech/logistics fixtures."

