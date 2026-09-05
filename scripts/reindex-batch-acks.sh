#!/usr/bin/env bash
# reindex-batch-acks.sh — measure, and optionally reclaim, btree bloat on
# pipeline_batch_acks. (F-BATCHACKS-REINDEX-CADENCE)
#
# Why this exists:
#   On 2026-08-19 this one table's indexes held 66 MB against ONE live row, and
#   were 77% of the entire prod database. The cause is structural, not a bug:
#   the kafka-sink worker inserts an ack per batch and deletes it once the batch
#   is acknowledged, so the table churns hard and stays tiny. Autovacuum reclaims
#   the heap fine. It cannot reclaim a btree — emptied index pages go onto the
#   index's own free list and are reused by that index, never returned to the OS.
#   Only REINDEX gives the space back. A REINDEX took the indexes to 104 kB and
#   the database from 86 MB to 20 MB, in 0.6 seconds.
#
#   And it WILL come back. unique_batch_ack_kafka had 1.65M scans and the same
#   number of insert/delete cycles behind them; the growth is the workload, not
#   an incident. So this is a cadence, not a one-off fix.
#
# Why a repo script and not a crontab on the VM:
#   A crontab lives on one machine and dies with it. This has to survive the
#   infrastructure being rebuilt, so it lives in the repo and the runbook points
#   at it. Wire it to whatever scheduler the new environment has; nothing here
#   assumes one.
#
# Usage (run on the prod VM, from anywhere):
#   scripts/reindex-batch-acks.sh              # measure and report — changes nothing
#   scripts/reindex-batch-acks.sh --apply      # reindex, but only if over threshold
#   scripts/reindex-batch-acks.sh --apply --force   # reindex regardless of size
#
# Environment:
#   ENV_FILE        path to the env file holding DATABASE_URL (default /root/rsync-ai/.env.prod)
#   THRESHOLD_MB    reindex at or above this total index size (default 16)
#   PG_IMAGE        postgres image to run psql from (default postgres:16-alpine)
#
# Exit codes: 0 = healthy, or reindexed and verified · 1 = over threshold and not
# applied (report mode) · 2 = setup error, pre-flight refusal, or the database could
# not be reached · 3 = the reindex failed, or ran and verification FAILED.
#
# Those four are the interface, so no other status may escape. A docker daemon that is
# not running exits 125 on its own, and 1 already means "over threshold" — every
# database call therefore goes through run_query, which maps any such failure to 2.
#
# The credential never reaches a command line. DATABASE_URL is passed into the
# container through --env-file and dereferenced INSIDE the container by a
# single-quoted shell string, so it never appears in this host's process table,
# shell history, or any log line this script writes.

set -euo pipefail

ENV_FILE="${ENV_FILE:-/root/rsync-ai/.env.prod}"
THRESHOLD_MB="${THRESHOLD_MB:-16}"
PG_IMAGE="${PG_IMAGE:-postgres:16-alpine}"

APPLY=0
FORCE=0
for arg in "$@"; do
  case "$arg" in
    --apply) APPLY=1 ;;
    --force) FORCE=1 ;;
    # Range ends at the `set` line rather than a fixed number, so editing the header
    # above cannot silently truncate --help.
    -h|--help) sed -n '2,/^set -euo/p' "$0" | sed '$d'; exit 0 ;;
    # The rejected argument is deliberately NOT echoed. The most likely thing an
    # operator types by mistake here is a connection string — reaching for the psql
    # habit of passing one positionally — and echoing it would put the prod
    # credential into terminal scrollback, CI logs and any scheduler that captures
    # stderr. That is the one failure mode this whole script is written to avoid.
    *) echo "ERROR: unrecognised argument. Run with --help for usage." >&2; exit 2 ;;
  esac
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found — run this on the prod VM, or set ENV_FILE=..." >&2
  exit 2
fi

# One helper for every database call, so the credential-handling rule is obeyed in
# exactly one place instead of being re-derived at each call site. --rm is not
# optional: these containers must not accumulate on the host.
#
# The field separator is ~ rather than the psql default |, because several of the
# values below come from pg_size_pretty and contain a space ("104 kB"). Splitting
# those on whitespace silently shifts every later field by one.
psql_q() {
  local sql="$1"
  sudo docker run --rm --network host --env-file "$ENV_FILE" "$PG_IMAGE" \
    sh -c 'psql "$DATABASE_URL" -qtAX -F"~" -c "$1"' sh "$sql"
}

# run_query <varname> <sql> — run a query and assign the result, or exit 2.
#
# Every read below goes through this rather than through `VAR="$(psql_q ...)"`, for
# two reasons that both end in a wrong answer rather than an error.
#
# The first is `set -e`. A here-string built from a command substitution —
#   IFS='~' read -r A B <<<"$(psql_q ...)"
# — reports `read`'s exit status, not the query's. read succeeds on an empty string,
# so a database that cannot be reached produces empty variables and a zero status,
# errexit never fires, and `[[ "" -lt 16777216 ]]` evaluates the empty string as 0 and
# reports the indexes are under threshold. A scheduler reading exit codes is then told
# every quarter that a database it never contacted is healthy.
#
# The second is that even where errexit does fire, it exits with whatever psql or
# docker returned — 1, 125, 126, 127 — and this script publishes 0/1/2/3 as an
# interface. A docker daemon that is not running must not exit 1, because 1 already
# means "over threshold, re-run with --apply".
#
# printf -v assigns in the caller's scope, so this runs as a plain command and its
# exit really does end the script. Called inside "$(...)" it would only end a subshell.
run_query() {
  local __var="$1" __out
  if ! __out="$(psql_q "$2")"; then
    echo "ERROR: database query failed — could not reach the database, or it refused" >&2
    echo "       the query. Check the docker daemon, ENV_FILE ($ENV_FILE) and the" >&2
    echo "       DATABASE_URL inside it." >&2
    exit 2
  fi
  printf -v "$__var" '%s' "$__out"
}

echo "=== pipeline_batch_acks index bloat check ==="
echo

# -------------------------------------------------------------------------
# Measure
# -------------------------------------------------------------------------
run_query MEASURE "
  SELECT (SELECT count(*) FROM pipeline_batch_acks),
         pg_indexes_size('pipeline_batch_acks'),
         pg_size_pretty(pg_indexes_size('pipeline_batch_acks')),
         pg_size_pretty(pg_database_size(current_database()))
"
# `|| true` because read returns non-zero at EOF without a trailing delimiter, and
# under errexit that would exit 1 — which this script publishes as "over threshold".
# The check below is what actually decides whether the read produced anything usable.
IFS='~' read -r LIVE_ROWS INDEX_BYTES INDEX_PRETTY DB_PRETTY <<<"$MEASURE" || true

# The threshold comparison is arithmetic, and [[ ]] evaluates a non-numeric string as
# 0 rather than complaining. So an unparseable size does not fail loudly, it reports
# "under threshold, nothing to do" — the answer that looks like good news. Assert the
# shape before anything is decided from it.
if [[ ! "$INDEX_BYTES" =~ ^[0-9]+$ ]]; then
  echo "ERROR: could not read the index size for pipeline_batch_acks. Got an" >&2
  echo "       unexpected result shape from the database; refusing to guess." >&2
  exit 2
fi

echo "live rows        : ${LIVE_ROWS}"
echo "index size       : ${INDEX_PRETTY} (${INDEX_BYTES} bytes)"
echo "database size    : ${DB_PRETTY}"
echo "threshold        : ${THRESHOLD_MB} MB"
echo
echo "--- per-index ---"
psql_q "
  SELECT rpad(indexrelname, 32) || ' ' ||
         lpad(pg_size_pretty(pg_relation_size(indexrelid)), 10) ||
         '  scans=' || COALESCE(idx_scan::text, 'never')
  FROM pg_stat_user_indexes
  WHERE relname = 'pipeline_batch_acks'
  ORDER BY pg_relation_size(indexrelid) DESC
"
echo

# Validated before it reaches $(( )), which evaluates its operand as a shell
# expression rather than as a number: THRESHOLD_MB comes from the environment, and an
# environment variable that lands inside arithmetic is a place where a value can turn
# into something executed.
if [[ ! "$THRESHOLD_MB" =~ ^[0-9]+$ ]]; then
  echo "ERROR: THRESHOLD_MB must be a whole number of megabytes." >&2
  exit 2
fi
THRESHOLD_BYTES=$(( THRESHOLD_MB * 1024 * 1024 ))

if [[ "$INDEX_BYTES" -lt "$THRESHOLD_BYTES" && "$FORCE" -ne 1 ]]; then
  echo "OK: under threshold. Nothing to do."
  exit 0
fi

if [[ "$APPLY" -ne 1 ]]; then
  echo "OVER THRESHOLD — re-run with --apply to reclaim it."
  exit 1
fi

# -------------------------------------------------------------------------
# Pre-flight: REINDEX CONCURRENTLY waits for transactions older than itself, and
# a run that fails or is cancelled leaves behind invalid *_ccnew indexes that
# still cost writes. Refusing to start while a long transaction is open is much
# cheaper than cleaning that up.
# -------------------------------------------------------------------------
run_query LONG_TX "
  SELECT count(*) FROM pg_stat_activity
  WHERE state <> 'idle'
    AND backend_type = 'client backend'
    AND pid <> pg_backend_pid()
    AND xact_start < now() - interval '30 seconds'
"
if [[ "$LONG_TX" != "0" ]]; then
  echo "ERROR: ${LONG_TX} transaction(s) open for over 30s. REINDEX CONCURRENTLY would" >&2
  echo "       wait on them and may fail, leaving invalid _ccnew indexes. Retry later." >&2
  exit 2
fi

run_query LEFTOVERS "SELECT count(*) FROM pg_class WHERE relname LIKE '%_ccnew%' OR relname LIKE '%_ccold%'"
if [[ "$LEFTOVERS" != "0" ]]; then
  echo "ERROR: ${LEFTOVERS} leftover _ccnew/_ccold relation(s) from an earlier interrupted" >&2
  echo "       REINDEX. Investigate and drop them before running another." >&2
  exit 2
fi

# -------------------------------------------------------------------------
# Apply. CONCURRENTLY so the sink keeps writing; it cannot run inside a
# transaction block, which is why this is a bare psql -c and not a migration.
# -------------------------------------------------------------------------
echo "reindexing..."

# Failure here does NOT end the script, and that is the whole reason this is written
# out rather than left as a bare call. Under `set -e` a failing REINDEX exits
# immediately, which skips the verification below — in the one run where it matters. A
# REINDEX CONCURRENTLY that fails partway is precisely the case that leaves invalid
# *_ccnew indexes behind, costing writes and answering nothing, and nothing else in
# the stack reports them. So the failure is recorded and the checks still run.
REINDEX_FAILED=0
if ! psql_q "REINDEX TABLE CONCURRENTLY pipeline_batch_acks"; then
  REINDEX_FAILED=1
  echo "ERROR: REINDEX did not complete. Checking what it left behind." >&2
fi

# -------------------------------------------------------------------------
# Verify. A REINDEX CONCURRENTLY that half-finished leaves indexes that exist but
# do not answer queries, and nothing else in the system will tell you. Both flags
# matter and they are not the same: indisvalid=false means the planner ignores
# it, indisready=false means writes do not maintain it. An index that is ready
# but not valid is a silently unused index; the reverse is a silently stale one.
# -------------------------------------------------------------------------
run_query BAD "
  SELECT count(*) FROM pg_index i
  JOIN pg_class c ON c.oid = i.indrelid
  WHERE c.relname = 'pipeline_batch_acks'
    AND NOT (i.indisvalid AND i.indisready)
"
run_query LEFTOVERS "SELECT count(*) FROM pg_class WHERE relname LIKE '%_ccnew%' OR relname LIKE '%_ccold%'"

run_query AFTER "
  SELECT pg_size_pretty(pg_indexes_size('pipeline_batch_acks')),
         pg_size_pretty(pg_database_size(current_database()))
"
IFS='~' read -r NEW_PRETTY NEW_DB <<<"$AFTER" || true

echo
echo "index size       : ${INDEX_PRETTY} -> ${NEW_PRETTY}"
echo "database size    : ${DB_PRETTY} -> ${NEW_DB}"
echo "invalid/not-ready: ${BAD}"
echo "_ccnew/_ccold    : ${LEFTOVERS}"

if [[ "$REINDEX_FAILED" -ne 0 || "$BAD" != "0" || "$LEFTOVERS" != "0" ]]; then
  echo
  echo "VERIFICATION FAILED — do not treat this run as successful. Inspect pg_index" >&2
  echo "for pipeline_batch_acks before the next sink deploy." >&2
  exit 3
fi

echo
echo "OK: reindexed and verified."
