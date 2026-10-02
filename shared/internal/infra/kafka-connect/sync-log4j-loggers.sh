#!/bin/bash
# Applies rsync.ai's logger levels (log4j-loggers.properties, next to this script) to
# the Kafka Connect worker's /kafka/config/log4j.properties at every start.
#
# Why at start and not at build: /kafka/config is a VOLUME in the debezium/connect
# base image, and Docker copies an image's files into a volume only when it creates
# that volume. compose's `up --force-recreate` keeps the anonymous volume, so a host
# runs the log4j.properties of the FIRST kafka-connect image it ever started, and a
# level a later image bakes into /kafka/config never reaches it. The base
# /docker-entrypoint.sh does not help: it seeds config from config.orig with `cp -n`,
# which skips a file that exists (and config.orig's log4j.properties is the BROKER's
# file anyway, with appenders the worker does not define). #1198 baked
# PgOutputMessageDecoder=ERROR into the image and prod still logged 1,136 of its
# WARNs after the 2026-09-25 deploy.
#
# Each logger in log4j-loggers.properties that the live file does not set is
# appended. A logger the live file already sets is left alone: an operator's level
# wins, and a CONNECT_LOG4J_* variable still applies afterwards, because the base
# entrypoint rewrites the line this appends. To override one of rsync.ai's levels, set
# the logger to another level -- deleting its line brings it back at the next start.
# Idempotent.
#
# Never stops the worker: a missing or read-only file skips the sync with a note.
# A missing live file is left to the base entrypoint to seed; creating it here
# would stop that seed and leave the worker with no root logger.
# Pinned by llm-service/tests/test_connect_log4j_logger_sync.py.
set -uo pipefail

src="$(dirname "$0")/log4j-loggers.properties"
dst="${KAFKA_HOME:-/kafka}/config/log4j.properties"

[ -f "$src" ] && [ -f "$dst" ] || exit 0
if [ ! -w "$dst" ]; then
  echo "rsync log4j sync: $dst is not writable; rsync's logger levels were not applied" >&2
  exit 0
fi

# The name a properties line sets, or nothing for a comment or a line without a separator.
key_of() {
  local line="$1"
  line="${line#"${line%%[![:space:]]*}"}"
  case "$line" in ''|'#'*|'!'*) return ;; esac
  case "$line" in *[=:]*) ;; *) return ;; esac
  line="${line%%[=:]*}"
  printf '%s' "${line%"${line##*[![:space:]]}"}"
}

# Every name the live file sets, one per line, read once.
live_keys="$(while IFS= read -r l || [ -n "$l" ]; do k="$(key_of "$l")"; [ -n "$k" ] && printf '%s\n' "$k"; done < "$dst")"

added=()
while IFS= read -r l || [ -n "$l" ]; do
  k="$(key_of "$l")"
  case "$k" in log4j.logger.?*) ;; *) continue ;; esac
  case $'\n'"$live_keys"$'\n' in *$'\n'"$k"$'\n'*) continue ;; esac
  added+=("${l#"${l%%[![:space:]]*}"}")
  live_keys="$live_keys"$'\n'"$k"
done < "$src"

[ "${#added[@]}" -gt 0 ] || exit 0
{
  # The live file may not end in a newline; an append must not join its last line.
  [ -z "$(tail -c1 "$dst")" ] || printf '\n'
  printf '%s\n' "${added[@]}"
} >> "$dst" || {
  echo "rsync log4j sync: could not append to $dst; rsync's logger levels were not applied" >&2
  exit 0
}
printf 'rsync log4j sync: set %s logger level(s) in %s: %s\n' \
  "${#added[@]}" "$dst" "$(printf '%s ' "${added[@]}")" >&2
