#!/usr/bin/env bash
# Every ${VAR:?...} in a shipped compose file must be satisfiable from the template
# an operator is told to copy.
#
# Why this exists: a single `:?` aborts interpolation for the ENTIRE merged config,
# so one variable missing from a template does not degrade that one service — it
# means nothing starts at all. That is exactly what shipped: docker-compose.prod.yml
# required MINIO_ACCESS_KEY_ID / MINIO_SECRET_ACCESS_KEY while .env.prod.example (the
# file scripts/deploy-service.sh templates .env.prod from) named neither, so standing
# up a new host from the shipped template rendered nothing. Same gap in
# .env.staging.example. Nobody noticed because a real .env on an existing host has
# the variables; only a FRESH stand-up hits it — i.e. precisely a cloud migration.
#
# NON-EMPTY is the discriminating check, not mere presence: `:?` rejects unset OR
# empty, so `MINIO_ACCESS_KEY_ID=` in a template is still a broken stand-up.
#
# Deliberately does not run `docker compose config`: this must pass on any runner,
# with or without a Docker daemon, in under a second.
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
note() { printf '  %s\n' "$*"; }

# Collect every ${VAR:?} across the given compose files.
# `|| true`: grep exits 1 on no-match, which under `set -o pipefail` would abort the
# script on a composition that legitimately has zero :?-required vars (the base compose).
# Zero is a real answer here, not an error — check_pair reports it as vacuous.
required_vars() {
  { grep -ohE '\$\{[A-Z_][A-Z0-9_]*:\?' "$@" || true; } | sed 's/^\${//; s/:?$//' | sort -u
}

# A template satisfies VAR only with a non-empty assignment.
satisfied_by_template() { grep -qE "^${1}=.+" "$2"; }

# ── The BYO-Kafka set ────────────────────────────────────────────────────────
# A second, weaker obligation on the same templates, and it needs a different
# predicate than the one above.
#
# Every BYO-Kafka variable is `${VAR:-}` — optional by construction, because
# unset must keep meaning "the bundled single-node PLAINTEXT broker". So none of
# them is ever `:?`-required and required_vars() above can never see one: a
# template naming not a single Kafka variable passes that check with full marks.
# That is exactly what shipped — 0 KAFKA_* lines in all three templates while
# the anchor carried 17 keys — and the cost is not a stack that fails to render,
# it is a self-hoster who cannot discover that the settings exist.
#
# The obligation is therefore DOCUMENTED, not assigned: a commented
# `# KAFKA_SASL_PASSWORD=...` line is the correct shape here, and an uncommented
# one would change local-dev behaviour. Hence a predicate that accepts either.
documented_by_template() { grep -qE "^[[:space:]]*#?[[:space:]]*${1}=" "$2"; }

# Extracted from the x-kafka-security anchor rather than listed here, so adding
# a key to the anchor adds it to this guard in the same commit. A hand-copied
# list is a second place to forget.
anchor_kafka_vars() {
  sed -n '/^x-kafka-security:/,/^services:/p' "$1" \
    | grep -oE '^  KAFKA_[A-Z0-9_]+:' | sed 's/^  //; s/:$//' | sort -u
}

# Three BYO variables live outside the anchor because they are not client
# security: the broker ADDRESS is declared per-service (docker-compose.yml:1001,
# :1180), and the two durability knobs are read only by the topic bootstrapper
# and the orchestrator (:255-256, :1022-1023). They are named literally, and the
# self-test below proves each one is really in a shipped compose file — a
# literal that has gone stale would otherwise demand a template document a
# variable nothing reads.
NON_ANCHOR_BYO_VARS="KAFKA_BROKERS KAFKA_REPLICATION_FACTOR KAFKA_MIN_INSYNC_REPLICAS"

byo_kafka_vars() {
  { anchor_kafka_vars docker-compose.yml; printf '%s\n' $NON_ANCHOR_BYO_VARS; } | sort -u
}

check_byo_kafka() {
  local template="$1" missing=() v
  printf '%s\n' "== BYO-Kafka set documented in ${template}"
  for v in $(byo_kafka_vars); do
    documented_by_template "$v" "$template" || missing+=("$v")
  done
  local n; n=$(byo_kafka_vars | wc -l | tr -d ' ')
  if [ ${#missing[@]} -gt 0 ]; then
    note "NOT DOCUMENTED in ${template}: ${missing[*]}"
    note "-> a self-hoster pointing this stack at MSK/Confluent/Redpanda cannot"
    note "   discover these from the file they were told to copy. Unset does not"
    note "   fail: it silently connects PLAINTEXT and anonymous."
    note "   Add them COMMENTED (# VAR=example) — an uncommented value would"
    note "   change local-dev behaviour."
    fail=1
  else
    note "ok — all ${n} BYO-Kafka vars documented (commented or set)"
  fi
  echo
}

check_pair() {
  local label="$1" template="$2"; shift 2
  local missing=() v
  printf '%s\n' "== ${label}"
  note "compose:  $*"
  note "template: ${template}"
  for v in $(required_vars "$@"); do
    satisfied_by_template "$v" "$template" || missing+=("$v")
  done
  local n; n=$(required_vars "$@" | wc -l | tr -d ' ')
  if [ ${#missing[@]} -gt 0 ]; then
    note "MISSING (or empty) in ${template}: ${missing[*]}"
    note "-> a fresh stand-up from this template renders NOTHING, not just these services."
    fail=1
  elif [ "$n" -eq 0 ]; then
    note "vacuous — this composition has no :?-required vars, so nothing was actually checked"
  else
    note "ok — all ${n} required vars present and non-empty"
  fi
  echo
}

# docker-compose.quickstart.yml has no template: install.sh GENERATES the .env it
# writes, so the guard is that install.sh emits every required var with a value.
check_installer() {
  local missing=() v
  printf '%s\n' "== quickstart (install.sh generates its own .env)"
  for v in $(required_vars docker-compose.quickstart.yml); do
    grep -qE "^${v}=.+" install.sh || missing+=("$v")
  done
  if [ ${#missing[@]} -gt 0 ]; then
    note "install.sh never writes: ${missing[*]}"
    note "-> the OSS one-liner installer produces a stack that cannot render."
    fail=1
  else
    note "ok — install.sh writes every :?-required quickstart var"
  fi
  echo
}

# Self-test FIRST. If required_vars ever stops matching — a compose refactor, a quoting
# change, a renamed file — every pair below passes vacuously and this guard silently
# becomes decoration. An empty set is the failure mode most likely to read as a pass.
total=$(required_vars $(ls docker-compose*.yml) | wc -l | tr -d ' ')
if [ "$total" -lt 5 ]; then
  echo "FAIL self-test: found only ${total} :?-required vars across all compose files."
  echo "  Expected >=5. Either the extractor broke or the fail-closed guards were removed;"
  echo "  both make every check below meaningless. Refusing to report a pass."
  exit 1
fi
echo "self-test: extractor finds ${total} :?-required vars across $(ls docker-compose*.yml | wc -l | tr -d ' ') compose files"

# Same reasoning for the BYO set, which has its own extractor and so its own way
# to silently return nothing. Three separate arms, because each fails differently:
#
#   1. COUNT. `sed` over an anchor that was renamed or reindented yields zero
#      matches and no error, and an empty set makes check_byo_kafka report "ok —
#      all 0 vars documented" on a template naming none of them.
#   2. SENTINEL. A count can be met by keys that are not the ones that matter, so
#      pin it to a name whose absence is unambiguous: without KAFKA_SASL_PASSWORD
#      the anchor is not the SASL anchor whatever else it holds.
#   3. THE LITERALS. NON_ANCHOR_BYO_VARS is the one hand-written list here. Each
#      entry must appear in a shipped compose file, or this guard is demanding
#      that templates document a variable the platform no longer reads.
byo_total=$(byo_kafka_vars | wc -l | tr -d ' ')
if [ "$byo_total" -lt 12 ]; then
  echo "FAIL self-test: the x-kafka-security extractor found only ${byo_total} BYO vars."
  echo "  Expected >=12. The anchor was probably renamed, reindented, or removed;"
  echo "  an empty set makes every BYO check below pass vacuously. Refusing to report a pass."
  exit 1
fi
if ! byo_kafka_vars | grep -qx KAFKA_SASL_PASSWORD; then
  echo "FAIL self-test: the BYO set does not contain KAFKA_SASL_PASSWORD."
  echo "  Whatever the extractor matched, it is not the SASL security anchor."
  exit 1
fi
for v in $NON_ANCHOR_BYO_VARS; do
  if ! grep -qE "(^|[^A-Z_])${v}[:=]" docker-compose.yml docker-compose.quickstart.yml; then
    echo "FAIL self-test: ${v} is in NON_ANCHOR_BYO_VARS but no shipped compose file reads it."
    echo "  Either it was renamed in compose, or this literal is stale — fix the list,"
    echo "  do not make the templates document a variable nothing consumes."
    exit 1
  fi
done
echo "self-test: BYO-Kafka set is ${byo_total} vars ($(anchor_kafka_vars docker-compose.yml | wc -l | tr -d ' ') from the x-kafka-security anchor + $(printf '%s\n' $NON_ANCHOR_BYO_VARS | wc -l | tr -d ' ') declared per-service)"
echo

check_pair "prod (scripts/deploy-service.sh)"  .env.prod.example \
  docker-compose.yml docker-compose.prod.yml
check_pair "staging (scripts/staging-up.sh)"   .env.staging.example \
  docker-compose.yml docker-compose.prod.yml docker-compose.staging.yml
check_pair "local dev (docs/getting-started/quickstart.md)" .env.example \
  docker-compose.yml
check_installer

check_byo_kafka .env.prod.example
check_byo_kafka .env.staging.example
check_byo_kafka .env.example

if [ "$fail" -ne 0 ]; then
  echo "FAIL: at least one shipped compose file requires a variable its template does not supply."
  exit 1
fi
echo "PASS: every :?-required variable is supplied by its template."
