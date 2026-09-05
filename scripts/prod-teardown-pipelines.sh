#!/usr/bin/env bash
#
# prod-teardown-pipelines.sh — Phase 0 steps 1-3 of the BYO-Kafka / EKS plan.
#
# Deletes every pipeline row, stops the Kafka-touching containers together, and
# clears the Kafka topic/consumer-group state so the stack can come back up on the
# `rsync.` namespace with a clean offset topic.
#
# DRY RUN IS THE DEFAULT. Nothing is deleted, stopped, or removed unless you pass
# BOTH `--execute` AND `--confirm=<token>`; the token is printed by the dry run and
# embeds the target database name, so a token minted against staging cannot execute
# against prod.
#
# Run it on the prod VM from the compose directory:
#     sudo bash scripts/prod-teardown-pipelines.sh --dry-run
#
# Or feed it over ssh stdin (keeps SQL quoting intact — see the runbook):
#     ssh -i <key> azureuser@<host> 'sudo bash -s -- --dry-run' < scripts/prod-teardown-pipelines.sh
#
# Ordering rationale (do not reshuffle):
#   1. Kafka Connect connectors are deleted FIRST, while Connect is still up. A
#      connector that outlives its pipeline resumes on restart against a
#      replication slot and topics that no longer exist.
#   2. Pipelines are deleted SECOND. 22 of the 23 FK children cascade;
#      `cdc_resources` is ON DELETE SET NULL, so its rows survive the delete and
#      the publications/slots they name leak on the SOURCE databases. The script
#      refuses to continue while that table is non-empty unless you assert the
#      source-side drop is done.
#   3. Containers are stopped THIRD, producers before the broker.
#   4. Topic state is cleared LAST. `--kafka=volume` (default) drops the Kafka
#      data volume: this is the ONLY moment `offsets.topic.num.partitions` can be
#      changed, and it also clears Connect's internal `_rsync-connect-*` trio and
#      the contents of `__consumer_offsets`. `--kafka=topics` deletes topics and
#      groups in place instead, leaving the offset topic's partition count fixed.
#
set -euo pipefail

# ---------------------------------------------------------------- configuration
PG_IMAGE="${PG_IMAGE:-postgres:16-alpine}"      # no Postgres container on prod
DOCKER_NET="${DOCKER_NET:-host}"                # prod: host net reaches Azure PG
ENV_FILE="${ENV_FILE:-.env.prod}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
MAX_PIPELINES="${MAX_PIPELINES:-25}"            # wrong-database tripwire
KAFKA_MODE="volume"                             # volume | topics
MODE="dry-run"
CONFIRM_TOKEN=""
CDC_SOURCE_DROPPED="no"

# Containers that produce to, consume from, or administer Kafka. Stopped together
# so nothing re-creates a topic between the delete and the restart. The broker
# itself is deliberately NOT in this list — it is stopped separately, after these.
KAFKA_CLIENT_CONTAINERS=(
    rsync-ai-api-gateway
    rsync-ai-orchestrator
    rsync-ai-temporal-adapter
    rsync-ai-planner
    kafka-connect
    rsync-ai-debezium-v1-0-0-mcp
    rsync-ai-kafka-mcp-sink-v1-0-0-mcp
    rsync-ai-kafka-init
)
KAFKA_BROKER_CANDIDATES=(rsync-ai-kafka rsync-kafka kafka)

usage() {
    cat <<'USAGE'
Usage: prod-teardown-pipelines.sh [--dry-run|--execute] [options]

  --dry-run                 Default. Reports current state and what would change.
  --execute                 Perform the teardown. Requires --confirm=<token>.
  --confirm=<token>         Token printed by the dry run; embeds the database name.
  --kafka=volume|topics     volume (default): drop the Kafka data volume.
                            topics: delete topics + consumer groups in place.
  --cdc-dropped-on-source   Assert that every publication and replication slot
                            named in cdc_resources has been dropped on its SOURCE
                            database. Only needed when cdc_resources is non-empty.
  --max-pipelines=N         Abort if more than N pipelines exist (default 25).
  -h, --help                This message.
USAGE
}

for arg in "$@"; do
    case "$arg" in
        --dry-run)                MODE="dry-run" ;;
        --execute)                MODE="execute" ;;
        --confirm=*)              CONFIRM_TOKEN="${arg#*=}" ;;
        --kafka=volume|--kafka=topics) KAFKA_MODE="${arg#*=}" ;;
        --cdc-dropped-on-source)  CDC_SOURCE_DROPPED="yes" ;;
        --max-pipelines=*)        MAX_PIPELINES="${arg#*=}" ;;
        -h|--help)                usage; exit 0 ;;
        *) echo "❌ unknown argument: $arg" >&2; usage >&2; exit 2 ;;
    esac
done

hr()   { printf '%s\n' "────────────────────────────────────────────────────────────────────────"; }
step() { hr; printf '  %s\n' "$*"; hr; }
say()  { printf '  %s\n' "$*"; }
plan() { printf '  [DRY RUN] would %s\n' "$*"; }

# `psql` is fed over stdin and the credential is forwarded through the environment
# (`-e PGURL` with no value), so neither the SQL nor the password ever appears in
# an argv element, in `ps`, or in shell history.
#
# stderr is deliberately NOT silenced anywhere in this script: a "column does not
# exist" error and an empty result set are indistinguishable once you redirect it.
psql_q() {
    docker run --rm --network "$DOCKER_NET" -e PGURL -i "$PG_IMAGE" \
        sh -c 'psql "$PGURL" -v ON_ERROR_STOP=1 -At -F "|" -f -' <<SQL
$1
SQL
}

# ─────────────────────────────────────────────────────────── preflight: database
step "PREFLIGHT"

[ -f "$ENV_FILE" ] || { echo "❌ $ENV_FILE not found — run from the compose directory" >&2; exit 1; }

# Sourced in a subshell-safe way: only DATABASE_URL is lifted out, and it is never
# echoed. `set -a` would export every secret in the file into the docker env.
PGURL="$(grep -E '^[[:space:]]*DATABASE_URL=' "$ENV_FILE" | tail -1 | cut -d= -f2- | sed 's/^"//; s/"$//')"
[ -n "$PGURL" ] || { echo "❌ DATABASE_URL not set in $ENV_FILE" >&2; exit 1; }
export PGURL

# Host and database name only — no user, no password.
DB_HOST="$(printf '%s' "$PGURL" | sed -E 's#^[^:]+://([^@]*@)?([^:/?]+).*#\2#')"
DB_NAME="$(printf '%s' "$PGURL" | sed -E 's#^[^?]*/([^/?]+)(\?.*)?$#\1#')"
say "database host : $DB_HOST"
say "database name : $DB_NAME"

server_version="$(psql_q 'SELECT current_setting('"'"'server_version'"'"');')"
say "server version: $server_version"

EXPECTED_TOKEN="DELETE-ALL-PIPELINES-${DB_NAME}"

# ────────────────────────────────────────────────── preflight: Kafka broker + vol
KAFKA_CONTAINER=""      # running broker — required for exec AND for volume resolution
KAFKA_CONTAINER_ANY=""  # any broker, running or not — used ONLY to report state

# Two passes, deliberately. `docker inspect` succeeds for a STOPPED container and
# `docker exec` then fails against it, which reads exactly like "Kafka is broken"
# when a healthy broker sits further down the candidate list -- so the running
# broker must win. A single pass that records the first *existing* candidate picks
# a stopped leftover from another project, and the volume this script deletes is
# then read from a DIFFERENT broker than the one it inventoried.
for candidate in "${KAFKA_BROKER_CANDIDATES[@]}"; do
    if [ "$(docker inspect -f '{{.State.Running}}' "$candidate" 2>/dev/null || true)" = "true" ]; then
        KAFKA_CONTAINER="$candidate"; KAFKA_CONTAINER_ANY="$candidate"; break
    fi
done
if [ -z "$KAFKA_CONTAINER_ANY" ]; then
    for candidate in "${KAFKA_BROKER_CANDIDATES[@]}"; do
        if docker inspect "$candidate" >/dev/null 2>&1; then KAFKA_CONTAINER_ANY="$candidate"; break; fi
    done
fi
if [ -n "$KAFKA_CONTAINER" ]; then
    say "kafka broker  : $KAFKA_CONTAINER (running)"
elif [ -n "$KAFKA_CONTAINER_ANY" ]; then
    say "kafka broker  : $KAFKA_CONTAINER_ANY (STOPPED — topic inventory will be skipped)"
else
    say "kafka broker  : not found (topic inventory will be skipped)"
fi

# Resolve the data volume from the RUNNING broker's own mount table, and from
# nothing else. Two earlier attempts got this wrong in the same way:
#   - `docker volume ls | grep kafka_data | head -1` matched four volumes on a host
#     with several compose projects and silently picked another project's data.
#   - Falling back to a STOPPED container of the same name did the same thing more
#     convincingly: with no broker running, it resolved a leftover dev container's
#     volume and removed it.
# If no broker is running, this script cannot confirm which broker the operator
# means -- the topic inventory was skipped for the same reason -- so it must refuse
# to delete a volume rather than guess. KAFKA_VOLUME_OVERRIDE is the deliberate
# escape hatch for that case.
KAFKA_VOLUME=""
if [ -n "$KAFKA_CONTAINER" ]; then
    KAFKA_VOLUME="$(docker inspect -f \
        '{{range .Mounts}}{{if eq .Destination "/var/lib/kafka/data"}}{{.Name}}{{end}}{{end}}' \
        "$KAFKA_CONTAINER" 2>/dev/null || true)"
    say "kafka volume  : ${KAFKA_VOLUME:-<broker has no volume at /var/lib/kafka/data>}"
else
    say "kafka volume  : UNRESOLVED — no running broker to ask."
    say "                --kafka=volume will refuse. Either start the broker and"
    say "                re-run, or pass KAFKA_VOLUME_OVERRIDE=<name> deliberately."
fi
if [ -n "${KAFKA_VOLUME_OVERRIDE:-}" ]; then
    KAFKA_VOLUME="$KAFKA_VOLUME_OVERRIDE"
    say "kafka volume  : $KAFKA_VOLUME (KAFKA_VOLUME_OVERRIDE — operator asserted)"
fi

# The apache/kafka image does not put /opt/kafka/bin on PATH and ships only the
# .sh names; cp-kafka ships the bare names. Probe absolute paths first.
KAFKA_TOPICS_BIN=""; KAFKA_GROUPS_BIN=""
if [ -n "$KAFKA_CONTAINER" ]; then
    for c in /opt/kafka/bin/kafka-topics.sh /opt/kafka/bin/kafka-topics kafka-topics.sh kafka-topics; do
        if docker exec "$KAFKA_CONTAINER" sh -c "command -v $c" >/dev/null 2>&1; then KAFKA_TOPICS_BIN="$c"; break; fi
    done
    KAFKA_GROUPS_BIN="$(printf '%s' "$KAFKA_TOPICS_BIN" | sed 's/kafka-topics/kafka-consumer-groups/')"
    [ -n "$KAFKA_TOPICS_BIN" ] || say "⚠️  no kafka-topics binary found in $KAFKA_CONTAINER"
fi
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"

kt() { docker exec "$KAFKA_CONTAINER" "$KAFKA_TOPICS_BIN"  --bootstrap-server "$KAFKA_BOOTSTRAP" "$@"; }
kg() { docker exec "$KAFKA_CONTAINER" "$KAFKA_GROUPS_BIN" --bootstrap-server "$KAFKA_BOOTSTRAP" "$@"; }

# ───────────────────────────────────────────────────────── inventory: FK children
step "INVENTORY — foreign keys referencing pipelines (live catalog, not migrations)"

# Read from pg_constraint, never from the migration files: two migrations declare
# `executions` with different ON DELETE rules and `CREATE TABLE IF NOT EXISTS`
# means whichever ran first won. Only the live catalog is authoritative.
FK_ROWS="$(psql_q "
SELECT c.conrelid::regclass::text,
       a.attname,
       CASE c.confdeltype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT'
            WHEN 'c' THEN 'CASCADE'   WHEN 'n' THEN 'SET NULL'
            WHEN 'd' THEN 'SET DEFAULT' END
FROM pg_constraint c
JOIN unnest(c.conkey) AS k(attnum) ON TRUE
JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
WHERE c.contype = 'f' AND c.confrelid = 'pipelines'::regclass
ORDER BY 3 DESC, 1;")"

printf '  %-34s %-22s %s\n' "CHILD TABLE" "FK COLUMN" "ON DELETE"
printf '  %-34s %-22s %s\n' "----------------------------------" "----------------------" "----------"
non_cascade=0
while IFS='|' read -r tbl col rule; do
    [ -n "$tbl" ] || continue
    printf '  %-34s %-22s %s\n' "$tbl" "$col" "$rule"
    [ "$rule" = "CASCADE" ] || non_cascade=$((non_cascade + 1))
done <<< "$FK_ROWS"
say ""
say "children: $(printf '%s\n' "$FK_ROWS" | grep -c . ) total, $non_cascade NOT cascading"

# ────────────────────────────────────────────────────────── inventory: row counts
step "INVENTORY — exact row counts"

# query_to_xml gives an exact count(*) per table in one round trip. reltuples is
# an estimate and returns -1 for a never-analyzed table, which reads as "empty".
COUNTS="$(psql_q "
WITH kids AS (
    SELECT DISTINCT c.conrelid::regclass::text AS t
    FROM pg_constraint c
    WHERE c.contype = 'f' AND c.confrelid = 'pipelines'::regclass
    UNION SELECT 'pipelines'
)
SELECT t,
       (xpath('/row/c/text()', query_to_xml(format('SELECT count(*) AS c FROM %s', t), false, true, '')))[1]::text::bigint
FROM kids ORDER BY 2 DESC, 1;")"

PIPELINE_COUNT=0; CDC_COUNT=0
printf '  %-34s %s\n' "TABLE" "ROWS"
printf '  %-34s %s\n' "----------------------------------" "------"
while IFS='|' read -r tbl n; do
    [ -n "$tbl" ] || continue
    [ "$n" = "0" ] && continue          # keep the report short; zeros summarised below
    printf '  %-34s %s\n' "$tbl" "$n"
    case "$tbl" in
        pipelines|public.pipelines)         PIPELINE_COUNT="$n" ;;
        cdc_resources|public.cdc_resources) CDC_COUNT="$n" ;;
    esac
done <<< "$COUNTS"
zero_tables="$(printf '%s\n' "$COUNTS" | awk -F'|' '$2==0' | wc -l | tr -d ' ')"
say ""
say "($zero_tables further tables are empty)"
say "pipelines to delete: $PIPELINE_COUNT"
say "cdc_resources rows : $CDC_COUNT  (ON DELETE SET NULL — these SURVIVE the delete)"

# ──────────────────────────────────────────────────────── inventory: CDC resources
if [ "$CDC_COUNT" != "0" ]; then
    step "INVENTORY — CDC resources that will be orphaned"
    # Names only. These identify server-side objects; no row values are read.
    psql_q "SELECT resource_type, resource_name, connection_id, pipeline_id
            FROM cdc_resources ORDER BY resource_type, resource_name;" \
      | while IFS='|' read -r rtype rname conn pid; do
            [ -n "$rtype" ] || continue
            printf '  %-18s %-38s conn=%s pipeline=%s\n' "$rtype" "$rname" "$conn" "${pid:-<null>}"
        done
    say ""
    say "⚠️  Each of these lives on a SOURCE database, not here. Deleting pipelines"
    say "    nulls the pipeline_id and leaves the publication/slot in place. An"
    say "    undropped replication slot pins WAL on the customer's server forever."
fi

# ───────────────────────────────────────────────────────── inventory: Kafka state
step "INVENTORY — Kafka topics and consumer groups"
TOPIC_LIST=""; GROUP_LIST=""
if [ -n "$KAFKA_CONTAINER" ] && [ -n "$KAFKA_TOPICS_BIN" ]; then
    TOPIC_LIST="$(kt --list || true)"
    GROUP_LIST="$(kg --list || true)"
    t_total=$(printf '%s\n' "$TOPIC_LIST" | grep -c . || true)
    t_pref=$(printf  '%s\n' "$TOPIC_LIST" | grep -c '^rsync\.' || true)
    t_int=$(printf   '%s\n' "$TOPIC_LIST" | grep -c '^_' || true)
    say "topics: $t_total total — $t_pref under rsync., $t_int internal (_ prefixed)"
    printf '%s\n' "$TOPIC_LIST" | sed 's/^/    /'
    say ""
    say "consumer groups: $(printf '%s\n' "$GROUP_LIST" | grep -c . || true)"
    printf '%s\n' "$GROUP_LIST" | grep . | sed 's/^/    /' || true
else
    say "⚠️  broker not reachable — topic/group inventory skipped."
    say "    Start Kafka and re-run the dry run before executing with --kafka=topics."
fi

# ────────────────────────────────────────────────────── inventory: Connect state
step "INVENTORY — Kafka Connect connectors"
CONNECTORS=""
if curl -sf --max-time 5 "$CONNECT_URL/connectors" >/dev/null 2>&1; then
    CONNECTORS="$(curl -sf "$CONNECT_URL/connectors" | tr -d '[]"' | tr ',' '\n' | grep -v '^$' || true)"
    if [ -n "$CONNECTORS" ]; then
        printf '%s\n' "$CONNECTORS" | sed 's/^/    /'
    else
        say "none registered"
    fi
else
    say "Connect REST not reachable at $CONNECT_URL — skipping connector cleanup"
fi

# ───────────────────────────────────────────────────────────────────────── guards
step "GUARDS"
fatal=0

if [ "$PIPELINE_COUNT" -gt "$MAX_PIPELINES" ]; then
    say "❌ $PIPELINE_COUNT pipelines exceeds --max-pipelines=$MAX_PIPELINES."
    say "   This tripwire exists because the cost of running this against the wrong"
    say "   database is unrecoverable. Re-check DB_HOST above, then raise the limit"
    say "   deliberately if the count is genuinely correct."
    fatal=1
else
    say "✅ pipeline count $PIPELINE_COUNT is within --max-pipelines=$MAX_PIPELINES"
fi

if [ "$CDC_COUNT" != "0" ] && [ "$CDC_SOURCE_DROPPED" != "yes" ]; then
    say "❌ cdc_resources has $CDC_COUNT row(s) and --cdc-dropped-on-source was not passed."
    say "   Drop each publication and replication slot listed above on its SOURCE"
    say "   database first (slots pin WAL indefinitely), then re-run with the flag."
    fatal=1
elif [ "$CDC_COUNT" = "0" ]; then
    say "✅ cdc_resources is empty — rsync tracks no CDC objects."
    say "   NOTE: this proves rsync tracks none, not that none exist. If any source"
    say "   database was ever CDC-provisioned outside this table, check it by hand:"
    say "     SELECT * FROM pg_replication_slots; SELECT * FROM pg_publication;"
else
    say "⚠️  cdc_resources has $CDC_COUNT row(s); proceeding on your assertion that"
    say "   the source-side publications and slots are already dropped."
fi

if [ "$MODE" = "execute" ]; then
    if [ "$CONFIRM_TOKEN" != "$EXPECTED_TOKEN" ]; then
        say "❌ --execute requires --confirm=$EXPECTED_TOKEN"
        fatal=1
    else
        say "✅ confirmation token matches target database '$DB_NAME'"
    fi
fi

if [ "$fatal" -ne 0 ] && [ "$MODE" = "execute" ]; then
    hr; echo "  ABORTED — guards failed. Nothing was changed."; hr; exit 1
fi
# In dry-run the guards report but never abort: the operator ran it precisely to
# see the whole picture, and stopping at the first failed guard hides the rest of
# the plan (topics, connectors, container list) behind a problem they are about to
# go fix anyway. `--execute` is where a failed guard is fatal.

# ─────────────────────────────────────────────────────────────────────── teardown
if [ "$MODE" = "dry-run" ]; then
    step "DRY RUN — no changes made"
    if [ "$fatal" -ne 0 ]; then
        say "⚠️  $fatal guard(s) failed above — --execute would ABORT right now."
        say "    The plan below is shown anyway so you can see the full scope."
        say ""
    fi
    say "To execute, re-run with:"
    say "    --execute --confirm=$EXPECTED_TOKEN --kafka=$KAFKA_MODE"
    [ "$CDC_COUNT" = "0" ] || say "    --cdc-dropped-on-source   (only after the source-side drop)"
    hr
fi

# ── step 1: Connect connectors (before the DB delete; Connect must still be up)
step "STEP 1 — delete Kafka Connect connectors"
if [ -z "$CONNECTORS" ]; then
    say "nothing to do"
elif [ "$MODE" = "dry-run" ]; then
    printf '%s\n' "$CONNECTORS" | while read -r c; do plan "DELETE $CONNECT_URL/connectors/$c"; done
else
    printf '%s\n' "$CONNECTORS" | while read -r c; do
        say "deleting connector $c"
        curl -sf -X DELETE "$CONNECT_URL/connectors/$c" || say "⚠️  delete failed for $c"
    done
    sleep 3
    say "remaining: $(curl -sf "$CONNECT_URL/connectors" || echo '<unreachable>')"
fi

# ── step 2: pipeline rows
step "STEP 2 — delete pipeline rows"
if [ "$PIPELINE_COUNT" = "0" ]; then
    say "pipelines is already empty — nothing to do (idempotent)"
elif [ "$MODE" = "dry-run" ]; then
    plan "DELETE FROM pipelines;  -- $PIPELINE_COUNT row(s), cascading to the children above"
else
    psql_q "BEGIN; DELETE FROM pipelines; COMMIT;"
    say "delete committed — re-counting to prove the cascade:"
    psql_q "
WITH kids AS (
    SELECT DISTINCT c.conrelid::regclass::text AS t
    FROM pg_constraint c WHERE c.contype='f' AND c.confrelid='pipelines'::regclass
    UNION SELECT 'pipelines'
)
SELECT t, (xpath('/row/c/text()', query_to_xml(format('SELECT count(*) AS c FROM %s', t), false, true, '')))[1]::text::bigint
FROM kids ORDER BY 2 DESC, 1;" | awk -F'|' '$2 != 0 {printf "    %-34s %s\n", $1, $2} END {print "    (every other table is now 0)"}'
fi

# ── step 3: stop the Kafka clients, then the broker
step "STEP 3 — stop Kafka-touching containers"
running_clients=()
for c in "${KAFKA_CLIENT_CONTAINERS[@]}"; do
    [ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null || true)" = "true" ] && running_clients+=("$c")
done
if [ "${#running_clients[@]}" -eq 0 ]; then
    say "no Kafka client containers are running"
elif [ "$MODE" = "dry-run" ]; then
    plan "docker stop ${running_clients[*]}"
else
    # One `docker stop` for all of them: stopping them one at a time leaves a
    # window in which a still-running producer re-creates a topic that a stopped
    # peer's restart policy then consumes from.
    docker stop "${running_clients[@]}"
    say "stopped: ${running_clients[*]}"
fi

# ── step 4: Kafka topic state
step "STEP 4 — clear Kafka topic state (--kafka=$KAFKA_MODE)"
if [ "$KAFKA_MODE" = "topics" ]; then
    if [ -z "$KAFKA_CONTAINER" ] || [ -z "$KAFKA_TOPICS_BIN" ]; then
        say "❌ --kafka=topics needs a running broker, and none was found. Either start"
        say "   Kafka and re-run, or use --kafka=volume."
        exit 1
    fi
    say "NOTE: this preserves __consumer_offsets, so offsets.topic.num.partitions"
    say "      stays at its current value. Use --kafka=volume to change it."
    del_topics="$(printf '%s\n' "$TOPIC_LIST" | grep -v '^__' || true)"
    if [ "$MODE" = "dry-run" ]; then
        printf '%s\n' "$GROUP_LIST"  | grep . | while read -r g; do plan "delete consumer group $g"; done
        printf '%s\n' "$del_topics" | grep . | while read -r t; do plan "delete topic $t"; done
    else
        # Groups BEFORE topics. Deleting a topic drops its committed offsets, which
        # takes the group with it -- the subsequent group delete then fails with
        # GroupIdNotFoundException, and that error is indistinguishable from a real
        # failure to delete a group that still matters.
        printf '%s\n' "$GROUP_LIST" | grep . | while read -r g; do
            say "deleting group $g"; kg --delete --group "$g" || true
        done
        printf '%s\n' "$del_topics" | grep . | while read -r t; do
            say "deleting topic $t"; kt --delete --topic "$t" || true
        done

        # Verify by re-listing, never by exit code: `kafka-consumer-groups.sh
        # --delete` prints "Deletion of some consumer groups failed" and STILL
        # exits 0, so `cmd || warn` reports a silent failure as a success.
        sleep 2
        left_topics="$(kt --list | grep -v '^__' || true)"
        left_groups="$(kg --list | grep . || true)"
        residue=0
        if [ -n "$left_topics" ]; then
            residue=1; say "❌ topics still present after delete:"
            printf '%s\n' "$left_topics" | sed 's/^/      /'
        fi
        if [ -n "$left_groups" ]; then
            residue=1; say "❌ consumer groups still present after delete:"
            printf '%s\n' "$left_groups" | sed 's/^/      /'
        fi
        if [ "$residue" -eq 0 ]; then
            say "✅ verified by re-listing: no non-internal topics, no consumer groups remain"
        else
            say "   Kafka deletes asynchronously; re-run to retry. If a topic persists,"
            say "   check that delete.topic.enable is true on the broker."
        fi
        say "remaining topics:"; kt --list | sed 's/^/    /'
    fi
else
    if [ -z "$KAFKA_VOLUME" ]; then
        say "❌ cannot use --kafka=volume: the target volume was not resolved from a"
        say "   running broker. Start Kafka and re-run the dry run to confirm which"
        say "   volume is in play, or set KAFKA_VOLUME_OVERRIDE=<name> if you are"
        say "   certain. Refusing to guess — removing the wrong broker's data volume"
        say "   is not recoverable."
        exit 1
    fi
    say "target volume $KAFKA_VOLUME (from broker ${KAFKA_CONTAINER:-<override>})"
    say "Dropping the volume also clears Connect's _rsync-connect-{configs,offsets,"
    say "status} and every __consumer_offsets record. That is intended: it is the"
    say "only point at which offsets.topic.num.partitions can be changed."
    if [ "$MODE" = "dry-run" ]; then
        [ -n "$KAFKA_CONTAINER" ] && plan "docker stop $KAFKA_CONTAINER"
        plan "docker volume rm $KAFKA_VOLUME"
    else
        if [ -n "$KAFKA_CONTAINER" ]; then docker stop "$KAFKA_CONTAINER"; say "stopped $KAFKA_CONTAINER"; fi
        # `docker volume rm` fails while any container still references the volume,
        # including stopped ones — remove the broker container, not just stop it.
        [ -n "$KAFKA_CONTAINER" ] && docker rm -f "$KAFKA_CONTAINER" >/dev/null 2>&1 || true
        docker volume rm "$KAFKA_VOLUME"
        say "removed volume $KAFKA_VOLUME"
    fi
fi

# ────────────────────────────────────────────────────────────────────────── next
step "NEXT — Phase 0 step 5 (not performed by this script)"
say "1. Confirm .env.prod sets the namespace explicitly:"
say "       KAFKA_TOPIC_PREFIX=rsync."
say "2. If you dropped the volume and want a different offset-topic partition"
say "   count, set KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS on the kafka service NOW —"
say "   it is fixed at first broker start and cannot be changed afterwards."
say "3. Bring the stack up:  docker compose up -d"
say "4. Verify kafka-init actually created topics (it created zero before the"
say "   Phase 0 step 4 fix — check the container exited 0 AND the topics exist):"
say "       docker logs rsync-ai-kafka-init"
say "       docker exec <broker> /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
hr
