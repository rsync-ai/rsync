#!/bin/bash
#
# Create all required Kafka topics for RSYNC AI
#
# Usage: ./scripts/create_kafka_topics.sh
#

set -e

# Counts creates the broker REJECTED. `set -e` cannot see them: create_topic
# handles its own non-zero exit, so the script would otherwise run to completion
# and exit 0 having created nothing.
FAILED_TOPICS=0

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║         Creating Kafka Topics for RSYNC AI                   ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

# Durability. Same two variable names the Go side reads
# (backend-orchestrator/internal/kafka/replication.go), so one operator setting
# covers both this script and the runtime topic-creation path.
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
MIN_INSYNC_REPLICAS="${KAFKA_MIN_INSYNC_REPLICAS:-}"

# A malformed value falls back to the default rather than failing, matching
# parsePositiveEnv() on the Go side.
case "$REPLICATION_FACTOR" in ''|*[!0-9]*) REPLICATION_FACTOR=1 ;; esac
if [ "$REPLICATION_FACTOR" -lt 1 ]; then REPLICATION_FACTOR=1; fi
case "$MIN_INSYNC_REPLICAS" in *[!0-9]*) MIN_INSYNC_REPLICAS='' ;; esac
# "0" is all-digits, so it survives the case above -- and it is the obvious way an
# operator writes "no floor". Unguarded it reaches the broker verbatim and every
# create dies with `Invalid value 0 for configuration min.insync.replicas`, taking
# the whole bootstrap with it while the Go side maps 0 to "operator stated
# nothing" and boots normally (parsePositiveEnv, replication.go:93-104). Treat it
# the same way here: unset, not zero.
if [ -n "$MIN_INSYNC_REPLICAS" ] && [ "$MIN_INSYNC_REPLICAS" -lt 1 ]; then MIN_INSYNC_REPLICAS=''; fi

# Clamp min.insync.replicas to the replication factor -- see
# clampMinInsyncReplicas() in replication.go. misr > RF creates a topic that
# exists, lists and is subscribable, and then rejects every acks=all produce with
# NOT_ENOUGH_REPLICAS, with nothing in any log naming the cause.
if [ -n "$MIN_INSYNC_REPLICAS" ] && [ "$MIN_INSYNC_REPLICAS" -gt "$REPLICATION_FACTOR" ]; then
    echo "⚠️  KAFKA_MIN_INSYNC_REPLICAS=$MIN_INSYNC_REPLICAS exceeds replication factor $REPLICATION_FACTOR — clamping to $REPLICATION_FACTOR (misr > RF: on brokers older than 3.7 every acks=all produce is then rejected with NOT_ENOUGH_REPLICAS)"
    MIN_INSYNC_REPLICAS="$REPLICATION_FACTOR"
fi

# Namespace, kept byte-for-byte identical to shared/go/kafkaclient/topics.go and
# llm-service/src/utils/kafka_topics.py. A topic created here under a different
# name than the services subscribe to raises nothing -- the consumer just blocks
# on a topic nobody writes.
#
# Bare `-`, not `:-`: an explicitly empty prefix is the migration lever for a
# deployment that already has live topics and committed offsets under the
# unprefixed names, and `:-` would substitute the default over that choice.
TOPIC_PREFIX="${KAFKA_TOPIC_PREFIX-rsync.}"
TOPIC_PREFIX="$(printf '%s' "$TOPIC_PREFIX" | tr -cd 'a-zA-Z0-9._-')"
case "$TOPIC_PREFIX" in
    ""|*[._-]) ;;
    *) TOPIC_PREFIX="${TOPIC_PREFIX}." ;;
esac

# The broker container is named rsync-ai-kafka in docker-compose.yml, not
# "kafka" -- and the apache/kafka image ships kafka-topics.sh, not kafka-topics.
# Both were hard-coded wrong here, so the connectivity probe below always failed
# and the script exited 1 with "Cannot connect to Kafka. Is it running?" while
# the broker was healthy. scripts/setup.sh:79 then swallowed the exit with
# `|| echo "Some Kafka topics may already exist"`. Resolve both at runtime.
KAFKA_CONTAINER="${KAFKA_CONTAINER:-}"
if [ -z "$KAFKA_CONTAINER" ]; then
    # `docker inspect` succeeds for a *stopped* container too, and `docker exec`
    # then fails against it -- which reads exactly like "Kafka is broken" when the
    # real broker is a healthy container further down this list. Require Running.
    for candidate in rsync-ai-kafka rsync-kafka kafka; do
        if [ "$(docker inspect -f '{{.State.Running}}' "$candidate" 2>/dev/null)" = "true" ]; then
            KAFKA_CONTAINER="$candidate"
            break
        fi
    done
fi
if [ -z "$KAFKA_CONTAINER" ]; then
    echo "❌ No Kafka container found (looked for: rsync-ai-kafka, rsync-kafka, kafka)"
    echo ""
    echo "Try: docker compose up -d kafka    # or set KAFKA_CONTAINER=<name>"
    exit 1
fi

# Absolute paths first: the apache/kafka image does NOT put /opt/kafka/bin on
# PATH, so a bare-name probe fails on the very image the compose file uses.
KAFKA_TOPICS=""
for candidate in /opt/kafka/bin/kafka-topics.sh /opt/kafka/bin/kafka-topics kafka-topics.sh kafka-topics; do
    if docker exec "$KAFKA_CONTAINER" sh -c "command -v $candidate" > /dev/null 2>&1; then
        KAFKA_TOPICS="$candidate"
        break
    fi
done
if [ -z "$KAFKA_TOPICS" ]; then
    echo "❌ Neither kafka-topics.sh nor kafka-topics found inside $KAFKA_CONTAINER"
    exit 1
fi

# Check if Kafka is accessible
echo "📡 Connecting to Kafka at $KAFKA_BOOTSTRAP via $KAFKA_CONTAINER ($KAFKA_TOPICS, prefix: ${TOPIC_PREFIX:-<none>})..."
if ! docker exec "$KAFKA_CONTAINER" "$KAFKA_TOPICS" --list --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo "❌ Cannot connect to Kafka. Is it running?"
    echo ""
    echo "Try: docker compose up -d kafka"
    exit 1
fi
echo "✅ Kafka connection successful"
echo ""

# =============================================================================
# TOPIC DEFINITIONS
# =============================================================================

# Every name below has a matching subscribe or produce call in a service. That
# is the entry criterion: a pre-created topic nobody reads is indistinguishable
# from a working one on the broker, and it is the same silent-divergence bug as
# a misspelled name -- it just fails in the other direction.
#
# Fourteen names were removed here in the `rsync.` cutover because the grep came
# back empty across api-gateway, backend-orchestrator, backend-temporal-adapter,
# llm-service and shared: agent.{intent,resolver,discovery,validator,telemetry}
# .requests, agent.telemetry.responses, all six agent.*.requests.dlq topics, and
# system.status.updates / pipeline.events. The live DLQ names are
# pipeline.failed.dlq and agent.failed.dlq (backend-temporal-adapter/internal/
# workflows/activities.go:169,200), which this script never created.

# Agent Request Topics
# Entry points that a service actually consumes.
AGENT_REQUEST_TOPICS=(
    "agent.planner.requests"      # llm-service/src/agents/planner/kafka_consumer.py:58
    "agent.executor.requests"     # backend-orchestrator/internal/agents/executor/executor.go:983
)

# Agent Response Topics
# Where agents publish results; consumed by the api-gateway WebSocket bridge
# (api-gateway/internal/websocket/kafka_bridge.go:62-80).
AGENT_RESPONSE_TOPICS=(
    "agent.intent.responses"      # Parsed intent
    "agent.resolver.responses"    # Resolved connections
    "agent.discovery.responses"   # Discovered schema
    "agent.planner.responses"     # Generated plan
    "agent.validator.responses"   # Validation result
    "agent.executor.responses"    # Execution result
)

# =============================================================================
# CREATE TOPICS
# =============================================================================

create_topic() {
    local topic=$1
    local partitions=${2:-1}
    # Default to the operator's replication factor, not a hardcoded 1: on a BYO
    # multi-broker cluster RF=1 topics go unavailable during routine rolling
    # maintenance (an MSK patch takes one broker down at a time).
    local replication=${3:-$REPLICATION_FACTOR}
    local misr_arg=""

    # Pin min.insync.replicas only when asked. Clamped per-topic as well, because
    # a caller may pass an explicit replication factor lower than the global one.
    if [ -n "$MIN_INSYNC_REPLICAS" ]; then
        local misr="$MIN_INSYNC_REPLICAS"
        if [ "$misr" -gt "$replication" ]; then misr="$replication"; fi
        misr_arg="--config min.insync.replicas=$misr"
    fi

    # Idempotent, like Topic() in Go and topic() in Python. An empty prefix makes
    # the pattern match everything, so this becomes a no-op.
    case "$topic" in
        "$TOPIC_PREFIX"*) ;;
        *) topic="${TOPIC_PREFIX}${topic}" ;;
    esac

    echo -n "  📝 $topic... "

    # Do NOT discard stderr. --if-not-exists already makes "topic exists" a
    # success, so anything on this path is a real rejection -- and the most likely
    # one is now reachable: KAFKA_REPLICATION_FACTOR larger than the live broker
    # count makes every create fail with InvalidReplicationFactorException. Before
    # RF was operator-settable that could not happen, and the old
    # `2>/dev/null` + "may already exist" turned zero topics created into a green
    # run whose message actively asserted the opposite.
    local out
    if out=$(docker exec "$KAFKA_CONTAINER" "$KAFKA_TOPICS" --create \
        --topic "$topic" \
        --bootstrap-server localhost:9092 \
        --partitions $partitions \
        --replication-factor $replication \
        $misr_arg \
        --if-not-exists 2>&1); then
        echo "✅"
    else
        echo "❌"
        echo "$out" | sed 's/^/       /'
        FAILED_TOPICS=$((FAILED_TOPICS + 1))
    fi
}

echo "📨 Creating Agent Request Topics..."
for topic in "${AGENT_REQUEST_TOPICS[@]}"; do
    create_topic "$topic"
done
echo ""

echo "📬 Creating Agent Response Topics..."
for topic in "${AGENT_RESPONSE_TOPICS[@]}"; do
    create_topic "$topic"
done
echo ""

# =============================================================================
# VERIFY
# =============================================================================

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📋 All Kafka Topics:"
echo ""
docker exec "$KAFKA_CONTAINER" "$KAFKA_TOPICS" --list --bootstrap-server localhost:9092 2>/dev/null | sort | sed 's/^/  /'
echo ""

TOTAL_TOPICS=$(docker exec "$KAFKA_CONTAINER" "$KAFKA_TOPICS" --list --bootstrap-server localhost:9092 2>/dev/null | wc -l | tr -d ' ')
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "✅ Total topics: $TOTAL_TOPICS"
echo ""
echo "🔄 Agentic Pipeline Flow:"
echo ""
echo "   User Request"
echo "        │"
echo "        ▼"
echo "   [${TOPIC_PREFIX}agent.control.commands.<agent>] ──► Intent · Resolver · Discovery"
echo "        │                                              Planner · Validator · Executor"
echo "        │   (created by the kafka-init service, not this script)"
echo "        ▼"
echo "   [${TOPIC_PREFIX}agent.<agent>.responses] ──► api-gateway WebSocket bridge"
echo "        │"
echo "        ▼"
echo "   [${TOPIC_PREFIX}pipeline.domain.events] ──► projector · sink · UI"
echo "        │"
echo "        ▼"
echo "   Pipeline Complete! 🎉"
echo ""

if [ "$FAILED_TOPICS" -gt 0 ]; then
    echo "❌ $FAILED_TOPICS topic(s) were REJECTED by the broker (see the messages above)."
    echo "   The most common cause is KAFKA_REPLICATION_FACTOR=$REPLICATION_FACTOR being"
    echo "   larger than the number of brokers in the cluster. Unlike the Go runtime path,"
    echo "   this script does not clamp to the live broker count."
    exit 1
fi

