#!/bin/bash

# ==============================================================================
# Kafka Topic Initialization Script - New Agentic Architecture
# ==============================================================================
# This script creates the new topics required for the control plane/data plane
# separation architecture following Temporal/Conductor patterns.
#
# New Topics:
# 1. task.assignments     - Orchestrator assigns tasks to agent workers
# 2. task.results         - Agent workers return results to orchestrator
# 3. pipeline.domain.events - Canonical domain events (infinite retention)
# 4. pipeline.agent.telemetry - Agent telemetry (7-day TTL, optional)
# ==============================================================================

set -e

# Support both apache/kafka (kafka-topics.sh) and cp-kafka (kafka-topics) script
# names. This was an `alias`, which bash expands only in interactive shells --
# under the container's `bash -lc` it expanded nowhere, so on the apache/kafka
# image (which ships only kafka-topics.sh) every call below died with
# "kafka-topics: command not found". Resolve the binary into a variable instead.
export PATH="/opt/kafka/bin:$PATH"
if command -v kafka-topics &>/dev/null; then
    KAFKA_TOPICS=kafka-topics
elif command -v kafka-topics.sh &>/dev/null; then
    KAFKA_TOPICS=kafka-topics.sh
else
    echo "❌ Neither kafka-topics nor kafka-topics.sh is on PATH ($PATH)"
    exit 1
fi

KAFKA_BROKER=${KAFKA_BROKER:-"localhost:9092"}
PARTITIONS=${PARTITIONS:-10}

# Durability. KAFKA_REPLICATION_FACTOR / KAFKA_MIN_INSYNC_REPLICAS are the names
# the Go side reads (backend-orchestrator/internal/kafka/replication.go), so an
# operator sets ONE pair of variables and both the bootstrapper and the runtime
# topic-creation path honour it. REPLICATION_FACTOR stays supported as the older
# script-local spelling.
REPLICATION_FACTOR=${KAFKA_REPLICATION_FACTOR:-${REPLICATION_FACTOR:-1}}
MIN_INSYNC_REPLICAS=${KAFKA_MIN_INSYNC_REPLICAS:-}

# Ignore a non-numeric or non-positive value rather than failing the boot, which
# is what parsePositiveEnv() does on the Go side.
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

# Clamp min.insync.replicas to the replication factor. This mirrors
# clampMinInsyncReplicas() in replication.go and is not cosmetic: misr > RF
# yields a topic that is created, listed and subscribable, and then rejects every
# acks=all produce with NOT_ENOUGH_REPLICAS. Nothing in any log names the
# replication factor as the cause -- the pipeline just reports running and moves
# zero rows. Clamping degrades durability to what the cluster can actually serve
# instead of minting an unwritable topic. Brokers from 3.7 on cap the effective
# value at the replica count and shrug this off; the older managed clusters a
# self-host points at do not. Measured -- see docker-compose.yml.
if [ -n "$MIN_INSYNC_REPLICAS" ] && [ "$MIN_INSYNC_REPLICAS" -gt "$REPLICATION_FACTOR" ]; then
    echo "⚠️  KAFKA_MIN_INSYNC_REPLICAS=$MIN_INSYNC_REPLICAS exceeds replication factor $REPLICATION_FACTOR — clamping to $REPLICATION_FACTOR (misr > RF: on brokers older than 3.7 every acks=all produce is then rejected with NOT_ENOUGH_REPLICAS)"
    MIN_INSYNC_REPLICAS=$REPLICATION_FACTOR
fi

# Namespace every topic this script creates. This is the third implementation of
# a contract whose other two halves are shared/go/kafkaclient/topics.go and
# llm-service/src/utils/kafka_topics.py, and all three must agree byte-for-byte:
# this script creates the topic, the services subscribe to it, and a divergence
# raises nothing -- the consumer simply blocks on a topic nobody writes.
#
# Bare `-`, not `:-`. Setting KAFKA_TOPIC_PREFIX to the empty string is the
# documented migration lever for a deployment that already has live topics and
# committed offsets under the unprefixed names; `:-` would substitute the
# default over that deliberate choice and silently rename everything.
TOPIC_PREFIX="${KAFKA_TOPIC_PREFIX-rsync.}"

# Same normalization the Go and Python helpers apply: drop anything outside
# Kafka's topic charset, then guarantee a trailing separator. Without the
# separator an operator prefix of "acme" yields "acmetask.results" here and
# "acme.task.results" in the services -- both legal topic names, so the split
# surfaces only as a consumer that never receives anything.
TOPIC_PREFIX="$(printf '%s' "$TOPIC_PREFIX" | tr -cd 'a-zA-Z0-9._-')"
case "$TOPIC_PREFIX" in
    ""|*[._-]) ;;
    *) TOPIC_PREFIX="${TOPIC_PREFIX}." ;;
esac

# ==============================================================================
# Kafka client security (BYO / managed broker)
# ==============================================================================
# The topic bootstrapper is a Kafka CLIENT too. Against a SASL or TLS listener a
# client whose security config does not match does NOT get an error -- it BLOCKS
# in the handshake until a timeout expires, so a misconfigured broker presents as
# a hung install rather than a rejected one. docker-compose.yml hands this script
# the same KAFKA_* variables every Go and Python client reads (the
# x-kafka-security anchor at the top of that file); here they become the
# --command-config properties file the JVM CLIs take.
#
# Built ONLY when the operator set at least one of them. With the bundled broker
# every variable is empty, KAFKA_CC stays empty, and every kafka-topics
# invocation below is byte-identical to what it was before this block existed --
# which is what keeps the default and prod paths unchanged.
#
# Hand-kept in lockstep with the inline builder in docker-compose.quickstart.yml
# and with deploy/helm/rsync-ai/templates/jobs/kafka-init.yaml. install.sh
# downloads only the quickstart compose file, so that copy cannot source this
# script; llm-service/tests/test_compose_kafka_security_env.py fails if the
# variable sets drift apart.
KAFKA_CC=""
KAFKA_SSL_SKIP_VERIFY="${KAFKA_SSL_SKIP_VERIFY:-${KAFKA_SSL_INSECURE_SKIP_VERIFY:-}}"

# Every key the x-kafka-security anchor delivers, concatenated. Non-empty means
# the operator configured something and we must speak it; empty means untouched
# defaults. Listing them by name (rather than globbing KAFKA_*) keeps this honest:
# the guard test asserts this line names every key the anchor sets, so adding one
# to the anchor without teaching this script fails CI instead of silently
# producing a client that ignores it.
_kafka_sec_any="${KAFKA_SECURITY_PROTOCOL}${KAFKA_SASL_MECHANISM}${KAFKA_SASL_USERNAME}${KAFKA_SASL_PASSWORD}"
_kafka_sec_any="${_kafka_sec_any}${KAFKA_SASL_OAUTHBEARER_CLIENT_ID}${KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET}"
_kafka_sec_any="${_kafka_sec_any}${KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT}${KAFKA_SASL_OAUTHBEARER_SCOPE}"
_kafka_sec_any="${_kafka_sec_any}${KAFKA_SASL_OAUTHBEARER_EXTENSIONS}${KAFKA_SASL_OAUTHBEARER_LOGIN_CALLBACK_HANDLER}"
_kafka_sec_any="${_kafka_sec_any}${KAFKA_AWS_REGION}${KAFKA_SSL_CA_LOCATION}${KAFKA_SSL_CERT_LOCATION}"
_kafka_sec_any="${_kafka_sec_any}${KAFKA_SSL_KEY_LOCATION}${KAFKA_SSL_KEYSTORE_LOCATION}${KAFKA_SSL_SKIP_VERIFY}"

if [ -n "$_kafka_sec_any" ]; then
    KAFKA_CLIENT_CONFIG="${KAFKA_CLIENT_CONFIG:-/tmp/kafka-client.properties}"
    # The file holds a SASL password in cleartext -- it is the only format the
    # JVM CLI accepts. 077 before creation, not chmod after: chmod leaves a
    # window in which the file exists world-readable.
    umask 077

    # Timeouts are not optional in this file: they are what turns a security
    # mismatch from an indefinite block into a named failure.
    {
        echo "request.timeout.ms=10000"
        echo "default.api.timeout.ms=15000"
        echo "security.protocol=${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}"
    } > "$KAFKA_CLIENT_CONFIG"

    # A SASL password crosses TWO grammars on its way to the broker: the Java
    # properties file, and the JAAS config string that lives inside one of its
    # values. A backslash or a double quote therefore needs escaping twice, and
    # the failure mode of getting it wrong is an authentication error that names
    # the user rather than the escaping -- which reads like a wrong password.
    esc() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g' | sed 's/\\/\\\\/g'; }

    if [ -n "$KAFKA_SASL_MECHANISM" ]; then
        case "$KAFKA_SASL_MECHANISM" in
            SCRAM-SHA-256|SCRAM-SHA-512)
                _jaas_module="org.apache.kafka.common.security.scram.ScramLoginModule" ;;
            PLAIN)
                _jaas_module="org.apache.kafka.common.security.plain.PlainLoginModule" ;;
            OAUTHBEARER)
                _jaas_module="org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule" ;;
            *)
                # AWS_MSK_IAM lands here. The Go and Python clients support it;
                # this JVM CLI image does not ship the aws-msk-iam-auth jar, so
                # pretending otherwise would produce a ClassNotFoundException at
                # topic-create time and no explanation.
                echo "❌ FATAL: KAFKA_SASL_MECHANISM='$KAFKA_SASL_MECHANISM' is not supported by the Kafka CLI in this image."
                echo "         Supported here: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER."
                echo "         AWS_MSK_IAM works for the services but not for this bootstrapper;"
                echo "         create the topics out of band -- see docs/deployment/kafka-acls.md."
                exit 1 ;;
        esac
        echo "sasl.mechanism=$KAFKA_SASL_MECHANISM" >> "$KAFKA_CLIENT_CONFIG"

        if [ "$KAFKA_SASL_MECHANISM" = "OAUTHBEARER" ]; then
            # Fail on absence rather than emit a half JAAS entry: the broker's
            # rejection would name the token, not the missing variable.
            for _req in KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT KAFKA_SASL_OAUTHBEARER_CLIENT_ID KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET; do
                eval "_val=\${$_req:-}"
                if [ -z "$_val" ]; then
                    echo "❌ FATAL: KAFKA_SASL_MECHANISM=OAUTHBEARER requires $_req to be set."
                    exit 1
                fi
            done
            _jaas="$_jaas_module required"
            _jaas="$_jaas clientId=\"$(esc "$KAFKA_SASL_OAUTHBEARER_CLIENT_ID")\""
            _jaas="$_jaas clientSecret=\"$(esc "$KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET")\""
            if [ -n "$KAFKA_SASL_OAUTHBEARER_SCOPE" ]; then
                _jaas="$_jaas scope=\"$(esc "$KAFKA_SASL_OAUTHBEARER_SCOPE")\""
            fi
            # Comma-separated k=v. An empty pair or the reserved name `auth`
            # makes the broker reject the whole handshake with a generic
            # SaslAuthenticationException, so reject them here where we can say why.
            if [ -n "$KAFKA_SASL_OAUTHBEARER_EXTENSIONS" ]; then
                _rest="$KAFKA_SASL_OAUTHBEARER_EXTENSIONS"
                while [ -n "$_rest" ]; do
                    case "$_rest" in
                        *,*) _pair="${_rest%%,*}"; _rest="${_rest#*,}" ;;
                        *)   _pair="$_rest"; _rest="" ;;
                    esac
                    [ -z "$_pair" ] && continue
                    _k="${_pair%%=*}"
                    _v="${_pair#*=}"
                    if [ -z "$_k" ] || [ "$_k" = "$_pair" ]; then
                        echo "❌ FATAL: KAFKA_SASL_OAUTHBEARER_EXTENSIONS entry '$_pair' is not key=value."
                        exit 1
                    fi
                    if [ "$_k" = "auth" ]; then
                        echo "❌ FATAL: KAFKA_SASL_OAUTHBEARER_EXTENSIONS may not set the reserved extension 'auth'."
                        exit 1
                    fi
                    _jaas="$_jaas extension_${_k}=\"$(esc "$_v")\""
                done
            fi
            echo "sasl.jaas.config=$_jaas;" >> "$KAFKA_CLIENT_CONFIG"
            # Always written. Without a callback handler the JVM builds an unsigned
            # placeholder token, and the broker rejects it as malformed -- an error
            # that names the token rather than the missing handler.
            echo "sasl.login.callback.handler.class=${KAFKA_SASL_OAUTHBEARER_LOGIN_CALLBACK_HANDLER:-org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler}" >> "$KAFKA_CLIENT_CONFIG"
            echo "sasl.oauthbearer.token.endpoint.url=$KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT" >> "$KAFKA_CLIENT_CONFIG"
        else
            if [ -z "$KAFKA_SASL_USERNAME" ] || [ -z "$KAFKA_SASL_PASSWORD" ]; then
                echo "❌ FATAL: KAFKA_SASL_MECHANISM=$KAFKA_SASL_MECHANISM requires KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD."
                exit 1
            fi
            echo "sasl.jaas.config=$_jaas_module required username=\"$(esc "$KAFKA_SASL_USERNAME")\" password=\"$(esc "$KAFKA_SASL_PASSWORD")\";" >> "$KAFKA_CLIENT_CONFIG"
        fi
    fi

    # type=PEM is the whole point (KIP-651, Kafka 2.7+): without it the JVM
    # defaults to JKS, fails to parse the PEM, and names neither the file nor the
    # setting. The Go and Python clients read the same file unconverted.
    if [ -n "$KAFKA_SSL_CA_LOCATION" ]; then
        {
            echo "ssl.truststore.type=PEM"
            echo "ssl.truststore.location=$KAFKA_SSL_CA_LOCATION"
        } >> "$KAFKA_CLIENT_CONFIG"
    fi

    # mTLS. KAFKA_SSL_KEYSTORE_LOCATION, NOT KAFKA_SSL_CERT_LOCATION: a JVM PEM
    # keystore is ONE file holding chain + key, and it cannot take the two paths
    # the Go and Python clients read. Pointing it at the cert half alone yields
    # "Failed to load PEM keystore" -- a message that names the file, so it reads
    # like a bad certificate rather than a missing key.
    if [ -n "$KAFKA_SSL_KEYSTORE_LOCATION" ]; then
        {
            echo "ssl.keystore.type=PEM"
            echo "ssl.keystore.location=$KAFKA_SSL_KEYSTORE_LOCATION"
        } >> "$KAFKA_CLIENT_CONFIG"
    elif [ -n "$KAFKA_SSL_CERT_LOCATION" ]; then
        echo "❌ FATAL: KAFKA_SSL_CERT_LOCATION is set but KAFKA_SSL_KEYSTORE_LOCATION is not."
        echo "         The Kafka CLI is a JVM and needs the keypair as ONE PEM file:"
        echo "           cat \$KAFKA_SSL_CERT_LOCATION \$KAFKA_SSL_KEY_LOCATION > /certs/client.pem"
        echo "         then set KAFKA_SSL_KEYSTORE_LOCATION=/certs/client.pem."
        exit 1
    fi

    if [ "$KAFKA_SSL_SKIP_VERIFY" = "true" ]; then
        # As close as a JVM gets: drops the HOSTNAME check only, the chain is
        # still validated. The Go client skips both, so this job can fail on a
        # certificate the data plane accepted. Deliberately not papered over.
        echo "ssl.endpoint.identification.algorithm=" >> "$KAFKA_CLIENT_CONFIG"
    fi

    # Unquoted on use, so it splits into two argv words. Every kafka-topics call
    # below takes it; a call that forgets it silently falls back to PLAINTEXT.
    KAFKA_CC="--command-config $KAFKA_CLIENT_CONFIG"
fi

echo "🚀 Creating new Kafka topics for agentic architecture..."
echo "Broker: $KAFKA_BROKER"
echo "Security: ${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}${KAFKA_SASL_MECHANISM:+ / $KAFKA_SASL_MECHANISM}"
echo "Partitions: $PARTITIONS"
echo "Replication Factor: $REPLICATION_FACTOR"
echo "Min ISR: ${MIN_INSYNC_REPLICAS:-<broker default>}"
echo "Topic prefix: ${TOPIC_PREFIX:-<none>}"
echo ""

# Function to create topic with retries
create_topic() {
    local topic_name=$1
    local retention_ms=$2
    local config_args=""

    # Idempotent, like Topic() in Go and topic() in Python: a name that already
    # carries the prefix must not become "rsync.rsync.task.results". With an
    # empty prefix the pattern matches everything and this is a no-op.
    case "$topic_name" in
        "$TOPIC_PREFIX"*) ;;
        *) topic_name="${TOPIC_PREFIX}${topic_name}" ;;
    esac
    
    if [ ! -z "$retention_ms" ]; then
        config_args="--config retention.ms=$retention_ms"
    fi

    # Pin min.insync.replicas explicitly when the operator asked for one. Left
    # unset the BROKER default applies, and that default is invisible from here --
    # on MSK and most managed clusters it is 2, so a topic created RF=1 against
    # such a broker is born unwritable.
    if [ -n "$MIN_INSYNC_REPLICAS" ]; then
        config_args="$config_args --config min.insync.replicas=$MIN_INSYNC_REPLICAS"
    fi

    echo "📝 Creating topic: $topic_name"
    
    # Wait for Kafka to be ready
    max_retries=30
    retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        if "$KAFKA_TOPICS" $KAFKA_CC --bootstrap-server $KAFKA_BROKER --list &> /dev/null; then
            echo "✅ Kafka is ready"
            break
        fi
        retry_count=$((retry_count + 1))
        echo "⏳ Waiting for Kafka... ($retry_count/$max_retries)"
        sleep 2
    done
    
    # Check if topic already exists
    # -x -F: the topic names contain dots, and as a regex "task.results" also
    # matches "taskXresults". A false match here reports "already exists" and
    # skips creating a topic that was never there.
    if "$KAFKA_TOPICS" $KAFKA_CC --bootstrap-server $KAFKA_BROKER --list 2>/dev/null | grep -qxF "$topic_name"; then
        echo "⚠️  Topic '$topic_name' already exists, skipping..."
        return 0
    fi
    
    # Create the topic
    "$KAFKA_TOPICS" --create \
        $KAFKA_CC --bootstrap-server $KAFKA_BROKER \
        --topic $topic_name \
        --partitions $PARTITIONS \
        --replication-factor $REPLICATION_FACTOR \
        $config_args \
        2>&1
    
    if [ $? -eq 0 ]; then
        echo "✅ Successfully created topic: $topic_name"
    else
        echo "❌ Failed to create topic: $topic_name"
        return 1
    fi
    echo ""
}

# ==============================================================================
# Create New Topics
# ==============================================================================

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 CREATING CONTROL PLANE TOPICS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# 1. Task Assignments (Orchestrator → Agents)
create_topic "task.assignments" ""

# 2. Task Results (Agents → Orchestrator)
create_topic "task.results" ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 CREATING EVENT SOURCING TOPICS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# 3. Domain Events (Infinite retention - source of truth)
create_topic "pipeline.domain.events" "-1"

# 4. Agent Telemetry (7 days retention - debug/monitoring)
SEVEN_DAYS_MS=$((7 * 24 * 60 * 60 * 1000))
create_topic "pipeline.agent.telemetry" "$SEVEN_DAYS_MS"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ ALL NEW TOPICS CREATED SUCCESSFULLY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# List all topics to verify
echo "📜 Current Kafka topics in the ${TOPIC_PREFIX:-<unprefixed>} namespace:"
"$KAFKA_TOPICS" $KAFKA_CC --bootstrap-server $KAFKA_BROKER --list 2>/dev/null | grep -F "$TOPIC_PREFIX" | grep -E "(task\.|pipeline\.)" | sort

echo ""
echo "🎉 Kafka topic initialization complete!"
echo ""
echo "📊 Topic Details:"
echo "  - ${TOPIC_PREFIX}task.assignments:          Orchestrator → Agent task assignment"
echo "  - ${TOPIC_PREFIX}task.results:             Agent → Orchestrator result reporting"
echo "  - ${TOPIC_PREFIX}pipeline.domain.events:   Canonical events (infinite retention)"
echo "  - ${TOPIC_PREFIX}pipeline.agent.telemetry: Debug/monitoring (7-day TTL)"

