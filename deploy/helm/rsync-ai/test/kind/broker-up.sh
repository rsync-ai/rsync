#!/usr/bin/env bash
# Bring up a REAL Kafka broker OUTSIDE the kind cluster, in the shape a customer's
# managed cluster actually has, and make it reachable from inside the cluster.
#
# Three deliberate choices, each of which a lazier setup gets wrong in a way that
# makes the whole test vacuous:
#
#  1. `auto.create.topics.enable=false`. This is the defining property of a
#     managed BYO cluster and the entire premise of the kafka-init Job. Leave it
#     on and every topic the platform forgot to create springs into existence,
#     the pipeline goes green, and the test proves nothing.
#
#  2. The EXTERNAL listener advertises an in-cluster DNS NAME, not an IP. A Kafka
#     client bootstraps once and then reconnects to whatever the broker
#     advertises, so advertising an IP would let a broken DNS path pass. The name
#     is backed by a selectorless Service + EndpointSlice, applied inline near
#     the end of this script, and is also what the TLS SAN must match in the
#     later stages.
#
#  3. A separate ADMIN listener bound inside the container, reached only via
#     `docker exec`. Ground truth about which topics exist must never travel the
#     same path that is under test -- otherwise a broken auth path reports
#     "no topics" and a working one reports "no topics" too.
set -euo pipefail

# Normalised: JOURNAL.md documents `./broker-up.sh s0`, and a lowercase arg used to
# fall straight through to the "not implemented" hard stop -- a confusing failure
# for a correct invocation.
STAGE="$(printf '%s' "${1:-S0}" | tr '[:lower:]' '[:upper:]')"
NAME="${BROKER_NAME:-byo-kafka}"
NET="${KIND_NETWORK:-kind}"
IMAGE="${BROKER_IMAGE:-apache/kafka:3.7.0}"
NS="${RSYNC_NAMESPACE:-rsync}"
# Must match the Service this script applies at the end, and the SAN in the TLS
# stages.
FQDN="${BROKER_FQDN:-$NAME.$NS.svc.cluster.local}"
CLUSTER_ID="${BROKER_CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}"
KCTX="${KUBE_CONTEXT:-kind-rsync}"

# 32 alphanumeric characters, for the credentials this script has to invent.
#
# NOT the obvious `tr -dc ... </dev/urandom | head -c 32`. That spelling is a
# SIGPIPE race under `set -e -o pipefail`: when head has its 32 bytes it exits,
# tr dies of SIGPIPE, pipefail promotes 141 to the pipeline's status and set -e
# aborts the script -- *after* the credential file has been written in full. So
# it fails exactly once, on the run that creates the file, and every run
# afterwards finds the file and skips the block. Measured, not theorised: a
# fresh S4 exited 141 with an empty stderr straight after "TLS material OK",
# and succeeded on the next invocation with no change to anything.
#
# Here `head` is the PRODUCER and bounds itself, so no process downstream can
# exit early and nothing can be signalled.
gen_secret() {
  local raw
  raw="$(LC_ALL=C head -c 512 /dev/urandom | base64 | LC_ALL=C tr -dc 'A-Za-z0-9')"
  # base64 of 512 bytes is ~684 characters and stripping +/=\n leaves the vast
  # majority, so this cannot legitimately fall short. Assert anyway: a truncated
  # secret is still a usable-looking string, and the resulting authentication
  # failure would be read as a chart bug.
  if [ "${#raw}" -lt 32 ]; then
    echo "FATAL: gen_secret produced only ${#raw} characters." >&2
    return 1
  fi
  printf '%s' "${raw:0:32}"
}

# SASL identity for the stages that have one. The password is NEVER a literal in
# this file: it comes from the environment, or is generated once into a
# gitignored file so repeated runs keep the same credential without a human ever
# having to see it. `chmod 600` because it is a real credential for as long as
# the broker is up.
SASL_USER="${BROKER_SASL_USER:-rsync}"
PW_FILE="${BROKER_SASL_PASSWORD_FILE:-$(dirname "$0")/.broker-sasl-password}"
if [ -z "${BROKER_SASL_PASSWORD:-}" ]; then
  if [ ! -s "$PW_FILE" ]; then
    (umask 077; gen_secret > "$PW_FILE")
  fi
  BROKER_SASL_PASSWORD="$(cat "$PW_FILE")"
fi
chmod 600 "$PW_FILE" 2>/dev/null || true

# OIDC identity for stage S4. Same discipline as the SASL password: never a
# literal in this file, generated once into a gitignored 0600 file so repeated
# runs keep the same credential and no human ever has to see it.
#
# Note which credential this is. It is what the Kafka CLIENTS present to the
# token endpoint -- their Kafka credential is the JWT that comes back. Two
# secrets, two trust boundaries; conflating them is how an OAUTHBEARER setup
# ends up sending a client secret to a broker.
OIDC_NAME="${OIDC_NAME:-oidc}"
OIDC_PORT="${OIDC_PORT:-8080}"
OIDC_CLIENT_ID="${OIDC_CLIENT_ID:-rsync}"
OIDC_AUDIENCE="${OIDC_AUDIENCE:-kafka}"
# The `iss` claim, and the broker's expected.issuer, must be the SAME opaque
# string -- and it cannot be a URL that both sides resolve, because the broker
# reaches this server over docker DNS (`oidc`) while the pods reach it over
# Kubernetes DNS (`oidc.<ns>.svc.cluster.local`). Kafka compares `iss` as a
# string and never dereferences it, so the Kubernetes name is the one to use:
# it is the name the thing under test actually dials.
OIDC_ISSUER="${OIDC_ISSUER:-http://$OIDC_NAME.$NS.svc.cluster.local:$OIDC_PORT}"
OIDC_SECRET_FILE="${OIDC_CLIENT_SECRET_FILE:-$(dirname "$0")/.oidc-client-secret}"
OIDC_IMAGE="${OIDC_IMAGE:-rsync-test-oidc:1}"
# A named volume, not a bind mount: the signing key must survive an `oidc`
# restart or every token minted afterwards fails against the JWKS the broker
# already cached -- and that failure reads exactly like a chart bug.
OIDC_KEY_VOLUME="${OIDC_KEY_VOLUME:-rsync-s4-oidc-keys}"

# Both spellings of these classes exist in 3.7.0: they were promoted out of the
# `...oauthbearer.secured` package in 3.6, and the old package was removed in
# 4.0. Pin the promoted names, which are the ones that survive a newer image.
OAUTH_LOGIN_HANDLER="org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler"
OAUTH_VALIDATOR_HANDLER="org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler"

# Bring up the throwaway OIDC provider S4 authenticates against, and give it a
# name the cluster can resolve.
#
# It mints RS256-signed tokens and publishes a JWKS rather than using Kafka's
# `OAuthBearerUnsecuredValidatorCallbackHandler`, which accepts any unsigned
# JWT. With the unsecured handler "the broker accepted our token" is true of
# every token that has ever existed, so the stage would be green and vacuous --
# the same class of mistake as leaving auto.create.topics.enable on.
ensure_oidc() {
  local dir; dir="$(cd "$(dirname "$0")/oidc" 2>/dev/null && pwd)" || {
    echo "FATAL: $(dirname "$0")/oidc is missing -- S4 has no identity provider." >&2
    exit 1; }

  if [ -z "${OIDC_CLIENT_SECRET:-}" ]; then
    if [ ! -s "$OIDC_SECRET_FILE" ]; then
      (umask 077; gen_secret > "$OIDC_SECRET_FILE")
    fi
    OIDC_CLIENT_SECRET="$(cat "$OIDC_SECRET_FILE")"
  fi
  chmod 600 "$OIDC_SECRET_FILE" 2>/dev/null || true

  if ! docker image inspect "$OIDC_IMAGE" >/dev/null 2>&1; then
    echo "building $OIDC_IMAGE from $dir"
    docker build -q -t "$OIDC_IMAGE" "$dir" >/dev/null
  fi

  docker rm -f "$OIDC_NAME" >/dev/null 2>&1 || true
  # --env-file, not -e, for the one value that is a secret: `docker run -e K=$V`
  # spells V in the HOST process table for the lifetime of the command.
  local envf; envf="$(mktemp)"; chmod 600 "$envf"
  printf 'OIDC_CLIENT_SECRET=%s
' "$OIDC_CLIENT_SECRET" > "$envf"
  docker run -d --name "$OIDC_NAME" --network "$NET"     --env-file "$envf"     -e OIDC_ISSUER="$OIDC_ISSUER"     -e OIDC_AUDIENCE="$OIDC_AUDIENCE"     -e OIDC_CLIENT_ID="$OIDC_CLIENT_ID"     -e OIDC_PORT="$OIDC_PORT"     -v "$OIDC_KEY_VOLUME:/keys"     "$OIDC_IMAGE" >/dev/null
  rm -f "$envf"

  local ready=""
  for i in $(seq 1 30); do
    if docker exec "$OIDC_NAME" python -c "
import sys, urllib.request
try:
    urllib.request.urlopen('http://127.0.0.1:$OIDC_PORT/healthz', timeout=2).read()
except Exception:
    sys.exit(1)" >/dev/null 2>&1; then
      ready=1; break
    fi
    sleep 1
  done
  if [ -z "$ready" ]; then
    echo "FATAL: the OIDC provider never became ready." >&2
    docker logs --tail 30 "$OIDC_NAME" >&2
    exit 1
  fi

  # A server that answers /healthz but serves an empty JWKS would make every
  # token unverifiable, and the broker's rejection would look like a client
  # bug. Assert the shape, not just the liveness -- and assert the kid, because
  # an empty `keys` list is valid JSON and a count of zero is not an error.
  docker exec "$OIDC_NAME" python -c "
import json, sys, urllib.request
d = json.load(urllib.request.urlopen('http://127.0.0.1:$OIDC_PORT/jwks', timeout=5))
ks = d.get('keys') or []
assert len(ks) == 1, 'JWKS has %d keys, expected exactly 1' % len(ks)
assert ks[0].get('kid') == 's4', 'unexpected kid %r' % ks[0].get('kid')
assert ks[0].get('n') and ks[0].get('e'), 'JWKS key has no modulus/exponent'
" || { echo "FATAL: the OIDC provider is not serving a usable JWKS." >&2; exit 1; }

  # CONTROL. If the token endpoint handed out a token to anyone who asked, the
  # client secret the chart carries would be decorative and S4 would prove only
  # that a JWT can be fetched -- not that the credential was checked.
  if docker exec -e CID="$OIDC_CLIENT_ID" "$OIDC_NAME" python -c "
import os, sys, urllib.request, urllib.parse, urllib.error
body = urllib.parse.urlencode({
    'grant_type': 'client_credentials',
    'client_id': os.environ['CID'],
    'client_secret': 'definitely-not-the-secret',
}).encode()
try:
    urllib.request.urlopen('http://127.0.0.1:$OIDC_PORT/token', body, timeout=5)
except urllib.error.HTTPError as e:
    sys.exit(0 if e.code == 401 else 1)
sys.exit(1)" >/dev/null 2>&1; then
    :
  else
    echo "FATAL: the OIDC token endpoint did not reject a wrong client secret." >&2
    echo "Every downstream 'authenticated' result would be meaningless." >&2
    exit 1
  fi
  echo "oidc control OK: /token refuses a wrong client secret"

  local oip
  oip="$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" "$OIDC_NAME")"
  if [ -z "$oip" ]; then
    echo "FATAL: the OIDC provider has no address on network $NET" >&2; exit 1
  fi

  # Same selectorless Service + EndpointSlice shape as the broker, and for the
  # same reason: the pods must reach this by the DNS name that appears in
  # `kafka.external.oauth.tokenEndpoint`, or the values file under test would be
  # describing something the cluster cannot dial.
  kubectl --context "$KCTX" apply -f - <<YAML >/dev/null
apiVersion: v1
kind: Service
metadata:
  name: $OIDC_NAME
  namespace: $NS
spec:
  clusterIP: None
  ports:
    - name: http
      port: $OIDC_PORT
      targetPort: $OIDC_PORT
      protocol: TCP
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: $OIDC_NAME
  namespace: $NS
  labels:
    kubernetes.io/service-name: $OIDC_NAME
addressType: IPv4
ports:
  - name: http
    port: $OIDC_PORT
    protocol: TCP
endpoints:
  - addresses: ["$oip"]
    conditions:
      ready: true
YAML
  echo "oidc ready at $oip on network $NET; issuer $OIDC_ISSUER"
}

# TLS material for the SASL_SSL stages. Generated once into a gitignored dir so
# repeated runs keep the same CA -- a CA that changes on every run would make the
# chart's `caCert` value stale the moment the broker restarts, and the resulting
# handshake failure would look exactly like a chart bug.
#
# Layout is two directories on purpose, and the modes are not decoration:
#
#   .tls/            0700  ca.key lives here and is NEVER mounted anywhere.
#   .tls/broker/     0755  broker.pem + ca.crt, both 0644, bind-mounted read-only.
#
# The broker runs as uid 1000 inside the container, and a bind mount carries the
# HOST inode's mode through. A 0700 mount point is therefore unreadable to the
# broker -- it fails at start-up with a keystore error that reads like a bad file
# format. So the leaf must be 0755/0644, and the confidentiality comes from the
# 0700 parent instead: no other host user can traverse into it.
TLS_DIR="${BROKER_TLS_DIR:-$(dirname "$0")/.tls}"
TLS_MOUNT="$TLS_DIR/broker"

# The private key MUST be PKCS#8 ("BEGIN PRIVATE KEY"). Kafka's PEM store cannot
# parse PKCS#1 ("BEGIN RSA PRIVATE KEY") and reports it as a malformed keystore,
# not as a wrong key format. LibreSSL -- which is what /usr/bin/openssl is on
# macOS -- also lacks `-addext`, so a SAN silently never lands and every client
# then fails hostname verification against a certificate that looks fine.
# Both failure modes are wrong answers rather than errors, so pick the binary
# deliberately and then assert on the artefact it produced.
pick_openssl() {
  local c
  for c in "${OPENSSL:-}" openssl /opt/homebrew/bin/openssl /usr/local/bin/openssl; do
    [ -n "$c" ] || continue
    command -v "$c" >/dev/null 2>&1 || continue
    case "$("$c" version 2>/dev/null)" in
      OpenSSL\ [3-9]*) echo "$c"; return 0 ;;
    esac
  done
  echo "FATAL: no OpenSSL 3.x binary found (LibreSSL will not do -- see comment)." >&2
  echo "Install one (brew install openssl) or set OPENSSL=/path/to/openssl." >&2
  return 1
}

ensure_tls_material() {
  local SSL; SSL="$(pick_openssl)" || exit 1

  if [ -s "$TLS_MOUNT/broker.pem" ] && [ -s "$TLS_MOUNT/ca.crt" ] && \
     "$SSL" x509 -in "$TLS_MOUNT/ca.crt" -noout -checkend 86400 >/dev/null 2>&1; then
    echo "reusing TLS material in $TLS_MOUNT"
  else
    echo "generating a throwaway CA and broker certificate in $TLS_MOUNT"
    rm -rf "$TLS_DIR"
    (umask 077; mkdir -p "$TLS_DIR")
    mkdir -p "$TLS_MOUNT"; chmod 755 "$TLS_MOUNT"

    "$SSL" genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
      -out "$TLS_DIR/ca.key" 2>/dev/null
    chmod 600 "$TLS_DIR/ca.key"
    "$SSL" req -x509 -new -key "$TLS_DIR/ca.key" -sha256 -days 3650 \
      -subj "/CN=rsync-byo-kind-test-ca/O=rsync-ai-test" \
      -out "$TLS_MOUNT/ca.crt" 2>/dev/null

    "$SSL" genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
      -out "$TLS_DIR/broker.key" 2>/dev/null
    chmod 600 "$TLS_DIR/broker.key"
    "$SSL" req -new -key "$TLS_DIR/broker.key" \
      -subj "/CN=$FQDN/O=rsync-ai-test" -out "$TLS_DIR/broker.csr" 2>/dev/null

    # The SAN is the whole point of the stage. It carries the advertised name --
    # clients bootstrap at $FQDN and reconnect to $FQDN -- plus the short name and
    # loopback, so an in-container probe can reach the same listener without the
    # certificate being the thing that decides the outcome.
    cat > "$TLS_DIR/ext.cnf" <<EXT
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:$FQDN, DNS:$NAME, DNS:localhost, IP:127.0.0.1
EXT
    "$SSL" x509 -req -in "$TLS_DIR/broker.csr" \
      -CA "$TLS_MOUNT/ca.crt" -CAkey "$TLS_DIR/ca.key" -CAcreateserial \
      -days 825 -sha256 -extfile "$TLS_DIR/ext.cnf" \
      -out "$TLS_DIR/broker.crt" 2>/dev/null

    # ONE file holding leaf + issuer + key: that is what a JVM PEM keystore is.
    # Java cannot take the separate cert/key paths the Go and Python clients read.
    cat "$TLS_DIR/broker.crt" "$TLS_MOUNT/ca.crt" "$TLS_DIR/broker.key" \
      > "$TLS_MOUNT/broker.pem"
    chmod 644 "$TLS_MOUNT/broker.pem" "$TLS_MOUNT/ca.crt"
  fi

  # Assert on the artefacts, not on the tool. Both of these are wrong-answer
  # failures at the broker: a PKCS#1 key and a missing SAN each surface as
  # something that reads like an unrelated problem.
  grep -q -- "-----BEGIN PRIVATE KEY-----" "$TLS_MOUNT/broker.pem" || {
    echo "FATAL: broker.pem does not contain a PKCS#8 private key." >&2
    echo "Kafka's PEM store rejects PKCS#1 with a 'malformed keystore' error." >&2
    exit 1; }
  "$SSL" x509 -in "$TLS_MOUNT/broker.pem" -noout -text 2>/dev/null \
    | grep -q "DNS:$FQDN" || {
    echo "FATAL: the broker certificate has no SAN for $FQDN." >&2
    echo "Every client would fail hostname verification, and the failure would" >&2
    echo "look like a chart TLS bug rather than a harness bug." >&2
    exit 1; }
  echo "TLS material OK: CA + broker cert with SAN DNS:$FQDN"
}

docker rm -f "$NAME" >/dev/null 2>&1 || true

# Per-stage extras. Declared here rather than inside the case so `set -u` cannot
# turn "this stage did not set it" into a crash on an unrelated line -- and so
# the plaintext stages keep exactly the shape they had before S3 existed.
mount_args=()
PROBE_PROTOCOL="SASL_PLAINTEXT"
PROBE_TLS_PROPS=""

common_env=(
  -e KAFKA_NODE_ID=1
  -e KAFKA_PROCESS_ROLES=broker,controller
  -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@localhost:9094"
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
  -e KAFKA_INTER_BROKER_LISTENER_NAME=ADMIN
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false
  -e KAFKA_LOG_DIRS=/tmp/kraft-combined-logs
  -e CLUSTER_ID="$CLUSTER_ID"
  -e KAFKA_HEAP_OPTS="-Xms256m -Xmx512m"
)

case "$STAGE" in
  S0)
    # PLAINTEXT everywhere. Baseline: proves the external-broker plumbing (DNS,
    # advertised listeners, kafka-init, Connect, the sink) with authentication
    # removed as a variable. If S0 is red, the harness is broken, not the chart.
    stage_env=(
      -e KAFKA_LISTENERS="ADMIN://0.0.0.0:9091,EXTERNAL://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094"
      -e KAFKA_ADVERTISED_LISTENERS="ADMIN://localhost:9091,EXTERNAL://$FQDN:9093"
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="ADMIN:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
    )
    MECH=""
    ;;
  S1)
    # SASL_PLAINTEXT + SCRAM-SHA-512 on the EXTERNAL listener only. The ADMIN
    # listener stays PLAINTEXT on purpose: ground truth about which topics exist
    # must not travel the path under test, or a broken auth path and a working
    # one both report "no topics".
    #
    # The env-var spelling is the apache/kafka image's property mangling:
    #   .  -> _      _ -> __      - -> ___
    # so listener.name.external.scram-sha-512.sasl.jaas.config becomes
    # LISTENER_NAME_EXTERNAL_SCRAM___SHA___512_SASL_JAAS_CONFIG. Get it wrong and
    # the property is silently ignored rather than rejected -- which is why the
    # negative control below is not optional.
    MECH="SCRAM-SHA-512"
    stage_env=(
      -e KAFKA_LISTENERS="ADMIN://0.0.0.0:9091,EXTERNAL://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094"
      -e KAFKA_ADVERTISED_LISTENERS="ADMIN://localhost:9091,EXTERNAL://$FQDN:9093"
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="ADMIN:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT"
      -e KAFKA_SASL_ENABLED_MECHANISMS="SCRAM-SHA-512"
      -e KAFKA_LISTENER_NAME_EXTERNAL_SCRAM___SHA___512_SASL_JAAS_CONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required;"
    )
    ;;
  S2)
    # SASL_PLAINTEXT + PLAIN. The ONLY delta from S1 is the mechanism, which is
    # the point: S1 and S2 differ by one broker property and one values key, so
    # anything that breaks here is attributable to mechanism handling and to
    # nothing else. This is the stage that would have caught the hardcoded
    # ScramLoginModule in CONNECT_SASL_JAAS_CONFIG.
    #
    # PLAIN has no dynamic credential store: unlike SCRAM there is no
    # kafka-configs.sh --add-config, the user list IS the listener's JAAS config
    # (user_<name>=<password>). So the credential has to exist at broker start,
    # which is why it is passed through --env-file below rather than -e: a
    # `docker run -e K=secret` puts the value in the HOST process table.
    #
    # `plain` carries no . _ or - so it mangles to itself:
    #   listener.name.external.plain.sasl.jaas.config
    #     -> KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG
    MECH="PLAIN"
    stage_env=(
      -e KAFKA_LISTENERS="ADMIN://0.0.0.0:9091,EXTERNAL://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094"
      -e KAFKA_ADVERTISED_LISTENERS="ADMIN://localhost:9091,EXTERNAL://$FQDN:9093"
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="ADMIN:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT"
      -e KAFKA_SASL_ENABLED_MECHANISMS="PLAIN"
    )
    ;;
  S3)
    # SASL_SSL + SCRAM-SHA-512 against a PRIVATE CA. The mechanism is deliberately
    # S1's, so S3 differs from S1 on exactly one axis -- transport -- and anything
    # that breaks here is attributable to TLS and to nothing else. (S2 already
    # isolated the mechanism axis the same way.)
    #
    # This is the stage the chart had no answer for at all: before this branch of
    # work there was no `kafka.external.tls` surface, so a customer on a private
    # CA had nowhere to put the bundle and every client failed its handshake.
    #
    # Broker side uses PEM keystores for the same reason the chart does (KIP-651,
    # Kafka 2.7+): one file, no keytool, no JKS conversion step. An unencrypted
    # PKCS#8 key needs no keystore password, which also keeps a password off the
    # docker argv.
    #
    # ADMIN stays PLAINTEXT: ground truth must not travel the path under test.
    # ssl.client.auth=none: this stage tests server-auth TLS + SASL. mTLS is a
    # separate axis, covered by the chart's render matrix rather than here.
    MECH="SCRAM-SHA-512"
    ensure_tls_material
    mount_args=( -v "$(cd "$TLS_MOUNT" && pwd):/etc/kafka/secrets:ro" )
    PROBE_PROTOCOL="SASL_SSL"
    PROBE_TLS_PROPS=$'ssl.truststore.type=PEM\nssl.truststore.location=/etc/kafka/secrets/ca.crt'
    stage_env=(
      -e KAFKA_LISTENERS="ADMIN://0.0.0.0:9091,EXTERNAL://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094"
      -e KAFKA_ADVERTISED_LISTENERS="ADMIN://localhost:9091,EXTERNAL://$FQDN:9093"
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="ADMIN:PLAINTEXT,EXTERNAL:SASL_SSL,CONTROLLER:PLAINTEXT"
      -e KAFKA_SASL_ENABLED_MECHANISMS="SCRAM-SHA-512"
      -e KAFKA_LISTENER_NAME_EXTERNAL_SCRAM___SHA___512_SASL_JAAS_CONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required;"
      -e KAFKA_SSL_KEYSTORE_TYPE=PEM
      -e KAFKA_SSL_KEYSTORE_LOCATION=/etc/kafka/secrets/broker.pem
      -e KAFKA_SSL_TRUSTSTORE_TYPE=PEM
      -e KAFKA_SSL_TRUSTSTORE_LOCATION=/etc/kafka/secrets/ca.crt
      -e KAFKA_SSL_CLIENT_AUTH=none
    )
    ;;
  S4)
    # SASL_SSL + OAUTHBEARER against a real OIDC provider. The transport is
    # S3's, so S4 differs from S3 on exactly one axis -- the mechanism -- and
    # anything that breaks here is attributable to the token path and to
    # nothing else. (S2 isolated the mechanism axis against S1 the same way,
    # and S3 isolated transport.)
    #
    # This is the only stage where the client's Kafka credential is not a
    # secret the chart holds. The chart holds an OIDC client secret; the Kafka
    # credential is the JWT the client fetches with it, and it expires. So the
    # thing under test is a two-hop path -- pod -> IdP -> broker -- and both
    # hops have to be wrong-answer-proof:
    #
    #   * the IdP refuses a wrong client secret (control in ensure_oidc), so
    #     "we got a token" is a statement about the credential;
    #   * the broker verifies the RS256 signature against a published JWKS, so
    #     "the broker accepted it" is a statement about the issuer. The rogue
    #     control near the end of this script is what proves that second half:
    #     it presents a well-formed token signed by a key the JWKS does not
    #     contain, and it MUST be refused.
    #
    # Server side needs no credentials in its JAAS line -- validation is the
    # callback handler's job, and the handler is the SECURED one on purpose.
    # `OAuthBearerUnsecuredValidatorCallbackHandler` would accept any unsigned
    # JWT, which is green and vacuous.
    MECH="OAUTHBEARER"
    ensure_tls_material
    ensure_oidc
    mount_args=( -v "$(cd "$TLS_MOUNT" && pwd):/etc/kafka/secrets:ro" )
    PROBE_PROTOCOL="SASL_SSL"
    PROBE_TLS_PROPS=$'ssl.truststore.type=PEM\nssl.truststore.location=/etc/kafka/secrets/ca.crt'
    # `oauthbearer` carries no . _ or - so it mangles to itself:
    #   listener.name.external.oauthbearer.sasl.jaas.config
    #     -> KAFKA_LISTENER_NAME_EXTERNAL_OAUTHBEARER_SASL_JAAS_CONFIG
    # The JWKS/audience/issuer trio is broker-level rather than listener-scoped
    # because only one listener speaks SASL at all here; a mistyped key is
    # dropped in silence either way, which is what the negative control exists
    # for.
    stage_env=(
      -e KAFKA_LISTENERS="ADMIN://0.0.0.0:9091,EXTERNAL://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094"
      -e KAFKA_ADVERTISED_LISTENERS="ADMIN://localhost:9091,EXTERNAL://$FQDN:9093"
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="ADMIN:PLAINTEXT,EXTERNAL:SASL_SSL,CONTROLLER:PLAINTEXT"
      -e KAFKA_SASL_ENABLED_MECHANISMS="OAUTHBEARER"
      -e KAFKA_LISTENER_NAME_EXTERNAL_OAUTHBEARER_SASL_JAAS_CONFIG="org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
      -e KAFKA_LISTENER_NAME_EXTERNAL_OAUTHBEARER_SASL_SERVER_CALLBACK_HANDLER_CLASS="$OAUTH_VALIDATOR_HANDLER"
      -e KAFKA_SASL_OAUTHBEARER_JWKS_ENDPOINT_URL="http://$OIDC_NAME:$OIDC_PORT/jwks"
      -e KAFKA_SASL_OAUTHBEARER_EXPECTED_AUDIENCE="$OIDC_AUDIENCE"
      -e KAFKA_SASL_OAUTHBEARER_EXPECTED_ISSUER="$OIDC_ISSUER"
      -e KAFKA_SSL_KEYSTORE_TYPE=PEM
      -e KAFKA_SSL_KEYSTORE_LOCATION=/etc/kafka/secrets/broker.pem
      -e KAFKA_SSL_TRUSTSTORE_TYPE=PEM
      -e KAFKA_SSL_TRUSTSTORE_LOCATION=/etc/kafka/secrets/ca.crt
      -e KAFKA_SSL_CLIENT_AUTH=none
    )
    ;;
  *)
    echo "FATAL: stage '$STAGE' is not implemented in this script yet." >&2
    echo "Refusing to fall back to S0 -- a stage that silently runs the wrong" >&2
    echo "auth mode produces a green run that means nothing." >&2
    exit 1
    ;;
esac

# No static --ip: kind creates its network without a user-configured subnet, so
# docker rejects one. The address is read back instead and the EndpointSlice is
# generated from it, which is the better shape anyway -- there is no IP constant
# in a second file to drift out of step with reality.
# The login module is a FUNCTION of the mechanism, on the broker side exactly as
# it is on the client side. Deriving it here rather than repeating a literal is
# what lets the positive control below be mechanism-agnostic -- and a control
# that only works for one mechanism is a control that silently stops testing the
# moment a stage changes.
case "${MECH:-}" in
  "")              LOGIN_MODULE="" ;;
  SCRAM-SHA-256|SCRAM-SHA-512) LOGIN_MODULE=org.apache.kafka.common.security.scram.ScramLoginModule ;;
  PLAIN)           LOGIN_MODULE=org.apache.kafka.common.security.plain.PlainLoginModule ;;
  OAUTHBEARER)     LOGIN_MODULE=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule ;;
  *) echo "FATAL: broker-up.sh has no login module for mechanism '$MECH'" >&2; exit 1 ;;
esac

# ...and so is the CLIENT-side credential shape, which is where the mechanisms
# stop being interchangeable. PLAIN and SCRAM take a username and a password as
# JAAS options and need nothing else. OAUTHBEARER takes a client id and secret
# as JAAS options, needs a login callback handler to do anything with them, and
# takes the token endpoint as a SEPARATE property -- three settings across two
# grammars, none of which the other mechanisms have.
#
# Deriving both here keeps the controls below mechanism-agnostic. Building the
# JAAS line inside each probe instead would mean three copies of that branch,
# and a control that only works for one mechanism silently stops testing the
# moment a stage changes.
PROBE_SASL_PROPS=""
case "${MECH:-}" in
  "")
    PROBE_JAAS=""
    ;;
  OAUTHBEARER)
    PROBE_JAAS="$LOGIN_MODULE required clientId=\"$OIDC_CLIENT_ID\" clientSecret=\"$OIDC_CLIENT_SECRET\";"
    # The handler is not optional and its absence is not an error: omit it and
    # OAuthBearerLoginModule falls back to its UNSECURED default, mints a
    # self-signed JWS locally, never contacts the IdP at all -- and the probe
    # then measures whether the broker accepts forgeries.
    PROBE_SASL_PROPS="sasl.login.callback.handler.class=$OAUTH_LOGIN_HANDLER
sasl.oauthbearer.token.endpoint.url=http://$OIDC_NAME:$OIDC_PORT/token"
    ;;
  *)
    PROBE_JAAS="$LOGIN_MODULE required username=\"$SASL_USER\" password=\"$BROKER_SASL_PASSWORD\";"
    ;;
esac

# PLAIN's user list lives in the broker's own JAAS config, so the credential must
# be present at start-up. It goes in through --env-file: `docker run -e K=$PW`
# would spell the password on the HOST argv, where any other process can read it
# for the lifetime of the command.
ENV_FILE=""
if [ "${MECH:-}" = "PLAIN" ]; then
  ENV_FILE="$(mktemp)"; chmod 600 "$ENV_FILE"
  trap 'rm -f "$ENV_FILE"' EXIT
  printf 'KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG=%s required user_%s="%s";\n' \
    "$LOGIN_MODULE" "$SASL_USER" "$BROKER_SASL_PASSWORD" > "$ENV_FILE"
fi

docker run -d --name "$NAME" --network "$NET" \
  ${ENV_FILE:+--env-file "$ENV_FILE"} \
  ${mount_args[@]+"${mount_args[@]}"} \
  "${common_env[@]}" "${stage_env[@]}" "$IMAGE" >/dev/null

IP="$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" "$NAME")"
if [ -z "$IP" ]; then
  echo "FATAL: broker has no address on network $NET" >&2; exit 1
fi
echo "started $NAME ($STAGE) at $IP on network $NET, advertising $FQDN:9093"

# Readiness on the ADMIN path only.
ready=""
for i in $(seq 1 60); do
  if docker exec "$NAME" /opt/kafka/bin/kafka-topics.sh \
       --bootstrap-server localhost:9091 --list >/dev/null 2>&1; then
    echo "broker ready after ${i}s"; ready=1; break
  fi
  sleep 1
done
if [ -z "$ready" ]; then
  echo "FATAL: broker did not become ready on the ADMIN listener" >&2
  docker logs --tail 40 "$NAME" >&2
  exit 1
fi

# SCRAM credentials are created over the ADMIN listener AFTER the broker is up.
# In KRaft they would otherwise have to be baked in at `kafka-storage format`
# time; that is only required for the inter-broker listener, and ours is
# PLAINTEXT, so runtime creation is both sufficient and easier to re-run.
if [ "$STAGE" != "S0" ]; then
  # Teach the broker container to resolve its own advertised name. A Kafka client
  # bootstraps at one address and then reconnects to whatever the broker
  # advertises, so without this every in-container probe dies on
  # UnknownHostException -- which would make the negative control below pass for
  # the wrong reason, and a control that passes for the wrong reason is worse
  # than no control at all.
  docker exec -u 0 "$NAME" sh -c "grep -q ' $FQDN\$' /etc/hosts || echo '$IP $FQDN' >> /etc/hosts"

  # Run through `sh -c` inside the container so $PW is expanded there. Spelling
  # it on the docker-exec argv instead would put the password in the HOST's
  # process table for as long as the command runs.
  # SCRAM only. PLAIN has no credential store to write to -- its user list IS the
  # listener's JAAS config, already supplied at start-up.
  case "$MECH" in
    SCRAM-SHA-*)
      docker exec -e PW="$BROKER_SASL_PASSWORD" -e U="$SASL_USER" -e M="$MECH" "$NAME" sh -c '
        /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9091 \
          --alter --entity-type users --entity-name "$U" \
          --add-config "$M=[password=$PW]"' >/dev/null
      echo "provisioned $MECH credential for user '"'"'$SASL_USER'"'"'"
      ;;
    PLAIN)
      echo "PLAIN: credential supplied in the listener JAAS config at start-up"
      ;;
    OAUTHBEARER)
      # Spelled out rather than left to fall through the case: "nothing to do"
      # and "this mechanism was forgotten here" look identical in silence.
      # There is no credential to write to the broker at all -- the client's
      # credential is a JWT it fetches from the IdP, and the broker only ever
      # holds the public half, over JWKS.
      echo "OAUTHBEARER: no broker-side credential; trust is the JWKS at http://$OIDC_NAME:$OIDC_PORT/jwks"
      ;;
  esac

  # NEGATIVE CONTROL. Without this the stage is vacuous: a listener whose SASL
  # config was silently ignored (one wrong underscore above) accepts anonymous
  # clients, everything downstream passes, and the run "proves" the chart handles
  # authentication when nothing was ever authenticated.
  # A PLAINTEXT client against a SASL_PLAINTEXT listener does NOT get a clean
  # authentication error -- the handshake never completes and the client stalls
  # until it is killed. So the assertion here is only "did not succeed"; it is
  # the POSITIVE control immediately below, on the same address, that separates
  # "rejected because unauthenticated" from "listener is simply broken". Neither
  # control means anything alone.
  if docker exec "$NAME" timeout 20 /opt/kafka/bin/kafka-topics.sh \
       --bootstrap-server "$FQDN:9093" --list >/dev/null 2>&1; then
    echo "FATAL: the EXTERNAL listener served an UNAUTHENTICATED client." >&2
    echo "SASL is not in force, so every result from this stage would be" >&2
    echo "meaningless. Check the property mangling in stage_env, then re-read" >&2
    echo "/opt/kafka/config/server.properties in the container to confirm which" >&2
    echo "keys actually landed -- a mistyped key is dropped, never rejected." >&2
    exit 1
  fi
  echo "negative control OK: EXTERNAL did not serve an unauthenticated client"

  # POSITIVE CONTROL. "Rejects unauthenticated clients" and "rejects everyone"
  # are the same observation from the outside -- a listener that is simply broken
  # passes the check above. Without this pair, every downstream failure would be
  # attributable to either the chart or the harness, and there would be no way to
  # tell which.
  docker exec -e JAAS="$PROBE_JAAS" -e EP="$FQDN:9093" \
              -e M="$MECH" -e PROTO="$PROBE_PROTOCOL" \
              -e SASLP="$PROBE_SASL_PROPS" \
              -e TLSP="$PROBE_TLS_PROPS" "$NAME" sh -c '
    umask 077
    cat > /tmp/probe.properties <<EOF
security.protocol=$PROTO
sasl.mechanism=$M
sasl.jaas.config=$JAAS
$SASLP
$TLSP
EOF
    timeout 25 /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$EP" \
      --command-config /tmp/probe.properties --list >/dev/null' || {
    echo "FATAL: the EXTERNAL listener rejected a client WITH valid credentials." >&2
    echo "The listener is broken, not secured -- fix the harness before reading" >&2
    echo "anything into what the chart does next." >&2
    exit 1
  }
  echo "positive control OK: EXTERNAL accepts the provisioned credential ($PROBE_PROTOCOL)"

  # THIRD CONTROL, TLS stages only, and the one that makes S3 mean anything.
  #
  # The pair above cannot distinguish "TLS is in force" from "TLS is configured
  # and irrelevant". A client carrying the right credential AND the right CA
  # succeeds either way; a client carrying neither fails either way. So this
  # probe holds the credential fixed and removes ONLY the CA: same user, same
  # mechanism, same SASL_SSL protocol, default JDK trust store.
  #
  # It must be REJECTED. If it is served, the broker is trusting something it
  # should not -- and every "the chart mounted the CA correctly" conclusion drawn
  # downstream would be unfalsifiable, because the clients would connect with or
  # without the bundle the chart went to such lengths to deliver.
  if [ -n "$PROBE_TLS_PROPS" ]; then
    if docker exec -e JAAS="$PROBE_JAAS" -e EP="$FQDN:9093" \
                   -e M="$MECH" -e SASLP="$PROBE_SASL_PROPS" "$NAME" sh -c '
      umask 077
      cat > /tmp/notrust.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=$M
sasl.jaas.config=$JAAS
$SASLP
EOF
      timeout 25 /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$EP" \
        --command-config /tmp/notrust.properties --list >/dev/null 2>&1'; then
      echo "FATAL: EXTERNAL served a client that did NOT trust the private CA." >&2
      echo "The broker certificate is chaining to a public root, or TLS is not" >&2
      echo "actually in force. Either way the CA plumbing under test is not being" >&2
      echo "exercised, so this stage would pass without proving anything." >&2
      exit 1
    fi
    echo "CA control OK: EXTERNAL rejects a valid credential presented without the CA"
  fi

  # FOURTH CONTROL, token mechanisms only, and the one that makes S4 mean
  # anything.
  #
  # The three controls above cannot distinguish "the broker verified our token"
  # from "the broker accepts any well-formed token". Both a valid JWT and a
  # forged one are bytes that parse; the SASL exchange looks the same from the
  # outside. So this probe holds EVERYTHING fixed -- same client id, same client
  # secret, same IdP, same CA, same protocol -- and changes one thing: it asks
  # the IdP for a token signed by a key that is NOT in the published JWKS.
  #
  # It must be REJECTED. If it is served, the validator is not validating: the
  # signature is unchecked, or the handler silently fell back to the UNSECURED
  # one, and every "OAUTHBEARER works" conclusion drawn from this stage would
  # hold equally for a token minted by anyone at all.
  if [ "$MECH" = "OAUTHBEARER" ]; then
    if docker exec -e JAAS="$PROBE_JAAS" -e EP="$FQDN:9093" -e M="$MECH" \
                   -e H="$OAUTH_LOGIN_HANDLER" -e TLSP="$PROBE_TLS_PROPS" \
                   -e ROGUE="http://$OIDC_NAME:$OIDC_PORT/token?rogue=1" "$NAME" sh -c '
      umask 077
      cat > /tmp/rogue.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=$M
sasl.jaas.config=$JAAS
sasl.login.callback.handler.class=$H
sasl.oauthbearer.token.endpoint.url=$ROGUE
$TLSP
EOF
      timeout 25 /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$EP" \
        --command-config /tmp/rogue.properties --list >/dev/null 2>&1'; then
      echo "FATAL: EXTERNAL served a client holding a token signed by a key" >&2
      echo "that is NOT in the published JWKS. The broker is not verifying" >&2
      echo "signatures -- check that the server callback handler is the SECURED" >&2
      echo "validator and that sasl.oauthbearer.jwks.endpoint.url actually" >&2
      echo "landed. A mistyped property is dropped, never rejected." >&2
      exit 1
    fi
    echo "rogue-token control OK: EXTERNAL rejects a token signed off the JWKS"
  fi
fi

# In-cluster DNS for a broker that is not in the cluster. Headless Service +
# hand-written EndpointSlice: the name resolves straight to the broker address,
# which is both what the broker advertises and what the TLS SAN must carry in the
# SASL_SSL stages.
kubectl --context "$KCTX" apply -f - <<YAML
apiVersion: v1
kind: Service
metadata:
  name: $NAME
  namespace: $NS
spec:
  clusterIP: None
  ports:
    - name: kafka
      port: 9093
      targetPort: 9093
      protocol: TCP
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: $NAME
  namespace: $NS
  labels:
    kubernetes.io/service-name: $NAME
addressType: IPv4
ports:
  - name: kafka
    port: 9093
    protocol: TCP
endpoints:
  - addresses: ["$IP"]
    conditions:
      ready: true
YAML

echo "OK: $FQDN:9093 -> $IP"
