#!/bin/bash
# rsync-ai Kafka Connect entrypoint: turns rsync's KAFKA_* security contract --
# the variables the Go and Python services read -- into the CONNECT_* worker
# properties, then hands over to the base image's /docker-entrypoint.sh.
#
# It lives in the IMAGE so compose and Helm run the same code. Both used to
# carry their own copy of part of this (Helm an inline prelude, compose
# nothing), and the gaps between them were the defects: compose's kafka-connect
# could not use KAFKA_SASL_USERNAME/PASSWORD or OAuth at all, and neither could
# use the two-path mTLS pair (KAFKA_SSL_CERT_LOCATION + KAFKA_SSL_KEY_LOCATION).
#
# Anything the deployer already set wins: a non-empty CONNECT_SASL_JAAS_CONFIG
# or CONNECT_SSL_KEYSTORE_LOCATION is left alone. Secrets are never echoed; the
# base entrypoint's SENSITIVE_PROPERTIES list hides the JAAS lines it prints.
#
# Runs for every command, not only `start`: a JVM client started in this image
# (schema-history probes, the security matrix) sees the same derived keystore
# file the worker does.
#
# Contract, pinned by deploy/helm/rsync-ai/test/kind/kafka-matrix (cx-* rows)
# and llm-service/tests/test_chart_kafka_security_env.py.
set -euo pipefail

# The combined keystore built from the two-path mTLS pair. Debezium's
# schema-history client runs in this container and is configured by
# llm-service (kafka_security.py debezium_schema_history_security) and the
# connector (_schema_history_security), which both point at this path --
# change all three together.
RSYNC_TLS_DIR=/kafka/rsync-tls
RSYNC_CLIENT_PEM=$RSYNC_TLS_DIR/client.pem

fatal() {
  printf 'FATAL: %s\n' "$1" >&2
  shift
  local l
  for l in "$@"; do printf '       %s\n' "$l" >&2; done
  exit 1
}

# A value placed inside a JAAS quoted string crosses TWO grammars, each of which
# eats a level of backslash:
#   1. /docker-entrypoint.sh writes every CONNECT_* var into
#      connect-distributed.properties, a Java .properties file;
#   2. Kafka parses sasl.jaas.config with a StreamTokenizer-backed grammar.
# So it is escaped twice. Escaping once corrupts identically to not escaping at
# all (C:\Users\svc -> C:Userssvc, a plain "authentication failed"), which is the
# trap: the site looks guarded and is not. Measured against Kafka's own parser:
# deploy/helm/rsync-ai/test/kind/jaas-probe/run.sh.
esc() {
  printf '%s' "$1" |
    sed 's/\\/\\\\/g; s/"/\\"/g' |
    sed 's/\\/\\\\/g'
}

# True when fetching a token from $1 would put the client secret on a wire.
#
# The client-credentials grant POSTs KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET to
# the token endpoint on EVERY token fetch, so http to a remote host hands a
# non-expiring credential to anyone on the path -- worse than the replayable
# bearer token it buys. Loopback is exempt because the request never reaches a
# wire: Google's Workload Identity token server is exactly this
# (http://localhost:14293). Mirrors _token_endpoint_is_insecure in
# llm-service/src/utils/kafka_security.py and the debezium connector, and
# tokenEndpointIsInsecure in shared/go/kafkaclient/config.go.
token_endpoint_is_insecure() {
  local url=$1 scheme rest hostport host
  scheme=${url%%://*}
  scheme=$(printf '%s' "$scheme" | tr '[:upper:]' '[:lower:]')
  [ "$scheme" = https ] && return 1
  rest=${url#*://}
  hostport=${rest%%/*}
  hostport=${hostport%%\?*}
  hostport=${hostport##*@}   # drop any userinfo
  if [[ "$hostport" == \[* ]]; then
    host=${hostport#\[}
    host=${host%%]*}          # IPv6 literal, brackets stripped
  else
    host=${hostport%%:*}
  fi
  host=$(printf '%s' "$host" | tr '[:upper:]' '[:lower:]')
  host=${host%.}              # localhost. is still loopback
  [ -n "$host" ] || return 1
  case "$host" in
    localhost | *.localhost) return 1 ;;
    ::1 | 0:0:0:0:0:0:0:1) return 1 ;;
  esac
  # 127.0.0.0/8 -- wider than the literal 127.0.0.1 an operator usually writes.
  [[ "$host" =~ ^127\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]] && return 1
  return 0
}

# Connect needs every client setting at THREE levels: the worker's own client,
# the producer each source task writes with and the consumer each sink task
# reads with. Task clients do not inherit worker security, so setting only the
# worker is the silent-partial failure: RUNNING connectors, zero rows.
setall() {
  local L
  for L in "" PRODUCER_ CONSUMER_; do
    export "CONNECT_${L}$1=$2"
  done
}

PROTO=${CONNECT_SECURITY_PROTOCOL:-${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}}
PROTO=${PROTO^^}
if [ -z "${CONNECT_SECURITY_PROTOCOL:-}" ] && [ -n "${KAFKA_SECURITY_PROTOCOL:-}" ]; then
  setall SECURITY_PROTOCOL "$PROTO"
fi
KEYSTORE_FROM=none
JAAS_FROM=none

# ── mTLS client certificate ─────────────────────────────────────────────────
# Keyed off the protocol, not the material, like the Go and Python helpers: a
# cert path left over under SASL_PLAINTEXT must not switch anything on.
case "$PROTO" in
  SSL | SASL_SSL)
    CERT=${KAFKA_SSL_CERT_LOCATION:-}
    KEY=${KAFKA_SSL_KEY_LOCATION:-}
    if [ -n "${CONNECT_SSL_KEYSTORE_LOCATION:-}" ]; then
      KEYSTORE_FROM=CONNECT_SSL_KEYSTORE_LOCATION
    elif [ -n "${KAFKA_SSL_KEYSTORE_LOCATION:-}" ]; then
      [ -r "$KAFKA_SSL_KEYSTORE_LOCATION" ] ||
        fatal "KAFKA_SSL_KEYSTORE_LOCATION=$KAFKA_SSL_KEYSTORE_LOCATION is not a readable file in this container."
      setall SSL_KEYSTORE_TYPE PEM
      setall SSL_KEYSTORE_LOCATION "$KAFKA_SSL_KEYSTORE_LOCATION"
      KEYSTORE_FROM=KAFKA_SSL_KEYSTORE_LOCATION
    elif [ -n "$CERT" ] || [ -n "$KEY" ]; then
      # Half a pair is a config error, not "no mTLS": connecting without the
      # certificate would fail at the broker with a handshake alert that names
      # neither variable.
      [ -n "$KEY" ] || fatal "KAFKA_SSL_CERT_LOCATION is set but KAFKA_SSL_KEY_LOCATION is empty." \
        "mTLS needs both halves of the pair (or one combined file in KAFKA_SSL_KEYSTORE_LOCATION)."
      [ -n "$CERT" ] || fatal "KAFKA_SSL_KEY_LOCATION is set but KAFKA_SSL_CERT_LOCATION is empty." \
        "mTLS needs both halves of the pair (or one combined file in KAFKA_SSL_KEYSTORE_LOCATION)."
      for f in "$CERT" "$KEY"; do
        [ -r "$f" ] || fatal "$f (from KAFKA_SSL_CERT_LOCATION / KAFKA_SSL_KEY_LOCATION) is not a readable file in this container."
      done
      grep -q -- '-----BEGIN CERTIFICATE-----' "$CERT" ||
        fatal "KAFKA_SSL_CERT_LOCATION=$CERT holds no PEM certificate (-----BEGIN CERTIFICATE-----)."
      # A JVM PEM keystore loads ONLY an unencrypted PKCS#8 key. PKCS#1 ("BEGIN
      # RSA PRIVATE KEY"), SEC1 ("BEGIN EC PRIVATE KEY") and encrypted keys fail
      # at the first handshake with a keystore error that reads like a bad file.
      # No trailing dashes: header + later "KEY-----" reads as a key to gitleaks.
      grep -q -- '-----BEGIN PRIVATE KEY' "$KEY" ||
        fatal "KAFKA_SSL_KEY_LOCATION=$KEY must be an unencrypted PKCS#8 key (BEGIN PRIVATE KEY)." \
          "The Kafka Connect JVM cannot load PKCS#1 (BEGIN RSA PRIVATE KEY), SEC1 (BEGIN EC PRIVATE KEY)" \
          "or encrypted keys. Convert once, and point every service at the result:" \
          "  openssl pkcs8 -topk8 -nocrypt -in client.key -out client.pk8.key" \
          "The Go and Python services read the PKCS#8 file unchanged."
      # One file, chain then key: the shape a JVM PEM keystore takes. Built
      # owner-only and swapped in whole, so a restart never reads half a file.
      (
        umask 077
        mkdir -p "$RSYNC_TLS_DIR"
        tmp=$(mktemp "$RSYNC_CLIENT_PEM.XXXXXX")
        { cat "$CERT"; echo; cat "$KEY"; } >"$tmp"
        mv -f "$tmp" "$RSYNC_CLIENT_PEM"
      )
      setall SSL_KEYSTORE_TYPE PEM
      setall SSL_KEYSTORE_LOCATION "$RSYNC_CLIENT_PEM"
      KEYSTORE_FROM="KAFKA_SSL_CERT_LOCATION+KAFKA_SSL_KEY_LOCATION -> $RSYNC_CLIENT_PEM"
    fi
    ;;
esac

# ── SASL login ──────────────────────────────────────────────────────────────
# The login module is a FUNCTION of the mechanism -- PlainLoginModule for
# PLAIN, ScramLoginModule for SCRAM-SHA-*, OAuthBearerLoginModule for
# OAUTHBEARER. Compose interpolation cannot choose between them and a Helm
# template cannot escape the password (it arrives by kubelet $(VAR)
# substitution, after rendering), which is why the line is built here.
case "$PROTO" in
  SASL_PLAINTEXT | SASL_SSL)
    MECH=${CONNECT_SASL_MECHANISM:-${KAFKA_SASL_MECHANISM:-}}
    MECH=${MECH^^}
    if [ -n "${CONNECT_SASL_JAAS_CONFIG:-}" ]; then
      JAAS_FROM=CONNECT_SASL_JAAS_CONFIG
    elif [[ "${KAFKA_OPTS:-}" == *java.security.auth.login.config* ]]; then
      JAAS_FROM="KAFKA_OPTS java.security.auth.login.config"
    else
      [ -n "$MECH" ] || fatal "security protocol $PROTO needs a SASL mechanism, and KAFKA_SASL_MECHANISM is empty."
      # One arm per mechanism, in the `MECH) LOGIN_MODULE=` shape that
      # llm-service/tests/test_kafka_init_builders_stay_in_lockstep.py parses: this
      # is one of the registered copies of the mapping, pinned to its CANONICAL.
      case "$MECH" in
        PLAIN) LOGIN_MODULE=org.apache.kafka.common.security.plain.PlainLoginModule ;;
        SCRAM-SHA-256|SCRAM-SHA-512) LOGIN_MODULE=org.apache.kafka.common.security.scram.ScramLoginModule ;;
        OAUTHBEARER) LOGIN_MODULE=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule ;;
        AWS_MSK_IAM)
          fatal "KAFKA_SASL_MECHANISM=AWS_MSK_IAM is not supported by the Kafka Connect image." \
            "The image ships no aws-msk-iam-auth jar, so there is no login module to use." \
            "The Go services support it; for CDC use SCRAM-SHA-512 or mTLS (both supported by MSK)."
          ;;
        *)
          fatal "KAFKA_SASL_MECHANISM=$MECH has no built-in JAAS line here." \
            "Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER. For anything else," \
            "supply the whole line in CONNECT_SASL_JAAS_CONFIG."
          ;;
      esac
      if [ "$MECH" = OAUTHBEARER ]; then
        # Credentials travel as JAAS OPTIONS -- there is no username= or
        # password= on the line -- and the token endpoint is a separate
        # property. A credential-less "LoginModule required;" line is refused
        # by the JVM ("The OAuth configuration option clientId value must be
        # non-null"). Measured: deploy/helm/rsync-ai/test/kind/jaas-probe/OAuthProbe.java.
        ENDPOINT=${CONNECT_SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL:-${KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT:-}}
        [ -n "$ENDPOINT" ] || fatal "KAFKA_SASL_MECHANISM=OAUTHBEARER but KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT is empty." \
          "Helm: kafka.external.oauth.tokenEndpoint."
        case "$ENDPOINT" in
          https://* | http://* | HTTPS://* | HTTP://*) ;;
          *) fatal "KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT=$ENDPOINT is not an http(s) URL." \
               "The client-credentials grant is an HTTP POST (want https://issuer/oauth2/token)." ;;
        esac
        if [ "${KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT:-}" != true ] &&
          token_endpoint_is_insecure "$ENDPOINT"; then
          fatal "KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT=$ENDPOINT is http and its host is not loopback." \
            "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET would be POSTed in the clear on every token fetch," \
            "so anyone on the path keeps a credential that does not expire." \
            "Use https, a loopback address, or set" \
            "KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT=true for a disposable test rig."
        fi
        for v in KAFKA_SASL_OAUTHBEARER_CLIENT_ID KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET; do
          [ -n "${!v:-}" ] || fatal "KAFKA_SASL_MECHANISM=OAUTHBEARER but $v is empty." \
            "Helm: kafka.external.oauth.clientId, and kafka.external.oauth.clientSecret or key" \
            "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET in secrets.existingSecret."
        done
        OPTS="$(printf ' clientId="%s" clientSecret="%s"' \
          "$(esc "$KAFKA_SASL_OAUTHBEARER_CLIENT_ID")" \
          "$(esc "$KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET")")"
        if [ -n "${KAFKA_SASL_OAUTHBEARER_EXTENSIONS:-}" ]; then
          IFS=, read -r -a EXTS <<<"$KAFKA_SASL_OAUTHBEARER_EXTENSIONS"
          for kv in "${EXTS[@]}"; do
            [[ "$kv" == *=* ]] || fatal "KAFKA_SASL_OAUTHBEARER_EXTENSIONS entry '$kv' has no '=' separator."
            EK=${kv%%=*}
            EV=${kv#*=}
            # 'auth' is the OAUTHBEARER frame's own key; sending it as an
            # extension makes a malformed frame the broker rejects without
            # naming the extension.
            if [ -z "$EK" ] || [ "$EK" = auth ]; then
              fatal "KAFKA_SASL_OAUTHBEARER_EXTENSIONS has an empty or reserved name in '$kv'."
            fi
            OPTS="$OPTS$(printf ' extension_%s="%s"' "$EK" "$(esc "$EV")")"
          done
        fi
        if [ -n "${KAFKA_SASL_OAUTHBEARER_SCOPE:-}" ]; then
          OPTS="$OPTS$(printf ' scope="%s"' "$(esc "$KAFKA_SASL_OAUTHBEARER_SCOPE")")"
        fi
        JAAS="$LOGIN_MODULE required$OPTS;"
        # Without the handler OAuthBearerLoginModule falls back to its
        # UNSECURED default and mints a self-signed JWS; the broker rejects a
        # token it cannot validate and the error describes the token, not the
        # missing handler. The class moved out of `...oauthbearer.secured` in
        # Kafka 3.6, hence the override.
        setall SASL_LOGIN_CALLBACK_HANDLER_CLASS \
          "${CONNECT_SASL_LOGIN_CALLBACK_HANDLER_CLASS:-${KAFKA_SASL_OAUTHBEARER_LOGIN_CALLBACK_HANDLER:-org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler}}"
        setall SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL "$ENDPOINT"
        # kafka-clients 3.9.2 gates the token endpoint on a JVM system
        # property; UNSET MEANS ALLOW ANY URL, so this is the only thing
        # stopping a connector config -- which an API caller supplies -- from
        # naming its own token endpoint and collecting the worker's secret.
        # It is a -D, not a client property, so it cannot go through setall.
        # An operator who already pinned it in KAFKA_OPTS keeps their value.
        if [[ "${KAFKA_OPTS:-}" != *org.apache.kafka.sasl.oauthbearer.allowed.urls* ]]; then
          export KAFKA_OPTS="${KAFKA_OPTS:+$KAFKA_OPTS }-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=$ENDPOINT"
        fi
      else
        for v in KAFKA_SASL_USERNAME KAFKA_SASL_PASSWORD; do
          [ -n "${!v:-}" ] || fatal "KAFKA_SASL_MECHANISM=$MECH but $v is empty." \
            "Helm: kafka.external.saslUsername, and kafka.external.saslPassword or key" \
            "KAFKA_SASL_PASSWORD in secrets.existingSecret."
        done
        JAAS="$(printf '%s required username="%s" password="%s";' "$LOGIN_MODULE" \
          "$(esc "$KAFKA_SASL_USERNAME")" "$(esc "$KAFKA_SASL_PASSWORD")")"
      fi
      setall SASL_MECHANISM "$MECH"
      setall SASL_JAAS_CONFIG "$JAAS"
      JAAS_FROM="built for $MECH"
    fi
    ;;
esac

echo "rsync connect-entrypoint: security.protocol=$PROTO keystore=[$KEYSTORE_FROM] jaas=[$JAAS_FROM]" >&2
exec /docker-entrypoint.sh "$@"
