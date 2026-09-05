{{/*
Naming.

Core Services are release-prefixed so two releases can share a namespace.
Connector Services are NOT: their names are derived from STACK_PREFIX and are
resolved by the orchestrator as literal DNS. Two releases in one namespace
therefore need distinct `global.stackPrefix` values, which the validation block
in _validate.tpl does not enforce because it cannot see the other release.
*/}}
{{- define "rsync-ai.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rsync-ai.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "rsync-ai.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rsync-ai.labels" -}}
helm.sh/chart: {{ include "rsync-ai.chart" . }}
{{ include "rsync-ai.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: rsync-ai
{{- end -}}

{{- define "rsync-ai.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rsync-ai.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "rsync-ai.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "rsync-ai.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "rsync-ai.secretName" -}}
{{- if .Values.secrets.existingSecret -}}
{{- .Values.secrets.existingSecret -}}
{{- else -}}
{{- printf "%s-secrets" (include "rsync-ai.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Image reference. Call as:
  {{ include "rsync-ai.image" (dict "root" $ "image" .Values.apiGateway.image) }}
A per-component `tag` wins over global.image.tag, which in turn wins over the
chart's own appVersion. The release workflow sets appVersion from the git tag,
so an untouched values.yaml pins the images that shipped with that chart.
A per-component `registry`
wins over global.image.registry. A repository containing a "/" is treated as
fully qualified and the registry is not prepended, so a customer mirroring into
their own registry can override one image without restructuring the values.
*/}}
{{- define "rsync-ai.image" -}}
{{- $g := .root.Values.global -}}
{{- $i := .image | default dict -}}
{{- $repo := $i.repository -}}
{{- $tag := $i.tag | default $g.image.tag | default .root.Chart.AppVersion -}}
{{- if contains "/" $repo -}}
{{- printf "%s:%s" $repo $tag -}}
{{- else -}}
{{- printf "%s/%s:%s" ($i.registry | default $g.image.registry) $repo $tag -}}
{{- end -}}
{{- end -}}

{{/*
Connector Service name: <stackPrefix>-<id>-v<X-Y-Z>-mcp.

This is the single most load-bearing string in the chart. The orchestrator
resolves a connector by GETting http://<this>:8000/health
(backend-orchestrator/internal/mcp/server_manager.go:1010) and pre-flight builds
the same name from STACK_PREFIX (internal/workers/infra_preflight.go:177-210).
The version is written the human way in values (v1.0.0) and converted here, so
values stay readable and the wire name stays exact.
*/}}
{{- define "rsync-ai.connectorServiceName" -}}
{{- $ver := .version | default "v1.0.0" | toString | trimPrefix "v" | replace "." "-" -}}
{{- printf "%s-%s-v%s-mcp" .prefix .id $ver | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* ── Infra endpoint resolution: in-chart vs BYO, one place ───────────────── */}}

{{- define "rsync-ai.postgres.host" -}}
{{- if .Values.postgresql.enabled -}}
{{- printf "%s-postgres" (include "rsync-ai.fullname" .) -}}
{{- else -}}
{{- required "postgresql.enabled=false requires postgresql.external.host" .Values.postgresql.external.host -}}
{{- end -}}
{{- end -}}

{{- define "rsync-ai.postgres.port" -}}
{{- if .Values.postgresql.enabled -}}5432{{- else -}}{{ .Values.postgresql.external.port | default 5432 }}{{- end -}}
{{- end -}}

{{/* sslmode for the api-gateway DATABASE_URL. The in-chart Postgres has no TLS. */}}
{{- define "rsync-ai.postgres.sslmode" -}}
{{- if .Values.postgresql.enabled -}}disable{{- else -}}{{ .Values.postgresql.external.sslMode | default "require" }}{{- end -}}
{{- end -}}

{{/*
Temporal does NOT read sslMode -- the auto-setup image has its own TLS switches
(SQL_TLS_ENABLED / SQL_HOST_VERIFICATION / SQL_HOST_NAME / SQL_CA, verified in
its config_template.yaml). Deriving them from the one sslMode knob keeps the
operator from having to know that: set sslMode=require and the whole stack,
Temporal included, speaks TLS. libpq's `allow` and `prefer` are "try TLS, fall
back to plaintext", which Temporal cannot express -- it is on or off -- so they
map to off, matching the weaker of the two behaviours rather than silently
promoting to a stricter one the operator did not ask for.
*/}}
{{- define "rsync-ai.postgres.tlsEnabled" -}}
{{- $m := include "rsync-ai.postgres.sslmode" . -}}
{{- if or (eq $m "disable") (eq $m "allow") (eq $m "prefer") -}}false{{- else -}}true{{- end -}}
{{- end -}}

{{/* Only verify-full checks the server's hostname; verify-ca stops at the chain. */}}
{{- define "rsync-ai.postgres.tlsHostVerification" -}}
{{- if eq (include "rsync-ai.postgres.sslmode" .) "verify-full" -}}true{{- else -}}false{{- end -}}
{{- end -}}

{{/*
  Same fact, opposite polarity, for temporal-sql-tool. auto-setup runs the
  schema tool BEFORE the server, and the tool is a separate binary with its own
  flag set -- verified from `temporal-sql-tool --help` on the pinned image:
  SQL_TLS / SQL_TLS_CA_FILE / SQL_TLS_SERVER_NAME / SQL_TLS_DISABLE_HOST_VERIFICATION.
  Note the last one is stated as "disable", the inverse of the server's
  SQL_HOST_VERIFICATION. Deriving both from sslMode is the point: hand-set as a
  pair they drift, and the drift is invisible until a TLS-mandatory database
  rejects the schema step.
*/}}
{{- define "rsync-ai.postgres.tlsDisableHostVerification" -}}
{{- if eq (include "rsync-ai.postgres.tlsHostVerification" .) "true" -}}false{{- else -}}true{{- end -}}
{{- end -}}

{{- define "rsync-ai.redis.host" -}}
{{- if .Values.redis.enabled -}}
{{- printf "%s-redis" (include "rsync-ai.fullname" .) -}}
{{- else -}}
{{- required "redis.enabled=false requires redis.external.host" .Values.redis.external.host -}}
{{- end -}}
{{- end -}}

{{- define "rsync-ai.redis.port" -}}
{{- if .Values.redis.enabled -}}6379{{- else -}}{{ .Values.redis.external.port | default 6379 }}{{- end -}}
{{- end -}}

{{- define "rsync-ai.redis.address" -}}
{{- printf "%s:%s" (include "rsync-ai.redis.host" .) (include "rsync-ai.redis.port" .) -}}
{{- end -}}

{{/*
Kafka bootstrap. A BYO CSV of several brokers is passed through verbatim —
collapsing it to one host is the failure mode where the platform looks healthy
until that single broker is the one that is down.
*/}}
{{- define "rsync-ai.kafka.bootstrap" -}}
{{- if .Values.kafka.enabled -}}
{{- printf "%s-kafka:9092" (include "rsync-ai.fullname" .) -}}
{{- else -}}
{{- required "kafka.enabled=false requires kafka.external.bootstrapServers" .Values.kafka.external.bootstrapServers -}}
{{- end -}}
{{- end -}}

{{/*
The replication factor every topic this chart creates is born with.

There is ONE definition because there used to be two, and on a BYO cluster with
kafka.replicationFactor unset they disagreed: connectors/cdc.yaml derived 3 for
Kafka Connect's three internal topics while jobs/kafka-init.yaml's shell derived
1 for the 14 static topics. Same chart, same cluster, same install. Neither can
see the cluster, so neither was more right than the other.

The 3 was the dangerous half, because Connect creates its internal topics itself
and nothing clamps them. On a BYO cluster with fewer than three brokers Connect
asks for three replicas and, after ~4 minutes of retries, dies with

    org.apache.kafka.common.errors.TimeoutException: Timeout expired while trying to create topic(s)

That message names neither the replication factor nor the broker count. The
broker logs nothing at all -- it answers InvalidReplicationFactor and Connect
reports only the elapsed retry budget. The requested value appears exactly once,
as `status.storage.replication.factor = 3` in a startup config dump hundreds of
lines earlier. And the post-install kafka-init hook cannot rescue it: `--wait`
holds hooks until the pods are ready, so a crash-looping Connect means the hook
that would have pre-created those topics never runs at all.

So a BYO cluster with nothing set fails at RENDER time instead. That is not a
downgrade in reach: it is the one branch where the chart was guessing a fact it
cannot observe, every shipped BYO values file (values-eks/gke/aks/byo-everything)
already sets the value explicitly, and the in-chart broker below is knowable.
*/}}
{{- define "rsync-ai.kafka.replicationFactor" -}}
{{- if .Values.kafka.replicationFactor -}}
{{- .Values.kafka.replicationFactor -}}
{{- else if .Values.kafka.enabled -}}
1
{{- else -}}
{{- fail "kafka.enabled=false requires an explicit kafka.replicationFactor.\n\nThe chart cannot see your cluster, so it cannot derive one. It used to assume 3,\nwhich is wrong for every cluster with fewer than three brokers -- and it does not\nfail at render time. Kafka Connect starts, asks for 3 replicas for its internal\ntopics, and crash-loops with\n\n    TimeoutException: Timeout expired while trying to create topic(s)\n\na message that names neither the replication factor nor the broker count.\n\nSet it to your broker count, capped at 3:\n\n    1 broker  -> kafka.replicationFactor: \"1\"\n    2 brokers -> kafka.replicationFactor: \"2\"\n    3 or more -> kafka.replicationFactor: \"3\"  (with kafka.minInsyncReplicas: \"2\")\n\nState your REAL broker count. Overstating it is not harmless: kafka-init clamps\ndown to the live broker count so the platform's own 14 topics degrade with a\nwarning rather than failing to create, but Kafka Connect's three internal topics\ncannot be clamped by anything -- Connect creates them itself, before any of this\nplatform's code runs -- so too large a value still crash-loops Connect exactly as\ndescribed above. That is precisely why the chart will not pick a value for you." -}}
{{- end -}}
{{- end -}}

{{- define "rsync-ai.temporal.address" -}}
{{- if .Values.temporal.enabled -}}
{{- printf "%s-temporal:7233" (include "rsync-ai.fullname" .) -}}
{{- else -}}
{{- required "temporal.enabled=false requires temporal.external.address" .Values.temporal.external.address -}}
{{- end -}}
{{- end -}}

{{- define "rsync-ai.minio.endpoint" -}}
{{- if eq .Values.objectStorage.mode "minio" -}}
{{- printf "http://%s-minio:9000" (include "rsync-ai.fullname" .) -}}
{{- else -}}
{{- required "objectStorage.mode != minio requires objectStorage.external.endpointUrl" .Values.objectStorage.external.endpointUrl -}}
{{- end -}}
{{- end -}}

{{/* ── Shared env blocks ───────────────────────────────────────────────────── */}}

{{/*
Every service. ENVIRONMENT is set explicitly and never left to default: the Go
default is "development" (connector-deployer/internal/config/config.go:61) and
that default unlocks a warn-and-allow branch on the deploy path. A hand-written
Deployment that omits it re-opens that branch silently, which is exactly why it
is spelled out here rather than inherited.
*/}}
{{- define "rsync-ai.commonEnv" -}}
- name: ENVIRONMENT
  value: "production"
- name: LOG_FORMAT
  value: "json"
- name: LOG_LEVEL
  value: {{ .Values.logLevel | default "info" | quote }}
{{- range $k, $v := .Values.extraEnv }}
- name: {{ $k }}
  value: {{ $v | quote }}
{{- end }}
{{- end -}}

{{/*
Kafka. Both KAFKA_BROKERS and KAFKA_BOOTSTRAP_SERVERS are set to the same value:
the Go client gives KAFKA_BROKERS precedence (shared/go/kafkaclient/config.go:190)
while some Python services read the other name, and setting both identically
removes the chance of a service silently reading an empty one.

replicationFactor / minInsyncReplicas are passed through EMPTY when unset, and
that is deliberate: these go to Go services that hold a LIVE broker list and
derive a better answer than this chart can (kafka/replication.go forCluster ->
topology.go clampToCluster -> pinMinInsyncReplicas). Defaulting them here would
override a derivation made against the real cluster with one made blind.

That is why this is not in tension with jobs/kafka-init.yaml, which DOES pin
min.insync.replicas to min(2, RF): kafka-init shells out to kafka-topics.sh with
no Go code in the path, so "unset" there means the BROKER default applies — 2 on
MSK and most managed clusters — and an RF=1 topic inheriting misr=2 is created
successfully and is then permanently unwritable. Empty here defers to a clamp;
empty there defers to nothing. Neither may ever emit misr > RF: such a topic
shows up as a pipeline that produces nothing rather than as an error.
*/}}
{{- define "rsync-ai.kafkaEnv" -}}
- name: KAFKA_BROKERS
  value: {{ include "rsync-ai.kafka.bootstrap" . | quote }}
- name: KAFKA_BOOTSTRAP_SERVERS
  value: {{ include "rsync-ai.kafka.bootstrap" . | quote }}
- name: KAFKA_TOPIC_PREFIX
  value: {{ .Values.kafka.topicPrefix | quote }}
- name: KAFKA_USE_AVRO
  value: "false"
- name: KAFKA_REPLICATION_FACTOR
  value: {{ .Values.kafka.replicationFactor | quote }}
- name: KAFKA_MIN_INSYNC_REPLICAS
  value: {{ .Values.kafka.minInsyncReplicas | quote }}
{{- include "rsync-ai.kafkaSecurityEnv" . }}
{{- end -}}

{{/*
The BYO-cluster security profile, on its own so that every Kafka client in the
chart can be given it from ONE definition. Each client that reads these names
independently is a client that can be forgotten: kafka-mcp-sink was, and its Go
worker -- which reads exactly these names and even fails closed on a half
config (kafka-mcp-sink/worker-src/cmd/kafka-sink-worker/kafka_security.go:55)
-- dialled a secured broker anonymously because the chart never set them.

Emitted only for a BYO cluster. The in-chart broker is PLAINTEXT on a
ClusterIP with no SASL configured at all, so setting a protocol there would
describe a listener that does not exist.
*/}}
{{/*
The JAAS login module for the configured SASL mechanism -- the Helm twin of
llm-service/src/utils/kafka_security.py:220-224, which is the authority. Kept
in lockstep with it: a mechanism this chart accepts and that file does not (or
the reverse) produces a Connect worker and a Python provisioner that disagree
about how to authenticate against the same cluster.

`fail` rather than a default, because every wrong answer here is silent at
render time and surfaces as a login-module error deep in a broker handshake.

This is also the chart's mechanism ALLOWLIST, and templates/validate.yaml
invokes it for exactly that -- discarding the output -- so an unsupported
mechanism is rejected even when the CDC plane is disabled and nothing else
would have called it. One definition, one message, no ordering dependence:
helm renders connectors/cdc.yaml before validate.yaml, so a duplicate guard
in validate.yaml would never be the one the operator sees.
*/}}
{{- define "rsync-ai.kafka.saslLoginModule" -}}
{{- $m := .Values.kafka.external.saslMechanism | upper -}}
{{- if eq $m "PLAIN" -}}
org.apache.kafka.common.security.plain.PlainLoginModule
{{- else if or (eq $m "SCRAM-SHA-256") (eq $m "SCRAM-SHA-512") -}}
org.apache.kafka.common.security.scram.ScramLoginModule
{{- else if eq $m "OAUTHBEARER" -}}
org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
{{- else -}}
{{- fail (printf "kafka.external.saslMechanism=%q is not supported by this chart.\n\nSupported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER.\n\nAWS_MSK_IAM is not, and the reason is a CHART gap, not a product one. The Go\ndata plane DOES implement it -- shared/go/kafkaclient/config.go:62 and :454,\nsigning tokens in tokenauth/msk.go, wired through saramaauth and kgoauth. What\nis missing here is the other two thirds: llm-service lists it as unimplemented\n(src/utils/kafka_security.py:123) and the Kafka Connect image ships no\naws-msk-iam-auth jar, so there is no JAAS login module for this helper to\nreturn. This chart also has no kafka.external.awsRegion knob, so even the Go\nhalf would fail its own region check.\n\nUse SCRAM-SHA-512 (MSK supports it, backed by Secrets Manager) or OAUTHBEARER."
  .Values.kafka.external.saslMechanism) -}}
{{- end -}}
{{- end -}}

{{/*
Whether the configured mechanism authenticates with a fetched token rather than
a username/password pair. Non-empty for true, empty for false, so it composes
with `if`/`and` like the TLS predicates above.

Worth a helper rather than an inline `eq`: the distinction changes what the env
block emits, what validate.yaml demands, and the shape of the JAAS line in two
shell preludes. Five inline comparisons is five places to forget one.
*/}}
{{- define "rsync-ai.kafka.isTokenMechanism" -}}
{{- if eq (.Values.kafka.external.saslMechanism | default "" | upper) "OAUTHBEARER" -}}true{{- end -}}
{{- end -}}

{{/*
The JVM's OAUTHBEARER login callback handler.

Omitting it is the quiet failure: OAuthBearerLoginModule then falls back to its
UNSECURED default, which mints a self-signed JWS. A broker with a real validator
rejects it, and the rejection describes the token -- not the missing handler.

The class was promoted out of `...oauthbearer.secured` in Kafka 3.6 and the old
spelling was removed in 4.0, so no single default is right across the range;
kafka.external.oauth.loginCallbackHandler overrides it. Both spellings exist in
3.7.0 (measured -- test/kind/jaas-probe/OAuthProbe.java).
*/}}
{{- define "rsync-ai.kafka.oauthLoginCallbackHandler" -}}
{{- $o := .Values.kafka.external.oauth | default dict -}}
{{- $o.loginCallbackHandler | default "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler" -}}
{{- end -}}

{{/*
TLS trust material: which files exist, where they are mounted, and under what
key they were stored. Split into single-fact helpers because every one of them
is consumed from at least three places -- the env block, the volume projection,
the Connect worker config and validate.yaml -- and a second inline copy of any
of them is a copy that can disagree.

Each returns a non-empty string for true and the empty string for false, which
is what Helm's `if` tests, so they compose with `and`/`or` directly.
*/}}
{{- define "rsync-ai.kafka.tlsDir" -}}/etc/rsync-ai/kafka-tls{{- end -}}

{{- define "rsync-ai.kafka.tlsSecretName" -}}
{{- $tls := .Values.kafka.external.tls | default dict -}}
{{- if $tls.existingSecret -}}
{{ $tls.existingSecret }}
{{- else -}}
{{ include "rsync-ai.fullname" . }}-kafka-tls
{{- end -}}
{{- end -}}

{{/*
Does the configured protocol actually negotiate TLS?

Keyed off the PROTOCOL, never off "is a caCert set". An operator who pastes a CA
in while leaving securityProtocol at SASL_PLAINTEXT gets an unencrypted
connection, and mounting the file would make it look otherwise.
*/}}
{{- define "rsync-ai.kafka.usesTLS" -}}
{{- if not .Values.kafka.enabled -}}
{{- $p := .Values.kafka.external.securityProtocol | default "PLAINTEXT" | upper -}}
{{- if or (eq $p "SSL") (eq $p "SASL_SSL") -}}true{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The key each file lives under in the Secret. With existingSecret the names are
the operator's, so they are read from values; with the chart-managed Secret they
are fixed, because the chart is the thing that wrote it. Either way the volume
projects them onto FIXED filenames, so nothing downstream depends on the naming.

An empty key means "this file is not present", and that is how server-only TLS
is distinguished from mTLS when the chart cannot look inside the Secret.
*/}}
{{- define "rsync-ai.kafka.tlsKey.ca" -}}
{{- $tls := .Values.kafka.external.tls | default dict -}}
{{- if $tls.existingSecret }}{{ $tls.caKey }}{{ else if $tls.caCert }}ca.crt{{ end -}}
{{- end -}}
{{- define "rsync-ai.kafka.tlsKey.cert" -}}
{{- $tls := .Values.kafka.external.tls | default dict -}}
{{- if $tls.existingSecret }}{{ $tls.clientCertKey }}{{ else if and $tls.clientCert $tls.clientKey }}tls.crt{{ end -}}
{{- end -}}
{{- define "rsync-ai.kafka.tlsKey.key" -}}
{{- $tls := .Values.kafka.external.tls | default dict -}}
{{- if $tls.existingSecret }}{{ $tls.clientKeyKey }}{{ else if and $tls.clientCert $tls.clientKey }}tls.key{{ end -}}
{{- end -}}
{{/*
The combined cert+key PEM. Java's PEM keystore is ONE file holding both, so
Kafka Connect cannot use tls.crt/tls.key the way the Go and Python clients do.
The chart concatenates them itself; with existingSecret the operator must supply
the combined file, and validate.yaml refuses the CDC plane without it rather
than rendering a Connect worker that cannot present a certificate.
*/}}
{{- define "rsync-ai.kafka.tlsKey.pem" -}}
{{- $tls := .Values.kafka.external.tls | default dict -}}
{{- if $tls.existingSecret }}{{ $tls.clientPemKey }}{{ else if and $tls.clientCert $tls.clientKey }}client.pem{{ end -}}
{{- end -}}

{{- define "rsync-ai.kafka.hasCA" -}}
{{- if include "rsync-ai.kafka.usesTLS" . }}{{ include "rsync-ai.kafka.tlsKey.ca" . }}{{ end -}}
{{- end -}}
{{- define "rsync-ai.kafka.hasClientCert" -}}
{{- if include "rsync-ai.kafka.usesTLS" . -}}
{{- if and (include "rsync-ai.kafka.tlsKey.cert" .) (include "rsync-ai.kafka.tlsKey.key" .) -}}true{{- end -}}
{{- end -}}
{{- end -}}
{{/* Anything to mount at all. */}}
{{- define "rsync-ai.kafka.hasTLSFiles" -}}
{{- if or (include "rsync-ai.kafka.hasCA" .) (include "rsync-ai.kafka.hasClientCert" .) -}}true{{- end -}}
{{- end -}}

{{/*
The volume and its mount, as a pair of one-line includes.

They are a pair on purpose: the census in
llm-service/tests/test_chart_kafka_security_env.py asserts that every container
given a Kafka security profile also carries the mount, because a container that
is handed KAFKA_SSL_CA_LOCATION and no file at that path fails at the handshake
with a "no such file" that names the path and not the omission -- and on the
Java side it fails only when a task starts, long after the pod is Ready.

Renders nothing at all when the cluster is PLAINTEXT, so an existing deployment
is byte-identical.
*/}}
{{- define "rsync-ai.kafkaTLSVolume" -}}
{{- if include "rsync-ai.kafka.hasTLSFiles" . }}
- name: kafka-tls
  secret:
    secretName: {{ include "rsync-ai.kafka.tlsSecretName" . }}
    {{/* fsGroup 1000 is set for every pod, so 0440 is readable by the
         container user and by nothing else. The client key is a private key. */}}
    defaultMode: 0440
    items:
      {{- with include "rsync-ai.kafka.tlsKey.ca" . }}
      - key: {{ . | quote }}
        path: ca.crt
      {{- end }}
      {{- if include "rsync-ai.kafka.hasClientCert" . }}
      - key: {{ include "rsync-ai.kafka.tlsKey.cert" . | quote }}
        path: tls.crt
      - key: {{ include "rsync-ai.kafka.tlsKey.key" . | quote }}
        path: tls.key
      {{- with include "rsync-ai.kafka.tlsKey.pem" . }}
      - key: {{ . | quote }}
        path: client.pem
      {{- end }}
      {{- end }}
{{- end }}
{{- end -}}

{{- define "rsync-ai.kafkaTLSVolumeMount" -}}
{{- if include "rsync-ai.kafka.hasTLSFiles" . }}
- name: kafka-tls
  mountPath: {{ include "rsync-ai.kafka.tlsDir" . | quote }}
  readOnly: true
{{- end }}
{{- end -}}

{{- define "rsync-ai.kafkaSecurityEnv" -}}
{{- if not .Values.kafka.enabled }}
- name: KAFKA_SECURITY_PROTOCOL
  value: {{ .Values.kafka.external.securityProtocol | default "PLAINTEXT" | quote }}
{{- if .Values.kafka.external.saslMechanism }}
- name: KAFKA_SASL_MECHANISM
  value: {{ .Values.kafka.external.saslMechanism | quote }}
{{- if include "rsync-ai.kafka.isTokenMechanism" . }}
{{/*
Read by shared/go/kafkaclient (config.go EnvOAuth*) and by
llm-service/src/utils/kafka_security.py (ENV_OAUTH_*). Both fetch the token
themselves; neither takes a username or a password, and emitting
KAFKA_SASL_USERNAME/PASSWORD here would put the client secret in a second
variable that nothing redacts.
*/}}
- name: KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT
  value: {{ (.Values.kafka.external.oauth | default dict).tokenEndpoint | quote }}
- name: KAFKA_SASL_OAUTHBEARER_CLIENT_ID
  value: {{ (.Values.kafka.external.oauth | default dict).clientId | quote }}
- name: KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ include "rsync-ai.secretName" . }}
      key: KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET
      optional: true
{{- with (.Values.kafka.external.oauth | default dict).scope }}
- name: KAFKA_SASL_OAUTHBEARER_SCOPE
  value: {{ . | quote }}
{{- end }}
{{- with (.Values.kafka.external.oauth | default dict).extensions }}
- name: KAFKA_SASL_OAUTHBEARER_EXTENSIONS
  value: {{ . | quote }}
{{- end }}
{{/*
Only the JVM consumes this one, but it is emitted alongside the rest because
Debezium's schema-history client is configured from the connector container's
environment -- see debezium_schema_history_security().
*/}}
- name: KAFKA_SASL_OAUTHBEARER_LOGIN_CALLBACK_HANDLER
  value: {{ include "rsync-ai.kafka.oauthLoginCallbackHandler" . | quote }}
{{- else }}
- name: KAFKA_SASL_USERNAME
  value: {{ .Values.kafka.external.saslUsername | quote }}
- name: KAFKA_SASL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "rsync-ai.secretName" . }}
      key: KAFKA_SASL_PASSWORD
      optional: true
{{- end }}
{{- end }}
{{- if include "rsync-ai.kafka.usesTLS" . }}
{{- $dir := include "rsync-ai.kafka.tlsDir" . }}
{{- if include "rsync-ai.kafka.hasCA" . }}
{{/*
Consumed as-is by shared/go/kafkaclient (EnvSSLCALocation, config.go:77) and by
llm-service/src/utils/kafka_security.py (ENV_SSL_CA_LOCATION:46). Both treat an
UNSET value as "use the system trust store", which is the correct answer for
managed Kafka -- so this is emitted only when a bundle was actually supplied.
*/}}
- name: KAFKA_SSL_CA_LOCATION
  value: {{ printf "%s/ca.crt" $dir | quote }}
{{- end }}
{{- if include "rsync-ai.kafka.hasClientCert" . }}
- name: KAFKA_SSL_CERT_LOCATION
  value: {{ printf "%s/tls.crt" $dir | quote }}
- name: KAFKA_SSL_KEY_LOCATION
  value: {{ printf "%s/tls.key" $dir | quote }}
{{- if include "rsync-ai.kafka.tlsKey.pem" . }}
{{/*
The same keypair in the shape a JVM can load. Inert in the Go and Python
services, which read the two paths above; kafka-init shells out to the Kafka
CLI and needs this one. Derived here rather than reassembled from
KAFKA_SSL_CERT_LOCATION by the script, so there is no second place that knows
the layout of the mount.
*/}}
- name: KAFKA_SSL_KEYSTORE_LOCATION
  value: {{ printf "%s/client.pem" $dir | quote }}
{{- end }}
{{- end }}
{{- if (.Values.kafka.external.tls | default dict).insecureSkipVerify }}
{{/*
KAFKA_SSL_SKIP_VERIFY, not KAFKA_SSL_INSECURE_SKIP_VERIFY. The platform shipped
two spellings for one setting: Python reads only this one
(kafka_security.py:49), Go reads both (config.go:80 and the alias at :115). So
this is the single spelling that reaches every service -- the other reaches the
Go half and silently leaves verification ON for the Python half, which is the
failure the alias was added to end. Documented as this name too
(docs/deployment/env-vars.md:92).
*/}}
- name: KAFKA_SSL_SKIP_VERIFY
  value: "true"
{{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{- define "rsync-ai.postgresEnv" -}}
- name: DB_HOST
  value: {{ include "rsync-ai.postgres.host" . | quote }}
- name: DB_PORT
  value: {{ include "rsync-ai.postgres.port" . | quote }}
- name: DB_USER
  value: {{ .Values.postgresql.username | quote }}
- name: DB_NAME
  value: {{ .Values.postgresql.database | quote }}
{{/*
  Load-bearing. internal/config/config.go:221 defaults DB_SSLMODE to "disable",
  and a pgx DSN that names no sslmode negotiates PLAINTEXT without erroring --
  so omitting this does not fail loudly against an external database, it ships
  the password in the clear while the api-gateway beside it (whose DATABASE_URL
  carries sslmode) connects over TLS and looks like proof the setting worked.
*/}}
- name: DB_SSLMODE
  value: {{ include "rsync-ai.postgres.sslmode" . | quote }}
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "rsync-ai.secretName" . }}
      key: POSTGRES_PASSWORD
{{- end -}}

{{- define "rsync-ai.redisEnv" -}}
- name: REDIS_HOST
  value: {{ include "rsync-ai.redis.host" . | quote }}
- name: REDIS_PORT
  value: {{ include "rsync-ai.redis.port" . | quote }}
- name: REDIS_ADDRESS
  value: {{ include "rsync-ai.redis.address" . | quote }}
- name: REDIS_DB
  value: "0"
- name: REDIS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "rsync-ai.secretName" . }}
      key: REDIS_PASSWORD
      optional: true
{{- end -}}

{{- define "rsync-ai.cryptoEnv" -}}
- name: JWT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ include "rsync-ai.secretName" . }}
      key: JWT_SECRET
- name: ENCRYPTION_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "rsync-ai.secretName" . }}
      key: ENCRYPTION_KEY
- name: INTERNAL_SERVICE_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ include "rsync-ai.secretName" . }}
      key: INTERNAL_SERVICE_SECRET
      optional: true
{{- end -}}

{{/*
Object storage for the connectors that stage data (minio-mcp, kafka-mcp-sink).

When mode != minio and no static keys are given, NO credential env is emitted at
all — that is the workload-identity path (IRSA / GKE Workload Identity / AKS),
where the SDK reads a projected token. Emitting empty strings instead would
override the token and produce an auth failure that reads like a wrong key.
*/}}
{{- define "rsync-ai.objectStorageEnv" -}}
- name: MINIO_ENDPOINT_URL
  value: {{ include "rsync-ai.minio.endpoint" . | quote }}
- name: MINIO_BUCKET
  value: {{ .Values.objectStorage.bucket | quote }}
{{/*
MINIO_PREFIX and the lifecycle rule the bucket Job installs MUST name the same
prefix. They are set from one value for that reason: an expiry rule pointed at a
prefix nothing writes to expires nothing and reaps no abandoned staging objects,
and it does so without any error — the bucket just grows.
*/}}
- name: MINIO_PREFIX
  value: {{ .Values.objectStorage.prefix | quote }}
{{- if eq .Values.objectStorage.mode "minio" }}
- name: MINIO_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "rsync-ai.secretName" . }}
      key: MINIO_ACCESS_KEY
- name: MINIO_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "rsync-ai.secretName" . }}
      key: MINIO_SECRET_KEY
{{- else }}
{{- if .Values.objectStorage.external.region }}
{{/*
BOTH names, from one value, and the second one is the one that actually works.

`objectStorage.external.region` was a knob that did nothing: the chart emitted
only AWS_REGION, and neither staging client ever reads it. Both pass a region to
boto3 EXPLICITLY -- blob_lane.go:62 `env("MINIO_REGION","us-east-1")` and the
minio connector's `pick("region","MINIO_REGION","us-east-1")` at connector.py:93
-> `region_name=` at :118 -- and botocore consults AWS_REGION only when
region_name is None. So the documented value was silently discarded and every
staging client stayed on us-east-1. Compose has had MINIO_REGION on these same
workloads all along (docker-compose.yml:678, :720, :920); this is the chart
catching up. (KI-CHART-REGION-KNOB-IS-INERT.)

AWS_REGION stays because it is NOT dead weight: kafkaclient/config.go:120-121
reads it as the MSK IAM signing-region fallback and retention/config.go:107 as
Archive.S3Region. The defect was a missing variable, not a wrong one.
*/}}
- name: AWS_REGION
  value: {{ .Values.objectStorage.external.region | quote }}
- name: MINIO_REGION
  value: {{ .Values.objectStorage.external.region | quote }}
{{- end }}
{{- if .Values.objectStorage.external.accessKeyId }}
- name: MINIO_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "rsync-ai.secretName" . }}
      key: OBJECT_STORAGE_ACCESS_KEY_ID
- name: MINIO_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "rsync-ai.secretName" . }}
      key: OBJECT_STORAGE_SECRET_ACCESS_KEY
{{- end }}
{{- end }}
{{- end -}}

{{/* ── Pod boilerplate ─────────────────────────────────────────────────────── */}}

{{- define "rsync-ai.podBoilerplate" -}}
serviceAccountName: {{ include "rsync-ai.serviceAccountName" . }}
{{- with .Values.global.image.pullSecrets }}
imagePullSecrets:
{{- toYaml . | nindent 2 }}
{{- end }}
securityContext:
{{- toYaml .Values.global.podSecurityContext | nindent 2 }}
{{- with .Values.global.nodeSelector }}
nodeSelector:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.global.tolerations }}
tolerations:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.global.affinity }}
affinity:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/*
Connector catalog initContainer + its volume.

This is what replaces the shared `mcp_connectors` Docker volume. Each pod copies
the baked catalog into its own emptyDir, so there is no ReadWriteMany PVC —
which matters because EBS, GCE PD and Azure Disk are all ReadWriteOnce, and
needing RWX would drag in EFS/Filestore/Azure Files as a hard dependency.

Safe because nothing in the Go services writes to this tree at runtime: the one
write (internal/mcp/server_manager.go:942) belongs to the stdio-venv fallback,
which is only reached when a connector has NO HTTP endpoint — and in this chart
every connector is an HTTP Service.
*/}}
{{- define "rsync-ai.catalogInitContainer" -}}
- name: connector-catalog
  image: {{ include "rsync-ai.image" (dict "root" . "image" .Values.connectors.catalog.image) }}
  imagePullPolicy: {{ .Values.global.image.pullPolicy }}
  command: ["sh", "-c", "cp -a /seed/. /target/ && echo 'connector catalog seeded'"]
  volumeMounts:
    - name: connector-catalog
      mountPath: /target
  securityContext:
{{- toYaml .Values.global.containerSecurityContext | nindent 4 }}
  resources:
    requests: { cpu: 50m, memory: 64Mi }
    limits: { memory: 256Mi }
{{- end -}}

{{/*
Ordering gate for the services that hold a database handle.

docker-compose.yml gives every one of these `postgres: condition: service_healthy`
(and kafka/redis alongside), and it says why at :1135 -- the cold-boot race where
a startup Dial loses and workflows come up disabled. Kubernetes has no depends_on,
and the chart shipped without an equivalent, so that race was wide open here.

It matters more on Kubernetes than it did on compose, because the api-gateway does
not crash when the database is missing at boot: it logs one warning, falls back to
"using mock data", never retries, and answers /health 200 forever. A pod that lost
the race is Running, Ready, and serving nothing real -- with no signal anywhere in
`kubectl get pods`. Waiting here is what makes the boot order deterministic instead
of leaving it to whichever pod the scheduler starts first.

Uses the calling service's OWN image so this adds no image to pull; every app image
in this chart carries sh and nc (verified in-cluster, not assumed). Args: root, image.
*/}}
{{- define "rsync-ai.waitForDepsInitContainer" -}}
{{- $root := .root -}}
- name: wait-for-deps
  image: {{ include "rsync-ai.image" (dict "root" $root "image" .image) }}
  imagePullPolicy: {{ $root.Values.global.image.pullPolicy }}
  command:
    - sh
    - -c
    - |
      set -e
      wait_for() {
        host="$1"; port="$2"; label="$3"; n=0
        until nc -z "$host" "$port" 2>/dev/null; do
          n=$((n + 1))
          if [ "$n" -ge 60 ]; then
            echo "ERROR: ${label} (${host}:${port}) not reachable after ${n} attempts" >&2
            exit 1
          fi
          echo "waiting for ${label} at ${host}:${port} (${n}/60)..."
          sleep 2
        done
        echo "${label} reachable at ${host}:${port}"
      }
      wait_for {{ include "rsync-ai.postgres.host" $root }} {{ include "rsync-ai.postgres.port" $root }} postgres
      wait_for {{ include "rsync-ai.redis.host" $root }} {{ include "rsync-ai.redis.port" $root }} redis
  securityContext:
{{- toYaml $root.Values.global.containerSecurityContext | nindent 4 }}
  resources:
    requests: { cpu: 25m, memory: 32Mi }
    limits: { memory: 128Mi }
{{- end -}}

{{- define "rsync-ai.catalogVolume" -}}
- name: connector-catalog
  emptyDir: {}
{{- end -}}

{{/*
The number of pods a tier is GUARANTEED to be running -- its floor, not its
configured size.

Under an HPA the Deployment stops emitting `replicas:` entirely (api-gateway.yaml
wraps it in `if not .Values.apiGateway.autoscaling.enabled`), so replicaCount
describes nothing at all and the autoscaler's minReplicas is the real floor.
Anything reasoning about disruption has to read this, not replicaCount, or it
reads a number the cluster is not using.

Both failure directions were live before this existed (KI-CHART-PDB-GATE-IGNORES-HPA):
replicaCount:1 with minReplicas:2 produced NO PodDisruptionBudget for a tier
genuinely running two pods, and replicaCount:2 with minReplicas:1 produced a PDB
whose minAvailable equalled the floor, so disruptionsAllowed was 0 and every
eviction blocked -- the drain deadlock pdb.yaml's own comment exists to prevent.

Takes a tier's values map, e.g.:
  include "rsync-ai.guaranteedReplicas" .Values.apiGateway
A tier with no autoscaling key (frontend) falls through to replicaCount, which
for it is always the real count.
*/}}
{{- define "rsync-ai.guaranteedReplicas" -}}
{{- if and .autoscaling .autoscaling.enabled -}}
{{- .autoscaling.minReplicas -}}
{{- else -}}
{{- .replicaCount -}}
{{- end -}}
{{- end -}}
