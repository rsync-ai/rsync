# Environment Variables & Secrets Reference

All required environment variables for running rsync-ai in production or demo mode.
Generate secrets with: `openssl rand -base64 32`

---

## Critical secrets (must back up)

| Variable | Required | Description |
|---|---|---|
| `ENCRYPTION_KEY` | **Yes** | AES key for encrypting stored connector credentials. **Losing this breaks all saved connections permanently.** |
| `JWT_SECRET` | **Yes** | Signs JWT authentication tokens |
| `INTERNAL_SERVICE_SECRET` | **Yes** | Shared secret authenticating service-to-service calls (api-gateway → orchestrator: connection tests, OAuth refresh). Must be non-empty and identical across api-gateway, orchestrator, and frontend — if empty, the orchestrator (`ENVIRONMENT=production`) fails `requirePrincipal` closed and internal calls return 401. Generate with `openssl rand -hex 32`. |

Store all three in AWS Secrets Manager or equivalent. Rotation of `ENCRYPTION_KEY`/`JWT_SECRET` requires re-encrypting all stored connector credentials; rotating `INTERNAL_SERVICE_SECRET` requires recreating api-gateway, orchestrator, and frontend together (a mismatch 401s in-flight internal calls).

---

## Database

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_HOST` | `postgres` | PostgreSQL hostname |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `rsync` | Database name |
| `POSTGRES_USER` | `rsync` | Database user |
| `POSTGRES_PASSWORD` | — | Database password |

---

## Redis

| Variable | Default | Description |
|---|---|---|
| `REDIS_HOST` | `redis` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | — | Redis password (empty = no auth) |

---

## Kafka

### Connection and naming

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list. `KAFKA_BOOTSTRAP_SERVERS` is accepted as an alias when this is unset (`shared/go/kafkaclient/config.go:204-207`). The compose files all set this to `kafka:29092`; the bare code default only applies if you run a service outside compose |
| `KAFKA_TOPIC_PREFIX` | `rsync.` | Namespace prepended to every topic **and every consumer group id** this product creates, so you can tell them apart from your other applications' on a shared cluster (`shared/go/kafkaclient/topics.go:27`, `llm-service/src/utils/kafka_topics.py:26`). Set to `` (empty) to disable and keep unprefixed names. A prefix not ending in `.`, `_` or `-` gets a `.` appended, and illegal characters are stripped |
| `KAFKA_GROUP_ID` | `go-orchestrator-group` | **Prefix**, not a literal group id, and it is itself namespaced. The orchestrator qualifies the configured value (`backend-orchestrator/internal/config/config.go:172` → `config/kafka_identity.go:56`) and then appends the topic, so the groups that actually join look like `rsync.go-orchestrator-group-rsync.cdc.abc12345` (`internal/kafka/manager.go:931`) |
| `KAFKA_CLIENT_ID` | `rsync-<service>` | `client.id` announced on every connection — `rsync-orchestrator`, `rsync-api-gateway`, etc. (`shared/go/kafkaclient/clientid.go:25`). Set it to pin one identity for the whole platform. Not cosmetic on MSK or Confluent Cloud: broker quotas and throttle metrics key off `client.id` |
| `CONSUMER_GROUP_PREFIX` | `rsync-pipeline` | Group-name prefix for the consumer-scaling agent only (`backend-orchestrator/internal/agents/consumer/config.go:142`, read at `:206`). Also namespaced, so the joined group is `rsync.rsync-pipeline-<topic>` (`consumer/kafka_identity.go:62`) |

> **`KAFKA_TOPIC_PREFIX` on an existing deployment.** Changing it renames every topic **and every
> consumer group**. Live topics and committed consumer-group offsets stay under the old names, so
> it takes ONE deploy of every Kafka-touching container at once — api-gateway, orchestrator,
> temporal-adapter, llm-service, debezium, kafka-mcp-sink — not a per-service rollout. A partial
> rollout does not error: the services still on the old value read topics nobody writes, and
> simply stop moving rows. Renamed groups have no committed offsets, so each consumer restarts
> from its `auto.offset.reset`. If you are upgrading and want the old names, set
> `KAFKA_TOPIC_PREFIX=` (empty) in `.env` first.

> **`KAFKA_GROUP_ID` when you write Kafka ACLs.** Granting a `Group` ACL on the *literal* value
> matches nothing — no consumer joins under the bare name. You no longer need a separate grant for
> it either: because group ids are namespaced under `KAFKA_TOPIC_PREFIX`, one `PREFIXED` grant on
> that prefix covers topics and groups together. A missing group ACL does not surface as a startup
> error; the consumer simply never receives records, which reads as a stalled pipeline rather than
> an authorization failure. Full ACL list: [kafka-acls.md](kafka-acls.md).

### Durability

| Variable | Default | Description |
|---|---|---|
| `KAFKA_REPLICATION_FACTOR` | unset ⇒ `1` on a single-broker cluster, else `min(3, brokers)` | Replication factor requested for topics this platform creates (`backend-orchestrator/internal/kafka/replication.go:31`, derivation at `:131-139`). A **request, not a guarantee**: it is clamped down to the live broker count, so a typo degrades instead of failing every creation. A non-positive or non-numeric value is ignored with a warning and the built-in default applies (`:93-105`). Also feeds the Kafka Connect worker's three internal topics (`docker-compose.yml:359-361`), which the clamp does **not** cover |
| `KAFKA_MIN_INSYNC_REPLICAS` | unset ⇒ `min(2, RF)` | Durability floor written explicitly onto every topic this platform creates (`replication.go:40`, `:157-175`). It is pinned even when you set nothing, because unset means the *broker's* default applies — 2 on MSK and most managed clusters — and an RF=1 topic inheriting `misr=2` is born permanently unwritable. Whatever you set is clamped to the topic's final RF, in that order (`:198-204`) |

### Authentication and TLS

Read by every Go service through `shared/go/kafkaclient` (`config.go:70-83`) and by the Python
services through their own reader. Everything below is unset by default, which means
`PLAINTEXT` with no credentials (`config.go:239-241`).

| Variable | Default | Description |
|---|---|---|
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | `PLAIN` when a SASL protocol is set, otherwise none | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `AWS_MSK_IAM`, `OAUTHBEARER` (`config.go:57-63`) |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | — | SASL credentials. Also serve as the OAuth client id/secret when the dedicated names are unset (`config.go:225-226`) |
| `KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT` | — | Plus `…_CLIENT_ID`, `…_CLIENT_SECRET`, `…_SCOPE`, `…_EXTENSIONS` for `OAUTHBEARER` (`config.go:92-98`) |
| `KAFKA_AWS_REGION` | falls back to `AWS_REGION`, then `AWS_DEFAULT_REGION` | Region for `AWS_MSK_IAM` signing (`config.go:120-121`). An EKS pod that already exports `AWS_REGION` needs no Kafka-specific duplicate |
| `KAFKA_SSL_CA_LOCATION` | — | CA bundle path. Alias: `KAFKA_TLS_CA` (`config.go:116`) |
| `KAFKA_SSL_CERT_LOCATION` / `KAFKA_SSL_KEY_LOCATION` | — | Client cert / key for mTLS. Aliases: `KAFKA_TLS_CERT`, `KAFKA_TLS_KEY` |
| `KAFKA_SSL_KEYSTORE_LOCATION` | — | The same client keypair in the one shape a JVM can load: a single PEM holding chain **and** key. Not derivable from the two paths above — build it with `cat client.crt client.key > client.pem`. Read by `kafka-init` and by Debezium's schema-history client; inert in the Go and Python services |
| `KAFKA_SSL_INSECURE_SKIP_VERIFY` | `false` | Disables server-certificate verification. `KAFKA_SSL_SKIP_VERIFY` is honored as an alias (`config.go:115`) — the two spellings previously reached different halves of the platform, so setting either now reaches both. Do not use outside local testing |

> **`AWS_MSK_IAM` is not implemented everywhere.** The Go services sign the token
> (`kafkaclient/tokenauth/msk.go`); the Python tier raises `KafkaSecurityError` for it
> (`llm-service/src/utils/kafka_security.py:123,422-427`) and the JVM Kafka Connect image
> ships no IAM login module. An IAM-only cluster therefore runs the Go data plane and leaves
> the CDC profile unable to authenticate — use `SCRAM-SHA-512` for a mixed stack. IAM also
> requires `KAFKA_SECURITY_PROTOCOL=SASL_SSL`; anything else is rejected at startup rather
> than warned about, because the token is a bearer credential (`config.go:461-464`).
>
> The paths above are **container-side**. Nothing mounts the files for you — the bind-mount
> overlay is in [Self-Hosting § Bring Your Own Kafka](self-hosting.md#bring-your-own-kafka).

**Kafka Connect (Debezium) takes its own copies.** The worker reads security settings in three
independent scopes — worker, `CONNECT_PRODUCER_*`, `CONNECT_CONSUMER_*` — and all three are wired
from the variables above in `docker-compose.yml:403-420`. Two are deliberately left as bare
pass-through keys with **no** default, so an unset variable is omitted from the container
environment rather than rendered as an empty string:

| Variable | Default | Description |
|---|---|---|
| `CONNECT_SSL_TRUSTSTORE_LOCATION` (+ `CONNECT_PRODUCER_…`, `CONNECT_CONSUMER_…`) | omitted | Truststore path for the Connect worker (`docker-compose.yml:446-448`). An empty value is read by Kafka as a real path and the worker exits 2, so it must be absent rather than empty. Confluent Cloud and MSK need no truststore at all |
| `CONNECT_SSL_TRUSTSTORE_PASSWORD` (+ the two scoped copies) | omitted | Same reasoning (`docker-compose.yml:462-464`); a PEM truststore rejects even an empty password |
| `KAFKA_SSL_TRUSTSTORE_TYPE` | `JKS` | Set `PEM` to point the three location variables at a `.pem` bundle (`docker-compose.yml:468-470`) |
| `KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM` | `https` | Set to the empty string to disable hostname verification — note the bare `-` in `${VAR-https}` at `docker-compose.yml:475-477`, which makes an explicitly-empty value stick |

### On Kubernetes: `kafka.external.*` (Helm)

The Helm chart does not take the variables above directly. You set `kafka.enabled: false`
and describe the cluster once under `kafka.external`; the chart fans that one description
out to the Go, Python **and** JVM clients, which do not configure any of this the same way.
Full annotations are in
[values.yaml](../../deploy/helm/rsync-ai/values.yaml) — this table is the map back to the
variables documented above.

| Helm value | Becomes | Notes |
|---|---|---|
| `kafka.external.bootstrapServers` | `KAFKA_BROKERS` | Comma-separated CSV is preserved end to end |
| `kafka.external.securityProtocol` | `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` \| `SSL` \| `SASL_PLAINTEXT` \| `SASL_SSL`. Default `PLAINTEXT` |
| `kafka.external.saslMechanism` | `KAFKA_SASL_MECHANISM` | `PLAIN` \| `SCRAM-SHA-256` \| `SCRAM-SHA-512` \| `OAUTHBEARER`. Anything else is rejected at render, not at runtime |
| `kafka.external.saslUsername` | `KAFKA_SASL_USERNAME` | **Not used by `OAUTHBEARER`** — setting it there is a render error rather than a silent no-op, because the value that means "username" on three mechanisms means nothing on the fourth |
| `kafka.external.saslPassword` | `KAFKA_SASL_PASSWORD` | Or key `KAFKA_SASL_PASSWORD` of `secrets.existingSecret` |
| `kafka.external.oauth.tokenEndpoint` | `KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT` | Required for `OAUTHBEARER` |
| `kafka.external.oauth.clientId` | `…_CLIENT_ID` | Required for `OAUTHBEARER`. A plain value, not a secret |
| `kafka.external.oauth.clientSecret` | `…_CLIENT_SECRET` | Or key `KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET` of `secrets.existingSecret` |
| `kafka.external.oauth.scope` | `…_SCOPE` | Optional. Sent as the `scope` form parameter *and* as a JAAS option |
| `kafka.external.oauth.extensions` | `…_EXTENSIONS` | Optional, `name=value,name2=value2`. Confluent Cloud uses it for `logicalCluster`/`identityPoolId`. `auth` is reserved by the mechanism and is rejected |
| `kafka.external.oauth.loginCallbackHandler` | `KAFKA_SASL_OAUTHBEARER_LOGIN_CALLBACK_HANDLER` | Consumed only by the JVM leg, as `sasl.login.callback.handler.class`. Leave empty for the default. Override **only** to match your broker's Kafka version — the class was promoted out of the `…oauthbearer.secured` package in 3.6 and the old spelling was removed in 4.0, so neither name spans the whole supported range |
| `kafka.external.tls.caCert` | `KAFKA_SSL_CA_LOCATION` + the JVM truststore | Inline PEM. Rendered into its own Secret and mounted read-only |
| `kafka.external.tls.clientCert` / `.clientKey` | `KAFKA_SSL_CERT_LOCATION` / `…_KEY_LOCATION` (+ `KAFKA_SSL_KEYSTORE_LOCATION`, the same pair as one PEM file, which is the only shape a JVM can load) | mTLS. **Both or neither** — half a pair fails closed rather than downgrading to server-only TLS |
| `kafka.external.tls.existingSecret` (+ `caKey`, `clientCertKey`, `clientKeyKey`, `clientPemKey`) | the same mounts | Bring your own Secret; the key *names* are yours and the chart projects them onto fixed filenames |
| `kafka.external.tls.insecureSkipVerify` | `KAFKA_SSL_SKIP_VERIFY` | **The chart emits the alias, not the primary name** — Python reads only `KAFKA_SSL_SKIP_VERIFY`, Go reads both, so this is the one spelling that reaches every service. See the warning below |

> **`insecureSkipVerify` is a hostname override, not a verification override — for two of
> the three runtimes.** Go honours it literally (`InsecureSkipVerify=true`, chain not
> validated). kafka-python maps it to `ssl_check_hostname=False` only, and its context is
> `ssl.PROTOCOL_TLS_CLIENT`, whose `verify_mode` is `CERT_REQUIRED` and is never
> reassigned — the chain is still validated. The JVM gets
> `ssl.endpoint.identification.algorithm=""` — hostname check off, chain still validated
> against the JDK `cacerts`. Against a private CA with no `caCert` you therefore get a
> platform where the Go services connect and every Python and JVM client fails its
> handshake, each with its own certificate error and none of them naming this setting.
> The chart refuses that combination at render time. Supply `caCert` instead.

> **TLS material is read only under `SASL_SSL` or `SSL`.** On a `PLAINTEXT` or
> `SASL_PLAINTEXT` listener there is no certificate to verify, so the whole `tls` block is
> ignored — the chart fails the render rather than let it look configured. The same guard
> runs the other way: SASL credentials set under `PLAINTEXT` would be discarded and every
> service would connect anonymously, which an unauthorized broker accepts without a word.

> **Leaving the `tls` block empty is the right answer for managed Kafka** whose certificate
> chains to a public root (MSK, Confluent Cloud, Aiven) — the images already trust those.
> It is the wrong answer for a private or self-signed CA.

Authorization is separate and is not covered by any of the above: see
[kafka-acls.md](kafka-acls.md). Note in particular that a grant of Read/Write with no
`Create` cannot run a pipeline, because each pipeline's data topic is named at runtime.

### Topology API and rollback levers

| Variable | Default | Description |
|---|---|---|
| `KAFKA_OWNED_TOPIC_PREFIXES` | — | Comma-separated extra prefixes the topology API will treat as this platform's own. **Adds** to the built-ins, never replaces them (`backend-orchestrator/internal/handlers/topology.go:91`) |
| `KAFKA_ALLOW_LEGACY_UNPREFIXED_TOPICS` | `false` | Re-admits the pre-namespace bare topic names (`agent.`, `pipeline.`, `cdc.`, `cdc-`, `schemahistory.`, `pii.`, `task.`) to the topology API's allowlist (`handlers/topology.go:95`). Accepts `1`/`true`/`yes`/`on` and logs once on first use. Off by default because those names are generic enough to collide with a customer's own topics — and the API can delete what it matches. Only turn it on during a migration window |
| `CDC_STREAMING_SINK_GROUP_PER_EXECUTION` | `false` | Rollback lever restoring the pre-fix, per-execution sink group id (`rsync.sink-<pid8>-<eid8>`) for `streaming_only`/`never` pipelines (`backend-orchestrator/internal/agents/executor/sink_consumer_group.go:17`, `:71`). A changing group name means no committed offsets, so leave it off unless you deliberately want that reset |

> **Running rsync against your own Kafka cluster?** If it is authorized, see
> [kafka-acls.md](kafka-acls.md) for the full list of ACLs the platform needs, and
> [kafka-topics.md](../architecture/kafka-topics.md) for the topic inventory.

---

## Temporal

| Variable | Default | Description |
|---|---|---|
| `TEMPORAL_HOST` | `temporal:7233` | Temporal frontend address |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |

---

## LLM / AI

| Variable | Default | Description |
|---|---|---|
| `LLM_PROVIDER` | auto-detect | `openai` · `azure` · `groq` · `ollama`. Unset ⇒ Azure endpoint → `azure`, else `OPENAI_API_KEY` → `openai`, else `ollama`. Groq is opt-in only. |
| `OPENAI_API_KEY` | — | If set, auto-detect selects OpenAI. Unset ⇒ Ollama. |
| `LLM_MODEL` | — | Overrides the model for **every** provider. On Azure this is the *deployment* name. |
| `OLLAMA_BASE_URL` | `http://host.docker.internal:11434` | Ollama server URL (wins over `OLLAMA_URL`; `/v1` appended automatically) |
| `OLLAMA_MODEL` | `qwen2.5:7b` | Default Ollama model for general agents |
| `EXPLORER_OFFLINE_ONLY` | **`false`** | Forces **only** the Explorer to Ollama. Set `LLM_PROVIDER=ollama` to take the whole stack offline. |
| `USE_MOCK_LLM` | `false` | Replace LLM calls with deterministic mocks (testing only) |

### Data Explorer overrides

The Explorer runs on its own model pool and can be pinned to a different provider from the rest of
the stack — that is how you keep schema metadata local while general agents still use a cloud model.
All of these are optional; unset means "inherit".

| Variable | Default | Description |
|---|---|---|
| `EXPLORER_LLM_PROVIDER` | inherits `LLM_PROVIDER` | Provider for Explorer prompts. Overridden by `EXPLORER_OFFLINE_ONLY=true`. |
| `EXPLORER_TABLE_LINK_MODEL` | `llama3:latest` offline, else `LLM_MODEL` | Picking tables for a question |
| `EXPLORER_COLUMN_LINK_MODEL` | ″ | Picking columns |
| `EXPLORER_QUERY_SPEC_MODEL` | ″ | Query-spec assembly |
| `EXPLORER_NEXT_STEPS_MODEL` | ″ | Follow-up suggestions |
| `EXPLORER_SQL_MODEL` | `OLLAMA_MODEL` or `sqlcoder:latest` offline, else `LLM_MODEL` | NL→SQL |
| `EXPLORER_SQL_MODEL_MYSQL` | same as `EXPLORER_SQL_MODEL` | NL→SQL on MySQL/MariaDB |
| `EXPLORER_SQL_PROVIDER` | inherits the Explorer provider | Provider for NL→SQL only |
| `EXPLORER_SQL_ALLOW_ONLINE` | `not EXPLORER_OFFLINE_ONLY` | Permits NL→SQL to reach a cloud provider while the rest of the Explorer is offline |
| `EXPLORER_SQL_OPENAI_MODEL` | prompt-registry model | NL→SQL model, applied only when the SQL provider resolves to `openai` |
| `EXPLORER_SQL_FALLBACK_MODELS` | `qwen2.5:7b,llama3:latest,codellama:7b-instruct` | Retry chain when SQL generation fails |
| `RANK_TABLES_LLM_PROVIDER` | inherits `EXPLORER_LLM_PROVIDER`, then `LLM_PROVIDER` | Provider for `/agents/rank-tables` (table recommendations during pipeline setup) only |
| `RANK_TABLES_MODEL` | `llama3:latest` offline, `gpt-4o-mini` on OpenAI | Model for `/agents/rank-tables`. Deliberately does **not** follow `LLM_MODEL` — it is a bulk metadata task pinned to a cheap model. |

On Azure the model argument **is** the deployment name, so an Explorer model left unset resolves to
`LLM_MODEL` → `AZURE_OPENAI_DEPLOYMENT` → the literal `gpt-4o-mini`; if none of your deployments is
called that, the last step is a 404.

> **These reach the container on all three compose files** — `docker-compose.yml` via `env_file:`,
> quickstart and prod via explicit passthrough. Before 2026-08-03 the quickstart forwarded none of
> them, so `EXPLORER_OFFLINE_ONLY=true` there was silently inert
> (`KI-EXPLORER-OFFLINE-FLAG-NOT-DELIVERED`).
> **Confirm rather than assume** — llm-service logs the resolved provider and every model at startup,
> once per entry point:
> `docker logs rsync-llm-service 2>&1 | grep -E "explorer (llm|router llm):|rank-tables llm:"`.
> **Three** lines, and all three must agree; `rank-tables` was added on 2026-08-03 after it was found
> resolving to OpenAI on deployments where the other two said `ollama`. Details in
> [ollama.md](ollama.md#verify-it--dont-assume-it).

---

## OAuth providers

| Variable | Description |
|---|---|
| `GITHUB_CLIENT_ID` | GitHub OAuth app client ID |
| `GITHUB_CLIENT_SECRET` | GitHub OAuth app client secret |
| `GOOGLE_CLIENT_ID` | Google OAuth client ID |
| `GOOGLE_CLIENT_SECRET` | Google OAuth client secret |

**Callback URLs to configure in each provider**:
- GitHub: `https://yourdomain.com/oauth/callback/github`
- Google: `https://yourdomain.com/oauth/callback/google`

---

## Application

| Variable | Default | Description |
|---|---|---|
| `DOMAIN` | `localhost` | Public domain name (no protocol prefix) |
| `NEXT_PUBLIC_API_URL` | `http://localhost:5001` | Frontend → API base URL |
| `RSYNC_ADMIN_EMAILS` | — | Comma-separated admin email addresses |
| `API_GATEWAY_PORT` | `5001` | API gateway listen port |
| `LOG_LEVEL` | `info` | Log level: debug / info / warn / error |

### Container log rotation

Every service in `docker-compose.yml` and `docker-compose.quickstart.yml` writes
to the `json-file` driver with an explicit cap. Worst-case disk per container is
`max-size` x `max-file`, so the 27-service base stack is bounded at roughly
`27 x 10m x 3` = **810 MB** at the defaults. A service with no cap inherits the
daemon default, which has none — that is how one collector container was found
holding 106 MB on its own.

| Variable | Default | Description |
|---|---|---|
| `RSYNC_LOG_MAX_SIZE` | `10m` | Max size of one container log file before it rotates |
| `RSYNC_LOG_MAX_FILE` | `3` | How many rotated files to keep per container |

Lower both on a small box (`RSYNC_LOG_MAX_SIZE=5m`, `RSYNC_LOG_MAX_FILE=2` caps
the same stack near 270 MB). Raise `max-file` rather than `max-size` if you want
more history — `docker logs` reads across rotated files, but a single oversized
file is what makes it slow.

---

## Traefik (self-hosted HTTPS without ALB)

Only needed when using built-in Traefik instead of AWS ALB:

| Variable | Description |
|---|---|
| `ACME_EMAIL` | Email for Let's Encrypt certificate notifications |

---

## S3 / MinIO

Used by the S3 MCP connector:

| Variable | Default | Description |
|---|---|---|
| `S3_ENDPOINT` | — | Custom endpoint (leave empty for real AWS S3, set to MinIO URL for local) |
| `S3_ACCESS_KEY` | — | Access key ID |
| `S3_SECRET_KEY` | — | Secret access key |
| `S3_REGION` | `us-east-1` | AWS region |

---

## Minimal .env for demo

```bash
# ── Required ──────────────────────────────────────────────────────────────────
DOMAIN=localhost
ENCRYPTION_KEY=<openssl rand -base64 32>
JWT_SECRET=<openssl rand -base64 32>
POSTGRES_PASSWORD=<openssl rand -base64 24>
REDIS_PASSWORD=<openssl rand -base64 24>

# ── LLM: pick one ─────────────────────────────────────────────────────────────
# Option A: OpenAI (easiest for demo)
OPENAI_API_KEY=sk-...

# Option B: Ollama on same VM
# OLLAMA_BASE_URL=http://host-gateway:11434
# EXPLORER_OFFLINE_ONLY=true
#   verify: docker logs rsync-llm-service 2>&1 \
#             | grep -E "explorer (llm|router llm):|rank-tables llm:"
#           → three lines, all provider=ollama

# ── Admin ─────────────────────────────────────────────────────────────────────
RSYNC_ADMIN_EMAILS=your@email.com

# ── S3-compatible (MinIO) ─────────────────────────────────────────────────────
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin123
```

---

## Minimal .env for production (AWS)

```bash
# ── Domain ────────────────────────────────────────────────────────────────────
DOMAIN=yourdomain.com
NEXT_PUBLIC_API_URL=https://yourdomain.com

# ── Security (BACK THESE UP) ──────────────────────────────────────────────────
ENCRYPTION_KEY=<generated>
JWT_SECRET=<generated>

# ── Database ──────────────────────────────────────────────────────────────────
POSTGRES_HOST=<rds-endpoint>
POSTGRES_PASSWORD=<generated>
POSTGRES_USER=rsync
POSTGRES_DB=rsync

# ── Cache ─────────────────────────────────────────────────────────────────────
REDIS_HOST=<elasticache-endpoint>
REDIS_PASSWORD=<generated>

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BROKERS=<msk-broker-1>:9092,<msk-broker-2>:9092

# ── Temporal ──────────────────────────────────────────────────────────────────
TEMPORAL_HOST=<temporal-cloud-or-internal>:7233

# ── LLM ───────────────────────────────────────────────────────────────────────
OPENAI_API_KEY=sk-...

# ── OAuth ─────────────────────────────────────────────────────────────────────
GITHUB_CLIENT_ID=<from GitHub>
GITHUB_CLIENT_SECRET=<from GitHub>
GOOGLE_CLIENT_ID=<from Google>
GOOGLE_CLIENT_SECRET=<from Google>

# ── Admin ─────────────────────────────────────────────────────────────────────
RSYNC_ADMIN_EMAILS=your@email.com
```
