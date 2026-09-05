## Schema Registry — LLD

### Compose Definition
Source: `docker-compose.yml` service `schema-registry`
- listens on `0.0.0.0:8080` in the container (`QUARKUS_HTTP_PORT: 8080`; the image bakes
  `-Dquarkus.http.host=0.0.0.0` into `JAVA_OPTIONS`)
- host mapped to `${RSYNC_HP_SCHEMA_REGISTRY:-8085}` to avoid conflicts
- **not** `8081` — that was the Confluent-era port, corrected everywhere else in 2026 and
  missed here because that pass grepped code, not `docs/`. In this stack `8081` is the
  orchestrator's host port ([docker-compose.yml:961](../../../../docker-compose.yml)), so the
  stale number pointed at a live and entirely unrelated service.

### Health
- `curl -f http://localhost:8080/health/ready` (Quarkus readiness probe; the compose
  `healthcheck` runs exactly this)

### Storage — EPHEMERAL, read before relying on it
The service sets no storage keys, so the `apicurio/apicurio-registry:3.0.6` defaults apply:
`apicurio.storage.kind=sql` + `apicurio.storage.sql.kind=h2` over
`apicurio.datasource.url=jdbc:h2:mem:db_${quarkus.uuid}` — **in-memory H2, keyed to a fresh
UUID on every boot** — and the service declares no `volumes:`. Every registered subject is
lost on restart or recreate.

There is no Kafka storage topic here. The service is **not** a Kafka client as configured;
it does not read `KAFKA_BOOTSTRAP_SERVERS` (see
`KI-COMPOSE-SCHEMA-REGISTRY-BOOTSTRAP-ENV-IS-INERT`).

### If you ever switch it to kafkasql — the `x-kafka-security` anchor is the WRONG remedy
`APICURIO_STORAGE_KIND=kafkasql` + `APICURIO_KAFKASQL_BOOTSTRAP_SERVERS` does make this a real
Kafka client. It does **not** make the `x-kafka-security` anchor apply. An earlier revision of
this file said it did; that was false, and it was false by the same measurement that retired the
inert key above.

All **17** anchor keys, in both the `KAFKA_*` env spelling and SmallRye's dotted equivalent
(**34** spellings), occur **0** times in the runner jar (2,419 entries) and **0** times across
all **356** shipped lib jars (57,339 entries) — against three controls measured in the same scan
that are positive on **both** legs, so neither zero can be a dead reader: `security.protocol`
3 runner / 6 lib entries, `sasl.mechanism` 7 / 3, `bootstrap.servers` 8 / 5. (A fourth,
`apicurio.kafkasql.security.sasl`, is 7 in the runner and **0** in the libs — that zero is
expected, since Apicurio's own classes ship in the runner jar, and it is reported here as an
observation rather than counted as a control.) Merging the anchor into a kafkasql registry would
leave it speaking PLAINTEXT to a SASL listener while every static check read as green.

The names the image actually reads are declared by
`io/apicurio/registry/storage/impl/kafkasql/KafkaSqlFactory.class` and defaulted in the image's
`application.properties`:

| Env variable | Property | Image default |
|---|---|---|
| `APICURIO_KAFKASQL_SECURITY_PROTOCOL` | `apicurio.kafkasql.security.protocol` | *(none in `application.properties`)* |
| `APICURIO_KAFKASQL_SECURITY_SASL_ENABLED` | `apicurio.kafkasql.security.sasl.enabled` | `false` (`:22`) |
| `APICURIO_KAFKASQL_SECURITY_SASL_MECHANISM` | `apicurio.kafkasql.security.sasl.mechanism` | `OAUTHBEARER` (`:196`) |
| `APICURIO_KAFKASQL_SECURITY_SASL_CLIENT_ID` | `apicurio.kafkasql.security.sasl.client-id` | `sa` (`:157`) |
| `APICURIO_KAFKASQL_SECURITY_SASL_CLIENT_SECRET` | `apicurio.kafkasql.security.sasl.client-secret` | `sa` (`:6`) |
| `APICURIO_KAFKASQL_SECURITY_SASL_TOKEN_ENDPOINT` | `apicurio.kafkasql.security.sasl.token.endpoint` | `http://localhost:8090` (`:87`) |
| `APICURIO_KAFKASQL_SECURITY_SASL_LOGIN_CALLBACK_HANDLER_CLASS` | `apicurio.kafkasql.security.sasl.login.callback.handler.class` | Strimzi `JaasClientOauthLoginCallbackHandler` (`:162`) |

Two of those properties carry a **hyphen** (`client-id`, `client-secret`); the env spellings use
`_` because SmallRye maps dots *and* dashes to `_`. The same class also declares TLS material
(`apicurio.kafkasql.security.ssl.truststore.{location,type}`,
`apicurio.kafkasql.ssl.truststore.password`, `apicurio.kafkasql.ssl.keystore.{location,password,type}`,
`apicurio.kafkasql.ssl.key.password`) — needed for a TLS listener, and deliberately not in the
guard's required set because a truststore path is worthless without the mounted file and no
static check can confirm that. One further observation from the class's constant pool, recorded
as an observation rather than a conclusion: the only `sasl.jaas.config` value it can build is an
`OAuthBearerLoginModule` template, so a non-OAUTHBEARER mechanism may not be expressible here at
all — verify before planning a SCRAM or PLAIN cluster.

[llm-service/tests/test_compose_kafka_security_env.py](../../../../llm-service/tests/test_compose_kafka_security_env.py)
enforces this: any service naming `APICURIO_KAFKASQL_BOOTSTRAP_SERVERS` must carry the seven
`APICURIO_KAFKASQL_SECURITY_*` names above, and the anchor cannot stand in for them.

### Enablement
Start with:
- `docker compose --profile schema-registry up -d schema-registry`
