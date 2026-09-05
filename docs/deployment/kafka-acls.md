# Kafka ACLs for a customer-managed (BYO) cluster

If you point rsync at a Kafka cluster you already run, and that cluster is
authorized (`authorizer.class.name` set), rsync's principal needs the ACLs below.
This page lists **every** operation the platform performs and the exact resource
names it performs them on, so you can grant a least-privilege set rather than
`--operation All --topic '*'`.

Connection and authentication settings (SASL mechanism, TLS material, `client.id`)
are in [env-vars.md](env-vars.md); this page is only about authorization.

> **Why this page is long.** Nearly every authorization failure in this platform
> is silent. A denied produce is `log.Warn` at most call sites; a denied
> `JoinGroup` leaves a consumer running with no partitions. Both read as "the
> pipeline is up and moving zero rows", not as an error. Granting too little is
> therefore expensive to debug — grant from this list rather than by trial.

> **What this list is derived from, and what it is not.** Every row below is read
> off the call site named beside it, so the *operations* are ground truth. The
> grants themselves have **not** been exercised against a live authorized broker:
> the BYO test matrix runs `authorizer.class.name` unset, which proves
> authentication and TLS end to end and proves nothing about authorization. Treat
> §6 as a starting point to apply and then verify with §7, not as a tested recipe.

---

## 1. One prefix covers topics **and** consumer groups

**`KAFKA_TOPIC_PREFIX` (default `rsync.`) now namespaces both.** Topic names go
through `kafkaclient.Topic()`; consumer group ids go through
[`kafkaclient.Group()`](../../shared/go/kafkaclient/groups.go), which
*delegates to `Topic()`* rather than reimplementing it
([groups.go:26-31](../../shared/go/kafkaclient/groups.go)) — deliberately, so a
group prefix cannot drift away from the topic prefix. The Python half is
[`kafka_topics.group()`](../../llm-service/src/utils/kafka_topics.py)
([kafka_topics.py:76](../../llm-service/src/utils/kafka_topics.py)), which
delegates to `topic()` for the same reason.

The practical consequence, and the headline of this page:

```bash
# topics
--operation Read --operation Write --operation Describe \
  --resource-pattern-type prefixed --topic 'rsync.'
# groups — same prefix, same string
--operation Read --resource-pattern-type prefixed --group 'rsync.'
```

Two `PREFIXED` grants, not a hand-maintained list of literal group names. This
replaces the previous guidance on this page, which enumerated fourteen bare group
names because that is what the code minted at the time.

### What still sits outside the prefix

| Resource | Actual name | Why it is not prefixed |
|---|---|---|
| **Connect cluster group** ⚠️ | `rsync-connect-cluster` | **The one consumer group in the platform that is NOT namespaced** — see the callout in §2 |
| Kafka Connect internal topics | `_rsync-connect-configs`, `_rsync-connect-offsets`, `_rsync-connect-status` | Connect worker config, hardcoded at [docker-compose.yml:338-340](../../docker-compose.yml) and [docker-compose.kafka-connect.yml:34-36](../../shared/internal/infra/kafka-connect/docker-compose.kafka-connect.yml) |
| Healer / notification / heartbeat topics | `rsync.notifications`, `rsync.healer.actions`, `rsync.healer.results`, `rsync.agents.heartbeat`, `rsync.sentinel.audit` | String literals that spell the default prefix themselves ([healer.go:50-52](../../backend-orchestrator/internal/agents/healer/healer.go), [sentinel.go:23-24](../../backend-orchestrator/internal/agents/sentinel/sentinel.go), [notifier.go:53-55](../../api-gateway/internal/notifier/notifier.go)). They are covered by a `rsync.` grant **only because the default prefix is also `rsync.`** |

**Debezium's schema history is no longer on this list.** It used to be
`schema-changes.<database_name>`; the connector now names it
`<KAFKA_TOPIC_PREFIX>schemahistory.<connector_name>`
([connector.py:528](../../shared/mcp-connectors/internal/debezium/versions/v1.0.0/connector.py)),
and `topic.prefix` is qualified the same way ([:516](../../shared/mcp-connectors/internal/debezium/versions/v1.0.0/connector.py)).
A `PREFIXED` topic ACL on your prefix covers both. If you are upgrading an
existing deployment, the *old* `schema-changes.*` topics are still on the broker
and can be dropped once every connector has restarted under the new name.

> ⚠️ **If you set `KAFKA_TOPIC_PREFIX` to anything that does not start with
> `rsync.`, do not expect the healer/notification topics to follow it.** Worse,
> the two halves disagree: the orchestrator's produce chokepoint qualifies
> ([manager.go:321](../../backend-orchestrator/internal/kafka/manager.go)), so it
> writes `<yourprefix>rsync.notifications`, while the api-gateway notifier
> subscribes to the bare literal `rsync.notifications`
> ([notifier.go:186](../../api-gateway/internal/notifier/notifier.go)). Every
> notification, healer result and healer action is then silently dropped. Tracked
> as `KI-NOTIFY-TOPICS-SPLIT-BRAIN-UNDER-CUSTOM-PREFIX`. **Until that is fixed, a custom
> prefix is not a supported configuration** — leave `KAFKA_TOPIC_PREFIX` at its
> default, or set it empty to keep pre-namespace names.

---

## 2. Consumer groups

Every group id below is produced by `kafkaclient.Group()` / `kafka_topics.group()`,
so **all of them are covered by one `PREFIXED` grant on `KAFKA_TOPIC_PREFIX`** —
`rsync.` by default. The table exists so you can recognise a group in
`kafka-consumer-groups.sh --list`, not because you have to enumerate them.

Names below are shown with the default prefix. `<pid8>` / `<eid8>` are the first
8 characters of a pipeline / execution UUID.

| Component | Group id that actually joins | Source |
|---|---|---|
| Orchestrator, per-topic consumers | `rsync.<KAFKA_GROUP_ID>-<qualified topic>` | qualified once at [config.go:172](../../backend-orchestrator/internal/config/config.go) → [kafka_identity.go:56](../../backend-orchestrator/internal/config/kafka_identity.go); joined at [manager.go:931-933](../../backend-orchestrator/internal/kafka/manager.go) |
| Orchestrator, single-group consumer (`RestartConsumerGroup`) | `rsync.<KAFKA_GROUP_ID>` | [manager.go:1380](../../backend-orchestrator/internal/kafka/manager.go) |
| Consumer-scaling agent | `rsync.<CONSUMER_GROUP_PREFIX>-<topic>` (default `rsync.rsync-pipeline-<topic>`) | [consumer/kafka_identity.go:62-63](../../backend-orchestrator/internal/agents/consumer/kafka_identity.go); default at [consumer/config.go:142](../../backend-orchestrator/internal/agents/consumer/config.go) |
| CDC table-stats agent | `rsync.cdc-table-stats-<pipeline uuid>` | [cdcstats/kafka_identity.go:33](../../backend-orchestrator/internal/agents/cdcstats/kafka_identity.go) |
| CDC schema-change agent | `rsync.cdc-schema-changes-<pipeline uuid>` | [cdcstats/kafka_identity.go:37](../../backend-orchestrator/internal/agents/cdcstats/kafka_identity.go) |
| CDC sink, batch backfill topic | `rsync.sink-<pid8>-batch` | [sink_consumer_group.go:68](../../backend-orchestrator/internal/agents/executor/sink_consumer_group.go) |
| CDC sink, streaming-only | `rsync.sink-<pid8>-stream` | [sink_consumer_group.go:73](../../backend-orchestrator/internal/agents/executor/sink_consumer_group.go) |
| CDC sink, default | `rsync.sink-<pid8>` | [sink_consumer_group.go:75](../../backend-orchestrator/internal/agents/executor/sink_consumer_group.go) |
| CDC sink, per-execution (**opt-in only**) | `rsync.sink-<pid8>-<eid8>` | [sink_consumer_group.go:71](../../backend-orchestrator/internal/agents/executor/sink_consumer_group.go), behind `CDC_STREAMING_SINK_GROUP_PER_EXECUTION` (default off, [:17](../../backend-orchestrator/internal/agents/executor/sink_consumer_group.go)) |
| Sentinel, DLQ protocol repair | `rsync.sentinel-protocol-fix-<sanitized topic>` | [healer.go:390](../../backend-orchestrator/internal/agents/sentinel/healer.go) → [`stableGroupID`:807](../../backend-orchestrator/internal/agents/sentinel/healer.go) |
| Sentinel, DLQ replay | `rsync.sentinel-dlq-replay-<sanitized topic>` | [healer.go:517](../../backend-orchestrator/internal/agents/sentinel/healer.go) |
| api-gateway, main consumer | `rsync.api-gateway-consumer-group` | [consumer.go:92](../../api-gateway/internal/kafka/consumer.go); logical name passed at [main.go:456](../../api-gateway/cmd/server/main.go) |
| api-gateway, projector | `rsync.api-gateway-projector` | [event_projector.go:89](../../api-gateway/internal/projector/event_projector.go) |
| api-gateway, notifier inbox | `rsync.api-gateway-notifier` | [notifier.go:113](../../api-gateway/internal/notifier/notifier.go) |
| api-gateway, domain events | `rsync.api-gateway-domain-events` | [domain_events.go:97](../../api-gateway/internal/handlers/domain_events.go) |
| api-gateway, WebSocket bridge | `rsync.websocket-bridge-<logical topic>` — the topic's own prefix is stripped first, so the prefix appears once | [kafka_bridge.go:109](../../api-gateway/internal/websocket/kafka_bridge.go) |
| temporal-adapter, agent results | `rsync.temporal-adapter-consumer` | [kafka_adapter.go:75](../../backend-temporal-adapter/internal/adapter/kafka_adapter.go) → `ConsumerGroupID()`; asserted at [kafka_identity_test.go](../../backend-temporal-adapter/cmd/adapter/kafka_identity_test.go) |
| llm-service, planner | `rsync.planner-service` | [planner/kafka_consumer.py:70](../../llm-service/src/agents/planner/kafka_consumer.py) |
| llm-service, PII scanner | `rsync.llm-service-pii-scanner` | [pii_scanner/kafka_consumer.py:33](../../llm-service/src/agents/pii_scanner/kafka_consumer.py) |
| llm-service, Avro fallback | `rsync.avro-consumer-<already-qualified topics>` — e.g. `rsync.avro-consumer-rsync.agent.planner.requests` | [avro_kafka.py:254](../../llm-service/src/utils/avro_kafka.py), wired at [:318](../../llm-service/src/utils/avro_kafka.py) |
| CDC sink worker (`kafka-mcp-sink`) | inherits the orchestrator's already-qualified id verbatim — mints nothing of its own | `config.consumer_group`, [main.go:3127](../../shared/mcp-connectors/internal/kafka-mcp-sink/worker-src/cmd/kafka-sink-worker/main.go) |

Every one of these needs `Read` on the `Group` resource (`Read` is what
`JoinGroup`, `SyncGroup`, `Heartbeat` and `OffsetCommit` check).

### ⚠️ One group is still NOT namespaced: `rsync-connect-cluster`

The Kafka Connect worker's `group.id` is a hardcoded literal —
[docker-compose.yml:337](../../docker-compose.yml) and
[docker-compose.kafka-connect.yml:33](../../shared/internal/infra/kafka-connect/docker-compose.kafka-connect.yml).
It is set as a container environment variable read by the Debezium image's
entrypoint, never by `kafkaclient.Group()`, so **`KAFKA_TOPIC_PREFIX` does not
reach it and a `PREFIXED` grant on `rsync.` does not cover it.** It needs its own
`LITERAL` `Group` ACL, and it is the reason §6's worked example still has a
literal-group command in it.

This is stated here rather than omitted because the failure mode is invisible:
a Connect worker denied `Read` on its own group does not crash — it retries the
group join forever, the REST API answers `200`, `GET /connectors` returns an
empty list, and every CDC pipeline reports provisioned while streaming nothing.

### Renaming groups resets offsets

Adopting the prefix on an **existing** deployment changes every group id, and a
group id is the key committed offsets are stored under. New id ⇒ no committed
offsets ⇒ each consumer starts from its configured `auto.offset.reset`. Plan the
cutover the same way as the topic rename (§1 of
[env-vars.md](env-vars.md#kafka)): one deploy of every Kafka-touching container,
not a per-service rollout. The old groups linger on the broker until
`offsets.retention.minutes` expires them or you delete them by hand.

---

## 3. Cluster- and group-level operations

| Operation | Resource | Needed by | Source |
|---|---|---|---|
| `Describe` | `Cluster` | every client's metadata refresh; `ListGroups` behind the topology API's group listing | [manager.go:1242](../../backend-orchestrator/internal/kafka/manager.go) (`ListTopics`), [topology.go:620](../../backend-orchestrator/internal/kafka/topology.go) (`ListConsumerGroups`) |
| `Create` | `Cluster` or `Topic` | topic provisioning — see §5 | [topology.go:317](../../backend-orchestrator/internal/kafka/topology.go) (`ensureTopicLocked`, the **only** creator in the service) |
| `Alter` | `Topic` | partition expansion on an existing topic | [topology.go:295](../../backend-orchestrator/internal/kafka/topology.go), [:703](../../backend-orchestrator/internal/kafka/topology.go) (`CreatePartitions`) |
| `Delete` | `Topic` | pipeline teardown and `DELETE /api/v1/topology/topics/:name` | [topology.go:668](../../backend-orchestrator/internal/kafka/topology.go), called from [cdc_kafka_teardown.go:345](../../backend-orchestrator/internal/handlers/cdc_kafka_teardown.go) |
| `Describe` | `Topic` | `ListTopics` metadata calls | [topology.go:282](../../backend-orchestrator/internal/kafka/topology.go), [:572](../../backend-orchestrator/internal/kafka/topology.go) |
| `Describe` | `Group` | consumer-lag reporting (`OffsetFetch`) | [manager.go:1046](../../backend-orchestrator/internal/kafka/manager.go) |
| `Delete` | `Group` | pipeline teardown deletes the pipeline's own `sink-*` groups | [topology.go:640](../../backend-orchestrator/internal/kafka/topology.go), called from [cdc_kafka_teardown.go:329](../../backend-orchestrator/internal/handlers/cdc_kafka_teardown.go) |

rsync does **not** call `DescribeConfigs` or `AlterConfigs` — you do not need to
grant them. Topic configs (`min.insync.replicas`, `cleanup.policy`, retention)
are set at creation time in the `CreateTopics` request, never altered afterwards.

> **`Delete` on `Topic` and `Group` is what pipeline deletion uses.** Withholding
> it does not break any data path: `cleanupPipelineKafkaResources` collects the
> failures into human-readable strings and the pipeline delete still succeeds
> ([cdc_kafka_teardown.go:301](../../backend-orchestrator/internal/handlers/cdc_kafka_teardown.go)).
> You get orphaned topics and groups instead of a failed delete. On a shared
> cluster that is a defensible trade — the topology API's authorization and
> tenant scoping are code-only and unverified against a live broker
> (`KI-SEC-TOPOLOGY-API-UNAUTHENTICATED`).

---

## 4. Topic read/write

| Operation | Resource pattern | Notes |
|---|---|---|
| `Write` | `PREFIXED` `<KAFKA_TOPIC_PREFIX>` | data plane, control plane, DLQs, Debezium `topic.prefix` **and** its schema history |
| `Read` | `PREFIXED` `<KAFKA_TOPIC_PREFIX>` | sinks, bridges, agents, Debezium schema-history recovery |
| `Write` + `Read` | `PREFIXED` `_rsync-connect-` | Connect internal topics (config / offset / status) |
| `Write` + `Read` | `LITERAL` × 5 hardcoded `rsync.*` topics from §1 | only needed separately if your prefix is not `rsync.` — and see the split-brain warning in §1 before you do that |

**Debezium's schema history is a separate Kafka client** with its own producer
and consumer halves. The producer half is exercised during snapshot; the
**consumer half only on connector restart**. A missing `Read` here therefore
looks completely healthy for as long as the connector stays up, and fails only
when it is restarted — potentially weeks later. Its security settings are
configured explicitly rather than inherited from the Connect worker
([`_schema_history_security`, connector.py:305](../../shared/mcp-connectors/internal/debezium/versions/v1.0.0/connector.py)).

### Connect worker security must be declared three times

The Debezium image maps `CONNECT_*` environment variables into
`connect-distributed.properties`. Kafka Connect reads security settings in three
independent scopes, and setting only the worker one is the classic partial
failure: the worker starts, the REST API answers, and then **source-task
producers and sink-task consumers** fail their own handshake later, far from the
config that caused it. All three are wired in
[docker-compose.yml:403-470](../../docker-compose.yml):

- `CONNECT_SECURITY_PROTOCOL` / `CONNECT_SASL_MECHANISM` / `CONNECT_SASL_JAAS_CONFIG`
- `CONNECT_PRODUCER_…` (same three)
- `CONNECT_CONSUMER_…` (same three)

Two of these are deliberately **bare pass-through keys** with no `${VAR:-}`
default — `CONNECT_*_SSL_TRUSTSTORE_LOCATION` ([:446-448](../../docker-compose.yml))
and `CONNECT_*_SSL_TRUSTSTORE_PASSWORD` ([:462-464](../../docker-compose.yml)).
A key with no value is omitted from the container environment; `${VAR:-}` would
render it present-but-empty, and the entrypoint writes every `CONNECT_` variable
it sees, empty included. Kafka reads an empty `ssl.truststore.location` as a real
path and the worker exits 2. Confluent Cloud and MSK need no truststore setting
at all, so an empty default would break the commonest BYO case outright. See
[env-vars.md](env-vars.md#kafka) for the variables that drive them.

### Replication factor and `min.insync.replicas`

`KAFKA_REPLICATION_FACTOR` is a **request, not a guarantee**: it is clamped down
to the live broker count so a typo degrades to what the cluster can serve rather
than failing every topic creation with `InvalidReplicationFactor`
([replication.go:131-139](../../backend-orchestrator/internal/kafka/replication.go)).
`KAFKA_MIN_INSYNC_REPLICAS` is clamped to the topic's final replication factor in
the same pass, and in that order — `misr > RF` produces a topic that is created,
listed and subscribable, and then rejects every `acks=all` produce with
`NOT_ENOUGH_REPLICAS`
([replication.go:157-175](../../backend-orchestrator/internal/kafka/replication.go),
[:198-204](../../backend-orchestrator/internal/kafka/replication.go)).

When you set neither, an explicit `min.insync.replicas` is still pinned onto
every topic this service creates — `min(2, RF)` — because leaving it unset means
the **broker's** default applies, and on MSK and most managed clusters that
default is 2. An RF=1 topic inheriting `misr=2` is born permanently unwritable
with nothing in any log naming the replication factor as the cause. Defaults and
exact parsing rules are in [env-vars.md](env-vars.md#kafka).

The Connect worker's three internal topics take the same variable —
`CONNECT_{CONFIG,OFFSET,STATUS}_STORAGE_REPLICATION_FACTOR`
([docker-compose.yml:359-361](../../docker-compose.yml)) — but they are created by
the Connect worker, not by rsync, so they are **not** subject to the clamp above.
On a single-broker cluster leave `KAFKA_REPLICATION_FACTOR` unset or at `1`.

---

## 5. If you run with `auto.create.topics.enable=false`

**Narrowed, not closed.** The batch data topic and the CDC topic are now
pre-created explicitly through the orchestrator's `TopologyManager`
([executor.go](../../backend-orchestrator/internal/agents/executor/executor.go)
via `Manager.EnsureTopicExists` → [manager.go:1260](../../backend-orchestrator/internal/kafka/manager.go)),
and a source-scanning guard test fails the build if a new produce target appears
in the orchestrator without a matching creator
([topology_produce_targets_test.go](../../backend-orchestrator/internal/kafka/topology_produce_targets_test.go)).

Topics that are **still** auto-create-only: `pipeline.failed.dlq` (produced by
the temporal-adapter) and `pii.scan.request` / `pii.scan.response` (produced by
the api-gateway) — neither service is covered by the orchestrator's guard test —
plus the five hardcoded `rsync.*` topics from §1. Details and the current
inventory are tracked as `KI-KAFKA-DATAPLANE-AUTOCREATE-ONLY`.

So on a cluster with auto-create off you must still grant `Create`.

### `Create` cannot be traded for pre-creation

Pre-creating the inventory in [kafka-topics.md](../architecture/kafka-topics.md)
out of band closes the *static* half of the problem and none of the half that
carries your rows.

**Every pipeline's data topic is named at runtime**, from the pipeline's own id —
`rsync.pipeline.<pipeline-id-8>.data` — in
[executor.go:2530](../../backend-orchestrator/internal/agents/executor/executor.go)
and [hybrid_cdc.go:345](../../backend-orchestrator/internal/agents/executor/hybrid_cdc.go).
The name does not exist until an operator creates the pipeline, so there is
nothing to pre-create and no static job that can cover it: the chart's
`kafka-init` Job creates 14 fixed topics and stops there.

> **A grant of Read/Write on `<KAFKA_TOPIC_PREFIX>` with no `Create` cannot run a
> single pipeline.** This is not a degraded mode — it is the data path. If your
> cluster policy forbids `Create`, the platform cannot be deployed against it as
> shipped.

The `Create` grant may be scoped: `PREFIXED` on `<KAFKA_TOPIC_PREFIX>` is enough,
and is what §6 grants. Cluster-wide `Create` is only needed if you leave the
prefix empty.

**Verified against a real auto-create-off cluster.** Earlier revisions of this
page said rsync had not been run against one; it has since been run against four
(`PLAINTEXT`, `SASL_PLAINTEXT`+SCRAM, `SASL_SSL`+SCRAM, `SASL_SSL`+OAUTHBEARER)
on a broker with `auto.create.topics.enable=false` confirmed live from the
broker's own `describe-configs`. All 29 topics present afterwards were therefore
created by clients, and pipelines moved rows end to end on every one.

---

## 6. Worked example

`kafka-acls.sh` against the default `KAFKA_TOPIC_PREFIX=rsync.`, for principal
`User:rsync`. Note that `KAFKA_GROUP_ID` no longer appears anywhere: it is
qualified under the prefix, so the prefixed group grant already covers it.

```bash
# --- topics: the prefixed world (includes Debezium schema history) ---
kafka-acls.sh --bootstrap-server "$BROKER" --command-config "$ADMIN_CFG" --add \
  --allow-principal User:rsync \
  --operation Read --operation Write --operation Describe --operation Delete \
  --resource-pattern-type prefixed --topic 'rsync.'

# --- topics: Connect internals ---
kafka-acls.sh --bootstrap-server "$BROKER" --command-config "$ADMIN_CFG" --add \
  --allow-principal User:rsync \
  --operation Read --operation Write --operation Describe \
  --resource-pattern-type prefixed --topic '_rsync-connect-'

# --- consumer groups: ONE prefixed grant, same string as the topic prefix ---
kafka-acls.sh --bootstrap-server "$BROKER" --command-config "$ADMIN_CFG" --add \
  --allow-principal User:rsync \
  --operation Read --operation Describe --operation Delete \
  --resource-pattern-type prefixed --group 'rsync.'

# --- the ONE group that is not namespaced: the Connect worker ---
kafka-acls.sh --bootstrap-server "$BROKER" --command-config "$ADMIN_CFG" --add \
  --allow-principal User:rsync --operation Read \
  --resource-pattern-type literal \
  --group 'rsync-connect-cluster'

# --- cluster: Describe always. Create is NOT optional -- per-pipeline data topics
#     are named at runtime and cannot be pre-created (§5). Kafka checks Create on
#     the Topic resource first and falls back to Cluster, so a PREFIXED topic
#     grant can replace this cluster-wide one; that narrowing is untested here. ---
kafka-acls.sh --bootstrap-server "$BROKER" --command-config "$ADMIN_CFG" --add \
  --allow-principal User:rsync \
  --operation Describe --operation Create --cluster
```

Notes on the operations above:

- `Delete` on topics and groups is only used by pipeline teardown (§3). Drop both
  `--operation Delete` flags if you would rather accept orphaned resources.
- The five hardcoded `rsync.*` topics from §1 are covered by the first command
  **only because the default prefix is itself `rsync.`**. Changing
  `KAFKA_TOPIC_PREFIX` does not merely require adding them as `LITERAL` topic
  ACLs — read the split-brain warning in §1 first.
- `rsync-connect-cluster` also needs `Read`/`Write`/`Describe` on the
  `_rsync-connect-` topics, which the second command already grants to the same
  principal. If the Connect worker authenticates as a *different* principal, both
  the group and the topic grants have to be repeated for it.

---

## 7. Verifying the grant

A missing ACL does not announce itself. After applying, confirm the platform is
actually consuming rather than merely running:

```bash
kafka-consumer-groups.sh --bootstrap-server "$BROKER" --command-config "$ADMIN_CFG" --list
```

Two things to check, in this order:

1. **Every group is namespaced.** With the default settings, every id except
   `rsync-connect-cluster` should start with `rsync.`. A bare group id in this
   listing means a consumer somewhere bypassed `kafkaclient.Group()` — file it,
   because the prefixed grant does not cover it.
2. **`--describe` on each shows assigned partitions.** A group that exists with
   **zero** assigned partitions is the signature of a missing `Group` ACL, not of
   an idle pipeline.

`rsync-connect-cluster` will not appear at all if its own `LITERAL` grant is
missing — the worker never completes the join, and its absence from this listing
is the only symptom you get.
