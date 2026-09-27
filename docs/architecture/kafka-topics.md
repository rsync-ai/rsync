# Kafka topics

Descriptive catalogue of every Kafka topic the platform names, and — the part that
matters for a BYO-Kafka deployment — **who creates each one**. Every claim below is
cited to a `file:line`; if you change a name, change the citation with it.

> This document was rewritten 2026-08-16. The previous version described an
> `agent.{name}.requests` fan-out with per-agent DLQs that the code has not used for
> a long time: 14 of the names it documented had zero producers and zero consumers
> across all five services. Don't restore it from git history.
>
> Revised again when the agent Kafka bus was removed in
> [#1227](https://github.com/rsync-ai/rsync-ai/pull/1227): agent stages hand off through
> Redis, and a default install provisions four platform topics.

## 1. The namespace prefix

Every topic name is qualified with a deployment-owned prefix, default `rsync.`, so an
operator on a shared cluster can tell which topics are ours.

| Language | Helper | Env var |
|---|---|---|
| Go | `kafkaclient.Topic()` / `Topics()` — [shared/go/kafkaclient/topics.go:46](../../shared/go/kafkaclient/topics.go) | `KAFKA_TOPIC_PREFIX` |
| Python | `topic()` / `topics()` — [llm-service/src/utils/kafka_topics.py:53](../../llm-service/src/utils/kafka_topics.py) | same |
| Shell | inline block in [scripts/kafka-init-new-topics.sh](../../scripts/kafka-init-new-topics.sh) and [scripts/create_kafka_topics.sh](../../scripts/create_kafka_topics.sh) | same |

All three implementations must agree byte-for-byte. They share four behaviours:

- **Default** `rsync.` when the variable is unset.
- **Normalize** — strip anything outside Kafka's `[a-zA-Z0-9._-]` charset, then append
  `.` if the last character is not already `.`, `_` or `-`. Without this, prefix `acme`
  yields `acmepipeline.domain.events` on one side and `acme.pipeline.domain.events` on the other — both
  legal topic names, so the split surfaces only as a consumer that receives nothing.
- **Idempotent** — `Topic("rsync.pipeline.abc12345.data")` is unchanged, never
  `rsync.rsync.pipeline.abc12345.data`. Topic names are persisted (`pipelines.kafka_topic`,
  connector configs) and read back on the next run.
- **Empty means empty.** Setting `KAFKA_TOPIC_PREFIX=""` disables qualification. That is
  the migration lever for a deployment with live topics and committed offsets under the
  unprefixed names. Shell code must therefore use `${KAFKA_TOPIC_PREFIX-rsync.}` with a
  **bare `-`** — `:-` substitutes on empty as well as unset and would silently overwrite
  the operator's deliberate choice.

`in_namespace()` (Python) recognizes both the prefixed and bare forms, because a
deployment mid-migration has some of each.

### Reclaiming pre-namespace topics — `KAFKA_ALLOW_LEGACY_UNPREFIXED_TOPICS`

| Var | Default | Set it when |
|---|---|---|
| `KAFKA_ALLOW_LEGACY_UNPREFIXED_TOPICS` | unset (false) | upgrading a deployment that has **adopted** the prefix but still has topics minted under the old bare names, and only until those are reclaimed |

The orchestrator's topology API only accepts topic names inside a namespace the
platform owns ([topology.go:176](../../backend-orchestrator/internal/handlers/topology.go)).
Since f1ee815e that allowlist is
the configured prefix plus the branded `_rsync-`. The seven pre-namespace prefixes —
`agent.` `pipeline.` `cdc.` `cdc-` `schemahistory.` `pii.` `task.` — are **out of it by
default**, because on a shared cluster they are generic enough to name another team's
topics, and `DELETE` is a verb Kafka cannot undo.

They come back in exactly two cases:

1. **`KAFKA_TOPIC_PREFIX` is empty.** Then the bare names *are* the platform's own names,
   and excluding them would strand the platform outside its own allowlist. No variable
   needed — this is automatic.
2. **`KAFKA_ALLOW_LEGACY_UNPREFIXED_TOPICS` is truthy** (`1` `true` `yes` `on`). The
   migration window. It logs a one-shot warning naming the variable and the risk, so the
   widening is never silent.

Do **not** put this in a compose file. The safe value is the default, and writing a
migration lever into compose is how a migration window becomes permanent. Unset it once
the old topics are gone.

Narrowing the allowlist cannot strand a teardown: the platform's own reclamation path
(`POST /cdc/kafka-teardown` → `cleanupPipelineKafkaResources`,
[cdc_kafka_teardown.go](../../backend-orchestrator/internal/handlers/cdc_kafka_teardown.go))
reaches the TopologyManager directly and never consults it. That sweep matches each
resource in **both** its bare and its qualified spelling for the same reason
`in_namespace()` does.

`KAFKA_OWNED_TOPIC_PREFIXES` (comma-separated) extends the allowlist for a deployment
that names topics differently. It adds to the built-ins and never replaces them.

Logical names are written **unprefixed** everywhere in code and in this document. The
helper adds the prefix at the call site — except inside the orchestrator, where
`Manager.ProduceWithContext` and `ProduceWithHeadersAndContext` qualify the name
themselves ([internal/kafka/manager.go:337](../../backend-orchestrator/internal/kafka/manager.go),
[:465](../../backend-orchestrator/internal/kafka/manager.go)). That is why orchestrator
call sites such as [workers/progress_events.go:268](../../backend-orchestrator/internal/workers/progress_events.go)
pass a bare `"pipeline.domain.events"` and still land in the namespace. It works because
`Topic()` is idempotent, so an already-qualified name (a stored `pipelines.kafka_topic`,
say) passes through unchanged.

## 2. Who creates what

The platform creates topics in the places below.

Every programmatic creation in the orchestrator funnels through one function —
`TopologyManager.ensureTopicLocked` — which is the invariant
[`topology_single_creator_test.go`](../../backend-orchestrator/internal/kafka/topology_single_creator_test.go)
exists to hold. That is what makes the replication-factor and `min.insync.replicas`
clamping unavoidable rather than opt-in: a second creator would be a second place for a
topic to be born with a floor above its replication factor, which produces a topic that
is created, listed, subscribable and permanently unwritable.

### 2a. `TopologyManager.EnsurePlatformTopics` — the startup creator

[backend-orchestrator/internal/kafka/topology.go:413](../../backend-orchestrator/internal/kafka/topology.go),
called at orchestrator startup through `ensurePlatformTopicsWithRetry`
([cmd/orchestrator/platform_topics.go](../../backend-orchestrator/cmd/orchestrator/platform_topics.go),
[main.go:513-520](../../backend-orchestrator/cmd/orchestrator/main.go)) **before any
consumer starts** — a consumer-group subscription auto-creates the topic it joins at the
broker's defaults, and this function never re-sizes an existing topic. It creates the
names `PlatformTopicNames()` returns (topology.go:375), through `EnsureTopic` →
`kafkaclient.Topic()`, so they are correctly prefixed:

| Topic | Always? |
|---|---|
| `pipeline.domain.events` | yes |
| `rsync.notifications` | yes |
| `pii.scan.request` | yes |
| `pii.scan.response` | yes |
| `rsync.healer.schema-changes` | only with `RSYNC_SCHEMA_DRIFT_ENABLED=true` |
| `rsync.healer.approved-changes` | only with `RSYNC_SCHEMA_DRIFT_ENABLED=true` |
| `rsync.healer.results` | only with `RSYNC_SCHEMA_DRIFT_ENABLED=true` |

The healer topics are gated because their producers and consumers are: with the flag off
nothing reads or writes them.

Every platform topic gets 3 partitions and `cleanup.policy=delete`,
`retention.ms=604800000` (7 days), `compression.type=snappy` (topology.go:345-358), with
`KeepExistingPartitions: true`: the records are keyed, and widening a live topic re-hashes
keys onto other partitions, so creating one and re-sizing an existing one are deliberately
different decisions. The values are not free choices either — §2b and §2c create three of
the same names with the same config, and no creator ALTERs an existing topic, so whichever
runs first on a given deployment wins permanently.

Replication factor is `KAFKA_REPLICATION_FACTOR` if set, else derived from the live broker
count, and clamped either way.

A failed create does not stop the others (the errors come back through `errors.Join`).
The startup wrapper retries up to 5 times with a doubling backoff from 1 s, then logs
`"⚠️  Failed to ensure platform topics"` at **Warn** and startup continues — the services
that use the topic surface their own produce or consume error.

This replaced `EnsureAgentControlTopics`, which created the agent bus
(`agent.control.*`, `agent.*.responses`, `pipeline.agent.telemetry`, the two
`*.failed.dlq` topics and the `rsync.healer.actions` / `rsync.sentinel.audit` pair), and
the `CreateTopicForPipeline` operator route. Both were removed in
[#1227](https://github.com/rsync-ai/rsync-ai/pull/1227); agent stages now hand off
through Redis, not Kafka.

### 2b. `kafka-init` — the compose and Helm bootstrappers

One-shot container in [docker-compose.yml](../../docker-compose.yml) (`kafka-init`),
running [scripts/kafka-init-new-topics.sh](../../scripts/kafka-init-new-topics.sh) after
the broker passes its healthcheck. It creates `pipeline.domain.events`,
`pii.scan.request` and `pii.scan.response`, each at 3 partitions, `cleanup.policy=delete`,
7 days' retention — the §2a config.

[docker-compose.quickstart.yml](../../docker-compose.quickstart.yml) has its own inline
variant and the Helm chart a `kafka-init` Job
([deploy/helm/rsync-ai/templates/jobs/kafka-init.yaml](../../deploy/helm/rsync-ai/templates/jobs/kafka-init.yaml));
both create the same three with the same config. All of them overlap §2a harmlessly —
`--if-not-exists`. `rsync.notifications` and the healer topics are created by §2a only.

### 2c. `scripts/create_kafka_topics.sh` — dev setup only

Invoked by [scripts/setup.sh:83](../../scripts/setup.sh); not part of any compose file,
so **it never runs on a deployed stack.** Creates the same three topics as §2b with the
same config.

### 2d. Runtime pre-creation on the pipeline path

The topics above are known at startup. The data-plane topics are not: their names carry a
pipeline id or a connector name, so they can only be created when a pipeline runs.
[`Manager.EnsureTopicExists`](../../backend-orchestrator/internal/kafka/manager.go) (:1671)
and its config-carrying sibling `EnsureTopicExistsWithConfig` (:1675) are that path, with
two wrappers that fix the config: `EnsureSignalTopic` (:1733) and `EnsureDDLTopic` (:1770).
They create the name the caller chose verbatim — these names are owned by Debezium and by
the sink's subscription config, and re-qualifying one would create a topic nobody reads —
while the replication clamping still applies.

| Call site | Topic | Partitions | Config |
|---|---|---|---|
| [executor.go:4054](../../backend-orchestrator/internal/agents/executor/executor.go) | `pipeline.<id8>.data` (batch) | 1 | — |
| [executor.go:6437](../../backend-orchestrator/internal/agents/executor/executor.go) | `cdc-<id8>.<db>.<table>`, or one unified topic for dimension tables | resolved per pipeline, fallback 1 (executor.go:6278) | — |
| [executor.go:3351](../../backend-orchestrator/internal/agents/executor/executor.go) | `schemahistory.<connector>` — historized engines only (MySQL/MariaDB, SQL Server, Oracle, Db2) | 1 | `cleanup.policy=delete`, `retention.ms=-1` |
| [executor.go:3371](../../backend-orchestrator/internal/agents/executor/executor.go) | the connector's DDL topic, its bare `topic.prefix` `cdc-<id8>` — historized engines only | 1 | `EnsureDDLTopic`: delete, 7 days |
| [executor.go:3403](../../backend-orchestrator/internal/agents/executor/executor.go) | `heartbeat.<topic.prefix>` (so `rsync.heartbeat.rsync.cdc-<id8>` under the default prefix) — PostgreSQL and MongoDB only | 1 | `cleanup.policy=delete` |
| [executor.go:2968](../../backend-orchestrator/internal/agents/executor/executor.go), [cdc_incremental.go:357](../../backend-orchestrator/internal/agents/executor/cdc_incremental.go) | `signals.<id8>` — PostgreSQL family and MongoDB only | 1 | `EnsureSignalTopic`: delete, 1 day, hourly segments |
| [manager.go:1016](../../backend-orchestrator/internal/kafka/manager.go) | `<consumed topic>.dlq` for each orchestrator consumer | 1 | `DLQTopicConfig`: delete, 7 days |

The sink worker creates its own `<source_topic>.dlq` before the first write, 1 partition
and 7 days' retention
([kafka-sink-worker main.go `ensureDLQTopic`](../../shared/mcp-connectors/internal/kafka-mcp-sink/worker-src/cmd/kafka-sink-worker/main.go)).

Every pre-create above is **best-effort**: a failure is logged at Warn and the run
continues, so a broker that does auto-create behaves exactly as it did before. That is
deliberate — the pre-creation removes a dependency, it does not add a new way to fail a
pipeline.

The schema-history entry is the one with a geometry that is a correctness requirement
rather than a preference. Debezium replays that topic in order to rebuild the source DDL,
so it must have exactly **1 partition**; the records are not keyed per schema object, so
`cleanup.policy` must be **`delete`** and never `compact`; and `retention.ms` must be
**-1**, because a finite retention expires the history and the connector then fails on its
first RESTART after expiry — days after any change, with an error that names nothing about
retention. The orchestrator passes the name it created to the connector in
`params["schema_history_topic"]` rather than letting both sides derive one, since two
copies of the naming rule that disagree would have the orchestrator create one topic and
Connect write to another. Pinned by `cdc_schema_history_topic_test.go` in
[internal/agents/executor](../../backend-orchestrator/internal/agents/executor) on the Go
side and by `test_topic_naming.py` on the connector side.

## 3. Topic catalogue

### Platform topics

| Topic | Producer | Consumer | Created by |
|---|---|---|---|
| `pipeline.domain.events` | orchestrator ([progress_events.go:268](../../backend-orchestrator/internal/workers/progress_events.go), [main.go:333](../../backend-orchestrator/cmd/orchestrator/main.go)); temporal-adapter ([activities.go:144](../../backend-temporal-adapter/internal/workflows/activities.go)); sink worker ([main.go:3933](../../shared/mcp-connectors/internal/kafka-mcp-sink/worker-src/cmd/kafka-sink-worker/main.go)) | api-gateway WebSocket bridge, event projector, domain-events consumer | §2a, §2b |
| `rsync.notifications` | orchestrator schema-drift healer ([healer.go:1274](../../backend-orchestrator/internal/agents/healer/healer.go)); temporal-adapter ([pipeline_failure_notification.go:61](../../backend-temporal-adapter/internal/workflows/pipeline_failure_notification.go)) | api-gateway notifier ([notifier.go:61](../../api-gateway/internal/notifier/notifier.go)) | §2a |
| `pii.scan.request` | api-gateway ([handlers/pii.go:302](../../api-gateway/internal/handlers/pii.go)) | llm-service ([pii_scanner/kafka_consumer.py:21](../../llm-service/src/agents/pii_scanner/kafka_consumer.py)) | §2a, §2b |
| `pii.scan.response` | llm-service PII scanner | api-gateway ([cmd/server/main.go:539](../../api-gateway/cmd/server/main.go)) | §2a, §2b |

### Schema-drift topics (only with `RSYNC_SCHEMA_DRIFT_ENABLED=true`)

| Topic | Producer | Consumer | Created by |
|---|---|---|---|
| `rsync.healer.schema-changes` | sink worker ([main.go:8462](../../shared/mcp-connectors/internal/kafka-mcp-sink/worker-src/cmd/kafka-sink-worker/main.go)), executor, cdcstats (per [topology.go:368](../../backend-orchestrator/internal/kafka/topology.go)) | orchestrator healer ([healer.go:180](../../backend-orchestrator/internal/agents/healer/healer.go)) | §2a |
| `rsync.healer.approved-changes` | api-gateway ([handlers/schema_evolution.go:390](../../api-gateway/internal/handlers/schema_evolution.go)) | orchestrator healer | §2a |
| `rsync.healer.results` | orchestrator healer ([healer.go:1346](../../backend-orchestrator/internal/agents/healer/healer.go)) | api-gateway notifier ([notifier.go:62](../../api-gateway/internal/notifier/notifier.go)) | §2a |

The orchestrator's agent workers no longer consume Kafka at all: they poll the Redis
correlation store. Its remaining consumers are these healer subscriptions and the cdcstats
agent's per-pipeline schema-change reader (Data plane, below).

### The WebSocket bridge

[api-gateway/internal/websocket/kafka_bridge.go:74](../../api-gateway/internal/websocket/kafka_bridge.go)
subscribes to one topic, `pipeline.domain.events`. It used to subscribe to many more; the
producer-less ones were pruned first, and the rest (`pipeline.agent.telemetry`,
`agent.planner.responses`, `agent.executor.responses`) went with the agent bus in
[#1227](https://github.com/rsync-ai/rsync-ai/pull/1227).

That was not cosmetic. A subscription is a topic dependency: the bridge opens one consumer
group per topic, and on a broker with `auto.create.topics.enable=true` a *fetch* against a
non-existent topic creates it. Topics were therefore being brought into existence by the
act of watching for messages that nothing ever sent — which both hid the auto-create
dependency (the topics existed, so nothing looked wrong) and made the grant a customer's
Kafka operator has to write longer than it needed to be. The remaining group id is pinned
by [`kafka_bridge_group_test.go`](../../api-gateway/internal/websocket/kafka_bridge_group_test.go).

### Data plane

| Topic | Shape | Producer | Consumer | Created by |
|---|---|---|---|---|
| `pipeline.<id8>.data` | batch row chunks / MinIO claim-check URLs | executor ([executor.go:2410](../../backend-orchestrator/internal/agents/executor/executor.go) names it) | `kafka-mcp-sink` | §2d |
| `cdc-<id8>.<db>.<table>` | Debezium per-table stream | Debezium via Kafka Connect | `kafka-mcp-sink` | §2d |
| `schemahistory.<connector>` | Debezium source-DDL history | Kafka Connect | Kafka Connect (on connector restart) | §2d |
| `cdc-<id8>` | Debezium DDL events (historized engines) | Kafka Connect | orchestrator cdcstats ([schema_changes.go](../../backend-orchestrator/internal/agents/cdcstats/schema_changes.go)) | §2d |
| `heartbeat.<topic.prefix>` | Debezium heartbeats (PostgreSQL, MongoDB) | Kafka Connect | none — it exists so Debezium can commit its position on an idle source | §2d |
| `signals.<id8>` | incremental / blocking snapshot signals | orchestrator ([cdc_incremental.go:362](../../backend-orchestrator/internal/agents/executor/cdc_incremental.go)) | Debezium | §2d |
| `<topic>.dlq` | dead-lettered messages of one consumed topic | sink worker, orchestrator consumers | operator | §2d |

`<id8>` is the first 8 characters of the pipeline UUID. The batch topic name is built at
[executor.go:2410](../../backend-orchestrator/internal/agents/executor/executor.go)
unless the pipeline carries a pre-provisioned one; there is no `cdc.<id8>` topic. The
fixed `pipeline.failed.dlq` and `agent.failed.dlq` were removed in
[#1227](https://github.com/rsync-ai/rsync-ai/pull/1227).

## 4. What this means for `auto.create.topics.enable=false`

**Status: the code no longer depends on auto-creation; the setting has not been flipped.**

This section used to read: the entire data plane and most of the response plane exist only
because the broker creates topics on first produce or first fetch, so turning
`auto.create.topics.enable` off breaks batch transfer, CDC, the DLQs and the PII scanner,
with a pipeline that transfers zero rows as the only visible symptom. That was accurate,
and it mattered because the setting is one this platform does **not own** on a
customer-managed cluster — several managed offerings ship it off, and a customer may
simply have turned it off.

Every topic in §3 now has a named creator: §2a provisions the platform topics (event,
notification, PII and, when enabled, healer) at startup, and §2d pre-creates the
per-pipeline data topics when a pipeline runs. The producer-less bridge subscriptions that
were creating topics by fetching from them are gone. The remaining hole is a documented one:
`schemahistory.<connector>` is pre-created by §2d, but Kafka Connect is what writes to it,
and if the pre-create fails Connect still expects the broker to have it.

Two things are still true, and both are why the compose default has **not** been changed:

1. **Nothing here has been proven against a real broker with the setting off.** The guards
   in this repo are static and unit-level — they assert that the code creates what it
   produces to, not that a live CDC pipeline survives with auto-creation disabled. That
   needs a `kind` cluster running an end-to-end CDC pipeline against a broker configured
   with `auto.create.topics.enable=false`.
2. **`docker-compose.yml` and `docker-compose.quickstart.yml` do not set
   `KAFKA_AUTO_CREATE_TOPICS_ENABLE` at all**, so both inherit the broker image's default
   of `true`. Flipping it is the last step, not the first: with auto-creation on, a missing
   pre-create is invisible, so the flip is also the only real test of the work above.

Do not describe the platform as safe on a broker with auto-creation off until that run has
happened.

## 5. Consumer groups

**Fixed.** Group ids go through `kafkaclient.Group()` (Go) or `kafka_topics.group()`
(Python), both of which *delegate to the topic qualifier* rather than reimplementing it —
[groups.go:26-31](../../shared/go/kafkaclient/groups.go),
[kafka_topics.py:76](../../llm-service/src/utils/kafka_topics.py) — so a group prefix
cannot drift away from the topic prefix. One `PREFIXED` ACL on `KAFKA_TOPIC_PREFIX`
covers topics and groups together, which is the shape an operator actually grants.

Two consequences worth stating explicitly:

- **`KAFKA_TOPIC_PREFIX=""` disables both halves together.** That is the migration lever
  for a deployment with live groups: a renamed group has no committed offsets and starts
  from `auto.offset.reset`, so an unwanted rename re-reads or skips a topic rather than
  failing.
- **A bare group id fails silently, not loudly.** Under a customer's `PREFIXED` grant,
  an unqualified id is refused at `JoinGroup`; kafka-go and sarama both surface that as a
  retrying consumer, so it presents as a queue that stops draining while the process
  stays healthy. That is why the invariant is enforced by source-scanning tests rather
  than by a list of expected ids — the failure mode is a *new* call site, which a list
  cannot see:
  [consumer_group_namespacing_test.go](../../api-gateway/internal/kafka/consumer_group_namespacing_test.go)
  (api-gateway, scans the whole module),
  [kafka_identity_test.go](../../backend-orchestrator/internal/agents/cdcstats/kafka_identity_test.go)
  (orchestrator, per package),
  [test_kafka_consumer_groups.py](../../llm-service/tests/test_kafka_consumer_groups.py)
  (llm-service).

One exception remains: **`rsync-connect-cluster`**, Kafka Connect's worker `group.id`, is
set in the Connect compose and is not namespaced. It needs its own `LITERAL` grant. See
[kafka-acls.md](../deployment/kafka-acls.md) for the full ACL set.

## 6. Conventions for new topics

1. Name it after its content, in dotted lowercase; do not embed the prefix.
2. Route the name through `kafkaclient.Topic()` (Go) or `topic()` (Python) at **every**
   call site — producer, consumer, and any connector config that carries the name.
3. Route the **consumer group id** through `kafkaclient.Group()` / `kafka_topics.group()`
   at the same call site. It is a separate name with the same failure mode, and the ACL
   an operator grants covers both or neither. Deriving the id from an already-qualified
   topic list is fine — the qualifier is idempotent — but do not hand-concatenate the
   prefix, or the two can drift.
4. Create it explicitly. A produced topic with no creator is a topic that works only
   while auto-create is on.
5. Add a row to §3 with the producer, the consumer and the creator, each cited.
