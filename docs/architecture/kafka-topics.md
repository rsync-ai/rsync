# Kafka topics

Descriptive catalogue of every Kafka topic the platform names, and — the part that
matters for a BYO-Kafka deployment — **who creates each one**. Every claim below is
cited to a `file:line`; if you change a name, change the citation with it.

> This document was rewritten 2026-08-16. The previous version described an
> `agent.{name}.requests` fan-out with per-agent DLQs that the code has not used for
> a long time: 14 of the names it documented had zero producers and zero consumers
> across all five services. Don't restore it from git history.

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
  yields `acmetask.results` on one side and `acme.task.results` on the other — both
  legal topic names, so the split surfaces only as a consumer that receives nothing.
- **Idempotent** — `Topic("rsync.cdc.abc12345")` is unchanged, never
  `rsync.rsync.cdc.abc12345`. Topic names are persisted (`pipelines.kafka_topic`,
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
platform owns ([topology.go:185](../../backend-orchestrator/internal/handlers/topology.go:185)).
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
themselves ([internal/kafka/manager.go:281](../../backend-orchestrator/internal/kafka/manager.go),
[:356](../../backend-orchestrator/internal/kafka/manager.go)). That is why ~60 orchestrator
call sites pass a bare `"pipeline.agent.telemetry"` and still land in the namespace. It
works because `Topic()` is idempotent, so a name already qualified by `generateTopicName`
passes through unchanged.

## 2. Who creates what

Four mechanisms create topics. Nothing else does.

Every programmatic creation in the orchestrator funnels through one function —
`TopologyManager.ensureTopicLocked` — which is the invariant
[`topology_single_creator_test.go`](../../backend-orchestrator/internal/kafka/topology_single_creator_test.go)
exists to hold. That is what makes the replication-factor and `min.insync.replicas`
clamping unavoidable rather than opt-in: a second creator would be a second place for a
topic to be born with a floor above its replication factor, which produces a topic that
is created, listed, subscribable and permanently unwritable.

### 2a. `TopologyManager.EnsureAgentControlTopics` — the startup creator

[backend-orchestrator/internal/kafka/topology.go:337](../../backend-orchestrator/internal/kafka/topology.go),
called once at orchestrator startup
([cmd/orchestrator/main.go:489](../../backend-orchestrator/cmd/orchestrator/main.go)).
Creates 25 topics through `EnsureTopic` → `kafkaclient.Topic()`, so they are correctly
prefixed:

- `agent.control.commands.{intent,resolver,discovery,planner,validator,executor,cost_estimator,capability_resolver,connection_validator}` — 9 topics, `retention.ms=86400000`, `compression.type=snappy`
- `agent.control.results`
- `agent.failed.dlq`
- `agent.executor.responses`, `pipeline.domain.events`, `pipeline.agent.telemetry`
- `rsync.notifications`, `rsync.healer.{actions,results,approved-changes,schema-changes}`, `rsync.agents.heartbeat`, `rsync.sentinel.audit`
- `agent.planner.responses`, `pii.scan.{request,response}`, `pipeline.failed.dlq`

Everything after the first eleven carries `KeepExistingPartitions: true`: those topics
hold KEYED records, and widening a live topic re-hashes keys onto other partitions, so
creating one and re-sizing an existing one are deliberately different decisions. The
retention values are not free choices either — where `kafka-init` or the quickstart
compose also creates the topic, the value here matches it verbatim, because neither
creator ALTERs an existing topic and whichever runs first on a given deployment wins
permanently.

Partitions come from `KAFKA_AGENT_TOPIC_PARTITIONS` (default 3, clamped);
replication factor is `min(3, brokerCount)` when the cluster has more than one broker,
else 1. **Partition count is the point** — auto-created topics get 1 partition, and a
1-partition command topic means only one orchestrator replica ever receives work.

Failure is logged at **Warn** and startup continues
(`"⚠️  Failed to ensure agent control topics (scaling may be limited)"`). On a broker
with `auto.create.topics.enable=false` that warning is the whole control plane failing
open — see §4.

`TopologyManager.CreateTopicForPipeline` (topology.go:334) exists and would create
per-pipeline topics with tuned partition counts, but its only caller is the HTTP route
`POST /api/v1/topology/topics/pipeline`
([internal/handlers/topology.go:146](../../backend-orchestrator/internal/handlers/topology.go)),
which **no service, job or frontend in this repo calls.** It is an operator endpoint, not
part of the pipeline flow.

### 2b. `kafka-init` — the compose bootstrapper

One-shot container in [docker-compose.yml](../../docker-compose.yml) (`kafka-init`),
running [scripts/kafka-init-new-topics.sh](../../scripts/kafka-init-new-topics.sh) after
the broker passes its healthcheck. Creates 4:

| Topic | Retention |
|---|---|
| `task.assignments` | broker default |
| `task.results` | broker default |
| `pipeline.domain.events` | `-1` (infinite — canonical event log) |
| `pipeline.agent.telemetry` | 7 days |

[docker-compose.quickstart.yml](../../docker-compose.quickstart.yml) has its own inline
variant creating 12 (the 9 control commands + `agent.planner.responses` +
`pipeline.domain.events` + `pii.scan.response`). It overlaps §2a harmlessly —
`--if-not-exists`.

### 2c. `scripts/create_kafka_topics.sh` — dev setup only

Invoked by [scripts/setup.sh:79](../../scripts/setup.sh); not part of any compose file,
so **it never runs on a deployed stack.** Creates 8: `agent.planner.requests`,
`agent.executor.requests`, and the six `agent.{intent,resolver,discovery,planner,validator,executor}.responses`.

### 2d. `Manager.EnsureTopicExists` — runtime pre-creation on the pipeline path

The topics above are known at startup. The data-plane topics are not: their names carry a
pipeline id or a connector name, so they can only be created when a pipeline runs.
[`Manager.EnsureTopicExists`](../../backend-orchestrator/internal/kafka/manager.go) (:1260)
and its config-carrying sibling `EnsureTopicExistsWithConfig` (:1277) are that path. Both
delegate to `TopologyManager.EnsureTopic` with `NameIsAuthoritative` and
`KeepExistingPartitions` set, so the name the caller chose is created verbatim — these
names are owned by Debezium and by the sink's subscription config, and re-qualifying one
would create a topic nobody reads — while the replication clamping still applies.

| Call site | Topic | Partitions | Config |
|---|---|---|---|
| [executor.go:3987](../../backend-orchestrator/internal/agents/executor/executor.go) | `pipeline.<id8>.data` | 1 | — |
| [executor.go:6314](../../backend-orchestrator/internal/agents/executor/executor.go) | `cdc.<id8>` / `cdc-<id>.<db>.<table>` | 3 | — |
| [executor.go:3416](../../backend-orchestrator/internal/agents/executor/executor.go) | `schemahistory.<connector>` | 1 | `cleanup.policy=delete`, `retention.ms=-1` |
| [cdc_incremental.go:322](../../backend-orchestrator/internal/agents/executor/cdc_incremental.go) | `<connector>.signals` | 1 | — |

All four are **best-effort**: a failure is logged at Warn and the run continues, so a
broker that does auto-create behaves exactly as it did before. That is deliberate — the
pre-creation removes a dependency, it does not add a new way to fail a pipeline.

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

### Control plane

| Topic | Producer | Consumer | Created by |
|---|---|---|---|
| `agent.control.commands.<agent>` (×9) | orchestrator dispatch | orchestrator agent workers — list pinned in [sentinel/health_monitor.go:248](../../backend-orchestrator/internal/agents/sentinel/health_monitor.go) and guarded by `consumed_topics_test.go` | §2a |
| `agent.control.results` | agent workers | Temporal adapter (V1 signal path) | §2a |
| `task.assignments` | **none** | **none** — named only in [sentinel/healer.go:294](../../backend-orchestrator/internal/agents/sentinel/healer.go) | §2b |
| `task.results` | agent workers | **none** — the WebSocket bridge stopped subscribing when the producer-less subscriptions were pruned | §2b |

`task.assignments` has no producer and no consumer anywhere. It is created because the
healer's `newArchTopics` list expects it to exist; deleting it would make the healer
report a missing topic.

### Events and telemetry

| Topic | Producer | Consumer | Created by |
|---|---|---|---|
| `pipeline.domain.events` | orchestrator workers | api-gateway WebSocket bridge; projector | §2a, §2b |
| `pipeline.agent.telemetry` | `workers/planner.go:512`, `intent.go:615`, `resolver.go:314` | bridge (debug mode) | §2a, §2b |

### Agent responses

[api-gateway/internal/websocket/kafka_bridge.go:83](../../api-gateway/internal/websocket/kafka_bridge.go)
subscribes to 4 topics, each of which has a real in-repo producer and is created by §2a:

`pipeline.domain.events` · `pipeline.agent.telemetry` · `agent.planner.responses` ·
`agent.executor.responses`

It used to subscribe to 15. The other 11 —
`agent.{intent,resolver,discovery,validator}.responses` ·
`agent.{resolver,orchestrator,discovery,planner}.progress` ·
`pipeline.status.updates` · `cdc.status.updates` · `task.results` — had **no producer
anywhere in the repo**, and the bridge's own comment marked the `agent.*` set
`LEGACY: … will be deprecated`. They are gone.

That was not cosmetic. A subscription is a topic dependency: the bridge opens one consumer
group per topic, and on a broker with `auto.create.topics.enable=true` a *fetch* against a
non-existent topic creates it. Eleven topics were therefore being brought into existence by
the act of watching for messages that nothing ever sent — which both hid the auto-create
dependency (the topics existed, so nothing looked wrong) and made the grant a customer's
Kafka operator has to write eleven entries longer than it needed to be. The remaining group
ids are pinned by
[`kafka_bridge_group_test.go`](../../api-gateway/internal/websocket/kafka_bridge_group_test.go).

### Data plane

| Topic | Shape | Producer | Consumer | Created by |
|---|---|---|---|---|
| `pipeline.<id8>.data` | batch row chunks / MinIO claim-check URLs | [executor.go:2439](../../backend-orchestrator/internal/agents/executor/executor.go) | `kafka-mcp-sink` | §2d |
| `cdc.<id8>` | Debezium envelopes | Debezium via Kafka Connect | `kafka-mcp-sink` | §2d |
| `cdc-<id>.<db>.<table>` | Debezium per-table stream | Debezium | `kafka-mcp-sink` | §2d |
| `schemahistory.<connector>` | Debezium source-DDL history | Kafka Connect | Kafka Connect (on connector restart) | §2d |
| the CDC signal topic | incremental-snapshot signals | [cdc_incremental.go:327](../../backend-orchestrator/internal/agents/executor/cdc_incremental.go) | Debezium | §2d |
| `pipeline.failed.dlq` | dead-lettered pipeline messages | [activities.go:169](../../backend-temporal-adapter/internal/workflows/activities.go) | operator | §2a |
| `agent.failed.dlq` | dead-lettered agent activities | [activities.go:200](../../backend-temporal-adapter/internal/workflows/activities.go) | operator | §2a |
| `pii.scan.request` | scan job | [handlers/pii.go:300](../../api-gateway/internal/handlers/pii.go) | [llm-service pii_scanner/kafka_consumer.py:18](../../llm-service/src/agents/pii_scanner/kafka_consumer.py) | §2a |
| `pii.scan.response` | scan result | llm-service | api-gateway ([cmd/server/main.go:437](../../api-gateway/cmd/server/main.go)) | §2a, §2b (quickstart) |

The naming rule for the per-pipeline topic is
[topology.go:400 `generateTopicName`](../../backend-orchestrator/internal/kafka/topology.go):
`cdc`/`streaming` → `cdc.<id8>`, everything else → `pipeline.<id8>.data`, where `<id8>` is
the first 8 characters of the pipeline UUID.

## 4. What this means for `auto.create.topics.enable=false`

**Status: the code no longer depends on auto-creation; the setting has not been flipped.**

This section used to read: the entire data plane and most of the response plane exist only
because the broker creates topics on first produce or first fetch, so turning
`auto.create.topics.enable` off breaks batch transfer, CDC, the DLQs and the PII scanner,
with a pipeline that transfers zero rows as the only visible symptom. That was accurate,
and it mattered because the setting is one this platform does **not own** on a
customer-managed cluster — several managed offerings ship it off, and a customer may
simply have turned it off.

Every topic in §3 now has a named creator: §2a provisions the whole control, event,
notification and healer set at startup, and §2d pre-creates the per-pipeline data topics
when a pipeline runs. The producer-less bridge subscriptions that were creating eleven
topics by fetching from them are gone. The remaining hole is a documented one:
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
