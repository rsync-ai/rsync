## Temporal Adapter — HLD

### Purpose
The Temporal Adapter is the **bridge between Temporal workflows and the orchestrator's agent workers**, which it reaches through the Redis correlation store.

It:
- runs the **Temporal worker** for the pipeline workflow(s),
- registers and executes workflow activities (V2),
- writes each stage's request to the Redis correlation store and waits there for the worker's reply,
- produces pipeline domain events and failure alerts to Kafka (it runs no Kafka consumer),
- writes authoritative pipeline state updates (via DB activity).

### Runtime Interface
- **Container**: `rsync-ai-temporal-adapter`
- **Internal port**: `8082` (primarily for health/debug; core is Temporal worker)

### Responsibilities
- **Workflow execution**
  - registers `NLPipelineWorkflowV2` and activities for intent → connector resolver → connection validator → planner → validator → cost estimator → executor.
- **Agent hand-off (Redis)**
  - each stage activity writes its request to the Redis correlation store and waits for the worker's reply there.
  - the adapter runs no Kafka consumer; the `agent.control.*` bridge was removed in [#1227](https://github.com/rsync-ai/rsync-ai/pull/1227).
- **Kafka producer**
  - publishes `pipeline.domain.events` and failure alerts to `rsync.notifications`.
- **State update**
  - `StateUpdateActivity` writes authoritative pipeline transitions into Postgres (best-effort gating).
- **Correlation store (V2)**
  - initializes Redis-based correlation store for request/reply activities.

### Dependencies
- **Temporal**: server at `temporal:7233`
- **Kafka**: producer only (`pipeline.domain.events`, `rsync.notifications`)
- **Postgres**: state update activity
- **Redis**: V2 correlation store

### Scaling / HA Notes
- Runs as a Temporal worker for a task queue (default: `pipeline-workflows`).
- Scaling requires understanding Temporal worker concurrency and idempotency of activities.

### Observability
- Logs are JSON formatted (docker/fluent-bit ingestion).
- Tracing is configured at compose level (OTEL envs) where enabled.


