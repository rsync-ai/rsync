## Temporal Adapter — HLD

### Purpose
The Temporal Adapter is the **bridge between Temporal workflows and Kafka-based agents**.

It:
- runs the **Temporal worker** for the pipeline workflow(s),
- registers and executes workflow activities (V2),
- emits commands to Kafka for agents to execute,
- consumes agent results and signals the correct workflow run,
- writes authoritative pipeline state updates (via DB activity).

### Runtime Interface
- **Container**: `rsync-ai-temporal-adapter`
- **Internal port**: `8082` (primarily for health/debug; core is Temporal worker)

### Responsibilities
- **Workflow execution**
  - registers `NLPipelineWorkflowV2` and activities for intent → resolver → planner → validator → executor.
- **Kafka bridge**
  - activities publish to `agent.control.commands`
  - adapter consumes `agent.control.results` and signals workflows.
- **State update**
  - `StateUpdateActivity` writes authoritative pipeline transitions into Postgres (best-effort gating).
- **Correlation store (V2)**
  - initializes Redis-based correlation store for request/reply activities.

### Dependencies
- **Temporal**: server at `temporal:7233`
- **Kafka**: control topics
- **Postgres**: state update activity
- **Redis**: V2 correlation store

### Scaling / HA Notes
- Runs as a Temporal worker for a task queue (default: `pipeline-workflows`).
- Scaling requires understanding Temporal worker concurrency and idempotency of activities.

### Observability
- Logs are JSON formatted (docker/fluent-bit ingestion).
- Tracing is configured at compose level (OTEL envs) where enabled.


