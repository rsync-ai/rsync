## Temporal Adapter — LLD

### Repo Location
- `backend-temporal-adapter/`

### Entry Point
- `backend-temporal-adapter/cmd/adapter/main.go`

### Key Modules
- `backend-temporal-adapter/internal/workflows`
  - workflow definition: `NLPipelineWorkflowV2`
  - activity implementations:
    - `IntentActivityV2`
    - `ConnectorResolverActivityV2`
    - `ConnectorAvailabilityActivityV2`
    - `GenerateConnectorActivityV2`
    - `ConnectionValidationActivityV2`
    - `PlannerActivityV2`
    - `ValidatorActivityV2`
    - `ExecutorActivityV2`
  - shared activities:
    - `EmitDomainEventActivity` (produces `pipeline.domain.events`), status update writers
- `backend-temporal-adapter/internal/correlation`
  - Redis correlation store the V2 activities write requests to and wait on for replies

The former `internal/adapter` Kafka consumer, the DLQ senders and the `agent.control.*`
topics were removed with the agent Kafka bus in
[#1227](https://github.com/rsync-ai/rsync-ai/pull/1227); the adapter is produce-only.
- `backend-temporal-adapter/internal/db`
  - Postgres connection init used by `StateUpdateActivity`

### Runtime Flow (V2)
1) API Gateway starts Temporal workflow (task queue `pipeline-workflows`)
2) Temporal Adapter executes activities
3) Each stage activity writes its request to the Redis correlation store
4) The matching orchestrator worker polls the store, claims the request and writes its reply there
5) The activity's `WaitForResponse` returns the reply to the workflow
6) StateUpdateActivity persists authoritative pipeline state transitions

### Configuration (Env Vars)
- `TEMPORAL_ADDRESS`
- `KAFKA_BROKERS` (or `KAFKA_BOOTSTRAP_SERVERS` depending on environment)
- `KAFKA_TOPIC_PREFIX` (default `rsync.`) — the topics it produces to
  (`pipeline.domain.events`, `rsync.notifications`) are **not** individually configurable;
  they are resolved in code through `kafkaclient.Topic(s)`, which applies this prefix.
- `DATABASE_URL` or Postgres host/user/pass vars (see `internal/db`)
- `REDIS_ADDRESS` (correlation store)


