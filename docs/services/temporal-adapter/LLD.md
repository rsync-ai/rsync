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
    - `EmitDomainEventActivity`, DLQ senders, status update writers
- `backend-temporal-adapter/internal/adapter`
  - Kafka consumer that reads agent results and signals Temporal workflows
- `backend-temporal-adapter/internal/db`
  - Postgres connection init used by `StateUpdateActivity`

### Runtime Flow (V2)
1) API Gateway starts Temporal workflow (task queue `pipeline-workflows`)
2) Temporal Adapter executes activities
3) Activities publish work requests to Kafka control topic(s)
4) Agents execute and publish results to Kafka results topic
5) Adapter consumes results and signals the workflow (completes activity futures)
6) StateUpdateActivity persists authoritative pipeline state transitions

### Configuration (Env Vars)
- `TEMPORAL_ADDRESS`
- `KAFKA_BROKERS` (or `KAFKA_BOOTSTRAP_SERVERS` depending on environment)
- `KAFKA_TOPIC_PREFIX` (default `rsync.`) — the control topics are **not** individually
  configurable; `agent.control.commands` / `agent.control.results` are resolved in code through
  `kafkaclient.Topic(s)`, which applies this prefix.
- `DATABASE_URL` or Postgres host/user/pass vars (see `internal/db`)
- `REDIS_ADDRESS` (correlation store)


