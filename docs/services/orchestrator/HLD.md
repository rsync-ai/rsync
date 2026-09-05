## Backend Orchestrator — HLD

### Purpose
The Orchestrator is the **execution brain** of rsync-ai. It:
- runs pipeline execution logic (Temporal-driven V2, Kafka messaging),
- validates connections/configs and routes MCP tool calls,
- manages agent workers, retries, and operational safety checks.

### Runtime Interface
- **Container**: `rsync-ai-orchestrator`
- **Internal port**: `8080`
- **Host port**: `8081` (debug/dev)

### Responsibilities
- **Pipeline execution** (agentic):
  - executes plan steps via MCP connector client(s),
  - publishes and consumes pipeline/agent events via Kafka,
  - coordinates with Temporal (workflows run in Temporal; Orchestrator provides services used by activities and runtime lookups).
- **Connection/config validation**:
  - validates against **version-specific** connector metadata/spec.
- **Connector runtime management**:
  - reads `shared/mcp-connectors` for connector metadata and routing,
  - can call tool-generator for self-healing deploy/start when connector containers are missing (best-effort).
- **Operational agents**:
  - retention, sentinel, schedulers.

### Dependencies
- **Postgres**: pipeline metadata, connection configs, status tables
- **Kafka**: agent command/results, domain events, CDC topics
- **Redis**: correlation store (V2 request/reply)
- **Shared volume**: `shared/mcp-connectors` (connector artifacts)
- **Tool Generator**: self-healing deployment URL (`TOOL_GENERATOR_URL`)

### Scaling / HA Notes (Important)
- Current docker-compose explicitly warns **do not scale** Orchestrator replicas > 1.
  - Reason: static membership / topic-per-agent and consumer fencing risks.
  - Scaling requires topic sharding and routing changes (see compose comments).

### Observability
- OpenTelemetry tracing + JSON logs (fluent-bit ingestion).
- Prometheus metrics endpoint exists (see LLD for exact path).

### Failure Modes
- **Kafka issues**: agent messaging delays; pipelines may stall/retry.
- **Connector container missing**: executor failures unless self-healing can redeploy.
- **Redis correlation store down**: V2 activities degrade/fail; may fall back depending on workflow.


