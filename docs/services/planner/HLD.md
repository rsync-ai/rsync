## Planner — HLD

### Purpose
The Planner converts **natural language** into an executable **pipeline plan**.

It also supports **JIT connector generation** (trigger tool-generator when a required connector is missing) and CDC planning helpers.

### Runtime Interface
- **Container**: `rsync-ai-planner`
- **Internal port**: `5011`
- **Host port**: `5011` (dev)
- **Entrypoint**: `python -m src.agents.planner.service`

### Responsibilities
- **Plan generation**:
  - uses an LLM planning strategy with a deterministic fallback strategy.
- **Tool registry awareness**:
  - uses a dynamic tool registry (reads installed connectors via tool-generator / shared dir).
- **JIT integration**:
  - can trigger connector generation when the user requests a tool not installed.
- **CDC support**:
  - generates Debezium connector configuration and strategy decisions.

### Dependencies
- **LLM Service**: `LLM_GATEWAY_URL`
- **Tool Generator**: `TOOL_GENERATOR_URL`
- **API Gateway**: `API_GATEWAY_URL`
- **Kafka**: optional consumer/producer path (feature-flagged)
- **Shared volume**: read-only connector existence checks

### Observability
- OpenTelemetry tracing + trace-context propagation to downstream services.


