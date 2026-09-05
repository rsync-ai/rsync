## Planner — LLD

### Repo Location
- `llm-service/src/agents/planner/`

### Entry Point
- `llm-service/src/agents/planner/service.py`

### Core Modules
- `llm-service/src/agents/planner/strategies/`
  - `LLMPlanningStrategy`: primary planning
  - `HeuristicPlanningStrategy`: deterministic fallback
  - `CompositePlanningStrategy`: orchestrates primary→fallback
  - `ToolRegistry`: installed connector awareness
  - `AsyncConnectorChecker`: JIT generation triggers (non-blocking patterns)
- `llm-service/src/agents/planner/cdc_config_generator.py`
  - Debezium configuration generation
- `llm-service/src/utils/llm_client.py`
  - shared LLM client wrapper
- `llm-service/src/utils/telemetry.py`
  - tracing + context injection

### Main HTTP Endpoints (typical)
Planner exposes endpoints for:
- creating a plan from natural language
- generating CDC connector configs
- recommending pipeline strategy based on table/data stats

Exact route paths are defined in `service.py` near the FastAPI router section.

### Configuration (Env Vars)
- `PORT`
- `TOOL_GENERATOR_URL`
- `API_GATEWAY_URL`
- `LLM_GATEWAY_URL`
- `CONNECTORS_DIR`
- `KAFKA_BOOTSTRAP_SERVERS`
- `ENABLE_KAFKA_CONSUMER` (feature flag)
- `OTEL_*`, `LOG_FORMAT`, `LOG_LEVEL`


