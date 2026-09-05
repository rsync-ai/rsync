## LLM Service — HLD

### Purpose
The LLM Service is an **internal FastAPI gateway** that provides:
- LLM-backed reasoning/intent helpers,
- prompt/knowledge utilities,
- suggestion endpoints used by other agents/services.

It is not directly exposed to the browser in docker-compose; other services call it over the docker network.

### Runtime Interface
- **Container**: `rsync-ai-llm-service`
- **Internal port**: `5000`
- **Host port**: none (internal only)
- **Entrypoint**: `python -m src.gateway.main`

### Responsibilities
- Provide LLM-backed endpoints (with safe fallbacks when LLM is unavailable).
- Centralize telemetry + prompt registry usage for agent services.
- Serve suggestion APIs (`src.agents.suggestions.api`) used by UX features.

### Dependencies
- LLM provider:
  - OpenAI (if configured) or local Ollama (if configured)
- Observability:
  - OpenTelemetry tracing + JSON logs

### Scaling / HA Notes
Stateless; can scale horizontally if:
- rate limits and LLM provider concurrency are managed,
- any in-memory caches are either disabled or made distributed.

### Failure Modes
- LLM provider unavailable ⇒ service can fall back to deterministic/mock logic (degraded quality).


