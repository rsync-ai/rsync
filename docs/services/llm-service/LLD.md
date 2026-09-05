## LLM Service — LLD

### Repo Location
- `llm-service/`

### Entry Point
- `llm-service/src/gateway/main.py`
  - started via `python -m src.gateway.main` (see `llm-service/Dockerfile`)

### Core Concepts
- **Prompt registry**: `src.utils.prompt_registry.PromptRegistry`
- **Knowledge helpers**: `src.utils.chat_knowledge.ChatKnowledge`
- **Telemetry**: `src.utils.telemetry` instruments FastAPI + HTTP calls and correlates logs

### Intent Fallback
When LLM is not available, the gateway contains deterministic/mock logic:
- `mock_intent_classification()` produces a JSON payload used by upstream callers.

### Suggestion API
- Router: `src.agents.suggestions.api`
- Used by UX features (e.g., suggestions shown in chat/connector selection flows).

### Configuration (Env Vars)
- `PORT` (default 5000)
- LLM provider vars (OpenAI/Ollama) as used by `src.utils.llm_client` / OpenAI client
- `OTEL_*`, `LOG_FORMAT`, `LOG_LEVEL`


