"""
The one answer every LLM-only route gives when no LLM is set up.

An install without an LLM is supported: pipeline intents are parsed by a
deterministic matcher, raw SQL runs in the Data Explorer, and existing connectors
need no model. Only the features that genuinely call a model — natural-language
SQL, table/column linking, diagnosis, suggestions, connector generation — need
one, and they must say so up front rather than send the prompt to an Ollama the
operator never started and time out.

The body is the same everywhere so the api-gateway and the UI can recognise it:

    503 {"error": "llm_not_configured", "message": "Set up an LLM first: ..."}

Routes that raise ``LLMNotConfigured`` without the handler registered still
return 503 with the same payload nested under ``detail``; the gateway accepts
both shapes.
"""

import os

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from src.utils.openai_client import (
    env_bool,
    explorer_llm_configured,
    llm_configured,
)

__all__ = [
    "LLM_NOT_CONFIGURED",
    "LLM_NOT_CONFIGURED_MESSAGE",
    "LLMNotConfigured",
    "llm_not_configured_body",
    "require_llm",
    "require_explorer_llm",
    "require_sql_llm",
    "sql_llm_configured",
    "llm_not_configured_handler",
    "register_llm_gate",
]

LLM_NOT_CONFIGURED = "llm_not_configured"
LLM_NOT_CONFIGURED_MESSAGE = (
    "Set up an LLM first: add OPENAI_API_KEY (or another provider's key) to .env, "
    "or set LLM_PROVIDER=ollama for a local model, then restart rsync."
)


def llm_not_configured_body() -> dict:
    return {"error": LLM_NOT_CONFIGURED, "message": LLM_NOT_CONFIGURED_MESSAGE}


class LLMNotConfigured(HTTPException):
    def __init__(self) -> None:
        super().__init__(status_code=503, detail=llm_not_configured_body())


def _mock_llm() -> bool:
    # USE_MOCK_LLM stubs every model call (dev/test only; the gateway refuses it
    # elsewhere), so there is nothing to set up.
    return env_bool("USE_MOCK_LLM", False)


def require_llm() -> None:
    """Raise LLMNotConfigured unless the stack's default LLM is set up."""
    if not _mock_llm() and not llm_configured():
        raise LLMNotConfigured()


def require_explorer_llm(override_env: str = "") -> None:
    """Raise LLMNotConfigured unless the Data Explorer's LLM is set up."""
    if not _mock_llm() and not explorer_llm_configured(override_env):
        raise LLMNotConfigured()


def sql_llm_configured() -> bool:
    """Whether the provider behind /api/v1/sql/generate is set up.

    Mirrors the gateway's sql_client selection: EXPLORER_SQL_PROVIDER when set,
    unless offline-only mode pins Ollama; otherwise the Explorer's provider.
    """
    offline_only = env_bool("EXPLORER_OFFLINE_ONLY", False)
    allow_online = env_bool("EXPLORER_SQL_ALLOW_ONLINE", not offline_only)
    if offline_only and not allow_online:
        return True
    sql_provider = (os.getenv("EXPLORER_SQL_PROVIDER") or "").strip()
    if sql_provider:
        return llm_configured(sql_provider)
    return explorer_llm_configured()


def require_sql_llm() -> None:
    if not _mock_llm() and not sql_llm_configured():
        raise LLMNotConfigured()


async def llm_not_configured_handler(_request: Request, _exc: LLMNotConfigured) -> JSONResponse:
    return JSONResponse(status_code=503, content=llm_not_configured_body())


def register_llm_gate(app) -> None:
    """Serve LLMNotConfigured as the flat body instead of FastAPI's ``detail`` wrapper."""
    app.add_exception_handler(LLMNotConfigured, llm_not_configured_handler)
