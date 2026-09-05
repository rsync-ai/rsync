"""
Shared OpenAI-compatible client factory.

Supports four providers via the LLM_PROVIDER environment variable:
  openai  — OpenAI API (default when OPENAI_API_KEY is set)
  azure   — Azure OpenAI (uses AzureOpenAI/AsyncAzureOpenAI with mandatory api-version)
  groq    — Groq API (groq.com, OpenAI-compatible; GROQ_API_KEY required)
  ollama  — Local Ollama (default when no cloud key is present)

Azure OpenAI environment variables:
  AZURE_OPENAI_ENDPOINT    — e.g. https://myresource.openai.azure.com
  AZURE_OPENAI_API_KEY     — Azure API key (falls back to OPENAI_API_KEY)
  AZURE_OPENAI_API_VERSION — default 2024-10-21 (latest GA)
  AZURE_OPENAI_DEPLOYMENT  — deployment name (falls back to LLM_MODEL)

Model selection priority:
  1. LLM_MODEL env var (overrides everything)
  2. Provider default: gpt-4o-mini (openai/azure), llama-3.3-70b-versatile (groq),
     OLLAMA_MODEL or qwen2.5:7b (ollama)

Usage:
    from src.utils.openai_client import make_sync_client, make_async_client, resolve_provider, get_default_model

    provider = resolve_provider()
    client   = make_sync_client()           # honours LLM_PROVIDER env var
    model    = get_default_model(provider)  # honours LLM_MODEL env var
"""

import os
from urllib.parse import urlparse
from openai import OpenAI, AsyncOpenAI, AzureOpenAI, AsyncAzureOpenAI

__all__ = [
    "resolve_provider",
    "get_default_model",
    "env_bool",
    "resolve_explorer_provider",
    "explorer_default_model",
    "explorer_default_sql_model",
    "rank_tables_default_model",
    "make_sync_client",
    "make_async_client",
    "client_egress_host",
    "_ollama_base_url",
]


def env_bool(name: str, default: bool) -> bool:
    """Read a boolean env var, treating an *empty* value as unset.

    Compose passes optional vars as ``${VAR:-}``, which reaches the container as
    an empty string rather than as an absent name. The naive form —
    ``os.getenv(name, "true" if default else "false")`` — returns ``""`` in that
    case and reads it as False, silently flipping every flag whose default is
    True. Empty must mean "operator said nothing", so the default stands.
    """
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


# Back-compat alias for the private name used before this moved into the shared module.
_env_bool = env_bool


def _ollama_base_url() -> str:
    """Return the Ollama OpenAI-compatible base URL."""
    url = (
        os.getenv("OLLAMA_BASE_URL") or os.getenv("OLLAMA_URL") or "http://host.docker.internal:11434"
    ).strip()
    if not url:
        url = "http://host.docker.internal:11434"
    if not url.rstrip("/").endswith("/v1"):
        url = url.rstrip("/") + "/v1"
    return url


def resolve_provider(explicit: str = "") -> str:
    """
    Resolve which LLM provider to use.

    Resolution order:
      1. ``explicit`` argument (if non-empty and valid)
      2. LLM_PROVIDER env var
      3. Auto-detect from available env vars:
         AZURE_OPENAI_ENDPOINT → "azure", else OPENAI_API_KEY → "openai",
         else "ollama". Groq is NEVER auto-selected — it is opt-in only via an
         explicit LLM_PROVIDER=groq, so a stray GROQ_API_KEY cannot silently
         route prompts to an undisclosed external LLM.
    """
    p = (explicit or os.getenv("LLM_PROVIDER", "")).strip().lower()

    if p == "azure":
        # Valid if endpoint or any key is set
        has_endpoint = bool((os.getenv("AZURE_OPENAI_ENDPOINT") or "").strip())
        has_key = bool((os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip())
        return "azure" if (has_endpoint or has_key) else "ollama"

    if p == "groq":
        return "groq" if (os.getenv("GROQ_API_KEY") or "").strip() else "ollama"

    if p == "openai":
        # Fail-open: no key → fall back to offline
        return "openai" if (os.getenv("OPENAI_API_KEY") or "").strip() else "ollama"

    if p == "ollama":
        return "ollama"

    # Auto-detect: Azure endpoint present takes priority
    if (os.getenv("AZURE_OPENAI_ENDPOINT") or "").strip():
        return "azure"
    if (os.getenv("OPENAI_API_KEY") or "").strip():
        return "openai"
    # Groq is opt-in only (explicit LLM_PROVIDER=groq); auto-detect never
    # silently routes prompts to Groq — an undisclosed external LLM egress.
    return "ollama"


def get_default_model(provider: str) -> str:
    """
    Return the default model name for the resolved provider.

    Always checks LLM_MODEL env var first so operators can override
    without touching code.
    """
    env_override = (os.getenv("LLM_MODEL") or "").strip()
    if env_override:
        return env_override

    if provider == "ollama":
        return (os.getenv("OLLAMA_MODEL") or "qwen2.5:7b").strip()
    if provider == "groq":
        return "llama-3.3-70b-versatile"
    if provider == "azure":
        return (os.getenv("AZURE_OPENAI_DEPLOYMENT") or "gpt-4o-mini").strip()
    # openai
    return "gpt-4o-mini"


def resolve_explorer_provider(override_env: str = "") -> str:
    """
    Resolve the provider that serves Data Explorer prompts.

    Precedence: EXPLORER_OFFLINE_ONLY pins Ollama unconditionally, then the
    endpoint's own override (``override_env``, e.g. RANK_TABLES_LLM_PROVIDER),
    then EXPLORER_LLM_PROVIDER, then the stack's LLM_PROVIDER.

    This is the single source of truth. Every Explorer entry point used to carry
    its own copy of this rule and they disagreed — so "the Explorer is offline"
    could be true of one endpoint and false of the next. Note in particular that
    an endpoint-specific override must still *inherit* when it is unset: the old
    private copies auto-detected from OPENAI_API_KEY instead, which meant
    LLM_PROVIDER=ollama did not move them offline while a key was in the env.
    """
    if _env_bool("EXPLORER_OFFLINE_ONLY", False):
        return "ollama"
    explicit = (os.getenv(override_env) or "").strip() if override_env else ""
    return resolve_provider(
        explicit or os.getenv("EXPLORER_LLM_PROVIDER") or os.getenv("LLM_PROVIDER", "")
    )


def explorer_default_model(provider: str) -> str:
    """
    Default chat model for Explorer prompts on ``provider``.

    LLM_MODEL wins for the cloud providers. On Azure the model argument is a
    *deployment* name, so a hardcoded "gpt-4o-mini" returns 404
    DeploymentNotFound unless a deployment happens to carry that name — the
    rest of the stack already runs on LLM_MODEL, so Explorer must too.
    """
    if provider in ("openai", "azure"):
        override = (os.getenv("LLM_MODEL") or "").strip()
        if override:
            return override
        if provider == "azure":
            deployment = (os.getenv("AZURE_OPENAI_DEPLOYMENT") or "").strip()
            if deployment:
                return deployment
        return "gpt-4o-mini"
    if provider == "groq":
        return "llama-3.3-70b-versatile"
    return "llama3:latest"


def explorer_default_sql_model(provider: str) -> str:
    """
    Default text-to-SQL model for Explorer on ``provider``.

    Offline gets a SQL-specialised model; the cloud providers reuse the chat
    default because their general models already outperform sqlcoder-7b.
    """
    if provider == "ollama":
        return (os.getenv("OLLAMA_MODEL") or "sqlcoder:latest").strip()
    return explorer_default_model(provider)


def rank_tables_default_model(provider: str) -> str:
    """
    Default model for the /agents/rank-tables endpoint.

    Deliberately does *not* follow LLM_MODEL on OpenAI: ranking is a bulk
    metadata task deliberately pinned to a cheap model, and inheriting a
    stack-wide upgrade to gpt-4o would silently multiply its cost.

    It does need a real local model offline, though. The old copy returned the
    literal "gpt-4o-mini" for every non-Azure provider, so pointing this
    endpoint at Ollama asked Ollama for a model it has never pulled.
    """
    explicit = (os.getenv("RANK_TABLES_MODEL") or "").strip()
    if explicit:
        return explicit
    if provider == "ollama":
        return "llama3:latest"
    if provider == "groq":
        return "llama-3.3-70b-versatile"
    if provider == "azure":
        # On Azure the model argument is a *deployment* name; falling through to
        # an OpenAI catalog name is a 404 DeploymentNotFound.
        return (os.getenv("AZURE_OPENAI_DEPLOYMENT") or os.getenv("LLM_MODEL") or "gpt-4o-mini").strip()
    return "gpt-4o-mini"


def make_sync_client(provider: str = "", timeout: float = 45.0, max_retries: int = 1) -> OpenAI:
    """
    Create a synchronous OpenAI-compatible client.

    Args:
        provider: explicit provider override; if empty, resolves from env
        timeout:  request timeout in seconds (default 45s to fit gateway windows)
        max_retries: SDK-level retry count
    """
    p = resolve_provider(provider)
    if p == "ollama":
        return OpenAI(
            base_url=_ollama_base_url(),
            api_key="ollama",
            timeout=timeout,
            max_retries=max_retries,
        )
    if p == "groq":
        return OpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=os.getenv("GROQ_API_KEY", ""),
            timeout=timeout,
            max_retries=max_retries,
        )
    if p == "azure":
        endpoint = (os.getenv("AZURE_OPENAI_ENDPOINT") or "").strip()
        api_key = (os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()
        api_version = (os.getenv("AZURE_OPENAI_API_VERSION") or "2024-10-21").strip()
        return AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version=api_version,
            timeout=timeout,
            max_retries=max_retries,
        )
    # openai
    kwargs: dict = {
        "api_key": os.getenv("OPENAI_API_KEY", ""),
        "timeout": timeout,
        "max_retries": max_retries,
    }
    _base = (os.getenv("OPENAI_BASE_URL") or "").strip()
    if _base:
        kwargs["base_url"] = _base
    return OpenAI(**kwargs)


def make_async_client(provider: str = "", timeout: float = 120.0) -> AsyncOpenAI:
    """
    Create an asynchronous OpenAI-compatible client.

    Args:
        provider: explicit provider override; if empty, resolves from env
        timeout:  request timeout in seconds, applied to EVERY branch.

    The timeout is not optional decoration. The OpenAI SDK default is 600s
    (10 minutes) on read/write/pool, so a single stalled call parks a Uvicorn
    worker for ten minutes. That was known — the openai branch carried an
    explicit 120s and said so in a comment — but the ollama and groq branches
    did not, which is the usual shape of this bug: a fix applied to one arm of
    a switch. It is a parameter now so callers that need a different window ask
    for one instead of building their own client.
    """
    p = resolve_provider(provider)
    if p == "ollama":
        return AsyncOpenAI(base_url=_ollama_base_url(), api_key="ollama", timeout=timeout)
    if p == "groq":
        return AsyncOpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=os.getenv("GROQ_API_KEY", ""),
            timeout=timeout,
        )
    if p == "azure":
        endpoint = (os.getenv("AZURE_OPENAI_ENDPOINT") or "").strip()
        api_key = (os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()
        api_version = (os.getenv("AZURE_OPENAI_API_VERSION") or "2024-10-21").strip()
        return AsyncAzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version=api_version,
            timeout=timeout,
        )
    # openai
    kwargs: dict = {"api_key": os.getenv("OPENAI_API_KEY", ""), "timeout": timeout}
    _base = (os.getenv("OPENAI_BASE_URL") or "").strip()
    if _base:
        kwargs["base_url"] = _base
    return AsyncOpenAI(**kwargs)


def client_egress_host(client) -> str:
    """Host the CONSTRUCTED client will actually talk to — for startup logs.

    Read off the client object, never off the config string. A log that echoes
    the resolved provider name can only ever confirm what the config said: when
    ``LLM_PROVIDER=groq`` built an OpenAI client (the private copies in
    src/gateway/main.py and src/agents/explorer/api.py had no groq branch), the
    startup line still read ``provider=groq`` and an operator checking where
    prompts went was told the wrong answer by the line added to tell them.

    Azure is reported as a marker rather than its host: the endpoint carries the
    customer's resource name, and these log lines promise no endpoints.
    """
    if client is None:
        return "-"
    host = urlparse(str(getattr(client, "base_url", "") or "")).hostname or "-"
    return "<azure-endpoint>" if host.endswith(".openai.azure.com") else host
