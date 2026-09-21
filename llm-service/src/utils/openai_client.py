"""
Shared OpenAI-compatible client factory.

Supports four providers via the LLM_PROVIDER environment variable:
  openai  — OpenAI API (default when OPENAI_API_KEY is set)
  azure   — Azure OpenAI (uses AzureOpenAI/AsyncAzureOpenAI with mandatory api-version)
  groq    — Groq API (groq.com, OpenAI-compatible; GROQ_API_KEY required)
  ollama  — Local Ollama (default when no cloud key is present)

OpenAI wire protocol, key from the VM (Vertex AI):
  OPENAI_API_KEY_SOURCE=gcp-metadata — send a Google OAuth access token from the
      GCE metadata server instead of OPENAI_API_KEY, refreshed before it expires.
      Only used when OPENAI_BASE_URL is an https://*.googleapis.com endpoint.

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

import logging
import os
import re
import threading
from typing import Awaitable, Callable, Union
from urllib.parse import urlparse
from openai import OpenAI, AsyncOpenAI, AzureOpenAI, AsyncAzureOpenAI

logger = logging.getLogger(__name__)

__all__ = [
    "resolve_provider",
    "llm_configured",
    "explorer_llm_configured",
    "get_default_model",
    "env_bool",
    "resolve_explorer_provider",
    "explorer_default_model",
    "explorer_default_sql_model",
    "rank_tables_default_model",
    "make_sync_client",
    "make_async_client",
    "openai_api_key",
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


# Example values this repo's env templates and docs have shipped in credential
# slots. An operator who copied one of those files has not set up an LLM, so these
# count as unset. Compared lower-cased.
_PLACEHOLDER_CREDENTIALS = frozenset({"sk-...", "sk-proj-...", "sk-xxx", "gsk_..."})
# "your-openai-key-here", "YOUR_API_KEY_HERE", "sk-proj-your-openai-key-here".
_PLACEHOLDER_SHAPE = re.compile(r"your[-_ ].*key[-_ ]here", re.IGNORECASE)
# "<azure-key>", "https://<resource>.openai.azure.com": a slot to fill in.
_TEMPLATE_SLOT = re.compile(r"<[^<>]+>")
# Variables already reported, so the warning is logged once per variable per process.
_PLACEHOLDER_WARNED: set = set()

# What a credential variable holds, read the way the client reads it.
_UNSET = "unset"
_EXAMPLE = "example"
_REAL = "real"


def _is_placeholder_credential(value: str, accept_examples: bool = False) -> bool:
    v = (value or "").strip()
    if not v:
        return False
    # A <slot> is never a key, not even a dummy one for a server that ignores keys.
    if _TEMPLATE_SLOT.search(v):
        return True
    if accept_examples:
        return False
    return v.lower() in _PLACEHOLDER_CREDENTIALS or bool(_PLACEHOLDER_SHAPE.search(v))


def _credential(*names: str, accept_examples: bool = False) -> str:
    """
    What the credential the client would send holds: ``_UNSET``, ``_EXAMPLE`` or ``_REAL``.

    ``names`` are read in order and the first one that is set is used, exactly as
    the clients read ``(os.getenv(A) or os.getenv(B) or "").strip()``: a variable
    holding only spaces is still the one sent, and it is sent blank. An example
    value copied from a template or the docs is logged once per variable, by name
    and never by value.

    ``accept_examples`` is for a key sent to an endpoint the operator named
    (OPENAI_BASE_URL): a local server that ignores keys is often given a dummy
    one such as ``sk-xxx``, and only that server can say whether the key works.
    A ``<slot>`` is still an example there.
    """
    for name in names:
        raw = os.getenv(name)
        if not raw:
            continue
        value = raw.strip()
        if not value:
            return _UNSET
        if _is_placeholder_credential(value, accept_examples=accept_examples):
            if name not in _PLACEHOLDER_WARNED:
                _PLACEHOLDER_WARNED.add(name)
                logger.warning(
                    "%s holds an example value copied from a template or the docs, "
                    "not a real one, so no LLM is set up from it. Put the real value "
                    "in .env, or leave it empty to run without an LLM.",
                    name,
                )
            return _EXAMPLE
        return _REAL
    return _UNSET


def _set_credential(*names: str, accept_examples: bool = False) -> bool:
    """True when the credential the client would send holds a real value."""
    return _credential(*names, accept_examples=accept_examples) == _REAL


def _resolve_azure() -> str:
    """
    "azure" when the Azure client could work, else "ollama".

    The client sends AZURE_OPENAI_ENDPOINT and the first set key of
    AZURE_OPENAI_API_KEY / OPENAI_API_KEY. If either one holds an example value
    the call cannot succeed, whatever the other holds. Otherwise a real endpoint
    or a real key is enough, as it always was.
    """
    endpoint = _credential("AZURE_OPENAI_ENDPOINT")
    key = _credential("AZURE_OPENAI_API_KEY", "OPENAI_API_KEY")
    if _EXAMPLE in (endpoint, key):
        return "ollama"
    return "azure" if _REAL in (endpoint, key) else "ollama"


# Provider names resolve_provider knows. Anything else OpenAI-compatible (Vertex AI,
# OpenRouter, LiteLLM) is "openai" with OPENAI_BASE_URL.
_KNOWN_PROVIDERS = ("openai", "azure", "groq", "ollama")
# LLM_PROVIDER values that mean "this install runs without an LLM".
_NO_LLM_CHOICES = frozenset({"none", "disabled", "off", "false", "0"})
# Fallbacks already reported, so each is logged once per process.
_FALLBACK_WARNED: set = set()


def _warn_once(key: str, message: str, *args) -> None:
    if key in _FALLBACK_WARNED:
        return
    _FALLBACK_WARNED.add(key)
    logger.warning(message, *args)


def _warn_fallback(provider: str, needs: str) -> None:
    """A chosen cloud provider is missing its credential, so the answer is "ollama"."""
    _warn_once(
        f"fallback:{provider}",
        "LLM provider %s was chosen but %s is not set to a real value, so this service "
        "resolves to the local Ollama fallback instead. Features that need an LLM report "
        "that none is set up. Put the value in .env and restart.",
        provider,
        needs,
    )


# OPENAI_API_KEY_SOURCE: where an OpenAI-protocol client gets its bearer token.
_KEY_SOURCE_GCP_METADATA = "gcp-metadata"


def _openai_key_source() -> str:
    """"" (the key is OPENAI_API_KEY) or ``gcp-metadata``."""
    raw = (os.getenv("OPENAI_API_KEY_SOURCE") or "").strip().lower()
    if raw in ("", "env"):
        return ""
    if raw == _KEY_SOURCE_GCP_METADATA:
        return raw
    # Not echoed: a key pasted into the wrong variable must not reach the log.
    _warn_once(
        "key-source",
        "OPENAI_API_KEY_SOURCE holds a value this service does not know (the one "
        "supported value is gcp-metadata), so OPENAI_API_KEY is used as the key.",
    )
    return ""


def _base_url_is_google() -> bool:
    """True when OPENAI_BASE_URL is an https endpoint on googleapis.com."""
    parsed = urlparse((os.getenv("OPENAI_BASE_URL") or "").strip())
    host = (parsed.hostname or "").lower()
    return parsed.scheme == "https" and (host == "googleapis.com" or host.endswith(".googleapis.com"))


def _gcp_metadata_key_usable() -> bool:
    """
    Whether OPENAI_API_KEY_SOURCE=gcp-metadata may be used.

    The token it sends is the VM service account's, good for every Google API the
    account can reach. It goes only to a googleapis.com host over https; any other
    OPENAI_BASE_URL (a proxy, a typo, OpenAI itself) gets nothing.
    """
    if _base_url_is_google():
        return True
    _warn_once(
        "key-source-host",
        "OPENAI_API_KEY_SOURCE=gcp-metadata sends this VM's Google service account token, "
        "so it is only used when OPENAI_BASE_URL is an https://*.googleapis.com endpoint "
        "such as Vertex AI. OPENAI_BASE_URL is not one, so no LLM is set up from it.",
    )
    return False


def _openai_key_set() -> bool:
    """True when an OpenAI-protocol client would send a usable bearer token."""
    if _openai_key_source() == _KEY_SOURCE_GCP_METADATA:
        return _gcp_metadata_key_usable()
    return _set_credential("OPENAI_API_KEY", accept_examples=_openai_base_url_is_custom())


_GCP_TOKEN = None
_GCP_TOKEN_LOCK = threading.Lock()


def _gcp_metadata_token():
    """The process-wide token cache, so every client shares one refresh."""
    global _GCP_TOKEN
    with _GCP_TOKEN_LOCK:
        if _GCP_TOKEN is None:
            from src.utils.gcp_access_token import MetadataAccessToken

            _GCP_TOKEN = MetadataAccessToken()
        return _GCP_TOKEN


def openai_api_key(async_client: bool = False) -> Union[str, Callable[[], str], Callable[[], Awaitable[str]]]:
    """
    What an OpenAI-protocol client is given as ``api_key``.

    OPENAI_API_KEY as it stands, or, with OPENAI_API_KEY_SOURCE=gcp-metadata and a
    googleapis.com base URL, a function the SDK calls before every request. A
    Vertex AI access token lasts an hour; a client built once at startup with the
    token as a string sent it until it expired and then failed every call.
    ``async_client`` picks the coroutine the async client needs. Returns "" when
    the metadata source is chosen for a host it may not be sent to.
    """
    if _openai_key_source() == _KEY_SOURCE_GCP_METADATA:
        if not _gcp_metadata_key_usable():
            return ""
        token = _gcp_metadata_token()
        return token.aget if async_client else token.get
    return os.getenv("OPENAI_API_KEY", "")


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

    A credential holding an example value from a template (``sk-...``,
    ``your-openai-key-here``, ``<azure-key>``) counts as empty, so no client is
    pointed at a cloud provider with a key that cannot work. The exception is
    OPENAI_API_KEY when OPENAI_BASE_URL names another endpoint: whatever key the
    operator gave that server is theirs to judge, unless it is a ``<slot>``.
    OPENAI_API_KEY_SOURCE=gcp-metadata stands in for OPENAI_API_KEY on a
    googleapis.com base URL.

    A chosen provider without its credential, and a provider name this function
    does not know, still resolve (to "ollama", and to auto-detect) but are logged
    once each. They used to be silent, so ``LLM_PROVIDER=groq`` with the key never
    reaching the container looked exactly like a working Groq setup.
    """
    p = (explicit or os.getenv("LLM_PROVIDER", "")).strip().lower()

    if p == "azure":
        resolved = _resolve_azure()
        if resolved == "ollama":
            _warn_fallback("azure", "AZURE_OPENAI_ENDPOINT or AZURE_OPENAI_API_KEY")
        return resolved

    if p == "groq":
        if _set_credential("GROQ_API_KEY"):
            return "groq"
        _warn_fallback("groq", "GROQ_API_KEY")
        return "ollama"

    if p == "openai":
        # Fail-open: no key → fall back to offline, and say so.
        if _openai_key_set():
            return "openai"
        _warn_fallback("openai", "OPENAI_API_KEY (or OPENAI_API_KEY_SOURCE)")
        return "ollama"

    if p == "ollama":
        return "ollama"

    if p and p not in _NO_LLM_CHOICES:
        # Not echoed, for the same reason as OPENAI_API_KEY_SOURCE.
        _warn_once(
            "unknown-provider",
            "The LLM provider setting holds a name this service does not know; the known "
            "ones are %s, or none to run without an LLM. For Vertex AI, OpenRouter or any "
            "other OpenAI-compatible endpoint use openai with OPENAI_BASE_URL. Choosing a "
            "provider from the credentials that are set instead.",
            ", ".join(_KNOWN_PROVIDERS),
        )

    # Auto-detect: an Azure endpoint takes priority. One that still holds an
    # example value means Azure was meant but not finished. Falling through to
    # OPENAI_API_KEY would send prompts, and perhaps an Azure key (the Azure
    # client also reads its key from OPENAI_API_KEY), to a different vendor.
    if _credential("AZURE_OPENAI_ENDPOINT") != _UNSET:
        return _resolve_azure()
    if _openai_key_set():
        return "openai"
    # Groq is opt-in only (explicit LLM_PROVIDER=groq); auto-detect never
    # silently routes prompts to Groq — an undisclosed external LLM egress.
    return "ollama"


def llm_configured(explicit: str = "") -> bool:
    """
    Report whether the operator actually set up an LLM.

    ``resolve_provider`` never says "none": with no usable cloud credentials it
    answers "ollama", so a stack with no LLM at all used to send every prompt to
    an Ollama nobody started and fail mid-request. The pipeline intent parser,
    raw SQL in the Data Explorer and the existing connectors need no LLM, so an
    install without one is legitimate; the LLM-only features must say "set up an
    LLM first" instead of timing out against a guess.

    Configured means one of:
      * the choice resolves to a cloud provider whose credentials are present
        (an example value copied from a template is not a credential);
      * the operator explicitly chose Ollama (LLM_PROVIDER=ollama).

    Ollama reached only by fallback is NOT configured. Compose always sends a
    default OLLAMA_URL, so the presence of that variable says nothing about
    whether an Ollama exists. An explicit ``none`` / ``off`` wins over any key
    left in the environment.
    """
    choice = (explicit or os.getenv("LLM_PROVIDER", "")).strip().lower()
    if choice in _NO_LLM_CHOICES:
        return False
    if resolve_provider(choice) != "ollama":
        return True
    return choice == "ollama"


def explorer_llm_configured(override_env: str = "") -> bool:
    """
    ``llm_configured`` for the provider that serves Data Explorer prompts.

    Follows the same precedence as ``resolve_explorer_provider``:
    EXPLORER_OFFLINE_ONLY is an explicit choice of Ollama, then the endpoint's own
    override, then EXPLORER_LLM_PROVIDER, then LLM_PROVIDER.
    """
    if _env_bool("EXPLORER_OFFLINE_ONLY", False):
        return True
    explicit = (os.getenv(override_env) or "").strip() if override_env else ""
    return llm_configured(
        explicit or os.getenv("EXPLORER_LLM_PROVIDER") or os.getenv("LLM_PROVIDER", "")
    )


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

    Offline reads OLLAMA_MODEL for the same reason it refuses LLM_MODEL: the
    name has to be one Ollama actually holds. Refusing LLM_MODEL keeps a cloud
    model name out of the offline path, but the literal it fell back to was no
    likelier to be in the volume — a deployment that pulled one model got a
    working /chat beside an Explorer asking for llama3:latest. OLLAMA_MODEL is
    an Ollama-side name by construction, so honouring it cannot reintroduce
    what the refusal was protecting against.
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
    if provider == "ollama":
        return (os.getenv("OLLAMA_MODEL") or "llama3:latest").strip()
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


def _openai_base_url_is_custom() -> bool:
    """True when OPENAI_BASE_URL points somewhere other than OpenAI's own API.

    An OpenAI-compatible endpoint accepts the same requests but serves a
    different model catalog, so an OpenAI catalog name is not a cheap default
    there — it is a 400. Only used to decide whether a hardcoded catalog literal
    is still meaningful; the client wiring reads the variable directly.
    """
    base = (os.getenv("OPENAI_BASE_URL") or "").strip()
    if not base:
        return False
    host = (urlparse(base).hostname or "").lower()
    return bool(host) and host != "api.openai.com" and not host.endswith(".api.openai.com")


def rank_tables_default_model(provider: str) -> str:
    """
    Default model for the /agents/rank-tables endpoint.

    Deliberately does *not* follow LLM_MODEL on OpenAI: ranking is a bulk
    metadata task deliberately pinned to a cheap model, and inheriting a
    stack-wide upgrade to gpt-4o would silently multiply its cost.

    It does need a real local model offline, though. The old copy returned the
    literal "gpt-4o-mini" for every non-Azure provider, so pointing this
    endpoint at Ollama asked Ollama for a model it has never pulled — and
    "llama3:latest" was the same bet on a different name, which is why the
    offline branch now follows OLLAMA_MODEL down to the model the deployment
    was actually given. RANK_TABLES_MODEL still beats both.

    The cheap pin is scoped to OpenAI's *own* catalog, which is the only place
    the cost argument holds. ``provider == "openai"`` really means "speaks the
    OpenAI wire protocol", and OPENAI_BASE_URL points that protocol at anything
    OpenAI-compatible — Vertex AI, OpenRouter, LiteLLM, a local gateway. On
    those, "gpt-4o-mini" is not a cheap model, it is a name the catalog has
    never heard of, and the endpoint answers HTTP 400 for every request while
    the rest of the stack works, because everything else resolves through
    LLM_MODEL. So when the base URL is not OpenAI's, follow LLM_MODEL like every
    other resolver; RANK_TABLES_MODEL still overrides both.
    """
    explicit = (os.getenv("RANK_TABLES_MODEL") or "").strip()
    if explicit:
        return explicit
    if provider == "ollama":
        return (os.getenv("OLLAMA_MODEL") or "llama3:latest").strip()
    if provider == "groq":
        return "llama-3.3-70b-versatile"
    if provider == "azure":
        # On Azure the model argument is a *deployment* name; falling through to
        # an OpenAI catalog name is a 404 DeploymentNotFound.
        return (os.getenv("AZURE_OPENAI_DEPLOYMENT") or os.getenv("LLM_MODEL") or "gpt-4o-mini").strip()
    if _openai_base_url_is_custom():
        override = (os.getenv("LLM_MODEL") or "").strip()
        if override:
            return override
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
        "api_key": openai_api_key(),
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
    kwargs: dict = {"api_key": openai_api_key(async_client=True), "timeout": timeout}
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
