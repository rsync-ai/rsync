"""Application-level Prometheus metrics for llm-service.

SigNoz-native observability (F-Obs-2, Obs-LLMService): each FastAPI app
mounts a pull-only ``/prometheus`` ASGI endpoint that the OTEL Collector's
``rsync-llm-*`` scrape jobs read and forward to SigNoz. No separate
Prometheus/Grafana stack.

Why ``/prometheus`` and not ``/metrics``: gateway/service.py already serve
business ``/metrics/*`` JSON endpoints; mounting the Prometheus ASGI app at
``/metrics`` would shadow them. Keep them disjoint.

Cardinality is bounded by design:
  - ``service``  = gateway | tool-generator | planner (fixed set of 3).
  - ``provider`` = openai | azure | ollama | groq | ... (small fixed set).
  - ``model``    = model name (bounded — the handful of models we call).
  - ``result``   = success | error.
  - ``kind``     = prompt | completion (token direction).

NO free-form labels (prompt text, user id, request id). Per-call drill-down
lives in traces/logs in SigNoz, correlated by trace id.

Double-counting note: only DIRECT provider calls are recorded here — the
gateway's OpenAI clients and tool-generator's BaseAgent. The planner and
api-gateway reach LLMs *through* the gateway over HTTP, so they are already
counted at the gateway and are deliberately NOT instrumented at the client.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from prometheus_client import Counter, Histogram, make_asgi_app

from src.utils.llm_cost import compute_cost_cents

logger = logging.getLogger("llm-metrics")

# --- Collectors -------------------------------------------------------------

LLM_CALLS_TOTAL = Counter(
    "rsync_llm_calls_total",
    "LLM provider calls by service + provider + model + result",
    ["service", "provider", "model", "result"],
)

LLM_CALL_DURATION_SECONDS = Histogram(
    "rsync_llm_call_duration_seconds",
    "Wall-clock latency of LLM provider calls",
    ["service", "provider", "model"],
    buckets=(0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120),
)

LLM_TOKENS_TOTAL = Counter(
    "rsync_llm_tokens_total",
    "LLM tokens consumed by service + provider + model + direction",
    ["service", "provider", "model", "kind"],  # kind = prompt|completion
)

LLM_COST_CENTS_TOTAL = Counter(
    "rsync_llm_cost_cents_total",
    "Estimated LLM spend in USD cents (list prices, see llm_cost._PRICE_TABLE)",
    ["service", "provider", "model"],
)


def record_llm_call(
    *,
    service: str,
    provider: str,
    model: str,
    result: str,
    duration_s: float,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
) -> None:
    """Record one LLM provider call. Never raises — metrics must not break
    the request path."""
    try:
        provider = provider or "unknown"
        model = model or "unknown"
        LLM_CALLS_TOTAL.labels(service, provider, model, result).inc()
        LLM_CALL_DURATION_SECONDS.labels(service, provider, model).observe(duration_s)
        if prompt_tokens:
            LLM_TOKENS_TOTAL.labels(service, provider, model, "prompt").inc(prompt_tokens)
        if completion_tokens:
            LLM_TOKENS_TOTAL.labels(service, provider, model, "completion").inc(completion_tokens)
        if prompt_tokens or completion_tokens:
            cents = compute_cost_cents(provider, model, prompt_tokens, completion_tokens)
            if cents:
                LLM_COST_CENTS_TOTAL.labels(service, provider, model).inc(cents)
    except Exception as e:  # pragma: no cover - defensive
        logger.debug("record_llm_call failed (ignored): %s", e)


def _usage_tokens(response) -> tuple[int, int]:
    """Best-effort (prompt_tokens, completion_tokens) from an OpenAI response."""
    usage = getattr(response, "usage", None)
    if not usage:
        return 0, 0
    return (
        int(getattr(usage, "prompt_tokens", 0) or 0),
        int(getattr(usage, "completion_tokens", 0) or 0),
    )


def instrument_openai_client(client, *, service: str, provider: str):
    """Wrap an (Async)OpenAI client's chat/completions ``create`` methods so
    every call is recorded — DRY across all scattered call sites. Idempotent:
    re-wrapping a client is a no-op. Returns the client for chaining.

    Behaviour is preserved: the wrapper times the call, records metrics, and
    returns / re-raises exactly what the original returned / raised.
    """
    if client is None:
        return client
    if getattr(client, "_rsync_metrics_wrapped", False):
        return client

    for resource_name in ("chat", None):
        resource = getattr(client, "chat").completions if resource_name == "chat" else getattr(client, "completions", None)
        if resource is None or not hasattr(resource, "create"):
            continue
        orig = resource.create

        def _make_wrapper(orig_create):
            async def _wrapped(*args, **kwargs):
                model = kwargs.get("model") or "unknown"
                start = time.monotonic()
                try:
                    resp = await orig_create(*args, **kwargs)
                except Exception:
                    record_llm_call(
                        service=service, provider=provider, model=model,
                        result="error", duration_s=time.monotonic() - start,
                    )
                    raise
                pt, ct = _usage_tokens(resp)
                record_llm_call(
                    service=service, provider=provider, model=model,
                    result="success", duration_s=time.monotonic() - start,
                    prompt_tokens=pt, completion_tokens=ct,
                )
                return resp
            return _wrapped

        try:
            resource.create = _make_wrapper(orig)
        except Exception as e:  # pragma: no cover - some SDK objects may be frozen
            logger.warning("could not instrument %s.create: %s", resource_name or "completions", e)

    try:
        client._rsync_metrics_wrapped = True
    except Exception:
        pass
    return client


def mount_metrics(app) -> None:
    """Mount the pull-only Prometheus ASGI endpoint at ``/prometheus``."""
    app.mount("/prometheus", make_asgi_app())
