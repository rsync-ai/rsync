"""
LLM cost tracking — token → USD cents conversion + Postgres logging.

Used by gateway/main.py to write one llm_usage_events row per /v1/completion
call. Callers also include the resolved cost in the response so the api-gateway
can enforce per-user quotas BEFORE expensive subsequent work (pipeline runs).

Pricing snapshot below is for May 2026 list prices. Update when contracts change.
Cost is computed as integer cents (rounded up) to avoid float drift across
hundreds of small calls.
"""
from __future__ import annotations

import logging
import math
import os
import threading
from typing import Optional

logger = logging.getLogger("llm-cost")

# (provider, model_lowercase) -> (input_per_1m_tokens_cents, output_per_1m_tokens_cents)
# All prices are in USD cents per 1M tokens.
_PRICE_TABLE = {
    # OpenAI (also Azure OpenAI — same list prices via Azure)
    ("openai", "gpt-4o"):                 (500, 1500),
    ("openai", "gpt-4o-2024-08-06"):      (500, 1500),
    ("openai", "gpt-4o-mini"):            (15, 60),
    ("openai", "gpt-4o-mini-2024-07-18"): (15, 60),
    ("openai", "gpt-4-turbo"):            (1000, 3000),
    ("openai", "gpt-4"):                  (3000, 6000),
    ("openai", "gpt-3.5-turbo"):          (50, 150),
    # Groq
    ("groq", "llama-3.3-70b-versatile"):  (59, 79),
    ("groq", "llama-3.1-8b-instant"):     (5, 8),
    ("groq", "mixtral-8x7b-32768"):       (24, 24),
    # Ollama / self-hosted = free (compute cost is amortised at the infra layer)
}

_DEFAULT_PER_1M_INPUT_CENTS  = 100  # conservative fallback if model unknown
_DEFAULT_PER_1M_OUTPUT_CENTS = 300


def compute_cost_cents(provider: str, model: str, prompt_tokens: int, completion_tokens: int) -> int:
    """Return integer USD cents (rounded up) for the given token counts."""
    if (provider or "").lower() == "ollama":
        return 0
    key = ((provider or "").lower(), (model or "").lower())
    if key in _PRICE_TABLE:
        in_rate, out_rate = _PRICE_TABLE[key]
    else:
        # Same model name across openai/azure-openai is common — try cross-lookup
        alt = ("openai", (model or "").lower())
        in_rate, out_rate = _PRICE_TABLE.get(alt, (_DEFAULT_PER_1M_INPUT_CENTS, _DEFAULT_PER_1M_OUTPUT_CENTS))
    cost = (prompt_tokens * in_rate + completion_tokens * out_rate) / 1_000_000.0
    return max(1, math.ceil(cost)) if cost > 0 else 0


# ---------------------------------------------------------------------------
# Async DB writer — fire-and-forget so the request path is not slowed by Postgres.
# A single background thread drains a bounded queue. If the queue is full we
# drop with a warning rather than blocking the LLM call.
# ---------------------------------------------------------------------------
import queue

_event_queue: "queue.Queue[dict]" = queue.Queue(maxsize=10_000)
_writer_started = False
_writer_lock = threading.Lock()


def _get_dsn() -> Optional[str]:
    # Mirror api-gateway / orchestrator DSN env vars
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN")
    if dsn:
        return dsn
    # Fall back to discrete POSTGRES_* vars. Without an explicit DSN AND with no
    # password, the assembled URL is unusable — psycopg2 fails every write with
    # "fe_sendauth: no password supplied", once per LLM call. Return None instead
    # so the writer disables cost logging cleanly with a single warning rather
    # than spamming the logs. (Cost tracking is best-effort; it must never become
    # a noise source when the DB env isn't wired.)
    host = os.getenv("POSTGRES_HOST", "postgres")
    user = os.getenv("POSTGRES_USER", "user")
    pw   = os.getenv("POSTGRES_PASSWORD", "")
    db   = os.getenv("POSTGRES_DB",   "pipeline_db")
    port = os.getenv("POSTGRES_PORT", "5432")
    if not pw:
        return None
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


def _writer_loop() -> None:
    try:
        import psycopg2  # type: ignore
    except ImportError:
        logger.warning("psycopg2 not installed; LLM cost logging disabled")
        return
    dsn = _get_dsn()
    if not dsn:
        logger.warning("No DATABASE_URL; LLM cost logging disabled")
        return
    conn = None
    while True:
        evt = _event_queue.get()
        if evt is None:  # shutdown sentinel
            break
        try:
            if conn is None or conn.closed:
                conn = psycopg2.connect(dsn)
                conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO llm_usage_events
                       (user_id, pipeline_id, prompt_name, provider, model,
                        prompt_tokens, completion_tokens, total_tokens, cost_usd_cents)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        evt.get("user_id") or None,
                        evt.get("pipeline_id") or None,
                        evt["prompt_name"],
                        evt["provider"],
                        evt["model"],
                        evt["prompt_tokens"],
                        evt["completion_tokens"],
                        evt["total_tokens"],
                        evt["cost_usd_cents"],
                    ),
                )
                # Also atomically charge the user's monthly counter so the
                # quota check at api-gateway sees an accurate running total.
                if evt.get("user_id"):
                    cur.execute(
                        "SELECT check_and_charge_user(%s::uuid, %s)",
                        (evt["user_id"], evt["cost_usd_cents"]),
                    )
        except Exception as exc:
            logger.warning("llm cost write failed: %s", exc)
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            conn = None


def _ensure_writer() -> None:
    global _writer_started
    if _writer_started:
        return
    with _writer_lock:
        if _writer_started:
            return
        t = threading.Thread(target=_writer_loop, name="llm-cost-writer", daemon=True)
        t.start()
        _writer_started = True


def record_usage(
    *,
    user_id: Optional[str],
    pipeline_id: Optional[str],
    prompt_name: str,
    provider: str,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
) -> int:
    """
    Compute cost in cents, enqueue for async DB write, and return cents so the
    caller can include it in the response. Never raises.
    """
    try:
        total = prompt_tokens + completion_tokens
        cents = compute_cost_cents(provider, model, prompt_tokens, completion_tokens)
        _ensure_writer()
        try:
            _event_queue.put_nowait({
                "user_id":          user_id,
                "pipeline_id":      pipeline_id,
                "prompt_name":      prompt_name,
                "provider":         provider,
                "model":            model,
                "prompt_tokens":    prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens":     total,
                "cost_usd_cents":   cents,
            })
        except queue.Full:
            logger.warning("llm cost queue full; dropping event for %s", prompt_name)
        return cents
    except Exception as exc:
        logger.warning("record_usage failed (non-fatal): %s", exc)
        return 0
