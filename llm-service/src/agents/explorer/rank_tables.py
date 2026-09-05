"""LLM-based table ranking endpoint.

Ranks a list of tables by relevance to a user's natural-language intent using
an LLM. Falls back gracefully: if called with no sample data the model uses
column names only, which is cheap and avoids accessing any actual row data.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from openai import AsyncOpenAI
from pydantic import BaseModel

logger = logging.getLogger("llm-rank-tables")

router = APIRouter(tags=["Explorer"])


# ---------------------------------------------------------------------------
# Provider resolution — shared, NOT duplicated
# ---------------------------------------------------------------------------
# This module used to carry its own copies of _env_bool / _ollama_base_url /
# _resolve_provider / _create_async_client "to keep it self-contained", and they
# had drifted into three live defects:
#
#   1. Its _resolve_provider auto-detected from OPENAI_API_KEY and never read
#      LLM_PROVIDER, so `LLM_PROVIDER=ollama` with a stale key in .env still
#      sent table names, column names and row counts to api.openai.com.
#   2. It ignored EXPLORER_OFFLINE_ONLY entirely — the flag whose whole purpose
#      is "schema metadata does not leave this deployment".
#   3. Its default model was the literal "gpt-4o-mini" on every non-Azure
#      provider, so even an operator who found RANK_TABLES_LLM_PROVIDER=ollama
#      asked Ollama for a model it has never pulled.
#
# Self-contained is not worth a silent egress. Import the shared resolver.
# ---------------------------------------------------------------------------

from src.utils.openai_client import (  # noqa: E402
    env_bool as _env_bool,
    make_async_client as _create_async_client,
    rank_tables_default_model as _rank_tables_default_model,
    resolve_explorer_provider as _resolve_explorer_provider,
    resolve_provider as _resolve_provider,
    _ollama_base_url,
)


def _extract_json_array(raw: str) -> str:
    """Extract JSON array from LLM output, handling markdown fences."""
    text = str(raw or "").strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, re.IGNORECASE)
    if m:
        text = m.group(1).strip()
    start = text.find("[")
    end = text.rfind("]")
    if start >= 0 and end > start:
        return text[start : end + 1]
    return "[]"


# ---------------------------------------------------------------------------
# Runtime config
# ---------------------------------------------------------------------------

USE_MOCK = _env_bool("USE_MOCK_LLM", False)
EXPLORER_OFFLINE_ONLY = _env_bool("EXPLORER_OFFLINE_ONLY", False)

# RANK_TABLES_LLM_PROVIDER overrides; unset inherits EXPLORER_LLM_PROVIDER then
# LLM_PROVIDER. EXPLORER_OFFLINE_ONLY beats all of them.
RANK_TABLES_PROVIDER = _resolve_explorer_provider("RANK_TABLES_LLM_PROVIDER")
RANK_TABLES_MODEL = _rank_tables_default_model(RANK_TABLES_PROVIDER)

_client: Optional[AsyncOpenAI] = None if USE_MOCK else _create_async_client(RANK_TABLES_PROVIDER)

# This endpoint sends table names, column names, types and row counts. Say where
# they go — the previous silence is what let it point at OpenAI for months while
# the operator believed EXPLORER_OFFLINE_ONLY covered the whole Explorer.
logger.info(
    "rank-tables llm: provider=%s offline_only=%s model=%s ollama_base=%s",
    RANK_TABLES_PROVIDER,
    EXPLORER_OFFLINE_ONLY,
    RANK_TABLES_MODEL,
    _ollama_base_url() if RANK_TABLES_PROVIDER == "ollama" else "-",
)
if EXPLORER_OFFLINE_ONLY and RANK_TABLES_PROVIDER != "ollama":
    logger.error(
        "EXPLORER_OFFLINE_ONLY=true but rank-tables resolved to provider=%s — "
        "schema metadata WILL leave this deployment",
        RANK_TABLES_PROVIDER,
    )


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class ColumnInfo(BaseModel):
    name: str
    data_type: Optional[str] = None


class TableInfo(BaseModel):
    name: str
    schema_name: Optional[str] = None
    row_count: Optional[int] = None
    columns: Optional[List[ColumnInfo]] = None


class RankTablesRequest(BaseModel):
    tables: List[TableInfo]
    intent: str
    use_case: Optional[str] = "replication"
    max_tables: Optional[int] = 10
    provider: Optional[str] = None


class RankedTable(BaseModel):
    name: str
    schema_name: Optional[str] = None
    row_count: Optional[int] = None
    confidence: float
    reason: str
    category: str
    has_pii: bool


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.post("/agents/rank-tables", response_model=List[RankedTable])
async def rank_tables(request: RankTablesRequest) -> List[RankedTable]:
    """Rank a list of database tables by relevance to the user's intent.

    Uses an LLM to produce semantic rankings with natural-language reasons.
    Sends only table/column names — no row data is transmitted to the model.
    """
    if not request.tables:
        return []

    # `provider` in the request body is a per-call override. Under offline mode
    # it is a caller-controlled egress selector, so refuse it rather than honour
    # it — a request body must not be able to route schema metadata off-box.
    if request.provider and EXPLORER_OFFLINE_ONLY:
        raise HTTPException(
            status_code=400,
            detail=(
                "EXPLORER_OFFLINE_ONLY is enabled; a per-request provider override "
                "is not permitted. Unset it to use the configured offline provider."
            ),
        )

    if request.provider:
        provider = _resolve_provider(request.provider)
        client = _create_async_client(provider)
        # The model has to move with the provider. Keeping the globally-resolved
        # RANK_TABLES_MODEL here would send e.g. "gpt-4o-mini" to Ollama.
        model = _rank_tables_default_model(provider)
    else:
        provider = RANK_TABLES_PROVIDER
        client = _client
        model = RANK_TABLES_MODEL

    # Build a compact table summary for the prompt (no row data)
    table_summaries = []
    for t in request.tables:
        cols = [c.name for c in (t.columns or [])][:10]  # cap at 10 columns
        cols_str = ", ".join(cols) if cols else "(no columns)"
        row_hint = f" ({t.row_count:,} rows)" if t.row_count else ""
        schema_prefix = f"{t.schema_name}." if t.schema_name else ""
        table_summaries.append(f"- {schema_prefix}{t.name}{row_hint}: columns [{cols_str}]")

    tables_text = "\n".join(table_summaries)
    max_tables = min(request.max_tables or 10, len(request.tables))

    prompt = f"""You are a data engineering assistant. Given these database tables:

{tables_text}

User intent: "{request.intent}"
Use case: "{request.use_case or 'replication'}"

Select the top {max_tables} most relevant tables and return a JSON array.
Each element must have exactly these fields:
  - "name": table name (string, no schema prefix)
  - "confidence": float between 0.0 and 1.0
  - "reason": one short sentence explaining why this table is relevant
  - "category": one of "user_data", "transactions", "logs", "analytics", "reference", "general"
  - "has_pii": true if the table likely contains personally-identifiable information

Return ONLY the JSON array, no other text."""

    if USE_MOCK or client is None:
        # Offline fallback: return tables sorted by name with low confidence
        return [
            RankedTable(
                name=t.name,
                schema_name=t.schema_name,
                row_count=t.row_count,
                confidence=0.3,
                reason="Heuristic fallback (LLM unavailable)",
                category="general",
                has_pii=False,
            )
            for t in request.tables[:max_tables]
        ]

    try:
        response = await client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=1024,
        )
        raw = response.choices[0].message.content or "[]"
        ranked_raw: list = json.loads(_extract_json_array(raw))
    except Exception as exc:
        logger.warning("LLM ranking failed: %s", exc)
        raise HTTPException(status_code=503, detail=f"LLM ranking unavailable: {exc}")

    # Build a lookup for original table metadata
    table_meta = {t.name: t for t in request.tables}

    results: List[RankedTable] = []
    for item in ranked_raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", ""))
        meta = table_meta.get(name)
        results.append(
            RankedTable(
                name=name,
                schema_name=meta.schema_name if meta else None,
                row_count=meta.row_count if meta else None,
                confidence=float(item.get("confidence", 0.5)),
                reason=str(item.get("reason", "")),
                category=str(item.get("category", "general")),
                has_pii=bool(item.get("has_pii", False)),
            )
        )

    return results
