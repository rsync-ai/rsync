from __future__ import annotations

import json
import logging
import os
import re

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict

from src.utils.prompt_registry import PromptRegistry
from src.utils.openai_client import (
    env_bool as _env_bool,
    resolve_explorer_provider as _resolve_explorer_provider,
    explorer_default_model as _explorer_default_model,
    # Imported, not reimplemented. The private copies this router used to carry
    # accepted LLM_PROVIDER=groq with no key check and had no groq branch to
    # build, so an explicit groq silently became an OpenAI client.
    make_async_client as _create_async_client,
    client_egress_host as _client_egress_host,
)

logger = logging.getLogger("llm-explorer")

router = APIRouter(tags=["Explorer"])


def _extract_json_text(raw: str) -> str:
    """Best-effort extraction of a JSON object from model output."""
    if raw is None:
        return "{}"
    text = str(raw).strip()
    # Prefer fenced JSON blocks
    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
    if m:
        text = m.group(1).strip()
    # Fall back to first {...} span
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return text[start : end + 1].strip()
    return text


# ------------------------------------------------------------------------------
# Runtime config (kept local to Explorer)
# ------------------------------------------------------------------------------

# Default should be FALSE; this is mainly for tests.
USE_MOCK = _env_bool("USE_MOCK_LLM", False)

# Operational guardrail (mirrors gateway/main.py).
if USE_MOCK:
    env = (
        (os.getenv("ENVIRONMENT") or os.getenv("APP_ENV") or os.getenv("ENV") or os.getenv("NODE_ENV") or "")
        .strip()
        .lower()
    )
    allowed = {"development", "dev", "test", "testing", "local", "docker"}
    if env not in allowed:
        env_display = env or "<unset>"
        raise RuntimeError(
            f"USE_MOCK_LLM=true is not allowed when ENVIRONMENT={env_display!r}. "
            "Set USE_MOCK_LLM=false (recommended) or run with ENVIRONMENT=development/test."
        )

# Explorer offline mode.
# Default is now False — Explorer uses the same LLM_PROVIDER as the rest of the stack
# (Azure OpenAI in production). Set EXPLORER_OFFLINE_ONLY=true to force Ollama for air-gapped
# or on-prem deployments where schema metadata must not leave the system.
EXPLORER_OFFLINE_ONLY = _env_bool("EXPLORER_OFFLINE_ONLY", False)
# Provider + model defaults come from src.utils.openai_client, shared with the
# gateway (src/gateway/main.py). This router previously resolved both itself and
# ignored LLM_MODEL, so on Azure its three endpoints sent the literal
# "gpt-4o-mini" as a *deployment* name — 404 DeploymentNotFound — while the rest
# of Explorer, resolving in main.py, used the configured deployment and worked.
EXPLORER_LLM_PROVIDER = _resolve_explorer_provider()

explorer_client = None if USE_MOCK else _create_async_client(EXPLORER_LLM_PROVIDER)

_default_explorer_model = _explorer_default_model(EXPLORER_LLM_PROVIDER)
EXPLORER_TABLE_LINK_MODEL = (os.getenv("EXPLORER_TABLE_LINK_MODEL") or _default_explorer_model).strip()
EXPLORER_COLUMN_LINK_MODEL = (os.getenv("EXPLORER_COLUMN_LINK_MODEL") or _default_explorer_model).strip()
EXPLORER_NEXT_STEPS_MODEL = (os.getenv("EXPLORER_NEXT_STEPS_MODEL") or _default_explorer_model).strip()

# `egress=` is read off the constructed client, so this line reports where
# prompts go rather than restating what LLM_PROVIDER said. See
# client_egress_host in src/utils/openai_client.py for why that matters.
logger.info(
    "explorer router llm: provider=%s offline_only=%s table_link_model=%s "
    "column_link_model=%s next_steps_model=%s egress=%s",
    EXPLORER_LLM_PROVIDER,
    EXPLORER_OFFLINE_ONLY,
    EXPLORER_TABLE_LINK_MODEL,
    EXPLORER_COLUMN_LINK_MODEL,
    EXPLORER_NEXT_STEPS_MODEL,
    _client_egress_host(explorer_client),
)

registry = PromptRegistry(prompts_dir="prompts")


# ------------------------------------------------------------------------------
# Models
# ------------------------------------------------------------------------------


class TableCandidate(BaseModel):
    """A candidate table with confidence score"""

    table: str
    # Schema must come from the user's source connection config, not a
    # hardcoded "public" default — many production deployments use other
    # schemas (analytics, tenant_*, app_*) and silent "public" fallback
    # makes queries against those schemas return empty.
    schema_name: str | None = None
    confidence: float
    reason: str


class TableLinkRequest(BaseModel):
    """Request to link question to tables"""

    question: str
    tables: list[dict]  # List of table metadata
    previous_context: str | None = None
    conversation_id: str | None = None


class TableLinkResponse(BaseModel):
    """Response with table candidates"""

    candidates: list[TableCandidate]
    confidence_overall: float
    needs_hitl: bool
    hitl_reason: str | None = None
    suggested_join_keys: list[str] | None = None


class ColumnMapping(BaseModel):
    """Column mapping for query parts"""

    select_cols: list[str] = []
    where_cols: list[str] = []
    group_by_cols: list[str] = []
    order_by_cols: list[str] = []


class JoinPlan(BaseModel):
    """Join plan between tables"""

    join_type: str = "INNER"
    left_table: str
    right_table: str
    condition: str


class ColumnLinkRequest(BaseModel):
    """Request to link question to columns"""

    question: str
    selected_tables: list[dict]  # Tables selected from table_link
    previous_context: str | None = None
    conversation_id: str | None = None


class ColumnLinkResponse(BaseModel):
    """Response with column mapping and join plan"""

    columns: ColumnMapping
    join_plan: list[JoinPlan]
    confidence: float
    needs_hitl: bool
    hitl_reason: str | None = None
    ambiguous_columns: list[str] | None = None


class NextStepSuggestion(BaseModel):
    """A suggested next action"""

    action_type: str  # metabase, download_csv, slack, email
    title: str
    description: str
    confidence: float = 0.8
    required_inputs: list[str] = []
    cta: str = "Execute"


class ResultProfile(BaseModel):
    """Metadata-only profile of a query result.

    Privacy contract (CLAUDE.md "LLM data privacy"): query RESULT ROWS never
    reach the LLM — only this profile does. extra="forbid" is the enforcement:
    a caller that attaches sample rows / result data fails validation (422)
    instead of silently leaking. Mirrors the Go producer struct
    (api-gateway/internal/cache/explorer_cache.go ResultProfile).
    """

    model_config = ConfigDict(extra="forbid")

    row_count: int = 0
    columns: list[str] = []
    min_timestamp: str | None = None
    max_timestamp: str | None = None


class NextStepsRequest(BaseModel):
    """Request for next step suggestions"""

    question: str
    sql: str
    result_profile: ResultProfile
    available_actions: list[str] = ["metabase", "download_csv", "slack", "email"]


class NextStepsResponse(BaseModel):
    """Response with suggested next actions"""

    suggestions: list[NextStepSuggestion]


# ------------------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------------------


@router.post("/api/v1/explorer/nl/resolve-tables", response_model=TableLinkResponse)
async def resolve_tables(request: TableLinkRequest):
    """Resolve natural language question to relevant database tables."""
    try:
        logger.info("📊 Table link request: %s...", (request.question or "")[:100])

        # Explorer is offline-only.
        if EXPLORER_OFFLINE_ONLY and USE_MOCK:
            return _mock_table_link(request)
        if explorer_client is None:
            raise HTTPException(status_code=503, detail="Explorer LLM client unavailable (offline-only)")

        prompt_name = "explorer/table_link"
        variables = {
            "question": request.question,
            "tables": (request.tables or [])[:30],
            "previous_context": request.previous_context,
        }
        messages = registry.render_messages(prompt_name, variables)
        cfg = registry.get_config(prompt_name)

        response = await explorer_client.chat.completions.create(
            model=EXPLORER_TABLE_LINK_MODEL,
            messages=messages,
            temperature=cfg["parameters"].get("temperature", 0.1),
            max_tokens=cfg["parameters"].get("max_tokens", 1024),
        )

        result_text = (response.choices[0].message.content or "").strip()
        result = json.loads(_extract_json_text(result_text))

        # Accept both 'schema' and 'schema_name' from models.
        parsed_candidates = []
        for c in (result.get("candidates", []) or []):
            if not isinstance(c, dict):
                continue
            if "schema_name" not in c and "schema" in c:
                c = {**c, "schema_name": c.get("schema")}
            parsed_candidates.append(TableCandidate(**c))

        return TableLinkResponse(
            candidates=parsed_candidates,
            confidence_overall=float(result.get("confidence_overall", 0.5) or 0.5),
            needs_hitl=bool(result.get("needs_hitl", False)),
            hitl_reason=result.get("hitl_reason"),
            suggested_join_keys=result.get("suggested_join_keys"),
        )
    except Exception as e:
        logger.exception("Table link failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


def _mock_table_link(request: TableLinkRequest) -> TableLinkResponse:
    """Mock table linking for testing (intentionally minimal)."""
    fallback = []
    for t in (request.tables or [])[:5]:
        fallback.append(
            TableCandidate(
                table=t.get("name", ""),
                # Mock fallback: use whatever the caller provided (typically
                # the source connection's schema). If absent, leave None and
                # let the caller resolve it from connection config.
                schema_name=t.get("schema") or t.get("schema_name"),
                confidence=0.35,
                reason="Mock mode: please confirm the right table(s)",
            )
        )
    return TableLinkResponse(
        candidates=fallback,
        confidence_overall=0.35 if fallback else 0.0,
        needs_hitl=True,
        hitl_reason="Mock mode enabled — need human confirmation",
        suggested_join_keys=None,
    )


@router.post("/api/v1/explorer/nl/resolve-columns", response_model=ColumnLinkResponse)
async def resolve_columns(request: ColumnLinkRequest):
    """Resolve natural language question to specific columns and join plan."""
    try:
        logger.info("📊 Column link request: %s...", (request.question or "")[:100])

        # Explorer is offline-only.
        if EXPLORER_OFFLINE_ONLY and USE_MOCK:
            return _mock_column_link(request)
        if explorer_client is None:
            raise HTTPException(status_code=503, detail="Explorer LLM client unavailable (offline-only)")

        prompt_name = "explorer/column_link"
        variables = {
            "question": request.question,
            "selected_tables": request.selected_tables,
            "previous_context": request.previous_context,
        }
        messages = registry.render_messages(prompt_name, variables)
        cfg = registry.get_config(prompt_name)

        response = await explorer_client.chat.completions.create(
            model=EXPLORER_COLUMN_LINK_MODEL,
            messages=messages,
            temperature=cfg["parameters"].get("temperature", 0.1),
            max_tokens=cfg["parameters"].get("max_tokens", 1024),
        )

        result_text = (response.choices[0].message.content or "").strip()
        result = json.loads(_extract_json_text(result_text))

        cols = result.get("columns", {})
        return ColumnLinkResponse(
            columns=ColumnMapping(
                select_cols=cols.get("select_cols", cols.get("select", [])),
                where_cols=cols.get("where_cols", cols.get("where", [])),
                group_by_cols=cols.get("group_by_cols", cols.get("group_by", [])),
                order_by_cols=cols.get("order_by_cols", cols.get("order_by", [])),
            ),
            join_plan=[
                JoinPlan(
                    join_type=j.get("join_type", j.get("type", "INNER")),
                    left_table=j.get("left_table", j.get("left", "")),
                    right_table=j.get("right_table", j.get("right", "")),
                    condition=j.get("condition", ""),
                )
                for j in result.get("join_plan", [])
            ],
            confidence=result.get("confidence", 0.5),
            needs_hitl=result.get("needs_hitl", False),
            hitl_reason=result.get("hitl_reason"),
            ambiguous_columns=result.get("ambiguous_columns"),
        )
    except Exception as e:
        logger.exception("Column link failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


def _mock_column_link(request: ColumnLinkRequest) -> ColumnLinkResponse:
    """Mock column linking for testing."""
    select_cols = []
    for t in request.selected_tables:
        table_name = t.get("name", "")
        for col in t.get("columns", [])[:5]:
            select_cols.append(f"{table_name}.{col.get('name', '')}")
    return ColumnLinkResponse(
        columns=ColumnMapping(
            select_cols=select_cols[:10],
            where_cols=[],
            group_by_cols=[],
            order_by_cols=[],
        ),
        join_plan=[],
        confidence=0.7,
        needs_hitl=len(request.selected_tables) > 1,
        hitl_reason="Multiple tables - please confirm join" if len(request.selected_tables) > 1 else None,
        ambiguous_columns=None,
    )


@router.post("/api/v1/explorer/nl/next-steps", response_model=NextStepsResponse)
async def get_next_steps(request: NextStepsRequest):
    """Suggest next actions based on query results."""
    try:
        logger.info("💡 Next steps request for: %s...", (request.question or "")[:50])

        # Explorer is offline-only.
        if EXPLORER_OFFLINE_ONLY and USE_MOCK:
            return _mock_next_steps(request)
        if explorer_client is None:
            raise HTTPException(status_code=503, detail="Explorer LLM client unavailable (offline-only)")

        prompt_name = "explorer/next_steps"
        variables = {
            "question": request.question,
            "sql": request.sql,
            "result_profile": request.result_profile.model_dump(),
        }
        messages = registry.render_messages(prompt_name, variables)
        cfg = registry.get_config(prompt_name)

        response = await explorer_client.chat.completions.create(
            model=EXPLORER_NEXT_STEPS_MODEL,
            messages=messages,
            temperature=cfg["parameters"].get("temperature", 0.3),
            max_tokens=cfg["parameters"].get("max_tokens", 512),
        )

        result_text = (response.choices[0].message.content or "").strip()
        result = json.loads(_extract_json_text(result_text))

        suggestions = []
        for s in (result.get("suggestions", []) or []):
            if not isinstance(s, dict):
                continue
            # Accept both 'type' (prompt spec) and 'action_type' (API model).
            if "action_type" not in s and "type" in s:
                s = {**s, "action_type": s.get("type")}
            suggestions.append(NextStepSuggestion(**s))

        return NextStepsResponse(suggestions=suggestions)
    except Exception as e:
        logger.exception("Next steps failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


def _mock_next_steps(request: NextStepsRequest) -> NextStepsResponse:
    """Mock next steps for testing."""
    suggestions = []
    row_count = request.result_profile.row_count

    # Always suggest Metabase for visualization
    suggestions.append(
        NextStepSuggestion(
            action_type="metabase",
            title="Create Dashboard",
            description="Visualize these results in Metabase",
            confidence=0.9,
            required_inputs=["dashboard_name"],
            cta="Create Dashboard",
        )
    )

    # Suggest download for larger results
    if row_count > 10:
        suggestions.append(
            NextStepSuggestion(
                action_type="download_csv",
                title="Download CSV",
                description=f"Export {row_count} rows to CSV",
                confidence=0.8,
                required_inputs=[],
                cta="Download",
            )
        )

    # Suggest sharing
    suggestions.append(
        NextStepSuggestion(
            action_type="slack",
            title="Share to Slack",
            description="Send results to a Slack channel",
            confidence=0.6,
            required_inputs=["channel"],
            cta="Share",
        )
    )

    return NextStepsResponse(suggestions=suggestions[:3])

