"""
API endpoints for Data Suggestions service
"""

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import logging

from .service import generate_suggestions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/agents/suggestions", tags=["Suggestions"])


class SuggestionsRequest(BaseModel):
    """Request schema for suggestions generation.

    `data_schema` is the inbound key under-the-hood; we expose it via the
    alias `schema` (which is what the frontend and the api-gateway proxy send),
    but avoid the Pydantic v2 BaseModel.schema() shadow warning at import time.
    """
    data_schema: Dict[str, Any] = Field(..., alias="schema")
    intent: Dict[str, Any] = Field(default_factory=dict)
    discovery_result: Optional[Dict[str, Any]] = None

    model_config = {"populate_by_name": True}


class SuggestionsResponse(BaseModel):
    """Response schema for suggestions"""
    success: bool
    pii_detected: bool
    pii_columns: List[Dict[str, Any]]
    transforms: List[Dict[str, Any]]
    optimizations: List[Dict[str, Any]]
    total_suggestions: int
    # Machine-readable error code so the frontend modal can choose a recovery
    # path (retry vs. continue-without-transforms vs. show a config error).
    # Values: "ok" | "invalid_schema" | "llm_unavailable" | "llm_timeout"
    #       | "llm_invalid_json" | "token_budget" | "execution_timeout"
    #       | "internal_error"
    error_code: str = "ok"
    # Human-readable error/info text suitable for direct display in the modal.
    error: Optional[str] = None
    # True when we returned heuristics-only results because an enrichment
    # path (ML PII, LLM transforms) failed but the response is still useful.
    degraded: bool = False


@router.post("/generate", response_model=SuggestionsResponse)
async def generate_data_suggestions(request: SuggestionsRequest):
    """
    Generate AI-powered data suggestions

    This endpoint uses LangGraph to analyze schema and provide:
    - PII detection
    - Transform suggestions
    - Optimization recommendations

    Error handling philosophy: NEVER raise a 5xx for partial/recoverable
    failures. The frontend modal must be able to show actionable text and let
    the user continue (with degraded suggestions) or retry. We only return a
    non-200 for clearly invalid input (400).
    """
    try:
        logger.info("Generating data suggestions")

        # Extract schema from discovery result if provided
        schema = request.data_schema
        if request.discovery_result and "schema" in request.discovery_result:
            schema = request.discovery_result["schema"]

        # Reject obviously empty/malformed schemas with a structured 400.
        # Previously these would either 422 (Pydantic) or sail through to a
        # downstream IndexError, both of which surfaced to the modal as a
        # generic "Failed to generate suggestions" banner.
        # Accept both shapes:
        #   (a) frontend pre-flattened:  {"columns": [...]}
        #   (b) nested per-table:        {"tables": [{"columns": [...]}, ...]}
        # Without the (b) walk, direct API callers (curl, planner-side calls,
        # smoke harnesses) hit a false `invalid_schema` even with valid input.
        columns = []
        if isinstance(schema, dict):
            top_cols = schema.get("columns")
            if isinstance(top_cols, list):
                columns = top_cols
            else:
                tables = schema.get("tables")
                if isinstance(tables, list):
                    for t in tables:
                        if isinstance(t, dict):
                            t_cols = t.get("columns")
                            if isinstance(t_cols, list):
                                columns.extend(t_cols)
        if not columns:
            return SuggestionsResponse(
                success=False,
                pii_detected=False,
                pii_columns=[],
                transforms=[],
                optimizations=[],
                total_suggestions=0,
                error_code="invalid_schema",
                error=(
                    "No columns found in the request schema. "
                    "Make sure the source connection's metadata fetch "
                    "succeeded and re-run."
                ),
            )

        # Generate suggestions using LangGraph
        suggestions = generate_suggestions(schema, request.intent or {})

        # Workflow-level error (e.g. token budget exceeded, execution time
        # exceeded, LLM call failed) is reported as a degraded success: the
        # PII/optimization heuristics still ran and may be useful, the user
        # can choose to continue or retry.
        workflow_error = suggestions.get("error")
        error_code = "ok"
        error_msg: Optional[str] = None
        degraded = False
        if workflow_error:
            degraded = True
            err_str = str(workflow_error)
            err_lower = err_str.lower()
            if "token budget" in err_lower:
                error_code = "token_budget"
                error_msg = (
                    "Transform suggestions skipped: schema too large for the "
                    "LLM token budget. PII and optimization suggestions are "
                    "still available."
                )
            elif "execution time" in err_lower or "timeout" in err_lower:
                error_code = "llm_timeout"
                error_msg = (
                    "Transform suggestions timed out. PII and optimization "
                    "suggestions are still available — you can apply those "
                    "and add transforms later."
                )
            elif "json" in err_lower:
                error_code = "llm_invalid_json"
                error_msg = (
                    "The model returned a response we couldn't parse. "
                    "PII and optimization suggestions are still available."
                )
            elif "openai" in err_lower or "api key" in err_lower or "unavailable" in err_lower:
                error_code = "llm_unavailable"
                error_msg = (
                    "The transform-suggestion model is unavailable right now. "
                    "PII and optimization suggestions are still available."
                )
            else:
                error_code = "internal_error"
                error_msg = (
                    f"Partial failure during suggestion generation: {err_str}. "
                    "You can apply the suggestions shown or continue without."
                )

        return SuggestionsResponse(
            success=not degraded,
            pii_detected=suggestions.get("pii_detected", False),
            pii_columns=suggestions.get("pii_columns", []),
            transforms=suggestions.get("transforms", []),
            optimizations=suggestions.get("optimizations", []),
            total_suggestions=suggestions.get("total_suggestions", 0),
            error_code=error_code,
            error=error_msg,
            degraded=degraded,
        )

    except Exception as e:
        # Catch-all: we still return a 200 with a structured error_code so the
        # frontend can show actionable text. A 500 here would hit the modal's
        # generic "Failed to generate suggestions" banner with no recovery info.
        logger.error(f"Failed to generate suggestions: {e}", exc_info=True)
        return SuggestionsResponse(
            success=False,
            pii_detected=False,
            pii_columns=[],
            transforms=[],
            optimizations=[],
            total_suggestions=0,
            error_code="internal_error",
            error=(
                f"Suggestion service hit an unexpected error: {type(e).__name__}: {e}. "
                "You can continue without transforms — the pipeline will sync "
                "the raw schema as-is."
            ),
        )


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "suggestions"}

