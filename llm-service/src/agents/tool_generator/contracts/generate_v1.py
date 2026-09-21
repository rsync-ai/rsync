"""Wire contract for ``POST /v1/generate`` — shared by both editions.

Two services answer this route with the same request and response shape:

* the **agentic** handler in ``..agents.integration`` (cloud only — research,
  architect, build, QA), and
* the **deterministic** handler in ``src.lifecycle.scaffold_routes`` (ships in
  the community image — OpenAPI/Swagger document in, connector out, no LLM).

The models live here, in a package both images ship, so there is exactly ONE
definition of the contract rather than a copy per handler. A hand-copied
contract is the defect class that #970 closed six instances of: a copy drifts
silently, and the drift is invisible until a field the api-gateway forwards
stops being read by one of the two handlers.

Import closure is ``pydantic`` + ``typing`` only. Nothing here may import
``..agents``, ``..config`` or ``..utils`` — the community image does not ship
them, and an import added later would break the lifecycle service at boot.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class GenerateRequestV1(BaseModel):
    """
    Request for agentic connector generation.

    Note: API Gateway may send additional fields (e.g. force_regenerate).
    We intentionally ignore unknown fields for forward/backward compatibility.
    """

    model_config = ConfigDict(extra="ignore")

    api_name: str = Field(..., description="Name of the API/connector to generate")
    description: str = Field("", description="Brief description of the API")
    docs_url: Optional[str] = Field(None, description="URL to API documentation")
    docs_text: Optional[str] = Field(None, description="Raw API documentation text")
    category: Optional[str] = Field(None, description="Force a specific category")
    api_category_hint: Optional[str] = Field(None, description="API category hint for resource fallbacks (crm, ecommerce, etc.)")
    use_context7: bool = Field(True, description="Fetch docs via Context7 MCP before generating")
    developer_mode: bool = Field(False, description="Enable developer mode features")
    enable_chaos: bool = Field(True, description="Enable chaos testing")
    save_artifacts: bool = Field(True, description="Persist generated connector into TOOLS_DIR (set false for dry-run regression)")
    output_dir: Optional[str] = Field(None, description="Custom output directory")
    # Phase 13a: When the discovery flow produced a confirmed contract, the
    # frontend passes its session_id here. The handler skips the full
    # LLM-driven pipeline and renders directly from the contract — fast,
    # deterministic, no hallucination.
    session_id: Optional[str] = Field(None, description="Discovery session id from /v1/discover")
    # Spec-first deterministic inputs. When any of these is present (or an
    # OpenAPI spec is auto-discoverable from docs_url), the connector is built
    # directly from the machine-readable contract — complete + repeatable — and
    # the LLM/Context7 pipeline is skipped. This is how arbitrary APIs (and a
    # user's own swagger/openapi or GraphQL schema) generate reliably.
    openapi_spec: Optional[str] = Field(None, description="Inline OpenAPI/Swagger spec (JSON or YAML) — parsed deterministically")
    openapi_spec_url: Optional[str] = Field(None, description="URL to an OpenAPI/Swagger spec (.json/.yaml/.yml)")
    graphql_schema: Optional[str] = Field(None, description="Inline GraphQL introspection JSON — parsed deterministically")
    graphql_endpoint: Optional[str] = Field(None, description="GraphQL endpoint URL for live introspection")
    base_url: Optional[str] = Field(
        None,
        description=(
            "Endpoint base URL for the connector. For inline GraphQL "
            "introspection (graphql_schema) where live introspection is "
            "auth-gated (e.g. GitHub), supply the real GraphQL endpoint here "
            "(e.g. https://api.github.com/graphql); it becomes the connector's "
            "base_url without triggering a live introspection request."
        ),
    )


class GenerateResponseV1(BaseModel):
    """
    Response shape expected by the frontend (superset of fields).
    """

    model_config = ConfigDict(populate_by_name=True)

    success: bool
    status: str

    connector_name: Optional[str] = None
    class_name: Optional[str] = None

    code_preview: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    output_path: Optional[str] = None
    # Concrete version created during the save step (e.g., "v1.0.4")
    version: Optional[str] = None

    iterations: int = 0
    total_time_ms: float = 0
    workflow_stages: List[Dict[str, Any]] = Field(default_factory=list)

    error_message: Optional[str] = None
    error_stage: Optional[str] = None
    suggestions: List[str] = Field(default_factory=list)

    # kept for compatibility with old clients; no async mode in minimal surface
    job_id: Optional[str] = None
    is_draft: bool = False
    draft_warnings: List[str] = Field(default_factory=list)

    # Populated when the generated connector uses OAuth2 — tells the user exactly
    # which env vars to set before the provider becomes active in the api-gateway.
    oauth_setup: Optional[Dict[str, Any]] = None

    # Phase 13a fast-path additions
    quality_tier: Optional[str] = None
    protocol: Optional[str] = None
    operation_count: Optional[int] = None
