"""Deterministic ``POST /v1/generate`` for the community connector-lifecycle service.

Answers the SAME wire contract as the cloud service's agentic handler
(``GenerateRequestV1`` / ``GenerateResponseV1``, imported — never copied — from
``src.agents.tool_generator.contracts.generate_v1``), so the api-gateway,
the frontend and any API client talk to one endpoint shape in both editions.

What it does, and only this: OpenAPI 3.x or Swagger 2.0 document in, working MCP
connector out. Convert, render, validate, persist. No model call, no API key, no
account, no outbound network request at any step.

What it deliberately refuses, and why refusing beats degrading:

* ``openapi_spec_url`` — this service sits on the connector network holding
  docker.sock, so fetching an operator-supplied URL server-side is a genuine
  SSRF primitive. The caller can fetch the document and pass it inline.
* ``docs_url`` / ``docs_text`` — learning an API from prose is the agentic
  pipeline's job and needs an LLM.
* ``graphql_schema`` / ``graphql_endpoint`` — the scaffolder converts OpenAPI
  only. Falling through to the REST path for a GraphQL API always ships the
  wrong shape (the D6 silent-fallthrough bug the cloud handler refuses on too).
* ``session_id`` — discovery sessions are served by ``/v1/discover``, which is
  cloud-only.

Every refusal is a 400 carrying both ``error`` and ``error_message`` so it
renders as an actionable sentence rather than "Bad Request": the api-gateway
copies ``error`` into ``error_message`` only when the latter is absent, and the
frontend's non-2xx reader looks at ``error`` first.

``quality_tier`` is intentionally left unset. The cloud fast path computes one
from spec attributes, in a module the community image does not ship; producing a
second definition of the same word here would be a copied contract, which is the
defect class this module's shared-model import exists to avoid.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from src.agents.tool_generator.contracts.generate_v1 import (
    GenerateRequestV1,
    GenerateResponseV1,
)
from src.agents.tool_generator.deployment.routes import (
    _canonicalize_connector_id,
    _managed_connectors_enabled,
    require_internal_secret,
)
from src.agents.tool_generator.deployment.service import (
    ConnectorArtifacts,
    DeploymentService,
)
from src.agents.tool_generator.scaffold.openapi_to_spec import (
    ConversionReport,
    OpenAPIConversionError,
    openapi_to_connector_spec,
)

logger = logging.getLogger(__name__)

scaffold_router = APIRouter(tags=["Connector Generation (Deterministic)"])

# The categories the scaffolder understands, mirroring scaffold/cli.py's choices.
# A request naming anything else falls back to the default rather than failing:
# `category` is a hint on this route, not a contract term.
_SCAFFOLD_CATEGORIES = (
    "api_saas",
    "relational_db",
    "document_db",
    "cloud_storage",
    "streaming",
    "data_warehouse",
    "wide_column_db",
)

_DEFAULT_CATEGORY = "api_saas"

_HOSTED = "The hosted service at rsync.ai runs the agentic pipeline that does."


def _refusal(
    connector_name: str,
    message: str,
    *,
    stage: str,
    suggestions: Optional[List[str]] = None,
    status_code: int = 400,
) -> JSONResponse:
    """A refusal the whole stack can read.

    `error` is what the frontend's non-2xx branch reads and what the api-gateway
    promotes into `error_message`; `error_message` is what a client reading the
    response model reads. Both are set so neither path shows a bare status line.
    """
    body = GenerateResponseV1(
        success=False,
        status="refused",
        connector_name=connector_name,
        error_message=message,
        error_stage=stage,
        suggestions=suggestions or [],
    ).model_dump(mode="json")
    body["error"] = message
    return JSONResponse(status_code=status_code, content=body)


def _parse_document(raw: str) -> Dict[str, Any]:
    """Parse an inline spec as JSON, falling back to YAML.

    Same order and the same PyYAML-absent message as ``scaffold/cli.py``'s
    ``load_document``: JSON first because every JSON document is also valid
    YAML but not the reverse, and its parser reports better error positions.
    Kept separate because that function reads a path or stdin, and this route
    already holds the bytes.
    """
    if not raw.strip():
        raise OpenAPIConversionError("The supplied OpenAPI document is empty")

    try:
        parsed: Any = json.loads(raw)
    except ValueError as json_error:
        try:
            import yaml
        except ImportError:
            raise OpenAPIConversionError(
                f"The supplied document is not valid JSON ({json_error}), and PyYAML is "
                "not installed so it cannot be read as YAML. Send JSON instead."
            )
        try:
            parsed = yaml.safe_load(raw)
        except Exception as yaml_error:  # noqa: BLE001 — any parser error is the same answer
            raise OpenAPIConversionError(
                f"The supplied document parses as neither JSON nor YAML: {yaml_error}"
            )

    if not isinstance(parsed, dict):
        raise OpenAPIConversionError(
            "The supplied document does not contain a mapping at its root, so it is "
            "not an OpenAPI or Swagger specification"
        )
    return parsed


def _unsupported_input(request: GenerateRequestV1) -> Optional[Tuple[str, List[str]]]:
    """The refusal owed to a request carrying no inline OpenAPI document.

    Returns (message, suggestions), or None when an inline spec is present —
    in which case any other input is simply unused, and saying so in a note is
    more useful than refusing work that can be done.
    """
    if request.openapi_spec:
        return None

    if request.openapi_spec_url:
        return (
            "This service does not fetch specifications by URL. It runs on the connector "
            "network with access to the Docker socket, so fetching an operator-supplied "
            "URL here would let a request reach internal addresses. Download "
            f"'{request.openapi_spec_url}' yourself and send its contents as 'openapi_spec'.",
            ["curl -sSL <spec-url> | jq -c '{api_name: \"my-api\", openapi_spec: (.|tostring)}'"],
        )

    if request.graphql_schema or request.graphql_endpoint:
        return (
            "This service generates connectors from OpenAPI and Swagger documents only. "
            "GraphQL introspection needs the agentic pipeline, and generating a REST-shaped "
            f"connector for a GraphQL API would be wrong rather than merely incomplete. {_HOSTED}",
            ["Send an OpenAPI document via 'openapi_spec' if the API also exposes one"],
        )

    if request.session_id:
        return (
            "Discovery sessions are served by /v1/discover, which is not part of the "
            f"community service. {_HOSTED} Generate here by sending the API's OpenAPI "
            "document inline as 'openapi_spec'.",
            [],
        )

    if request.docs_url or request.docs_text:
        return (
            "Reading an API's documentation to infer its shape requires the agentic "
            f"pipeline. {_HOSTED} This service needs the machine-readable contract: send "
            "the API's OpenAPI or Swagger document inline as 'openapi_spec'.",
            [
                "Most APIs publish one at /openapi.json, /swagger.json or /v3/api-docs",
            ],
        )

    return (
        "No OpenAPI document was supplied. Send the API's OpenAPI 3.x or Swagger 2.0 "
        "specification inline as 'openapi_spec' (JSON or YAML). This service generates "
        f"connectors deterministically from that document alone. {_HOSTED}",
        [
            "Most APIs publish one at /openapi.json, /swagger.json or /v3/api-docs",
            "Offline alternative: python -m src.agents.tool_generator.scaffold.cli spec.json --out ./connector",
        ],
    )


def _stage(name: str, started: float) -> Dict[str, Any]:
    return {"name": name, "status": "completed", "duration_ms": round((time.monotonic() - started) * 1000, 2)}


@scaffold_router.post("/generate", dependencies=[Depends(require_internal_secret)])
async def generate_connector_deterministic(request: GenerateRequestV1):
    """Render an MCP connector from an OpenAPI document. No LLM at any step.

    Shares ``require_internal_secret`` with /v1/deploy for the same reason: in
    managed mode this path builds and starts a container over docker.sock.
    """
    began = time.monotonic()
    connector_name = _canonicalize_connector_id(request.api_name)
    logger.info("🧱 /v1/generate (deterministic) request for: %s", connector_name)

    refusal = _unsupported_input(request)
    if refusal is not None:
        message, suggestions = refusal
        logger.info("↩️  refusing %s: %s", connector_name, message.split(".")[0])
        return _refusal(connector_name, message, stage="input_validation", suggestions=suggestions)

    stages: List[Dict[str, Any]] = []
    notes: List[str] = []

    # ── Convert ──────────────────────────────────────────────────────────────
    stage_began = time.monotonic()
    category = request.category if request.category in _SCAFFOLD_CATEGORIES else _DEFAULT_CATEGORY
    if request.category and request.category != category:
        notes.append(
            f"category '{request.category}' is not one this service renders; used '{category}'"
        )
    try:
        document = _parse_document(request.openapi_spec or "")
        report: ConversionReport = openapi_to_connector_spec(
            document,
            name=connector_name,
            category=category,
            base_url=request.base_url,
        )
    except OpenAPIConversionError as exc:
        return _refusal(connector_name, str(exc), stage="spec_conversion")
    stages.append(_stage("convert", stage_began))

    spec = report.spec
    notes.extend(report.notes)
    if report.skipped_paths:
        notes.append(f"{len(report.skipped_paths)} path(s) had no generatable collection and were skipped")

    # ── Render ───────────────────────────────────────────────────────────────
    stage_began = time.monotonic()
    try:
        from src.agents.tool_generator.generator.builder import ConnectorBuilder
        from src.agents.tool_generator.schemas.spec import ConnectorSpec

        # ConnectorBuilder.build_from_dict does exactly this, but keeping the spec
        # object lets `class_name` and `connector_type` come from the schema that
        # defines them rather than from those rules restated here.
        spec_obj = ConnectorSpec(**spec)
        generated = ConnectorBuilder().build(spec_obj)
    except Exception as exc:  # noqa: BLE001 — any render failure is one answer to the caller
        logger.error("❌ rendering %s failed: %s", connector_name, exc, exc_info=True)
        return _refusal(
            connector_name,
            f"Rendering the connector failed: {exc}",
            stage="render",
            status_code=500,
        )
    stages.append(_stage("render", stage_began))

    # The converter slugs the connector name from `api_name` (or info.title), so the
    # spec is the authority on what this connector is called, not the raw request.
    connector_name = spec_obj.connector_type

    # ── Validate ─────────────────────────────────────────────────────────────
    # The same two refusal guards scaffold/cli.py's write_artifacts applies, for
    # the same reason: an invalid or empty connector must not reach disk.
    if not generated.is_valid:
        return _refusal(
            connector_name,
            "The connector rendered from this specification did not validate: "
            + "; ".join(generated.validation_errors),
            stage="validation",
            suggestions=list(generated.validation_errors),
        )
    if not (generated.code or "").strip():
        return _refusal(
            connector_name,
            "The connector rendered from this specification has an empty body",
            stage="validation",
        )

    try:
        metadata: Dict[str, Any] = json.loads(generated.metadata)
    except ValueError:  # pragma: no cover — the builder always emits JSON here
        metadata = {}

    response = GenerateResponseV1(
        success=True,
        status="completed",
        connector_name=connector_name,
        class_name=spec_obj.class_name,
        code_preview=generated.code[:500] + "...",
        metadata=metadata,
        workflow_stages=stages,
        protocol="rest",
        operation_count=report.resource_count,
        draft_warnings=list(generated.validation_warnings),
    )
    response.metadata["generation_mode"] = "deterministic_openapi"
    if notes:
        response.metadata["notes"] = notes

    if not request.save_artifacts:
        logger.info("🧪 dry run for %s: save_artifacts=false", connector_name)
        response.status = "completed_dry_run"
        response.total_time_ms = round((time.monotonic() - began) * 1000, 2)
        return response.model_dump(exclude_none=False, exclude_defaults=False)

    # ── Persist ──────────────────────────────────────────────────────────────
    stage_began = time.monotonic()
    try:
        managed = _managed_connectors_enabled()
        deploy_result = await DeploymentService().deploy(
            artifacts=ConnectorArtifacts(
                name=connector_name,
                code=generated.code,
                metadata_json=generated.metadata,
                requirements_txt=generated.requirements,
                dockerfile=generated.dockerfile,
                spec_json=json.dumps(spec, indent=2, sort_keys=True),
                # Let DeploymentService compute the next version so the saved
                # directory and any image tag agree.
                version="latest",
            ),
            build_docker=managed,
            start_container=managed,
            verify=True,
            validation_passed=True,
        )
        if not deploy_result.success:
            raise RuntimeError(deploy_result.error_message or "Failed to save connector artifacts")

        response.output_path = deploy_result.output_path
        response.version = (deploy_result.version or "").strip() or None
        if response.version:
            response.metadata["version_dir"] = response.version
            response.metadata["version"] = response.version.lstrip("v")
        response.metadata["managed_connectors"] = managed
        if not managed:
            response.metadata["deployment_mode"] = "external_mcp"
        logger.info("✅ saved %s to %s", connector_name, deploy_result.output_path)
    except Exception as exc:  # noqa: BLE001
        # F-20-followup: a persistence failure surfaces through the response
        # BODY, never as a bare HTTPException. Clients read `error_message` off
        # the model; a raised 500 would hand them `{"detail": ...}` instead and
        # they would see success:false with no reason.
        logger.error("❌ persisting %s failed: %s", connector_name, exc, exc_info=True)
        response.success = False
        response.status = "failed"
        response.error_message = str(exc) or "Failed to save the generated connector"
        response.error_stage = "artifact_persistence"
        response.total_time_ms = round((time.monotonic() - began) * 1000, 2)
        return response.model_dump(exclude_none=False, exclude_defaults=False)
    stages.append(_stage("persist", stage_began))

    response.total_time_ms = round((time.monotonic() - began) * 1000, 2)
    return response.model_dump(exclude_none=False, exclude_defaults=False)
