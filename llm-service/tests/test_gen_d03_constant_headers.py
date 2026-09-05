"""RED regression for DEFECT d03_constant_headers.

OpenAPI ``example`` header values must NOT be baked into a connector's
constant ``default_headers`` (they are illustrative, not normative). Only a
genuinely forced value — schema ``const`` / single-valued ``enum`` / schema
``default`` — or a vendor-declared constant may become a constant header.

Two cases, per the defect spec:
  (a) header params that carry ONLY an ``example`` (Content-Range / Digest)
      → ABSENT from the generated connector's constant default headers.
  (b) a vendor-declared constant header (Notion-Version: "2022-06-28",
      declared as a single-valued enum — the forced-value path) → PRESENT
      in the generated connector's default headers and thus in generated
      requests.

Render recipe mirrors test_connector_auth_contract.py's spirit but drives the
in-process deterministic generator directly (no network, no persistence to
shared/mcp-connectors, no docker), exactly like the existing generator test
``src/agents/tool_generator/tests/test_architect_graphql_phase_5.py``:

    build a GenerationContract
      -> agents.architect_rest.build_rest_spec(contract)
      -> generator.builder.ConnectorBuilder().build(spec, include_dockerfile=False)
      -> assert on spec.default_headers and the returned generated code string.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# The generator modules (architect_rest, builder, openapi_discovery) import
# their siblings with BARE names (``from schemas.contract import ...``), so the
# tool_generator directory must be on sys.path — same shim the in-tree
# generator tests use.
_TOOLGEN = str(
    Path(__file__).resolve().parents[1]
    / "src"
    / "agents"
    / "tool_generator"
)
if _TOOLGEN not in sys.path:
    sys.path.insert(0, _TOOLGEN)

from schemas.contract import (  # noqa: E402
    Dimension,
    Fact,
    FactSource,
    empty_contract,
)
from schemas.spec import ConnectorSpec  # noqa: E402
from agents.architect_rest import build_rest_spec  # noqa: E402
from generator.builder import ConnectorBuilder  # noqa: E402
from utils.openapi_discovery import _infer_required_headers  # noqa: E402


# --------------------------------------------------------------------------- #
# Header param fixtures — the exact spec shapes the defect is about.
# --------------------------------------------------------------------------- #

# (a) EXAMPLE-ONLY headers: no const / no default / no enum — just an
# illustrative example value. These must NOT become constant headers.
_EXAMPLE_ONLY_PARAMS = [
    {
        "name": "Content-Range",
        "in": "header",
        "required": True,
        # schema-level example (sch.get("example"))
        "schema": {"type": "string", "example": "bytes 0-49/100"},
    },
    {
        "name": "Digest",
        "in": "header",
        "required": True,
        # param-level example (p.get("example"))
        "example": "SHA-256=abc123",
        "schema": {"type": "string"},
    },
]
_EXAMPLE_VALUES = ("bytes 0-49/100", "SHA-256=abc123")

# (b) VENDOR-DECLARED CONSTANT: a single-valued enum forces the value — this is
# a genuine constant (Notion pins its API version), so it SHOULD be baked.
_VENDOR_CONST_PARAMS = [
    {
        "name": "Notion-Version",
        "in": "header",
        "required": True,
        "schema": {"type": "string", "enum": ["2022-06-28"]},
    }
]
_NOTION_VERSION_VALUE = "2022-06-28"


def _build_contract(header_params):
    """Minimal REST GenerationContract carrying one GET endpoint whose header
    params are `header_params`. header_hint is produced by the real discovery
    function so the test exercises the actual defect path end to end."""
    header_hint = _infer_required_headers({}, header_params)
    contract = empty_contract("d03", "widgetco")
    contract.set_fact(Fact(
        dimension=Dimension.PROTOCOL, value="rest",
        confidence=1.0, source=FactSource.USER_PROVIDED,
    ))
    contract.set_fact(Fact(
        dimension=Dimension.BASE_URL, value="https://api.widgetco.example/v1",
        confidence=1.0, source=FactSource.USER_PROVIDED,
    ))
    contract.set_fact(Fact(
        dimension=Dimension.AUTH_TYPE, value="bearer",
        confidence=1.0, source=FactSource.USER_PROVIDED,
    ))
    # Guarantees a syncable resource even if endpoint-derivation changes.
    contract.set_fact(Fact(
        dimension=Dimension.OPERATIONS, value=["widgets"],
        confidence=1.0, source=FactSource.DOC_PARSE,
    ))
    # _aggregate_default_headers reads operations_summary.endpoints[].header_hint.
    contract.metadata["operations_summary"] = {
        "endpoints": [
            {
                "path": "/widgets",
                "method": "GET",
                "header_hint": header_hint,
            }
        ]
    }
    return contract


def _generate_code(contract) -> str:
    spec = build_rest_spec(contract)
    assert isinstance(spec, ConnectorSpec)
    result = ConnectorBuilder().build(spec, include_dockerfile=False)
    assert result.is_valid, result.validation_errors
    assert result.code and result.code.strip()
    return result.code


# --------------------------------------------------------------------------- #
# Root-cause pin: the discovery function must not promote an example to a value.
# --------------------------------------------------------------------------- #

def test_infer_required_headers_does_not_treat_example_as_constant():
    hints = _infer_required_headers({}, _EXAMPLE_ONLY_PARAMS)
    # The header may still be surfaced (as a valueless / config-driven header),
    # but its example must never be adopted as a fixed value.
    assert not hints.get("Content-Range"), (
        f"Content-Range example was baked as a constant value: {hints!r}"
    )
    assert not hints.get("Digest"), (
        f"Digest example was baked as a constant value: {hints!r}"
    )


def test_infer_required_headers_keeps_single_enum_constant():
    hints = _infer_required_headers({}, _VENDOR_CONST_PARAMS)
    assert hints.get("Notion-Version") == _NOTION_VERSION_VALUE, (
        f"vendor-declared single-enum constant was lost: {hints!r}"
    )


# --------------------------------------------------------------------------- #
# (a) Example-only headers → absent from generated constant default_headers.
# --------------------------------------------------------------------------- #

def test_example_only_headers_absent_from_generated_default_headers():
    contract = _build_contract(_EXAMPLE_ONLY_PARAMS)
    spec = build_rest_spec(contract)

    # The illustrative example values must not appear as baked constants.
    assert "Content-Range" not in spec.default_headers, (
        f"Content-Range wrongly baked as a constant header: "
        f"{spec.default_headers!r}"
    )
    assert "Digest" not in spec.default_headers, (
        f"Digest wrongly baked as a constant header: {spec.default_headers!r}"
    )

    code = _generate_code(contract)
    # The example VALUE strings only ever reach the code via the constant
    # default_headers render (header_config_keys render the NAME only), so
    # their absence proves the examples were not sent on every request.
    for val in _EXAMPLE_VALUES:
        assert val not in code, (
            f"OpenAPI example value {val!r} leaked into generated connector "
            f"code as a constant header."
        )


# --------------------------------------------------------------------------- #
# (b) Vendor-declared constant header → present in generated requests.
# --------------------------------------------------------------------------- #

def test_vendor_declared_constant_header_present_in_generated_requests():
    contract = _build_contract(_VENDOR_CONST_PARAMS)
    spec = build_rest_spec(contract)

    assert spec.default_headers.get("Notion-Version") == _NOTION_VERSION_VALUE, (
        f"vendor-declared constant Notion-Version missing from default_headers: "
        f"{spec.default_headers!r}"
    )

    code = _generate_code(contract)
    assert _NOTION_VERSION_VALUE in code, (
        "vendor-declared constant header value not rendered into generated "
        "connector code."
    )
    assert "Notion-Version" in code
