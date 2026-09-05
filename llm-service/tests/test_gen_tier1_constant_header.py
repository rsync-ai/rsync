"""RED regression for defect notion-version — tier-1 vendors can't send a
constant header.

PR #350's d03 landed the constant-header RENDER path: a valued header in
``spec.default_headers`` is emitted on every request, and ``_aggregate_default_headers``
promotes forced-value headers. But that aggregator reads ONLY OpenAPI
``operations_summary.endpoints[].header_hint`` — the Tier-2 path. A TIER-1 curated
vendor (``vendor_apis.yaml``) has no ``operations_summary`` and no way to declare a
constant header, so Notion's required ``Notion-Version: 2022-06-28`` header is never
sent and every Notion request 400s. (Notion previously declared ``notion_version``
as a runtime_config_field, which surfaced a config box the connector never wired
into a header.)

The fix adds an API-variant-level ``constant_headers`` map to the vendor registry
(``ApiVariant.constant_headers``), injects it into the contract, and folds it into
``default_headers`` in ``_aggregate_default_headers``. Notion then declares
``constant_headers: {Notion-Version: "2022-06-28"}``.

Renders fully IN-PROCESS (no network, no docker, no persistence to
shared/mcp-connectors) via vendor-registry → inject → build_rest_spec →
ConnectorBuilder.

RED today: ``ApiVariant`` has no ``constant_headers`` and Notion's generated
``default_headers`` is empty.
GREEN after fix: the parsed variant carries the header and Notion's generated
connector sends ``Notion-Version: 2022-06-28`` on every request.
"""

from __future__ import annotations

import os
import sys

import pytest

# tool_generator package root: .../llm-service/src/agents/tool_generator
_TOOLGEN = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "src",
        "agents",
        "tool_generator",
    )
)
if _TOOLGEN not in sys.path:
    sys.path.insert(0, _TOOLGEN)

from schemas.contract import (  # noqa: E402
    Dimension,
    Fact,
    FactSource,
    empty_contract,
)
from agents.architect_rest import build_rest_spec  # noqa: E402
from agents.session_fast_path import _inject_vendor_rest_resources  # noqa: E402
from generator.builder import ConnectorBuilder  # noqa: E402
from utils.vendor_registry import _parse_api  # noqa: E402

_NOTION_VERSION = "2022-06-28"


def _rest_contract(vendor_id: str, base_url: str) -> "object":
    contract = empty_contract(vendor_id, vendor_id)
    for dim, value in (
        (Dimension.PROTOCOL, "rest"),
        (Dimension.BASE_URL, base_url),
        (Dimension.AUTH_TYPE, "bearer"),
    ):
        contract.set_fact(
            Fact(dimension=dim, value=value, confidence=1.0, source=FactSource.VENDOR_YAML)
        )
    return contract


# --------------------------------------------------------------------------- #
# Unit: ApiVariant / _parse_api must carry a constant_headers map.
# --------------------------------------------------------------------------- #

def test_parse_api_carries_constant_headers():
    api = _parse_api(
        "rest",
        {
            "id": "rest",
            "name": "REST",
            "protocol": "rest",
            "endpoint_template": "https://api.example.com/v1",
            "constant_headers": {"X-Api-Version": "2024-01-01"},
        },
    )
    assert api is not None, "well-formed api entry was rejected"
    assert getattr(api, "constant_headers", None) == {"X-Api-Version": "2024-01-01"}, (
        "ApiVariant dropped constant_headers (got "
        f"{getattr(api, 'constant_headers', '<missing attr>')!r})"
    )


def test_parse_api_defaults_constant_headers_to_empty():
    api = _parse_api(
        "rest",
        {"id": "rest", "protocol": "rest", "endpoint_template": "https://x/v1"},
    )
    assert api is not None
    assert getattr(api, "constant_headers", None) == {}


# --------------------------------------------------------------------------- #
# Integration: the REAL Notion tier-1 entry must send Notion-Version everywhere.
# --------------------------------------------------------------------------- #

def test_notion_tier1_default_headers_carry_notion_version():
    contract = _rest_contract("notion", "https://api.notion.com/v1")
    _inject_vendor_rest_resources(contract, "notion")
    spec = build_rest_spec(
        contract, connector_name="notion", display_name="Notion",
    )
    assert spec.default_headers.get("Notion-Version") == _NOTION_VERSION, (
        "Notion tier-1 connector must pin Notion-Version as a constant header; "
        f"default_headers was {spec.default_headers!r}"
    )


def test_notion_tier1_sends_notion_version_in_generated_code():
    contract = _rest_contract("notion", "https://api.notion.com/v1")
    _inject_vendor_rest_resources(contract, "notion")
    spec = build_rest_spec(
        contract, connector_name="notion", display_name="Notion",
    )
    generated = ConnectorBuilder().build(spec, include_dockerfile=False)
    assert generated.is_valid, generated.validation_errors
    code = generated.code
    assert "Notion-Version" in code, "Notion-Version header not rendered"
    assert _NOTION_VERSION in code, "Notion-Version value not rendered into code"
