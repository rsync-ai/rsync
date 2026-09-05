"""RED regression for defect d02-tier1 — tier-1 vendor resources drop update_method.

PR #350 landed the update-verb MECHANISM: ``ResourceConfig.update_method`` and the
template's per-resource ``resource_update_methods`` map. The Tier-2 (OpenAPI) path
auto-detects PATCH from the item route, and ``_resource_from_dict`` already reads
``d.get("update_method")``. But the TIER-1 curated path (``vendor_apis.yaml`` →
``RestResourceSpec`` → ``_inject_vendor_rest_resources`` dict → ``_resource_from_dict``)
DROPS the field: ``RestResourceSpec`` has no ``update_method`` attribute, so
``_parse_rest_resources`` never captures it and ``_inject_vendor_rest_resources``
never serializes it. Result: HubSpot CRM v3 (whose object updates require
``PATCH /crm/v3/objects/{object}/{id}``) is generated with a hard-coded PUT and
every id-bearing upsert row 405s.

The fix threads ``update_method`` through the tier-1 dataclass + parser + injector,
and declares ``update_method: PATCH`` on HubSpot's writable resources.

Renders fully IN-PROCESS (no network, no docker, no persistence to
shared/mcp-connectors) via the production vendor-registry → inject → build_rest_spec
→ ConnectorBuilder path.

RED today: ``RestResourceSpec`` has no ``update_method`` (AttributeError / field lost),
and the real HubSpot ``contacts`` resource renders a PUT upsert.
GREEN after fix: the parsed spec carries the verb and HubSpot ``contacts`` upserts
via PATCH.
"""

from __future__ import annotations

import os
import re
import sys

import pytest

# tool_generator package root: .../llm-service/src/agents/tool_generator
_TOOLGEN = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),  # .../llm-service/tests
        "..",                       # .../llm-service
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
from utils.vendor_registry import _parse_rest_resources  # noqa: E402


def _verb_map(code: str) -> dict[str, str]:
    """Extract the generated per-resource update-verb wiring, e.g.
    ``"contacts": "PATCH"``. Only PUT/PATCH values are considered so unrelated
    string maps (endpoints / id_fields) don't pollute the result."""
    return {
        name: verb
        for name, verb in re.findall(
            r'"([A-Za-z0-9_]+)"\s*:\s*"(PUT|PATCH)"', code
        )
    }


def _rest_contract(vendor_id: str, base_url: str) -> "object":
    """A minimal generate-ready REST contract; resources are injected from the
    REAL vendor registry (vendor_apis.yaml) exactly as the fast-path does."""
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
# Unit: RestResourceSpec / _parse_rest_resources must carry update_method.
# --------------------------------------------------------------------------- #

def test_parse_rest_resources_carries_update_method():
    specs = _parse_rest_resources([
        {
            "name": "contacts",
            "endpoint": "/crm/v3/objects/contacts",
            "supports_update": True,
            "update_method": "patch",   # lowercase — parser must normalize
        }
    ])
    assert specs, "no RestResourceSpec parsed"
    assert specs[0].update_method == "PATCH", (
        "tier-1 RestResourceSpec dropped update_method (got "
        f"{getattr(specs[0], 'update_method', '<missing attr>')!r})"
    )


def test_parse_rest_resources_defaults_update_method_to_put():
    specs = _parse_rest_resources([{"name": "widgets", "endpoint": "/widgets"}])
    assert specs and specs[0].update_method == "PUT"


def test_parse_rest_resources_rejects_bogus_update_method():
    """A non PUT/PATCH verb normalizes to the safe PUT default."""
    specs = _parse_rest_resources([
        {"name": "x", "endpoint": "/x", "update_method": "DELETE"}
    ])
    assert specs and specs[0].update_method == "PUT"


# --------------------------------------------------------------------------- #
# Integration: the REAL HubSpot tier-1 entry must upsert contacts via PATCH.
# --------------------------------------------------------------------------- #

def test_hubspot_tier1_injects_patch_verb():
    contract = _rest_contract("hubspot", "https://api.hubapi.com")
    _inject_vendor_rest_resources(contract, "hubspot")
    vrr = contract.metadata.get("vendor_rest_resources") or []
    assert vrr, "hubspot vendor rest_resources were not injected"
    verbs = {r["name"]: r.get("update_method") for r in vrr}
    assert verbs.get("contacts") == "PATCH", (
        f"hubspot contacts must inject update_method=PATCH; got {verbs!r}"
    )


def test_hubspot_tier1_contacts_upsert_emits_patch():
    contract = _rest_contract("hubspot", "https://api.hubapi.com")
    _inject_vendor_rest_resources(contract, "hubspot")
    spec = build_rest_spec(
        contract, connector_name="hubspot", display_name="HubSpot",
    )
    generated = ConnectorBuilder().build(spec, include_dockerfile=False)
    assert generated.is_valid, generated.validation_errors
    code = generated.code
    assert "def upsert_data(" in code, "upsert_data was not generated"
    assert _verb_map(code).get("contacts") == "PATCH", (
        "HubSpot contacts must upsert via PATCH (PUT 405s on CRM v3); "
        f"generated verb map was {_verb_map(code)!r}"
    )
