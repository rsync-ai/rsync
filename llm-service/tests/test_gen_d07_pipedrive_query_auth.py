"""RED regression for defect d07 — tier-1 pipedrive sends its API token as an
``Authorization`` header instead of the ``?api_token=<token>`` query param the
Pipedrive REST API v1 actually requires, so every request 401s.

Root cause (all on the TIER-1 curated path — pipedrive fetches no OpenAPI spec):
  1. ``config/vendor_apis.yaml`` declares ``method: api_key`` (a HEADER method); the
     query-param intent lives only in the free-text ``description``.
  2. ``utils/vendor_registry.py`` ``_VALID_AUTH_METHODS`` excludes ``api_key_query`` —
     the parser would DROP it even if declared.
  3. ``SupportedAuth`` (vendor_registry) has no ``query_param`` field — the tier-1
     carrier structurally cannot hold a query-param name.
  4. ``agents/session_fast_path.py`` never forwards ``query_param`` into the contract
     metadata handed to the REST architect.

Downstream is already query-param-capable: ``_build_supported_methods`` reads
``query_param`` (architect_rest.py), ``SupportedAuthMethod``/``AuthConfig`` validate
it, and ``connector.py.j2`` renders ``?api_token=`` when the auth method carries a
``query_param``. The template needs NO change — the fix threads the query-param name
through the tier-1 carrier and corrects pipedrive's declaration.

Renders fully IN-PROCESS (no network, no docker, no persistence to
shared/mcp-connectors) via the production vendor-registry → inject → build_rest_spec
→ ConnectorBuilder path.

RED today: ``_parse_supported_auth_methods`` drops ``api_key_query`` and has no
``query_param`` field; the real pipedrive tier-1 entry resolves to
``AuthConfig(type=api_key, header_name='Authorization')`` and the generated connector
sends the token in a header.
GREEN after fix: pipedrive resolves to ``AuthConfig(type=api_key_query,
query_param='api_token')`` and the generated connector injects ``?api_token=<token>``.
"""

from __future__ import annotations

import os
import sys

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
from schemas.spec import AuthType  # noqa: E402
from agents.architect_rest import build_rest_spec  # noqa: E402
from agents.session_fast_path import _inject_vendor_rest_resources  # noqa: E402
from generator.builder import ConnectorBuilder  # noqa: E402
from utils.vendor_registry import _parse_supported_auth_methods  # noqa: E402


def _rest_contract(vendor_id: str, base_url: str, auth_type: str):
    """A minimal generate-ready REST contract; resources + supported_auth_methods
    are injected from the REAL vendor registry (vendor_apis.yaml) exactly as the
    fast-path does via ``_inject_vendor_rest_resources``."""
    contract = empty_contract(vendor_id, vendor_id)
    for dim, value in (
        (Dimension.PROTOCOL, "rest"),
        (Dimension.BASE_URL, base_url),
        (Dimension.AUTH_TYPE, auth_type),
    ):
        contract.set_fact(
            Fact(dimension=dim, value=value, confidence=1.0, source=FactSource.VENDOR_YAML)
        )
    return contract


# --------------------------------------------------------------------------- #
# Unit: the tier-1 vendor parser must ACCEPT api_key_query and CARRY query_param.
# --------------------------------------------------------------------------- #

def test_vendor_registry_accepts_api_key_query():
    specs = _parse_supported_auth_methods([
        {
            "method": "api_key_query",
            "header_name": "",
            "query_param": "api_token",
            "config_keys": ["api_token"],
            "description": "token as ?api_token=",
        }
    ])
    assert specs, "api_key_query was dropped by _VALID_AUTH_METHODS"
    assert specs[0].method == "api_key_query"


def test_vendor_registry_carries_query_param():
    specs = _parse_supported_auth_methods([
        {"method": "api_key_query", "query_param": "api_token", "config_keys": ["api_token"]}
    ])
    assert specs, "api_key_query was dropped"
    assert getattr(specs[0], "query_param", None) == "api_token", (
        "SupportedAuth dropped query_param (got "
        f"{getattr(specs[0], 'query_param', '<missing attr>')!r})"
    )


# --------------------------------------------------------------------------- #
# Integration: the REAL pipedrive tier-1 entry must resolve to query-param auth.
# --------------------------------------------------------------------------- #

def _pipedrive_spec():
    contract = _rest_contract("pipedrive", "https://api.pipedrive.com/v1", "pipedrive")
    _inject_vendor_rest_resources(contract, "pipedrive")
    return build_rest_spec(
        contract, connector_name="pipedrive", display_name="Pipedrive",
    )


def test_pipedrive_authconfig_is_api_key_query():
    spec = _pipedrive_spec()
    assert spec.auth.type == AuthType.API_KEY_QUERY, (
        f"pipedrive auth must be api_key_query; got {spec.auth.type!r}"
    )
    assert spec.auth.query_param == "api_token", (
        f"pipedrive must send ?api_token=; got query_param={spec.auth.query_param!r}"
    )


def test_pipedrive_render_injects_query_param_not_header():
    spec = _pipedrive_spec()
    generated = ConnectorBuilder().build(spec, include_dockerfile=False)
    assert generated.is_valid, generated.validation_errors
    code = generated.code
    # The default auth method must be the query-param form, not a header api_key.
    assert 'DEFAULT_AUTH_METHOD = "api_key_query"' in code, (
        "generated connector default auth is not api_key_query"
    )
    assert 'DEFAULT_AUTH_METHOD = "api_key"' not in code, (
        "generated connector still defaults to header api_key auth"
    )
    # The auth-method table must declare the query param name so _make_request_v2
    # injects params["api_token"] = <token> instead of an auth header.
    assert '"query_param": "api_token"' in code, (
        "generated SUPPORTED_AUTH_METHODS does not carry query_param=api_token"
    )
