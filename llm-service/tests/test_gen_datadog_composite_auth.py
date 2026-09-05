"""d04: Datadog-style composite (AND-required) multi-header auth.

Datadog requires BOTH ``DD-API-KEY`` and ``DD-APPLICATION-KEY`` on every request.
In OpenAPI this is ONE security requirement block listing two apiKey-header
schemes (``security: [{apiKeyAuth: [], appKeyAuth: []}]`` = AND). The generator
historically collapsed multi-scheme auth to a single header -> 403. These tests
pin the fix:
  * ``co_required_credential_headers()`` detects the AND group (and ONLY an AND
    group — separate blocks are OR alternatives and must be ignored)
  * the architect turns the detected set into ``AuthConfig.extra_required_headers``
    plus a secret ``ConfigField`` for the second credential
  * the rendered connector declares + stamps BOTH headers on every request
"""
import os
import sys

BASE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "src", "agents", "tool_generator",
)
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from utils.openapi_discovery import (  # noqa: E402
    AuthSchemeType,
    SecurityScheme,
    co_required_credential_headers,
)
from schemas.contract import Dimension, Fact, FactSource, empty_contract  # noqa: E402
from agents.architect_rest import build_rest_spec  # noqa: E402
from generator.builder import ConnectorBuilder  # noqa: E402


def _dd_schemes():
    return [
        SecurityScheme(
            name="apiKeyAuth", scheme_type=AuthSchemeType.API_KEY,
            api_key_in="header", api_key_name="DD-API-KEY",
        ),
        SecurityScheme(
            name="appKeyAuth", scheme_type=AuthSchemeType.API_KEY,
            api_key_in="header", api_key_name="DD-APPLICATION-KEY",
        ),
    ]


def test_co_required_detects_and_group():
    """One requirement block with two apiKey-header schemes = AND = co-required."""
    got = co_required_credential_headers(
        _dd_schemes(), [{"apiKeyAuth": [], "appKeyAuth": []}]
    )
    names = {h["header_name"] for h in got}
    assert names == {"DD-API-KEY", "DD-APPLICATION-KEY"}


def test_co_required_ignores_or_alternatives():
    """Separate requirement blocks = OR alternatives, NOT co-required."""
    got = co_required_credential_headers(
        _dd_schemes(), [{"apiKeyAuth": []}, {"appKeyAuth": []}]
    )
    assert got == []


def test_co_required_ignores_single_scheme_block():
    got = co_required_credential_headers(_dd_schemes(), [{"apiKeyAuth": []}])
    assert got == []


def test_co_required_ignores_non_header_apikey():
    """A header cred AND a query cred in one block is not a multi-HEADER group."""
    schemes = [
        SecurityScheme(
            name="apiKeyAuth", scheme_type=AuthSchemeType.API_KEY,
            api_key_in="header", api_key_name="DD-API-KEY",
        ),
        SecurityScheme(
            name="qpAuth", scheme_type=AuthSchemeType.API_KEY,
            api_key_in="query", api_key_name="api_key",
        ),
    ]
    got = co_required_credential_headers(schemes, [{"apiKeyAuth": [], "qpAuth": []}])
    assert got == []


def _dd_contract():
    c = empty_contract("datadog", "datadog")
    for dim, val in (
        (Dimension.PROTOCOL, "rest"),
        (Dimension.BASE_URL, "https://api.datadoghq.com"),
        (Dimension.AUTH_TYPE, "api_key"),
    ):
        c.set_fact(Fact(dimension=dim, value=val, confidence=1.0, source=FactSource.VENDOR_YAML))
    # Primary apiKey header the architect resolves from the parsed spec schemes.
    c.metadata["openapi_auth_schemes"] = {
        "api_key": {"header_name": "DD-API-KEY", "header_prefix": "", "query_param": None}
    }
    # The co-required set the discoverer detected (the SECOND header; the primary
    # DD-API-KEY is already carried by the default api_key method).
    c.metadata["extra_required_credential_headers"] = [
        {"scheme_name": "appKeyAuth", "header_name": "DD-APPLICATION-KEY"},
    ]
    return c


def test_architect_populates_extra_required_headers():
    spec = build_rest_spec(_dd_contract(), connector_name="datadog", display_name="Datadog")
    extras = spec.auth.extra_required_headers
    assert len(extras) == 1
    assert extras[0].header_name == "DD-APPLICATION-KEY"
    key = extras[0].config_keys[0]
    # a SECRET ConfigField exists so the connection form collects the 2nd key
    field = next((f for f in spec.config_fields if f.name == key), None)
    assert field is not None and field.secret is True


def test_architect_excludes_primary_header_from_extras():
    """If the co-required set echoes the primary header, it must not be re-added."""
    c = _dd_contract()
    c.metadata["extra_required_credential_headers"] = [
        {"scheme_name": "apiKeyAuth", "header_name": "DD-API-KEY"},
        {"scheme_name": "appKeyAuth", "header_name": "DD-APPLICATION-KEY"},
    ]
    spec = build_rest_spec(c, connector_name="datadog", display_name="Datadog")
    names = {h.header_name for h in spec.auth.extra_required_headers}
    assert names == {"DD-APPLICATION-KEY"}  # DD-API-KEY (primary) excluded


def test_datadog_render_stamps_both_headers():
    spec = build_rest_spec(_dd_contract(), connector_name="datadog", display_name="Datadog")
    gen = ConnectorBuilder().build(spec, include_dockerfile=False)
    code = gen.code
    assert gen.is_valid, "generated datadog connector must be valid"
    # primary header carried by the default api_key method
    assert "DD-API-KEY" in code
    # the extra co-required header is declared + stamped from config
    assert "EXTRA_REQUIRED_HEADERS" in code
    assert "DD-APPLICATION-KEY" in code
    key = spec.auth.extra_required_headers[0].config_keys[0]
    assert key in code
