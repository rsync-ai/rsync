"""Unit tests for protocol_invariants — the post-generation gate that
catches deterministic protocol bugs before persistence.

Covers exactly the failure modes the Shopify→Postgres E2E surfaced:
  - GraphQL connector with Relay queries but no default in export()
  - Connector silently missing /mcp JSON-RPC route
  - REST connector with cursor pagination but empty cursor_path
"""

from __future__ import annotations

import pytest

from agents.tool_generator.validation.protocol_invariants import (
    ProtocolInvariantError,
    validate,
    validate_or_raise,
)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


CODE_WITH_MCP = '''
@app.post("/mcp")
def api_mcp(request: dict):
    return connector.handle_request(request)
'''

CODE_WITHOUT_MCP = '''
@app.post("/discover_schema")
def api_discover_schema(params: dict = {}):
    return connector.discover_schema(params)
'''

CODE_WITH_RELAY_DEFAULT = '''
@app.post("/mcp")
def api_mcp(request: dict):
    return connector.handle_request(request)

def export(self, params):
    variables = (params or {}).get("variables") or {}
    if "first" not in variables and "last" not in variables:
        variables["first"] = 250
    return self.execute_operation(operation_name, variables=variables)
'''

CODE_WITHOUT_RELAY_DEFAULT = '''
@app.post("/mcp")
def api_mcp(request: dict):
    return connector.handle_request(request)

GRAPHQL_OPERATIONS = {"products": {"query": "query GetProducts($first: Int)"}}

def export(self, params):
    variables = (params or {}).get("variables") or {}
    return self.execute_operation(operation_name, variables=variables)
'''


GRAPHQL_RELAY_METADATA = {
    "protocol": "graphql",
    "operations": [
        {
            "name": "products",
            "graphql_query": "query GetProducts($first: Int, $after: String) { products(first: $first) }",
        },
    ],
}

GRAPHQL_NO_RELAY_METADATA = {
    "protocol": "graphql",
    "operations": [
        {"name": "shop", "graphql_query": "query Shop { shop { id name } }"},
    ],
}

REST_VALID_METADATA = {
    "protocol": "rest",
    "resources": [
        {
            "name": "customers",
            "pagination_type": "cursor",
            "pagination_param": "starting_after",
            "cursor_mode": "last_item_id",
            "cursor_path": "",
        },
        {
            "name": "events",
            "pagination_type": "page",
            "pagination_param": "page",
        },
    ],
}

REST_MISSING_PAGINATION_METADATA = {
    "protocol": "rest",
    "resources": [
        {"name": "customers"},  # no pagination_type
    ],
}

REST_CURSOR_NO_PATH_METADATA = {
    "protocol": "rest",
    "resources": [
        {
            "name": "events",
            "pagination_type": "cursor",
            "cursor_mode": "response",
            "cursor_path": "",
        },
    ],
}


# --------------------------------------------------------------------------- #
# Universal: /mcp route
# --------------------------------------------------------------------------- #


def test_missing_mcp_route_violates():
    """The exact shopify-admin-graphql bug before the template patch."""
    report = validate(code=CODE_WITHOUT_MCP, metadata={})
    assert not report.passed
    assert any("/mcp" in v for v in report.violations)


def test_mcp_route_present_passes_universal():
    report = validate(code=CODE_WITH_MCP, metadata={})
    assert report.passed


def test_mcp_route_present_but_no_handle_request_violates():
    code = '@app.post("/mcp")\ndef api_mcp(request: dict):\n    return {"ok": True}\n'
    report = validate(code=code, metadata={})
    assert not report.passed
    assert any("handle_request" in v for v in report.violations)


# --------------------------------------------------------------------------- #
# GraphQL: Relay default
# --------------------------------------------------------------------------- #


def test_graphql_relay_without_default_violates():
    """If query template has $first but export() doesn't default it,
    Shopify rejects with 'first or last must be provided'."""
    report = validate(
        code=CODE_WITHOUT_RELAY_DEFAULT,
        metadata=GRAPHQL_RELAY_METADATA,
    )
    assert not report.passed
    assert any("Relay" in v for v in report.violations)


def test_graphql_relay_with_default_passes():
    report = validate(
        code=CODE_WITH_RELAY_DEFAULT,
        metadata=GRAPHQL_RELAY_METADATA,
    )
    assert report.passed


def test_graphql_no_relay_queries_does_not_require_default():
    """Non-Relay GraphQL queries (e.g. {shop {id}}) don't need the default."""
    code = '@app.post("/mcp")\ndef api_mcp(request: dict):\n    return connector.handle_request(request)\n'
    report = validate(code=code, metadata=GRAPHQL_NO_RELAY_METADATA)
    assert report.passed


# --------------------------------------------------------------------------- #
# REST: pagination determinism
# --------------------------------------------------------------------------- #


def test_rest_missing_pagination_type_violates():
    """The deterministic version of the silent default('cursor') Jinja bug."""
    report = validate(
        code=CODE_WITH_MCP,
        metadata=REST_MISSING_PAGINATION_METADATA,
        protocol="rest",
    )
    assert not report.passed
    assert any("pagination_type" in v for v in report.violations)


def test_rest_cursor_with_empty_path_warns_not_violates():
    """cursor_mode=response + empty cursor_path is no longer a hard violation:
    BaseMCPConnector.PaginationHandler auto-discovers the cursor location at
    runtime (bounded by seen_cursors / duplicate-hash / max_pages guards, so it
    can't infinite-loop). It passes but is surfaced as a warning."""
    report = validate(
        code=CODE_WITH_MCP,
        metadata=REST_CURSOR_NO_PATH_METADATA,
        protocol="rest",
    )
    assert report.passed
    assert not any("cursor_path" in v for v in report.violations)
    assert any("cursor_path" in w for w in report.warnings)


def test_rest_valid_pagination_passes():
    report = validate(
        code=CODE_WITH_MCP,
        metadata=REST_VALID_METADATA,
        protocol="rest",
    )
    assert report.passed


def test_rest_invalid_pagination_type_violates():
    metadata = {
        "protocol": "rest",
        "resources": [{"name": "x", "pagination_type": "magical-mystery-cursor"}],
    }
    report = validate(code=CODE_WITH_MCP, metadata=metadata, protocol="rest")
    assert not report.passed


# --------------------------------------------------------------------------- #
# Driver behavior
# --------------------------------------------------------------------------- #


def test_validate_or_raise_raises_on_violation():
    with pytest.raises(ProtocolInvariantError, match="/mcp"):
        validate_or_raise(code=CODE_WITHOUT_MCP, metadata={})


def test_validate_or_raise_returns_report_on_pass():
    report = validate_or_raise(code=CODE_WITH_MCP, metadata={})
    assert report.passed


def test_metadata_as_json_string_is_parsed():
    """validate() accepts metadata as either dict or JSON string."""
    import json
    report = validate(
        code=CODE_WITHOUT_RELAY_DEFAULT,
        metadata=json.dumps(GRAPHQL_RELAY_METADATA),
    )
    assert not report.passed


def test_corrupt_metadata_string_does_not_crash():
    """A corrupt metadata string falls back to empty dict, not a crash."""
    report = validate(code=CODE_WITH_MCP, metadata="not json {")
    # The /mcp check still runs; metadata-driven checks default to no-op.
    assert report.passed


def test_protocol_inferred_from_metadata_when_unset():
    """If protocol arg is omitted, it's inferred from metadata content."""
    report = validate(
        code=CODE_WITHOUT_RELAY_DEFAULT,
        metadata=GRAPHQL_RELAY_METADATA,
        # protocol arg deliberately omitted
    )
    # Should still detect the missing Relay default.
    assert not report.passed
