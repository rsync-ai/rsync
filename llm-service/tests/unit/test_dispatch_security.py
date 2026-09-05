"""
Security regression: BaseMCPConnector._handle_tool_call must never dispatch to
private/internal methods.

A crafted JSON-RPC tools/call name like "<type>__cleanup_worker" strips the
"<type>_" prefix to "_cleanup_worker"; without a guard, getattr would resolve it
and expose internals (e.g. _spawn_worker_process, upload_to_staging) to any
unauthenticated caller. This test pins the public-only dispatch contract on the
canonical shared base class that every connector inherits.
"""

import os
import sys

import pytest

# Import the canonical shared base connector (the hardened dispatcher).
_SHARED = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../shared/mcp-connectors"))
if _SHARED not in sys.path:
    sys.path.insert(0, _SHARED)

try:
    import base_connector as bc
except Exception as e:  # pragma: no cover - environment without optional deps
    pytest.skip(f"base_connector import failed: {e}", allow_module_level=True)


class _Probe(bc.BaseMCPConnector):
    """Minimal concrete connector exposing one public tool and one private method."""

    def __init__(self):
        super().__init__()
        self.connector_type = "x"
        self.connector_category = "database"
        self.private_called = False
        self.public_called = False

    # Abstract surface
    def test_connection(self, *a, **k):
        return {"success": True}

    def discover_schema(self, *a, **k):
        return {"success": True}

    def export(self, *a, **k):
        return {"success": True}

    def validate_config(self, *a, **k):
        return {"valid": True, "errors": []}

    # A legitimate public tool handler + an internal method that must stay unreachable.
    def ping_tool(self, args):
        self.public_called = True
        return {"success": True, "pong": True}

    def _danger(self, args):
        self.private_called = True
        return {"success": True, "secret": "leaked"}


@pytest.mark.parametrize(
    "tool_name",
    [
        "x__danger",  # "x_" prefix strips to "_danger"
        "_danger",  # bare private name
        "x__cleanup_worker",  # strips to "_cleanup_worker"
        "_spawn_worker_process",  # bare internal
    ],
)
def test_handle_tool_call_blocks_private_methods(tool_name):
    p = _Probe()
    res = p._handle_tool_call({"name": tool_name, "arguments": {}})
    assert res.get("success") is False, f"{tool_name!r} was not rejected: {res}"
    assert p.private_called is False, f"private method reachable via {tool_name!r}"


def test_handle_tool_call_allows_public_tools():
    p = _Probe()
    res = p._handle_tool_call({"name": "x_ping_tool", "arguments": {}})
    assert p.public_called is True, "public tool was blocked by the private-method guard"
    assert res.get("pong") is True
