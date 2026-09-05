"""Regression tests for cross-connection state leakage on the shared connector
instance (KI-RESTMCP-STICKY-BASE-URL).

``create_http_app()`` builds **one** connector per process and
``_handle_tool_call()`` re-runs ``configure_from_connection()`` on that shared
instance for every request. Every assignment inside that method used to be
guarded — the auth ``if/elif`` chain has no ``else``, and ``base_url`` was
written only ``if 'base_url' in connection_config`` — so a request whose config
omitted a field silently kept the **previous** request's value.

Concretely, with connections A and B served by the same container:

    A: {"api_key": "sk_live_A", "base_url": "https://a.example.com"}
    B: {}                       -> still holding A's key, still pointed at A's host

That is a cross-tenant credential leak in both directions: B authenticates as A,
and (per the SSRF-guard comment at the top of ``base_connector``) a ``base_url``
that survives from A means B's own credential is sent to A's host, since request
headers are rebuilt from the *current* config on every call.

The fix is reset-then-apply: ``_reset_connection_state()`` restores the
per-connection fields to their post-``__init__`` values before the new config is
applied. It is snapshot-and-restore rather than a blind wipe because
``__init__`` seeds real defaults from the environment
(``api_key = os.getenv('MCP_API_KEY', '')``, ``base_url`` from
``MCP_BASE_URL``) that a reset-to-``None`` would destroy — see
``test_env_configured_defaults_survive_the_reset``.

Sibling footgun, already covered by ``test_base_url_robustness.py``: the
two-arg ``os.getenv(k, default)`` returns ``""`` for a set-but-empty env var,
which is what made the stale ``base_url`` reachable in the first place.
"""
from __future__ import annotations

import importlib.util
import os
from unittest import mock

_BC_PATH = os.path.join(os.path.dirname(__file__), "..", "base_connector.py")
_spec = importlib.util.spec_from_file_location("base_connector_connstate", _BC_PATH)
_bc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_bc)


class _MiniConnector(_bc.BaseMCPConnector):
    """Mirrors a generated REST connector.

    ``BaseMCPConnector.__init__`` seeds ``api_key`` from ``MCP_API_KEY`` but
    never touches ``base_url`` — resolving the endpoint is the *generated
    subclass's* job. Reproducing that here is what makes the pristine snapshot
    under test the same one production takes.
    """

    SPEC_BASE_URL = "https://spec.example.com"

    def __init__(self):
        super().__init__()
        # `os.getenv(k) or default`, never the two-arg form: a set-but-empty
        # MCP_BASE_URL returns "" and the default would never apply.
        self.base_url = os.getenv("MCP_BASE_URL") or self.SPEC_BASE_URL

    def test_connection(self, params=None):
        return {"success": True}

    def discover_schema(self, params=None):
        return {"tables": []}

    def validate_config(self, params=None):
        return {"valid": True}

    def export(self, params):
        return {"success": True, "data": []}


_TENANT_A = {
    "id": "conn-A",
    "api_key": "sk_live_TENANT_A",
    "base_url": "https://a.tenant.example.com",
}
# Tenant B's config carries NO credential and NO base_url. This is the shape
# that leaks: every write below it in configure_from_connection is guarded.
_TENANT_B = {"id": "conn-B"}

_STATE_FIELDS = ("api_key", "auth_type", "base_url", "_oauth_token")


def _state(conn):
    return {f: getattr(conn, f, None) for f in _STATE_FIELDS}


def _fresh(**env):
    """A connector built under a controlled environment, as the container does."""
    with mock.patch.dict(os.environ, env, clear=False):
        for k in ("MCP_API_KEY", "MCP_BASE_URL"):
            if k not in env:
                os.environ.pop(k, None)
        return _MiniConnector()


def test_credential_does_not_survive_into_the_next_connection():
    conn = _fresh()
    pristine = _state(conn)

    conn.configure_from_connection(_TENANT_A)
    assert conn.api_key == "sk_live_TENANT_A", "precondition: A's key was applied"

    conn.configure_from_connection(_TENANT_B)
    assert conn.api_key != "sk_live_TENANT_A", (
        "connection B is holding connection A's api_key — every call B makes "
        "authenticates as A"
    )
    assert conn.api_key == pristine["api_key"]


def test_base_url_does_not_survive_into_the_next_connection():
    conn = _fresh()
    pristine = _state(conn)

    conn.configure_from_connection(_TENANT_A)
    assert conn.base_url == "https://a.tenant.example.com"

    conn.configure_from_connection(_TENANT_B)
    assert conn.base_url != "https://a.tenant.example.com", (
        "connection B would send its own credential to connection A's host"
    )
    assert conn.base_url == pristine["base_url"]


def test_oauth_token_does_not_survive_into_the_next_connection():
    conn = _fresh()
    pristine = _state(conn)

    conn.configure_from_connection(
        {"id": "conn-A", "oauth_token": {"access_token": "at_TENANT_A"}}
    )
    conn.configure_from_connection(_TENANT_B)

    assert conn._oauth_token == pristine["_oauth_token"], (
        "connection B inherited connection A's OAuth token"
    )
    assert conn.auth_type == pristine["auth_type"], (
        "auth_type stayed on 'oauth' after a connection that supplied no token"
    )


def test_whole_connection_state_returns_to_pristine():
    """The general invariant, not just the three fields enumerated above.

    A connection that specifies nothing must leave the instance exactly as a
    freshly constructed one — otherwise some field added later leaks silently.
    """
    conn = _fresh()
    pristine = _state(conn)

    conn.configure_from_connection(_TENANT_A)
    conn.configure_from_connection(_TENANT_B)

    assert _state(conn) == pristine


def test_env_configured_defaults_survive_the_reset():
    """The reset must restore ``__init__``'s values, not ``None``.

    Self-hosted deployments configure a connector purely through
    ``MCP_API_KEY`` / ``MCP_BASE_URL`` and send configs with no credential at
    all. A blind wipe would break every one of them, so this test is what keeps
    the fix from being written as ``self.api_key = None``.
    """
    conn = _fresh(MCP_API_KEY="env_default_key", MCP_BASE_URL="https://env.example.com")
    assert conn.api_key == "env_default_key", "precondition: env default applied"

    conn.configure_from_connection(_TENANT_A)
    conn.configure_from_connection(_TENANT_B)

    assert conn.api_key == "env_default_key", (
        "the reset wiped the MCP_API_KEY default instead of restoring it"
    )
    assert conn.base_url == "https://env.example.com"


def test_reset_is_actually_reached_from_configure_from_connection():
    """Guards the wiring, not just the outcome.

    Every assertion above would also pass if some *other* code path happened to
    clear the fields. Asserting the call itself is what makes this suite
    specific to the fix.
    """
    conn = _fresh()
    with mock.patch.object(
        conn, "_reset_connection_state", wraps=conn._reset_connection_state
    ) as reset:
        conn.configure_from_connection(_TENANT_A)
        conn.configure_from_connection(_TENANT_B)
    assert reset.call_count == 2, (
        "configure_from_connection must reset on EVERY call, not just the first"
    )
