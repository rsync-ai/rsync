"""Regression tests for base_connector._make_request_v2 base_url robustness.

A generated connector whose base_url resolves to an empty string (the classic
`ENV MCP_BASE_URL=""` + `os.getenv('MCP_BASE_URL', default)` footgun — a
set-but-empty env var returns "" instead of the baked default) builds a
RELATIVE request URL like "/pokemon". That is:

  * NOT an SSRF attempt — a relative URL has no host to reach, so the old
    behaviour of blocking it with a cryptic "only http/https allowed; got
    scheme=''" SSRF message misled operators into chasing a phantom security
    problem; and
  * NOT sendable — `requests` raises "No scheme supplied" on a relative URL.

The connector must instead return a clear, actionable CONFIGURATION error and
must NOT attempt any network call.
"""
from __future__ import annotations

import importlib.util
import os
from unittest import mock

_BC_PATH = os.path.join(os.path.dirname(__file__), "..", "base_connector.py")
_spec = importlib.util.spec_from_file_location("base_connector_baseurl", _BC_PATH)
_bc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_bc)


class _MiniConnector(_bc.BaseMCPConnector):
    def test_connection(self, params=None):
        return {"success": True}

    def discover_schema(self, params=None):
        return {"tables": []}

    def validate_config(self, params=None):
        return {"valid": True}

    def export(self, params):
        return {"success": True, "data": []}


def _ok_resp():
    r = mock.Mock()
    r.is_redirect = False
    r.status_code = 200
    r.headers = {}
    r.json.return_value = {"ok": True}
    r.text = '{"ok": true}'
    return r


# Relative / schemeless URLs that a resolved-empty base_url produces:
#   f"{''.rstrip('/')}/{'pokemon'.lstrip('/')}"  ->  "/pokemon"
_RELATIVE_URLS = ["/pokemon", "pokemon", "/v1/charges", "//evil.example.com/x", ""]


def test_relative_url_is_a_config_error_not_ssrf():
    conn = _MiniConnector()
    with mock.patch("requests.request") as req:
        for u in _RELATIVE_URLS:
            success, status, data, _headers = conn._make_request_v2("GET", u)
            assert success is False, f"relative url {u!r} unexpectedly succeeded"
            assert status == 0
            err = (data or {}).get("error", "")
            # Clear, actionable config error — mentions base_url, NOT "SSRF".
            assert "base_url" in err, f"error for {u!r} should name base_url: {err!r}"
            assert "SSRF" not in err, f"relative url {u!r} misclassified as SSRF: {err!r}"
        # A relative URL can never reach the network: no request was attempted.
        assert req.call_count == 0, "a relative URL must not trigger a network call"


def test_absolute_url_still_reaches_the_network():
    """The guard must NOT block a normal absolute URL."""
    conn = _MiniConnector()
    with mock.patch("requests.request", return_value=_ok_resp()) as req:
        success, status, data, _headers = conn._make_request_v2(
            "GET", "https://pokeapi.co/api/v2/pokemon"
        )
    assert success is True
    assert status == 200
    assert req.call_count == 1


def test_none_url_is_a_config_error_not_a_crash():
    conn = _MiniConnector()
    with mock.patch("requests.request") as req:
        success, status, data, _headers = conn._make_request_v2("GET", None)  # type: ignore[arg-type]
    assert success is False
    assert "base_url" in (data or {}).get("error", "")
    assert req.call_count == 0
