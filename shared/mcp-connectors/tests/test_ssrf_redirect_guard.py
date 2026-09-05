"""Regression tests for the SSRF redirect guard in base_connector._make_request_v2.

A validated public endpoint must not be able to 30x-redirect the connector to a
private/link-local host (cloud IMDS) or leak the connector's auth header to a
different host on redirect.
"""
from __future__ import annotations

import importlib.util
import os
from unittest import mock

_BC_PATH = os.path.join(os.path.dirname(__file__), "..", "base_connector.py")
_spec = importlib.util.spec_from_file_location("base_connector_ssrf", _BC_PATH)
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


def _redirect_resp(location, from_url="https://api.vendor.com/start"):
    r = mock.Mock()
    r.is_redirect = True
    r.status_code = 302
    r.headers = {"Location": location}
    r.url = from_url
    return r


def _ok_resp():
    r = mock.Mock()
    r.is_redirect = False
    r.status_code = 200
    r.headers = {}
    r.json.return_value = {"ok": True}
    r.text = '{"ok": true}'
    return r


def test_redirect_to_imds_is_blocked_fail_closed():
    conn = _MiniConnector()
    with mock.patch("requests.request") as req:
        req.return_value = _redirect_resp("http://169.254.169.254/latest/meta-data/")
        success, status, data, _headers = conn._make_request_v2(
            "GET", "https://api.vendor.com/start", headers={"Authorization": "Bearer sk_live_x"}
        )
    assert success is False
    assert "SSRF guard (redirect)" in data.get("error", "")


def test_cross_host_redirect_strips_auth_header():
    conn = _MiniConnector()
    # First call: 302 to a different PUBLIC host (passes SSRF check). Second
    # call must NOT carry the Authorization header.
    responses = [
        _redirect_resp("https://evil.example.com/collect", from_url="https://api.vendor.com/start"),
        _ok_resp(),
    ]
    with mock.patch("requests.request", side_effect=responses) as req:
        conn._make_request_v2(
            "GET", "https://api.vendor.com/start", headers={"Authorization": "Bearer sk_live_x"}
        )
    # Second requests.request call = the followed redirect.
    follow_kwargs = req.call_args_list[1].kwargs
    follow_headers = follow_kwargs.get("headers") or {}
    assert "Authorization" not in follow_headers, "auth header leaked to cross-host redirect"


def test_same_host_redirect_keeps_auth_header():
    conn = _MiniConnector()
    responses = [
        _redirect_resp("https://api.vendor.com/next", from_url="https://api.vendor.com/start"),
        _ok_resp(),
    ]
    with mock.patch("requests.request", side_effect=responses) as req:
        conn._make_request_v2(
            "GET", "https://api.vendor.com/start", headers={"Authorization": "Bearer sk_live_x"}
        )
    follow_headers = req.call_args_list[1].kwargs.get("headers") or {}
    assert follow_headers.get("Authorization") == "Bearer sk_live_x"
