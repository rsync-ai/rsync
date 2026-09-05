"""Regression tests for form-encoded write bodies in base_connector._make_request_v2.

Some APIs (Twilio classic, Stripe) require application/x-www-form-urlencoded write
bodies and IGNORE a JSON body (→ 400 "A 'To' number is required" / empty params).
The generated connector passes form_encoded=True; the base helper must then send the
body via requests' data= (which form-encodes a dict) instead of json=. The default
(form_encoded=False) must be byte-identical to the prior json= behavior — every
existing connector relies on it.
"""
from __future__ import annotations

import importlib.util
import os
from unittest import mock

_BC_PATH = os.path.join(os.path.dirname(__file__), "..", "base_connector.py")
_spec = importlib.util.spec_from_file_location("base_connector_form", _BC_PATH)
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


def test_form_encoded_write_uses_data_param():
    conn = _MiniConnector()
    body = {"To": "+15558675310", "From": "+15017122661", "Body": "hi"}
    with mock.patch("requests.request") as req:
        req.return_value = _ok_resp()
        conn._make_request_v2(
            "POST", "https://api.vendor.com/Messages.json",
            json_data=body, form_encoded=True,
        )
    kwargs = req.call_args.kwargs
    # requests form-encodes a dict passed to data= AND sets the
    # application/x-www-form-urlencoded header itself.
    assert kwargs.get("data") == body, "form_encoded=True must send the body via data="
    assert "json" not in kwargs, "form_encoded=True must NOT also pass json="


def test_default_write_uses_json_param_unchanged():
    conn = _MiniConnector()
    body = {"name": "widget", "amount": 42}
    with mock.patch("requests.request") as req:
        req.return_value = _ok_resp()
        conn._make_request_v2(
            "POST", "https://api.vendor.com/objects", json_data=body,
        )  # form_encoded defaults to False
    kwargs = req.call_args.kwargs
    assert kwargs.get("json") == body, "default must send the body via json= (unchanged)"
    assert "data" not in kwargs, "default must NOT pass data="


def test_form_encoded_with_no_body_falls_back_to_json_none():
    # form_encoded=True but no body → nothing to form-encode; behave like json=None
    # (a bodyless form POST). Must not crash or send an empty form dict oddly.
    conn = _MiniConnector()
    with mock.patch("requests.request") as req:
        req.return_value = _ok_resp()
        conn._make_request_v2(
            "POST", "https://api.vendor.com/action", json_data=None, form_encoded=True,
        )
    kwargs = req.call_args.kwargs
    assert kwargs.get("json") is None and "data" not in kwargs
