"""Regression tests for the base_connector side of the NASA APOD real-data E2E
fix (tool-generator campaign, Finding C).

Finding C — an UNDECLARED `limit` must NOT be injected. When a generated
connector's resource carries an EMPTY ``limit_param`` (the architect now emits ""
when the OpenAPI op declares no pagination params at all), PaginationHandler must
send NO limit query param. Injecting an undeclared ``limit=100`` makes strict APIs
reject the request (NASA APOD → HTTP 400 "incorrect field passed"). A resource that
DECLARES a limit param is unaffected.

The NASA-realistic path is exercised end-to-end: empty limit_param + the
record_is_object flag (the generator now sets it for degenerate/opaque-array specs
— see test_infer_record_shape_opaque_array.py) → a bare-object 2xx yields exactly
one wrapped row, with only the caller's own params on the wire.
"""
from __future__ import annotations

import importlib.util
import os

_BC_PATH = os.path.join(os.path.dirname(__file__), "..", "base_connector.py")
_spec = importlib.util.spec_from_file_location("base_connector_pag", _BC_PATH)
_bc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_bc)
PaginationHandler = _bc.PaginationHandler
ApiHandler = _bc.ApiHandler

_BARE_ENTITY = {"date": "2026-07-06", "title": "Dueling Bands", "url": "u",
                "media_type": "image", "explanation": "..."}


def test_empty_limit_param_cursor_sends_no_limit_and_wraps():
    # NASA-realistic resource config: cursor default (no pagination declared),
    # EMPTY limit_param, and record_is_object=True (set by the generator for the
    # opaque-array spec). Expect: only the caller's api_key on the wire, exactly
    # one page, and the bare object wrapped as one row.
    ph = PaginationHandler(pagination_type="cursor", pagination_param="after",
                           limit_param="", max_page_size=100, record_is_object=True)
    seen = []

    def fetch(params):
        seen.append(dict(params))
        return True, 200, dict(_BARE_ENTITY), {}

    records, errors, _cursor = ph.fetch_all_pages(
        fetch_page_fn=fetch, max_pages=3, max_records=100,
        initial_params={"api_key": "DEMO_KEY"},
    )
    assert len(seen) == 1                       # bare object, no cursor → 1 page
    assert seen[0] == {"api_key": "DEMO_KEY"}   # NO limit injected
    assert "" not in seen[0]                    # and no empty-string key ("" : 100)
    assert "limit" not in seen[0]
    assert records == [_BARE_ENTITY]            # wrapped via record_is_object
    assert errors == []


def test_empty_limit_param_offset_sends_offset_but_no_limit():
    ph = PaginationHandler(pagination_type="offset", pagination_param="offset",
                           limit_param="", max_page_size=100)
    seen = []

    def fetch(params):
        seen.append(dict(params))
        return True, 200, {"results": []}, {}  # empty page → stop

    ph.fetch_all_pages(fetch_page_fn=fetch, max_pages=2, max_records=100,
                       initial_params={})
    assert seen[0].get("offset") == 0
    assert "limit" not in seen[0]
    assert "" not in seen[0]


def test_declared_limit_param_is_still_injected():
    ph = PaginationHandler(pagination_type="offset", pagination_param="offset",
                           limit_param="limit", max_page_size=50)
    seen = []

    def fetch(params):
        seen.append(dict(params))
        return True, 200, {"results": [{"a": 1}]}, {}  # < page size → stop

    records, _errors, _cursor = ph.fetch_all_pages(
        fetch_page_fn=fetch, max_pages=3, max_records=100, initial_params={},
    )
    assert seen[0].get("limit") == 50
    assert seen[0].get("offset") == 0
    assert records == [{"a": 1}]


def test_link_pagination_empty_limit_sends_no_limit():
    # Coverage gap flagged in adversarial review: the link branch must also honour
    # an empty limit_param on the first page (no next_url yet).
    ph = PaginationHandler(pagination_type="link", pagination_param="",
                           limit_param="", max_page_size=100)
    seen = []

    def fetch(params):
        seen.append(dict(params))
        return True, 200, {"items": []}, {}  # empty page → stop

    ph.fetch_all_pages(fetch_page_fn=fetch, max_pages=2, max_records=100,
                       initial_params={"api_key": "K"})
    assert seen[0] == {"api_key": "K"}
    assert "limit" not in seen[0] and "" not in seen[0]


def test_record_is_object_true_but_real_array_still_wins():
    # Finding D robustness: record_is_object is the LAST-RESORT wrap. If the
    # endpoint actually returns a real list, the list must win — the flag must
    # not wrap a genuine array into a single row.
    ph = PaginationHandler(pagination_type="cursor", pagination_param="after",
                           limit_param="", max_page_size=100, record_is_object=True)

    def fetch(params):
        return True, 200, [{"id": 1}, {"id": 2}], {}  # a real array

    records, errors, _cursor = ph.fetch_all_pages(
        fetch_page_fn=fetch, max_pages=1, max_records=100, initial_params={},
    )
    assert records == [{"id": 1}, {"id": 2}]   # array wins, NOT [[...]] or 1 row
    assert errors == []


# --------------------------------------------------------------------------- #
# ApiHandler.export_resource — the REAL generated-connector export path.
# Adversarial review found the ResourceConfig `limit_param=""` was silently
# coerced back to the connector-wide default here (base_connector.py:3911 `or`),
# re-injecting the undeclared limit for multi-resource connectors. The direct
# PaginationHandler tests above bypass this coercion site, so these exercise it.
# --------------------------------------------------------------------------- #
class _FakeConn:
    """Minimal connector implementing _make_request_v2; records outgoing params."""
    base_url = "https://api.nasa.gov/planetary"

    def __init__(self, response):
        self._response = response
        self.seen = []

    def _make_request_v2(self, method, endpoint, config, params=None, data=None):
        self.seen.append(dict(params or {}))
        return True, 200, self._response, {}


def test_export_resource_empty_limit_param_not_coerced_to_connector_default():
    # Connector-wide default declares a limit (resources[0] had pagination), but
    # THIS resource declares none (architect emits limit_param=""). The per-call
    # empty value must be honoured — no limit re-injected (NASA APOD → HTTP 400).
    conn = _FakeConn({"date": "2026-07-06", "title": "Dueling Bands"})
    handler = ApiHandler(conn, pagination_type="offset", pagination_param="offset",
                         limit_param="limit", max_page_size=100)
    res = handler.export_resource(
        connector=conn, config={"api_key": "DEMO_KEY"}, resource="apod",
        endpoint="/apod", params={"api_key": "DEMO_KEY"}, max_pages=1, max_records=5,
        resource_config={"pagination_type": "cursor", "pagination_param": "after",
                         "limit_param": "", "record_is_object": True},
    )
    assert conn.seen, "no request was made"
    assert "limit" not in conn.seen[0], f"undeclared limit re-injected: {conn.seen[0]!r}"
    assert "" not in conn.seen[0], f"empty-key limit re-injected: {conn.seen[0]!r}"
    assert conn.seen[0] == {"api_key": "DEMO_KEY"}
    assert res.records == [{"date": "2026-07-06", "title": "Dueling Bands"}]  # wrapped


def test_export_resource_absent_limit_param_falls_back_to_connector_default():
    # Negative control: when resource_config carries NO limit_param key at all
    # (e.g. the _get_resource_config unknown-resource fallback), the connector-wide
    # default must still be used — the fix must not suppress declared pagination.
    conn = _FakeConn({"results": [{"id": 1}]})
    handler = ApiHandler(conn, pagination_type="offset", pagination_param="offset",
                         limit_param="limit", max_page_size=25)
    handler.export_resource(
        connector=conn, config={}, resource="things", endpoint="/things",
        params={}, max_pages=1, max_records=5,
        resource_config={"pagination_type": "offset", "pagination_param": "offset",
                         "response_data_key": "results"},  # NO limit_param key
    )
    assert conn.seen[0].get("limit") == 25, f"connector default dropped: {conn.seen[0]!r}"
