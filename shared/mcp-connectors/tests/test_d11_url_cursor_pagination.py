"""RED regression for defect d11 (cursor) — the shared runtime PaginationHandler
must follow a FULL next-page URL cursor (Twilio's meta.next_page_uri, Salesforce
nextRecordsUrl) via ``_override_url`` instead of sending the URL as a query-param
value, and its runtime cursor autodiscovery must locate ``meta.next_page_uri``.

Why this matters: Twilio paginates via ``meta.next_page_uri`` — a full URL, not an
opaque token. Generation leaves cursor_path empty (the field isn't a token in the
2xx schema), so runtime autodiscovery must find it AND the cursor loop must FOLLOW
the URL. Today the loop unconditionally does ``params[pagination_param] = cursor``
(base_connector.py), so a URL cursor would be sent as ``?PageToken=https://...``
(400), and ``meta.next_page_uri`` isn't in the autodiscovery ``_CURSOR_PATHS`` —
so Twilio silently truncates to page 1.

RED today:
  - _extract_next_cursor() does not find ``meta.next_page_uri`` (Test A).
  - a URL cursor is applied as a query param, not ``_override_url`` (Test B, D).
GREEN after fix:
  - ``["meta","next_page_uri"]`` / ``["meta","next_page_url"]`` added to
    ``_CURSOR_PATHS``; the cursor loop routes absolute-URL cursors to
    ``_override_url`` (consumed by the connector's _make_request_v2).

Imports the shared runtime base_connector.py directly (no network, no docker).
"""

from __future__ import annotations

import importlib.util
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_BC_PATH = os.path.join(_HERE, "..", "base_connector.py")
_spec = importlib.util.spec_from_file_location("base_connector_d11", _BC_PATH)
_bc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_bc)
PaginationHandler = _bc.PaginationHandler


# A realistic Twilio next-page URL (absolute).
URL2 = (
    "https://api.twilio.com/2010-04-01/Accounts/ACxxxx/Messages.json"
    "?PageSize=50&Page=1&PageToken=PAxxxx"
)


def _handler(**kw):
    kw.setdefault("pagination_type", "cursor")
    kw.setdefault("pagination_param", "PageToken")
    kw.setdefault("limit_param", "PageSize")
    kw.setdefault("max_page_size", 50)
    kw.setdefault("response_data_key", "data")
    return PaginationHandler(**kw)


# --------------------------------------------------------------------------- #
# Test A — runtime autodiscovery must locate Twilio's meta.next_page_uri.
# --------------------------------------------------------------------------- #

def test_extract_cursor_finds_meta_next_page_uri():
    h = _handler()
    got = h._extract_next_cursor(
        {"data": [{"sid": "1"}], "meta": {"next_page_uri": URL2, "page": 0}}
    )
    assert got == URL2, f"meta.next_page_uri should be autodiscovered; got {got!r}"


# --------------------------------------------------------------------------- #
# Test B — an absolute-URL cursor is FOLLOWED via _override_url (not a query param).
# Uses an explicit cursor_path so this isolates the URL-follow behaviour.
# --------------------------------------------------------------------------- #

def test_url_cursor_followed_via_override_url():
    h = _handler(cursor_path="meta.next_page_uri")
    seen = []

    def fetch(params):
        seen.append(dict(params))
        if len(seen) == 1:
            return (True, 200, {"data": [{"sid": "a"}], "meta": {"next_page_uri": URL2}}, {})
        return (True, 200, {"data": [{"sid": "b"}], "meta": {"next_page_uri": None}}, {})

    h.fetch_all_pages(fetch, max_pages=5, initial_params={})
    assert len(seen) >= 2, "pagination stopped after page 1 (URL cursor not followed)"
    p2 = seen[1]
    assert p2.get("_override_url") == URL2, (
        f"page 2 must follow the full URL via _override_url; got {p2!r}"
    )
    assert p2.get("PageToken") != URL2, "the full URL must NOT be sent as a query-param value"


# --------------------------------------------------------------------------- #
# Test C — a plain token cursor still rides the query param (regression guard).
# --------------------------------------------------------------------------- #

def test_token_cursor_still_rides_query_param():
    h = _handler(cursor_path="next_cursor")
    seen = []

    def fetch(params):
        seen.append(dict(params))
        if len(seen) == 1:
            return (True, 200, {"data": [{"sid": "a"}], "next_cursor": "TOK2"}, {})
        return (True, 200, {"data": [{"sid": "b"}], "next_cursor": None}, {})

    h.fetch_all_pages(fetch, max_pages=5, initial_params={})
    assert len(seen) >= 2
    p2 = seen[1]
    assert p2.get("PageToken") == "TOK2", f"token cursor must ride the query param; got {p2!r}"
    assert "_override_url" not in p2, "a token cursor must not be treated as a URL"


# --------------------------------------------------------------------------- #
# Test D — end-to-end Twilio shape via AUTODISCOVERY (empty cursor_path).
# --------------------------------------------------------------------------- #

def test_twilio_shape_paginates_via_autodiscovery():
    h = _handler()  # empty cursor_path -> autodiscovery must kick in
    seen = []

    def fetch(params):
        seen.append(dict(params))
        if len(seen) == 1:
            return (True, 200, {"data": [{"sid": "a"}], "meta": {"next_page_uri": URL2}}, {})
        return (True, 200, {"data": [{"sid": "b"}], "meta": {"next_page_uri": None}}, {})

    records, _errors, _final = h.fetch_all_pages(fetch, max_pages=5, initial_params={})
    assert len(seen) >= 2, "Twilio meta.next_page_uri not autodiscovered -> truncated to page 1"
    assert seen[1].get("_override_url") == URL2
    assert len(records) == 2


# =========================================================================== #
# PR #354 follow-up (post-merge review findings): the REAL Twilio/Salesforce
# shapes are top-level and ROOT-RELATIVE — absolute-only routing left d11
# unfixed for them; resumed runs re-sent the stale URL cursor as a query
# param; followed URLs weren't host-pinned; query-param credentials leaked
# into exception logs.
# =========================================================================== #

ApiHandler = _bc.ApiHandler

# Real legacy-Twilio (2010-04-01) shape: TOP-LEVEL, ROOT-RELATIVE next_page_uri.
REL2 = "/2010-04-01/Accounts/ACxxxx/Messages.json?PageSize=50&Page=1&PageToken=PAxxxx"


def test_legacy_twilio_toplevel_relative_uri_followed():
    """Top-level relative next_page_uri must be autodiscovered AND routed to
    _override_url — not stamped into the PageToken query param (400)."""
    h = _handler()  # empty cursor_path -> autodiscovery
    seen = []

    def fetch(params):
        seen.append(dict(params))
        if len(seen) == 1:
            return (True, 200, {"data": [{"sid": "a"}], "next_page_uri": REL2, "page": 0}, {})
        return (True, 200, {"data": [{"sid": "b"}], "next_page_uri": None}, {})

    records, _errors, _final = h.fetch_all_pages(fetch, max_pages=5, initial_params={})
    assert len(seen) >= 2, "top-level relative next_page_uri not followed -> page-1 truncation"
    assert seen[1].get("_override_url") == REL2
    assert seen[1].get("PageToken") != REL2, "relative URL must not ride the query param"
    assert len(records) == 2


def test_salesforce_relative_nextrecordsurl_followed():
    sf_next = "/services/data/v58.0/query/01gXX0000000001-2000"
    h = _handler(pagination_param="cursor", limit_param="limit")
    seen = []

    def fetch(params):
        seen.append(dict(params))
        if len(seen) == 1:
            return (True, 200, {"data": [{"Id": "a"}], "nextRecordsUrl": sf_next, "done": False}, {})
        return (True, 200, {"data": [{"Id": "b"}], "done": True}, {})

    h.fetch_all_pages(fetch, max_pages=5, initial_params={})
    assert len(seen) >= 2, "relative nextRecordsUrl not followed"
    assert seen[1].get("_override_url") == sf_next
    assert seen[1].get("cursor") != sf_next


def test_resume_url_cursor_strips_stale_query_param():
    """Chunked-continuation resume: the executor passes the prior URL cursor
    back inside initial_params[pagination_param]. It must be routed to
    _override_url and REMOVED from the query params — not sent as both."""
    h = _handler(cursor_path="meta.next_page_uri")
    seen = []

    def fetch(params):
        seen.append(dict(params))
        return (True, 200, {"data": [{"sid": "z"}], "meta": {"next_page_uri": None}}, {})

    h.fetch_all_pages(fetch, max_pages=3, initial_params={"PageToken": URL2})
    p1 = seen[0]
    assert p1.get("_override_url") == URL2, "resume URL cursor must be followed via _override_url"
    assert "PageToken" not in p1, (
        f"stale resume cursor must not ride the query param alongside the override; got {p1!r}"
    )


class _FakeGenConnector:
    """Mimics a generated connector's _make_request_v2 signature."""

    base_url = "https://api.twilio.com"

    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def _make_request_v2(self, method, endpoint, config, params, data):
        self.calls.append({"endpoint": endpoint, "params": dict(params or {})})
        page = self.pages[min(len(self.calls) - 1, len(self.pages) - 1)]
        return (True, 200, page, {})


def _export(fake, config, **kw):
    handler = ApiHandler(
        connector=fake, pagination_type="cursor",
        pagination_param="PageToken", limit_param="PageSize", max_page_size=50,
    )
    return handler.export_resource(
        connector=fake, config=config, resource="messages",
        endpoint="/2010-04-01/Accounts/ACxxxx/Messages.json", params={},
        max_pages=5, max_records=100,
        resource_config={
            "pagination_type": "cursor", "pagination_param": "PageToken",
            "limit_param": "PageSize", "max_page_size": 50,
            "response_data_key": "data",
        }, **kw,
    )


def test_apihandler_resolves_relative_cursor_against_base_url():
    fake = _FakeGenConnector([
        {"data": [{"sid": "a"}], "next_page_uri": REL2},
        {"data": [{"sid": "b"}], "next_page_uri": None},
    ])
    result = _export(fake, {"base_url": "https://api.twilio.com"})
    assert len(fake.calls) == 2, "relative cursor not followed end-to-end"
    p2 = fake.calls[1]["params"]
    assert p2.get("_override_url") == "https://api.twilio.com" + REL2, (
        f"relative cursor must be resolved absolute against base_url; got {p2!r}"
    )
    assert "PageToken" not in p2
    assert len(result.records) == 2


def test_apihandler_pins_cursor_url_to_connector_host():
    """A cursor URL pointing at a foreign host must NOT be requested — the
    connector's auth headers would go to a third party."""
    fake = _FakeGenConnector([
        {"data": [{"sid": "a"}], "next_page_uri": "https://evil.example.com/steal"},
    ])
    result = _export(fake, {"base_url": "https://api.twilio.com"})
    assert len(fake.calls) == 1, "foreign-host cursor URL must never reach the connector"
    assert result.errors, "host-mismatch block must surface as a page error"
    assert any("does not match" in str(e.get("data")) for e in result.errors)
    assert len(result.records) == 1  # page-1 data kept


def test_apihandler_allows_same_host_with_explicit_default_port():
    """An explicit :443 on an https cursor URL is the SAME endpoint — the pin
    must normalize default ports, not string-compare netloc."""
    fake = _FakeGenConnector([
        {"data": [{"sid": "a"}], "next_page_uri": "https://api.twilio.com:443" + REL2},
        {"data": [{"sid": "b"}], "next_page_uri": None},
    ])
    result = _export(fake, {"base_url": "https://api.twilio.com"})
    assert len(fake.calls) == 2, "explicit default port must not trip the host pin"
    assert len(result.records) == 2
    assert not result.errors


def test_apihandler_fails_closed_on_relative_cursor_without_base():
    class _NoBase:
        def __init__(self):
            self.calls = []

        def _make_request_v2(self, method, endpoint, config, params, data):
            self.calls.append(dict(params or {}))
            return (True, 200, {"data": [{"sid": "a"}], "next_page_uri": REL2}, {})

    fake = _NoBase()
    result = _export(fake, {})
    assert len(fake.calls) == 1, "unresolvable relative cursor must not produce a request"
    assert result.errors and any("no base_url" in str(e.get("data")) for e in result.errors)


# --------------------------------------------------------------------------- #
# Query-param credential scrubbing in exception logs (d07 api_key_query leak).
# --------------------------------------------------------------------------- #

def test_scrub_url_secrets_masks_credentials():
    msg = (
        "HTTPSConnectionPool(host='api.pipedrive.com', port=443): Max retries "
        "exceeded with url: /v1/deals?limit=100&api_token=abc123SECRET "
        "(Caused by ConnectTimeoutError)"
    )
    out = _bc._scrub_url_secrets(msg)
    assert "abc123SECRET" not in out
    assert "api_token=***" in out
    assert "/v1/deals?limit=100" in out  # harmless parts survive


def test_scrub_url_secrets_preserves_plain_params():
    msg = "GET /v1/deals?limit=100&start=0 timed out after 30s"
    assert _bc._scrub_url_secrets(msg) == msg
