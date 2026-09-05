"""Regression tests for OData v4 (@odata.nextLink) cursor pagination in the shared
base_connector runtime (tool-generator campaign, Dynamics 365 fix #1).

A Microsoft Dataverse / Dynamics 365 list page returns its server-driven paging
continuation as a single JSON key literally named ``@odata.nextLink`` whose value
is a FULL next-page URL (an opaque ``$skiptoken``). Before the fix
``@odata.nextLink`` was absent from ``PaginationHandler._extract_next_cursor``'s
``_CURSOR_PATHS``, so cursor auto-discovery never found it and any OData list
SILENTLY stopped after the first server page — truncating every table larger than
one page (Dataverse default 5000 rows) with no error. The cursor loop already
follows a full-URL cursor via ``_override_url`` (like Salesforce
``nextRecordsUrl``); these lock in that ``@odata.nextLink`` is discovered and
followed to exhaustion.
"""
from __future__ import annotations

import importlib.util
import os

_BC_PATH = os.path.join(os.path.dirname(__file__), "..", "base_connector.py")
_spec = importlib.util.spec_from_file_location("base_connector_odata", _BC_PATH)
_bc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_bc)
PaginationHandler = _bc.PaginationHandler


def test_extract_next_cursor_finds_odata_nextlink():
    ph = PaginationHandler(pagination_type="cursor", pagination_param="after",
                           limit_param="")
    url = "https://org.crm.dynamics.com/api/data/v9.2/accounts?$skiptoken=%3Ccookie%3E"
    got = ph._extract_next_cursor(
        {"@odata.context": "c", "@odata.nextLink": url, "value": [{"accountid": "1"}]}
    )
    assert got == url, f"@odata.nextLink not discovered: {got!r}"


def test_no_nextlink_means_last_page():
    ph = PaginationHandler(pagination_type="cursor", pagination_param="after",
                           limit_param="")
    assert ph._extract_next_cursor({"@odata.context": "c", "value": []}) is None


def test_odata_nextlink_paginates_to_exhaustion_and_follows_full_url():
    ph = PaginationHandler(pagination_type="cursor", pagination_param="after",
                           limit_param="", max_page_size=100, response_data_key="value")
    next_url = "https://org.crm.dynamics.com/api/data/v9.2/accounts?$skiptoken=P2"
    seen = []

    def fetch(params):
        seen.append(dict(params))
        if "_override_url" not in params:
            # page 1: has a nextLink → exactly one more page
            return True, 200, {"@odata.context": "c", "@odata.nextLink": next_url,
                               "value": [{"accountid": "1"}]}, {}
        # page 2 (followed via _override_url): no nextLink → stop
        return True, 200, {"@odata.context": "c", "value": [{"accountid": "2"}]}, {}

    records, errors, _cursor = ph.fetch_all_pages(
        fetch_page_fn=fetch, max_pages=10, max_records=100, initial_params={},
    )
    assert len(seen) == 2, f"expected 2 server pages, got {len(seen)}: {seen!r}"
    # Page 2 followed the FULL @odata.nextLink URL via _override_url — NOT ?after=<url>.
    assert seen[1].get("_override_url") == next_url, f"nextLink not followed: {seen[1]!r}"
    assert "after" not in seen[1], f"URL wrongly sent as query param: {seen[1]!r}"
    assert records == [{"accountid": "1"}, {"accountid": "2"}], f"got {records!r}"
    assert errors == []


def test_odata_value_envelope_auto_extracts_without_declared_key():
    # `accounts` can come out with response_data_key="" (degraded envelope); the
    # runtime must still extract the value[] array via RESPONSE_DATA_KEYS, and
    # @odata.nextLink must still drive pagination.
    ph = PaginationHandler(pagination_type="cursor", pagination_param="after",
                           limit_param="", max_page_size=100, response_data_key="")
    nxt = "https://org.crm.dynamics.com/api/data/v9.2/contacts?$skiptoken=P2"
    seen = []

    def fetch(params):
        seen.append(dict(params))
        if "_override_url" not in params:
            return True, 200, {"@odata.nextLink": nxt, "value": [{"contactid": "a"}]}, {}
        return True, 200, {"value": [{"contactid": "b"}]}, {}

    records, errors, _cursor = ph.fetch_all_pages(
        fetch_page_fn=fetch, max_pages=10, max_records=100, initial_params={},
    )
    assert len(seen) == 2, f"expected 2 pages, got {seen!r}"
    assert records == [{"contactid": "a"}, {"contactid": "b"}], f"got {records!r}"
    assert errors == []
