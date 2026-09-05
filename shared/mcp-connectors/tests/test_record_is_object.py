"""Regression tests for single-entity reads in PaginationHandler._extract_records.

Some GETs return a single entity whose whole body IS the record (Twilio
/Balance.json → {"balance": ..., "currency": ...}, Jira /myself). With no records
array, _extract_records finds no list and returns [] → 0 rows (the F5 defect).
When the generator flags the resource (record_is_object=True), the handler must
wrap the object as a one-row list. The wrap is applied LAST — only when tiers 1–3
find nothing — so a real collection (its array found normally) is never overridden,
and an unflagged single object still yields [] (byte-identical to prior behavior).
"""
from __future__ import annotations

import importlib.util
import os

_BC_PATH = os.path.join(os.path.dirname(__file__), "..", "base_connector.py")
_spec = importlib.util.spec_from_file_location("base_connector_rio", _BC_PATH)
_bc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_bc)

PaginationHandler = _bc.PaginationHandler


def test_single_object_wrapped_when_flagged():
    h = PaginationHandler(record_is_object=True)
    body = {"balance": "100.50", "currency": "USD", "account_sid": "AC123"}
    assert h._extract_records(body) == [body]


def test_single_object_not_wrapped_when_unflagged():
    # Default behavior must be unchanged: no list, no flag → [] (0 rows).
    h = PaginationHandler()
    body = {"balance": "100.50", "currency": "USD"}
    assert h._extract_records(body) == []


def test_flagged_but_real_collection_array_still_wins():
    # Defensive ordering: even if a resource is flagged, a genuine records array
    # in the response (well-known key) is returned — the wrap never overrides it.
    h = PaginationHandler(record_is_object=True)
    rows = [{"id": 1}, {"id": 2}]
    assert h._extract_records({"data": rows}) == rows


def test_flagged_empty_object_yields_empty():
    # An empty dict has nothing to wrap → [] (don't emit a bogus empty record).
    h = PaginationHandler(record_is_object=True)
    assert h._extract_records({}) == []


def test_flagged_list_input_returned_as_is():
    # A list response is already records — flag is irrelevant.
    h = PaginationHandler(record_is_object=True)
    rows = [{"id": 1}]
    assert h._extract_records(rows) == rows


def test_explicit_data_key_still_wins_over_wrap():
    h = PaginationHandler(record_is_object=True, response_data_key="results")
    rows = [{"id": 9}]
    assert h._extract_records({"results": rows}) == rows
