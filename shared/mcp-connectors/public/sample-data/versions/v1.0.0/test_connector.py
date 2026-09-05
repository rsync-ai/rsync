#!/usr/bin/env python3
"""Offline unit tests for the sample-data source connector.

Runs with either pytest (`pytest test_connector.py`) or plain stdlib
(`python3 test_connector.py`) — no third-party dependencies, matching the
connector itself.
"""

import connector
from connector import TABLES, SampleDataMCPServer

SRV = SampleDataMCPServer()


def test_test_connection_ok():
    res = SRV.test_connection({})
    assert res["success"] is True
    assert res["version"] == connector.CONNECTOR_VERSION


def test_validate_config_needs_nothing():
    res = SRV.validate_config({})
    assert res["success"] is True and res["valid"] is True
    assert res["errors"] == []


def test_discover_schema_shape():
    res = SRV.discover_schema({})
    assert res["success"] is True
    assert res["total_tables"] == 3
    names = {t["name"] for t in res["tables"]}
    assert names == {"customers", "orders", "products"}
    for t in res["tables"]:
        assert t["row_count"] == len(TABLES[t["name"]]["rows"])
        assert t["primary_keys"] == TABLES[t["name"]]["primary_key"]
        # every column carries name/type/nullable
        for col in t["columns"]:
            assert set(col.keys()) == {"name", "type", "nullable"}


def test_export_full_table_signals_eof():
    res = SRV.export({"table": "customers"})
    assert res["success"] is True
    assert res["row_count"] == 12
    assert len(res["data"]) == 12
    assert res["has_more"] is False  # whole table fits in the default page
    assert res["columns"] == [c["name"] for c in TABLES["customers"]["columns"]]
    # rows are plain dicts carrying every column
    assert set(res["data"][0].keys()) == set(res["columns"])


def test_export_pagination_and_eof_boundary():
    orders = TABLES["orders"]["rows"]
    assert len(orders) == 20

    # first page of 5 -> more remain
    p1 = SRV.export({"table": "orders", "limit": 5, "offset": 0})
    assert p1["row_count"] == 5 and p1["has_more"] is True
    assert p1["data"][0]["order_id"] == orders[0]["order_id"]

    # a full page that lands exactly on the end -> no more
    p_end = SRV.export({"table": "orders", "limit": 5, "offset": 15})
    assert p_end["row_count"] == 5 and p_end["has_more"] is False

    # a short final page -> row_count < limit AND has_more False (double EOF signal)
    p_short = SRV.export({"table": "orders", "limit": 7, "offset": 18})
    assert p_short["row_count"] == 2 and p_short["has_more"] is False

    # offset past the end -> empty page, EOF
    p_past = SRV.export({"table": "orders", "limit": 5, "offset": 100})
    assert p_past["row_count"] == 0 and p_past["has_more"] is False and p_past["data"] == []


def test_export_unknown_table_fails_cleanly():
    res = SRV.export({"table": "not_a_table"})
    assert res["success"] is False
    assert "Unknown table" in res["error"]


def test_export_tolerates_schema_qualified_and_bad_paging():
    # schema-qualified / quoted names resolve to the bare table
    res = SRV.export({"table": '"public"."products"', "limit": "oops", "offset": None})
    assert res["success"] is True
    assert res["row_count"] == 10  # falls back to default limit, offset 0


def test_demo_data_is_referentially_consistent():
    """Guards the datasets so the demo keeps joining cleanly in Explorer."""
    customer_ids = {c["customer_id"] for c in TABLES["customers"]["rows"]}
    product_ids = {p["product_id"] for p in TABLES["products"]["rows"]}
    for o in TABLES["orders"]["rows"]:
        assert o["customer_id"] in customer_ids, f"order {o['order_id']} → missing customer"
        assert o["product_id"] in product_ids, f"order {o['order_id']} → missing product"


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as e:
                failures += 1
                print(f"FAIL {name}: {e}")
    raise SystemExit(1 if failures else 0)
