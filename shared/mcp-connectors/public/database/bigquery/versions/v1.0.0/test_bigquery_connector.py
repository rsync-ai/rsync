#!/usr/bin/env python3
"""Offline unit tests for the BigQuery MCP connector (thin server over the adapter).

Pins the connector's contract without GCP:
  - identity + capability flags (data_warehouse, source+dest, no CDC in v1.0.0);
  - the DestinationLoadSpec advertises the optimized bulk path
    (load_method=bq_load_job, staged_load_available) so the orchestrator can
    discover it via load_strategy_capability();
  - every op delegates to the shared BigQueryWarehouseAdapter (so the optimized
    export/load live in one place);
  - metadata.json / spec.json are valid and declare the GCP auth config with the
    service-account secret marked.

Run standalone:  python3 test_bigquery_connector.py
Under pytest:    pytest test_bigquery_connector.py -q
"""
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)  # `import connector`, `import _bq_fakes`
# Walk up to locate base_connector.py (shared/mcp-connectors) and
# warehouse_adapters.py (shared/mcp-connectors/public).
_d = _HERE
for _ in range(8):
    if os.path.exists(os.path.join(_d, "base_connector.py")):
        sys.path.insert(0, _d)
    if os.path.exists(os.path.join(_d, "warehouse_adapters.py")):
        sys.path.insert(0, _d)
    _d = os.path.dirname(_d)

import connector as C  # noqa: E402
import warehouse_adapters  # noqa: E402
import _bq_fakes  # noqa: E402


def _server():
    return C.BigqueryMCPServer()


# ============================== identity / flags =============================

def test_connector_identity_and_capability_flags():
    s = _server()
    assert s.connector_type == "bigquery", s.connector_type
    assert s.connector_category == "data_warehouse", s.connector_category
    assert s.supports_source is True
    assert s.supports_destination is True
    assert s.supports_cdc is False  # CDC merge stays dormant in v1.0.0


# ============================ load spec / capability =========================

def test_load_spec_declares_bulk_load_job():
    s = _server()
    assert s.load_spec.load_method == "bq_load_job", s.load_spec
    assert s.load_spec.merge_method == "merge", s.load_spec
    assert s.load_spec.supports_staging is True, s.load_spec


def test_load_strategy_capability_is_discoverable():
    s = _server()
    cap = s.load_strategy_capability()
    assert cap["load_method"] == "bq_load_job", cap
    assert cap["staged_load_available"] is True, cap


# ============================== adapter delegation ==========================

def test_uses_shared_bigquery_adapter():
    s = _server()
    assert isinstance(s._warehouse_adapter, warehouse_adapters.BigQueryWarehouseAdapter)


def test_export_delegates_to_adapter_pagination():
    """server.export must return the adapter's paginated envelope."""
    s = _server()
    # Swap the adapter's client/bq seams for fakes (offline).
    rows = [{"id": 1}, {"id": 2}, {"id": 3}]
    fake = _bq_fakes.FakeClient(query_rows=rows)
    s._warehouse_adapter._client = lambda config: fake
    s._warehouse_adapter._bq = lambda: _bq_fakes.FakeBigQueryModule()
    out = s.export({"config": {"project_id": "p", "dataset_id": "d"},
                    "table": "t", "cursor_column": "id", "limit": 3})
    assert out["success"] is True, out
    assert out["paging_mode"] == "keyset", out
    assert out["next_cursor"] == 3, out


def test_load_delegates_to_adapter_bulk_path():
    """server.load must run the adapter bulk path (load job when flag on)."""
    s = _server()
    fake = _bq_fakes.FakeClient()
    s._warehouse_adapter._client = lambda config: fake
    s._warehouse_adapter._bq = lambda: _bq_fakes.FakeBigQueryModule()
    big = [{"id": i} for i in range(50)]
    prev = os.environ.get("RSYNC_BQ_LOAD_JOB")
    os.environ["RSYNC_BQ_LOAD_JOB"] = "true"
    try:
        out = s.load({"config": {"project_id": "p", "dataset_id": "d"},
                      "table": "t", "data": big, "staging_threshold_rows": 10})
    finally:
        if prev is None:
            os.environ.pop("RSYNC_BQ_LOAD_JOB", None)
        else:
            os.environ["RSYNC_BQ_LOAD_JOB"] = prev
    assert out["success"] is True, out
    assert out["method"] == "load_job", out
    assert len(fake.loaded) == 1, fake.loaded


# ============================== metadata / spec =============================

def _load_json(name):
    with open(os.path.join(_HERE, name), "r", encoding="utf-8") as f:
        return json.load(f)


def test_metadata_json_valid_and_warehouse():
    m = _load_json("metadata.json")
    assert m.get("category") == "data_warehouse", m.get("category")
    assert m.get("supports_source") is True
    assert m.get("supports_destination") is True
    req = m.get("required_config") or []
    assert "project_id" in req and "dataset_id" in req, req


def test_spec_json_marks_service_account_secret():
    spec = _load_json("spec.json")
    fields = {f["name"]: f for f in (spec.get("config_fields") or [])}
    assert "service_account_json" in fields, list(fields)
    assert fields["service_account_json"].get("secret") is True, fields["service_account_json"]
    assert "project_id" in fields and "dataset_id" in fields, list(fields)


# ================================= runner ===================================

def _run():
    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_run())
