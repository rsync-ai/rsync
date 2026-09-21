"""list_namespaces on BigQuery lists the project's datasets (the shared
BigQueryWarehouseAdapter answers; the connector delegates) and reports the
configured dataset_id as "current".

Offline: _bq_fakes swaps the adapter's _client seam; no GCP needed.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
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

CONFIG = {"project_id": "acme", "dataset_id": "raw"}


def test_adapter_lists_the_projects_datasets():
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters)
    client.datasets = ["raw", "marts", "raw"]
    out = adapter.list_namespaces(CONFIG)
    assert out == {"success": True, "namespaces": ["marts", "raw"], "current": "raw"}, out
    assert client.listed_projects == ["acme"]


def test_adapter_needs_a_project():
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters)
    out = adapter.list_namespaces({"dataset_id": "raw"})
    assert out["success"] is False and "project_id" in out["error"], out
    assert client.listed_projects == []


def test_adapter_failure_is_an_error():
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters)

    def boom(project=None):
        raise RuntimeError("403 Access Denied")

    client.list_datasets = boom
    out = adapter.list_namespaces(CONFIG)
    assert out["success"] is False and "403" in out["error"], out


def test_connector_delegates_to_the_adapter():
    s = C.BigqueryMCPServer()
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters)
    client.datasets = ["raw"]
    s._warehouse_adapter = adapter
    out = s.list_namespaces({"config": CONFIG})
    assert out == {"success": True, "namespaces": ["raw"], "current": "raw"}, out


def test_connector_without_the_adapter_says_so():
    s = C.BigqueryMCPServer()
    s._warehouse_adapter = None
    out = s.list_namespaces({"config": CONFIG})
    assert out["success"] is False and "adapter unavailable" in out["error"], out
