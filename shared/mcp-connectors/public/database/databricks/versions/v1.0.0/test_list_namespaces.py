"""list_namespaces on Databricks lists the schemas that hold user tables in
the configured catalog, by the filter discover_schema uses
(_USER_TABLES_WHERE), and reports the configured schema as "current".

Offline: _db_fakes swaps the _connect seam; no workspace needed.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import connector as dbx  # noqa: E402
import _db_fakes  # noqa: E402

CFG = {"config": {"server_hostname": "dbc-x.cloud.databricks.com",
                  "http_path": "/sql/1.0/warehouses/abc", "access_token": "dapi123",
                  "catalog": "main", "schema": "default"}}
WHERE = dbx.DatabricksMCPServer._USER_TABLES_WHERE


def test_lists_each_schema_once_in_the_catalog():
    rows = [{"table_schema": "sales"}, {"table_schema": "default"}, {"table_schema": "sales"}]
    s, conn = _db_fakes.make_connector(dbx, rows=rows)
    out = s.list_namespaces(CFG)
    assert out == {"success": True, "namespaces": ["default", "sales"], "current": "default"}, out
    sql = conn.all_sql()[-1]
    assert "FROM `main`.information_schema.tables" in sql, sql
    assert "SELECT DISTINCT table_schema" in sql and WHERE in sql, sql
    assert conn.closed


def test_without_a_catalog_the_session_default_answers():
    s, conn = _db_fakes.make_connector(dbx, rows=[])
    cfg = {"config": {k: v for k, v in CFG["config"].items() if k != "catalog"}}
    out = s.list_namespaces(cfg)
    assert out["success"] is True and out["namespaces"] == [], out
    assert "FROM information_schema.tables" in conn.all_sql()[-1]


def test_discovery_reads_the_same_view_with_the_same_where():
    s, conn = _db_fakes.make_connector(dbx, rows=[{"table_schema": "default", "table_name": "t"}])
    s.discover_schema(CFG)
    sql = conn.all_sql()[-1]
    assert "FROM `main`.information_schema.tables" in sql and WHERE in sql, sql


def test_connection_failure_is_an_error():
    s, _ = _db_fakes.make_connector(dbx)

    def boom(config):
        raise RuntimeError("401")

    s._connect = boom
    out = s.list_namespaces(CFG)
    assert out["success"] is False and "401" in out["error"], out
