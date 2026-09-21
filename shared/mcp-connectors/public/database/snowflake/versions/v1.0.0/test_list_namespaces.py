"""list_namespaces on Snowflake lists the schemas that hold user tables in the
connection's database, by the filter discover_schema uses (_USER_TABLES_WHERE),
and reports the configured schema as "current".

Offline: _sf_fakes swaps the _connect seam; no account needed.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import connector as sf  # noqa: E402
import _sf_fakes  # noqa: E402

CFG = {"config": {"account": "xy12345.us-east-1", "user": "u", "password": "p",
                  "warehouse": "WH", "database": "DB", "schema": "PUBLIC"}}
WHERE = sf.SnowflakeMCPServer._USER_TABLES_WHERE


def test_lists_each_schema_once_by_discoverys_filter():
    # DictCursor rows may come back with UPPERCASE keys.
    rows = [{"TABLE_SCHEMA": "SALES"}, {"TABLE_SCHEMA": "PUBLIC"}, {"TABLE_SCHEMA": "SALES"}]
    s, conn = _sf_fakes.make_connector(sf, rows=rows)
    out = s.list_namespaces(CFG)
    assert out == {"success": True, "namespaces": ["PUBLIC", "SALES"], "current": "PUBLIC"}, out
    sql = conn.all_sql()[-1]
    assert "SELECT DISTINCT table_schema" in sql and WHERE in sql, sql
    assert conn.closed


def test_discovery_filters_by_the_same_where():
    s, conn = _sf_fakes.make_connector(sf, rows=[{"table_schema": "PUBLIC", "table_name": "T"}])
    s.discover_schema(CFG)
    assert WHERE in conn.all_sql()[-1]
    assert "INFORMATION_SCHEMA" in WHERE


def test_connection_failure_is_an_error():
    s, _ = _sf_fakes.make_connector(sf)

    def boom(config):
        raise RuntimeError("bad account")

    s._connect = boom
    out = s.list_namespaces(CFG)
    assert out["success"] is False and "bad account" in out["error"], out
