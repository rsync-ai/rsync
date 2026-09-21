"""list_namespaces on Redshift lists the schemas that hold user tables, by the
filter discover_schema uses (_USER_TABLES_WHERE), and reports the configured
schema as "current".

Offline: _rs_fakes swaps the _connect seam; no cluster needed.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import connector as rs  # noqa: E402
import _rs_fakes  # noqa: E402

CFG = {"config": {"host": "c.redshift.amazonaws.com", "port": 5439,
                  "database": "dev", "user": "u", "password": "p",
                  "schema": "public"}}
WHERE = rs.RedshiftMCPServer._USER_TABLES_WHERE


def test_lists_each_schema_once_by_discoverys_filter():
    rows = [{"table_schema": "sales"}, {"table_schema": "public"}, {"table_schema": "sales"}]
    s, conn = _rs_fakes.make_connector(rs, rows=rows)
    out = s.list_namespaces(CFG)
    assert out == {"success": True, "namespaces": ["public", "sales"], "current": "public"}, out
    ex = conn.all_execs()[-1]
    assert "SELECT DISTINCT table_schema" in ex["sql"] and WHERE in ex["sql"], ex
    assert ex["params"] == ["pg_temp%"], ex
    assert conn.closed


def test_discovery_filters_by_the_same_where():
    s, conn = _rs_fakes.make_connector(rs, rows=[{"table_schema": "public", "table_name": "t"}])
    s.discover_schema(CFG)
    assert WHERE in conn.all_sql()[-1]
    assert "pg_catalog" in WHERE and "pg_temp" not in WHERE  # temp schemas go by the bound LIKE


def test_connection_failure_is_an_error():
    s, _ = _rs_fakes.make_connector(rs)

    def boom(config):
        raise RuntimeError("timeout")

    s._connect = boom
    out = s.list_namespaces(CFG)
    assert out["success"] is False and "timeout" in out["error"], out
