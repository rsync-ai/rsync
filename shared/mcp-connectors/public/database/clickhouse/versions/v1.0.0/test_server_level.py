"""Server-level ClickHouse connections: no database named on the connection.

Discovery spans every database the login can see except system and
information_schema, narrowed by the Scope filter, and keeps each table's
database in "schema". Table reads must name their database (<database>.<table>)
inside the scope. Writes with no destination namespace still go to "default".
Offline: the _connect seam answers from an in-memory catalog.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import ClickHouseMCPServer  # noqa: E402
from test_clickhouse_connector import FakeClient, FakeResult  # noqa: E402

DBS = {
    "INFORMATION_SCHEMA": ["COLUMNS"],
    "default": ["events"],
    "information_schema": ["tables"],
    "shop": ["orders", "users"],
    "shop_archive": ["users"],
    "system": ["tables"],
}


def router(sql, p):
    if "version()" in sql:
        return FakeResult(["v"], [["25.5"]])
    if "system.databases" in sql:
        return FakeResult(["name"], [[d] for d in sorted(DBS)])
    if "system.tables" in sql and "dbs" in p:
        return FakeResult(["database", "name", "row_estimate"],
                          [[d, t, 5] for d in sorted(p["dbs"]) for t in sorted(DBS.get(d, []))])
    if "system.columns" in sql and "dbs" in p:
        return FakeResult(["database", "table", "name", "type", "is_in_primary_key"],
                          [[d, t, "id", "Int64", 1] for d in sorted(p["dbs"]) for t in sorted(DBS.get(d, []))])
    if "system.tables" in sql:
        return FakeResult(["name", "row_estimate"], [[t, 5] for t in sorted(DBS.get(p["db"], []))])
    if "system.columns" in sql:
        return FakeResult(["table", "name", "type", "is_in_primary_key"],
                          [[t, "id", "Int64", 1] for t in sorted(DBS.get(p["db"], []))])
    return FakeResult(["id"], [[1]])


@pytest.fixture(autouse=True)
def _no_env_database(monkeypatch):
    monkeypatch.delenv("CLICKHOUSE_DATABASE", raising=False)


def _srv():
    client = FakeClient(query_router=router)
    srv = ClickHouseMCPServer()
    srv._connect = lambda config: client
    return srv, client


def _cfg(**extra):
    return {"host": "localhost", "port": 8123, "user": "default", "password": "p", **extra}


def _names(out):
    return [(t["schema"], t["name"]) for t in out["tables"]]


def test_discovery_spans_every_user_database():
    srv, _ = _srv()
    out = srv.discover_schema({"config": _cfg()})
    assert out["overall_status"] == "success", out
    assert _names(out) == [
        ("default", "events"), ("shop", "orders"), ("shop", "users"), ("shop_archive", "users"),
    ]
    assert out["total_tables_available"] == 4
    assert all(t["primary_keys"] == ["id"] for t in out["tables"])


def test_scope_include_and_exclude_with_wildcards():
    srv, _ = _srv()
    inc = srv.discover_schema({"config": _cfg(
        namespace_filter_mode="include", namespace_filter_patterns="shop*")})
    assert [s for s, _ in _names(inc)] == ["shop", "shop", "shop_archive"]
    exc = srv.discover_schema({"config": _cfg(
        namespace_filter_mode="exclude", namespace_filter_patterns="*archive,DEFAULT")})
    assert _names(exc) == [("shop", "orders"), ("shop", "users")]


def test_system_databases_are_never_reachable():
    srv, client = _srv()
    out = srv.discover_schema({"config": _cfg(
        namespace_filter_mode="include", namespace_filter_patterns="system,information_schema")})
    assert out["tables"] == [] and out["overall_status"] == "success"
    assert "matched no databases" in " ".join(out["warnings_messages"])
    assert not any("system.tables" in q for q, _ in client.queries)


def test_invalid_scope_fails_closed():
    srv, client = _srv()
    out = srv.discover_schema({"config": _cfg(namespace_filter_mode="include")})
    assert out["overall_status"] == "failed" and out["tables"] == []
    assert client.queries == []
    bad = srv.validate_config({"config": _cfg(namespace_filter_mode="sometimes")})
    assert bad["valid"] is False and any("namespace_filter_mode" in e for e in bad["errors"])


def test_named_database_ignores_the_scope():
    srv, _ = _srv()
    out = srv.discover_schema({"config": _cfg(
        database="shop", namespace_filter_mode="include", namespace_filter_patterns="default")})
    assert _names(out) == [("shop", "orders"), ("shop", "users")]


def test_env_database_is_not_server_level(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_DATABASE", "shop")
    srv, _ = _srv()
    assert _names(srv.discover_schema({"config": _cfg()})) == [("shop", "orders"), ("shop", "users")]


def test_max_tables_is_shared_across_databases():
    srv, _ = _srv()
    out = srv.discover_schema({"config": _cfg(), "max_tables": 2})
    assert _names(out) == [("default", "events"), ("shop", "orders")]
    assert out["total_tables_available"] == 4


@pytest.mark.parametrize("table,error", [
    ("users", "pass the table as <database>.<table>"),
    ("shop_archive.users", "outside this connection's scope"),
    ("system.tables", "outside this connection's scope"),
])
def test_export_needs_a_qualified_in_scope_table(table, error):
    srv, client = _srv()
    out = srv.export({"config": _cfg(namespace_filter_mode="exclude",
                                     namespace_filter_patterns="*archive"), "table": table})
    assert out["success"] is False and error in out["error"]
    assert client.queries == []


def test_export_reads_the_named_database():
    srv, client = _srv()
    out = srv.export({"config": _cfg(), "table": "shop.users", "limit": 5})
    assert out["success"] is True, out
    assert any("`shop`.`users`" in q for q, _ in client.queries)


def test_writes_without_a_namespace_still_go_to_default():
    srv, client = _srv()
    out = srv.import_data({"config": _cfg(), "table": "t", "data": [{"id": 1}]})
    assert out.get("success") is True, out
    assert any("`default`.`t`" in c for c in client.commands) or \
        any("`default`.`t`" in i["table"] or i["table"] == "default.t" for i in client.inserts), \
        (client.commands, client.inserts)
