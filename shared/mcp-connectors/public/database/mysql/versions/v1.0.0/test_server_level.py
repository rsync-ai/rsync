"""Server-level MySQL connections: no database named on the connection.

Discovery lists every database the login can see except MySQL's own,
narrowed by the Scope filter (namespace_filter_mode / namespace_filter_patterns),
and keeps each table's database in "schema". Reads must name the database
(<database>.<table>) and stay inside the scope; writes need a destination
namespace from the pipeline. Offline: a fake catalog answers by SQL shape.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import connector as C  # noqa: E402
from connector import MysqlMCPServer  # noqa: E402

DBS = {
    "information_schema": ["TABLES"],
    "mysql": ["user"],
    "performance_schema": ["threads"],
    "sys": ["host_summary"],
    "shop": ["orders", "users"],
    "shop_archive": ["users"],
    "test": ["logs"],
}


class Catalog:
    """A cursor over DBS that answers the information_schema queries discovery runs."""

    def __init__(self, fail_db=None):
        self.fail_db = fail_db
        self.executed = []
        self._rows = []

    def execute(self, sql, params=None):
        q = " ".join(sql.split())
        params = tuple(params or ())
        self.executed.append((q, params))
        self._rows = []
        if "information_schema.schemata" in q:
            self._rows = [(d,) for d in sorted(DBS)]
        elif "SELECT COUNT(*) AS cnt FROM information_schema.tables" in q:
            if params[0] == self.fail_db:
                raise RuntimeError("SELECT command denied")
            self._rows = [{"cnt": len(DBS.get(params[0], []))}]
        elif "TABLE_ROWS AS row_estimate" in q:
            db, limit = params
            self._rows = [
                {"name": t, "schema_name": db, "row_estimate": 3}
                for t in sorted(DBS.get(db, []))[:limit]
            ]
        elif "FROM information_schema.columns" in q:
            db, names = params[0], params[1:]
            self._rows = [
                {"table_name": t, "name": "id", "data_type": "int",
                 "column_type": "int", "is_nullable": "NO"}
                for t in names if t in DBS.get(db, [])
            ]
        elif "CONSTRAINT_TYPE = 'PRIMARY KEY'" in q:
            db, names = params[0], params[1:]
            self._rows = [{"table_name": t, "column_name": "id"} for t in names if t in DBS.get(db, [])]
        elif q.startswith("SELECT VERSION()"):
            self._rows = [("8.0.36",)]

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass


class Conn:
    def __init__(self, cur):
        self.cur = cur

    def cursor(self, *a, **k):
        return self.cur

    def close(self):
        pass


@pytest.fixture(autouse=True)
def _no_env_database(monkeypatch):
    monkeypatch.delenv("MYSQL_DATABASE", raising=False)


def _srv(cur):
    s = MysqlMCPServer()
    conn = Conn(cur)
    s._get_connection = lambda config: conn
    s._get_cursor = lambda c, as_dict=True: c.cursor()
    return s


def _cfg(**extra):
    return {"host": "h", "port": 3306, "user": "u", "password": "p", **extra}


def _names(out):
    return [(t["schema"], t["name"]) for t in out["tables"]]


def test_discovery_spans_every_user_database():
    out = _srv(Catalog()).discover_schema({"config": _cfg()})
    assert out["overall_status"] == "success"
    assert _names(out) == [
        ("shop", "orders"), ("shop", "users"), ("shop_archive", "users"), ("test", "logs"),
    ]
    assert out["total_tables_available"] == 4
    assert all(t["primary_keys"] == ["id"] for t in out["tables"])
    assert all(t["columns"][0]["name"] == "id" for t in out["tables"])


def test_scope_include_and_exclude_with_wildcards():
    inc = _srv(Catalog()).discover_schema({"config": _cfg(
        namespace_filter_mode="include", namespace_filter_patterns="shop*")})
    assert [s for s, _ in _names(inc)] == ["shop", "shop", "shop_archive"]
    exc = _srv(Catalog()).discover_schema({"config": _cfg(
        namespace_filter_mode="exclude", namespace_filter_patterns="*archive, TEST")})
    assert _names(exc) == [("shop", "orders"), ("shop", "users")]


def test_system_databases_are_never_reachable():
    out = _srv(Catalog()).discover_schema({"config": _cfg(
        namespace_filter_mode="include", namespace_filter_patterns="mysql, sys, information_schema")})
    assert out["tables"] == []
    assert out["overall_status"] == "success"
    assert "matched no databases" in " ".join(out["warnings_messages"])


def test_invalid_scope_fails_closed():
    cur = Catalog()
    out = _srv(cur).discover_schema({"config": _cfg(namespace_filter_mode="include")})
    assert out["overall_status"] == "failed"
    assert out["tables"] == []
    assert not any("information_schema.tables" in q for q, _ in cur.executed)
    bad = _srv(cur).validate_config({"config": _cfg(namespace_filter_mode="sometimes")})
    assert bad["valid"] is False and any("namespace_filter_mode" in e for e in bad["errors"])


def test_validate_config_no_longer_requires_a_database():
    out = _srv(Catalog()).validate_config({"config": _cfg()})
    assert out["valid"] is True, out["errors"]


def test_named_database_ignores_the_scope():
    out = _srv(Catalog()).discover_schema({"config": _cfg(
        database="test", namespace_filter_mode="include", namespace_filter_patterns="shop")})
    assert _names(out) == [("test", "logs")]


def test_max_tables_is_shared_across_databases():
    out = _srv(Catalog()).discover_schema({"config": _cfg(), "max_tables": 3})
    assert _names(out) == [("shop", "orders"), ("shop", "users"), ("shop_archive", "users")]
    assert out["total_tables_available"] == 4


def test_one_unreadable_database_is_a_partial_success():
    out = _srv(Catalog(fail_db="shop_archive")).discover_schema({"config": _cfg()})
    assert out["overall_status"] == "partial_success"
    assert _names(out) == [("shop", "orders"), ("shop", "users"), ("test", "logs")]
    assert any(m.startswith("shop_archive: ") for m in out["warnings_messages"])


@pytest.mark.parametrize("table,error", [
    ("users", "pass the table as <database>.<table>"),
    ("shop_archive.users", "outside this connection's scope"),
    ("mysql.user", "outside this connection's scope"),
])
def test_export_needs_a_qualified_in_scope_table(table, error):
    cur = Catalog()
    out = _srv(cur).export({
        "config": _cfg(namespace_filter_mode="exclude", namespace_filter_patterns="*archive"),
        "table": table,
    })
    assert out["success"] is False and error in out["error"]
    assert cur.executed == []


def test_export_reads_the_named_database():
    cur = Catalog()
    _srv(cur).export({"config": _cfg(), "table": "shop.users", "limit": 5})
    assert any("`shop`.`users`" in q for q, _ in cur.executed), cur.executed


@pytest.mark.parametrize("method", ["import_data", "upsert_data", "delete_data"])
def test_writes_without_a_database_or_namespace_say_so(method):
    cur = Catalog()
    out = getattr(_srv(cur), method)({"config": _cfg(), "table": "users", "data": [{"id": 1}]})
    assert out["success"] is False
    assert out["error"] == C._NO_WRITE_DATABASE
    assert cur.executed == []
