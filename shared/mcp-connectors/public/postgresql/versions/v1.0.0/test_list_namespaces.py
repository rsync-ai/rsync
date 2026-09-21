"""list_namespaces lists the schemas one level above a table, leaves the
PostgreSQL catalogs out, reports the schema the connection pins as "current",
and closes what it opens.

The method is the shared list_namespaces block (the same text in the
postgresql, mysql, oracle and sqlserver connectors and connector_database.py.j2),
so the branches for the other drivers are covered here once.

Offline: a fake connection answers by SQL shape; no Postgres needed.
"""
import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import connector as C  # noqa: E402
from connector import PostgresqlMCPServer  # noqa: E402


class FakeCursor:
    def __init__(self, rows_by_shape):
        self.rows_by_shape = rows_by_shape
        self.executed = []
        self.closed = False
        self._rows = []

    def execute(self, sql, params=None):
        self.executed.append(" ".join(sql.split()))
        self._rows = []
        for shape, rows in self.rows_by_shape.items():
            if shape in sql:
                if isinstance(rows, Exception):
                    raise rows
                self._rows = list(rows)
                return

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self, cursor=None, databases=()):
        self.cur = cursor
        self.databases = databases
        self.closed = False

    def cursor(self, *a, **k):
        return self.cur

    def list_database_names(self):
        return list(self.databases)

    def close(self):
        self.closed = True


def _srv(conn, pattern=None):
    s = PostgresqlMCPServer.__new__(PostgresqlMCPServer)  # bypass __init__
    s.driver_pattern = pattern or C._DRIVER_PATTERN
    s._get_connection = lambda config: conn
    # The real _get_cursor imports psycopg2 even for a plain cursor.
    s._get_cursor = lambda c, as_dict=True: c.cursor()
    return s


def test_lists_user_schemas_sorted_and_closes():
    cur = FakeCursor({"information_schema.schemata": [("shop",), ("public",), ("shop",)]})
    conn = FakeConn(cur)
    out = _srv(conn).list_namespaces({"config": {"host": "h", "database": "app"}})
    assert out == {"success": True, "namespaces": ["public", "shop"], "current": ""}
    sql = cur.executed[0]
    for hidden in ("'pg_catalog'", "'information_schema'", "'pg_temp%'", "'pg_toast%'"):
        assert hidden in sql
    assert cur.closed and conn.closed


def test_current_is_the_schema_the_connection_pins():
    cur = FakeCursor({"information_schema.schemata": [("public",), ("shop",)]})
    out = _srv(FakeConn(cur)).list_namespaces({"config": {"host": "h", "schema": "shop"}})
    assert out["current"] == "shop"
    assert out["namespaces"] == ["public", "shop"]


def test_discovery_uses_the_same_helper():
    # The list and discovery must agree on what a system schema is.
    src = inspect.getsource(PostgresqlMCPServer._discover_postgres_schema_v2)
    assert "self._postgres_user_schemas(cursor)" in src
    assert "information_schema.schemata" not in src


def test_connection_failure_is_an_error_not_an_empty_list():
    s = PostgresqlMCPServer.__new__(PostgresqlMCPServer)
    s.driver_pattern = C._DRIVER_PATTERN

    def boom(config):
        raise RuntimeError("could not connect")

    s._get_connection = boom
    out = s.list_namespaces({"config": {"host": "h"}})
    assert out["success"] is False
    assert "could not connect" in out["error"]


def test_query_failure_closes_cursor_and_connection():
    cur = FakeCursor({"information_schema.schemata": RuntimeError("permission denied")})
    conn = FakeConn(cur)
    out = _srv(conn).list_namespaces({"config": {"host": "h"}})
    assert out["success"] is False and "permission denied" in out["error"]
    assert cur.closed and conn.closed


# --- the other branches of the shared block ---------------------------------

def test_mongo_lists_databases_without_its_own():
    conn = FakeConn(databases=["admin", "shop", "local", "config", "app"])
    s = _srv(conn, pattern=C.DRIVER_PATTERNS["mongodb"])
    out = s.list_namespaces({"config": {"host": "h", "database": "shop"}})
    assert out == {"success": True, "namespaces": ["app", "shop"], "current": "shop"}
    assert conn.closed


def test_other_nosql_is_not_supported():
    s = _srv(FakeConn(), pattern=C.DRIVER_PATTERNS["redis"])
    out = s.list_namespaces({"config": {"host": "h"}})
    assert out["success"] is False and "not supported" in out["error"]


def test_sqlite_has_one_namespace():
    s = _srv(FakeConn(FakeCursor({})), pattern=C.DRIVER_PATTERNS["sqlite"])
    out = s.list_namespaces({"config": {"host": "h"}})
    assert out == {"success": True, "namespaces": ["main"], "current": "main"}


def test_unknown_sql_driver_is_not_supported():
    cur = FakeCursor({})
    conn = FakeConn(cur)
    out = _srv(conn, pattern={"module": "somedriver"}).list_namespaces({"config": {"host": "h"}})
    assert out["success"] is False and "somedriver" in out["error"]
    assert cur.closed and conn.closed


def test_warehouse_adapter_answers_for_itself():
    class Adapter:
        def list_namespaces(self, config):
            return {"success": True, "namespaces": ["raw"], "current": config.get("dataset_id", "")}

    s = _srv(FakeConn())
    s._warehouse_adapter = Adapter()
    out = s.list_namespaces({"config": {"host": "h", "dataset_id": "raw"}})
    assert out == {"success": True, "namespaces": ["raw"], "current": "raw"}

    s._warehouse_adapter = object()
    out = s.list_namespaces({"config": {"host": "h"}})
    assert out["success"] is False
