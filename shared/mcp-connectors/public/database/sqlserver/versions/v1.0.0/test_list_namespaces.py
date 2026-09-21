"""list_namespaces on SQL Server lists the user schemas (dbo kept; sys,
INFORMATION_SCHEMA, guest and the fixed database-role schemas left out) and
reports the schema the connection pins as "current".

The method is the shared list_namespaces block; the postgresql connector's
test_list_namespaces.py covers its other branches. Offline: a fake connection
answers by SQL shape; no SQL Server needed.
"""
import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import connector as C  # noqa: E402
from connector import MysqlMCPServer  # noqa: E402  (sqlserver reuses this class)


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
    def __init__(self, cursor):
        self.cur = cursor
        self.closed = False

    def cursor(self, *a, **k):
        return self.cur

    def close(self):
        self.closed = True


def _srv(conn):
    s = MysqlMCPServer.__new__(MysqlMCPServer)  # bypass __init__
    s.driver_pattern = C._DRIVER_PATTERN
    s._get_connection = lambda config: conn
    s._get_cursor = lambda c, as_dict=True: c.cursor()
    return s


def test_lists_user_schemas():
    cur = FakeCursor({"FROM sys.schemas": [("sales",), ("dbo",)]})
    conn = FakeConn(cur)
    out = _srv(conn).list_namespaces({"config": {"host": "h", "database": "app"}})
    assert out == {"success": True, "namespaces": ["dbo", "sales"], "current": ""}
    sql = cur.executed[0]
    for hidden in ("'sys'", "'INFORMATION_SCHEMA'", "'guest'", "'db_owner'", "'db_denydatawriter'"):
        assert hidden in sql
    assert "'dbo'" not in sql
    assert cur.closed and conn.closed


def test_current_is_the_schema_the_connection_pins():
    cur = FakeCursor({"FROM sys.schemas": [("dbo",), ("sales",)]})
    out = _srv(FakeConn(cur)).list_namespaces({"config": {"host": "h", "schema": "sales"}})
    assert out["current"] == "sales"


def test_discovery_uses_the_same_helper():
    src = inspect.getsource(MysqlMCPServer._discover_sqlserver_schema_v2)
    assert "self._sqlserver_user_schemas(cursor)" in src
    assert "FROM sys.schemas" not in src
