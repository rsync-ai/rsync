"""list_namespaces on MySQL lists the databases the login can see, leaves
MySQL's own (information_schema, mysql, performance_schema, sys) out, and
reports the connection's database as "current".

The method is the shared list_namespaces block; the postgresql connector's
test_list_namespaces.py covers its other branches. Offline: a fake connection
answers by SQL shape; no MySQL needed.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import connector as C  # noqa: E402
from connector import MysqlMCPServer  # noqa: E402


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


def test_lists_user_databases_without_mysqls_own():
    cur = FakeCursor({"information_schema.schemata": [
        ("information_schema",), ("mysql",), ("performance_schema",), ("sys",),
        ("shop",), ("App",), ("MySQL",),
    ]})
    conn = FakeConn(cur)
    out = _srv(conn).list_namespaces({"config": {"host": "h", "database": "shop"}})
    assert out == {"success": True, "namespaces": ["App", "shop"], "current": "shop"}
    assert cur.closed and conn.closed


def test_query_failure_is_an_error():
    cur = FakeCursor({"information_schema.schemata": RuntimeError("access denied")})
    conn = FakeConn(cur)
    out = _srv(conn).list_namespaces({"config": {"host": "h", "database": "shop"}})
    assert out["success"] is False and "access denied" in out["error"]
    assert cur.closed and conn.closed
