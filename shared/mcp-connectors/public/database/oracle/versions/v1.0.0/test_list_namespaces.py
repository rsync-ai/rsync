"""list_namespaces on Oracle lists the owners that are not Oracle's own, by the
same fallback chain discovery uses (oracle_maintained, then the denylist, then
the current schema), and reports the owner the connection pins, upper-cased,
as "current".

The method is the shared list_namespaces block; the postgresql connector's
test_list_namespaces.py covers its other branches. Offline: a fake connection
answers by SQL shape; no Oracle needed.
"""
import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import connector as C  # noqa: E402
from connector import OracleMCPServer  # noqa: E402


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
    s = OracleMCPServer.__new__(OracleMCPServer)  # bypass __init__
    s.driver_pattern = C._DRIVER_PATTERN
    s._get_connection = lambda config: conn
    s._get_cursor = lambda c, as_dict=True: c.cursor()
    return s


def test_lists_non_oracle_owners():
    cur = FakeCursor({"oracle_maintained": [("SALES",), ("APP",), ("APEX_230100",), ("SYS",)]})
    conn = FakeConn(cur)
    out = _srv(conn).list_namespaces({"config": {"host": "h", "schema": "app"}})
    assert out == {"success": True, "namespaces": ["APP", "SALES"], "current": "APP"}
    assert cur.closed and conn.closed


def test_old_oracle_falls_back_to_the_denylist():
    cur = FakeCursor({
        "oracle_maintained": RuntimeError("ORA-00904: invalid identifier"),
        "SELECT DISTINCT owner FROM all_tables": [("APP",), ("SYS",), ("XDB",), ("FLOWS_FILES",)],
    })
    out = _srv(FakeConn(cur)).list_namespaces({"config": {"host": "h"}})
    assert out == {"success": True, "namespaces": ["APP"], "current": ""}


def test_last_resort_is_the_current_schema():
    cur = FakeCursor({
        "oracle_maintained": [],
        "SELECT DISTINCT owner FROM all_tables": [("SYS",)],
        "CURRENT_SCHEMA": [("RSYNCUSER",)],
    })
    out = _srv(FakeConn(cur)).list_namespaces({"config": {"host": "h"}})
    assert out["namespaces"] == ["RSYNCUSER"]


def test_discovery_uses_the_same_helper():
    src = inspect.getsource(OracleMCPServer._discover_oracle_schema_v2)
    assert "self._oracle_user_owners(cursor)" in src
    assert "oracle_maintained" not in src
