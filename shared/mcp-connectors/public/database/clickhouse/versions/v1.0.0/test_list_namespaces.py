"""list_namespaces on ClickHouse lists the databases, without ClickHouse's own
(system, information_schema / INFORMATION_SCHEMA), and reports the database the
connection uses as "current" ("default" when the config names none).

Offline: a fake client answers query(); no server needed.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import ClickHouseMCPServer  # noqa: E402


class FakeResult:
    def __init__(self, rows):
        self.result_rows = [list(r) for r in rows]


class FakeClient:
    def __init__(self, rows=(), error=None):
        self.rows, self.error = rows, error
        self.queries = []
        self.closed = False

    def query(self, sql, parameters=None):
        self.queries.append(sql)
        if self.error:
            raise self.error
        return FakeResult(self.rows)

    def close(self):
        self.closed = True


def _srv(client):
    s = ClickHouseMCPServer()
    s._connect = lambda config: client
    return s


def test_lists_user_databases():
    client = FakeClient(rows=[("system",), ("INFORMATION_SCHEMA",), ("information_schema",),
                              ("default",), ("analytics",)])
    out = _srv(client).list_namespaces({"config": {"host": "h", "database": "analytics"}})
    assert out == {"success": True, "namespaces": ["analytics", "default"], "current": "analytics"}, out
    assert "system.databases" in client.queries[0]
    assert client.closed


def test_current_defaults_like_the_connection():
    out = _srv(FakeClient(rows=[("default",)])).list_namespaces({"config": {"host": "h"}})
    assert out["current"] == "default", out


def test_query_failure_is_an_error_and_closes():
    client = FakeClient(error=RuntimeError("ACCESS_DENIED"))
    out = _srv(client).list_namespaces({"config": {"host": "h"}})
    assert out["success"] is False and "ACCESS_DENIED" in out["error"], out
    assert client.closed
