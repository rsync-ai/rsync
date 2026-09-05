"""Offline test doubles for ``snowflake.connector``.

``snowflake-connector-python`` is intentionally NOT installed in the unit-test
environment — these tests must run with stdlib only. ``SnowflakeMCPServer`` touches
the driver only through one seam: ``_connect(config)`` returns a DB-API connection.
Tests swap that seam for ``FakeConn`` below, so there is no network / driver /
account.

The fakes record every executed statement (SQL + params) so tests can assert the
generated SQL — keyset pagination, batched multi-row INSERT, COPY routing, and the
Snowflake-correct native MERGE upsert.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class FakeCursor:
    def __init__(self, rows: Optional[List[Dict[str, Any]]] = None):
        self._rows = rows if rows is not None else []
        # call log: list of {"sql": ..., "params": ...}
        self.executed: List[Dict[str, Any]] = []

    def execute(self, sql, params=None):
        self.executed.append({"sql": sql, "params": params})
        return None

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    """Records cursors so a test can read back every statement executed."""

    def __init__(self, rows: Optional[List[Dict[str, Any]]] = None):
        self._rows = rows if rows is not None else []
        self.cursors: List[FakeCursor] = []
        self.committed = 0
        self.rolled_back = 0
        self.closed = False

    def cursor(self, *args, **kwargs):
        # SnowflakeMCPServer._dict_cursor calls conn.cursor(DictCursor); the arg
        # is ignored here since the fake already yields dict rows.
        c = FakeCursor(rows=self._rows)
        self.cursors.append(c)
        return c

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    # -- test helpers -------------------------------------------------------
    def all_sql(self) -> List[str]:
        out: List[str] = []
        for c in self.cursors:
            out.extend(e["sql"] for e in c.executed)
        return out

    def all_execs(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for c in self.cursors:
            out.extend(c.executed)
        return out


def make_connector(connector_module, *, rows=None):
    """Build a SnowflakeMCPServer wired to a FakeConn (the _connect seam)."""
    server = connector_module.SnowflakeMCPServer()
    conn = FakeConn(rows=rows)
    server._connect = lambda config: conn  # type: ignore[assignment]
    return server, conn
