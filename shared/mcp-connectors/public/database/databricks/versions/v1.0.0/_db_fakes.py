"""Offline test doubles for ``databricks.sql``.

``databricks-sql-connector`` is intentionally NOT installed in the unit-test
environment — these tests must run with stdlib only. ``DatabricksMCPServer`` touches
the driver only through one seam: ``_connect(config)`` returns a DB-API connection.
Tests swap that seam for ``FakeConn`` below, so there is no network / driver /
warehouse.

Fidelity note: the real Databricks driver returns ``Row`` objects (namedtuple-like
with ``asDict()``), NOT dicts. ``FakeRow`` mimics that so the connector's
``_row_to_dict`` conversion (asDict / cursor.description) is exercised, and the
cursor exposes ``description`` like a real DB-API cursor.

The fakes record every executed statement (SQL + params) so tests can assert the
generated SQL — keyset pagination, batched multi-row INSERT, COPY routing, and the
Databricks-correct native Delta MERGE upsert (backtick-quoted).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class FakeRow:
    """Mimics a databricks ``Row``: asDict() + positional access, NOT a dict."""

    def __init__(self, d: Dict[str, Any]):
        self._d = dict(d)

    def asDict(self) -> Dict[str, Any]:
        return dict(self._d)

    def __getitem__(self, i):
        return list(self._d.values())[i]

    def __iter__(self):
        return iter(self._d.values())


class FakeCursor:
    def __init__(self, rows: Optional[List[Dict[str, Any]]] = None):
        self._rows = rows if rows is not None else []
        # DB-API description: list of 7-tuples; only name (index 0) matters here.
        self.description = None
        if self._rows:
            self.description = [(k,) for k in self._rows[0].keys()]
        # call log: list of {"sql": ..., "params": ...}
        self.executed: List[Dict[str, Any]] = []

    def execute(self, sql, params=None):
        self.executed.append({"sql": sql, "params": params})
        return None

    def fetchall(self):
        return [FakeRow(r) for r in self._rows]

    def fetchone(self):
        return FakeRow(self._rows[0]) if self._rows else None

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
    """Build a DatabricksMCPServer wired to a FakeConn (the _connect seam)."""
    server = connector_module.DatabricksMCPServer()
    conn = FakeConn(rows=rows)
    server._connect = lambda config: conn  # type: ignore[assignment]
    return server, conn
