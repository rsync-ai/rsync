"""Offline test doubles for ``pyodbc`` — the SQL Server connector's DB-API seam.

The connector class is ``MysqlMCPServer`` reused for SQL Server (``connector_type
= "sqlserver"``); every SQL Server path runs through the ``_ss_*`` methods and the
``pyodbc`` qmark/bracket dialect. The driver is touched through exactly one seam:
``_get_connection(config)`` returns a DB-API connection. Tests swap that seam for
:class:`FakeConn` so there is no ODBC driver, network, or server.

Two cursor behaviours are modelled:

  * **simple mode** (``columns`` + ``rows``) — every ``SELECT`` yields the same
    preset rows as *tuples* (pyodbc returns sequences, NOT dicts), plus a DB-API
    ``description``. This exercises ``export``'s ``dict(zip(columns, row))``
    normalization and single-statement writes.
  * **router mode** (``router(sql, params) -> rows | None``) — returns different
    rows per statement, for multi-query flows like ``discover_schema`` (version →
    COUNT → table list → per-table columns).

Every ``execute``/``executemany`` is logged (SQL + params) so tests assert the
generated SQL: ``TOP (n)`` keyset paging, ``OFFSET/FETCH`` offset paging, bracketed
``INSERT``, and the SQL Server ``MERGE`` upsert. ``executemany`` sets ``rowcount``
(insert counting); ``DELETE`` sets ``rowcount = 1`` per row (delete counting).
``FakeConn`` can fail the first ``executemany`` with an "Invalid object name" error
to drive the auto-create-then-retry path.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional


class FakeCursor:
    def __init__(self, columns=None, rows=None, router=None, conn=None):
        self._columns = columns
        self._rows = rows if rows is not None else []
        self._router = router
        self._conn = conn
        self.description = None
        self._result: List[Any] = []
        self.executed: List[Dict[str, Any]] = []
        self.rowcount = -1

    def execute(self, sql, params=None):
        self.executed.append({"sql": sql, "params": params})
        verb = sql.lstrip()[:6].upper()
        if self._router is not None:
            routed = self._router(sql, params)
            self._result = list(routed) if routed is not None else []
            self.description = None  # router flows read rows positionally
        else:
            self._result = list(self._rows)
            self.description = (
                [(c, None, None, None, None, None, None) for c in self._columns]
                if self._columns is not None else None
            )
        if verb.startswith("DELETE"):
            self.rowcount = 1  # one row matched per key tuple
        return None

    def executemany(self, sql, seq):
        seq = list(seq)
        self.executed.append({"sql": sql, "params": seq, "many": True})
        # Drive the auto-create-then-retry path: fail the first executemany with
        # the exact substring the connector greps for ("invalid object name").
        if self._conn is not None and self._conn.fail_executemany_once:
            self._conn.fail_executemany_once = False
            raise Exception("('42S02', \"Invalid object name 'dbo.t'\")")
        self.rowcount = len(seq)
        return None

    def fetchall(self):
        return list(self._result)

    def fetchone(self):
        return self._result[0] if self._result else None

    def close(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    """Records every cursor so a test can read back all executed statements."""

    def __init__(self, columns=None, rows=None, router=None, fail_executemany_once=False):
        self._columns = columns
        self._rows = rows
        self._router = router
        self.fail_executemany_once = fail_executemany_once
        self.cursors: List[FakeCursor] = []
        self.committed = 0
        self.rolled_back = 0
        self.closed = False

    def cursor(self, *args, **kwargs):
        c = FakeCursor(columns=self._columns, rows=self._rows, router=self._router, conn=self)
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


def make_connector(connector_module, *, columns=None, rows=None, router=None,
                   fail_executemany_once=False):
    """Build a (sqlserver) MysqlMCPServer wired to a FakeConn (the _get_connection seam)."""
    server = connector_module.MysqlMCPServer()
    conn = FakeConn(columns=columns, rows=rows, router=router,
                    fail_executemany_once=fail_executemany_once)
    server._get_connection = lambda config: conn  # type: ignore[assignment]
    return server, conn


def discover_router(tables: Dict[str, Dict[str, Any]],
                    version: str = "Microsoft SQL Server 2019 (RTM-CU18)") -> Callable:
    """Build a router for ``discover_schema``.

    ``tables`` maps table name -> {"row_estimate": int, "columns": [(name, dtype,
    is_nullable), ...]}. Routes @@VERSION, the sys.tables COUNT, the sys.partitions
    table list, and the per-table sys.columns query (keyed on the tname param).
    """
    def route(sql: str, params):
        if "@@VERSION" in sql:
            return [(version,)]
        if "COUNT(*)" in sql and "sys.tables" in sql:
            return [(len(tables),)]
        if "sys.columns c" in sql:
            tname = params[1] if params and len(params) > 1 else None
            spec = tables.get(tname) or {}
            return list(spec.get("columns", []))
        if "sys.tables" in sql and "FETCH NEXT" in sql:
            return [(name, spec.get("row_estimate", 0)) for name, spec in tables.items()]
        return []
    return route
