"""Offline test doubles for ``oracledb`` — the Oracle connector's DB-API seam.

The connector class is ``OracleMCPServer`` (``connector_type == "oracle"``); every
Oracle path runs through the ``_ora_*`` methods and the ``oracledb`` numbered-bind
(``:N``) / double-quote dialect. The driver is touched through exactly one seam:
``_get_connection(config)`` returns a DB-API connection. Tests swap that seam for
:class:`FakeConn` so there is no oracledb driver, network, or server.

Two cursor behaviours are modelled:

  * **simple mode** (``columns`` + ``rows``) — every ``SELECT`` yields the same
    preset rows as *tuples* (oracledb returns sequences, NOT dicts), plus a DB-API
    ``description``. Exercises ``export``'s ``dict(zip(columns, row))`` normalization
    and single-statement writes.
  * **router mode** (``router(sql, params) -> rows | None``) — returns different
    rows per statement, for multi-query flows (discover_schema, existence probes).

Every ``execute``/``executemany`` is logged (SQL + params) so tests assert the
generated SQL: ``FETCH FIRST n ROWS ONLY`` keyset paging, ``OFFSET/FETCH NEXT``
offset paging, double-quoted ``INSERT``, the ``MERGE ... FROM dual`` upsert, and the
``_rsync_cdc_offsets`` MERGE. ``executemany`` sets ``rowcount``; ``DELETE`` sets
``rowcount = 1`` per row. ``FakeConn`` can fail the first ``executemany`` with an
"ORA-00942" error to drive the auto-create-then-retry path.
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
        # the exact substring the connector greps for ("ORA-00942").
        if self._conn is not None and self._conn.fail_executemany_once:
            self._conn.fail_executemany_once = False
            raise Exception("ORA-00942: table or view does not exist")
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
    """Build an OracleMCPServer wired to a FakeConn (the _get_connection seam)."""
    server = connector_module.OracleMCPServer()
    conn = FakeConn(columns=columns, rows=rows, router=router,
                    fail_executemany_once=fail_executemany_once)
    server._get_connection = lambda config: conn  # type: ignore[assignment]
    return server, conn


def discover_router(tables: Dict[str, Dict[str, Any]],
                    version: str = "Oracle Database 19c Enterprise Edition") -> Callable:
    """Build a router for ``discover_schema``.

    ``tables`` maps table name -> {"row_estimate": int, "columns": [(name, dtype,
    nullable, data_length, data_precision, data_scale), ...]}. Routes the table
    list (user_tables), per-table columns (user_tab_columns), and PK/FK/index
    probes. Only the shapes _discover_oracle_schema_v2 reads are modelled.
    """
    def route(sql: str, params):
        s = sql.lower()
        if "from user_tables" in s and "num_rows" in s:
            return [(name, spec.get("row_estimate", 0)) for name, spec in tables.items()]
        if "from user_tables" in s:
            return [(name,) for name in tables.items()]
        if "user_tab_columns" in s:
            tname = params[0] if params else None
            spec = tables.get(tname) or {}
            return list(spec.get("columns", []))
        return []
    return route
