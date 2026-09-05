"""DB-source incremental sync: bootstrap + delta-predicate contract.

Pins the two halves of the fix for the append-only defect (INCREMENTAL.md §5):

1. **Bootstrap.** The connector must emit ``stats.watermark`` on the FIRST run,
   before any ``since`` exists. It used to gate the watermark on ``since``
   while the executor gated ``since`` on the watermark — neither side could go
   first, so a DB source never entered timestamp-incremental mode and every
   run degraded to keyset-by-PK.

2. **Delta predicate.** With a baseline in hand the WHERE clause must be
   ``pk > <page cursor> AND ( updated_at > <since> OR pk > <since_cursor> )``.
   The OR is load-bearing in both directions: AND-ing the halves drops every
   UPDATE to an already-synced row (an updated row keeps its old PK), while the
   timestamp alone drops every INSERT whose incremental column is NULL or
   hand-maintained.

Offline: the connectors are loaded by file path and driven through a fake
DB-API cursor, so no driver and no database is involved. Runs against all four
hand-curated DB connectors, since the bug and the fix are identical in each.
"""
from __future__ import annotations

import importlib.util
import os
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_PUBLIC = os.path.abspath(os.path.join(_HERE, "..", "public"))

# (connector name, path to its CURRENT versioned dir, server class name).
# Paths mirror `latest.json.current_version` — the dir the Docker build uses.
CONNECTORS = [
    ("postgresql", os.path.join(_PUBLIC, "postgresql", "versions", "v1.0.0"), "PostgresqlMCPServer"),
    ("mysql", os.path.join(_PUBLIC, "database", "mysql", "versions", "v1.0.0"), "MysqlMCPServer"),
    ("oracle", os.path.join(_PUBLIC, "database", "oracle", "versions", "v1.0.0"), "OracleMCPServer"),
    # The sqlserver connector's class is literally named MysqlMCPServer — a
    # leftover from the shared database template. Cosmetic (dispatch is on the
    # instance), but pinned here so this test doesn't silently skip.
    ("sqlserver", os.path.join(_PUBLIC, "database", "sqlserver", "versions", "v1.0.0"), "MysqlMCPServer"),
]


def _load_server(name: str, version_dir: str, class_name: str):
    """Import a connector by file path the way the image does, then return its
    server class. Skips cleanly when a connector's optional deps are absent."""
    for p in (os.path.dirname(version_dir), version_dir, _PUBLIC):
        if p not in sys.path:
            sys.path.insert(0, p)
    spec = importlib.util.spec_from_file_location(
        f"_conn_{name}", os.path.join(version_dir, "connector.py")
    )
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"{name} connector not importable offline: {exc}")
    cls = getattr(module, class_name, None)
    if cls is None:  # pragma: no cover - guards a rename
        pytest.skip(f"{name}: {class_name} not found")
    return cls


class FakeCursor:
    """Minimal DB-API cursor that records the SQL it was handed."""

    def __init__(self, columns, rows):
        self._columns = columns
        self._rows = rows
        self.executed = []  # list of (sql, binds)

    def execute(self, sql, binds=None):
        self.executed.append((sql, binds))

    @property
    def description(self):
        return [(c,) for c in self._columns]

    def fetchall(self):
        return list(self._rows)

    def close(self):
        pass


class FakeConn:
    def close(self):
        pass


def _make_server(cls, columns, rows):
    """Instantiate a connector with its DB access stubbed out."""
    srv = cls()
    cursor = FakeCursor(columns, rows)
    srv._get_connection = lambda config: FakeConn()
    srv._get_cursor = lambda conn: cursor
    # The keyset PK is normally probed from the catalog; the executor supplies
    # it explicitly in these cases via `cursor_column`, but pin the fallback so
    # a probe never reaches the fake cursor.
    srv._get_primary_key_column = lambda cur, table: "id"
    srv._is_invisible_column = lambda cur, table, col: False
    return srv, cursor


BASE_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "testdb",
    "username": "u",
    "password": "p",
}


def _export(srv, **params):
    p = dict(params)
    p.setdefault("config", dict(BASE_CONFIG))
    p.setdefault("table", "orders")
    p.setdefault("format", "json")
    p.setdefault("limit", 1000)
    return srv.export(p)


@pytest.mark.parametrize("name,version_dir,class_name", CONNECTORS, ids=[c[0] for c in CONNECTORS])
def test_first_run_emits_watermark_without_since(name, version_dir, class_name):
    """Bootstrap: no `since` yet, but `updated_at` is a real column, so the
    connector must still report a watermark. Without this emission the executor
    never writes `mode`+`watermark` into the checkpoint and can never send
    `since` on a later run — the deadlock that made every DB source
    append-only."""
    cls = _load_server(name, version_dir, class_name)
    srv, _cursor = _make_server(
        cls,
        columns=["id", "name", "updated_at"],
        rows=[
            {"id": 1, "name": "a", "updated_at": "2026-08-01T06:00:00+00:00"},
            {"id": 2, "name": "b", "updated_at": "2026-08-01T07:30:00+00:00"},
        ],
    )
    res = _export(srv, use_keyset_paging=True, cursor_column="id")

    assert res.get("success") is not False, res
    wm = (res.get("stats") or {}).get("watermark")
    assert wm is not None, f"{name}: no watermark on the first run — bootstrap still broken"
    assert wm["field"].lower() == "updated_at"
    assert wm["value"] == "2026-08-01T07:30:00+00:00"


@pytest.mark.parametrize("name,version_dir,class_name", CONNECTORS, ids=[c[0] for c in CONNECTORS])
def test_no_incremental_column_emits_no_watermark(name, version_dir, class_name):
    """A table with no `updated_at` must NOT get a watermark — it correctly
    stays on the append-only keyset path. The existence check is against
    cursor.description, not against the request params."""
    cls = _load_server(name, version_dir, class_name)
    srv, _cursor = _make_server(
        cls,
        columns=["id", "name"],
        rows=[{"id": 1, "name": "a"}],
    )
    res = _export(srv, use_keyset_paging=True, cursor_column="id")

    assert res.get("success") is not False, res
    assert (res.get("stats") or {}).get("watermark") is None, f"{name}: phantom watermark"
    assert "max_watermark" not in res


@pytest.mark.parametrize("name,version_dir,class_name", CONNECTORS, ids=[c[0] for c in CONNECTORS])
def test_delta_predicate_ors_timestamp_with_since_cursor(name, version_dir, class_name):
    """The whole fix. `since` + `since_cursor` must produce an OR group, so an
    UPDATE to row 5 (old PK, new updated_at) comes back alongside INSERTs past
    the PK high-water. Before the fix these two were AND-ed and no UPDATE could
    ever satisfy `pk > last_pk`."""
    cls = _load_server(name, version_dir, class_name)
    srv, cursor = _make_server(
        cls,
        columns=["id", "name", "updated_at"],
        rows=[{"id": 5, "name": "updated", "updated_at": "2026-08-01T08:00:00+00:00"}],
    )
    # A high-water value that cannot collide with the LIMIT/FETCH literal, so
    # the "never interpolated" assertion below means what it says.
    _export(
        srv,
        use_keyset_paging=True,
        cursor_column="id",
        since="2026-08-01T07:00:00+00:00",
        since_cursor=987654,
        incremental_field="updated_at",
    )

    assert cursor.executed, f"{name}: no SQL executed"
    sql, binds = cursor.executed[-1]
    normalized = " ".join(sql.split()).lower()
    assert " or " in normalized, f"{name}: delta halves are not OR-ed:\n{sql}"
    assert "updated_at" in normalized and "id" in normalized, sql
    # Both halves are bound, and as parameters — never interpolated.
    assert binds is not None, f"{name}: delta values were not bound: {sql}"
    assert "2026-08-01T07:00:00+00:00" in [str(b) for b in binds], binds
    assert "987654" in [str(b) for b in binds], binds
    assert "987654" not in normalized, f"{name}: since_cursor interpolated into SQL:\n{sql}"


@pytest.mark.parametrize("name,version_dir,class_name", CONNECTORS, ids=[c[0] for c in CONNECTORS])
def test_paging_cursor_is_anded_not_ored(name, version_dir, class_name):
    """The intra-run paging cursor stays AND-ed. If it joined the OR group,
    already-returned low-PK rows would repeat on every page and the export
    would never terminate."""
    cls = _load_server(name, version_dir, class_name)
    srv, cursor = _make_server(
        cls,
        columns=["id", "name", "updated_at"],
        rows=[{"id": 7, "name": "c", "updated_at": "2026-08-01T09:00:00+00:00"}],
    )
    _export(
        srv,
        use_keyset_paging=True,
        cursor_column="id",
        cursor=6,
        since="2026-08-01T07:00:00+00:00",
        since_cursor=100,
        incremental_field="updated_at",
    )

    sql, binds = cursor.executed[-1]
    normalized = " ".join(sql.split()).lower()
    where = normalized.split(" where ", 1)[1]
    # The paging predicate sits OUTSIDE the parenthesized delta group.
    before_group = where.split("(", 1)[0]
    assert " and " in before_group, f"{name}: paging cursor folded into the OR group:\n{sql}"
    assert len(binds) == 3, f"{name}: expected cursor + since + since_cursor binds, got {binds}"


@pytest.mark.parametrize("name,version_dir,class_name", CONNECTORS, ids=[c[0] for c in CONNECTORS])
def test_since_cursor_alone_reproduces_append_only_behavior(name, version_dir, class_name):
    """A table with no incremental column but a `since_cursor` must emit
    exactly the old `pk > N` filter — the fix is a strict superset of the
    previous behavior, never a change to it."""
    cls = _load_server(name, version_dir, class_name)
    srv, cursor = _make_server(
        cls,
        columns=["id", "name"],
        rows=[{"id": 101, "name": "new"}],
    )
    _export(srv, use_keyset_paging=True, cursor_column="id", since_cursor=100)

    sql, binds = cursor.executed[-1]
    normalized = " ".join(sql.split()).lower()
    assert " or " not in normalized, f"{name}: lone predicate should not be an OR group:\n{sql}"
    assert binds is not None and len(binds) == 1, binds
    assert str(binds[0]) == "100"
