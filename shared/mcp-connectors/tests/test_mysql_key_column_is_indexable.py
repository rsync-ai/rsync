"""A MySQL destination's key column must be a type MySQL can put a UNIQUE index on.

Bug class: an upsert key column created with an unbounded type. MySQL refuses a
UNIQUE index on TEXT/BLOB/JSON without a prefix length (error 1170), both
``ensure_table`` paths swallowed that error, and ``INSERT … ON DUPLICATE KEY
UPDATE`` with no unique index never finds a duplicate: every replay, re-snapshot or
retried batch inserted the rows again while the write reported success.

The keyless synthetic key ``_rsync_row_hash`` is declared canonical ``string``,
which the shared type map turned into TEXT, so every keyless table streamed into
MySQL hit it. A source ``text`` primary key hit it too. SQL Server and Oracle were
already bounded through ``canonical_to_ddl(..., is_key=True)``; MySQL was missing
from that map.

These drive the real ensure-table methods against a fake cursor and assert on the
DDL they emit; nothing here needs a live database.
"""
from __future__ import annotations

import importlib.util
import json
import logging
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # shared/mcp-connectors
TEMPLATE = (
    ROOT.parents[1]
    / "llm-service" / "src" / "agents" / "tool_generator" / "templates"
    / "connector_database.py.j2"
)
sys.path.insert(0, str(ROOT))  # base_connector
sys.path.insert(0, str(ROOT / "public"))
from canonical_types import canonical_to_ddl  # noqa: E402

UNINDEXABLE = re.compile(r"^(TINY|MEDIUM|LONG)?(TEXT|BLOB)$|^JSON$", re.I)


def _load_mysql():
    base = ROOT / "public" / "database" / "mysql"
    current = json.loads((base / "latest.json").read_text())["current_version"]
    path = base / "versions" / current / "connector.py"
    spec = importlib.util.spec_from_file_location("_key_idx_mysql_connector", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_key_idx_mysql_connector"] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    """Records SQL; answers information_schema reads from ``existing``."""

    def __init__(self, existing=None, fail_index=False):
        self.sql: list[str] = []
        self.existing = existing or {}
        self.fail_index = fail_index
        self._last = ""

    def execute(self, query, values=None):
        q = " ".join(str(query).split())
        self.sql.append(q)
        self._last = q
        if self.fail_index and q.upper().startswith("CREATE UNIQUE INDEX"):
            raise RuntimeError("(1170, \"BLOB/TEXT column used in key specification without a key length\")")

    def fetchall(self):
        if "information_schema.columns" in self._last:
            return [{"column_name": c, "data_type": t} for c, t in self.existing.items()]
        return []

    def fetchone(self):
        return None  # no index yet


def _server():
    mod = _load_mysql()
    srv = mod.MysqlMCPServer.__new__(mod.MysqlMCPServer)
    srv._split_mysql_db_table = lambda config, table, params=None: ("dest", table)
    return srv


def _column_ddl(create_sql: str, col: str) -> str:
    m = re.search(rf"`{col}` ([A-Za-z0-9_]+(?:\([0-9, ]+\))?)", create_sql)
    assert m, create_sql
    return m.group(1)


def _create(cur):
    return next(s for s in cur.sql if s.upper().startswith("CREATE TABLE"))


def test_canonical_mysql_key_types_are_bounded():
    assert canonical_to_ddl("mysql", "string") == "TEXT"  # non-key stays unbounded
    for canon in ("string", "json", "binary"):
        assert not UNINDEXABLE.match(canonical_to_ddl("mysql", canon, is_key=True, single_key=True))
        assert not UNINDEXABLE.match(canonical_to_ddl("mysql", canon, is_key=True, single_key=False))


def test_cdc_path_creates_the_synthetic_key_as_an_indexable_type():
    srv, cur = _server(), FakeCursor()
    srv._ensure_table_for_cdc(
        cur, {}, "orders", ["name", "_rsync_row_hash"], ["_rsync_row_hash"],
        {"name": "string", "_rsync_row_hash": "string"},
    )
    create = _create(cur)
    assert not UNINDEXABLE.match(_column_ddl(create, "_rsync_row_hash")), create
    assert _column_ddl(create, "name") == "TEXT", create  # a non-key column is untouched
    assert any(s.upper().startswith("CREATE UNIQUE INDEX") for s in cur.sql)


def test_cdc_path_bounds_a_text_business_key():
    srv, cur = _server(), FakeCursor()
    srv._ensure_table_for_cdc(cur, {}, "tags", ["tag", "n"], ["tag"], {"tag": "TEXT", "n": "BIGINT"})
    assert not UNINDEXABLE.match(_column_ddl(_create(cur), "tag"))


def test_cdc_path_narrows_an_existing_text_key_column_before_indexing_it():
    """Tables created before the fix already hold a TEXT key; the index must still land."""
    srv = _server()
    cur = FakeCursor(existing={"name": "text", "_rsync_row_hash": "text"})
    srv._ensure_table_for_cdc(
        cur, {}, "orders", ["name", "_rsync_row_hash"], ["_rsync_row_hash"],
        {"name": "string", "_rsync_row_hash": "string"},
    )
    modify = [i for i, s in enumerate(cur.sql) if s.startswith("ALTER TABLE") and "MODIFY COLUMN `_rsync_row_hash`" in s]
    index = [i for i, s in enumerate(cur.sql) if s.upper().startswith("CREATE UNIQUE INDEX")]
    assert modify and index and modify[0] < index[0], cur.sql
    assert not any("MODIFY COLUMN `name`" in s for s in cur.sql), cur.sql


def test_untyped_path_creates_key_columns_as_an_indexable_type():
    srv, cur = _server(), FakeCursor()
    srv._ensure_table_for_mysql(cur, {}, "orders", ["name", "_rsync_row_hash"], ["_rsync_row_hash"])
    create = _create(cur)
    assert not UNINDEXABLE.match(_column_ddl(create, "_rsync_row_hash")), create
    assert _column_ddl(create, "name") == "TEXT", create


def test_untyped_path_narrows_an_existing_text_key_column():
    srv = _server()
    cur = FakeCursor(existing={"name": "text", "_rsync_row_hash": "text"})
    srv._ensure_table_for_mysql(cur, {}, "orders", ["name", "_rsync_row_hash"], ["_rsync_row_hash"])
    modify = [i for i, s in enumerate(cur.sql) if "MODIFY COLUMN `_rsync_row_hash`" in s]
    index = [i for i, s in enumerate(cur.sql) if s.upper().startswith("CREATE UNIQUE INDEX")]
    assert modify and index and modify[0] < index[0], cur.sql


def test_a_unique_index_that_cannot_be_created_is_logged_not_silent(caplog):
    srv, cur = _server(), FakeCursor(fail_index=True)
    with caplog.at_level(logging.WARNING):
        srv._ensure_table_for_cdc(cur, {}, "orders", ["id"], ["id"], {"id": "BIGINT"})
        srv._ensure_table_for_mysql(cur, {}, "orders", ["id"], ["id"])
    hits = [r for r in caplog.records if "unique index" in r.getMessage().lower()]
    assert len(hits) == 2, [r.getMessage() for r in caplog.records]


def test_template_mysql_ensure_table_bounds_key_columns():
    """The generated-connector template carries the same MySQL ensure-table path."""
    src = TEMPLATE.read_text()
    start = src.index("def _ensure_table_for_mysql(")
    body = src[start:src.index("\n    def ", start + 10)]
    assert "is_key=True" in body, "template MySQL ensure_table must bound key columns"
    assert "MODIFY COLUMN" in body, "template must narrow an existing unbounded key column"
    # the widen pass must see the bounded key type, or it widens the key back to TEXT
    assert '_widen_existing_columns(cursor, db, name, safe_cols, ct, "mysql")' not in body
    assert "except Exception:\n                pass" not in body.split("CREATE UNIQUE INDEX")[-1][:400]
