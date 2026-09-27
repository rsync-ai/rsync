"""An unchanged TOAST column must survive the upsert, not be overwritten with NULL.

Debezium does not resend a large (TOAST-able) column that did not change in an
UPDATE; it sends the sentinel ``__debezium_unavailable_value`` instead. The
kafka-mcp-sink's ``filterDebeziumUnavailable`` (kafka-sink-worker main.go) DROPS
that key from the row, on one explicit contract:

    a missing key means "leave this column alone".

The destination connectors broke that contract. ``upsert_data`` derived ONE column
list per batch — the union of every row's keys, added by KI-RAGGED-COLS-1 so that
ragged json_flatten output would not lose columns unique to later rows — and then
bound every row against it with ``row.get(col)``. A key the sink had dropped came
back as ``None``, the column stayed in the SET clause, and
``SET "body" = EXCLUDED."body"`` wrote NULL over the destination's good value.

The union is NULL-safe (no column is lost), which is what KI-RAGGED-COLS-1 claimed.
It is not PRESERVE-safe, which is what this filter needs. The bug could only fire on
a batch that MIXED shapes — one row carrying the TOAST column, one not — because a
single-row batch's union is that row's own keys. That is the normal shape of a CDC
batch, and it is invisible: the write succeeds and reports the right row count.

These scenarios drive the REAL ``upsert_data`` of each destination connector against
a fake cursor and assert on the SQL it emits, so nothing here depends on a live
database. The union is still asserted to reach DDL, where it is correct.
"""
from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]  # shared/mcp-connectors
TEMPLATE = (
    ROOT.parents[1]
    / "llm-service"
    / "src"
    / "agents"
    / "tool_generator"
    / "templates"
    / "connector_database.py.j2"
)

# The row the source sent in full, and the row whose TOAST column the sink dropped.
ROW_FULL = {"id": 1, "name": "alice", "body": "a very large document"}
ROW_TOAST_DROPPED = {"id": 2, "name": "bob"}  # "body" removed by filterDebeziumUnavailable


def _current_dir(rel: str) -> Path:
    """Resolve versions/<latest.json.current_version>, never the highest directory."""
    base = ROOT / rel
    latest = json.loads((base / "latest.json").read_text())
    return base / "versions" / latest["current_version"]


def _load(rel: str, alias: str):
    path = _current_dir(rel) / "connector.py"
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, query, values=None):
        self.log.append(("execute", " ".join(str(query).split()), values))

    def executemany(self, query, values):
        self.log.append(("executemany", " ".join(str(query).split()), list(values)))

    def close(self):
        pass


class FakeConn:
    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _sql(log):
    return [entry[1] for entry in log]


def _writes(log):
    """Only the data statements — the connectors also introspect column types."""
    return [e for e in log if e[1].lstrip().upper().startswith(("INSERT", "MERGE", "DELETE"))]


# --------------------------------------------------------------------------- #
# PostgreSQL — the destination the sink's comment names                        #
# --------------------------------------------------------------------------- #
def _pg(rows, *, staged=False):
    mod = _load("public/postgresql", "_toast_pg_connector")
    srv = mod.PostgresqlMCPServer.__new__(mod.PostgresqlMCPServer)
    srv.driver_pattern = {"module": "psycopg2", "is_nosql": False}
    log: list = []
    staged_calls: list = []
    ddl_calls: list = []

    srv.prepare_import_data = lambda params: {
        "success": True,
        "data": params["data"],
        "config": {},
        "table": "public.orders",
    }
    srv._normalize_table_for_postgresql = lambda config, table, params=None: table
    srv._safe_qualified_table = lambda table, quote='"': table
    srv._acquire_conn = lambda config: (FakeConn(), False)
    srv._get_cursor = lambda conn, as_dict=True: FakeCursor(log)
    srv.supports_staged_load = lambda: staged
    srv.staged_upsert = lambda cursor, table, columns, data, key_fields, col_types: (
        staged_calls.append(list(columns)) or len(data)
    )
    srv._merge_conflict_keys = lambda cursor, target_table, columns, key_fields: ["id"]
    srv._qualified_quoted_table = lambda table: '"public"."orders"'
    srv._ensure_table_for_cdc = lambda *a, **k: ddl_calls.append(a)
    srv._write_cdc_offsets = lambda cursor, params: None
    srv._release_conn = lambda conn, pooled, config, broken=False: None

    result = srv.upsert_data({"data": rows, "key_fields": ["id"]})
    return result, log, staged_calls


def test_postgres_does_not_null_a_column_the_row_never_carried():
    result, log, _ = _pg([dict(ROW_FULL), dict(ROW_TOAST_DROPPED)])
    assert result["success"] is True, result
    statements = _sql(log)
    assert len(statements) == 2, statements

    # The row that carried "body" updates it.
    assert '"body" = EXCLUDED."body"' in statements[0]

    # The row whose "body" the sink dropped must not mention the column AT ALL —
    # not in the column list, not in VALUES, not in the SET clause. Anything else
    # binds NULL and destroys the destination's unchanged TOAST value.
    assert "body" not in statements[1], statements[1]

    # ...and it must not have smuggled a NULL in as a positional parameter either.
    values = log[1][2]
    assert values == (2, "bob"), values


def test_postgres_still_writes_an_explicit_null():
    """An absent key means "leave it alone"; a key present with None means "set NULL"."""
    _, log, _ = _pg([dict(ROW_FULL), {"id": 2, "name": "bob", "body": None}])
    statements = _sql(log)
    assert '"body" = EXCLUDED."body"' in statements[1], statements[1]
    assert log[1][2] == (2, "bob", None), log[1][2]


def test_postgres_uniform_batch_keeps_the_bulk_staged_path():
    """Control: the raggedness gate must not cost the fast path on a normal batch."""
    _, _, staged_calls = _pg(
        [dict(ROW_FULL), {"id": 2, "name": "bob", "body": "another document"}],
        staged=True,
    )
    assert staged_calls == [["id", "name", "body"]], staged_calls


def test_postgres_ragged_batch_skips_the_staged_path():
    """staged_upsert bulk-loads one fixed column list, so it cannot omit a column."""
    _, log, staged_calls = _pg(
        [dict(ROW_FULL), dict(ROW_TOAST_DROPPED)],
        staged=True,
    )
    assert staged_calls == [], staged_calls
    assert len(_sql(log)) == 2


def test_postgres_rejects_a_row_with_no_columns_instead_of_writing_all_nulls():
    result, _, _ = _pg([dict(ROW_FULL), {}])
    assert result["success"] is False
    assert "carries no columns" in result["error"], result


# --------------------------------------------------------------------------- #
# MySQL — same contract, executemany shape                                     #
# --------------------------------------------------------------------------- #
def _mysql(rows):
    mod = _load("public/database/mysql", "_toast_mysql_connector")
    srv = mod.MysqlMCPServer.__new__(mod.MysqlMCPServer)
    srv.max_batch_size = 1000
    log: list = []

    srv.prepare_import_data = lambda params: {
        "success": True,
        "data": params["data"],
        "config": {},
        "table": "orders",
    }
    srv._safe_qualified_table = lambda table, quote="`": table
    srv._split_mysql_db_table = lambda config, table, params=None: ("appdb", "orders")
    srv._get_connection = lambda config: FakeConn()
    srv._get_cursor = lambda conn, as_dict=True: FakeCursor(log)
    srv._write_cdc_offsets = lambda cursor, params: None

    result = srv.upsert_data({"data": rows, "key_fields": ["id"]})
    return result, log


def test_mysql_does_not_null_a_column_the_row_never_carried():
    result, log = _mysql([dict(ROW_FULL), dict(ROW_TOAST_DROPPED)])
    assert result["success"] is True, result
    writes = _writes(log)
    # Two shapes -> two statements, never one union statement covering both.
    assert len(writes) == 2, _sql(log)
    assert "`body`=VALUES(`body`)" in writes[0][1]
    assert "body" not in writes[1][1], writes[1][1]
    assert writes[1][2] == [(2, "bob")], writes[1][2]


def test_mysql_uniform_batch_is_still_one_executemany():
    """Control: a normal batch must not be split into one statement per row."""
    result, log = _mysql(
        [dict(ROW_FULL), {"id": 2, "name": "bob", "body": "another document"}]
    )
    assert result["success"] is True, result
    writes = _writes(log)
    assert len(writes) == 1, _sql(log)
    assert writes[0][0] == "executemany"
    assert len(writes[0][2]) == 2


# --------------------------------------------------------------------------- #
# _group_rows_by_shape — ordering is part of the contract                      #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "rel,alias",
    [
        ("public/database/mysql", "_shape_mysql"),
        ("public/database/oracle", "_shape_oracle"),
        ("public/database/sqlserver", "_shape_sqlserver"),
    ],
)
def test_shape_runs_are_consecutive_never_regrouped(rel, alias):
    """Two changes to the same key must still apply in arrival order.

    Grouping every same-shape row together would move the LAST write of a key
    ahead of an earlier one of a different shape, so the stale value would win.
    Runs are therefore consecutive: the price is an extra statement, never a
    reordered write.
    """
    group = _load(rel, alias)._group_rows_by_shape
    columns = ["id", "name", "body"]
    rows = [
        {"id": 1, "name": "a", "body": "x"},
        {"id": 1, "name": "a2"},
        {"id": 1, "name": "a3", "body": "y"},
    ]
    groups = group(rows, columns)
    assert [cols for cols, _ in groups] == [
        ["id", "name", "body"],
        ["id", "name"],
        ["id", "name", "body"],
    ]
    assert [len(r) for _, r in groups] == [1, 1, 1]

    # A uniform batch is ONE run — the previous single-statement behaviour.
    uniform = [{"id": 1, "name": "a", "body": "x"}, {"id": 2, "name": "b", "body": "y"}]
    assert len(group(uniform, columns)) == 1


# --------------------------------------------------------------------------- #
# The Jinja template every future database connector is generated from         #
# --------------------------------------------------------------------------- #
def _template_upsert_body() -> str:
    src = TEMPLATE.read_text()
    start = src.index("    def upsert_data(self, params: Dict = None)")
    end = src.index("\n    def ", start + 10)
    return src[start:end]


def test_template_binds_row_columns_not_the_batch_union():
    body = _template_upsert_body()
    assert "row_cols = [c for c in columns if c in row]" in body
    # No value binding may iterate the union any more.
    leaked = re.findall(r"row\.get\(col\) for col in columns", body)
    assert leaked == [], leaked


def test_template_still_passes_the_union_to_ddl():
    """DDL wants every column the batch will write — that union is correct there."""
    body = _template_upsert_body()
    assert "self._ensure_table_for_cdc(cursor, table, columns, conflict_cols_list, _ct)" in body
