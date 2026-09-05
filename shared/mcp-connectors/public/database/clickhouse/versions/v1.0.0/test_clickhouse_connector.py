#!/usr/bin/env python3
"""Offline unit tests for the ClickHouse MCP connector.

The clickhouse-connect driver is never imported — every test swaps the
``_connect`` seam for an in-memory fake that captures the SQL/DDL/insert calls
and answers queries from canned data. Run:

    python -m pytest test_clickhouse_connector.py -q
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from connector import ClickHouseMCPServer  # noqa: E402


class FakeResult:
    def __init__(self, column_names, result_rows):
        self.column_names = list(column_names)
        self.result_rows = [list(r) for r in result_rows]


class FakeClient:
    """Captures command()/raw_insert() and answers query() from a router."""

    def __init__(self, query_router=None):
        self.commands = []
        self.inserts = []
        self.queries = []
        self._router = query_router or (lambda sql, params: FakeResult([], []))

    def command(self, sql, settings=None):
        self.commands.append(sql)
        return ""

    def query(self, sql, parameters=None):
        self.queries.append((sql, parameters))
        return self._router(sql, parameters or {})

    def raw_insert(self, table, column_names=None, insert_block=None,
                   fmt=None, settings=None, compression=None):
        self.inserts.append({
            "table": table,
            "column_names": list(column_names or []),
            "block": insert_block.decode("utf-8") if isinstance(insert_block, (bytes, bytearray)) else insert_block,
            "fmt": fmt,
            "settings": settings or {},
        })

    def close(self):
        pass


def _server_with(client):
    srv = ClickHouseMCPServer()
    srv._connect = lambda config: client  # swap the driver seam
    return srv


CFG = {"host": "h", "port": 8123, "database": "demo", "user": "default", "password": "p"}


# --------------------------------------------------------------------------- #
# Type mapping
# --------------------------------------------------------------------------- #
def test_ch_ddl_type_canonical():
    s = ClickHouseMCPServer()
    assert s._ch_ddl_type("string") == "String"
    assert s._ch_ddl_type("integer") == "Int64"
    assert s._ch_ddl_type("number") == "Float64"
    assert s._ch_ddl_type("boolean") == "Bool"
    assert s._ch_ddl_type("timestamp") == "DateTime64(3)"
    assert s._ch_ddl_type("date") == "Date32"
    assert s._ch_ddl_type("json") == "String"
    assert s._ch_ddl_type("binary") == "String"
    assert s._ch_ddl_type("decimal") == "Decimal(38, 9)"
    assert s._ch_ddl_type("decimal(12,4)") == "Decimal(12, 4)"
    assert s._ch_ddl_type("weirdunknown") == "String"


def test_canonical_from_ch():
    s = ClickHouseMCPServer()
    assert s._canonical_from_ch("Nullable(String)") == "string"
    assert s._canonical_from_ch("LowCardinality(Nullable(String))") == "string"
    assert s._canonical_from_ch("Int64") == "integer"
    assert s._canonical_from_ch("UInt32") == "integer"
    assert s._canonical_from_ch("Float64") == "number"
    assert s._canonical_from_ch("Decimal(38, 9)") == "decimal"
    assert s._canonical_from_ch("DateTime64(3)") == "timestamp"
    assert s._canonical_from_ch("Date32") == "date"
    assert s._canonical_from_ch("Bool") == "boolean"
    assert s._canonical_from_ch("Array(String)") == "json"
    assert s._canonical_from_ch("UUID") == "string"


def test_canonical_from_ch_wide_integers_are_not_bigint():
    # _canonical_from_ch now delegates to the shared dialect-scoped
    # canonicalize_type("clickhouse"). This fixes the old local prefix-matcher
    # bug where every int/uint collapsed to "integer" (int64), overflowing the
    # destination BIGINT for wide unsigned/128+/256-bit widths.
    s = ClickHouseMCPServer()
    # UInt64 (up to ~1.8e19) fits NUMERIC(38,9) -> decimal, NOT int64.
    assert s._canonical_from_ch("UInt64") == "decimal"
    assert s._canonical_from_ch("Nullable(UInt64)") == "decimal"
    # 128/256-bit ints exceed NUMERIC(38) -> string (lossless), NOT int64.
    for t in ("Int128", "UInt128", "Int256", "UInt256"):
        assert s._canonical_from_ch(t) == "string", t
    assert s._canonical_from_ch("LowCardinality(UInt256)") == "string"
    # Narrow signed/unsigned still map to integer.
    for t in ("Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32"):
        assert s._canonical_from_ch(t) == "integer", t
    # Bonus corrections vs the old matcher (were dumped to "string"):
    assert s._canonical_from_ch("BFloat16") == "number"
    assert s._canonical_from_ch("Time") == "time"
    assert s._canonical_from_ch("Time64(3)") == "time"


def test_jsonify():
    import datetime
    import decimal
    import uuid
    s = ClickHouseMCPServer()
    assert s._jsonify(datetime.datetime(2021, 1, 2, 3, 4, 5)) == "2021-01-02T03:04:05"
    assert s._jsonify(datetime.date(2021, 1, 2)) == "2021-01-02"
    assert s._jsonify(decimal.Decimal("1.50")) == "1.50"
    u = uuid.uuid4()
    assert s._jsonify(u) == str(u)
    assert s._jsonify([1, decimal.Decimal("2")]) == [1, "2"]


# --------------------------------------------------------------------------- #
# ensure_table DDL
# --------------------------------------------------------------------------- #
def test_ensure_table_keyed():
    client = FakeClient()
    srv = _server_with(client)
    res = srv.ensure_table({
        "config": CFG,
        "table": "orders",
        "columns": ["id", "amount", "note"],
        "column_types": {"id": "integer", "amount": "decimal", "note": "string"},
        "key_fields": ["id"],
    })
    assert res["success"], res
    create = next(c for c in client.commands if c.startswith("CREATE TABLE"))
    assert "`demo`.`orders`" in create
    assert "ReplacingMergeTree" in create
    assert "ORDER BY (`id`)" in create
    # key column non-Nullable, others Nullable
    assert "`id` Int64" in create and "Nullable(Int64)" not in create.split("`id`")[1][:20]
    assert "`amount` Nullable(Decimal(38, 9))" in create
    assert "`note` Nullable(String)" in create


def test_ensure_table_keyless_synthetic_pk():
    client = FakeClient()
    srv = _server_with(client)
    res = srv.ensure_table({
        "config": CFG,
        "table": "events",
        "columns": ["a", "b"],
        "column_types": {"a": "string", "b": "integer"},
        "key_fields": [],
    })
    assert res["success"], res
    create = next(c for c in client.commands if c.startswith("CREATE TABLE"))
    assert "ReplacingMergeTree" in create
    assert "ORDER BY (`_rsync_row_hash`)" in create
    assert "`_rsync_row_hash` String" in create
    assert "`_rsync_synced_at` Nullable(DateTime64(3))" in create


def test_ensure_table_append_mode_no_key():
    client = FakeClient()
    srv = _server_with(client)
    res = srv.ensure_table({
        "config": CFG,
        "table": "log",
        "columns": ["a"],
        "column_types": {"a": "string"},
        "key_fields": [],
        "append_mode": True,
    })
    assert res["success"], res
    create = next(c for c in client.commands if c.startswith("CREATE TABLE"))
    assert "MergeTree" in create and "ReplacingMergeTree" not in create
    assert "ORDER BY tuple()" in create
    assert "_rsync_row_hash" not in create


def test_ensure_table_namespace_override():
    client = FakeClient()
    srv = _server_with(client)
    res = srv.ensure_table({
        "config": CFG,
        "table": "orders",              # bare table
        "namespace": "pipe_ns",         # single-namespace override
        "columns": ["id"],
        "column_types": {"id": "integer"},
        "key_fields": ["id"],
    })
    assert res["success"], res
    assert any("CREATE DATABASE IF NOT EXISTS `pipe_ns`" in c for c in client.commands)
    assert any("`pipe_ns`.`orders`" in c for c in client.commands if c.startswith("CREATE TABLE"))


def test_ensure_table_unsafe_identifier_rejected():
    srv = _server_with(FakeClient())
    res = srv.ensure_table({"config": CFG, "table": "bad; DROP TABLE x",
                            "columns": ["id"], "key_fields": ["id"]})
    assert not res["success"]
    assert "Unsafe" in res["error"] or "Invalid" in res["error"]


# --------------------------------------------------------------------------- #
# writes
# --------------------------------------------------------------------------- #
def test_import_data_jsoneachrow():
    client = FakeClient()
    srv = _server_with(client)
    res = srv.import_data({
        "config": CFG,
        "table": "orders",
        "columns": ["id", "meta"],
        "key_fields": ["id"],  # keyed -> no synthetic hash; keep this test on serialization
        "data": [{"id": 1, "meta": {"k": "v"}}, {"id": 2, "meta": None}],
    })
    assert res["success"], res
    assert res["rows_loaded"] == 2
    assert len(client.inserts) == 1
    ins = client.inserts[0]
    assert ins["table"] == "`demo`.`orders`"
    assert ins["fmt"] == "JSONEachRow"
    assert ins["column_names"] == ["id", "meta"]
    assert ins["settings"].get("date_time_input_format") == "best_effort"
    lines = ins["block"].splitlines()
    assert len(lines) == 2
    # nested dict is JSON-stringified so it lands in a String column
    assert '"meta": "{\\"k\\": \\"v\\"}"' in lines[0] or '\\"k\\": \\"v\\"' in lines[0]


def test_upsert_data_runs_optimize_final():
    client = FakeClient()
    srv = _server_with(client)
    res = srv.upsert_data({
        "config": CFG,
        "table": "orders",
        "columns": ["id", "v"],
        "data": [{"id": 1, "v": 10}],
        "key_fields": ["id"],
    })
    assert res["success"], res
    assert res["optimized"] is True
    assert any("OPTIMIZE TABLE `demo`.`orders` FINAL" in c for c in client.commands)


def test_upsert_keyless_optout_skips_optimize():
    # A keyless upsert now synthesizes a row-hash key by default (see H1), so the
    # ONLY way to skip the synthetic key + OPTIMIZE is an explicit opt-out.
    client = FakeClient()
    srv = _server_with(client)
    res = srv.upsert_data({"config": CFG, "table": "orders",
                           "columns": ["v"], "data": [{"v": 1}],
                           "synthetic_pk": False})
    assert res["success"], res
    assert res["optimized"] is False
    assert not any("OPTIMIZE" in c for c in client.commands)
    assert "_rsync_row_hash" not in client.inserts[0]["column_names"]


def test_write_empty_data_noop():
    srv = _server_with(FakeClient())
    res = srv.import_data({"config": CFG, "table": "orders", "data": []})
    assert res["success"] and res["rows_loaded"] == 0


def test_write_unsafe_column_rejected():
    srv = _server_with(FakeClient())
    res = srv.import_data({"config": CFG, "table": "orders",
                           "data": [{"ok": 1, "bad col": 2}]})
    assert not res["success"]
    assert "Unsafe column" in res["error"]


# --------------------------------------------------------------------------- #
# synthetic-PK write path (PK-less sources) — regression guard against the
# silent full-table collapse where every row shares an empty _rsync_row_hash.
# --------------------------------------------------------------------------- #
def _rows_from_insert(ins):
    import json as _json
    return [_json.loads(line) for line in ins["block"].splitlines()]


def test_synthetic_pk_write_computes_per_row_hash():
    client = FakeClient()
    srv = _server_with(client)
    data = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]
    res = srv.import_data({"config": CFG, "table": "keyless",
                           "data": data, "synthetic_pk": True})
    assert res["success"], res
    ins = client.inserts[0]
    # synthetic columns are added to the column universe
    assert "_rsync_row_hash" in ins["column_names"]
    assert "_rsync_synced_at" in ins["column_names"]
    out = _rows_from_insert(ins)
    hashes = [r["_rsync_row_hash"] for r in out]
    # every row gets a NON-empty, DISTINCT hash (the bug produced all-empty keys)
    assert all(h for h in hashes), hashes
    assert len(set(hashes)) == 3, hashes
    assert all(r["_rsync_synced_at"] for r in out)


def test_synthetic_pk_hash_is_stable_and_content_addressed():
    from connector import _compute_row_hash
    client = FakeClient()
    srv = _server_with(client)
    # two identical source rows must hash identically (idempotent reload);
    # a changed value must hash differently.
    data = [{"a": 1, "b": "x"}, {"a": 1, "b": "x"}, {"a": 1, "b": "CHANGED"}]
    srv.import_data({"config": CFG, "table": "keyless",
                     "data": data, "synthetic_pk": True})
    out = _rows_from_insert(client.inserts[0])
    assert out[0]["_rsync_row_hash"] == out[1]["_rsync_row_hash"]
    assert out[0]["_rsync_row_hash"] != out[2]["_rsync_row_hash"]
    # matches the shared cross-connector helper (lockstep with mysql/oracle/sqlserver)
    assert out[0]["_rsync_row_hash"] == _compute_row_hash({"a": 1, "b": "x"}, ["a", "b"])


def test_synthetic_pk_upsert_optimizes_on_hash_key():
    client = FakeClient()
    srv = _server_with(client)
    res = srv.upsert_data({"config": CFG, "table": "keyless",
                           "data": [{"a": 1}], "synthetic_pk": True})
    assert res["success"] and res["optimized"] is True
    # OPTIMIZE FINAL fires because the synthetic run has a (synthetic) key
    assert any("OPTIMIZE TABLE `demo`.`keyless` FINAL" in c for c in client.commands)


# H1 regression: _write synthetic detection MIRRORS ensure_table. ensure_table
# builds a ReplacingMergeTree ORDER BY _rsync_row_hash for ANY keyless table, so
# a write that did not stamp the per-row hash (because the caller omitted the
# explicit synthetic_pk flag) left every row sharing an empty key -> the whole
# batch collapsed to one row. _write now stamps the hash on any keyless,
# non-append write, closing that asymmetry.
def test_write_keyless_without_synthetic_flag_stamps_hash():
    client = FakeClient()
    srv = _server_with(client)
    data = [{"a": i, "b": f"v{i}"} for i in range(4)]
    res = srv.upsert_data({"config": CFG, "table": "keyless", "data": data})  # NO synthetic_pk
    assert res["success"], res
    ins = client.inserts[0]
    assert "_rsync_row_hash" in ins["column_names"]
    out = _rows_from_insert(ins)
    hashes = [r["_rsync_row_hash"] for r in out]
    assert all(h for h in hashes), hashes        # non-empty (bug produced all-empty)
    assert len(set(hashes)) == 4, hashes         # distinct -> no collapse


def test_write_append_mode_keyless_no_synthetic_hash():
    # append_mode opts out (ensure_table builds MergeTree ORDER BY tuple());
    # _write must NOT stamp a hash so a pure append keeps duplicate rows.
    client = FakeClient()
    srv = _server_with(client)
    res = srv.import_data({"config": CFG, "table": "appendk",
                           "data": [{"v": "x"}, {"v": "x"}], "append_mode": True})
    assert res["success"], res
    assert "_rsync_row_hash" not in client.inserts[0]["column_names"]


def test_write_explicit_synthetic_false_opts_out():
    # explicit synthetic_pk=False opts out even without append_mode.
    client = FakeClient()
    srv = _server_with(client)
    res = srv.import_data({"config": CFG, "table": "optout",
                           "data": [{"v": "x"}], "synthetic_pk": False})
    assert res["success"], res
    assert "_rsync_row_hash" not in client.inserts[0]["column_names"]


def test_write_keyed_no_synthetic_hash():
    # a real key means no synthetic hash is stamped.
    client = FakeClient()
    srv = _server_with(client)
    res = srv.upsert_data({"config": CFG, "table": "keyed",
                           "data": [{"id": 1, "v": "x"}], "key_fields": ["id"]})
    assert res["success"], res
    assert "_rsync_row_hash" not in client.inserts[0]["column_names"]


# --------------------------------------------------------------------------- #
# execute op (Data Explorer delegated write / read)
# --------------------------------------------------------------------------- #
def test_execute_read_returns_rows():
    rows = [[1, "a"], [2, "b"]]
    client = FakeClient(query_router=lambda sql, p: FakeResult(["id", "name"], rows))
    srv = _server_with(client)
    res = srv.execute({"config": CFG, "statement": "SELECT id, name FROM demo.orders"})
    assert res["success"], res
    assert res["row_count"] == 2
    assert res["data"][0] == {"id": 1, "name": "a"}
    # a read-shaped statement must NOT go through command()
    assert client.commands == []


def test_execute_write_runs_command():
    client = FakeClient()
    srv = _server_with(client)
    res = srv.execute({"config": CFG,
                       "statement": "ALTER TABLE demo.orders DELETE WHERE id = 1"})
    assert res["success"] and res["executed"] is True
    assert any("ALTER TABLE demo.orders DELETE" in c for c in client.commands)
    assert client.queries == []  # not a read


def test_execute_missing_statement():
    srv = _server_with(FakeClient())
    res = srv.execute({"config": CFG})
    assert not res["success"] and "Missing" in res["error"]


# H4 regression: a comment before a read must still classify as a read (run via
# query(), not command()) so the Explorer gets rows back.
def test_execute_comment_prefixed_select_is_read():
    for stmt in ("/* c */ SELECT count() AS n FROM t",
                 "-- lead\nSELECT count() AS n FROM t",
                 "  /* a */ -- b\n  WITH x AS (SELECT 1) SELECT * FROM x"):
        client = FakeClient(query_router=lambda sql, p: FakeResult(["n"], [[7]]))
        srv = _server_with(client)
        res = srv.execute({"config": CFG, "statement": stmt})
        assert res["success"], (stmt, res)
        assert res.get("data") is not None, (stmt, res)   # read path taken
        assert client.commands == [], (stmt, client.commands)


def test_strip_leading_sql_comments():
    s = ClickHouseMCPServer()
    assert s._strip_leading_sql_comments("/* x */ SELECT 1").startswith("SELECT")
    assert s._strip_leading_sql_comments("-- c\nSELECT 1").startswith("SELECT")
    assert s._strip_leading_sql_comments("  \n /* a */\n SELECT 1").startswith("SELECT")
    assert s._strip_leading_sql_comments("INSERT INTO t VALUES (1)").startswith("INSERT")


def test_execute_op_advertised_in_capabilities():
    caps = ClickHouseMCPServer().get_capabilities()
    methods = {op["method"] for op in caps["operations"]}
    assert "clickhouse_execute" in methods


# --------------------------------------------------------------------------- #
# export
# --------------------------------------------------------------------------- #
def test_export_offset_pagination():
    rows = [[i, f"n{i}"] for i in range(3)]
    client = FakeClient(query_router=lambda sql, p: FakeResult(["id", "name"], rows))
    srv = _server_with(client)
    res = srv.export({"config": CFG, "table": "orders", "limit": 3, "offset": 0})
    assert res["success"], res
    assert res["paging_mode"] == "offset"
    assert res["total_records"] == 3
    assert res["has_more"] is True            # full page → more
    assert res["next_cursor"] == "3"
    assert res["data"][0] == {"id": 0, "name": "n0"}


def test_export_last_page():
    rows = [[1, "a"]]
    client = FakeClient(query_router=lambda sql, p: FakeResult(["id", "n"], rows))
    srv = _server_with(client)
    res = srv.export({"config": CFG, "table": "orders", "limit": 10})
    assert res["has_more"] is False
    assert res["next_cursor"] is None


def test_export_keyset_builds_where():
    client = FakeClient(query_router=lambda sql, p: FakeResult(["id"], [[5]]))
    srv = _server_with(client)
    srv.export({"config": CFG, "table": "orders", "cursor_column": "id",
                "cursor_value": 4, "limit": 1})
    sql = client.queries[-1][0]
    assert "`id` > 4" in sql
    assert "ORDER BY `id` ASC LIMIT 1" in sql


def test_export_unsafe_cursor_rejected():
    srv = _server_with(FakeClient())
    res = srv.export({"config": CFG, "table": "orders", "cursor_column": "a; b"})
    assert not res["success"]


# --------------------------------------------------------------------------- #
# discover / drop / capabilities / validate
# --------------------------------------------------------------------------- #
def test_discover_schema_parses_columns_and_pks():
    def router(sql, p):
        if "version()" in sql:
            return FakeResult(["v"], [["25.5.6.14"]])
        if "system.tables" in sql:
            return FakeResult(["name", "row_estimate"], [["orders", 100]])
        if "system.columns" in sql:
            return FakeResult(
                ["table", "name", "type", "is_in_primary_key"],
                [["orders", "id", "Int64", 1],
                 ["orders", "amount", "Nullable(Decimal(38, 9))", 0]],
            )
        return FakeResult([], [])
    client = FakeClient(query_router=router)
    srv = _server_with(client)
    res = srv.discover_schema({"config": CFG})
    assert res["overall_status"] == "success"
    t = res["tables"][0]
    assert t["name"] == "orders" and t["row_count"] == 100
    assert t["primary_keys"] == ["id"]
    idc = {c["name"]: c for c in t["columns"]}
    assert idc["id"]["type"] == "integer" and idc["id"]["is_primary_key"] is True
    assert idc["amount"]["type"] == "decimal" and idc["amount"]["nullable"] is True


def test_drop_table():
    client = FakeClient()
    srv = _server_with(client)
    res = srv.drop_table({"config": CFG, "table": "orders"})
    assert res["success"] and res["dropped"] is True
    assert any("DROP TABLE IF EXISTS `demo`.`orders`" in c for c in client.commands)


def test_get_capabilities_flags():
    caps = ClickHouseMCPServer().get_capabilities()
    assert caps["supports_source"] and caps["supports_destination"]
    assert caps["supports_ddl"] is True
    assert caps["auto_create_destination_tables"] is True
    methods = {op["method"] for op in caps["operations"]}
    assert "clickhouse_ensure_table" in methods
    assert "clickhouse_upsert_data" in methods


def test_validate_config():
    srv = ClickHouseMCPServer()
    assert srv.validate_config({"config": {"host": "h", "user": "u", "database": "d"}})["valid"]
    bad = srv.validate_config({"config": {"database": "d"}})
    assert not bad["valid"]


def test_test_connection_ok():
    client = FakeClient(query_router=lambda sql, p: FakeResult(["v"], [["25.5.6"]]))
    srv = _server_with(client)
    res = srv.test_connection({"config": CFG})
    assert res["success"] and res["version"] == "25.5.6"


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
