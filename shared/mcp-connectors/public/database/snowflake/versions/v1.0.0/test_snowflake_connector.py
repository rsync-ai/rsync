#!/usr/bin/env python3
"""Offline unit tests for the Snowflake MCP connector.

Snowflake ships a PEP-249 DB-API driver (snowflake-connector-python), so this
connector rides the DB-API path — NOT a warehouse adapter. Snowflake-specific
correctness rules pinned here because "just copy redshift" would be wrong:

  * Snowflake HAS ``MERGE INTO`` — upsert is a single native MERGE (Redshift used
    DELETE USING / INSERT SELECT). merge_method must be ``merge``.
  * Auth is password OR key-pair (``private_key_file``); validate_config accepts
    either and rejects neither.
  * Default bulk path is a batched multi-row ``INSERT``; the flag-gated
    ``COPY INTO`` from a stage is dormant in v1.0.0 and must fall back to the
    multi-row INSERT (never a silent drop).
  * Tables may be 3-level ``database.schema.table``, double-quoted.

Plus the shared read optimization (keyset/offset pagination) and identifier safety.

Run standalone (no pytest, no snowflake driver):
    python3 test_snowflake_connector.py
"""
import contextlib
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import connector as sf  # noqa: E402  (the module under test)
import _sf_fakes  # noqa: E402

CFG = {"config": {"account": "xy12345.us-east-1", "user": "u", "password": "p",
                  "warehouse": "WH", "database": "DB", "schema": "PUBLIC"}}


@contextlib.contextmanager
def env(**kw):
    saved = {k: os.environ.get(k) for k in kw}
    try:
        for k, v in kw.items():
            os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
        yield
    finally:
        for k, v in saved.items():
            os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)


def _rows(n):
    return [{"ID": i, "V": f"r{i}"} for i in range(n)]


# ============================== identity ====================================

def test_connector_identity_and_capabilities():
    s = sf.SnowflakeMCPServer()
    assert s.connector_type == "snowflake", s.connector_type
    assert s.connector_category == "data_warehouse", s.connector_category
    assert s.supports_source is True and s.supports_destination is True
    assert s.supports_cdc is False, "CDC/merge is dormant in v1.0.0"
    caps = s.get_capabilities()
    assert caps["connector_type"] == "snowflake", caps
    ls = caps.get("load_strategy") or {}
    assert ls.get("load_method"), ls
    assert ls.get("merge_method") == "merge", ls   # native MERGE, NOT delete_insert


def test_validate_config_requires_account_user_database():
    s = sf.SnowflakeMCPServer()
    bad = s.validate_config({"config": {"user": "u"}})
    assert bad["valid"] is False, bad
    good = s.validate_config(CFG)
    assert good["valid"] is True, good


def test_validate_config_accepts_key_pair_without_password():
    s = sf.SnowflakeMCPServer()
    out = s.validate_config({"config": {"account": "a", "user": "u",
                                        "database": "DB",
                                        "private_key_file": "/keys/rsa.p8"}})
    assert out["valid"] is True, out


def test_validate_config_rejects_no_credentials():
    s = sf.SnowflakeMCPServer()
    out = s.validate_config({"config": {"account": "a", "user": "u",
                                        "database": "DB"}})
    assert out["valid"] is False, out
    assert any("credential" in e.lower() for e in out["errors"]), out


# ============================ READ: pagination ==============================

def test_export_keyset_returns_next_cursor_and_has_more():
    s, conn = _sf_fakes.make_connector(sf, rows=[{"ID": 1}, {"ID": 2}, {"ID": 3}])
    out = s.export({**CFG, "table": "T", "cursor_column": "ID", "limit": 3})
    assert out["success"] is True, out
    assert out["paging_mode"] == "keyset", out
    assert out["next_cursor"] == 3 and out["has_more"] is True, out
    sql = conn.all_sql()[-1]
    assert 'ORDER BY "ID" ASC' in sql and "LIMIT 3" in sql, sql
    assert '"PUBLIC"."T"' in sql, sql


def test_export_keyset_continuation_uses_bound_param():
    s, conn = _sf_fakes.make_connector(sf, rows=[{"ID": 9}])
    out = s.export({**CFG, "table": "T", "cursor_column": "ID",
                    "cursor_value": 8, "limit": 10})
    assert out["success"] is True, out
    e = conn.all_execs()[-1]
    assert 'WHERE "ID" > %s' in e["sql"], e["sql"]
    assert e["params"] == [8], e["params"]           # parameterized, not inlined


def test_export_three_level_qualification():
    s, conn = _sf_fakes.make_connector(sf, rows=[{"ID": 1}])
    out = s.export({**CFG, "table": "OTHERDB.SALES.ORDERS", "limit": 5})
    assert out["success"] is True, out
    assert '"OTHERDB"."SALES"."ORDERS"' in conn.all_sql()[-1], conn.all_sql()


def test_export_offset_mode_emits_no_cursor_but_signals_has_more():
    """Offset paging (keyless table) must NOT emit a next_cursor. The executor
    advances `offset` itself only when the connector returns no cursor
    (executor.go: ``if cursor == nil``). Emitting str(offset+limit) made it treat
    offset paging as keyset — never advancing offset — silently truncating
    keyless tables at `limit`. has_more stays True so the loop keeps going."""
    s, conn = _sf_fakes.make_connector(sf, rows=[{"A": 1}, {"A": 2}])
    out = s.export({**CFG, "table": "T", "limit": 2, "offset": 4})
    assert out["paging_mode"] == "offset", out
    assert out["has_more"] is True, out
    assert out["next_cursor"] is None, out              # regression guard: no offset cursor
    assert "LIMIT 2 OFFSET 4" in conn.all_sql()[-1], conn.all_sql()


def test_export_rejects_unsafe_cursor_column():
    s, conn = _sf_fakes.make_connector(sf, rows=[{"ID": 1}])
    out = s.export({**CFG, "table": "T",
                    "cursor_column": 'id"; DROP TABLE x; --', "limit": 5})
    assert out["success"] is False, out
    assert "cursor" in out.get("error", "").lower(), out
    assert all("DROP TABLE" not in q for q in conn.all_sql()), conn.all_sql()


def test_export_query_mode_flags_truncation():
    s, _ = _sf_fakes.make_connector(sf, rows=[{"ID": i} for i in range(5)])
    out = s.export({**CFG, "query": "SELECT * FROM t", "limit": 5})
    assert out["paging_mode"] == "query" and out.get("truncated") is True, out


# ============================ WRITE: bulk load ==============================

def test_load_default_is_batched_multi_row_insert():
    s, conn = _sf_fakes.make_connector(sf)
    with env(RSYNC_SNOWFLAKE_COPY_STAGE=None):
        out = s.load({**CFG, "table": "T", "data": _rows(3)})
    assert out["success"] is True, out
    assert out["method"] == "multi_insert", out
    assert out["fell_back"] is False and out["rows_loaded"] == 3, out
    inserts = [e for e in conn.all_execs() if e["sql"].startswith("INSERT INTO")]
    assert len(inserts) == 1, inserts
    sql = inserts[0]["sql"]
    assert sql.count("(%s, %s)") == 3, sql            # 3 value tuples, one statement
    assert '"ID"' in sql and '"V"' in sql, sql
    assert conn.committed >= 1, "load must commit"


def test_load_chunks_by_page_size():
    s, conn = _sf_fakes.make_connector(sf)
    out = s.load({**CFG, "table": "T", "data": _rows(5), "page_size": 2})
    assert out["rows_loaded"] == 5, out
    inserts = [e for e in conn.all_execs() if e["sql"].startswith("INSERT INTO")]
    assert len(inserts) == 3, inserts                  # 2 + 2 + 1


def test_load_copy_stage_flag_is_dormant_and_falls_back():
    """copy_stage flag on: the path is dormant in v1.0.0, so it must fall back to
    the multi-row INSERT (rows still land) and report fell_back — never a silent
    drop."""
    s, conn = _sf_fakes.make_connector(sf)
    out = s.load({**CFG, "table": "T", "data": _rows(4), "copy_stage": True})
    assert out["success"] is True, out
    assert out["method"] == "multi_insert", out
    assert out["fell_back"] is True, out
    assert out["rows_loaded"] == 4, out


def test_load_rejects_unsafe_column_identifier():
    s, _ = _sf_fakes.make_connector(sf)
    out = s.load({**CFG, "table": "T",
                  "data": [{"ID": 1, 'bad"col': 2}]})
    assert out["success"] is False, out
    assert "column" in out.get("error", "").lower(), out


# =================== dialect correctness (the gotchas) ======================

def test_build_copy_sql_uses_stage_not_stdin():
    sql = sf.SnowflakeMCPServer._build_copy_sql('"DB"."PUBLIC"."T"', "@my_stage/f.json")
    assert "FROM '@my_stage/f.json'" in sql, sql
    assert "STDIN" not in sql, sql
    assert "COPY INTO" in sql and "FILE_FORMAT" in sql, sql


def test_build_merge_sql_uses_native_merge_not_on_conflict():
    sql = sf.SnowflakeMCPServer._build_merge_sql(
        '"DB"."PUBLIC"."T"', '"_RSYNC_STG_SNOWFLAKE"', ["ID", "V"], ["ID"])
    assert "ON CONFLICT" not in sql, sql
    assert sql.startswith("MERGE INTO"), sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql, sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql, sql
    assert '"V" =' in sql, sql                         # non-key column updated
    assert '"ID" =' not in sql.split("WHEN NOT MATCHED")[0].split(
        "UPDATE SET")[1], "key column must not be in UPDATE SET"


def test_merge_stages_and_merges():
    s, conn = _sf_fakes.make_connector(sf)
    out = s.merge({**CFG, "table": "T", "data": _rows(2), "key_fields": ["ID"]})
    assert out["success"] is True, out
    sqls = conn.all_sql()
    assert any(q.startswith("CREATE TEMPORARY TABLE") for q in sqls), sqls
    assert any(q.startswith("MERGE INTO") for q in sqls), sqls
    assert conn.committed >= 1, "merge must commit"


def test_bulk_insert_caps_bind_params_for_wide_table():
    """A wide table at the default 1000-row page can exceed Snowflake's bind
    limit; _bulk_insert must chunk so every INSERT stays within _MAX_BIND_PARAMS
    (parity with the databricks fix root-caused on a live warehouse write)."""
    s, conn = _sf_fakes.make_connector(sf)
    ncols = 19
    wide = [{f"C{j}": (i * 100 + j) for j in range(ncols)} for i in range(1000)]
    out = s.load({**CFG, "table": "T", "data": wide})   # default page_size
    assert out["rows_loaded"] == 1000, out
    inserts = [e for e in conn.all_execs() if e["sql"].startswith("INSERT INTO")]
    cap = sf.SnowflakeMCPServer._MAX_BIND_PARAMS
    for e in inserts:
        assert len(e["params"]) <= cap, f"INSERT bound {len(e['params'])} > cap {cap}"
    assert sum(len(e["params"]) for e in inserts) == 1000 * ncols  # no rows lost


def test_load_auto_creates_table_before_insert():
    """The kafka-mcp-sink SKIPS ensure_table for WAREHOUSE dests (main.go
    `!isWarehouse` gate), so load() must create the table itself (CREATE TABLE IF
    NOT EXISTS) BEFORE the INSERT, or a fresh destination DLQs every row."""
    s, conn = _sf_fakes.make_connector(sf)
    out = s.load({**CFG, "table": "T", "data": [{"ID": 1, "V": "a"}, {"ID": 2, "V": "b"}],
                  "column_types": {"ID": "integer", "V": "string"}})
    assert out["rows_loaded"] == 2, out
    sqls = conn.all_sql()
    ci = next((i for i, q in enumerate(sqls) if q.startswith("CREATE TABLE IF NOT EXISTS")), -1)
    ii = next((i for i, q in enumerate(sqls) if q.startswith("INSERT INTO")), -1)
    assert ci >= 0 and ii >= 0 and ci < ii, f"CREATE must precede INSERT: {sqls}"
    assert '"PUBLIC"."T"' in sqls[ci] and "USING DELTA" not in sqls[ci], sqls[ci]
    assert '"ID" NUMBER(38,0)' in sqls[ci] and '"V" VARCHAR' in sqls[ci], sqls[ci]


def test_load_namespace_override_routes_both_create_and_insert_to_namespace():
    """destination_namespace arrives as a separate ``namespace`` param with a BARE
    table. BOTH the auto-CREATE and the INSERT must target ``<namespace>.<table>`` —
    NOT the connection's config["schema"] (PUBLIC). Regression for the live silent
    mis-route where ensure_table honored the namespace but load()'s INSERT fell back
    to config["schema"]. Mirrors databricks."""
    s, conn = _sf_fakes.make_connector(sf)
    out = s.load({**CFG, "table": "T", "namespace": "DBX_SINK_FINAL",
                  "data": [{"ID": 1, "V": "a"}, {"ID": 2, "V": "b"}],
                  "column_types": {"ID": "integer", "V": "string"}})
    assert out["rows_loaded"] == 2, out
    sqls = conn.all_sql()
    create = next(q for q in sqls if q.startswith("CREATE TABLE IF NOT EXISTS"))
    insert = next(q for q in sqls if q.startswith("INSERT INTO"))
    assert '"DBX_SINK_FINAL"."T"' in create, create
    assert '"DBX_SINK_FINAL"."T"' in insert, insert
    assert '"PUBLIC"."T"' not in insert, insert
    assert '"PUBLIC"."T"' not in create, create


# ============ destination DDL (auto-create + reload) — sink parity ==========

def test_get_capabilities_accepts_params_and_advertises_ddl():
    """get_capabilities must accept the /mcp tool_args (a no-arg override broke the
    sink probe with a TypeError) AND advertise supports_ddl + auto_create +
    ensure_table, or the sink silently skips table creation and DLQs every row."""
    s = sf.SnowflakeMCPServer()
    caps = s.get_capabilities({})                        # must not raise
    assert caps["connector_type"] == "snowflake", caps
    assert caps.get("success") is True, caps             # sink DDL probe gate
    assert caps.get("supports_ddl") is True, caps
    assert caps.get("auto_create_destination_tables") is True, caps
    ops = {o["name"] for o in caps.get("operations", [])}
    assert {"ensure_table", "drop_table"} <= ops, caps


def test_ensure_table_creates_schema_then_table_double_quoted_typed():
    s, conn = _sf_fakes.make_connector(sf)
    out = s.ensure_table({
        **CFG, "table": "TRIPS",
        "columns": ["ID", "AMOUNT", "TS", "DESCR"],
        "column_types": {"ID": "integer", "AMOUNT": "number",
                         "TS": "timestamp", "DESCR": "string"},
        "key_fields": ["ID"],
    })
    assert out["success"] is True and out["created"] is True, out
    sqls = conn.all_sql()
    assert any(q.startswith("CREATE SCHEMA IF NOT EXISTS") and '"PUBLIC"' in q
               for q in sqls), sqls
    create = [q for q in sqls if q.startswith("CREATE TABLE IF NOT EXISTS")]
    assert create, sqls
    c = create[0]
    assert '"PUBLIC"."TRIPS"' in c, c
    assert "USING DELTA" not in c, c                     # snowflake, not Delta
    assert '"ID" NUMBER(38,0)' in c and '"AMOUNT" FLOAT' in c, c
    assert '"TS" TIMESTAMP_NTZ' in c and '"DESCR" VARCHAR' in c, c


def test_ensure_table_honors_namespace_override_and_injects_key_columns():
    s, conn = _sf_fakes.make_connector(sf)
    out = s.ensure_table({
        **CFG, "table": "ORDERS", "namespace": "SALES",
        "columns": ["TOTAL"], "column_types": {"TOTAL": "number"},
        "key_fields": ["ORDER_ID"],   # absent from columns -> must be added
    })
    assert out["success"] is True, out
    c = [q for q in conn.all_sql() if q.startswith("CREATE TABLE IF NOT EXISTS")][0]
    assert '"SALES"."ORDERS"' in c, c
    assert '"ORDER_ID" VARCHAR' in c, c
    assert '"TOTAL" FLOAT' in c, c


def test_ensure_table_passes_ddl_types_through_when_types_are_ddl():
    s, conn = _sf_fakes.make_connector(sf)
    out = s.ensure_table({
        **CFG, "table": "T", "columns": ["A"],
        "column_types": {"A": "NUMBER(10,2)"}, "types_are_ddl": True,
    })
    assert out["success"] is True, out
    c = [q for q in conn.all_sql() if q.startswith("CREATE TABLE")][0]
    assert '"A" NUMBER(10,2)' in c, c


def test_ensure_table_requires_table():
    s, _ = _sf_fakes.make_connector(sf)
    out = s.ensure_table({**CFG, "columns": ["A"]})
    assert out["success"] is False and "table" in out.get("error", "").lower(), out


def test_drop_table_issues_drop_if_exists_double_quoted():
    s, conn = _sf_fakes.make_connector(sf)
    out = s.drop_table({**CFG, "table": "T"})
    assert out["success"] is True and out["dropped"] is True, out
    assert any(q.startswith("DROP TABLE IF EXISTS") and '"PUBLIC"."T"' in q
               for q in conn.all_sql()), conn.all_sql()


def test_drop_table_honors_namespace_override():
    s, conn = _sf_fakes.make_connector(sf)
    out = s.drop_table({**CFG, "table": "ORDERS", "namespace": "SALES"})
    assert out["success"] is True, out
    assert any('"SALES"."ORDERS"' in q for q in conn.all_sql()), conn.all_sql()


# ================================= runner ===================================

def _run():
    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_run())
