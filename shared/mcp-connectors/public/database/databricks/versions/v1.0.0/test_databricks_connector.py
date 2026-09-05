#!/usr/bin/env python3
"""Offline unit tests for the Databricks MCP connector.

Databricks ships a PEP-249 DB-API driver (databricks-sql-connector), so this
connector rides the DB-API path — NOT a warehouse adapter. Databricks-specific
correctness rules pinned here because "just copy snowflake" would be wrong:

  * Databricks (Spark SQL) quotes identifiers with BACKTICKS, not double quotes,
    and tables are Unity Catalog 3-level ``catalog.schema.table``.
  * The driver returns ``Row`` objects (not dicts); export must normalize them to
    dicts via asDict()/description.
  * Delta supports ``MERGE INTO`` — upsert is a single native MERGE.
  * Default bulk path is a batched multi-row ``INSERT``; the flag-gated
    ``COPY INTO`` from cloud storage is dormant in v1.0.0 and must fall back to the
    multi-row INSERT (never a silent drop).
  * Auth is a Personal Access Token (server_hostname + http_path + access_token).

Run standalone (no pytest, no databricks driver):
    python3 test_databricks_connector.py
"""
import contextlib
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import connector as db  # noqa: E402  (the module under test)
import _db_fakes  # noqa: E402

CFG = {"config": {"server_hostname": "dbc-x.cloud.databricks.com",
                  "http_path": "/sql/1.0/warehouses/abc", "access_token": "dapi123",
                  "catalog": "main", "schema": "default"}}


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
    return [{"id": i, "v": f"r{i}"} for i in range(n)]


# ============================== identity ====================================

def test_connector_identity_and_capabilities():
    s = db.DatabricksMCPServer()
    assert s.connector_type == "databricks", s.connector_type
    assert s.connector_category == "data_warehouse", s.connector_category
    assert s.supports_source is True and s.supports_destination is True
    assert s.supports_cdc is True, "CDC-as-destination (soft-delete Delta MERGE) is live"
    caps = s.get_capabilities()
    assert caps["connector_type"] == "databricks", caps
    ls = caps.get("load_strategy") or {}
    assert ls.get("load_method"), ls
    assert ls.get("merge_method") == "merge", ls   # native Delta MERGE


def test_validate_config_requires_host_path_token():
    s = db.DatabricksMCPServer()
    bad = s.validate_config({"config": {"server_hostname": "h"}})
    assert bad["valid"] is False, bad
    assert any("http_path" in e for e in bad["errors"]), bad
    assert any("access_token" in e for e in bad["errors"]), bad
    good = s.validate_config(CFG)
    assert good["valid"] is True, good


# ============================ READ: pagination + rows =======================

def test_export_keyset_returns_next_cursor_and_has_more():
    s, conn = _db_fakes.make_connector(db, rows=[{"id": 1}, {"id": 2}, {"id": 3}])
    out = s.export({**CFG, "table": "t", "cursor_column": "id", "limit": 3})
    assert out["success"] is True, out
    assert out["paging_mode"] == "keyset", out
    assert out["next_cursor"] == 3 and out["has_more"] is True, out
    # rows came back as Row objects but must be normalized to dicts
    assert out["data"] == [{"id": 1}, {"id": 2}, {"id": 3}], out["data"]
    sql = conn.all_sql()[-1]
    assert "ORDER BY `id` ASC" in sql and "LIMIT 3" in sql, sql
    assert "`main`.`default`.`t`" in sql, sql          # backtick 3-level qualify


def test_export_keyset_continuation_uses_bound_param():
    s, conn = _db_fakes.make_connector(db, rows=[{"id": 9}])
    out = s.export({**CFG, "table": "t", "cursor_column": "id",
                    "cursor_value": 8, "limit": 10})
    assert out["success"] is True, out
    e = conn.all_execs()[-1]
    assert "WHERE `id` > ?" in e["sql"], e["sql"]       # Databricks native marker, not %s
    assert "%s" not in e["sql"], e["sql"]               # regression guard: %s is rejected by the driver
    assert e["params"] == [8], e["params"]             # parameterized, not inlined


def test_export_two_level_prepends_catalog():
    s, conn = _db_fakes.make_connector(db, rows=[{"id": 1}])
    out = s.export({**CFG, "table": "sales.orders", "limit": 5})
    assert out["success"] is True, out
    assert "`main`.`sales`.`orders`" in conn.all_sql()[-1], conn.all_sql()


def test_export_offset_mode_emits_no_cursor_but_signals_has_more():
    """Offset paging (keyless table) must NOT emit a next_cursor. The executor
    advances `offset` itself only when the connector returns no cursor
    (executor.go: ``if cursor == nil``). Emitting str(offset+limit) made the
    executor treat offset paging as keyset — it never advanced offset, re-read
    page 1, tripped the non-advancing-cursor guard, and SILENTLY TRUNCATED
    keyless tables at `limit` (the live samples.nyctaxi.trips read stopped at
    10000 of 21932). has_more must still be True so the loop keeps going."""
    s, conn = _db_fakes.make_connector(db, rows=[{"a": 1}, {"a": 2}])
    out = s.export({**CFG, "table": "t", "limit": 2, "offset": 4})
    assert out["paging_mode"] == "offset", out
    assert out["has_more"] is True, out
    assert out["next_cursor"] is None, out              # regression guard: no offset cursor
    assert "LIMIT 2 OFFSET 4" in conn.all_sql()[-1], conn.all_sql()


def test_export_offset_partial_page_stops():
    """A short final page (< limit) reports has_more False so the executor stops."""
    s, _ = _db_fakes.make_connector(db, rows=[{"a": 1}])
    out = s.export({**CFG, "table": "t", "limit": 2, "offset": 4})
    assert out["paging_mode"] == "offset" and out["has_more"] is False, out
    assert out["next_cursor"] is None, out


def test_export_rejects_unsafe_cursor_column():
    s, conn = _db_fakes.make_connector(db, rows=[{"id": 1}])
    out = s.export({**CFG, "table": "t",
                    "cursor_column": "id`; DROP TABLE x; --", "limit": 5})
    assert out["success"] is False, out
    assert "cursor" in out.get("error", "").lower(), out
    assert all("DROP TABLE" not in q for q in conn.all_sql()), conn.all_sql()


def test_export_query_mode_flags_truncation():
    s, _ = _db_fakes.make_connector(db, rows=[{"id": i} for i in range(5)])
    out = s.export({**CFG, "query": "SELECT * FROM t", "limit": 5})
    assert out["paging_mode"] == "query" and out.get("truncated") is True, out


# ============================ WRITE: bulk load ==============================

def test_load_default_is_batched_multi_row_insert():
    s, conn = _db_fakes.make_connector(db)
    with env(RSYNC_DATABRICKS_COPY_INTO=None):
        out = s.load({**CFG, "table": "t", "data": _rows(3)})
    assert out["success"] is True, out
    assert out["method"] == "multi_insert", out
    assert out["fell_back"] is False and out["rows_loaded"] == 3, out
    inserts = [e for e in conn.all_execs() if e["sql"].startswith("INSERT INTO")]
    assert len(inserts) == 1, inserts
    sql = inserts[0]["sql"]
    assert sql.count("(?, ?)") == 3, sql               # 3 value tuples, one statement (native ? markers)
    assert "%s" not in sql, sql                        # regression guard: %s is rejected by the driver
    assert "`id`" in sql and "`v`" in sql, sql          # backtick columns


def test_load_chunks_by_page_size():
    s, conn = _db_fakes.make_connector(db)
    out = s.load({**CFG, "table": "t", "data": _rows(5), "page_size": 2})
    assert out["rows_loaded"] == 5, out
    inserts = [e for e in conn.all_execs() if e["sql"].startswith("INSERT INTO")]
    assert len(inserts) == 3, inserts                  # 2 + 2 + 1


def test_bulk_insert_caps_bind_params_for_wide_table():
    """Databricks' Thrift backend rejects >10000 bind markers per query. A wide
    table at the default 1000-row page (1000 * 19 cols = 19000) must be chunked so
    every INSERT stays within _MAX_BIND_PARAMS — otherwise the driver 400s and the
    whole batch is DLQ'd (root-caused on a live MySQL->Databricks 50k run)."""
    s, conn = _db_fakes.make_connector(db)
    ncols = 19
    wide = [{f"c{j}": (i * 100 + j) for j in range(ncols)} for i in range(1000)]
    out = s.load({**CFG, "table": "t", "data": wide})   # default page_size (1000)
    assert out["rows_loaded"] == 1000, out
    inserts = [e for e in conn.all_execs() if e["sql"].startswith("INSERT INTO")]
    cap = db.DatabricksMCPServer._MAX_BIND_PARAMS
    for e in inserts:
        assert len(e["params"]) <= cap, f"INSERT bound {len(e['params'])} > cap {cap}"
    assert len(inserts) >= 2, "1000*19=19000 params must split into >=2 INSERTs"
    assert sum(len(e["params"]) for e in inserts) == 1000 * ncols  # no rows lost


def test_load_auto_creates_table_before_insert():
    """The kafka-mcp-sink SKIPS its ensure_table step for WAREHOUSE destinations
    (main.go `!isWarehouse` gate), so load() must create the table itself (CREATE
    TABLE IF NOT EXISTS ... USING DELTA) BEFORE the INSERT — otherwise a fresh
    destination DLQs every row with TABLE_OR_VIEW_NOT_FOUND (live-root-caused)."""
    s, conn = _db_fakes.make_connector(db)
    out = s.load({**CFG, "table": "t", "data": _rows(3),
                  "column_types": {"id": "integer", "v": "string"}})
    assert out["rows_loaded"] == 3, out
    sqls = conn.all_sql()
    ci = next((i for i, q in enumerate(sqls) if q.startswith("CREATE TABLE IF NOT EXISTS")), -1)
    ii = next((i for i, q in enumerate(sqls) if q.startswith("INSERT INTO")), -1)
    assert ci >= 0 and ii >= 0 and ci < ii, f"CREATE must precede INSERT: {sqls}"
    assert "`main`.`default`.`t`" in sqls[ci] and "USING DELTA" in sqls[ci], sqls[ci]
    assert "`id` BIGINT" in sqls[ci] and "`v` STRING" in sqls[ci], sqls[ci]


def test_load_namespace_override_routes_both_create_and_insert_to_namespace():
    """The pipeline's destination_namespace arrives as a separate ``namespace`` param
    with a BARE table (the sink's contract). BOTH the auto-CREATE and the INSERT must
    target ``<catalog>.<namespace>.<table>`` — NOT the connection's config["schema"].
    Regression for the live silent mis-route: ensure_table honored the namespace but
    load()'s INSERT fell back to config["schema"], so MySQL->Databricks rows piled into
    the connection default schema while the reload DROP hit the requested namespace ->
    duplicate accumulation (280k rows / 30k distinct)."""
    s, conn = _db_fakes.make_connector(db)
    out = s.load({**CFG, "table": "reviews", "namespace": "dbx_sink_final",
                  "data": _rows(3), "column_types": {"id": "integer", "v": "string"}})
    assert out["rows_loaded"] == 3, out
    sqls = conn.all_sql()
    create = next(q for q in sqls if q.startswith("CREATE TABLE IF NOT EXISTS"))
    insert = next(q for q in sqls if q.startswith("INSERT INTO"))
    schema = next(q for q in sqls if q.startswith("CREATE SCHEMA IF NOT EXISTS"))
    assert "`main`.`dbx_sink_final`.`reviews`" in create, create
    assert "`main`.`dbx_sink_final`.`reviews`" in insert, insert
    assert "`main`.`dbx_sink_final`" in schema, schema
    # Must NOT leak into the connection's default schema.
    assert "`default`.`reviews`" not in insert, insert
    assert "`default`.`reviews`" not in create, create


def test_load_copy_into_flag_is_dormant_and_falls_back():
    """copy_into flag on: the path is dormant in v1.0.0, so it must fall back to
    the multi-row INSERT (rows still land) and report fell_back — never a silent
    drop."""
    s, conn = _db_fakes.make_connector(db)
    out = s.load({**CFG, "table": "t", "data": _rows(4), "copy_into": True})
    assert out["success"] is True, out
    assert out["method"] == "multi_insert", out
    assert out["fell_back"] is True, out
    assert out["rows_loaded"] == 4, out


def test_load_rejects_unsafe_column_identifier():
    s, _ = _db_fakes.make_connector(db)
    out = s.load({**CFG, "table": "t",
                  "data": [{"id": 1, "bad`col": 2}]})
    assert out["success"] is False, out
    assert "column" in out.get("error", "").lower(), out


# =================== dialect correctness (the gotchas) ======================

def test_build_copy_sql_uses_storage_not_stdin():
    sql = db.DatabricksMCPServer._build_copy_sql(
        "`main`.`default`.`t`", "s3://bucket/prefix/")
    assert "FROM 's3://bucket/prefix/'" in sql, sql
    assert "STDIN" not in sql, sql
    assert "COPY INTO" in sql and "FILEFORMAT" in sql, sql


def test_build_merge_sql_soft_delete_three_branches():
    """CDC MERGE must be the soft-delete shape (matches warehouse_adapters.py):
    tombstone on delete, upsert + clear-tombstone on live, insert only for live
    not-matched. Sentinels are managed, never copied as business columns; every
    apply stamps _rsync_synced_at. Backtick-quoted, never double-quote. Target/
    source MUST be aliased T/S — a 3-level catalog.schema.table.column path is
    unbindable in Spark (UNRESOLVED_COLUMN)."""
    sql = db.DatabricksMCPServer._build_merge_sql(
        "`main`.`default`.`t`", "`main`.`default`.`_stg`", ["id", "v"], ["id"])
    assert sql.startswith(
        "MERGE INTO `main`.`default`.`t` AS T USING `main`.`default`.`_stg` AS S ON "), sql
    assert "ON CONFLICT" not in sql, sql
    assert '"' not in sql, "Databricks must not use double-quote identifiers"
    # REGRESSION GUARD: no 4-part catalog.schema.table.column reference anywhere.
    assert "`main`.`default`.`t`.`" not in sql, sql
    assert "`main`.`default`.`_stg`.`" not in sql, sql
    # keyed ON via alias
    assert "T.`id` = S.`id`" in sql, sql
    # 1. matched + tombstone -> soft delete
    assert "WHEN MATCHED AND COALESCE(S.`_rsync_deleted`, false) = true THEN UPDATE SET T.`_rsync_deleted` = true" in sql, sql
    # 2. matched + live -> upsert business col + clear tombstone
    assert "WHEN MATCHED AND COALESCE(S.`_rsync_deleted`, false) = false THEN UPDATE SET T.`v` = S.`v`" in sql, sql
    assert "T.`_rsync_deleted` = false" in sql, sql
    # 3. not-matched + live -> insert business + sentinels
    assert "WHEN NOT MATCHED AND COALESCE(S.`_rsync_deleted`, false) = false THEN INSERT (`id`, `v`, `_rsync_deleted`, `_rsync_synced_at`) VALUES (S.`id`, S.`v`, COALESCE(S.`_rsync_deleted`, false), current_timestamp())" in sql, sql
    # sentinels are NOT copied from staging on the business-update path
    assert "T.`_rsync_synced_at` = S.`_rsync_synced_at`" not in sql, sql


def test_build_merge_sql_keys_only_table_has_no_business_update():
    """A key-only row still gets a valid live-branch UPDATE (clear tombstone +
    stamp synced) with no dangling comma."""
    sql = db.DatabricksMCPServer._build_merge_sql(
        "`c`.`s`.`t`", "`c`.`s`.`_stg`", ["id"], ["id"])
    assert "= false THEN UPDATE SET T.`_rsync_deleted` = false, T.`_rsync_synced_at` = current_timestamp()" in sql, sql


def test_merge_stages_ensures_sentinels_creates_like_and_merges():
    s, conn = _db_fakes.make_connector(db)
    out = s.merge({**CFG, "table": "t", "data": _rows(2), "key_fields": ["id"]})
    assert out["success"] is True, out
    sqls = conn.all_sql()
    # CDC-only bootstrap: target created if absent (schema + IF NOT EXISTS table)
    assert any("CREATE SCHEMA IF NOT EXISTS" in q for q in sqls), sqls
    assert any(q.startswith("CREATE TABLE IF NOT EXISTS") and "USING DELTA" in q
               for q in sqls), sqls
    # sentinels provisioned before staging (no live columns visible -> ADD both)
    assert any(q.startswith("SHOW COLUMNS IN") for q in sqls), sqls
    assert any("ADD COLUMNS (`_rsync_deleted` BOOLEAN)" in q for q in sqls), sqls
    assert any("ADD COLUMNS (`_rsync_synced_at` TIMESTAMP)" in q for q in sqls), sqls
    assert any("CREATE TABLE" in q and "LIKE" in q for q in sqls), sqls
    assert any(q.startswith("MERGE INTO") for q in sqls), sqls
    assert any(q.startswith("DROP TABLE IF EXISTS") for q in sqls), sqls


def test_merge_skips_alter_when_sentinels_already_present():
    """Idempotent: if SHOW COLUMNS already reports the sentinels, no ALTER runs
    (Delta ADD COLUMNS has no IF NOT EXISTS, so a redundant add would error)."""
    existing = [{"col_name": "id"}, {"col_name": "v"},
                {"col_name": "_rsync_deleted"}, {"col_name": "_rsync_synced_at"}]
    s, conn = _db_fakes.make_connector(db, rows=existing)
    out = s.merge({**CFG, "table": "t", "data": _rows(2), "key_fields": ["id"]})
    assert out["success"] is True, out
    assert not any("ADD COLUMNS" in q for q in conn.all_sql()), conn.all_sql()


def test_merge_honors_namespace_override_for_target_and_staging():
    """CDC merge must land in the sink-forwarded per-pipeline namespace (bare
    table + `namespace`), not the connection default — the batch-path fix applied
    to the CDC path."""
    s, conn = _db_fakes.make_connector(db)
    out = s.merge({**CFG, "table": "t", "namespace": "cdc_ns",
                   "data": _rows(2), "key_fields": ["id"]})
    assert out["success"] is True, out
    sqls = conn.all_sql()
    assert any("MERGE INTO `main`.`cdc_ns`.`t`" in q for q in sqls), sqls
    assert any("`main`.`cdc_ns`.`_rsync_stg_t`" in q for q in sqls), sqls
    # nothing leaked into the connection default schema
    assert not any("`main`.`default`.`t`" in q for q in sqls), sqls


def test_merge_delete_row_carries_tombstone_into_staging():
    """A sink delete micro-batch (Before image + _rsync_deleted=true) must insert
    the tombstone flag into staging so the MERGE delete branch fires."""
    s, conn = _db_fakes.make_connector(db)
    row = {"id": 7, "v": "gone", "_rsync_deleted": True}
    out = s.merge({**CFG, "table": "t", "data": [row], "key_fields": ["id"]})
    assert out["success"] is True, out
    execs = conn.all_execs()
    ins = [e for e in execs if e["sql"].startswith("INSERT INTO") and "_rsync_stg_t" in e["sql"]]
    assert ins, execs
    assert "`_rsync_deleted`" in ins[0]["sql"], ins[0]["sql"]
    assert True in (ins[0]["params"] or []), ins[0]["params"]


def test_connect_passes_bounded_timeout_and_retry_kwargs():
    """Regression: an unreachable host must not trigger the driver's ~15-min
    retry storm (DatabricksRetryPolicy). ``_connect`` must hand ``dbsql.connect``
    a socket timeout plus bounded retry knobs. Runs the REAL ``_connect`` with a
    fake ``databricks.sql`` module injected, capturing the kwargs it passes."""
    import types

    def _capture(cfg):
        captured = {}
        fake_sql = types.ModuleType("databricks.sql")
        fake_sql.connect = lambda **kw: (captured.update(kw) or _db_fakes.FakeConn())
        fake_pkg = types.ModuleType("databricks")
        fake_pkg.sql = fake_sql
        saved = {k: sys.modules.get(k) for k in ("databricks", "databricks.sql")}
        sys.modules["databricks"], sys.modules["databricks.sql"] = fake_pkg, fake_sql
        try:
            db.DatabricksMCPServer()._connect(cfg)  # real _connect, not swapped
        finally:
            for k, v in saved.items():
                sys.modules.pop(k, None) if v is None else sys.modules.__setitem__(k, v)
        return captured

    base = {"server_hostname": "h", "http_path": "/p", "access_token": "t"}
    c = _capture({**base, "connect_timeout": 12})
    assert c.get("_socket_timeout") == 12, c
    assert c.get("_retry_stop_after_attempts_count") == 5, c
    assert c.get("_retry_stop_after_attempts_duration") == 12.0, c
    d = _capture(base)                       # defaults when connect_timeout unset
    assert d.get("_socket_timeout") == 30, d
    assert d.get("_retry_stop_after_attempts_duration") == 30.0, d


# ================= destination DDL + capabilities (reload / sink) ===========

def test_get_capabilities_accepts_params_arg():
    """The base_connector /mcp dispatch calls get_capabilities(tool_args). A
    no-arg override made the sink's destination DDL probe fail with
    'get_capabilities() takes 1 positional argument but 2 were given'."""
    s = db.DatabricksMCPServer()
    caps = s.get_capabilities({})                        # must not raise
    assert caps["connector_type"] == "databricks", caps
    ops = {o["name"] for o in caps.get("operations", [])}
    assert "drop_table" in ops, caps                     # advertised for reload


def test_drop_table_issues_drop_if_exists_backtick_qualified():
    s, conn = _db_fakes.make_connector(db)
    out = s.drop_table({**CFG, "table": "t"})
    assert out["success"] is True and out["dropped"] is True, out
    sqls = conn.all_sql()
    assert any(q.startswith("DROP TABLE IF EXISTS") and "`main`.`default`.`t`" in q
               for q in sqls), sqls


def test_drop_table_honors_namespace_override_for_bare_table():
    s, conn = _db_fakes.make_connector(db)
    out = s.drop_table({**CFG, "table": "orders", "namespace": "sales"})
    assert out["success"] is True, out
    assert any("`main`.`sales`.`orders`" in q for q in conn.all_sql()), conn.all_sql()


def test_drop_table_requires_table():
    s, _ = _db_fakes.make_connector(db)
    out = s.drop_table({**CFG})
    assert out["success"] is False and "table" in out.get("error", "").lower(), out


def test_get_capabilities_advertises_ensure_table_ddl():
    """The sink gates auto-create on BOTH supports_ddl && auto_create_destination_
    tables (probeDDLSupport) plus an ensure_table operation; missing any one makes
    the sink silently skip table creation and DLQ every write row."""
    s = db.DatabricksMCPServer()
    caps = s.get_capabilities({})
    # the sink's callDestinationTool treats a result without success==true as a
    # failed probe -> ensure_table skipped -> writes DLQ'd. Must be present.
    assert caps.get("success") is True, caps
    assert caps.get("supports_ddl") is True, caps
    assert caps.get("auto_create_destination_tables") is True, caps
    ops = {o["name"] for o in caps.get("operations", [])}
    assert "ensure_table" in ops, caps


def test_ensure_table_creates_schema_then_delta_table_backtick_typed():
    s, conn = _db_fakes.make_connector(db)
    out = s.ensure_table({
        **CFG, "table": "trips",
        "columns": ["id", "amount", "ts", "descr"],
        "column_types": {"id": "integer", "amount": "number",
                         "ts": "timestamp", "descr": "string"},
        "key_fields": ["id"],
    })
    assert out["success"] is True and out["created"] is True, out
    sqls = conn.all_sql()
    # Schema created first — Unity Catalog does NOT auto-create it on table create.
    assert any(q.startswith("CREATE SCHEMA IF NOT EXISTS") and "`main`.`default`" in q
               for q in sqls), sqls
    create = [q for q in sqls if q.startswith("CREATE TABLE IF NOT EXISTS")]
    assert create, sqls
    c = create[0]
    assert "`main`.`default`.`trips`" in c, c
    assert c.rstrip().endswith("USING DELTA"), c            # Delta table
    # canonical -> Delta type mapping, backtick-quoted identifiers
    assert "`id` BIGINT" in c and "`amount` DOUBLE" in c, c
    assert "`ts` TIMESTAMP" in c and "`descr` STRING" in c, c


def test_ensure_table_honors_namespace_override_and_injects_key_columns():
    s, conn = _db_fakes.make_connector(db)
    out = s.ensure_table({
        **CFG, "table": "orders", "namespace": "sales",
        "columns": ["total"], "column_types": {"total": "number"},
        "key_fields": ["order_id"],   # absent from columns -> must be added
    })
    assert out["success"] is True, out
    c = [q for q in conn.all_sql() if q.startswith("CREATE TABLE IF NOT EXISTS")][0]
    assert "`main`.`sales`.`orders`" in c, c
    assert "`order_id` STRING" in c, c        # key column injected (default type)
    assert "`total` DOUBLE" in c, c


def test_ensure_table_passes_ddl_types_through_when_types_are_ddl():
    """CDC path sends already-resolved Databricks types + types_are_ddl=true; they
    must be used verbatim, never re-mapped through the canonical table."""
    s, conn = _db_fakes.make_connector(db)
    out = s.ensure_table({
        **CFG, "table": "t", "columns": ["a"],
        "column_types": {"a": "DECIMAL(10,2)"}, "types_are_ddl": True,
    })
    assert out["success"] is True, out
    c = [q for q in conn.all_sql() if q.startswith("CREATE TABLE")][0]
    assert "`a` DECIMAL(10,2)" in c, c


def test_ensure_table_requires_table():
    s, _ = _db_fakes.make_connector(db)
    out = s.ensure_table({**CFG, "columns": ["a"]})
    assert out["success"] is False and "table" in out.get("error", "").lower(), out


def test_ensure_table_rejects_when_no_safe_columns():
    s, _ = _db_fakes.make_connector(db)
    out = s.ensure_table({**CFG, "table": "t", "columns": ["1bad", "sel*ect"]})
    assert out["success"] is False, out


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
