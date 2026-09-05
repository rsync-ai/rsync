#!/usr/bin/env python3
"""Offline unit tests for the SQL Server MCP connector.

SQL Server rides the ``pyodbc`` DB-API path via the class ``MysqlMCPServer``
(reused; ``connector_type == "sqlserver"``). These tests mock the one driver seam
(``_get_connection`` → :class:`_ss_fakes.FakeConn`) so they run offline with no
ODBC driver / network / server. SQL Server-specific correctness pinned here —
"just copy MySQL" would be wrong:

  * Identifiers on the WRITE/DDL path are BRACKET-quoted ``[dbo].[t]`` (``_ss_quote``);
    in ``export`` they are DOUBLE-quoted ``"col"`` (QUOTED_IDENTIFIER ON) and the
    table is interpolated unquoted.
  * Bind marker is the qmark ``?`` (pyodbc), never ``%s``.
  * There is NO ``LIMIT``: a keyset page is ``SELECT TOP (n) ... ORDER BY "pk"``;
    an offset page is ``ORDER BY (SELECT NULL) OFFSET n ROWS FETCH NEXT m ROWS ONLY``.
  * pyodbc rows are sequences → ``export`` normalizes via ``dict(zip(columns, row))``.
  * On a no-bind keyset page ``execute`` is called with NO 2nd arg (``execute(sql,
    None)`` is "0 markers, 1 supplied" in pyodbc).
  * Upsert is a native ``MERGE`` (per-row via executemany); INSERT/MERGE auto-create
    the table on "Invalid object name" and retry (the sink skips ensure_table).

Run standalone (no pytest needed):  python3 test_sqlserver_connector.py
"""
import contextlib
import os
import sys
import types

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import connector as ss  # noqa: E402  (the module under test)
import _ss_fakes  # noqa: E402

CFG = {"config": {"host": "localhost", "port": 1433, "database": "appdb",
                  "user": "sa", "password": "pw"}}


def _rows(n):
    return [{"id": i, "name": f"r{i}"} for i in range(n)]


# ============================== identity ====================================

def test_connector_identity_and_capabilities():
    s = ss.MysqlMCPServer()
    assert s.connector_type == "sqlserver", s.connector_type
    assert s.connector_category == "relational_db", s.connector_category
    assert s.supports_source is True and s.supports_destination is True
    assert s.supports_cdc is True and s.supports_ddl is True
    assert s.auto_create_destination_tables is True


def test_get_capabilities_accepts_params_and_advertises_dest_ddl():
    """The sink calls get_capabilities(tool_args) and gates auto-create on
    supports_ddl + auto_create_destination_tables + an ensure_table op; a no-arg
    override or a missing flag makes it silently DLQ every write row."""
    s = ss.MysqlMCPServer()
    caps = s.get_capabilities({})  # must not raise on the positional arg
    assert caps["success"] is True, caps
    assert caps["connector_type"] == "sqlserver", caps
    assert caps["driver_pattern"] == "pyodbc", caps
    assert caps["supports_destination"] is True and caps["supports_ddl"] is True, caps
    assert caps["auto_create_destination_tables"] is True, caps
    ops = {o["name"] for o in caps.get("operations", [])}
    for op in ("export", "import_data", "upsert_data", "ensure_table", "drop_table"):
        assert op in ops, (op, ops)
    assert caps["capabilities"]["max_batch_size"] == 10000, caps


def test_validate_config_requires_core_fields():
    s = ss.MysqlMCPServer()
    bad = s.validate_config({"config": {"host": "h"}})
    assert bad["valid"] is False, bad
    for missing in ("port", "database", "user", "password"):
        assert any(missing in e for e in bad["errors"]), (missing, bad)
    good = s.validate_config(CFG)
    assert good["valid"] is True, good


# ============================ CONNECT =======================================

def test_test_connection_runs_select_1():
    s, conn = _ss_fakes.make_connector(ss, rows=[(1,)])
    out = s.test_connection(CFG)
    assert out["success"] is True and out["message"] == "Connection successful", out
    assert "SELECT 1" in conn.all_sql(), conn.all_sql()


def test_test_connection_reports_driver_failure():
    s = ss.MysqlMCPServer()
    def _boom(config):
        raise Exception("login timeout expired")
    s._get_connection = _boom  # type: ignore[assignment]
    out = s.test_connection(CFG)
    assert out["success"] is False, out
    assert "login timeout" in out["error"], out


def test_get_connection_string_style_builds_host_aware_connstr():
    """Runs the REAL _get_connection_string_style with a fake ``pyodbc`` module
    injected, capturing the exact connection string. Encryption is host-aware
    (local self-signed → Encrypt=yes/Trust=yes; Azure → Trust=no; explicit
    disable → Encrypt=no); driver 18 is preferred; server is ``host,port`` with a
    comma; qmark-family port defaults 1433."""
    pattern = ss._get_driver_pattern("sqlserver")

    def _capture(config):
        captured = {}
        fake = types.ModuleType("pyodbc")
        fake.drivers = lambda: ["ODBC Driver 17 for SQL Server", "ODBC Driver 18 for SQL Server"]
        fake.connect = lambda cs: captured.setdefault("conn_str", cs) or _ss_fakes.FakeConn()
        saved = sys.modules.get("pyodbc")
        sys.modules["pyodbc"] = fake
        try:
            ss.MysqlMCPServer()._get_connection_string_style(fake, pattern, config)
        finally:
            if saved is None:
                sys.modules.pop("pyodbc", None)
            else:
                sys.modules["pyodbc"] = saved
        return captured["conn_str"]

    local = _capture({"host": "localhost", "database": "appdb", "user": "sa", "password": "pw"})
    assert "DRIVER={ODBC Driver 18 for SQL Server}" in local, local   # newest picked
    assert "SERVER=localhost,1433" in local, local                    # comma + default port
    assert "DATABASE=appdb" in local and "UID=sa" in local and "PWD=pw" in local, local
    assert "Encrypt=yes;TrustServerCertificate=yes" in local, local   # local self-signed
    assert "Connection Timeout=10" in local, local

    azure = _capture({"host": "srv.database.windows.net", "database": "d", "user": "u", "password": "p"})
    assert "Encrypt=yes;TrustServerCertificate=no" in azure, azure    # real CA — verify

    off = _capture({"host": "box", "database": "d", "user": "u", "password": "p", "encrypt": "disable"})
    assert "Encrypt=no;TrustServerCertificate=no" in off, off         # explicit opt-out


# ============================ DISCOVER ======================================

def test_discover_schema_uses_sys_catalogs():
    tables = {
        "orders": {"row_estimate": 42,
                   "columns": [("id", "int", False), ("total", "money", True)]},
        "users": {"row_estimate": 7,
                  "columns": [("id", "int", False), ("email", "nvarchar", True)]},
    }
    s, conn = _ss_fakes.make_connector(ss, router=_ss_fakes.discover_router(tables))
    out = s.discover_schema(CFG)
    assert out["schema_version"] == "2.0", out
    assert out["connector_type"] == "sqlserver", out
    assert out["overall_status"] == "success", out
    assert str(out["database_version"]).startswith("SQL Server"), out
    assert out["total_tables_available"] == 2, out
    assert out["total_tables_discovered"] == 2, out
    names = {t["name"]: t for t in out["tables"]}
    assert set(names) == {"orders", "users"}, names
    orders = names["orders"]
    assert orders["schema"] == "dbo", orders
    assert orders["row_count"] == 42 and orders["is_exact_count"] is False, orders
    cols = {c["name"]: c for c in orders["columns"]}
    # discover_schema emits CANONICAL types since #527 (raw kept in source_type):
    # int -> integer, money -> decimal.
    assert cols["id"]["type"] == "integer" and cols["id"]["source_type"] == "int", cols
    assert cols["id"]["nullable"] is False, cols
    assert cols["total"]["type"] == "decimal" and cols["total"]["source_type"] == "money", cols
    assert cols["total"]["nullable"] is True, cols
    # It must read the sys catalogs (not INFORMATION_SCHEMA) with a schema param.
    sqls = conn.all_sql()
    assert any("sys.tables" in q and "COUNT(*)" in q for q in sqls), sqls
    assert any("sys.columns c" in q for q in sqls), sqls


# ============================ EXPORT: pagination ============================

def test_export_keyset_top_orderby_and_bound_cursor():
    s, conn = _ss_fakes.make_connector(
        ss, columns=["id", "name"], rows=[(1, "a"), (2, "b"), (3, "c")])
    out = s.export({**CFG, "table": "t", "cursor_column": "id",
                    "cursor": 10, "limit": 3})
    assert out["success"] is True, out
    # pyodbc rows are tuples → normalized to dicts.
    assert out["data"] == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"},
                           {"id": 3, "name": "c"}], out["data"]
    assert out["next_cursor"] == 3, out
    assert out["paging_mode"] == "keyset" and out["cursor_column"] == "id", out
    assert out["has_more"] is True, out               # len(3) >= limit(3)
    e = conn.all_execs()[-1]
    sql = e["sql"]
    assert sql.startswith('SELECT TOP (3) * FROM "t" WHERE '), sql   # TOP, quoted table (SEC-M-04)
    assert '"id" > ?' in sql, sql                     # double-quoted pk, qmark bind
    assert 'ORDER BY "id"' in sql, sql
    assert "%s" not in sql and "LIMIT" not in sql, sql
    assert e["params"] == (10,), e["params"]          # parameterized, not inlined


def test_export_keyset_first_page_omits_the_bind_arg():
    """No cursor value → no WHERE and NO 2nd execute arg. pyodbc treats
    execute(sql, None) as '0 markers, 1 supplied' and errors; the connector must
    omit the arg entirely on the no-bind path."""
    s, conn = _ss_fakes.make_connector(ss, columns=["id"], rows=[(1,), (2,)])
    out = s.export({**CFG, "table": "t", "cursor_column": "id", "limit": 5})
    assert out["success"] is True, out
    e = conn.all_execs()[-1]
    assert e["params"] is None, e                      # execute(sql) — no 2nd arg
    assert "WHERE" not in e["sql"], e["sql"]
    assert e["sql"].startswith("SELECT TOP (5) * FROM \"t\" ORDER BY \"id\""), e["sql"]
    assert out["has_more"] is False, out               # len(2) < limit(5)


def test_export_offset_mode_uses_fetch_next_and_no_cursor():
    """Keyless read → OFFSET/FETCH with a no-op ORDER BY (required by SQL Server).
    Offset paging must NOT emit next_cursor/paging_mode (the executor advances
    offset itself)."""
    s, conn = _ss_fakes.make_connector(
        ss, columns=["a"], rows=[(1,), (2,)])
    out = s.export({**CFG, "table": "t", "limit": 2, "offset": 4})
    assert out["success"] is True, out
    assert "next_cursor" not in out and "paging_mode" not in out, out
    assert out["has_more"] is True, out                # len(2) >= limit(2)
    sql = conn.all_execs()[-1]["sql"]
    assert sql == ("SELECT * FROM \"t\" ORDER BY (SELECT NULL) "
                   "OFFSET 4 ROWS FETCH NEXT 2 ROWS ONLY"), sql


def test_export_offset_partial_page_stops():
    s, _ = _ss_fakes.make_connector(ss, columns=["a"], rows=[(1,)])
    out = s.export({**CFG, "table": "t", "limit": 2, "offset": 4})
    assert out["has_more"] is False, out               # short final page
    assert "next_cursor" not in out, out


def test_export_missing_table():
    s, _ = _ss_fakes.make_connector(ss, columns=["a"], rows=[])
    out = s.export({**CFG})
    assert out["success"] is False and "table" in out["error"].lower(), out


def test_export_unsafe_cursor_column_never_reaches_sql():
    """An unsafe cursor_column is rejected by _safe_column_identifier (→ None), so
    keyset is not used and the identifier never lands in SQL (injection guard)."""
    s, conn = _ss_fakes.make_connector(ss, columns=["a"], rows=[(1,)])
    out = s.export({**CFG, "table": "t",
                    "cursor_column": 'id"; DROP TABLE x; --', "limit": 5})
    assert out["success"] is True, out
    sqls = conn.all_sql()
    assert all("DROP TABLE" not in q for q in sqls), sqls
    # fell back to offset paging (no safe pk)
    assert any("OFFSET" in q and "FETCH NEXT" in q for q in sqls), sqls


# ============================ WRITE: INSERT (append) =======================

def test_import_data_batched_bracket_insert():
    s, conn = _ss_fakes.make_connector(ss)
    out = s.import_data({**CFG, "table": "t", "data": _rows(2),
                         "column_types": {"id": "integer", "name": "string"}})
    assert out["success"] is True and out["rows_inserted"] == 2, out
    ins = [e for e in conn.all_execs()
           if e.get("many") and e["sql"].startswith("INSERT INTO")]
    assert len(ins) == 1, ins
    sql = ins[0]["sql"]
    assert sql == "INSERT INTO [dbo].[t] ([id], [name]) VALUES (?, ?)", sql
    assert "%s" not in sql, sql
    assert len(ins[0]["params"]) == 2, ins[0]["params"]   # 2 value tuples in one executemany


def test_import_data_honors_namespace_as_schema():
    s, conn = _ss_fakes.make_connector(ss)
    out = s.import_data({**CFG, "table": "reviews", "namespace": "sales",
                         "data": _rows(1), "column_types": {"id": "integer", "name": "string"}})
    assert out["success"] is True, out
    ins = [e for e in conn.all_execs() if e["sql"].startswith("INSERT INTO")][0]
    assert "[sales].[reviews]" in ins["sql"], ins["sql"]
    assert "[dbo]." not in ins["sql"], ins["sql"]


def test_import_data_rejects_unsafe_table_identifier():
    s, _ = _ss_fakes.make_connector(ss)
    out = s.import_data({**CFG, "table": "t; DROP TABLE u", "data": _rows(1)})
    assert out["success"] is False, out
    assert "unsafe" in out["error"].lower() or "identifier" in out["error"].lower(), out


def test_import_data_empty_is_noop():
    s, _ = _ss_fakes.make_connector(ss)
    out = s.import_data({**CFG, "table": "t", "data": []})
    assert out["success"] is True and out["rows_inserted"] == 0, out


def test_import_data_auto_creates_table_then_retries():
    """The sink skips ensure_table for some destinations, so a fresh table 400s
    the first executemany with 'Invalid object name'. The connector must CREATE
    TABLE (bracket-typed) and retry — never DLQ the batch."""
    s, conn = _ss_fakes.make_connector(ss, fail_executemany_once=True)
    out = s.import_data({**CFG, "table": "t", "data": _rows(2),
                         "column_types": {"id": "integer", "name": "string"}})
    assert out["success"] is True and out["rows_inserted"] == 2, out
    sqls = conn.all_sql()
    assert any(q.startswith("IF OBJECT_ID(") and "CREATE TABLE [dbo].[t]" in q
               for q in sqls), sqls
    # two INSERT executemany attempts: the failed one + the post-create retry.
    inserts = [e for e in conn.all_execs()
               if e.get("many") and e["sql"].startswith("INSERT INTO")]
    assert len(inserts) == 2, inserts


# ============================ WRITE: MERGE upsert ==========================

def test_upsert_builds_native_merge():
    s, conn = _ss_fakes.make_connector(ss)
    out = s.upsert_data({**CFG, "table": "t", "data": _rows(2), "key_fields": ["id"],
                         "column_types": {"id": "integer", "name": "string"}})
    assert out["success"] is True and out["rows_upserted"] == 2, out
    merge = [e for e in conn.all_execs()
             if e.get("many") and e["sql"].startswith("MERGE INTO")]
    assert len(merge) == 1, merge
    sql = merge[0]["sql"]
    assert sql == (
        "MERGE INTO [dbo].[t] AS tgt USING (VALUES (?, ?)) AS src ([id], [name]) "
        "ON tgt.[id] = src.[id] "
        "WHEN MATCHED THEN UPDATE SET tgt.[name] = src.[name] "
        "WHEN NOT MATCHED THEN INSERT ([id], [name]) VALUES (src.[id], src.[name]);"
    ), sql


def test_upsert_keys_only_table_has_no_matched_clause():
    """When every column is a key there is nothing to UPDATE — the MATCHED clause
    must be omitted (a dangling 'SET' would be invalid SQL)."""
    s, conn = _ss_fakes.make_connector(ss)
    out = s.upsert_data({**CFG, "table": "t",
                         "data": [{"id": 1}, {"id": 2}], "key_fields": ["id"],
                         "column_types": {"id": "integer"}})
    assert out["success"] is True, out
    sql = [e for e in conn.all_execs() if e["sql"].startswith("MERGE INTO")][0]["sql"]
    assert "WHEN MATCHED THEN UPDATE SET" not in sql, sql
    assert "ON tgt.[id] = src.[id] WHEN NOT MATCHED THEN INSERT" in sql, sql


def test_upsert_composite_key_and_clause():
    s, conn = _ss_fakes.make_connector(ss)
    out = s.upsert_data({**CFG, "table": "t",
                         "data": [{"a": 1, "b": 2, "v": 9}], "key_fields": ["a", "b"],
                         "column_types": {"a": "integer", "b": "integer", "v": "integer"}})
    assert out["success"] is True, out
    sql = [e for e in conn.all_execs() if e["sql"].startswith("MERGE INTO")][0]["sql"]
    assert "ON tgt.[a] = src.[a] AND tgt.[b] = src.[b]" in sql, sql
    assert "UPDATE SET tgt.[v] = src.[v]" in sql, sql


# ============================ WRITE: DELETE ================================

def test_delete_by_key_casts_nvarchar_and_counts():
    s, conn = _ss_fakes.make_connector(ss)
    out = s.delete_data({**CFG, "table": "t",
                         "data": [{"id": 1}, {"id": 2}], "key_fields": ["id"]})
    assert out["success"] is True and out["rows_deleted"] == 2, out
    dels = [e for e in conn.all_execs() if e["sql"].startswith("DELETE FROM")]
    assert len(dels) == 2, dels                        # per-row execute (not executemany)
    sql = dels[0]["sql"]
    assert sql == ("DELETE FROM [dbo].[t] WHERE "
                   "CAST([id] AS NVARCHAR(4000)) = CAST(? AS NVARCHAR(4000))"), sql
    assert dels[0]["params"] == ("1",), dels[0]["params"]   # key cast to str


def test_delete_missing_key_field_errors():
    s, _ = _ss_fakes.make_connector(ss)
    out = s.delete_data({**CFG, "table": "t",
                         "data": [{"other": 1}], "key_fields": ["id"]})
    assert out["success"] is False and "missing key" in out["error"].lower(), out


# ===================== dialect primitives (pure helpers) ===================

def test_ss_quote_escapes_closing_bracket():
    assert ss.MysqlMCPServer._ss_quote("a") == "[a]"
    assert ss.MysqlMCPServer._ss_quote("a]b") == "[a]]b]"   # ] doubled


def test_ss_col_type_canonical_mapping():
    s = ss.MysqlMCPServer()
    assert s._ss_col_type("integer") == "BIGINT"
    assert s._ss_col_type("bigint") == "BIGINT"
    # exact decimal -> DECIMAL(38,9) since #530 (was the lossy FLOAT); canonical
    # `number` (approximate) legitimately stays FLOAT.
    assert s._ss_col_type("decimal") == "DECIMAL(38,9)" and s._ss_col_type("number") == "FLOAT"
    assert s._ss_col_type("boolean") == "BIT"
    assert s._ss_col_type("timestamp") == "DATETIME2" and s._ss_col_type("date") == "DATETIME2"
    assert s._ss_col_type("string") == "NVARCHAR(MAX)"
    # key columns are bounded to fit the 900-byte index limit
    assert s._ss_col_type("string", is_key=True, single_key=True) == "NVARCHAR(450)"
    assert s._ss_col_type("string", is_key=True, single_key=False) == "NVARCHAR(200)"


def test_split_sqlserver_schema_table_precedence():
    s = ss.MysqlMCPServer()
    assert s._split_sqlserver_schema_table({}, "sales.orders", {}) == ("sales", "orders")
    assert s._split_sqlserver_schema_table({}, "orders", {"namespace": "sales"}) == ("sales", "orders")
    assert s._split_sqlserver_schema_table({}, "orders", {}) == ("dbo", "orders")
    # a 'default' namespace is ignored → dbo
    assert s._split_sqlserver_schema_table({}, "orders", {"namespace": "default"}) == ("dbo", "orders")


def test_safe_identifier_guards():
    s = ss.MysqlMCPServer()
    assert s._is_safe_ident("good_col1") is True
    assert not s._is_safe_ident("a b") and not s._is_safe_ident("a;b")
    assert s._safe_column_identifier("id") == "id"
    assert s._safe_column_identifier("id; DROP") is None


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
