#!/usr/bin/env python3
"""Offline unit tests for the Oracle MCP connector.

Oracle rides the ``oracledb`` (python-oracledb, thin mode) DB-API path via the
class ``OracleMCPServer`` (``connector_type == "oracle"``). These tests mock the
one driver seam (``_get_connection`` -> :class:`_ora_fakes.FakeConn`) so they run
offline with no oracledb driver / network / server. Oracle-specific correctness
pinned here — "just copy SQL Server / MySQL" would be wrong:

  * Identifiers are DOUBLE-quoted and case-preserving (``"col"`` / ``"OWNER"."T"``).
  * Bind marker is the numbered positional ``:N`` (oracledb), never ``%s`` / ``?``.
  * There is NO ``LIMIT``: a keyset page is ``... ORDER BY "pk" FETCH FIRST n ROWS
    ONLY``; an offset page is ``OFFSET n ROWS FETCH NEXT m ROWS ONLY``.
  * Upsert is ``MERGE ... USING (SELECT :1 AS "c" ... FROM dual)`` (per-row via
    executemany); INSERT/MERGE auto-create the table on ORA-00942 and retry.
  * Oracle DDL auto-commits, so the CDC offsets table is created BEFORE the data
    DML (never mid-txn) — the CREATE precedes the INSERT/MERGE in the SQL log.
  * Deletes ``TO_CHAR("k") = :n`` to tolerate NUMBER-vs-text key mismatches.

Run standalone (no pytest needed):  python3 test_oracle_connector.py
"""
import datetime as _dt
import os
import sys
import types

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import connector as ora  # noqa: E402  (the module under test)
import _ora_fakes  # noqa: E402

CFG = {"config": {"host": "db.example.com", "port": 1521,
                  "service_name": "ORCLPDB1", "user": "APP", "password": "pw"}}


def _rows(n):
    return [{"id": i, "name": f"r{i}"} for i in range(n)]


# ============================== identity ====================================

def test_connector_identity_and_capabilities():
    s = ora.OracleMCPServer()
    assert s.connector_type == "oracle", s.connector_type
    assert s.connector_category == "relational_db", s.connector_category
    assert s.supports_source is True and s.supports_destination is True
    assert s.supports_cdc is True and s.supports_ddl is True
    assert s.auto_create_destination_tables is True
    assert s._is_oracle_dialect() is True


def test_get_capabilities_advertises_oracle_methods_and_ddl():
    s = ora.OracleMCPServer()
    caps = s.get_capabilities({})  # must not raise on the positional arg
    assert caps["success"] is True, caps
    assert caps["supports_ddl"] is True
    methods = {op["method"] for op in caps["operations"]}
    for m in ("oracle_export", "oracle_import_data", "oracle_upsert_data",
              "oracle_delete_data", "oracle_get_cdc_offsets"):
        assert m in methods, (m, methods)


# ============================== pure helpers ================================

def test_col_type_mapping():
    s = ora.OracleMCPServer()
    assert s._ora_col_type("integer") == "NUMBER(19)"
    assert s._ora_col_type("bigint") == "NUMBER(19)"
    assert s._ora_col_type("boolean") == "NUMBER(1)"
    assert s._ora_col_type("timestamp") == "TIMESTAMP"
    assert s._ora_col_type("date") == "DATE"
    assert s._ora_col_type("time") == "VARCHAR2(64)"
    # canonical `number` (inexact) and `decimal` (exact) both -> Oracle NUMBER —
    # unchanged by the migration to the shared canonical->DDL authority.
    assert s._ora_col_type("number") == "NUMBER"
    assert s._ora_col_type("decimal") == "NUMBER"
    # datetime2/smalldatetime preserved as TIMESTAMP (shared flat-map fold), so a
    # non-canonical SQL Server source leaking the raw token still lands correctly.
    assert s._ora_col_type("datetime2") == "TIMESTAMP"
    # Post-migration a RAW `float`/`double` alias — which only a non-canonical
    # source could still emit — folds via canonicalize_type to canonical `number`
    # -> NUMBER (was BINARY_DOUBLE). Canonical `number` columns are unaffected.
    assert s._ora_col_type("float") == "NUMBER"
    assert s._ora_col_type("string", is_key=True, single_key=True) == "VARCHAR2(400)"
    assert s._ora_col_type("string", is_key=True, single_key=False) == "VARCHAR2(200)"
    assert s._ora_col_type("string") == "CLOB"


def test_split_oracle_schema_table():
    s = ora.OracleMCPServer()
    assert s._split_oracle_schema_table({}, "HR.EMPLOYEES", {}) == ("HR", "EMPLOYEES")
    assert s._split_oracle_schema_table({}, "EMPLOYEES", {}) == (None, "EMPLOYEES")
    # Per-pipeline namespace is intentionally NOT mapped to a foreign owner (v1).
    assert s._split_oracle_schema_table({}, "ORDERS", {"namespace": "PUBLIC"}) == (None, "ORDERS")


def test_quote_and_qname():
    s = ora.OracleMCPServer()
    assert s._ora_quote("Col") == '"Col"'
    assert s._ora_quote('a"b') == '"a""b"'
    assert s._ora_qname("HR", "EMP") == '"HR"."EMP"'
    assert s._ora_qname(None, "EMP") == '"EMP"'


def test_oracle_bind_value():
    assert ora._oracle_bind_value(True, "boolean") == 1
    assert ora._oracle_bind_value(False, "boolean") == 0
    assert ora._oracle_bind_value("true", "boolean") == 1
    assert ora._oracle_bind_value(None, "timestamp") is None
    ts = ora._oracle_bind_value("2026-01-02T03:04:05Z", "timestamp")
    assert isinstance(ts, _dt.datetime) and ts.tzinfo is None
    assert (ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second) == (2026, 1, 2, 3, 4, 5)
    j = ora._oracle_bind_value({"a": 1}, "json")
    assert j == '{"a": 1}'
    assert ora._oracle_bind_value("plain") == "plain"


# ============================== export (source) =============================

def test_export_keyset_uses_fetch_first_and_double_quote():
    s, conn = _ora_fakes.make_connector(ora, columns=["ID", "NAME"], rows=[(1, "a"), (2, "b")])
    res = s.export({**CFG, "table": "orders", "use_keyset_paging": True,
                    "cursor_column": "ID", "limit": 100})
    assert res["success"] is True, res
    sql = " ".join(conn.all_sql())
    assert 'ORDER BY "ID" FETCH FIRST 100 ROWS ONLY' in sql, sql
    assert "LIMIT" not in sql
    assert res.get("next_cursor") == 2
    assert res.get("paging_mode") == "keyset"


def test_export_keyset_with_cursor_binds_numbered_placeholder():
    s, conn = _ora_fakes.make_connector(ora, columns=["ID", "NAME"], rows=[(3, "c")])
    s.export({**CFG, "table": "orders", "use_keyset_paging": True,
              "cursor_column": "ID", "cursor": 2, "limit": 10})
    exec0 = [e for e in conn.all_execs() if "FETCH FIRST" in e["sql"]][0]
    assert '"ID" > :1' in exec0["sql"], exec0["sql"]
    assert exec0["params"] == (2,), exec0["params"]


def test_export_offset_uses_offset_fetch_next():
    s, conn = _ora_fakes.make_connector(ora, columns=["A"], rows=[(1,)])
    s.export({**CFG, "table": "t", "limit": 50, "offset": 20})
    sql = " ".join(conn.all_sql())
    assert "OFFSET 20 ROWS FETCH NEXT 50 ROWS ONLY" in sql, sql


def test_export_normalizes_tuple_rows_to_dicts():
    s, conn = _ora_fakes.make_connector(ora, columns=["ID", "NAME"], rows=[(1, "a"), (2, "b")])
    res = s.export({**CFG, "table": "t", "limit": 10})
    data = res.get("data") or res.get("rows")
    assert data and isinstance(data[0], dict) and data[0]["NAME"] == "a", res


# ============================== import (append) =============================

def test_import_data_double_quoted_numbered_binds():
    s, conn = _ora_fakes.make_connector(ora)
    res = s.import_data({**CFG, "table": "ORDERS",
                         "data": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
                         "column_types": {"id": "integer", "name": "string"}})
    assert res["success"] is True and res["rows_inserted"] == 2, res
    ins = [e for e in conn.all_execs() if e["sql"].startswith("INSERT INTO")][0]
    assert ins["sql"] == 'INSERT INTO "ORDERS" ("id", "name") VALUES (:1, :2)', ins["sql"]
    assert ins["params"] == [(1, "a"), (2, "b")], ins["params"]


def test_import_data_auto_creates_on_ora_00942_then_retries():
    # rows=[] -> the user_tables existence probe returns "not exists" -> CREATE.
    s, conn = _ora_fakes.make_connector(ora, rows=[], fail_executemany_once=True)
    res = s.import_data({**CFG, "table": "ORDERS",
                         "data": [{"id": 1, "name": "a"}],
                         "column_types": {"id": "integer", "name": "string"}})
    assert res["success"] is True and res["rows_inserted"] == 1, res
    sql = " ".join(conn.all_sql())
    assert 'CREATE TABLE "ORDERS"' in sql, sql
    assert conn.rolled_back >= 1


# ============================== upsert (MERGE) ==============================

def test_upsert_data_builds_merge_from_dual():
    s, conn = _ora_fakes.make_connector(ora)
    res = s.upsert_data({**CFG, "table": "ORDERS",
                         "data": [{"id": 1, "name": "a"}],
                         "key_fields": ["id"],
                         "column_types": {"id": "integer", "name": "string"}})
    assert res["success"] is True and res["rows_upserted"] == 1, res
    merge = [e for e in conn.all_execs() if e["sql"].startswith("MERGE INTO")][0]["sql"]
    assert 'MERGE INTO "ORDERS" t USING (SELECT :1 AS "id", :2 AS "name" FROM dual) s' in merge, merge
    assert 'ON (t."id" = s."id")' in merge, merge
    assert 'WHEN MATCHED THEN UPDATE SET t."name" = s."name"' in merge, merge
    assert 'WHEN NOT MATCHED THEN INSERT ("id", "name") VALUES (s."id", s."name")' in merge, merge


# ============================== delete =====================================

def test_delete_data_uses_to_char_and_numbered_binds():
    s, conn = _ora_fakes.make_connector(ora)
    res = s.delete_data({**CFG, "table": "ORDERS",
                         "data": [{"id": 1}], "key_fields": ["id"]})
    assert res["success"] is True and res["rows_deleted"] == 1, res
    dele = [e for e in conn.all_execs() if e["sql"].startswith("DELETE FROM")][0]
    assert dele["sql"] == 'DELETE FROM "ORDERS" WHERE TO_CHAR("id") = :1', dele["sql"]
    assert dele["params"] == ("1",), dele["params"]


# ============================== CDC offsets ================================

def test_cdc_offsets_table_provisioned_before_data_then_merged():
    s, conn = _ora_fakes.make_connector(ora)
    ko = [{"pipeline_id": "p1", "topic": "srv.SCH.ORDERS", "partition": 0, "offset": 42}]
    res = s.import_data({**CFG, "table": "ORDERS",
                         "data": [{"id": 1, "name": "a"}],
                         "column_types": {"id": "integer", "name": "string"},
                         "kafka_offset": ko})
    assert res["success"] is True, res
    sqls = conn.all_sql()
    create_idx = next(i for i, q in enumerate(sqls) if 'CREATE TABLE "_rsync_cdc_offsets"' in q)
    insert_idx = next(i for i, q in enumerate(sqls) if q.startswith("INSERT INTO"))
    merge_idx = next(i for i, q in enumerate(sqls) if 'MERGE INTO "_rsync_cdc_offsets"' in q)
    # DDL (auto-commits) must precede the data INSERT; the offset MERGE follows it.
    assert create_idx < insert_idx < merge_idx, (create_idx, insert_idx, merge_idx)


def test_get_cdc_offsets_reads_back():
    s, conn = _ora_fakes.make_connector(ora, rows=[("srv.SCH.ORDERS", 0, 42)])
    res = s.get_cdc_offsets({**CFG, "pipeline_id": "p1"})
    assert res["success"] is True, res
    assert res["offsets"] == [{"topic": "srv.SCH.ORDERS", "partition": 0, "offset": 42}], res
    sql = " ".join(conn.all_sql())
    assert 'FROM "_rsync_cdc_offsets" WHERE pipeline_id = :1' in sql, sql


# ============================== connection builder =========================

def test_get_oracle_connection_thin_service_name_and_fetch_lobs():
    fake = types.ModuleType("oracledb")
    captured = {}

    class _Defaults:
        fetch_lobs = True
    fake.defaults = _Defaults()

    def makedsn(host, port, service_name=None, sid=None):
        if sid:
            return f"DSN(host={host};port={port};sid={sid})"
        return f"DSN(host={host};port={port};service_name={service_name})"
    fake.makedsn = makedsn

    def connect(**kwargs):
        captured.update(kwargs)
        return object()
    fake.connect = connect

    prev = sys.modules.get("oracledb")
    sys.modules["oracledb"] = fake
    try:
        s = ora.OracleMCPServer()  # real _get_connection (not the FakeConn seam)
        s._get_connection({"host": "h", "port": 1521, "service_name": "SVC",
                           "user": "u", "password": "p"})
    finally:
        if prev is not None:
            sys.modules["oracledb"] = prev
        else:
            sys.modules.pop("oracledb", None)

    assert fake.defaults.fetch_lobs is False
    assert captured.get("user") == "u" and captured.get("password") == "p"
    assert "service_name=SVC" in captured.get("dsn", ""), captured


def test_get_oracle_connection_sid_and_wallet():
    fake = types.ModuleType("oracledb")
    captured = {}

    class _Defaults:
        fetch_lobs = True
    fake.defaults = _Defaults()
    fake.makedsn = lambda host, port, service_name=None, sid=None: (
        f"sid={sid}" if sid else f"svc={service_name}")
    fake.connect = lambda **kw: captured.update(kw) or object()

    prev = sys.modules.get("oracledb")
    sys.modules["oracledb"] = fake
    try:
        s = ora.OracleMCPServer()
        s._get_connection({"host": "h", "port": 1521, "sid": "ORCL",
                           "user": "u", "password": "p",
                           "wallet_location": "/wallet"})
    finally:
        if prev is not None:
            sys.modules["oracledb"] = prev
        else:
            sys.modules.pop("oracledb", None)

    assert "sid=ORCL" in captured.get("dsn", ""), captured
    assert captured.get("wallet_location") == "/wallet"
    assert captured.get("config_dir") == "/wallet"


# ============================== runner =====================================

def _run():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = 0
    failed = []
    for t in tests:
        try:
            t()
            passed += 1
            print(f"  PASS {t.__name__}")
        except Exception as e:  # noqa: BLE001
            failed.append((t.__name__, e))
            print(f"  FAIL {t.__name__}: {e}")
    print(f"\n{passed}/{len(tests)} passed")
    if failed:
        for name, e in failed:
            print(f"    - {name}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    _run()
