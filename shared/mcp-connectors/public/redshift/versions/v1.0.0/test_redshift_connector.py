#!/usr/bin/env python3
"""Offline unit tests for the Amazon Redshift MCP connector.

Redshift speaks the PostgreSQL wire protocol (psycopg2), so this connector rides
the DB-API path — NOT a warehouse adapter. Two Redshift-specific correctness
rules are pinned here because "just copy postgresql" would be wrong:

  * Redshift has NO ``COPY ... FROM STDIN`` — the default bulk path is a batched
    multi-row ``INSERT`` (the execute_values strategy). A flag-gated
    ``COPY FROM s3`` path exists but is dormant in v1.0.0 and must fall back to
    the multi-row INSERT (never a silent drop).
  * Redshift has NO ``INSERT ... ON CONFLICT`` — upsert is stage -> DELETE USING
    -> INSERT SELECT.

Plus the shared read optimization (keyset/offset pagination) and identifier
safety carried over from the BigQuery hardening.

Run standalone (no pytest, no psycopg2):
    python3 test_redshift_connector.py
"""
import contextlib
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import connector as rs  # noqa: E402  (the module under test)
import _rs_fakes  # noqa: E402

CFG = {"config": {"host": "c.redshift.amazonaws.com", "port": 5439,
                  "database": "dev", "user": "u", "password": "p",
                  "schema": "public"}}


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
    s = rs.RedshiftMCPServer()
    assert s.connector_type == "redshift", s.connector_type
    assert s.supports_source is True and s.supports_destination is True
    assert s.supports_cdc is False, "CDC/merge is dormant in v1.0.0"
    caps = s.get_capabilities()
    assert caps["connector_type"] == "redshift", caps
    ls = caps.get("load_strategy") or {}
    assert ls.get("load_method"), ls
    assert ls.get("merge_method") == "delete_insert", ls   # NOT on_conflict


def test_validate_config_requires_host_and_database():
    s = rs.RedshiftMCPServer()
    bad = s.validate_config({"config": {"user": "u"}})
    assert bad["valid"] is False, bad
    good = s.validate_config(CFG)
    assert good["valid"] is True, good


# ============================ READ: pagination ==============================

def test_export_keyset_returns_next_cursor_and_has_more():
    s, conn = _rs_fakes.make_connector(rs, rows=[{"id": 1}, {"id": 2}, {"id": 3}])
    out = s.export({**CFG, "table": "t", "cursor_column": "id", "limit": 3})
    assert out["success"] is True, out
    assert out["paging_mode"] == "keyset", out
    assert out["next_cursor"] == 3 and out["has_more"] is True, out
    sql = conn.all_sql()[-1]
    assert 'ORDER BY "id" ASC' in sql and "LIMIT 3" in sql, sql
    assert '"public"."t"' in sql, sql


def test_export_keyset_continuation_uses_bound_param():
    s, conn = _rs_fakes.make_connector(rs, rows=[{"id": 9}])
    out = s.export({**CFG, "table": "t", "cursor_column": "id",
                    "cursor_value": 8, "limit": 10})
    assert out["success"] is True, out
    e = conn.all_execs()[-1]
    assert 'WHERE "id" > %s' in e["sql"], e["sql"]
    assert e["params"] == [8], e["params"]           # parameterized, not inlined


def test_export_offset_fallback_advances():
    s, conn = _rs_fakes.make_connector(rs, rows=[{"a": 1}, {"a": 2}])
    out = s.export({**CFG, "table": "t", "limit": 2, "offset": 4})
    assert out["paging_mode"] == "offset", out
    assert out["has_more"] is True and out["next_cursor"] == "6", out
    assert "LIMIT 2 OFFSET 4" in conn.all_sql()[-1], conn.all_sql()


def test_export_rejects_unsafe_cursor_column():
    s, conn = _rs_fakes.make_connector(rs, rows=[{"id": 1}])
    out = s.export({**CFG, "table": "t",
                    "cursor_column": 'id"; DROP TABLE x; --', "limit": 5})
    assert out["success"] is False, out
    assert "cursor" in out.get("error", "").lower(), out
    assert all("DROP TABLE" not in q for q in conn.all_sql()), conn.all_sql()


def test_export_query_mode_flags_truncation():
    s, _ = _rs_fakes.make_connector(rs, rows=[{"id": i} for i in range(5)])
    out = s.export({**CFG, "query": "SELECT * FROM t", "limit": 5})
    assert out["paging_mode"] == "query" and out.get("truncated") is True, out


# ============================ WRITE: bulk load ==============================

def test_load_default_is_batched_multi_row_insert():
    s, conn = _rs_fakes.make_connector(rs)
    with env(RSYNC_REDSHIFT_COPY_S3=None):
        out = s.load({**CFG, "table": "t", "data": _rows(3)})
    assert out["success"] is True, out
    assert out["method"] == "multi_insert", out
    assert out["fell_back"] is False and out["rows_loaded"] == 3, out
    inserts = [e for e in conn.all_execs() if e["sql"].startswith("INSERT INTO")]
    assert len(inserts) == 1, inserts
    sql = inserts[0]["sql"]
    assert sql.count("(%s, %s)") == 3, sql            # 3 value tuples, one statement
    assert '"id"' in sql and '"v"' in sql, sql
    assert conn.committed >= 1, "load must commit"


def test_load_chunks_by_page_size():
    s, conn = _rs_fakes.make_connector(rs)
    out = s.load({**CFG, "table": "t", "data": _rows(5), "page_size": 2})
    assert out["rows_loaded"] == 5, out
    inserts = [e for e in conn.all_execs() if e["sql"].startswith("INSERT INTO")]
    assert len(inserts) == 3, inserts                  # 2 + 2 + 1


def test_load_copy_s3_flag_is_dormant_and_falls_back():
    """copy_s3 flag on: the path is dormant in v1.0.0, so it must fall back to the
    multi-row INSERT (rows still land) and report fell_back — never silent-drop."""
    s, conn = _rs_fakes.make_connector(rs)
    out = s.load({**CFG, "table": "t", "data": _rows(4), "copy_s3": True})
    assert out["success"] is True, out
    assert out["method"] == "multi_insert", out
    assert out["fell_back"] is True, out
    assert out["rows_loaded"] == 4, out


def test_load_rejects_unsafe_column_identifier():
    s, _ = _rs_fakes.make_connector(rs)
    out = s.load({**CFG, "table": "t",
                  "data": [{"id": 1, 'bad"col': 2}]})
    assert out["success"] is False, out
    assert "column" in out.get("error", "").lower(), out


# =================== dialect correctness (the gotchas) ======================

def test_build_copy_sql_uses_s3_not_stdin():
    sql = rs.RedshiftMCPServer._build_copy_sql(
        '"public"."t"', "s3://bucket/key.json", iam_role="arn:aws:iam::1:role/r")
    assert "FROM 's3://bucket/key.json'" in sql, sql
    assert "STDIN" not in sql, sql
    assert "IAM_ROLE" in sql and "FORMAT AS JSON" in sql, sql


def test_upsert_sql_uses_delete_insert_not_on_conflict():
    delete_sql, insert_sql = rs.RedshiftMCPServer._build_upsert_sql(
        '"public"."t"', '"public"."_stg"', ["id", "v"], ["id"])
    assert "ON CONFLICT" not in delete_sql and "ON CONFLICT" not in insert_sql, \
        (delete_sql, insert_sql)
    assert delete_sql.startswith("DELETE FROM"), delete_sql
    assert "USING" in delete_sql and '"id"' in delete_sql, delete_sql
    assert insert_sql.startswith("INSERT INTO") and "SELECT" in insert_sql, insert_sql


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
