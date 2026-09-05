#!/usr/bin/env python3
"""Offline unit tests for the BigQuery warehouse adapter's OPTIMIZED paths.

Two optimizations are pinned here (both run with stdlib only — no GCP, no
google-cloud-bigquery, via the fakes in ``_bq_fakes``):

  READ  — ``export`` must paginate. Keyset mode (``cursor_column``) emits a
          ``WHERE col > @cursor ORDER BY col LIMIT n`` query and returns
          ``next_cursor`` / ``has_more`` so the orchestrator resumes to EOF
          instead of capping at ``LIMIT``. Offset mode is the fallback.

  WRITE — the destination bulk path is flag-gated (``RSYNC_BQ_LOAD_JOB`` /
          ``bulk_load`` param). When enabled it uses a BigQuery load job;
          when a load job fails it FALLS BACK to streaming inserts (never a
          silent drop), reporting the method actually used.

Run standalone (no pytest needed):
    python3 test_bigquery_adapter.py
Or under pytest in the connector image:
    pytest test_bigquery_adapter.py -q
"""
import contextlib
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)  # for `import _bq_fakes`
# Walk up to locate warehouse_adapters.py (lives in shared/mcp-connectors/public).
_d = _HERE
for _ in range(8):
    if os.path.exists(os.path.join(_d, "warehouse_adapters.py")):
        sys.path.insert(0, _d)
        break
    _d = os.path.dirname(_d)

import warehouse_adapters  # noqa: E402
import _bq_fakes  # noqa: E402

CONFIG = {"project_id": "proj", "dataset_id": "ds"}


@contextlib.contextmanager
def env(**kw):
    saved = {k: os.environ.get(k) for k in kw}
    try:
        for k, v in kw.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


# ============================ READ: export pagination ========================

def test_export_keyset_returns_next_cursor_and_has_more():
    """A full page in keyset mode yields next_cursor (last row's key) + has_more."""
    rows = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}]
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters, query_rows=rows)
    out = adapter.export(CONFIG, {"table": "t", "cursor_column": "id", "limit": 3})
    assert out["success"] is True, out
    assert out["paging_mode"] == "keyset", out
    assert out["next_cursor"] == 3, out            # last row's id
    assert out["has_more"] is True, out            # full page => maybe more
    sql = client.queries[-1]
    assert "ORDER BY" in sql and "`id`" in sql, sql
    assert "LIMIT 3" in sql, sql


def test_export_keyset_continuation_filters_on_cursor_value():
    """Passing cursor_value builds a WHERE col > @cursor predicate (parameterized)."""
    rows = [{"id": 5}]
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters, query_rows=rows)
    out = adapter.export(CONFIG, {"table": "t", "cursor_column": "id",
                                  "cursor_value": 4, "limit": 10})
    assert out["success"] is True, out
    sql = client.queries[-1]
    assert "WHERE" in sql and ">" in sql, sql
    cfg = client.query_configs[-1]
    params = getattr(cfg, "query_parameters", [])
    assert params and params[0].value == 4, (sql, params)


def test_export_keyset_terminal_page_stops():
    """A short page (fewer than limit) means EOF: has_more False, next_cursor None."""
    rows = [{"id": 1}, {"id": 2}]
    adapter, _ = _bq_fakes.make_adapter(warehouse_adapters, query_rows=rows)
    out = adapter.export(CONFIG, {"table": "t", "cursor_column": "id", "limit": 100})
    assert out["has_more"] is False, out
    assert out["next_cursor"] is None, out
    assert out["total_records"] == 2, out


def test_export_offset_fallback_without_cursor_column():
    """No cursor_column => deterministic offset mode; next_cursor advances offset."""
    rows = [{"a": 1}, {"a": 2}]
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters, query_rows=rows)
    out = adapter.export(CONFIG, {"table": "t", "limit": 2, "offset": 4})
    assert out["paging_mode"] == "offset", out
    assert out["has_more"] is True, out            # full page
    assert out["next_cursor"] == "6", out          # 4 + 2
    sql = client.queries[-1]
    assert "OFFSET 4" in sql and "LIMIT 2" in sql, sql


# ============================ WRITE: bulk load gating ========================

def _rows(n):
    return [{"id": i, "v": f"r{i}"} for i in range(n)]


def test_load_flag_off_uses_streaming_even_for_large_batch():
    """Default (flag off): even a large batch streams — no load job attempted."""
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters)
    with env(RSYNC_BQ_LOAD_JOB=None):
        out = adapter.load(CONFIG, {"table": "t", "data": _rows(50),
                                    "staging_threshold_rows": 10})
    assert out["success"] is True, out
    assert out["method"] == "streaming", out
    assert client.loaded == [], "no load job should run when flag is off"
    assert len(client.inserted) == 1, client.inserted


def test_load_flag_on_large_batch_uses_load_job():
    """Flag on + batch over threshold => bulk NDJSON load job."""
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters)
    with env(RSYNC_BQ_LOAD_JOB="true"):
        out = adapter.load(CONFIG, {"table": "t", "data": _rows(50),
                                    "staging_threshold_rows": 10})
    assert out["success"] is True, out
    assert out["method"] == "load_job", out
    assert len(client.loaded) == 1, client.loaded
    assert out.get("fell_back") is False, out


def test_load_bulk_param_forces_load_job_even_small_batch():
    """bulk_load=True forces the load job regardless of size/env flag."""
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters)
    with env(RSYNC_BQ_LOAD_JOB=None):
        out = adapter.load(CONFIG, {"table": "t", "data": _rows(2), "bulk_load": True})
    assert out["method"] == "load_job", out
    assert len(client.loaded) == 1, client.loaded


def test_load_job_failure_falls_back_to_streaming():
    """A failing load job must fall back to streaming inserts (never silent drop)."""
    boom = RuntimeError("load job blew up")
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters, load_job_raises=boom)
    with env(RSYNC_BQ_LOAD_JOB="true"):
        out = adapter.load(CONFIG, {"table": "t", "data": _rows(50),
                                    "staging_threshold_rows": 10})
    assert out["success"] is True, out
    assert out["method"] == "streaming", out
    assert out["fell_back"] is True, out
    assert len(client.inserted) == 1, "rows must still land via streaming"
    assert out["rows_loaded"] == 50, out


def test_load_job_uses_readable_binary_file_handle_not_write_only():
    """Regression (found via LIVE BigQuery testing 2026-07-03): the load-job temp
    file must be opened in a READABLE binary mode. google-cloud-bigquery rejects
    a write-only ('wb') handle with 'Cannot upload files opened in text mode', so
    a 'wb' regression makes EVERY bulk load job throw and silently degrade to
    streaming — a dead-on-arrival bulk path (and a hard 403 on free-tier
    BigQuery, since streaming is billing-only). The fake now enforces that same
    contract, so this test fails if the handle regresses to write-only.
    """
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters)
    with env(RSYNC_BQ_LOAD_JOB="true"):
        out = adapter.load(CONFIG, {"table": "t", "data": _rows(3), "bulk_load": True})
    assert out["method"] == "load_job", out            # NOT "streaming"
    assert out.get("fell_back") is False, out
    assert "load_job_error" not in out, out            # no swallowed load-job error
    assert len(client.loaded) == 1, client.loaded
    assert client.inserted == [], "must not fall back to streaming inserts"


# =================== hardening (cold-review findings) =======================

def test_export_rejects_unsafe_cursor_column():
    """#1 identifier injection: a cursor_column that is not a plain identifier
    must be rejected up front (never interpolated into the SQL)."""
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters, query_rows=[{"id": 1}])
    out = adapter.export(CONFIG, {"table": "t",
                                  "cursor_column": "id`; DROP TABLE x; --",
                                  "limit": 10})
    assert out["success"] is False, out
    assert "cursor" in out.get("error", "").lower(), out
    # the injected text must never have reached a query
    assert all("DROP TABLE" not in q for q in client.queries), client.queries


def test_export_date_cursor_uses_date_param_type():
    """#4 a plain datetime.date cursor must bind as DATE (not TIMESTAMP), else a
    live DATE-typed keyset column raises a type-mismatch."""
    import datetime as _dt
    adapter, client = _bq_fakes.make_adapter(warehouse_adapters, query_rows=[{"d": 1}])
    adapter.export(CONFIG, {"table": "t", "cursor_column": "d",
                            "cursor_value": _dt.date(2020, 1, 1), "limit": 10})
    p = client.query_configs[-1].query_parameters[0]
    assert p.type_ == "DATE", p.type_
    # a full datetime still binds as TIMESTAMP
    adapter.export(CONFIG, {"table": "t", "cursor_column": "d",
                            "cursor_value": _dt.datetime(2020, 1, 1, 3, 4, 5), "limit": 10})
    p2 = client.query_configs[-1].query_parameters[0]
    assert p2.type_ == "TIMESTAMP", p2.type_


def test_export_keyset_missing_cursor_in_rows_fails_loud():
    """#3 if a full page's rows lack the cursor column, next_cursor would be
    None while has_more is True — silent truncation/loop. Fail loud instead."""
    rows = [{"other": 1}, {"other": 2}]           # full page, no 'id' key
    adapter, _ = _bq_fakes.make_adapter(warehouse_adapters, query_rows=rows)
    out = adapter.export(CONFIG, {"table": "t", "cursor_column": "id", "limit": 2})
    assert out["success"] is False, out
    assert "cursor" in out.get("error", "").lower(), out


def test_load_job_result_raises_but_committed_no_double_write():
    """#5 if load_job.result() raises AFTER the job committed, must NOT fall back
    to streaming (that would double-write). Report load_job success."""
    boom = RuntimeError("client-side poll timeout")
    adapter, client = _bq_fakes.make_adapter(
        warehouse_adapters, load_job_raises=boom, load_job_committed=True)
    with env(RSYNC_BQ_LOAD_JOB="true"):
        out = adapter.load(CONFIG, {"table": "t", "data": _rows(50),
                                    "staging_threshold_rows": 10})
    assert out["success"] is True, out
    assert out["method"] == "load_job", out
    assert out["fell_back"] is False, out
    assert client.inserted == [], "must NOT double-write via streaming"
    assert out["rows_loaded"] == 50, out


def test_export_query_mode_flags_truncation():
    """#7 a verbatim query truncated at max_results must be flagged, not silent."""
    adapter, _ = _bq_fakes.make_adapter(warehouse_adapters,
                                        query_rows=[{"id": i} for i in range(5)])
    out = adapter.export(CONFIG, {"query": "SELECT * FROM t", "limit": 5})
    assert out["paging_mode"] == "query", out
    assert out.get("truncated") is True, out       # hit the cap => maybe more
    # a short read is not truncated
    adapter2, _ = _bq_fakes.make_adapter(warehouse_adapters,
                                         query_rows=[{"id": 1}])
    out2 = adapter2.export(CONFIG, {"query": "SELECT * FROM t", "limit": 5})
    assert out2.get("truncated") is False, out2


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
