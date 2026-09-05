"""Regression test: MySQL destination must FAIL LOUD on silent truncation/coercion.

Bug (O3, narrow form): when a destination column has drifted to a too-narrow /
type-incompatible shape (e.g. VARCHAR(5) holding a 10-char value, or a stale
TINYINT/DECIMAL(10,0)), a non-strict MySQL server silently truncates/zero-coerces
on write instead of raising. ``upsert_data`` then reported ``len(batch)`` and the
pipeline completed with corrupted data. Two holes:

  1. The connector relied on the *server's* sql_mode. A permissive self-hosted
     MySQL (``sql_mode=''``) truncated silently. Fix: the connection now forces
     ``STRICT_ALL_TABLES`` so a real data-loss coercion RAISES, regardless of the
     server default (an explicit ``config['sql_mode']`` still wins).
  2. The all-key-columns path used ``INSERT IGNORE``, which downgrades data
     truncation to a *warning even under strict mode*. Fix: that path now uses a
     no-op ``ON DUPLICATE KEY UPDATE k=k`` (same dedup semantics) so strict mode
     can raise on truncation there too.

These tests pin both halves without a live DB. The live fail-loud proof is run
separately against the real MySQL.

Run inside the mysql connector image (deps available):
    docker cp <thisfile> rsync-ai-mysql-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-mysql-v1-0-0-mcp python /app/test_upsert_strict_failloud.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import MysqlMCPServer  # noqa: E402


_MYSQL_PATTERN = {
    "connect": "mysql.connector.connect",
    "module": "mysql.connector",
    "params": ["host", "user", "password", "database", "port"],
    "default_port": 3306,
}


class _FakeDriver:
    """Stand-in for the imported driver module; records connect() kwargs."""

    def __init__(self):
        self.captured = None

    def connect(self, **kwargs):
        self.captured = kwargs
        return object()


class _RecordingCursor:
    def __init__(self):
        self.executed = []
        self.rowcount = 1

    def execute(self, query, params=None):
        self.executed.append(query)

    def executemany(self, query, values=None):
        self.executed.append(query)

    def fetchall(self):
        return []

    def fetchone(self):
        return None

    def close(self):
        pass


class _FakeConn:
    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _server_with_recording_cursor():
    srv = MysqlMCPServer()
    rec = _RecordingCursor()
    conn = _FakeConn()
    srv.prepare_import_data = lambda params: {
        "success": True,
        "data": params["data"],
        "config": params.get("config", {}),
        "table": params["table"],
    }
    srv._get_connection = lambda config: conn
    srv._get_cursor = lambda c, as_dict=True: rec
    srv._split_mysql_db_table = lambda config, table, params: (None, table)
    srv._write_cdc_offsets = lambda cursor, params: None
    return srv, rec


# ---- connection forces strict mode -----------------------------------------

def test_connection_forces_strict_sql_mode():
    srv = MysqlMCPServer()
    drv = _FakeDriver()
    config = {"host": "db.example.com", "user": "u", "password": "p", "database": "d"}
    srv._get_param_style_connection(drv, _MYSQL_PATTERN, config)
    assert drv.captured is not None, "connect() was never called"
    assert drv.captured.get("sql_mode") == "STRICT_ALL_TABLES", (
        f"expected forced STRICT_ALL_TABLES, got {drv.captured.get('sql_mode')!r} — "
        "the connector must not depend on the server's sql_mode"
    )


def test_connection_respects_explicit_sql_mode():
    srv = MysqlMCPServer()
    drv = _FakeDriver()
    config = {"host": "db.example.com", "user": "u", "password": "p",
              "database": "d", "sql_mode": "TRADITIONAL"}
    srv._get_param_style_connection(drv, _MYSQL_PATTERN, config)
    assert drv.captured.get("sql_mode") == "TRADITIONAL", (
        "an explicit config sql_mode must win over the forced default"
    )


# ---- all-key upsert no longer uses INSERT IGNORE ---------------------------

def test_upsert_all_key_columns_uses_failloud_dedup():
    srv, rec = _server_with_recording_cursor()
    out = srv.upsert_data({
        "data": [{"a": 1, "b": 2}],
        "config": {},
        "table": "t",
        "key_fields": ["a", "b"],           # every column is a key -> all-key path
        "column_types": {"a": "int", "b": "int"},
    })
    assert out.get("success") is True, out
    joined = " | ".join(rec.executed)
    assert "INSERT IGNORE" not in joined, (
        f"all-key upsert must not use INSERT IGNORE (it hides truncation under "
        f"strict mode); got: {joined}"
    )
    assert "ON DUPLICATE KEY UPDATE" in joined, (
        f"all-key upsert should dedup via a no-op ON DUPLICATE KEY UPDATE; got: {joined}"
    )


def test_upsert_with_update_cols_still_uses_on_duplicate():
    srv, rec = _server_with_recording_cursor()
    out = srv.upsert_data({
        "data": [{"a": 1, "b": 2, "c": 3}],
        "config": {},
        "table": "t",
        "key_fields": ["a"],
        "column_types": {"a": "int", "b": "int", "c": "int"},
    })
    assert out.get("success") is True, out
    joined = " | ".join(rec.executed)
    assert "ON DUPLICATE KEY UPDATE" in joined and "INSERT IGNORE" not in joined, joined


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"ERROR {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    sys.exit(1 if failed else 0)
