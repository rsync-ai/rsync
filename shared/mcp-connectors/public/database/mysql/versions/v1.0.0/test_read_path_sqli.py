#!/usr/bin/env python3
"""Regression tests for SEC-M-04: read-path SQL-injection hardening.

The export() read path and the MySQL SHOW KEYS PK-discovery path interpolate the
table name directly into SQL. These tests pin the fix:

  * _safe_qualified_table() rejects an injection payload as the relation name
    (returns None) and re-quotes each segment of a legit qualified name.
  * export() returns a fail-closed error dict for an injection table (the
    connection seam is mocked so the guard is exercised offline).
  * _get_primary_key_column() (MySQL branch) never issues a raw
    ``SHOW KEYS FROM <payload>`` — it validates + backtick-quotes first.

Run standalone (no pytest needed):  python3 test_read_path_sqli.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import MysqlMCPServer as _Server  # noqa: E402

# Driver identifier quote char used by export() for this connector.
_QUOTE = '`'

_INJECTIONS = [
    "users; DROP TABLE x",
    "users WHERE 1=1--",
    "users) UNION SELECT password FROM secrets --",
    "public.users; DELETE FROM audit",
    "",
    "   ",
    ".",
]

_LEGIT = {
    "users": f'{_QUOTE}users{_QUOTE}',
    "public.users": f'{_QUOTE}public{_QUOTE}.{_QUOTE}users{_QUOTE}',
    "db.tbl": f'{_QUOTE}db{_QUOTE}.{_QUOTE}tbl{_QUOTE}',
    # case + leading digit preserved (each segment is quoted)
    "MySchema.Orders": f'{_QUOTE}MySchema{_QUOTE}.{_QUOTE}Orders{_QUOTE}',
    "db.123tbl": f'{_QUOTE}db{_QUOTE}.{_QUOTE}123tbl{_QUOTE}',
}


class _FakeCursor:
    def __init__(self, rows=None):
        self.executed = []
        self._rows = rows or []

    def execute(self, sql, params=None):
        self.executed.append(sql)

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


def test_safe_qualified_table_rejects_injection():
    s = _Server()
    for payload in _INJECTIONS:
        assert s._safe_qualified_table(payload, _QUOTE) is None, payload


def test_safe_qualified_table_accepts_and_quotes_legit():
    s = _Server()
    for raw, expected in _LEGIT.items():
        assert s._safe_qualified_table(raw, _QUOTE) == expected, (raw, expected)


def test_export_rejects_injection_table_failclosed():
    s = _Server()
    s._warehouse_adapter = None
    s._get_connection = lambda config: object()
    s._get_cursor = lambda conn, as_dict=True: _FakeCursor()
    res = s.export({"table": "users; DROP TABLE x",
                    "config": {"host": "h", "user": "u",
                               "password": "p", "database": "d"}})
    assert isinstance(res, dict) and res.get("success") is False, res
    assert "Unsafe table identifier" in res.get("error", ""), res


def test_get_primary_key_column_rejects_injection_no_raw_show_keys():
    s = _Server()
    s.driver_pattern = {"module": "mysql.connector"}
    cur = _FakeCursor(rows=[{"Column_name": "id", "Seq_in_index": 1}])
    # Injection payload -> validation fails, no SQL issued, returns None.
    assert s._get_primary_key_column(cur, "users; DROP TABLE x") is None
    assert cur.executed == [], cur.executed
    # Legit qualified name -> backtick-quoted SHOW KEYS, PK resolved.
    cur2 = _FakeCursor(rows=[{"Column_name": "id", "Seq_in_index": 1}])
    assert s._get_primary_key_column(cur2, "db.tbl") == "id"
    assert cur2.executed == ["SHOW KEYS FROM `db`.`tbl` WHERE Key_name = 'PRIMARY'"], cur2.executed


# ---------------------------------------------------------------------------
# SEC (write-path SQLi): destination-table hardening — same class as SEC-M-04
# but on the WRITE path. A tenant-controlled destination table must be rejected
# fail-closed at the entry of upsert_data/import_data/delete_data, before ANY
# raw {table} interpolation (INSERT/MERGE/DELETE / _qualified_quoted_table /
# _split_schema_table). Pin: a stacked-query payload is refused; a legit table
# is not; and _safe_qualified_table rejects malformed qualification.
# ---------------------------------------------------------------------------

_WRITE_INJECTION = 'orders" WHERE 1=0; DROP TABLE secrets; --'
_WRITE_CFG = {"host": "h", "user": "u", "password": "p", "database": "d"}


def _no_conn(*_a, **_k):
    raise RuntimeError("connection attempted -- write-path guard bypassed")


def _mk_write_server():
    """Server pinned to a generic (non-dialect) driver so the public write path
    and its entry guard run, with the connection seam blocked so a guard bypass
    surfaces as a loud failure rather than a real/hung connection."""
    s = _Server()
    s.driver_pattern = {"module": "sqlite3"}
    s._get_connection = _no_conn
    s._acquire_conn = _no_conn
    return s


def test_write_path_rejects_injection_destination_table():
    s = _mk_write_server()
    for _m in (s.upsert_data, s.import_data, s.delete_data):
        res = _m({"table": _WRITE_INJECTION, "data": [{"id": 1}],
                  "config": dict(_WRITE_CFG), "key_fields": ["id"]})
        assert isinstance(res, dict) and res.get("success") is False, (_m, res)
        assert "Unsafe table identifier" in res.get("error", ""), (_m, res)


def test_write_path_allows_legit_destination_table():
    # A legit table clears the guard; control reaches the (blocked) connection,
    # so the resulting error is NOT the guard's rejection.
    s = _mk_write_server()
    for _m in (s.upsert_data, s.import_data, s.delete_data):
        res = _m({"table": "orders", "data": [{"id": 1}],
                  "config": dict(_WRITE_CFG), "key_fields": ["id"]})
        assert "Unsafe table identifier" not in res.get("error", ""), (_m, res)


def test_safe_qualified_table_rejects_empty_and_over_two_segments():
    s = _Server()
    for _bad in ("a.b.c", "a.b.c.d", "schema..table", "db.", ".tbl", ".."):
        assert s._safe_qualified_table(_bad, _QUOTE) is None, _bad
    # legit 1- and 2-segment names are still accepted and re-quoted
    assert s._safe_qualified_table("orders", _QUOTE) == f'{_QUOTE}orders{_QUOTE}'
    assert s._safe_qualified_table("public.orders", _QUOTE) == \
        f'{_QUOTE}public{_QUOTE}.{_QUOTE}orders{_QUOTE}'


if __name__ == "__main__":
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            _fn()
            print(f"ok  {_name}")
    print("ALL READ-PATH SQLi REGRESSION TESTS PASSED")
