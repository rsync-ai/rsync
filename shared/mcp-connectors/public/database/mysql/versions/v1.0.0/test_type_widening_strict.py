"""Regression tests: MySQL ensure_table safe widening + strict type-change reporting.

Companion to the postgresql connector's test_type_widening.py (KI-DRIFT-TYPECHANGE-
APPLY-FAILS). MySQL's ensure_table already applied non-lossy widenings on existing
columns (the _WIDEN whitelist); this locks that behavior AND the new strict_type_change
contract used by the healer's approved-type-change path: a requested change that is NOT a
safe widening must surface a clear error (here: raise, which the base-connector request
handler turns into an error response) instead of a silent no-op. The batch/CDC path leaves
strict unset and stays best-effort (skip) so writes remain robust.

Drives the real ensure_table against a fake cursor/connection — no live MySQL.

Run inside the mysql connector image:
    docker cp <thisfile> rsync-ai-mysql-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-mysql-v1-0-0-mcp python /app/test_type_widening_strict.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import MysqlMCPServer  # noqa: E402


def _srv():
    s = MysqlMCPServer.__new__(MysqlMCPServer)  # bypass __init__
    s.supports_ddl = True
    return s


class _FakeCursor:
    def __init__(self, existing):
        self._existing = existing  # {col_name: information_schema data_type}
        self.executed = []
        self._pending_introspection = False

    def execute(self, sql, params=None):
        self.executed.append(sql)
        self._pending_introspection = "information_schema.columns" in sql

    def fetchall(self):
        if self._pending_introspection:
            self._pending_introspection = False
            return [{"column_name": c, "data_type": t} for c, t in self._existing.items()]
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


def _run_ensure_table(existing, params):
    s = _srv()
    cur = _FakeCursor(existing)
    conn = _FakeConn()
    s._get_config = lambda p: {}
    s._get_connection = lambda cfg: conn
    s._get_cursor = lambda c, as_dict=True: cur
    s._split_mysql_db_table = lambda cfg, table, p=None: ("testdb", table.split(".")[-1])
    resp = s.ensure_table(params)
    return resp, cur


def _modify_stmts(cur):
    return [x for x in cur.executed if "MODIFY COLUMN" in x]


def test_safe_widening_is_applied():
    # Existing dest column qty is int; source change -> canonical "number" maps to DOUBLE.
    # int -> double is a non-lossy widening (_WIDEN), so ensure_table MODIFYs it.
    resp, cur = _run_ensure_table(
        {"qty": "int"},
        {"table": "products", "columns": ["qty"], "column_types": {"qty": "number"},
         "synthetic_pk": False, "strict_type_change": True},
    )
    assert resp["success"] is True, resp
    stmts = _modify_stmts(cur)
    assert len(stmts) == 1, cur.executed
    assert "MODIFY COLUMN `qty` DOUBLE" in stmts[0]


def test_nonwidening_strict_raises_clear_error():
    # text -> double is not a safe widening; strict must surface it (raise → the base
    # connector request handler returns a JSON-RPC error to the caller).
    raised = None
    try:
        _run_ensure_table(
            {"status": "text"},
            {"table": "products", "columns": ["status"], "column_types": {"status": "number"},
             "synthetic_pk": False, "strict_type_change": True},
        )
    except Exception as e:  # noqa: BLE001
        raised = str(e)
    assert raised is not None, "strict non-widening change must raise"
    assert "not a safe non-lossy widening" in raised, raised


def test_nonwidening_without_strict_is_best_effort_skip():
    resp, cur = _run_ensure_table(
        {"status": "text"},
        {"table": "products", "columns": ["status"], "column_types": {"status": "number"},
         "synthetic_pk": False},
    )
    assert resp["success"] is True, resp
    assert _modify_stmts(cur) == []


# --------------------------------------------------------------------------- #
# Widen-matrix corner cases — the whitelist IS the data-loss guard.            #
# --------------------------------------------------------------------------- #
def test_widen_matrix_includes_nonobvious_safe_promotions():
    w = _srv()._WIDEN
    for src, dst in [
        ("tinyint", "float"), ("tinyint", "double"),
        ("smallint", "float"), ("mediumint", "float"),  # all fit a 24-bit float mantissa
        ("mediumint", "int"), ("int", "double"), ("int", "decimal"),
        ("float", "double"),
        ("char", "varchar"), ("varchar", "mediumtext"), ("text", "longtext"),
        ("tinyblob", "blob"), ("blob", "longblob"),
        ("date", "datetime"), ("timestamp", "datetime"),
    ]:
        assert dst in w.get(src, set()), f"{src} -> {dst} should be a safe widening"


def test_widen_matrix_excludes_lossy_and_narrowing():
    w = _srv()._WIDEN
    for src, dst in [
        ("int", "float"),       # 32-bit int > 24-bit float mantissa (lossy)
        ("bigint", "double"),   # 64-bit int > 53-bit double mantissa (lossy)
        ("bigint", "float"),    # lossy
        ("bigint", "int"),      # narrowing
        ("int", "smallint"),    # narrowing
        ("int", "tinyint"),     # narrowing
        ("double", "float"),    # narrowing
        ("text", "varchar"),    # narrowing (may truncate)
        ("longtext", "text"),   # narrowing
        ("datetime", "date"),   # truncates the time-of-day
        ("varchar", "char"),    # narrowing
    ]:
        assert dst not in w.get(src, set()), f"{src} -> {dst} must NOT be an allowed widening (data loss)"
    # decimal is exact — never auto-promoted to an approximate/other type.
    assert w.get("decimal", set()) == set()


def test_normalize_aliases():
    n = _srv()._normalize_mysql_ddl_type
    assert n("integer") == "int"
    assert n("boolean") == n("bool") == "tinyint"
    assert n("double precision") == "double"
    assert n("character varying") == "varchar"
    assert n("character") == "char"
    assert n("int unsigned") == "int"        # unsigned suffix stripped
    assert n("bigint zerofill") == "bigint"  # zerofill suffix stripped
    assert n("varchar(255)") == "varchar"
    assert n("decimal(12,4)") == "decimal"


def test_multi_column_all_safe_applies_each():
    # Two approved widenings in one call → one MODIFY per column.
    resp, cur = _run_ensure_table(
        {"a": "int", "b": "tinyint"},
        {"table": "products", "columns": ["a", "b"],
         "column_types": {"a": "number", "b": "number"},  # both canonical -> DOUBLE
         "synthetic_pk": False, "strict_type_change": True},
    )
    assert resp["success"] is True, resp
    stmts = _modify_stmts(cur)
    assert len(stmts) == 2, cur.executed
    assert any("MODIFY COLUMN `a` DOUBLE" in x for x in stmts)
    assert any("MODIFY COLUMN `b` DOUBLE" in x for x in stmts)


def test_multi_column_strict_raises_on_an_unsafe_change():
    # A non-widening column under strict must raise (fail the whole call), naming the
    # offending column — even alongside a valid widening.
    raised = None
    try:
        _run_ensure_table(
            {"status": "text", "qty": "int"},
            {"table": "products", "columns": ["status", "qty"],
             "column_types": {"status": "number", "qty": "number"},
             "synthetic_pk": False, "strict_type_change": True},
        )
    except Exception as e:  # noqa: BLE001
        raised = str(e)
    assert raised is not None, "strict non-widening change must raise"
    assert "not a safe non-lossy widening" in raised, raised
    assert "status" in raised, raised


if __name__ == "__main__":
    test_safe_widening_is_applied()
    test_nonwidening_strict_raises_clear_error()
    test_nonwidening_without_strict_is_best_effort_skip()
    test_widen_matrix_includes_nonobvious_safe_promotions()
    test_widen_matrix_excludes_lossy_and_narrowing()
    test_normalize_aliases()
    test_multi_column_all_safe_applies_each()
    test_multi_column_strict_raises_on_an_unsafe_change()
    print("OK: mysql type-widening strict tests passed")
