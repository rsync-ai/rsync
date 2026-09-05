"""Regression tests: safe type-widening on a Postgres destination (ensure_table).

Fixes KI-DRIFT-TYPECHANGE-APPLY-FAILS. An approved drift modify_column is routed by
the healer through ensure_table with the bare table, the per-pipeline namespace, and the
canonical column type (types_are_ddl=false), plus strict_type_change=true. ensure_table
must then:
  - map the canonical type to a concrete PG type (via _pg_type_for), and
  - when the EXISTING column's type differs and the change is a NON-LOSSY widening
    (per _PG_WIDEN), ALTER COLUMN ... TYPE ... USING it in place — mirroring the MySQL
    connector's existing _WIDEN behavior; otherwise (strict) report a clear error instead
    of a silent no-op.

Pure-helper tests bypass __init__ (like test_decimal_scale_preservation.py). The
ensure_table tests drive the real method against a fake cursor/connection so the widening
loop's emitted SQL and the strict-raise path are exercised with no live database.

Run inside the postgresql connector image:
    docker cp <thisfile> rsync-ai-postgresql-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-postgresql-v1-0-0-mcp python /app/test_type_widening.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import PostgresqlMCPServer  # noqa: E402


def _srv():
    return PostgresqlMCPServer.__new__(PostgresqlMCPServer)  # bypass __init__


# --------------------------------------------------------------------------- #
# Pure decision logic: _normalize_pg_ddl_type + _PG_WIDEN                      #
# --------------------------------------------------------------------------- #
def test_normalize_folds_ddl_and_information_schema_to_same_base():
    s = _srv()
    # A desired DDL spelling and the information_schema spelling of the SAME type
    # must fold to one base, so equal types never look like a spurious change.
    assert s._normalize_pg_ddl_type("BIGINT") == s._normalize_pg_ddl_type("bigint") == "bigint"
    assert s._normalize_pg_ddl_type("DOUBLE PRECISION") == "double precision"
    assert s._normalize_pg_ddl_type("TIMESTAMPTZ") == "timestamp with time zone"
    assert s._normalize_pg_ddl_type("TIMESTAMP") == "timestamp without time zone"
    assert s._normalize_pg_ddl_type("VARCHAR(255)") == "character varying"
    assert s._normalize_pg_ddl_type("NUMERIC(38,9)") == "numeric"
    assert s._normalize_pg_ddl_type("decimal") == "numeric"
    assert s._normalize_pg_ddl_type("int4") == "integer"
    assert s._normalize_pg_ddl_type("bool") == "boolean"
    assert s._normalize_pg_ddl_type("") == ""


def test_widen_allows_only_nonlossy_promotions():
    w = _srv()._PG_WIDEN
    # Allowed widenings
    assert "bigint" in w["integer"]
    assert "double precision" in w["integer"]
    assert "text" in w["character varying"]
    assert "timestamp with time zone" in w["date"]
    assert "text" in w["boolean"]
    # NOT allowed (narrowing / lossy / no path)
    assert "integer" not in w.get("bigint", set())          # narrowing
    assert "double precision" not in w.get("numeric", set())  # lossy (exact -> approx)
    assert "double precision" not in w.get("bigint", set())   # loses precision for big ints
    assert w.get("text", set()) == set()                    # text is already widest string


# --------------------------------------------------------------------------- #
# Fake DB seam                                                                 #
# --------------------------------------------------------------------------- #
class _FakeCursor:
    """Records executed SQL; answers the information_schema introspection with a
    seeded {column: data_type} map."""

    def __init__(self, existing):
        self._existing = existing  # {col_name: information_schema data_type}
        self.executed = []         # list of SQL strings, in order
        self._pending_introspection = False

    def execute(self, sql, params=None):
        self.executed.append(sql)
        self._pending_introspection = "information_schema.columns" in sql

    def fetchall(self):
        if self._pending_introspection:
            self._pending_introspection = False
            return [(c, t) for c, t in self._existing.items()]
        return []

    def fetchone(self):
        return None

    def close(self):
        pass


class _FakeConn:
    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def _run_ensure_table(existing, params):
    """Drive the real ensure_table against a fake cursor; return (response, cursor)."""
    s = _srv()
    cur = _FakeCursor(existing)
    conn = _FakeConn()
    s._get_config = lambda p: {}
    s._get_connection = lambda cfg: conn
    s._get_cursor = lambda c, as_dict=False: cur
    # Keep table resolution pure/offline: honor an explicit qualifier, else public.*
    s._normalize_table_for_postgresql = lambda cfg, table, p: table if "." in table else "public." + table
    resp = s.ensure_table(params)
    return resp, cur


def _alter_type_stmts(cur):
    return [x for x in cur.executed if "ALTER COLUMN" in x and " TYPE " in x]


# --------------------------------------------------------------------------- #
# ensure_table widening behavior                                              #
# --------------------------------------------------------------------------- #
def test_safe_widening_is_applied():
    # Existing dest column qty is integer; approved source change -> "integer" maps to
    # BIGINT. integer -> bigint is a safe widening, so ensure_table must ALTER it.
    resp, cur = _run_ensure_table(
        {"qty": "integer"},
        {"table": "products", "columns": ["qty"], "column_types": {"qty": "integer"},
         "synthetic_pk": False, "strict_type_change": True},
    )
    assert resp["success"] is True, resp
    stmts = _alter_type_stmts(cur)
    assert len(stmts) == 1, cur.executed
    assert 'ALTER COLUMN "qty" TYPE BIGINT' in stmts[0]
    assert 'USING "qty"::BIGINT' in stmts[0]


def test_no_alter_when_type_already_matches():
    # Existing qty already bigint; desired "integer" -> BIGINT (canonical widens int to
    # bigint), so current == desired -> no ALTER, idempotent.
    resp, cur = _run_ensure_table(
        {"qty": "bigint"},
        {"table": "products", "columns": ["qty"], "column_types": {"qty": "integer"},
         "synthetic_pk": False, "strict_type_change": True},
    )
    assert resp["success"] is True, resp
    assert _alter_type_stmts(cur) == []


def test_nonwidening_strict_reports_clear_error():
    # Existing status is text; approved change -> "integer" (BIGINT). text -> bigint is a
    # narrowing/incompatible change, NOT in _PG_WIDEN. Under strict, ensure_table must
    # fail with a clear message rather than silently succeeding.
    resp, cur = _run_ensure_table(
        {"status": "text"},
        {"table": "products", "columns": ["status"], "column_types": {"status": "integer"},
         "synthetic_pk": False, "strict_type_change": True},
    )
    assert resp["success"] is False, resp
    assert "not a safe non-lossy widening" in resp["error"], resp
    assert _alter_type_stmts(cur) == []


def test_nonwidening_without_strict_is_best_effort_skip():
    # Same non-widening, but the batch path (no strict flag) must NOT fail — it leaves the
    # column as-is (parity with MySQL's best-effort widen) so writes stay robust.
    resp, cur = _run_ensure_table(
        {"status": "text"},
        {"table": "products", "columns": ["status"], "column_types": {"status": "integer"},
         "synthetic_pk": False},
    )
    assert resp["success"] is True, resp
    assert _alter_type_stmts(cur) == []


def test_synthetic_pk_false_does_not_inject_hash_columns():
    # A column-migration call passes synthetic_pk=false; ensure_table must NOT add the
    # Fivetran-style _rsync_row_hash/_rsync_synced_at columns or a unique index onto the
    # existing table.
    resp, cur = _run_ensure_table(
        {"qty": "integer"},
        {"table": "products", "columns": ["qty"], "column_types": {"qty": "integer"},
         "synthetic_pk": False, "strict_type_change": True},
    )
    assert resp["success"] is True, resp
    joined = "\n".join(cur.executed)
    assert "_rsync_row_hash" not in joined, cur.executed
    assert "CREATE UNIQUE INDEX" not in joined, cur.executed


# --------------------------------------------------------------------------- #
# Widen-matrix corner cases — the whitelist IS the data-loss guard. Assert the #
# non-obvious SAFE promotions are present AND that every lossy / narrowing pair #
# is absent, so a future edit that widens the whitelist can't silently corrupt  #
# data on an approved drift apply.                                              #
# --------------------------------------------------------------------------- #
def test_widen_matrix_includes_nonobvious_safe_promotions():
    w = _srv()._PG_WIDEN
    for src, dst in [
        ("smallint", "real"),               # 16-bit int fits a 24-bit float mantissa
        ("smallint", "double precision"),
        ("smallint", "bigint"),
        ("integer", "double precision"),     # 32-bit int fits a 53-bit double mantissa
        ("integer", "numeric"),
        ("real", "double precision"),
        ("real", "numeric"),
        ("character", "character varying"),
        ("character", "text"),
        ("date", "timestamp without time zone"),
        ("date", "timestamp with time zone"),
        ("timestamp without time zone", "timestamp with time zone"),
        ("boolean", "text"),
        ("uuid", "text"),
    ]:
        assert dst in w.get(src, set()), f"{src} -> {dst} should be a safe widening"


def test_widen_matrix_excludes_lossy_and_narrowing():
    w = _srv()._PG_WIDEN
    for src, dst in [
        ("integer", "real"),                 # 32-bit int > 24-bit float mantissa (lossy)
        ("bigint", "real"),                  # lossy
        ("bigint", "double precision"),      # 64-bit int > 53-bit double mantissa (lossy)
        ("numeric", "double precision"),     # exact -> approximate (lossy)
        ("numeric", "real"),                 # exact -> approximate (lossy)
        ("integer", "smallint"),             # narrowing
        ("bigint", "integer"),               # narrowing
        ("double precision", "real"),        # narrowing
        ("double precision", "integer"),     # truncating
        ("real", "integer"),                 # truncating
        ("character varying", "character"),  # narrowing (may truncate)
        ("text", "character varying"),       # narrowing
        ("text", "integer"),                 # incompatible
        ("timestamp with time zone", "date"),      # truncates the time-of-day
        ("timestamp without time zone", "date"),   # truncates the time-of-day
        ("boolean", "integer"),              # not a whitelisted promotion
    ]:
        assert dst not in w.get(src, set()), f"{src} -> {dst} must NOT be an allowed widening (data loss)"
    # text is already the widest string type — no promotions at all.
    assert w.get("text", set()) == set()


def test_normalize_aliases_cover_common_spellings():
    n = _srv()._normalize_pg_ddl_type
    assert n("int2") == "smallint"
    assert n("int4") == n("int") == "integer"
    assert n("int8") == "bigint"
    assert n("serial") == "integer"
    assert n("bigserial") == "bigint"
    assert n("float4") == "real"
    assert n("float8") == "double precision"
    assert n("bpchar") == "character"
    assert n("char(10)") == "character"
    assert n("varchar(255)") == "character varying"
    assert n("decimal(12,4)") == "numeric"
    assert n("bool") == "boolean"
    assert n("timestamptz") == "timestamp with time zone"
    # DDL vs information_schema spellings of the same type fold together.
    assert n("BIGINT") == n("bigint") == "bigint"
    assert n("DOUBLE PRECISION") == "double precision"


def test_precision_only_change_is_a_noop():
    # The normalize deliberately strips precision/scale, so a pure precision change
    # (numeric(10,2) -> numeric(38,9)) folds to the SAME base ("numeric") → no
    # ALTER. PG numeric columns land unbounded, so this never truncates; it just
    # isn't re-tightened. (A string widening varchar->text is a REAL change and is
    # covered elsewhere — canonical "string" maps to TEXT.)
    resp, cur = _run_ensure_table(
        {"amt": "numeric"},
        {"table": "products", "columns": ["amt"], "column_types": {"amt": "decimal"},
         "synthetic_pk": False, "strict_type_change": True},
    )
    assert resp["success"] is True, resp
    assert _alter_type_stmts(cur) == [], cur.executed


def test_multi_column_all_safe_applies_each():
    # Two approved widenings in one call → one ALTER per column, all applied.
    resp, cur = _run_ensure_table(
        {"qty": "integer", "amt": "smallint"},
        {"table": "products", "columns": ["qty", "amt"],
         "column_types": {"qty": "integer", "amt": "integer"},  # both canonical -> BIGINT
         "synthetic_pk": False, "strict_type_change": True},
    )
    assert resp["success"] is True, resp
    stmts = _alter_type_stmts(cur)
    assert len(stmts) == 2, cur.executed
    assert any('ALTER COLUMN "qty" TYPE BIGINT' in x for x in stmts)
    assert any('ALTER COLUMN "amt" TYPE BIGINT' in x for x in stmts)


def test_multi_column_strict_fails_whole_call_on_an_unsafe_change():
    # A non-widening column under strict must fail the WHOLE call (no silent partial
    # apply), naming the offending column — even alongside a valid widening.
    resp, cur = _run_ensure_table(
        {"status": "text", "qty": "integer"},
        {"table": "products", "columns": ["status", "qty"],
         "column_types": {"status": "integer", "qty": "integer"},
         "synthetic_pk": False, "strict_type_change": True},
    )
    assert resp["success"] is False, resp
    assert "not a safe non-lossy widening" in resp["error"], resp
    assert "status" in resp["error"], resp


if __name__ == "__main__":
    test_normalize_folds_ddl_and_information_schema_to_same_base()
    test_widen_allows_only_nonlossy_promotions()
    test_safe_widening_is_applied()
    test_no_alter_when_type_already_matches()
    test_nonwidening_strict_reports_clear_error()
    test_nonwidening_without_strict_is_best_effort_skip()
    test_synthetic_pk_false_does_not_inject_hash_columns()
    test_widen_matrix_includes_nonobvious_safe_promotions()
    test_widen_matrix_excludes_lossy_and_narrowing()
    test_normalize_aliases_cover_common_spellings()
    test_precision_only_change_is_a_noop()
    test_multi_column_all_safe_applies_each()
    test_multi_column_strict_fails_whole_call_on_an_unsafe_change()
    print("OK: postgresql type-widening tests passed")
