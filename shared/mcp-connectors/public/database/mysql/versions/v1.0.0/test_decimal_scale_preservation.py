"""Regression test: PG numeric(p,s) -> MySQL decimal(p,s), scale preserved.

Bug: a Postgres ``numeric(10,2)`` synced batch to a MySQL destination created
the column as ``decimal(10,0)`` (MySQL's default for a bare ``DECIMAL``), so
every fractional value was silently rounded (1.50 -> 2) while row counts still
matched. Root cause: the sink hands ``ensure_table`` a canonical/bare type for
the batch path; bare ``decimal``/``numeric`` are in ``_MYSQL_DDL_BASES`` and the
step-0 passthrough returned the bare keyword, so MySQL applied ``DECIMAL(10,0)``.

These tests pin ``_ensure_table_for_cdc``'s DDL builder:
  * a parametric ``numeric(10,2)`` from the source is preserved with scale, and
  * a bare ``decimal`` falls back to the lossless ``DECIMAL(38,9)``, never the
    silently-truncating bare ``DECIMAL`` / ``decimal(10,0)``.

Run inside the mysql connector image (deps available):
    docker cp <thisfile> rsync-ai-mysql-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-mysql-v1-0-0-mcp python /app/test_decimal_scale_preservation.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import MysqlMCPServer  # noqa: E402


class _RecordingCursor:
    """Minimal DB-API cursor stub that records executed SQL and reports the
    destination table as not-yet-existing (so we observe the CREATE/ADD DDL)."""

    def __init__(self):
        self.sql = []

    def execute(self, query, params=None):
        self.sql.append(query)

    def fetchall(self):
        return []  # no existing columns -> ensure_table emits CREATE / ADD COLUMN

    def fetchone(self):
        return None

    def close(self):
        pass


def _ddl_for(col_type: str) -> str:
    """Return the concatenated DDL ensure_table emits for a single ``amount``
    column of the given source type."""
    srv = MysqlMCPServer.__new__(MysqlMCPServer)  # bypass __init__ (no DB/env)
    cur = _RecordingCursor()
    srv._ensure_table_for_cdc(
        cur,
        {"database": "testdb"},
        "t290_dec_fix",
        ["amount"],
        [],
        {"amount": col_type},
        {},
    )
    return " ".join(cur.sql).lower()


def test_parametric_numeric_preserves_scale():
    ddl = _ddl_for("numeric(10,2)")
    assert "(10,2)" in ddl, f"scale dropped: {ddl}"
    assert "decimal(10,0)" not in ddl, f"silent (10,0) round: {ddl}"


def test_parametric_decimal_preserves_scale():
    ddl = _ddl_for("decimal(12,4)")
    assert "(12,4)" in ddl, f"scale dropped: {ddl}"


def test_bare_decimal_falls_back_to_wide_lossless():
    # Bare canonical "decimal" must NOT become MySQL's default DECIMAL(10,0).
    ddl = _ddl_for("decimal")
    assert "decimal(38,9)" in ddl, f"bare decimal not widened: {ddl}"
    assert "decimal(10,0)" not in ddl, f"silent (10,0) round: {ddl}"


def test_bare_numeric_falls_back_to_wide_lossless():
    ddl = _ddl_for("numeric")
    assert "decimal(38,9)" in ddl, f"bare numeric not widened: {ddl}"


if __name__ == "__main__":
    test_parametric_numeric_preserves_scale()
    test_parametric_decimal_preserves_scale()
    test_bare_decimal_falls_back_to_wide_lossless()
    test_bare_numeric_falls_back_to_wide_lossless()
    print("OK: mysql decimal scale-preservation tests passed")
