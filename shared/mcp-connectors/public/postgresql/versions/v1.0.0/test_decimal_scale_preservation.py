"""Regression test: numeric(p,s) scale preserved on a Postgres destination.

Companion to the mysql connector's test of the same name. The PG→MySQL bug
(numeric(10,2) -> decimal(10,0), values rounded) was fixed in three places:
  1. discovery now emits "numeric(p,s)" (not bare "decimal"),
  2. the mysql sink no longer collapses bare decimal to DECIMAL(10,0), and
  3. ``_pg_type_for`` (here) preserves an explicit numeric(p,s) so a PG
     destination keeps the scale too, while a bare "decimal" still maps to the
     lossless NUMERIC(38,9).

Run inside the postgresql connector image:
    docker cp <thisfile> rsync-ai-postgresql-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-postgresql-v1-0-0-mcp python /app/test_decimal_scale_preservation.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import PostgresqlMCPServer  # noqa: E402


def _srv():
    return PostgresqlMCPServer.__new__(PostgresqlMCPServer)  # bypass __init__


def test_parametric_numeric_preserved():
    assert _srv()._pg_type_for("numeric(10,2)") == "numeric(10,2)"


def test_parametric_decimal_preserved():
    assert _srv()._pg_type_for("decimal(12,4)") == "decimal(12,4)"


def test_bare_decimal_is_wide_lossless_not_default():
    # Bare "decimal"/"numeric" -> NUMERIC(38,9), never an unconstrained numeric
    # that some clients round to scale 0.
    assert _srv()._pg_type_for("decimal") == "NUMERIC(38,9)"
    assert _srv()._pg_type_for("numeric") == "NUMERIC(38,9)"


def test_pg_type_for_delegates_to_shared_authority():
    # _pg_type_for now delegates to the shared canonical_to_ddl("postgresql", ...)
    # for everything except the two PG-specific guards. Lock the full mapping so
    # the delegation stays behavior-preserving.
    s = _srv()
    # canonical vocabulary
    assert s._pg_type_for("string") == "TEXT"
    assert s._pg_type_for("integer") == "BIGINT"
    assert s._pg_type_for("number") == "DOUBLE PRECISION"
    assert s._pg_type_for("boolean") == "BOOLEAN"
    assert s._pg_type_for("timestamp") == "TIMESTAMPTZ"
    assert s._pg_type_for("date") == "DATE"
    assert s._pg_type_for("time") == "TIME"
    assert s._pg_type_for("json") == "JSONB"
    assert s._pg_type_for("binary") == "BYTEA"
    # native uuid preserved (canonical vocab collapses uuid->string; guard keeps it)
    assert s._pg_type_for("uuid") == "UUID"
    # raw MySQL-dialect tokens still fold correctly (shared canonicalize_type)
    assert s._pg_type_for("tinyint") == "BIGINT"
    assert s._pg_type_for("varchar(255)") == "TEXT"
    assert s._pg_type_for("jsonb") == "JSONB"
    assert s._pg_type_for("datetime") == "TIMESTAMPTZ"
    assert s._pg_type_for("blob") == "BYTEA"
    # unknown / non-str -> TEXT (never fails CREATE TABLE)
    assert s._pg_type_for("some_unknown_type") == "TEXT"
    assert s._pg_type_for(None) == "TEXT"


if __name__ == "__main__":
    test_parametric_numeric_preserved()
    test_parametric_decimal_preserved()
    test_bare_decimal_is_wide_lossless_not_default()
    test_pg_type_for_delegates_to_shared_authority()
    print("OK: postgresql decimal scale-preservation tests passed")
