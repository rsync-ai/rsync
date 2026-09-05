"""Regression lock: the PG source's `_canonical_sql_type` delegates to the
shared `canonicalize_type` authority (canonical_types.py).

Companion to the ClickHouse source's delegation. `discover_schema`
(`_discover_postgres_schema_v2` + its polyglot `_discover_mysql_schema_v2`
sibling) emits each column's canonical type via `_canonical_sql_type`, which now
routes through the single shared authority instead of a per-connector table.
Behavior-preserving for the common tokens; strictly fixes the ones the old local
table missed (they defaulted to "string"), verified by a 62-identical / 17
"string"->correct equivalence sweep.

Run inside the postgresql connector image:
    docker cp <thisfile> rsync-ai-postgresql-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-postgresql-v1-0-0-mcp python -m pytest /app/test_source_canonical_types.py -q
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import _canonical_sql_type  # noqa: E402


def test_common_pg_tokens_unchanged():
    assert _canonical_sql_type("character varying") == "string"
    assert _canonical_sql_type("varchar(255)") == "string"
    assert _canonical_sql_type("uuid") == "string"
    assert _canonical_sql_type("bigint") == "integer"
    assert _canonical_sql_type("double precision") == "number"
    assert _canonical_sql_type("numeric") == "decimal"
    assert _canonical_sql_type("boolean") == "boolean"
    assert _canonical_sql_type("timestamp with time zone") == "timestamp"
    assert _canonical_sql_type("timestamp without time zone") == "timestamp"
    assert _canonical_sql_type("date") == "date"
    assert _canonical_sql_type("time without time zone") == "time"
    assert _canonical_sql_type("bytea") == "binary"
    assert _canonical_sql_type("jsonb") == "json"
    assert _canonical_sql_type("ARRAY") == "json"


def test_previously_missed_tokens_now_fold_correctly():
    # These fell to the "string" default in the old local table; the shared
    # authority resolves them (strict fixes, no regressions).
    for t in ("int", "int2", "int4", "int8", "serial", "bigserial", "smallserial"):
        assert _canonical_sql_type(t) == "integer", t
    assert _canonical_sql_type("bit varying") == "binary"
    assert _canonical_sql_type("bool") == "boolean"


def test_unknown_falls_back_to_string():
    assert _canonical_sql_type("totally_unknown_type") == "string"
    assert _canonical_sql_type("") == "string"
    assert _canonical_sql_type(None) == "string"


if __name__ == "__main__":
    test_common_pg_tokens_unchanged()
    test_previously_missed_tokens_now_fold_correctly()
    test_unknown_falls_back_to_string()
    print("OK: postgresql source _canonical_sql_type delegation tests passed")
