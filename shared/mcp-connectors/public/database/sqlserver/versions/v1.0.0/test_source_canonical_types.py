"""Regression lock: the SQL Server source's `_canonical_sqlserver_type` delegates
to the shared `canonicalize_type` authority (canonical_types.py).

`discover_schema` (`_discover_sqlserver_schema_v2`) emits each column's canonical
type via `_canonical_sqlserver_type`, which now routes through the single shared
authority (`_SQLSERVER_CANONICAL`, reached with dialect="sqlserver") instead of a
per-connector table. Behavior-preserving for every real `sys.types` name — the
local map and the shared dialect map are token-identical, verified by an 85-identical
/ 0-regression equivalence sweep (the only divergences are non-SS tokens the local
map dumped to "string", which the shared flat path now resolves — strict fixes that
can't occur in real SQL Server output).

Companion to the PostgreSQL/MySQL/ClickHouse/Oracle source delegations — SQL Server
was the last source still on a local ingress map (closes Phase 1 for real).

Run inside the sqlserver connector image:
    docker cp <thisfile> rsync-ai-sqlserver-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-sqlserver-v1-0-0-mcp python -m pytest /app/test_source_canonical_types.py -q
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import _canonical_sqlserver_type  # noqa: E402


def test_common_sqlserver_tokens_unchanged():
    # exact/approximate numerics
    assert _canonical_sqlserver_type("tinyint") == "integer"
    assert _canonical_sqlserver_type("int") == "integer"
    assert _canonical_sqlserver_type("bigint") == "integer"
    assert _canonical_sqlserver_type("float") == "number"
    assert _canonical_sqlserver_type("real") == "number"
    assert _canonical_sqlserver_type("decimal(12,4)") == "decimal"
    assert _canonical_sqlserver_type("numeric") == "decimal"
    assert _canonical_sqlserver_type("money") == "decimal"
    assert _canonical_sqlserver_type("smallmoney") == "decimal"
    # bit is a 0/1 boolean in SQL Server
    assert _canonical_sqlserver_type("bit") == "boolean"
    # character / unicode / xml / guid
    assert _canonical_sqlserver_type("varchar(255)") == "string"
    assert _canonical_sqlserver_type("nvarchar(max)") == "string"
    assert _canonical_sqlserver_type("ntext") == "string"
    assert _canonical_sqlserver_type("xml") == "string"
    assert _canonical_sqlserver_type("uniqueidentifier") == "string"
    assert _canonical_sqlserver_type("sysname") == "string"
    # date / time
    assert _canonical_sqlserver_type("date") == "date"
    assert _canonical_sqlserver_type("time") == "time"
    assert _canonical_sqlserver_type("datetime2") == "timestamp"
    assert _canonical_sqlserver_type("datetimeoffset") == "timestamp"
    assert _canonical_sqlserver_type("smalldatetime") == "timestamp"


def test_rowversion_timestamp_quirk_preserved():
    # In SQL Server `timestamp`/`rowversion` is an 8-byte binary row-version,
    # NOT a datetime — the shared authority preserves this critical quirk.
    assert _canonical_sqlserver_type("timestamp") == "binary"
    assert _canonical_sqlserver_type("rowversion") == "binary"
    assert _canonical_sqlserver_type("binary") == "binary"
    assert _canonical_sqlserver_type("varbinary(16)") == "binary"
    assert _canonical_sqlserver_type("image") == "binary"


def test_case_insensitive_and_precision_stripped():
    assert _canonical_sqlserver_type("DATETIME2") == "timestamp"
    assert _canonical_sqlserver_type("  Decimal(38,9) ") == "decimal"
    assert _canonical_sqlserver_type("UNIQUEIDENTIFIER") == "string"


def test_unknown_and_non_string_fall_back_to_string():
    assert _canonical_sqlserver_type("totally_unknown_type") == "string"
    assert _canonical_sqlserver_type("") == "string"
    assert _canonical_sqlserver_type("   ") == "string"
    assert _canonical_sqlserver_type(None) == "string"
    assert _canonical_sqlserver_type(123) == "string"


if __name__ == "__main__":
    test_common_sqlserver_tokens_unchanged()
    test_rowversion_timestamp_quirk_preserved()
    test_case_insensitive_and_precision_stripped()
    test_unknown_and_non_string_fall_back_to_string()
    print("OK: sqlserver source _canonical_sqlserver_type delegation tests passed")
