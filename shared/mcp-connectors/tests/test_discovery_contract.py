"""Discovery type-contract conformance tests.

Phase 0 of the generic multi-schema + type-conversion consolidation
(see the plan tracked in BACKLOG.md). Every SOURCE connector's
``discover_schema()`` must emit column types drawn from the shared canonical
vocabulary (``CANONICAL_TYPES``), so any SINK can translate canonical -> its
own dialect with a single mapper (N+M, not N*M).

These tests pin that contract at its single source of truth --
``schemas/response_schemas.py`` -- and, critically, prove WHY canonicalization
must be scoped by SOURCE dialect: a flat token->canonical map mis-maps tokens
that collide across dialects (``bit``, ``NUMBER``, ``timestamp``). That flat
lookup is the class of bug behind the "all-String columns + synthetic
``_rsync_row_hash`` PK" destination failures that recur per connector
(PostgreSQL #525, Oracle #526, SQL Server #527).

Offline + dependency-light: loads only ``response_schemas.py`` (pydantic), no
DB drivers, no live connections. Loaded by file path via importlib so it never
depends on PYTHONPATH ordering.
"""
from __future__ import annotations

import importlib.util
import os
import sys

import pytest

# Load the shippable authority the SAME way every connector does at runtime:
# `import canonical_types` after putting the shared public/ dir on sys.path
# (in-image it lives at /app/canonical_types.py). Using the module cache means
# the re-export identity check below is reliable.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "public")))
import canonical_types  # noqa: E402

canonicalize_type = canonical_types.canonicalize_type
CANONICAL_TYPES = canonical_types.CANONICAL_TYPES
canonical_to_ddl = canonical_types.canonical_to_ddl


def test_response_schemas_reexports_the_same_authority():
    # The pydantic validator (schemas/response_schemas.py) must IMPORT the
    # canonicalizer from canonical_types, not duplicate it — otherwise the
    # CI validator and the in-image connector logic could drift. Assert object
    # identity so a copy-paste regression fails loudly.
    rs_path = os.path.join(os.path.dirname(__file__), "..", "schemas", "response_schemas.py")
    spec = importlib.util.spec_from_file_location("response_schemas_reexport", rs_path)
    rs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(rs)
    assert rs.canonicalize_type is canonicalize_type
    assert rs.CANONICAL_TYPES is CANONICAL_TYPES


# ---------------------------------------------------------------------------
# 1. the canonical vocabulary is stable
# ---------------------------------------------------------------------------

def test_canonical_vocabulary_is_the_expected_set():
    # Guards against silent additions/removals; every sink's canonical->dialect
    # mapper is written against exactly this set.
    assert CANONICAL_TYPES == {
        "string", "integer", "number", "decimal", "boolean",
        "timestamp", "date", "time", "json", "binary",
    }


# ---------------------------------------------------------------------------
# 2. dialect-aware canonicalization is correct (the executable contract)
# ---------------------------------------------------------------------------
#
# (dialect, raw_source_token, expected_canonical). These are real
# data-dictionary type names as each source connector's discover_schema sees
# them (Oracle all_tab_columns.data_type, SQL Server sys.types.name,
# ClickHouse system.columns.type). Every row here is a promise the pipeline
# keeps end to end.
_DIALECT_CASES = [
    # ---- Oracle ----
    ("oracle", "VARCHAR2", "string"),
    ("oracle", "NVARCHAR2", "string"),
    ("oracle", "CHAR", "string"),
    ("oracle", "CLOB", "string"),
    ("oracle", "NUMBER", "decimal"),                       # exact, NOT float
    ("oracle", "FLOAT", "number"),
    ("oracle", "BINARY_DOUBLE", "number"),
    ("oracle", "DATE", "timestamp"),                       # Oracle DATE has a time component
    ("oracle", "TIMESTAMP(6)", "timestamp"),
    ("oracle", "TIMESTAMP(6) WITH TIME ZONE", "timestamp"),
    ("oracle", "RAW", "binary"),
    ("oracle", "BLOB", "binary"),
    # ---- SQL Server ----
    ("sqlserver", "nvarchar", "string"),
    ("sqlserver", "varchar", "string"),
    ("sqlserver", "uniqueidentifier", "string"),
    ("sqlserver", "bit", "boolean"),                       # NOT binary
    ("sqlserver", "int", "integer"),
    ("sqlserver", "bigint", "integer"),
    ("sqlserver", "decimal", "decimal"),                   # NOT float
    ("sqlserver", "money", "decimal"),
    ("sqlserver", "float", "number"),
    ("sqlserver", "date", "date"),
    ("sqlserver", "datetime2", "timestamp"),               # NOT string
    ("sqlserver", "datetimeoffset", "timestamp"),
    ("sqlserver", "varbinary", "binary"),
    ("sqlserver", "timestamp", "binary"),                  # SS `timestamp` == rowversion
    # ---- ClickHouse ----
    ("clickhouse", "String", "string"),
    ("clickhouse", "LowCardinality(String)", "string"),
    ("clickhouse", "UUID", "string"),
    ("clickhouse", "Int64", "integer"),
    ("clickhouse", "UInt32", "integer"),
    ("clickhouse", "Nullable(Int64)", "integer"),
    ("clickhouse", "UInt64", "decimal"),                   # exceeds int64 -> decimal, NOT integer
    ("clickhouse", "Nullable(UInt64)", "decimal"),
    ("clickhouse", "Int128", "string"),                    # exceeds NUMERIC(38) -> string
    ("clickhouse", "UInt256", "string"),
    ("clickhouse", "Float64", "number"),
    ("clickhouse", "Decimal(18, 2)", "decimal"),
    ("clickhouse", "Bool", "boolean"),
    ("clickhouse", "Date", "date"),
    ("clickhouse", "DateTime64(3)", "timestamp"),
    ("clickhouse", "Array(String)", "json"),
    # ---- Oracle multi-word token: LONG RAW is binary, not the character LONG ----
    ("oracle", "LONG RAW", "binary"),
    ("oracle", "LONG", "string"),
    # ---- PostgreSQL / MySQL: already correct via the flat map; verify the
    #      dialect-scoped path never regresses them ----
    ("postgresql", "timestamp with time zone", "timestamp"),
    ("postgresql", "jsonb", "json"),
    ("mysql", "tinyint", "integer"),
    ("mysql", "double", "number"),
    # MySQL varbinary must fold to binary (not string) once discovery routes
    # through canonicalize_type — the flat map gained "varbinary": "binary" so
    # the mysql source's binary columns are not typed as text at the sink.
    ("mysql", "varbinary", "binary"),
    ("mysql", "blob", "binary"),
]


@pytest.mark.parametrize("dialect,token,expected", _DIALECT_CASES)
def test_dialect_aware_canonicalization_is_correct(dialect, token, expected):
    result = canonicalize_type(token, dialect=dialect)
    assert result == expected, f"{dialect}:{token} -> {result!r}, expected {expected!r}"
    # every result must be a member of the canonical vocabulary
    assert result in CANONICAL_TYPES


def test_oracle_number_precision_scale_distinguishes_integer_from_decimal():
    # NUMBER(p,0) that fits int64 -> integer; NUMBER(p,s>0) or wide-precision
    # NUMBER(p,0) -> decimal (never overflow int64 at the destination).
    assert canonicalize_type("NUMBER", dialect="oracle", scale=0, precision=9) == "integer"
    assert canonicalize_type("NUMBER", dialect="oracle", scale=2, precision=12) == "decimal"
    assert canonicalize_type("NUMBER", dialect="oracle", scale=0, precision=38) == "decimal"
    assert canonicalize_type("NUMBER", dialect="oracle") == "decimal"


def test_clickhouse_wide_integers_never_map_to_int64_integer():
    # int64-safe widths stay "integer"
    for t in ("Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32"):
        assert canonicalize_type(t, dialect="clickhouse") == "integer", t
    # UInt64 (up to ~1.8e19) overflows int64 -> decimal (fits NUMERIC(38,9))
    assert canonicalize_type("UInt64", dialect="clickhouse") == "decimal"
    assert canonicalize_type("Nullable(UInt64)", dialect="clickhouse") == "decimal"
    # 128/256-bit ints exceed even NUMERIC(38) -> string (lossless), never integer
    for t in ("Int128", "UInt128", "Int256", "UInt256"):
        assert canonicalize_type(t, dialect="clickhouse") == "string", t


# ---------------------------------------------------------------------------
# 3. backward compatibility: the flat (no-dialect) path is unchanged
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("token,expected", [
    ("String", "string"), ("Int", "integer"), ("Float", "number"),
    ("Boolean", "boolean"), ("DateTime", "timestamp"), ("JSON", "json"),
    ("numeric", "decimal"), ("timestamp with time zone", "timestamp"),
    ("jsonb", "json"), ("bytea", "binary"), ("", "string"),
    (None, "string"), (12345, "string"),
])
def test_flat_path_backward_compatible(token, expected):
    # No dialect kwarg -> byte-identical to the historical behavior.
    assert canonicalize_type(token) == expected


# ---------------------------------------------------------------------------
# 4. the gap this closes: the flat path mis-maps dialect-colliding tokens
# ---------------------------------------------------------------------------
#
# Documents *why* every source connector must pass its dialect. Each token here
# is canonicalized WRONG by the flat path but CORRECT once the dialect is known.
# When Phase 1 routes every source's discover_schema through the dialect-aware
# path, the destination stops receiving corrupted types.
@pytest.mark.parametrize("dialect,token,flat_wrong,dialect_right", [
    ("oracle", "NUMBER", "number", "decimal"),            # inexact float vs exact fixed-point
    ("oracle", "BINARY_DOUBLE", "string", "number"),
    ("sqlserver", "bit", "binary", "boolean"),            # SS boolean vs PG bit-string (binary)
    ("clickhouse", "Int64", "string", "integer"),
    ("clickhouse", "Float64", "string", "number"),
    ("clickhouse", "DateTime64(3)", "string", "timestamp"),
])
def test_flat_path_gap_is_fixed_by_dialect_scoping(dialect, token, flat_wrong, dialect_right):
    # today's dialect-agnostic behavior (the bug)
    assert canonicalize_type(token) == flat_wrong
    # fixed the moment the source dialect is supplied
    assert canonicalize_type(token, dialect=dialect) == dialect_right


def test_sqlserver_datetime_aliases_fold_in_the_flat_map():
    # datetime2/smalldatetime are UNAMBIGUOUS (a timestamp in every dialect), so
    # unlike `bit`/`NUMBER` they are safe to fold in the flat map too. A sink's
    # canonical_to_ddl() therefore maps a leaked raw datetime2 (from a source not
    # yet emitting canonical) to a real timestamp column, not a text fallback —
    # and the Oracle sink migration stays behavior-preserving (its hand-written
    # mapper already mapped datetime2/smalldatetime -> TIMESTAMP).
    for tok in ("datetime2", "smalldatetime", "DATETIME2", "SmallDateTime"):
        assert canonicalize_type(tok) == "timestamp", tok
    assert canonical_to_ddl("oracle", "datetime2") == "TIMESTAMP"
    assert canonical_to_ddl("oracle", "smalldatetime") == "TIMESTAMP"
    assert canonical_to_ddl("sqlserver", "datetime2") == "DATETIME2"
    assert canonical_to_ddl("postgresql", "smalldatetime") == "TIMESTAMPTZ"


# ---------------------------------------------------------------------------
# 5. canonical -> dialect DDL (egress / sink authority)
# ---------------------------------------------------------------------------
#
# Pins the NON-KEY batch-path DDL each sink emits, so migrating a sink onto the
# shared canonical_to_ddl() is provably behavior-preserving. Values mirror the
# current per-connector mappers EXACTLY, except SQL Server decimal (was the
# lossy FLOAT, now DECIMAL). Every future sink migration is gated by this table.
_DDL_MATRIX = {
    "postgresql": {"string": "TEXT", "integer": "BIGINT", "number": "DOUBLE PRECISION",
                   "decimal": "NUMERIC(38,9)", "boolean": "BOOLEAN", "timestamp": "TIMESTAMPTZ",
                   "date": "DATE", "time": "TIME", "json": "JSONB", "binary": "BYTEA"},
    "mysql": {"string": "TEXT", "integer": "BIGINT", "number": "DOUBLE",
              "decimal": "DECIMAL(38,9)", "boolean": "TINYINT(1)", "timestamp": "DATETIME(6)",
              "date": "DATE", "time": "TIME", "json": "JSON", "binary": "BLOB"},
    "clickhouse": {"string": "String", "integer": "Int64", "number": "Float64",
                   "decimal": "Decimal(38, 9)", "boolean": "Bool", "timestamp": "DateTime64(3)",
                   "date": "Date32", "time": "String", "json": "String", "binary": "String"},
    "snowflake": {"string": "VARCHAR", "integer": "NUMBER(38,0)", "number": "FLOAT",
                  "decimal": "NUMBER(38,9)", "boolean": "BOOLEAN", "timestamp": "TIMESTAMP_NTZ",
                  "date": "DATE", "time": "TIME", "json": "VARCHAR", "binary": "BINARY"},
    "databricks": {"string": "STRING", "integer": "BIGINT", "number": "DOUBLE",
                   "decimal": "DECIMAL(38,9)", "boolean": "BOOLEAN", "timestamp": "TIMESTAMP",
                   "date": "DATE", "time": "STRING", "json": "STRING", "binary": "BINARY"},
    "sqlserver": {"string": "NVARCHAR(MAX)", "integer": "BIGINT", "number": "FLOAT",
                  "decimal": "DECIMAL(38,9)", "boolean": "BIT", "timestamp": "DATETIME2",
                  "date": "DATETIME2", "time": "DATETIME2", "json": "NVARCHAR(MAX)", "binary": "NVARCHAR(MAX)"},
    "oracle": {"string": "CLOB", "integer": "NUMBER(19)", "number": "NUMBER",
               "decimal": "NUMBER", "boolean": "NUMBER(1)", "timestamp": "TIMESTAMP",
               "date": "DATE", "time": "VARCHAR2(64)", "json": "CLOB", "binary": "CLOB"},
}


@pytest.mark.parametrize("dialect", sorted(_DDL_MATRIX))
def test_canonical_to_ddl_matches_each_sink(dialect):
    for canon, expected in _DDL_MATRIX[dialect].items():
        got = canonical_to_ddl(dialect, canon)
        assert got == expected, f"{dialect}:{canon} -> {got!r}, expected {expected!r}"


def test_sqlserver_decimal_is_exact_not_float():
    # The one intended fix: exact decimals must NOT become IEEE FLOAT.
    assert canonical_to_ddl("sqlserver", "decimal") == "DECIMAL(38,9)"
    assert canonical_to_ddl("sqlserver", "decimal(12,4)") == "DECIMAL(12,4)"
    # money/numeric fold to canonical decimal -> also exact now
    assert canonical_to_ddl("sqlserver", "money") == "DECIMAL(38,9)"
    # canonical `number` (approximate) legitimately stays FLOAT
    assert canonical_to_ddl("sqlserver", "number") == "FLOAT"


@pytest.mark.parametrize("dialect,lone,composite", [
    ("sqlserver", "NVARCHAR(450)", "NVARCHAR(200)"),
    ("oracle", "VARCHAR2(400)", "VARCHAR2(200)"),
])
def test_unbounded_key_types_are_bounded(dialect, lone, composite):
    # non-key string is unbounded; a lone key / composite key must be bounded so
    # it can be indexed. Applies to string/json/binary.
    for canon in ("string", "json", "binary"):
        assert canonical_to_ddl(dialect, canon, is_key=False) == _DDL_MATRIX[dialect][canon]
        assert canonical_to_ddl(dialect, canon, is_key=True, single_key=True) == lone
        assert canonical_to_ddl(dialect, canon, is_key=True, single_key=False) == composite
    # integer keys are already bounded — is_key must not change them
    assert canonical_to_ddl(dialect, "integer", is_key=True) == _DDL_MATRIX[dialect]["integer"]


@pytest.mark.parametrize("dialect,expected", [
    ("postgresql", "NUMERIC(12,4)"),
    ("mysql", "DECIMAL(12,4)"),
    ("clickhouse", "Decimal(12, 4)"),
    ("sqlserver", "DECIMAL(12,4)"),
])
def test_decimal_precision_preserved_where_supported(dialect, expected):
    assert canonical_to_ddl(dialect, "decimal(12,4)") == expected


@pytest.mark.parametrize("dialect,fixed", [
    ("snowflake", "NUMBER(38,9)"),
    ("databricks", "DECIMAL(38,9)"),
])
def test_decimal_precision_fixed_where_not_supported(dialect, fixed):
    # snowflake/databricks currently ignore source precision (behavior-preserving)
    assert canonical_to_ddl(dialect, "decimal(12,4)") == fixed


def test_canonical_to_ddl_unknown_dialect_raises():
    with pytest.raises(ValueError):
        canonical_to_ddl("db2", "string")


def test_canonical_to_ddl_dialect_aliases():
    assert canonical_to_ddl("mssql", "decimal") == canonical_to_ddl("sqlserver", "decimal")
    assert canonical_to_ddl("pg", "json") == canonical_to_ddl("postgresql", "json")
