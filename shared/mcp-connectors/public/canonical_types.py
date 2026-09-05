#!/usr/bin/env python3
"""Canonical type system — the single, dependency-free source of truth.

This module is deliberately **pydantic-free** and lives under ``public/`` (the
``shared`` Docker build context) so it can be shipped into every connector
image via ``COPY --from=shared canonical_types.py /app/canonical_types.py``
(the same mechanism as ``warehouse_adapters.py``). Both the discovery-response
validator (`schemas/response_schemas.py`, CI/llm-service) and every source /
sink connector at runtime import canonicalization from HERE, so there is one
authority and no per-connector drift.

Contract:
  * Every source connector's discover_schema() emits one of ``CANONICAL_TYPES``
    for each column (call ``canonicalize_type(raw, dialect=...)``).
  * Every sink translates canonical -> its own dialect with one mapper.
Adding a source = follow the contract. Adding a sink = one canonical->dialect
mapper. Zero cross-connector code.
"""
import re
from typing import Any, Optional

CANONICAL_TYPES = {
    "string",     # text / varchar
    "integer",    # int64
    "number",     # float64 (inexact: float/double/real)
    "decimal",    # exact fixed-point: DECIMAL/NUMERIC/money -> NUMERIC(38,9)
    "boolean",    # bool
    "timestamp",  # instant with timezone
    "date",       # calendar date
    "time",       # wall clock
    "json",       # semi-structured: object, array, custom scalar, GraphQL Connection
    "binary",     # bytes
}

# Mapping of common source-dialect types to canonical. Source connectors
# can either emit canonical directly (preferred) or rely on this map
# during normalization. Unknown types fall back to "string" so a typo or
# unsupported scalar never blocks a pipeline.
_DIALECT_TO_CANONICAL = {
    # GraphQL scalars (Shopify, GitHub, Linear, ...)
    "String": "string", "string": "string",
    "ID": "string",
    "Int": "integer", "int": "integer",
    "Float": "number", "float": "number",
    "Boolean": "boolean", "bool": "boolean", "boolean": "boolean",
    "DateTime": "timestamp", "Datetime": "timestamp", "datetime": "timestamp",
    "Date": "date", "date": "date",
    "Time": "time", "time": "time",
    "JSON": "json", "json": "json",
    # PostgreSQL information_schema.data_type values
    "text": "string", "character varying": "string", "varchar": "string",
    "char": "string", "character": "string", "uuid": "string", "name": "string",
    "smallint": "integer", "integer": "integer", "bigint": "integer",
    "serial": "integer", "bigserial": "integer", "smallserial": "integer",
    "int2": "integer", "int4": "integer", "int8": "integer",
    "real": "number", "double precision": "number",
    "numeric": "decimal", "decimal": "decimal", "money": "decimal",
    "timestamp without time zone": "timestamp", "timestamp with time zone": "timestamp",
    "timestamptz": "timestamp", "timestamp": "timestamp",
    # SQL Server datetime aliases (unambiguous; also in _SQLSERVER_CANONICAL).
    # Folded in the flat map too so a sink's canonical_to_ddl() still maps them
    # to a timestamp column even if a non-canonical source leaks the raw token.
    "datetime2": "timestamp", "smalldatetime": "timestamp",
    "interval": "string",
    "bytea": "binary", "bit": "binary", "bit varying": "binary",
    "jsonb": "json",
    "ARRAY": "json", "array": "json",
    # MySQL information_schema.columns DATA_TYPE values
    "tinyint": "integer", "mediumint": "integer", "year": "integer",
    "longtext": "string", "mediumtext": "string", "tinytext": "string",
    "double": "number",
    "datetime": "timestamp",
    "blob": "binary", "tinyblob": "binary", "mediumblob": "binary", "longblob": "binary",
    # varbinary is binary in every dialect that has it (MySQL, SQL Server); the
    # SQL Server-scoped map already covers it, so add it to the flat path too so
    # a MySQL source's varbinary column folds to binary (not string).
    "varbinary": "binary",
    # JSON Schema flavours (REST inference)
    "object": "json", "array": "json", "null": "string",
}


# =============================================================================
# DIALECT-SCOPED CANONICAL MAPS
# =============================================================================
#
# A single FLAT token->canonical map (the _DIALECT_TO_CANONICAL above) is
# UNSOUND across dialects because the same token name means different things:
#   * SQL Server `bit` is boolean, but PostgreSQL `bit` is a bit-string (binary)
#   * Oracle `NUMBER` is exact fixed-point, but the canonical name "number"
#     means INEXACT float
#   * SQL Server `timestamp` is an 8-byte rowversion (binary), NOT an instant
#   * ClickHouse `Int64`/`Float64`/`DateTime64` aren't in the flat map at all,
#     so they silently degrade to "string"
# so canonicalization for these dialects MUST be scoped by the SOURCE dialect.
# A flat lookup here is the root cause of the "all-String columns + synthetic
# _rsync_row_hash PK" destination failures. Keys are the lowercased BASE token
# (parameters, ClickHouse Nullable()/LowCardinality() wrappers, and trailing
# " unsigned" stripped before lookup). PostgreSQL and MySQL are already covered
# correctly by the flat map, so they intentionally fall through to it.

_ORACLE_CANONICAL = {
    "varchar2": "string", "nvarchar2": "string", "varchar": "string",
    "char": "string", "nchar": "string", "clob": "string", "nclob": "string",
    "long": "string", "rowid": "string", "urowid": "string", "interval": "string",
    "number": "decimal", "float": "number",
    "binary_float": "number", "binary_double": "number",
    # Oracle DATE carries a time component (second precision) -> timestamp, not
    # date, so the wall-clock time is never silently dropped at the destination.
    "date": "timestamp", "timestamp": "timestamp",
    # "long raw" is a (legacy) binary type; it is matched as a full token BEFORE
    # the bare "long" (character) base reduction, which would misclassify it as
    # string. See the full-token-first lookup in canonicalize_type().
    "long raw": "binary",
    "raw": "binary", "blob": "binary", "bfile": "binary",
    "boolean": "boolean",
}
_SQLSERVER_CANONICAL = {
    "char": "string", "varchar": "string", "nchar": "string", "nvarchar": "string",
    "text": "string", "ntext": "string", "sysname": "string", "xml": "string",
    "uniqueidentifier": "string",
    "bit": "boolean",
    "tinyint": "integer", "smallint": "integer", "int": "integer", "bigint": "integer",
    "decimal": "decimal", "numeric": "decimal", "money": "decimal", "smallmoney": "decimal",
    "float": "number", "real": "number",
    "date": "date", "time": "time",
    "datetime": "timestamp", "datetime2": "timestamp", "smalldatetime": "timestamp",
    "datetimeoffset": "timestamp",
    "binary": "binary", "varbinary": "binary", "image": "binary",
    # SQL Server `timestamp`/`rowversion` is an 8-byte binary row-version, not an instant.
    "rowversion": "binary", "timestamp": "binary",
}
_CLICKHOUSE_CANONICAL = {
    "string": "string", "fixedstring": "string", "uuid": "string",
    "ipv4": "string", "ipv6": "string", "enum": "string", "enum8": "string", "enum16": "string",
    "int8": "integer", "int16": "integer", "int32": "integer", "int64": "integer",
    "uint8": "integer", "uint16": "integer", "uint32": "integer",
    # int64-UNSAFE widths must NOT map to canonical "integer" (int64), or they
    # overflow the destination BIGINT — the same overflow the Oracle NUMBER path
    # guards. UInt64 (up to ~1.8e19, 20 digits) fits NUMERIC(38,9) -> decimal;
    # 128/256-bit ints (up to ~5.8e76) exceed even NUMERIC(38), so -> string to
    # stay lossless.
    "uint64": "decimal",
    "int128": "string", "uint128": "string", "int256": "string", "uint256": "string",
    "float32": "number", "float64": "number", "bfloat16": "number",
    "decimal": "decimal", "decimal32": "decimal", "decimal64": "decimal",
    "decimal128": "decimal", "decimal256": "decimal",
    "bool": "boolean",
    "date": "date", "date32": "date",
    "datetime": "timestamp", "datetime64": "timestamp",
    "time": "time", "time64": "time",
    "array": "json", "tuple": "json", "map": "json", "nested": "json",
    "json": "json", "object": "json",
}
_DIALECT_CANONICAL_MAPS = {
    "oracle": _ORACLE_CANONICAL,
    "sqlserver": _SQLSERVER_CANONICAL,
    "mssql": _SQLSERVER_CANONICAL,
    "clickhouse": _CLICKHOUSE_CANONICAL,
}

# NUMBER(p,0) is emitted as canonical "integer" only when it fits a 64-bit
# integer at the destination; wider precisions stay "decimal" to avoid int64
# overflow (Oracle NUMBER goes to precision 38).
_INT64_SAFE_PRECISION = 18


def _unwrap_clickhouse(token: str) -> str:
    """Strip ClickHouse Nullable(...) / LowCardinality(...) wrappers, repeatedly."""
    low = token.strip()
    for _ in range(8):  # bounded: real nesting is shallow
        lo = low.lower()
        for wrapper in ("nullable(", "lowcardinality("):
            if lo.startswith(wrapper) and low.endswith(")"):
                low = low[len(wrapper):-1].strip()
                break
        else:
            break
    return low


def _base_token(token: str) -> str:
    """Lowercased base name: drop `(params)`, trailing ` unsigned`, whitespace."""
    return token.lower().split("(", 1)[0].split(" ", 1)[0].strip()


def canonicalize_type(t: Any, *, dialect: Optional[str] = None,
                      scale: Optional[int] = None,
                      precision: Optional[int] = None) -> str:
    """Coerce a source-dialect type string to one of CANONICAL_TYPES.

    Pass ``dialect`` (e.g. "oracle", "sqlserver", "clickhouse") for correct,
    collision-free mapping. The flat, dialect-agnostic path (``dialect=None``,
    the historical behavior) cannot disambiguate tokens like ``bit``,
    ``NUMBER`` or ``timestamp`` that mean different things per dialect, so
    sources for those dialects MUST pass their dialect. ``scale``/``precision``
    refine numerics: Oracle NUMBER(p,0) that fits int64 is an integer, otherwise
    a decimal.

    Unknown / non-string inputs become "string" — defensive default so a
    pipeline never blocks on an unrecognized scalar. Calling with no dialect is
    100% backward compatible with the prior flat behavior.
    """
    if not isinstance(t, str):
        return "string"
    raw = t.strip()
    if not raw:
        return "string"

    if dialect:
        d = dialect.strip().lower()
        dmap = _DIALECT_CANONICAL_MAPS.get(d)
        if dmap is not None:
            token = _unwrap_clickhouse(raw) if d == "clickhouse" else raw
            base = _base_token(token)
            # Multi-word types (e.g. Oracle "LONG RAW") must match as a full
            # token — with params stripped but spaces kept — BEFORE the bare
            # base token, which would collide with a single-word entry ("long").
            full = token.lower().split("(", 1)[0].strip()
            mapped = dmap.get(full)
            if mapped is None:
                mapped = dmap.get(base)
            if mapped is not None:
                if d == "oracle" and base == "number":
                    # NUMBER(p,0) -> integer only when it fits int64; else decimal.
                    if scale == 0 and precision is not None and precision <= _INT64_SAFE_PRECISION:
                        return "integer"
                    return "decimal"
                return mapped
            # Unknown token within a known dialect: fall through to the flat
            # path below (never worse than the historical behavior).

    # ---- flat, dialect-agnostic path (unchanged historical behavior) ----
    if raw in CANONICAL_TYPES:
        return raw
    mapped = _DIALECT_TO_CANONICAL.get(raw)
    if mapped is not None:
        return mapped
    # case-insensitive last-chance lookup
    lower = raw.lower()
    if lower in CANONICAL_TYPES:
        return lower
    mapped = _DIALECT_TO_CANONICAL.get(lower)
    if mapped is not None:
        return mapped
    # MySQL/PG can append "(N)" or " unsigned" — strip and retry.
    base = lower.split("(", 1)[0].split(" ", 1)[0].strip()
    if base in CANONICAL_TYPES:
        return base
    return _DIALECT_TO_CANONICAL.get(base, "string")


# =============================================================================
# CANONICAL -> DIALECT DDL (egress / sink side)
# =============================================================================
#
# The mirror of canonicalize_type(): every SINK translates a canonical column
# type into its own CREATE TABLE / ALTER TABLE DDL type via ONE mapper here,
# instead of 7 hand-written per-connector mappers that drift. Faithfully
# reproduces each destination connector's current batch-path behavior, with one
# deliberate fix: SQL Server `decimal` was mapped to FLOAT (IEEE approximate —
# loses exact scale, e.g. money 1.10); it now maps to DECIMAL(p,s), matching
# every other exact-decimal dialect. (Canonical `number` stays FLOAT — it
# denotes approximate float by definition.)
#
# Non-key base DDL per dialect. Key columns of the UNBOUNDED text types
# (string/json/binary) get a bounded form (see _KEY_BOUNDED_DDL) so they can be
# indexed as a primary/unique key. ClickHouse's key handling is a Nullable()
# wrapper applied by the caller, NOT a base-type change, so is_key is a no-op
# here for ClickHouse.
_CANONICAL_TO_DDL = {
    "postgresql": {
        "string": "TEXT", "integer": "BIGINT", "number": "DOUBLE PRECISION",
        "decimal": "NUMERIC(38,9)", "boolean": "BOOLEAN", "timestamp": "TIMESTAMPTZ",
        "date": "DATE", "time": "TIME", "json": "JSONB", "binary": "BYTEA",
    },
    "mysql": {
        "string": "TEXT", "integer": "BIGINT", "number": "DOUBLE",
        "decimal": "DECIMAL(38,9)", "boolean": "TINYINT(1)", "timestamp": "DATETIME(6)",
        "date": "DATE", "time": "TIME", "json": "JSON", "binary": "BLOB",
    },
    "clickhouse": {
        "string": "String", "integer": "Int64", "number": "Float64",
        "decimal": "Decimal(38, 9)", "boolean": "Bool", "timestamp": "DateTime64(3)",
        "date": "Date32", "time": "String", "json": "String", "binary": "String",
    },
    "snowflake": {
        "string": "VARCHAR", "integer": "NUMBER(38,0)", "number": "FLOAT",
        "decimal": "NUMBER(38,9)", "boolean": "BOOLEAN", "timestamp": "TIMESTAMP_NTZ",
        "date": "DATE", "time": "TIME", "json": "VARCHAR", "binary": "BINARY",
    },
    "databricks": {
        "string": "STRING", "integer": "BIGINT", "number": "DOUBLE",
        "decimal": "DECIMAL(38,9)", "boolean": "BOOLEAN", "timestamp": "TIMESTAMP",
        "date": "DATE", "time": "STRING", "json": "STRING", "binary": "BINARY",
    },
    "sqlserver": {
        "string": "NVARCHAR(MAX)", "integer": "BIGINT", "number": "FLOAT",
        "decimal": "DECIMAL(38,9)",  # FIX: was FLOAT (lossy for exact decimals/money)
        "boolean": "BIT", "timestamp": "DATETIME2",
        "date": "DATETIME2", "time": "DATETIME2", "json": "NVARCHAR(MAX)", "binary": "NVARCHAR(MAX)",
    },
    "oracle": {
        "string": "CLOB", "integer": "NUMBER(19)", "number": "NUMBER",
        "decimal": "NUMBER", "boolean": "NUMBER(1)", "timestamp": "TIMESTAMP",
        "date": "DATE", "time": "VARCHAR2(64)", "json": "CLOB", "binary": "CLOB",
    },
}
_DDL_DIALECT_ALIASES = {"mssql": "sqlserver", "postgres": "postgresql", "pg": "postgresql"}

# Bounded DDL for KEY columns of unbounded text canonical types (string/json/
# binary). "lone" = the table's only key column; "composite" = one of several
# (kept smaller to fit the dialect's index-key byte limit).
_KEY_BOUNDED_DDL = {
    "sqlserver": {"lone": "NVARCHAR(450)", "composite": "NVARCHAR(200)"},
    "oracle": {"lone": "VARCHAR2(400)", "composite": "VARCHAR2(200)"},
}
_UNBOUNDED_KEY_CANONICALS = {"string", "json", "binary"}
# Dialects whose decimal DDL preserves source (p,s); the rest use a fixed width.
_DECIMAL_PRECISION_DIALECTS = {"postgresql", "mysql", "clickhouse", "sqlserver"}


def _parse_precision_scale(type_str: Any):
    """Extract (precision, scale) from e.g. 'decimal(12,4)'.

    Single-arg 'decimal(10)' -> (10, 0); no parenthetical -> None.
    """
    if not isinstance(type_str, str):
        return None
    m = re.search(r"\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)", type_str)
    if not m:
        return None
    p = int(m.group(1))
    s = int(m.group(2)) if m.group(2) is not None else 0
    return (p, s)


def canonical_to_ddl(dialect: str, type_str: Any, *,
                     is_key: bool = False, single_key: bool = True) -> str:
    """Translate a canonical (or raw-ish) column type into a sink dialect's DDL.

    ``type_str`` is normalized through the flat ``canonicalize_type`` first, so
    stray dialect tokens (numeric/money/double/bigint/…) fold to canonical
    before mapping. ``is_key``/``single_key`` bound the unbounded text types so
    a key column is indexable (SQL Server / Oracle). Decimal precision/scale is
    preserved for the dialects that support it (parsed from ``type_str``).

    Raises ValueError for an unknown sink dialect.
    """
    d = (dialect or "").strip().lower()
    d = _DDL_DIALECT_ALIASES.get(d, d)
    table = _CANONICAL_TO_DDL.get(d)
    if table is None:
        raise ValueError("canonical_to_ddl: unknown sink dialect %r" % (dialect,))

    raw = type_str if isinstance(type_str, str) else ""
    canon = canonicalize_type(raw)  # fold raw dialect leftovers -> canonical
    if canon not in table:
        canon = "string"  # every sink's unknown-type fallback

    # Exact-decimal precision preservation.
    if canon == "decimal" and d in _DECIMAL_PRECISION_DIALECTS:
        ps = _parse_precision_scale(raw)
        if ps:
            p, s = ps
            if d == "postgresql":
                return "NUMERIC(%d,%d)" % (p, s)
            if d == "clickhouse":
                return "Decimal(%d, %d)" % (p, s)
            return "DECIMAL(%d,%d)" % (p, s)  # mysql, sqlserver

    # Bounded key form for unbounded text types.
    if is_key and canon in _UNBOUNDED_KEY_CANONICALS and d in _KEY_BOUNDED_DDL:
        b = _KEY_BOUNDED_DDL[d]
        return b["lone"] if single_key else b["composite"]

    return table[canon]
