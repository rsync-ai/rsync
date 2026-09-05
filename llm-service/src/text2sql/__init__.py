"""
Text2SQL module for robust NL→SQL generation across dialects.

This module implements:
- Typed schema models (columns with types, PK/FK relationships)
- QuerySpec intermediate representation
- Dialect-aware SQL compiler (sqlglot-based)
- AST + schema validation
- Bounded repair loop
"""

from .schema import (
    ColumnSchema,
    TableSchema,
    ForeignKey,
    TypedSchema,
    parse_schema_string,
)
from .query_spec import (
    SelectColumn,
    Filter,
    JoinSpec,
    TimeRange,
    Ambiguity,
    QuerySpec,
)
from .compiler import compile_query_spec, SUPPORTED_DIALECTS
from .validator import validate_sql, ValidationResult

__all__ = [
    # Schema
    "ColumnSchema",
    "TableSchema",
    "ForeignKey",
    "TypedSchema",
    "parse_schema_string",
    # QuerySpec
    "SelectColumn",
    "Filter",
    "JoinSpec",
    "TimeRange",
    "Ambiguity",
    "QuerySpec",
    # Compiler
    "compile_query_spec",
    "SUPPORTED_DIALECTS",
    # Validator
    "validate_sql",
    "ValidationResult",
]
