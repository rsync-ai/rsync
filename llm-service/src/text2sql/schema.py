"""
Typed schema models for NL→SQL generation.

These models represent database schema with full type information,
enabling proper validation and join candidate generation.
"""

from __future__ import annotations

import re
from typing import Optional
from pydantic import BaseModel, Field


class ColumnSchema(BaseModel):
    """A column in a database table with type information."""

    name: str
    type: str = "unknown"  # SQL type (varchar, int, timestamp, etc.)
    is_primary_key: bool = False
    is_nullable: bool = True
    description: Optional[str] = None

    def is_temporal(self) -> bool:
        """Check if this column is likely a temporal/timestamp column."""
        t = self.type.lower()
        n = self.name.lower()
        temporal_types = ("timestamp", "datetime", "date", "time")
        temporal_names = ("_at", "_date", "_time", "created", "updated", "timestamp")
        return any(tt in t for tt in temporal_types) or any(tn in n for tn in temporal_names)

    def is_numeric(self) -> bool:
        """Check if this column is numeric."""
        t = self.type.lower()
        numeric_types = ("int", "decimal", "numeric", "float", "double", "real", "money", "bigint", "smallint")
        return any(nt in t for nt in numeric_types)


class ForeignKey(BaseModel):
    """A foreign key relationship between tables."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    confidence: float = 1.0  # 1.0 for explicit FK, lower for inferred


class TableSchema(BaseModel):
    """A database table with its columns."""

    name: str
    # Database schema (not to be confused with TypedSchema).
    # Defaults to None — callers MUST populate this from the source
    # connection's configured schema. Defaulting to "public" silently
    # breaks deployments that use non-default schemas (analytics, tenant_*, etc.).
    schema_name: Optional[str] = None
    columns: list[ColumnSchema] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)
    row_count: Optional[int] = None
    description: Optional[str] = None

    def get_column(self, name: str) -> Optional[ColumnSchema]:
        """Get a column by name (case-insensitive)."""
        name_lower = name.lower()
        for col in self.columns:
            if col.name.lower() == name_lower:
                return col
        return None

    def has_column(self, name: str) -> bool:
        """Check if the table has a column with the given name."""
        return self.get_column(name) is not None

    def get_temporal_columns(self) -> list[ColumnSchema]:
        """Get all temporal columns in the table."""
        return [c for c in self.columns if c.is_temporal()]

    def get_numeric_columns(self) -> list[ColumnSchema]:
        """Get all numeric columns in the table."""
        return [c for c in self.columns if c.is_numeric()]

    @property
    def column_names(self) -> list[str]:
        """Get all column names."""
        return [c.name for c in self.columns]


class TypedSchema(BaseModel):
    """
    Complete typed schema for NL→SQL generation.

    Includes tables, columns with types, and foreign key relationships.
    """

    tables: list[TableSchema] = Field(default_factory=list)
    foreign_keys: list[ForeignKey] = Field(default_factory=list)

    def get_table(self, name: str, schema_name: Optional[str] = None) -> Optional[TableSchema]:
        """Get a table by name (case-insensitive).

        If schema_name is provided, only return a table that matches it
        (case-insensitive). If a stored table has no schema_name (None), it
        will only match when the caller also passes None (i.e. "any schema").
        """
        # Accept qualified names like "public.orders" or "db.public.orders"
        name_norm = (name or "").strip()
        if "." in name_norm:
            name_norm = name_norm.split(".")[-1]
        name_lower = name_norm.lower()
        wanted = schema_name.lower() if schema_name else None
        for table in self.tables:
            if table.name.lower() != name_lower:
                continue
            if wanted is None:
                return table
            stored = (table.schema_name or "").lower()
            if stored == wanted:
                return table
        return None

    def has_table(self, name: str, schema_name: Optional[str] = None) -> bool:
        """Check if the schema has a table with the given name."""
        return self.get_table(name, schema_name) is not None

    def has_column(self, table_name: str, column_name: str) -> bool:
        """Check if a table has a column with the given name."""
        table = self.get_table(table_name)
        return table is not None and table.has_column(column_name)

    def get_join_candidates(self, from_table: str, to_table: str) -> list[ForeignKey]:
        """Get all possible join paths between two tables."""
        from_lower = from_table.lower()
        to_lower = to_table.lower()
        candidates = []
        for fk in self.foreign_keys:
            if (fk.from_table.lower() == from_lower and fk.to_table.lower() == to_lower) or \
               (fk.from_table.lower() == to_lower and fk.to_table.lower() == from_lower):
                candidates.append(fk)
        return sorted(candidates, key=lambda fk: -fk.confidence)

    def get_related_tables(self, table_name: str) -> list[str]:
        """Get all tables that have a relationship with the given table."""
        name_lower = table_name.lower()
        related = set()
        for fk in self.foreign_keys:
            if fk.from_table.lower() == name_lower:
                related.add(fk.to_table)
            elif fk.to_table.lower() == name_lower:
                related.add(fk.from_table)
        return list(related)

    @property
    def table_names(self) -> list[str]:
        """Get all table names."""
        return [t.name for t in self.tables]


def parse_schema_string(schema_str: str) -> TypedSchema:
    """
    Parse a legacy schema string into a TypedSchema.

    Format: "table1(col1, col2, col3); table2(col4, col5)"

    This provides backward compatibility with the old string-based schema format.
    Column types will be set to "unknown".
    """
    tables: list[TableSchema] = []

    if not schema_str:
        return TypedSchema(tables=tables)

    # Match patterns like: table_name(col1, col2, col3)
    for match in re.finditer(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(([^)]*)\)", schema_str):
        table_name = match.group(1).strip()
        cols_raw = match.group(2)
        columns = []

        for col_str in cols_raw.split(","):
            col_str = col_str.strip()
            if not col_str:
                continue

            # Check for type annotation: "col_name:type" or just "col_name"
            if ":" in col_str:
                col_name, col_type = col_str.split(":", 1)
                col_name = col_name.strip()
                col_type = col_type.strip()
            else:
                col_name = col_str
                col_type = "unknown"

            # Detect if it's likely a primary key (heuristic: "id" column)
            is_pk = col_name.lower() == "id" or col_name.lower().endswith("_id") and len(columns) == 0

            columns.append(
                ColumnSchema(
                    name=col_name,
                    type=col_type,
                    is_primary_key=is_pk and col_name.lower() == "id",
                )
            )

        tables.append(
            TableSchema(
                name=table_name,
                columns=columns,
                primary_key=["id"] if any(c.name.lower() == "id" for c in columns) else [],
            )
        )

    return TypedSchema(tables=tables)
