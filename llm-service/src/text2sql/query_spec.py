"""
QuerySpec: Structured intermediate representation for NL→SQL.

The QuerySpec captures the *intent* of a query in a dialect-agnostic way,
which is then compiled to SQL by the compiler module.
"""

from __future__ import annotations

from enum import Enum
from typing import Literal, Optional, Union
from pydantic import BaseModel, Field


class AggregationType(str, Enum):
    """Supported aggregation functions."""

    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT_DISTINCT = "count_distinct"


class FilterOperator(str, Enum):
    """Supported filter operators."""

    EQ = "eq"  # =
    NE = "ne"  # != / <>
    GT = "gt"  # >
    GE = "ge"  # >=
    LT = "lt"  # <
    LE = "le"  # <=
    LIKE = "like"
    ILIKE = "ilike"  # case-insensitive like
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    BETWEEN = "between"


class JoinType(str, Enum):
    """Supported join types."""

    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


class OrderDirection(str, Enum):
    """Order by direction."""

    ASC = "asc"
    DESC = "desc"


class TimeGrain(str, Enum):
    """Time bucketing granularity."""

    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class SelectColumn(BaseModel):
    """A column in the SELECT clause."""

    column: str  # Column name or expression (e.g., "orders.total")
    alias: Optional[str] = None
    aggregation: Optional[AggregationType] = None
    table: Optional[str] = None  # Source table (for disambiguation)

    def qualified_name(self) -> str:
        """Get the fully qualified column name."""
        if self.table:
            return f"{self.table}.{self.column}"
        return self.column


class Filter(BaseModel):
    """A WHERE clause filter condition."""

    column: str
    operator: FilterOperator
    value: Optional[Union[str, int, float, bool, list]] = None  # Can be None for IS_NULL
    logical_connector: Literal["and", "or"] = "and"
    table: Optional[str] = None


class JoinSpec(BaseModel):
    """A JOIN specification."""

    table: str
    join_type: JoinType = JoinType.INNER
    on_conditions: list[str] = Field(default_factory=list)  # e.g., ["orders.customer_id = customers.id"]
    alias: Optional[str] = None


class OrderBy(BaseModel):
    """An ORDER BY specification."""

    column: str
    direction: OrderDirection = OrderDirection.ASC
    table: Optional[str] = None


class TimeRange(BaseModel):
    """Time range for filtering."""

    # Absolute range
    start: Optional[str] = None  # ISO date/datetime string
    end: Optional[str] = None

    # Or relative range
    relative: Optional[str] = None  # e.g., "last_30_days", "last_calendar_month", "this_year"

    def is_relative(self) -> bool:
        return self.relative is not None


class Ambiguity(BaseModel):
    """
    Represents an ambiguity detected during query spec generation.

    When the model detects that a query is ambiguous (e.g., "sales last month"
    could mean calendar month or last 30 days), it should surface these
    ambiguities so HITL can be triggered.
    """

    field: str  # Which QuerySpec field is ambiguous
    options: list[str]  # Possible interpretations
    chosen: str  # What the model chose
    confidence: float  # How confident the model is (0-1)
    reason: Optional[str] = None  # Why it's ambiguous


class QuerySpec(BaseModel):
    """
    Structured query specification (intermediate representation).

    This captures the *intent* of a query in a dialect-agnostic way.
    The compiler converts this to SQL for the target dialect.
    """

    # Natural language intent summary (helps repair loop converge faster)
    intent_description: str = ""

    # Core query structure
    from_table: str
    from_table_alias: Optional[str] = None

    select_columns: list[SelectColumn] = Field(default_factory=list)
    joins: list[JoinSpec] = Field(default_factory=list)
    filters: list[Filter] = Field(default_factory=list)
    group_by: list[str] = Field(default_factory=list)
    having: Optional[Filter] = None
    order_by: list[OrderBy] = Field(default_factory=list)
    limit: Optional[int] = None

    # Time bucketing (Phase 2)
    time_column: Optional[str] = None
    time_grain: Optional[TimeGrain] = None
    time_range: Optional[TimeRange] = None

    # Join disambiguation (Phase 3)
    preferred_join_path: Optional[str] = None  # Chosen join path ID when multiple options exist

    # Ambiguity tracking
    ambiguities: list[Ambiguity] = Field(default_factory=list)

    # Metadata
    requires_hitl: bool = False
    hitl_reason: Optional[str] = None

    def has_aggregations(self) -> bool:
        """Check if the query has any aggregations."""
        return any(col.aggregation is not None for col in self.select_columns)

    def has_ambiguities(self, threshold: float = 0.7) -> bool:
        """Check if there are low-confidence ambiguities that should trigger HITL."""
        return any(a.confidence < threshold for a in self.ambiguities)

    def get_all_tables(self) -> list[str]:
        """Get all tables referenced in the query."""
        tables = [self.from_table]
        for join in self.joins:
            tables.append(join.table)
        return tables

    def get_all_columns(self) -> list[str]:
        """Get all columns referenced in the query."""
        columns = []
        for col in self.select_columns:
            columns.append(col.qualified_name())
        for f in self.filters:
            if f.table:
                columns.append(f"{f.table}.{f.column}")
            else:
                columns.append(f.column)
        columns.extend(self.group_by)
        for ob in self.order_by:
            if ob.table:
                columns.append(f"{ob.table}.{ob.column}")
            else:
                columns.append(ob.column)
        if self.time_column:
            columns.append(self.time_column)
        return columns


def create_minimal_query_spec(
    from_table: str,
    select_columns: Optional[list[str]] = None,
    intent: str = "",
    limit: int = 100,
) -> QuerySpec:
    """Create a minimal QuerySpec for simple queries."""
    cols = select_columns or ["*"]
    return QuerySpec(
        intent_description=intent,
        from_table=from_table,
        select_columns=[SelectColumn(column=c) for c in cols],
        limit=limit,
    )
