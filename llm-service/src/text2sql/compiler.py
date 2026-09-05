"""
Dialect-aware SQL compiler using sqlglot.

This module compiles QuerySpec to SQL for any supported dialect,
handling dialect-specific syntax (especially time bucketing) in one place.
"""

from __future__ import annotations

import re
from typing import Optional

import sqlglot
from sqlglot import exp, MappingSchema
from sqlglot.dialects import Dialects

from .schema import TypedSchema
from .query_spec import (
    QuerySpec,
    SelectColumn,
    Filter,
    FilterOperator,
    JoinSpec,
    JoinType,
    OrderBy,
    TimeGrain,
    AggregationType,
)


# Map connector/dialect strings to sqlglot dialect identifiers
DIALECT_MAP: dict[str, str] = {
    # PostgreSQL variants
    "postgresql": "postgres",
    "postgres": "postgres",
    "redshift": "redshift",
    # MySQL variants
    "mysql": "mysql",
    "mariadb": "mysql",
    # Cloud warehouses
    "bigquery": "bigquery",
    "snowflake": "snowflake",
    "databricks": "databricks",
    "spark": "spark",
    # Others
    "sqlite": "sqlite",
    "duckdb": "duckdb",
    "trino": "trino",
    "presto": "presto",
    "clickhouse": "clickhouse",
    "oracle": "oracle",
    "tsql": "tsql",
    "mssql": "tsql",
    "sqlserver": "tsql",
    "hive": "hive",
}

SUPPORTED_DIALECTS = list(DIALECT_MAP.keys())


def resolve_dialect(dialect: str) -> str:
    """Resolve a dialect string to a sqlglot dialect identifier."""
    d = dialect.lower().strip()
    if d in DIALECT_MAP:
        return DIALECT_MAP[d]
    # If not found, try to use as-is (sqlglot may support it)
    return d


def _build_time_bucket(
    time_column: str,
    grain: TimeGrain,
    dialect: str,
    table_alias: Optional[str] = None,
) -> exp.Expression:
    """
    Build a time bucketing expression for the given dialect.

    This is the central place where dialect-specific time functions are handled.
    """
    # Build the column reference
    if table_alias:
        col_ref = exp.column(time_column, table_alias)
    else:
        col_ref = exp.column(time_column)

    sqlglot_dialect = resolve_dialect(dialect)

    # Postgres-style dialects (DATE_TRUNC)
    if sqlglot_dialect in ("postgres", "redshift", "snowflake", "duckdb", "trino", "presto"):
        grain_str = grain.value.lower()
        return exp.func("DATE_TRUNC", exp.Literal.string(grain_str), col_ref)

    # MySQL / MariaDB (DATE_FORMAT or DATE())
    if sqlglot_dialect == "mysql":
        if grain == TimeGrain.HOUR:
            return exp.func("DATE_FORMAT", col_ref, exp.Literal.string("%Y-%m-%d %H:00:00"))
        elif grain == TimeGrain.DAY:
            return exp.func("DATE", col_ref)
        elif grain == TimeGrain.WEEK:
            # YEARWEEK gives YYYYWW; alternatively DATE_FORMAT with %Y-%u
            return exp.func("DATE_FORMAT", col_ref, exp.Literal.string("%Y-%u"))
        elif grain == TimeGrain.MONTH:
            return exp.func("DATE_FORMAT", col_ref, exp.Literal.string("%Y-%m-01"))
        elif grain == TimeGrain.QUARTER:
            # MySQL: CONCAT(YEAR, '-Q', QUARTER)
            year_part = exp.func("YEAR", col_ref)
            quarter_part = exp.func("QUARTER", col_ref)
            return exp.func("CONCAT", year_part, exp.Literal.string("-Q"), quarter_part)
        elif grain == TimeGrain.YEAR:
            return exp.func("YEAR", col_ref)

    # BigQuery (TIMESTAMP_TRUNC or DATE_TRUNC)
    if sqlglot_dialect == "bigquery":
        grain_str = grain.value.upper()
        return exp.func("TIMESTAMP_TRUNC", col_ref, exp.Literal.string(grain_str))

    # SQLite (strftime)
    if sqlglot_dialect == "sqlite":
        format_map = {
            TimeGrain.HOUR: "%Y-%m-%d %H:00:00",
            TimeGrain.DAY: "%Y-%m-%d",
            TimeGrain.WEEK: "%Y-%W",
            TimeGrain.MONTH: "%Y-%m",
            TimeGrain.QUARTER: "%Y",  # SQLite doesn't have quarters; approximate
            TimeGrain.YEAR: "%Y",
        }
        fmt = format_map.get(grain, "%Y-%m-%d")
        return exp.func("strftime", exp.Literal.string(fmt), col_ref)

    # ClickHouse
    if sqlglot_dialect == "clickhouse":
        grain_func = {
            TimeGrain.HOUR: "toStartOfHour",
            TimeGrain.DAY: "toDate",
            TimeGrain.WEEK: "toMonday",
            TimeGrain.MONTH: "toStartOfMonth",
            TimeGrain.QUARTER: "toStartOfQuarter",
            TimeGrain.YEAR: "toStartOfYear",
        }
        func_name = grain_func.get(grain, "toDate")
        return exp.func(func_name, col_ref)

    # Default fallback: use DATE_TRUNC (may not work for all dialects)
    grain_str = grain.value.lower()
    return exp.func("DATE_TRUNC", exp.Literal.string(grain_str), col_ref)


def _time_range_bounds(relative: str, dialect: str) -> tuple[str, str] | tuple[None, None]:
    """
    Convert a relative time range string into (low, high) bound SQL expressions.

    Supported examples:
    - last_30_days, last_7_days
    - last_6_months, last_12_months
    - last_calendar_month
    - this_year
    """
    rel = (relative or "").strip().lower()
    if not rel:
        return (None, None)

    d = resolve_dialect(dialect)
    is_mysql = d == "mysql"
    is_pg = d in ("postgres", "redshift", "snowflake", "duckdb", "trino", "presto")

    # Helper to format dialect-specific interval expressions.
    def _sub_days(n: int) -> str:
        if is_mysql:
            return f"DATE_SUB(CURRENT_DATE, INTERVAL {n} DAY)"
        if is_pg:
            return f"CURRENT_DATE - INTERVAL '{n} days'"
        return f"CURRENT_DATE - INTERVAL '{n} days'"

    def _sub_months(n: int) -> str:
        if is_mysql:
            return f"DATE_SUB(CURRENT_DATE, INTERVAL {n} MONTH)"
        if is_pg:
            return f"CURRENT_DATE - INTERVAL '{n} months'"
        return f"CURRENT_DATE - INTERVAL '{n} months'"

    high = "CURRENT_DATE" if is_mysql or is_pg else "CURRENT_DATE"

    m = re.match(r"^last_(\\d+)_days$", rel)
    if m:
        n = int(m.group(1))
        return (_sub_days(n), high)

    m = re.match(r"^last_(\\d+)_months$", rel)
    if m:
        n = int(m.group(1))
        return (_sub_months(n), high)

    if rel == "last_30_days":
        return (_sub_days(30), high)

    if rel == "last_90_days":
        return (_sub_days(90), high)

    if rel == "last_calendar_month":
        if is_mysql:
            low = "DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), '%Y-%m-01')"
            high2 = "DATE_FORMAT(CURRENT_DATE, '%Y-%m-01')"
            return (low, high2)
        if is_pg:
            low = "DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')"
            high2 = "DATE_TRUNC('month', CURRENT_DATE)"
            return (low, high2)
        return (None, None)

    if rel == "this_year":
        if is_mysql:
            low = "DATE_FORMAT(CURRENT_DATE, '%Y-01-01')"
            return (low, high)
        if is_pg:
            low = "DATE_TRUNC('year', CURRENT_DATE)"
            return (low, high)
        return (None, None)

    return (None, None)


def _build_time_range_filter(spec: QuerySpec, dialect: str, table_aliases: dict[str, str]) -> exp.Expression | None:
    """
    Build a time range filter expression from QuerySpec.time_range + time_column.

    Note: This is intentionally simple. It supports relative ranges and absolute start/end strings.
    """
    if not spec.time_column or not spec.time_range:
        return None

    # Avoid duplicating a time filter if the model already emitted one in spec.filters.
    for f in spec.filters or []:
        if (f.column or "").strip().lower() == (spec.time_column or "").strip().lower():
            return None

    # Resolve time column reference to an Expression.
    tc_raw = (spec.time_column or "").strip()
    tc_raw = tc_raw.split()[-1]  # defensive: strip accidental aliases
    tc_raw = tc_raw.strip()

    if "." in tc_raw:
        t, c = tc_raw.split(".", 1)
        t_eff = table_aliases.get(t, t)
        time_col_expr = exp.column(c, t_eff)
    else:
        time_col_expr = exp.column(tc_raw, spec.from_table_alias or spec.from_table)

    # Absolute range
    start = getattr(spec.time_range, "start", None)
    end = getattr(spec.time_range, "end", None)
    if start and end:
        low = exp.Literal.string(str(start))
        high = exp.Literal.string(str(end))
        return exp.Between(this=time_col_expr, low=low, high=high)

    # Relative range
    rel = getattr(spec.time_range, "relative", None)
    if rel:
        low_s, high_s = _time_range_bounds(str(rel), dialect)
        if low_s and high_s:
            low = _parse_expr(low_s, dialect)
            high = _parse_expr(high_s, dialect)
            # Use BETWEEN for broad compatibility (inclusive bounds).
            return exp.Between(this=time_col_expr, low=low, high=high)

    return None


def _build_aggregation(col_expr: exp.Expression, agg: AggregationType) -> exp.Expression:
    """Build an aggregation expression."""
    if agg == AggregationType.COUNT:
        return exp.func("COUNT", col_expr)
    elif agg == AggregationType.COUNT_DISTINCT:
        return exp.func("COUNT", exp.Distinct(expressions=[col_expr]))
    elif agg == AggregationType.SUM:
        return exp.func("SUM", col_expr)
    elif agg == AggregationType.AVG:
        return exp.func("AVG", col_expr)
    elif agg == AggregationType.MIN:
        return exp.func("MIN", col_expr)
    elif agg == AggregationType.MAX:
        return exp.func("MAX", col_expr)
    return col_expr


_AGG_FUNC_RE = re.compile(r"^\s*(count_distinct|count|sum|avg|min|max)\s*\((.*)\)\s*$", re.IGNORECASE)


def _parse_expr(expr_str: str, dialect: str) -> exp.Expression:
    """
    Parse a SQL expression string into a sqlglot Expression.

    Falls back to treating as a column identifier if parsing fails.
    """
    s = (expr_str or "").strip()
    if not s:
        return exp.Literal.string("")
    try:
        return sqlglot.parse_one(s, dialect=resolve_dialect(dialect), into=exp.Expression)
    except Exception:
        # Treat as identifier/column
        if "." in s:
            t, c = s.split(".", 1)
            return exp.column(c, t)
        return exp.column(s)


def _num_literal(v) -> exp.Expression:
    """Build a numeric literal, failing safe on non-numeric input.

    exp.Literal.number() emits its argument UNQUOTED, so a raw string like
    '0 OR 1=1' would inject unescaped SQL into the predicate. Coerce to a real
    int/float; if the value is not numeric, fall back to a properly quoted
    string literal (sqlglot escapes it) instead of emitting raw text.
    """
    # bool is a subclass of int, so this MUST precede the int check. Render a
    # real SQL boolean literal (TRUE/FALSE) — emitting 1/0 breaks Postgres on
    # boolean columns ("operator does not exist: boolean = integer").
    if isinstance(v, bool):
        return exp.Boolean(this=v)
    if isinstance(v, (int, float)):
        return exp.Literal.number(v)
    try:
        return exp.Literal.number(int(str(v)))
    except (TypeError, ValueError):
        pass
    try:
        return exp.Literal.number(float(str(v)))
    except (TypeError, ValueError):
        return exp.Literal.string(str(v))


def _build_filter_expr(f: Filter) -> exp.Expression:
    """Build a filter expression."""
    col = exp.column(f.column, f.table) if f.table else exp.column(f.column)

    if f.operator == FilterOperator.EQ:
        return exp.EQ(this=col, expression=exp.Literal.string(str(f.value)) if isinstance(f.value, str) else _num_literal(f.value))
    elif f.operator == FilterOperator.NE:
        return exp.NEQ(this=col, expression=exp.Literal.string(str(f.value)) if isinstance(f.value, str) else _num_literal(f.value))
    elif f.operator == FilterOperator.GT:
        return exp.GT(this=col, expression=_num_literal(f.value))
    elif f.operator == FilterOperator.GE:
        return exp.GTE(this=col, expression=_num_literal(f.value))
    elif f.operator == FilterOperator.LT:
        return exp.LT(this=col, expression=_num_literal(f.value))
    elif f.operator == FilterOperator.LE:
        return exp.LTE(this=col, expression=_num_literal(f.value))
    elif f.operator == FilterOperator.LIKE:
        return exp.Like(this=col, expression=exp.Literal.string(str(f.value)))
    elif f.operator == FilterOperator.ILIKE:
        return exp.ILike(this=col, expression=exp.Literal.string(str(f.value)))
    elif f.operator == FilterOperator.IN:
        values = [exp.Literal.string(str(v)) if isinstance(v, str) else _num_literal(v) for v in (f.value or [])]
        return exp.In(this=col, expressions=values)
    elif f.operator == FilterOperator.NOT_IN:
        values = [exp.Literal.string(str(v)) if isinstance(v, str) else _num_literal(v) for v in (f.value or [])]
        return exp.Not(this=exp.In(this=col, expressions=values))
    elif f.operator == FilterOperator.IS_NULL:
        return exp.Is(this=col, expression=exp.Null())
    elif f.operator == FilterOperator.IS_NOT_NULL:
        return exp.Not(this=exp.Is(this=col, expression=exp.Null()))
    elif f.operator == FilterOperator.BETWEEN:
        if isinstance(f.value, list) and len(f.value) >= 2:
            low = exp.Literal.string(str(f.value[0])) if isinstance(f.value[0], str) else _num_literal(f.value[0])
            high = exp.Literal.string(str(f.value[1])) if isinstance(f.value[1], str) else _num_literal(f.value[1])
            return exp.Between(this=col, low=low, high=high)

    # Fallback: equality
    return exp.EQ(this=col, expression=exp.Literal.string(str(f.value)))


def _build_join_type(jt: JoinType) -> str:
    """Map JoinType enum to sqlglot join kind."""
    mapping = {
        JoinType.INNER: "INNER",
        JoinType.LEFT: "LEFT",
        JoinType.RIGHT: "RIGHT",
        JoinType.FULL: "FULL",
        JoinType.CROSS: "CROSS",
    }
    return mapping.get(jt, "INNER")


def compile_query_spec(
    spec: QuerySpec,
    dialect: str,
    schema: Optional[TypedSchema] = None,
) -> str:
    """
    Compile a QuerySpec to SQL for the given dialect.

    Args:
        spec: The QuerySpec to compile
        dialect: Target SQL dialect (e.g., "postgresql", "mysql", "bigquery")
        schema: Optional typed schema for validation (not used in compilation)

    Returns:
        SQL query string for the target dialect
    """
    sqlglot_dialect = resolve_dialect(dialect)

    # Build FROM clause
    from_table = exp.Table(this=exp.to_identifier(spec.from_table))
    if spec.from_table_alias:
        from_table = from_table.as_(spec.from_table_alias)

    # Map base table names to SQL aliases (so SELECT/WHERE can reference alias consistently)
    table_aliases: dict[str, str] = {}
    if spec.from_table_alias:
        table_aliases[spec.from_table] = spec.from_table_alias
    for j in spec.joins or []:
        if j.alias:
            table_aliases[j.table] = j.alias

    def _rewrite_expr_str(s: str) -> str:
        out = (s or "").strip()
        if not out:
            return out
        # MySQL doesn't have a "public" schema; strip common Postgres prefixes from model output.
        if sqlglot_dialect == "mysql":
            out = re.sub(r"\bpublic\.", "", out, flags=re.IGNORECASE)
        # If a table has an alias, rewrite `table.col` → `alias.col`.
        for base, alias in table_aliases.items():
            if base and alias and base != alias:
                out = re.sub(rf"\b{re.escape(base)}\s*\.", f"{alias}.", out)
        return out

    # Build SELECT expressions
    select_exprs: list[exp.Expression] = []
    non_agg_group_exprs: list[exp.Expression] = []

    # If time bucketing is requested, add it first
    time_bucket_alias = None
    if spec.time_column and spec.time_grain:
        time_bucket = _build_time_bucket(
            spec.time_column,
            spec.time_grain,
            dialect,
            spec.from_table_alias or spec.from_table,
        )
        time_bucket_alias = f"{spec.time_grain.value}_bucket"
        select_exprs.append(exp.alias_(time_bucket, time_bucket_alias))

    # Add other select columns
    for col in spec.select_columns:
        raw_col = (col.column or "").strip()
        if raw_col == "*":
            col_expr = exp.Star()
        else:
            # If the model put an aggregation in the column string (e.g., "SUM(amount)"),
            # parse it and convert to our structured aggregation form.
            m = _AGG_FUNC_RE.match(raw_col)
            if m:
                agg_name = m.group(1).lower()
                inner = (m.group(2) or "").strip()
                # If the model embedded an aggregation in the column string, use the inner expression
                # to avoid generating SUM(SUM(x)).
                if inner:
                    raw_col = inner

                # Normalize aggregation if not already provided.
                if not col.aggregation:
                    if agg_name == "count_distinct":
                        col.aggregation = AggregationType.COUNT_DISTINCT
                    else:
                        try:
                            col.aggregation = AggregationType(agg_name)
                        except Exception:
                            col.aggregation = None

            # Parse expressions (functions, arithmetic) safely; fallback to column.
            effective_table = table_aliases.get(col.table, col.table) if getattr(col, "table", None) else None
            raw_col_rewritten = _rewrite_expr_str(raw_col)
            if effective_table and "." not in raw_col_rewritten and raw_col_rewritten.isidentifier():
                col_expr = exp.column(raw_col_rewritten, effective_table)
            else:
                # If raw_col is already qualified (contains '.'), don't prepend col.table again.
                expr_s = raw_col_rewritten
                if effective_table and "." not in expr_s:
                    expr_s = f"{effective_table}.{expr_s}"
                col_expr = _parse_expr(expr_s, dialect)

        if col.aggregation:
            col_expr = _build_aggregation(col_expr, col.aggregation)
        else:
            # Track non-aggregated select expressions for GROUP BY auto-fix (MySQL only_full_group_by).
            if raw_col != "*" and not isinstance(col_expr, exp.Star):
                non_agg_group_exprs.append(col_expr)

        if col.alias:
            col_expr = exp.alias_(col_expr, col.alias)

        select_exprs.append(col_expr)

    # If no select columns, default to *
    if not select_exprs:
        select_exprs = [exp.Star()]

    # Build the SELECT statement
    select = exp.Select().select(*select_exprs).from_(from_table)

    # Add JOINs
    for join in spec.joins:
        join_table = exp.Table(this=exp.to_identifier(join.table))
        if join.alias:
            join_table = join_table.as_(join.alias)

        # Parse ON conditions
        on_expr = None
        if join.on_conditions:
            # For simplicity, parse the first condition; in production, parse all
            on_str = _rewrite_expr_str(" AND ".join(join.on_conditions))
            try:
                on_expr = sqlglot.parse_one(on_str, into=exp.Expression, dialect=sqlglot_dialect)
            except Exception:
                # Fallback: use raw string
                on_expr = exp.Literal.string(on_str)

        select = select.join(
            join_table,
            join_type=_build_join_type(join.join_type),
            on=on_expr,
        )

    # Add WHERE clause
    where_exprs: list[exp.Expression] = []
    if spec.filters:
        for f in spec.filters:
            # Respect aliases and strip invalid schema prefixes in model output.
            f_col = _rewrite_expr_str(f.column)
            f_table = table_aliases.get(f.table, f.table) if getattr(f, "table", None) else None
            ff = Filter(
                column=f_col,
                operator=f.operator,
                value=f.value,
                logical_connector=f.logical_connector,
                table=f_table,
            )
            filter_expr = _build_filter_expr(ff)
            where_exprs.append(filter_expr)

    tr = _build_time_range_filter(spec, dialect, table_aliases)
    if tr is not None:
        where_exprs.append(tr)

    if where_exprs:
        combined_where = where_exprs[0]
        for we in where_exprs[1:]:
            combined_where = exp.And(this=combined_where, expression=we)
        select = select.where(combined_where)

    # Add GROUP BY
    group_by_exprs: list[exp.Expression] = []
    group_by_sql: set[str] = set()

    # If time bucketing, group by the time bucket
    if time_bucket_alias:
        gb1 = exp.Literal.number(1)  # GROUP BY 1 (positional)
        group_by_exprs.append(gb1)
        try:
            group_by_sql.add(gb1.sql(dialect=sqlglot_dialect))
        except Exception:
            pass

    # Add explicit group_by columns
    for gb in spec.group_by:
        gb_s = str(gb or "").strip()
        if not gb_s:
            continue
        gb_s = _rewrite_expr_str(gb_s)
        # If we already bucketed time, skip additional time-bucket-like groupings.
        if time_bucket_alias and spec.time_column and spec.time_grain:
            lc = gb_s.lower()
            if lc == spec.time_column.lower():
                continue
            if lc.endswith("." + spec.time_column.lower()):
                continue
            if spec.time_column.lower() in lc and any(k in lc for k in ("date_trunc", "date_format", "extract(", "month(", "year(", "quarter(", "week(", "day(", "date(")):
                continue
        # Positional group by
        if gb_s.isdigit():
            gb_lit = exp.Literal.number(int(gb_s))
            group_by_exprs.append(gb_lit)
            try:
                group_by_sql.add(gb_lit.sql(dialect=sqlglot_dialect))
            except Exception:
                pass
            continue
        gb_expr = _parse_expr(gb_s, dialect)
        group_by_exprs.append(gb_expr)
        try:
            group_by_sql.add(gb_expr.sql(dialect=sqlglot_dialect))
        except Exception:
            pass

    # MySQL safety: if there are aggregations and we SELECT non-aggregated columns, GROUP BY them too.
    if spec.has_aggregations() and non_agg_group_exprs:
        for e in non_agg_group_exprs:
            try:
                e_sql = e.sql(dialect=sqlglot_dialect)
            except Exception:
                e_sql = None
            if e_sql and e_sql in group_by_sql:
                continue
            group_by_exprs.append(e)
            if e_sql:
                group_by_sql.add(e_sql)

    if group_by_exprs:
        select = select.group_by(*group_by_exprs)
    else:
        # If the query has aggregations and non-aggregated select columns, GROUP BY them.
        if spec.has_aggregations() and non_agg_group_exprs:
            select = select.group_by(*non_agg_group_exprs)

    # Add HAVING
    if spec.having:
        having_expr = _build_filter_expr(spec.having)
        select = select.having(having_expr)

    # Add ORDER BY
    order_exprs: list[exp.Expression] = []

    # If time bucketing, order by the time bucket first
    if time_bucket_alias:
        order_exprs.append(exp.Ordered(this=exp.Literal.number(1), desc=False))

    for ob in spec.order_by:
        ob_col = _rewrite_expr_str(str(ob.column))
        ob_table = table_aliases.get(ob.table, ob.table) if getattr(ob, "table", None) else None
        if ob_table and ob_col and "." not in ob_col and ob_col.strip().isidentifier():
            col = exp.column(ob_col, ob_table)
        else:
            # If ob.column is already qualified, don't prepend ob.table again.
            col = _parse_expr(ob_col, dialect) if (not ob_table or "." in ob_col) else _parse_expr(f"{ob_table}.{ob_col}", dialect)
        order_exprs.append(exp.Ordered(this=col, desc=ob.direction.value == "desc"))

    if order_exprs:
        select = select.order_by(*order_exprs)

    # Add LIMIT
    if spec.limit:
        select = select.limit(spec.limit)

    # Generate SQL for the target dialect
    sql = select.sql(dialect=sqlglot_dialect, pretty=False)

    return sql


class CompilationError(Exception):
    """Raised when query compilation fails."""

    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.details = details or {}
