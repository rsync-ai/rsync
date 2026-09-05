"""
AST-based SQL validation using sqlglot.

This module validates compiled SQL against:
- Syntax correctness (via sqlglot parsing)
- Schema compliance (tables/columns exist)
- Safety (SELECT-only, has LIMIT)
- Semantic correctness (time grain matches QuerySpec)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import re
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from .schema import TypedSchema
from .query_spec import QuerySpec, TimeGrain
from .compiler import resolve_dialect


@dataclass
class ValidationError:
    """A single validation error."""

    code: str
    message: str
    severity: str = "error"  # error, warning
    field: Optional[str] = None  # Which part of the query caused the error
    fix_hint: Optional[str] = None  # Actionable hint for repair


@dataclass
class ValidationResult:
    """Result of SQL validation."""

    valid: bool
    sql: str
    dialect: str
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)
    referenced_tables: list[str] = field(default_factory=list)
    referenced_columns: list[str] = field(default_factory=list)

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def has_warnings(self) -> bool:
        return len(self.warnings) > 0

    def add_error(
        self,
        code: str,
        message: str,
        field: Optional[str] = None,
        fix_hint: Optional[str] = None,
    ) -> None:
        self.errors.append(ValidationError(
            code=code,
            message=message,
            severity="error",
            field=field,
            fix_hint=fix_hint,
        ))
        self.valid = False

    def add_warning(
        self,
        code: str,
        message: str,
        field: Optional[str] = None,
        fix_hint: Optional[str] = None,
    ) -> None:
        self.warnings.append(ValidationError(
            code=code,
            message=message,
            severity="warning",
            field=field,
            fix_hint=fix_hint,
        ))

    def to_dict(self) -> dict:
        return {
            "valid": self.valid,
            "sql": self.sql,
            "dialect": self.dialect,
            "errors": [
                {"code": e.code, "message": e.message, "field": e.field, "fix_hint": e.fix_hint}
                for e in self.errors
            ],
            "warnings": [
                {"code": w.code, "message": w.message, "field": w.field, "fix_hint": w.fix_hint}
                for w in self.warnings
            ],
            "referenced_tables": self.referenced_tables,
            "referenced_columns": self.referenced_columns,
        }


def _out_of_scope_column_refs(ast: exp.Expression) -> list[str]:
    """Return ['alias.col', ...] for columns whose table-qualifier is not visible in that column's
    query scope nor any ancestor scope (correlation-safe). Catches a CTE/subquery-internal alias
    leaked into an outer query (e.g. a windowed top-N that references `p.price` in the final SELECT
    where `p` is only defined inside a CTE) -- such SQL passes flat alias resolution but errors at
    execution. Fail-open: returns [] on any analysis error (never raises, never blocks valid SQL).
    """
    try:
        from sqlglot.optimizer.scope import traverse_scope
        scopes = traverse_scope(ast)
    except Exception:
        return []
    out: list[str] = []
    seen: set[str] = set()
    try:
        for sc in scopes:
            visible: set[str] = set()
            s = sc
            while s is not None:
                try:
                    visible |= {str(k).lower() for k in s.sources.keys()}
                except Exception:
                    pass
                s = s.parent
            for col in sc.columns:
                t = col.table
                if t and str(t).lower() not in visible:
                    ref = f"{t}.{col.name}"
                    if ref not in seen:
                        seen.add(ref)
                        out.append(ref)
    except Exception:
        return []
    return out


def validate_sql(
    sql: str,
    dialect: str,
    schema: Optional[TypedSchema] = None,
    spec: Optional[QuerySpec] = None,
    require_limit: bool = True,
    max_limit: int = 10000,
) -> ValidationResult:
    """
    Validate SQL using AST analysis.

    Performs multiple validation layers:
    1. Parse check (syntax correctness)
    2. Safety check (SELECT-only, no dangerous operations)
    3. Schema validation (tables/columns exist)
    4. Semantic validation (matches QuerySpec intent)

    Args:
        sql: The SQL query to validate
        dialect: Target SQL dialect
        schema: Optional typed schema for column/table validation
        spec: Optional QuerySpec to validate semantic correctness
        require_limit: Whether to require a LIMIT clause
        max_limit: Maximum allowed LIMIT value

    Returns:
        ValidationResult with errors, warnings, and metadata
    """
    result = ValidationResult(valid=True, sql=sql, dialect=dialect)
    sqlglot_dialect = resolve_dialect(dialect)

    # Layer 1: Parse check
    try:
        ast = sqlglot.parse_one(sql, dialect=sqlglot_dialect)
    except ParseError as e:
        result.add_error(
            code="PARSE_ERROR",
            message=f"SQL syntax error: {str(e)}",
            fix_hint="Check SQL syntax and ensure it's valid for the target dialect.",
        )
        return result
    except Exception as e:
        result.add_error(
            code="PARSE_ERROR",
            message=f"Failed to parse SQL: {str(e)}",
        )
        return result

    # Layer 2: Safety check - SELECT only
    if not isinstance(ast, exp.Select):
        # Check if it's a WITH (CTE) followed by SELECT
        if isinstance(ast, exp.With):
            # CTEs are allowed if they end with SELECT
            pass
        else:
            stmt_type = type(ast).__name__
            result.add_error(
                code="NOT_SELECT",
                message=f"Only SELECT queries are allowed. Found: {stmt_type}",
                fix_hint="Rewrite as a SELECT query.",
            )
            return result

    # Check for dangerous operations within the query
    # Note: sqlglot expression classes vary by version; keep this list conservative.
    dangerous_types = (exp.Delete, exp.Update, exp.Insert, exp.Drop, exp.Create, exp.Alter)
    for node in ast.walk():
        if isinstance(node, dangerous_types):
            result.add_error(
                code="DANGEROUS_OPERATION",
                message=f"Dangerous operation found: {type(node).__name__}",
                fix_hint="Remove any INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, or TRUNCATE operations.",
            )
            return result

    # Extract referenced tables
    tables = []
    alias_to_table: dict[str, str] = {}
    for table_node in ast.find_all(exp.Table):
        table_name = table_node.name
        if table_name:
            tables.append(table_name)
            result.referenced_tables.append(table_name)
            # Capture aliases so we can validate `alias.col` references.
            alias = table_node.args.get("alias")
            try:
                alias_name = alias.name if alias is not None else None
            except AttributeError:
                alias_name = None
            if alias_name:
                alias_to_table[alias_name] = table_name

    # Collect SELECT aliases (used for ORDER BY alias support)
    select_aliases: set[str] = set()
    try:
        select_node = ast if isinstance(ast, exp.Select) else ast.find(exp.Select)
    except AttributeError:
        select_node = None
    if select_node is not None:
        try:
            for e in (getattr(select_node, "expressions", None) or []):
                if isinstance(e, exp.Alias) and getattr(e, "alias", None):
                    select_aliases.add(str(e.alias).lower())
        except (AttributeError, TypeError):
            select_aliases = set()

    # Collect CTE (WITH ...) names and their projected output columns. A CTE reference
    # is not a base table and its projected columns are derived, so neither can be
    # validated against the physical schema. Example:
    #   WITH monthly_revenue AS (
    #     SELECT DATE_FORMAT(created_at, '%Y-%m') AS year_month, SUM(total_amount) AS total_revenue
    #     FROM orders GROUP BY year_month
    #   )
    #   SELECT year_month, total_revenue FROM monthly_revenue ORDER BY year_month
    # Here `monthly_revenue` (table) and `year_month`/`total_revenue` (columns) are all valid;
    # without this they were mis-flagged as Unknown table / Unknown column (false 422).
    cte_names: set[str] = set()
    cte_output_cols: set[str] = set()
    try:
        for cte in ast.find_all(exp.CTE):
            try:
                cte_name = cte.alias
            except AttributeError:
                cte_name = None
            if cte_name:
                cte_names.add(str(cte_name).lower())
            inner = getattr(cte, "this", None)
            inner_select = inner if isinstance(inner, exp.Select) else (inner.find(exp.Select) if inner is not None else None)
            if inner_select is not None:
                for e in (getattr(inner_select, "expressions", None) or []):
                    try:
                        nm = e.alias_or_name
                    except AttributeError:
                        nm = None
                    if nm and nm != "*":
                        cte_output_cols.add(str(nm).lower())
    except Exception:
        cte_names, cte_output_cols = set(), set()

    # Outer queries frequently alias a CTE, e.g. `FROM monthly_revenue tr ... SELECT tr.total_revenue`.
    # That alias resolves to a CTE (a derived relation), not a physical table, so columns qualified by
    # it must not be validated against the physical schema. `alias_to_table` already maps alias -> name.
    cte_alias_names: set[str] = {
        a.lower() for a, t in alias_to_table.items() if str(t).lower() in cte_names
    }

    # Column nodes inside ORDER BY (we only exempt aliases in ORDER BY, not elsewhere).
    order_by_col_ids: set[int] = set()
    try:
        order_node = select_node.args.get("order") if select_node is not None else None
    except AttributeError:
        order_node = None
    if order_node is not None:
        try:
            order_by_col_ids = {id(c) for c in order_node.find_all(exp.Column)}
        except AttributeError:
            order_by_col_ids = set()

    # Column nodes inside GROUP BY (some dialects allow GROUP BY alias, e.g. MySQL).
    group_by_col_ids: set[int] = set()
    try:
        group_node = select_node.args.get("group") if select_node is not None else None
    except AttributeError:
        group_node = None
    if group_node is not None:
        try:
            group_by_col_ids = {id(c) for c in group_node.find_all(exp.Column)}
        except AttributeError:
            group_by_col_ids = set()

    # Extract referenced columns
    columns = []
    for col_node in ast.find_all(exp.Column):
        col_name = col_node.name
        table_name = col_node.table
        # Allow ORDER BY references to SELECT aliases (common and valid in many dialects).
        # Example: SELECT SUM(x) AS total FROM t ORDER BY total DESC
        if (not table_name) and col_name and (str(col_name).lower() in select_aliases) and (id(col_node) in order_by_col_ids):
            continue
        # Allow GROUP BY references to SELECT aliases for MySQL/MariaDB.
        # Example: SELECT DATE_FORMAT(created_at, '%Y-%m-01') AS month_bucket FROM t GROUP BY month_bucket
        if (not table_name) and col_name and (str(col_name).lower() in select_aliases) and (id(col_node) in group_by_col_ids):
            try:
                d = (dialect or "").lower()
            except Exception:
                d = ""
            if "mysql" in d or "mariadb" in d:
                continue
        # Columns qualified by a CTE name (e.g. monthly_revenue.year_month) reference a
        # derived relation and can't be validated against the physical schema.
        if table_name and (str(table_name).lower() in cte_names
                           or str(table_name).lower() in cte_alias_names):
            continue
        # Unqualified references to a CTE-projected (derived) column are valid too.
        if (not table_name) and col_name and (str(col_name).lower() in cte_output_cols):
            continue
        if col_name and col_name != "*":
            if table_name:
                columns.append(f"{table_name}.{col_name}")
            else:
                columns.append(col_name)
            result.referenced_columns.append(columns[-1])

    # Layer 2.5: Placeholder literal detection (common LLM failure mode).
    # If we see '...' or "..." in string literals, force repair rather than returning non-executable SQL.
    for lit in ast.find_all(exp.Literal):
        try:
            is_str = bool(getattr(lit, "is_string", False))
        except Exception:
            is_str = False
        if not is_str:
            continue
        try:
            val = str(getattr(lit, "this", "") or "")
        except Exception:
            val = ""
        if "..." in val:
            result.add_error(
                code="PLACEHOLDER_LITERAL",
                message="Query contains placeholder literal '...'.",
                field="filters",
                fix_hint="Replace placeholders with concrete relative time filters (e.g., CURRENT_DATE - INTERVAL ...) or explicit dates.",
            )
            break

    # MySQL-specific: INTERVAL '30' DAY is invalid (number must not be quoted).
    try:
        d = (dialect or "").lower()
    except Exception:
        d = ""
    if "mysql" in d or "mariadb" in d:
        sql_text = ast.sql().lower()
        if re.search(r"\\binterval\\s+'\\d+'\\s+(day|month|year|hour|minute|second|week)\\b", sql_text):
            result.add_error(
                code="MYSQL_INTERVAL_QUOTED",
                message="MySQL INTERVAL value is quoted (e.g. INTERVAL '30' DAY).",
                field="filters",
                fix_hint="In MySQL, write INTERVAL 30 DAY (no quotes) or use DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY).",
            )

    # Layer 2.9: Scope-aware out-of-scope reference detection (structural; no schema needed).
    # A CTE/subquery-internal alias leaked into an outer query (e.g. a windowed top-N referencing
    # `p.price` in the final SELECT where `p` is only defined inside a CTE) passes flat alias
    # resolution but errors at execution (MySQL 1054 / PG "invalid reference"). Flag it so the
    # repair loop regenerates in-scope SQL instead of a query that fails at run time.
    for _oos_ref in _out_of_scope_column_refs(ast):
        result.add_error(
            code="OUT_OF_SCOPE_REFERENCE",
            message=f"Column references '{_oos_ref}' whose table/alias is not in scope of its query.",
            field="select_columns",
            fix_hint="Reference only tables/aliases available at the current query level. If the value comes from a CTE or subquery, select it from that derived relation by its projected column name.",
        )

    # Layer 3: Schema validation
    if schema:
        joined_tables = set([t for t in tables if t]) | set(alias_to_table.values())

        # Check tables exist
        for table in tables:
            # CTE names are valid relations, just not physical tables — don't flag them.
            if table and str(table).lower() in cte_names:
                continue
            if not schema.has_table(table):
                result.add_error(
                    code="UNKNOWN_TABLE",
                    message=f"Unknown table: {table}",
                    field="from_table",
                    fix_hint=f"Table '{table}' not found in schema. Available tables: {', '.join(schema.table_names[:10])}",
                )

        # Check columns exist (including unqualified columns).
        candidate_tables: list[str] = []
        if spec is not None:
            candidate_tables = spec.get_all_tables()
        # If no QuerySpec is provided (legacy SQL path), validate unqualified columns against
        # the actual tables present in the FROM/JOIN clause.
        if not candidate_tables:
            candidate_tables = list(joined_tables)

        for col_ref in columns:
            if "." in col_ref:
                table_name, col_name = col_ref.split(".", 1)
                table = schema.get_table(table_name)
                # If `table_name` is actually an alias, resolve to the underlying table.
                resolved_table_name = table_name
                if table is None and table_name in alias_to_table:
                    resolved_table_name = alias_to_table[table_name]
                    table = schema.get_table(resolved_table_name)

                # If the query references a table-qualified column for a table that isn't in FROM/JOIN,
                # MySQL/Postgres will error at execution time. Catch it here so the repair loop can add JOINs.
                if table_name not in alias_to_table and resolved_table_name not in joined_tables:
                    result.add_error(
                        code="UNJOINED_TABLE_REFERENCE",
                        message=f"Column references table '{resolved_table_name}' which is not present in FROM/JOIN.",
                        field="joins",
                        fix_hint=f"Add a JOIN for '{resolved_table_name}' (or remove its columns) so all referenced tables are included.",
                    )
                    continue

                if table is None:
                    result.add_error(
                        code="UNKNOWN_TABLE_REFERENCE",
                        message=f"Unknown table referenced in column: {table_name} (from {col_ref})",
                        field="select_columns",
                        fix_hint=f"Use one of the joined tables: {', '.join(schema.table_names[:10])}",
                    )
                elif not schema.has_column(resolved_table_name, col_name):
                    result.add_error(
                        code="UNKNOWN_COLUMN",
                        message=f"Unknown column: {col_ref}",
                        field="select_columns",
                        fix_hint=f"Column '{col_name}' not found in table '{resolved_table_name}'. Available columns: {', '.join(table.column_names[:10])}",
                    )
                continue

            # Unqualified column: try resolving against candidate tables.
            col_name = col_ref
            if candidate_tables:
                hits = [t for t in candidate_tables if schema.has_column(t, col_name)]
                if len(hits) == 1:
                    # resolved ok
                    continue
                if len(hits) == 0:
                    result.add_error(
                        code="UNKNOWN_COLUMN",
                        message=f"Unknown column: {col_name}",
                        field="select_columns",
                        fix_hint=f"Column '{col_name}' not found in any of: {', '.join(candidate_tables)}",
                    )
                else:
                    result.add_error(
                        code="AMBIGUOUS_COLUMN",
                        message=f"Ambiguous column '{col_name}' found in multiple tables: {', '.join(hits)}",
                        field="select_columns",
                        fix_hint="Qualify the column with its table name.",
                    )

        # Layer 3.5: Aggregation sanity checks using column types.
        # Catch SUM/AVG on non-numeric columns early (common source of nonsense results).
        agg_classes = []
        for cls_name in ("Sum", "Avg"):
            cls = getattr(exp, cls_name, None)
            if cls is not None:
                agg_classes.append(cls)
        if agg_classes:
            for agg_node in ast.find_all(tuple(agg_classes)):
                arg = getattr(agg_node, "this", None)
                if not isinstance(arg, exp.Column):
                    continue
                col_name = arg.name
                tbl = arg.table
                if not col_name or col_name == "*":
                    result.add_error(
                        code="INVALID_AGG_ARGUMENT",
                        message="Aggregation over '*' is not allowed for SUM/AVG.",
                        field="select_columns",
                        fix_hint="Use SUM/AVG over a numeric column, or use COUNT(*) for row counts.",
                    )
                    continue
                resolved_tbl = tbl
                if resolved_tbl in alias_to_table:
                    resolved_tbl = alias_to_table[resolved_tbl]
                if not resolved_tbl:
                    continue
                t = schema.get_table(resolved_tbl)
                if not t:
                    continue
                c = t.get_column(col_name)
                if c and not c.is_numeric():
                    result.add_error(
                        code="NON_NUMERIC_AGG",
                        message=f"{type(agg_node).__name__.upper()} used on non-numeric column '{resolved_tbl}.{col_name}' (type={c.type}).",
                        field="select_columns",
                        fix_hint="Use SUM/AVG only on numeric columns; use GROUP BY for categorical fields.",
                    )

    # Layer 4: LIMIT check
    limit_node = ast.find(exp.Limit)
    if limit_node:
        try:
            limit_value = int(limit_node.expression.this)
            if limit_value > max_limit:
                result.add_warning(
                    code="LIMIT_TOO_HIGH",
                    message=f"LIMIT {limit_value} exceeds maximum ({max_limit})",
                    fix_hint=f"Consider reducing LIMIT to {max_limit} or less.",
                )
        except (ValueError, AttributeError):
            pass
    elif require_limit:
        result.add_warning(
            code="NO_LIMIT",
            message="Query has no LIMIT clause",
            fix_hint="Add a LIMIT clause to prevent returning too many rows.",
        )

    # Layer 5: Semantic validation against QuerySpec
    if spec:
        # Check time bucketing if requested
        if spec.time_grain:
            if not _has_time_bucketing(ast, spec.time_grain, dialect):
                result.add_warning(
                    code="TIME_GRAIN_MISMATCH",
                    message=f"Requested time grain '{spec.time_grain.value}' may not be present in SQL",
                    field="time_grain",
                    fix_hint=f"Ensure the SQL groups by {spec.time_grain.value} buckets.",
                )

    return result


def _has_time_bucketing(ast: exp.Expression, grain: TimeGrain, dialect: str) -> bool:
    """
    Check if the AST contains appropriate time bucketing for the requested grain.

    This is a heuristic check - it looks for common time function patterns.
    """
    sqlglot_dialect = resolve_dialect(dialect)

    # Time function patterns to look for
    time_functions = {
        "postgres": ["date_trunc"],
        "mysql": ["date_format", "date", "year", "month", "quarter", "week", "yearweek"],
        "bigquery": ["timestamp_trunc", "date_trunc"],
        "snowflake": ["date_trunc"],
        "redshift": ["date_trunc"],
        "clickhouse": ["tostartoflhour", "todate", "tomonday", "tostartofmonth", "tostartofquarter", "tostartofyear"],
        "sqlite": ["strftime"],
    }

    expected_funcs = time_functions.get(sqlglot_dialect, ["date_trunc"])

    # Look for any of the expected time functions in the AST
    for func_node in ast.find_all(exp.Func):
        func_name = func_node.name.lower() if hasattr(func_node, 'name') else ""
        if func_name and func_name in expected_funcs:
            return True

    # Also check for common patterns
    sql_lower = ast.sql().lower()
    grain_patterns = {
        TimeGrain.HOUR: ["hour", "hh", "%h"],
        TimeGrain.DAY: ["day", "date(", "%y-%m-%d"],
        TimeGrain.WEEK: ["week", "yearweek", "%y-%u", "%y-w"],
        TimeGrain.MONTH: ["month", "'month'", "%y-%m", "yyyy-mm"],
        TimeGrain.QUARTER: ["quarter", "-q"],
        TimeGrain.YEAR: ["year", "%y"],
    }

    patterns = grain_patterns.get(grain, [])
    return any(p in sql_lower for p in patterns)


def extract_fix_hints(result: ValidationResult, spec: QuerySpec, schema: TypedSchema) -> list[str]:
    """
    Generate actionable fix hints from validation errors.

    These hints are designed to help the repair loop converge faster.
    """
    hints = []

    for error in result.errors:
        if error.fix_hint:
            hints.append(error.fix_hint)
        elif error.code == "UNKNOWN_TABLE":
            hints.append(f"Available tables: {', '.join(schema.table_names[:10])}")
        elif error.code == "UNKNOWN_COLUMN":
            # Try to find the table and list its columns
            if error.field and "." in str(error.field):
                table_name = str(error.field).split(".")[0]
                table = schema.get_table(table_name)
                if table:
                    hints.append(f"Columns in {table_name}: {', '.join(table.column_names[:10])}")

    # Add hints for temporal columns if time grain is requested
    if spec.time_grain and not spec.time_column:
        temporal_cols = []
        for table in schema.tables:
            for col in table.get_temporal_columns():
                temporal_cols.append(f"{table.name}.{col.name}")
        if temporal_cols:
            hints.append(f"Available time columns: {', '.join(temporal_cols[:5])}")

    return hints
