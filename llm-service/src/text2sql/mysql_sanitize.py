"""
MySQL SQL sanitizers for execution safety.

These fix common model-generation pitfalls that cause runtime errors under typical
MySQL configurations (notably sql_mode=only_full_group_by).
"""

from __future__ import annotations

import re
from typing import Iterable

import sqlglot
from sqlglot import exp


_MYSQL_BUILTIN_FUNCS = [
    "DATE_FORMAT",
    "DATE",
    "YEAR",
    "MONTH",
    "QUARTER",
    "WEEK",
    "YEARWEEK",
    "STR_TO_DATE",
    "TIMESTAMPDIFF",
    "DATEDIFF",
]


def unqualify_builtin_functions(sql: str, qualifiers: Iterable[str]) -> str:
    """
    MySQL quirk: `schema_or_table.DATE_FORMAT(...)` is interpreted as calling a stored routine
    `DATE_FORMAT` under schema `schema_or_table`, requiring EXECUTE privileges.

    This strips schema/table qualification for common built-in functions, but only when the
    qualifier is one of the provided table/schema identifiers.
    """
    if not sql:
        return sql
    qs = {q.strip() for q in qualifiers or [] if str(q or "").strip()}
    if not qs:
        return sql
    out = sql
    for q in qs:
        q_esc = re.escape(q)
        for f in _MYSQL_BUILTIN_FUNCS:
            f_esc = re.escape(f)
            pat = rf"`?{q_esc}`?\s*\.\s*`?{f_esc}`?\s*\("
            out = re.sub(pat, f"{f}(", out, flags=re.IGNORECASE)
    return out


def fix_only_full_group_by_order(sql: str) -> str:
    """
    Fix MySQL Error 1055 / only_full_group_by issues caused by ORDER BY including expressions
    that are not grouped/aggregated.

    Strategy:
    - If query has GROUP BY and ORDER BY:
      - keep only ORDER BY terms that are clearly group-safe (positional references like ORDER BY 1,
        or expressions that match a GROUP BY expression)
      - if none are safe, replace ORDER BY with ORDER BY 1 (stable + commonly desired)
    """
    if not sql or not sql.strip():
        return sql
    try:
        tree = sqlglot.parse_one(sql, dialect="mysql")
    except Exception:
        return sql

    # Find the SELECT to operate on (handle WITH ... SELECT).
    select = tree.find(exp.Select)
    if select is None:
        return sql

    group = select.args.get("group")
    order = select.args.get("order")
    if not group or not order:
        return sql

    group_exprs = list(group.expressions or [])
    if not group_exprs:
        return sql

    # Prepare group expression signatures for comparison.
    group_sql = {g.sql(dialect="mysql") for g in group_exprs}

    # SELECT-list aliases whose value is an aggregate (e.g. `SUM(...) AS total_revenue`).
    # ORDER BY on an aggregate — or on an alias bound to one — is VALID under only_full_group_by
    # (MySQL permits ordering by aggregated values). Without this, a "top N by <measure>" query
    # like `ORDER BY total_revenue DESC` was wrongly judged unsafe and clobbered to `ORDER BY 1`,
    # silently turning a ranking into a name-sorted list.
    aggregate_aliases: set[str] = set()
    for col in (select.expressions or []):
        name = col.alias_or_name
        if not name:
            continue
        underlying = col.this if isinstance(col, exp.Alias) else col
        try:
            if underlying.find(exp.AggFunc) is not None:
                aggregate_aliases.add(name)
        except Exception:
            pass

    safe_orders: list[exp.Ordered] = []
    for o in order.expressions or []:
        if not isinstance(o, exp.Ordered):
            continue
        expr = o.this
        if isinstance(expr, exp.Literal) and str(expr.this).isdigit():
            idx = int(expr.this)
            if idx >= 1 and idx <= len(select.expressions or []):
                # Positional ORDER BY is safe for only_full_group_by.
                safe_orders.append(o)
                continue
        # ORDER BY directly on an aggregate expression (e.g. SUM(...)) is safe — preserves direction.
        try:
            if expr.find(exp.AggFunc) is not None:
                safe_orders.append(o)
                continue
        except Exception:
            pass
        # ORDER BY referencing a SELECT alias bound to an aggregate (e.g. total_revenue) is safe.
        if isinstance(expr, exp.Column) and not expr.table and expr.name in aggregate_aliases:
            safe_orders.append(o)
            continue
        # If the order expression is exactly one of the GROUP BY expressions, it is safe.
        try:
            if expr.sql(dialect="mysql") in group_sql:
                safe_orders.append(o)
                continue
        except Exception:
            pass

    if safe_orders:
        select.set("order", exp.Order(expressions=safe_orders[:1]))
    else:
        # Stable fallback: ORDER BY 1
        select.set("order", exp.Order(expressions=[exp.Ordered(this=exp.Literal.number(1), desc=False)]))

    return tree.sql(dialect="mysql", pretty=False)


def quote_reserved_identifiers(sql: str) -> str:
    """
    Backtick-quote identifiers that collide with MySQL reserved keywords.

    The model frequently aliases expressions to names that are MySQL reserved words
    (e.g. ``DATE_FORMAT(...) AS year_month`` — ``YEAR_MONTH`` is reserved), which is a
    syntax error (Error 1064) when emitted unquoted. This is a very common failure mode
    for time-bucket queries ("revenue per month"). sqlglot's MySQL generator auto-quotes
    reserved identifiers on render, so a parse → render round-trip fixes this transparently
    while leaving non-reserved identifiers (table names, ordinary aliases) untouched.

    Returns the input unchanged if it cannot be parsed (never raises).
    """
    if not sql or not sql.strip():
        return sql
    try:
        tree = sqlglot.parse_one(sql, dialect="mysql")
        if tree is None:
            return sql
        return tree.sql(dialect="mysql", pretty=False)
    except Exception:
        return sql

