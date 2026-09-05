"""
QuerySpec normalization and validation helpers.

These helpers:
- enforce defaults (LIMIT, SELECT *)
- derive missing fields from typed schema (time_column)
- surface HITL needs based on ambiguity confidence or schema mismatches
"""

from __future__ import annotations

import re
from typing import Optional

from .schema import TypedSchema
from .query_spec import QuerySpec, SelectColumn, TimeGrain, AggregationType


def normalize_query_spec(spec: QuerySpec, schema: Optional[TypedSchema]) -> QuerySpec:
    """
    Normalize a QuerySpec so compilation/validation becomes deterministic.

    This function does not attempt to "fix intent"; it only:
    - fills safe defaults
    - picks time_column when time_grain is present
    - marks requires_hitl when ambiguity is too high or schema references are invalid
    """

    # Default limit for safety
    if spec.limit is None:
        spec.limit = 100

    # Normalize qualified table names like "public.orders" → "orders"
    if spec.from_table and "." in spec.from_table:
        spec.from_table = spec.from_table.split(".")[-1].strip()
    for j in spec.joins or []:
        if j.table and "." in j.table:
            j.table = j.table.split(".")[-1].strip()
    for c in spec.select_columns or []:
        if getattr(c, "table", None) and "." in c.table:
            c.table = c.table.split(".")[-1].strip()
    for f in spec.filters or []:
        if getattr(f, "table", None) and "." in f.table:
            f.table = f.table.split(".")[-1].strip()
    for ob in spec.order_by or []:
        if getattr(ob, "table", None) and "." in ob.table:
            ob.table = ob.table.split(".")[-1].strip()

    # Infer missing aliases when the model uses alias-qualified columns in JOIN conditions.
    # Example: JOIN customers AS c ON o.customer_id = c.id  (but from_table_alias omitted)
    if not spec.from_table_alias and spec.joins:
        table_names = {spec.from_table} | {j.table for j in (spec.joins or []) if j.table}
        join_aliases = {j.alias for j in (spec.joins or []) if j.alias}
        # Find qualifiers like "o." or "c." in join predicates.
        qual_re = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.")
        used_quals: set[str] = set()
        for j in spec.joins or []:
            for cond in (j.on_conditions or []):
                for m in qual_re.finditer(str(cond or "")):
                    used_quals.add(m.group(1))
        # Candidate aliases are qualifiers not equal to a table name and not already a JOIN alias.
        candidates = [q for q in used_quals if q not in table_names and q not in join_aliases and q.lower() not in ("public", "dbo")]
        # If exactly one short candidate exists, assume it's the FROM table alias.
        if len(candidates) == 1 and 1 <= len(candidates[0]) <= 2:
            spec.from_table_alias = candidates[0]

    # Default select
    if not spec.select_columns:
        spec.select_columns = [SelectColumn(column="*")]

    # If time grain is requested but time_column is missing, pick a typed temporal column.
    if spec.time_grain and not spec.time_column and schema is not None:
        time_col = pick_time_column(schema, spec.from_table)
        if time_col:
            spec.time_column = time_col
    # If time_column is present but invalid (or looks like an expression), override with a real temporal column.
    if spec.time_grain and schema is not None and spec.time_column:
        t = schema.get_table(spec.from_table)
        if t is not None:
            time_col_names = {c.name for c in (t.columns or [])}
            if spec.time_column not in time_col_names:
                time_col = pick_time_column(schema, spec.from_table)
                if time_col:
                    spec.time_column = time_col

    # If time bucketing is present, drop duplicate time-bucket expressions from select_columns
    # (compiler will generate the bucket deterministically).
    if spec.time_grain and spec.time_column and spec.select_columns:
        bucket_markers = ("date_trunc", "date_format", "extract(", "month(", "year(", "quarter(", "week(", "day(", "date(")
        cleaned = []
        for c in spec.select_columns:
            col_s = (c.column or "").strip()
            if c.aggregation is None and col_s and spec.time_column.lower() in col_s.lower():
                if any(m in col_s.lower() for m in bucket_markers):
                    continue
            cleaned.append(c)
        spec.select_columns = cleaned

    # Heuristic: if the model is trying to SUM/AVG a time-extraction expression, replace it with a numeric metric.
    if schema is not None and spec.select_columns:
        metric = pick_numeric_metric_column(schema, spec.from_table)
        if metric:
            for c in spec.select_columns:
                if c.aggregation in (AggregationType.SUM, AggregationType.AVG):
                    col_s = (c.column or "").strip().lower()
                    if not col_s:
                        continue
                    if any(m in col_s for m in ("date_trunc", "date_format", "extract(", "month(", "year(", "quarter(", "week(", "day(", "date(")):
                        c.column = metric
                        c.table = spec.from_table

    # If ambiguities include low confidence, require HITL
    if spec.has_ambiguities(threshold=0.7):
        spec.requires_hitl = True
        if not spec.hitl_reason:
            spec.hitl_reason = "Ambiguous request (requires confirmation)"

    # Schema sanity checks (shallow)
    if schema is not None:
        if not schema.has_table(spec.from_table):
            spec.requires_hitl = True
            spec.hitl_reason = f"Unknown table: {spec.from_table}"

    return spec


def coerce_query_spec_payload(raw: dict) -> dict:
    """
    Best-effort coercion of model JSON into the QuerySpec schema.

    Offline models sometimes return:
    - group_by as objects like {column: "...", direction: ...}
    - select_columns as strings instead of objects
    - missing lists (null)
    This function normalizes these common deviations before pydantic validation.
    """
    if not isinstance(raw, dict):
        return {}

    # Treat empty strings for optional fields as "unset" (models often emit "" instead of null).
    for key in (
        "intent_description",
        "from_table_alias",
        "time_column",
        "time_grain",
        "preferred_join_path",
        "hitl_reason",
    ):
        v = raw.get(key)
        if isinstance(v, str) and not v.strip():
            raw.pop(key, None)

    # Ensure list fields are lists
    for key in ("select_columns", "joins", "filters", "group_by", "order_by", "ambiguities"):
        if raw.get(key) is None:
            raw[key] = []

    # group_by: accept list[str] or list[dict]
    gb = raw.get("group_by")
    if isinstance(gb, list):
        new_gb: list[str] = []
        for item in gb:
            if isinstance(item, str):
                if item.strip():
                    new_gb.append(item.strip())
                continue
            if isinstance(item, dict):
                col = item.get("column") or item.get("expr") or item.get("expression")
                table = item.get("table")
                if isinstance(col, str) and col.strip():
                    col_s = col.strip()
                    if table and isinstance(table, str) and "." not in col_s:
                        col_s = f"{table.strip()}.{col_s}"
                    new_gb.append(col_s)
        raw["group_by"] = new_gb

    # select_columns: accept list[str] and coerce to objects
    sc = raw.get("select_columns")
    if isinstance(sc, list):
        new_sc = []
        for item in sc:
            if isinstance(item, str):
                new_sc.append({"column": item.strip()})
            elif isinstance(item, dict):
                # Some models incorrectly nest a SelectColumn object inside the `column` field:
                #   {"column": {"column":"amount_cents","table":"orders","aggregation":"sum"}}
                col_field = item.get("column")
                if isinstance(col_field, dict):
                    merged = dict(item)
                    merged.update(col_field)
                    # Ensure `column` becomes a string
                    merged["column"] = str(col_field.get("column") or "").strip() or str(item.get("column") or "").strip()
                    item = merged
                elif isinstance(col_field, list) and col_field:
                    # If column comes as list, take first string-ish.
                    item["column"] = str(col_field[0]).strip()
                # Normalize enum casing
                agg = item.get("aggregation")
                if isinstance(agg, str):
                    agg_s = agg.strip().lower()
                    if not agg_s:
                        item.pop("aggregation", None)
                    else:
                        item["aggregation"] = agg_s
                # Defensive: ensure column is a string
                if not isinstance(item.get("column"), str):
                    item["column"] = str(item.get("column") or "").strip()
                # Drop empty optional strings
                for k in ("table", "alias"):
                    vv = item.get(k)
                    if isinstance(vv, str) and not vv.strip():
                        item.pop(k, None)
                new_sc.append(item)
        raw["select_columns"] = new_sc

    # filters: normalize operator casing
    flt = raw.get("filters")
    if isinstance(flt, list):
        new_f = []
        for item in flt:
            if isinstance(item, dict):
                op = item.get("operator")
                if isinstance(op, str):
                    op_s = op.strip().lower()
                    if not op_s:
                        item.pop("operator", None)
                    else:
                        item["operator"] = op_s
                lc = item.get("logical_connector")
                if isinstance(lc, str):
                    lc_s = lc.strip().lower()
                    if not lc_s:
                        item.pop("logical_connector", None)
                    else:
                        item["logical_connector"] = lc_s
                for k in ("table", "column"):
                    vv = item.get(k)
                    if isinstance(vv, str) and not vv.strip():
                        item.pop(k, None)
                new_f.append(item)
        raw["filters"] = new_f

    # joins: normalize join_type casing
    joins = raw.get("joins")
    if isinstance(joins, list):
        new_j = []
        for item in joins:
            if isinstance(item, dict):
                jt = item.get("join_type")
                if isinstance(jt, str):
                    jt_s = jt.strip().lower()
                    if not jt_s:
                        item.pop("join_type", None)
                    else:
                        item["join_type"] = jt_s
                for k in ("table", "alias"):
                    vv = item.get(k)
                    if isinstance(vv, str) and not vv.strip():
                        item.pop(k, None)
                new_j.append(item)
        raw["joins"] = new_j

    # order_by: normalize direction casing and default when null
    ob = raw.get("order_by")
    if isinstance(ob, list):
        new_ob = []
        for item in ob:
            if isinstance(item, dict):
                d = item.get("direction")
                if d is None:
                    item["direction"] = "asc"
                elif isinstance(d, str):
                    d_s = d.strip().lower()
                    item["direction"] = d_s or "asc"
                for k in ("table", "column"):
                    vv = item.get(k)
                    if isinstance(vv, str) and not vv.strip():
                        item.pop(k, None)
                new_ob.append(item)
        raw["order_by"] = new_ob

    # time_grain: normalize casing
    tg = raw.get("time_grain")
    if isinstance(tg, str):
        tg_s = tg.strip().lower()
        if not tg_s:
            raw.pop("time_grain", None)
        else:
            raw["time_grain"] = tg_s

    # preferred_join_path: sometimes returned as list; coerce to string.
    pjp = raw.get("preferred_join_path")
    if isinstance(pjp, list) and pjp:
        raw["preferred_join_path"] = str(pjp[0]).strip()
    elif pjp is not None and not isinstance(pjp, str):
        raw["preferred_join_path"] = str(pjp).strip()

    return raw


def pick_time_column(schema: TypedSchema, from_table: str) -> str:
    """
    Pick a reasonable time column for a given table using typed schema.

    Priority:
    - typed temporal columns on the from_table
    - common names (created_at, updated_at, ...)
    - any temporal-looking column in the whole schema (fallback)
    """
    table = schema.get_table(from_table) if schema else None
    if table:
        temporal = table.get_temporal_columns()
        if temporal:
            # Prefer created_at/updated_at if present
            for name in ("created_at", "updated_at"):
                c = table.get_column(name)
                if c and c.is_temporal():
                    return c.name
            return temporal[0].name

    # Fallback across schema
    for t in schema.tables:
        temporal = t.get_temporal_columns()
        if temporal:
            return temporal[0].name
    return ""


def pick_numeric_metric_column(schema: TypedSchema, from_table: str) -> str:
    """
    Pick a likely numeric metric column (for SUM/AVG) from a table.
    """
    t = schema.get_table(from_table)
    if t is None:
        return ""
    numeric = [c.name for c in (t.columns or []) if getattr(c, "is_numeric", lambda: False)()]
    if not numeric:
        return ""
    preferred = ("amount", "revenue", "total", "price", "cost", "cents", "value", "spend")
    for p in preferred:
        for n in numeric:
            if p in n.lower():
                return n
    return numeric[0]

