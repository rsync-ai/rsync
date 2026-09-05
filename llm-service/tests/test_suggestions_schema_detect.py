"""Tests for the deterministic schema-transform detection node (P3).

detect_schema_transforms_node makes NO LLM call and NO external request — it
maps column names/types to rename_columns / json_flatten / array_expand
suggestions. These tests pin that mapping so the suggestions modal keeps
surfacing the new transform types.
"""

import pytest

pytest.importorskip("langgraph")  # service module imports LangGraph

from src.agents.suggestions.service import (  # noqa: E402
    detect_schema_transforms_node,
    finalize_suggestions_node,
    _expand_column_name,
    _is_string_type,
    _recommend_type,
)


def _detect(columns):
    state = {"columns": columns, "schema_transform_suggestions": []}
    return detect_schema_transforms_node(state)["schema_transform_suggestions"]


def _by_type(suggestions):
    out = {}
    for s in suggestions:
        out.setdefault(s["type"], []).append(s)
    return out


def test_rename_columns_expands_abbreviations():
    s = _detect([
        {"name": "cust_nm", "type": "varchar"},
        {"name": "order_amt", "type": "numeric"},
        {"name": "plain_name", "type": "varchar"},
    ])
    renames = _by_type(s).get("rename_columns", [])
    assert len(renames) == 1, "expected a single aggregated rename suggestion"
    mappings = renames[0]["config"]["mappings"]
    assert mappings["cust_nm"] == "customer_name"
    assert mappings["order_amt"] == "order_amount"
    # A column with no abbreviation tokens must not appear in the mapping.
    assert "plain_name" not in mappings


def test_no_rename_when_nothing_to_expand():
    s = _detect([{"name": "customer_name", "type": "varchar"}])
    assert _by_type(s).get("rename_columns") is None


def test_json_flatten_by_type_and_by_name():
    s = _detect([
        {"name": "metadata", "type": "jsonb"},      # by type
        {"name": "event_payload", "type": "varchar"},  # by name suffix
    ])
    flats = _by_type(s).get("json_flatten", [])
    cols = {f["config"]["column"] for f in flats}
    assert cols == {"metadata", "event_payload"}
    for f in flats:
        assert f["config"]["max_depth"] == 2
        assert f["config"]["separator"] == "_"


def test_array_expand_by_type_and_by_name_and_is_wide_capped():
    s = _detect([
        {"name": "scores", "type": "integer[]"},   # by type suffix
        {"name": "tag_ids", "type": "varchar"},      # by name suffix
    ])
    arrs = _by_type(s).get("array_expand", [])
    cols = {a["config"]["column"] for a in arrs}
    assert cols == {"scores", "tag_ids"}
    for a in arrs:
        assert a["config"]["max_elements"] == 10  # wide-format cap


def test_array_precedence_over_json_for_ambiguous_ids_column():
    # tag_ids ends with _ids (array) — must NOT also produce json_flatten.
    s = _detect([{"name": "tag_ids", "type": "text[]"}])
    types = {x["type"] for x in s}
    assert "array_expand" in types
    assert "json_flatten" not in types


def test_finalize_merges_schema_and_llm_transforms_deduped():
    llm = [{"type": "filter", "config": {"condition": "x > 0"}}]
    schema = [{"type": "json_flatten", "config": {"column": "meta", "prefix": "meta_",
                                                  "separator": "_", "max_depth": 2}}]
    state = {
        "pii_columns": [],
        "transform_suggestions": llm,
        "schema_transform_suggestions": schema,
        "optimization_suggestions": [],
        "error": None,
    }
    final = finalize_suggestions_node(state)["final_suggestions"]
    kinds = [t["type"] for t in final["transforms"]]
    # Schema-driven transform appears first, LLM transform second.
    assert kinds == ["json_flatten", "filter"]


@pytest.mark.parametrize("raw,expected", [
    ("qty", "quantity"),
    ("cust_nm", "customer_name"),
    ("order_dt", "order_date"),
    ("already_clear", None),
    ("name", None),
])
def test_expand_column_name(raw, expected):
    assert _expand_column_name(raw) == expected


def test_type_convert_recommends_numeric_and_bool_for_string_columns():
    s = _detect([
        {"name": "price", "type": "varchar"},        # → float
        {"name": "quantity", "type": "text"},         # → int
        {"name": "is_active", "type": "varchar"},     # → bool
        {"name": "customer_name", "type": "varchar"},   # → none (stays string)
        {"name": "order_id", "type": "varchar"},        # → none (*_id excluded)
        {"name": "total_cents", "type": "integer"},     # → none (already numeric)
    ])
    conv = {c["config"]["column"]: c["config"]["to"] for c in _by_type(s).get("type_convert", [])}
    assert conv.get("price") == "float"
    assert conv.get("quantity") == "int"
    assert conv.get("is_active") == "bool"
    assert "customer_name" not in conv
    assert "order_id" not in conv
    assert "total_cents" not in conv
    for c in _by_type(s).get("type_convert", []):
        assert c["config"]["on_error"] == "null"


@pytest.mark.parametrize("declared,expected", [
    ("varchar", True), ("varchar(255)", True), ("text", True),
    ("character varying", True), ("citext", True),
    ("integer", False), ("numeric", False), ("boolean", False), ("", False),
])
def test_is_string_type(declared, expected):
    assert _is_string_type(declared) is expected


@pytest.mark.parametrize("name,expected", [
    ("price", "float"), ("unit_amount", "float"),
    ("quantity", "int"), ("line_count", "int"),
    ("is_active", "bool"), ("has_shipped", "bool"), ("deleted", "bool"),
    ("name", None), ("description", None), ("order_id", None), ("id", None),
])
def test_recommend_type(name, expected):
    assert _recommend_type(name) == expected
