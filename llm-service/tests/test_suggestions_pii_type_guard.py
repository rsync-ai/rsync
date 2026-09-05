"""Tests for the boolean type-guard in detect_pii_node.

A boolean source column (e.g. Shopify `verifiedEmail`) must NOT be classified as PII:
masking it yields a string hash that cannot be inserted into the BOOLEAN destination
column, silently dropping the whole table. Numeric PII (ssn/salary) must stay maskable —
the sink reconciles masked columns to TEXT at the destination.
"""

import pytest

pytest.importorskip("langgraph")  # service module imports LangGraph

from src.agents.suggestions.service import (  # noqa: E402
    detect_pii_node,
    _is_boolean_column_type,
)


def _pii(columns, monkeypatch):
    # Heuristics-only: deterministic, no Presidio/ML dependency.
    monkeypatch.setenv("SUGGESTIONS_PII_USE_ML", "false")
    return detect_pii_node({"columns": columns})["pii_columns"]


def _cols(pii):
    return {p["column"] for p in pii}


def test_boolean_verified_email_not_pii(monkeypatch):
    pii = _pii([{"name": "verifiedEmail", "type": "Boolean"}], monkeypatch)
    assert _cols(pii) == set(), f"boolean verifiedEmail must not be flagged PII, got {pii}"


def test_string_email_still_pii(monkeypatch):
    pii = _pii([{"name": "defaultEmailAddress", "type": "varchar"}], monkeypatch)
    assert "defaultEmailAddress" in _cols(pii)


def test_numeric_pii_stays_maskable(monkeypatch):
    pii = _pii(
        [
            {"name": "ssn", "type": "integer"},
            {"name": "salary", "type": "numeric"},
        ],
        monkeypatch,
    )
    cols = _cols(pii)
    assert "ssn" in cols and "salary" in cols, f"numeric PII must stay maskable, got {cols}"


def test_is_boolean_column_type():
    for t in ["boolean", "Boolean", "BOOL", "bool", "bit", "tinyint(1)"]:
        assert _is_boolean_column_type(t), t
    for t in ["varchar", "text", "integer", "numeric", "tinyint", "", None]:
        assert not _is_boolean_column_type(t), t
