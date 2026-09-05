"""
Self-test for the value-level reconcile primitive.

Runs WITHOUT a live stack: it exercises ``reconcile_rows`` (the pure comparison
core) and ``canon_cell`` (the cross-engine canonicaliser) with synthetic raw
rows that mimic what psql/mysql emit, including the "nasty data" classes the
real fixtures will carry: NULLs, unicode, decimal-scale rendering differences,
negatives, empty strings, dropped rows, extra rows, and corrupted values.

Run:  python3 -m pytest e2e/_support/test_reconcile.py -q
  or:  python3 e2e/_support/test_reconcile.py   (built-in runner, no pytest)
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from reconcile import (  # noqa: E402
    NULL_SENTINEL,
    ReconcileResult,
    canon_cell,
    reconcile_rows,
)

COLUMNS = ["id", "name", "amount", "note"]
PK = [0]  # "id"


def _reconcile(src, dst, normalizers=None) -> ReconcileResult:
    return reconcile_rows(src, dst, COLUMNS, PK, normalizers)


# --------------------------------------------------------------------------- #
# canon_cell
# --------------------------------------------------------------------------- #


def test_canon_null_sentinel_to_none():
    assert canon_cell(NULL_SENTINEL) is None


def test_canon_decimal_trailing_zeros_equal():
    # MySQL renders DECIMAL(10,2) as "1.50"; Postgres NUMERIC as "1.5".
    assert canon_cell("1.50") == canon_cell("1.5") == "1.5"
    assert canon_cell("2.00") == "2"
    assert canon_cell("-3.140") == "-3.14"


def test_canon_integer_untouched():
    assert canon_cell("100") == "100"
    assert canon_cell("-7") == "-7"


def test_canon_unicode_preserved():
    assert canon_cell("café ☕ 北京") == "café ☕ 北京"


def test_canon_empty_string_is_not_null():
    # An empty string is a real value distinct from NULL.
    assert canon_cell("") == ""
    assert canon_cell("") is not None


# --------------------------------------------------------------------------- #
# reconcile_rows — happy path
# --------------------------------------------------------------------------- #


def test_identical_tables_reconcile():
    rows = [
        ["1", "alice", "10.00", "hi"],
        ["2", "bob", "20.5", NULL_SENTINEL],
    ]
    # Dest renders decimals with different trailing zeros but is otherwise identical.
    dst = [
        ["1", "alice", "10", "hi"],
        ["2", "bob", "20.50", NULL_SENTINEL],
    ]
    r = _reconcile(rows, dst)
    assert r.ok, r.reason()
    assert r.source_count == r.dest_count == 2


def test_unordered_input_still_reconciles_by_pk():
    src = [["1", "a", "1.0", "x"], ["2", "b", "2.0", "y"]]
    dst = [["2", "b", "2", "y"], ["1", "a", "1", "x"]]
    r = _reconcile(src, dst)
    assert r.ok, r.reason()


# --------------------------------------------------------------------------- #
# reconcile_rows — each bug class is caught
# --------------------------------------------------------------------------- #


def test_dropped_row_detected_as_missing_pk():
    src = [["1", "a", "1", "x"], ["2", "b", "2", "y"]]
    dst = [["1", "a", "1", "x"]]
    r = _reconcile(src, dst)
    assert not r.ok
    assert r.missing_pks == [("2",)]
    assert "missing in dest" in r.reason()


def test_duplicated_or_extra_row_detected():
    src = [["1", "a", "1", "x"]]
    dst = [["1", "a", "1", "x"], ["2", "ghost", "9", "z"]]
    r = _reconcile(src, dst)
    assert not r.ok
    assert r.extra_pks == [("2",)]


def test_corrupted_value_detected():
    src = [["1", "alice", "10", "hello"]]
    dst = [["1", "alice", "10", "h3llo"]]  # note column corrupted
    r = _reconcile(src, dst)
    assert not r.ok
    assert len(r.value_diffs) == 1
    d = r.value_diffs[0]
    assert d.column == "note" and d.source == "hello" and d.dest == "h3llo"


def test_null_vs_value_is_a_diff():
    src = [["1", "alice", "10", NULL_SENTINEL]]
    dst = [["1", "alice", "10", ""]]  # empty string is NOT null
    r = _reconcile(src, dst)
    assert not r.ok
    assert len(r.value_diffs) == 1
    assert r.value_diffs[0].source is None
    assert r.value_diffs[0].dest == ""


def test_decimal_scale_loss_detected():
    # Real corruption (not just rendering): source 10.25, dest truncated to 10.
    src = [["1", "alice", "10.25", "x"]]
    dst = [["1", "alice", "10", "x"]]
    r = _reconcile(src, dst)
    assert not r.ok
    assert r.value_diffs[0].column == "amount"


def test_count_match_but_wrong_rows():
    # Same count, but a re-keyed row: classic "looks fine by COUNT" bug.
    src = [["1", "a", "1", "x"], ["2", "b", "2", "y"]]
    dst = [["1", "a", "1", "x"], ["3", "b", "2", "y"]]
    r = _reconcile(src, dst)
    assert not r.ok
    assert r.missing_pks == [("2",)] and r.extra_pks == [("3",)]


def test_boolean_normalizer_unifies_engines():
    # Postgres bool 't'/'f' vs MySQL tinyint '1'/'0' — reconciled via normalizer.
    cols = ["id", "active"]
    src = [["1", "t"], ["2", "f"]]
    dst = [["1", "1"], ["2", "0"]]

    def boolnorm(v: str) -> str:
        return {"t": "1", "true": "1", "f": "0", "false": "0"}.get(v.lower(), v)

    r = reconcile_rows(src, dst, cols, [0], {"active": boolnorm})
    assert r.ok, r.reason()


# --------------------------------------------------------------------------- #
# Built-in runner (no pytest dependency)
# --------------------------------------------------------------------------- #


def _main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL  {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(_main())
