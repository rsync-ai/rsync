"""Unit tests for GraphQL multi-page cursor pagination (Shopify pilot).

Covers the regression that truncated 251–9,999-row tables to their first 250
rows: the regular export path now loops every Relay page inside one ``export()``
call via ``GraphQLPaginationHandler.fetch_all_pages``.

Run from this directory:
    python3 -m pytest test_pagination.py -q
or standalone:
    python3 test_pagination.py
"""
import os
import sys

# The container builds from this version dir; both base_connector and connector
# live alongside this file. Make them importable regardless of cwd.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from base_connector import GraphQLPaginationHandler  # noqa: E402
from connector import _normalize_to_rows, _max_updated_at  # noqa: E402


def _page(node_list, has_next, end_cursor):
    """Build a Shopify-shaped GraphQL ``data`` page for one connection slice."""
    return {
        "products": {
            "edges": [{"node": n} for n in node_list],
            "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
        }
    }


def _node(i):
    return {"id": f"gid://shopify/Product/{i}", "title": f"P{i}"}


def _fake_pages(pages):
    """Return an ``execute_page_fn`` that yields ``pages`` in order and records
    the ``variables`` it was called with (so tests can assert first/after)."""
    calls = []

    def execute_page_fn(variables):
        calls.append(dict(variables))
        idx = len(calls) - 1
        if idx >= len(pages):
            # Beyond defined pages — emulate an exhausted connection.
            return _page([], False, None)
        return pages[idx]

    execute_page_fn.calls = calls
    return execute_page_fn


# ---------------------------------------------------------------------------
# 1. Three full pages then exhaustion -> all rows, has_more False.
# ---------------------------------------------------------------------------
def test_three_pages_accumulate_and_exhaust():
    pages = [
        _page([_node(i) for i in range(0, 250)], True, "c1"),
        _page([_node(i) for i in range(250, 500)], True, "c2"),
        _page([_node(i) for i in range(500, 750)], False, None),
    ]
    fn = _fake_pages(pages)
    rows, cursor, has_more = GraphQLPaginationHandler().fetch_all_pages(
        fn, {}, page_size=250, row_extractor=_normalize_to_rows
    )
    assert len(rows) == 750, f"expected 750 rows, got {len(rows)}"
    assert has_more is False
    assert cursor is None
    assert len(fn.calls) == 3
    # Page 1 must NOT carry an after cursor; pages 2/3 must chain c1, c2.
    assert "after" not in fn.calls[0]
    assert fn.calls[1]["after"] == "c1"
    assert fn.calls[2]["after"] == "c2"


# ---------------------------------------------------------------------------
# 2. Record cap stops early and reports a resumable cursor.
# ---------------------------------------------------------------------------
def test_max_records_cap_reports_resumable():
    # Endless connection: every page is full and claims more.
    pages = [_page([_node(i) for i in range(p * 250, p * 250 + 250)], True, f"c{p}")
             for p in range(10)]
    fn = _fake_pages(pages)
    rows, cursor, has_more = GraphQLPaginationHandler().fetch_all_pages(
        fn, {}, page_size=250, max_records=500, row_extractor=_normalize_to_rows
    )
    assert len(rows) == 500, f"expected cap at 500, got {len(rows)}"
    assert has_more is True
    assert cursor == "c1"  # end cursor of the 2nd (capping) page


# ---------------------------------------------------------------------------
# 3. Cycle guard: a repeating endCursor must not loop forever.
# ---------------------------------------------------------------------------
def test_cycle_guard_on_repeating_cursor():
    pages = [
        _page([_node(0)], True, "same"),
        _page([_node(1)], True, "same"),  # repeats -> guard trips
        _page([_node(2)], True, "same"),  # should never be reached
    ]
    fn = _fake_pages(pages)
    rows, cursor, has_more = GraphQLPaginationHandler().fetch_all_pages(
        fn, {}, page_size=250, row_extractor=_normalize_to_rows
    )
    # Page 1 accepted (cursor 'same' recorded), page 2 fetched then guard trips.
    assert len(fn.calls) == 2, f"expected 2 calls before guard, got {len(fn.calls)}"
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# 4. Ascending updatedAt -> watermark reaches the TAIL (last page's max),
#    proving the truncation/tail-miss is fixed once all pages are accumulated.
# ---------------------------------------------------------------------------
def test_watermark_reaches_tail_when_ascending():
    def stamped(i):
        # Ascending timestamps: 2024-01-01T00:00:00Z .. increasing by index.
        return {"id": f"gid://shopify/Order/{i}",
                "updatedAt": f"2024-01-01T00:{i:02d}:00Z"}

    pages = [
        _page([stamped(i) for i in range(0, 3)], True, "c1"),
        _page([stamped(i) for i in range(3, 6)], False, None),
    ]
    fn = _fake_pages(pages)
    rows, _cursor, _has_more = GraphQLPaginationHandler().fetch_all_pages(
        fn, {}, page_size=3, row_extractor=_normalize_to_rows
    )
    assert len(rows) == 6
    running = _max_updated_at(rows, None)
    assert running is not None
    # Newest row is the LAST one (index 5) — the tail, not the first-page max.
    assert running[0] == "2024-01-01T00:05:00Z", running[0]


# ---------------------------------------------------------------------------
# 5. start_after seeds the first page's ``after`` cursor (resume path).
# ---------------------------------------------------------------------------
def test_start_after_seeds_first_page_cursor():
    pages = [_page([_node(0)], False, None)]
    fn = _fake_pages(pages)
    GraphQLPaginationHandler().fetch_all_pages(
        fn, {}, page_size=250, start_after="RESUME_CURSOR",
        row_extractor=_normalize_to_rows,
    )
    assert fn.calls[0]["after"] == "RESUME_CURSOR"
    assert fn.calls[0]["first"] == 250


# ---------------------------------------------------------------------------
# 6. Missing row_extractor is a programming error -> ValueError.
# ---------------------------------------------------------------------------
def test_missing_row_extractor_raises():
    try:
        GraphQLPaginationHandler().fetch_all_pages(lambda v: {}, {})
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError without row_extractor")


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"FAIL {name}: {exc!r}")
    sys.exit(1 if failures else 0)
