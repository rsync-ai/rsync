#!/usr/bin/env python3
"""Unit tests for bulk_operations.py.

Pure-function tests with mocked transport. Run inline:

    python3 -m unittest discover -s <this dir> -p 'test_*.py' -v

or directly:

    python3 test_bulk_operations.py
"""

from __future__ import annotations

import os
import sys
import unittest
from typing import Any, Dict, List, Optional, Tuple

# Ensure the directory is on sys.path so `import bulk_operations` resolves
# when this test file is invoked from any cwd.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import bulk_operations as bo  # noqa: E402


# ---------------------------------------------------------------------------
# Fake transports
# ---------------------------------------------------------------------------


class FakeGraphQL:
    """Scripted post_graphql callable.

    Each call consumes one entry from ``script`` in order. Entries are
    either:
      - a dict to return as the GraphQL data payload, OR
      - a callable (query, variables) -> dict, OR
      - an Exception to raise.
    """

    def __init__(self, script: List[Any]) -> None:
        self.script = list(script)
        self.calls: List[Tuple[str, Optional[Dict[str, Any]]]] = []

    def __call__(self, query: str, variables: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        self.calls.append((query, variables))
        if not self.script:
            raise AssertionError("FakeGraphQL ran out of scripted responses")
        nxt = self.script.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        if callable(nxt):
            return nxt(query, variables)
        return nxt


class FakeDownloader:
    def __init__(self, status: int, body: str, exc: Optional[Exception] = None) -> None:
        self.status = status
        self.body = body
        self.exc = exc
        self.calls: List[Tuple[str, int]] = []

    def __call__(self, url: str, timeout: int) -> Tuple[int, str]:
        self.calls.append((url, timeout))
        if self.exc is not None:
            raise self.exc
        return self.status, self.body


def _no_sleep(_secs: float) -> None:
    pass


def _fake_clock_factory(start: float = 1000.0, step: float = 1.0):
    """Returns (clock_fn, advance). clock_fn() returns monotonically increasing."""
    state = {"t": start}

    def clock() -> float:
        return state["t"]

    def advance(by: float) -> None:
        state["t"] += by

    return clock, advance


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------


# Standard inner query for the per-resource Shopify operations. We reuse
# the orders query shape (matches the prod connector verbatim).
ORDERS_QUERY = (
    "query GetOrders($first: Int, $after: String, $query: String) {\n"
    "  orders(first: $first, after: $after, query: $query) {\n"
    "    edges { node { id name createdAt } cursor }\n"
    "    pageInfo { hasNextPage endCursor }\n"
    "  }\n"
    "}"
)


class TestStripQueryForBulkOps(unittest.TestCase):
    def test_strips_outer_args_and_header(self) -> None:
        stripped = bo.strip_query_for_bulk_ops(ORDERS_QUERY)
        # No more `$first` variable header.
        self.assertNotIn("$first", stripped)
        # The top-level `orders(...)` call must have lost its arg list.
        self.assertIn("orders {", stripped)
        # But the inner structure stays.
        self.assertIn("edges", stripped)
        self.assertIn("node", stripped)

    def test_strips_nested_connection_args(self) -> None:
        q = (
            "query GetProducts($first: Int) {\n"
            "  products(first: $first) {\n"
            "    edges { node { id variants(first: 5) { edges { node { id } } } } }\n"
            "  }\n"
            "}"
        )
        stripped = bo.strip_query_for_bulk_ops(q)
        # Both top-level and nested connection args must be stripped.
        self.assertIn("products {", stripped)
        self.assertIn("variants {", stripped)
        self.assertNotIn("first: $first", stripped)
        self.assertNotIn("first: 5", stripped)

    def test_leaves_non_paginator_args_alone(self) -> None:
        # quantities(names: ["available"]) is a real arg list, not pagination.
        q = (
            "query Q {\n"
            "  inventoryItems {\n"
            "    edges { node { quantities(names: [\"available\"]) { name quantity } } }\n"
            "  }\n"
            "}"
        )
        stripped = bo.strip_query_for_bulk_ops(q)
        self.assertIn("quantities(names: [\"available\"])", stripped)


class TestParseJsonl(unittest.TestCase):
    def test_flat_records(self) -> None:
        body = (
            '{"id":"gid://shopify/Order/1","name":"#1001"}\n'
            '{"id":"gid://shopify/Order/2","name":"#1002"}\n'
            '{"id":"gid://shopify/Order/3","name":"#1003"}\n'
        )
        rows = bo.parse_jsonl(body)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["name"], "#1001")

    def test_nested_parent_id_reassembly(self) -> None:
        body = (
            '{"id":"gid://shopify/Order/1","name":"#1001"}\n'
            '{"id":"gid://shopify/LineItem/10","title":"A","__parentId":"gid://shopify/Order/1"}\n'
            '{"id":"gid://shopify/LineItem/11","title":"B","__parentId":"gid://shopify/Order/1"}\n'
            '{"id":"gid://shopify/Order/2","name":"#1002"}\n'
        )
        rows = bo.parse_jsonl(body)
        self.assertEqual(len(rows), 2)
        order1 = next(r for r in rows if r["id"] == "gid://shopify/Order/1")
        self.assertEqual(len(order1["__children"]), 2)
        order2 = next(r for r in rows if r["id"] == "gid://shopify/Order/2")
        self.assertNotIn("__children", order2)

    def test_empty_body(self) -> None:
        self.assertEqual(bo.parse_jsonl(""), [])
        self.assertEqual(bo.parse_jsonl("\n\n"), [])

    def test_malformed_lines_skipped(self) -> None:
        body = (
            '{"id":"gid://shopify/Order/1"}\n'
            'this is not json\n'
            '{"id":"gid://shopify/Order/2"}\n'
        )
        rows = bo.parse_jsonl(body)
        self.assertEqual(len(rows), 2)


def _make_completed_submit() -> Dict[str, Any]:
    return {
        "bulkOperationRunQuery": {
            "bulkOperation": {"id": "gid://shopify/BulkOperation/123", "status": "CREATED"},
            "userErrors": [],
        }
    }


def _make_poll(status: str, **kwargs: Any) -> Dict[str, Any]:
    op = {"id": "gid://shopify/BulkOperation/123", "status": status, "errorCode": None}
    op.update(kwargs)
    return {"currentBulkOperation": op}


class TestRunBulkExportHappyPath(unittest.TestCase):
    def test_submit_poll_completed_download_3_records(self) -> None:
        submit = _make_completed_submit()
        poll1 = _make_poll("RUNNING")
        poll2 = _make_poll(
            "COMPLETED",
            objectCount="3",
            fileSize="123",
            url="https://storage.googleapis.com/shopify-bulk/file.jsonl?sig=abc",
        )
        fake_gql = FakeGraphQL([submit, poll1, poll2])
        body = (
            '{"id":"gid://shopify/Order/1","name":"#1001"}\n'
            '{"id":"gid://shopify/Order/2","name":"#1002"}\n'
            '{"id":"gid://shopify/Order/3","name":"#1003"}\n'
        )
        fake_dl = FakeDownloader(200, body)

        result = bo.run_bulk_export(
            inner_query=ORDERS_QUERY,
            post_graphql=fake_gql,
            get_url=fake_dl,
            poll_interval_sec=0.0,
            timeout_sec=60,
            sleep_fn=_no_sleep,
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.row_count, 3)
        self.assertEqual(result.object_count, 3)
        self.assertEqual(result.file_size, 123)
        self.assertEqual(result.operation_id, "gid://shopify/BulkOperation/123")
        # Download URL should be redacted (signed query stripped).
        self.assertIn("<signed>", result.download_url or "")
        self.assertNotIn("sig=abc", result.download_url or "")

    def test_completed_with_no_url_returns_empty(self) -> None:
        # Some shops have 0 rows matching — Shopify returns COMPLETED with
        # url=null. Treat as a successful empty export.
        fake_gql = FakeGraphQL([
            _make_completed_submit(),
            _make_poll("COMPLETED", objectCount="0", url=None),
        ])
        fake_dl = FakeDownloader(200, "")
        result = bo.run_bulk_export(
            inner_query=ORDERS_QUERY,
            post_graphql=fake_gql,
            get_url=fake_dl,
            poll_interval_sec=0.0,
            timeout_sec=60,
            sleep_fn=_no_sleep,
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.row_count, 0)
        self.assertEqual(len(fake_dl.calls), 0)  # never downloaded


class TestRunBulkExportFailures(unittest.TestCase):
    def test_submit_user_errors_falls_back(self) -> None:
        fake_gql = FakeGraphQL([{
            "bulkOperationRunQuery": {
                "bulkOperation": None,
                "userErrors": [{"field": ["query"], "message": "A bulk query operation for this app is already running"}],
            }
        }])
        result = bo.run_bulk_export(
            inner_query=ORDERS_QUERY,
            post_graphql=fake_gql,
            get_url=FakeDownloader(200, ""),
            poll_interval_sec=0.0,
            timeout_sec=60,
            sleep_fn=_no_sleep,
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.status, "userError")
        self.assertIn("already running", result.error or "")

    def test_poll_status_failed_falls_back(self) -> None:
        fake_gql = FakeGraphQL([
            _make_completed_submit(),
            _make_poll("FAILED", errorCode="INTERNAL_SERVER_ERROR"),
        ])
        result = bo.run_bulk_export(
            inner_query=ORDERS_QUERY,
            post_graphql=fake_gql,
            get_url=FakeDownloader(200, ""),
            poll_interval_sec=0.0,
            timeout_sec=60,
            sleep_fn=_no_sleep,
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.status, "failed")
        self.assertIn("FAILED", result.error or "")
        self.assertIn("INTERNAL_SERVER_ERROR", result.error or "")

    def test_poll_status_canceled_falls_back(self) -> None:
        fake_gql = FakeGraphQL([
            _make_completed_submit(),
            _make_poll("CANCELED"),
        ])
        result = bo.run_bulk_export(
            inner_query=ORDERS_QUERY,
            post_graphql=fake_gql,
            get_url=FakeDownloader(200, ""),
            poll_interval_sec=0.0,
            timeout_sec=60,
            sleep_fn=_no_sleep,
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.status, "cancelled")

    def test_poll_timeout_falls_back(self) -> None:
        # Submit ok, then RUNNING forever — controlled by the fake clock
        # advancing past the timeout.
        clock_fn, advance = _fake_clock_factory()

        def running_poll(_q: str, _v: Optional[Dict[str, Any]]) -> Dict[str, Any]:
            advance(20.0)  # each poll advances 20s
            return _make_poll("RUNNING")

        fake_gql = FakeGraphQL([_make_completed_submit()] + [running_poll] * 20)
        result = bo.run_bulk_export(
            inner_query=ORDERS_QUERY,
            post_graphql=fake_gql,
            get_url=FakeDownloader(200, ""),
            poll_interval_sec=0.0,
            timeout_sec=30,  # 30s budget; first poll advances clock 20s, second advances another 20s -> 40s > 30s
            sleep_fn=_no_sleep,
            clock_fn=clock_fn,
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.status, "timeout")

    def test_download_non_200_falls_back(self) -> None:
        fake_gql = FakeGraphQL([
            _make_completed_submit(),
            _make_poll("COMPLETED", url="https://storage.googleapis.com/x.jsonl?sig=abc"),
        ])
        fake_dl = FakeDownloader(403, "")
        result = bo.run_bulk_export(
            inner_query=ORDERS_QUERY,
            post_graphql=fake_gql,
            get_url=fake_dl,
            poll_interval_sec=0.0,
            timeout_sec=60,
            sleep_fn=_no_sleep,
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.status, "downloadError")
        self.assertIn("403", result.error or "")

    def test_download_exception_falls_back(self) -> None:
        fake_gql = FakeGraphQL([
            _make_completed_submit(),
            _make_poll("COMPLETED", url="https://storage.googleapis.com/x.jsonl?sig=abc"),
        ])
        fake_dl = FakeDownloader(200, "", exc=RuntimeError("connection reset"))
        result = bo.run_bulk_export(
            inner_query=ORDERS_QUERY,
            post_graphql=fake_gql,
            get_url=fake_dl,
            poll_interval_sec=0.0,
            timeout_sec=60,
            sleep_fn=_no_sleep,
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.status, "downloadError")


class TestSecretScrubbing(unittest.TestCase):
    def test_scrub_redacts_shopify_token(self) -> None:
        scrubbed = bo._scrub("transport error: X-Shopify-Access-Token: shpat_abc123def456")
        self.assertNotIn("shpat_abc123def456", scrubbed)
        self.assertIn("<redacted>", scrubbed)

    def test_redact_url_strips_signature(self) -> None:
        u = "https://storage.googleapis.com/shopify-bulk/file.jsonl?GoogleAccessId=xx&Signature=secret"
        red = bo._redact_url(u)
        self.assertNotIn("Signature=secret", red)
        self.assertIn("<signed>", red)
        self.assertIn("shopify-bulk/file.jsonl", red)


class TestEnvConfig(unittest.TestCase):
    def test_env_threshold_default(self) -> None:
        os.environ.pop("SHOPIFY_BULK_OPS_THRESHOLD", None)
        self.assertEqual(bo.env_threshold(10000), 10000)

    def test_env_threshold_override(self) -> None:
        os.environ["SHOPIFY_BULK_OPS_THRESHOLD"] = "500"
        try:
            self.assertEqual(bo.env_threshold(10000), 500)
        finally:
            os.environ.pop("SHOPIFY_BULK_OPS_THRESHOLD")

    def test_env_threshold_invalid_falls_to_default(self) -> None:
        os.environ["SHOPIFY_BULK_OPS_THRESHOLD"] = "not-a-number"
        try:
            self.assertEqual(bo.env_threshold(10000), 10000)
        finally:
            os.environ.pop("SHOPIFY_BULK_OPS_THRESHOLD")


if __name__ == "__main__":
    unittest.main()
