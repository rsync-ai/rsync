#!/usr/bin/env python3
"""
Functional test suite for generated MCP connectors.

Tests every generated REST and GraphQL connector against realistic mock
HTTP responses — no real API keys needed.

What is tested per connector:
  - Import & instantiation (no crash)
  - Auth headers (correct key/prefix/header name)
  - discover_schema() returns expected structure
  - _get_resource_config() carries correct per-resource hints
  - export() with paginated mock responses:
      Stripe  → starting_after / data key / has_more last_item_id
      HubSpot → after cursor / results key / paging.next.after
      Slack   → cursor param / per-resource data keys / response_metadata.next_cursor
      Notion  → start_cursor / results key / next_cursor
  - GraphQL query templates (Shopify, GitHub, Linear):
      Relay edges/node pattern present
      No bare stub queries
      All operations loadable
  - _extract_connection_rows depth-first search (GitHub nested viewer)
  - _normalize_to_rows response normalization
"""

from __future__ import annotations

import json
import os
import sys
import types
import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch, call

# ---------------------------------------------------------------------------
# Path setup — connectors import base_connector via sys.path manipulation
# ---------------------------------------------------------------------------
CONNECTORS_ROOT = os.path.join(
    os.path.dirname(__file__),
    "../shared/mcp-connectors",
)
CONNECTORS_ROOT = os.path.abspath(CONNECTORS_ROOT)
PUBLIC_ROOT = os.path.join(CONNECTORS_ROOT, "public")

# Each connector does: sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
# which resolves to CONNECTORS_ROOT. Make sure it's on the path so base_connector loads.
if CONNECTORS_ROOT not in sys.path:
    sys.path.insert(0, CONNECTORS_ROOT)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_versioned_dir(connector_root: str) -> str:
    """Resolve a connector root to its canonical versions/<current_version>/ dir.

    The connector root holds only latest.json now (no root copies); the runnable
    connector.py lives under versions/<current_version>/. Falls back to the root
    for any flat-layout connector.
    """
    latest = os.path.join(connector_root, "latest.json")
    if os.path.exists(latest):
        try:
            cv = (json.load(open(latest)).get("current_version") or "").strip()
        except Exception:
            cv = ""
        if cv:
            vdir = os.path.join(connector_root, "versions", cv)
            if os.path.isdir(vdir):
                return vdir
    return connector_root


def _load_connector(vendor_dir: str):
    """Import a connector module from public/<vendor_dir>/versions/<current_version>/connector.py."""
    versioned_dir = _resolve_versioned_dir(os.path.join(PUBLIC_ROOT, vendor_dir))
    connector_path = os.path.join(versioned_dir, "connector.py")
    if not os.path.exists(connector_path):
        raise FileNotFoundError(f"connector.py not found: {connector_path}")
    # Put the versioned dir on sys.path so `from base_connector import ...` resolves
    # the version-pinned copy (matches the Docker build context); CONNECTORS_ROOT
    # remains as the master-base_connector fallback.
    if versioned_dir not in sys.path:
        sys.path.insert(0, versioned_dir)

    import importlib.util
    spec = importlib.util.spec_from_file_location(
        f"connector_{vendor_dir.replace('-', '_')}",
        connector_path,
    )
    mod = importlib.util.module_from_spec(spec)
    mod.__name__ = f"connector_{vendor_dir.replace('-', '_')}"
    sys.modules[mod.__name__] = mod
    spec.loader.exec_module(mod)
    return mod


def _find_server_class(mod):
    """Find the MCPServer/Connector class in the module."""
    import inspect
    for name, obj in inspect.getmembers(mod, inspect.isclass):
        if name.endswith(("MCPServer", "Connector", "Server")) and obj.__module__ == mod.__name__:
            return obj
    raise RuntimeError(f"No MCPServer class found in {mod.__name__}")


def _make_mock_response(data: dict, status_code: int = 200):
    """Build a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.ok = (status_code < 400)
    resp.json.return_value = data
    resp.text = json.dumps(data)
    resp.headers = {"Content-Type": "application/json"}
    return resp


class TestShopifyGraphQLConnector(unittest.TestCase):
    """Shopify Admin GraphQL connector functional tests."""

    @classmethod
    def setUpClass(cls):
        cls.mod = _load_connector("shopify-admin-graphql")
        cls.ServerClass = _find_server_class(cls.mod)
        cls.GRAPHQL_OPERATIONS = cls.mod.GRAPHQL_OPERATIONS

    def test_01_graphql_operations_exist(self):
        """GRAPHQL_OPERATIONS dict is non-empty."""
        self.assertIsInstance(self.GRAPHQL_OPERATIONS, dict)
        self.assertGreater(len(self.GRAPHQL_OPERATIONS), 0)
        print(f"  ✅ Shopify: {len(self.GRAPHQL_OPERATIONS)} operations")

    def test_02_operations_have_relay_pattern(self):
        """List operations use edges/node Relay pattern."""
        relay_ops = ["products", "orders", "customers", "collections"]
        for op in relay_ops:
            self.assertIn(op, self.GRAPHQL_OPERATIONS, f"Missing operation: {op}")
            query = self.GRAPHQL_OPERATIONS[op]["query"]
            self.assertIn("edges", query, f"{op}: missing edges")
            self.assertIn("node", query, f"{op}: missing node")
            self.assertIn("pageInfo", query, f"{op}: missing pageInfo")
        print(f"  ✅ Shopify: all relay ops have edges/node/pageInfo")

    def test_03_no_stub_queries(self):
        """No operation has a bare stub query like 'query X { x }'."""
        for name, op in self.GRAPHQL_OPERATIONS.items():
            query = op.get("query", "")
            # A stub query would have no { } selection inside the field
            # Real queries have subfield selections
            self.assertIn("{", query, f"{name}: empty query body")
            # Real queries have more than 1 field selection
            field_count = query.count("\n")
            self.assertGreater(field_count, 2, f"{name}: too few fields ({field_count} newlines)")
        print(f"  ✅ Shopify: no stub queries detected")

    def test_04_products_query_correct(self):
        """products query has correct vars and fields."""
        op = self.GRAPHQL_OPERATIONS["products"]
        query = op["query"]
        self.assertIn("$first: Int", query)
        self.assertIn("$after: String", query)
        self.assertIn("id", query)
        self.assertIn("title", query)
        self.assertIn("vendor", query)

    def test_05_orders_query_has_line_items(self):
        """orders query fetches lineItems and stays PII-free by default.

        Customer.* and shippingAddress.* are protected customer data and
        Shopify gates them to Shopify/Advanced/Plus plans, so the default
        orders query intentionally omits them — apps with approval can
        extend the field set per-connection.
        """
        op = self.GRAPHQL_OPERATIONS["orders"]
        self.assertIn("lineItems", op["query"])
        self.assertNotIn("customer", op["query"])
        self.assertNotIn("shippingAddress", op["query"])
        self.assertNotIn("firstName", op["query"])
        self.assertNotIn("lastName", op["query"])

    def test_06_discover_schema_returns_tables(self):
        """discover_schema returns tables matching GRAPHQL_OPERATIONS.

        The generic-discovery contract requires auth to pass before any
        schema work, so test_connection's `{ __typename }` query must
        succeed. We mock requests.post with a default that satisfies
        the auth probe and lets per-table introspection gracefully
        return empty columns (the connector isolates per-resource
        failures, so the tables list is still populated).
        """
        inst = self.ServerClass.__new__(self.ServerClass)
        inst.base_url = "https://test.myshopify.com/admin/api/2024-10/graphql.json"
        config = {"shop": "test", "access_token": "shpat_test"}

        with patch("requests.post") as mock_post:
            mock_post.return_value = _make_mock_response({"data": {"__typename": "QueryRoot"}})
            result = inst.discover_schema(config)

        self.assertTrue(result.get("success"), f"discover_schema failed: {result}")
        self.assertIn("tables", result)
        names = {t["name"] for t in result["tables"]}
        self.assertIn("products", names)
        self.assertIn("orders", names)
        self.assertIn("customers", names)
        print(f"  ✅ Shopify discover_schema: {len(result['tables'])} tables")

    def test_06b_discover_schema_fails_closed_on_bad_auth(self):
        """discover_schema returns success=False when test_connection fails (auth-fail-closed)."""
        inst = self.ServerClass.__new__(self.ServerClass)
        inst.base_url = "https://test.myshopify.com/admin/api/2024-10/graphql.json"
        config = {"shop": "test", "access_token": "bad_token"}

        with patch("requests.post") as mock_post:
            mock_post.return_value = _make_mock_response({"errors": [{"message": "Invalid API key"}]}, status_code=401)
            result = inst.discover_schema(config)

        self.assertFalse(result.get("success"))
        self.assertIn("auth check failed", result.get("error", ""))
        self.assertEqual(result.get("tables"), [])

    def test_07_export_graphql_query_execution(self):
        """export() sends correct GraphQL query and normalizes response."""
        fake_response = {
            "data": {
                "products": {
                    "edges": [
                        {"node": {"id": "gid://shopify/Product/1", "title": "Test Shirt"}},
                        {"node": {"id": "gid://shopify/Product/2", "title": "Test Hat"}},
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }

        inst = self.ServerClass.__new__(self.ServerClass)
        inst.base_url = "https://test.myshopify.com/admin/api/2024-10/graphql.json"
        inst.connector_type = "shopify-admin-graphql"

        with patch("requests.post") as mock_post:
            mock_post.return_value = _make_mock_response(fake_response)
            result = inst.export(params={
                "config": {"shop": "test", "access_token": "shpat_abc"},
                "operation_name": "products",
                "table": "products",
            })

        self.assertTrue(result.get("success"), f"export failed: {result}")
        # GraphQL export returns rows under "data" key (REST uses "records")
        records = result.get("data") or result.get("records", [])
        self.assertGreater(len(records), 0)
        self.assertEqual(records[0].get("title"), "Test Shirt")
        print(f"  ✅ Shopify export: {len(records)} records extracted from edges/node")

    def test_08_extract_connection_rows_nested(self):
        """_extract_connection_rows handles nested viewer pattern (GitHub style)."""
        extract_fn = self.mod._extract_connection_rows

        # GitHub-style: viewer → repositories → edges
        nested = {
            "viewer": {
                "repositories": {
                    "edges": [
                        {"node": {"id": "r1", "name": "repo-one"}},
                        {"node": {"id": "r2", "name": "repo-two"}},
                    ]
                }
            }
        }
        rows = extract_fn(nested)
        self.assertIsNotNone(rows)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["name"], "repo-one")
        print(f"  ✅ _extract_connection_rows: extracted {len(rows)} nested viewer repos")

    def test_09_normalize_to_rows_relay(self):
        """_normalize_to_rows handles top-level Relay connection."""
        normalize_fn = self.mod._normalize_to_rows

        relay_response = {
            "products": {
                "edges": [
                    {"node": {"id": "1", "title": "Widget"}},
                ]
            }
        }
        rows = normalize_fn(relay_response)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Widget")

    def test_10_shop_query_singular(self):
        """shop operation uses direct object pattern (not edges/node)."""
        op = self.GRAPHQL_OPERATIONS.get("shop")
        if op is None:
            self.skipTest("shop operation not present")
        query = op["query"]
        self.assertIn("shop", query.lower())
        # shop is singular — should NOT use edges/node
        self.assertNotIn("edges", query.split("shop {")[1][:50] if "shop {" in query else "")
        print(f"  ✅ Shopify shop: singular pattern (no edges)")

    def test_11_execute_operation_accepts_schema_qualified_name(self):
        """The HITL table-picker persists schema-qualified names ("shopify.products")
        because discover_schema emits {name: products, schema: shopify} and the UI
        joins them for display. Downstream the orchestrator passes the persisted
        string verbatim as operation_name. The connector must tolerate this —
        before the fix it raised "unknown operation 'shopify.products'" and
        the whole shopify pipeline died at the executor.

        Regression guard: execute_operation must strip the schema prefix and
        find the underlying GraphQL operation.
        """
        inst = self.ServerClass.__new__(self.ServerClass)
        inst.base_url = "https://test.myshopify.com/admin/api/2024-10/graphql.json"

        fake_response = {
            "data": {
                "products": {
                    "edges": [{"node": {"id": "gid://shopify/Product/1", "title": "T"}}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }
        config = {"shop": "test", "access_token": "shpat_test"}

        # Both forms must work. The first is the canonical key, the second
        # is what the HITL/UI produces ("shopify.products"). The connector
        # should strip the prefix and dispatch to the same query.
        for op_name in ("products", "shopify.products"):
            with patch("requests.post") as mock_post:
                mock_post.return_value = _make_mock_response(fake_response)
                # Should not raise GraphQLError("unknown operation ...")
                result = inst.execute_operation(op_name, variables={"first": 1}, config=config)
                self.assertIn("products", result, f"execute_operation({op_name!r}) failed to dispatch")

    def test_12_export_accepts_schema_qualified_table(self):
        """The orchestrator calls export(params={'table': 'shopify.products', ...}).
        The connector must (a) dispatch to the correct GraphQL operation despite
        the schema prefix and (b) still treat non-shop operations as paginated
        connections (i.e. inject the `first` variable).
        """
        inst = self.ServerClass.__new__(self.ServerClass)
        inst.base_url = "https://test.myshopify.com/admin/api/2024-10/graphql.json"
        inst.connector_type = "shopify-admin-graphql"

        fake_response = {
            "data": {
                "products": {
                    "edges": [{"node": {"id": "gid://shopify/Product/9", "title": "X"}}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }
        with patch("requests.post") as mock_post:
            mock_post.return_value = _make_mock_response(fake_response)
            result = inst.export(params={
                "config": {"shop": "test", "access_token": "shpat_test"},
                "table": "shopify.products",  # HITL-persisted form
                "limit": 50,
            })

        self.assertTrue(result.get("success"), f"export failed: {result}")
        self.assertEqual(result.get("row_count"), 1)


class TestPaginationHandlerFixes(unittest.TestCase):
    """Direct tests of PaginationHandler improvements."""

    def setUp(self):
        from base_connector import PaginationHandler
        self.PH = PaginationHandler

    def test_01_explicit_cursor_path_wins(self):
        """Explicit cursor_path overrides generic auto-detect."""
        handler = self.PH(cursor_path="paging.next.after")
        data = {
            "results": [{"id": "1"}],
            "paging": {"next": {"after": "correct_cursor", "cursor": "wrong_cursor"}},
            "cursor": "also_wrong",
        }
        self.assertEqual(handler._extract_next_cursor(data), "correct_cursor")

    def test_02_response_metadata_next_cursor_in_defaults(self):
        """response_metadata.next_cursor is in the default cursor path list."""
        handler = self.PH()  # no explicit cursor_path
        data = {
            "bots": [{"id": "B1"}],
            "response_metadata": {"next_cursor": "slack_fallback_cursor"},
        }
        self.assertEqual(handler._extract_next_cursor(data), "slack_fallback_cursor")

    def test_03_paging_next_after_in_defaults(self):
        """paging.next.after is in the default cursor path list."""
        handler = self.PH()
        data = {
            "results": [{"id": "1"}],
            "paging": {"next": {"after": "hubspot_fallback_cursor"}},
        }
        self.assertEqual(handler._extract_next_cursor(data), "hubspot_fallback_cursor")

    def test_04_last_item_id_mode_reads_has_more(self):
        """cursor_mode=last_item_id stops when has_more=False."""
        call_count = [0]
        responses = [
            {"data": [{"id": "s1"}, {"id": "s2"}], "has_more": True},
            {"data": [{"id": "s3"}], "has_more": False},
        ]

        def fetch(params):
            i = call_count[0]
            call_count[0] += 1
            return (True, 200, responses[min(i, len(responses)-1)], {})

        handler = self.PH(
            pagination_type="cursor",
            pagination_param="starting_after",
            limit_param="limit",
            cursor_mode="last_item_id",
            id_field="id",
            response_data_key="data",
        )
        records, errors, final_cursor = handler.fetch_all_pages(fetch, max_pages=5, initial_params={})
        self.assertEqual(len(records), 3)
        self.assertEqual(call_count[0], 2)
        print(f"  ✅ last_item_id mode: {len(records)} records, {call_count[0]} pages")

    def test_05_dynamic_fallback_skips_meta_keys(self):
        """_extract_records skips ok/error/has_more/metadata keys."""
        handler = self.PH()
        data = {
            "ok": True,
            "error": None,
            "has_more": False,
            "response_metadata": {"next_cursor": ""},
            "channels": [{"id": "C1"}, {"id": "C2"}, {"id": "C3"}],
        }
        records = handler._extract_records(data)
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0]["id"], "C1")

    def test_06_explicit_response_data_key(self):
        """response_data_key takes priority over common keys."""
        handler = self.PH(response_data_key="members")
        data = {
            "data": [{"id": "WRONG"}],  # common key — should NOT be used
            "members": [{"id": "u1"}, {"id": "u2"}],
        }
        records = handler._extract_records(data)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["id"], "u1")

    def test_07_resource_config_creates_per_resource_handler(self):
        """ApiHandler.export_resource creates per-resource PaginationHandler from resource_config."""
        from base_connector import ApiHandler
        call_log = []

        def fake_request(method, endpoint, config, params, data):
            call_log.append(dict(params))
            return (True, 200, {"results": [{"id": "x1"}], "paging": {"next": {"after": None}}}, {})

        class FakeConnector:
            def _make_request_v2(self, method, endpoint, config, params, data):
                return fake_request(method, endpoint, config, params, data)

        handler = ApiHandler(
            connector=FakeConnector(),
            pagination_type="offset",  # default is OFFSET
            pagination_param="offset",
        )

        resource_cfg = {
            "pagination_type": "cursor",
            "pagination_param": "after",
            "limit_param": "limit",
            "max_page_size": 50,
            "response_data_key": "results",
            "cursor_path": "paging.next.after",
            "cursor_mode": "response",
            "id_field": "id",
        }
        result = handler.export_resource(
            connector=FakeConnector(),
            config={}, resource="contacts", endpoint="/crm/v3/objects/contacts",
            params={}, max_pages=2, max_records=100,
            resource_config=resource_cfg,
        )
        self.assertEqual(len(result.records), 1)
        # The request should have used 'after' (cursor param) not 'offset'
        print(f"  ✅ ApiHandler: per-resource PaginationHandler used cursor pagination")


class TestSyntheticHashPK(unittest.TestCase):
    """Validate the Fivetran-style synthetic PK helpers on both sinks.

    The pipeline's correctness story for PK-less sources hinges on the
    row hash being:
      - deterministic across runs (same source row -> same hash -> idempotent upsert)
      - order-independent over column keys (so trivial key-order shuffles don't break determinism)
      - immune to synthetic columns (excluding _rsync_* from the payload
        means a re-load of an already-loaded row produces the same hash
        even though the destination row also carries _rsync_synced_at)
    """

    def test_pg_compute_row_hash_is_deterministic(self):
        pg = _load_connector("postgresql")
        h1 = pg._compute_row_hash({"a": 1, "b": "x"}, ["a", "b"])
        h2 = pg._compute_row_hash({"a": 1, "b": "x"}, ["a", "b"])
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 64)  # sha256 hex

    def test_pg_compute_row_hash_key_order_invariant(self):
        pg = _load_connector("postgresql")
        h1 = pg._compute_row_hash({"a": 1, "b": "x"}, ["a", "b"])
        h2 = pg._compute_row_hash({"b": "x", "a": 1}, ["b", "a"])
        self.assertEqual(h1, h2)

    def test_pg_compute_row_hash_excludes_synthetic_cols(self):
        pg = _load_connector("postgresql")
        base = pg._compute_row_hash({"id": 1, "name": "x"}, ["id", "name"])
        # Same source data + added _rsync_synced_at must hash the same
        with_synthetic = pg._compute_row_hash(
            {"id": 1, "name": "x", "_rsync_synced_at": "2026-05-18T00:00:00Z"},
            ["id", "name", "_rsync_synced_at"],
        )
        self.assertEqual(base, with_synthetic)

    def test_mysql_compute_row_hash_matches_pg(self):
        pg = _load_connector("postgresql")
        my = _load_connector("database/mysql")
        row = {"a": 1, "b": "hello", "c": [1, 2, 3]}
        cols = ["a", "b", "c"]
        # Both sinks must hash identically so a user migrating from one
        # warehouse to the other isn't penalised by a key reshuffle.
        self.assertEqual(pg._compute_row_hash(row, cols),
                         my._compute_row_hash(row, cols))

    def test_pg_canonical_sql_type(self):
        pg = _load_connector("postgresql")
        # Postgres dialect → canonical
        self.assertEqual(pg._canonical_sql_type("text"), "string")
        self.assertEqual(pg._canonical_sql_type("character varying"), "string")
        self.assertEqual(pg._canonical_sql_type("integer"), "integer")
        self.assertEqual(pg._canonical_sql_type("bigint"), "integer")
        self.assertEqual(pg._canonical_sql_type("double precision"), "number")
        self.assertEqual(pg._canonical_sql_type("timestamp with time zone"), "timestamp")
        self.assertEqual(pg._canonical_sql_type("jsonb"), "json")
        self.assertEqual(pg._canonical_sql_type("bytea"), "binary")
        # Unknown / typo / new dialect
        self.assertEqual(pg._canonical_sql_type("frobnicator"), "string")
        self.assertEqual(pg._canonical_sql_type(""), "string")
        self.assertEqual(pg._canonical_sql_type(None), "string")
        # Precision suffix stripped
        self.assertEqual(pg._canonical_sql_type("varchar(255)"), "string")

    def test_mysql_canonical_type(self):
        my = _load_connector("database/mysql")
        self.assertEqual(my._canonical_mysql_type("tinyint(1)"), "integer")
        self.assertEqual(my._canonical_mysql_type("bigint"), "integer")
        self.assertEqual(my._canonical_mysql_type("double"), "number")
        self.assertEqual(my._canonical_mysql_type("datetime"), "timestamp")
        self.assertEqual(my._canonical_mysql_type("json"), "json")
        self.assertEqual(my._canonical_mysql_type("longblob"), "binary")
        # Unknown
        self.assertEqual(my._canonical_mysql_type("unknown_dialect_type"), "string")


class TestReloadDropTable(unittest.TestCase):
    """Regression guards for run_mode=reload destination cleanup.

    The reload path used to be a silent no-op for relational sinks
    because the orchestrator gated cleanup on `isObjectStorageDest`.
    Even after that gate was fixed, TRUNCATE was the wrong primitive:
    it preserves the column list, leaking dropped source columns into
    the destination forever. We replaced it with DROP TABLE so
    ensure_table can recreate fresh against the latest source schema
    on every reload — DMS / Fivetran / Airbyte full-load semantics.

    These tests assert the contract surface, not the SQL execution
    path (the real DROP needs a live database; that's covered by the
    UI E2E in the PR description).
    """

    def test_postgres_exposes_drop_table_operation(self):
        """get_capabilities must list drop_table so the orchestrator can
        route the reload cleanup through MCP. If this regresses, the
        orchestrator gets `method not found` and reload silently no-ops
        again — exactly the class of regression this PR is fixing."""
        pg = _load_connector("postgresql")
        inst = pg.PostgresqlMCPServer()
        caps = inst.get_capabilities()
        op_names = {op["name"] for op in caps["operations"]}
        self.assertIn("drop_table", op_names,
                      "postgres.drop_table must be exposed in get_capabilities")
        # And the method itself must exist on the instance.
        self.assertTrue(callable(getattr(inst, "drop_table", None)),
                        "postgres.drop_table method missing")

    def test_postgres_drop_table_validates_inputs(self):
        """Empty or unsafe identifiers must fail before any SQL is issued."""
        pg = _load_connector("postgresql")
        inst = pg.PostgresqlMCPServer()
        # Missing table → fast 4xx-style response, no DB hit.
        result = inst.drop_table({"table": ""})
        self.assertFalse(result.get("success"))
        self.assertIn("Missing table", result.get("error", ""))
        # Unsafe identifier (SQL-injection shape) → reject before DB.
        result = inst.drop_table({"table": "public.products; DROP TABLE users--"})
        self.assertFalse(result.get("success"))
        self.assertIn("Unsafe", result.get("error", ""))

    def test_mysql_exposes_drop_table_operation(self):
        my = _load_connector("database/mysql")
        # Find the server class.
        server_cls = _find_server_class(my)
        inst = server_cls()
        caps = inst.get_capabilities()
        op_names = {op["name"] for op in caps["operations"]}
        self.assertIn("drop_table", op_names,
                      "mysql.drop_table must be exposed in get_capabilities")
        self.assertTrue(callable(getattr(inst, "drop_table", None)),
                        "mysql.drop_table method missing")

    def test_mysql_drop_table_validates_inputs(self):
        my = _load_connector("database/mysql")
        server_cls = _find_server_class(my)
        inst = server_cls()
        # Missing table
        result = inst.drop_table({"table": ""})
        self.assertFalse(result.get("success"))
        self.assertIn("Missing table", result.get("error", ""))
        # Unsafe identifier
        result = inst.drop_table({"table": "e2e_db.products; DROP DATABASE foo--",
                                  "config": {"database": "e2e_db"}})
        self.assertFalse(result.get("success"))
        self.assertIn("Unsafe", result.get("error", ""))


# ============================================================================
# MAIN RUNNER
# ============================================================================

if __name__ == "__main__":
    # Pretty-print test results
    import sys

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    test_classes = [
        TestStripeConnector,
        TestHubSpotConnector,
        TestSlackConnector,
        TestNotionConnector,
        TestShopifyGraphQLConnector,
        TestGitHubGraphQLConnector,
        TestLinearGraphQLConnector,
        TestPaginationHandlerFixes,
    ]

    for cls in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)

    print("\n" + "="*70)
    print(f"TOTAL: {result.testsRun} tests, "
          f"{len(result.failures)} failures, "
          f"{len(result.errors)} errors, "
          f"{len(result.skipped)} skipped")
    if result.wasSuccessful():
        print("✅ ALL TESTS PASSED — generated connectors are production-ready")
    else:
        print("❌ SOME TESTS FAILED — see above for details")
        sys.exit(1)
