"""Unit tests for the Shopify GraphQL DESTINATION (v1.1.0).

v1.0.0 was source-only; v1.1.0 adds a products write path backed by
productCreate/productUpdate mutations with Shopify `userErrors` parsing
(Shopify returns HTTP 200 with errors inside the mutation payload, so the
generic per-record dispatcher would otherwise count a failed write as a
success). The sink calls `<type>_import_data` (append) and
`<type>_upsert_data` (keyed) with rows under the `data` key + a `table`.

Run inside the connector image (has requests; no pytest):
    docker run --rm -v "$PWD:/work" -w /work mcp-shopify-admin-graphql:v1.0.0 \
        python -m unittest test_destination -v
or standalone:
    python3 test_destination.py
"""
import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from connector import ShopifyConnector, _build_product_input  # noqa: E402

_CFG = {"shop": "demo", "access_token": "tok"}


def _connector_with_capture(responses):
    """Return (connector, calls) where _execute_graphql is stubbed to pop the
    next response from `responses` and record (query, variables)."""
    c = ShopifyConnector()
    calls = []
    queue = list(responses)

    def fake_exec(query, variables, config, **kw):
        calls.append({"query": query, "variables": variables})
        return queue.pop(0) if queue else {}

    c._execute_graphql = fake_exec  # type: ignore[assignment]
    return c, calls


class TestProductInputMapping(unittest.TestCase):
    def test_aliases_and_coercions(self):
        inp, is_update = _build_product_input({
            "description": "<p>hi</p>",
            "product_type": "Shirt",
            "tags": "a, b ,c",
            "status": "active",
            "title": "Tee",
        })
        self.assertFalse(is_update)
        self.assertEqual(inp["descriptionHtml"], "<p>hi</p>")
        self.assertEqual(inp["productType"], "Shirt")
        self.assertEqual(inp["tags"], ["a", "b", "c"])
        self.assertEqual(inp["status"], "ACTIVE")
        self.assertNotIn("id", inp)

    def test_gid_id_marks_update(self):
        inp, is_update = _build_product_input({
            "id": "gid://shopify/Product/123", "title": "Tee",
        })
        self.assertTrue(is_update)
        self.assertEqual(inp["id"], "gid://shopify/Product/123")


class TestImportData(unittest.TestCase):
    def test_append_creates_product(self):
        c, calls = _connector_with_capture([
            {"productCreate": {"product": {"id": "gid://shopify/Product/1"}, "userErrors": []}},
        ])
        res = c.import_data({"config": _CFG, "table": "products",
                             "data": [{"title": "Tee", "handle": "tee"}]})
        self.assertTrue(res["success"])
        self.assertEqual(res["rows_inserted"], 1)
        self.assertIn("productCreate", calls[0]["query"])
        # append must NOT send an id even if present
        self.assertNotIn("id", calls[0]["variables"]["input"])

    def test_user_errors_counted_as_failure(self):
        c, _ = _connector_with_capture([
            {"productCreate": {"product": None,
                               "userErrors": [{"field": ["title"], "message": "can't be blank"}]}},
        ])
        res = c.import_data({"config": _CFG, "table": "products",
                             "data": [{"handle": "no-title"}]})
        self.assertFalse(res["success"])
        self.assertEqual(res["rows_inserted"], 0)
        self.assertTrue(res["errors"])
        self.assertIn("can't be blank", " ".join(res["errors"]))

    def test_unsupported_resource_errors(self):
        c, _ = _connector_with_capture([{}])
        res = c.import_data({"config": _CFG, "table": "orders",
                             "data": [{"id": "gid://shopify/Order/1"}]})
        self.assertFalse(res["success"])
        self.assertIn("orders", str(res.get("error", "")).lower())

    def test_empty_data_is_noop(self):
        c, _ = _connector_with_capture([])
        res = c.import_data({"config": _CFG, "table": "products", "data": []})
        self.assertTrue(res["success"])
        self.assertEqual(res["rows_inserted"], 0)


class TestUpsertData(unittest.TestCase):
    def test_upsert_updates_when_gid_present(self):
        c, calls = _connector_with_capture([
            {"productUpdate": {"product": {"id": "gid://shopify/Product/9"}, "userErrors": []}},
        ])
        res = c.upsert_data({"config": _CFG, "table": "products",
                             "data": [{"id": "gid://shopify/Product/9", "title": "New"}]})
        self.assertTrue(res["success"])
        self.assertEqual(res["rows_upserted"], 1)
        self.assertIn("productUpdate", calls[0]["query"])
        self.assertEqual(calls[0]["variables"]["input"]["id"], "gid://shopify/Product/9")

    def test_upsert_creates_when_no_id(self):
        c, calls = _connector_with_capture([
            {"productCreate": {"product": {"id": "gid://shopify/Product/2"}, "userErrors": []}},
        ])
        res = c.upsert_data({"config": _CFG, "table": "products",
                             "data": [{"title": "Fresh", "handle": "fresh"}]})
        self.assertTrue(res["success"])
        self.assertEqual(res["rows_upserted"], 1)
        self.assertIn("productCreate", calls[0]["query"])


class TestCapabilities(unittest.TestCase):
    def test_destination_advertised(self):
        caps = ShopifyConnector().get_capabilities()
        self.assertTrue(caps["supports_destination"])
        names = {op["name"] for op in caps["operations"]}
        self.assertIn("import_data", names)
        self.assertIn("upsert_data", names)
        dest_ops = [op for op in caps["operations"] if op.get("type") == "destination"]
        self.assertTrue(dest_ops)


if __name__ == "__main__":
    unittest.main(verbosity=2)
