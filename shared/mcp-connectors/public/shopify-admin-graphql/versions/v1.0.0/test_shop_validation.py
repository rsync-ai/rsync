"""Unit tests for the Shopify host-injection guard (data-plane token-exfil).

The user-supplied ``shop`` is spliced into the data-plane endpoint
``https://{shop}.myshopify.com/admin/...`` while the tenant's Shopify Admin
access token rides in the ``X-Shopify-Access-Token`` request header. If ``shop``
(or an ``endpoint`` override) is not validated to be a real ``*.myshopify.com``
host, the connector would POST that access token to an attacker-controlled
server (credential exfil). ``_sanitize_shop`` + the ``_resolve_endpoint`` host
pin prevent that. Follow-up to #625, which fixed the OAuth control-plane path.

Run standalone:
    python3 test_shop_validation.py
or:
    python3 -m unittest test_shop_validation -v
"""
import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from connector import ShopifyConnector, GraphQLError, _sanitize_shop  # noqa: E402


class TestSanitizeShop(unittest.TestCase):
    def test_accepts_bare_label(self):
        self.assertEqual(_sanitize_shop("good"), "good")

    def test_accepts_full_domain(self):
        self.assertEqual(_sanitize_shop("good.myshopify.com"), "good")

    def test_accepts_hyphen_and_case_and_whitespace(self):
        self.assertEqual(_sanitize_shop("  My-Store  "), "My-Store")

    def test_rejects_foreign_host(self):
        with self.assertRaises(ValueError):
            _sanitize_shop("attacker.com")

    def test_rejects_suffix_smuggle(self):
        with self.assertRaises(ValueError):
            _sanitize_shop("x.myshopify.com.attacker.com")

    def test_rejects_imds_ip(self):
        with self.assertRaises(ValueError):
            _sanitize_shop("169.254.169.254")

    def test_rejects_path_char(self):
        with self.assertRaises(ValueError):
            _sanitize_shop("foo/bar")

    def test_rejects_fragment_and_query(self):
        for raw in ("attacker.com#", "attacker.com?", "attacker.com/"):
            with self.assertRaises(ValueError):
                _sanitize_shop(raw)

    def test_rejects_userinfo(self):
        with self.assertRaises(ValueError):
            _sanitize_shop("user:pass@evil.com")

    def test_rejects_empty_and_none(self):
        for raw in ("", "   ", None):
            with self.assertRaises(ValueError):
                _sanitize_shop(raw)

    def test_rejects_leading_trailing_hyphen(self):
        for raw in ("-lead", "trail-"):
            with self.assertRaises(ValueError):
                _sanitize_shop(raw)


class TestResolveEndpointHostPin(unittest.TestCase):
    def setUp(self):
        self.conn = ShopifyConnector()

    def test_legit_shop_resolves(self):
        url = self.conn._resolve_endpoint({"shop": "demo"})
        self.assertEqual(
            url, "https://demo.myshopify.com/admin/api/2024-10/graphql.json"
        )

    def test_full_domain_shop_resolves(self):
        url = self.conn._resolve_endpoint({"shop": "demo.myshopify.com"})
        self.assertEqual(
            url, "https://demo.myshopify.com/admin/api/2024-10/graphql.json"
        )

    def test_endpoint_override_to_foreign_host_blocked(self):
        with self.assertRaises(GraphQLError):
            self.conn._resolve_endpoint(
                {"shop": "demo", "endpoint": "https://attacker.com/collect"}
            )

    def test_shop_host_escape_blocked(self):
        for shop in ("attacker.com#", "attacker.com/", "169.254.169.254"):
            with self.assertRaises(GraphQLError):
                self.conn._resolve_endpoint({"shop": shop})

    def test_missing_shop_blocked(self):
        with self.assertRaises(GraphQLError):
            self.conn._resolve_endpoint({"access_token": "t"})

    def test_endpoint_override_staying_on_myshopify_allowed(self):
        url = self.conn._resolve_endpoint(
            {
                "shop": "demo",
                "endpoint": "https://{shop}.myshopify.com/admin/api/2025-01/graphql.json",
            }
        )
        self.assertEqual(
            url, "https://demo.myshopify.com/admin/api/2025-01/graphql.json"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
