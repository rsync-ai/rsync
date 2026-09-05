#!/usr/bin/env python3
"""
Sample Data MCP Connector (public) — a zero-setup demo SOURCE.

Ships three small, referentially-consistent bundled datasets (customers,
orders, products) so a brand-new user can run a REAL source -> destination
sync WITHOUT connecting a database of their own. No credentials and no
external service: every operation answers from in-memory Python data, so a
sync flows straight through the existing batch pipeline to the user's chosen
destination, and the freshly-synced tables are then queryable in Data Explorer.

The `email` column on `customers` is intentional — it exercises rsync's
PII-detection/masking gate during the demo, so the trust story is visible too.

Implements the minimal SOURCE contract the orchestrator batch executor calls
(tool name = "<connector_type>_<operation>"):
  sample-data_test_connection   -> {success, message, version}
  sample-data_validate_config   -> {success, valid, errors, warnings}
  sample-data_discover_schema   -> {success, tables:[...], total_tables}
  sample-data_export            -> {success, data, columns, row_count, has_more}
  sample-data_get_capabilities  -> {success, capabilities}

Cloned from the internal MinIO connector's self-contained stdlib HTTP/JSON-RPC
skeleton (no base_connector, no third-party deps).
"""

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List

CONNECTOR_TYPE = "sample-data"
CONNECTOR_VERSION = "1.0.0"

# --------------------------------------------------------------------------
# Bundled demo datasets. Synthetic values only (no real PII). Referentially
# consistent: orders.customer_id -> customers.customer_id and
# orders.product_id -> products.product_id, so the demo data joins cleanly in
# Data Explorer ("total revenue by country", "top products", etc.).
# --------------------------------------------------------------------------

_PRODUCTS: List[Dict[str, Any]] = [
    {"product_id": 1, "name": "Wireless Mouse", "category": "Accessories", "price": 24.99, "in_stock": True, "created_at": "2025-11-02"},
    {"product_id": 2, "name": "Mechanical Keyboard", "category": "Accessories", "price": 89.00, "in_stock": True, "created_at": "2025-11-05"},
    {"product_id": 3, "name": "USB-C Hub", "category": "Accessories", "price": 39.50, "in_stock": True, "created_at": "2025-11-10"},
    {"product_id": 4, "name": "4K Monitor", "category": "Displays", "price": 329.00, "in_stock": True, "created_at": "2025-11-12"},
    {"product_id": 5, "name": "Laptop Stand", "category": "Accessories", "price": 45.00, "in_stock": False, "created_at": "2025-11-15"},
    {"product_id": 6, "name": "1080p Webcam", "category": "Peripherals", "price": 59.99, "in_stock": True, "created_at": "2025-11-18"},
    {"product_id": 7, "name": "Desk Lamp", "category": "Office", "price": 32.00, "in_stock": True, "created_at": "2025-11-20"},
    {"product_id": 8, "name": "Noise-Cancelling Headphones", "category": "Audio", "price": 199.00, "in_stock": True, "created_at": "2025-11-22"},
    {"product_id": 9, "name": "Standing Desk", "category": "Furniture", "price": 549.00, "in_stock": False, "created_at": "2025-11-25"},
    {"product_id": 10, "name": "Ergonomic Chair", "category": "Furniture", "price": 279.00, "in_stock": True, "created_at": "2025-11-28"},
]

_CUSTOMERS: List[Dict[str, Any]] = [
    {"customer_id": 1, "first_name": "Ava", "last_name": "Martinez", "email": "ava.martinez@example.com", "country": "US", "plan": "pro", "signup_date": "2025-10-01"},
    {"customer_id": 2, "first_name": "Liam", "last_name": "Chen", "email": "liam.chen@example.com", "country": "CA", "plan": "free", "signup_date": "2025-10-03"},
    {"customer_id": 3, "first_name": "Sofia", "last_name": "Rossi", "email": "sofia.rossi@example.com", "country": "IT", "plan": "enterprise", "signup_date": "2025-10-05"},
    {"customer_id": 4, "first_name": "Noah", "last_name": "Kim", "email": "noah.kim@example.com", "country": "KR", "plan": "pro", "signup_date": "2025-10-08"},
    {"customer_id": 5, "first_name": "Emma", "last_name": "Dubois", "email": "emma.dubois@example.com", "country": "FR", "plan": "free", "signup_date": "2025-10-11"},
    {"customer_id": 6, "first_name": "Oliver", "last_name": "Smith", "email": "oliver.smith@example.com", "country": "GB", "plan": "pro", "signup_date": "2025-10-14"},
    {"customer_id": 7, "first_name": "Mia", "last_name": "Nguyen", "email": "mia.nguyen@example.com", "country": "VN", "plan": "enterprise", "signup_date": "2025-10-17"},
    {"customer_id": 8, "first_name": "Lucas", "last_name": "Silva", "email": "lucas.silva@example.com", "country": "BR", "plan": "free", "signup_date": "2025-10-20"},
    {"customer_id": 9, "first_name": "Amelia", "last_name": "Muller", "email": "amelia.muller@example.com", "country": "DE", "plan": "pro", "signup_date": "2025-10-23"},
    {"customer_id": 10, "first_name": "Ethan", "last_name": "Johnson", "email": "ethan.johnson@example.com", "country": "US", "plan": "free", "signup_date": "2025-10-26"},
    {"customer_id": 11, "first_name": "Isabella", "last_name": "Garcia", "email": "isabella.garcia@example.com", "country": "ES", "plan": "pro", "signup_date": "2025-10-29"},
    {"customer_id": 12, "first_name": "Arjun", "last_name": "Patel", "email": "arjun.patel@example.com", "country": "IN", "plan": "enterprise", "signup_date": "2025-11-01"},
]

_ORDERS: List[Dict[str, Any]] = [
    {"order_id": 1001, "customer_id": 1, "product_id": 4, "quantity": 1, "total_amount": 329.00, "status": "delivered", "order_date": "2025-12-02"},
    {"order_id": 1002, "customer_id": 2, "product_id": 1, "quantity": 2, "total_amount": 49.98, "status": "shipped", "order_date": "2025-12-03"},
    {"order_id": 1003, "customer_id": 3, "product_id": 8, "quantity": 1, "total_amount": 199.00, "status": "delivered", "order_date": "2025-12-03"},
    {"order_id": 1004, "customer_id": 4, "product_id": 2, "quantity": 1, "total_amount": 89.00, "status": "pending", "order_date": "2025-12-04"},
    {"order_id": 1005, "customer_id": 5, "product_id": 3, "quantity": 3, "total_amount": 118.50, "status": "delivered", "order_date": "2025-12-05"},
    {"order_id": 1006, "customer_id": 1, "product_id": 6, "quantity": 1, "total_amount": 59.99, "status": "cancelled", "order_date": "2025-12-06"},
    {"order_id": 1007, "customer_id": 6, "product_id": 10, "quantity": 1, "total_amount": 279.00, "status": "delivered", "order_date": "2025-12-07"},
    {"order_id": 1008, "customer_id": 7, "product_id": 9, "quantity": 1, "total_amount": 549.00, "status": "pending", "order_date": "2025-12-08"},
    {"order_id": 1009, "customer_id": 8, "product_id": 7, "quantity": 4, "total_amount": 128.00, "status": "shipped", "order_date": "2025-12-09"},
    {"order_id": 1010, "customer_id": 9, "product_id": 5, "quantity": 2, "total_amount": 90.00, "status": "delivered", "order_date": "2025-12-10"},
    {"order_id": 1011, "customer_id": 10, "product_id": 1, "quantity": 1, "total_amount": 24.99, "status": "delivered", "order_date": "2025-12-11"},
    {"order_id": 1012, "customer_id": 11, "product_id": 4, "quantity": 2, "total_amount": 658.00, "status": "shipped", "order_date": "2025-12-12"},
    {"order_id": 1013, "customer_id": 12, "product_id": 8, "quantity": 1, "total_amount": 199.00, "status": "pending", "order_date": "2025-12-13"},
    {"order_id": 1014, "customer_id": 2, "product_id": 2, "quantity": 1, "total_amount": 89.00, "status": "delivered", "order_date": "2025-12-14"},
    {"order_id": 1015, "customer_id": 3, "product_id": 3, "quantity": 2, "total_amount": 79.00, "status": "cancelled", "order_date": "2025-12-15"},
    {"order_id": 1016, "customer_id": 4, "product_id": 6, "quantity": 1, "total_amount": 59.99, "status": "shipped", "order_date": "2025-12-16"},
    {"order_id": 1017, "customer_id": 5, "product_id": 10, "quantity": 1, "total_amount": 279.00, "status": "delivered", "order_date": "2025-12-17"},
    {"order_id": 1018, "customer_id": 6, "product_id": 7, "quantity": 3, "total_amount": 96.00, "status": "delivered", "order_date": "2025-12-18"},
    {"order_id": 1019, "customer_id": 7, "product_id": 9, "quantity": 1, "total_amount": 549.00, "status": "pending", "order_date": "2025-12-19"},
    {"order_id": 1020, "customer_id": 8, "product_id": 2, "quantity": 2, "total_amount": 178.00, "status": "shipped", "order_date": "2025-12-20"},
]

# Table registry: name -> {columns:[{name,type,nullable}], primary_key:[...], rows:[...]}
TABLES: Dict[str, Dict[str, Any]] = {
    "customers": {
        "columns": [
            {"name": "customer_id", "type": "integer", "nullable": False},
            {"name": "first_name", "type": "string", "nullable": False},
            {"name": "last_name", "type": "string", "nullable": False},
            {"name": "email", "type": "string", "nullable": False},
            {"name": "country", "type": "string", "nullable": True},
            {"name": "plan", "type": "string", "nullable": True},
            {"name": "signup_date", "type": "date", "nullable": True},
        ],
        "primary_key": ["customer_id"],
        "rows": _CUSTOMERS,
    },
    "orders": {
        "columns": [
            {"name": "order_id", "type": "integer", "nullable": False},
            {"name": "customer_id", "type": "integer", "nullable": False},
            {"name": "product_id", "type": "integer", "nullable": False},
            {"name": "quantity", "type": "integer", "nullable": False},
            {"name": "total_amount", "type": "numeric", "nullable": False},
            {"name": "status", "type": "string", "nullable": False},
            {"name": "order_date", "type": "date", "nullable": False},
        ],
        "primary_key": ["order_id"],
        "rows": _ORDERS,
    },
    "products": {
        "columns": [
            {"name": "product_id", "type": "integer", "nullable": False},
            {"name": "name", "type": "string", "nullable": False},
            {"name": "category", "type": "string", "nullable": True},
            {"name": "price", "type": "numeric", "nullable": False},
            {"name": "in_stock", "type": "boolean", "nullable": False},
            {"name": "created_at", "type": "date", "nullable": True},
        ],
        "primary_key": ["product_id"],
        "rows": _PRODUCTS,
    },
}


def _column_names(table: str) -> List[str]:
    return [c["name"] for c in TABLES[table]["columns"]]


def _resolve_table(raw: Any) -> str:
    """Normalize an incoming table name — tolerate schema-qualified or quoted
    forms (e.g. "public"."customers") and return the bare table key."""
    name = str(raw or "").split(".")[-1].strip().strip('"').strip("`").strip("'")
    return name


class SampleDataMCPServer:
    def __init__(self) -> None:
        self.connector_type = CONNECTOR_TYPE

    def test_connection(self, params: Dict = None) -> Dict[str, Any]:
        # No external service to reach — the sample source is always ready.
        return {"success": True, "message": "Sample data source ready", "version": CONNECTOR_VERSION}

    def validate_config(self, params: Dict = None) -> Dict[str, Any]:
        # No credentials or configuration required.
        return {"success": True, "valid": True, "errors": [], "warnings": []}

    def get_capabilities(self, params: Dict = None) -> Dict[str, Any]:
        return {
            "success": True,
            "capabilities": {
                "supported_formats": ["json"],
                "supports_cdc": False,
                "supports_source": True,
                "supports_destination": False,
                "tables": list(TABLES.keys()),
            },
        }

    def discover_schema(self, params: Dict = None) -> Dict[str, Any]:
        params = params or {}
        include_row_counts = params.get("include_row_counts", True)
        include_columns = params.get("include_columns", True)
        tables = []
        for name, spec in TABLES.items():
            entry: Dict[str, Any] = {"name": name}
            if include_columns:
                entry["columns"] = [dict(c) for c in spec["columns"]]
            entry["primary_keys"] = list(spec["primary_key"])
            if include_row_counts:
                entry["row_count"] = len(spec["rows"])
            tables.append(entry)
        return {"success": True, "tables": tables, "total_tables": len(tables)}

    def export(self, params: Dict = None) -> Dict[str, Any]:
        params = params or {}
        table = _resolve_table(params.get("table") or params.get("resource"))
        if table not in TABLES:
            return {
                "success": False,
                "error": f"Unknown table: {table!r}. Available tables: {', '.join(TABLES)}",
            }

        def _as_int(value: Any, fallback: int) -> int:
            try:
                out = int(value)
            except (TypeError, ValueError):
                return fallback
            return out

        limit = _as_int(params.get("limit", 10000), 10000)
        offset = _as_int(params.get("offset", 0), 0)
        if limit <= 0:
            limit = 10000
        if offset < 0:
            offset = 0

        rows = TABLES[table]["rows"]
        page = [dict(r) for r in rows[offset:offset + limit]]
        return {
            "success": True,
            "data": page,
            "columns": _column_names(table),
            "row_count": len(page),
            # EOF signalled two ways the batch executor understands: has_more
            # False AND the final page's row_count < the requested limit.
            "has_more": (offset + len(page)) < len(rows),
        }


# --------------------------------------------------------------------------
# stdlib HTTP / JSON-RPC transport (cloned from the internal MinIO connector).
# --------------------------------------------------------------------------

def _jsonrpc_error(req_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def _jsonrpc_result(req_id: Any, result: Dict[str, Any]) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def serve_http() -> None:
    server = SampleDataMCPServer()
    port = int(os.getenv("MCP_PORT") or os.getenv("PORT") or "8000")
    prefix = CONNECTOR_TYPE + "_"
    dispatch = {
        "test_connection": server.test_connection,
        "validate_config": server.validate_config,
        "discover_schema": server.discover_schema,
        "export": server.export,
        "get_capabilities": server.get_capabilities,
        "health": lambda _args: {"success": True, "status": "ok"},
    }

    class Handler(BaseHTTPRequestHandler):
        def _send_json(self, status: int, obj: Dict[str, Any]) -> None:
            body = json.dumps(obj).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            if self.path == "/health":
                self._send_json(200, {"status": "ok"})
            else:
                self._send_json(404, {"error": "not found"})

        def do_POST(self):  # noqa: N802
            if self.path != "/mcp":
                self._send_json(404, {"error": "not found"})
                return

            try:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length > 0 else b"{}"
                req = json.loads(raw.decode("utf-8") or "{}")
            except Exception:
                self._send_json(200, _jsonrpc_error(None, -32700, "parse error"))
                return

            req_id = req.get("id")
            if req.get("jsonrpc") != "2.0":
                self._send_json(200, _jsonrpc_error(req_id, -32600, "invalid request"))
                return
            if req.get("method") != "tools/call":
                self._send_json(200, _jsonrpc_error(req_id, -32601, "method not found"))
                return

            params = req.get("params") or {}
            tool = params.get("name") or ""
            args = params.get("arguments") or {}
            if not isinstance(tool, str) or not tool:
                self._send_json(200, _jsonrpc_error(req_id, -32602, "missing tool name"))
                return
            if not isinstance(args, dict):
                self._send_json(200, _jsonrpc_error(req_id, -32602, "arguments must be an object"))
                return
            if not tool.startswith(prefix):
                self._send_json(200, _jsonrpc_error(req_id, -32601, "tool not found"))
                return

            op = tool[len(prefix):].strip()
            fn = dispatch.get(op)
            if fn is None:
                self._send_json(200, _jsonrpc_error(req_id, -32601, "tool not found"))
                return
            try:
                res = fn(args)
            except Exception as e:  # never crash the server on a bad request
                res = {"success": False, "error": str(e)}
            self._send_json(200, _jsonrpc_result(req_id, res))

        def log_message(self, format: str, *args):  # noqa: A002
            return

    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    httpd.serve_forever()


if __name__ == "__main__":
    serve_http()
