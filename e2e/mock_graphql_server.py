#!/usr/bin/env python3
"""Deterministic mock GraphQL server for the generated-GraphQL connector E2E.

Companion to mock_github_server.py (REST). Serves a single POST /graphql
endpoint implementing just enough of a Relay-style GraphQL API for a
tool-generator-GENERATED connector (built from
e2e/fixtures/widgets_graphql_introspection.json) to drive a real pipeline:

  * test_connection()   -> `{ __typename }`                -> data.__typename
  * discover_schema()   -> `query IntrospectType($name){__type(name:$name){...}}`
  * export("widgets")   -> `query { widgets(first,after){edges{node{...}} pageInfo{...}} }`

Matching is intentionally PERMISSIVE (by substring, not exact query string) so
the mock does not couple to the exact field selection / operation name the
generator emits — only to the operation shape (typename / __type / widgets).

Stdlib only (no pip deps), so it runs in a bare `python:3.11-alpine` container:

    docker run -d --name rsync-ai-mock-graphql --network rsync-ai_default \
      --network-alias mock-graphql -v $PWD/e2e:/e2e python:3.11-alpine \
      python /e2e/mock_graphql_server.py

Env:
  PORT (default 8080)   — listen port
"""
from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---- Deterministic dataset -------------------------------------------------- #
# The rows the pipeline must land in Postgres. 5 widgets, stable ids.
WIDGETS = [
    {"id": "widget-1", "name": "Alpha", "value": 10},
    {"id": "widget-2", "name": "Bravo", "value": 20},
    {"id": "widget-3", "name": "Charlie", "value": 30},
    {"id": "widget-4", "name": "Delta", "value": 40},
    {"id": "widget-5", "name": "Echo", "value": 50},
]

# __type introspection responses per type name (leaf fields the discoverer walks).
_TYPE_FIELDS = {
    "Widget": [
        {"name": "id", "type": {"name": None, "kind": "NON_NULL",
                                 "ofType": {"name": "ID", "kind": "SCALAR", "ofType": None}}},
        {"name": "name", "type": {"name": "String", "kind": "SCALAR", "ofType": None}},
        {"name": "value", "type": {"name": "Int", "kind": "SCALAR", "ofType": None}},
    ],
}

# Observability so the test can assert the connector actually hit us.
COUNTERS = {"graphql_calls": 0, "typename_calls": 0, "introspect_calls": 0, "widgets_calls": 0}


def _widgets_connection() -> dict:
    """Single-page Relay connection over WIDGETS (hasNextPage=False)."""
    edges = [{"node": dict(w), "cursor": f"cursor-{i}"} for i, w in enumerate(WIDGETS)]
    return {
        "edges": edges,
        "nodes": [dict(w) for w in WIDGETS],  # `nodes` shortcut, in case the generator uses it
        "pageInfo": {
            "hasNextPage": False,
            "hasPreviousPage": False,
            "startCursor": edges[0]["cursor"] if edges else None,
            "endCursor": edges[-1]["cursor"] if edges else None,
        },
    }


def _resolve_graphql(query: str, variables: dict) -> dict:
    """Map an incoming GraphQL document to a deterministic response envelope."""
    q = query or ""
    COUNTERS["graphql_calls"] += 1

    # discover_schema(): __type(name: ...) introspection. Checked FIRST because a
    # `{ __typename }` probe also contains the substring "__type" — but never
    # "__type(" nor a `name` variable, so the two never collide.
    if "__type(" in q or ("__type" in q and "name" in (variables or {})):
        COUNTERS["introspect_calls"] += 1
        name = (variables or {}).get("name") or "Widget"
        fields = _TYPE_FIELDS.get(name, _TYPE_FIELDS["Widget"])
        return {"data": {"__type": {"name": name, "kind": "OBJECT", "fields": fields}}}

    # export("widgets"): the Relay list query.
    if "widgets" in q:
        COUNTERS["widgets_calls"] += 1
        return {"data": {"widgets": _widgets_connection()}}

    # test_connection(): `{ __typename }`
    if "__typename" in q:
        COUNTERS["typename_calls"] += 1
        return {"data": {"__typename": "Query"}}

    # Unknown query — return empty data (never an errors[] envelope, which the
    # connector treats as a hard failure).
    return {"data": {}}


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, payload) -> None:
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_a):  # quiet
        pass

    def do_GET(self):
        if self.path.startswith("/health"):
            return self._send(200, {"status": "ok"})
        if self.path.startswith("/metrics"):
            return self._send(200, dict(COUNTERS))
        return self._send(404, {"error": "not found"})

    def do_POST(self):
        if not self.path.startswith("/graphql"):
            return self._send(404, {"error": "not found"})
        try:
            n = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(n) if n else b"{}"
            body = json.loads(raw or b"{}")
        except (ValueError, json.JSONDecodeError):
            return self._send(400, {"errors": [{"message": "invalid JSON body"}]})
        query = body.get("query") or ""
        variables = body.get("variables") or {}
        return self._send(200, _resolve_graphql(query, variables))


def main() -> None:
    port = int(os.getenv("PORT", "8080"))
    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"mock-graphql listening on :{port} (POST /graphql, GET /health, /metrics)", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
