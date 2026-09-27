"""tool-generator's OAuth token-store routes carry the S2S gate.

THE EXPOSURE. ``/v1/oauth/exchange``, ``GET /v1/oauth/connections`` and
``DELETE /v1/oauth/connections/{connection_id}`` are the HTTP face of
``TokenManager`` -- the ``tool_generator_oauth_tokens`` volume, one flat map
keyed by ``connection_id`` with no tenant axis. They shipped with no dependency
at all while their sibling ``DELETE /v1/connectors/{name}`` in the same file
carried ``require_internal_secret``.

WHY "NO HOST PORT" IS NOT THE ANSWER. docker-compose.yml publishes no port for
tool-generator, but puts it on the ``rsync-ai-mcp`` network *with the untrusted
JIT connectors it builds* -- which is verbatim the threat model
``require_internal_secret``'s own docstring was written for. From any container
on that network the three routes let a caller enumerate every connection id that
holds a token, delete any of them, or store an attacker-controlled token under
someone else's connection_id.

WHY THIS IS A DECLARATION TEST AND NOT A TestClient ONE. The gate's BEHAVIOUR is
already proven end to end, over the wire, in
``test_s2s_gate_fails_closed_unless_dev.py`` -- 503 when the secret is unset
outside dev, 401 on a wrong secret, 200 on the right one. What was missing and is
new here is only "these particular routes declare it", and ``service.py`` cannot
be imported without the service's full runtime (httpx, the OTel FastAPI
instrumentor), so reading the decorators is what makes the check runnable in a
plain unit environment. Verified by mutation: dropping ``dependencies=`` from any
one of the three routes turns this RED naming that route.

The vacuity guard matters -- an AST walk that matched nothing would otherwise
report a clean pass over zero routes.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _cut_collection import skip_if_cut  # noqa: E402

skip_if_cut("llm-service/src/agents/tool_generator/service.py")

SERVICE = (
    Path(__file__).resolve().parents[1]
    / "src/agents/tool_generator/service.py"
)

# Routes that read or write the token store. Everything else under /v1/oauth/ is
# metadata (provider list, authorize-URL builder) and is deliberately not here.
TOKEN_STORE_ROUTES = {
    ("post", "/v1/oauth/exchange"),
    ("get", "/v1/oauth/connections"),
    ("delete", "/v1/oauth/connections/{connection_id}"),
}

GATE = "require_internal_secret"


def _route_decorators():
    """Yield (method, path, gated) for every ``@app.<method>("<path>", …)``."""
    tree = ast.parse(SERVICE.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for dec in node.decorator_list:
            if not isinstance(dec, ast.Call) or not isinstance(dec.func, ast.Attribute):
                continue
            if not (isinstance(dec.func.value, ast.Name) and dec.func.value.id == "app"):
                continue
            if not dec.args or not isinstance(dec.args[0], ast.Constant):
                continue
            method = dec.func.attr.lower()
            path = dec.args[0].value
            gated = any(
                kw.arg == "dependencies" and GATE in ast.unparse(kw.value)
                for kw in dec.keywords
            )
            yield method, path, gated


def test_the_service_declares_the_routes_this_suite_is_about():
    """Vacuity guard: a parse that found nothing must not read as a pass."""
    found = {(m, p) for m, p, _ in _route_decorators()}
    assert found, "parsed no @app routes out of service.py"
    missing = TOKEN_STORE_ROUTES - found
    assert not missing, (
        f"{missing} are no longer declared in service.py -- if they were removed, "
        "delete this suite; if they were renamed, rename TOKEN_STORE_ROUTES."
    )


@pytest.mark.parametrize(
    "method,path", sorted(TOKEN_STORE_ROUTES), ids=lambda v: v.strip("/").replace("/", "_")
)
def test_every_token_store_route_is_gated(method, path):
    gated = {(m, p): g for m, p, g in _route_decorators()}
    assert gated[(method, path)], (
        f"{method.upper()} {path} touches the OAuth token store with no "
        f"dependencies=[Depends({GATE})]; it is reachable from every container "
        "on the rsync-ai-mcp network, including the untrusted JIT connectors."
    )


def test_the_destructive_connector_route_is_still_gated():
    """Control: the route that already had the gate must keep it.

    Without this the suite would stay green if someone stripped the gate
    everywhere *except* the three routes named above.
    """
    gated = {(m, p): g for m, p, g in _route_decorators()}
    assert gated[("delete", "/v1/connectors/{connector_name}")]
