"""RED regression for defect d08_path_param_binding.

A REST resource whose endpoint carries a path placeholder (e.g. sendgrid's
``/mail/batch/{batch_id}``) must NOT cause the generated connector to emit a
bare, undefined placeholder name into an f-string. The generated get_/update_/
delete_ CRUD helpers currently render::

    endpoint = f"/mail/batch/{batch_id}/{record_id}"

where ``{batch_id}`` is interpreted as an f-string interpolation of a Python
name ``batch_id`` that is never bound anywhere in the module -> NameError at
runtime (and an undefined-name lint finding). The correct behaviour is to pass
the literal ``{batch_id}`` template through to ``_make_request``, which routes
it through the ``_sub_path_param`` binder that substitutes placeholders from
connection config / call params at request time.

This test renders the connector fully IN-PROCESS (no network, no persistence to
shared/mcp-connectors, no docker):

    GenerationContract -> agents.architect_rest.build_rest_spec
                       -> generator.builder.ConnectorBuilder().build(spec)

and then, on the returned generated ``connector.py`` source, asserts:

  1. it compiles (py_compile-equivalent ``compile()``), and
  2. no endpoint path-parameter placeholder name (``batch_id``) survives as an
     undefined ``Name`` load anywhere in the module.

It fails on today's template output and passes once the three CRUD f-string
sites stop baking the placeholder into an f-string.

Mirrors the import + in-proc render recipe of
``src/agents/tool_generator/tests/test_harness_matrix.py`` and
``tests/test_connector_auth_contract.py``.
"""

from __future__ import annotations

import ast
import builtins
import os
import re
import sys

import pytest

# ---------------------------------------------------------------------------
# Import path setup.
#
# The repo-level ``llm-service/conftest.py`` already puts ``llm-service`` and
# ``llm-service/src`` on sys.path. The tool-generator modules
# (``agents.architect_rest``, ``generator.builder``, ``schemas.contract`` /
# ``schemas.spec``) use TOP-LEVEL absolute imports rooted at the tool_generator
# package dir (e.g. ``from schemas.contract import ...``), so that directory
# must also be on sys.path — exactly as test_harness_matrix.py does.
# ---------------------------------------------------------------------------
_TOOLGEN = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "src",
    "agents",
    "tool_generator",
)
if _TOOLGEN not in sys.path:
    sys.path.insert(0, _TOOLGEN)

from schemas.contract import (  # noqa: E402
    Dimension,
    Fact,
    FactSource,
    empty_contract,
)
from agents.architect_rest import build_rest_spec  # noqa: E402
from generator.builder import ConnectorBuilder  # noqa: E402


_PLACEHOLDER_RE = re.compile(r"\{([^/{}]+)\}")


# ---------------------------------------------------------------------------
# In-process contract -> spec -> code render
# ---------------------------------------------------------------------------

def _build_contract_with_path_param_resource():
    """Minimal REST contract with ONE resource whose endpoint has a path param.

    Uses the same injection point the session fast-path uses:
    ``contract.metadata['vendor_rest_resources']`` (consumed by
    ``architect_rest._build_resources`` -> ``_resource_from_dict``). The
    resource declares create/update/delete so the buggy get_/update_/delete_
    CRUD helpers are all emitted (get is always emitted by _resource_from_dict).
    """
    contract = empty_contract(session_id="d08-test", api_name="sendgrid_like")
    # build_rest_spec reads BASE_URL + AUTH_TYPE via _get_str, which requires a
    # present (non-MISSING) fact.
    contract.set_fact(
        Fact(
            dimension=Dimension.BASE_URL,
            value="https://api.sendgrid.com/v3",
            confidence=1.0,
            source=FactSource.VENDOR_YAML,
            evidence="test",
        )
    )
    contract.set_fact(
        Fact(
            dimension=Dimension.AUTH_TYPE,
            value="bearer",
            confidence=1.0,
            source=FactSource.VENDOR_YAML,
            evidence="test",
        )
    )
    contract.metadata["vendor_rest_resources"] = [
        {
            "name": "batch",
            # The load-bearing input: a path placeholder in the endpoint.
            "endpoint": "/mail/batch/{batch_id}",
            "id_field": "batch_id",
            "supports_create": True,
            "supports_update": True,
            "supports_delete": True,
        }
    ]
    return contract


def _render_connector_code() -> str:
    contract = _build_contract_with_path_param_resource()
    spec = build_rest_spec(contract)
    # include_dockerfile=False: in-process QA only inspects connector.py and
    # never persists a build context.
    result = ConnectorBuilder().build(spec, include_dockerfile=False)
    code = result.code or ""
    assert code, (
        "generator returned empty connector code; "
        f"validation_errors={getattr(result, 'validation_errors', None)}"
    )
    return code


def _endpoint_placeholder_names(contract) -> set:
    names: set = set()
    for r in contract.metadata.get("vendor_rest_resources", []):
        for m in _PLACEHOLDER_RE.finditer(r.get("endpoint", "") or ""):
            names.add(m.group(1))
    return names


# ---------------------------------------------------------------------------
# Undefined-name analysis (stdlib-only; robust over-approximation of "defined")
# ---------------------------------------------------------------------------

def _collect_bound_names(nodes) -> set:
    """All names BOUND by the given AST nodes (order-insensitive).

    Over-approximates deliberately: assignment targets, annotated assigns,
    aug-assigns, walrus, for/with/except/comprehension targets, function &
    class defs, lambda args, import bindings, global/nonlocal declarations.
    Over-approximation avoids use-before-assignment false positives while still
    exposing a name that is bound NOWHERE (the defect's ``batch_id``).
    """
    bound: set = set()

    def bind_target(t):
        if isinstance(t, ast.Name):
            bound.add(t.id)
        elif isinstance(t, (ast.Tuple, ast.List, ast.Starred)):
            for el in getattr(t, "elts", []) or []:
                bind_target(el)
            if isinstance(t, ast.Starred):
                bind_target(t.value)

    def add_args(args: ast.arguments):
        for a in (
            list(getattr(args, "posonlyargs", []) or [])
            + list(args.args or [])
            + list(args.kwonlyargs or [])
        ):
            bound.add(a.arg)
        if args.vararg:
            bound.add(args.vararg.arg)
        if args.kwarg:
            bound.add(args.kwarg.arg)

    for node in nodes:
        for n in ast.walk(node):
            if isinstance(n, ast.Assign):
                for t in n.targets:
                    bind_target(t)
            elif isinstance(n, (ast.AnnAssign, ast.AugAssign)):
                bind_target(n.target)
            elif isinstance(n, ast.NamedExpr):  # walrus
                bind_target(n.target)
            elif isinstance(n, (ast.For, ast.AsyncFor)):
                bind_target(n.target)
            elif isinstance(n, (ast.With, ast.AsyncWith)):
                for item in n.items:
                    if item.optional_vars is not None:
                        bind_target(item.optional_vars)
            elif isinstance(n, ast.ExceptHandler):
                if n.name:
                    bound.add(n.name)
            elif isinstance(n, (ast.comprehension,)):
                bind_target(n.target)
            elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                bound.add(n.name)
            elif isinstance(n, ast.Lambda):
                add_args(n.args)
            elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                add_args(n.args)
            elif isinstance(n, ast.Import):
                for alias in n.names:
                    bound.add((alias.asname or alias.name).split(".")[0])
            elif isinstance(n, ast.ImportFrom):
                for alias in n.names:
                    bound.add(alias.asname or alias.name)
            elif isinstance(n, (ast.Global, ast.Nonlocal)):
                for nm in n.names:
                    bound.add(nm)

    # Function args are only reachable via ast.walk hitting the def nodes above;
    # also handle top-level def/lambda arg binding explicitly for the direct
    # children (covered by the walk, but belt-and-suspenders for lambdas).
    for node in nodes:
        for n in ast.walk(node):
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                add_args(n.args)
    return bound


def _undefined_name_loads(code: str) -> set:
    """Return the set of Name *loads* that are bound nowhere resolvable.

    "Defined" = builtins + implicit ('self','cls','__name__','__file__', etc.)
    + every name bound anywhere in the whole module (module scope) + every name
    bound in any function that (transitively) contains the load. Because the
    generated module is a single file with straightforward binding, a
    whole-module + per-enclosing-function union is a safe over-approximation:
    it never flags a name that IS bound somewhere sensible, but still flags
    ``batch_id`` (bound nowhere).
    """
    tree = ast.parse(code)

    implicit = {
        "self", "cls", "__name__", "__file__", "__doc__", "__package__",
        "__loader__", "__spec__", "__builtins__", "__annotations__", "__dict__",
        "__class__",
    }
    module_bound = _collect_bound_names([tree]) | set(dir(builtins)) | implicit

    # Map each function node to the union of names bound in itself and all its
    # ancestor functions, plus module_bound.
    undefined: set = set()

    def visit(node, enclosing_bound: set):
        # Names bound directly by this scope (function body) — collected from
        # the function node (includes its args + all bindings in its body).
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            scope_bound = enclosing_bound | _collect_bound_names([node])
        else:
            scope_bound = enclosing_bound

        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load):
                if child.id not in scope_bound:
                    undefined.add(child.id)
            elif isinstance(
                child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
            ):
                visit(child, scope_bound)
            else:
                visit(child, scope_bound)

    visit(tree, module_bound)
    return undefined


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_generated_connector_compiles():
    """Sanity: the generated module is at least syntactically valid Python.

    NOTE: this passes even WITH the bug (an f-string with an undefined name is
    syntactically valid) — the undefined-name test below is the RED signal.
    """
    code = _render_connector_code()
    compile(code, "<generated_connector.py>", "exec")


def test_path_param_placeholder_not_emitted_as_undefined_name():
    """RED: ``/mail/batch/{batch_id}`` must not leak ``batch_id`` as an
    undefined f-string interpolation in the generated CRUD helpers."""
    contract = _build_contract_with_path_param_resource()
    placeholders = _endpoint_placeholder_names(contract)
    assert placeholders, "test setup error: no endpoint placeholder to check"

    code = _render_connector_code()
    undefined = _undefined_name_loads(code)

    leaked = sorted(placeholders & undefined)
    assert not leaked, (
        "generated connector emits endpoint path-parameter placeholder(s) as "
        f"undefined name(s): {leaked}. The get_/update_/delete_ CRUD helpers "
        "bake the raw endpoint into an f-string (e.g. "
        'f\"/mail/batch/{batch_id}/{record_id}\") so the placeholder becomes an '
        "unbound Python name -> NameError at runtime. Pass the literal "
        "'{batch_id}' template through to _make_request / _sub_path_param "
        "instead (use a plain string literal + f-string suffix)."
    )


def test_generated_connector_has_no_undefined_names():
    """Stronger form of the same guarantee: the generated module has ZERO
    undefined name loads (as the defect brief requires). Kept narrow enough to
    stay robust: the analyzer over-approximates the 'defined' set so it never
    false-flags a legitimately-bound name."""
    code = _render_connector_code()
    undefined = _undefined_name_loads(code)
    assert not undefined, (
        f"generated connector references undefined name(s): {sorted(undefined)}"
    )