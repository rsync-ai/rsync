"""Source contract for `configure_from_connection` overrides on generated REST connectors.

`test_connection_state_isolation.py` proves the BEHAVIOUR on the base class. It
cannot cover these three, because each generated `api_saas` connector *overrides*
`configure_from_connection` and never calls `super()` — the override exists to
preserve the subclass's own `auth_type`, so the base method's body (including its
reset) is not on their path at all. Mutating the override back to its pre-fix
shape leaves that behavioural suite fully green.

These files are also GENERATED. The realistic regression is not someone editing
stripe/connector.py by hand — it is a regeneration, or a new REST connector,
landing with the guarded write the generator used to emit. So this asserts the
shape at the source level, over every connector that overrides the method plus
the Jinja template they come from, and auto-covers connectors that do not exist
yet.

The two properties, and the bug behind each (KI-RESTMCP-STICKY-BASE-URL):

  1. The override calls ``self._reset_connection_state()``. ``create_http_app()``
     builds ONE connector per process and re-runs this method on that shared
     instance for every request. The api_key if/elif chain has no else and the
     oauth branch returns early, so without the reset a connection carrying no
     credential keeps the PREVIOUS tenant's ``api_key`` / ``_oauth_token``.

  2. ``self.base_url`` is assigned UNCONDITIONALLY — never inside an ``if``. A
     guarded write leaves the previous connection's ``base_url`` standing, and
     the next caller's credential (headers are built from the current config) is
     sent to that inherited host. ``base_url`` is a declared, documented,
     user-settable field, so one tenant using it as intended was enough.

Static analysis is the right tool for property 2 specifically: "assigned on every
path" is a statement about control flow, and an `ast` walk answers it exactly,
whereas a behavioural test can only sample the paths someone thought to write.
"""
from __future__ import annotations

import ast
import os

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_PUBLIC = os.path.abspath(os.path.join(_HERE, "..", "public"))
_TEMPLATE = os.path.abspath(
    os.path.join(
        _HERE, "..", "..", "..", "llm-service", "src", "agents", "tool_generator",
        "templates", "connector.py.j2",
    )
)


def _connectors_overriding_configure():
    """Every generated connector.py that defines its own configure_from_connection.

    Walks `public/` rather than taking a hardcoded list so a new REST connector is
    gated the day it lands — the hardcoded-list habit is what let the shared tests
    dir sit un-run in CI for as long as it did.
    """
    found = []
    for root, _dirs, files in os.walk(_PUBLIC):
        if "connector.py" not in files or os.sep + "versions" + os.sep not in root + os.sep:
            continue
        path = os.path.join(root, "connector.py")
        try:
            tree = ast.parse(open(path, encoding="utf-8").read())
        except SyntaxError:  # pragma: no cover - a broken connector fails elsewhere
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "configure_from_connection":
                found.append((os.path.relpath(path, _PUBLIC), node))
                break
    return sorted(found)


_OVERRIDES = _connectors_overriding_configure()


def test_the_scan_found_the_known_overrides():
    """A gate that inspects ZERO files must not report success.

    If the connector layout moves, every parametrised test below would silently
    collapse to an empty set and this file would go green having checked nothing.
    """
    names = {p for p, _ in _OVERRIDES}
    assert names, "no connector overrides configure_from_connection — layout drifted?"
    for expected in ("stripe", "github-rest", "petstore"):
        assert any(n.startswith(expected + os.sep) for n in names), expected


@pytest.mark.parametrize("rel,fn", _OVERRIDES, ids=[p for p, _ in _OVERRIDES])
def test_override_resets_connection_state(rel, fn):
    calls = {
        n.func.attr
        for n in ast.walk(fn)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
    }
    assert "_reset_connection_state" in calls, (
        f"{rel}: configure_from_connection must call self._reset_connection_state() "
        "or the previous connection's api_key/_oauth_token survives into this one"
    )


def _base_url_assignments(fn):
    """(unconditional, conditional) counts of `self.base_url = ...` in `fn`.

    Conditional = the assignment has an If/Try/loop between it and the function
    body. That is exactly the pre-fix shape (`if connection_config.get("base_url"):`).
    """
    unconditional = conditional = 0

    def walk(body, guarded):
        nonlocal unconditional, conditional
        for stmt in body:
            if isinstance(stmt, ast.Assign):
                for t in stmt.targets:
                    if (
                        isinstance(t, ast.Attribute)
                        and t.attr == "base_url"
                        and isinstance(t.value, ast.Name)
                        and t.value.id == "self"
                    ):
                        if guarded:
                            conditional += 1
                        else:
                            unconditional += 1
            for field in ("body", "orelse", "finalbody"):
                inner = getattr(stmt, field, None)
                if isinstance(inner, list):
                    walk(inner, guarded or not isinstance(stmt, ast.FunctionDef))
            for handler in getattr(stmt, "handlers", []) or []:
                walk(handler.body, True)

    walk(fn.body, False)
    return unconditional, conditional


@pytest.mark.parametrize("rel,fn", _OVERRIDES, ids=[p for p, _ in _OVERRIDES])
def test_override_assigns_base_url_unconditionally(rel, fn):
    unconditional, conditional = _base_url_assignments(fn)
    assert unconditional >= 1, (
        f"{rel}: configure_from_connection must assign self.base_url on EVERY path; "
        "a guarded write leaves the previous connection's host standing"
    )
    assert conditional == 0, (
        f"{rel}: found {conditional} conditional `self.base_url = ...` assignment(s) — "
        "this is the KI-RESTMCP-STICKY-BASE-URL shape"
    )


def test_generated_connectors_never_use_two_arg_getenv_for_base_url():
    """`os.getenv(k, default)` returns "" for a SET-but-empty var, and Dockerfile.j2
    bakes `ENV MCP_BASE_URL=""` as an operator override slot — so the two-arg form
    makes the declared default unreachable. The `or` form is the only correct one."""
    offenders = []
    for root, _dirs, files in os.walk(_PUBLIC):
        if "connector.py" not in files:
            continue
        path = os.path.join(root, "connector.py")
        try:
            tree = ast.parse(open(path, encoding="utf-8").read())
        except SyntaxError:  # pragma: no cover
            continue
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "getenv"
                and len(node.args) == 2
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == "MCP_BASE_URL"
            ):
                offenders.append(f"{os.path.relpath(path, _PUBLIC)}:{node.lineno}")
    assert not offenders, (
        "two-arg os.getenv('MCP_BASE_URL', ...) found — use `os.getenv(k) or default`: "
        + ", ".join(offenders)
    )


# --------------------------------------------------------------------------- #
# The generator, not just its output                                          #
# --------------------------------------------------------------------------- #
# Fixing the connectors without fixing the template means the next regeneration
# reintroduces the bug — the repo rule ("fix the Jinja template when you fix a
# hand-curated connector") exists because that already happened. Rendering Jinja
# needs a jinja2 install this lane does not guarantee, so assert on the template
# SOURCE: cheap, dependency-free, and it fails for the right reason.
@pytest.mark.skipif(not os.path.exists(_TEMPLATE), reason="template not in this checkout")
def test_template_emits_the_fixed_shape():
    src = open(_TEMPLATE, encoding="utf-8").read()
    assert "self._reset_connection_state()" in src, (
        "connector.py.j2 no longer emits the connection-state reset — every connector "
        "generated from it would ship the cross-tenant leak"
    )
    assert 'os.getenv(\'MCP_BASE_URL\') or self.SPEC_BASE_URL' in src, (
        "connector.py.j2 must resolve base_url with the `or` form against SPEC_BASE_URL"
    )
    assert 'os.getenv(\'MCP_BASE_URL\', ' not in src, (
        "connector.py.j2 emits the two-arg getenv form for MCP_BASE_URL"
    )
    # The guarded write, verbatim as the template used to emit it.
    assert 'if connection_config.get("base_url"):' not in src, (
        "connector.py.j2 emits a CONDITIONAL self.base_url assignment"
    )
