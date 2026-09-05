"""
Guard: logging must be configured before any first-party module is imported.

The bug this exists to prevent shipped in PR #737 and reached prod on
2026-08-04. `src/gateway/main.py` imported three router modules at lines 28-30
and only called `setup_logging()` at line 41. Two of those routers emit a
*provenance* log at module scope — the line that says which LLM provider and
model that endpoint resolved to:

    src/agents/explorer/api.py:131   "explorer router llm: provider=… "
    src/agents/explorer/rank_tables.py:84   "rank-tables llm: provider=… "

Because they ran during the import at line 28-30, they were emitted with no
handler installed. `logging.lastResort` has level WARNING, so both INFO records
were dropped on the floor. Measured on prod that day:

    grep -c "explorer router llm:"  -> 0     (import-time, swallowed)
    grep -c "rank-tables llm:"      -> 0     (import-time, swallowed)
    grep -c "explorer llm: provider"-> 1     (emitted from main.py after setup)

That is the worst possible failure mode for this particular line. Its stated
purpose is to make LLM egress auditable — rank_tables.py's own comment says the
"previous silence is what let it point at OpenAI for months". A silence that
looks identical to the silence it was written to end is not an improvement.

Note the ERROR-level sibling at rank_tables.py:92 (the "EXPLORER_OFFLINE_ONLY
is set but this endpoint resolved to a cloud provider" alarm) *did* survive,
because lastResort passes WARNING and above. So the alarm worked while the
reassurance did not — the asymmetry that makes this hard to notice by reading
logs.

These tests are pure-stdlib AST checks: they need no llm-service dependency and
do not import the gateway.
"""

import ast
import pathlib

import pytest

LLM_SERVICE_ROOT = pathlib.Path(__file__).resolve().parents[1]
MAIN_PY = LLM_SERVICE_ROOT / "src" / "gateway" / "main.py"

LOG_METHODS = {"debug", "info", "warning", "error", "critical", "exception"}

# The only first-party import allowed to precede the setup_logging() call: the
# module that *provides* setup_logging. Anything else added here needs a reason
# recorded alongside it — an entry in this set is a module whose import-time
# logging is knowingly forfeited.
IMPORTS_ALLOWED_BEFORE_SETUP = {"src.utils.telemetry"}


def _parse(path: pathlib.Path) -> ast.Module:
    return ast.parse(path.read_text(), filename=str(path))


def _module_scope_nodes(tree: ast.Module):
    """Yield nodes that execute at import time.

    Deliberately does NOT descend into function or class bodies: a logging call
    there runs when the function is called, not when the module is imported, and
    counting those would inflate the census into meaninglessness (196 sites
    across llm-service/src rather than the 16 that actually run at import).
    """
    stack = list(tree.body)
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        yield node
        for field in ("body", "orelse", "finalbody", "handlers"):
            stack.extend(getattr(node, field, []) or [])


def _setup_logging_lineno(tree: ast.Module) -> int:
    for node in _module_scope_nodes(tree):
        if not isinstance(node, ast.Expr):
            continue
        call = node.value
        if isinstance(call, ast.Call):
            func = call.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
            if name == "setup_logging":
                return call.lineno
    return -1


def _first_party_imports(tree: ast.Module):
    """[(module, lineno)] for every `from src… import` / `import src…` at module scope."""
    found = []
    for node in _module_scope_nodes(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and (node.module == "src" or node.module.startswith("src.")):
                found.append((node.module, node.lineno))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "src" or alias.name.startswith("src."):
                    found.append((alias.name, node.lineno))
    return sorted(found, key=lambda pair: pair[1])


def _import_time_log_calls(path: pathlib.Path):
    """[(lineno, method)] for logging calls that run when `path` is imported."""
    calls = []
    for node in _module_scope_nodes(_parse(path)):
        if not isinstance(node, ast.Expr):
            continue
        call = node.value
        if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)):
            continue
        if call.func.attr not in LOG_METHODS:
            continue
        value = call.func.value
        receiver = value.id if isinstance(value, ast.Name) else getattr(value, "attr", "")
        if "log" in receiver.lower():
            calls.append((call.lineno, call.func.attr))
    return sorted(calls)


def test_setup_logging_precedes_every_first_party_import():
    """setup_logging() must run before any `src.*` module is imported.

    Ordering is the whole invariant. Any first-party module may log at import
    time — today two do, tomorrow another might — so the fix has to be "logging
    is ready before we import our own code", not "move this one log line".
    """
    tree = _parse(MAIN_PY)
    setup_line = _setup_logging_lineno(tree)
    imports = _first_party_imports(tree)

    # Vacuity guards. A census that silently matches nothing passes forever and
    # asserts nothing — the exact failure mode PR #738 found in #736's census.
    assert setup_line > 0, f"no module-scope setup_logging() call found in {MAIN_PY}"
    assert imports, f"no first-party `src.*` imports found in {MAIN_PY} — check the parser"

    too_early = [
        (module, lineno)
        for module, lineno in imports
        if lineno < setup_line and module not in IMPORTS_ALLOWED_BEFORE_SETUP
    ]
    assert not too_early, (
        f"setup_logging() is called at {MAIN_PY.name}:{setup_line}, but these first-party "
        f"modules are imported before it: {too_early}. Any logging they emit at import time "
        f"is dropped by logging.lastResort (which passes WARNING and above only), so INFO "
        f"provenance lines vanish. Move the setup_logging() call above these imports."
    )


@pytest.mark.parametrize(
    "relpath",
    ["src/agents/explorer/api.py", "src/agents/explorer/rank_tables.py"],
)
def test_explorer_provenance_logs_still_exist_and_are_covered(relpath):
    """The two modules whose import-time logs were swallowed still emit them.

    Without this, deleting the provenance lines would make the ordering test
    above pass for the wrong reason — nothing left to swallow. The guard has to
    fail when it stops guarding something.
    """
    path = LLM_SERVICE_ROOT / relpath
    assert path.exists(), f"{relpath} moved — update this guard"

    calls = _import_time_log_calls(path)
    assert calls, (
        f"{relpath} no longer logs at import time. If that was deliberate, drop it from "
        f"this test; if not, the provenance line that makes LLM egress auditable is gone."
    )

    # It must be imported by main.py, and after setup_logging — otherwise the
    # lines it emits are still lost.
    tree = _parse(MAIN_PY)
    setup_line = _setup_logging_lineno(tree)
    module_name = relpath.replace("/", ".").removesuffix(".py")
    matching = [ln for mod, ln in _first_party_imports(tree) if mod == module_name]
    assert matching, f"{module_name} is not imported by main.py — update this guard"
    assert all(ln > setup_line for ln in matching), (
        f"{module_name} is imported at line(s) {matching}, before setup_logging() at "
        f"line {setup_line}; its import-time logs at {calls} would be dropped."
    )
