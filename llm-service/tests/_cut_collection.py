"""Shared detector for suites whose subject the public cut removes.

Not a test module (the name is deliberately not ``test_*``), and not imported by
anything that ships -- only by the two conftests beside it.

THE PROBLEM. ``llm-service/oss-strip-list.txt`` deletes
``src/agents/tool_generator/{agents,generator,schemas,utils,validation,...}``.
Suites under ``llm-service/tests/`` reach into that tree two different ways:

  * absolutely -- ``from agents.tool_generator.utils import vendor_registry``
    (five files under ``tests/unit/``); and
  * through a ``sys.path`` shim -- ``sys.path.insert(0, <tool_generator dir>)``
    followed by ``from schemas.contract import ...`` / ``from generator.builder
    import ...`` (thirteen ``test_gen_*.py`` files under ``tests/``).

In the public repo neither is a failing test. Both are COLLECTION errors, and one
collection error aborts the whole pytest run with exit 2, ``Interrupted: N errors
during collection``. ``ci.yml`` passes ``tests/unit`` and ``tests/`` as
DIRECTORIES, so all nineteen would take every other suite in their directory down
with them on the public repo's first CI run. The ``$TG_ARGS`` conditional already
in ``ci.yml`` covers a missing path ARGUMENT (exit 4); it cannot cover files
pytest discovers for itself.

WHY DETECTION AND NOT A LIST. A hand-written ignore list -- in ``ci.yml`` or here
-- is a second copy of a fact, and both families are still growing. The fourteenth
``test_gen_*.py`` would be added by someone who has never read this file, and it
would break the public repo silently while every private check stayed green.
Reading the imports means a new importer is handled the moment it lands, and a
file that stops importing the moat starts running again with no edit here.

WHY IT CANNOT SKIP SOMETHING IT SHOULDN'T. The gate is the presence of the
directories themselves. In the private repo they exist, ``ignored_modules()`` is
never consulted, and all nineteen suites run normally -- so a rename or an
accidental deletion still goes red instead of quietly skipping. That is the
distinction between a guard keyed on its subject and one keyed on an env var or
the repository name, and this repo has shipped the second kind before.

WHY CONFTEST IS NOT ENOUGH, AND ``skip_if_cut`` EXISTS. Everything above runs out
of ``conftest.py`` as ``collect_ignore``, and ``collect_ignore`` only governs paths
pytest DISCOVERS for itself. A path named on the command line is collected
unconditionally -- ``pytest_ignore_collect`` is bypassed too, measured both ways on
pytest 9.1.1 (2026-09-04):

    bare discovery, hook returns True  ->  1 passed     (the bad module ignored)
    same tree, module named as an arg  ->  Interrupted: 1 error during collection

and in the second run the innocent SIBLING did not execute either, because one
collection error aborts the whole session with exit 2. ``ci.yml`` passes
directories, so conftest covers it; ``doc-links.yml`` names all twelve doc guards
as FILES, so conftest covers none of it. Measured on the materialised public tree
before this was fixed: two modules read a cut file at import time, pytest exited 2,
and 0 of those 12 guards ran.

``skip_if_cut`` is the mechanism that survives both invocation styles: a
module-level ``pytest.skip``, evaluated when the module is imported, whichever way
it got there. It is keyed on the SUBJECT file exactly as the detector above is --
never on an env var, ``$CI``, or the repository name -- so a rename or an
accidental deletion in the private repo still goes red rather than quietly
skipping, and the skip is named in the summary rather than silent.
"""

import ast
import os
import sys

# Top-level names the sys.path shim makes resolvable inside the stripped tree.
# ``agents`` is deliberately absent: ``src/agents`` also claims it and survives the
# cut, so matching on it would ignore suites that are fine.
_SHIMMED_ROOTS = (
    "schemas",
    "generator",
    "validation",
    "utils",
    "harness",
    "mock_server",
    "contracts",
)

_ABSOLUTE_PREFIX = "agents.tool_generator"


def _imported_modules(path):
    """Top-level module names a file imports, or None if it cannot be parsed."""
    try:
        with open(path, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
    except (OSError, SyntaxError, UnicodeDecodeError):
        return None
    modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            modules.append(node.module)
    return modules


def imports_stripped_tree(path):
    """True if this file cannot be imported once the tool_generator tree is cut.

    Both reach-in shapes count. The shim shape additionally requires the file to
    name ``tool_generator`` somewhere -- ``schemas`` and ``generator`` are ordinary
    words, and ``shared/mcp-connectors/schemas`` is a real surviving package, so
    the bare import alone would over-match.
    """
    modules = _imported_modules(path)
    if modules is None:
        return False
    if any(m == _ABSOLUTE_PREFIX or m.startswith(_ABSOLUTE_PREFIX + ".") for m in modules):
        return True
    if not any(m in _SHIMMED_ROOTS or m.split(".")[0] in _SHIMMED_ROOTS for m in modules):
        return False
    try:
        with open(path, encoding="utf-8") as handle:
            return "tool_generator" in handle.read()
    except (OSError, UnicodeDecodeError):
        return False


def ignored_modules(directory):
    """Sorted basenames of the test files in `directory` that the cut orphans."""
    names = []
    for name in sorted(os.listdir(directory)):
        if name.startswith("test_") and name.endswith(".py"):
            if imports_stripped_tree(os.path.join(directory, name)):
                names.append(name)
    return names


_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def skip_if_cut(*rel_paths):
    """Skip this whole module when a subject of its is absent from the tree.

    Call at module level, BEFORE the first read of the subject -- the failures this
    exists to prevent are ``FileNotFoundError`` during import, not failed
    assertions, and a collection error takes every other suite in the run with it.

    Paths are repo-relative. Any one of them missing skips the module: a guard that
    cross-references two documents has nothing to say when either is gone.
    """
    import pytest

    missing = [p for p in rel_paths if not os.path.exists(os.path.join(_REPO, p))]
    if missing:
        # Name the caller. pytest reports a module-level skip at the line that RAISED
        # it, which is the line below -- so without this every such skip in the CI log
        # reads "_cut_collection.py:NNN" and the reader cannot tell which guard stood
        # down, or whether the same one stood down three times.
        caller = os.path.basename(sys._getframe(1).f_globals.get("__file__", "?"))
        pytest.skip(
            f"{caller}: subject absent from this tree, removed by the public cut "
            "(scripts/flip/excludes.txt or llm-service/oss-strip-list.txt): "
            + ", ".join(missing),
            allow_module_level=True,
        )
