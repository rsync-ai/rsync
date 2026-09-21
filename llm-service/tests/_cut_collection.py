"""Shared detector for suites whose subject the public cut removes.

Not a test module (the name is deliberately not ``test_*``), and not imported by
anything that ships -- only by the two conftests beside it and by ``_flip_cut.py``.

THE PROBLEM. ``llm-service/oss-strip-list.txt`` deletes part of
``src/agents/tool_generator/`` -- today ``{agents,config,harness,mock_server,
prompts,tests,utils}`` plus three loose modules. Suites under ``llm-service/tests/``
reach into that tree two different ways:

  * absolutely -- ``from agents.tool_generator.utils import vendor_registry``
    (four files under ``tests/unit/``); and
  * through a ``sys.path`` shim -- ``sys.path.insert(0, <tool_generator dir>)``
    followed by ``from agents.architect_rest import ...`` (twelve ``test_gen_*.py``
    files under ``tests/``).

In the public repo neither is a failing test. Both are COLLECTION errors, and one
collection error aborts the whole pytest run with exit 2, ``Interrupted: N errors
during collection``. ``ci.yml`` passes ``tests/unit`` and ``tests/`` as
DIRECTORIES, so all sixteen would take every other suite in their directory down
with them on the public repo's first CI run. The ``$TG_ARGS`` conditional already
in ``ci.yml`` covers a missing path ARGUMENT (exit 4); it cannot cover files
pytest discovers for itself.

WHY DETECTION AND NOT A LIST. A hand-written ignore list -- in ``ci.yml`` or here
-- is a second copy of a fact, and both families are still growing. The thirteenth
``test_gen_*.py`` would be added by someone who has never read this file, and it
would break the public repo silently while every private check stayed green.
Reading the imports means a new importer is handled the moment it lands, and a
file that stops importing the moat starts running again with no edit here.

WHY THE NAMES ARE DERIVED AND NOT TYPED OUT. This file used to carry the stripped
subpackage names as a literal tuple, and so did both conftests and ``_flip_cut.py``
-- four hand-copies of one fact that ``oss-strip-list.txt`` already states. Moving
six subtrees OUT of the moat (contracts/, generator/, scaffold/, schemas/,
templates/, validation/ -- the deterministic renderer, promoted so a self-hosted
user can generate a connector from a spec) falsified all four at once, and nothing
in the private repo could go red for it: every copy still named a real directory,
which is exactly why the assertions kept passing while their subject moved. The
public consequence was three separate hard failures. So the names come from the
strip list, which SURVIVES the cut -- it is not in ``scripts/flip/excludes.txt``
and is present in the published tree -- and the next promotion moves all four
gates by editing one line.

WHY IT CANNOT SKIP SOMETHING IT SHOULDN'T. The gate is the presence of the
directories themselves. In the private repo they exist, ``ignored_modules()`` is
never consulted, and all sixteen suites run normally -- so a rename or an
accidental deletion still goes red instead of quietly skipping. That is the
distinction between a guard keyed on its subject and one keyed on an env var or
the repository name, and this repo has shipped the second kind before.

WHY THE MATCH IS SUBPACKAGE-PRECISE. ``agents.tool_generator`` is no longer a
usable prefix on its own: half the package ships now, so matching the prefix would
ignore ``test_scaffold_renders_offline.py`` -- a guard for the very feature the
promotion exists to deliver -- and hide it in the public repo behind a green tick.
The absolute rule therefore matches on the THIRD dotted component. The shim rule
matches a bare top-level name, and ``agents`` is the hard case: ``src/agents``
claims that name too and survives the cut, so a bare ``agents.X`` counts as a
reach-in only when ``src/agents/X`` does not exist. Both rules additionally
require the file to name ``tool_generator`` somewhere, because ``config`` and
``utils`` are ordinary words.

Measured against ground truth -- every test module imported in a subprocess against
a materialised post-cut tree, first-party misses told from absent third-party deps
by whether the module resolves to a path in this repo -- the rule above ignores
exactly the 16 suites the cut orphans on this branch, and exactly the 18 it orphans
on ``origin/main``: no misses (which would red the public build) and no
over-matches (which would silently drop coverage).

WHY CONFTEST IS NOT ENOUGH, AND ``skip_if_cut`` EXISTS. Everything above runs out
of ``conftest.py`` as ``collect_ignore``, and ``collect_ignore`` only governs paths
pytest DISCOVERS for itself. A path named on the command line is collected
unconditionally -- ``pytest_ignore_collect`` is bypassed too, measured both ways on
pytest 9.1.1 (2026-09-04):

    bare discovery, hook returns True  ->  1 passed     (the bad module ignored)
    same tree, module named as an arg  ->  Interrupted: 1 error during collection

and in the second run the innocent SIBLING did not execute either, because one
collection error aborts the whole session with exit 2. ``ci.yml`` passes
directories, so conftest covers it; ``doc-links.yml`` names every doc guard
as a FILE, so conftest covers none of it. Measured on the materialised public tree
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

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_LLM_SERVICE = os.path.dirname(_TESTS_DIR)
_REPO = os.path.dirname(_LLM_SERVICE)

STRIP_LIST = os.path.join(_LLM_SERVICE, "oss-strip-list.txt")

# Relative to llm-service/. The package the strip list carves in half.
_PACKAGE = os.path.join("src", "agents", "tool_generator")
_ABSOLUTE_PREFIX = "agents.tool_generator"

# ``src/agents`` survives the cut and claims the same top-level name as the
# stripped ``src/agents/tool_generator/agents``. Telling them apart is what keeps
# the shim rule from matching every absolute import in the package.
_SRC_AGENTS = os.path.join(_LLM_SERVICE, "src", "agents")


def _read_strip_list():
    """Direct children of the tool_generator package that the strip list removes.

    Returns their names exactly as the file spells them, so ``service.py`` stays
    ``service.py``. Nested entries (none today) and entries outside the package are
    not this function's subject.
    """
    prefix = _PACKAGE + os.sep
    names = set()
    with open(STRIP_LIST, encoding="utf-8") as handle:
        for raw in handle:
            entry = raw.strip()
            if not entry or entry.startswith("#"):
                continue
            entry = entry.replace("/", os.sep)
            if not entry.startswith(prefix):
                continue
            tail = entry[len(prefix):]
            if os.sep not in tail:
                names.add(tail)
    if not names:
        # A parser that stops matching would ignore nothing, collect the orphaned
        # suites, and abort the public run with exit 2 -- the exact failure this
        # file exists to prevent, arriving silently. Fail where it is readable.
        raise RuntimeError(
            "%s named no direct child of %s. Either the list moved, or its format "
            "changed and this parser did not." % (STRIP_LIST, _PACKAGE)
        )
    return frozenset(names)


#: Paths (as spelled in the strip list) used to ask whether the cut has run.
STRIPPED_PATHS = _read_strip_list()

#: The importable subset of the above: directory names, and module names with the
#: ``.py`` dropped. ``AGENTIC_PIPELINE.md`` is a strip subject but not a module.
STRIPPED_MODULES = frozenset(
    name[:-3] if name.endswith(".py") else name
    for name in STRIPPED_PATHS
    if not name.endswith(".md")
)


def tree_is_intact():
    """True in the private repo: at least one stripped path is still on disk.

    All-or-nothing by construction -- the flip removes the whole list in one pass,
    so any survivor means the cut has not run here.
    """
    package = os.path.join(_LLM_SERVICE, _PACKAGE)
    return any(os.path.exists(os.path.join(package, name)) for name in STRIPPED_PATHS)


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


def _reaches_into_the_moat(module):
    """True if this dotted module name resolves inside a stripped subpackage."""
    parts = module.split(".")
    if module == _ABSOLUTE_PREFIX or module.startswith(_ABSOLUTE_PREFIX + "."):
        # agents.tool_generator.<subpackage>... -- the third component decides.
        # The prefix alone means nothing now that half the package ships.
        return len(parts) > 2 and parts[2] in STRIPPED_MODULES
    if parts[0] not in STRIPPED_MODULES:
        return False
    if parts[0] == "agents":
        # Ambiguous: could be the shimmed src/agents/tool_generator/agents, or the
        # ordinary src/agents that survives. Only the first is a reach-in, and the
        # child name tells them apart in BOTH trees -- src/agents/planner exists
        # publicly, src/agents/architect_rest does not exist anywhere.
        if len(parts) < 2:
            return False
        sibling = os.path.join(_SRC_AGENTS, parts[1])
        return not (os.path.isdir(sibling) or os.path.exists(sibling + ".py"))
    return True


def is_first_party(module):
    """True if this dotted module name is one of OURS, cut or not.

    Told apart from a third-party dependency, which is the distinction a guard
    needs before it can call a failed import a boundary defect: a shipped module
    importing ``agents.tool_generator.agents`` is the boundary cutting through an
    import and must fail the build, while one importing ``fastapi`` merely names a
    package the current environment lacks and must not.

    Asking the filesystem is the obvious way to draw that line, and it is correct
    in exactly one of the two trees. After the flip the stripped half is gone, so
    every reach-in into it answers ``no path for that`` -- indistinguishable from
    an absent dependency -- and the guard files the live defect under "third-party"
    and goes green. That is the same shape as the four hand-copied name lists above:
    a fact read from a place the cut deletes. The second clause reads it from the
    strip list instead, which survives the cut, so the verdict is the same on both
    sides of the flip.
    """
    candidate = os.path.join(_LLM_SERVICE, "src", *module.split("."))
    if os.path.isdir(candidate) or os.path.exists(candidate + ".py"):
        return True
    return _reaches_into_the_moat(module)


def imports_stripped_tree(path):
    """True if this file cannot be imported once the moat half of the tree is cut.

    Both reach-in shapes count. Either shape additionally requires the file to name
    ``tool_generator`` somewhere -- ``config`` and ``utils`` are ordinary words, and
    ``shared/mcp-connectors/schemas`` is a real surviving package, so a bare import
    alone would over-match.
    """
    modules = _imported_modules(path)
    if modules is None:
        return False
    if not any(_reaches_into_the_moat(m) for m in modules):
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
