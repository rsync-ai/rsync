"""Make ``agents.tool_generator`` importable under the flat-layout ``agents`` shadow.

Twelve ``test_gen_*.py`` suites in this directory import the generation package
through its FLAT layout: each one puts ``llm-service/src/agents/tool_generator``
on ``sys.path`` (the shim at the top of ``test_gen_d01_test_connection.py``) so
that ``schemas``, ``generator`` and ``agents`` resolve as bare top-level names.
That directory holds its own ``agents/`` sub-package -- the agentic half of the
generator -- so the insert makes the name ``agents`` resolve to
``src/agents/tool_generator/agents``, and ``sys.modules["agents"]`` stays bound
to it for the rest of the session.

Anything collected afterwards that wants the REAL ``llm-service/src/agents``
therefore gets the wrong package, under which ``agents.tool_generator`` does not
exist. pytest collects alphabetically, so ``test_gen_*`` always lands before
``test_scaffold_*`` and the two scaffolder suites always inherit the shadow --
but only in a run that collects both, which is why a single-file run of either
suite passes and CI's whole-directory run does not.

``llm-service/conftest.py`` purges ``agents*`` for this same reason and cannot
help here: it runs once, at conftest import time, before any test module has
been imported and therefore before the shadow exists.

The repair is deliberately ADDITIVE: it appends ``llm-service/src/agents`` to
whatever ``agents`` package is already bound, rather than replacing the binding.
Replacing it was tried first and broke two tests in ``test_gen_dryrun_metadata``:
the shadow package's ``__init__`` re-exports a dozen names, and
``src/agents/tool_generator/agents/session_fast_path.py`` imports
``agents.architect_graphql`` LAZILY, inside a function body, so it is looked up
at RUN time -- after every test module, including this one, has been imported --
against whatever ``agents`` then resolves to. Appending keeps the shadow module
object, its re-exports and its own modules exactly as they were, and only adds a
second directory to search when a name is not found in the first. The two trees
share no module names, so nothing that resolved before resolves differently now;
``tool_generator`` simply stops being a miss.

This is a repair at the point of use rather than a new guard on purpose: the
failure it prevents is a collection error that stops the run, not a silent wrong
answer, so it cannot go unnoticed the way the copied-constant defects elsewhere
in this tree did.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys

_SRC_AGENTS = os.path.realpath(
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src", "agents"
    )
)


def _load_the_real_package():
    """Import ``llm-service/src/agents`` by path, for a tree not on ``sys.path``."""
    init = os.path.join(_SRC_AGENTS, "__init__.py")
    spec = importlib.util.spec_from_file_location(
        "agents", init, submodule_search_locations=[_SRC_AGENTS]
    )
    if spec is None or spec.loader is None:  # pragma: no cover - the tree is always here
        raise ImportError(f"cannot load the agents package from {init}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["agents"] = module
    spec.loader.exec_module(module)
    return module


def make_agents_tool_generator_importable() -> None:
    """Ensure ``agents.tool_generator`` resolves. Idempotent; safe post-cut."""
    package = sys.modules.get("agents")
    if package is None:
        try:
            package = importlib.import_module("agents")
        except ImportError:
            package = _load_the_real_package()

    search_path = getattr(package, "__path__", None)
    if search_path is None:  # pragma: no cover - `agents` is a package everywhere
        raise ImportError("the name `agents` is bound to a module, not a package")

    if not any(os.path.realpath(entry) == _SRC_AGENTS for entry in search_path):
        search_path.append(_SRC_AGENTS)
