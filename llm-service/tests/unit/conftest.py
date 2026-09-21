"""Collection guard for tests/unit/ -- the sibling of ../conftest.py.

A ``collect_ignore`` list governs only the directory holding the conftest that
declares it, so the parent's list does not reach in here; this file is why
``ci.yml``'s first llm-service-unit invocation (``pytest tests/unit ...``) still
runs on the public repo instead of aborting with exit 2.

Four suites here import the stripped half of ``agents.tool_generator`` directly.
Which subtrees those are is read from ``llm-service/oss-strip-list.txt``, not typed
out here. The mechanism and the rationale are in
``llm-service/tests/_cut_collection.py``.
"""

import os
import sys

_UNIT_DIR = os.path.dirname(os.path.abspath(__file__))
_TESTS_DIR = os.path.dirname(_UNIT_DIR)

sys.path.insert(0, _TESTS_DIR)
from _cut_collection import STRIPPED_PATHS, ignored_modules, tree_is_intact  # noqa: E402

collect_ignore = []

if not tree_is_intact():
    _orphaned = ignored_modules(_UNIT_DIR)
    print(
        "NOTE: src/agents/tool_generator/{%s} are absent -- stripped by "
        "llm-service/oss-strip-list.txt. "
        "%d suite(s) under llm-service/tests/unit/ import that tree and have no "
        "subject here, so they are not collected: %s. Everything else still runs."
        % (",".join(sorted(STRIPPED_PATHS)), len(_orphaned),
           ", ".join(_orphaned) or "(none)")
    )
    collect_ignore.extend(_orphaned)
