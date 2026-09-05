"""Collection guard for tests/unit/ -- the sibling of ../conftest.py.

A ``collect_ignore`` list governs only the directory holding the conftest that
declares it, so the parent's list does not reach in here; this file is why
``ci.yml``'s first llm-service-unit invocation (``pytest tests/unit ...``) still
runs on the public repo instead of aborting with exit 2.

Five suites here import ``agents.tool_generator.{agents,utils,validation}``
directly. The mechanism and the rationale are in
``llm-service/tests/_cut_collection.py``.
"""

import os
import sys

_UNIT_DIR = os.path.dirname(os.path.abspath(__file__))
_TESTS_DIR = os.path.dirname(_UNIT_DIR)
_LLM_SERVICE = os.path.dirname(_TESTS_DIR)

sys.path.insert(0, _TESTS_DIR)
from _cut_collection import ignored_modules  # noqa: E402

_TOOL_GENERATOR = os.path.join(_LLM_SERVICE, "src", "agents", "tool_generator")
_DEPS = ("agents", "utils", "validation")

collect_ignore = []

if not any(os.path.isdir(os.path.join(_TOOL_GENERATOR, d)) for d in _DEPS):
    _orphaned = ignored_modules(_UNIT_DIR)
    print(
        "NOTE: src/agents/tool_generator/{agents,utils,validation} are absent -- "
        "stripped by llm-service/oss-strip-list.txt. "
        f"{len(_orphaned)} suite(s) under llm-service/tests/unit/ import that tree "
        "and have no subject here, so they are not collected: "
        f"{', '.join(_orphaned) or '(none)'}. Everything else still runs."
    )
    collect_ignore.extend(_orphaned)
