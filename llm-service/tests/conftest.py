"""Collection guard: the suites under tests/ whose subject the public cut removes.

The mechanism, and the reasoning behind detecting rather than listing, live in
``llm-service/tests/_cut_collection.py``. Read that file first -- this one is only
the wiring.

One subject is handled here: ``src/agents/tool_generator/{schemas,generator}``,
stripped by ``llm-service/oss-strip-list.txt`` -- thirteen ``test_gen_*.py`` suites
import it through a ``sys.path`` shim. With that tree present the ignore list is
EMPTY and every suite runs.

Nothing else belongs in this file. ``collect_ignore`` governs only paths pytest
discovers for itself; a module named as a command-line ARGUMENT is collected
regardless, and so is one that ``pytest_ignore_collect`` returns True for --
measured both ways, see ``_cut_collection.py``. ``ci.yml`` passes directories so
this mechanism reaches it, but ``doc-links.yml`` names its twelve doc guards as
files and this mechanism reaches none of them. A guard whose SUBJECT the cut
removes calls ``skip_if_cut`` in its own module instead; that works under both
invocation styles. This file previously carried a ``CAPABILITIES.md`` entry that
looked like it covered ``test_doc_merge_claims_are_true.py`` and did not.
"""

import os

from _cut_collection import ignored_modules

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_LLM_SERVICE = os.path.dirname(_TESTS_DIR)

_TOOL_GENERATOR = os.path.join(_LLM_SERVICE, "src", "agents", "tool_generator")
# All-or-nothing: one list removes them in one pass, so either present => intact.
_DEPS = ("schemas", "generator")

collect_ignore = []

if not any(os.path.isdir(os.path.join(_TOOL_GENERATOR, d)) for d in _DEPS):
    _orphaned = ignored_modules(_TESTS_DIR)
    # Printed, never silent. "The suites were stripped" and "the suites vanished"
    # read identically off a pytest summary line.
    print(
        "NOTE: src/agents/tool_generator/{schemas,generator} are absent -- stripped "
        "by llm-service/oss-strip-list.txt. "
        f"{len(_orphaned)} connector-generation suite(s) under llm-service/tests/ "
        "import that tree and have no subject here, so they are not collected: "
        f"{', '.join(_orphaned) or '(none)'}. Every other suite still runs."
    )
    collect_ignore.extend(_orphaned)
