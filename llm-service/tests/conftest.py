"""Collection guard: the suites under tests/ whose subject the public cut removes.

The mechanism, and the reasoning behind detecting rather than listing, live in
``llm-service/tests/_cut_collection.py``. Read that file first -- this one is only
the wiring.

One subject is handled here: the half of ``src/agents/tool_generator/`` that
``llm-service/oss-strip-list.txt`` removes -- twelve ``test_gen_*.py`` suites reach
into it through a ``sys.path`` shim. Which subtrees those are is read from the
strip list, never typed out here; four copies of that fact went stale in one commit
once, and the reasoning is in ``_cut_collection.py``. With the tree present the
ignore list is EMPTY and every suite runs.

Nothing else belongs in this file. ``collect_ignore`` governs only paths pytest
discovers for itself; a module named as a command-line ARGUMENT is collected
regardless, and so is one that ``pytest_ignore_collect`` returns True for --
measured both ways, see ``_cut_collection.py``. ``ci.yml`` passes directories so
this mechanism reaches it, but ``doc-links.yml`` names its doc guards as
files and this mechanism reaches none of them. A guard whose SUBJECT the cut
removes calls ``skip_if_cut`` in its own module instead; that works under both
invocation styles. This file previously carried a ``CAPABILITIES.md`` entry that
looked like it covered ``test_doc_merge_claims_are_true.py`` and did not.
"""

import os

from _cut_collection import STRIPPED_PATHS, ignored_modules, tree_is_intact

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))

collect_ignore = []

if not tree_is_intact():
    _orphaned = ignored_modules(_TESTS_DIR)
    # Printed, never silent. "The suites were stripped" and "the suites vanished"
    # read identically off a pytest summary line.
    print(
        "NOTE: src/agents/tool_generator/{%s} are absent -- stripped by "
        "llm-service/oss-strip-list.txt. "
        "%d connector-generation suite(s) under llm-service/tests/ import that tree "
        "and have no subject here, so they are not collected: %s. "
        "Every other suite still runs."
        % (",".join(sorted(STRIPPED_PATHS)), len(_orphaned),
           ", ".join(_orphaned) or "(none)")
    )
    collect_ignore.extend(_orphaned)
