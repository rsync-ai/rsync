"""Has the public-flip cut already run on this checkout?

Not a test module (the name is deliberately not ``test_*``), and not imported by
anything that ships -- only by the flip guards beside it.

WHAT BROKE. Two guards under ``llm-service/tests/`` read the flip's own inputs --
``scripts/flip/excludes.txt`` and ``scripts/flip/delink-docs.sh`` -- to check that
every doc link and every compose entrypoint survives the cut. Those inputs are
themselves cut: ``excludes.txt:132`` is the single line ``scripts/flip``. The
guards' own directory is NOT cut, and deliberately so -- the header of
``excludes.txt`` explains that deleting a test to make the flip quieter is
forbidden.

So both guards ship to the public repo with their subject deleted underneath them.
Run there, they do not skip and they do not pass: every assertion hits a missing
file and the module fails. Six failures on the public repo's first CI run, in the
two files whose entire purpose is proving the cut was clean. That is the same
defect the guards were written to catch, aimed at the guards.

WHY NOT "skip if the file is missing". Because that is a guard that disarms itself.
Rename ``excludes.txt`` in THIS repo and the checks would quietly stop running, with
a green tick and no subject -- the vacuous pass this suite exists to prevent.

THE DISCRIMINATOR. A cut tree is not a broken tree, and the difference is visible:
the cut removes things from BOTH lists. ``scripts/flip`` goes with
``excludes.txt``; the moat half of ``src/agents/tool_generator/`` goes with
``oss-strip-list.txt``. Both gone means the cut ran and there is nothing left to
check. Both present means this is the private repo and the guards must run. One of
each is a repo in a state no procedure produces -- a rename, a bad merge, a partial
delete -- and that fails loudly rather than skipping, which is the whole point.

WHICH PATH WITNESSES THE STRIP LIST. Not one named here. This file used to hard-code
``src/agents/tool_generator/generator`` as the moat witness, and then that package
was promoted OUT of the moat and into the community image. The line still named a
real directory, so nothing in the private repo went red -- but in the public repo
the witness was present while ``excludes.txt`` was gone, which is precisely the
"one of each" case, so every guard importing this module would have failed with
``half-cut tree`` on the first CI run. The witness is now whatever
``oss-strip-list.txt`` currently names (``_cut_collection.tree_is_intact``), so
promoting a subtree cannot leave a stale witness behind.
"""

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _cut_collection import tree_is_intact  # noqa: E402

# One witness from each half of the cut. excludes.txt is what the flip reads to know
# what to remove; the other half is derived, see the docstring.
_FROM_EXCLUDES = os.path.join("scripts", "flip", "excludes.txt")
_FROM_STRIP_LIST = "the paths llm-service/oss-strip-list.txt removes"


def _present(rel):
    return os.path.exists(os.path.join(REPO_ROOT, rel))


def is_a_pre_cut_tree():
    """True in the private repo, False in the public one; fail loudly on a half-cut tree.

    The predicate behind ``require_a_pre_cut_tree`` below, split out because not
    every caller wants a skip. Some need the plain fact of which tree they are in
    -- the release tags now live in only one of the two, and a guard that reads
    them has to know which -- and calling a skipping helper to learn that would
    end the test at the moment it acquired the information.
    """
    import pytest

    excludes, moat = _present(_FROM_EXCLUDES), tree_is_intact()
    if excludes and moat:
        return True
    if not excludes and not moat:
        return False
    pytest.fail(
        "half-cut tree: %s is %s but %s is %s. No step of the flip produces this -- "
        "the runbook removes both, and the private repo has both. Something was "
        "renamed, half-deleted, or merged wrong, and skipping here would hide it."
        % (_FROM_EXCLUDES, "present" if excludes else "gone",
           _FROM_STRIP_LIST, "present" if moat else "gone"))


def require_a_pre_cut_tree():
    """Skip on a cut tree, fail on a half-cut one, return on the private repo."""
    import pytest

    if is_a_pre_cut_tree():
        return
    pytest.skip(
        "the public cut has already run on this checkout (%s and %s are both "
        "gone) -- these guards check that the cut WILL be clean, so there is "
        "nothing left for them to read" % (_FROM_EXCLUDES, _FROM_STRIP_LIST))
