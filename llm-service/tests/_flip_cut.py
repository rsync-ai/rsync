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
``excludes.txt``; ``src/agents/tool_generator/generator`` goes with
``oss-strip-list.txt:32``. Both gone means the cut ran and there is nothing left to
check. Both present means this is the private repo and the guards must run. One of
each is a repo in a state no procedure produces -- a rename, a bad merge, a partial
delete -- and that fails loudly rather than skipping, which is the whole point.
"""

import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# One witness from each half of the cut. Neither is incidental: excludes.txt is what
# the flip reads to know what to remove, and the generator package is the moat.
_FROM_EXCLUDES = os.path.join("scripts", "flip", "excludes.txt")
_FROM_STRIP_LIST = os.path.join("llm-service", "src", "agents", "tool_generator", "generator")


def _present(rel):
    return os.path.exists(os.path.join(REPO_ROOT, rel))


def require_a_pre_cut_tree():
    """Skip on a cut tree, fail on a half-cut one, return on the private repo."""
    import pytest

    excludes, moat = _present(_FROM_EXCLUDES), _present(_FROM_STRIP_LIST)
    if excludes and moat:
        return
    if not excludes and not moat:
        pytest.skip(
            "the public cut has already run on this checkout (%s and %s are both "
            "gone) -- these guards check that the cut WILL be clean, so there is "
            "nothing left for them to read" % (_FROM_EXCLUDES, _FROM_STRIP_LIST))
    pytest.fail(
        "half-cut tree: %s is %s but %s is %s. No step of the flip produces this -- "
        "the runbook removes both, and the private repo has both. Something was "
        "renamed, half-deleted, or merged wrong, and skipping here would hide it."
        % (_FROM_EXCLUDES, "present" if excludes else "gone",
           _FROM_STRIP_LIST, "present" if moat else "gone"))
