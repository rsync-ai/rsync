"""Moving a job to a hosted runner must also move the prose above it.

WHAT BROKE. ``apply-ci-split.py`` rewrites ``runs-on: [self-hosted, macOS,
ARM64]`` to ``ubuntu-latest`` for every job in the public tree, because no
organisation runner group admits a public repository. That half is mechanical
and ``move_job`` does it. The other half -- the comment above the job
explaining that the flag is CRITICAL, that the cache is absent because the box
is persistent, that helm is present because the runners are developer laptops
-- is judgement, and it only travels if somebody wrote an ``EDITS`` entry for
it. Nobody did for most of them. ``doc-links.yml`` had no ``EDITS`` key at all.
So the public tree shipped comments asserting hardware it cannot be allocated,
and those comments do not merely go stale: each one reads as an instruction,
and it is what talks the next reader into moving the job back.

WHY THE FIX IS DERIVED. Naming today's stale comments in the guard would
repeat the mistake one file over -- a literal calibrated against today's prose
goes vacuous the first time somebody rewrites the block, and a vacuous guard is
indistinguishable from a passing one. So the check is a post-condition measured
on the text this script produced: a comment may not claim, in the present
tense, that this tree runs on the self-hosted pool.

WHY THAT MEASUREMENT IS SOUND. There are exactly three legitimate reasons for
the pool to appear in a tree that has no such runner -- it is history, it is a
hypothetical, or it is an instruction about the pool -- and all three are
expressible with a past-tense or conditional marker. A mention carrying none of
them is asserting the pool exists. The guard therefore refuses on a DEFINITE
reference (a determiner binding a self-hosted / macOS-runner noun) that no
dating word accompanies, which leaves ``a macOS self-hosted runner`` (a
hypothetical) and ``self-hosted job`` (a shape, not this tree's hardware)
legitimately sayable.

ITS COST, STATED. The unit is the comment BLOCK, not the sentence. Splitting on
``.`` is not available in prose full of ``~2.95 GB``, ``bash 3.2``, ``ci.yml``
and pinned action versions, and a splitter that mangles those produces false
positives that get "fixed" by weakening the guard. A block is what a reader
rewrites anyway. The cost is that a long block dating one mention satisfies the
guard for every other mention in the same block, so the guard is a floor and
not a substitute for reading the file.

THE CONTROL. A test asserting the produced tree is clean would pass just as
happily against a guard that never fires. ``test_the_defect_it_was_written_for``
runs the OLD pipeline -- ``move_job`` alone, no ``EDITS`` -- over the real
private ``ci.yml`` and asserts the guard REFUSES it by name.
"""

import importlib.util
import os

import pytest

import _flip_cut

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT = os.path.join(REPO_ROOT, "scripts", "flip", "apply-ci-split.py")
WORKFLOWS = os.path.join(REPO_ROOT, ".github", "workflows")
# Named as a file so the census (test_ci_filter_covers_every_guard_subject.py) can derive a
# subject that still exists in the public tree. This guard's other subject, scripts/flip,
# is cut; with only that and the WORKFLOWS directory the census finds no subject there and
# fails instead of skipping.
CI_YML = os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")


@pytest.fixture(autouse=True)
def _only_before_the_cut():
    """The subject is flip tooling, which the cut deletes; see _flip_cut."""
    _flip_cut.require_a_pre_cut_tree()


def _module():
    spec = importlib.util.spec_from_file_location("apply_ci_split", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _produced(mod, fname, with_edits=True):
    """The text ``main()`` would write for one file, in main()'s own order."""
    with open(os.path.join(WORKFLOWS, fname)) as fh:
        text = fh.read()
    for job in mod.MOVE_TO_HOSTED.get(fname, []):
        text, _ = mod.move_job(text, job)
    if with_edits:
        for entry in mod.EDITS.get(fname, []):
            old, new, label, count = entry[:4]
            marker = entry[4] if len(entry) > 4 else None
            text, _ = mod.sub1(text, old, new, f"{fname}: {label}", count, marker)
    for job in mod.DROP_JOBS.get(fname, []):
        text, _ = mod.drop_job(text, job)
    text, _ = mod.drop_orphaned_changes_outputs(text)
    return text


def _files(mod):
    return sorted(set(mod.MOVE_TO_HOSTED) | set(mod.EDITS))


def test_every_produced_workflow_is_clean():
    """The whole point, over the real tree rather than a fixture."""
    mod = _module()
    for fname in _files(mod):
        mod.refuse_on_undated_pool_claims(_produced(mod, fname), fname)


def test_the_defect_it_was_written_for():
    """The control: the pipeline without EDITS must be REFUSED.

    If this passes, ``move_job`` alone already produces prose that carries no
    present-tense claim, the EDITS added alongside this guard are doing nothing,
    and the guard above is vacuous.
    """
    mod = _module()
    assert os.path.isfile(CI_YML), "the control reads the real ci.yml"
    stale = _produced(mod, "ci.yml", with_edits=False)
    with pytest.raises(mod.Refuse) as exc:
        mod.refuse_on_undated_pool_claims(stale, "ci.yml")
    # Not just "it raised": it has to name several distinct sites, because a
    # guard that only ever fires on one line is a guard fitted to one line.
    assert str(exc.value).count("ci.yml:") >= 4, str(exc.value)


def test_the_guard_runs_on_every_file_main_writes():
    """A post-condition defined and not called is the vacuity case again."""
    with open(SCRIPT) as fh:
        src = fh.read()
    body = src[src.index("def main("):]
    assert "refuse_on_undated_pool_claims(text, fname)" in body, (
        "the guard is defined but main() never calls it")


def test_a_dated_mention_is_allowed():
    """History must stay sayable, or the fix becomes deletion."""
    mod = _module()
    mod.refuse_on_undated_pool_claims(
        "# It was CRITICAL while these jobs ran on the self-hosted Macs.\n",
        "fixture.yml")
    mod.refuse_on_undated_pool_claims(
        "# Do not move these jobs back to the self-hosted Macs.\n",
        "fixture.yml")
    mod.refuse_on_undated_pool_claims(
        "# If one of these self-hosted jobs ever returns, re-read 4b.\n",
        "fixture.yml")


def test_an_undated_mention_is_refused():
    mod = _module()
    with pytest.raises(mod.Refuse, match="fixture.yml:1"):
        mod.refuse_on_undated_pool_claims(
            "# The build cache persists on these self-hosted runners.\n",
            "fixture.yml")


def test_a_trailing_comment_on_a_code_line_is_scanned():
    """Where one of the staler claims actually lived.

    ``cache: false   # self-hosted: ...`` is invisible to any scan that looks
    only at whole-line comments, and both Go setup steps in security.yml
    carried exactly that shape.
    """
    mod = _module()
    with pytest.raises(mod.Refuse, match="fixture.yml:2"):
        mod.refuse_on_undated_pool_claims(
            "        with:\n"
            "          cache: false   # these macOS runners upload for hours\n",
            "fixture.yml")


def test_a_bare_plural_is_definite_without_a_determiner():
    """"runs on developer Macs" asserts the pool as squarely as "the Macs".

    Requiring a determiner missed exactly that, and the miss is not theoretical:
    ci.yml's a11y note read "The runners are developer Macs, so ...". The
    singular is deliberately still allowed -- that is the hypothetical below.
    """
    mod = _module()
    for claim in ("# The runners are developer Macs.\n",
                  "# Uploading it added ~9 min on self-hosted runners.\n",
                  "# bash 3.2 is what macOS runners ship.\n"):
        with pytest.raises(mod.Refuse):
            mod.refuse_on_undated_pool_claims(claim, "fixture.yml")


def test_a_hypothetical_and_a_shape_are_not_claims():
    """Both are legitimate in a tree that has no such runner.

    The second also keeps ``test_self_hosted_jobs_are_fork_guarded.py`` -- a
    filename, not an assertion about hardware -- out of the refusal set.
    """
    mod = _module()
    mod.refuse_on_undated_pool_claims(
        "# setup-python fails on a macOS self-hosted runner.\n", "fixture.yml")
    mod.refuse_on_undated_pool_claims(
        "# Required on every self-hosted job reachable from a fork PR by\n"
        "# llm-service/tests/test_self_hosted_jobs_are_fork_guarded.py.\n",
        "fixture.yml")


def test_the_block_is_the_unit_and_the_guard_says_so():
    """The documented cost, asserted so it cannot be quietly lost.

    A reader who assumes sentence granularity will trust the guard further than
    it goes. Pinning the behaviour here is what makes the docstring above a
    description rather than an aspiration.
    """
    mod = _module()
    mod.refuse_on_undated_pool_claims(
        "# It was true while these jobs ran on the Macs.\n"
        "# The build cache persists on these self-hosted runners.\n",
        "fixture.yml")
    with pytest.raises(mod.Refuse):
        mod.refuse_on_undated_pool_claims(
            "# It was true while these jobs ran on the Macs.\n"
            "\n"
            "# The build cache persists on these self-hosted runners.\n",
            "fixture.yml")


def test_the_deliberate_do_not_revert_block_survives_the_guard():
    """security.yml explains, in the public tree, why nothing here is
    self-hosted: a ``pull_request`` run executes the workflow from the PR head,
    so a fork-reachable self-hosted job hands arbitrary contributors code
    execution on the machines holding the deploy key. That block MUST keep
    naming the pool. It is the case the guard has to let through, and the
    reason ``DATED`` admits ``do not move`` and ``if it ever``.
    """
    mod = _module()
    produced = _produced(mod, "security.yml")
    assert "self-hosted" in produced, (
        "the do-not-revert note is gone, so this test is passing vacuously")
    mod.refuse_on_undated_pool_claims(produced, "security.yml")
