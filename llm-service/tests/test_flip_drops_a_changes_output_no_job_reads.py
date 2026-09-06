"""Dropping a job must also drop the detector output only that job read.

WHAT BROKE. ``apply-ci-split.py`` deletes ``data-pipeline-gate`` and
``data-pipeline-smoke`` from the public tree, because no organisation runner
group admits a public repository and a self-hosted job there queues forever.
The smoke job was the only reader of ``needs.changes.outputs.datapath``.
Deleting it therefore left the public ``ci.yml`` declaring an output no job
reads, computed by a sixty-line ``dorny/paths-filter`` block whose comments
explain, at length, which paths must trigger a gate this tree does not have.
Every run paid for the filter and nothing consumed the answer.

WHY THE FIX IS DERIVED. Naming ``datapath`` in an ``EDITS`` entry would fix
this tree and only this tree: the next job dropped strands a different output,
and a literal calibrated against today's workflow is precisely the failure mode
this whole section exists to remove -- the same shape as the ``-eq 2`` count
``assert-ci-split.py`` replaced. So the orphan set is measured against the
post-drop text: an output survives iff some job still reads it.

WHY THAT MEASUREMENT IS SOUND. ``needs.<job>.outputs.<name>`` resolves only
inside the workflow that declares the job, so the consumer set is closed by the
file -- unless the workflow is callable, in which case a caller can read its
outputs from outside. ``drop_orphaned_changes_outputs`` refuses on
``workflow_call`` and on ``needs[...]`` indexing rather than counting a form it
cannot see.

THE SECOND HALF. Deleting a job leaves its NAME in the prose of other jobs.
``refuse_on_surviving_job_names`` is a post-condition over the whole file, so
the tool refuses to write a tree whose comments cite a gate it does not have.
The remedy it prescribes is a reword that is true in both trees, which is what
keeps the private and public files portable rather than divergent.

THE CONTROL. A test that only checks the output is gone would pass against a
tool that deleted the file. ``test_the_defect_it_was_written_for`` runs the OLD
pipeline -- the drop alone, without the sweep -- and asserts the orphan and its
filter block SURVIVE.
"""

import importlib.util
import os

import pytest

import _flip_cut

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT = os.path.join(REPO_ROOT, "scripts", "flip", "apply-ci-split.py")
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


# `kept` is read by a surviving job; `orphan` is read only by `doomed`. Both
# filter entries carry a comment block, because the block is half of what has
# to go and the walk that finds it is the half most likely to be wrong.
FIXTURE = """\
name: fixture
on:
  pull_request:
jobs:
  changes:
    runs-on: ubuntu-latest
    outputs:
      kept: ${{ steps.filter.outputs.kept }}
      orphan: ${{ steps.filter.outputs.orphan }}
    steps:
      - uses: dorny/paths-filter@v4
        id: filter
        with:
          filters: |
            # Why kept exists.
            kept:
              - 'src/**'
            # Why orphan exists, at length,
            # over several lines.
            orphan:
              - 'datapath/**'
              # An item-level note, indented deeper.
              - 'other/**'
            trailing:
              - 'z/**'

  survivor:
    needs: changes
    if: ${{ needs.changes.outputs.kept == 'true' }}
    runs-on: ubuntu-latest
    steps:
      - run: echo survivor

  doomed:
    needs: changes
    if: ${{ needs.changes.outputs.orphan == 'true' }}
    runs-on: ubuntu-latest
    steps:
      - run: echo doomed
"""


def _after_the_drop(mod, text=FIXTURE, job="doomed"):
    out, changed = mod.drop_job(text, job)
    assert changed, "the fixture's %r job was already absent" % job
    return out


def test_the_orphan_output_and_its_filter_block_both_go():
    mod = _module()
    out, swept = mod.drop_orphaned_changes_outputs(_after_the_drop(mod))
    assert swept == ["orphan"]
    assert "steps.filter.outputs.orphan" not in out
    assert "datapath/**" not in out
    assert "An item-level note, indented deeper." not in out
    assert "Why orphan exists, at length," not in out
    assert "over several lines." not in out


def test_the_defect_it_was_written_for():
    """The drop alone leaves both halves behind. The control."""
    stale = _after_the_drop(_module())
    assert "steps.filter.outputs.orphan" in stale, (
        "the drop already removes the output, so the sweep would be doing "
        "nothing and this whole guard would be vacuous")
    assert "datapath/**" in stale
    assert "Why orphan exists, at length," in stale


def test_a_consumed_output_is_untouched():
    mod = _module()
    out, _ = mod.drop_orphaned_changes_outputs(_after_the_drop(mod))
    assert "kept: ${{ steps.filter.outputs.kept }}" in out
    assert "# Why kept exists." in out
    assert "- 'src/**'" in out


def test_the_neighbouring_filter_entries_survive_intact():
    """The span must stop at the next key, not run to the end of the block."""
    mod = _module()
    out, _ = mod.drop_orphaned_changes_outputs(_after_the_drop(mod))
    lines = out.split("\n")
    i = lines.index("            trailing:")
    assert lines[i - 1] == "              - 'src/**'", (
        "the deletion ate into a neighbour: %r" % lines[i - 1])
    assert "              - 'z/**'" in lines


def test_nothing_is_swept_while_the_reader_still_exists():
    mod = _module()
    out, swept = mod.drop_orphaned_changes_outputs(FIXTURE)
    assert swept == []
    assert out == FIXTURE


def test_it_is_idempotent():
    mod = _module()
    once, swept = mod.drop_orphaned_changes_outputs(_after_the_drop(mod))
    assert swept
    twice, again = mod.drop_orphaned_changes_outputs(once)
    assert again == []
    assert twice == once


def test_it_refuses_when_the_workflow_is_callable():
    """A caller can read an output no job in the file reads."""
    mod = _module()
    callable_fixture = FIXTURE.replace("on:\n  pull_request:\n",
                                       "on:\n  pull_request:\n  workflow_call:\n")
    assert "workflow_call" in callable_fixture
    with pytest.raises(mod.Refuse, match="workflow_call"):
        mod.drop_orphaned_changes_outputs(_after_the_drop(mod, callable_fixture))


def test_it_refuses_rather_than_emptying_the_job():
    """Every output unread means the job is dead; that is a different fix."""
    mod = _module()
    out = _after_the_drop(mod)
    out = _after_the_drop(mod, out, "survivor")
    with pytest.raises(mod.Refuse, match="the job itself is dead"):
        mod.drop_orphaned_changes_outputs(out)


def test_a_dropped_job_name_may_not_survive_in_prose():
    mod = _module()
    clean = _after_the_drop(mod)
    mod.refuse_on_surviving_job_names(clean, ["doomed"], "fixture.yml")

    cited = clean.replace("      - run: echo survivor",
                          "      # mirrors what doomed used to check\n"
                          "      - run: echo survivor")
    with pytest.raises(mod.Refuse, match="still appears"):
        mod.refuse_on_surviving_job_names(cited, ["doomed"], "fixture.yml")


def test_the_real_ci_yml_comes_out_with_no_orphan_and_no_citation():
    """The derived invariant, on the tree the flip actually runs against."""
    yaml = pytest.importorskip("yaml")
    mod = _module()
    with open(CI_YML, encoding="utf-8") as fh:
        text = fh.read()

    jobs = mod.DROP_JOBS["ci.yml"]
    assert jobs, "DROP_JOBS lost its ci.yml entry; this guard has no subject"
    for job in jobs:
        text, changed = mod.drop_job(text, job)
        assert changed, "%s was already absent from ci.yml" % job

    swept_text, swept = mod.drop_orphaned_changes_outputs(text)
    assert swept, (
        "the drops stranded no `changes` output. Either the sweep stopped "
        "measuring, or ci.yml changed -- read it before relaxing this.")
    for name in swept:
        assert "steps.filter.outputs.%s" % name in text, (
            "%r was not an output before the sweep, so the sweep is not "
            "measuring what this asserts" % name)

    # The reworded prose is the point: no comment may name a dropped job.
    mod.refuse_on_surviving_job_names(swept_text, jobs, "ci.yml")

    doc = yaml.safe_load(swept_text)
    outputs = doc["jobs"]["changes"]["outputs"]
    for name in swept:
        assert name not in outputs
    for name in outputs:
        assert "needs.changes.outputs.%s" % name in swept_text, (
            "%r survived the sweep with no reader" % name)
