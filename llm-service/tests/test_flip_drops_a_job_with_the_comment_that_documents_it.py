"""A dropped CI job must take the comment block that documents it.

WHAT BROKE. ``apply-ci-split.py`` locates a job with ``job_span``, which starts
at the ``  <job>:`` line. Every non-trivial job in ``ci.yml`` is preceded by a
comment block explaining what it does and why -- twenty-one lines, in the case
of ``data-pipeline-gate``. Deleting the span alone left all of that prose in the
public tree: a paragraph describing a job the file no longer has.

The second one is worse than untidy. ``data-pipeline-smoke``'s comment opens by
positioning itself against "the full post-merge/nightly ``data-pipeline-gate``
above", so dropping both jobs left a comment pointing at a job that is also
gone. A reader of the public ``ci.yml`` would have found two blocks of
confident documentation for machinery that does not exist -- the same
stale-claim class this section was written to remove, produced by the tool that
removes it.

WHY A FIXTURE AND NOT ONLY THE REAL TREE. The real ``ci.yml`` answers "did this
particular drop come out clean", which is worth asserting and is asserted below.
It cannot answer "does the walk stop where it should", because the shapes that
would break it -- a job with no comment at all, two commented jobs adjacent, a
comment sitting directly under the previous job's body -- are not all present in
one file, and the ones that are present would vanish the moment someone
reordered the jobs. Those go to fixtures, where the answer is known by
construction.

THE CONTROL. A test that only checks the comment is gone passes just as well
against a tool that deletes the whole file. ``test_the_defect_it_was_written_for``
runs the OLD span logic on the same fixture and asserts the comment SURVIVES, so
the helper is proven load-bearing rather than merely present.
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


# Two commented jobs in a row, then an uncommented one -- every boundary the
# backwards walk has to respect, in the order it has to respect them.
FIXTURE = """\
name: fixture
jobs:
  first:
    runs-on: ubuntu-latest
    steps:
      - run: echo first
        # a step-level comment, indented deeper than a job comment

  # Second job. Explains itself over
  # more than one line.
  second:
    runs-on: ubuntu-latest
    steps:
      - run: echo second

  # Third job, which refers to `second` above.
  third:
    runs-on: ubuntu-latest
    steps:
      - run: echo third

  fourth:
    runs-on: ubuntu-latest
    steps:
      - run: echo fourth
"""


def test_the_helper_takes_the_comment_block_with_the_job():
    out, changed = _module().drop_job(FIXTURE, "second")
    assert changed
    assert "Second job. Explains itself over" not in out
    assert "more than one line." not in out
    assert "echo second" not in out


def test_the_defect_it_was_written_for():
    """The old logic -- span alone -- leaves the comment behind. The control."""
    mod = _module()
    lo, hi = mod.job_span(FIXTURE, "second")
    stale = FIXTURE[:lo] + FIXTURE[hi:]
    assert "Second job. Explains itself over" in stale, (
        "job_span already excludes the comment block, so comment_block_start "
        "would be doing nothing and this whole guard would be vacuous")


def test_the_walk_cannot_cross_into_the_previous_job():
    out, _ = _module().drop_job(FIXTURE, "second")
    assert "echo first" in out
    assert "# a step-level comment, indented deeper than a job comment" in out


def test_a_job_with_no_comment_block_keeps_its_separator():
    mod = _module()
    lo, _ = mod.job_span(FIXTURE, "fourth")
    assert mod.comment_block_start(FIXTURE, lo) == lo


def test_dropping_both_commented_jobs_leaves_exactly_one_blank_seam():
    mod = _module()
    out = FIXTURE
    for job in ("second", "third"):
        out, _ = mod.drop_job(out, job)
    assert "\n\n\n" not in out, "double blank left where the jobs were"
    lines = out.split("\n")
    i = lines.index("  fourth:")
    assert lines[i - 1] == "", (
        "the surviving job lost the blank line above it -- the deletion ate "
        "both separators and welded it onto the previous job")
    assert "      - run: echo first" in lines, "the previous job's body was damaged"


def test_the_real_ci_yml_loses_both_comment_blocks():
    """The derived invariant, on the tree the flip actually runs against."""
    yaml = pytest.importorskip("yaml")
    mod = _module()
    with open(CI_YML, encoding="utf-8") as fh:
        text = fh.read()
    before = len(yaml.safe_load(text)["jobs"])

    jobs = mod.DROP_JOBS["ci.yml"]
    assert jobs, "DROP_JOBS lost its ci.yml entry; this guard has no subject"
    for job in jobs:
        text, changed = mod.drop_job(text, job)
        assert changed, "%s was already absent from ci.yml" % job

    after = yaml.safe_load(text)["jobs"]
    assert len(after) == before - len(jobs)
    for job in jobs:
        assert job not in after

    # The header line of each dropped job's own comment block. Not a wordlist
    # sweep: these are the exact first lines of the two blocks, so a block left
    # behind is caught by its own opening sentence.
    for header in ("Data-pipeline correctness GATE",
                   "Data-pipeline SMOKE gate"):
        assert header not in text, (
            "the comment block opening %r outlived the job it documents" % header)
