"""The flip-day CI split must apply to the workflows as they are TODAY.

WHAT BROKE. ``scripts/flip/apply-ci-split.py`` moves or drops every self-hosted job
by NAME, and refuses -- after doing the work -- if any ``runs-on: [self-hosted, ...]``
survives, because a survivor queues forever on a public repo. That refusal is the
right behaviour and it fires at the right place, but the place is flip day: nothing
ran the script against the real ``.github/workflows`` on a PR. Every guard that
reads it (``test_flip_drops_*``, ``test_flip_refuses_*``) exercises a piece of the
pipeline on a fixture or on ONE file, so none of them can see a job that is in
neither ``MOVE_TO_HOSTED`` nor ``DROP_JOBS``.

Measured 2026-09-21, with 242 flip guards green: the script REFUSED on the current
tree, for two jobs added since it was last run -- ``kafka-security-matrix`` (#1092,
#1094) and ``public-cut`` (#1104, a job whose script ``scripts/flip`` the cut
deletes). Each was added by a PR that had no reason to know the list existed.

WHY THE FIX IS THE WHOLE SCRIPT. Re-deriving "which jobs are self-hosted" inside a
test would re-implement the tool and agree with it by construction. The subject is
the tool's own verdict on the real tree, so the test runs it, as a subprocess, on a
copy of the real workflows, and reads the exit code. A new self-hosted job in any
workflow now fails the PR that adds it, with the job named.

THE CONTROL. ``test_a_new_self_hosted_job_is_refused_by_name`` appends a job the
script has never heard of to a copy of a real workflow and asserts the refusal. If
the script were changed to ignore survivors, the first test would keep passing on
the real tree; the control is what makes it a guard rather than a status line.
"""

import os
import shutil
import subprocess
import sys

import pytest

import _flip_cut

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SPLIT = os.path.join(REPO_ROOT, "scripts", "flip", "apply-ci-split.py")
GATE = os.path.join(REPO_ROOT, "scripts", "flip", "assert-ci-split.py")
WORKFLOWS = os.path.join(REPO_ROOT, ".github", "workflows")
# Named as files, not only reached through WORKFLOWS, because the census
# (test_ci_filter_covers_every_guard_subject.py) derives a guard's subjects from the file
# paths it spells out. Without one that survives the cut, this guard has no subject in
# the public tree and that census fails there instead of skipping.
CI_YML = os.path.join(WORKFLOWS, "ci.yml")
DOC_LINKS = os.path.join(REPO_ROOT, ".github", "workflows", "doc-links.yml")

SELF_HOSTED = "runs-on: [self-hosted, macOS, ARM64]"


@pytest.fixture(autouse=True)
def _only_before_the_cut():
    """The subject is flip tooling, which the cut deletes; see _flip_cut."""
    _flip_cut.require_a_pre_cut_tree()


def _copy_of_the_real_workflows(tmp_path):
    dest = tmp_path / "workflows"
    dest.mkdir()
    for name in os.listdir(WORKFLOWS):
        if name.endswith((".yml", ".yaml")):
            shutil.copy(os.path.join(WORKFLOWS, name), dest / name)
    return dest


def _run(script, *args):
    return subprocess.run(
        [sys.executable, script, *args], capture_output=True, text=True, timeout=120
    )


def test_the_split_applies_to_the_real_workflows(tmp_path):
    """Every self-hosted job in the tree is one the script knows what to do with."""
    wf = _copy_of_the_real_workflows(tmp_path)
    assert SELF_HOSTED in open(CI_YML).read() and any(
        SELF_HOSTED in p.read_text() for p in wf.iterdir()
    ), (
        "the copied workflows carry no self-hosted job at all -- the private tree "
        "always does, so this is reading the wrong directory and would pass vacuously"
    )
    done = _run(SPLIT, "--workflows", str(wf))
    assert done.returncode == 0, (
        "apply-ci-split.py refuses on the current .github/workflows, so flip day "
        "would halt. Add each named job to MOVE_TO_HOSTED (having proved it runs on "
        "ubuntu-latest) or to DROP_JOBS.\n" + done.stdout + done.stderr
    )
    survivors = [p.name for p in wf.iterdir() if SELF_HOSTED in p.read_text()]
    assert not survivors, f"self-hosted runs-on survived the split in {survivors}"


def test_the_produced_tree_passes_the_ci_split_gate(tmp_path):
    """The script performs edits; assert-ci-split.py is what judges the result."""
    wf = _copy_of_the_real_workflows(tmp_path)
    assert _run(SPLIT, "--workflows", str(wf)).returncode == 0
    judged = _run(GATE, "--workflows", str(wf), "--repo", "rsync-ai/rsync")
    assert judged.returncode == 0, judged.stdout + judged.stderr


def test_a_new_self_hosted_job_is_refused_by_name(tmp_path):
    """The control: a job in neither list must make the script refuse and name it."""
    wf = _copy_of_the_real_workflows(tmp_path)
    target = wf / os.path.basename(DOC_LINKS)
    target.write_text(
        open(DOC_LINKS).read()
        + f"\n  brand-new-gate:\n    {SELF_HOSTED}\n    steps:\n      - run: echo hi\n"
    )
    done = _run(SPLIT, "--workflows", str(wf))
    assert done.returncode != 0, "a self-hosted job in neither list was silently accepted"
    assert "brand-new-gate" in done.stdout + done.stderr, (
        "the refusal must name the job, or the next person has to write a scan to "
        "find it:\n" + done.stdout + done.stderr
    )
