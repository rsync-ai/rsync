"""Every self-hosted job reachable from a fork PR must carry the fork guard.

WHY THIS EXISTS. The repo's self-hosted runners are long-lived Macs that also
hold the prod deploy key (ci.yml: "these runners double as dev machines", "The
runner is a long-lived box"). While the repo is private that is fine -- nobody
can open a PR. The hour it goes public, every `pull_request`-triggered job on
those boxes becomes reachable by any GitHub account that clicks Fork.

The guard that WAS here did not do this. `github.repository == '<owner>/<repo>'`
names the BASE repo on a `pull_request` event, so for a PR from a fork against
this repo it evaluates TRUE. It only ever skipped runs happening *inside*
someone's own fork -- an availability guard (forks have no runner), not a
security one, which is exactly what its comment said it was for. It is written
here without a literal slug on purpose: the flaw is in the comparison's
semantics, so naming a repo would suggest renaming one could fix it.

SCOPE -- read this before trusting the guard. A `pull_request` run executes the
workflow file FROM THE PR HEAD, so an attacker can simply delete the `if:` line
this test enforces. The guard is defense-in-depth; it stops an unmodified fork
PR, not a hostile one. The control that actually holds is the repository setting
Settings -> Actions -> "Require approval for all external contributors", which
lives outside the tree and therefore cannot be asserted here. This test exists
so the in-tree half does not silently rot, not so the setting can be skipped.
"""
from pathlib import Path

import re

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"

# The disjunct is load-bearing. On a non-pull_request event
# `github.event.pull_request` is null, so `...head.repo.full_name` renders as ''
# while `github.repository` does not -- a bare equality is FALSE and would
# disable every job on push and schedule. Bug shape: CI goes quiet, green.
REQUIRED = "github.event.pull_request.head.repo.full_name == github.repository"
EVENT_DISJUNCT = "github.event_name != 'pull_request'"

# Matches the `runs-on: [self-hosted, ...]` line form ci.yml uses throughout.
_SELF_HOSTED_RUNS_ON = re.compile(r"^\s*runs-on:\s*\[\s*self-hosted", re.M)


def _workflows(root=None):
    return sorted((root or WORKFLOWS).glob("*.yml"))


def _load(p):
    d = yaml.safe_load(p.read_text())
    # PyYAML resolves the bare key `on:` to the boolean True (YAML 1.1).
    return d, (d.get(True) or d.get("on") or {})


def _pr_reachable_self_hosted(root=None):
    out = []
    for p in _workflows(root):
        doc, on = _load(p)
        if "pull_request" not in (on if isinstance(on, dict) else {str(on): 1}):
            continue
        for name, job in (doc.get("jobs") or {}).items():
            if "self-hosted" in str(job.get("runs-on", "")):
                out.append((p.name, name, str(job.get("if", ""))))
    return out


def _grepped_self_hosted_lines(root=None):
    """Count self-hosted `runs-on:` LINES in PR-triggered workflows, by regex.

    Deliberately a second, independent measurement of the quantity
    _pr_reachable_self_hosted() derives by parsing YAML. Comparing the two is
    what makes the denominator below self-calibrating.
    """
    total = 0
    for p in _workflows(root):
        _doc, on = _load(p)
        if "pull_request" not in (on if isinstance(on, dict) else {str(on): 1}):
            continue
        total += len(_SELF_HOSTED_RUNS_ON.findall(p.read_text()))
    return total


# A workflow with one self-hosted job and one hosted one, triggered by
# pull_request. Both mechanisms must report exactly 1 against it. It is written
# out to a tmp_path rather than kept as a file in the tree so that nothing in
# CI can ever try to run it.
_PR_TRIGGERED_FIXTURE = """\
name: fixture (pull_request-triggered)
on:
  pull_request:
    branches: [main]
jobs:
  hosted:
    runs-on: ubuntu-latest
    steps:
      - run: "true"
  on-the-macs:
    runs-on: [self-hosted, macOS, ARM64]
    steps:
      - run: "true"
"""

# The same job, on a workflow no pull_request can reach. Both mechanisms must
# report 0. Without this second fixture the first one proves only that the
# mechanisms count SOMETHING, not that the pull_request filter discriminates.
_SCHEDULE_ONLY_FIXTURE = """\
name: fixture (schedule-only)
on:
  schedule:
    - cron: "0 3 * * *"
jobs:
  on-the-macs:
    runs-on: [self-hosted, macOS, ARM64]
    steps:
      - run: "true"
"""


def _fixture_dir(tmp_path, name, body):
    d = tmp_path / name
    d.mkdir()
    (d / "fixture.yml").write_text(body)
    return d


def test_both_mechanisms_see_a_self_hosted_job_that_is_known_to_be_there(tmp_path):
    """The positive control. Without it the equality below can pass on nothing.

    This test exists because of what the previous version of it did. The floor
    was the literal `len(jobs) >= 15`, calibrated to the private tree, which
    made the suite fail BY CONSTRUCTION at the one moment it matters most --
    the public flip's CI split, which correctly brings the census down. That
    was recognised and the literal was lowered to `>= 2`, on the reasoning that
    the two data-pipeline gate jobs stay self-hosted forever. They do not: the
    org runner group does not admit public repositories, so in the public tree
    those two jobs can never be allocated a runner and the correct census there
    is ZERO. A floor of 2 is the same defect one notch down -- a number in this
    file mirroring a fact that lives in `.github/workflows`, going red on
    exactly the change the tree needs.

    The invariant that is actually wanted is "both mechanisms still work", and
    that is a property of the mechanisms, not of the repo's current job count.
    So it is measured where the answer is known by construction: a synthetic
    workflow written to a temp directory. The real tree is then held only to
    the cross-check the two mechanisms make of each other, which needs no
    number at all and is true at 23, at 2, and at 0.
    """
    root = _fixture_dir(tmp_path, "pr-triggered", _PR_TRIGGERED_FIXTURE)

    walked = _pr_reachable_self_hosted(root)
    grepped = _grepped_self_hosted_lines(root)

    assert [j for _wf, j, _c in walked] == ["on-the-macs"], (
        f"the YAML walk found {walked} in a fixture containing exactly one "
        "self-hosted job on a pull_request-triggered workflow. The walk is no "
        "longer recognising the `runs-on` shape, or no longer reading `on:`."
    )
    assert grepped == 1, (
        f"the regex found {grepped} self-hosted `runs-on:` line(s) in a fixture "
        "containing exactly one. _SELF_HOSTED_RUNS_ON has stopped matching the "
        "line form the workflows use."
    )


def test_neither_mechanism_counts_a_job_no_pull_request_can_reach(tmp_path):
    """The negative control, which is what makes the positive one mean anything.

    A mechanism that counted every self-hosted job regardless of trigger would
    also return 1 above. Both must return 0 here.
    """
    root = _fixture_dir(tmp_path, "schedule-only", _SCHEDULE_ONLY_FIXTURE)

    assert _pr_reachable_self_hosted(root) == [], (
        "the YAML walk counted a self-hosted job on a schedule-only workflow, "
        "so its pull_request filter is not discriminating and the census above "
        "is measuring the wrong set."
    )
    assert _grepped_self_hosted_lines(root) == 0, (
        "the regex counted a self-hosted `runs-on:` line in a schedule-only "
        "workflow, so it is not honouring the pull_request filter either."
    )


def test_the_two_mechanisms_agree_about_the_real_tree():
    """A zero here is not a pass on its own -- the controls above are what arm it.

    If a refactor renames `runs-on` targets or moves the workflow directory,
    every parametrised test below silently vanishes and the suite reports green
    while enforcing nothing. That is guarded in two halves. The fixtures above
    prove both mechanisms still see a self-hosted job when there is one to see;
    this test then requires the two to agree about the real tree.

    There is deliberately no floor on the count. The correct number is 23 in
    the private tree, 2 in a tree with §4a-4c of the flip runbook applied, and
    0 in the public tree, whose runner group admits no public repository -- so
    any literal here would be a number in one file mirroring a fact in another,
    which is the drift shape this repo keeps paying for.
    """
    files = _workflows()
    assert len(files) >= 4, f"expected >=4 workflow files, found {[f.name for f in files]}"

    jobs = _pr_reachable_self_hosted()
    grepped = _grepped_self_hosted_lines()

    assert len(jobs) == grepped, (
        f"the YAML walk found {len(jobs)} PR-reachable self-hosted job(s) but a "
        f"regex over the same files found {grepped} self-hosted `runs-on:` line(s). "
        "Every guard assertion below is parametrised on the YAML walk, so the "
        "difference is exactly the set of jobs nothing is checking. Two things "
        "produce this: a `runs-on` shape the walk's `\"self-hosted\" in str(...)` "
        "test does not recognise, or a job the parser dropped."
    )


@pytest.mark.parametrize(
    "wf,job,cond",
    [pytest.param(w, j, c, id=f"{w}::{j}") for w, j, c in _pr_reachable_self_hosted()],
)
def test_self_hosted_job_carries_the_fork_guard(wf, job, cond):
    assert REQUIRED in cond, (
        f"{wf} job '{job}' runs on a self-hosted runner and is reachable from a "
        f"fork pull_request, but its `if:` does not contain the head-repo guard.\n"
        f"  if: {cond or '<none>'}\n"
        f"Add:  {EVENT_DISJUNCT} || {REQUIRED}"
    )
    assert EVENT_DISJUNCT in cond, (
        f"{wf} job '{job}' has the head-repo equality but not the event_name "
        f"disjunct, so it will evaluate FALSE on push/schedule and the job will "
        f"stop running entirely.\n  if: {cond}"
    )


def test_the_old_ineffective_guard_is_not_reintroduced_alone():
    """`github.repository ==` may remain, but never as the only guard."""
    offenders = [
        (wf, job)
        for wf, job, cond in _pr_reachable_self_hosted()
        if "github.repository ==" in cond and REQUIRED not in cond
    ]
    assert not offenders, (
        f"these jobs rely on `github.repository ==` as their fork guard: {offenders}. "
        "On a fork PR that predicate names the BASE repo and evaluates TRUE."
    )
