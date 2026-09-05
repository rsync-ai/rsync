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


def _workflows():
    return sorted(WORKFLOWS.glob("*.yml"))


def _load(p):
    d = yaml.safe_load(p.read_text())
    # PyYAML resolves the bare key `on:` to the boolean True (YAML 1.1).
    return d, (d.get(True) or d.get("on") or {})


def _pr_reachable_self_hosted():
    out = []
    for p in _workflows():
        doc, on = _load(p)
        if "pull_request" not in (on if isinstance(on, dict) else {str(on): 1}):
            continue
        for name, job in (doc.get("jobs") or {}).items():
            if "self-hosted" in str(job.get("runs-on", "")):
                out.append((p.name, name, str(job.get("if", ""))))
    return out


def _grepped_self_hosted_lines():
    """Count self-hosted `runs-on:` LINES in PR-triggered workflows, by regex.

    Deliberately a second, independent measurement of the quantity
    _pr_reachable_self_hosted() derives by parsing YAML. Comparing the two is
    what makes the denominator below self-calibrating.
    """
    total = 0
    for p in _workflows():
        _doc, on = _load(p)
        if "pull_request" not in (on if isinstance(on, dict) else {str(on): 1}):
            continue
        total += len(_SELF_HOSTED_RUNS_ON.findall(p.read_text()))
    return total


def test_the_census_is_not_empty():
    """A zero here is not a pass.

    If a refactor renames `runs-on` targets or moves the workflow directory,
    every parametrised test below silently vanishes and the suite reports green
    while enforcing nothing. Pin the denominator so that failure is loud.

    The floor used to be the literal `>= 15`, calibrated to the private tree.
    That made this test fail BY CONSTRUCTION at the one moment it matters most:
    the public flip's CI split (public-flip-runbook.md 4a-4d) moves every job
    except the two data-pipeline gate jobs to ubuntu-latest, so the correct
    census becomes 2 and a floor of 15 goes red. The runbook's answer was for
    the flip-day diff to hand-edit the number in the same commit -- a literal
    hand-mirroring a fact that lives in another file, which is the drift shape
    this repo keeps paying for, and whose failure mode here is an operator
    reading a red suite as "a guard regressed" and skipping the gate.

    So the floor is derived instead. The invariant is not "there are >= N jobs",
    it is "the YAML walk saw every self-hosted job there is" -- checked against
    a regex count of the same lines, an independent mechanism. It holds at 23
    today and at 2 after the split, with no number to maintain in either place.
    """
    files = _workflows()
    assert len(files) >= 4, f"expected >=4 workflow files, found {[f.name for f in files]}"

    jobs = _pr_reachable_self_hosted()
    grepped = _grepped_self_hosted_lines()

    assert grepped >= 2, (
        f"only {grepped} self-hosted `runs-on:` line(s) in PR-triggered workflows. "
        "Even after the flip's CI split the two data-pipeline gate jobs stay "
        "self-hosted, so a count below 2 means the regex stopped matching or the "
        "workflows moved -- not that the runners were retired."
    )
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
