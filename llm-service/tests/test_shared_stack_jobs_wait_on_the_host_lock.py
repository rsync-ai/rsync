"""The jobs that share the rsync-ci stack must WAIT for each other, not cancel.

`data-pipeline-gate` (nightly) and `data-pipeline-smoke` (per PR) drive the one
`rsync-ci` compose project, and all four self-hosted runners are on one Mac, so
two of them at once would tear down each other's stack. They used to be
serialized by a shared GitHub `concurrency` group. A group holds ONE pending job,
so every new arrival CANCELLED the one waiting ahead of it: 34 smoke gates in 100
ci.yml runs never ran a step, and on 2026-09-27 the nightly gate was cancelled the
same way (KI-SHARED-STACK-GATES-CANCEL-EACH-OTHER).

They are now serialized only by the host lock `e2e/run_gate.sh` takes
(`scripts/_stack_lock.sh`), which queues instead of cancelling. That swaps one
failure for another unless the numbers line up, because the lock gives up after
`STACK_LOCK_TIMEOUT` (default 300 s -- shorter than either job):

  * each job's wait must outlast the OTHER job's hold, or waiting becomes a
    lock-timeout failure -- a red X with a different wrong explanation;
  * each job's wait plus its own hold must fit in its `timeout-minutes`, or the
    runner kills a job that was still legitimately waiting.

Neither YAML validation nor actionlint can see either: every number here is a
valid number on its own.
"""

import pathlib
import re

import pytest
import yaml

import _flip_cut

REPO = pathlib.Path(__file__).resolve().parents[2]
CI = REPO / ".github" / "workflows" / "ci.yml"
GATE_SCRIPT = REPO / "e2e" / "run_gate.sh"
LOCK_LIB = REPO / "scripts" / "_stack_lock.sh"

# The longest a job may HOLD the lock once it has it, in minutes. Measured
# 2026-09-27 over the last 100 ci.yml runs: the smoke's longest real run was
# 16.7 min (its old timeout-minutes, 25, is the budget); the full gate ran 17 min
# green (2026-09-22) and 58 min failing (2026-09-25, 480 s per-test timeouts).
HOLD_BUDGET_MIN = {
    "data-pipeline-gate": 60,
    "data-pipeline-smoke": 25,
}


def _jobs():
    return yaml.safe_load(CI.read_text()).get("jobs") or {}


def _gate_step(job):
    steps = [s for s in job.get("steps") or [] if "e2e/run_gate.sh" in str(s.get("run", ""))]
    assert len(steps) == 1, f"expected ONE step running e2e/run_gate.sh, found {len(steps)}"
    return steps[0]


def _lockers():
    """Every ci.yml job that runs e2e/run_gate.sh, i.e. takes the host lock."""
    return {
        name: job
        for name, job in _jobs().items()
        if any("e2e/run_gate.sh" in str(s.get("run", "")) for s in job.get("steps") or [])
    }


def _lock_wait_min(name, job):
    env = _gate_step(job).get("env") or {}
    raw = str(env.get("STACK_LOCK_TIMEOUT", "")).strip()
    assert raw.isdigit() and int(raw) > 0, (
        f"`{name}` runs e2e/run_gate.sh without a positive STACK_LOCK_TIMEOUT "
        f"(got {raw!r}). The 300 s default is shorter than either job's hold, so "
        f"the lock would fail a job that only had to wait; 0 (forever) hands the "
        f"cap to timeout-minutes, which kills the job without naming the holder."
    )
    return int(raw) / 60


def test_the_census_is_the_two_known_jobs():
    """A zero here is not a pass, and a third locker needs a hold budget."""
    lockers = set(_lockers())
    if not lockers and not _flip_cut.is_a_pre_cut_tree():
        # The public cut drops both jobs whole (scripts/flip/apply-ci-split.py);
        # there is nothing to serialize there. Dropped WHOLE is still checked.
        jobs = _jobs()
        assert not set(HOLD_BUDGET_MIN) & set(jobs)
        return
    assert lockers == set(HOLD_BUDGET_MIN), (
        f"jobs running e2e/run_gate.sh: {sorted(lockers)}; HOLD_BUDGET_MIN declares "
        f"{sorted(HOLD_BUDGET_MIN)}. A new job taking the host lock needs a measured "
        f"hold budget here, or the waits below are sized against a job they never saw."
    )


def _pairs():
    if not _lockers():
        return []
    return [pytest.param(n, id=n) for n in sorted(_lockers())]


@pytest.mark.parametrize("name", _pairs())
def test_no_github_concurrency_group(name):
    job = _lockers()[name]
    assert "concurrency" not in job, (
        f"`{name}` has a job-level concurrency group again. It is serialized by the "
        f"host lock in e2e/run_gate.sh; a GitHub group on top keeps ONE pending job "
        f"and cancels it when the next one queues -- the phantom this file replaced."
    )


def test_all_lockers_share_one_stack():
    """The premise: same STACK_PREFIX means same lock dir means real contention."""
    if not _lockers() and not _flip_cut.is_a_pre_cut_tree():
        pytest.skip("the public cut drops both lockers whole; the census test checks that")
    prefixes = {
        n: (_gate_step(j).get("env") or {}).get("STACK_PREFIX") for n, j in _lockers().items()
    }
    assert len(set(prefixes.values())) == 1, (
        f"the lockers no longer share one STACK_PREFIX: {prefixes}. They then take "
        f"different locks and drive different stacks; re-aim this file."
    )


@pytest.mark.parametrize("name", _pairs())
def test_the_wait_outlasts_every_other_holder(name):
    wait = _lock_wait_min(name, _lockers()[name])
    for other in _lockers():
        if other == name:
            continue
        assert wait >= HOLD_BUDGET_MIN[other], (
            f"`{name}` waits {wait:g} min for the host lock, but `{other}` may hold "
            f"it for {HOLD_BUDGET_MIN[other]} min. A job queued behind it would fail "
            f"on the lock timeout instead of running."
        )


@pytest.mark.parametrize("name", _pairs())
def test_the_wait_plus_the_run_fits_the_job_timeout(name):
    job = _lockers()[name]
    wait = _lock_wait_min(name, job)
    budget = int(job.get("timeout-minutes", 360))
    need = wait + HOLD_BUDGET_MIN[name]
    assert need <= budget, (
        f"`{name}`: {wait:g} min lock wait + {HOLD_BUDGET_MIN[name]} min own run = "
        f"{need:g} min, over its timeout-minutes {budget}. The runner would kill a "
        f"job that was still legitimately waiting."
    )


def test_the_env_reaches_the_lock():
    """The waits above are fiction unless the lock reads the variable they set."""
    lib = LOCK_LIB.read_text()
    assert re.search(r'timeout="\$\{STACK_LOCK_TIMEOUT:-\d+\}"', lib), (
        f"{LOCK_LIB.name} no longer reads STACK_LOCK_TIMEOUT from the environment"
    )
    gate = GATE_SCRIPT.read_text()
    assert re.search(r"^acquire_stack_lock ", gate, re.M), (
        f"{GATE_SCRIPT.name} no longer calls acquire_stack_lock"
    )
    assert not re.search(r"^\s*(export\s+)?STACK_LOCK_TIMEOUT=", gate, re.M), (
        f"{GATE_SCRIPT.name} assigns STACK_LOCK_TIMEOUT itself, overriding the wait "
        f"ci.yml sets"
    )
