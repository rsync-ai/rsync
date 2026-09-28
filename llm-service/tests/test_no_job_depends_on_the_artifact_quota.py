"""No job's verdict may depend on the account's artifact storage quota.

That quota is shared by every workflow and it RUNS OUT: no artifact has landed
since 2026-08-23, and an exhausted quota fails `actions/upload-artifact` outright
("Artifact storage quota has been hit"). On the 2026-09-27 nightly that turned
the Frontend job red on a build that had passed -- its "Cache build output" step
was an upload-artifact handing `.next` to frontend-a11y -- and skipped a11y.

So, for every workflow:

  * an `upload-artifact` step is a convenience copy: it carries
    `continue-on-error: true`, itself or through its job;
  * nothing hands a job its INPUT through `download-artifact`, since a consumer
    cannot soft-fail on a missing input. Hand-offs go through the actions cache
    (a separate 10 GB per-repo budget), `actions/cache/save` -> `/restore`;
  * each such restore pairs with a save in a job it `needs`, on the same `path`
    (part of the cache version, so a respelling is a silent miss) and the same
    key, or a restore-keys prefix of it.
"""

import pathlib

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
WORKFLOWS = sorted((REPO / ".github" / "workflows").glob("*.y*ml"))

UPLOAD = "actions/upload-artifact"
DOWNLOAD = "actions/download-artifact"
SAVE = "actions/cache/save"
RESTORE = "actions/cache/restore"


def _uses(step):
    return str(step.get("uses", "")).split("@")[0]


def _steps():
    """(workflow, job name, job, step) for every step of every workflow."""
    for wf in WORKFLOWS:
        jobs = (yaml.safe_load(wf.read_text()) or {}).get("jobs") or {}
        for name, job in jobs.items():
            for step in job.get("steps") or []:
                yield wf, name, job, step


def _where(wf, name, step):
    return f"{wf.name} › {name} › {step.get('name') or _uses(step)}"


def test_the_census_is_not_empty():
    """Zero upload-artifact steps would make the next test pass on nothing."""
    assert any(_uses(s) == UPLOAD for *_, s in _steps()), (
        "no workflow step uses actions/upload-artifact; the soft-fail check "
        "below would be vacuous -- re-aim this file"
    )


@pytest.mark.parametrize(
    "wf,name,job,step",
    [pytest.param(*t, id=_where(t[0], t[1], t[3])) for t in _steps() if _uses(t[3]) == UPLOAD],
)
def test_every_artifact_upload_is_soft(wf, name, job, step):
    assert step.get("continue-on-error") is True or job.get("continue-on-error") is True, (
        f"{_where(wf, name, step)} uploads an artifact without continue-on-error. "
        f"The artifact storage quota is shared and runs out; when it does this step "
        f"fails, and the job goes red for a reason that is not its code."
    )


def test_no_job_takes_its_input_from_an_artifact():
    bad = [_where(wf, n, s) for wf, n, _, s in _steps() if _uses(s) == DOWNLOAD]
    assert not bad, (
        f"{bad} hand a job its input through download-artifact, which only works "
        f"while the artifact quota has room. Hand it over through the actions cache "
        f"(actions/cache/save in the producer, actions/cache/restore in the consumer)."
    )


def _restores():
    return [t for t in _steps() if _uses(t[3]) == RESTORE]


@pytest.mark.parametrize(
    "wf,name,job,step", [pytest.param(*t, id=_where(t[0], t[1], t[3])) for t in _restores()]
)
def test_every_cache_handoff_restores_what_was_saved(wf, name, job, step):
    jobs = (yaml.safe_load(wf.read_text()) or {}).get("jobs") or {}
    needs = job.get("needs") or []
    needs = [needs] if isinstance(needs, str) else needs
    want = step.get("with") or {}
    saves = [
        s.get("with") or {}
        for n in needs
        for s in jobs.get(n, {}).get("steps") or []
        if _uses(s) == SAVE
    ]
    assert saves, (
        f"{_where(wf, name, step)} restores a cache, but no job it `needs` saves one. "
        f"It would restore whatever an earlier run left, or nothing."
    )
    same_path = [s for s in saves if s.get("path") == want.get("path")]
    assert same_path, (
        f"{_where(wf, name, step)} restores path {want.get('path')!r}; the saves in "
        f"{needs} use {[s.get('path') for s in saves]}. `path` is part of the cache "
        f"version, so a different spelling never matches."
    )
    # One prefix per LINE: a key is an expression with spaces inside `${{ }}`, so
    # a whitespace split yields `frontend-build-${{`, a prefix of every key.
    restore_keys = str(want.get("restore-keys") or "").splitlines()
    prefixes = [str(want.get("key", "")).strip()] + [k.strip() for k in restore_keys if k.strip()]
    for save in same_path:
        key = str(save.get("key", ""))
        assert any(p and key.startswith(p) for p in prefixes), (
            f"{_where(wf, name, step)} looks for {prefixes}, none of which matches the "
            f"saved key {key!r}"
        )
    assert want.get("fail-on-cache-miss") is True, (
        f"{_where(wf, name, step)} restores a build hand-off without "
        f"fail-on-cache-miss, so a miss surfaces later as a confusing missing file"
    )
