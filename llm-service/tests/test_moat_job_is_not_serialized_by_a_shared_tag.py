"""A constant `concurrency` group cancels queued jobs; a cancelled job reads as a failure.

GitHub keeps exactly ONE pending job per concurrency group. `cancel-in-progress:
false` protects the job that is *running*; it does nothing for the one that is
*waiting*. When a third run queues, the second is CANCELLED -- and a cancelled
check renders on the PR as a red X that is indistinguishable from the guard
having run and found something.

That is what happened to `oss-leak-proof`, whose group existed only to stop two
runs overwriting the fixed image tags `rsync-oss-leaktest:{community,lifecycle}`.
Census over 60 ci.yml runs, taken when the group was removed:

    OSS images are moat-free            cancelled/0 steps  12   ran  41
    Data pipeline smoke gate (fast PR)  cancelled/0 steps   5   ran   7
    Data pipeline gate (batch + CDC)    cancelled/0 steps   5   ran   2
    OSS connector-lifecycle deploy      cancelled/0 steps   0   ran   2

`steps == 0` is the tell: a run that really executes this job has 6 steps and
takes 19-47s, and `actions/jobs/<id>/logs` returns BlobNotFound for a job that
produced none.

The moat job's collision was over a NAME, so it is fixed by making the name
unique per run rather than by serializing. The other three protect a genuine
singleton (one compose project, one host port) and cannot be parameterized away,
so they keep their group and are declared below -- the point of the declaration
is that adding a fourth is a deliberate act taken against the cost above, not an
edit nobody weighs.

Neither `helm lint`-style structural validation nor `actionlint` can see any of
this: a `concurrency:` block is valid YAML whatever it names, and a cancelled job
is a perfectly ordinary API state.
"""

import pathlib
import re

import pytest
import yaml

import _flip_cut

REPO = pathlib.Path(__file__).resolve().parents[2]
WORKFLOWS = REPO / ".github" / "workflows"
SCRIPT = REPO / "scripts" / "oss-leak-proof-test.sh"

MOAT_JOB = "oss-leak-proof"
SUFFIX_VAR = "LEAKTEST_TAG_SUFFIX"
IMAGE = "rsync-oss-leaktest"

# Jobs allowed to keep a constant group, and the singleton each one protects.
# An entry here is a statement that the resource CANNOT be made per-run -- not
# that the cancellation cost was acceptable. test_every_declared_job_still_has_a
# _constant_group below deletes the entry's cover if that stops being true, so a
# declaration cannot decay into a blind spot for a job somebody later fixed.
SERIALIZED_ON_PURPOSE = {
    ("ci.yml", "data-pipeline-gate"): (
        "drives the ONE shared compose project rsync-ci with hard-pinned "
        "container_names; the box cannot fit a second"
    ),
    ("ci.yml", "data-pipeline-smoke"): (
        "same compose project rsync-ci as data-pipeline-gate, which is why the "
        "two deliberately share one group"
    ),
    ("ci.yml", "oss-deploy-smoke"): (
        "fixed isolated resource names rsync-oss-smoke-* AND a fixed host port"
    ),
}

# `${{ ... }}` anywhere in the group means it varies per ref/run/PR, so the group
# cannot serialize unrelated work and this guard has nothing to say about it.
_INTERPOLATED = re.compile(r"\$\{\{")
# Second, independent measurement of the same quantity: an INDENTED `concurrency:`
# is job-level, a column-0 one is workflow-level. `[ \t]`, not `\s` -- `\s` matches
# a newline, so `^\s+` also matches a column-0 key preceded by a blank line and
# counted all four workflow-level blocks as job-level (7 vs 3). The denominator
# below caught that while this file was being written.
_JOB_LEVEL_LINE = re.compile(r"^[ \t]+concurrency:[ \t]*$", re.M)


def _workflows():
    return sorted(WORKFLOWS.glob("*.yml"))


def _load(p):
    # PyYAML resolves the bare key `on:` to the boolean True (YAML 1.1).
    return yaml.safe_load(p.read_text())


def _job_level_groups():
    """(workflow, job, group) for every job that declares its own concurrency."""
    out = []
    for p in _workflows():
        for name, job in (_load(p).get("jobs") or {}).items():
            conc = job.get("concurrency")
            if conc is None:
                continue
            group = conc if isinstance(conc, str) else str(conc.get("group", ""))
            out.append((p.name, name, group))
    return out


def _constant_groups():
    return [t for t in _job_level_groups() if not _INTERPOLATED.search(t[2])]


def _moat_job():
    doc = _load(WORKFLOWS / "ci.yml")
    job = (doc.get("jobs") or {}).get(MOAT_JOB)
    assert job is not None, (
        f"ci.yml has no job `{MOAT_JOB}`. If it was renamed, re-aim this file at "
        f"the new name -- do not delete it: the shape it holds is a property of "
        f"the leak proof, not of the string."
    )
    return job


def test_the_census_is_not_empty():
    """A zero here is not a pass.

    If the workflow directory moves or the YAML shape changes, every walk below
    returns nothing and the parametrised tests silently vanish while the suite
    reports green. Two independent measurements of one quantity -- a YAML walk
    and a regex over indented `concurrency:` lines -- pinned equal.
    """
    walked = _job_level_groups()
    grepped = sum(len(_JOB_LEVEL_LINE.findall(p.read_text())) for p in _workflows())
    assert walked, "found no job-level concurrency groups at all -- the walk is broken"
    assert len(walked) == grepped, (
        f"YAML walk found {len(walked)} job-level concurrency groups, the regex "
        f"found {grepped}. One of the two is not seeing the file's real shape."
    )


@pytest.mark.parametrize(
    "wf,job,group",
    [pytest.param(*t, id=f"{t[0]}-{t[1]}") for t in _constant_groups()],
)
def test_a_constant_group_is_declared_with_the_singleton_it_protects(wf, job, group):
    assert (wf, job) in SERIALIZED_ON_PURPOSE, (
        f"{wf} job `{job}` declares the constant concurrency group `{group}`.\n\n"
        f"A constant group holds ONE pending job: the moment a third run queues, "
        f"the second is CANCELLED, the job does not execute, and the PR shows a "
        f"red X that reads exactly like the check having failed. Measured cost "
        f"when `{MOAT_JOB}` carried one: 12 of 53 non-skipped runs cancelled with "
        f"zero steps.\n\n"
        f"If the collision is over a NAME (a tag, a file, a container name), make "
        f"the name per-run instead -- that is what `{SUFFIX_VAR}` does for the "
        f"moat job. If it is a genuine singleton, add `('{wf}', '{job}')` to "
        f"SERIALIZED_ON_PURPOSE saying which one."
    )


@pytest.mark.parametrize(
    "wf,job", [pytest.param(*k, id=f"{k[0]}-{k[1]}") for k in SERIALIZED_ON_PURPOSE]
)
def test_every_declared_job_still_has_a_constant_group(wf, job):
    """An exemption must not outlive its subject.

    A declaration for a job that was renamed, deleted, or since parameterized is
    dead cover: it would silently re-admit a constant group under a name somebody
    already fixed.
    """
    match = [t for t in _job_level_groups() if (t[0], t[1]) == (wf, job)]
    if not match and not _flip_cut.is_a_pre_cut_tree():
        # The public flip DROPS data-pipeline-gate and data-pipeline-smoke: both
        # drive the one shared 23-container compose project, which a 2-core hosted
        # runner cannot bring up, and the self-hosted Macs are organisation-level
        # runners a public repo is not admitted to. So over there this entry names
        # a job that is SUPPOSED to be gone, and the same literal list is dead
        # cover in one tree and load-bearing in the other -- the asymmetry this
        # split keeps producing, in a data table rather than a comment.
        #
        # Not a skip, because a skip is the silent version of the defect: the
        # assertion is re-aimed at what is true over there. A cut tree must have
        # dropped the job WHOLE, not merely un-serialized it, and that is checked.
        # Nothing is lost either way -- a RENAME still fails in both trees via
        # test_a_constant_group_is_declared_with_the_singleton_it_protects, since
        # the new name is absent from SERIALIZED_ON_PURPOSE. What the branch below
        # gives up is only the whole-job DELETION case, and the tree where this
        # list is edited, and where such a deletion would be authored, is this one.
        jobs = _load(WORKFLOWS / wf).get("jobs") or {}
        assert job not in jobs, (
            f"{wf} job `{job}` exists in this cut tree but declares no concurrency "
            f"group, while SERIALIZED_ON_PURPOSE still exempts it. That is dead "
            f"cover here too: delete the entry, or restore the group."
        )
        return
    assert match, (
        f"SERIALIZED_ON_PURPOSE declares {wf} job `{job}`, which no longer has a "
        f"job-level concurrency group (or no longer exists). Delete the entry."
    )
    assert not _INTERPOLATED.search(match[0][2]), (
        f"{wf} job `{job}` now interpolates its group (`{match[0][2]}`), so it is "
        f"no longer serialized repo-wide. Delete its SERIALIZED_ON_PURPOSE entry."
    )


def test_the_moat_job_declares_no_concurrency_group():
    """The regression this file exists for."""
    assert "concurrency" not in _moat_job(), (
        f"`{MOAT_JOB}` has a job-level concurrency group again. Its only "
        f"collision is the image tag, and {SUFFIX_VAR} already makes that "
        f"per-run; a group here buys nothing and costs a cancelled guard."
    )


def test_the_moat_job_scopes_its_tags_to_the_run():
    env = _moat_job().get("env") or {}
    assert SUFFIX_VAR in env, (
        f"`{MOAT_JOB}` no longer sets {SUFFIX_VAR}. Without it the script falls "
        f"back to the fixed tags and two concurrent runs overwrite each other "
        f"mid-scan -- silently, since the scan would still find a real image."
    )
    assert "github.run_id" in str(env[SUFFIX_VAR]), (
        f"{SUFFIX_VAR} is `{env[SUFFIX_VAR]}`, which does not vary per run. "
        f"A suffix that is the same for every run is the fixed tag again."
    )


def test_the_script_builds_its_tag_from_the_suffix():
    body = SCRIPT.read_text()
    tags = [ln for ln in body.splitlines() if f'"{IMAGE}:' in ln]
    assert tags, f"no line in {SCRIPT.name} builds a {IMAGE} tag -- re-aim this test"
    for ln in tags:
        assert SUFFIX_VAR in ln, (
            f"{SCRIPT.name} builds a tag without {SUFFIX_VAR}:\n  {ln.strip()}\n"
            f"Every tag the script creates must be run-scoped, or concurrent runs "
            f"collide on the one that is not."
        )


def test_the_cleanup_removes_this_runs_tags_and_not_the_bare_ones():
    """The second collision the mutex was masking.

    With unique tags but a fixed teardown, run A's cleanup would untag run B's
    image while B was still scanning it.
    """
    steps = _moat_job().get("steps") or []
    rmi = [s for s in steps if "docker rmi" in str(s.get("run", ""))]
    assert rmi, f"`{MOAT_JOB}` no longer removes its images; the box is long-lived"
    for step in rmi:
        run = str(step["run"])
        assert SUFFIX_VAR in run, (
            f"cleanup step `{step.get('name')}` does not scope to {SUFFIX_VAR}:\n{run}"
        )
        for bare in (f"{IMAGE}:community ", f"{IMAGE}:lifecycle "):
            assert bare not in run, (
                f"cleanup step `{step.get('name')}` still names the unscoped tag "
                f"`{bare.strip()}`, which belongs to whatever OTHER run built it."
            )
