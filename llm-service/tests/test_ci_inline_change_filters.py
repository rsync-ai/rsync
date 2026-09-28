"""The inlined change filters cannot go silently green.

WHY THIS EXISTS. ci.yml used to detect changes in one `changes` job that nine
other jobs waited on through `needs:`. On this fleet -- four self-hosted agents
serving every workflow in the repo -- that `needs:` did not cost the ~11 s the
job ran, it cost a second traversal of the runner queue: measured across runs
36025851448, 36024668512 and 36023391830, `Detect changes` waited 495-925 s for
a slot and its dependents waited another 153-907 s after it released one. So the
filters moved to .github/paths-filters.yml and each job now runs
dorny/paths-filter itself and gates its own steps on `env.RUN`.

That trade buys ~8-15 min of wall clock and takes on ONE new failure mode, which
is the whole subject of this file. Under `needs:`, a job with nothing to do was
skipped by the engine -- there was no way for it to report success having done
nothing. Now the job always starts, and "do nothing" is expressed by every step
carrying `if: env.RUN == '1'`. Miss that `if:` on one step and the step runs on
PRs it was never meant to. Miss the gate the other way -- leave a step outside
the RUN set that should have been inside -- and the job goes green having
checked nothing, which is exactly KI-DATAPATH-FILTER-EXCLUDES-THE-WORKFLOW:
a check that cannot fail for the right reason.

REACHABILITY. This guard runs in the `llm-service unit tests` job, which is
itself gated on the `llm` filter. `llm` lists both .github/workflows/ci.yml and
.github/paths-filters.yml, so a PR editing either re-runs this file. Neither
entry was there before the filters moved -- `llm` listed `llm-service/**` and
not ci.yml -- so editing the workflow used to leave this class of guard unrun.
`test_every_filter_protects_the_files_that_define_it` is what keeps that true
for all eight filters, not just `llm`.
"""

import re
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

REPO = Path(__file__).resolve().parents[2]
CI_YML = REPO / ".github" / "workflows" / "ci.yml"
FILTERS = REPO / ".github" / "paths-filters.yml"

# The two files that decide what the filters mean. A filter that does not list
# them cannot fire on a change to itself.
SELF = (".github/paths-filters.yml", ".github/workflows/ci.yml")

GATE = "env.RUN == '1'"
SHARED = ".github/paths-filters.yml"


def _ci():
    return yaml.safe_load(CI_YML.read_text(encoding="utf-8"))


def _filters():
    return yaml.safe_load(FILTERS.read_text(encoding="utf-8"))


def _gated_jobs():
    """-> {job name: steps} for every job that gates itself on a filter."""
    return {
        name: job["steps"]
        for name, job in _ci()["jobs"].items()
        if any(st.get("id") == "filter" for st in job.get("steps", []))
    }


def test_there_are_gated_jobs_at_all():
    """Denominator check: every assertion below is vacuous on an empty set.

    Six, not the private tree's eight: the public cut drops the self-hosted
    `data-pipeline-smoke` and `kafka-security-matrix` jobs
    (scripts/flip/apply-ci-split.py DROP_JOBS), and this file ships and runs
    there too. A floor only one tree can meet is a red
    check on the other tree's first PR.
    """
    assert len(_gated_jobs()) >= 6, (
        "fewer than six jobs gate themselves on a filter -- either the jobs "
        "were dropped or `id: filter` was renamed, and the checks below are "
        "now asserting about almost nothing."
    )


def test_no_job_waits_on_a_changes_job():
    """The queue traversal this whole shape exists to remove."""
    text = CI_YML.read_text(encoding="utf-8")
    doc = _ci()
    assert "changes" not in doc["jobs"], (
        "a `changes` job is back in ci.yml. It costs every dependent a second "
        "trip through a queue whose waits were measured at 495-925 s; read "
        '"Why there is no `changes` job" in ci.yml before reintroducing it.'
    )
    assert "needs.changes." not in text, "a dead `needs.changes.*` reference survives"


def test_the_gate_reads_the_one_shared_filter_file():
    """No job may restate a rule that .github/paths-filters.yml already states."""
    for name, steps in _gated_jobs().items():
        step = next(st for st in steps if st.get("id") == "filter")
        got = (step.get("with") or {}).get("filters")
        assert got == SHARED, (
            f"{name} filters on {got!r} instead of {SHARED}. Two definitions "
            f"drift, and the one that drifts silently is the one no PR reads."
        )


def test_the_filter_and_its_gate_come_before_any_real_work():
    """Nothing substantive may run ahead of the decision to run nothing."""
    for name, steps in _gated_jobs().items():
        idx = next(i for i, st in enumerate(steps) if st.get("id") == "filter")
        before = steps[:idx]
        assert all("checkout" in str(st.get("uses", "")) for st in before), (
            f"{name} runs {[st.get('name') or st.get('uses') for st in before]} "
            f"before deciding whether it has anything to do. Only the checkout "
            f"the filter itself needs may precede it."
        )
        gate = steps[idx + 1]
        assert "RUN=1" in str(gate.get("run", "")) and "RUN=0" in str(gate.get("run", "")), (
            f"{name}: the step after the filter is {gate.get('name')!r}, not the "
            f"step that sets RUN. Anything between them runs unconditionally."
        )


def test_every_step_after_the_gate_is_inside_it():
    """THE check. One unguarded step is a job that works when it must not.

    An `if:` that already existed is AND-ed, not replaced -- including
    `always()` cleanups, which stay valid because the expression still contains
    a status function and so still bypasses the default `success()`.
    """
    offenders = []
    for name, steps in _gated_jobs().items():
        idx = next(i for i, st in enumerate(steps) if st.get("id") == "filter")
        for st in steps[idx + 2:]:
            if GATE not in str(st.get("if", "")):
                offenders.append(f"{name}: {st.get('name') or st.get('uses')}")
    assert not offenders, (
        "these steps run whether or not the job's paths changed:\n  "
        + "\n  ".join(offenders)
        + f"\nEvery step after the RUN gate needs `if: {GATE}`, AND-ed onto any "
        "condition it already had."
    )


def test_each_gate_names_only_filters_that_exist():
    """A typo'd filter key renders empty, so the gate reads false forever.

    That is the quiet direction of the failure: the job reports success on
    every PR, having run none of its steps, and nothing says so.
    """
    known = set(_filters())
    assert known, "paths-filters.yml parsed empty -- this check would pass vacuously"
    for name, steps in _gated_jobs().items():
        idx = next(i for i, st in enumerate(steps) if st.get("id") == "filter")
        gate_env = steps[idx + 1].get("env") or {}
        cond = str(gate_env.get("WANTED", ""))
        assert cond, f"{name}: the RUN gate reads no filter output at all"
        # Every gate variable, not just WANTED: a typo in a second one (the
        # matrix's COMPOSE) fails the same quiet way.
        used = set(re.findall(r"steps\.filter\.outputs\.([A-Za-z0-9_-]+)", " ".join(map(str, gate_env.values()))))
        assert used, f"{name}: gate condition {cond!r} names no filter output"
        assert used <= known, (
            f"{name} gates on {sorted(used - known)}, which "
            f"paths-filters.yml does not define. An undefined output is '', so "
            f"the job would skip every step on every PR and still report green."
        )


def test_every_filter_protects_the_files_that_define_it():
    """KI-DATAPATH-FILTER-EXCLUDES-THE-WORKFLOW, applied to all eight filters.

    A filter that omits the file it is written in cannot fire on a change to
    itself: break a rule here and the jobs that would have caught it are the
    ones the broken rule skips.
    """
    filters = _filters()
    assert len(filters) >= 8, f"only {len(filters)} filters: {sorted(filters)}"
    for name, patterns in filters.items():
        flat = [str(p) for p in patterns]
        for f in SELF:
            assert f in flat, (
                f"the `{name}` filter does not list {f}, so a PR that changes "
                f"how `{name}` is computed does not re-run the jobs `{name}` "
                f"gates. They report green having checked nothing."
            )


def test_the_shared_file_is_the_only_definition():
    """ci.yml may not carry an inline copy of a rule the shared file states."""
    doc = _ci()
    inline = []
    for name, job in doc["jobs"].items():
        for st in job.get("steps", []):
            if "paths-filter" not in str(st.get("uses", "")):
                continue
            spec = str((st.get("with") or {}).get("filters", ""))
            if "\n" in spec and st.get("id") == "filter":
                inline.append(f"{name}/{st.get('id')}")
    assert not inline, (
        f"{inline} declare their gate filters inline instead of reading "
        f"{SHARED}. The job-internal `svc` filter in data-pipeline-smoke is "
        f"deliberately not a gate and is allowed to stay inline; a step with "
        f"`id: filter` is a gate and must read the shared file."
    )
