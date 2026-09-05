"""Every SARIF-reporting job must put its findings somewhere that always works.

The reporting scanners (gosec, Semgrep, Trivy) had two destinations and on
2026-08-24/25 both were dead at once:

  * ``codeql-action/upload-sarif`` needs GitHub code scanning, which a PRIVATE
    repo without GHAS does not have -- and that step is ``continue-on-error``,
    so it failed in silence.
  * the ``Archive SARIF`` artifact upload failed on every run, ``main``
    included, with ``Artifact storage quota has been hit``.

So the findings reached nobody while ``security-audit-results/REPORT.md`` said
the archive step "preserves findings". It also turned all six reporting jobs
red, and a red ``gosec (api-gateway)`` reads as a security finding rather than a
billing condition -- gosec runs ``-no-fail`` and cannot go red that way.

The first fix (#868) added a job-summary destination, which needs neither GHAS nor
artifact storage. This test exists because that fix is silently reversible: delete
the summarize step and everything still parses, every job still runs, and the
findings quietly go back to having nowhere to land. Nothing else would fail.

The archive step is asserted to be ``continue-on-error`` for the same reason in
reverse -- if it ever goes back to being able to fail the job, a storage-billing
condition can once again wear a security finding's clothes.

The job summary is NOT a third machine-readable sink and this module must not be
read as claiming it is. ``scripts/summarize-sarif.sh`` renders a rule-level TALLY
-- level, ruleId, count, capped at 40 rules -- with no file, no line and no
message, so nothing can be triaged from it. When both real sinks refuse, the
actionable detail is still discarded. That is why the second fix is a REPORT
rather than another sink: ``scripts/report-sarif-sinks.sh`` states in the run
which destinations actually took the SARIF, because both uploads are
``continue-on-error`` and that rewrites their API ``conclusion`` to ``success`` --
so "reached neither" and "reached both" were indistinguishable from anywhere but a
raw log.

Three properties of that report are load-bearing, and each has a test below
because each is a way this kind of step lies:

  * it must NOT run on a cancelled run. ``security.yml`` sets
    ``cancel-in-progress: true``, so a second push to a PR branch cancels the run
    in flight, and ``always()`` RUNS on cancellation -- which would stamp four
    gosec jobs + semgrep + trivy with a red security annotation blaming GHAS and
    billing every time somebody pushes twice.
  * an EMPTY or ``skipped`` outcome is "not attempted", never "failed". Reporting
    ``the artifact copy failed (unset)`` in the steady state points every reader
    at a cause that is not the cause.
  * a ``success`` from ``actions/upload-artifact`` with ``if-no-files-found:
    ignore`` can mean zero files. Certifying that as a copy is this repo's
    documented "an empty set reads as a pass" class, reproduced inside the step
    written to end it.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"

SUMMARIZER = "summarize-sarif.sh"
SINK_REPORTER = "report-sarif-sinks.sh"
UPLOAD_SARIF = "codeql-action/upload-sarif"
UPLOAD_ARTIFACT = "actions/upload-artifact"

_ALWAYS = re.compile(r"\balways\s*\(\s*\)")
_NOT_CANCELLED = re.compile(r"!\s*cancelled\s*\(\s*\)")


def skips_on_cancellation(cond) -> bool:
    """Does this ``if:`` run after a failure but NOT on a cancelled run?

    Deliberately a PROPERTY check, not a string pin. The previous version of this
    module asserted ``str(step.get("if","")).strip() == "always()"``, which made the
    correct spelling fail the guard -- a guard pinned to the wrong shape is worse
    than no guard, because it actively blocks the fix.

    So: the condition must contain ``!cancelled()`` (GitHub's documented spelling
    for "everything except a cancelled run") and must not contain ``always()``
    (which by GitHub's own definition runs even then). Any surrounding ``${{ }}``,
    extra conjuncts, or whitespace are accepted.
    """
    text = str(cond or "")
    return bool(_NOT_CANCELLED.search(text)) and not _ALWAYS.search(text)


def _reporting_jobs() -> list[tuple[str, str, dict]]:
    """(workflow, job id, job) for every job that uploads SARIF to code scanning."""
    found: list[tuple[str, str, dict]] = []
    for path in sorted(WORKFLOW_DIR.glob("*.yml")):
        doc = yaml.safe_load(path.read_text()) or {}
        for job_id, job in (doc.get("jobs") or {}).items():
            steps = job.get("steps") or []
            if any(UPLOAD_SARIF in str(s.get("uses", "")) for s in steps):
                found.append((path.name, job_id, job))
    return found


def _named(steps: list[dict], needle: str) -> list[int]:
    return [i for i, s in enumerate(steps) if needle in str(s.get("name", ""))]


def test_there_are_reporting_jobs_to_check():
    """Positive denominator: an empty census would make every test below vacuous."""
    jobs = _reporting_jobs()
    assert len(jobs) >= 3, (
        f"expected at least 3 SARIF-reporting jobs, found {len(jobs)}: {jobs!r}. "
        "If the reporting jobs were genuinely removed, delete this file too -- "
        "but a zero here otherwise means the census stopped finding its subject "
        "and every assertion in this module silently passes on nothing."
    )


def test_the_summarizer_script_exists_and_is_executable():
    script = REPO_ROOT / "scripts" / SUMMARIZER
    assert script.is_file(), f"{script} is missing; every reporting job invokes it"
    assert os.access(script, os.X_OK), (
        f"{script} is not executable -- the workflow runs it as ./scripts/{SUMMARIZER}, "
        "which fails without the exec bit"
    )


@pytest.mark.parametrize(
    "workflow,job_id,job",
    _reporting_jobs(),
    ids=[f"{w}:{j}" for w, j, _ in _reporting_jobs()],
)
def test_reporting_job_summarizes_its_sarif(workflow: str, job_id: str, job: dict):
    steps = job.get("steps") or []

    summarize = [
        i for i, s in enumerate(steps) if SUMMARIZER in str(s.get("run", ""))
    ]
    assert summarize, (
        f"{workflow}:{job_id} uploads SARIF to code scanning but never runs "
        f"scripts/{SUMMARIZER}. On a private repo without GHAS that upload is a "
        "no-op, so without the job summary this job's findings reach nobody."
    )

    step = steps[summarize[0]]
    assert skips_on_cancellation(step.get("if")), (
        f"{workflow}:{job_id} summarize step must run after a failure but NOT on a "
        f"cancelled run; its `if:` is {step.get('if')!r}. It used to be pinned to the "
        "literal `always()`, which was wrong in both directions: `always()` runs on "
        "cancellation, so a superseded run executed this script against the SARIF the "
        "cancelled scanner never wrote and it exited 2 (`the scanner produced no "
        "SARIF`) -- and pinning the spelling meant correcting it FAILED the guard. "
        "See skips_on_cancellation for what is actually required."
    )
    assert "continue-on-error" not in step, (
        f"{workflow}:{job_id} summarize step must NOT be continue-on-error. It is the "
        "one step whose failure means a real scanner problem: the script exits 2 only "
        "when the SARIF is missing or unparseable."
    )

    archive = _named(steps, "Archive SARIF")
    if archive:
        assert summarize[0] < archive[0], (
            f"{workflow}:{job_id} summarizes after archiving; the summary must be "
            "written first so it survives an archive failure."
        )
        assert steps[archive[0]].get("continue-on-error") is True, (
            f"{workflow}:{job_id} Archive SARIF must be continue-on-error. Without it "
            "an artifact storage quota -- a billing condition -- turns this reporting "
            "job red and impersonates a security finding, which is what happened on "
            "2026-08-24."
        )


# ---------------------------------------------------------------------------
# Part 2 -- the delivery must be STATED in the run, and stating it must not
# invent failures of its own.
#
# #868 gave the findings a job-summary readout but left the run silent about
# which sinks actually took the SARIF. Both uploads are ``continue-on-error``,
# which rewrites their API ``conclusion`` to ``success``, so "reached no
# machine-readable sink at all" and "reached both" look identical from anywhere
# but the raw log. security.yml's header used to say exactly that, in prose, and
# then tell a human to go read the raw log by hand.
#
# ``scripts/report-sarif-sinks.sh`` says it in the run instead, and stays GREEN
# while doing so: a red reporting job impersonates a security finding -- gosec
# runs ``-no-fail`` and cannot go red that way -- which is the misread #868
# removed. Loud-but-green is the deliberate middle.
# ---------------------------------------------------------------------------


def test_the_sink_reporter_script_exists_and_is_executable():
    script = REPO_ROOT / "scripts" / SINK_REPORTER
    assert script.is_file(), f"{script} is missing; every reporting job invokes it"
    assert os.access(script, os.X_OK), (
        f"{script} is not executable -- the workflow runs it as "
        f"./scripts/{SINK_REPORTER}, which fails without the exec bit"
    )


def test_both_sarif_scripts_are_in_the_ci_paths_filter():
    """This module only guards anything on the runs where it is allowed to run.

    Its subjects are security.yml AND the two scripts that carry the reporting
    tier's output. ``.github/workflows/**`` covers the first; nothing covered the
    second, so a PR deleting ``summarize-sarif.sh`` and touching nothing else ran
    no guard at all. A guard gated on a paths filter that excludes its own subject
    is not a guard -- this repo has hit that shape five times.
    """
    ci = (WORKFLOW_DIR / "ci.yml").read_text()
    for script in (SUMMARIZER, SINK_REPORTER):
        assert f"'scripts/{script}'" in ci, (
            f"scripts/{script} is not in ci.yml's paths filter, so a PR changing only "
            "that file runs none of these tests. Add it to the `llm:` filter."
        )


@pytest.mark.parametrize(
    "workflow,job_id,job",
    _reporting_jobs(),
    ids=[f"{w}:{j}" for w, j, _ in _reporting_jobs()],
)
def test_reporting_job_reports_which_sinks_took_the_sarif(
    workflow: str, job_id: str, job: dict
):
    steps = job.get("steps") or []

    upload_idx = _named(steps, "Upload SARIF to code-scanning")
    assert upload_idx, f"{workflow}:{job_id} has no code-scanning upload step"
    upload = steps[upload_idx[0]]
    assert upload.get("id"), (
        f"{workflow}:{job_id} the code-scanning upload needs an `id:` -- without one "
        "nothing downstream can read its outcome, and its outcome is the only honest "
        "signal it has (continue-on-error rewrites its conclusion to success)."
    )

    report = [i for i, s in enumerate(steps) if SINK_REPORTER in str(s.get("run", ""))]
    assert report, (
        f"{workflow}:{job_id} never runs scripts/{SINK_REPORTER}. Both SARIF uploads "
        "are continue-on-error, so without this step a run whose findings reached NO "
        "machine-readable sink is indistinguishable from one that reached both."
    )
    step = steps[report[0]]

    wired = " ".join(str(v) for v in (step.get("env") or {}).values())
    assert f"steps.{upload['id']}.outcome" in wired, (
        f"{workflow}:{job_id} the sink report must read the code-scanning step's "
        "`.outcome`. `.conclusion` is rewritten to success by continue-on-error and "
        "would report green on exactly the runs this exists to catch."
    )
    assert ".conclusion" not in wired, (
        f"{workflow}:{job_id} the sink report reads a `.conclusion`, which "
        "continue-on-error rewrites to success. Read `.outcome`."
    )

    # It must be handed the SARIF path, not only the outcomes. `upload-artifact`
    # with `if-no-files-found: ignore` succeeds on zero files, so an outcome-only
    # report certifies a copy that contains nothing when the scanner crashed.
    run = str(step.get("run", ""))
    assert ".sarif" in run, (
        f"{workflow}:{job_id} runs {SINK_REPORTER} without passing the SARIF path: "
        f"{run!r}. Outcomes alone cannot tell a stored copy from a stored nothing -- "
        "`if-no-files-found: ignore` makes an empty upload succeed."
    )

    archive_idx = _named(steps, "Archive SARIF")
    if archive_idx:
        assert archive_idx[0] < report[0], (
            f"{workflow}:{job_id} reports the sinks before archiving to one of them; "
            "the report must run last so it sees the archive's real outcome."
        )
        archive = steps[archive_idx[0]]
        assert archive.get("id"), (
            f"{workflow}:{job_id} Archive SARIF needs an `id:` so the sink report can "
            "read its outcome."
        )
        assert f"steps.{archive['id']}.outcome" in wired, (
            f"{workflow}:{job_id} the sink report does not read the archive step's "
            "`.outcome`, so it cannot tell a one-sink failure from a two-sink one."
        )
        retention = (archive.get("with") or {}).get("retention-days")
        assert retention is not None, (
            f"{workflow}:{job_id} Archive SARIF has no retention-days, so it inherits "
            "the 90-day repo default -- which is how 4,630 SARIF artifacts accumulated."
        )
        # BOUNDED, not "long enough" -- this deliberately asserts no minimum.
        #
        # An earlier version of this file pinned `>= 14` as an enforced floor. That
        # turned a contested judgement into a CI gate, and it pointed the wrong way:
        # the obvious remedy if the artifact quota is exhausted again is to CUT
        # retention, and a floor would have failed `llm-service unit tests` until
        # somebody edited this guard to permit the fix. The repo has agreed no
        # minimum, so none is asserted. The reasoning for today's value of 14 is
        # recorded beside the value in security.yml, where whoever changes it reads
        # it -- as a judgement, not a rule.
        #
        # 0 is still rejected: upload-artifact reads 0 as "use the repository
        # default", i.e. unbounded again, spelled as though it were bounded.
        assert int(retention) >= 1, (
            f"{workflow}:{job_id} Archive SARIF sets retention-days={retention}; "
            "upload-artifact reads 0 (and anything below 1) as 'use the repository "
            "default', which is the unbounded 90 days this guard exists to prevent."
        )


@pytest.mark.parametrize(
    "workflow,job_id,job",
    _reporting_jobs(),
    ids=[f"{w}:{j}" for w, j, _ in _reporting_jobs()],
)
def test_no_post_scan_step_runs_on_a_cancelled_run(
    workflow: str, job_id: str, job: dict
):
    """``always()`` is the wrong gate here, and it is wrong ROUTINELY.

    ``security.yml`` sets ``concurrency: cancel-in-progress: true``, so every
    second push to a PR branch cancels the Security run in flight. ``always()``
    runs on cancellation -- that is precisely its documented difference from
    ``success() || failure()``. With ``always()`` on these three steps a
    superseded run would run ``summarize-sarif.sh`` against a SARIF the cancelled
    scanner never wrote (exit 2, "the scanner produced no SARIF"), upload an
    artifact of nothing, and stamp four gosec jobs + semgrep + trivy with an
    ``::error`` annotation blaming GHAS and the artifact quota for a cancellation.
    Six false security errors per superseded push, on a routine event.
    """
    steps = job.get("steps") or []
    subjects = [
        s
        for s in steps
        if any(
            n in str(s.get("name", ""))
            for n in ("Summarize SARIF", "Archive SARIF", "Report which SARIF sinks")
        )
    ]
    assert len(subjects) >= 3, (
        f"{workflow}:{job_id} has only {len(subjects)} post-scan steps; expected the "
        "summarize, archive and sink-report trio. A shrinking denominator here would "
        "make this assertion pass on nothing."
    )
    for step in subjects:
        assert skips_on_cancellation(step.get("if")), (
            f"{workflow}:{job_id} step {step.get('name')!r} has "
            f"`if: {step.get('if')!r}`, which runs on a cancelled run. Use "
            "`!cancelled()` -- see this test's docstring for what `always()` costs on "
            "a workflow with cancel-in-progress."
        )


# ---------------------------------------------------------------------------
# Behavioural: run the script, read what it emits. The table below is written out
# in full rather than derived, so it is an independent statement of the intended
# behaviour and not a mirror of the script's own branch order.
#
# ``cs`` = the code-scanning upload's outcome, ``ar`` = the archive's. ``""`` is
# the empty string GitHub may render for a step that never ran; the first version
# of this table tested only the hoped-for ``skipped`` spelling, and on the empty
# one the script announced "the artifact copy failed (unset) -- check the account
# artifact quota" on every reporting job of every run.
# ---------------------------------------------------------------------------

QUIET, WARN, ERROR = "quiet", "::warning", "::error"
OUTCOMES = ["success", "failure", "skipped", "cancelled", ""]

DELIVERY_MATRIX = {
    # Code scanning took it -> at most a warning, never an error.
    ("success", "success"): QUIET,
    ("success", "failure"): WARN,     # quota; the findings still have a durable home
    ("success", "skipped"): QUIET,    # not attempted
    ("success", "cancelled"): QUIET,  # a cancelled run is not a delivery failure
    ("success", ""): QUIET,           # empty == not attempted, NOT "failed (unset)"
    # Code scanning refused -> the artifact is the only machine-readable copy.
    ("failure", "success"): WARN,
    ("failure", "failure"): ERROR,    # the condition this whole change exists for
    ("failure", "skipped"): ERROR,
    ("failure", "cancelled"): QUIET,
    ("failure", ""): ERROR,
    # A skipped upload is not a delivery either.
    ("skipped", "success"): WARN,
    ("skipped", "failure"): ERROR,
    ("skipped", "skipped"): ERROR,
    ("skipped", "cancelled"): QUIET,
    ("skipped", ""): ERROR,
    # Cancellation dominates: quiet on every pairing.
    ("cancelled", "success"): QUIET,
    ("cancelled", "failure"): QUIET,
    ("cancelled", "skipped"): QUIET,
    ("cancelled", "cancelled"): QUIET,
    ("cancelled", ""): QUIET,
    # An unreadable code-scanning outcome must never read as success.
    ("", "success"): WARN,
    ("", "failure"): ERROR,
    ("", "skipped"): ERROR,
    ("", "cancelled"): QUIET,
    ("", ""): ERROR,                  # both unwired: reported as a miswiring
}


def test_the_outcome_matrix_is_complete():
    """Positive denominator for the behavioural tests below."""
    assert len(DELIVERY_MATRIX) == 25 == len(OUTCOMES) * len(OUTCOMES), (
        f"the matrix has {len(DELIVERY_MATRIX)} rows; every pairing of "
        f"{OUTCOMES!r} must be stated, because an untested pairing is exactly where "
        "the next false annotation hides."
    )
    assert set(DELIVERY_MATRIX) == {(a, b) for a in OUTCOMES for b in OUTCOMES}


def _run_reporter(code_scanning, archive, sarif, summary=None):
    env = dict(os.environ)
    env["CODE_SCANNING_OUTCOME"] = code_scanning
    env["ARCHIVE_OUTCOME"] = archive
    if summary is not None:
        env["GITHUB_STEP_SUMMARY"] = str(summary)
    else:
        env.pop("GITHUB_STEP_SUMMARY", None)
    return subprocess.run(
        [str(REPO_ROOT / "scripts" / SINK_REPORTER), "gosec (api-gateway)", str(sarif)],
        capture_output=True,
        text=True,
        env=env,
    )


def _kind(stdout: str) -> str:
    if "::error" in stdout:
        return ERROR
    if "::warning" in stdout:
        return WARN
    return QUIET


@pytest.mark.parametrize(
    "code_scanning,archive",
    list(DELIVERY_MATRIX),
    ids=[f"cs={a or 'EMPTY'}-ar={b or 'EMPTY'}" for a, b in DELIVERY_MATRIX],
)
def test_sink_reporter_annotates_correctly_and_never_fails(
    code_scanning: str, archive: str, tmp_path
):
    """Exit 0 on every row is the load-bearing half.

    The reporting tier must not block, and a red ``gosec (api-gateway)`` reads as
    "gosec found something" -- which gosec, running ``-no-fail``, cannot produce.
    Letting a storage quota redden these jobs is the misread #868 removed; a new
    step name must not quietly reintroduce it.
    """
    sarif = tmp_path / "gosec-api-gateway.sarif"
    sarif.write_text(
        '{"version":"2.1.0","runs":[{"tool":{"driver":{"name":"gosec"}},"results":[]}]}'
    )
    summary = tmp_path / "step-summary.md"
    summary.touch()

    proc = _run_reporter(code_scanning, archive, sarif, summary)
    assert proc.returncode == 0, (
        f"cs={code_scanning!r}/ar={archive!r} exited {proc.returncode}: {proc.stderr}. "
        "This script must never fail the job -- see its header."
    )
    expected = DELIVERY_MATRIX[(code_scanning, archive)]
    assert _kind(proc.stdout) == expected, (
        f"cs={code_scanning!r}/ar={archive!r} should be {expected}; got:\n"
        f"{proc.stdout[:600]}"
    )
    # The delivery table goes to the job summary on every path, so the run records
    # where the findings landed even when nothing is annotated.
    assert "where these findings landed" in summary.read_text(), (
        f"cs={code_scanning!r}/ar={archive!r} wrote no delivery table to the summary"
    )


@pytest.mark.parametrize(
    "code_scanning,archive",
    list(DELIVERY_MATRIX),
    ids=[f"cs={a or 'EMPTY'}-ar={b or 'EMPTY'}" for a, b in DELIVERY_MATRIX],
)
def test_sink_reporter_never_certifies_a_delivery_of_nothing(
    code_scanning: str, archive: str, tmp_path
):
    """A ``success`` from upload-artifact can mean zero files.

    ``if-no-files-found: ignore`` succeeds when the glob matches nothing, so when
    the scanner crashes and writes no SARIF the archive step goes green and an
    outcome-only report would announce that a copy was stored. Repo-documented
    class: an empty set reads as a pass. The reporter is handed the SARIF path so
    that it can refuse to say that.
    """
    missing = tmp_path / "gosec-api-gateway.sarif"  # deliberately never written
    summary = tmp_path / "step-summary.md"
    summary.touch()

    proc = _run_reporter(code_scanning, archive, missing, summary)
    assert proc.returncode == 0, proc.stderr
    if "cancelled" in (code_scanning, archive):
        # Cancellation still dominates: nothing finished, so there is nothing to
        # say about a file that was never going to exist yet.
        assert _kind(proc.stdout) == QUIET, proc.stdout[:600]
        return
    assert "::error" in proc.stdout and "No SARIF was produced" in proc.stdout, (
        f"cs={code_scanning!r}/ar={archive!r} with NO SARIF on disk must report the "
        f"scanner failure, not a delivery; got:\n{proc.stdout[:600]}"
    )
    assert "archived" not in proc.stdout, (
        f"cs={code_scanning!r}/ar={archive!r} claims the SARIF was archived when no "
        f"SARIF exists:\n{proc.stdout[:600]}"
    )


def test_sink_reporter_is_quiet_when_the_archive_outcome_is_empty(tmp_path):
    """The designed steady state must not annotate anything at all.

    This is the case the first version of the table omitted. It assumed GitHub
    renders a step skipped by its ``if:`` as the literal ``skipped``; if it renders
    an empty string instead, ``${ARCHIVE_OUTCOME:-unset}`` became ``unset`` and the
    script printed "the artifact copy failed (unset) ... check the account artifact
    quota" -- a false alarm on every reporting job of every run, naming a cause
    that is not the cause.
    """
    sarif = tmp_path / "s.sarif"
    sarif.write_text('{"version":"2.1.0","runs":[]}')
    summary = tmp_path / "sum.md"
    summary.touch()
    proc = _run_reporter("success", "", sarif, summary)
    assert proc.returncode == 0, proc.stderr
    assert _kind(proc.stdout) == QUIET, (
        "an empty archive outcome with code scanning green is the steady state, not "
        f"an incident:\n{proc.stdout[:600]}"
    )
    # The exact regression, word for word: `the artifact copy failed (unset) ...
    # check the account artifact quota`. ("quota" on its own is not the tell --
    # the delivery table names it as what the artifact sink NEEDS, on every path.)
    assert "copy failed" not in proc.stdout and "unset" not in proc.stdout, (
        "an empty outcome must read as 'not attempted', never as a failed copy "
        f"blamed on the artifact quota:\n{proc.stdout[:600]}"
    )
    assert "not attempted" in proc.stdout, (
        f"an empty outcome should be described as not attempted:\n{proc.stdout[:600]}"
    )


def test_sink_reporter_stays_quiet_on_a_cancelled_outcome(tmp_path):
    """Belt and braces behind the workflow's ``!cancelled()`` gate.

    The gate is the real fix; this is the second line, so that restoring
    ``always()`` upstream -- by edit, or by copy-pasting one of these jobs into a
    new one -- cannot on its own resurrect six red annotations per superseded push.
    """
    sarif = tmp_path / "s.sarif"
    sarif.write_text('{"version":"2.1.0","runs":[]}')
    summary = tmp_path / "sum.md"
    summary.touch()
    proc = _run_reporter("cancelled", "cancelled", sarif, summary)
    assert proc.returncode == 0, proc.stderr
    assert _kind(proc.stdout) == QUIET, (
        f"a cancelled run is not a security condition:\n{proc.stdout[:600]}"
    )
    assert "cancelled" in proc.stdout.lower()


def test_sink_reporter_survives_an_unwritable_step_summary(tmp_path):
    """A full disk or an unwritable summary file must not redden a security job.

    The script runs under ``set -euo pipefail``, so the append to
    ``$GITHUB_STEP_SUMMARY`` is the one statement that can abort it before its
    ``exit 0``. Unguarded, a storage condition reddens ``gosec (api-gateway)`` --
    which reads as "gosec found something", the exact misread this whole script
    exists to prevent, reproduced by the step written to end it. Control: with the
    guard removed this case exits 1 with "Permission denied".

    The verdict must also survive: it falls back to the step log rather than being
    swallowed, or the run goes quiet again in the one condition it must not.
    """
    sarif = tmp_path / "s.sarif"
    sarif.write_text('{"version":"2.1.0","runs":[]}')
    unwritable = tmp_path / "step-summary.md"
    unwritable.write_text("")
    unwritable.chmod(0o444)

    proc = _run_reporter("failure", "failure", sarif, unwritable)
    assert proc.returncode == 0, (
        "an unwritable $GITHUB_STEP_SUMMARY failed the job "
        f"(rc={proc.returncode}): {proc.stderr!r}. A storage condition must not "
        "wear a security finding's clothes."
    )
    assert "where these findings landed" in proc.stdout, (
        "the delivery table was lost when the summary file could not be written; "
        f"it must fall back to the step log:\n{proc.stdout[:600]}"
    )
    assert "::error" in proc.stdout, (
        "cs=failure/ar=failure is the double-sink failure; it must still annotate "
        f"when only the summary file is unwritable:\n{proc.stdout[:600]}"
    )


def test_a_cancelled_half_does_not_deny_the_other_halfs_delivery(tmp_path):
    """One cancelled step is not "nothing was delivered".

    A cancellation can take a single step without taking the job, so
    (code scanning: success, archive: cancelled) is a real state -- and the blanket
    line this branch used to print, "run cancelled before the SARIF was delivered",
    was simply false for it: code scanning demonstrably took it. Still QUIET (no
    verdict can be drawn for the half that did not finish), but the half that DID
    finish must be reported as what it was.
    """
    sarif = tmp_path / "s.sarif"
    sarif.write_text('{"version":"2.1.0","runs":[]}')
    summary = tmp_path / "sum.md"
    summary.touch()

    proc = _run_reporter("success", "cancelled", sarif, summary)
    assert proc.returncode == 0, proc.stderr
    assert _kind(proc.stdout) == QUIET, proc.stdout[:600]
    assert "took it" in proc.stdout, (
        "code scanning accepted the SARIF and the report does not say so:\n"
        f"{proc.stdout[:600]}"
    )
    assert "before the SARIF was delivered" not in proc.stdout, (
        "the report denies a delivery that the outcomes record as having happened:"
        f"\n{proc.stdout[:600]}"
    )
    # The mirror image, so the fix cannot be half-applied.
    mirror = _run_reporter("cancelled", "success", sarif, summary)
    assert mirror.returncode == 0, mirror.stderr
    assert _kind(mirror.stdout) == QUIET, mirror.stdout[:600]
    assert "artifact took it" in mirror.stdout, (
        f"the archive succeeded and the report does not say so:\n{mirror.stdout[:600]}"
    )


RETIRED_CLAIMS = {
    # Two sentences retired from security.yml's header on 2026-08-29. They must not
    # be reproduced verbatim ANYWHERE that is read as current: a quoted false claim
    # greps identically to a live one, so "retired with the quote kept for context"
    # leaves the defect in place for the next reader who arrives by grep. Describe
    # them instead -- as security.yml's header now does.
    "whole delivery mechanism": "the artifact was never the whole delivery mechanism",
    "bounded rather than muted": "same sentence, second half",
    "from the RAW LOG": "nobody has to read a raw log any more",
    "check the artifact quota first": "a hit quota no longer reddens anything",
}


@pytest.mark.parametrize("claim", sorted(RETIRED_CLAIMS))
def test_retired_claims_are_not_quoted_verbatim_in_live_files(claim: str):
    """Sweep, do not annotate. Repo class: #806 / #857."""
    targets = sorted(WORKFLOW_DIR.glob("*.yml")) + [
        REPO_ROOT / "scripts" / SINK_REPORTER,
        REPO_ROOT / "scripts" / SUMMARIZER,
        REPO_ROOT / "security-audit-results" / "REPORT.md",
    ]
    present = [t for t in targets if t.is_file()]
    assert len(present) > 3, (
        f"only {len(present)} of {len(targets)} sweep targets exist; a vacuous "
        "sweep passes for free. Check the paths before believing this test."
    )
    hits = [
        str(t.relative_to(REPO_ROOT))
        for t in present
        if claim in t.read_text(errors="replace")
    ]
    assert not hits, (
        f"the retired claim {claim!r} ({RETIRED_CLAIMS[claim]}) is still quoted "
        f"verbatim in {hits}. A reader grepping for it finds a live file and reads "
        "it as current guidance. Describe the retired claim; do not reproduce it."
    )


def test_sink_reporter_treats_unwired_outcomes_as_unknown_not_success(tmp_path):
    """A miswired step id yields no outcome, and that must not read as delivered.

    Defaulting the other way is the exact failure this module is about: an empty
    set reading as a pass. It is reported as a MISWIRING rather than as a double
    sink failure, because saying "neither sink took it" would blame billing for a
    workflow typo.
    """
    sarif = tmp_path / "s.sarif"
    sarif.write_text('{"version":"2.1.0","runs":[]}')
    env = dict(os.environ)
    for key in ("CODE_SCANNING_OUTCOME", "ARCHIVE_OUTCOME", "GITHUB_STEP_SUMMARY"):
        env.pop(key, None)
    proc = subprocess.run(
        [str(REPO_ROOT / "scripts" / SINK_REPORTER), "probe", str(sarif)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert proc.returncode == 0, proc.stderr
    assert "::error" in proc.stdout and "miswired" in proc.stdout, (
        "unset outcomes must report as unknown, not silently pass:\n"
        + proc.stdout[:600]
    )


def test_sink_reporter_requires_the_sarif_path():
    """The path is mandatory, so the empty-upload check cannot be lost by omission."""
    env = dict(os.environ)
    env["CODE_SCANNING_OUTCOME"] = "failure"
    env["ARCHIVE_OUTCOME"] = "success"
    proc = subprocess.run(
        [str(REPO_ROOT / "scripts" / SINK_REPORTER), "label-only"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert proc.returncode != 0, (
        "called with no SARIF path the script reported a delivery anyway; the path "
        f"must be required:\n{proc.stdout[:400]}"
    )
