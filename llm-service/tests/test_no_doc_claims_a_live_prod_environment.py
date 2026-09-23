"""No doc may answer "is anything deployed" on its own — in either direction.

This guard exists because of how the claim failed, not because of the claim itself, and it
has now failed **both** ways round.

First the Azure VM behind `app.rsync.ai` was deleted in late August 2026, and
`PRODUCT_STATUS.md` kept carrying "🟢 LIVE — prod HEAD `e6371a8a`" while `CAPABILITIES.md`
kept carrying "✅ prod is level with origin/main". Nothing failed, because a status line has
no expiry: both were written true, and both went false on their own while every test in the
repo stayed green. Sessions quoted them back as current state.

Then the correction rotted the same way. A GCP VM took over `app.rsync.ai`, and the docs went
on asserting "THERE IS NO DEPLOYED ENVIRONMENT" for weeks — so agents declined to deploy,
declined to test against it, and reported merged work as fully delivered while it ran nowhere.
That second failure was the more expensive one, and it is the reason this file no longer
pins the *answer*. It pins the *shape* of the answer:

  1. every doc that carries prod status must point at the one file that owns the answer,
  2. none of them may assert a live prod environment in the present tense, and
  3. none of them may assert that nothing is deployed either — that is equally a claim with
     an expiry, and it is the one that was wrong most recently.

The status doc itself is exempt from 2 and 3, because answering is its job. What it owes
instead is `test_the_status_doc_hands_you_a_probe`: it must carry the commands that let a
reader falsify it in one step. An assertion asks for trust and rots silently; a probe does
not. If the environment changes again, this is still the order — correct
`docs/deployment/prod-environment-status.md`, then this file, then the banners.

THE PUBLIC CUT. This file has a MIXED subject and therefore stays, running, in the public
repo. Half of it — the sweep over every tracked `*.md` — is a public invariant: a public doc
answering this question on its own is exactly the confusion this guard was written to stop,
and 110 markdown files survive the cut to be swept. The other half pins four docs by name,
and `scripts/flip/excludes.txt` deletes all four. Those four cases skip when their subject is
absent, and `test_the_pinned_doc_set_is_all_or_nothing` is the positive denominator that
stops the skip from turning into a quiet hole: absence is only allowed when EVERY pinned doc
is absent, i.e. the whole cut ran. Deleting or renaming one of them in the private repo is
still red.
"""

from __future__ import annotations

import datetime
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

STATUS_DOC = "docs/deployment/prod-environment-status.md"

# Docs that carry prod status and are read as current state by both humans and agents.
DOCS_THAT_MUST_POINT_AT_THE_STATUS_DOC = (
    "CLAUDE.md",
    "CAPABILITIES.md",
    "PRODUCT_STATUS.md",
    "docs/runbook.md",
)

# Present-tense assertions that an environment is up. Each is anchored on a verb or a
# status glyph so the historical record ("was deployed 2026-08-23", "PROD-DEPLOYED
# `e6371a8a`", "[HISTORICAL] prod was level with…") is untouched — that log is evidence a
# fix was exercised against a real system and must survive.
#
# These stay banned even though something IS deployed now. The point was never that the
# claim was false; it is that a second copy of the answer drifts from the first.
LIVE_CLAIM_PATTERNS = (
    r"🟢\s*LIVE",
    r"\bprod is level with\b",
    r"\bprod is on `main`",
    r"\bis live on prod\b",
    r"\bprod is currently\b",
    r"\bprod HEAD is\b",
)

# The mirror image, and the failure this file was last extended for. A doc asserting that
# nothing is deployed is a claim with exactly the same expiry as one asserting that
# something is — and between the GCP host coming up and 2026-09-23, this was the half that
# was wrong, in seven files at once.
NO_ENVIRONMENT_PATTERNS = (
    r"(?i)there is no deployed environment",
    r"(?i)\bno deployed environment exists\b",
    r"(?i)there is nothing to deploy to",
    r"(?i)\bnothing is deployed\b",
    r"(?i)\bhave no host\b",
    r"(?i)\bthere is no deploy target\b",
)

# Files whose job is to record history, or to hold the correction itself.
EXEMPT = {
    STATUS_DOC,
    "llm-service/tests/test_no_doc_claims_a_live_prod_environment.py",
}

# CAPABILITIES-ARCHIVE.md is exempt only above this heading. Below it is text the index
# split moved out of CAPABILITIES.md (active write-ups, full status rows), which this
# sweep checked before the move.
ARCHIVE = "CAPABILITIES-ARCHIVE.md"
ARCHIVE_TAIL = "## Active Known issues — full write-ups"

# A marker earns an exemption only when it PRECEDES the claim it qualifies.
HISTORY_MARKERS = ("[HISTORICAL]", "HISTORICAL —", "DECOMMISSIONED")


# The sweep's floor. 171 tracked `*.md` in the private repo, 110 after the public cut
# (`scripts/flip/excludes.txt` removes docs/internal, CAPABILITIES.md, the status boards,
# docs/runbook.md, …). A number well under both, because the point of the floor is only to
# tell "the corpus shrank a bit" apart from "git ls-files returned nothing and every
# assertion below passed on an empty set".
MIN_TRACKED_MARKDOWN = 80


def _tracked_markdown() -> list[Path]:
    import subprocess

    out = subprocess.run(
        ["git", "ls-files", "*.md"], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout.split()
    # `git ls-files` is the index, not the disk: a file staged-then-deleted, or one being
    # moved by a concurrent edit, is listed but unreadable. Dropping it is safe here only
    # because `test_the_markdown_sweep_has_a_denominator` asserts the corpus is still
    # large — silently sweeping nothing is the failure this whole file is about.
    return [p for p in (REPO / q for q in out if q not in EXEMPT) if p.is_file()]


def _offenders(pattern: str) -> list[str]:
    """Every tracked-markdown line matching `pattern` without a preceding history marker."""
    rx = re.compile(pattern)
    hits: list[str] = []
    for path in _tracked_markdown():
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:  # pragma: no cover - defensive
            continue
        first = lines.index(ARCHIVE_TAIL) + 1 if path == REPO / ARCHIVE else 1
        for n, line in enumerate(lines[first - 1 :], first):
            hit = rx.search(line)
            if not hit:
                continue
            # The marker must come BEFORE the claim it qualifies -- a line that merely
            # mentions "[HISTORICAL]" somewhere is not thereby exempt. This is not
            # hypothetical: the CAPABILITIES.md row recording the original fix quoted the
            # retired claim verbatim, and an anywhere-in-the-line check waved it through.
            marker = min(
                (line.find(m) for m in HISTORY_MARKERS if line.find(m) != -1),
                default=-1,
            )
            if marker != -1 and marker < hit.start():
                continue
            hits.append(f"{path.relative_to(REPO)}:{n}: {line.strip()[:160]}")
    return hits


def test_the_markdown_sweep_has_a_denominator() -> None:
    """Positive denominator for the sweeps below, which pass on an empty corpus."""
    found = _tracked_markdown()
    assert len(found) >= MIN_TRACKED_MARKDOWN, (
        f"only {len(found)} tracked markdown files were readable, expected at least "
        f"{MIN_TRACKED_MARKDOWN}. The sweeps below cannot have checked anything "
        "meaningful; fix the enumeration rather than trusting their pass."
    )


# Which of the pinned docs this checkout actually has. The public repo has none of them.
_PINNED_PRESENT = tuple(
    rel for rel in DOCS_THAT_MUST_POINT_AT_THE_STATUS_DOC if (REPO / rel).is_file()
)


def test_the_pinned_doc_set_is_all_or_nothing() -> None:
    """The positive denominator for the per-doc skips below.

    `scripts/flip/excludes.txt` removes all four of these together, so "none present"
    is the public repo and is fine. "Some present" is the case the skip must never
    cover: a rename or a deletion in the private repo, which is precisely how a pinned
    list stops pinning anything. Stated as its own test so the failure names the cause
    rather than surfacing as four quiet skips.
    """
    missing = [r for r in DOCS_THAT_MUST_POINT_AT_THE_STATUS_DOC if r not in _PINNED_PRESENT]
    if not _PINNED_PRESENT:
        return  # the public cut removed all four; nothing to pin
    assert not missing, (
        f"{sorted(missing)} are gone while {sorted(_PINNED_PRESENT)} remain. Either the "
        "docs were renamed — repoint DOCS_THAT_MUST_POINT_AT_THE_STATUS_DOC — or a "
        "partial cut has left prod-status docs behind with no guard on them."
    )


def _status_doc_text() -> str | None:
    """The status doc's contents, or None when a consistent public cut removed it."""
    doc = REPO / STATUS_DOC
    if not doc.is_file() and not _PINNED_PRESENT:
        # Consistent public cut: excludes.txt removes the status doc (:143) and all four
        # docs that point at it. Nothing in this tree makes a prod claim for it to answer.
        # The `and not _PINNED_PRESENT` half is load-bearing — a status doc deleted while
        # the docs citing it remain still fails, on the assert below.
        return None
    assert doc.is_file(), f"{STATUS_DOC} is missing — the other guards here point at it"
    return doc.read_text(encoding="utf-8")


# What a reader must be able to run to falsify the file, rather than trust it. Pinned as
# literals on purpose: the previous version of this guard asserted the *answer*
# ("There is no deployed environment"), which is exactly the sentence that went stale and
# stayed green. An assertion about the answer cannot survive the answer changing; an
# assertion that a probe is present can.
REQUIRED_PROBES = (
    "gcloud compute instances list --project rsync-ai-prod",
    '--resolve "app.rsync.ai:443:$IP"',  # quoted + derived: no literal address in a doc
    "/api/health",
)

LAST_VERIFIED = re.compile(r"_Last verified: (\d{4})-(\d{2})-(\d{2})")

# Long enough that an unrelated PR never trips it, short enough that an abandoned
# claim does. This test is ALLOWED to fail on a quiet week -- that is the point: the
# doc asserts a fact with an expiry, so its guard carries the same expiry.
MAX_VERIFICATION_AGE_DAYS = 180


def date_today() -> "datetime.date":
    """Indirection so a test can pin 'today' without patching the stdlib."""
    return datetime.date.today()


def test_the_status_doc_hands_you_a_probe() -> None:
    """The single source of truth must be falsifiable in one command, not merely asserted."""
    text = _status_doc_text()
    if text is None:
        pytest.skip(
            f"{STATUS_DOC} and all four docs pinned to it are absent "
            f"(scripts/flip/excludes.txt)"
        )
    missing = [p for p in REQUIRED_PROBES if p not in text]
    assert not missing, (
        f"{STATUS_DOC} no longer tells the reader how to check it: {missing} are gone. "
        "This file answers 'is anything deployed' for the whole repo, and an answer with "
        "no probe beside it is the claim-with-an-expiry that rotted twice already. If the "
        "environment moved, update the probe — do not delete it."
    )


def test_the_status_doc_is_dated() -> None:
    """A state claim without a date cannot be aged by the reader."""
    text = _status_doc_text()
    if text is None:
        pytest.skip(f"{STATUS_DOC} is absent (scripts/flip/excludes.txt)")
    match = LAST_VERIFIED.search(text)
    assert match, (
        f"{STATUS_DOC} has no `_Last verified: YYYY-MM-DD` line. The date is what lets a "
        "reader weigh the claim against how long ago anyone actually looked."
    )

    verified = datetime.date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    today = date_today()
    assert verified <= today, (
        f"{STATUS_DOC} claims it was verified on {verified}, which is in the future. "
        "Nobody ran the probe on a day that has not happened; fix the date."
    )
    age = (today - verified).days
    assert age <= MAX_VERIFICATION_AGE_DAYS, (
        f"{STATUS_DOC} was last verified {age} days ago ({verified}), past the "
        f"{MAX_VERIFICATION_AGE_DAYS}-day bound. Nothing here says the environment CHANGED "
        "-- it says nobody has looked. This file is the repo's single answer to 'is anything "
        "deployed', and an unchecked answer is how it went wrong in both directions already: "
        "it read LIVE for days after the Azure VM was deleted, then read 'nothing is deployed' "
        "for weeks after a GCP VM took the name over. Re-run the probes in the doc's "
        "'Verify it yourself' section, write down what they actually return, and move the "
        "date. Do not move the date without running them."
    )


@pytest.mark.parametrize("rel", DOCS_THAT_MUST_POINT_AT_THE_STATUS_DOC)
def test_prod_status_docs_link_to_the_status_doc(rel: str) -> None:
    """A reader who lands mid-file must be one link from the real answer."""
    path = REPO / rel
    if not path.is_file():
        # Removed by the public cut. Guarded against becoming a hole by
        # test_the_pinned_doc_set_is_all_or_nothing, which fails if only SOME are gone.
        pytest.skip(f"{rel} is not in this tree (removed by scripts/flip/excludes.txt)")
    text = path.read_text(encoding="utf-8")
    assert "prod-environment-status.md" in text, (
        f"{rel} carries prod status but never links {STATUS_DOC}. A dated status line with "
        f"no pointer to the environment's actual state is how this went wrong the first time."
    )


@pytest.mark.parametrize("pattern", LIVE_CLAIM_PATTERNS)
def test_no_doc_asserts_a_live_prod_environment(pattern: str) -> None:
    """Present-tense 'prod is up' claims. The dated historical record is deliberately spared."""
    offenders = _offenders(pattern)
    assert not offenders, (
        "These lines assert a live prod environment outside the one file that owns the "
        "answer. Something IS deployed today, but that is not the point: a second copy of "
        "the answer drifts from the first, and the gap to `main` changes on every merge.\n"
        f"Mark the line [HISTORICAL] and keep its evidence, or defer to {STATUS_DOC}.\n  "
        + "\n  ".join(offenders)
    )


@pytest.mark.parametrize("pattern", NO_ENVIRONMENT_PATTERNS)
def test_no_doc_asserts_that_nothing_is_deployed(pattern: str) -> None:
    """The mirror image, and the half that was wrong most recently.

    The Azure VM's deletion was true when written and these sentences were correct for
    three weeks. Then a GCP VM took over `app.rsync.ai` and they all went false at once,
    with nothing to fail — the identical mechanism as the original 🟢 LIVE rot, pointing
    the other way. Same remedy: history gets a marker, everything else defers.
    """
    offenders = _offenders(pattern)
    assert not offenders, (
        "These lines assert that nothing is deployed. A single GCP VM "
        "(`rsync-mongo-gcs-e2e`, project `rsync-ai-prod`) has served `app.rsync.ai` since "
        "September 2026.\n"
        f"Mark the line [HISTORICAL] if it is about the deleted Azure host, or defer to "
        f"{STATUS_DOC}.\n  " + "\n  ".join(offenders)
    )
