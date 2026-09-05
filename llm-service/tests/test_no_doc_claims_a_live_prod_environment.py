"""The Azure VM behind app.rsync.ai was deleted in late August 2026.

This guard exists because of how the claim failed, not because of the claim itself.
`PRODUCT_STATUS.md` carried "🟢 LIVE — prod HEAD `e6371a8a`" and `CAPABILITIES.md`
carried "✅ prod is level with origin/main" for days after the machine was gone. Nothing
failed, because a status line has no expiry: it was written true, and it went false on its
own while every test in the repo stayed green. Sessions then quoted it back as current
state and reported how far "prod" lagged `main` against a host that did not exist.

So the durable fix is not the wording — it is a check that fails if the wording comes back.
Two halves:

  1. every doc that carries prod status must point at the one file that owns the answer, and
  2. none of them may assert a live prod environment in the present tense.

If a new environment IS stood up, this guard is what you edit first: correct
`docs/deployment/prod-environment-status.md`, then this file, then the banners. That
ordering is the point — the status lives in one place, and changing it is a deliberate act
with a test in the way, rather than a line that quietly rots.

THE PUBLIC CUT. This file has a MIXED subject and therefore stays, running, in the public
repo. Half of it — the sweep over every tracked `*.md` — is a public invariant: a public
doc claiming a live prod is exactly the confusion this guard was written to stop, and 110
markdown files survive the cut to be swept. The other half pins four docs by name, and
`scripts/flip/excludes.txt` deletes all four. Those four cases skip when their subject is
absent, and `test_the_pinned_doc_set_is_all_or_nothing` is the positive denominator that
stops the skip from turning into a quiet hole: absence is only allowed when EVERY pinned
doc is absent, i.e. the whole cut ran. Deleting or renaming one of them in the private
repo is still red.
"""

from __future__ import annotations

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
LIVE_CLAIM_PATTERNS = (
    r"🟢\s*LIVE",
    r"\bprod is level with\b",
    r"\bprod is on `main`",
    r"\bis live on prod\b",
    r"\bprod is currently\b",
    r"\bprod HEAD is\b",
)

# Files whose job is to record history, or to hold the correction itself.
EXEMPT = {
    STATUS_DOC,
    "CAPABILITIES-ARCHIVE.md",
    "llm-service/tests/test_no_doc_claims_a_live_prod_environment.py",
}


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


def test_the_markdown_sweep_has_a_denominator() -> None:
    """Positive denominator for the sweep below, which passes on an empty corpus."""
    found = _tracked_markdown()
    assert len(found) >= MIN_TRACKED_MARKDOWN, (
        f"only {len(found)} tracked markdown files were readable, expected at least "
        f"{MIN_TRACKED_MARKDOWN}. The sweep below cannot have checked anything "
        "meaningful; fix the enumeration rather than trusting its pass."
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


def test_the_status_doc_exists_and_says_there_is_no_environment() -> None:
    """The single source of truth. Everything else defers to this file."""
    doc = REPO / STATUS_DOC
    if not doc.is_file() and not _PINNED_PRESENT:
        # Consistent public cut: excludes.txt removes the status doc (:122) and all
        # four docs that point at it. Nothing in this tree makes a prod claim for it
        # to answer. The `and not _PINNED_PRESENT` half is load-bearing — a status doc
        # deleted while the docs citing it remain still fails, on the line below.
        pytest.skip(
            f"{STATUS_DOC} and all four docs pinned to it are absent "
            f"(scripts/flip/excludes.txt)"
        )
    assert doc.is_file(), f"{STATUS_DOC} is missing — the other guards below point at it"
    text = doc.read_text(encoding="utf-8")
    assert "There is no deployed environment" in text or "no deployed environment" in text, (
        f"{STATUS_DOC} no longer states that nothing is deployed. If an environment was "
        f"stood up, update this guard deliberately — do not delete the sentence."
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
    rx = re.compile(pattern)
    offenders: list[str] = []
    for path in _tracked_markdown():
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:  # pragma: no cover - defensive
            continue
        for n, line in enumerate(lines, 1):
            hit = rx.search(line)
            if not hit:
                continue
            # The marker must come BEFORE the claim it qualifies -- a line that merely
            # mentions "[HISTORICAL]" somewhere is not thereby exempt. This is not
            # hypothetical: the CAPABILITIES.md row recording this very fix quoted the
            # retired claim verbatim, and an anywhere-in-the-line check waved it through.
            marker = min(
                (line.find(m) for m in ("[HISTORICAL]", "HISTORICAL —", "DECOMMISSIONED")
                 if line.find(m) != -1),
                default=-1,
            )
            if marker != -1 and marker < hit.start():
                continue
            offenders.append(f"{path.relative_to(REPO)}:{n}: {line.strip()[:160]}")
    assert not offenders, (
        "These lines assert a live prod environment. The Azure VM was deleted in late "
        "August 2026 and nothing has replaced it.\n"
        "Mark the line [HISTORICAL] and keep its evidence, or delete the claim.\n  "
        + "\n  ".join(offenders)
    )
