"""The chart's api-gateway readinessProbe moved from `/health` to `/ready`.

That one line falsified a sentence the repo had written down eleven times: "the
api-gateway logs one warning on a failed connect, stays `1/1` Ready, and serves mock
data, so `helm install` and `kubectl get pods` both look successful." Both halves are
false today, for two independent reasons, and each is worth stating because a future
reader will otherwise assume one of them still holds.

  *Ready.* `/health` is a static literal (api-gateway/cmd/server/main.go:602); `/ready`
  calls `readinessVerdict` (api-gateway/cmd/server/ready.go) and answers 503
  `db_not_connected` / `db_ping_failed` / `schema_not_migrated`. A DB-dead replica now
  stalls at `0/1`, the Deployment never becomes Available, and its Service has no
  endpoints. The failure is loud. It is also permanent: `db.Init` retries the ping for
  60s and then gives up, and `main()` runs migrations only inside Init's success branch,
  so Postgres arriving late does not heal it.

  *Mock data.* The emitter gates on `db.GetDB() == nil`
  (api-gateway/internal/handlers/connections.go:504), and `db.Init` assigns `DB` from
  `sql.Open` BEFORE it pings, so a ping failure leaves `DB` non-nil and the branch
  unreachable.

WHY THIS IS A GUARD AND NOT JUST A SWEEP. An earlier pass rewrote ten operator-facing
documents and recorded that it had "falsified all ten". It had not. Run against the tree
that pass left behind, the sweep below finds 16 assertions of the retired claim across 12
files -- and only 2 of those files are markdown. The other 10 are `.tpl`, `.yaml`, `.sh`
and `.py`, plus one HLD bullet these patterns deliberately do not match. Every doc guard
in this repo sweeps `git ls-files '*.md'`, so an inherited-scope guard would have caught 2
of 12: the retired wording survived in exactly the file types nothing scans. This guard
therefore sweeps EVERY tracked file. A behavioural claim does not become safe by being
written in a comment.

WHAT IT DOES NOT GUARD, AND WHY. The word "silent" on its own is not a discriminator.
Measured across the tree before this file was written, a pattern matching "the failure is
silent" / "fails silently at runtime" / "the silent failure" raised eight hits and NONE of
them was this claim -- they were about Kafka ACLs, a docker network, dead connector env
slots, a session-token migration, and a test's own denominators. A guard that needs eight
exemptions on the day it lands is a guard that will be exempted to death, so the patterns
below anchor on the two phrasings that can only be this claim. Prose calling the
password failure "silent" is fixed by hand and is not defended here.

THE PUBLIC CUT. This guard ships. Its sweep is a public invariant -- the operator-facing
files that carried the claim (the chart README, `values.yaml`, `docs/deployment/
kubernetes.md`, the chart templates) all survive the cut, and better than two thousand
tracked files remain to sweep. `EXEMPT_HISTORY` names four narrative files that record
the incident and are all removed by `scripts/flip/excludes.txt`, so in the public tree
the exemption set is empty by construction; `test_the_exempt_history_set_is_all_or_nothing`
is the positive denominator that stops a private-repo rename from turning that into a
quiet hole.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

# The two phrasings that can only be this claim. Both are matched against
# whitespace-normalized text, because every live site the earlier pass left behind was
# line-wrapped -- a per-line regex sees "stays `1/1` Ready, and serves" on one line and
# "mock data" on the next, and matches neither.
#
# `[^.!?]` bounds each window at a sentence end so the two halves have to be making one
# assertion, not merely co-occurring in a paragraph.
RETIRED_CLAIM_PATTERNS = (
    # "stays 1/1 Ready ... and serves mock data", and the reverse order.
    r"(?:\b1/1\b|\bReady\b)[^.!?]{0,200}?(?:mock data|nothing real|serving fiction)",
    r"(?:mock data|nothing real|serving fiction)[^.!?]{0,200}?(?:\b1/1\b|\bReady\b)",
    # The mechanism the claim rested on. `db.Init` retries the ping for 60s
    # (connectRetryInterval / connectTimeout, api-gateway/internal/db/db.go).
    r"\bnever retries\b",
)

# Markers that turn a quotation into a historical record. The marker must appear BEFORE
# the claim it qualifies -- a file that merely mentions the probe change somewhere is not
# thereby licensed to assert the retired behaviour in the present tense. This ordering rule is
# not hypothetical: the sibling guard test_no_doc_claims_a_live_prod_environment.py
# failed on the CAPABILITIES.md row describing its own fix precisely because an
# anywhere-in-the-line check waved the quotation through.
# Each marker names the change itself rather than the PR that made it, so the sentence a
# marker licenses reads the same in this tree and in the published one.
HISTORY_MARKERS = (
    "before readiness moved to /ready",
    "until readiness moved to /ready",
    "[HISTORICAL]",
)

# How far back the marker may sit. Wide enough for a wrapped sentence, narrow enough that
# a citation in a previous paragraph does not license a fresh claim.
MARKER_LOOKBACK = 200

# Files whose job is to record the incident rather than to describe today's behaviour.
# All four are removed by scripts/flip/excludes.txt (:99, :100, :103, :144), so this set
# is empty in the public tree -- see test_the_exempt_history_set_is_all_or_nothing.
EXEMPT_HISTORY = (
    "CAPABILITIES.md",
    "CAPABILITIES-ARCHIVE.md",
    "BACKLOG.md",
    "deploy/helm/rsync-ai/test/kind/JOURNAL.md",
)

# This file quotes the retired sentence as its own subject.
EXEMPT_SELF = ("llm-service/tests/test_no_doc_promises_a_silent_gateway_db_failure.py",)

EXEMPT = set(EXEMPT_HISTORY) | set(EXEMPT_SELF)

# The sweep's floor. Order 2.4k tracked files in the private repo, 2.2k after the public
# cut -- magnitudes, not counts: an exact number here would be wrong the moment this file
# became the tree's newest tracked file, which is how it was wrong when first written.
# Well under both, because the floor only has to tell "the tree shrank" apart from
# "git ls-files returned nothing and every assertion below passed on an empty set".
MIN_TRACKED_FILES = 1500

_WHITESPACE = re.compile(r"\s+")


def _tracked_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout.split("\0")
    # `git ls-files` is the index, not the disk: a file staged-then-deleted is listed but
    # unreadable. Dropping it is safe only because the denominator test below asserts the
    # corpus is still large -- silently sweeping nothing is this file's whole failure mode.
    return [p for p in (REPO / q for q in out if q and q not in EXEMPT) if p.is_file()]


def _readable_text(path: Path) -> str | None:
    """Whitespace-normalized contents, or None for anything that is not text."""
    try:
        raw = path.read_bytes()
    except OSError:  # pragma: no cover - defensive
        return None
    if b"\0" in raw[:8192]:
        return None
    try:
        return _WHITESPACE.sub(" ", raw.decode("utf-8"))
    except UnicodeDecodeError:
        return None


def _offenders(pattern: str) -> list[str]:
    rx = re.compile(pattern)
    hits: list[str] = []
    for path in _tracked_files():
        text = _readable_text(path)
        if text is None:
            continue
        for m in rx.finditer(text):
            back = text[max(0, m.start() - MARKER_LOOKBACK) : m.start()]
            if any(marker in back for marker in HISTORY_MARKERS):
                continue
            hits.append(f"{path.relative_to(REPO)}: ...{text[m.start():m.end()][:140]}...")
    return hits


def test_the_sweep_has_a_denominator() -> None:
    """Positive denominator. The sweep below passes on an empty corpus."""
    found = _tracked_files()
    assert len(found) >= MIN_TRACKED_FILES, (
        f"only {len(found)} tracked files were readable, expected at least "
        f"{MIN_TRACKED_FILES}. The sweep below cannot have checked anything meaningful; "
        "fix the enumeration rather than trusting its pass."
    )


def test_the_exempt_history_set_is_all_or_nothing() -> None:
    """The public cut removes all four narrative files together, so "none present" is the
    public repo and is fine. "Some present" is the case an exemption must never cover: a
    rename or a deletion in the private repo, which is how an exemption list quietly stops
    naming anything and starts excusing whatever inherits the name.
    """
    present = [rel for rel in EXEMPT_HISTORY if (REPO / rel).is_file()]
    if not present:
        return  # the public cut removed all four
    missing = [rel for rel in EXEMPT_HISTORY if rel not in present]
    assert not missing, (
        f"{sorted(missing)} are gone while {sorted(present)} remain. Either they were "
        "renamed -- repoint EXEMPT_HISTORY -- or a partial cut has left narrative files "
        "exempt under names that no longer describe them."
    )


def test_this_guard_exempts_itself_by_a_path_that_exists() -> None:
    """EXEMPT_SELF is a string literal, and a renamed file would silently un-exempt this
    module -- turning its own docstring into an offender and the failure into a puzzle.
    """
    for rel in EXEMPT_SELF:
        assert (REPO / rel).is_file(), f"EXEMPT_SELF names {rel}, which is not in the tree"


@pytest.mark.parametrize("pattern", RETIRED_CLAIM_PATTERNS)
def test_the_patterns_match_the_sentence_they_retire(pattern: str) -> None:
    """Non-vacuity, checked in-process rather than assumed.

    Every assertion in this file is an absence, and absence is what a broken regex also
    produces. These three inputs are the wording as it was actually shipped -- the chart
    README, the kind values overlay, and the chart's own test hook -- wrapped the way they
    were wrapped on disk, so a pattern that stops matching wrapped prose fails here rather
    than going quietly green over the whole tree.
    """
    shipped = {
        RETIRED_CLAIM_PATTERNS[0]: (
            "the api-gateway logs one warning on a failed connect, stays `1/1` Ready,\n"
            "and serves mock data."
        ),
        RETIRED_CLAIM_PATTERNS[1]: (
            "a gateway that lost the cold-boot race against Postgres and is quietly\n"
            "serving mock data fails the test while its pod still reports `Ready`."
        ),
        RETIRED_CLAIM_PATTERNS[2]: (
            "it logs one warning, falls back to mock data, never retries, and answers\n"
            "/health 200 forever."
        ),
    }[pattern]
    assert re.search(pattern, _WHITESPACE.sub(" ", shipped)), (
        "this pattern no longer matches the sentence it exists to retire, so its sweep "
        "over the tree proves nothing"
    )


@pytest.mark.parametrize("pattern", RETIRED_CLAIM_PATTERNS)
def test_no_tracked_file_promises_a_silent_gateway_db_failure(pattern: str) -> None:
    """The sweep. Every tracked file, not just markdown -- that restriction is why 10 of
    the 12 files still carrying the claim were invisible to every other doc guard here."""
    hits = _offenders(pattern)
    assert not hits, (
        "These describe a failure mode the chart no longer has. The api-gateway's "
        "readinessProbe is /ready, so a DB-dead replica stalls at 0/1 with "
        "503 db_ping_failed instead of reporting 1/1 Ready; and the mock-data branch is "
        "unreachable because db.Init assigns DB before it pings.\n"
        "Say what happens today, or quote the old wording behind one of "
        f"{list(HISTORY_MARKERS)} placed BEFORE the quotation.\n  " + "\n  ".join(hits)
    )
