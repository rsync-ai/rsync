"""A doc that says "NOT merged" about code that is in `main` is worse than no doc.

CAPABILITIES.md is the file every agent is instructed to read first, and its merge-status
claims steer what work looks outstanding. Those claims are written at a moment and read
forever, so they rot in one direction only: code merges, the row does not get updated, and
the row goes on advertising finished work as pending.

This has now happened twice, at scale. On 2026-08-12 two rows were caught and corrected
("this said unmerged"). On 2026-08-17 nine more were found in one pass -- including four
that had been false since 2026-07-03 (45 days) and, worst, KI-SEC-TOPOLOGY-API-UNAUTHENTICATED,
where "NOT merged, NOT deployed" hid the fact that the *merge* had happened and only the
*deploy* had not. That inversion matters: "not merged" reads as "work still to do", while
"merged but not deployed" reads as "a security fix is sitting on the shelf". It was the
second one, and prod had been serving the unauthenticated route the whole time.

The check is deliberately crude, because the precise version is not decidable from text: a
row that claims its code is unmerged, while every source path it links to already exists on
`main`, is treated as stale. That is exactly the shape all nine had.

It happened a third time on 2026-08-23 -- and this file was watching and did not fire.
CAPABILITIES.md:886 read "NOT merged, NOT deployed" for four days after the code merged
and both migrations applied on prod. The row linked eight targets, seven of them source
files already on `main`; the eighth was a `.md` with a `#section` fragment, which the
local link regex here did not strip, so it was not exempted as documentation, failed the
git probe as if it were missing source, and put the row at 7-of-8 -- one short of the
all-present trigger. Six of the doc's 528 unique targets parsed that way. The parser now
lives in `doclinks`, shared with `test_doc_links_resolve.py`, which already had it right;
a second copy is a second set of bugs, and the uncopied pins are where it drifts.

Venue note, corrected 2026-08-29. This paragraph used to read "CI checks out shallow (no
`fetch-depth` anywhere in .github/workflows), so `origin/main` may be absent there and the
test skips". That was already false when written -- ci.yml's llm-service job carries
`fetch-depth: 0` -- and it papered over the real gap, which sat one level up and is the same
one the header of .github/workflows/doc-links.yml describes for the link gate. The job that
runs this test is gated on a paths-filter carrying no `.md` entry, inside a workflow whose
`pull_request` trigger carries `paths-ignore: ['**.md', 'docs/**', ...]`. So the guard whose
only input is CAPABILITIES.md could not run on a change to CAPABILITIES.md; it fired at all
only when a PR happened to touch code as well.

That bill came due on 2026-08-29. #883 merged, this test went red on `main` naming ten stale
rows -- and #884, the docs-only PR that fixed them, ran exactly ONE check (the link script),
never this one, then merged to a green `main` that had skipped it. A wrong fix would have
looked identical. The gate now also runs in doc-links.yml, which is deliberately unfiltered.

The skip below is kept for its one honest case: a local shallow clone with no `main` ref. In
a venue that PROMISES the ref, skipping would be "an empty set reads as a pass" in a new
costume, so that venue sets DOC_GUARDS_REQUIRE_MAIN_REF=1 and a missing ref fails, named.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

from doclinks import repo_path_targets

# CAPABILITIES.md is removed by scripts/flip/excludes.txt, and this module reads
# it during collection (the parametrize below). Without this, `pytest
# tests/test_doc_merge_claims_are_true.py ...` -- doc-links.yml's exact
# invocation -- aborts the whole session with exit 2 and none of the other
# eleven doc guards run. See _cut_collection.py for why conftest cannot do this.
from _cut_collection import skip_if_cut

skip_if_cut("CAPABILITIES.md")

REPO = Path(__file__).resolve().parents[2]
DOC = REPO / "CAPABILITIES.md"

# The claim itself. Lower-cased before matching.
#
# The commit-status spellings are here because of a 2026-08-30 miss: two rows read
# "PR pending -- code is in the working tree, not committed" about code that had
# merged in #883 the previous day, and this guard did not fire because none of those
# words is "merged". A row claiming its code is uncommitted makes the same promise to
# the reader as one claiming it is unmerged -- work still to do -- and goes stale the
# same way, so it is the same claim and gets the same probe.
#
# Scope stays CAPABILITIES.md, and that is now measured rather than asserted.
#
# A 2026-08-30 sweep looked for this claim in every tracked *.md, because a guard
# scoped to the file you were handed finds a fraction of the class. It found the
# class in five other files and TEN false instances -- four in
# security-audit-results/REPORT.md (Tier A/B and the CI scanners, all merged in #435
# `75f6a427` and in the deployed prod build), two in INVENTORY.md and one in
# PRODUCT_STATUS.md (MongoDB destination, #467 `d7ab6024`), one in BACKLOG.md
# (#760 `c9f61e86`), and the tool-generator evidence file's F-2/F-3/F-7 (#61
# `56198116`). None of them could ever have fired here. That is the cost of the
# scope, stated honestly.
#
# The sweep then measured what widening would actually buy, by running the probe
# below over those files. The answer was: nothing, and it would cost correctness.
#   * INVENTORY.md, PRODUCT_STATUS.md -- 0 claim lines to judge.
#   * BACKLOG.md:484 -- WOULD FAIL, and the row was FALSE.
#     CORRECTED 2026-09-04: this bullet read "the row is TRUE ... a real false
#     positive on a verified-true row", and it was wrong -- so the one concrete
#     false positive the scope decision rested on was actually a TRUE positive.
#     The error was in the test it chose, not in the sweep: it asked whether
#     `sumBatchAcks` had gained a lower bound on the ack sum, but that is the
#     `ackBaseline` design the row itself records as "unsound and was not used"
#     (it races the sink's async drain). Its absence is the fix working as
#     designed. What shipped moved BOTH sides onto the whole execution via the
#     producer outbox -- `sumDispatchedRows` (silent_drop_check.go:261) and
#     `reconcileInputs` (:302), both on origin/main, both added by `c9f61e86`
#     (#760, merged 2026-08-09). The sweep eight lines above had this right,
#     listing BACKLOG.md's instance among the false claims and pinning it to
#     that same commit; only this bullet's verification contradicted it.
#     Probing for a retracted design and reading its absence as "unmerged" is
#     the same shape as the defect this guard exists to catch.
#     The scope conclusion below still holds, but now rests on the archive
#     bullet and the soundness argument alone. Widening to BACKLOG.md today
#     would still misfire: the row was corrected on 2026-09-04 and now carries
#     the retired "NOT merged, NOT deployed" wording inside a correction
#     narrative, so `_CLAIM` matches a row that is true -- the same
#     quotes-its-own-fix trap that has bitten a sibling guard before.
#   * CAPABILITIES-ARCHIVE.md -- WOULD FAIL twice, both on the senses named below.
# So the presence probe is unsound outside CAPABILITIES.md: it judges whether the
# LINKED FILES exist, which only tracks merge status when the fix ADDED them.
#
# The archive's own two: "uncommitted"/"not committed" appear there in an unrelated
# Kafka sense ("offsets are not committed until a write succeeds", :1000) and about
# generated connectors that are deliberately untracked (:28); and its rows mostly
# link files that pre-date their fix. Pointing this at the archive would manufacture
# false positives, and the exemptions needed to silence them would hollow out the
# guard. Closing the residual gap needs a different probe -- one that asks whether
# the CHANGE landed (`git log -S` on a distinctive string) rather than whether the
# file exists -- not a wider glob over this one.
_CLAIM = re.compile(
    r"not merged|unmerged|not yet merged"
    r"|not committed|uncommitted|in a working tree|in the working tree"
)

# A line that carries its own correction has already been through this check by hand;
# the phrase survives only inside the quoted history ('this row said "NOT merged"').
_ALREADY_RECONCILED = re.compile(r"correct(ion|ed)", re.I)

# A row that NAMES the PR its fix landed in has answered the merge question on its own
# face, so an "unmerged" phrase left in it is history rather than a live claim. This
# replaces the older `startswith("| ~~")` exemption, which skipped every struck-through
# row and so could not see the defect it most needed to: a row marked RESOLVED that
# simultaneously says its own fix has not merged. That claim self-destructs the moment
# the PR lands, and the strikethrough rule guaranteed nobody looked.
#
# Found 2026-09-04, when CAPABILITIES.md:806 and :807 had each carried "fix in the
# unmerged working tree" for two days after #933 merged. Both matched _CLAIM, neither
# was already-reconciled, and every source path they linked was on main -- the probe
# would have failed them correctly on the day they were written.
#
# Measured before widening, over the whole doc in both states: the stale doc yields
# exactly {806, 807} and the corrected doc yields none. A wordlist would not have done
# this. Dropping the strikethrough rule with no replacement raises three false positives
# (:898, :900, :901), all reconciled rows citing #760 whose exemption reads "Re-verified
# against the tree" -- wording _ALREADY_RECONCILED never scans. Keying on whether a PR is
# named at all is structural, so it does not care which words the row chose.
#
# Residual hole, stated rather than hidden: a row that names a PR *and* still falsely
# claims to be unmerged stays exempt. Pinned by
# test_naming_a_pr_exempts_a_row_even_if_it_still_claims_unmerged, which fails the day
# someone closes it, so the gap cannot be mistaken for coverage.
_NAMES_A_PR = re.compile(r"/pull/\d+")

# Paths that are not source files -- linking one proves nothing about a merge.
# Read only against a target `doclinks.normalize_target` has already stripped: this
# anchors on the end of the string, so an un-stripped `#fragment` makes every `.md`
# link read as a source path. See _linked_source_paths.
_NOT_SOURCE = re.compile(r"\.(md|png|svg|jpg)$")


def _main_ref() -> str | None:
    for ref in ("origin/main", "main"):
        if subprocess.run(
            ["git", "rev-parse", "--verify", "-q", ref],
            cwd=REPO, capture_output=True,
        ).returncode == 0:
            return ref
    return None


# Set by the venue that GUARANTEES the ref: doc-links.yml's `doc-guards` job, which
# checks out with `fetch-depth: 0`. There, "no ref" does not mean "nothing to check",
# it means that promise regressed -- so it is a failure. Skipping would be the same
# empty-set-reads-as-a-pass shape this whole file exists to catch.
_REQUIRE_MAIN_REF = "DOC_GUARDS_REQUIRE_MAIN_REF"


def _main_ref_or_bail() -> str:
    """The ref, or end the test -- as a FAILURE anywhere the ref was promised."""
    ref = _main_ref()
    if ref is not None:
        return ref
    if os.getenv(_REQUIRE_MAIN_REF) == "1":
        pytest.fail(
            f"{_REQUIRE_MAIN_REF}=1 promises an unshallow checkout, but neither "
            "`origin/main` nor `main` resolves here. The job's checkout has lost its "
            "`fetch-depth: 0`, so every claim below would have skipped and reported green."
        )
    pytest.skip("no origin/main or main ref (shallow local checkout) -- see module docstring")


def _exists_on_main(ref: str, path: str) -> bool:
    return subprocess.run(
        ["git", "cat-file", "-e", f"{ref}:{path}"],
        cwd=REPO, capture_output=True,
    ).returncode == 0


def _linked_source_paths(line: str) -> list[str]:
    """Repo paths on `line` that are source files, so their presence means something.

    Every exclusion here works by suffix, which is only correct once the fragment and
    the line anchor are gone -- hence the shared normalizer rather than a local regex.
    Each path this drops or mangles is a free exemption for the row carrying it: a
    mis-parsed target fails the `git cat-file` probe, reads as "absent from main", and
    breaks the all-present condition the assertion below is looking for.
    """
    return [p for p in repo_path_targets(line) if not _NOT_SOURCE.search(p)]


def _is_live_claim(line: str) -> bool:
    """One line's verdict, factored out so the tests below exercise the real rule.

    Inlining this in _live_claims would let a regression test drift from the predicate
    it claims to pin -- the failure mode this whole file exists to catch.
    """
    if not _CLAIM.search(line.lower()):
        return False
    return not (_ALREADY_RECONCILED.search(line) or _NAMES_A_PR.search(line))


def _live_claims() -> list[tuple[int, str]]:
    """Lines asserting something is unmerged, minus already-corrected ones and ones naming a PR.

    Deliberately does NOT exempt struck-through rows: a row marked RESOLVED that still
    says its own fix is unmerged is the defect, not the exemption. See _NAMES_A_PR.
    """
    return [
        (n, line)
        for n, line in enumerate(DOC.read_text().split("\n"), start=1)
        if _is_live_claim(line)
    ]


def test_the_doc_and_the_link_scanner_both_work():
    """Vacuity floor. A parser that finds nothing would make every other test here green."""
    text = DOC.read_text()
    assert len(text.split("\n")) > 800, "CAPABILITIES.md is far shorter than expected -- wrong file?"
    all_links = [p for line in text.split("\n") for p in _linked_source_paths(line)]
    assert len(all_links) > 100, (
        f"only {len(all_links)} repo-path links found across the whole doc; the link regex "
        "has stopped matching, so an absent claim below would mean nothing"
    )


def test_the_presence_probe_can_answer_both_ways():
    """Two-sided control on the git probe itself.

    A probe stuck on 'absent' would silently exempt every claim. Pin it against one path
    that certainly is on main and one that certainly is not.
    """
    ref = _main_ref_or_bail()
    assert _exists_on_main(ref, "CAPABILITIES.md"), f"{ref} has no CAPABILITIES.md -- probe is broken"
    assert not _exists_on_main(ref, "this/path/does/not/exist.go"), "probe reports absent paths as present"


def test_a_doc_link_does_not_buy_an_exemption():
    """The 2026-08-23 miss, pinned as a line rather than as a live doc row.

    CAPABILITIES.md:886 claimed "NOT merged, NOT deployed" for four days after the
    code merged and deployed, and this test did not fire. The row linked eight
    targets; seven were source files present on main, and the eighth was a `.md`
    with a `#section` fragment. The old local regex stripped only `#L\\d+`, so the
    fragment survived, `_NOT_SOURCE` did not recognise a `.md`, the target failed
    the git probe as a source path, and 7-of-8-present fell one short of the
    all-present condition. One documentation link bought the row an exemption.

    Asserted against a literal, not against the doc: the row has since been
    corrected, so reading the live file would make this test green for the wrong
    reason forever after.
    """
    line = (
        "| **Saved-query edits** | ✅ | Code-only, **NOT merged** — "
        "[handler](api-gateway/internal/handlers/saved_queries.go:114-119), "
        "[migration](api-gateway/migrations/097_saved_query_version_retention.sql), "
        "[route](api-gateway/cmd/server/main.go:1199), "
        "[docs](docs/explorer/saved-queries-and-models.md#6-version-history-diff-and-restore) |"
    )
    got = _linked_source_paths(line)
    assert got == [
        "api-gateway/internal/handlers/saved_queries.go",
        "api-gateway/migrations/097_saved_query_version_retention.sql",
        "api-gateway/cmd/server/main.go",
    ], got


@pytest.mark.parametrize(
    "lineno,line",
    _live_claims() or [pytest.param(0, "", id="no-live-claims")],
    ids=lambda v: f"L{v}" if isinstance(v, int) else None,
)
def test_an_unmerged_claim_is_not_contradicted_by_main(lineno: int, line: str):
    if lineno == 0:
        pytest.skip("CAPABILITIES.md currently makes no un-reconciled 'not merged' claim")

    ref = _main_ref_or_bail()

    paths = _linked_source_paths(line)
    if not paths:
        pytest.skip("claim links no source path, so there is nothing to check it against")

    present = [p for p in paths if _exists_on_main(ref, p)]
    assert len(present) != len(paths), (
        f"CAPABILITIES.md:{lineno} says its code is not merged, but all {len(paths)} source "
        f"path(s) it links already exist on {ref}:\n  "
        + "\n  ".join(f"git cat-file -e {ref}:{p}   # exists" for p in present)
        + "\n\nEither the claim is stale (find the landing commit with "
        "`git log --oneline --diff-filter=A " + ref + " -1 -- <path>` and rewrite the row with "
        "it), or the files pre-date the fix -- in which case say so on the line and include "
        "the word 'corrected' with the date you checked."
    )


# --- the strikethrough gap, closed 2026-09-04 -------------------------------------
#
# These three pin the rule that replaced `startswith("| ~~")`. The first two are the
# real lines from the day the gap was found, verbatim, so they keep their meaning even
# if CAPABILITIES.md is later rewritten around them.

_STALE_806 = (
    "| ~~**The prod overlay throws away the marker that parks a bundled service, so "
    "every `docker-compose.prod.yml + byo-kafka` stack renders zero services**~~ "
    "✅ **RESOLVED 2026-09-02** — fix in the unmerged working tree; entry moved to "
    "[CAPABILITIES-ARCHIVE.md](CAPABILITIES-ARCHIVE.md). Guard 11/11 green | "
    "[docker-compose.prod.yml:311](docker-compose.prod.yml:311) |"
)

_RECONCILED_898 = (
    "| ~~**sqlserver connector**~~ "
    "[connector.py:377](shared/mcp-connectors/public/database/sqlserver/versions/v1.0.0/connector.py:377) | "
    "✅ **RESOLVED — [#760](https://github.com/rsync-ai/rsync-ai/pull/760) `c9f61e86`, "
    "merged + prod-deployed 2026-08-09.** This cell claimed the fix was unmerged until "
    "2026-08-12: the branch was folded into #760 and the claim was never re-tested. "
    "Re-verified against the tree, not the branch name. |"
)


def test_a_resolved_row_naming_no_pr_is_still_probed():
    """The defect the old exemption could not see.

    CAPABILITIES.md:806 was struck through AND marked RESOLVED AND said its own fix was
    unmerged, two days after #933 carried it. `startswith("| ~~")` skipped it on the
    strikethrough alone. It must be collected now.
    """
    assert _is_live_claim(_STALE_806), (
        "a struck-through row that claims its own fix is unmerged and names no PR is "
        "exactly the self-destructing claim this guard exists to catch"
    )
    # And the probe must actually be able to fail it: every path it links is on main.
    ref = _main_ref_or_bail()
    paths = _linked_source_paths(_STALE_806)
    assert paths, "fixture stopped linking source paths; the assertion above went vacuous"
    assert all(_exists_on_main(ref, p) for p in paths)


def test_a_row_that_names_its_pr_is_exempt():
    """The false positives a naive widening would have raised.

    :898/:900/:901 are reconciled rows citing #760 whose exemption reads "Re-verified
    against the tree" -- wording _ALREADY_RECONCILED never scans. They are exempt because
    they NAME the PR, which is structural and does not depend on their phrasing.
    """
    assert not _is_live_claim(_RECONCILED_898)
    assert not _ALREADY_RECONCILED.search(_RECONCILED_898), (
        "fixture no longer demonstrates the point: it is now exempt via the "
        "correction regex, so it stops proving the PR rule carries it"
    )


def test_naming_a_pr_exempts_a_row_even_if_it_still_claims_unmerged():
    """The residual hole, pinned so it cannot be mistaken for coverage.

    A row naming a PR is exempt unconditionally. If someone tightens that, this test
    fails and should be deleted along with the caveat in _NAMES_A_PR's comment.
    """
    still_uncaught = (
        "| ~~**Something**~~ ✅ RESOLVED — written in "
        "[#999](https://github.com/rsync-ai/rsync-ai/pull/999) but NOT merged | "
        "[executor.go:1](backend-orchestrator/internal/agents/executor/executor.go:1) |"
    )
    assert not _is_live_claim(still_uncaught)
