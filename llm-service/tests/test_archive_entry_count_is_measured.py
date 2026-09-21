"""The archived-entry count in CAPABILITIES.md must equal the archive's real census.

This number has gone wrong twice, each time differently, and both times silently.

First it simply stopped being updated: it read `65` long after the archive had grown
past 180, because a count written as a literal is a claim with an expiry and nothing
fires when it expires. #889 re-measured it to 190 and wrote down that the count is
*measured, not incremented*.

Then the corrected line and the stale one ended up in the file **together** -- one
saying 190, the one directly below it still saying 65 -- the duplicated-line class
from #905, where a patch carries pre-fix text as context and resurrects what a
merged PR had deleted. A reader hitting those two adjacent lines cannot tell which
is current, and neither could grep.

So the count stops being prose and becomes an assertion. The test recomputes the
census the doc says it is quoting and compares. Archive an entry without updating
the number and this fails; leave two copies of the line behind and this fails too.

The same class one level up: a whole ENTRY left behind, not a line.
--------------------------------------------------------------------------------

Archiving is a move, and a move that only does the copy leaves the original where it
was. `KI-CHANGELOG-ANNOUNCES-A-RELEASE-THAT-NEVER-EXISTED` was archived by #941 with a
full resolution write-up, its status-board row was struck through as RESOLVED in the
same PR -- and the `### ... x (active, MED/honesty ...)` entry stayed in
CAPABILITIES.md for two days afterwards, saying the changelog still announces a 1.0.0
that no tag points at. It did not. The archived copy was a strict superset of the
stranded one, so nothing was lost by deleting it and nothing flagged it either.

This is not the first time. CAPABILITIES.md itself records the 2026-07-30 audit that
found "a *second, contradictory* entry for this same KI ... reading '(active, found
2026-07-14)'" for `KI-SQLSERVER-SCHEMA-SCOPE`, and deleted it by hand. A defect that
recurs and is caught by eye twice is one an assertion should be catching.

The discriminator is structural, not a wordlist: a `### ` heading in CAPABILITIES.md
that is neither struck through nor marked resolved, naming a `KI-` slug that also
heads an entry in CAPABILITIES-ARCHIVE.md. Measured against the tree before it was
written, it separated exactly one entry from the other 41 -- so it is aimed at the
defect, not at the shape of every entry.
"""

from __future__ import annotations

import re
from pathlib import Path

from _cut_collection import skip_if_cut

# Both inputs are removed by scripts/flip/excludes.txt (:78, :79). This guard
# measures one against the other, so either one missing leaves it nothing to say.
skip_if_cut("CAPABILITIES.md", "CAPABILITIES-ARCHIVE.md")

REPO = Path(__file__).resolve().parents[2]

CAPABILITIES = REPO / "CAPABILITIES.md"
ARCHIVE = REPO / "CAPABILITIES-ARCHIVE.md"

# The banner line in CAPABILITIES.md that quotes the count.
BANNER = re.compile(r"^> 📦 \*\*Resolved / historical Known issues\*\* \((\d+) closed entries")

# What the banner claims to be counting, stated once in the banner itself:
# `### ` headings naming a `KI-` in CAPABILITIES-ARCHIVE.md.
ARCHIVE_ENTRY = re.compile(r"^### .*KI-")


def _banner_lines() -> list[tuple[int, str]]:
    return [
        (n, line)
        for n, line in enumerate(CAPABILITIES.read_text(encoding="utf-8").splitlines(), 1)
        if line.startswith("> 📦 **Resolved / historical Known issues**")
    ]


def test_the_inputs_exist():
    """Guard the denominator: an empty scan must fail, not read as a pass."""
    assert CAPABILITIES.is_file() and CAPABILITIES.read_text(encoding="utf-8").strip()
    assert ARCHIVE.is_file() and ARCHIVE.read_text(encoding="utf-8").strip()


def test_there_is_exactly_one_archived_count_banner():
    banners = _banner_lines()
    assert len(banners) == 1, (
        "CAPABILITIES.md should carry exactly one archived-entry banner, found "
        f"{len(banners)}: " + ", ".join(f"line {n}" for n, _ in banners) + ". Two copies "
        "of this line disagreed about the count once already (190 vs 65); a reader "
        "cannot tell which is current."
    )


def test_the_stated_count_equals_the_measured_census():
    (lineno, line), = _banner_lines()
    match = BANNER.match(line)
    assert match, f"CAPABILITIES.md:{lineno}: banner present but no count parsed from: {line[:120]}"
    stated = int(match.group(1))
    measured = sum(
        1 for l in ARCHIVE.read_text(encoding="utf-8").splitlines() if ARCHIVE_ENTRY.match(l)
    )
    assert stated == measured, (
        f"CAPABILITIES.md:{lineno} says {stated} archived entries; CAPABILITIES-ARCHIVE.md "
        f"actually contains {measured} (`### ` headings naming a `KI-`). The count is "
        "measured, not incremented -- re-run the census and update the banner."
    )


# A Known-issue heading in either file, e.g. `### KI-FOO-BAR x (active, ...` or
# `### ~~KI-FOO-BAR~~ [check] RESOLVED ...`. Group 1 is the strikethrough, group 2 the slug.
KI_HEADING = re.compile(r"^###\s+(~~)?(KI-[A-Z0-9][A-Z0-9-]*)")

# The two markers this repo uses for "no longer active", per the definition-of-done
# table in CLAUDE.md: strike the heading through, and mark it RESOLVED.
_RESOLVED_MARK = "\u2705"


def _ki_slugs(path: Path) -> set[str]:
    """Every KI slug that heads an entry in `path`, resolved or not."""
    return {
        m.group(2)
        for m in (KI_HEADING.match(l) for l in path.read_text(encoding="utf-8").splitlines())
        if m
    }


def _active_ki_headings(path: Path) -> list[tuple[int, str]]:
    """`(line number, slug)` for entries still presented as open."""
    out = []
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        m = KI_HEADING.match(line)
        if m and not m.group(1) and _RESOLVED_MARK not in line:
            out.append((n, m.group(2)))
    return out


def test_the_known_issue_parser_is_not_vacuous():
    """The control. Both assertions below are 'no overlap' shaped, and two empty sets
    have no overlap either -- so the extractors are pinned to fixtures first."""
    fixture = Path(__file__).parent / "_ki_heading_fixture.md"
    fixture.write_text(
        "# Known issues\n\n"
        "### KI-STILL-BROKEN \u274c (active, HIGH) \u2014 a thing is broken\n"
        "### ~~KI-WAS-BROKEN~~ \u2705 RESOLVED 2026-01-01 by #1 \u2014 it was fixed\n"
        "### KI-ALSO-FIXED \u2705 RESOLVED \u2014 struck through nowhere, marked resolved anyway\n"
        "Prose naming ### KI-NOT-A-HEADING mid-sentence is not a heading.\n"
        "## KI-WRONG-DEPTH is not a `### ` heading.\n",
        encoding="utf-8",
    )
    try:
        assert _ki_slugs(fixture) == {"KI-STILL-BROKEN", "KI-WAS-BROKEN", "KI-ALSO-FIXED"}
        assert [s for _, s in _active_ki_headings(fixture)] == ["KI-STILL-BROKEN"], (
            "The active-entry extractor does not separate open entries from resolved "
            "ones, so its answer on the real file means nothing."
        )
    finally:
        fixture.unlink()


def test_both_files_yield_entries():
    """The data floor. `no overlap` must be measured, not produced by an empty scan."""
    active = _active_ki_headings(CAPABILITIES)
    archived = _ki_slugs(ARCHIVE)
    assert len(active) >= 10, (
        f"CAPABILITIES.md yields only {len(active)} active Known-issue headings. The "
        "overlap check below would be comparing almost nothing and reporting green."
    )
    assert len(archived) >= 100, (
        f"CAPABILITIES-ARCHIVE.md yields only {len(archived)} entry headings, against "
        f"a stated count of 238. The overlap check below would find nothing to hit."
    )


def test_no_resolved_entry_is_still_listed_as_active():
    """Archiving is a move. A copy that leaves the original behind is a doc that
    reports a fixed bug as broken, in the file CLAUDE.md sends every agent to first."""
    archived = _ki_slugs(ARCHIVE)
    stranded = [(n, s) for n, s in _active_ki_headings(CAPABILITIES) if s in archived]
    assert not stranded, (
        "CAPABILITIES.md still presents these as active, but CAPABILITIES-ARCHIVE.md "
        "already carries a resolved write-up for each:\n"
        + "\n".join(f"  CAPABILITIES.md:{n}  {s}" for n, s in stranded)
        + "\n\nArchiving is a move, not a copy -- delete the stranded entry. A reader "
        "hitting both cannot tell which is current, and gate 1 of CLAUDE.md sends "
        "every agent to the active list first."
    )
