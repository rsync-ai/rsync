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
