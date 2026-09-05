"""CHANGELOG.md may not announce a version that no tag has ever pointed at.

The defect this closes. `CHANGELOG.md` opened with `## [1.0.0] - December 2025` and
that was its *only* version heading. The complete set of tags that had ever existed on
the remote was `v0.1.0` and `v0.1.1`, the first cut 2026-08-19 -- the repo's first tag
ever. So the file told a reader the product shipped 1.0 nine months before its first
release. Nothing could disagree with it: `docker-publish.yml` derives every version
from `GITHUB_REF_NAME`, so the changelog is read by no machine at all.

Why the vacuity floor here is NOT "at least one version heading" or "at least one tag".
Both are the obvious shape and both are wrong for this subject, in the same direction:

  * After the fix the file's only heading is `## [Unreleased]`, so the set of version
    headings is legitimately EMPTY, and a floor of >= 1 heading fails on the correct
    file.
  * The public repo is cut as a fresh orphan and starts with ZERO tags by design
    (docs/internal/public-flip-runbook.md), so a floor of >= 1 tag fails on day one in
    the exact repo this guard exists to protect.

An empty set reading as a pass is the failure mode this repo has been bitten by
repeatedly, so the floor cannot simply be dropped either. The answer is to move it off
the data and onto the PARSER: `test_the_heading_parser_is_not_vacuous` and
`test_the_tag_reader_is_not_vacuous` run the two extractors against fixtures whose
answers are known and non-empty. If the real file yields nothing, that is now a fact
about the file rather than a broken regex -- which is the only thing "zero findings"
was ever supposed to mean.
"""

import os
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
CHANGELOG = REPO / "CHANGELOG.md"

# Bracketed (`## [1.0.0]`, the Keep-a-Changelog form this file uses) and bare
# (`## 1.0.0`), with or without a `v`. Matching only the bracketed form would leave
# dropping the brackets as a one-character evasion.
_HEADING = re.compile(r"(?m)^##\s+\[?v?(\d+\.\d+\.\d+[^\]\s]*)\]?")

# A fenced block can contain anything, including a line that looks like a heading.
_FENCE = re.compile(r"(?ms)^```.*?^```")

# Set by the venue that promises an unshallow checkout -- doc-links.yml's `doc-guards`
# job, whose `fetch-depth: 0` fetches tags as well as history. There, "this checkout
# has no tags" cannot mean "not fetched", so it is not allowed to become a skip.
_REQUIRE_REF = "DOC_GUARDS_REQUIRE_MAIN_REF"


def _version_headings(text: str) -> list[str]:
    return _HEADING.findall(_FENCE.sub("", text))


def _parse_tags(stdout: str) -> set[str]:
    """Both spellings of every tag, so `## [1.0.0]` matches a tag cut as `v1.0.0`."""
    out: set[str] = set()
    for line in stdout.splitlines():
        tag = line.strip()
        if not tag:
            continue
        out.add(tag)
        out.add(tag[1:] if tag.startswith("v") else "v" + tag)
    return out


def _tags() -> set[str]:
    # Local refs only: no network, so this behaves the same in CI, in a clone and in a
    # worktree. `git clone` fetches tags by default and `fetch-depth: 0` fetches them
    # in Actions, so an empty answer here is a real fact about the checkout.
    proc = subprocess.run(
        ["git", "tag", "--list"], cwd=REPO, capture_output=True, text=True
    )
    if proc.returncode != 0:
        return set()
    return _parse_tags(proc.stdout)


def test_the_heading_parser_is_not_vacuous():
    """The control. Without this, a regex that matched nothing would pass silently."""
    fixture = (
        "# Changelog\n\n"
        "## [Unreleased]\n\n"
        "## [1.0.0] - December 2025\n"
        "## [v0.9.1]\n"
        "## 0.8.0\n"
        "Prose mentioning ## [7.7.7] mid-sentence is not a heading.\n"
        "```bash\n"
        "## [6.6.6] inside a fence is not a heading either\n"
        "```\n"
    )
    assert _version_headings(fixture) == ["1.0.0", "0.9.1", "0.8.0"], (
        "The heading parser does not extract the headings it is pointed at, so its "
        "answer on the real file means nothing."
    )


def test_the_tag_reader_is_not_vacuous():
    """The second control: `1.0.0` in a heading must match a tag cut as `v1.0.0`."""
    tags = _parse_tags("v0.1.0\n0.2.0\n\n")
    assert {"v0.1.0", "0.1.0", "v0.2.0", "0.2.0"} == tags, tags


def test_the_changelog_exists_and_has_headings():
    """A positive denominator that stays true after the public cut.

    CHANGELOG.md is NOT in scripts/flip/excludes.txt -- it ships. If this file were
    ever emptied or renamed, every assertion below would pass over nothing, and this
    is the one floor that can be stated without assuming a release exists.
    """
    assert CHANGELOG.is_file(), f"{CHANGELOG} is missing"
    text = CHANGELOG.read_text(encoding="utf-8")
    assert len(re.findall(r"(?m)^##\s+\S", text)) >= 3, (
        "CHANGELOG.md has almost no sections left. The version check below would be "
        "reading an empty file and reporting green."
    )


def test_every_version_heading_names_a_tag_that_exists():
    claimed = _version_headings(CHANGELOG.read_text(encoding="utf-8"))
    if not claimed:
        # The correct state today: the body sits under `## [Unreleased]`. The two
        # control tests above are what prove this zero was measured, not assumed.
        return

    tags = _tags()
    if not tags:
        msg = (
            f"CHANGELOG.md announces {claimed}, but this checkout has no tags at all. "
            "Either the release was never cut -- which is the bug this guard exists "
            "for, and the heading should be `## [Unreleased]` until it is -- or the "
            "checkout is shallow and needs `fetch-depth: 0`."
        )
        if os.getenv(_REQUIRE_REF) == "1":
            pytest.fail(msg)
        pytest.skip(msg)

    missing = [v for v in claimed if v not in tags]
    assert not missing, (
        f"CHANGELOG.md announces {missing}, and no such tag exists. A reader arriving "
        "at the public repo is told a release shipped that they cannot check out, "
        "install, or diff. Retitle the section `## [Unreleased]` until the tag is cut."
    )
