"""An exclude naming a path that no longer exists protects nothing, and says nothing.

`scripts/flip/excludes.txt` is the flip's cut list. The runbook applies it as
`rm -rf "./$p"` per entry (§2b), and `rm -rf` on a path that is not there succeeds
silently. So the day somebody renames or moves an excluded file, its entry becomes a
no-op: the file it was meant to keep out of the public repo now ships, `rm` reports
success, the cut reports success, and nothing anywhere is red.

That is the failure this file exists to catch, and the repo has already paid for the
class twice under different mechanisms -- `mcp-minio` pointed a publish job at a
deleted `Dockerfile` for the workflow's entire life (#853), and `test/kind/build-and-load.sh`
kept a `TAG` literal the chart had moved past (#859). Both cost nothing right up until
the one run that mattered. An exclude list is the same shape with a worse blast radius,
because its entries are the moat: `docs/internal`, `scripts/purge-secrets-history.sh`,
the tool-generator design docs.

WHY EXISTENCE AND NOT JUST TRACKEDNESS
--------------------------------------
Both, for different reasons. Existence is the load-bearing one: it is what a rename
breaks. Trackedness is asserted separately with exactly one named exception, `.claude`,
which #926 (`2cdf2ed0`) removed from the index deliberately. That exception is not
bookkeeping -- it is the assertion. If `.claude` is ever re-tracked, this goes red, and
re-tracking it would put `.claude/worktrees/` (live git worktrees) back into the repo.

THE PARSE IS THE SHELL'S, DELIBERATELY
--------------------------------------
The runbook and `scripts/flip/assert-public-suite.sh` both read the list as
`sed 's/#.*//' | tr -d '[:blank:]' | grep -v '^$'`. This mirrors those three steps
rather than inventing a tidier parser: a guard that reads the file differently from the
thing that consumes it can pass on entries the cut never sees, which is the vacuity this
whole suite exists to prevent.
"""

import os
import subprocess

import pytest

import _flip_cut

_TESTS = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(_TESTS))
# Named REPO_ROOT, and the path built with os.path.join, on purpose:
# test_ci_filter_covers_every_guard_subject.py derives a guard's subjects by
# AST-walking for exactly that shape (first arg a Name called REPO_ROOT or ROOT,
# the rest string constants). Built any other way, this guard's subject is
# invisible to the census, which then SKIPS the case -- measured on 2026-09-05,
# and a skipped case prints indistinguishably from a covered one.
_EXCLUDES = os.path.join(REPO_ROOT, "scripts", "flip", "excludes.txt")

# Measured 2026-09-05: 30 entries. The floor is not the measurement -- it sits well
# below it so ordinary edits do not trip it, and well above zero so a parser that stops
# matching cannot report a clean sweep over nothing.
_MIN_ENTRIES = 20

# The single path excluded while deliberately untracked. See module docstring.
_UNTRACKED_BY_DESIGN = {".claude"}


@pytest.fixture(autouse=True)
def _only_before_the_cut():
    """This guard reads a file the cut deletes; see _flip_cut for why not a skipif.

    `excludes.txt:132` is the bare path `scripts/flip` -- the flip directory removing
    itself -- while `llm-service/tests/` is deliberately kept. So on the public repo
    this module's subject is gone and every assertion below would fail on its first CI
    run: the guard, aimed at the guard. `require_a_pre_cut_tree` skips only when BOTH
    halves of the cut are gone and FAILS on a half-cut tree, because a missing-file
    skip is the vacuous pass this suite exists to prevent.
    """
    _flip_cut.require_a_pre_cut_tree()


def _entries() -> list[str]:
    """The list, parsed the way the cut itself parses it."""
    out = []
    with open(_EXCLUDES, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.split("#", 1)[0]                    # sed 's/#.*//'
            line = "".join(line.split())                   # tr -d '[:blank:]'
            if line:                                       # grep -v '^$'
                out.append(line)
    return out


def test_the_list_is_where_this_guard_looks():
    """A rename of the list itself must not turn this file into a silent skip."""
    assert os.path.isfile(_EXCLUDES), (
        f"{_EXCLUDES} is missing. If the cut list moved, point this guard at it -- "
        "do not let it disappear, because every assertion below would stop running "
        "and report nothing."
    )


def test_the_parser_still_matches_something():
    """The anti-vacuity floor. An empty parse would pass every other test here."""
    entries = _entries()
    assert len(entries) >= _MIN_ENTRIES, (
        f"parsed {len(entries)} entries from excludes.txt, expected >= {_MIN_ENTRIES}. "
        "Either the list was gutted or the parse stopped matching; both make the "
        "existence sweep below vacuous."
    )


def test_every_excluded_path_exists():
    """The load-bearing one: `rm -rf` on a stale path succeeds and protects nothing.

    `_UNTRACKED_BY_DESIGN` is exempt, and the reason is the bug the exemption was written
    to fix. The cut runs inside a `git clone` (runbook 2a), so a deliberately untracked
    path is absent there BY CONSTRUCTION -- while in a developer worktree it sits on disk
    and `os.path.exists` answers yes. That is a guard which passes on every machine that
    edits it and fails only on a clean checkout: the same shape as
    `test_doc_links_resolve.py`, which keys on `os.path.exists` and therefore cannot go
    red locally. This file went red on its first CI run for that reason and no other.

    The entry is not dead weight, which is why it stays in the list rather than being
    deleted: run any Claude Code session inside the scratch clone and `.claude` reappears,
    at which point the exclude is the only thing keeping live git worktrees and session
    transcripts out of the public repo. Staleness for this one path is covered instead by
    `test_the_designed_exception_is_still_an_exception`, which asserts it stays untracked.
    """
    missing = [
        p
        for p in _entries()
        if p not in _UNTRACKED_BY_DESIGN
        and not os.path.exists(os.path.join(REPO_ROOT, p))
    ]
    assert not missing, (
        "excludes.txt names paths that do not exist:\n  "
        + "\n  ".join(missing)
        + "\n\nThe cut applies these with `rm -rf`, which succeeds on a missing path. "
        "If one of these was renamed, the file it named now SHIPS to the public repo "
        "and nothing else will tell you. Re-point the entry, or delete it if the "
        "subject is genuinely gone."
    )


def test_every_excluded_path_is_tracked_except_the_one_that_is_not():
    """`.claude` is untracked by design (#926). Anything else untracked is a mistake."""
    untracked = []
    for p in _entries():
        if p in _UNTRACKED_BY_DESIGN:
            continue
        proc = subprocess.run(
            ["git", "ls-files", "--", p],
            cwd=REPO_ROOT, capture_output=True, text=True, check=False,
        )
        if not proc.stdout.strip():
            untracked.append(p)
    assert not untracked, (
        "excludes.txt names paths git does not track:\n  "
        + "\n  ".join(untracked)
        + "\n\nThe cut is taken from a clone, so an untracked path is not there to "
        "remove. Either the entry is dead, or the file was meant to be committed."
    )


def test_the_designed_exception_is_still_an_exception():
    """If `.claude` gets re-tracked, that is a regression, not a reason to edit the set.

    Re-tracking it would put `.claude/worktrees/` -- live git worktrees -- back in the
    index. This asserts the state #926 established still holds.
    """
    if ".claude" not in _entries():
        pytest.skip(".claude is no longer excluded; the exception has no subject")
    proc = subprocess.run(
        ["git", "ls-files", "--", ".claude"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=False,
    )
    assert not proc.stdout.strip(), (
        ".claude is tracked again. #926 (2cdf2ed0) removed it from the index on "
        "purpose -- it holds live git worktrees. Untrack it with "
        "`git rm -r --cached .claude` rather than relaxing this test."
    )
