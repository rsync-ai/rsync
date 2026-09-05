"""Documented CI Python facts must match what the workflows actually do.

Two rows in this repo described a CI Python setup that had not existed for months.
`docs/runbook.md` said "Python 3.11.15 pre-seeded in tool cache per runner `.env`"
and CAPABILITIES.md carried a caveat row explaining that jobs pass only once the
runner is restarted so `AGENT_TOOLSDIRECTORY` propagates to `actions/setup-python`.

Neither claim had a subject. The commit that removed `actions/setup-python` from
every workflow landed 47 seconds BEFORE the known-issue entry describing the
failure was written, so the entry was obsolete the moment it was filed, and a later
sweep carried it forward without re-reading the workflow. A patch-level version
literal is the ideal thing to check mechanically: `3.11.15` appeared in two
documents and in zero files under `.github/`, and no human sweep is going to
re-derive that.

Both tests below are cheap and exact, and they close the loop the usual way -- the
documentation names a version or a tool, and the workflow tree is asked whether it
agrees.

THE PUBLIC CUT. Both scanned files are on `scripts/flip/excludes.txt`, so this guard's
entire subject is gone in the public repo. The earlier note here said "whoever runs the
public flip removes this guard alongside them", and that turned out not to be available:
excludes.txt is forbidden from naming anything under `llm-service/` (that tree is cut by
`oss-strip-list.txt` instead, because a whole-directory entry there would delete the build
context of a published image), and this file lives under `llm-service/tests/`. So it
cannot be cut -- it ships, and it must skip cleanly instead of erroring.

The skip is all-or-nothing across `SCANNED`, keyed on those files' own existence and
nothing else. Both gone = the cut ran, and there is no claim left to check. EITHER one
still present = this is a repo that still has documentation making CI-Python claims, the
module runs, and `test_the_scan_set_is_non_empty` turns the other one's absence into a
named failure rather than a quiet skip.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

# Explicit, not a glob. CAPABILITIES-ARCHIVE.md is deliberately absent: an archived
# entry is allowed -- required, even -- to quote the retired wording verbatim, and
# that file is the one place the dead literals belong.
SCANNED = ("CAPABILITIES.md", "docs/runbook.md")

if not any((REPO / rel).is_file() for rel in SCANNED):
    pytest.skip(
        f"none of {SCANNED} is in this tree -- scripts/flip/excludes.txt removes them "
        "all, so this guard has no claim left to check. Keyed on the subject files "
        "themselves, never on an env var or the repository name: if even ONE of them "
        "comes back, this module runs and test_the_scan_set_is_non_empty fails on the "
        "rest.",
        allow_module_level=True,
    )

# A three-component version attached to a capital `Python`. Both guards in the
# character class are load-bearing:
#   - the capital P and the `[-\w]` lookbehind keep `kafka-python 2.3.1`
#     (CAPABILITIES.md:349) from reading as a Python version claim;
#   - allowing ` `, `/` and `@` covers the two shapes actually written,
#     `Python 3.11.15` and `Python/3.11.15`.
# Two-component pins like `python@3.11` are intentionally NOT matched -- those name
# the brew formula the workflows really use, and are correct.
PYTHON_VERSION = re.compile(r"(?<![-\w])Python[ /@]v?(\d+\.\d+\.\d+)")

SETUP_PYTHON_USE = re.compile(r"uses:\s*\S*actions/setup-python")


def _workflow_files() -> list[Path]:
    """Tracked workflow files, via git rather than a filesystem walk.

    `git ls-files` is the repo convention: an untracked scratch workflow must not
    be able to satisfy a documentation claim.
    """
    out = subprocess.run(
        ["git", "-C", str(REPO), "ls-files", "-z", ".github/workflows"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [REPO / p for p in out.split("\0") if p]


def _workflows_text() -> str:
    return "\n".join(p.read_text(encoding="utf-8") for p in _workflow_files())


def test_the_scan_set_is_non_empty():
    """A zero-denominator scan reads exactly like a pass. Assert it is not one."""
    workflows = _workflow_files()
    assert workflows, "no tracked files under .github/workflows -- the scan would be vacuous"
    assert _workflows_text().strip(), "every tracked workflow file is empty"
    for rel in SCANNED:
        path = REPO / rel
        assert path.is_file(), f"{rel} is missing; this guard scans it by name"
        assert path.read_text(encoding="utf-8").strip(), f"{rel} is empty"


def test_a_documented_ci_python_version_exists_in_the_workflows():
    workflows_text = _workflows_text()
    offenders = []
    for rel in SCANNED:
        path = REPO / rel
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            for version in PYTHON_VERSION.findall(line):
                if version not in workflows_text:
                    offenders.append(f"{rel}:{lineno} claims Python {version}")
    assert not offenders, (
        "documentation names a Python version that appears in no workflow under "
        ".github/workflows:\n  " + "\n  ".join(offenders)
    )


def test_docs_do_not_describe_a_setup_python_step_that_no_workflow_runs():
    if SETUP_PYTHON_USE.search(_workflows_text()):
        return  # The action is in use; describing it is correct.
    offenders = []
    for rel in SCANNED:
        path = REPO / rel
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if "setup-python" in line:
                offenders.append(f"{rel}:{lineno}")
    assert not offenders, (
        "no workflow uses actions/setup-python, but these lines still describe a "
        "setup-python step:\n  " + "\n  ".join(offenders) + "\n\n"
        "The ban is on the bare name, including in a sentence saying the action is NOT "
        "used: the scanned files should point at the comment above `Set up Python venv` "
        "in .github/workflows/ci.yml, which is where that explanation lives and which "
        "this guard deliberately does not scan. Two copies of a rationale is how the "
        "original rows went stale."
    )
