#!/usr/bin/env python3
"""Fail a `fix` pull request that changes no test file.

usage: check-fix-has-test.py

A bug fixed without a test is a bug that can come back unnoticed. The rule this
checks: a PR whose title starts `fix:` or `fix(<scope>):` changes at least one
test file -- the test that fails before the fix and passes after it. When no
automated test can express the bug (a deploy file, a config value, a doc), the
PR description says so on a line of its own:

    No-test: <why no test can catch this, at least 10 characters>

Any other PR, and any event but `pull_request`, passes without a look.

The title, description and file list are read from the API when the step runs,
not from the event payload. The payload is frozen when the run starts, and a
re-run replays it, so an author who adds a test-exemption line and re-runs the
job would otherwise be judged on the description they just replaced.

Fails closed: a PR that cannot be read is a red step asking for a re-run, not a
pass. A check that passes whenever the API is unreachable has stopped checking.

Calibrated on the 120 `fix` commits on main before 2026-09-27: 114 change a file
this counts as a test, and the other 6 -- two compose memory/restart fixes, a
Traefik option, a chart appVersion and two shell-script fixes -- are what the
`No-test:` line is for.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

FIX_TITLE = re.compile(r"^fix(\([^)]*\))?!?:", re.I)

# A path counts as a test when it is one, or is data only a test reads.
TEST_PATH = re.compile(
    r"""
      (^|/)(tests?|__tests__|testdata|fixtures)/   # a test directory
    | (^|/)test_[^/]+$                             # test_*.py, test_*.sh
    | _test\.(go|py)$                              # Go and Python test files
    | \.(test|spec)\.[cm]?[jt]sx?$                 # frontend unit tests
    | (^|/)[^/]*golden[^/]*$                       # golden files the tests compare against
    | ^(frontend/)?e2e/                            # end-to-end suites
    | ^scripts/check-[^/]+$                        # CI guard scripts
    | ^scripts/flip/assert-[^/]+$                  # the public-cut judges
    """,
    re.X,
)

# At least 10 characters of reason, so `No-test: n/a` does not count.
NO_TEST_LINE = re.compile(r"^[ \t]*No-test:[ \t]*(\S.{9,})$", re.I | re.M)

# GitHub lists at most 3,000 files for a pull request.
PR_FILES_API_CAP = 3000


def is_fix_title(title: str) -> bool:
    return bool(FIX_TITLE.match(title.strip()))


def is_test_path(path: str) -> bool:
    return bool(TEST_PATH.search(path))


def no_test_reason(body: str | None) -> str | None:
    hit = NO_TEST_LINE.search(body or "")
    return hit.group(1).strip() if hit else None


def verdict(title: str, body: str | None, files: list[str]) -> tuple[bool, str]:
    """(passes, why) for a PR with this title, description and changed files.

    `files` holds the paths the PR adds or modifies; a file it only deletes is
    left out by the caller, since deleting a test is not writing one.
    """
    if not is_fix_title(title):
        return True, "not a fix PR"
    tests = [f for f in files if is_test_path(f)]
    if tests:
        shown = ", ".join(tests[:5]) + (f" and {len(tests) - 5} more" if len(tests) > 5 else "")
        return True, f"fix PR with a test change: {shown}"
    reason = no_test_reason(body)
    if reason:
        return True, f"fix PR with no test change, exempted by its description: No-test: {reason}"
    return False, "fix PR that changes no test file and has no `No-test:` line"


FAILURE_HELP = """\
This PR's title marks it a bug fix, and it changes no test file.

Add the test that fails without the fix and passes with it, so the bug cannot
come back unnoticed. Test files are: anything under a tests/, test/, __tests__/,
testdata/ or fixtures/ directory, test_*.py/.sh, *_test.go/.py, *.test.ts(x) and
*.spec.ts(x), golden files, e2e/ and frontend/e2e/, and scripts/check-*.

If no automated test can express this bug, add a line to the PR description:

    No-test: <why no test can catch this, at least 10 characters>

then re-run this job. It reads the title, description and files fresh."""


def _get(url: str, token: str):
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    last: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
        except (OSError, ValueError, urllib.error.URLError) as exc:
            last = exc
            if attempt < 2:
                time.sleep(1 + attempt)
    raise RuntimeError(f"GET {url}: {last}")


def fetch_pr(api: str, repo: str, number: int, token: str) -> tuple[str, str | None, list[str], bool]:
    """(title, body, added-or-modified files, capped) for a PR, read live."""
    pr = _get(f"{api}/repos/{repo}/pulls/{number}", token)
    files: list[str] = []
    listed = 0
    for page in range(1, PR_FILES_API_CAP // 100 + 2):
        batch = _get(f"{api}/repos/{repo}/pulls/{number}/files?per_page=100&page={page}", token)
        for entry in batch:
            listed += 1
            if entry.get("status") != "removed":
                files.append(entry["filename"])
        if len(batch) < 100:
            break
    return pr["title"], pr.get("body"), files, listed >= PR_FILES_API_CAP


def main() -> int:
    if os.environ.get("GITHUB_EVENT_NAME") != "pull_request":
        print(f"not a pull_request event ({os.environ.get('GITHUB_EVENT_NAME') or 'unset'}); nothing to check")
        return 0
    api = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""
    try:
        with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as f:
            number = int(json.load(f)["pull_request"]["number"])
        if not (repo and token):
            raise RuntimeError("GITHUB_REPOSITORY or GH_TOKEN is not set")
        title, body, files, capped = fetch_pr(api, repo, number, token)
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        print(f"::error title=Could not read the pull request::{exc}. Re-run this job; "
              "it fails rather than passing a PR it could not read.")
        return 1
    ok, why = verdict(title, body, files)
    if not ok and capped:
        # The unseen files past GitHub's cap may hold the test.
        print(f"::warning::{why}, but GitHub lists only the first {PR_FILES_API_CAP} files; not failing")
        return 0
    print(f'PR #{number} "{title}": {why}')
    if ok:
        return 0
    print(f"::error title=Fix PR changes no test::{why}")
    print(FAILURE_HELP)
    return 1


if __name__ == "__main__":
    sys.exit(main())
