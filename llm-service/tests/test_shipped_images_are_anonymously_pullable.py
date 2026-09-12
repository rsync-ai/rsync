"""No image this repo pulls may live behind a registry auth wall.

WHY THIS EXISTS. On 2026-09-12 a `curl | bash` install of this project died
before a single container started:

    Image minio/minio:RELEASE.2025-09-07T16-13-09Z Error pull access denied for
    minio/minio, repository does not exist or may require 'docker login'

Nothing in the repo had changed. MinIO had withdrawn anonymous pulls from
`docker.io/minio/*` -- every tag, the server image and the `mc` client alike,
401 to an unauthenticated client, and the Docker Hub API reports the repository
as absent rather than private. The host that hit this was *already running* that
exact image: it had pulled it 28 hours earlier. So the break is invisible to
every machine that already has the layers cached, and total for every new one.

THIS IS A DIFFERENT FAILURE MODE FROM A FLOATING TAG, AND ITS SIBLING.
test_shipped_images_are_pinned.py covers the case where the tag stays and the
bits move. This covers the case where the bits stay and the *source* goes away.
Both arrive with no diff on our side, no test run, and no way for CI to go red,
which is exactly why each needs a guard rather than a habit.

WHY A DENYLIST AND NOT A LIVE PROBE. Actually asking each registry whether it
serves an anonymous pull is the check you want and the check you cannot have in
CI: it needs network, it is slow per image, and a vendor outage would turn every
PR red for a reason no author could act on. A test that is red for reasons
outside the PR gets ignored, and an ignored test protects nothing. So what is
asserted here is the *decision*: MinIO images come from quay.io. That is
checkable offline, it is exactly the thing a future edit would undo by accident,
and it fails on the PR that introduces the regression rather than on the install
six weeks later.

WHY quay.io IS A SAFE SWAP AND NOT A DIFFERENT IMAGE. quay.io/minio publishes
the same `RELEASE.*` tags. At the time of the move, `quay.io/minio/minio:
RELEASE.2025-09-07T16-13-09Z` returned manifest digest
sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e --
byte-identical to the digest of the Docker Hub image the affected host was
already running. The registry changed; the bits did not. That comparison is what
separates a registry move from an upgrade, and it is the same technique the
MinIO tag pins used to prove they were a no-op.

A GUARD IS ONLY WORTH ITS REACH. This file runs from ci.yml's
`llm-service-unit` job, gated on the `llm` paths filter. The subjects here are
not only compose files: five of them are shell and Python e2e scripts that
`docker run` the `mc` client directly, and the `llm` filter did not name
`e2e/**` or `tests/**` when this was written. The last test below is what keeps
that true, and adding those two patterns to the filter is what made it pass --
a skipped job and a passing one look identical in a PR's check list.
"""

import fnmatch
import os
import re
import subprocess

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Docker Hub repository -> the registry-qualified name to use instead.
#
# Keyed by the *bare* Docker Hub name because that is the form a regression
# takes: someone copies an upstream README, or an LLM completes the image line
# from memory, and the unqualified name is what both produce.
AUTH_WALLED = {
    "minio/minio": "quay.io/minio/minio",
    "minio/mc": "quay.io/minio/mc",
}

# Files that record history rather than pull images. A dated entry describing
# what the repo used to reference is evidence; rewriting it to match today would
# falsify the record and fix nothing.
#
# Kept to exactly the files that need it. The first draft also exempted
# CAPABILITIES.md and CAPABILITIES-ARCHIVE.md on the same reasoning, and
# test_every_exemption_is_still_needed below rejected both: neither names a
# walled image, so both entries were holes covering nothing. That is the whole
# argument for having that test -- an exemption written from caution rather than
# from a read of the file is indistinguishable from one that is load-bearing.
EXEMPT = {
    "BACKLOG.md": "historical: names the pre-move image in a closed row",
    # This file names every walled repository by definition.
    os.path.relpath(__file__, REPO_ROOT): "this guard's own denylist",
}

# Exempt paths the public repo does not have. This file ships to both repos, and
# the public cut strips BACKLOG.md, so there the exemption covers nothing -- it
# must not fail, and it must not be quietly deleted from the private repo where
# it is load-bearing. Declaring the expectation is what keeps an absent path from
# becoming an inert typo: a path absent for any OTHER reason still fails.
STRIPPED_FROM_THE_PUBLIC_REPO = {
    "BACKLOG.md": "scripts/flip/excludes.txt",
}

# The exclude list itself is stripped from the public cut, so the cross-check
# below runs only in the private repo -- which is the one where a typo could
# hide, since that is where the exemption does work.
FLIP_EXCLUDES = os.path.join(REPO_ROOT, "scripts", "flip", "excludes.txt")

# A Docker-Hub-hosted reference to one of the repositories above: the bare name,
# with nothing registry-like in front of it. The lookbehind is the whole trick --
# `quay.io/minio/minio` ends in `/minio`, so a naive search for `minio/minio`
# matches the fixed form too and the guard would fail on its own fix.
_WALLED_RE = re.compile(
    r"(?<![A-Za-z0-9_./-])(" + "|".join(re.escape(k) for k in AUTH_WALLED) + r")(?![A-Za-z0-9_/-])"
)

# The census must keep finding the *fixed* references. If a rename or a file move
# empties it, every assertion below passes while checking nothing -- the shape of
# a vacuous green. Floored below the 21 references present at the time of writing
# so ordinary churn does not trip it, but far enough above zero to catch a
# wholesale disappearance.
_MIN_FIXED_REFS = 12


def _tracked_files():
    """-> every file git tracks, as repo-relative paths.

    Deliberately not os.walk: an untracked scratch file with a walled reference
    is not a defect, and a build directory full of vendored YAML would make the
    census noisy and slow. A git failure raises rather than yielding an empty
    list, because an empty census is the vacuous-pass failure mode.
    """
    out = subprocess.run(
        ["git", "-C", REPO_ROOT, "ls-files"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [line for line in out.splitlines() if line]


def _read(rel):
    try:
        with open(os.path.join(REPO_ROOT, rel), encoding="utf-8") as fh:
            return fh.read()
    except (UnicodeDecodeError, IsADirectoryError, FileNotFoundError):
        return None


def _scan():
    """-> (walled, fixed): [(file, line, text)] for each reference shape."""
    walled, fixed = [], []
    for rel in _tracked_files():
        text = _read(rel)
        if text is None:
            continue
        for n, line in enumerate(text.splitlines(), start=1):
            if rel not in EXEMPT and _WALLED_RE.search(line):
                walled.append((rel, n, line.strip()))
            if any(repl in line for repl in AUTH_WALLED.values()):
                fixed.append((rel, n, line.strip()))
    return walled, fixed


_WALLED, _FIXED = _scan()


def test_the_census_read_the_tree():
    """The denominator. Everything else is vacuous if this is empty."""
    assert len(_tracked_files()) > 100, (
        "git ls-files returned almost nothing, so the scan below examined almost "
        "nothing and would pass on a repo full of walled images."
    )


def test_no_tracked_file_pulls_an_image_from_behind_an_auth_wall():
    assert not _WALLED, (
        "These lines name a Docker Hub repository that no longer serves anonymous "
        "pulls, so `docker compose pull` (or `docker run`) fails on any host that "
        "has not already cached the image:\n  "
        + "\n  ".join(f"{f}:{n}  {t}" for f, n, t in _WALLED)
        + "\nUse the registry-qualified name instead: "
        + ", ".join(f"{k} -> {v}" for k, v in sorted(AUTH_WALLED.items()))
    )


@pytest.mark.parametrize("walled,replacement", sorted(AUTH_WALLED.items()))
def test_every_denylisted_repository_has_a_replacement_the_repo_actually_uses(
    walled, replacement
):
    """A denylist entry whose replacement appears nowhere is advice, not a rule.

    It would also mean the image was dropped rather than moved -- worth knowing,
    and worth failing on, because the next person to need that image will reach
    for the walled name.
    """
    users = sorted({f for f, _, t in _FIXED if replacement in t})
    assert users, (
        f"`{walled}` is denylisted in favour of `{replacement}`, but no tracked "
        f"file references `{replacement}`. Either the image is no longer used -- "
        f"in which case drop the denylist entry -- or the replacement name is wrong."
    )


def test_the_fixed_references_are_still_there():
    assert len(_FIXED) >= _MIN_FIXED_REFS, (
        f"only {len(_FIXED)} registry-qualified references found, expected at least "
        f"{_MIN_FIXED_REFS}. The guard above passes trivially when its subject has "
        f"vanished; if these images really were removed, drop the denylist entry "
        f"and this floor together."
    )


@pytest.mark.parametrize("rel,reason", sorted(EXEMPT.items()))
def test_every_exemption_is_still_needed(rel, reason):
    """A stale exemption is a hole nobody is looking at."""
    text = _read(rel)
    if text is None:
        stripped_by = STRIPPED_FROM_THE_PUBLIC_REPO.get(rel)
        assert stripped_by, (
            f"exempt file {rel} does not exist, so the entry exempts nothing "
            f"({reason}). Either the path is a typo or the file is gone: fix it or "
            f"drop the entry. If it is absent because the public cut strips it, say "
            f"so in STRIPPED_FROM_THE_PUBLIC_REPO."
        )
        if os.path.exists(FLIP_EXCLUDES):
            names = [
                ln.strip()
                for ln in open(FLIP_EXCLUDES, encoding="utf-8")
                if ln.strip() and not ln.startswith("#")
            ]
            assert rel in names, (
                f"{rel} is declared absent because {stripped_by} strips it, but that "
                f"file does not name it. The claim is wrong, or the path moved."
            )
        pytest.skip(f"{rel} is stripped from this repo by {stripped_by}")
    assert _WALLED_RE.search(text), (
        f"{rel} is exempt from this guard ({reason}) but no longer contains a "
        f"walled reference, so the exemption only widens the blind spot. Drop it."
    )


def test_at_least_one_exemption_is_live():
    """The skip above must not be able to hollow the exemption check out.

    A skipped case and a passing one are indistinguishable in a summary line, so
    if every exemption were absent this test would be the only thing left saying
    the set was checked against a real file.
    """
    live = [rel for rel in EXEMPT if _read(rel) is not None]
    assert live, (
        "every exempt path is absent, so test_every_exemption_is_still_needed "
        f"skipped all of them and checked nothing: {sorted(EXEMPT)}"
    )


# --------------------------------------------------------------------------
# Reach. Everything above is worthless on a PR that does not run it.
# --------------------------------------------------------------------------

CI_WORKFLOW = os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")


def _llm_filter_patterns():
    doc = yaml.safe_load(open(CI_WORKFLOW))
    step = next(
        st
        for st in doc["jobs"]["changes"]["steps"]
        if "paths-filter" in str(st.get("uses", "")) and "filters" in (st.get("with") or {})
    )
    return yaml.safe_load(step["with"]["filters"])["llm"]


def test_the_llm_job_is_still_the_one_gated_on_that_filter():
    doc = yaml.safe_load(open(CI_WORKFLOW))
    job = doc["jobs"]["llm-service-unit"]
    assert "needs.changes.outputs.llm == 'true'" in str(job.get("if", "")), (
        "llm-service-unit is no longer gated on the `llm` paths filter, so the "
        "reach test below asserts against a filter that decides nothing. Point it "
        "at whatever gates the job now."
    )


@pytest.mark.parametrize(
    "subject", [pytest.param(f, id=f) for f in sorted({f for f, _, _ in _FIXED})]
)
def test_the_ci_filter_covers_every_file_holding_one_of_these_images(subject):
    """A file that pulls one of these images must be able to trigger this guard.

    Matching is fnmatch rather than the picomatch dorny/paths-filter uses.
    fnmatch's `*` crosses `/` where picomatch's does not, so this is strictly the
    more permissive of the two: it can never fail a pattern CI would honour.
    """
    pats = _llm_filter_patterns()
    assert any(fnmatch.fnmatch(subject, p) for p in pats), (
        f"`{subject}` pulls a registry-qualified MinIO image, but no `llm` "
        f"paths-filter pattern in ci.yml matches it. A PR that reverted that line "
        f"to the Docker Hub name would skip llm-service-unit, and a skipped check "
        f"reads as a passing one.\n"
        f"Add a pattern covering it to the `llm:` filter in "
        f"{os.path.relpath(CI_WORKFLOW, REPO_ROOT)}.\nPatterns today: {pats}"
    )
