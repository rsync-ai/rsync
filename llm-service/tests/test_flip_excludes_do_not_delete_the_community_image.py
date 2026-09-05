"""The public flip runs two path lists, and they must not overlap.

`scripts/flip/excludes.txt` (runbook 2b) removes whole trees from the public repo.
`llm-service/oss-strip-list.txt` (runbook 2a) cuts INSIDE llm-service, keeping the
parts the published images need. Different questions, both run, same cut.

The failure this file exists to catch: excludes.txt naming a path that
`Dockerfile.community` COPYs. That Dockerfile builds `llm-service-oss` -- the image
`docker-compose.quickstart.yml` pulls and `install.sh` starts. Delete its sources
from the public tree and the flip still *looks* clean: the image is already on GHCR,
so the one-command install keeps working, and the break only surfaces at the next
release, from a tree that can no longer build what it publishes.

The two lists were written in separate PRs against different assumptions -- #868's
excludes.txt named five paths under llm-service/ (whole-directory deletes, written
when no llm-service image was published at all), while #867 published one and kept
four of those five. Nothing compared them until this test.

THE PUBLIC CUT. This is a PRIVATE-REPO guard end to end: its entire subject is
`scripts/flip/excludes.txt`, and that file's own last entry (`scripts/flip`) deletes the
directory it lives in -- shipping the list of stripped paths would hand a reader the map.
So the whole module skips once the flip has run.

It skips rather than being deleted because it CANNOT be deleted the usual way: every
consumer of excludes.txt is under llm-service/tests/, and this very file's
`test_excludes_name_nothing_under_llm_service` forbids excludes.txt from naming anything
under llm-service/. Relaxing that bright line to carve out a tests/ exception, in order to
delete the test that enforces it, trades a rule a reader can follow for one they cannot.
A module skip is the cheaper price.

The skip predicate is the DIRECTORY, not the file. `scripts/flip` gone means the cut ran;
`scripts/flip` present with no `excludes.txt` in it means the list was renamed, and this
guard has to be red for that -- a silently-skipped comparison is how the two lists were
allowed to diverge in the first place.
"""

import os
import re

import pytest

LLM_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
REPO_ROOT = os.path.normpath(os.path.join(LLM_DIR, ".."))
FLIP_DIR = os.path.join(REPO_ROOT, "scripts", "flip")
EXCLUDES = os.path.join(FLIP_DIR, "excludes.txt")
DOCKERFILE_COMMUNITY = os.path.join(LLM_DIR, "Dockerfile.community")
DOCKERFILE_LIFECYCLE = os.path.join(LLM_DIR, "Dockerfile.oss")

if not os.path.isdir(FLIP_DIR):
    pytest.skip(
        "scripts/flip/ is not in this tree: the public cut removed it (it is the last "
        "entry in excludes.txt, which names every stripped path). Nothing here has a "
        "subject any more. Deliberately NOT keyed on an env var or the repo name -- "
        "the subject's own absence is the only honest signal.",
        allow_module_level=True,
    )


def _read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _excludes():
    """Repo-root-relative paths the flip deletes. Comments and blanks dropped."""
    out = []
    for line in _read(EXCLUDES).splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            out.append(line.rstrip("/"))
    return out


def _copy_sources(dockerfile_path):
    """Source paths named by COPY lines, relative to the build context."""
    srcs = []
    for line in _read(dockerfile_path).splitlines():
        m = re.match(r"^\s*COPY\s+(?!--)(\S+)\s+(\S+)\s*$", line)
        if m:
            srcs.append(m.group(1).rstrip("/"))
    return srcs


def _overlaps(a, b):
    """True if deleting `a` would remove any part of `b`, or vice versa."""
    return a == b or a.startswith(b + "/") or b.startswith(a + "/")


# --------------------------------------------------------------------------- #
# Vacuity guard. Both assertions below pass against empty inputs, so a parser
# that stops matching would turn this file green rather than red.
# --------------------------------------------------------------------------- #

def test_the_excludes_file_is_where_this_guard_looks():
    """Reached only when scripts/flip/ exists, so a missing list here is a rename.

    Stated before every other assertion because the module skip above deliberately
    does NOT cover this case: a renamed excludes.txt with the directory still in place
    would otherwise read as "the flip ran" and take the whole file green with it.
    """
    assert os.path.isfile(EXCLUDES), (
        f"{FLIP_DIR} exists but excludes.txt is not in it. The list was renamed or "
        "moved; repoint EXCLUDES. Every comparison below is against that one file."
    )


def test_inputs_are_non_empty():
    excludes = _excludes()
    assert len(excludes) >= 15, f"excludes.txt parsed to {len(excludes)}: {excludes}"

    community = _copy_sources(DOCKERFILE_COMMUNITY)
    assert len(community) >= 15, f"Dockerfile.community COPY parse: {community}"

    lifecycle = _copy_sources(DOCKERFILE_LIFECYCLE)
    assert len(lifecycle) >= 4, f"Dockerfile.oss COPY parse: {lifecycle}"


def test_excludes_name_nothing_under_llm_service():
    """The documented split: llm-service is 2a's tree, not 2b's.

    Stated as its own test because it is the rule a reader can actually follow.
    The overlap test below is the consequence; this one is the cause.
    """
    strays = [p for p in _excludes() if p == "llm-service" or p.startswith("llm-service/")]
    assert not strays, (
        f"scripts/flip/excludes.txt names {len(strays)} path(s) under llm-service/: {strays}. "
        "That tree is cut by llm-service/oss-strip-list.txt (runbook 2a), which removes the "
        "moat from INSIDE the packages the published images are built from. A whole-directory "
        "entry here deletes what Dockerfile.community COPYs."
    )


def test_no_exclude_deletes_a_path_a_published_image_copies():
    """The actual failure mode, checked directly rather than via the rule above.

    Prefix-aware in both directions: an exclude that is a parent of a COPY source
    deletes it outright, and one that sits inside a COPY source deletes part of it.
    """
    excludes = _excludes()
    for dockerfile, image in (
        (DOCKERFILE_COMMUNITY, "llm-service-oss"),
        (DOCKERFILE_LIFECYCLE, "connector-lifecycle"),
    ):
        copied = ["llm-service/" + s for s in _copy_sources(dockerfile)]
        collisions = [
            (e, s) for e in excludes for s in copied if _overlaps(e, s)
        ]
        assert not collisions, (
            f"the flip would delete build context of the published {image} image: "
            + "; ".join(f"excludes.txt '{e}' removes '{s}'" for e, s in collisions)
            + f". Either drop the entry from scripts/flip/excludes.txt or stop COPYing "
            f"the path in {os.path.basename(dockerfile)} -- the public repo must be able "
            f"to rebuild every image it publishes."
        )
