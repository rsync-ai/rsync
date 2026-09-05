"""No surviving doc may link into the tool-generator moat unless the de-linker repairs it.

WHAT BROKE. `scripts/flip/delink-docs.sh` reads its removal set from ONE list,
`scripts/flip/excludes.txt`. The public cut removes paths from TWO: runbook §2a
strips the moat via `llm-service/oss-strip-list.txt` first, §2b cuts excludes.txt
second. Three links pointed into the moat, the de-linker had no idea those paths
were going away, and `./scripts/check-doc-links.sh` -- the last step of §2c, under
`set -e` -- aborted the cut:

    FAIL: 3 dead link(s) -- each one is a 404 for a stranger:
      docs/connectors/optimization.md: [../../llm-service/.../AGENTIC_PIPELINE.md]
      docs/connectors/optimization.md: [../../llm-service/.../AGENTIC_PIPELINE.md]
      docs/connectors/reference.md:    [../../llm-service/.../capability_rules.yaml]

Measured by replaying §2a+§2b+§2c in a scratch clone: rc=1 before the fix, rc=0
and "553 relative links ... OK" after, with a second de-link run a no-op.

WHY A STATIC GUARD AND NOT A REPLAY. Proving this properly means executing the
cut, and the cut is a `git rm -r` + `rm -rf` over 261 paths -- not something a
unit test may do in a working checkout. What a test CAN hold is the invariant the
replay verified: every link a surviving doc aims into the moat must be a link
`delink-docs.sh` knows about. The script's REWRITES entries carry the old text
verbatim, so "does the script name this target?" is a real question with a real
answer, and it is the question that was silently answered "no" for three links.

WHAT IT CATCHES. The fourth link. Someone documents the generation pipeline next
month, points at `llm-service/src/agents/tool_generator/prompts/`, and every
private check stays green -- `check-doc-links.sh` passes because the moat is right
there in this repo. It is only on flip day, mid-cut, that it fails. This test
fails the moment the link lands.

WHY IT CANNOT PASS VACUOUSLY. Two floors. The moat list must parse to a non-empty
set of paths that EXIST (a rename of the strip list would otherwise make every
assertion below range over nothing), and the link scan must find a plausible
number of repo-path links across the docs it sweeps. A count of zero is not a
pass here; it is the parser being broken.
"""

import os

import pytest

import _flip_cut
from doclinks import repo_path_targets

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MOAT_LIST = os.path.join("llm-service", "oss-strip-list.txt")
MOAT_PREFIX = "llm-service/"
EXCLUDES_LIST = os.path.join("scripts", "flip", "excludes.txt")
DELINKER = os.path.join("scripts", "flip", "delink-docs.sh")

# The link sweep has to find at least this many repo-path links before any absence
# below means anything. The tree carries ~550; a floor far under that still fails
# loudly if the extractor breaks, without tracking every doc edit.
MIN_LINKS = 100


@pytest.fixture(autouse=True)
def _only_before_the_cut():
    """These guards read files the cut deletes; see _flip_cut for why not a skipif."""
    _flip_cut.require_a_pre_cut_tree()



def _entries(rel, prefix=""):
    path = os.path.join(REPO_ROOT, rel)
    if not os.path.isfile(path):
        return []
    out = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                out.append((prefix + line).rstrip("/"))
    return out


MOAT = _entries(MOAT_LIST, MOAT_PREFIX)
EXCLUDES = _entries(EXCLUDES_LIST)


def _removed_by(path, removals):
    return any(path == r or path.startswith(r + "/") for r in removals)


def _surviving_docs():
    """Tracked `*.md` the cut keeps -- the only files a stranger will ever read."""
    import subprocess

    out = subprocess.run(
        ["git", "ls-files", "*.md"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=True,
    ).stdout.split()
    return [p for p in out if not _removed_by(p, EXCLUDES) and not _removed_by(p, MOAT)]


def _links_into_the_moat():
    """(doc, target) for every link a surviving doc aims at a path the moat strip removes."""
    hits, total = [], 0
    for doc in _surviving_docs():
        full = os.path.join(REPO_ROOT, doc)
        try:
            with open(full, encoding="utf-8") as fh:
                text = fh.read()
        except (OSError, UnicodeDecodeError):
            continue
        for t in repo_path_targets(text):
            total += 1
            base = "" if t.startswith("/") else os.path.dirname(doc)
            resolved = os.path.normpath(os.path.join(base, t.lstrip("/")))
            if _removed_by(resolved, MOAT):
                hits.append((doc, t))
    return hits, total


def test_the_moat_list_parses_to_paths_that_exist():
    """The denominator. Without it every assertion below ranges over an empty set."""
    assert MOAT, (
        f"{MOAT_LIST} parsed to zero paths -- renamed or reformatted. Every other "
        "assertion in this file is vacuous until this is fixed."
    )
    present = [p for p in MOAT if os.path.exists(os.path.join(REPO_ROOT, p))]
    assert len(present) >= len(MOAT) // 2, (
        f"only {len(present)} of {len(MOAT)} {MOAT_LIST} entries exist on disk. The "
        "list has drifted from the tree it is supposed to strip."
    )


def test_the_link_scan_actually_finds_links():
    """A count of zero is not a pass -- it is a broken extractor."""
    _, total = _links_into_the_moat()
    assert total >= MIN_LINKS, (
        f"the sweep found only {total} repo-path links across the surviving docs "
        f"(floor {MIN_LINKS}). The link extractor is broken, not the docs."
    )


def test_the_delinker_reads_the_moat_list_too():
    """The root cause, asserted directly: one removal set is not the cut's removal set."""
    path = os.path.join(REPO_ROOT, DELINKER)
    assert os.path.isfile(path), f"{DELINKER} is missing"
    with open(path, encoding="utf-8") as fh:
        src = fh.read()
    assert MOAT_LIST.replace(os.sep, "/") in src, (
        f"{DELINKER} does not read {MOAT_LIST}. It repairs links into the paths "
        f"{EXCLUDES_LIST} removes and is blind to the ones §2a strips, so the cut "
        "aborts at check-doc-links.sh with the moat links still pointing at nothing."
    )


def test_every_link_into_the_moat_is_one_the_delinker_repairs():
    hits, _ = _links_into_the_moat()
    if not hits:
        pytest.skip("no surviving doc links into the moat -- nothing to repair")
    with open(os.path.join(REPO_ROOT, DELINKER), encoding="utf-8") as fh:
        src = fh.read()
    orphans = sorted({(d, t) for d, t in hits if t not in src})
    assert not orphans, (
        "these links point into paths the public cut strips, and "
        f"{DELINKER} never mentions them -- on flip day check-doc-links.sh will "
        "report each one as a dead link and abort §2c:\n"
        + "\n".join(f"  {d}: [{t}]" for d, t in orphans)
        + f"\n\nFix: add a REWRITES entry to {DELINKER} that states the fact without "
        "the pointer. De-linking alone is not enough here -- it leaves a bare path a "
        "stranger cannot open."
    )
