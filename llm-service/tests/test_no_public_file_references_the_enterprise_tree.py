"""No file that survives the public cut may reference the paid-layer tree.

This is mechanism 2 of the four that make the boundary in §3.3 of
`docs/internal/public-vs-cloud-feature-split.md` real. Mechanism 1 -- the entry in
`scripts/flip/excludes.txt` -- states the intent and, on its own, protects nothing: the
flip was a one-time event, nothing re-runs that cut on a schedule, and an exclude line is
a promise about a future no one has scheduled. This guard is a promise about every pull
request.

WHAT THE BOUNDARY ACTUALLY HAS TO STOP
--------------------------------------
Not a leaked file. A leaked file is the easy case and the cut already removes it. The
failure that ends the boundary is a *public file that needs a private one*: an import, a
build context, a COPY, a module path. On flip day the cut removes the tree, the public
build then fails on a dangling reference, and the only repairs available are to publish
the paid code or to un-ship the public feature. Both are the boundary ending. So the
subject here is references, not occurrences.

WHY THE DETECTOR IS PATH- AND IMPORT-SHAPED, NEVER THE BARE WORD
----------------------------------------------------------------
Measured over all 2985 tracked files on 2026-09-22, before this guard existed:

  * the bare word appears in 16 files, as ordinary English ("Enterprise CRM with
    comprehensive bulk data import APIs", "Oracle Database 19c Enterprise Edition");
  * it is also a live *data value* -- the top plan tier. `047_workspaces.sql:9` declares
    `free | pro | enterprise`, and the sample-data connector seeds rows with
    `"plan": "enterprise"`;
  * path-shaped hits: 3, all of them in files this same cut removes
    (`docs/internal/public-vs-cloud-feature-split.md` and `excludes.txt` itself);
  * import-shaped hits, in any of Go, Python or TypeScript: 0.

A guard matching the word would therefore have been red on the day it was written, for
reasons that are all correct code. Red-on-arrival guards get relaxed, and a relaxed guard
is worse than none because it still prints green. The patterns below match only shapes
that mean "this file reaches into that tree", which is why the surviving census reads 0
today and why a prose sentence cannot make it lie.

The one accepted false positive is a doc that writes the tree name immediately followed
by a slash for prose reasons ("enterprise/cloud"). That is rare, it is caught here rather
than on flip day, and the fix is a rephrase.

WHY THIS GUARD RUNS IN BOTH TREES AND SKIPS IN NEITHER
------------------------------------------------------
Most guards in this directory that read a cut file use `_flip_cut.require_a_pre_cut_tree`
to skip themselves in the public repo, because their subject is gone there. This one's
subject is never gone -- it is the surviving tree, which is exactly what the public repo
is. The two trees ask the same question and both answers matter:

  * private: does anything that WILL ship reference the tree?
  * public:  did anything that DID ship reference it?

So `scripts/flip/assert-public-suite.sh`, which materialises the cut and re-runs this
directory's guards inside it, runs this file twice per pull request and both runs assert.
The only thing that differs between the trees is how the surviving set is computed --
`excludes.txt` is itself cut, so in the public tree there is no list to subtract and
"surviving" is simply "tracked", which is the correct answer there.

ANTI-VACUITY
------------
Three separate ways this file refuses to pass over nothing, because a census that
silently scans an empty set is the exact failure the rest of this suite exists to
prevent:

  * a floor on the census denominator (`_MIN_SURVIVING`), well under the 2895 measured;
  * a positive control per pattern, so a regex that stops compiling to anything useful
    is caught by the detector failing to fire on a reference it is supposed to catch;
  * a negative control built from the real strings measured above, so the detector
    cannot be "fixed" into matching the plan tier and the prose.
"""

import os
import re
import subprocess

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(_TESTS))

# Declared this way on purpose: test_ci_filter_covers_every_guard_subject.py derives a
# guard's subjects by AST-walking for os.path.join(REPO_ROOT, <string constants>).
# Built any other way this guard declares no subject and the census skips it.
_EXCLUDES = os.path.join(REPO_ROOT, "scripts", "flip", "excludes.txt")

# The directory name the boundary is drawn around. One constant, so the exclude entry,
# the failure messages and the patterns below cannot drift apart.
_TREE = "enterprise"

# Measured 2026-09-22: 2985 tracked, 90 removed by the 32 exclude entries, 2895
# surviving. The floor sits far below that so ordinary growth or pruning never trips it,
# and far above zero so a census that stopped enumerating cannot report a clean sweep.
_MIN_SURVIVING = 1000

# Files this guard may not scan for itself, because they carry the patterns as data.
# Computed rather than hardcoded so a rename cannot turn the exclusion into a miss.
_SELF = os.path.relpath(os.path.abspath(__file__), REPO_ROOT)


def _rx(pattern: str) -> "re.Pattern[str]":
    return re.compile(pattern.replace("TREE", re.escape(_TREE)))


# (name, regex, what a hit means). Every one of these is a shape that only appears when
# a file reaches INTO the tree -- never a shape the word can take in prose or in data.
_PATTERNS = (
    (
        "path",
        _rx(r"(?<![A-Za-z0-9_.\-])TREE/"),
        "a path into the tree",
    ),
    (
        "go-import",
        _rx(r'"[A-Za-z0-9_.\-]+(?:/[A-Za-z0-9_.\-]+)*/TREE(?:/[A-Za-z0-9_./\-]*)?"'),
        "a Go import path ending in the tree",
    ),
    (
        "py-import",
        _rx(r"(?m)^[ \t]*(?:from|import)[ \t]+TREE\b"),
        "a Python import of the tree",
    ),
    (
        "ts-import",
        _rx(r"""(?:from|import|require\()\s*['"](?:@/|\.{1,2}/)?TREE(?:/|['"])"""),
        "a TypeScript/JavaScript import of the tree",
    ),
    (
        "docker-copy",
        _rx(r"(?im)^[ \t]*(?:COPY|ADD)[ \t]+(?:--\S+[ \t]+)*(?:\./)?TREE(?=[ \t/])"),
        "a Dockerfile copying the tree into an image",
    ),
    (
        "build-context",
        _rx(r"(?im)^[ \t]*context:[ \t]*\.?/?TREE[ \t]*$"),
        "a compose build context pointing at the tree",
    ),
)


def _scan(text: str) -> list[tuple[str, int, str]]:
    """Every reference-shaped hit in one file, as (pattern name, 1-based line, line)."""
    found = []
    for name, rx, _why in _PATTERNS:
        for m in rx.finditer(text):
            line_no = text.count("\n", 0, m.start()) + 1
            line = text.split("\n")[line_no - 1].strip()
            found.append((name, line_no, line[:160]))
    return found


def _git(*args: str) -> str:
    proc = subprocess.run(
        ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0, f"git {' '.join(args)} failed: {proc.stderr.strip()}"
    return proc.stdout


def _exclude_entries() -> list[str]:
    """`excludes.txt`, parsed the way the cut itself parses it.

    `sed 's/#.*//' | tr -d '[:blank:]' | grep -v '^$'`, mirrored rather than tidied: a
    guard that reads the list differently from the script that applies it can pass on
    entries the cut never sees.
    """
    out = []
    with open(_EXCLUDES, encoding="utf-8") as fh:
        for raw in fh:
            line = "".join(raw.split("#", 1)[0].split())
            if line:
                out.append(line)
    return out


def _surviving_files() -> list[str]:
    """Every tracked file that is still there after the cut.

    In a pre-cut tree that is `git ls-files` minus the cut manifest, built with the same
    three pathspec forms `materialise-cut.sh` expands each entry into. In the public tree
    `excludes.txt` is gone -- it excludes itself -- and there is nothing to subtract,
    which is correct: everything tracked there has already survived.
    """
    tracked = [p for p in _git("ls-files").split("\n") if p]
    if not os.path.isfile(_EXCLUDES):
        return tracked
    pathspecs = []
    for entry in _exclude_entries():
        pathspecs += [entry, f":(glob)**/{entry}", f":(glob)**/{entry}/**"]
    cut = {p for p in _git("ls-files", "--", *pathspecs).split("\n") if p}
    return [p for p in tracked if p not in cut]


def _read(rel: str) -> str | None:
    """File text, or None for anything that is not text."""
    try:
        with open(os.path.join(REPO_ROOT, rel), "rb") as fh:
            blob = fh.read()
    except (IsADirectoryError, FileNotFoundError, PermissionError):
        return None
    if b"\0" in blob[:8192]:
        return None
    return blob.decode("utf-8", errors="replace")


# --------------------------------------------------------------------------------------
# controls: the detector has to be able to fail, and to fail for the right reason
# --------------------------------------------------------------------------------------

_MUST_FIRE = [
    ("path", 'COPY enterprise/service /app/service'),
    ("path", '  - ./enterprise/config:/etc/rsync'),
    ("go-import", '\timport "github.com/rsync-ai/enterprise/identity"'),
    ("py-import", 'from enterprise.identity import assert_sso'),
    ("py-import", 'import enterprise'),
    ("ts-import", 'import { Sso } from "@/enterprise/sso"'),
    ("ts-import", "const x = require('../enterprise')"),
    ("docker-copy", 'COPY --chown=1000:1000 enterprise /srv/enterprise-svc'),
    ("build-context", '    context: ./enterprise'),
]

# Every one of these is a real line measured in the tree on 2026-09-22, or the prose
# shape it takes. A detector that matches any of them is matching the plan tier or the
# English word, and would be relaxed away within a release.
_MUST_NOT_FIRE = [
    "    plan        VARCHAR(50) DEFAULT 'free',      -- free | pro | enterprise",
    '{"customer_id": 3, "country": "IT", "plan": "enterprise"}',
    '                "database": "enterprise"',
    '    reason: "Enterprise CRM with comprehensive bulk data import APIs"',
    '                    version: str = "Oracle Database 19c Enterprise Edition"',
    "## Enterprise layer (option A1: separate private service, no runtime gate)",
    "# Enterprise requirement: Production systems cannot execute blind.",
    "requires Atlas or Enterprise Advanced, is JDBC/ODBC-only,",
    "an enterprise buyer pays for governance, not for a checkbox",
]


@pytest.mark.parametrize(
    ("expected", "line"),
    [pytest.param(k, s, id=f"{k}:{s[:40]}") for k, s in _MUST_FIRE],
)
def test_the_detector_fires_on_every_reference_shape_it_claims_to_catch(expected, line):
    """The positive control. Without it, a dead regex reads exactly like a clean tree."""
    names = {name for name, _no, _text in _scan(line)}
    assert expected in names, (
        f"the {expected!r} pattern did not fire on a reference it exists to catch:\n"
        f"  {line}\n\nThe census below would report a clean sweep over a tree this "
        "detector can no longer see into."
    )


@pytest.mark.parametrize("line", _MUST_NOT_FIRE, ids=lambda s: s.strip()[:40])
def test_the_detector_ignores_the_word_in_prose_and_in_data(line):
    """The negative control, built from lines that really are in the tree.

    The top plan tier is spelled the same as the private directory. A detector widened
    to the bare word goes red on `047_workspaces.sql`, on the sample-data seed rows and
    on a dozen sentences of ordinary English -- and the repair anyone reaches for is to
    weaken it until it passes, at which point it protects nothing and still prints green.
    """
    hits = _scan(line)
    assert not hits, (
        f"the detector fired on a line that is not a reference:\n  {line}\n"
        f"  matched: {hits}\n\nThis is the word used as prose or as the plan-tier value, "
        "not a path into the tree. Narrow the pattern rather than exempting the file."
    )


# --------------------------------------------------------------------------------------
# the census
# --------------------------------------------------------------------------------------


def test_the_census_enumerates_a_real_tree():
    """The anti-vacuity floor. An empty census passes the sweep below for free."""
    surviving = _surviving_files()
    assert len(surviving) >= _MIN_SURVIVING, (
        f"the surviving census found {len(surviving)} files, expected at least "
        f"{_MIN_SURVIVING} (2895 measured 2026-09-22). Either `git ls-files` returned "
        "nothing or the exclude subtraction is removing the whole tree; either way the "
        "sweep below is scanning nothing and would pass on any reference at all."
    )


def test_this_guard_scans_everything_except_itself():
    """Self-exclusion has to be exactly one file, and that file has to be in the census.

    This guard carries the patterns as data, so it must skip itself. Two things can go
    wrong quietly: the relative path stops matching after a move, in which case the guard
    fails on its own docstring; or the guard is itself excluded from the cut, in which
    case it never runs in the public tree and its second, load-bearing assertion is gone.
    """
    surviving = _surviving_files()
    assert _SELF in surviving, (
        f"{_SELF} is not in the surviving set. This guard has to ship to the public "
        "repo -- there it is the assertion that no reference survived the cut. Do not "
        "add it to excludes.txt."
    )
    assert _scan(_read(_SELF) or ""), (
        f"{_SELF} no longer matches its own patterns, which means the detector is not "
        "seeing the reference shapes written into this file as controls."
    )


def test_no_surviving_file_references_the_paid_tree():
    """The assertion. Every tracked file that ships, scanned for a reference.

    A hit here is not a style problem. It is a public file that will not build once the
    tree is gone, and the only repairs on flip day are to publish the paid code or to
    delete the public feature.
    """
    offenders = []
    scanned = 0
    for rel in _surviving_files():
        if rel == _SELF:
            continue
        text = _read(rel)
        if text is None:
            continue
        scanned += 1
        for name, line_no, line in _scan(text):
            offenders.append(f"{rel}:{line_no}: [{name}] {line}")

    assert scanned >= _MIN_SURVIVING, (
        f"only {scanned} of the surviving files were readable as text; expected at "
        f"least {_MIN_SURVIVING}. The sweep did not cover the tree it claims to."
    )
    assert not offenders, (
        f"{len(offenders)} file(s) that survive the public cut reference the "
        f"`{_TREE}/` tree:\n  "
        + "\n  ".join(offenders)
        + "\n\nPublic code must not reach into the paid tree -- not by import, not by "
        "path, not by build context. Call a generic integration point with a working "
        "community default instead, and let the paid implementation register itself. "
        "See §3.3 of docs/internal/public-vs-cloud-feature-split.md."
    )


def test_the_cut_still_removes_the_paid_tree():
    """The exclude entry is mechanism 1. Deleting it must not be silent.

    `test_flip_excludes_name_paths_that_exist.py` asserts every listed path exists. It
    cannot assert that a path is listed -- so on its own, deleting this entry is a green
    change that ships the whole tree. Skipped in the public repo, where `excludes.txt`
    has already excluded itself; `test_the_census_enumerates_a_real_tree` is what keeps
    that skip from being the whole file going quiet.
    """
    if not os.path.isfile(_EXCLUDES):
        pytest.skip("post-cut tree: excludes.txt is itself cut, so there is no list")
    entries = _exclude_entries()
    assert len(entries) >= 20, (
        f"parsed {len(entries)} exclude entries; the parse has stopped matching and "
        "the membership check below is vacuous."
    )
    assert _TREE in entries, (
        f"`{_TREE}` is no longer an entry in scripts/flip/excludes.txt. The next cut "
        "would carry the entire paid tree into the public repository."
    )
    tracked_under = [p for p in _git("ls-files", "--", _TREE).split("\n") if p]
    assert tracked_under, (
        f"nothing is tracked under `{_TREE}/`, so the exclude entry removes nothing and "
        "the boundary has no subject. If the tree was renamed, re-point the entry."
    )
    assert not any(p in _surviving_files() for p in tracked_under), (
        f"files under `{_TREE}/` survive the cut: "
        f"{[p for p in tracked_under if p in _surviving_files()][:5]}"
    )
