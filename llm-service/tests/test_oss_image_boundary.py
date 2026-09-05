"""The moat-free boundary is one list, and every consumer must agree with it.

`llm-service/oss-strip-list.txt` names the connector-generation moat. Three
artifacts are built from that boundary and each fails differently when it drifts:

  * `Dockerfile.community` — the published `llm-service-oss` image. Ships too
    little and the community stack loses a feature with no error at build time;
    ships too much and the moat becomes a pullable artifact, which is
    irreversible the moment the registry is public.
  * `Dockerfile.oss` — the published `connector-lifecycle` image. A strict
    subset: it may ship less than the community image, never more.
  * `docs/internal/public-flip-runbook.md` — the paths deleted from the public
    source tree. If it deletes something the community image COPYs, the public
    repo cannot build its own image; if it keeps something stripped here, the
    moat ships as source.

The allowlist COPY in both Dockerfiles is the right shape (a stripped path can
never reach a layer, not even an intermediate one) but it is silent when a NEW
module appears: nothing errors, the module is just missing at runtime. That is
what `test_every_tracked_source_file_is_either_shipped_or_stripped` exists for —
it walks `git ls-files` and requires every tracked file under `src/` and
`prompts/` to be covered by exactly one of the two lists.

`git ls-files`, not a filesystem walk: `__pycache__` and locally generated
artifacts are untracked by design and are not part of the build context question.

THE PUBLIC CUT. `oss-strip-list.txt` and both Dockerfiles survive the cut; the paths the
list NAMES do not, by construction -- that is what the cut is. So two of this file's
questions have different answers on the two sides:

  * "does every strip entry still exist on disk" is a PRIVATE question (in the public repo
    the correct answer is no, for all of them). It is now conditioned on the moat tree
    being present, and the condition is all-or-nothing: if ANY entry resolves, every entry
    must, so a rename is still red and only a completed cut skips.
  * `docs/internal/public-flip-runbook.md` is deleted by `scripts/flip/excludes.txt`, so
    the runbook cross-check skips there -- but only when the whole `docs/internal` tree is
    gone. A runbook that vanished on its own is still red.

Everything else in this file is a PUBLIC invariant and keeps running: the partition of the
surviving `src/` + `prompts/` trees against `Dockerfile.community`'s allowlist is what
stops a new module from being silently absent from the image the quickstart pulls, and
that question is if anything more important once the repo is public.
"""

import os
import re
import subprocess

import pytest

LLM_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
REPO_ROOT = os.path.normpath(os.path.join(LLM_DIR, ".."))
STRIP_LIST = os.path.join(LLM_DIR, "oss-strip-list.txt")
DOCKERFILE_COMMUNITY = os.path.join(LLM_DIR, "Dockerfile.community")
DOCKERFILE_LIFECYCLE = os.path.join(LLM_DIR, "Dockerfile.oss")
FLIP_RUNBOOK = os.path.join(REPO_ROOT, "docs", "internal", "public-flip-runbook.md")

# Only these two trees are partitioned. Everything else in the build context
# (requirements*.txt, conftest.py, the Dockerfiles themselves) is either copied
# explicitly or irrelevant to the moat question.
PARTITIONED_TREES = ("src", "prompts")


def _read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _strip_list():
    """Moat paths, relative to llm-service/, comments and blanks dropped."""
    out = []
    for line in _read(STRIP_LIST).splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.append(line.rstrip("/"))
    return out


def _copy_sources(dockerfile_path):
    """Source paths named by COPY lines, relative to the build context."""
    srcs = []
    for line in _read(dockerfile_path).splitlines():
        m = re.match(r"^\s*COPY\s+(?!--)(\S+)\s+(\S+)\s*$", line)
        if not m:
            continue
        srcs.append(m.group(1).rstrip("/"))
    return srcs


def _tracked(tree):
    out = subprocess.run(
        ["git", "ls-files", tree],
        cwd=LLM_DIR, capture_output=True, text=True, check=True,
    ).stdout.split()
    return [p for p in out if p]


def _under(path, prefix):
    return path == prefix or path.startswith(prefix + "/")


def _strip_entries_on_disk():
    """(present, missing) split of the strip list against this checkout."""
    present, missing = [], []
    for p in _strip_list():
        (present if os.path.exists(os.path.join(LLM_DIR, p)) else missing).append(p)
    return present, missing


# --------------------------------------------------------------------------- #
# Vacuity guards: every assertion below is trivially true against empty inputs.
# --------------------------------------------------------------------------- #

def test_inputs_are_non_empty():
    """A parser that silently stops matching turns this whole file green."""
    strips = _strip_list()
    assert len(strips) >= 15, f"strip list parsed to {len(strips)} entries: {strips}"

    community = _copy_sources(DOCKERFILE_COMMUNITY)
    assert len(community) >= 15, f"Dockerfile.community COPY parse: {community}"

    lifecycle = _copy_sources(DOCKERFILE_LIFECYCLE)
    assert len(lifecycle) >= 4, f"Dockerfile.oss COPY parse: {lifecycle}"

    # 277 tracked files under src/ + prompts/ in the private repo, 97 after the public
    # cut removes the generation package. The floor only has to separate "the corpus
    # shrank" from "git ls-files returned nothing", so it sits below both.
    tracked = [p for tree in PARTITIONED_TREES for p in _tracked(tree)]
    assert len(tracked) >= 60, f"git ls-files found {len(tracked)} files"


def test_the_strip_list_is_internally_consistent():
    """True on both sides of the cut, because it asks nothing about the disk.

    A duplicate or a nested entry is a no-op wearing the look of coverage, and an
    entry that climbs out of llm-service/ would have the 2a loop deleting something
    2b owns. None of that is detectable by the existence check below, which skips in
    the public repo -- so it lives here, where it always runs.
    """
    entries = _strip_list()
    dupes = sorted({p for p in entries if entries.count(p) > 1})
    assert not dupes, f"oss-strip-list.txt repeats: {dupes}"

    escapes = [p for p in entries if p.startswith("/") or p.startswith("../") or p == ".."]
    assert not escapes, (
        f"oss-strip-list.txt entries must be relative to llm-service/: {escapes}"
    )

    nested = sorted(
        {a for a in entries for b in entries if a != b and _under(a, b)}
    )
    assert not nested, (
        "these strip entries are already covered by a parent entry, so they can be "
        f"renamed away without any check noticing: {nested}"
    )


def test_every_strip_entry_exists_on_disk():
    """A rename must not silently turn a strip entry into a no-op.

    This is the failure that ships the moat: `rm -rf` / "not in the allowlist"
    both succeed against a path that moved, so the only signal is this check.

    Conditioned on the moat tree being present, because in the public repo every one
    of these paths is correctly gone. The condition is all-or-nothing rather than
    per-entry: a single surviving entry means this is a tree the cut has not run on,
    and every other entry has to resolve. That is the positive denominator -- the
    skip can only be reached by a COMPLETED cut, never by a rename.
    """
    present, missing = _strip_entries_on_disk()
    if not present:
        pytest.skip(
            "no strip-list path resolves: this is a tree the public cut has already "
            "run on (runbook 2a), so their absence is the intended state"
        )
    assert not missing, (
        "oss-strip-list.txt names paths that no longer exist — they were renamed or "
        f"deleted and the strip is now a no-op: {missing}. "
        f"({len(present)} sibling entries DO resolve, so this is not the public cut.)"
    )


def test_every_tracked_source_file_is_either_shipped_or_stripped():
    """No tracked file may be in neither list (silently absent) or both."""
    strips = _strip_list()
    copies = _copy_sources(DOCKERFILE_COMMUNITY)

    neither, both = [], []
    for tree in PARTITIONED_TREES:
        for path in _tracked(tree):
            shipped = any(_under(path, c) for c in copies)
            stripped = any(_under(path, s) for s in strips)
            if shipped and stripped:
                both.append(path)
            elif not shipped and not stripped:
                neither.append(path)

    assert not both, (
        "these files are both COPY'd into the community image and listed as moat — "
        f"the image leaks them: {sorted(both)[:20]}"
    )
    assert not neither, (
        "these tracked files reach neither Dockerfile.community's allowlist nor "
        "oss-strip-list.txt, so they are silently missing from the community image. "
        f"Add a COPY line or a strip entry: {sorted(neither)[:20]}"
    )


def test_lifecycle_image_ships_no_stripped_path():
    """Dockerfile.oss may ship less than the community image, never more."""
    strips = _strip_list()
    leaks = [
        src for src in _copy_sources(DOCKERFILE_LIFECYCLE)
        if any(_under(src, s) or _under(s, src) for s in strips)
    ]
    assert not leaks, f"Dockerfile.oss COPYs a moat path: {leaks}"


def test_flip_runbook_derives_its_deletions_from_this_list():
    """The public tree and the published image must be cut on the same line.

    The runbook used to transcribe its own path list. Two lists meant the
    community image could COPY something the flip had already deleted — a break
    that surfaces only on flip day, in a scratch clone, with the repo about to
    go public.

    Skipped once the flip has run, since `scripts/flip/excludes.txt` deletes the whole
    `docs/internal` tree the runbook lives in. Keyed on that TREE, not on the file: a
    runbook that was renamed or deleted while its directory still stands is the drift
    this test exists for, and still fails.
    """
    if not os.path.exists(FLIP_RUNBOOK):
        internal = os.path.join(REPO_ROOT, "docs", "internal")
        assert not os.path.isdir(internal), (
            "docs/internal/public-flip-runbook.md is gone but docs/internal/ is still "
            "here, so this is a rename rather than the public cut. Repoint FLIP_RUNBOOK "
            "-- as written this check would silently stop comparing the two path lists."
        )
        pytest.skip("docs/internal was removed by scripts/flip/excludes.txt")
    text = _read(FLIP_RUNBOOK)
    assert "llm-service/oss-strip-list.txt" in text, (
        "public-flip-runbook.md no longer derives its moat deletions from "
        "llm-service/oss-strip-list.txt — the two lists have been allowed to diverge"
    )

    # Nothing the community image needs may appear in the runbook's rm -rf.
    needed = [
        "llm-service/src/gateway",
        "llm-service/src/agents/planner",
        "llm-service/prompts",
        "llm-service/src/agents/tool_generator/deployment",
    ]
    # The verb set is deliberately wider than `rm -rf`: a hard-coded path is just as
    # gone via `rm -r`, `git clean -xfd`, `rmdir` or `find ... -delete`, and keying on
    # two literal strings let all four through.
    rm_lines = [
        ln for ln in text.splitlines()
        if re.search(r"\brm\s+-|\bgit\s+rm\b|\bgit\s+clean\b|\brmdir\b|-delete\b", ln)
    ]
    haystack = "\n".join(rm_lines)
    deleted = [p for p in needed if p in haystack]
    assert not deleted, (
        "public-flip-runbook.md deletes paths the community image COPYs, so the "
        f"public repo could not build llm-service-oss: {deleted}"
    )

    # B9. Everything above can only see a HARD-CODED path. A deletion driven by a shell
    # variable names nothing the substring scan can match, so it removes the same tree
    # while passing silently. The line §2d retired was exactly that -- `rm -rf "${EXCLUDES[@]}"`
    # over a list whose bare `.claude` entry expands, in a developer checkout, to
    # `rm -rf .claude` across every live worktree.
    #
    # Line-anchored at `^\s*rm`, so the commented-out copy §2d keeps -- quoting the retired
    # line in order to explain its own fix -- can never match, because `#` is not whitespace.
    # Fires only on a RECURSIVE rm whose operand has no literal prefix, so §2a's
    # `rm -rf "llm-service/$rel"` stays legal: it is bounded by a literal, its target is
    # existence-checked on the line above it, and its CONTENT question is already answered
    # by test_every_tracked_source_file_is_either_shipped_or_stripped.
    rm_call = re.compile(r"^\s*rm\s+((?:-[A-Za-z]+\s+)*)(\S+)")

    # Positive denominator: the assertion below is vacuously true if this scanner stops
    # matching. The runbook has 3 rm calls today (§2a:297, §2d:465, §2d:474).
    scanned = [ln for ln in text.splitlines() if rm_call.match(ln)]
    assert len(scanned) >= 3, (
        f"the rm-call scanner matched {len(scanned)} lines in public-flip-runbook.md; it "
        "has stopped parsing and the unanchored-deletion check below is vacuous"
    )

    unanchored = []
    for n, ln in enumerate(text.splitlines(), 1):
        m = rm_call.match(ln)
        if not m:
            continue
        flags = m.group(1).replace("-", "").lower()
        if "r" not in flags:
            continue
        if re.match(r"""^["']?\$""", m.group(2)):
            unanchored.append(f"{n}: {ln.strip()}")
    assert not unanchored, (
        "public-flip-runbook.md runs a recursive `rm` on a bare variable expansion. "
        "Nothing literal bounds what it deletes, and the hard-coded-path check above is "
        "blind to it. Delete a captured manifest path-by-path as §2d does, or give the "
        f"operand a literal prefix: {unanchored}"
    )
