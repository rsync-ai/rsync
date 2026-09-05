#!/usr/bin/env bash
# Every relative link in a tracked *.md must resolve to something the repo actually ships.
#
# Why this exists: docs/README.md advertised four files under docs/internal/ that the
# public-flip cut (§2b of the flip runbook) deletes. Nothing failed -- a dead markdown
# link produces a 404 for a stranger and no error for us, so the first person to notice
# would have been a visitor to the public repo. The same class had already bitten twice:
# 22 more dead targets across the docs tree, and PR #857, where 2 of 8 checked targets
# had never existed at ANY commit (`--diff-filter=D` finds only the deleted third).
#
# Run it AFTER the flip cut as well as in CI: the cut deletes files, and this is the only
# thing that then reports which links the cut just broke.
#
# TRACKED is the predicate, not on-disk existence: a cloner gets the index, not your
# worktree, so an untracked-but-present target is dead for everyone but you.
#
# The link scanner balances parentheses instead of stopping at the first `)`. A regex that
# stops early reports the frontend's Next.js route groups -- app/(dashboard), app/(auth) --
# as broken paths, and a gate with false positives gets switched off.
set -euo pipefail
cd "$(dirname "$0")/.."

MIN_LINKS="${MIN_LINKS:-400}"

python3 - "$MIN_LINKS" <<'PY'
import re, subprocess, sys, os
from pathlib import Path
from urllib.parse import unquote

min_links = int(sys.argv[1])

def git(*a):
    return subprocess.run(["git", *a], capture_output=True, text=True, check=True).stdout

tracked = set(git("ls-files").splitlines())
# A link may target a directory; it resolves if the index holds anything beneath it.
tracked_dirs = set()
for f in tracked:
    p = Path(f)
    for parent in p.parents:
        if str(parent) != ".":
            tracked_dirs.add(str(parent))

FENCE = re.compile(r"^\s*(```|~~~)")

def strip_code(text):
    """Drop fenced blocks and inline spans -- both hold example links that are not claims."""
    out, in_fence = [], False
    for line in text.split("\n"):
        if FENCE.match(line):
            in_fence = not in_fence
            out.append("")
            continue
        out.append("" if in_fence else re.sub(r"`[^`]*`", "", line))
    return "\n".join(out)

def targets(text):
    """Yield link targets, balancing parens so app/(dashboard) survives intact."""
    i = 0
    while True:
        j = text.find("](", i)
        if j < 0:
            return
        k, depth = j + 2, 1
        while k < len(text) and depth:
            if text[k] == "(":
                depth += 1
            elif text[k] == ")":
                depth -= 1
            k += 1
        if depth:                      # unterminated -- not a link
            i = j + 2
            continue
        yield text[j + 2 : k - 1].strip()
        i = k

SKIP = re.compile(r"^(https?:|mailto:|tel:|ftp:|data:|#|<)", re.I)

# This repo cites code as `[label](path/file.go:123)` -- CLAUDE.md gate 2 mandates it and the
# suffix makes the link jump to the line in Claude Code. Resolve the PATH half: a citation is
# rot when the file is gone, which is what this gate is for.
#
# Caveat worth knowing: github.com does not resolve the `:123` suffix, so these render as 404
# for a web visitor. The portable spelling is `#L123`. Converting ~1600 of them is a mechanical
# but separate change, and it would cost the in-editor jump -- not folded in here silently.
LINE_SUFFIX = re.compile(r":\d+(?:-\d+)?$")

# .claude/ is internal scratch that the flip cut deletes wholesale (runbook 2b) and whose plan
# files address paths from the repo root rather than from themselves. Gating on it would report
# ~1700 findings that no published page can ever 404 on.
EXCLUDE_DIRS = (".claude/",)

total = 0
dead = []
md_files = sorted(
    f for f in tracked
    if f.endswith(".md") and not f.startswith(EXCLUDE_DIRS)
)
for md in md_files:
    try:
        raw_body = Path(md).read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        # Tracked in the index but absent from the worktree. Not a hypothetical: it is the
        # exact mid-cut state the flip produces if `rm -rf` runs before the docs are staged,
        # or if a stray `git add -A` re-indexes paths that were just removed. Report it --
        # crashing with a traceback here would obscure the one finding that matters most.
        dead.append((md, "(file itself)", "tracked but missing from the worktree"))
        continue
    body = strip_code(raw_body)
    # reference definitions: [label]: path
    refs = [m.group(1).strip() for m in re.finditer(r"^\s*\[[^\]]+\]:\s*(\S+)", body, re.M)]
    for raw in list(targets(body)) + refs:
        if not raw or SKIP.match(raw):
            continue
        # `[t](path "title")` -- drop the title, then the anchor/query
        t = raw.split(" ", 1)[0] if raw[0] not in "(<" else raw
        t = unquote(t.split("#", 1)[0].split("?", 1)[0])
        t = LINE_SUFFIX.sub("", t)
        if not t:
            continue
        total += 1
        resolved = os.path.normpath(os.path.join(os.path.dirname(md), t))
        if resolved.startswith(".."):
            dead.append((md, raw, "escapes the repo"))
        elif resolved not in tracked and resolved not in tracked_dirs:
            dead.append((md, raw, "not tracked"))

print(f"scanned {len(md_files)} tracked *.md, {total} relative links")

# Positive denominator: a scanner that silently matched nothing would otherwise pass clean.
if total < min_links:
    print(f"FATAL: only {total} links parsed (expected >= {min_links}). The scanner is broken,")
    print("       not the docs. A count of zero is not a pass.")
    sys.exit(1)

if dead:
    print(f"\nFAIL: {len(dead)} dead link(s) -- each one is a 404 for a stranger:\n")
    for md, raw, why in dead:
        print(f"  {md}: [{raw}] -- {why}")
    sys.exit(1)

print("OK: every relative markdown link resolves to a tracked path")
PY
