"""One markdown-link parser, shared by every doc guard in this directory.

There used to be two, and the weaker one is why a guard missed the row it was
written for.

`test_doc_links_resolve.py` had a balanced-paren extractor that strips the
`#fragment` and the `:NN` / `:NN-NN` line anchor before it looks at a target.
`test_doc_merge_claims_are_true.py` had a one-line regex that stripped only
`#L\\d+`. On 2026-08-23, CAPABILITIES.md:886 still claimed "NOT merged, NOT
deployed" four days after the code merged and deployed, and the merge guard did
not fire: the row linked

    docs/explorer/saved-queries-and-models.md#6-version-history-diff-and-restore

whose fragment survived the weaker strip, so the target did not end in `.md`, was
not exempted as a non-source file, failed the `git cat-file` probe, and counted as
"absent from main" -- leaving the row at 7 of 8 paths present, one short of the
all-present threshold that flags a claim as stale. Measured across the whole doc,
6 of 528 unique targets were mis-parsed that way, and every one of them was a free
exemption for whatever row happened to carry it.

That was one defect of four, all of the same kind: the copy nothing tested
directly had drifted from the copy that was tested. So the fix is not three more
regex tweaks -- it is one parser, in one place, with its pins next to it in
`test_doc_links_resolve.py`.
"""

import re

# Targets that are not repo paths and must not be existence-checked.
SKIP_PREFIXES = ("http://", "https://", "mailto:", "#", "//")

# A trailing line anchor: `:120`, or a `:114-119` range (the docs use both a
# hyphen and an en dash). Stripped before any path check for the reason line
# numbers are not asserted at all -- they drift constantly, and pinning them
# would make these guards a maintenance tax rather than a guard.
_LINE_ANCHOR = re.compile(r":\d+(?:[-–]\d+)?$")


def extract_targets(text):
    """Markdown link targets, parsed with balanced-paren awareness.

    A naive `\\]\\(([^)]*)\\)` truncates `](frontend/src/app/(dashboard)/page.tsx)`
    at the first `)` and reports a phantom break -- Next.js route groups put
    parentheses in real directory names, and this repo has them. Counting depth
    costs three lines and removes the whole false-positive class; a guard that
    cries wolf gets muted, which is worse than not having it.
    """
    targets = []
    for m in re.finditer(r"\]\(", text):
        i = m.end()
        depth = 1
        start = i
        while i < len(text) and depth:
            if text[i] == "(":
                depth += 1
            elif text[i] == ")":
                depth -= 1
                if not depth:
                    break
            elif text[i] in "\n":
                depth = 0  # unterminated -- not a link
                start = None
                break
            i += 1
        if start is not None and i < len(text):
            targets.append(text[start:i])
    return targets


def normalize_target(raw):
    """A raw link target reduced to the repo path it names, or None if it names none.

    Order matters: the fragment goes before the line anchor, and both go before
    any caller inspects the extension. A caller that classifies by suffix -- "is
    this a `.md`, so does linking it prove anything?" -- sees the wrong answer for
    every anchored link if it looks first.
    """
    t = (raw or "").strip()
    if not t or t.startswith(SKIP_PREFIXES):
        return None
    t = t.split("#", 1)[0]
    t = _LINE_ANCHOR.sub("", t)
    return t or None


def repo_path_targets(text):
    """Every markdown link in `text` that names a repo path, normalized."""
    out = []
    for raw in extract_targets(text):
        t = normalize_target(raw)
        if t:
            out.append(t)
    return out
