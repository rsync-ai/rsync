"""A markdown table row with more cells than its header silently loses the extra ones.

GitHub-Flavored Markdown splits a table row into cells on `|` **before** any inline
parsing runs. A pipe inside a backtick code span is therefore still a cell separator,
and every cell past the header's column count is **discarded by the renderer**. There
is no error, no warning, and no visual artifact: the text sits in the file, greps
clean, reads correctly in a diff, and is simply absent from the rendered page.

That combination is what makes it worth a guard. The failure is invisible from every
angle an author normally checks -- the source looks right, so the only way to notice
is to read the rendered table and miss something you never knew was there.

Found 2026-09-02: 11 rows across CAPABILITIES.md (5), INVENTORY.md (5) and
PRODUCT_STATUS.md (1). The losses were not cosmetic. PRODUCT_STATUS.md:238 dropped the
entire explanation of why the first orphaned-topic filter was wrong and would have
deleted live `agent.control.*` infrastructure -- the most load-bearing sentence in the
row. INVENTORY.md:1157 dropped the whole rationale for why the Helm chart's 0.1.1 pin
is not interchangeable with 0.1.0. Both rows rendered as confident, truncated prose
that stopped mid-thought without looking truncated.

TWO SUB-CLASSES, and they take different fixes -- which is why this file reports the
arithmetic rather than offering to autofix:

  * an unescaped `|` inside a code span (`404 || 403`, `run: |`, a `grep -vE` alternation).
    Fix: escape it as `\\|`. The code span still renders the pipe.
  * a row that genuinely carries MORE FIELDS than the header has columns -- e.g. a row
    adding a fourth "evidence" field to a three-column table. Escaping is the wrong fix
    here; nothing is malformed. Either fold the extra field into the last real column or
    give the table another column. INVENTORY.md:852-853 were this shape.

WHY THE POSITIVE CONTROL EXISTS. The natural way to write this guard is "scan the docs,
assert zero overflow rows", and a parser that silently stopped matching tables would
satisfy that perfectly -- zero findings, green suite, no coverage. A count of zero is
not self-validating. `test_the_detector_catches_a_known_overflow` feeds the detector a
synthetic row it MUST flag, so a clean sweep of the real corpus means the detector
looked and found nothing rather than never having looked.

WHY THE FLOOR IS SMALL. Three of the documents this currently protects
(CAPABILITIES.md, INVENTORY.md, PRODUCT_STATUS.md) are on `scripts/flip/excludes.txt`
and do not exist in the public repo. Measured 2026-09-02: 552 tables across 104 files
privately, 244 across 58 after the cut. A floor calibrated to the private tree would go
red the day the flip succeeds -- the exact defect #929 removed from the self-hosted
census. The floor is set well under the post-cut number, and the positive control above
is what actually defends against a dead parser.
"""

import os
import re
import subprocess

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Survives the public cut with room to spare (244 tables / 58 files post-cut), and is
# far above zero. Deliberately not pinned near the real count: this number defends
# against a parser that matches nothing, not against ordinary editing.
MIN_TABLES = 40
MIN_FILES_WITH_TABLES = 15

_DELIM = re.compile(r"^\|?[\s:|-]+\|?$")


def _split_cells(row):
    """Split a table row the way GFM does: on unescaped `|`, before inline parsing.

    One leading and one trailing pipe are structural and stripped first. A trailing
    `\\|` is an escaped pipe, not a structural one.
    """
    r = row.strip()
    if r.startswith("|"):
        r = r[1:]
    if r.endswith("|") and not r.endswith("\\|"):
        r = r[:-1]
    out, cur, i = [], "", 0
    while i < len(r):
        if r[i] == "\\" and i + 1 < len(r):
            cur += r[i:i + 2]
            i += 2
            continue
        if r[i] == "|":
            out.append(cur)
            cur = ""
            i += 1
            continue
        cur += r[i]
        i += 1
    out.append(cur)
    return out


def _scan(lines):
    """Yield (row_index, header_cols, row_cols) for rows whose cells would be dropped.

    Fenced code blocks are skipped: a `|` in a shell snippet is not table syntax.
    """
    in_fence = False
    i = 0
    while i < len(lines) - 1:
        stripped = lines[i].strip()
        if stripped.startswith("```") or stripped.startswith("~~~"):
            in_fence = not in_fence
            i += 1
            continue
        if in_fence:
            i += 1
            continue
        nxt = lines[i + 1].strip()
        if stripped.startswith("|") and "-" in nxt and _DELIM.fullmatch(nxt):
            ncol = len(_split_cells(lines[i]))
            j = i + 2
            while j < len(lines) and lines[j].strip().startswith("|"):
                n = len(_split_cells(lines[j]))
                if n > ncol:
                    yield j, ncol, n
                j += 1
            i = j
            continue
        i += 1


def _tracked_markdown():
    """Tracked files only, matching `check-doc-links.sh`'s `git ls-files` predicate.

    An untracked scratch file is not published and is not this guard's business.
    """
    out = subprocess.run(
        ["git", "ls-files", "*.md"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=True,
    ).stdout.split()
    return [p for p in out if os.path.exists(os.path.join(REPO_ROOT, p))]


def _corpus():
    total_tables = 0
    files_with_tables = 0
    offenders = []
    for rel in _tracked_markdown():
        path = os.path.join(REPO_ROOT, rel)
        try:
            lines = open(path, encoding="utf-8").read().split("\n")
        except (OSError, UnicodeDecodeError):
            continue
        seen = False
        in_fence = False
        for i in range(len(lines) - 1):
            s = lines[i].strip()
            if s.startswith("```") or s.startswith("~~~"):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            nxt = lines[i + 1].strip()
            if s.startswith("|") and "-" in nxt and _DELIM.fullmatch(nxt):
                total_tables += 1
                seen = True
        if seen:
            files_with_tables += 1
        for idx, ncol, n in _scan(lines):
            offenders.append((rel, idx + 1, ncol, n, lines[idx]))
    return total_tables, files_with_tables, offenders


def test_the_detector_catches_a_known_overflow():
    """Positive control. Without this, a parser that matched nothing would report a
    clean corpus and look identical to a correct one."""
    good = [
        "| a | b |",
        "|---|---|",
        "| one | two |",
        "| pipe `x | y` here | two |",
    ]
    hits = list(_scan(good))
    assert hits, "detector failed to flag a row whose code span splits the table"
    assert hits[0][0] == 3 and hits[0][1] == 2 and hits[0][2] == 3, hits

    escaped = list(_scan(good[:3] + ["| pipe `x \\| y` here | two |"]))
    assert not escaped, f"escaping the pipe should resolve the overflow, got {escaped}"

    assert not list(_scan(["| a | b |", "|---|---|", "| one | two |"])), "false positive"


def test_fenced_code_is_not_read_as_a_table():
    """A `|` in a shell snippet is not table syntax; reading it as one would make this
    guard fire on documentation it has no business policing."""
    fenced = [
        "```bash",
        "| a | b |",
        "|---|---|",
        "| x | y | z |",
        "```",
    ]
    assert not list(_scan(fenced))


def test_the_corpus_is_not_empty():
    """Vacuity floor. If `git ls-files` or the table matcher stops producing, every
    assertion below passes over nothing."""
    total_tables, files_with_tables, _ = _corpus()
    assert total_tables >= MIN_TABLES, (
        f"only {total_tables} markdown tables found across the tracked corpus; "
        "the scanner is probably broken, not the docs"
    )
    assert files_with_tables >= MIN_FILES_WITH_TABLES, (
        f"only {files_with_tables} files with tables found"
    )


def test_no_table_row_drops_cells():
    _, _, offenders = _corpus()
    if offenders:
        lines = []
        for rel, lineno, ncol, n, text in offenders:
            dropped = _split_cells(text)[ncol:]
            lines.append(
                f"  {rel}:{lineno} -- header has {ncol} columns, row supplies {n}; "
                f"GFM DISCARDS {n - ncol}: {dropped!r}"
            )
        pytest.fail(
            "markdown table rows whose trailing cells are silently dropped by the "
            "renderer (the text is in the file but invisible on github.com):\n"
            + "\n".join(lines)
            + "\n\nFix: if the extra cell came from an unescaped `|` inside a code "
              "span, escape it as `\\|`. If the row genuinely carries more fields "
              "than the header has columns, fold the last field into the previous "
              "cell or add a column to the table."
        )
