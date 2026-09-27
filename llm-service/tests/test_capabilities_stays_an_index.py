"""CAPABILITIES.md must stay an index: one short line per status row, evidence in the archive.

CLAUDE.md tells every agent session to consult CAPABILITIES.md before claiming a feature
works or is broken. By 2026-09-16 the file was 1.6 MB. A single status-board row ran to
several thousand characters -- `file:line` citations, a `Verify` command, the PR trail,
the full debugging narrative -- and its 121 Known issue entries (50 of them active) carried their
whole Symptom / Repro / Root cause / Fix bodies inline. Nobody could read it; agents read
slices of it instead, and a slice of a 1.6 MB file is exactly where a stale or
contradictory claim goes unnoticed. The cost was paid in context window on every task.

The split kept every byte and moved it: each status row became a one-liner (status,
headline, `KI-` ids, PR numbers) and its full text went verbatim to
CAPABILITIES-ARCHIVE.md § Status board -- full rows; each active Known issue became a
`### KI-…` heading, a summary of at most three lines and a `→ Full write-up:` link to
its body in the archive.

Nothing about that shape holds by itself. A row is only one line until someone pastes
the evidence back into it, and the file only stays small while that is rare. So this
guard turns the convention into assertions:

  * every row of the four status tables is at most ROW_MAX characters;
  * no two status rows share a headline -- the defect a `merge=union` driver produces
    when two branches edit the same row (see `.gitattributes`): both versions survive,
    and the file then states two different things about one feature;
  * every active Known issue is a heading, <= 3 summary lines and one write-up link;
  * the file stays under DOC_MAX_BYTES;
  * every `#fragment` link into, out of, or within the two files lands on a heading or
    an `<a id>`, and the archive's `<a id>`s are unique and shadow no heading.

The last one is here because nothing else checks it: `scripts/check-doc-links.sh` and
`test_doc_links_resolve.py` both strip the fragment before testing that the file exists,
and after the split 60 links in CAPABILITIES.md and 73 in the archive cross between the
two files by fragment -- most to an `<a id>`, the rest to a heading slug that changes
when someone rewords the heading.

Every count this guard relies on is cross-checked against a second, independent count of
the same thing (status-looking lines vs parsed rows, `KI-` headings vs parsed blocks, raw
`CAPABILITIES*.md#` mentions vs parsed links), so a parser that stops matching fails
instead of passing every assertion on a smaller set.

Measured 2026-09-16 on the split: 588 status rows, the longest 361 characters; 50 active
Known issues, none over two summary lines; CAPABILITIES.md 135,344 bytes; 60 anchor links
from CAPABILITIES.md into the archive (55 to an `<a id>`, 5 to a heading).
"""

from __future__ import annotations

import re
import subprocess
import unicodedata
from collections import Counter
from pathlib import Path

from _cut_collection import skip_if_cut

# Both files are removed by scripts/flip/excludes.txt. This guard measures one against
# the other, so either one missing leaves it nothing to say.
skip_if_cut(
    "CAPABILITIES.md",
    "CAPABILITIES-ARCHIVE.md",
    "docs/status/verified.md",
    "docs/status/untested.md",
    "CLAUDE.md",
)

REPO = Path(__file__).resolve().parents[2]
DOC = REPO / "CAPABILITIES.md"
ARCHIVE = REPO / "CAPABILITIES-ARCHIVE.md"
# The ✅ and 🔬 tables, moved out of the index on 2026-09-24. They hold 423 of the board's
# 480 rows and none of them is reading material: CLAUDE.md tells a session not to
# re-investigate a ✅, and a 🔬 is verified on real data rather than by reading its row. They
# are looked up with `scripts/cap.py`, which searches all three files at once. Their shape
# is the index's, and every assertion below covers them the same way -- a row that moves out
# of CAPABILITIES.md must not move out of the guard with it.
SATELLITES = (REPO / "docs" / "status" / "verified.md", REPO / "docs" / "status" / "untested.md")
CLAUDE_MD = REPO / "CLAUDE.md"
WORKFLOW = REPO / ".github" / "workflows" / "doc-links.yml"
CENSUS = Path(__file__).resolve().parent / "test_doc_link_gate_runs_on_markdown_only_prs.py"

# A one-liner with two KI ids and two PR numbers fits in well under half of this; the
# longest row on the day of the split was 361. The headroom is for a long headline,
# not for evidence -- evidence goes in the archive.
ROW_MAX = 400

# 65,398 bytes after the ✅/🔬 tables moved to docs/status/ on 2026-09-24, down from 143,608
# against a 150,000 budget it was within 5% of. The number is a context budget, not a disk
# one: CLAUDE.md sends every session here before it may claim a feature works or is broken,
# so this file's size is paid on most tasks, and 150 KB of it was ~36,000 tokens -- 18% of a
# 200k window -- before any code was read. When it trips, retire resolved rows to the archive
# or move a table to a satellite. Do not raise the number; raising it is how it reached 96%
# of the last one.
DOC_MAX_BYTES = 80_000

# Each satellite is looked up by scripts/cap.py, never read, so its budget is looser than the
# index's -- but it is not unbounded: the fold appends here now, and a file with no ceiling is
# the state this whole split exists to undo. untested.md was 46,076 bytes on the day it moved.
SATELLITE_MAX_BYTES = 60_000

# CLAUDE.md is the only file in this family loaded into EVERY turn of every session, so a byte
# here costs more than a byte anywhere else in the repo. Trimmed 2026-09-24 from 16,508 to
# 13,160 (~3,300 tokens a turn) by moving each rule's rationale into the doc that rule links
# to -- the drop-in/union story to docs/capabilities.d/README.md, the Azure-host history to
# prod-environment-status.md, the edition-gate argument to oss-cloud-runtime-split.md -- and
# deleting a "where the commands live" section the docs index already covered. What remains is
# decisions, at ~120 bytes per docs-index row, so the next real saving is a rule leaving, not
# rewording. The headroom is for one new rule. Lower this number after a trim; never raise it.
CLAUDE_MD_MAX_BYTES = 14_000

BOARD = "## Live status board (read this first)"
KNOWN_ISSUES = "## Known issues"
STATUS_HEADER = "| Status | What | Refs |"
# The four tables the status board is made of, by the emoji their `### ` heading opens with.
BOARD_TABLES = ("✅", "⚠️", "❌", "🔬")
WRITE_UP = re.compile(r"^→ Full write-up: \[[^\]]+\]\(CAPABILITIES-ARCHIVE\.md#[^)\s]+\)$")
SUMMARY_MAX_LINES = 3
# The longest `### KI-` heading on 2026-09-18, after the merge/deploy history was cut out
# of the status notes, was 123. A heading is id, status and a short note; the history of
# the fix belongs in the write-up.
KI_HEADING_MAX = 140

LEGEND = "> **Status legend.**"
LEGEND_ENTRY = re.compile(r"^> - (\S+) \*\*")
ACTIVE_WRITE_UPS = "## Active Known issues — full write-ups"
RESOLVED_KIS = "## Known issues (resolved — historical)"
RETIRED_KI_STUBS = "## Retired Known-issue stubs"
KI_ID = re.compile(r"KI-[A-Z0-9]+(?:-[A-Z0-9]+)*")

# A row held in a satellite writes `](../../CAPABILITIES-ARCHIVE.md#…)` where the identical row
# in the index writes `](CAPABILITIES-ARCHIVE.md#…)`: the fold re-roots it on the way in, because
# docs/status/ is two directories down. Those six characters are the file's depth, not the row's
# content. Measured raw, ROW_MAX would mean two different things depending on which file a row
# landed in, and a row that passed the cap on the PR that wrote it could fail it weeks later in
# the scheduled fold, for moving. Normalised out, the cap measures what it is for: how much a
# row says. Two rows were at 397 and went to 403 on the 2026-09-24 split -- that is the defect.
UP_DIR = re.compile(r"(?<=\]\()(?:\.\./)+")

FENCE = re.compile(r"^\s{0,3}(```|~~~)")
CODE_SPAN = re.compile(r"(`+)(?:(?!\1).)+?\1")
HEADING = re.compile(r"^\s{0,3}(#{1,6})\s+(.*?)\s*#*\s*$")
HTML_ID = re.compile(r"""<a\s+(?:id|name)=["']([^"']+)["']""")
CELL_SPLIT = re.compile(r"(?<!\\)\|")


def _lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").split("\n")


def _outside_fences(lines: list[str]):
    """(1-based line number, line with code spans removed) for every line GitHub renders
    as markdown. A heading, id or link inside a fence or a code span is text, not a target."""
    fenced = False
    for n, line in enumerate(lines, start=1):
        if FENCE.match(line):
            fenced = not fenced
            continue
        if not fenced:
            yield n, CODE_SPAN.sub("", line)


# github-slugger keeps Unicode's Alphabetic property, which also covers these enclosed letters (Ⓐ, 🅐).
_ENCLOSED_LETTERS = ((0x24B6, 0x24E9), (0x1F130, 0x1F149), (0x1F150, 0x1F169), (0x1F170, 0x1F189))


def _slug_keeps(c: str) -> bool:
    cat = unicodedata.category(c)
    return (
        c in "- "
        or cat[0] in "LM"
        or cat in ("Nd", "Nl", "Pc")
        or any(a <= ord(c) <= b for a, b in _ENCLOSED_LETTERS)
    )


def gh_slug(heading: str) -> str:
    """The anchor GitHub gives a heading: rendered text, lowercased, with everything but
    letters, marks, decimal and letter numbers, connector punctuation, hyphens and spaces
    dropped, and spaces turned into hyphens. Not Python's `\\w`: that drops marks -- the
    U+FE0F inside `⚠️` survives on GitHub -- and keeps other numbers such as `²` and `½`.
    A code span is text, so `<conn>` inside backticks keeps `conn`; only a tag outside one
    is markup and drops out."""
    text = re.sub(r"`([^`]*)`|<[^>]+>", lambda m: m.group(1) or "", heading)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = text.replace("`", "").replace("*", "").replace("~~", "")
    return "".join(c for c in text.strip().lower() if _slug_keeps(c)).replace(" ", "-")


def _headings(lines: list[str]):
    """(1-based line number, level, raw heading text) for every heading outside a fence.
    Raw: a code span in a heading keeps its text, both in the slug and in a `KI-` id."""
    fenced = False
    for n, line in enumerate(lines, start=1):
        if FENCE.match(line):
            fenced = not fenced
            continue
        m = None if fenced else HEADING.match(line)
        if m:
            yield n, len(m.group(1)), m.group(2)


def _heading_slugs(lines: list[str]) -> set[str]:
    seen: Counter[str] = Counter()
    slugs = set()
    for _, _, text in _headings(lines):
        slug = gh_slug(text)
        slugs.add(slug if seen[slug] == 0 else f"{slug}-{seen[slug]}")
        seen[slug] += 1
    return slugs


def _html_ids(lines: list[str]) -> list[str]:
    return [i for _, line in _outside_fences(lines) for i in HTML_ID.findall(line)]


def _targets(lines: list[str]) -> set[str]:
    return _heading_slugs(lines) | set(_html_ids(lines))


def _fragment_links(lines: list[str], into: str | None):
    """(line number, fragment) for `](#x)` when `into` is None, else for `](<into>#x)`."""
    prefix = r"\]\(#" if into is None else r"\]\((?:\./|\.\./)*(?:[\w.-]+/)*" + re.escape(into) + "#"
    pattern = re.compile(prefix + r"([^)\s]+)\)")
    return [(n, frag) for n, line in _outside_fences(lines) for frag in pattern.findall(line)]


def _section(lines: list[str], heading: str) -> tuple[int, int]:
    """[start, end) 0-based: `heading` up to the next `## ` heading outside a fence."""
    start = lines.index(heading)
    fenced = False
    for i in range(start + 1, len(lines)):
        if FENCE.match(lines[i]):
            fenced = not fenced
        elif not fenced and lines[i].startswith("## "):
            return start, i
    return start, len(lines)


def _board_sources() -> list[tuple[str, list[str], int, int]]:
    """(name, lines, start, end) for every file the board lives in: the index's
    `## Live status board` section, and each satellite whole. A satellite is one table with
    a header above it, so its section is the file."""
    doc = _lines(DOC)
    start, end = _section(doc, BOARD)
    sources = [("CAPABILITIES.md", doc, start, end)]
    for path in SATELLITES:
        lines = _lines(path)
        # Bare name when the path is not under the repo: the fold's tests point this guard at
        # throwaway copies in a tmp dir, and a ValueError there would fail for the wrong reason.
        try:
            name = path.relative_to(REPO).as_posix()
        except ValueError:
            name = path.name
        sources.append((name, lines, 0, len(lines)))
    return sources


def _status_tables() -> dict[str, list[tuple[str, str]]]:
    """The four board tables: emoji -> [("file:line", row)], header and delimiter excluded.

    Keyed by emoji across every source, so a table that exists twice -- the defect a split
    invites, one copy left behind in the index and one moved -- fails on the second one
    rather than quietly shadowing the first."""
    tables: dict[str, list[tuple[str, str]]] = {}
    for name, lines, start, end in _board_sources():
        heading, rows = None, None
        for i in range(start, end):
            line = lines[i]
            if line.startswith("### "):
                heading, rows = line[4:], None
            elif line.strip() == STATUS_HEADER and heading:
                key = next((e for e in BOARD_TABLES if heading.startswith(e)), None)
                assert key, f"{name}:{i + 1}: a status table under an unrecognised heading: {heading!r}"
                assert key not in tables, f"{name}:{i + 1}: a second {key} status table"
                rows = tables[key] = []
            elif rows is not None and line.startswith("|"):
                if not re.fullmatch(r"\|[\s:|-]+\|", line.strip()):
                    rows.append((f"{name}:{i + 1}", line))
            else:
                rows = None
    return tables


def _cells(row: str) -> list[str]:
    return [c.strip() for c in CELL_SPLIT.split(row.strip()[1:-1])]


def _row_len(row: str) -> int:
    """A row's length in content, with the relative-path depth of whichever file holds it
    normalised away. See UP_DIR."""
    return len(UP_DIR.sub("", row))


def _active_known_issues() -> list[tuple[int, str, list[str]]]:
    """(line number, heading, non-blank body lines) for every `### …KI-…` under Known issues.
    A block ends at the next real heading; a `#` inside a fence or at the start of a
    wrapped line is body text."""
    lines = _lines(DOC)
    start, end = _section(lines, KNOWN_ISSUES)
    blocks: list[tuple[int, str, list[str] | None]] = []
    fenced = False
    for i in range(start + 1, end):
        line = lines[i]
        if FENCE.match(line):
            fenced = not fenced
        elif not fenced and HEADING.match(line):
            if line.startswith("### ") and "KI-" in line:
                blocks.append((i + 1, line, []))
            elif blocks and blocks[-1][2] is not None:
                blocks.append((i + 1, line, None))  # closes the previous block
            continue
        if blocks and blocks[-1][2] is not None and line.strip():
            blocks[-1][2].append(line)
    return [b for b in blocks if b[2] is not None]


def _legend() -> set[str]:
    lines = _lines(DOC)
    start = lines.index(LEGEND)
    entries = set()
    for line in lines[start + 1 :]:
        if not line.startswith(">"):
            break
        m = LEGEND_ENTRY.match(line)
        if m:
            entries.add(m.group(1))
    return entries


def _raw_mentions(lines: list[str], into: str | None) -> list[tuple[int, int]]:
    """(line number, count) of every `<into>#` (or `](#`) outside code, by a pattern that knows
    nothing about link syntax -- the independent count `_fragment_links` must agree with."""
    pattern = re.compile(r"\]\(#" if into is None else r"(?<![\w-])" + re.escape(into) + "#")
    counts = Counter(n for n, line in _outside_fences(lines) for _ in pattern.finditer(line))
    return sorted(counts.items())


def _parsed_mentions(lines: list[str], into: str | None) -> list[tuple[int, int]]:
    return sorted(Counter(n for n, _ in _fragment_links(lines, into)).items())


def test_the_scan_finds_the_board_and_the_known_issues():
    """The denominator under every assertion below."""
    tables = _status_tables()
    assert sorted(tables) == sorted(BOARD_TABLES), (
        f"found status tables {sorted(tables)} under {BOARD!r}, expected one each for "
        f"{BOARD_TABLES} with the header {STATUS_HEADER!r} -- the board was restructured "
        "and this guard is now checking less than all of it"
    )
    legend = _legend()
    assert set(BOARD_TABLES) <= legend, f"the status legend lists {sorted(legend)}, missing {set(BOARD_TABLES) - legend}"

    # Every line in the board that starts with a legend status must be a row the table walk
    # found. A blank line or a stray heading splitting a table ends the walk early.
    parsed = {w for rows in tables.values() for w, _ in rows}
    assert parsed, "the table walk found no status rows"
    status_lines: dict[str, str] = {}
    for name, src, start, end in _board_sources():
        for i in range(start, end):
            line = src[i]
            if line.startswith("|") and line.count("|") > 1 and _cells(line)[0] in legend:
                status_lines[f"{name}:{i + 1}"] = line
    lost = sorted(set(status_lines) - parsed)
    assert not lost, (
        "status-looking rows under the board that no table walk reached (a blank line or "
        "heading split the table, or the row sits outside the four tables):\n"
        + "\n".join(f"{w}: {status_lines[w][:120]}" for w in lost)
    )
    unknown = sorted(
        f"{w}: {row[:120]}"
        for rows in tables.values()
        for w, row in rows
        if _cells(row)[0] not in legend
    )
    assert not unknown, (
        "status rows whose status is not in the legend at the top of CAPABILITIES.md -- add it "
        "to the legend or use a listed one:\n" + "\n".join(unknown)
    )

    lines = _lines(DOC)
    kis = _active_known_issues()
    assert kis, "no active Known issues parsed"
    ki_start, ki_end = _section(lines, KNOWN_ISSUES)
    headings = [
        n for n, line in _outside_fences(lines[ki_start:ki_end]) if HEADING.match(line) and "KI-" in line
    ]
    headings = [n + ki_start for n in headings]
    assert headings == [n for n, _, _ in kis], (
        "`KI-` headings under Known issues that are not `### ` blocks the parser reads: "
        f"{sorted(set(headings) ^ {n for n, _, _ in kis})}"
    )
    write_ups = sum(line.startswith("→ Full write-up:") for line in lines[ki_start:ki_end])
    assert write_ups == len(kis), f"{write_ups} `→ Full write-up:` lines for {len(kis)} Known issues"


def test_every_status_row_is_one_short_line():
    """Evidence pasted back into a row is the regression this file exists to stop."""
    offenders = [
        f"{w}: {_row_len(row)} chars: {row[:120]}…"
        for rows in _status_tables().values()
        for w, row in rows
        if _row_len(row) > ROW_MAX
    ]
    assert not offenders, (
        f"status rows longer than {ROW_MAX} characters. Keep the one-liner to status, "
        "headline and refs; put the evidence in the row's full text in "
        "CAPABILITIES-ARCHIVE.md § Status board -- full rows:\n" + "\n".join(offenders)
    )
    bad_shape = [
        f"{w}: {len(_cells(row))} cells: {row[:120]}"
        for rows in _status_tables().values()
        for w, row in rows
        if len(_cells(row)) != 3 or not _cells(row)[1]
    ]
    assert not bad_shape, "status rows that are not `| status | headline | refs |`:\n" + "\n".join(bad_shape)
    cut_off = [
        f"{w}: {row[:120]}"
        for rows in _status_tables().values()
        for w, row in rows
        if _cells(row)[1].endswith("…")
    ]
    assert not cut_off, (
        "status rows whose headline ends in `…` -- a headline cut to fit reads as a claim it "
        "never finishes. Write a short, complete one that keeps the opening words of the "
        "archive's full row, so a grep for them still lands:\n" + "\n".join(cut_off)
    )


def test_no_status_row_is_listed_twice():
    """Both sides of a union merge survive. One feature with two rows has two statuses."""
    seen: dict[str, list[str]] = {}
    for rows in _status_tables().values():
        for w, row in rows:
            seen.setdefault(_cells(row)[1], []).append(w)
    dupes = [f"{', '.join(ws)}: {what}" for what, ws in seen.items() if len(ws) > 1]
    assert not dupes, (
        "status rows sharing a headline -- keep the current one and delete the other, or "
        "make the headlines say what distinguishes them:\n" + "\n".join(dupes)
    )


def test_every_active_known_issue_is_a_summary_and_a_link():
    offenders = []
    for n, heading, body in _active_known_issues():
        if not body or not WRITE_UP.match(body[-1]):
            offenders.append(f"CAPABILITIES.md:{n}: {heading[:90]} -- last line is not a `→ Full write-up:` link")
        elif len(body) - 1 > SUMMARY_MAX_LINES:
            offenders.append(f"CAPABILITIES.md:{n}: {heading[:90]} -- {len(body) - 1} summary lines")
        elif sum(bool(WRITE_UP.match(b)) for b in body) != 1:
            offenders.append(f"CAPABILITIES.md:{n}: {heading[:90]} -- more than one write-up link")
        if len(heading) > KI_HEADING_MAX:
            offenders.append(f"CAPABILITIES.md:{n}: {heading[:90]} -- heading is {len(heading)} chars, over {KI_HEADING_MAX}")
    assert not offenders, (
        f"active Known issues must be a heading, <= {SUMMARY_MAX_LINES} summary lines and one "
        "`→ Full write-up:` link; the Symptom / Repro / Root cause / Fix body belongs under "
        "CAPABILITIES-ARCHIVE.md § Active Known issues -- full write-ups:\n" + "\n".join(offenders)
    )


def test_the_index_stays_small():
    size = DOC.stat().st_size
    assert size <= DOC_MAX_BYTES, (
        f"CAPABILITIES.md is {size:,} bytes, over the {DOC_MAX_BYTES:,} budget. Retire resolved "
        "rows and Known issues to CAPABILITIES-ARCHIVE.md, or move a whole table to a "
        "docs/status/ satellite as the ✅ and 🔬 tables were on 2026-09-24 -- rather than raising "
        "the budget. Every works/broken claim starts from this file, so its size is paid in "
        "context on most tasks."
    )


def test_each_satellite_stays_small():
    """A table that left the index is still on the board, and still grows every time the fold
    runs. Without its own ceiling it becomes the thing the split was meant to prevent."""
    over = [
        f"{p.relative_to(REPO).as_posix()} is {p.stat().st_size:,} bytes, over {SATELLITE_MAX_BYTES:,}"
        for p in SATELLITES
        if p.stat().st_size > SATELLITE_MAX_BYTES
    ]
    assert not over, (
        "status-board satellites over budget. Retire resolved rows to "
        "CAPABILITIES-ARCHIVE.md § Retired status-board rows:\n" + "\n".join(over)
    )


def test_the_always_loaded_rules_file_stays_small():
    """CLAUDE.md is loaded into EVERY turn of every session -- the only file here that is.
    A byte costs more than a byte anywhere else in the repo, and nothing else measures it:
    the index guard above bounds the file a session reads once, this bounds the file it
    re-reads on every turn. A rule belongs here; the reasoning behind it belongs in the doc
    the rule links to."""
    size = CLAUDE_MD.stat().st_size
    assert size <= CLAUDE_MD_MAX_BYTES, (
        f"CLAUDE.md is {size:,} bytes (~{size // 4:,} tokens, paid on every turn), over the "
        f"{CLAUDE_MD_MAX_BYTES:,} budget. Move the rationale behind the new rule into the doc it "
        "links to and leave the decision here, rather than raising the budget."
    )


def test_every_anchor_between_the_two_files_resolves():
    doc, arc = _lines(DOC), _lines(ARCHIVE)
    doc_targets, arc_targets = _targets(doc), _targets(arc)
    into_archive = _fragment_links(doc, "CAPABILITIES-ARCHIVE.md")
    assert into_archive, "no `CAPABILITIES-ARCHIVE.md#…` links found in CAPABILITIES.md"
    unparsed = [
        f"{name}:{n}"
        for name, lines, into in (
            ("CAPABILITIES.md", doc, "CAPABILITIES-ARCHIVE.md"),
            ("CAPABILITIES.md", doc, None),
            ("CAPABILITIES-ARCHIVE.md", arc, "CAPABILITIES.md"),
            ("CAPABILITIES-ARCHIVE.md", arc, None),
        )
        for n, _ in sorted(set(_raw_mentions(lines, into)) ^ set(_parsed_mentions(lines, into)))
    ]
    assert not unparsed, (
        "lines where an anchor mention is not a link this guard can parse (a title, a space, "
        "an angle-bracket target) -- write it as a plain `](FILE#fragment)`:\n" + "\n".join(unparsed)
    )
    checks = [
        ("CAPABILITIES.md", into_archive, arc_targets),
        ("CAPABILITIES.md", _fragment_links(doc, None), doc_targets),
        ("CAPABILITIES-ARCHIVE.md", _fragment_links(arc, "CAPABILITIES.md"), doc_targets),
        ("CAPABILITIES-ARCHIVE.md", _fragment_links(arc, None), arc_targets),
    ]
    dead = [f"{name}:{n}: #{frag}" for name, links, targets in checks for n, frag in links if frag not in targets]
    assert not dead, (
        "anchor links with no heading or <a id> to land on (GitHub scrolls nowhere; the "
        "reader is left at the top of a 3 MB file):\n" + "\n".join(dead)
    )


def test_links_from_other_docs_land_on_an_anchor():
    """A split, or a reworded heading, silently breaks every `CAPABILITIES.md#section` elsewhere."""
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--", "*.md"], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout.split("\0")
    targets = {"CAPABILITIES.md": _targets(_lines(DOC)), "CAPABILITIES-ARCHIVE.md": _targets(_lines(ARCHIVE))}
    # A drop-in's Known-issue summary links its own write-up's `<a id>`, which reaches the
    # archive only when the daily fold copies it there (docs/capabilities.d/README.md).
    for rel in listed:
        if rel.startswith("docs/capabilities.d/") and Path(rel).name != "README.md" and (REPO / rel).is_file():
            targets["CAPABILITIES-ARCHIVE.md"] |= set(_html_ids(_lines(REPO / rel)))
    others = [p for p in listed if p and Path(p).name not in targets and (REPO / p).is_file()]
    assert len(others) > 50, f"git ls-files listed only {len(others)} other markdown files"
    texts = {rel: _lines(REPO / rel) for rel in others}
    unparsed = [
        f"{rel}:{n}: {name}#"
        for rel, lines in texts.items()
        for name in targets
        for n, _ in sorted(set(_raw_mentions(lines, name)) ^ set(_parsed_mentions(lines, name)))
    ]
    assert not unparsed, "anchor mentions this guard cannot parse as a plain link:\n" + "\n".join(unparsed)
    dead = [
        f"{rel}:{n}: {name}#{frag}"
        for rel, lines in texts.items()
        for name, found in targets.items()
        for n, frag in _fragment_links(lines, name)
        if frag not in found
    ]
    assert not dead, "links into CAPABILITIES*.md whose #fragment no longer exists:\n" + "\n".join(dead)


def test_archive_ids_are_unique_and_shadow_no_heading():
    """GitHub scrolls to the first match; a second `<a id>` or a heading with the same slug
    makes a write-up link open on the wrong entry, with nothing looking broken."""
    arc = _lines(ARCHIVE)
    ids = _html_ids(arc)
    assert ids, "no <a id> anchors in the archive"
    dupes = sorted(i for i, c in Counter(ids).items() if c > 1)
    shadowed = sorted(set(ids) & _heading_slugs(arc))
    assert not dupes, f"<a id> values used more than once in CAPABILITIES-ARCHIVE.md: {dupes}"
    assert not shadowed, f"<a id> values that equal a heading slug in CAPABILITIES-ARCHIVE.md: {shadowed}"


def test_the_slug_matches_github():
    """Pinned to the github-slugger rules github.com applies, so a resolver edit cannot quietly
    agree with itself. GitHub's /markdown API no longer returns heading ids, so there is no live
    render to compare against from CI; the corroboration is that all 70 hand-written `](#…)`
    links in the pre-split CAPABILITIES.md resolve under this function, and that it flags the
    seven `](#known-issues)` links the pre-split archive carried with no such heading."""
    assert gh_slug("Live status board (read this first)") == "live-status-board-read-this-first"
    assert gh_slug("✅ Working (verified end-to-end in this session)") == "-working-verified-end-to-end-in-this-session"
    assert gh_slug("KI-BOTH-REPOS-PUBLISH-THE-SAME-IMAGE-NAMES ❌ (active, HIGH/release-safety)") == (
        "ki-both-repos-publish-the-same-image-names--active-highrelease-safety"
    )
    assert gh_slug("`KI-X` in [a doc](x.md) **bold**") == "ki-x-in-a-doc-bold"
    # A code span's `<conn>` is text; a bare `<b>` is a tag. Four archive headings carry the first.
    assert gh_slug("~~KI-X~~ called a `<conn>_query` tool, <b>not</b> `<div>`") == (
        "ki-x-called-a-conn_query-tool-not-div"
    )
    assert gh_slug("⚠️ Partial / known caveat") == "\ufe0f-partial--known-caveat"
    assert gh_slug("x² ½ ③") == "x--"
    assert gh_slug("e\u0301") == "e\u0301"
    assert gh_slug("Ⅻ roman") == "ⅻ-roman"
    assert _heading_slugs(["# Notes", "## Notes", "```", "# Notes", "```"]) == {"notes", "notes-1"}


def test_every_active_write_up_has_one_summary_and_no_resolved_twin():
    """An archive write-up nothing links to is a Known issue the index forgot; one that two
    summaries link to -- or one KI with two summaries, which a union merge produces the same
    way it duplicates a status row -- says two things about one issue; and a KI that is both
    an active summary and a resolved heading in the archive has two statuses."""
    arc = _lines(ARCHIVE)
    start, end = _section(arc, ACTIVE_WRITE_UPS)
    section = list(_outside_fences(arc[start:end]))
    ids = [i for _, line in section for i in HTML_ID.findall(line)]
    ki_headings = [line for _, line in section if line.startswith("#### ") and "KI-" in line]
    assert ids, f"no <a id> under {ACTIVE_WRITE_UPS!r}"
    assert len(ids) == len(ki_headings), f"{len(ids)} <a id>s but {len(ki_headings)} `#### KI-` write-ups"

    kis = _active_known_issues()
    active = [(n, m.group(0)) for n, heading, _ in kis for m in [KI_ID.search(heading)] if m]
    links = [
        (n, m.group(1))
        for n, _, body in kis
        for m in [re.search(r"CAPABILITIES-ARCHIVE\.md#([^)\s]+)\)", body[-1] if body else "")]
        if m
    ]
    assert active, "no KI ids in the active Known issue headings"
    for what, pairs in (
        ("Known issues with more than one summary", active),
        ("write-ups linked from more than one summary", links),
    ):
        twice = sorted(k for k, c in Counter(k for _, k in pairs).items() if c > 1)
        assert not twice, f"{what} in CAPABILITIES.md -- keep one:\n" + "\n".join(
            f"CAPABILITIES.md:{', '.join(str(n) for n, x in pairs if x == k)}: {k}" for k in twice
        )
    linked = {frag for _, frag in links}
    orphans, strays = sorted(set(ids) - linked), sorted(linked - set(ids))
    assert not orphans, f"archive write-ups no Known issue in CAPABILITIES.md links to: {orphans}"
    assert not strays, f"write-up links that do not land in {ACTIVE_WRITE_UPS!r}: {strays}"

    # A heading is resolved when it strikes something or says RESOLVED, at any level. Its
    # subjects are the ids it strikes plus the first id it names; an id further along is a
    # cross-reference -- `~~KI-HEAL-TWO-ZOMBIE-SWEEPERS-RACE~~ ✅ RESOLVED` names the still
    # active KI-HEAL-ZOMBIE-SWEEP-LEAVES-PIPELINE-RUNNING as the bug its duplicate reintroduced.
    resolved: list[tuple[int, str]] = []
    for name in (RESOLVED_KIS, RETIRED_KI_STUBS):
        r_start, r_end = _section(arc, name)
        found = [
            (r_start + n, k)
            for n, _, text in _headings(arc[r_start:r_end])
            if "~~" in text or "RESOLVED" in text
            for first in [KI_ID.search(text)]
            if first
            for struck in [{s for span in re.findall(r"~~(.+?)~~", text) for s in KI_ID.findall(span)}]
            for k in {first.group(0)} | struck
        ]
        assert found, f"no resolved `KI-` headings parsed under {name!r}"
        resolved += found
    active_ids = {k for _, k in active}
    both = sorted(f"CAPABILITIES-ARCHIVE.md:{n}: {k}" for n, k in resolved if k in active_ids)
    assert not both, (
        "Known issues that are active in CAPABILITIES.md and resolved in the archive -- drop the "
        "summary if the issue is fixed, or the resolved heading if it is not:\n" + "\n".join(both)
    )


def test_this_guard_runs_where_its_subject_can_change_it():
    """In doc-links.yml, which has no paths filter, and in the census."""
    me = Path(__file__).name
    assert me in WORKFLOW.read_text(encoding="utf-8"), (
        f"{me} is not in {WORKFLOW.relative_to(REPO)}; a markdown-subject guard "
        "left in a path-filtered job cannot see a docs-only PR"
    )
    assert me in CENSUS.read_text(encoding="utf-8"), f"{me} is not enrolled in the GUARDS census in {CENSUS.name}"
