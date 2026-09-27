"""A `docs/capabilities.d/` drop-in must fold into documents the index guard still accepts.

Why the drop-ins exist
----------------------
CLAUDE.md's definition of done puts a status row in almost every change, so almost every
change edits CAPABILITIES.md: 23 of the 25 commits merged before 2026-09-24 touched it.
Two open PRs append to the same table, in the same place, nearly every time.

`.gitattributes` sets `merge=union` on both files for that case. It works locally and
**GitHub's merge engine ignores it**, so the PR reads CONFLICTING -- and a conflicting PR
produces no workflow runs at all. The absent checks read as slow runners, the author
rebases, and a full CI cycle is spent re-proving an unchanged tree.

So a PR now writes its own file under `docs/capabilities.d/`, named after itself, and
`scripts/fold-capabilities-drops.py` appends them to the two documents on `main`, where
nothing is racing it. Two PRs never touch one path, so the conflict cannot occur.

What this guard is for
----------------------
A drop-in is written days before it is folded, and it is folded by a scheduled job whose
failure nobody is watching for. Anything wrong with it -- a row too long, a Known issue
with no write-up, a section name that silently does nothing -- would otherwise surface
long after the PR that introduced it merged, in a job the author never sees.

So the checks run on the PR that writes the drop-in:

  * every drop-in parses, and its name says which PR it came from;
  * a board row is at most ROW_MAX characters and brings its archive row -- the index may
    not claim something whose evidence it cannot show;
  * a Known issue and its write-up arrive together, sharing an id and its anchor;
  * the constants the fold script mirrors still match this directory's index guard;
  * and, the one that makes the rest mean something: folding really does produce files
    that `test_capabilities_stays_an_index.py` accepts, checked by running that guard's
    own assertions over the folded copies -- with a deliberately malformed drop-in proving
    the check can still fail.
"""

from __future__ import annotations

import importlib.util
import re
import shutil
from pathlib import Path

import pytest

from _cut_collection import skip_if_cut

# The two documents, the drop-in directory and the fold script are all removed by
# scripts/flip/excludes.txt, so on the public tree there is nothing here to check.
#
# THIS CALL MUST STAY ABOVE THE `index_guard` IMPORT -- it is not import-order noise.
# test_capabilities_stays_an_index.py calls skip_if_cut itself, at ITS module level. Import
# it first and that call fires during OUR import, so the Skipped is raised with the index
# guard's name in the message and pytest books the skip against ITS file: the log read
# "SKIPPED [2] ... test_capabilities_stays_an_index.py" and this module contributed
# nothing under a name that looked accounted for. assert-public-suite.sh caught it --
# "present on the cut tree but collected nothing and skipped nothing" -- because a guard
# contributing zero tests prints exactly like one that passed.
skip_if_cut("CAPABILITIES.md", "CAPABILITIES-ARCHIVE.md")

import test_capabilities_stays_an_index as index_guard  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
DROPS = REPO / "docs" / "capabilities.d"
SCRIPT = REPO / "scripts" / "fold-capabilities-drops.py"


def _load_fold():
    spec = importlib.util.spec_from_file_location("fold_capabilities_drops", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fold_script = _load_fold()

# Captured at import, before any test can monkeypatch it. _fold_into_copies runs more than
# once inside a single test -- the rejection control folds a bad drop-in, then an orphan --
# and after the first call fold_script.SATELLITES already points into tmp_path, so copying
# from it would be a file onto itself.
REAL_SATELLITES = dict(fold_script.SATELLITES)


def drop_ins() -> list[Path]:
    return sorted(p for p in DROPS.glob("*.md") if p.name != "README.md")


# A drop-in exercising every section, so the fold is tested even when the directory is
# empty -- which it is most of the time, and an empty directory would otherwise let every
# assertion below pass without reading anything.
SPECIMEN = """## board 🔬
| 🔧 | Specimen row from test_capabilities_drops_are_foldable.py; never folded into the real file | [#0](https://github.com/rsync-ai/rsync-ai/pull/0) |

## archive board
| 🔧 **Specimen row** — written by the drop-in guard | Folded only into a temporary copy. **Verify:** `python3 scripts/fold-capabilities-drops.py --check` | [#0](https://github.com/rsync-ai/rsync-ai/pull/0) |

## known issue KI-DROP-IN-SPECIMEN
### KI-DROP-IN-SPECIMEN 🔬 (specimen, not a real defect)
Written by this guard to prove a drop-in folds into a shape the index guard accepts.
→ Full write-up: [CAPABILITIES-ARCHIVE.md § KI-DROP-IN-SPECIMEN](CAPABILITIES-ARCHIVE.md#ki-drop-in-specimen)

## archive known issue KI-DROP-IN-SPECIMEN
<a id="ki-drop-in-specimen"></a>
#### KI-DROP-IN-SPECIMEN 🔬 (specimen, not a real defect)
**Symptom** — none; this block exists so the fold has a write-up to place.
**Files** — `scripts/fold-capabilities-drops.py`, `llm-service/tests/test_capabilities_drops_are_foldable.py`.
"""


def test_the_fold_script_and_the_index_guard_agree_on_the_shape():
    """The script keeps its own copies of these because it runs from the repo root, not
    from this test root. Copies drift; this is what stops them drifting silently."""
    assert fold_script.ROW_MAX == index_guard.ROW_MAX
    assert fold_script.BOARD_TABLES == index_guard.BOARD_TABLES
    assert fold_script.KNOWN_ISSUES == index_guard.KNOWN_ISSUES
    assert fold_script.ACTIVE_WRITE_UPS == index_guard.ACTIVE_WRITE_UPS
    assert fold_script.DOC_MAX_BYTES == index_guard.DOC_MAX_BYTES

    # And the destinations those names point at still exist, or the fold appends nowhere.
    doc = index_guard._lines(index_guard.DOC)
    arch = index_guard._lines(index_guard.ARCHIVE)

    # CELLS_PER_ROW against the real headers. Hard-coding 3 in the script is only safe
    # while both tables really are three columns, and the cost of being wrong is invisible:
    # GFM discards a row's surplus cells without rendering anything amiss.
    headers = [l for l in doc if l.strip() == index_guard.STATUS_HEADER]
    headers += [l for l in arch if l.strip() == "| What | How it was verified | PR |"]
    assert headers, "neither destination table header found; the fold has no column count to match"
    for header in headers:
        columns = len(fold_script.CELL_SPLIT.split(header.strip())[1:-1])
        assert columns == fold_script.CELLS_PER_ROW, (
            f"{header.strip()!r} has {columns} columns, fold script expects "
            f"{fold_script.CELLS_PER_ROW}"
        )

    assert any(l.strip() == fold_script.KNOWN_ISSUES for l in doc)
    assert any(l.strip() == fold_script.ACTIVE_WRITE_UPS for l in arch)
    for emoji in fold_script.BOARD_TABLES:
        assert any(l.startswith("### " + emoji) for l in doc), f"no `### {emoji}` table"


def test_every_drop_in_parses_and_is_named_after_its_pr():
    for path in drop_ins():
        assert re.match(r"^\d+-[a-z0-9][a-z0-9-]*\.md$", path.name), (
            f"{path.name}: name a drop-in <pr-number>-<lower-kebab-slug>.md so the fold "
            f"commit says which PR each block came from"
        )
        fold_script.parse_drop(path)  # raises DropError with the reason


def test_a_drop_in_brings_the_evidence_for_what_it_claims():
    for path in drop_ins():
        parsed = fold_script.parse_drop(path)

        if parsed["board"] and not parsed["archive board"]:
            pytest.fail(
                f"{path.name}: a `## board` row with no `## archive board` row. The index "
                f"is a headline; the evidence behind it lives in the archive."
            )
        for table, text in parsed["board"]:
            for row in (l for l in text.split("\n") if l.strip()):
                assert len(row) <= index_guard.ROW_MAX, (
                    f"{path.name}: {len(row)}-char status row, limit {index_guard.ROW_MAX}. "
                    f"Move the evidence to the `## archive board` section."
                )

        summaries = {ki for ki, _ in parsed["known issue"]}
        write_ups = {ki for ki, _ in parsed["archive known issue"]}
        assert summaries == write_ups, (
            f"{path.name}: Known issues and write-ups must pair up; "
            f"summary only {sorted(summaries - write_ups)}, write-up only {sorted(write_ups - summaries)}"
        )
        for ki, text in parsed["known issue"]:
            body = [l for l in text.split("\n") if l.strip() and not l.startswith("#")]
            assert any(index_guard.WRITE_UP.match(l) for l in body), (
                f"{path.name}: {ki} needs a `→ Full write-up: [...](CAPABILITIES-ARCHIVE.md#...)` line"
            )
            summary = [l for l in body if not index_guard.WRITE_UP.match(l)]
            assert len(summary) <= index_guard.SUMMARY_MAX_LINES, (
                f"{path.name}: {ki} has {len(summary)} summary lines, "
                f"limit {index_guard.SUMMARY_MAX_LINES}"
            )
        for ki, text in parsed["archive known issue"]:
            assert f'<a id="{ki.lower()}"></a>' in text, (
                f"{path.name}: {ki}'s write-up needs `<a id=\"{ki.lower()}\"></a>`, or the "
                f"summary's link lands nowhere"
            )


def _fold_into_copies(tmp_path: Path, monkeypatch, drops: list[Path], *, cap: bool = False) -> None:
    """Fold `drops` into throwaway copies of the two documents and point both the fold
    script and the index guard at the result. Nothing under the repo is touched.

    `cap=False` lifts DOC_MAX_BYTES, because whether a *synthetic* row fits in the real
    file's remaining headroom is not what the structural tests are asking. Conflating the
    two made the fold's own size check fail the shape assertions, which is the wrong
    failure for the wrong reason -- test_the_pending_drop_ins_still_fit asks the size
    question on its own, about the drop-ins that will really be folded."""
    doc, arch = tmp_path / "CAPABILITIES.md", tmp_path / "CAPABILITIES-ARCHIVE.md"
    shutil.copy(index_guard.DOC, doc)
    shutil.copy(index_guard.ARCHIVE, arch)
    monkeypatch.setattr(fold_script, "DOC", doc)
    monkeypatch.setattr(fold_script, "ARCHIVE", arch)

    # The ✅ and 🔬 tables moved to docs/status/ on 2026-09-24, so a board row no longer
    # necessarily lands in DOC. Redirecting DOC alone would leave the fold writing this test's
    # synthetic rows into the real satellites -- which is exactly what happened when they were
    # added, three runs appending eleven rows to the tracked file before the duplicate-row
    # assertion caught it. Every destination the fold can write to is redirected here.
    satellites = {}
    for emoji, real in REAL_SATELLITES.items():
        copy = tmp_path / real.name
        shutil.copy(real, copy)
        satellites[emoji] = copy
    monkeypatch.setattr(fold_script, "SATELLITES", satellites)
    monkeypatch.setattr(index_guard, "SATELLITES", tuple(satellites.values()))

    if not cap:
        monkeypatch.setattr(fold_script, "DOC_MAX_BYTES", 1 << 30)
        monkeypatch.setattr(fold_script, "SATELLITE_MAX_BYTES", 1 << 30)
    fold_script.fold(drops, write=True)  # deletes the copies in tmp_path, not the originals
    monkeypatch.setattr(index_guard, "DOC", doc)
    monkeypatch.setattr(index_guard, "ARCHIVE", arch)


def _staged(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def test_the_pending_drop_ins_still_fit(tmp_path, monkeypatch):
    """The size question, asked about what will really be folded. CAPABILITIES.md is close
    to DOC_MAX_BYTES, so a drop-in can be perfectly well-formed and still have nowhere to
    land -- and it would find that out in the scheduled fold, days after the PR merged."""
    pending = drop_ins()
    if not pending:
        pytest.skip("nothing pending to fold")
    staged = [_staged(tmp_path, p.name, p.read_text(encoding="utf-8")) for p in pending]
    _fold_into_copies(tmp_path, monkeypatch, staged, cap=True)  # raises DropError if not

    folded = index_guard.DOC.stat().st_size
    headroom = index_guard.DOC_MAX_BYTES - folded
    assert headroom >= 0, f"{folded:,} bytes after folding, {-headroom:,} over the limit"
    print(f"\nCAPABILITIES.md after folding {len(pending)} pending drop-in(s): "
          f"{folded:,} bytes, {headroom:,} to spare")


def test_folding_produces_files_the_index_guard_accepts(tmp_path, monkeypatch):
    """The assertion the rest of this file rests on. Everything above checks a drop-in
    against rules restated by hand; this runs the real guard over the real fold."""
    staged = [_staged(tmp_path, "0-specimen.md", SPECIMEN)]
    staged += [_staged(tmp_path, p.name, p.read_text(encoding="utf-8")) for p in drop_ins()]
    _fold_into_copies(tmp_path, monkeypatch, staged)

    index_guard.test_the_scan_finds_the_board_and_the_known_issues()
    index_guard.test_every_status_row_is_one_short_line()
    index_guard.test_no_status_row_is_listed_twice()
    index_guard.test_every_active_known_issue_is_a_summary_and_a_link()
    index_guard.test_every_anchor_between_the_two_files_resolves()
    index_guard.test_archive_ids_are_unique_and_shadow_no_heading()
    index_guard.test_every_active_write_up_has_one_summary_and_no_resolved_twin()

    # Every block the specimen carries must have LANDED, each checked separately and in
    # the table or section it named. Checking only that the file mentions the specimen
    # somewhere is what let a neutered `_append_to_table` pass this whole file: the
    # Known-issue append goes through a different function and covered for it.
    tables = index_guard._status_tables()
    assert any("Specimen row from" in row for _, row in tables["🔬"]), (
        "the `## board 🔬` row is not in the 🔬 table after folding"
    )
    for other in set(index_guard.BOARD_TABLES) - {"🔬"}:
        assert not any("Specimen row from" in row for _, row in tables[other]), (
            f"the `## board 🔬` row landed in the {other} table"
        )

    doc_text = index_guard.DOC.read_text(encoding="utf-8")
    arch_text = index_guard.ARCHIVE.read_text(encoding="utf-8")
    assert "KI-DROP-IN-SPECIMEN" in doc_text, "the `## known issue` block is missing"
    assert "Specimen row** — written by the drop-in guard" in arch_text, (
        "the `## archive board` row is missing"
    )
    assert 'id="ki-drop-in-specimen"' in arch_text, "the `## archive known issue` block is missing"


def test_the_fold_rejects_a_drop_in_the_index_guard_would_reject(tmp_path, monkeypatch):
    """The control. Without it, the test above passes just as well on a fold that silently
    does nothing -- so arm it with input that must fail, and fail for the stated reason."""
    over = SPECIMEN.replace(
        "Specimen row from test_capabilities_drops_are_foldable.py; never folded into the real file",
        "x" * (index_guard.ROW_MAX + 1),
    )
    with pytest.raises(fold_script.DropError, match=f"over {index_guard.ROW_MAX} chars"):
        _fold_into_copies(tmp_path, monkeypatch, [_staged(tmp_path, "0-too-long.md", over)])

    unknown = SPECIMEN + "\n## archive footnote\nnot a section the fold knows\n"
    with pytest.raises(fold_script.DropError, match="unknown section"):
        fold_script.parse_drop(_staged(tmp_path, "0-unknown-section.md", unknown))

    with pytest.raises(fold_script.DropError, match="name must be"):
        fold_script.parse_drop(_staged(tmp_path, "no-pr-number.md", SPECIMEN))

    # A write-up that lost its `<a id>` still folds -- the fold does not judge content --
    # but the index guard must then refuse the result, or a summary would link to nothing.
    orphan = SPECIMEN.replace('<a id="ki-drop-in-specimen"></a>\n', "")
    _fold_into_copies(tmp_path, monkeypatch, [_staged(tmp_path, "0-orphan.md", orphan)])
    with pytest.raises(AssertionError):
        index_guard.test_every_anchor_between_the_two_files_resolves()
