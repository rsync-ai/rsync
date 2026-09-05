"""No post-cut runbook command may name a path the cut deletes.

`scripts/flip/excludes.txt` is the list of paths the public-flip cut removes.
Two of its entries are the flip's own tooling: `scripts/flip` (the gates) and
`docs/internal` (this runbook). So every step after section 2 runs against a
tree that no longer contains either -- and a command written as a bare relative
`scripts/flip/assert-ci-split.py` resolves to a deleted file the moment the
operator's shell is inside the orphan clone.

That is not a style point. The CI-split gate in section 4 is a HARD GATE on the
visibility toggle, and its failure mode is silent in the worst direction: the
operator sees `No such file or directory`, reads it as a setup problem, and the
one control that is ordered before the flip never runs. This is the #930 shape
-- an instruction whose subject the flip itself removes -- and the repo has now
hit it three times (the moat grep in 2b, the excludes-as-deletion-list foot-gun,
this gate). The remedy is the same each time: re-aim the instruction at a tree
where its subject still exists, never satisfy it by deleting the assertion.

Post-cut references are allowed exactly one shape: rooted at `$FLIP_TOOLS`, the
private checkout, which by construction still holds them.

Scope note: sections 1 and 2 are PRE-cut -- section 2 is where the cut happens
-- so relative paths there are correct and are deliberately not scanned.

This file survives the cut (`llm-service/tests` is never excluded; excludes.txt
lines 17/24/25 are comments saying so). Its subject does not, so it skips in the
public repo -- an honest skip: there is nothing left to check once the runbook
is gone. In the private repo, where the flip is actually run from, it runs.
"""

import os
import re

import pytest

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
RUNBOOK = os.path.join(REPO_ROOT, "docs", "internal", "public-flip-runbook.md")
EXCLUDES = os.path.join(REPO_ROOT, "scripts", "flip", "excludes.txt")

subject_present = pytest.mark.skipif(
    not (os.path.isfile(RUNBOOK) and os.path.isfile(EXCLUDES)),
    reason="flip runbook / excludes list absent — cut already applied, nothing to check",
)

# The two variables the runbook binds to the two trees. A reference rooted at
# either one names its tree explicitly and is correct -- $FLIP_TOOLS because the
# private checkout still holds the path, $ORPHAN because naming the cut tree on
# purpose (asserting a path is gone, say) is exactly right. Only the BARE
# RELATIVE form is the defect: it silently means "whichever tree the shell
# happens to be in", and after section 2 that is the tree without the tooling.
ROOTED = re.compile(r"\$\{?(?:FLIP_TOOLS|ORPHAN)\}?/$")


def _active_excludes():
    out = []
    for line in open(EXCLUDES):
        line = line.strip()
        if line and not line.startswith("#"):
            out.append(line)
    return out


def _post_cut_bash(body):
    """Fenced bash blocks from section 3 onward, comment lines dropped.

    Section 2 IS the cut, so everything up to section 3 runs on a tree that
    still has the tooling.
    """
    m = re.search(r"^## 3\.", body, re.M)
    assert m, "runbook has no '## 3.' heading — the pre/post-cut split is unanchored"
    tail = body[m.start():]
    blocks = re.findall(r"^```(?:bash|sh)\n(.*?)^```", tail, re.M | re.S)
    lines = []
    for b in blocks:
        for ln in b.splitlines():
            if ln.strip() and not ln.strip().startswith("#"):
                lines.append(ln)
    return lines


def _offenders(lines, excludes):
    bad = []
    for ln in lines:
        for pref in excludes:
            for m in re.finditer(re.escape(pref) + r"(?![A-Za-z0-9_-])", ln):
                if not ROOTED.search(ln[: m.start()]):
                    bad.append((pref, ln.strip()))
                    break
    return bad


@subject_present
def test_the_detector_is_not_blind():
    """Anti-vacuity: the scan must actually see the runbook's own commands.

    A regex that matched nothing would report a clean tree forever. Zero
    post-cut bash lines, or an excludes list that parsed short, means the
    measurement failed -- which reads identically to a pass.
    """
    lines = _post_cut_bash(open(RUNBOOK).read())
    excludes = _active_excludes()
    # Measured, not guessed: sections 3 and 4 hold the only post-cut bash blocks
    # (20 non-comment lines as of 2026-09-02). A floor picked out of the air is
    # its own blind spot -- it either never fires or fires on a correct tree.
    assert len(lines) >= 15, f"only {len(lines)} post-cut bash lines parsed"
    assert len(excludes) >= 18, f"parsed {len(excludes)} excludes; expected ~28"
    assert "scripts/flip" in excludes, "the gate's own directory left the exclude list"
    # The discriminating anchor: the scan must reach the section-4 gate itself,
    # the exact command this file exists to keep runnable. A line-count floor
    # alone would still pass if the parser stopped before section 4.
    assert any("assert-ci-split.py" in l for l in lines), (
        "the scan never reached the section-4 CI-split gate invocation, so a "
        "regression there would not be seen"
    )
    # The detector must fire on a known-bad line, or it proves nothing below.
    planted = _offenders(['"$PY" scripts/flip/assert-ci-split.py --baseline /tmp/b.json'], excludes)
    assert planted, "detector did not fire on a planted bare relative invocation"
    # ...and must NOT fire on the rooted form, or every line would be an offender.
    ok = _offenders(['"$PY" "$FLIP_TOOLS/scripts/flip/assert-ci-split.py" --baseline /tmp/b.json'], excludes)
    assert not ok, f"detector fired on the correct rooted form: {ok}"


@subject_present
def test_no_post_cut_command_names_a_path_the_cut_deletes():
    excludes = _active_excludes()
    bad = _offenders(_post_cut_bash(open(RUNBOOK).read()), excludes)
    assert not bad, (
        "these post-cut runbook commands name paths the cut removes, so they "
        "resolve to deleted files when run from the orphan tree:\n  "
        + "\n  ".join(f"[{p}] {l}" for p, l in bad)
        + "\n\nRoot them at $FLIP_TOOLS (the private checkout) and pass the "
        "orphan tree as data — e.g. --workflows \"$ORPHAN/.github/workflows\"."
    )
