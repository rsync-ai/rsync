"""No deployment doc may hand the operator a step the product now performs.

Three changes removed three manual steps from the self-host paths, and each one
left its instructions behind in prose:

  * the Helm chart's `db-init` hook creates `pipeline_db`, `temporal` and
    `temporal_visibility` plus the `uuid-ossp` and `pg_trgm` extensions on an
    external-Postgres install (templates/jobs/db-init.yaml);
  * `docker-compose.byo-postgres.yml`'s `db-init` service does the same for the
    compose BYO path;
  * `ollama-pull` -- the compose service and the chart's post-install hook Job --
    downloads the model into the volume before anything asks Ollama for it.

A leftover instruction is not merely redundant. `CREATE DATABASE temporal OWNER
rsync` run by hand creates a database owned by whichever role the operator was
logged in as, which is how a managed instance ends up with a `temporal` database
the application role cannot write to -- a failure that surfaces as a
CrashLoopBackOff with a permissions error, several steps away from the document
that caused it. And a doc that still says "pull the model yourself" teaches the
reader that the stack does not do it, so they size the disk for one model rather
than for the volume the puller fills.

This is the "a doc claim goes stale with nobody touching the line" shape, same
family as test_docs_do_not_call_built_connectors_unbuilt.py: the sentence was
true when written and a commit in another tree falsified it.

## Why a marker and not a wordlist

Four paths still leave these steps to the operator, and the docs must keep
saying so:

  * `postgresql.dbInit.enabled=false` -- the chart renders no Job;
  * `postgresql.external.iamAuth=true` -- the hook has no password to
    authenticate with, so the chart renders no Job there either. Both gates are
    read verbatim by the `rsync-ai.dbInit.enabled` helper in _helpers.tpl;
  * `docker-compose.prod.yml` -- the production compose overlay defines no
    db-init service at all; it only sets SKIP_DB_CREATE=true. db-init lives on
    `docker-compose.byo-postgres.yml` and nowhere else, so the production path
    against a managed instance still needs `CREATE DATABASE pipeline_db`;
  * an Ollama the operator runs themselves (ollama.md Options B and C) --
    nothing in this repo can pull into a server it does not start.

So the discriminating fact is not the words in the fence, it is WHICH path the
fence documents, and no wordlist can read that. The exemption is therefore an
explicit marker naming the path, and it must PRECEDE the fence it exempts --
a marker that merely appears somewhere in the file would exempt the whole
document, which is the mistake test_no_doc_claims_a_live_prod_environment.py
made and had to correct.

The reason vocabulary is CLOSED (`_REASONS` below). A marker taking free text
would be a rubber stamp: anything a future author wrote would satisfy it, and a
check that accepts every input is not a check.

## Why only fenced code

An instruction is something the reader runs, and a runnable thing lives in a
code fence. Prose is where these same statements get *discussed*, and the
discussion has to stay: self-hosting.md quotes Postgres's own
`permission denied to create extension "uuid-ossp"` while explaining why
`IF NOT EXISTS` does not make the extension optional. Matching that line would
push an author to delete an explanation in order to satisfy a guard about
instructions, which is worse than the defect. The cost is that a manual step
written as prose with no command is invisible here; that shape is rarer, and a
doc that says "create it yourself" without saying how is a different bug.

Subject: markdown. So it belongs in .github/workflows/doc-links.yml and NOT in a
path-filtered ci.yml job -- ci.yml's `pull_request` ignores `**.md`, so the
docs-only PR that reintroduces one of these instructions would skip it entirely.
Registered in the GUARDS census in test_doc_link_gate_runs_on_markdown_only_prs.py.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

WORKFLOW = REPO / ".github" / "workflows" / "doc-links.yml"
CENSUS = Path(__file__).resolve().parent / "test_doc_link_gate_runs_on_markdown_only_prs.py"

# The subject set, defined structurally rather than as a hand-kept list: every
# tracked markdown file a self-hoster is pointed at to deploy the product. A new
# docs/deployment/*.md is in scope the moment it is added, which is the point --
# a hand-kept list is the thing that goes stale here.
SUBJECT_GLOBS = ("docs/deployment/*.md", "deploy/helm/rsync-ai/README.md")

# The three databases and two extensions db-init creates, and the model pull.
#
# `temporal` and `temporal_visibility` are matched as whole words after CREATE
# DATABASE, not anywhere in the line, because both names occur constantly in
# prose that is not an instruction.
_DDL = re.compile(
    r"""CREATE\s+DATABASE\s+"?(pipeline_db|temporal|temporal_visibility)\b"""
    r"""|CREATE\s+EXTENSION\s+(?:IF\s+NOT\s+EXISTS\s+)?["']?(uuid-ossp|pg_trgm)\b""",
    re.I,
)
_PULL = re.compile(r"\bollama\s+pull\b", re.I)

MARKER = re.compile(r"<!--\s*manual-step-ok:\s*(\S[^>]*?)\s*-->")

# Closed vocabulary. Each token names a real path on which the product does NOT
# do the step, so the doc must keep telling the operator to.
_REASONS = frozenset(
    {
        "dbInit-disabled",   # postgresql.dbInit.enabled=false renders no Job
        "iam-auth",          # external.iamAuth=true renders no Job either
        "prod-compose",      # docker-compose.prod.yml defines no db-init service
        "operator-ollama",   # an Ollama this repo does not start
    }
)

# How far above a fence the marker may sit. Enough for a blank line and a
# sentence of lead-in prose, not enough to reach the previous section.
MARKER_WINDOW = 8

_FENCE = re.compile(r"^\s*(?:```|~~~)")


def _marker_window(lines: list[str], fence_at: int) -> list[str]:
    """The lines a marker for the fence opening at `fence_at` may occupy.

    MARKER_WINDOW lines, but truncated at any fence boundary in between: two
    fences a few lines apart are common (a ```sql block, a sentence, another
    ```sql block), and without the truncation one marker would exempt both --
    silently, since the second fence is the one nobody looked at. One marker,
    one fence.
    """
    start = max(0, fence_at - MARKER_WINDOW)
    for j in range(fence_at - 1, start - 1, -1):
        if _FENCE.match(lines[j]):
            start = j + 1
            break
    return lines[start:fence_at]


def _tracked() -> list[Path]:
    out = subprocess.run(
        ["git", "-C", str(REPO), "ls-files", *SUBJECT_GLOBS],
        capture_output=True, text=True, check=True,
    ).stdout.split()
    return [REPO / p for p in out]


def _offences(text: str) -> list[tuple[int, str]]:
    """(1-indexed line, the line) for every unexempted manual step in `text`.

    Only statements inside a fenced block count, and a fence is exempted by a
    marker in the `MARKER_WINDOW` lines above its OPENING -- above the fence,
    not above the statement, so a long fence is covered by one marker and the
    marker still has to precede what it waves through.
    """
    lines = text.splitlines()
    found: list[tuple[int, str]] = []
    fence_open: int | None = None   # 0-indexed line of the open ``` we are in
    exempt = False
    for i, line in enumerate(lines):
        if _FENCE.match(line):
            if fence_open is None:
                fence_open = i
                exempt = any(
                    (m := MARKER.search(w)) and m.group(1) in _REASONS
                    for w in _marker_window(lines, i)
                )
            else:
                fence_open = None
                exempt = False
            continue
        if fence_open is None or exempt:
            continue
        if _DDL.search(line) or _PULL.search(line):
            found.append((i + 1, line.strip()))
    return found


@pytest.fixture(scope="module")
def subjects() -> list[Path]:
    files = _tracked()
    # A positive denominator. An empty or near-empty subject set is what a
    # broken glob looks like, and it reads exactly like a clean tree.
    assert len(files) >= 10, f"subject set collapsed to {len(files)}: {files}"
    return files


def test_the_detectors_fire_on_a_known_bad_document():
    """A probe that returns nothing for everything is a green that checked nothing."""
    bad = "\n".join(
        [
            "Create Temporal's databases yourself:",
            "```sql",
            "CREATE DATABASE temporal OWNER rsync;",
            'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";',
            "```",
            "```bash",
            "docker exec rsync-ollama ollama pull qwen2.5:7b",
            "```",
        ]
    )
    hits = _offences(bad)
    assert [n for n, _ in hits] == [3, 4, 7], hits


def test_prose_is_not_an_instruction():
    """The line self-hosting.md needs to keep: Postgres's own error, quoted.

    Without the fence rule this reads as `CREATE EXTENSION "uuid-ossp"` and the
    only way to satisfy the guard is to delete the explanation.
    """
    prose = (
        "privilege, it still raises — `permission denied to create extension "
        '"uuid-ossp"` on RDS/Cloud SQL.'
    )
    assert _offences(prose) == []
    # ... and the same statement inside a fence still counts.
    assert len(_offences('```sql\nCREATE EXTENSION IF NOT EXISTS "uuid-ossp";\n```')) == 1


def test_the_marker_exempts_only_the_fence_below_it():
    """PRECEDE, not merely appear. A marker under the fence exempts nothing."""
    above = "<!-- manual-step-ok: iam-auth -->\n```sql\nCREATE DATABASE temporal;\n```"
    below = "```sql\nCREATE DATABASE temporal;\n```\n<!-- manual-step-ok: iam-auth -->"
    assert _offences(above) == []
    assert len(_offences(below)) == 1


def test_the_marker_does_not_carry_to_the_next_fence():
    """One marker exempts one fence, not the rest of the document."""
    doc = (
        "<!-- manual-step-ok: iam-auth -->\n"
        "```sql\nCREATE DATABASE temporal;\n```\n"
        "\n"
        "```sql\nCREATE DATABASE temporal_visibility;\n```"
    )
    assert len(_offences(doc)) == 1


def test_an_unlisted_reason_does_not_exempt():
    """The vocabulary is closed, so a marker cannot be a rubber stamp."""
    bad = "<!-- manual-step-ok: because -->\n```sql\nCREATE DATABASE temporal;\n```"
    ok = "<!-- manual-step-ok: iam-auth -->\n```sql\nCREATE DATABASE temporal;\n```"
    assert len(_offences(bad)) == 1
    assert _offences(ok) == []


def test_a_marker_further_up_the_page_does_not_reach():
    """One marker must not exempt a whole document."""
    far = "<!-- manual-step-ok: iam-auth -->" + "\nfiller" * (MARKER_WINDOW + 2)
    assert len(_offences(far + "\n```sql\nCREATE DATABASE temporal;\n```")) == 1


def test_no_deployment_doc_asks_the_operator_to_run_a_step_the_product_runs(subjects):
    offences = {}
    for path in subjects:
        hits = _offences(path.read_text(encoding="utf-8"))
        if hits:
            offences[path.relative_to(REPO)] = hits
    assert not offences, (
        "these deployment docs still hand the operator a step the product now "
        "performs (db-init creates the databases and extensions; ollama-pull "
        "downloads the model). Delete the instruction, or -- if the fence "
        "documents a path where the product genuinely does NOT do it -- put "
        f"`<!-- manual-step-ok: <reason> -->` above it, reason from {sorted(_REASONS)}:\n"
        + "\n".join(
            f"  {p}:{n}  {line}" for p, hits in offences.items() for n, line in hits
        )
    )


def test_every_reason_in_the_vocabulary_is_actually_used(subjects):
    """A token nothing claims is a token nobody maintains.

    This is the other half of the closed vocabulary: the set may not accumulate
    entries that describe paths the docs stopped documenting, because a stale
    entry silently widens what a future marker can wave through.
    """
    used = {
        m.group(1)
        for path in subjects
        for m in MARKER.finditer(path.read_text(encoding="utf-8"))
    }
    assert used <= _REASONS, f"marker reasons outside the vocabulary: {sorted(used - _REASONS)}"
    assert used == _REASONS, f"vocabulary entries no doc uses: {sorted(_REASONS - used)}"


def test_this_guard_runs_where_its_subject_can_change_it():
    """In doc-links.yml, which has no paths filter, and in the census."""
    me = Path(__file__).name
    assert me in WORKFLOW.read_text(encoding="utf-8"), (
        f"{me} is not in {WORKFLOW.relative_to(REPO)}; a markdown-subject guard "
        "left in a path-filtered job cannot see a docs-only PR"
    )
    assert me in CENSUS.read_text(encoding="utf-8"), (
        f"{me} is not enrolled in the GUARDS census in {CENSUS.name}"
    )
