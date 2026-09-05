"""Every ```bash block in a tracked *.md must survive `bash -n`.

Nothing in this repo syntax-checks documented shell. `scripts/*.sh` are executed
by CI and by the install/gate paths, so a broken one fails loudly; the shell we
*print for a reader to paste* is checked by nobody. A missing quote or an
unbalanced `fi` in a runbook block is invisible until an operator pastes it into
a terminal during the procedure the block exists to describe -- which is the
worst possible moment for it to be wrong.

This is a syntax gate, not a semantic one. `bash -n` parses without executing:
it catches unbalanced quotes, `if` without `fi`, a dangling `&&`, a heredoc
whose terminator never arrives. It says nothing about whether the command is
correct, whether the flags exist, or whether the host is reachable -- those are
other guards' jobs (test_documented_install_commands_render.py,
test_no_doc_claims_a_live_prod_environment.py). Do not read a pass here as
"the runbook works."

Scope and its two honest limits:

  * Only fences tagged `bash`, `sh`, `shell` or `zsh` are checked (292 + 2 = 294
    blocks across 53 files at the time of writing; docs/runbook.md holds 23).
    281 fences carry NO language tag at all. Most are output samples rather than
    commands, and retagging them wholesale would be a large docs change that
    this guard does not justify -- so an untagged fence is an evasion route, and
    saying so here is more useful than pretending otherwise.
  * `zsh` blocks are parsed by bash. The two shells' grammars diverge, so this
    is approximate for those. There are none today; the tag is accepted so a
    future one is checked approximately rather than not at all.

PLACEHOLDERS. Documentation writes `ssh azureuser@<public-ip>`, and bash reads
`<public-ip>` as an input redirection followed by a syntax error. Sixteen blocks
fail on this and every one of them is correct prose. The naive fix -- skip any
block containing angle brackets -- would exempt 39 blocks entirely, including
whatever real error one of them later grows. Instead the placeholder is
substituted for a word and the rest of the block stays in scope.

The substitution pattern is deliberately narrow, because one real redirection in
this repo looks superficially like a placeholder: `tr -dc … </dev/urandom |
head -c 32 > …` matches `<…>` if you let the pattern span a pipe. A placeholder
is a *word* standing where an operand goes, so it may not contain a shell
metacharacter or a quote, may not open with a path character, and may not be
the second `<` of a `<<`. Those four clauses were not written from first
principles -- the first draft carried only two and this file's own fixtures
caught it eating the `<'EOF' >` out of `cat <<'EOF' > out`, a shape the real
tree happens not to contain today. All four together exclude the urandom line
and both heredoc forms while keeping all 51 genuine placeholders; the count is
the check that the tightening cost nothing.
"""

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

_SHELL_TAGS = {"bash", "sh", "shell", "zsh"}

# Indentation-aware so a fence nested in a list item closes on its own indent.
_FENCE = re.compile(
    r"(?ms)^(?P<indent>[ \t]*)```(?P<info>[^\n`]*)\n(?P<body>.*?)^(?P=indent)```[ \t]*$"
)

# A documentation placeholder: `<...>` containing no shell metacharacter and not
# opening on a path character. See the module docstring for why both halves of
# that are load-bearing.
_PLACEHOLDER = re.compile(r"(?<!<)<(?![/.~])[^<>\n|&;`$()'\"]+>")

# Enough that a broken extractor cannot pass by finding nothing, low enough to
# stay true after the public cut removes a slice of docs/. The real non-vacuity
# proof is test_the_fence_extractor_is_not_vacuous, which does not depend on the
# tree at all.
_MIN_BLOCKS = 20


def _tracked_markdown() -> list[str]:
    proc = subprocess.run(
        ["git", "ls-files", "*.md"], cwd=REPO, capture_output=True, text=True
    )
    if proc.returncode != 0:
        return []
    return proc.stdout.split()


def _shell_blocks(text: str) -> list[tuple[int, str, str]]:
    """(1-indexed line of the opening fence, tag, body) for each shell fence."""
    out = []
    for m in _FENCE.finditer(text):
        info = m.group("info").strip()
        tag = info.split()[0].lower() if info else ""
        if tag in _SHELL_TAGS:
            out.append((text[: m.start()].count("\n") + 1, tag, m.group("body")))
    return out


def _collect() -> list[tuple[str, int, str, str]]:
    found = []
    for rel in _tracked_markdown():
        path = REPO / rel
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for line, tag, body in _shell_blocks(text):
            found.append((rel, line, tag, body))
    return found


def _parses(body: str) -> tuple[bool, str]:
    proc = subprocess.run(
        ["bash", "-n"], input=_PLACEHOLDER.sub("PLACEHOLDER", body),
        capture_output=True, text=True,
    )
    return proc.returncode == 0, proc.stderr.strip()


# --------------------------------------------------------------------------
# Non-vacuity. Each of the three moving parts -- the extractor, the placeholder
# pattern, and bash itself -- is exercised against an input whose answer is
# known, so a zero from the real tree is a fact about the tree and not a broken
# component reporting nothing to do.
# --------------------------------------------------------------------------


def test_the_fence_extractor_is_not_vacuous():
    fixture = (
        "intro\n"
        "```bash\necho one\n```\n"
        "```python\nprint('not shell')\n```\n"
        "```\necho untagged is out of scope\n```\n"
        "```sh\necho two\n```\n"
        "- a list item:\n"
        "  ```bash\n  echo nested\n  ```\n"
        "```bash title=\"info string with attributes\"\necho three\n```\n"
    )
    got = [(tag, body.strip()) for _, tag, body in _shell_blocks(fixture)]
    assert got == [
        ("bash", "echo one"),
        ("sh", "echo two"),
        ("bash", "echo nested"),
        ("bash", "echo three"),
    ]


def test_the_placeholder_pattern_spares_real_redirection():
    placeholder = "ssh azureuser@<public-ip>"
    assert _PLACEHOLDER.sub("PLACEHOLDER", placeholder) == "ssh azureuser@PLACEHOLDER"

    # The one line in this repo that a loose pattern would eat. It must come
    # back byte-identical, or the guard would be silently skipping real shell.
    redirection = "tr -dc 'A-Za-z0-9' </dev/urandom | head -c 32 > secret.txt"
    assert _PLACEHOLDER.sub("PLACEHOLDER", redirection) == redirection

    heredoc = "cat <<'EOF' > out\nbody\nEOF"
    assert _PLACEHOLDER.sub("PLACEHOLDER", heredoc) == heredoc


def test_bash_rejects_a_known_bad_script():
    """Arm the probe. A `bash -n` that returned 0 for everything -- wrong path,
    missing binary, swallowed error -- would make every assertion below vacuous
    while green, so establish a known non-zero case first."""
    assert shutil.which("bash"), "bash is absent; this guard cannot check anything"
    ok, _ = _parses("echo fine\nif true; then echo yes; fi\n")
    assert ok, "a valid script was rejected -- the probe itself is broken"
    bad, err = _parses('if true; then echo "unterminated\n')
    assert not bad and err, "bash -n accepted a broken script; it is checking nothing"


# --------------------------------------------------------------------------
# The subject.
# --------------------------------------------------------------------------


def test_the_repo_has_documented_shell_to_check():
    blocks = _collect()
    assert len(blocks) >= _MIN_BLOCKS, (
        f"only {len(blocks)} shell block(s) found across tracked *.md. Either the "
        "docs lost their command blocks or the extractor stopped matching -- both "
        "would make the assertion below pass by finding nothing."
    )


def test_every_documented_shell_block_parses():
    broken = []
    for rel, line, tag, body in _collect():
        ok, err = _parses(body)
        if not ok:
            first = err.splitlines()[0] if err else "(no stderr)"
            broken.append(f"  {rel}:{line} (```{tag}) -- {first}")
    assert not broken, (
        "shell that a reader is invited to paste does not parse:\n"
        + "\n".join(broken)
        + "\n\nIf the offending text is a placeholder the reader is meant to "
        "substitute, write it as <one-word-or-phrase> with no shell "
        "metacharacters inside -- that form is recognised and skipped."
    )
