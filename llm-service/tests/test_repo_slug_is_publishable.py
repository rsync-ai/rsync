"""Every command a reader can copy must name the repository they can actually clone.

The public repo is `rsync-ai/rsync`; the private one repeats the owner. Neither
this paragraph nor the rest of this file writes that private slug out, because
the gate below correctly refuses it in prose too -- see the note above EXEMPT. A
stale slug in a `git clone` line is a first-run failure with a confusing message
(GitHub 404s a private repo exactly as it 404s a missing one, so the reader
concludes the project does not exist). In `install.sh` it is worse: the script
fetches its compose files from `raw.githubusercontent.com/<slug>/main/...`, so a
wrong slug means the one-line install downloads nothing and dies partway through.

The subtle one, and the reason this is a test rather than a note in a runbook, is
`ci.yml`. Two jobs are gated on the repository they are running in. Under a slug
the repo does not have, that guard is simply FALSE forever: the jobs are skipped,
the workflow is green, and no failure anywhere indicates that a check stopped
running. A silently-skipped security job is worse than a deleted one, because the
checklist still lists it.

That cuts both ways, which is the part a naive sweep gets wrong. Rewriting those
guards to the public slug alone disables them TODAY, on the repo the work is
happening in -- observed, not theorised: the sweep this file guards turned `OSS
images are moat-free` from a passing job into a skipped one, and every check on
the pull request still reported green. So the guards name BOTH slugs for the
duration of the transition, and the test below asserts each guard admits the
public repo *and* the one this checkout actually points at.

Not everything is a defect, so the gate discriminates rather than banning the
string outright:

  * Historical permalinks (`/pull/123`, `/issues/45`, `/commit/<sha>`) point at
    the private repo's history, which the public repo will not have -- it starts
    from a fresh orphan commit with no PRs 1-882. Rewriting those would turn 950
    correct-but-inaccessible links into 950 confidently wrong ones.
  * `docs/internal/public-flip-runbook.md` is *about* the private repo. Its whole
    argument is "keep the old repo private permanently and push to a new
    repository instead", so substituting the slug there would invert the document.
"""

import json
import pathlib
import re
import subprocess

# Assembled rather than written literally, so this file does not match its own
# search and so the gate cannot be defeated by editing the pattern to miss.
OWNER = "rsync-ai"
PRIVATE_SLUG = f"{OWNER}/{OWNER}"
PUBLIC_SLUG = f"{OWNER}/rsync"

REPO = pathlib.Path(__file__).resolve().parents[2]

# Paths under the private slug that refer to history the public repo will not have.
HISTORICAL = re.compile(
    re.escape(PRIVATE_SLUG) + r"/(pull|issues|commit|actions/runs|compare|blob|tree|releases/tag)/"
)

# A CI expression of the form `contains(fromJSON('["a", "b"]'), github.repository)`.
# When it names both slugs it is the deliberate transition allowlist described
# above -- a machine-evaluated set, not a link a reader can follow -- so the gate
# strips it. An allowlist naming only the private slug is NOT stripped: that is
# the defect this file exists for, wearing a list for a hat.
ALLOWLIST = re.compile(r"contains\(fromJSON\('(\[[^']*\])'\),\s*github\.repository\)")


def _allowed_repositories(text):
    """Every guard in `text`, as a list of the repository sets each one admits.

    Both spellings count: the equality form and the allowlist form. A guard that
    is invisible to this function is a guard the tests below cannot check.
    """
    sets = [{slug} for slug in re.findall(r"github\.repository\s*==\s*'([^']+)'", text)]
    for raw in ALLOWLIST.findall(text):
        sets.append(set(json.loads(raw)))
    return sets


def _strip_transition_allowlists(line):
    def replace(match):
        slugs = set(json.loads(match.group(1)))
        return "" if {PRIVATE_SLUG, PUBLIC_SLUG} <= slugs else match.group(0)

    return ALLOWLIST.sub(replace, line)


# Files whose subject IS the private repo, mapped to the tree whose removal would
# account for the file being absent. Listed individually: an exemption should be an
# argument someone had to make, not a directory that quietly grows.
#
# This file is deliberately NOT exempt, even though it is about the slug. It has
# to survive its own gate, so its prose says "the private repo" where it means
# the literal -- if you write the literal back in for readability, CI fails, and
# that is the gate working. Note this only became observable when the file was
# committed: `git ls-files` cannot see an untracked file, so for its entire
# working-tree life this gate was blind to itself.
#
# The second element is what keeps the public cut from turning the exemption audit
# into a silent pass. `scripts/flip/excludes.txt` deletes `docs/internal` whole, so
# in the public repo this exemption correctly has no subject -- but ONLY when the
# whole tree went with it. A file that disappeared on its own, with its directory
# still standing, is the "silent hole" case and stays red.
# scripts/flip/assert-ci-split.py is the flip-day gate that asserts the public
# repo's CI has been split off the private one. Its `--repo` default names the
# private repo because that is the repo it inspects, and it never ships: the cut
# deletes `scripts/flip` whole. It landed as an untracked file and so was invisible
# to this gate for its entire working-tree life -- the same blindness recorded
# above, hit a second time by the very next file added to this tree.
EXEMPT = {
    "docs/internal/public-flip-runbook.md": "docs/internal",
    "scripts/flip/assert-ci-split.py": "scripts/flip",
}


def _tracked_files():
    """`git ls-files`, never `find` -- four generated connector dirs are untracked
    on purpose and walking the tree would sweep them in."""
    out = subprocess.run(
        ["git", "-C", str(REPO), "ls-files", "-z"],
        capture_output=True, text=True, timeout=120, check=True,
    ).stdout
    return [p for p in out.split("\0") if p]


def _offending_lines():
    offenders = []
    scanned = 0
    for rel in _tracked_files():
        if rel in EXEMPT:
            continue
        path = REPO / rel
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError, FileNotFoundError):
            continue  # binaries, symlinks into nowhere
        scanned += 1
        if PRIVATE_SLUG not in text:
            continue
        for number, line in enumerate(text.splitlines(), start=1):
            if PRIVATE_SLUG not in line:
                continue
            # A line may carry both a permalink and a live reference; strip the
            # permalinks and the transition allowlists, then see whether anything
            # is left.
            if PRIVATE_SLUG in _strip_transition_allowlists(HISTORICAL.sub("", line)):
                offenders.append((rel, number, line.strip()[:160]))
    return offenders, scanned


def test_the_census_actually_reads_the_repo():
    """Anti-vacuity. A broken `git ls-files` would make the gate below pass on nothing.

    This is the failure mode that makes slug sweeps unreliable: the check runs,
    finds zero, and reports success -- indistinguishable from a clean tree.
    """
    _, scanned = _offending_lines()
    assert scanned > 500, (
        f"Only {scanned} text files were read. The gate below cannot have checked "
        "anything meaningful; fix the enumeration rather than trusting the pass."
    )


def test_the_exemptions_still_exist_and_still_need_exempting():
    """An exemption for a file that no longer contains the string is dead weight,
    and an exemption for a file that no longer exists is a silent hole.

    The one absence this tolerates is the public cut, and it is tolerated by evidence
    rather than by assumption: the exempted file may be missing only if the tree
    `scripts/flip/excludes.txt` deletes it with is missing too. The vacuity floor for
    the gate as a whole is `test_the_census_actually_reads_the_repo` above, which
    reads 2155 files in the public repo and stays a hard assertion on both sides.
    """
    for rel, cut_with in sorted(EXEMPT.items()):
        path = REPO / rel
        if not path.exists():
            tree = REPO / cut_with
            assert not tree.exists(), (
                f"{rel} is exempted from the slug gate but no longer exists, while "
                f"{cut_with}/ is still here -- so this is a rename, not the public cut. "
                "Repoint or remove the exemption: as written it would silently cover a "
                "future file at that path."
            )
            continue
        assert PRIVATE_SLUG in path.read_text(encoding="utf-8"), (
            f"{rel} no longer mentions the private slug, so its exemption does "
            "nothing. Delete it and let the gate cover the file."
        )


def test_the_gate_can_see_a_live_reference():
    """Prove the matcher distinguishes the two cases it exists to distinguish.

    Without this, a regex that matched everything -- or nothing -- would look
    exactly like a clean sweep.
    """
    permalink = f"see https://github.com/{PRIVATE_SLUG}/pull/882 for history"
    assert PRIVATE_SLUG not in HISTORICAL.sub("", permalink), (
        "A historical permalink is being treated as a live reference; the gate "
        "would demand rewriting ~950 links into ones that cannot resolve."
    )
    live = f"git clone https://github.com/{PRIVATE_SLUG}.git"
    assert PRIVATE_SLUG in HISTORICAL.sub("", live), (
        "A clone command is NOT being flagged -- the gate is blind to the exact "
        "defect it was written for."
    )


def test_no_tracked_file_points_a_reader_at_the_private_repo():
    offenders, _ = _offending_lines()
    assert not offenders, (
        f"These lines name the private repo ({PRIVATE_SLUG}) somewhere a reader or "
        f"a CI expression will act on it. Use {PUBLIC_SLUG}:\n"
        + "\n".join(f"    {rel}:{n}: {line}" for rel, n, line in offenders)
        + "\n\n  If a line is a deliberate reference to the private repo's own "
        "history, link it as a /pull/, /issues/ or /commit/ URL. If a whole file "
        "is about the private repo, add it to EXEMPT with a reason."
    )


def test_the_installer_defaults_to_the_public_repo():
    """install.sh derives every download URL from this one variable."""
    text = (REPO / "install.sh").read_text(encoding="utf-8")
    assert f'RSYNC_REPO="${{RSYNC_REPO:-{PUBLIC_SLUG}}}"' in text, (
        "install.sh does not default RSYNC_REPO to the public slug. Every compose "
        "file it fetches is built from that value, so the one-line install would "
        "404 partway through and leave a half-configured directory behind."
    )
    assert 'RAW_BASE="https://raw.githubusercontent.com/${RSYNC_REPO}/${RSYNC_REF}"' in text, (
        "RAW_BASE no longer derives from RSYNC_REPO, so overriding the repo would "
        "silently keep downloading from the old one."
    )


def _origin_slug():
    """`owner/name` from the origin remote, or None when there is no usable one."""
    try:
        url = subprocess.run(
            ["git", "-C", str(REPO), "remote", "get-url", "origin"],
            capture_output=True, text=True, timeout=30, check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, OSError, subprocess.TimeoutExpired):
        return None
    match = re.search(r"[:/]([^/:]+)/([^/]+?)(?:\.git)?$", url)
    return f"{match.group(1)}/{match.group(2)}" if match else None


def test_the_fork_guarded_jobs_admit_the_public_repo():
    """A guard naming a repo that does not exist is false forever, and skipping is silent."""
    text = (REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    guards = _allowed_repositories(text)
    assert guards, (
        "No repository guard found in ci.yml -- neither `github.repository == '...'` "
        "nor `contains(fromJSON('[...]'), github.repository)`. If those guards were "
        "removed, delete this test deliberately; do not leave it passing on an empty "
        "list. If they were merely RESPELLED, teach _allowed_repositories the new "
        "spelling -- a guard this function cannot see is a guard nothing checks."
    )
    blind = [sorted(g) for g in guards if PUBLIC_SLUG not in g]
    assert not blind, (
        f"These ci.yml guards do not admit {PUBLIC_SLUG}: {blind}. They will "
        "evaluate FALSE on the public repo, so the jobs skip and the workflow still "
        "reports green."
    )


def test_the_fork_guarded_jobs_still_admit_the_repo_this_checkout_points_at():
    """The inverse, and the one that catches a slug sweep in the act.

    Rewriting these guards to the public slug alone is silently destructive: the
    comparison goes false on the repo the work is happening in, the jobs skip, and
    a skipped job is presented identically to a passing one. That is not
    hypothetical -- it is what the sweep this file guards actually did to `OSS
    images are moat-free`, and no check on the pull request went red.

    Scoped to the `rsync-ai` owner so a fork, whose slug legitimately appears in
    no guard, stays green.
    """
    origin = _origin_slug()
    if origin is None or not origin.startswith(f"{OWNER}/"):
        return  # a fork or a checkout with no origin: nothing to assert

    text = (REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    dead = [sorted(g) for g in _allowed_repositories(text) if origin not in g]
    assert not dead, (
        f"These ci.yml guards exclude {origin}, which is the repository this "
        f"checkout points at: {dead}. Every job behind them is skipped here and "
        "now, and skipping is silent. Keep both slugs in the allowlist until the "
        "flip is finished and the old repo is gone."
    )


def test_the_allowlist_matcher_is_not_vacuous():
    """Both halves of the transition rule, or the two tests above prove nothing.

    The stripper must let a genuine defect through: an allowlist naming ONLY the
    private repo is exactly the bug, and hiding it because it happens to be
    written as a list would be worse than not stripping at all.
    """
    both = f"""if: ${{{{ contains(fromJSON('["{PRIVATE_SLUG}", "{PUBLIC_SLUG}"]'), github.repository) }}}}"""
    assert _allowed_repositories(both) == [{PRIVATE_SLUG, PUBLIC_SLUG}], (
        "the allowlist form is not being parsed; the guard tests would silently "
        "check an empty list"
    )
    assert PRIVATE_SLUG not in _strip_transition_allowlists(both), (
        "a two-slug transition allowlist is being reported as a reader-facing link"
    )

    private_only = f"""if: ${{{{ contains(fromJSON('["{PRIVATE_SLUG}"]'), github.repository) }}}}"""
    assert PRIVATE_SLUG in _strip_transition_allowlists(private_only), (
        "an allowlist naming only the private repo is being stripped -- the gate is "
        "blind to the defect it exists for as soon as it is written as a list"
    )


def test_no_history_permalink_was_repointed_at_the_public_repo():
    """The inverse defect, and the more dangerous one.

    A sweep that replaces the slug everywhere also rewrites `/pull/184` into a
    path under the public repo. That link does not 404 forever -- the public repo
    grows its own PR numbers, so eventually it resolves to a DIFFERENT change,
    and a reader following a citation lands on unrelated work with no indication
    anything is wrong. (This test exists because exactly that happened during the
    sweep it now guards.)
    """
    pattern = re.compile(
        re.escape(PUBLIC_SLUG)
        + r"/(pull|issues|commit|actions/runs|compare|releases/tag)/[0-9a-f]"
    )
    offenders = []
    for rel in _tracked_files():
        path = REPO / rel
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError, FileNotFoundError):
            continue
        for number, line in enumerate(text.splitlines(), start=1):
            if pattern.search(line):
                offenders.append((rel, number, line.strip()[:160]))

    assert not offenders, (
        "These links cite a specific PR, issue or commit under the PUBLIC repo. "
        "That history lives in the private repo -- the public one starts from a "
        f"fresh orphan commit -- so they must stay on {PRIVATE_SLUG}:\n"
        + "\n".join(f"    {rel}:{n}: {line}" for rel, n, line in offenders)
    )


def test_that_inverse_gate_is_not_vacuous():
    """It must flag a numbered permalink and leave a live path alone."""
    pattern = re.compile(
        re.escape(PUBLIC_SLUG)
        + r"/(pull|issues|commit|actions/runs|compare|releases/tag)/[0-9a-f]"
    )
    assert pattern.search(f"https://github.com/{PUBLIC_SLUG}/pull/184"), (
        "the inverse gate cannot see a numbered permalink"
    )
    assert not pattern.search(f"https://github.com/{PUBLIC_SLUG}/issues"), (
        "the inverse gate flags the live issue tracker, which is a correct link"
    )
