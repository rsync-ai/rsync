"""This repo ships no bundled observability vendor, and nothing in it may say otherwise.

The self-hosted stack emits OpenTelemetry over OTLP and writes to `docker logs`.
It does not deploy an observability backend, and the operator brings their own.
That decision was executed by deleting a vendor's compose stack, its ClickHouse
config tree, its dashboards, two setup scripts, a Go handler, a React button and
the prose around all of it -- 4223 deleted lines across 111 files.

A deletion that large is not the risk. The risk is the next commit. The name is
still in this repository's own history, so a revert, a cherry-pick from a branch
cut before this landed, or a doc paragraph pasted back from an old review puts it
right back -- and puts it back as a reference to files that no longer exist. A
reader following it gets a `docker-compose.yaml` that is not in the tree; a build
following it gets an import that does not resolve.

Nothing else in CI catches that shape. `check-doc-links.sh` resolves markdown
links, so it catches a link to a deleted path but not a paragraph naming the
product in prose, and not a Go or TypeScript symbol. The Go and frontend builds
catch a dangling import but say nothing about documentation. So the check is a
census: every tracked file, name and content, case-insensitive.

## Why this file is in the doc-guards job and not `llm-service-unit`

Its subject is the whole tree, so no paths filter can cover it -- a paths filter
is a list of what a PR touched, and this guard exists for the PR that touches
the one file nobody thought to list. `.github/workflows/doc-links.yml` is the
only workflow with no paths filter in either direction, which is why its own
header says a guard whose subject can be falsified by a commit touching nothing
the filter names belongs there. A skipped job reads exactly like a passing one,
and this guard skipping is indistinguishable from this guard finding nothing.

## Why the file is not named after what it forbids

Naming it `test_no_<vendor>...py` would put the vendor's name into every place
that names the file -- starting with the `pytest` argument list in
`doc-links.yml`, which this check then has to exempt, which then exempts every
other line of that workflow too. The guard caught that on its first run. The
name it forbids appears in exactly one file, this one.

## The exemptions, and why a stale exemption fails

Migration 100 renames three `sentinel_config` rows off the vendor's name. It has
to name the old keys to rename them: that is what a rename is. That, and this
file. Both are allowlisted by path, and the allowlist is checked in both
directions -- an entry naming a file which does not exist, or which no longer
matches, fails. An exemption that has outlived its subject is a hole, not a pass.

## Vacuity

Three floors, because a census that scans nothing reports zero offences and
looks identical to a clean tree. The regex is armed against a synthetic string
so a broken pattern cannot report zero; the file count and the scanned byte
total are floored so an empty or failed enumeration cannot; and the allowlist
above doubles as a live positive control through real repository bytes.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

NEEDLE = re.compile(r"signoz", re.IGNORECASE)

# Paths allowed to name the retired vendor, each with the reason it must.
# Checked in both directions -- see the module docstring.
ALLOWED = {
    "api-gateway/migrations/100_sentinel_config_backend_neutral_keys.sql":
        "renames the legacy sentinel_config keys, so it must name them",
    "llm-service/tests/test_no_bundled_observability_vendor.py":
        "this guard; it has to spell what it forbids",
}

# Measured 2026-09-08: 2179 tracked files, ~2170 of them text. The floor is well
# under that -- it exists to catch an enumeration that returned nothing, not to
# track the repo's size.
MIN_TRACKED_FILES = 1500
MIN_SCANNED_BYTES = 5_000_000


def _tracked_files() -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(REPO), "ls-files", "-z"],
        capture_output=True, text=True, timeout=120, check=True,
    ).stdout
    return [rel for rel in out.split("\0") if rel]


def _scan() -> tuple[list[str], int, int]:
    """Return (paths whose content matches, files read, bytes read)."""
    hits: list[str] = []
    files = 0
    total = 0
    for rel in _tracked_files():
        path = REPO / rel
        try:
            raw = path.read_bytes()
        except (OSError, ValueError):
            continue
        if b"\0" in raw[:8192]:  # binary; a logo or a font cannot say anything
            continue
        files += 1
        total += len(raw)
        if NEEDLE.search(raw.decode("utf-8", errors="ignore")):
            hits.append(rel)
    return hits, files, total


def test_no_tracked_path_is_named_after_the_retired_vendor() -> None:
    named = [rel for rel in _tracked_files() if NEEDLE.search(rel)]
    assert named == [], (
        "These paths are named after an observability backend this repo does not "
        "ship, so anything referring to them refers to nothing:\n  "
        + "\n  ".join(named)
    )


def test_no_tracked_file_mentions_the_retired_vendor() -> None:
    hits, _, _ = _scan()
    unexpected = [rel for rel in hits if rel not in ALLOWED]
    assert unexpected == [], (
        "This repo ships no bundled observability backend. These files name one, "
        "which points a reader (or a build) at files that are not in this tree:\n  "
        + "\n  ".join(unexpected)
        + "\n\nIf a file genuinely has to name it -- a migration renaming legacy "
          "keys is the only case so far -- add it to ALLOWED with the reason."
    )


def test_every_exemption_still_has_a_subject() -> None:
    """A stale exemption is a hole. It must name a file that exists and matches."""
    hits, _, _ = _scan()
    for rel, reason in sorted(ALLOWED.items()):
        assert (REPO / rel).is_file(), (
            f"ALLOWED names {rel} ({reason}) but that file is not in the tree. "
            "Delete the entry -- an exemption for a file nobody ships exempts "
            "whatever is added at that path next."
        )
        assert rel in hits, (
            f"ALLOWED exempts {rel} ({reason}) but it no longer names the vendor. "
            "Delete the entry. Until it is deleted, this path is silently exempt."
        )


def test_the_census_is_not_vacuous() -> None:
    assert NEEDLE.search("otel-collector, not SigNoz, ships here"), (
        "The pattern matches nothing, so every result above is a false pass."
    )
    _, files, total = _scan()
    assert files >= MIN_TRACKED_FILES, (
        f"Scanned {files} files, expected at least {MIN_TRACKED_FILES}. The "
        "enumeration is broken, and a broken enumeration reports zero offences."
    )
    assert total >= MIN_SCANNED_BYTES, (
        f"Scanned {total} bytes, expected at least {MIN_SCANNED_BYTES}. Files "
        "were listed but not read."
    )
