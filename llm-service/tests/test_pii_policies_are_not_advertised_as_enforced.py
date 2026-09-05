"""`pii_policies` is a table with no reader. The docs must keep saying so.

The four `/pii/policies` endpoints read and write `pii_policies`, and nothing in
the pipeline runtime ever reads it back. A policy created through that API changes
no pipeline's behaviour: it is a row. Masking that actually happens is the
`mask_pii` transform in `shared/go/transforms`, configured per pipeline, which
does not consult this table.

Until #887 the product said otherwise -- the UI offered "Configure how different
PII types are handled" under a green padlock counting "Active Policies", and the
API reference listed the endpoints with no caveat. Nothing was broken, so nothing
failed; an operator reading the page would simply have believed they had a
control they did not have.

This file makes the state of the world and the claim about it fail together. It
asserts BOTH directions, which is the point:

  * no unexpected reader has appeared -- so the caveat is still true, and
  * the caveat is still present -- so the truth is still being told.

Wire the table up and the first assertion fails, forcing the docs to be corrected
in the same change. Delete the caveat and the second fails. Neither half can rot
quietly, which is what went wrong the first time.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

TABLE = "pii_policies"

# The identifier standing alone -- not embedded in a longer one. Without the
# lookarounds this file fails on its OWN name: wiring it into doc-links.yml puts
# the string `test_pii_policies_are_not_advertised_as_enforced.py` in a workflow,
# and a bare substring search reads that as a new reader of the table. Excluding
# it structurally beats adding the workflow to ALLOWED, which would also stop the
# check noticing a genuine reference there.
IDENTIFIER = re.compile(r"(?<![A-Za-z0-9_])" + TABLE + r"(?![A-Za-z0-9_])")

# Where the table is legitimately named. Anything else mentioning it is either a
# new reader (the interesting case) or a new doc claim that needs review.
#
# Prefixes, matched against the repo-relative POSIX path.
ALLOWED = (
    "api-gateway/internal/handlers/",   # the CRUD surface itself
    "api-gateway/migrations/",          # 009 creates it, 013/077/078 amend it
    "llm-service/tests/",               # this file
    "docs/",                            # the caveat lives here
    "INVENTORY.md",
    "CAPABILITIES.md",
    "BACKLOG.md",
    "PRODUCT_STATUS.md",
)

# The caveat, wherever it must appear. Matched on substance rather than exact
# wording so an editor can rephrase, but not so loosely that deleting the point
# still passes.
CAVEAT_REQUIRED = {
    "INVENTORY.md": re.compile(r"`pii_policies` is not enforced", re.I),
    "docs/api/README.md": re.compile(
        r"stores policies; it does not enforce them", re.I
    ),
}


def _tracked() -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(REPO), "ls-files", "-z"],
        capture_output=True, text=True, timeout=120, check=True,
    ).stdout
    return [p for p in out.split("\0") if p]


def _files_naming_the_table() -> list[str]:
    hits = []
    for rel in _tracked():
        path = REPO / rel
        try:
            if IDENTIFIER.search(path.read_text(encoding="utf-8", errors="ignore")):
                hits.append(rel)
        except (OSError, UnicodeDecodeError):
            continue
    return hits


def test_the_census_finds_the_table_at_all():
    """Anti-vacuity: every assertion below passes on an empty census."""
    hits = _files_naming_the_table()
    assert len(hits) >= 5, (
        f"only {len(hits)} tracked files mention {TABLE!r}; the scan is broken or the "
        f"table was renamed. Retarget this file rather than deleting it -- a zero "
        f"census makes the enforcement check below pass while proving nothing."
    )
    assert "api-gateway/internal/handlers/pii.go" in hits, (
        "the CRUD handler no longer names the table; retarget this test"
    )


def test_the_identifier_matcher_discriminates():
    """The lookarounds are load-bearing; assert them rather than assume them."""
    assert IDENTIFIER.search("SELECT * FROM pii_policies WHERE id = $1")
    assert IDENTIFIER.search("`pii_policies` is not enforced")
    assert IDENTIFIER.search("CREATE TABLE pii_policies (")
    # ...but not this file's own name, which is how the check first failed.
    assert not IDENTIFIER.search("tests/test_pii_policies_are_not_advertised_as_enforced.py")
    assert not IDENTIFIER.search("pii_policies_v2")
    assert not IDENTIFIER.search("legacy_pii_policies")


def test_no_runtime_reader_has_appeared():
    """The claim `pii_policies` is unenforced must still be TRUE."""
    unexpected = [
        rel for rel in _files_naming_the_table()
        if not rel.startswith(ALLOWED) and rel not in ALLOWED
    ]
    assert not unexpected, (
        f"{TABLE} is now named outside the CRUD handlers, migrations and docs:\n  "
        + "\n  ".join(unexpected)
        + f"\n\nIf you have wired {TABLE} into the runtime, that is good news -- but the "
        f"docs currently tell operators the table is inert. Update INVENTORY.md, "
        f"docs/api/README.md and the PII page's copy in the SAME change, then add the "
        f"new path to ALLOWED here."
    )


def test_at_least_one_caveat_site_is_present():
    """Denominator for the per-case skip below.

    `INVENTORY.md` is removed by scripts/flip/excludes.txt:80 and `docs/api/README.md`
    ships, so on the public tree exactly one of the two cases below skips. Nothing must
    make BOTH skip -- that is the shape where this guard goes quiet while still
    reporting green, so it is asserted here rather than left implicit in two skips.
    """
    present = [rel for rel in CAVEAT_REQUIRED if (REPO / rel).exists()]
    assert present, (
        f"none of {sorted(CAVEAT_REQUIRED)} exist, so every case below would skip and "
        f"this suite would guard nothing. Retarget CAVEAT_REQUIRED at whatever document "
        f"now tells operators that {TABLE} is stored but not enforced."
    )


@pytest.mark.parametrize("rel,pattern", sorted(CAVEAT_REQUIRED.items()))
def test_the_caveat_is_still_stated(rel, pattern):
    """The claim must still be MADE, in the places an operator reads."""
    path = REPO / rel
    if not path.exists():
        # INVENTORY.md is one of these and the public cut removes it. Skipping the
        # one case keeps the other -- the operator-facing API doc, which ships --
        # guarded. test_at_least_one_caveat_site_is_present is the floor.
        pytest.skip(f"{rel} is absent from this tree (scripts/flip/excludes.txt)")
    text = path.read_text(encoding="utf-8")
    assert pattern.search(text), (
        f"{rel} no longer states that {TABLE} is stored but not enforced "
        f"(looked for {pattern.pattern!r}).\n"
        f"If the table became enforced, delete this expectation and the ALLOWED entry "
        f"together. If it did not, put the caveat back -- an operator reading this file "
        f"will otherwise believe they have a masking control that does not exist."
    )


def test_the_ui_does_not_promise_enforcement():
    """The PII page must not offer to "configure how PII is handled"."""
    page = REPO / "frontend/src/app/(dashboard)/pii/page.tsx"
    assert page.exists(), "the PII page moved; retarget this test"
    text = page.read_text(encoding="utf-8")

    assert "Configure how different PII types are handled" not in text, (
        "the PII page again describes the policies tab as configuring how PII is "
        "handled. It does not: the rows are stored and never read."
    )
    # The honest replacement, and the pointer to the thing that does work.
    assert "not enforced by the pipeline runtime" in text, (
        "the PII policies card no longer tells the operator the policies are inert"
    )
    assert "mask_pii" in text, (
        "the PII page no longer points at the transform that actually masks; without "
        "it the caveat is a dead end rather than a redirect"
    )
    # "Active" was the word doing the damage in the stat card and the row badge.
    assert "Active Policies" not in text, (
        'the stat card again counts "Active Policies"; they are stored, not active'
    )
