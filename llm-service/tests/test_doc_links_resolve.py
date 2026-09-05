"""Every relative markdown link in the top-level docs must point at something real.

Five links in CAPABILITIES.md rotted without anyone noticing (found 2026-08-19).
They rotted in three distinct ways, and the mix is the reason a guard is worth
having -- no single habit would have caught all of them:

  * two pointed at connector **root copies**, deleted repo-wide by `6e1aa12d`
    (#184) when `versions/<current_version>/` became the single source; one of
    those also pinned `v1.0.14`, a version consolidated away by `bf72cc8c` (#94);
  * one pointed at `chat_temporal_v2.go`, a 287-LoC handler deliberately deleted
    by `a1e33ced` (#68) two days after the row citing it was written;
  * two -- `validators/url_validator.go` and `workers/intent_worker.go` -- had
    **never existed in this repo at any commit**. They were plausible-looking
    paths for real code that lives elsewhere (`base_connector.py::_ssrf_check_url`
    and `workers/intent.go`). A stale-link check that only looked for *deletions*
    would have missed both.

The link text is not checked, only the target: a link may legitimately name a
symbol or a line range that drifts. What must hold is that the file is there.

Line anchors (`path.py:123`) are stripped before the existence check for the same
reason -- line numbers drift constantly and asserting them would make this test a
maintenance tax rather than a guard. Anchors are still worth getting right by
hand; they are simply not what this file defends.

THE PUBLIC CUT. Three of the four pinned docs (CAPABILITIES.md, CAPABILITIES-ARCHIVE.md,
INVENTORY.md) are on `scripts/flip/excludes.txt`; ARCHITECTURE.md is not. So this file has
a mixed subject and stays, running, in the public repo -- a rotted link in ARCHITECTURE.md
is a public-facing defect and this is the only thing that catches it.

The vacuity floor is what needed rethinking, and it is the delicate part. `>= 200 links
across DOCS` was a single number covering four documents, so under the cut it could only
be met by documents that no longer exist -- and lowering it to something 110 surviving
docs can meet would have made it meet-able by a BROKEN parser too, which is the exact
thing it was written to detect. It is now a floor PER DOCUMENT (`MIN_LINKS`), skipped for
the documents that are absent, so each surviving document still has to yield a plausible
number of links on its own. `test_at_least_one_pinned_doc_is_present` is the denominator
underneath that: if every pinned doc vanished, the per-doc floors would all skip and the
existence sweep would run over an empty list, which is the failure mode this whole file
exists to avoid.
"""

import os

import pytest

from doclinks import extract_targets, normalize_target, repo_path_targets

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# The docs CLAUDE.md designates as load-bearing. Pinned rather than globbed: a
# glob cannot tell "this doc was renamed" apart from "this doc stopped being
# scanned", and a scan that silently covers nothing passes every assertion.
DOCS = ("CAPABILITIES.md", "CAPABILITIES-ARCHIVE.md", "INVENTORY.md", "ARCHITECTURE.md")

# Per-document vacuity floor. Measured 2026-09-02: 2133 / 1122 / 1083 / 18 links
# respectively in the private repo, and 10 in ARCHITECTURE.md after the public cut's
# de-linking pass (scripts/flip/delink-docs.sh strips links whose target it deletes).
# Each floor is far below the real count and far above zero: enough to catch a parser
# that stopped matching, loose enough that ordinary editing never trips it.
MIN_LINKS = {
    "CAPABILITIES.md": 100,
    "CAPABILITIES-ARCHIVE.md": 100,
    "INVENTORY.md": 100,
    "ARCHITECTURE.md": 5,
}

PRESENT_DOCS = tuple(d for d in DOCS if os.path.isfile(os.path.join(REPO_ROOT, d)))


def _repo_links(doc):
    path = os.path.join(REPO_ROOT, doc)
    if not os.path.isfile(path):
        return []
    with open(path) as fh:
        text = fh.read()
    return [(doc, t) for t in repo_path_targets(text)]


ALL_LINKS = [lk for d in DOCS for lk in _repo_links(d)]


def test_at_least_one_pinned_doc_is_present():
    """The denominator under every skip in this file.

    ARCHITECTURE.md survives the public cut, so this holds in both repos. If DOCS ever
    reduces to nothing on disk, the per-doc floors below all skip and the existence
    sweep runs over an empty parametrization -- a green file that checked nothing,
    which is the shape this guard was written against.
    """
    assert PRESENT_DOCS, (
        f"none of {DOCS} exists under {REPO_ROOT}. Every assertion in this file is "
        "now passing on an empty set. Repoint DOCS at the docs that replaced them."
    )


@pytest.mark.parametrize("doc", DOCS)
def test_the_link_scan_actually_finds_links(doc):
    """Vacuity floor, per document. A parser that extracts nothing passes every case
    below it, and a single repo-wide total could be met by one large document while
    another silently contributed zero."""
    if doc not in PRESENT_DOCS:
        pytest.skip(f"{doc} is not in this tree (removed by scripts/flip/excludes.txt)")
    found = [t for d, t in ALL_LINKS if d == doc]
    assert len(found) >= MIN_LINKS[doc], (
        f"only extracted {len(found)} relative links from {doc}, expected at least "
        f"{MIN_LINKS[doc]} -- the parser is broken, and every assertion below it is "
        "passing on an empty set"
    )


def test_the_parser_handles_parenthesised_paths():
    """Regression for the false positive that made the ad-hoc grep untrustworthy."""
    got = extract_targets("see [x](frontend/src/app/(dashboard)/page.tsx:120) ok")
    assert got == ["frontend/src/app/(dashboard)/page.tsx:120"], got


# The pins below belong to `doclinks`, not to this file's own assertions. They live
# here because this is where the parser was written and correct; the second copy that
# `test_doc_merge_claims_are_true.py` used to carry was the one that drifted, and it
# drifted precisely in the cases nothing pinned.
@pytest.mark.parametrize(
    "raw,want",
    [
        # the shape that let CAPABILITIES.md:886 through the merge guard
        ("docs/explorer/saved-queries-and-models.md#6-version-history-diff-and-restore",
         "docs/explorer/saved-queries-and-models.md"),
        ("api-gateway/internal/handlers/saved_queries.go#L114-L119",
         "api-gateway/internal/handlers/saved_queries.go"),
        ("api-gateway/cmd/server/main.go:1199", "api-gateway/cmd/server/main.go"),
        ("api-gateway/cmd/server/main.go:114-119", "api-gateway/cmd/server/main.go"),
        ("api-gateway/cmd/server/main.go:114–119", "api-gateway/cmd/server/main.go"),  # en dash
        ("frontend/src/app/(dashboard)/page.tsx:120", "frontend/src/app/(dashboard)/page.tsx"),
        # not repo paths
        ("https://github.com/rsync-ai/rsync", None),
        ("#a-heading-in-this-file", None),
        ("", None),
    ],
)
def test_normalize_target_strips_fragment_and_line_anchor(raw, want):
    """Fragment first, then line anchor, then nothing else touches the extension.

    A caller that classifies by suffix -- "is this a `.md`, so does linking it prove
    anything about a merge?" -- gets the wrong answer for every anchored link unless
    both are stripped first. That mis-classification is the whole bug this guard pair
    was missing.
    """
    assert normalize_target(raw) == want


@pytest.mark.parametrize("doc,target", ALL_LINKS, ids=lambda v: v)
def test_every_relative_doc_link_exists(doc, target):
    assert os.path.exists(os.path.join(REPO_ROOT, target)), (
        f"{doc} links to {target}, which does not exist.\n"
        "If a connector artifact: there are NO root copies -- point at "
        "versions/<current_version>/ (current_version comes from latest.json, "
        "not the highest dir). If the file was deleted, check whether the claim "
        "the link supports is still true before repointing it; per CLAUDE.md "
        "gate 3, trust the code and fix the doc."
    )
