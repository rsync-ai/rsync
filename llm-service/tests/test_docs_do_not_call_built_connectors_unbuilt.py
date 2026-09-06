"""No tracked doc may describe a connector that EXISTS as still to be built.

`docs/connectors/cloud-storage-config.md` opened with a Scope line calling `gcs`
and `azure-blob` "(to build)". Both had shipped at `v1.0.0`; both are built by
`docker-compose.mcp.yml`; and line 3 of that same file already described a
"source v1 on aws-s3/gcs/azure-blob". The file contradicted itself in eight
lines and nothing noticed, because a status claim in prose has no reader that
can disagree with it.

The cost is not cosmetic. That doc is the configuration contract a user opens
when wiring an object-storage destination, so the stale half tells someone
setting up a Mongo-to-GCS sync that the connector they are about to configure
does not exist yet.

This is the "a doc claim goes stale with nobody touching the line" shape: the
sentence was true when written and was falsified by a commit somewhere else.
Only a check that re-derives the claim from disk closes it.

Subject: markdown. So it belongs in .github/workflows/doc-links.yml and NOT in a
path-filtered ci.yml job -- ci.yml's `pull_request` ignores `**.md`, so a PR that
builds a connector and hand-edits the doc would skip this file entirely.
Registered in the GUARDS census in test_doc_link_gate_runs_on_markdown_only_prs.py.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# Phrases that assert a connector does not exist yet. Deliberately narrow: this
# guard's subject is the EXISTENCE claim, not the many true things a
# parenthetical can say about a connector that does exist ("beta", "untested",
# "source only"). Widening it is a change of subject -- measure any addition
# against the tree before making it, because a phrase that also occurs beside a
# genuinely absent connector turns this guard into noise.
UNBUILT = re.compile(
    r"\b(to build|to be built|not built|not yet built|"
    r"not yet implemented|does not exist yet|coming soon|planned)\b",
    re.I,
)


def _unbuilt_claims(line: str, connector_id: str) -> list[str]:
    """Status phrases inside the parenthetical that IMMEDIATELY follows `id`.

    The binding is the load-bearing part. A first draft searched a fixed-width
    window after the id and flagged `aws-s3` in

        `aws-s3` (exists, `v1.0.0`), `gcs` (to build), `azure-blob` (to build)

    because the window ran past the end of its own parenthetical and into the
    next connector's. A connector the line explicitly calls "exists" must never
    be reported, so the phrase has to sit in the id's OWN parenthetical.

    An unbackticked mention is prose about the service, not a claim about an
    artifact in this repo, and is deliberately not matched.
    """
    pattern = re.compile(r"`" + re.escape(connector_id) + r"`\s*\(([^)]*)\)")
    return [m.group(0) for m in pattern.finditer(line) if UNBUILT.search(m.group(1))]


def _git(*args: str) -> list[str]:
    return subprocess.run(
        ["git", "-C", str(REPO), *args],
        capture_output=True, text=True, check=True,
    ).stdout.split()


def _built_connectors() -> dict[str, str]:
    """Every TRACKED connector whose current version ships a `connector.py`.

    Discovery is `git ls-files`, not a filesystem walk, for the same reason
    test_connector_reference_matches_disk.py uses it: `iter_connector_dirs`
    rglobs and so also finds locally generated, deliberately untracked
    connectors -- four of them on a typical dev machine. A guard that sees a
    different set of connectors locally than in CI reports different findings
    in the two places, which is worse than reporting none.

    The pathspec is a single `*` on purpose: git's wildcard crosses `/`, so it
    matches BOTH shapes this tree uses -- `public/<id>` and
    `public/<category>/<id>`. A hand-rolled fixed-depth glob covers one shape
    and silently misses the other; that is how an earlier pass over this same
    tree found 10 of 21 connectors with no error anywhere.
    """
    tracked = set(_git("ls-files", "shared/mcp-connectors"))
    built: dict[str, str] = {}
    for rel in _git("ls-files", "shared/mcp-connectors/*/latest.json"):
        if "/versions/" in rel:
            continue  # a nested pointer inside a version dir is not a connector root
        directory = Path(rel).parent
        current = json.loads((REPO / rel).read_text(encoding="utf-8"))["current_version"]
        if f"{directory}/versions/{current}/connector.py" in tracked:
            built[directory.name] = current
    return built


def test_the_census_is_not_vacuous():
    """Every assertion below is quantified over this set; an empty one passes."""
    built = _built_connectors()
    assert len(built) >= 20, (
        f"connector census found only {len(built)} built connectors, which is far "
        "below this repo's count -- the discovery is broken, and every assertion "
        "in this file is vacuous while it is."
    )


def test_the_census_spans_both_directory_depths():
    """The specific way this census breaks is silent, so assert against it.

    `shared/mcp-connectors` is mixed: some connectors carry a category segment
    and some do not. A discovery that covers one depth returns a plausible,
    non-empty, entirely wrong denominator.
    """
    pointers = [p for p in _git("ls-files", "shared/mcp-connectors/*/latest.json")
                if "/versions/" not in p]
    depths = {p.count("/") for p in pointers}
    assert len(depths) > 1, (
        f"every connector pointer sits at the same depth {depths} -- either the "
        "tree stopped being mixed (delete this test deliberately) or the pathspec "
        "stopped crossing '/' and is now covering half the tree."
    )


def test_the_matcher_fires_on_the_defect_it_was_written_for():
    """Restore the exact line that was live, unmodified, under this matcher."""
    defect = (
        "`aws-s3` (exists, `v1.0.0`), `gcs` (to build), `azure-blob` (to build). "
        "These are **not**"
    )
    assert _unbuilt_claims(defect, "gcs"), "the matcher no longer sees the original defect"
    assert _unbuilt_claims(defect, "azure-blob"), "the matcher no longer sees the original defect"


def test_the_matcher_does_not_flag_a_connector_the_line_calls_present():
    """The false positive the fixed-width first draft produced, as a control."""
    defect = (
        "`aws-s3` (exists, `v1.0.0`), `gcs` (to build), `azure-blob` (to build). "
        "These are **not**"
    )
    assert not _unbuilt_claims(defect, "aws-s3"), (
        "the matcher is reading past the end of `aws-s3`'s own parenthetical into "
        "the next connector's, so it flags a connector the same line calls present"
    )


def test_no_tracked_doc_calls_a_built_connector_unbuilt():
    built = _built_connectors()
    findings = []
    for rel in _git("ls-files", "*.md"):
        path = REPO / rel
        if not path.is_file():
            continue
        for number, line in enumerate(
            path.read_text(encoding="utf-8", errors="replace").splitlines(), 1
        ):
            for connector_id, version in built.items():
                for claim in _unbuilt_claims(line, connector_id):
                    findings.append(f"  {rel}:{number}  {claim}  (ships at {version})")
    assert not findings, (
        "these docs describe a connector that exists as still to be built:\n"
        + "\n".join(sorted(findings))
        + "\n\nEither the connector was built and the doc was not updated, or the "
        "connector was removed and this census is wrong. Check the tree first."
    )
