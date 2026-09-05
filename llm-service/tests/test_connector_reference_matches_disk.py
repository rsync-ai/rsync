"""The connector catalogue in docs/connectors/reference.md must match the tree.

That table was hand-maintained until #888 and had drifted into fiction: it
documented four connectors that do not exist (SQLite, HubSpot, Slack, Twitter/X),
omitted thirteen that do, and called AWS S3, GCS, Shopify and Stripe
one-directional when all four declare both source and destination. Nothing
caught it, because prose has no reader that can disagree with it.

Generating the table fixes the drift that existed on the day it was generated.
It does not stop the NEXT one: `docs/connectors/reference.md` is a committed file
and a connector added tomorrow leaves it stale exactly as before, silently. This
suite is the part that keeps it true -- it regenerates the block and fails if the
committed doc differs.

Subject: markdown. So it belongs in .github/workflows/doc-links.yml and NOT in a
path-filtered ci.yml job -- ci.yml's `pull_request` ignores `**.md`, so a PR that
adds a connector and updates the doc by hand would skip this file entirely.
Registered in the GUARDS census in test_doc_link_gate_runs_on_markdown_only_prs.py.
"""

from __future__ import annotations

import importlib.util
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "generate_connector_reference.py"
DOC = REPO / "docs" / "connectors" / "reference.md"

# Rows look like: | Display Name | `id` | ✅ | ✅ | — | Category |
ROW = re.compile(r"^\|\s*(?P<name>[^|]+?)\s*\|\s*`(?P<id>[^`]+)`\s*\|"
                 r"\s*(?P<src>[^|]+?)\s*\|\s*(?P<dst>[^|]+?)\s*\|"
                 r"\s*(?P<cdc>[^|]+?)\s*\|\s*(?P<cat>[^|]+?)\s*\|$", re.M)


@pytest.fixture(scope="module")
def gen():
    spec = importlib.util.spec_from_file_location("connector_reference_gen", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def block(gen):
    """The generated region as it currently stands in the committed doc."""
    text = DOC.read_text()
    start, end = text.find(gen.BEGIN), text.find(gen.END)
    assert start != -1 and end != -1, "reference.md lost its generated-block markers"
    return text[start:end + len(gen.END)]


def _tracked_ids(subtree: str) -> set[str]:
    """Connector IDs from git, which is the only discovery this doc may use.

    Not a filesystem walk: `iter_connector_dirs` rglobs and so also finds
    locally generated, deliberately untracked connectors -- four of them on a
    typical dev machine. A published catalogue describes the repository, not
    whichever laptop last ran the generator.
    """
    out = subprocess.run(
        ["git", "ls-files", f"shared/mcp-connectors/{subtree}/*/latest.json"],
        cwd=REPO, capture_output=True, text=True, check=True,
    ).stdout.split()
    root = f"shared/mcp-connectors/{subtree}/"
    return {p[len(root):-len("/latest.json")] for p in out}


def test_the_row_parser_actually_matches_rows(block):
    """Vacuity floor. Every assertion below is quantified over parsed rows, so a
    regex that silently matches nothing would make all of them pass."""
    assert len(ROW.findall(block)) >= 20, "row parser found almost nothing -- it is broken"


def test_committed_doc_is_not_stale(gen):
    """The whole point: regenerate and compare."""
    current = DOC.read_text()
    assert gen.splice(current, gen.build_block()) == current, (
        "docs/connectors/reference.md is stale -- run: make connector-reference")


def test_the_staleness_check_can_actually_fail(gen):
    """Control. A check that cannot fail looks exactly like one that passes.

    Mutate a row and confirm regeneration both notices and repairs it -- proving
    the committed block is derived from the tree rather than merely adjacent to it.
    """
    current = DOC.read_text()
    mutated = current.replace("| MySQL |", "| MySQL (hand-edited) |", 1)
    assert mutated != current, "fixture row vanished -- update this control"
    assert gen.splice(mutated, gen.build_block()) != mutated, "mutation went undetected"
    assert gen.splice(mutated, gen.build_block()) == current, "regeneration did not repair it"


def test_every_tracked_connector_is_documented(block):
    """Catches the omission half: thirteen real connectors were missing."""
    documented = {m.group("id") for m in ROW.finditer(block)}
    missing = (_tracked_ids("public") | _tracked_ids("internal")) - documented
    assert not missing, f"connectors on disk but absent from the catalogue: {sorted(missing)}"


def test_every_documented_connector_exists(block):
    """Catches the phantom half: SQLite, HubSpot, Slack and Twitter/X were listed
    for months and none of them has ever existed in this repo."""
    documented = {m.group("id") for m in ROW.finditer(block)}
    real = _tracked_ids("public") | _tracked_ids("internal")
    phantom = documented - real
    assert not phantom, f"catalogue documents connectors that do not exist: {sorted(phantom)}"


def test_cdc_column_follows_policy_not_metadata(gen, block):
    """A ✅ in the CDC column is a promise the product keeps.

    `supports_cdc` in metadata is NOT that promise -- the tools API overwrites it
    with isCDCExposed(category, id) (api-gateway/internal/handlers/tools.go:610).
    Databricks declares supports_cdc: true while being a data_warehouse with no
    registered CDC provider, so a table that copied the field would advertise a
    capability every code path refuses. Assert the published claim matches the
    policy, for the public catalogue where the policy applies.
    """
    allowlist = gen.load_cdc_allowlist()
    public = _tracked_ids("public")
    checked = 0
    for m in ROW.finditer(block):
        cid = m.group("id")
        if cid not in public:
            continue  # internal components are engine parts, not endpoints
        checked += 1
        if m.group("cdc") != gen.YES:
            continue
        leaf = cid.rsplit("/", 1)[-1].lower().replace(" ", "_").replace("-", "_")
        assert leaf in allowlist, (
            f"{cid} advertises CDC but is not in cdc_policy.debezium_supported_databases")
    assert checked >= 20, "did not actually check the public catalogue"
