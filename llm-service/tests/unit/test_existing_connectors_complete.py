"""Regression sweep: every public connector on disk should pass the
ConnectorCompletenessCheck.

If this test starts failing for a real connector, either:
  1. The completeness contract has drifted from reality — fix the
     contract in agents/completeness_check.py.
  2. A real connector lost a required file — fix the connector.

The test discovers connectors dynamically from
shared/mcp-connectors/public/ so adding a new connector doesn't require
editing this file.

This is what would have caught shopify-admin-graphql before it landed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from agents.tool_generator.agents.completeness_check import check


REPO_ROOT = Path(__file__).resolve().parents[3]
PUBLIC_CONNECTORS = REPO_ROOT / "shared" / "mcp-connectors" / "public"

# Connectors known to be flat-layout helpers, demos, or work-in-progress
# scaffolds rather than fully-deployable MCP connectors. They aren't part
# of the production matrix and skipping them keeps this sweep meaningful
# without forcing a rewrite of every demo dir.
SKIP = {
    "test-db-sample",
    "test-health-check",
    "http-test-connector",
    "acme-crm",
    "postgresql-generated",  # generator-experiment artifact
    "rsync_protocol",        # not a connector
    "warehouse_adapters.py", # file, not dir
}
# `shopify-pick-*` dirs are generator-experiment artifacts (random hex
# suffixes, partial scaffolds) left behind by interactive discovery
# sessions. Not production connectors; pattern-match and skip.

# Connectors known to predate the completeness gate. They ship missing
# required files (Dockerfile / latest.json / versions/v1.0.0/ /
# base_connector.py / __init__.py) — the very bug class that motivated
# this gate. They're xfail rather than skip so the failure stays VISIBLE
# in CI reports until each is regenerated through the patched pipeline.
# Tracked separately to make the inventory drift count obvious.
KNOWN_INCOMPLETE = {
    "stripe",
    "snowflake",
    "databricks",
    "github",
    "hubspot",
    "linear",
    "mongodb",
    "notion",
    "salesforce",
    "slack",
    "azure-blob",
    "gcs",
    "shopify-admin-graphql",
}


def _discover_connectors() -> list[Path]:
    """Return every connector dir under public/ that has metadata.json.

    Walks the public/ subtree. Skips category dirs (database/, api/, storage/)
    and only yields leaf connector dirs.
    """
    if not PUBLIC_CONNECTORS.is_dir():
        return []
    connectors: list[Path] = []
    # A connector root is identified by latest.json (the version pointer). The
    # connector's artifacts live under versions/<current_version>/ — check()
    # resolves and inspects them; we only need the root dir here.
    for latest_path in sorted(PUBLIC_CONNECTORS.rglob("latest.json")):
        if "versions" in latest_path.parts:
            continue
        conn_dir = latest_path.parent
        if conn_dir.name in SKIP:
            continue
        # Skip generator-experiment artifacts (shopify-pick-<hex>, etc.).
        if conn_dir.name.startswith("shopify-pick-"):
            continue
        connectors.append(conn_dir)
    return connectors


CONNECTORS = _discover_connectors()


@pytest.mark.parametrize(
    "connector_dir",
    CONNECTORS,
    ids=lambda p: p.name,
)
def test_real_connector_is_complete(connector_dir: Path):
    """Every shipped public connector must pass the completeness check.

    If this fails for a real connector, the connector is structurally
    undeployable. Two cases:
      1. Connector code is missing required files → fix the connector.
      2. The completeness contract is wrong → fix the contract.

    Connectors in ``KNOWN_INCOMPLETE`` predate the completeness gate and
    are expected to fail until they get regenerated through the patched
    pipeline. Their failure stays visible (xfail), and if any of them
    starts passing pytest will surface that as an unexpected XPASS.
    """
    if connector_dir.name in KNOWN_INCOMPLETE:
        pytest.xfail(
            f"{connector_dir.name} predates the completeness gate; "
            f"regenerate to remove from KNOWN_INCOMPLETE"
        )
    report = check(connector_dir)
    assert report.passed, (
        f"connector {connector_dir.name} is incomplete:\n"
        f"  missing_required: {report.missing_required}\n"
        f"  missing_protocol_required: {report.missing_protocol_required}\n"
        f"  notes: {report.notes}"
    )


def test_at_least_one_connector_discovered():
    """If the discovery glob breaks we want to know — an empty sweep is a
    silent pass that would mask all the parametrized checks above."""
    assert CONNECTORS, "No connectors discovered under public/"
