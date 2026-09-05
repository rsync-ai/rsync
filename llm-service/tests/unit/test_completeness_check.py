"""Unit tests for ConnectorCompletenessCheck.

The check is the post-generation gate that asserts a connector on disk
is structurally deployable. These tests cover every failure mode that
let shopify-admin-graphql ship undeployable: missing Dockerfile, missing
latest.json, missing versions/<v>/ snapshot, missing base_connector.py.

Pure filesystem inspection — no LLM, no docker, no network. Runs in the
existing llm-service-unit CI lane.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from agents.tool_generator.agents.completeness_check import (
    CompletenessError,
    check,
    check_or_raise,
)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


REQUIRED_VERSIONED = (
    "connector.py",
    "metadata.json",
    "requirements.txt",
    "Dockerfile",
    "__init__.py",
    "base_connector.py",
)


def _make_complete_connector(
    root: Path, *, version: str = "v1.0.0", category: str = "api_saas",
    auth_methods: list[dict] | None = None,
) -> None:
    """Build a fully-complete connector layout at ``root``. Tests then
    selectively remove pieces to assert the check catches each gap.

    New contract (no root copies): only latest.json lives at the connector
    root; all other artifacts are under versions/<version>/."""
    root.mkdir(parents=True, exist_ok=True)
    versions_dir = root / "versions" / version
    versions_dir.mkdir(parents=True, exist_ok=True)

    # Root: only the version pointer.
    (root / "latest.json").write_text(json.dumps({"current_version": version}))

    # Versioned dir: all connector artifacts including category/auth metadata.
    for fname in REQUIRED_VERSIONED:
        if fname == "metadata.json":
            (versions_dir / fname).write_text(json.dumps({
                "id": root.name,
                "category": category,
                "supported_auth_methods": auth_methods or [],
            }))
        else:
            (versions_dir / fname).write_text(f"# placeholder versioned {fname}\n")


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


def test_complete_connector_passes(tmp_path: Path):
    """A fully-formed connector should pass."""
    conn = tmp_path / "stripe"
    _make_complete_connector(conn)
    report = check(conn)
    assert report.passed
    assert report.missing_required == []
    assert report.missing_protocol_required == []


def test_missing_dockerfile_fails(tmp_path: Path):
    """The original shopify-admin-graphql bug."""
    conn = tmp_path / "shopify-admin-graphql"
    _make_complete_connector(conn)
    (conn / "versions" / "v1.0.0" / "Dockerfile").unlink()
    report = check(conn)
    assert not report.passed
    assert "versions/v1.0.0/Dockerfile" in report.missing_required


def test_missing_latest_json_fails(tmp_path: Path):
    """latest.json drives the compose generator — its absence is fatal."""
    conn = tmp_path / "github"
    _make_complete_connector(conn)
    (conn / "latest.json").unlink()
    report = check(conn)
    assert not report.passed
    assert "latest.json" in report.missing_required


def test_missing_versions_dir_fails(tmp_path: Path):
    """The orchestrator's connector resolver expects versions/<v>/metadata.json."""
    conn = tmp_path / "linear"
    _make_complete_connector(conn)
    import shutil
    shutil.rmtree(conn / "versions")
    report = check(conn)
    assert not report.passed
    assert any("versions/" in m for m in report.missing_required)


def test_missing_versioned_metadata_fails(tmp_path: Path):
    """Specifically what the orchestrator's loader fails to find."""
    conn = tmp_path / "notion"
    _make_complete_connector(conn)
    (conn / "versions" / "v1.0.0" / "metadata.json").unlink()
    report = check(conn)
    assert not report.passed
    assert "versions/v1.0.0/metadata.json" in report.missing_required


def test_missing_base_connector_fails(tmp_path: Path):
    """base_connector.py is the runtime import every connector depends on."""
    conn = tmp_path / "hubspot"
    _make_complete_connector(conn)
    (conn / "versions" / "v1.0.0" / "base_connector.py").unlink()
    report = check(conn)
    assert not report.passed
    assert "versions/v1.0.0/base_connector.py" in report.missing_required


def test_database_connector_needs_warehouse_adapters(tmp_path: Path):
    """Category-conditional requirement: data warehouses need the helper."""
    conn = tmp_path / "snowflake"
    _make_complete_connector(conn, category="data_warehouse")
    # Without warehouse_adapters.py the check must fail with a
    # protocol_required-class violation, not a generic required one.
    # warehouse_adapters.py must be absent (not in REQUIRED_VERSIONED).
    report = check(conn)
    assert not report.passed
    assert "versions/v1.0.0/warehouse_adapters.py" in report.missing_protocol_required


def test_data_warehouse_may_delegate_to_shared_adapter(tmp_path: Path):
    """A data_warehouse connector need not ship a physical warehouse_adapters.py
    when it delegates to the single canonical shared adapter: the module exists
    at public/warehouse_adapters.py and the Dockerfile pulls it via the `shared`
    named context (COPY --from=shared). This is the bigquery / generator
    (connector_database.py.j2) design — a physical per-connector copy is the
    anti-drift pattern this repo deliberately removed, so it must NOT be forced.
    """
    public = tmp_path / "public"
    (public / "database").mkdir(parents=True)
    # Single canonical shared adapter at the public/ root.
    (public / "warehouse_adapters.py").write_text("# shared adapter\n")
    conn = public / "database" / "bigquery"
    _make_complete_connector(conn, category="data_warehouse")
    # Dockerfile pulls the shared adapter via the named context (overwrite the
    # fixture placeholder).
    (conn / "versions" / "v1.0.0" / "Dockerfile").write_text(
        "FROM python:3.11-slim\n"
        "COPY --from=shared warehouse_adapters.py /app/warehouse_adapters.py\n"
    )
    # No physical per-version copy exists — delegation is the only path.
    assert not (conn / "versions" / "v1.0.0" / "warehouse_adapters.py").exists()
    report = check(conn)
    assert report.passed, report.as_dict()
    assert all(
        "warehouse_adapters" not in m for m in report.missing_protocol_required
    )


def test_data_warehouse_without_dockerfile_pull_still_fails(tmp_path: Path):
    """Shared adapter present but the Dockerfile never pulls it → the connector
    cannot import it at runtime, so the completeness gate must still flag it.
    This keeps the delegation path meaningful (not a blanket exemption)."""
    public = tmp_path / "public"
    (public / "database").mkdir(parents=True)
    (public / "warehouse_adapters.py").write_text("# shared adapter\n")
    conn = public / "database" / "redshift"
    _make_complete_connector(conn, category="data_warehouse")
    # Fixture Dockerfile is a placeholder without a `--from=shared` pull.
    report = check(conn)
    assert not report.passed
    assert "versions/v1.0.0/warehouse_adapters.py" in report.missing_protocol_required


def test_api_saas_connector_does_not_need_warehouse_adapters(tmp_path: Path):
    """API/SaaS connectors should NOT require warehouse_adapters.py."""
    conn = tmp_path / "stripe"
    _make_complete_connector(conn, category="api_saas")
    report = check(conn)
    assert report.passed
    # warehouse_adapters absence shouldn't even surface as protocol_required.
    assert all("warehouse_adapters" not in m for m in report.missing_protocol_required)


def test_oauth_metadata_without_oauth_dir_only_warns(tmp_path: Path):
    """OAuth connectors get a note when oauth/ is absent but still pass."""
    conn = tmp_path / "salesforce"
    _make_complete_connector(
        conn,
        auth_methods=[{"method": "oauth2"}],
    )
    report = check(conn)
    assert report.passed  # still passes
    assert any("oauth" in n.lower() for n in report.notes)


def test_recommended_files_missing_does_not_fail(tmp_path: Path):
    """spec.json / mock_profile.json absent is a warning, not a fail."""
    conn = tmp_path / "slack"
    _make_complete_connector(conn)
    # spec.json and mock_profile.json never created by the fixture.
    report = check(conn)
    assert report.passed
    assert "spec.json" in report.missing_recommended
    assert "mock_profile.json" in report.missing_recommended


def test_corrupt_latest_json_falls_back_to_default_version(tmp_path: Path):
    """A corrupt latest.json must not crash the checker — it should fall
    back to v1.0.0 and continue."""
    conn = tmp_path / "stripe"
    _make_complete_connector(conn)
    (conn / "latest.json").write_text("{not json")
    # The check still runs; version inference defaults to v1.0.0 which
    # matches the directory we created.
    report = check(conn)
    # latest.json itself is "present" so it's not in missing_required,
    # but it's still corrupt; the resolver uses the default version.
    assert report.version == "v1.0.0"


def test_check_or_raise_raises_on_incomplete(tmp_path: Path):
    """The assert-style entrypoint must raise CompletenessError."""
    conn = tmp_path / "broken"
    _make_complete_connector(conn)
    (conn / "versions" / "v1.0.0" / "Dockerfile").unlink()
    (conn / "latest.json").unlink()
    with pytest.raises(CompletenessError, match="Dockerfile"):
        check_or_raise(conn)


def test_check_or_raise_returns_report_on_pass(tmp_path: Path):
    """On success, check_or_raise returns the report (for warning logs)."""
    conn = tmp_path / "stripe"
    _make_complete_connector(conn)
    report = check_or_raise(conn)
    assert report.passed
    assert report.connector_dir == conn.resolve()


def test_missing_connector_dir_returns_failure(tmp_path: Path):
    """A path that doesn't exist returns a failed report (no crash)."""
    report = check(tmp_path / "does-not-exist")
    assert not report.passed


def test_resolves_version_from_latest_json(tmp_path: Path):
    """When latest.json declares a non-default version, the check looks
    under versions/<that-version>/ rather than versions/v1.0.0/."""
    conn = tmp_path / "postgresql"
    # _make_complete_connector with version="v1.0.14" already places files
    # under versions/v1.0.14/ AND writes latest.json with that version.
    _make_complete_connector(conn, version="v1.0.14")
    report = check(conn)
    assert report.version == "v1.0.14"
    assert report.passed
