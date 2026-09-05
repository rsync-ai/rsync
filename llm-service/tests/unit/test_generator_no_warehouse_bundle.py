"""Regression guard: the connector generator must NEVER physically bundle a
per-version ``warehouse_adapters.py`` into a generated connector's
``versions/<v>/`` dir.

Why this test exists
--------------------
``warehouse_adapters.py`` lives ONCE at ``public/warehouse_adapters.py`` and is
pulled into every connector image at build time via the Dockerfile's
``COPY --from=shared warehouse_adapters.py`` (the `shared` BuildKit named
context). A physical per-connector copy silently goes stale: the generator used
to bundle one at generation time, which froze three dead copies
(stripe/github-rest/petstore, generated 2026-06-19) still carrying the pre-fix
load-job ``mode="wb"`` bug long after the canonical file was fixed. Those copies
were dead at runtime (overwritten by ``COPY --from=shared``) but were exactly the
per-version-copy drift this repo eliminated (see the Connector Patch Sync Ledger
in CAPABILITIES-ARCHIVE.md).

This guards the fix: ``_materialize_required_helper(..., bundle=False)`` for
warehouse_adapters.py (verify-but-don't-copy), and both fallback Dockerfile
generators delegating via ``COPY --from=shared`` so a warehouse connector that
hits a fallback still passes the completeness delegation contract.
"""

from __future__ import annotations

import types
from pathlib import Path

import pytest

from agents.tool_generator.agents.session_fast_path import (
    MissingRequiredHelperError,
    _default_dockerfile,
    _materialize_required_helper,
)
from agents.tool_generator.deployment.service import ConnectorArtifacts


def _make_layout(tmp_path: Path, *, with_shared: bool) -> tuple[Path, Path]:
    """Build a public/<connector>/versions/v1.0.0 layout. When ``with_shared``,
    place the canonical shared helpers at the public/ root (one level up from the
    connector dir), mirroring the real repo."""
    public = tmp_path / "public"
    conn = public / "acme"
    versions_dir = conn / "versions" / "v1.0.0"
    versions_dir.mkdir(parents=True)
    if with_shared:
        (public / "warehouse_adapters.py").write_text("# canonical shared adapter\n")
        (public / "base_connector.py").write_text("# canonical base connector\n")
    return conn, versions_dir


def test_warehouse_adapters_is_not_bundled_into_version_dir(tmp_path: Path):
    """bundle=False: the shared file exists and is found, but is NOT copied into
    versions/<v>/ — it will be pulled at build time via COPY --from=shared."""
    conn, versions_dir = _make_layout(tmp_path, with_shared=True)

    _materialize_required_helper(
        "warehouse_adapters.py", conn, versions_dir, required=True, bundle=False,
    )

    assert not (versions_dir / "warehouse_adapters.py").exists(), (
        "generator bundled a per-version warehouse_adapters.py — this is the "
        "stale-copy drift the fix removed"
    )


def test_base_connector_is_still_bundled(tmp_path: Path):
    """Control case: base_connector.py uses the copy-in (bundle=True default)
    pattern and MUST still be physically materialized."""
    conn, versions_dir = _make_layout(tmp_path, with_shared=True)

    _materialize_required_helper(
        "base_connector.py", conn, versions_dir, required=True,
    )

    assert (versions_dir / "base_connector.py").is_file()


def test_missing_shared_warehouse_adapter_still_fails_loud(tmp_path: Path):
    """bundle=False must not weaken the fail-loud contract: a database connector
    (required=True) whose shared adapter is unreachable still raises, rather than
    silently shipping a connector that cannot import it."""
    conn, versions_dir = _make_layout(tmp_path, with_shared=False)

    with pytest.raises(MissingRequiredHelperError):
        _materialize_required_helper(
            "warehouse_adapters.py", conn, versions_dir, required=True, bundle=False,
        )
    # And still no physical copy was written.
    assert not (versions_dir / "warehouse_adapters.py").exists()


def test_optional_missing_shared_adapter_is_noop(tmp_path: Path):
    """api_saas connectors (required=False): a missing shared adapter is a no-op,
    never a write and never a raise."""
    conn, versions_dir = _make_layout(tmp_path, with_shared=False)

    _materialize_required_helper(
        "warehouse_adapters.py", conn, versions_dir, required=False, bundle=False,
    )

    assert not (versions_dir / "warehouse_adapters.py").exists()


def test_both_fallback_dockerfiles_delegate_shared_warehouse_adapter():
    """Both fallback Dockerfile generators must emit `COPY --from=shared
    warehouse_adapters.py` so a warehouse connector that hits a fallback (instead
    of Dockerfile.j2) still satisfies the completeness delegation contract
    (`_dockerfile_pulls_shared_adapter` requires both tokens) and imports the
    adapter at runtime once we no longer bundle a physical copy."""
    spec_stub = types.SimpleNamespace(
        display_name="Acme", name="acme", connector_type="acme",
        category=types.SimpleNamespace(value="data_warehouse"), version="1.0.0",
    )
    default_df = _default_dockerfile(spec_stub)
    assert "--from=shared" in default_df and "warehouse_adapters.py" in default_df

    # generate_dockerfile only reads self.name — call with a stub to decouple
    # from the ConnectorArtifacts constructor signature.
    artifacts_df = ConnectorArtifacts.generate_dockerfile(
        types.SimpleNamespace(name="acme")
    )
    assert "--from=shared" in artifacts_df and "warehouse_adapters.py" in artifacts_df
