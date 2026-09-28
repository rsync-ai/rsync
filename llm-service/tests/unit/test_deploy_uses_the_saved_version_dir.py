"""DeploymentService builds, verifies and removes the directory it actually saved.

The version manager saves a generated connector under
``<base>/public/<name>/versions/<v>/`` (public/ when it exists, a category
folder when the connector already lives in one). deploy() pointed the Docker
build and the verification step at ``<base>/<name>`` instead -- a directory
nothing writes -- so every generation came back PARTIAL with "Connector
directory not found" and "Deployment verification failed", and
undeploy(remove_files=True) deleted nothing. The class is "a deploy step that
joins the connector name onto the base path itself", so the build, the verify,
the reported output path and the file removal each get checked here.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from agents.tool_generator.deployment.service import (
    ConnectorArtifacts,
    DeploymentService,
    DeploymentStatus,
)


class _RecordingBuilder:
    def __init__(self):
        self.dirs: list[Path] = []

    async def build(self, connector_dir, name, version):
        connector_dir = Path(connector_dir)
        self.dirs.append(connector_dir)
        ok = (connector_dir / "Dockerfile").is_file()
        return SimpleNamespace(
            success=ok,
            image_name=f"mcp-{name}",
            image_tag=version,
            build_time_seconds=0.0,
            full_image_name=f"mcp-{name}:{version}",
            error_message=None if ok else f"Connector directory not found: {connector_dir}",
        )


def _artifacts(name: str) -> ConnectorArtifacts:
    return ConnectorArtifacts(
        name=name,
        code="def handler():\n    return 1\n",
        metadata_json=json.dumps({"id": name, "connector_type": name, "name": name}),
        requirements_txt="requests\n",
        version="latest",
    )


@pytest.fixture
def service(tmp_path: Path, monkeypatch) -> DeploymentService:
    (tmp_path / "public").mkdir()
    (tmp_path / "public" / "base_connector.py").write_text("# base\n")
    svc = DeploymentService(connectors_path=str(tmp_path))
    svc._docker_builder = _RecordingBuilder()

    async def _no_logo(self, connector_name, output_dir):
        return False

    monkeypatch.setattr(DeploymentService, "_download_logo_to_path", _no_logo)
    return svc


def test_generation_builds_and_verifies_the_saved_version_dir(service, tmp_path):
    result = asyncio.run(
        service.deploy(_artifacts("xkcd"), build_docker=True, start_container=False, verify=True)
    )

    saved = tmp_path / "public" / "xkcd" / "versions" / result.version
    assert (saved / "connector.py").is_file()
    assert result.status == DeploymentStatus.SUCCESS, result.warnings
    assert result.verified is True
    assert service.docker_builder.dirs == [saved]
    assert Path(result.output_path) == saved


def test_undeploy_removes_the_saved_connector(service, tmp_path):
    asyncio.run(service.deploy(_artifacts("xkcd"), build_docker=False, start_container=False))
    root = tmp_path / "public" / "xkcd"
    assert (root / "latest.json").is_file()

    assert asyncio.run(service.undeploy("xkcd", remove_files=True, remove_docker=False))
    assert not root.exists()
