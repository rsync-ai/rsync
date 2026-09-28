"""A generated connector is refused where no other service could ever read it.

The Helm chart gives every pod its OWN copy of the connector catalog: an
initContainer copies the ``connector-seed`` image into a per-pod emptyDir
(deploy/helm/rsync-ai/templates/_helpers.tpl, ``rsync-ai.catalogInitContainer``).
There is no shared volume, by design -- it would need ReadWriteMany storage. So
when tool-generator saved a connector generated from an OpenAPI document (#1264),
the files landed in tool-generator's emptyDir only: the api-gateway, the
orchestrator and the temporal-adapter read their own copies and never saw it,
and the next tool-generator restart deleted it. The spec-upload screen still
said "Generated" and named the path it was saved to.

The chart now sets ``RSYNC_CONNECTOR_CATALOG_SHARED=false`` on tool-generator,
and ``DeploymentService.deploy`` -- the one writer every generation path goes
through -- refuses before it writes anything. The class is "a generated
connector saved into a catalog only its writer reads", so the guard sits on the
writer, not on one route; the route test below checks the refusal reaches the
screen as a readable message rather than as a success.

Unset (every Docker install) keeps today's behaviour, and each accepted spelling
of "false" is pinned, because a guard that only knew one spelling would let the
chart's value drift past it.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.agents.tool_generator.deployment.service import (
    ConnectorArtifacts,
    DeploymentService,
    DeploymentStatus,
)
from src.lifecycle.scaffold_routes import scaffold_router

FLAG = "RSYNC_CONNECTOR_CATALOG_SHARED"

SPEC = json.dumps(
    {
        "openapi": "3.0.0",
        "info": {"title": "Widget API", "version": "1.0"},
        "servers": [{"url": "https://api.example.com/v1"}],
        "paths": {
            "/widgets": {
                "get": {"operationId": "listWidgets", "responses": {"200": {"description": "ok"}}}
            }
        },
    }
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
def catalog(tmp_path: Path, monkeypatch) -> Path:
    (tmp_path / "public").mkdir()
    (tmp_path / "public" / "base_connector.py").write_text("# base\n")
    monkeypatch.setenv("TOOLS_DIR", str(tmp_path))
    monkeypatch.delenv("RSYNC_MANAGED_CONNECTORS", raising=False)

    async def _no_logo(self, connector_name, output_dir):
        return False

    monkeypatch.setattr(DeploymentService, "_download_logo_to_path", _no_logo)
    return tmp_path


def _written(catalog: Path) -> list[str]:
    return sorted(
        str(p.relative_to(catalog))
        for p in catalog.rglob("*")
        if p.is_file() and p.name != "base_connector.py"
    )


@pytest.mark.parametrize("value", ["false", "False", "0", "no"])
def test_a_private_catalog_refuses_before_writing(catalog, monkeypatch, value):
    monkeypatch.setenv(FLAG, value)
    result = asyncio.run(
        DeploymentService(connectors_path=str(catalog)).deploy(
            _artifacts("xkcd"), build_docker=False, start_container=False
        )
    )
    assert result.success is False
    assert result.status == DeploymentStatus.FAILED
    assert FLAG in result.error_message, result.error_message
    assert "Docker install" in result.error_message, (
        "the refusal must say where generated connectors do work: " + result.error_message
    )
    assert _written(catalog) == [], "a refused generation still wrote files"


@pytest.mark.parametrize("value", [None, "", "true", "1"])
def test_a_shared_catalog_still_saves(catalog, monkeypatch, value):
    # Control: without it, the refusal above would also pass on a writer that
    # could not write at all.
    if value is None:
        monkeypatch.delenv(FLAG, raising=False)
    else:
        monkeypatch.setenv(FLAG, value)
    result = asyncio.run(
        DeploymentService(connectors_path=str(catalog)).deploy(
            _artifacts("xkcd"), build_docker=False, start_container=False, verify=False
        )
    )
    assert result.success is True, result.error_message
    assert any(p.endswith("connector.py") for p in _written(catalog)), _written(catalog)


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("INTERNAL_SERVICE_SECRET", raising=False)
    app = FastAPI()
    app.include_router(scaffold_router, prefix="/v1")
    return TestClient(app)


def test_the_spec_upload_screen_gets_the_refusal_not_a_success(catalog, client, monkeypatch):
    monkeypatch.setenv(FLAG, "false")
    res = client.post(
        "/v1/generate",
        json={"api_name": "widget_api", "openapi_spec": SPEC, "save_artifacts": True},
    )
    body = res.json()
    # frontend/src/components/connectors/discovery/SpecUploadFlow.tsx shows
    # "Generated" on success and error_message otherwise.
    assert body["success"] is False, body
    assert FLAG in (body["error_message"] or ""), body
    assert body["output_path"] is None, "a refusal must not name a path it did not keep"
    assert _written(catalog) == []


def test_a_dry_run_is_still_allowed_on_a_private_catalog(catalog, client, monkeypatch):
    # Previewing the rendered code writes nothing, so it stays available.
    monkeypatch.setenv(FLAG, "false")
    res = client.post(
        "/v1/generate",
        json={"api_name": "widget_api", "openapi_spec": SPEC, "save_artifacts": False},
    )
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "completed_dry_run"
