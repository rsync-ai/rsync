"""The service-to-service gate on connector deploy/generate fails closed.

``require_internal_secret`` guards ``/v1/deploy``, ``/v1/generate`` and
``DELETE /v1/connectors/{name}`` -- routes that build and start containers over
docker.sock. With ``INTERNAL_SERVICE_SECRET`` unset it used to refuse only when
``ENVIRONMENT`` said production/prod and let everything else through, so a
container started without ``ENVIRONMENT`` (a hand-written ``docker run`` or
Deployment) served those routes to any caller on the network.

Now only an explicit development/dev opens it. connector-deployer's Go gate
(``config.IsDev``) keeps the same set; ``TestAuth`` covers that side.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from src.agents.tool_generator.deployment.routes import require_internal_secret


@pytest.fixture()
def gated():
    app = FastAPI()

    @app.post("/probe", dependencies=[Depends(require_internal_secret)])
    def probe():
        return {"ok": True}

    return TestClient(app)


@pytest.mark.parametrize(
    "environment",
    [
        pytest.param(None, id="ENVIRONMENT_unset"),
        pytest.param("", id="ENVIRONMENT_empty"),
        pytest.param("production", id="production"),
        pytest.param("prod", id="prod"),
        pytest.param("staging", id="staging"),
        pytest.param("local", id="local"),
        pytest.param("prodution", id="typo"),
    ],
)
def test_an_unset_secret_is_refused_unless_environment_is_dev(
    gated, monkeypatch, environment
):
    monkeypatch.delenv("INTERNAL_SERVICE_SECRET", raising=False)
    if environment is None:
        monkeypatch.delenv("ENVIRONMENT", raising=False)
    else:
        monkeypatch.setenv("ENVIRONMENT", environment)

    res = gated.post("/probe")

    assert res.status_code == 503, res.text
    assert res.json()["detail"] == "internal_secret_not_configured"


@pytest.mark.parametrize("environment", ["development", "dev", " Development "])
def test_the_dev_compose_still_runs_without_a_secret(gated, monkeypatch, environment):
    """docker-compose.yml sets ENVIRONMENT=development and no secret by default."""
    monkeypatch.delenv("INTERNAL_SERVICE_SECRET", raising=False)
    monkeypatch.setenv("ENVIRONMENT", environment)

    res = gated.post("/probe")

    assert res.status_code == 200, res.text


def test_a_blank_secret_counts_as_unset(gated, monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "   ")
    monkeypatch.delenv("ENVIRONMENT", raising=False)

    assert gated.post("/probe").status_code == 503


@pytest.mark.parametrize("environment", ["development", "production"])
def test_a_set_secret_is_enforced_in_every_environment(gated, monkeypatch, environment):
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "FAKEPLACEHOLDER-s2s")
    monkeypatch.setenv("ENVIRONMENT", environment)

    assert gated.post("/probe").status_code == 401
    assert (
        gated.post("/probe", headers={"X-Internal-Secret": "FAKEPLACEHOLDER-wrong"}).status_code
        == 401
    )
    assert (
        gated.post("/probe", headers={"X-Internal-Secret": "FAKEPLACEHOLDER-s2s"}).status_code
        == 200
    )


# Every compose file that runs a service carrying this gate must say which side of
# it the service is on. A service block with no ENVIRONMENT now fails closed, which
# is safe but would silently break connector deploy on that stack.
_GATED_SERVICES = ("llm-service", "tool-generator", "connector-deployer")


def _compose_files():
    root = Path(__file__).resolve().parents[2]
    return sorted(root.glob("docker-compose*.yml")) + sorted((root / "deploy").glob("*.yaml"))


def _service_blocks(text):
    blocks, name, lines = {}, None, []
    for line in text.splitlines():
        m = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
        if m or re.match(r"^\S", line):
            if name:
                blocks[name] = "\n".join(lines)
            name, lines = (m.group(1) if m else None), []
            continue
        if name:
            lines.append(line)
    if name:
        blocks[name] = "\n".join(lines)
    return blocks


def test_every_compose_service_with_the_gate_sets_environment():
    seen = 0
    missing = []
    for path in _compose_files():
        for name, body in _service_blocks(path.read_text()).items():
            if name not in _GATED_SERVICES or "image:" not in body and "build:" not in body:
                continue
            seen += 1
            if not re.search(r"^\s*-?\s*ENVIRONMENT\s*[:=]", body, re.M):
                missing.append(f"{path.name}:{name}")
    # Vacuity guard: the dev and quickstart stacks each run all three, and the OSS
    # compose runs connector-deployer and tool-generator -- 8 blocks in this tree.
    assert seen >= 8, f"found only {seen} gated service blocks"
    assert not missing, (
        f"{missing} run a service whose deploy gate fails closed without "
        "ENVIRONMENT; set it explicitly (development for a dev stack)."
    )
