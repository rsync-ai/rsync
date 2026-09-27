"""The kafka-mcp-sink must be able to reach the minio connector on a Helm install.

Batches too large to travel inline are claim-checked to MinIO, and the sink
fetches them back through the minio connector's MCP endpoint. The sink worker
dialled the literal `http://minio-mcp:8000/mcp`, which is the Compose service
name; the chart names that Service `<stackPrefix>-minio-v1-0-0-mcp`. On GKE
`rsync-v016` (2026-09-27) every 10,000-row batch was dead-lettered with
`lookup minio-mcp ... no such host` while 2,000-row inline batches landed
(#1245). The worker now reads MINIO_MCP_URL; this
checks the chart sets it to a host that is a rendered Service.

This proves the rendered manifests, not a live install.
"""

import pathlib
import shutil
import subprocess
from urllib.parse import urlparse

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
CHART = REPO / "deploy" / "helm" / "rsync-ai"

RENDER_FLAGS = [
    "--set", "secrets.jwtSecret=FAKEPLACEHOLDER",
    "--set", "secrets.encryptionKey=FAKEPLACEHOLDER",
    "--set", "secrets.postgresPassword=FAKEPLACEHOLDER",
    "--set", "secrets.minioAccessKey=FAKEPLACEHOLDER",
    "--set", "secrets.minioSecretKey=FAKEPLACEHOLDER",
    "--set", "frontend.publicUrl=https://app.example.com",
    "--set", "frontend.apiUrl=https://api.example.com",
]

SINK = "kafka-mcp-sink"
WORKER_DEFAULT_HOST = "minio-mcp"


def _render(*extra):
    proc = subprocess.run(
        ["helm", "template", "r", str(CHART), *RENDER_FLAGS, *extra],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(REPO),
    )
    assert proc.returncode == 0, f"helm template failed:\n{proc.stderr[-3000:]}"
    docs = [d for d in yaml.safe_load_all(proc.stdout) if d]
    assert len(docs) >= 20, f"only {len(docs)} documents rendered"
    return docs


def _sink_env(docs):
    for d in docs:
        if d.get("kind") == "Deployment":
            for c in d["spec"]["template"]["spec"]["containers"]:
                if c["name"] == SINK:
                    return {e["name"]: e.get("value") for e in c.get("env", [])}
    pytest.fail(f"no {SINK} container rendered")


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_sink_minio_mcp_url_names_a_rendered_service():
    docs = _render()
    services = {d["metadata"]["name"] for d in docs if d.get("kind") == "Service"}
    # The control: the worker's Compose default must NOT be a Service here,
    # otherwise the assertion below could pass on a chart that sets nothing.
    assert WORKER_DEFAULT_HOST not in services

    url = _sink_env(docs).get("MINIO_MCP_URL")
    assert url, (
        f"{SINK} has no MINIO_MCP_URL, so the worker dials '{WORKER_DEFAULT_HOST}', "
        "which no Service carries, and every claim-checked batch is dead-lettered"
    )
    parsed = urlparse(url)
    assert parsed.hostname in services, f"MINIO_MCP_URL={url!r} names no rendered Service"
    assert parsed.port == 8000 and parsed.path == "/mcp", url


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_sink_gets_no_minio_url_when_the_minio_connector_is_off():
    env = _sink_env(_render("--set", "connectors.minio.enabled=false"))
    assert "MINIO_MCP_URL" not in env, "MINIO_MCP_URL points at a Service that is not rendered"
