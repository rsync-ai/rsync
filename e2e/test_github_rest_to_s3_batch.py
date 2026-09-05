"""
First GENERATED-connector → OBJECT-STORE end-to-end pipeline test.

Exercises github-rest (a tool-generator-GENERATED REST/OpenAPI connector, source)
-> aws-s3 (destination) writing to a MinIO bucket, through the full control plane:
  - api-gateway: login, create connections, create pipeline, trigger run
  - orchestrator/executor: batch extract → load, dest-type-agnostic routing
  - kafka-mcp-sink → aws-s3 MCP → MinIO: object write

Why this test exists: PR #406 proved a generated connector → *Postgres* (relational
dest). This is the companion for a **non-relational** destination — every dest
class except PG/MySQL was previously unproven from a generated source. It reuses
the same sink→aws-s3→MinIO write leg already verified for CDC
(e2e/test_db_cdc_to_local_minio.sh), so the only new surface is
"generated REST source + object-store dest in one BATCH pipeline".

Data source: the connector reads the PUBLIC, token-free GitHub endpoint
`GET https://api.github.com/licenses` (~13 rows). No credentials embedded.

Destination: a **separate, throwaway external MinIO** (`ghrest-dest-minio`) that
this test provisions itself. It must NOT be rsync-ai's internal MinIO — the
aws-s3 connector's storage-safety guard (`storage_safety.py`) rejects the
internal `minio`/`rsync-ai-minio` host as an `endpoint_url`.

Because it depends on the public GitHub API, this test is DELIBERATELY NOT part
of the deterministic merge-gate (same treatment as test_shopify_to_postgres_batch.py
and test_github_rest_to_postgres_batch.py). Run directly / nightly:

    pytest e2e/test_github_rest_to_s3_batch.py -v -s

Skips cleanly (never fails) when api-gateway or the aws-s3 MCP is down, docker is
unavailable, or GitHub is unreachable/rate-limited.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from typing import Any

import pytest
import requests


# --------------------------------------------------------------------------- #
# Configuration (all overridable via env)
# --------------------------------------------------------------------------- #

API_GATEWAY_URL = os.getenv("API_GATEWAY_URL", "http://localhost:5001")

MCP_NET = os.getenv("E2E_MCP_NET", "rsync-ai-mcp")
AWS_S3_MCP_CONTAINER = os.getenv("E2E_AWS_S3_MCP", "rsync-ai-aws-s3-v1-0-0-mcp")
S3_CONNECTOR_VERSION = os.getenv("E2E_AWS_S3_CONNECTOR_VERSION", "v1.0.0")

# Throwaway external MinIO this test stands up (NOT the internal rsync-ai-minio).
DEST_MINIO = os.getenv("E2E_DEST_MINIO", "ghrest-dest-minio")
MINIO_USER = os.getenv("E2E_DEST_MINIO_USER", "minioadmin")
MINIO_PASS = os.getenv("E2E_DEST_MINIO_PASS", "minioadmin")
BUCKET = os.getenv("E2E_DEST_BUCKET", "ghrest-dest")
# Unique prefix so re-runs never collide and cleanup is scoped.
PATH_PREFIX = f"ghrest_{uuid.uuid4().hex[:8]}"
FILE_FORMAT = os.getenv("E2E_S3_FILE_FORMAT", "jsonl")

LOGIN_EMAIL = os.getenv("E2E_USER_EMAIL", "default@rsync-ai.local")
LOGIN_PASSWORD = os.getenv("E2E_USER_PASSWORD", "password123")

GH_RESOURCE = os.getenv("E2E_GITHUB_RESOURCE", "licenses")
GH_LICENSES_URL = "https://api.github.com/licenses"

RUN_TIMEOUT_S = int(os.getenv("E2E_RUN_TIMEOUT_S", "600"))
POLL_INTERVAL_S = float(os.getenv("E2E_POLL_INTERVAL_S", "5"))

CONTROL_PLANE_DB_CONTAINER = os.getenv("E2E_CONTROL_PG_CONTAINER", "postgres")
CONTROL_PLANE_DB_USER = os.getenv("E2E_CONTROL_PG_USER", "user")
CONTROL_PLANE_DB_NAME = os.getenv("E2E_CONTROL_PG_DB", "pipeline_db")


# --------------------------------------------------------------------------- #
# Skip predicates — SKIP (never fail) on missing infra / GitHub.
# --------------------------------------------------------------------------- #


def _docker_ok() -> bool:
    try:
        subprocess.check_output(["docker", "version", "-f", "{{.Server.Version}}"],
                                text=True, stderr=subprocess.DEVNULL)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _backend_reachable() -> bool:
    try:
        return requests.get(f"{API_GATEWAY_URL}/health", timeout=3).ok
    except requests.RequestException:
        return False


def _container_running(name: str) -> bool:
    try:
        out = subprocess.check_output(
            ["docker", "inspect", "-f", "{{.State.Running}}", name],
            text=True, stderr=subprocess.DEVNULL,
        ).strip()
        return out == "true"
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _github_reachable() -> bool:
    try:
        r = requests.get(GH_LICENSES_URL, timeout=8)
        return r.status_code == 200 and isinstance(r.json(), list) and len(r.json()) >= 1
    except (requests.RequestException, ValueError):
        return False


def _skip_reason() -> str | None:
    if not _docker_ok():
        return "docker not available"
    if not _backend_reachable():
        return f"api-gateway not reachable at {API_GATEWAY_URL}"
    if not _container_running(AWS_S3_MCP_CONTAINER):
        return f"aws-s3 MCP container '{AWS_S3_MCP_CONTAINER}' not running"
    if not _github_reachable():
        return f"{GH_LICENSES_URL} not reachable / rate-limited (anonymous 60/hr)"
    return None


pytestmark = pytest.mark.skipif(_skip_reason() is not None, reason=_skip_reason() or "")


# --------------------------------------------------------------------------- #
# MinIO helpers (disposable minio/mc container on the MCP network)
# --------------------------------------------------------------------------- #


def _mc(cmd: str) -> str:
    """Run an `mc` command against the throwaway dest MinIO; return stdout."""
    full = (f"mc alias set d http://{DEST_MINIO}:9000 {MINIO_USER} {MINIO_PASS} "
            f">/dev/null 2>&1; {cmd}")
    return subprocess.check_output(
        ["docker", "run", "--rm", "--network", MCP_NET,
         "--entrypoint", "sh", "minio/mc:latest", "-c", full],
        text=True, stderr=subprocess.DEVNULL,
    )


def _count_data_objects() -> int:
    """Count data part-files the pipeline wrote under BUCKET/PATH_PREFIX/.
    Excludes the _SUCCESS manifest so we assert on the data, not just the marker."""
    try:
        out = _mc(f"mc ls --recursive d/{BUCKET}/{PATH_PREFIX}/ 2>/dev/null")
    except subprocess.CalledProcessError:
        return 0
    lines = [ln for ln in out.splitlines() if ln.strip()]
    return sum(1 for ln in lines if "_SUCCESS" not in ln and ln.rstrip().split()[-1:] != [""])


# --------------------------------------------------------------------------- #
# API helpers
# --------------------------------------------------------------------------- #


def _login() -> str:
    resp = requests.post(f"{API_GATEWAY_URL}/api/v1/auth/login",
                         json={"email": LOGIN_EMAIL, "password": LOGIN_PASSWORD}, timeout=10)
    assert resp.ok, f"login failed: {resp.status_code} {resp.text}"
    token = resp.json().get("token")
    assert token, f"no token in login response: {resp.text}"
    return token


def _create_connection(token: str, payload: dict[str, Any]) -> str:
    resp = requests.post(f"{API_GATEWAY_URL}/api/v1/connections", json=payload,
                         cookies={"auth_token": token}, timeout=20)
    assert resp.ok, f"create connection failed: {resp.status_code} {resp.text}"
    data = resp.json()
    conn_id = data.get("id") or (data.get("connection") or {}).get("id")
    assert conn_id, f"no connection id in response: {data}"
    return conn_id


def _create_pipeline(token: str, source_id: str, dest_id: str) -> str:
    payload = {
        "name": f"github-rest-{GH_RESOURCE}-to-s3-{uuid.uuid4().hex[:8]}",
        "request": f"Sync the {GH_RESOURCE} list from GitHub into S3/MinIO",
        "source_connection_id": source_id,
        "destination_connection_id": dest_id,
        "sync_mode": "batch",
        # token-free github-rest can't discover_schema (whoami 401); pre-select so
        # the executor skips discover/HITL.
        "selected_tables": [GH_RESOURCE],
    }
    resp = requests.post(f"{API_GATEWAY_URL}/api/v1/pipelines",
                         params={"allow_draft": "true"}, json=payload,
                         cookies={"auth_token": token}, timeout=30)
    assert resp.ok, f"create pipeline failed: {resp.status_code} {resp.text}"
    data = resp.json()
    pipe_id = data.get("id") or (data.get("pipeline") or {}).get("id")
    assert pipe_id, f"no pipeline id in response: {data}"
    return pipe_id


def _run_pipeline(token: str, pipe_id: str) -> dict:
    resp = requests.post(f"{API_GATEWAY_URL}/api/v1/pipelines/{pipe_id}/run",
                         params={"allow_draft": "true", "ack_warnings": "true"},
                         json={"ack_warnings": True}, cookies={"auth_token": token}, timeout=30)
    assert resp.ok, f"run pipeline failed: {resp.status_code} {resp.text}"
    return resp.json()


def _get_pipeline_status(token: str, pipe_id: str) -> dict:
    resp = requests.get(f"{API_GATEWAY_URL}/api/v1/pipelines/{pipe_id}",
                        cookies={"auth_token": token}, timeout=10)
    assert resp.ok, f"get pipeline failed: {resp.status_code} {resp.text}"
    return resp.json()


def _query_control_plane(sql: str) -> str:
    return subprocess.check_output(
        ["docker", "exec", "-i", CONTROL_PLANE_DB_CONTAINER,
         "psql", "-U", CONTROL_PLANE_DB_USER, "-d", CONTROL_PLANE_DB_NAME, "-tAc", sql],
        text=True,
    ).strip()


def _wait_for_completion(token: str, pipe_id: str) -> dict:
    """Poll the orchestrator executions table until terminal (api-gateway status
    flips to completed while executor work is still queued)."""
    terminal = {"completed", "succeeded", "failed", "error", "cancelled", "expired"}
    last_marker = None
    started = time.time()
    while time.time() - started < RUN_TIMEOUT_S:
        info = _get_pipeline_status(token, pipe_id)
        api_status = (info.get("status") or info.get("state") or "").lower()
        rows = _query_control_plane(
            f"SELECT status, end_time, error_message FROM executions "
            f"WHERE pipeline_id = '{pipe_id}' ORDER BY start_time DESC LIMIT 1;")
        if rows:
            parts = rows.split("|")
            exec_status = parts[0].lower() if parts else ""
            end_time = parts[1] if len(parts) > 1 else ""
            err = parts[2] if len(parts) > 2 else ""
        else:
            exec_status, end_time, err = "", "", ""
        marker = f"api={api_status!r} exec={exec_status!r}"
        if marker != last_marker:
            print(f"[t+{int(time.time() - started)}s] {marker}")
            last_marker = marker
        if exec_status in terminal and end_time:
            return {"status": exec_status, "end_time": end_time, "error_message": err}
        time.sleep(POLL_INTERVAL_S)
    raise AssertionError(
        f"pipeline {pipe_id} did not reach terminal within {RUN_TIMEOUT_S}s; last: {last_marker!r}")


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def dest_minio():
    """Provision a throwaway external MinIO + bucket for the destination, and
    tear it down afterwards. Kept separate from the internal rsync-ai-minio,
    which the aws-s3 connector's storage-safety guard rejects."""
    if not _container_running(DEST_MINIO):
        subprocess.run(["docker", "rm", "-f", DEST_MINIO],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.check_call(
            ["docker", "run", "-d", "--name", DEST_MINIO, "--network", MCP_NET,
             "-e", f"MINIO_ROOT_USER={MINIO_USER}", "-e", f"MINIO_ROOT_PASSWORD={MINIO_PASS}",
             "minio/minio:latest", "server", "/data"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # create the bucket (retry until MinIO is ready)
    ready = False
    for _ in range(15):
        try:
            _mc(f"mc mb -p d/{BUCKET} >/dev/null 2>&1; mc ls d >/dev/null 2>&1")
            ready = True
            break
        except subprocess.CalledProcessError:
            time.sleep(2)
    assert ready, f"external MinIO {DEST_MINIO} did not become ready"
    yield
    # Teardown: remove the throwaway MinIO entirely (drops the data with it).
    subprocess.run(["docker", "rm", "-f", DEST_MINIO],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


@pytest.fixture(scope="module")
def auth_token() -> str:
    return _login()


@pytest.fixture(scope="module")
def github_source_id(auth_token: str) -> str:
    return _create_connection(auth_token, {
        "name": f"e2e-github-rest-src-{uuid.uuid4().hex[:8]}",
        "connection_type": "source",
        "connector_type": "github-rest",
        "config": {},
        "sync_mode": "batch",
        "force_save": True,
    })


@pytest.fixture(scope="module")
def s3_dest_id(auth_token: str, dest_minio) -> str:
    return _create_connection(auth_token, {
        "name": f"e2e-s3-dest-{uuid.uuid4().hex[:8]}",
        "connection_type": "destination",
        "connector_type": "aws-s3",
        "connector_version": S3_CONNECTOR_VERSION,
        "config": {
            "bucket": BUCKET,
            "endpoint_url": f"http://{DEST_MINIO}:9000",
            "access_key_id": MINIO_USER,
            "secret_access_key": MINIO_PASS,
            "region": "us-east-1",
            "path_prefix": PATH_PREFIX,
            "file_format": FILE_FORMAT,
        },
        "force_save": True,
    })


@pytest.fixture(scope="module")
def pipeline_id(auth_token: str, github_source_id: str, s3_dest_id: str) -> str:
    return _create_pipeline(auth_token, github_source_id, s3_dest_id)


# --------------------------------------------------------------------------- #
# The test
# --------------------------------------------------------------------------- #


def test_github_licenses_arrive_in_s3(auth_token: str, pipeline_id: str) -> None:
    """Run the pipeline, wait for terminal success, assert ≥1 data object landed
    in the MinIO bucket under the run's prefix."""
    _run_pipeline(auth_token, pipeline_id)
    info = _wait_for_completion(auth_token, pipeline_id)

    status = (info.get("status") or "").lower()
    assert status in ("completed", "succeeded"), (
        f"pipeline ended non-success: {status!r} err={info.get('error_message')!r}")

    n = _count_data_objects()
    listing = ""
    try:
        listing = _mc(f"mc ls --recursive d/{BUCKET}/{PATH_PREFIX}/ 2>/dev/null")
    except subprocess.CalledProcessError:
        pass
    print(f"data objects under {BUCKET}/{PATH_PREFIX}/: {n}\n{listing}")
    assert n >= 1, (
        f"pipeline reported success but no data object exists under "
        f"{BUCKET}/{PATH_PREFIX}/ — the connector emitted rows but the object "
        f"write dropped them (or only a _SUCCESS marker was written).")
