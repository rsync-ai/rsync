"""
First GENERATED-connector → DB end-to-end pipeline test in the repo.

Exercises github-rest (a tool-generator-GENERATED REST/OpenAPI connector, source)
-> PostgreSQL (destination) through the full control plane:
  - api-gateway: login, create connections, create pipeline, trigger run
  - orchestrator: Preflight spawns the github-rest MCP container on the
    `rsync-ai-mcp` docker network (no static docker-compose.mcp.yml entry needed)
  - planner/executor: batch extract -> load through the standard sink
  - postgres destination: receives the rows

Why this test exists: github-rest -> Postgres (PR #270) is the first — and long
the ONLY durable — proof that a *generated* connector can drive a real pipeline
to a destination. It guards the SaaS/REST -> DB batch seam, in particular the
executor's `records`-key extraction contract (executor.go) that once silently
dropped every row from ApiHandler connectors. This turns that manual proof into
a re-runnable regression test.

Data source: the connector reads the PUBLIC, token-free GitHub endpoint
`GET https://api.github.com/licenses` (~13 rows). No credentials are needed or
embedded. Because it depends on the public GitHub API, this test is DELIBERATELY
NOT part of the deterministic merge-gate (e2e/run_gate.sh) — same treatment as
test_shopify_to_postgres_batch.py. Run it directly or on a schedule:

    # against the e2e gate stack (defaults below):
    pytest e2e/test_github_rest_to_postgres_batch.py -v -s

    # against a local dev stack (override the dest container/creds):
    E2E_PG_CONTAINER=postgres E2E_PG_USER=user E2E_PG_DB=<db> \
    E2E_PG_PASSWORD=<pw> E2E_PG_HOST_FOR_GATEWAY=postgres \
    pytest e2e/test_github_rest_to_postgres_batch.py -v -s

Skips cleanly (never fails) when: api-gateway is unreachable, the dest Postgres
container isn't running, or GitHub is unreachable / rate-limited (anonymous API
is 60 req/hr/IP — a shared runner may hit that; a skip is correct, not a failure).
"""

from __future__ import annotations

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

# Destination Postgres. Defaults match the e2e gate stack; override for a local
# dev stack (see module docstring).
PG_CONTAINER = os.getenv("E2E_PG_CONTAINER", "rsync-ai-postgres-e2e")
PG_USER = os.getenv("E2E_PG_USER", "e2e_user")
PG_DB = os.getenv("E2E_PG_DB", "e2e_db")
PG_PASSWORD = os.getenv("E2E_PG_PASSWORD", "e2e_password")
PG_HOST_FOR_GATEWAY = os.getenv("E2E_PG_HOST_FOR_GATEWAY", "rsync-ai-postgres-e2e")
# Pin the dest connector to the version with a running MCP container. The repo's
# postgresql current_version is v1.0.0; do NOT inherit the shopify template's
# stale v1.0.14 default (that container does not exist -> "no container reachable").
PG_CONNECTOR_VERSION = os.getenv("E2E_POSTGRES_CONNECTOR_VERSION", "v1.0.0")

LOGIN_EMAIL = os.getenv("E2E_USER_EMAIL", "default@rsync-ai.local")
LOGIN_PASSWORD = os.getenv("E2E_USER_PASSWORD", "password123")

# github-rest source. base_url defaults to https://api.github.com inside the
# connector; `licenses` is the public, token-free resource we sync.
GH_RESOURCE = os.getenv("E2E_GITHUB_RESOURCE", "licenses")
GH_LICENSES_URL = "https://api.github.com/licenses"

RUN_TIMEOUT_S = int(os.getenv("E2E_RUN_TIMEOUT_S", "600"))
POLL_INTERVAL_S = float(os.getenv("E2E_POLL_INTERVAL_S", "5"))

# Unique destination schema so re-runs never collide and cleanup is a single DROP.
DEST_SCHEMA = f"ghrest_{uuid.uuid4().hex[:8]}"


# --------------------------------------------------------------------------- #
# Skip predicates — this test SKIPS (never fails) on missing infra / GitHub.
# --------------------------------------------------------------------------- #


def _backend_reachable() -> bool:
    try:
        return requests.get(f"{API_GATEWAY_URL}/health", timeout=3).ok
    except requests.RequestException:
        return False


def _pg_container_running() -> bool:
    try:
        out = subprocess.check_output(
            ["docker", "inspect", "-f", "{{.State.Running}}", PG_CONTAINER],
            text=True, stderr=subprocess.DEVNULL,
        ).strip()
        return out == "true"
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _github_reachable() -> bool:
    """Pre-check the public source. A 403/429 (anonymous rate limit) or a network
    error means SKIP, not FAIL — the test can't run, but nothing is broken."""
    try:
        r = requests.get(GH_LICENSES_URL, timeout=8)
        if r.status_code != 200:
            return False
        return isinstance(r.json(), list) and len(r.json()) >= 1
    except (requests.RequestException, ValueError):
        return False


def _skip_reason() -> str | None:
    if not _backend_reachable():
        return f"api-gateway not reachable at {API_GATEWAY_URL}"
    if not _pg_container_running():
        return f"docker container '{PG_CONTAINER}' not running"
    if not _github_reachable():
        return f"{GH_LICENSES_URL} not reachable / rate-limited (anonymous 60/hr)"
    return None


pytestmark = pytest.mark.skipif(
    _skip_reason() is not None,
    reason=_skip_reason() or "",
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _login() -> str:
    resp = requests.post(
        f"{API_GATEWAY_URL}/api/v1/auth/login",
        json={"email": LOGIN_EMAIL, "password": LOGIN_PASSWORD},
        timeout=10,
    )
    assert resp.ok, f"login failed: {resp.status_code} {resp.text}"
    token = resp.json().get("token")
    assert token, f"no token in login response: {resp.text}"
    return token


def _create_connection(token: str, payload: dict[str, Any]) -> str:
    resp = requests.post(
        f"{API_GATEWAY_URL}/api/v1/connections",
        json=payload,
        cookies={"auth_token": token},
        timeout=20,
    )
    assert resp.ok, f"create connection failed: {resp.status_code} {resp.text}"
    data = resp.json()
    conn_id = data.get("id") or (data.get("connection") or {}).get("id")
    assert conn_id, f"no connection id in response: {data}"
    return conn_id


def _create_pipeline(token: str, source_id: str, dest_id: str) -> str:
    payload = {
        "name": f"github-rest-{GH_RESOURCE}-to-postgres-{uuid.uuid4().hex[:8]}",
        "request": f"Sync the {GH_RESOURCE} list from GitHub into PostgreSQL",
        "source_connection_id": source_id,
        "destination_connection_id": dest_id,
        "sync_mode": "batch",
        # github-rest token-free cannot discover_schema (whoami 401 -> tables:[]),
        # so pre-select the resource here; the executor then skips discover/HITL.
        "selected_tables": [GH_RESOURCE],
        "destination_namespace": DEST_SCHEMA,
    }
    resp = requests.post(
        f"{API_GATEWAY_URL}/api/v1/pipelines",
        # A freshly-generated connector is lifecycle 'draft' (never validated
        # against the real vendor API); allow_draft lets the pipeline reference it.
        params={"allow_draft": "true"},
        json=payload,
        cookies={"auth_token": token},
        timeout=30,
    )
    assert resp.ok, f"create pipeline failed: {resp.status_code} {resp.text}"
    data = resp.json()
    pipe_id = data.get("id") or (data.get("pipeline") or {}).get("id")
    assert pipe_id, f"no pipeline id in response: {data}"
    return pipe_id


def _run_pipeline(token: str, pipe_id: str) -> dict:
    resp = requests.post(
        f"{API_GATEWAY_URL}/api/v1/pipelines/{pipe_id}/run",
        params={"allow_draft": "true", "ack_warnings": "true"},
        json={"ack_warnings": True},
        cookies={"auth_token": token},
        timeout=30,
    )
    assert resp.ok, f"run pipeline failed: {resp.status_code} {resp.text}"
    return resp.json()


def _get_pipeline_status(token: str, pipe_id: str) -> dict:
    resp = requests.get(
        f"{API_GATEWAY_URL}/api/v1/pipelines/{pipe_id}",
        cookies={"auth_token": token},
        timeout=10,
    )
    assert resp.ok, f"get pipeline failed: {resp.status_code} {resp.text}"
    return resp.json()


# Control-plane (orchestrator executions table) — the api-gateway pipeline.status
# flips to "completed" while executor work is still queued, so we poll the
# executions row for the true terminal state (same approach as the shopify test).
CONTROL_PLANE_DB_CONTAINER = os.getenv("E2E_CONTROL_PG_CONTAINER", "postgres")
CONTROL_PLANE_DB_USER = os.getenv("E2E_CONTROL_PG_USER", "user")
CONTROL_PLANE_DB_NAME = os.getenv("E2E_CONTROL_PG_DB", "pipeline_db")


def _query_control_plane(sql: str) -> str:
    return subprocess.check_output(
        ["docker", "exec", "-i", CONTROL_PLANE_DB_CONTAINER,
         "psql", "-U", CONTROL_PLANE_DB_USER, "-d", CONTROL_PLANE_DB_NAME,
         "-tAc", sql],
        text=True,
    ).strip()


def _psql(sql: str) -> str:
    return subprocess.check_output(
        ["docker", "exec", "-i", PG_CONTAINER,
         "psql", "-U", PG_USER, "-d", PG_DB, "-tAc", sql],
        text=True,
    ).strip()


def _wait_for_completion(token: str, pipe_id: str) -> dict:
    """Poll the orchestrator executions table until terminal. Returns the row."""
    terminal = {"completed", "succeeded", "failed", "error", "cancelled", "expired"}
    last_marker = None
    started = time.time()
    while time.time() - started < RUN_TIMEOUT_S:
        info = _get_pipeline_status(token, pipe_id)
        api_status = (info.get("status") or info.get("state") or "").lower()
        rows = _query_control_plane(
            f"SELECT status, end_time, error_message FROM executions "
            f"WHERE pipeline_id = '{pipe_id}' ORDER BY start_time DESC LIMIT 1;"
        )
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
        f"pipeline {pipe_id} did not reach a terminal execution status within "
        f"{RUN_TIMEOUT_S}s; last marker: {last_marker!r}"
    )


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def auth_token() -> str:
    return _login()


@pytest.fixture(scope="module")
def github_source_id(auth_token: str) -> str:
    # force_save=true: skip the save-time connectivity probe (token-free whoami is
    # not the path under test — export(/licenses) is). No cleanup: deleting a
    # connection mid-execution nil-panics the orchestrator (shopify-test bug note).
    return _create_connection(
        auth_token,
        {
            "name": f"e2e-github-rest-src-{uuid.uuid4().hex[:8]}",
            "connection_type": "source",
            "connector_type": "github-rest",
            "config": {},
            "sync_mode": "batch",
            "force_save": True,
        },
    )


@pytest.fixture(scope="module")
def postgres_dest_id(auth_token: str) -> str:
    return _create_connection(
        auth_token,
        {
            "name": f"e2e-postgres-dest-{uuid.uuid4().hex[:8]}",
            "connection_type": "destination",
            "connector_type": "postgresql",
            "connector_version": PG_CONNECTOR_VERSION,
            "config": {
                "host": PG_HOST_FOR_GATEWAY,
                "port": 5432,
                "database": PG_DB,
                "user": PG_USER,
                "password": PG_PASSWORD,
                "sslmode": "disable",
            },
        },
    )


@pytest.fixture(scope="module")
def pipeline_id(auth_token: str, github_source_id: str, postgres_dest_id: str) -> str:
    return _create_pipeline(auth_token, github_source_id, postgres_dest_id)


# --------------------------------------------------------------------------- #
# The test
# --------------------------------------------------------------------------- #


def test_github_licenses_arrive_in_postgres(auth_token: str, pipeline_id: str) -> None:
    """Run the pipeline, wait for terminal success, assert >=1 license row landed
    in the destination schema, then drop the schema."""
    try:
        _run_pipeline(auth_token, pipeline_id)
        info = _wait_for_completion(auth_token, pipeline_id)

        status = (info.get("status") or "").lower()
        assert status in ("completed", "succeeded"), (
            f"pipeline ended non-success: {status!r} err={info.get('error_message')!r}"
        )

        table = f'{DEST_SCHEMA}."{GH_RESOURCE}"'
        # The table exists only if >=1 row landed (sink DDLs on first row).
        exists = _psql(
            "SELECT to_regclass('" + f"{DEST_SCHEMA}.{GH_RESOURCE}" + "');"
        )
        assert exists and exists != "", (
            f"pipeline reported success but no {DEST_SCHEMA}.{GH_RESOURCE} table "
            f"exists — the connector emitted rows but the load step dropped them "
            f"(the executor records-key regression this test guards)."
        )
        count = int(_psql(f"SELECT COUNT(*) FROM {table};"))
        print(f"landed {count} rows in {table}")
        assert count >= 1, f"{table} exists but is empty"
    finally:
        # Best-effort cleanup: drop the disposable schema regardless of outcome.
        try:
            _psql(f"DROP SCHEMA IF EXISTS {DEST_SCHEMA} CASCADE;")
        except subprocess.CalledProcessError:
            pass
