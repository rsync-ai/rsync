"""First GENERATED GraphQL-connector -> DB end-to-end pipeline test.

Companion to test_github_rest_to_postgres_batch.py (which proves a generated
REST/OpenAPI connector). This proves the OTHER generator protocol: a
tool-generator-GENERATED **GraphQL** connector driving a real pipeline to a
destination, closing the last protocol-coverage gap for the generator's mission
(api/SaaS · GraphQL · REST · OSS-MCP).

  widgets-graphql (GENERATED from e2e/fixtures/widgets_graphql_introspection.json,
  source, Relay cursor pagination) -> PostgreSQL (destination) through the full
  control plane: api-gateway -> orchestrator (reuses the running MCP container) ->
  planner/executor batch extract -> sink -> Postgres.

DETERMINISTIC + mock-backed (same discipline as test_github_rest_oauth_refresh.py):
the source is a local `mock-graphql` server (e2e/mock_graphql_server.py) that
serves exactly 5 widgets, so the test asserts EXACT row parity (== 5), not just
">= 1". No external network dependency, so this is eligible for an opt-in CI lane.

Topology (set up by run_gate.sh's E2E_INCLUDE_GRAPHQL=1 lane, or manually for a
local dev run — see docs):
  * mock-graphql            : container `rsync-ai-mock-graphql`, alias `mock-graphql`
                              on rsync-ai_default, serves POST /graphql.
  * widgets-graphql MCP     : container `rsync-ai-widgets-graphql-v1-0-0-mcp` on
                              {rsync-ai-mcp, rsync-ai_default} with
                              MCP_ALLOW_PRIVATE_NETWORKS=true so its outbound SSRF
                              guard permits the private mock host.
  * source connection config: {"endpoint": "http://mock-graphql:8080/graphql"}
                              overrides the connector's baked DEFAULT_ENDPOINT.

Run:
    # against the e2e gate stack (defaults below):
    E2E_INCLUDE_GRAPHQL=1 e2e/run_gate.sh          # (opt-in lane)
    pytest e2e/test_graphql_connector_to_postgres_batch.py -v -s

    # against a local dev stack (dest = control-plane postgres):
    E2E_PG_CONTAINER=postgres E2E_PG_USER=user E2E_PG_DB=pipeline_db \
    E2E_PG_PASSWORD=password E2E_PG_HOST_FOR_GATEWAY=postgres \
    pytest e2e/test_graphql_connector_to_postgres_batch.py -v -s

Skips cleanly (never fails) when api-gateway, the dest Postgres container, the
mock, or the generated connector container are not up — the test can't run, but
nothing is broken.
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

# Isolated-stack prefix: run_gate.sh exports STACK_PREFIX=rsync-ci for a fully
# isolated CI stack; unset => rsync-ai (historical shared-stack behavior). Every
# container/network name below derives from PFX so both stacks coexist.
PFX = os.environ.get("STACK_PREFIX", "rsync-ai")

API_GATEWAY_URL = os.getenv("API_GATEWAY_URL", "http://localhost:5001")

# Destination Postgres. Defaults match the e2e gate stack; override for a local
# dev stack (see module docstring).
PG_CONTAINER = os.getenv("E2E_PG_CONTAINER") or os.getenv("PG_CONTAINER") or f"{PFX}-postgres-e2e"
PG_USER = os.getenv("E2E_PG_USER", "e2e_user")
PG_DB = os.getenv("E2E_PG_DB", "e2e_db")
PG_PASSWORD = os.getenv("E2E_PG_PASSWORD", "e2e_password")
# Dest connector-config host = the dest container's docker-network hostname.
PG_HOST_FOR_GATEWAY = os.getenv("E2E_PG_HOST_FOR_GATEWAY", PG_CONTAINER)
PG_CONNECTOR_VERSION = os.getenv("E2E_POSTGRES_CONNECTOR_VERSION", "v1.0.0")

LOGIN_EMAIL = os.getenv("E2E_USER_EMAIL", "default@rsync-ai.local")
LOGIN_PASSWORD = os.getenv("E2E_USER_PASSWORD", "password123")

# Generated GraphQL source.
GQL_CONNECTOR = os.getenv("E2E_GQL_CONNECTOR", "widgets-graphql")
GQL_CONTAINER = os.getenv("E2E_GQL_CONTAINER", f"{PFX}-widgets-graphql-v1-0-0-mcp")
MOCK_CONTAINER = os.getenv("E2E_GQL_MOCK_CONTAINER", f"{PFX}-mock-graphql")
# The endpoint the connector container uses to reach the mock (docker DNS).
MOCK_ENDPOINT = os.getenv("E2E_GQL_MOCK_ENDPOINT", "http://mock-graphql:8080/graphql")
GQL_RESOURCE = os.getenv("E2E_GQL_RESOURCE", "widgets")
EXPECTED_ROWS = int(os.getenv("E2E_GQL_EXPECTED_ROWS", "5"))

RUN_TIMEOUT_S = int(os.getenv("E2E_RUN_TIMEOUT_S", "600"))
POLL_INTERVAL_S = float(os.getenv("E2E_POLL_INTERVAL_S", "5"))

DEST_SCHEMA = f"widgets_gql_{uuid.uuid4().hex[:8]}"


# --------------------------------------------------------------------------- #
# Skip predicates — SKIP (never fail) on missing infra.
# --------------------------------------------------------------------------- #


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


def _skip_reason() -> str | None:
    if not _backend_reachable():
        return f"api-gateway not reachable at {API_GATEWAY_URL}"
    if not _container_running(PG_CONTAINER):
        return f"dest postgres container '{PG_CONTAINER}' not running"
    if not _container_running(GQL_CONTAINER):
        return f"generated connector container '{GQL_CONTAINER}' not running"
    if not _container_running(MOCK_CONTAINER):
        return f"mock-graphql container '{MOCK_CONTAINER}' not running"
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
        "name": f"widgets-graphql-to-postgres-{uuid.uuid4().hex[:8]}",
        "request": f"Sync the {GQL_RESOURCE} list from the widgets GraphQL API into PostgreSQL",
        "source_connection_id": source_id,
        "destination_connection_id": dest_id,
        "sync_mode": "batch",
        # Pre-select the resource so the executor skips discover_schema/HITL.
        "selected_tables": [GQL_RESOURCE],
        "destination_namespace": DEST_SCHEMA,
    }
    resp = requests.post(
        f"{API_GATEWAY_URL}/api/v1/pipelines",
        # A freshly-generated connector is draft-eligible; allow_draft lets the
        # pipeline reference it without a prior live vendor validation.
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


# Control-plane executions table lives in the main postgres (pipeline_db).
CONTROL_PLANE_DB_CONTAINER = os.getenv("E2E_CONTROL_PG_CONTAINER") or os.getenv("ORCH_PG_CONTAINER", "postgres")
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
def gql_source_id(auth_token: str) -> str:
    # force_save=true: skip the save-time connectivity probe (the private mock is
    # not the path under test — export(widgets) is). endpoint overrides the baked
    # DEFAULT_ENDPOINT so the connector reaches the local mock. No cleanup:
    # deleting a connection mid-execution nil-panics the orchestrator.
    return _create_connection(
        auth_token,
        {
            "name": f"e2e-widgets-graphql-src-{uuid.uuid4().hex[:8]}",
            "connection_type": "source",
            "connector_type": GQL_CONNECTOR,
            "config": {"endpoint": MOCK_ENDPOINT},
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
def pipeline_id(auth_token: str, gql_source_id: str, postgres_dest_id: str) -> str:
    return _create_pipeline(auth_token, gql_source_id, postgres_dest_id)


# --------------------------------------------------------------------------- #
# The test
# --------------------------------------------------------------------------- #


def test_widgets_arrive_in_postgres(auth_token: str, pipeline_id: str) -> None:
    """Run the pipeline, wait for terminal success, assert EXACTLY the 5 mock
    widgets landed in the destination schema with content parity, then drop it."""
    try:
        _run_pipeline(auth_token, pipeline_id)
        info = _wait_for_completion(auth_token, pipeline_id)

        status = (info.get("status") or "").lower()
        assert status in ("completed", "succeeded"), (
            f"pipeline ended non-success: {status!r} err={info.get('error_message')!r}"
        )

        # The table exists only if >=1 row landed (sink DDLs on first row).
        exists = _psql("SELECT to_regclass('" + f"{DEST_SCHEMA}.{GQL_RESOURCE}" + "');")
        assert exists and exists != "", (
            f"pipeline reported success but no {DEST_SCHEMA}.{GQL_RESOURCE} table "
            f"exists — the generated GraphQL connector emitted rows but the load "
            f"step dropped them."
        )

        table = f'{DEST_SCHEMA}."{GQL_RESOURCE}"'
        count = int(_psql(f"SELECT COUNT(*) FROM {table};"))
        print(f"landed {count} rows in {table}")
        assert count == EXPECTED_ROWS, (
            f"expected exactly {EXPECTED_ROWS} widgets, got {count}"
        )

        # Content parity: the deterministic mock guarantees widget-3 == 'Charlie'.
        name = _psql(f"SELECT name FROM {table} WHERE id = 'widget-3';")
        assert name == "Charlie", f"content mismatch for widget-3: name={name!r}"
    finally:
        try:
            _psql(f"DROP SCHEMA IF EXISTS {DEST_SCHEMA} CASCADE;")
        except subprocess.CalledProcessError:
            pass


if __name__ == "__main__":
    # run_gate.sh invokes .py tests as `python3 <file>` (not pytest), so provide a
    # __main__ that shells into pytest and propagates the real exit code.
    raise SystemExit(pytest.main([__file__, "-v", "-s", "-p", "no:cacheprovider"]))
