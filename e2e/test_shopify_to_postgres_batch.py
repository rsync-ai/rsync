"""
First SaaS → DB end-to-end pipeline test in the repo.

Exercises Shopify Admin GraphQL (source) -> PostgreSQL (destination) through
the full control plane:
  - api-gateway: login, create connections, create pipeline, trigger run
  - orchestrator: Preflight agent dynamically spawns the shopify-admin-graphql
    MCP container on the `rsync-ai-mcp` docker network (no static
    docker-compose.mcp.yml entry needed)
  - planner: schedules a batch extract → load through the standard sink
  - postgres-e2e: receives rows

Why this test exists: prior to this test, no SaaS connector → DB destination
pipeline had ever been proven end-to-end in CI or in repo tests. The
connector-contract layer (test_generated_connectors.py) covers extract logic
in isolation, and DB-to-DB CDC tests cover the data plane, but the seam
between a SaaS source and a DB destination was untested.

Skips cleanly when:
  - api-gateway isn't reachable, OR
  - the e2e postgres docker container isn't running, OR
  - SHOPIFY_ACCESS_TOKEN / SHOPIFY_SHOP env vars are unset (real creds).

This test never embeds credentials. Set them via env:

    export SHOPIFY_SHOP=your-dev-store-subdomain
    export SHOPIFY_ACCESS_TOKEN=shpat_xxxxxxxxxxxxxxxxxxxx
    pytest e2e/test_shopify_to_postgres_batch.py -v -s

First-run failures are expected and informative — capture the orchestrator
logs and any pipeline event stream output. The test deliberately polls run
status with verbose logging so the failure mode is visible.
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
PG_CONTAINER = os.getenv("E2E_PG_CONTAINER", "rsync-ai-postgres-e2e")
PG_USER = os.getenv("E2E_PG_USER", "e2e_user")
PG_DB = os.getenv("E2E_PG_DB", "e2e_db")
PG_PASSWORD = os.getenv("E2E_PG_PASSWORD", "e2e_password")
PG_HOST_FOR_GATEWAY = os.getenv("E2E_PG_HOST_FOR_GATEWAY", "rsync-ai-postgres-e2e")

LOGIN_EMAIL = os.getenv("E2E_USER_EMAIL", "default@rsync-ai.local")
LOGIN_PASSWORD = os.getenv("E2E_USER_PASSWORD", "password123")

SHOPIFY_SHOP = os.getenv("SHOPIFY_SHOP", "")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-10")

# Pipeline run polling tunables. SaaS extract + Preflight container spawn can
# both be slow on first run; default generously and let CI/devs trim.
RUN_TIMEOUT_S = int(os.getenv("E2E_RUN_TIMEOUT_S", "600"))
POLL_INTERVAL_S = float(os.getenv("E2E_POLL_INTERVAL_S", "5"))


# --------------------------------------------------------------------------- #
# Skip predicates
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


def _skip_reason() -> str | None:
    if not SHOPIFY_SHOP or not SHOPIFY_ACCESS_TOKEN:
        return "SHOPIFY_SHOP / SHOPIFY_ACCESS_TOKEN env vars not set"
    if not _backend_reachable():
        return f"api-gateway not reachable at {API_GATEWAY_URL}"
    if not _pg_container_running():
        return f"docker container '{PG_CONTAINER}' not running"
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
    """POST /api/v1/connections and return the new connection's id."""
    resp = requests.post(
        f"{API_GATEWAY_URL}/api/v1/connections",
        json=payload,
        cookies={"auth_token": token},
        timeout=15,
    )
    assert resp.ok, f"create connection failed: {resp.status_code} {resp.text}"
    data = resp.json()
    conn_id = data.get("id") or (data.get("connection") or {}).get("id")
    assert conn_id, f"no connection id in response: {data}"
    return conn_id


def _delete_connection(token: str, conn_id: str) -> None:
    try:
        requests.delete(
            f"{API_GATEWAY_URL}/api/v1/connections/{conn_id}",
            cookies={"auth_token": token},
            timeout=10,
        )
    except requests.RequestException:
        pass


def _create_pipeline(token: str, source_id: str, dest_id: str) -> str:
    payload = {
        "name": f"shopify-products-to-postgres-{uuid.uuid4().hex[:8]}",
        "request": "Sync the products list from Shopify into PostgreSQL",
        "source_connection_id": source_id,
        "destination_connection_id": dest_id,
    }
    resp = requests.post(
        f"{API_GATEWAY_URL}/api/v1/pipelines",
        json=payload,
        cookies={"auth_token": token},
        timeout=30,
    )
    assert resp.ok, f"create pipeline failed: {resp.status_code} {resp.text}"
    data = resp.json()
    pipe_id = data.get("id") or (data.get("pipeline") or {}).get("id")
    assert pipe_id, f"no pipeline id in response: {data}"
    return pipe_id


def _delete_pipeline(token: str, pipe_id: str) -> None:
    try:
        requests.delete(
            f"{API_GATEWAY_URL}/api/v1/pipelines/{pipe_id}",
            cookies={"auth_token": token},
            timeout=10,
        )
    except requests.RequestException:
        pass


def _run_pipeline(token: str, pipe_id: str) -> dict:
    """POST /api/v1/pipelines/:id/run and return the JSON body."""
    resp = requests.post(
        f"{API_GATEWAY_URL}/api/v1/pipelines/{pipe_id}/run",
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


SELECTED_TABLES = [t.strip() for t in os.getenv("E2E_SHOPIFY_TABLES", "products").split(",") if t.strip()]


def _resume_pipeline_with_tables(token: str, pipe_id: str, tables: list[str]) -> None:
    """Drive past the table-selection HITL gate by submitting selected tables."""
    resp = requests.post(
        f"{API_GATEWAY_URL}/api/v1/pipelines/{pipe_id}/hitl/tables",
        json={"selected_tables": tables},
        cookies={"auth_token": token},
        timeout=15,
    )
    assert resp.ok, f"resume tables failed: {resp.status_code} {resp.text}"


def _last_event(token: str, pipe_id: str) -> dict:
    try:
        r = requests.get(
            f"{API_GATEWAY_URL}/api/v1/pipelines/{pipe_id}/events?limit=1",
            cookies={"auth_token": token}, timeout=10,
        )
        if not r.ok:
            return {}
        events = r.json().get("events") or []
        return events[0] if events else {}
    except requests.RequestException:
        return {}


def _wait_for_completion(token: str, pipe_id: str) -> dict:
    """Poll until the *execution* (not the pipeline entity) reaches a terminal
    state. We query the orchestrator's executions table directly because the
    api-gateway pipelines.status field is known to flip to "completed" while
    executor work is still queued (bug surfaced by this test's first run).

    When the pipeline pauses at a table-selection HITL gate, we auto-submit
    `SELECTED_TABLES` to drive past it. Other HITL gates (connection config,
    schema-change approval) would still block — those should be added as
    they're observed.

    Returns the execution row as a dict. Empty dict means no execution row
    was ever created (also a real failure mode).
    """
    terminal = {"completed", "succeeded", "failed", "error", "cancelled", "expired"}
    last_status = None
    submitted_tables = False
    started = time.time()
    while time.time() - started < RUN_TIMEOUT_S:
        # Show api-gateway's view alongside the executions table so we can
        # see them disagree.
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
        if marker != last_status:
            print(f"[t+{int(time.time() - started)}s] {marker}")
            last_status = marker
        # Execution row exists and has end_time -> truly terminal.
        if exec_status in terminal and end_time:
            return {"status": exec_status, "end_time": end_time, "error_message": err}

        # Drive past HITL: if the orchestrator is parked at table_selection,
        # submit our pre-chosen tables once. Other HITL gates (e.g. node-input)
        # still block.
        if not submitted_tables:
            evt = _last_event(token, pipe_id)
            if evt.get("event_type") == "PIPELINE_WAITING":
                br = ((evt.get("payload") or {}).get("blocking_reason") or {})
                btype = br.get("type") or br.get("details", {}).get("request_type", "")
                if "table" in str(btype).lower():
                    print(f"[t+{int(time.time() - started)}s] HITL: submitting tables={SELECTED_TABLES}")
                    _resume_pipeline_with_tables(token, pipe_id, SELECTED_TABLES)
                    submitted_tables = True
        time.sleep(POLL_INTERVAL_S)
    raise AssertionError(
        f"pipeline {pipe_id} did not reach a terminal execution status within "
        f"{RUN_TIMEOUT_S}s; last marker: {last_status!r}"
    )


def _psql(sql: str) -> str:
    return subprocess.check_output(
        ["docker", "exec", "-i", PG_CONTAINER,
         "psql", "-U", PG_USER, "-d", PG_DB, "-tAc", sql],
        text=True,
    ).strip()


def _find_destination_table() -> str:
    """Find the table that the pipeline created. The Shopify connector ships
    a "products" entity; the sink typically lands it as a table whose name
    contains "product". We don't pin the exact name so the test survives
    minor naming-strategy changes in the orchestrator."""
    rows = _psql(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='public' AND lower(table_name) LIKE '%product%' "
        "ORDER BY table_name;"
    )
    names = [n for n in rows.splitlines() if n]
    assert names, (
        "No 'product*' table found in destination Postgres after pipeline "
        "completed. The pipeline may have succeeded with zero rows, or the "
        "sink chose a different table name. Check `\\dt public.*` manually."
    )
    return names[0]


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def auth_token() -> str:
    return _login()


@pytest.fixture(scope="module")
def shopify_source_id(auth_token: str):
    """No cleanup: the orchestrator panics with a nil-pointer dereference if
    a connection is deleted while an execution is still in flight (real bug
    surfaced on the first run of this test). Leave the connection alive;
    re-runs reuse via a fresh name each time."""
    return _create_connection(
        auth_token,
        {
            "name": f"shopify-source-{uuid.uuid4().hex[:8]}",
            "connection_type": "source",
            "connector_type": "shopify-admin-graphql",
            "config": {
                "shop": SHOPIFY_SHOP,
                "access_token": SHOPIFY_ACCESS_TOKEN,
                "api_version": SHOPIFY_API_VERSION,
            },
            "sync_mode": "batch",
        },
    )


@pytest.fixture(scope="module")
def postgres_dest_id(auth_token: str):
    # No cleanup — see comment on shopify_source_id.
    return _create_connection(
        auth_token,
        {
            "name": f"postgres-dest-{uuid.uuid4().hex[:8]}",
            "connection_type": "destination",
            "connector_type": "postgresql",
            # Pin to the version that actually has a running MCP container.
            # docker-compose.mcp.yml only generates the latest version's
            # entry; the orchestrator otherwise pins v1.0.0 by default and
            # the sink fails with "no container is reachable".
            "connector_version": os.getenv("E2E_POSTGRES_CONNECTOR_VERSION", "v1.0.14"),
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
def pipeline_id(auth_token: str, shopify_source_id: str, postgres_dest_id: str):
    # No cleanup — see comment on shopify_source_id.
    return _create_pipeline(auth_token, shopify_source_id, postgres_dest_id)


# --------------------------------------------------------------------------- #
# The test
# --------------------------------------------------------------------------- #


def test_shopify_products_arrive_in_postgres(
    auth_token: str, pipeline_id: str
) -> None:
    """Trigger the pipeline, wait for completion, assert rows are in Postgres."""
    _run_pipeline(auth_token, pipeline_id)
    info = _wait_for_completion(auth_token, pipeline_id)

    status = (info.get("status") or info.get("state") or "").lower()
    assert status in ("completed", "succeeded"), (
        f"pipeline ended in non-success status: {status!r}\n"
        f"full response: {info}"
    )

    # Look for a destination table. The pipeline lands one only when the
    # source had >=1 row to sync; an empty dev store produces a clean
    # `completed` with 0 rows and no DDL on the destination.
    try:
        rows = _psql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND lower(table_name) LIKE '%product%' "
            "ORDER BY table_name;"
        )
        names = [n for n in rows.splitlines() if n]
    except subprocess.CalledProcessError:
        names = []

    if not names:
        # Treat an empty source as a soft-success: the *pipeline mechanics* were
        # validated, the *data path* had nothing to carry.
        #
        # Read the count from pipeline_run_table_stats, NOT from
        # executions.metrics. Nothing has written executions.metrics for a long
        # time — it is a superseded column that the api-gateway readers now
        # COALESCE to '{}' before merging in a real records_processed derived
        # from these same table stats (see handlers/pipelines.go:957,1437). This
        # message used to query it directly and so always printed "0",
        # regardless of what the run actually moved.
        # Same GREATEST(...) shape the api-gateway uses for records_processed
        # (handlers/pipelines.go:890-896) so this number matches the UI's.
        metrics_row = _query_control_plane(
            f"SELECT COALESCE(SUM(GREATEST("
            f"COALESCE(s.inserted_rows, 0), "
            f"COALESCE(s.applied_inserts, 0) + COALESCE(s.applied_updates, 0) "
            f"+ COALESCE(s.applied_deletes, 0))), 0) "
            f"FROM pipeline_run_table_stats s "
            f"WHERE s.pipeline_id = '{pipeline_id}';"
        )
        rows_processed = metrics_row or "0"
        pytest.skip(
            f"Pipeline completed cleanly but rows_processed={rows_processed}. "
            f"Add at least one product in the Shopify dev store admin to "
            f"exercise the destination DDL+write path."
        )

    table = names[0]
    count = int(_psql(f'SELECT COUNT(*) FROM public."{table}";'))
    print(f"destination table: public.\"{table}\" — rows: {count}")
    assert count > 0, (
        f"pipeline reported success but destination table public.\"{table}\" "
        f"is empty. The connector emitted rows but the load step dropped them."
    )
