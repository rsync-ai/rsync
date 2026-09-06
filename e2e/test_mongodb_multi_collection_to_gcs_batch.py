"""
MongoDB (multi-collection) → GCS BATCH pipeline, end-to-end through the control plane.

Why this test exists
--------------------
Two gaps met here. (1) MongoDB was only ever proven as a *CDC* source
(e2e/test_mongodb_cdc_to_postgres.py) and only ever for ONE collection — its
batch `export` path (`_id` keyset paging) had no end-to-end coverage at all.
(2) GCS was only ever proven as a *CDC* destination via a shell probe that the
gate does not run (e2e/test_db_cdc_to_emulated_gcs_azure.sh). The combination —
a client configuring several collections in one batch pipeline into an object
store — was completely unexercised.

The assertion that earns the test is per-collection, not aggregate. A pipeline
that collapses every collection into one prefix, or that syncs only the first
selected table, still writes objects and still reports `completed`; only an
exact per-collection document count distinguishes those from a correct run.
Each collection is seeded with a DIFFERENT, prime-ish row count (7 / 11 / 5) so
a cross-wired subtree cannot coincidentally match.

Object layout under test (kafka-sink-worker `partKey`/`tablePrefix`):

    <path_prefix>/<dataset>/<db_or_schema>/<collection>/dt=YYYY-MM-DD/part-000000.jsonl
                                                        + sibling _SUCCESS / _MANIFEST.json

`dataset` is `slugify(pipeline_id)` for object-storage destinations
(executor.go resolveBatchDataset) and `db_or_schema` is the Mongo database name
(executor.go, taken from the source connection's `database`), so each collection
lands in its own subtree by construction — this test is what proves the
construction actually holds end to end.

Credential-free by design
-------------------------
The destination is a `fsouza/fake-gcs-server` emulator. The gcs connector falls
back to `AnonymousCredentials` whenever no `service_account_json` is supplied
(public/storage/gcs/versions/v1.0.0/connector.py `_get_gcs_client`), so no real GCP credential
is ever created, stored, or handled. The emulator host also passes the INV-1
storage-safety guard, which denies only rsync-ai's internal MinIO.

Not in the deterministic merge gate: it provisions its own MongoDB and
fake-gcs-server containers, which the gate stack does not bring up. It is listed
in run_gate.sh UNGATED_TESTS. Run directly:

    pytest e2e/test_mongodb_multi_collection_to_gcs_batch.py -v -s

Against the isolated CI stack, add:

    API_GATEWAY_URL=http://localhost:15001 E2E_MCP_NET=rsync-ci-mcp \
    E2E_MONGO_MCP=rsync-ci-mongodb-v1-0-0-mcp E2E_GCS_MCP=rsync-ci-gcs-v1-0-0-mcp \
    E2E_CONTROL_PG_CONTAINER=rsync-ci-postgres

Skips cleanly (never fails) when docker, api-gateway, or either MCP is missing.
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
MONGO_MCP = os.getenv("E2E_MONGO_MCP", "rsync-ai-mongodb-v1-0-0-mcp")
GCS_MCP = os.getenv("E2E_GCS_MCP", "rsync-ai-gcs-v1-0-0-mcp")
MONGO_CONNECTOR_VERSION = os.getenv("E2E_MONGO_CONNECTOR_VERSION", "v1.0.0")
GCS_CONNECTOR_VERSION = os.getenv("E2E_GCS_CONNECTOR_VERSION", "v1.0.0")

# Throwaway fixtures this test provisions and removes itself.
MONGO_SRV = os.getenv("E2E_MONGO_SRV", "mongo-multi-e2e")
GCS_EMU = os.getenv("E2E_GCS_EMU", "gcs-multi-fake")
MONGO_DB = os.getenv("E2E_MONGO_DB", "client_db")
BUCKET = os.getenv("E2E_GCS_BUCKET", "client-dest")
# Unique prefix so re-runs never collide and the readback is scoped to this run.
PATH_PREFIX = f"mongo_multi_{uuid.uuid4().hex[:8]}"
FILE_FORMAT = os.getenv("E2E_GCS_FILE_FORMAT", "jsonl")

# The whole point of the test: several collections, each a DIFFERENT size, so a
# cross-wired or collapsed subtree cannot accidentally produce the right count.
COLLECTIONS: dict[str, int] = {"customers": 7, "orders": 11, "products": 5}

LOGIN_EMAIL = os.getenv("E2E_USER_EMAIL", "default@rsync-ai.local")
LOGIN_PASSWORD = os.getenv("E2E_USER_PASSWORD", "password123")

RUN_TIMEOUT_S = int(os.getenv("E2E_RUN_TIMEOUT_S", "900"))
POLL_INTERVAL_S = float(os.getenv("E2E_POLL_INTERVAL_S", "5"))

CONTROL_PLANE_DB_CONTAINER = os.getenv("E2E_CONTROL_PG_CONTAINER", "postgres")
CONTROL_PLANE_DB_USER = os.getenv("E2E_CONTROL_PG_USER", "user")
CONTROL_PLANE_DB_NAME = os.getenv("E2E_CONTROL_PG_DB", "pipeline_db")


# --------------------------------------------------------------------------- #
# Skip predicates — SKIP (never fail) on missing infra.
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


def _skip_reason() -> str | None:
    if not _docker_ok():
        return "docker not available"
    if not _backend_reachable():
        return f"api-gateway not reachable at {API_GATEWAY_URL}"
    if not _container_running(MONGO_MCP):
        return f"mongodb MCP container '{MONGO_MCP}' not running"
    if not _container_running(GCS_MCP):
        return f"gcs MCP container '{GCS_MCP}' not running"
    return None


pytestmark = pytest.mark.skipif(_skip_reason() is not None, reason=_skip_reason() or "")


# --------------------------------------------------------------------------- #
# Fixture helpers (docker-provisioned Mongo + fake-gcs-server)
# --------------------------------------------------------------------------- #


def _mongosh(script: str) -> str:
    return subprocess.check_output(
        ["docker", "exec", MONGO_SRV, "mongosh", "--quiet", "--eval", script],
        text=True, stderr=subprocess.STDOUT,
    )


def _gcs_py(script: str) -> str:
    """Run a python snippet INSIDE the gcs MCP container (it is the only place
    with google-cloud-storage installed and a route to the emulator network)."""
    return subprocess.check_output(
        ["docker", "exec", "-i", GCS_MCP, "python3", "-"],
        input=script, text=True, stderr=subprocess.STDOUT,
    )


_GCS_CLIENT_PREAMBLE = f"""
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials
_c = storage.Client(project="emulator", credentials=AnonymousCredentials(),
                    client_options={{"api_endpoint": "http://{GCS_EMU}:4443"}})
"""


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
                         cookies={"auth_token": token}, timeout=30)
    assert resp.ok, f"create connection failed: {resp.status_code} {resp.text}"
    data = resp.json()
    conn_id = data.get("id") or (data.get("connection") or {}).get("id")
    assert conn_id, f"no connection id in response: {data}"
    return conn_id


def _create_pipeline(token: str, source_id: str, dest_id: str) -> str:
    payload = {
        "name": f"mongo-multi-to-gcs-{uuid.uuid4().hex[:8]}",
        "request": (f"Sync the {', '.join(COLLECTIONS)} collections from MongoDB "
                    f"database {MONGO_DB} into a GCS bucket"),
        "source_connection_id": source_id,
        "destination_connection_id": dest_id,
        "sync_mode": "batch",
        # Pre-select all three so the executor skips the table-selection HITL.
        "selected_tables": sorted(COLLECTIONS),
    }
    resp = requests.post(f"{API_GATEWAY_URL}/api/v1/pipelines",
                         params={"allow_draft": "true"}, json=payload,
                         cookies={"auth_token": token}, timeout=60)
    assert resp.ok, f"create pipeline failed: {resp.status_code} {resp.text}"
    data = resp.json()
    pipe_id = data.get("id") or (data.get("pipeline") or {}).get("id")
    assert pipe_id, f"no pipeline id in response: {data}"
    return pipe_id


def _run_pipeline(token: str, pipe_id: str) -> dict:
    resp = requests.post(f"{API_GATEWAY_URL}/api/v1/pipelines/{pipe_id}/run",
                         params={"allow_draft": "true", "ack_warnings": "true"},
                         json={"ack_warnings": True}, cookies={"auth_token": token}, timeout=60)
    assert resp.ok, f"run pipeline failed: {resp.status_code} {resp.text}"
    return resp.json()


def _query_control_plane(sql: str) -> str:
    return subprocess.check_output(
        ["docker", "exec", "-i", CONTROL_PLANE_DB_CONTAINER,
         "psql", "-U", CONTROL_PLANE_DB_USER, "-d", CONTROL_PLANE_DB_NAME, "-tAc", sql],
        text=True,
    ).strip()


def _wait_for_completion(pipe_id: str) -> dict:
    """Poll the orchestrator executions table until terminal — the api-gateway
    status flips to completed while executor work is still queued."""
    terminal = {"completed", "succeeded", "failed", "error", "cancelled", "expired"}
    last_marker = None
    started = time.time()
    while time.time() - started < RUN_TIMEOUT_S:
        rows = _query_control_plane(
            f"SELECT status, end_time, error_message FROM executions "
            f"WHERE pipeline_id = '{pipe_id}' ORDER BY start_time DESC LIMIT 1;")
        parts = rows.split("|") if rows else []
        exec_status = parts[0].lower() if parts else ""
        end_time = parts[1] if len(parts) > 1 else ""
        err = parts[2] if len(parts) > 2 else ""
        if exec_status != last_marker:
            print(f"[t+{int(time.time() - started)}s] exec={exec_status!r}")
            last_marker = exec_status
        if exec_status in terminal and end_time:
            return {"status": exec_status, "end_time": end_time, "error_message": err}
        time.sleep(POLL_INTERVAL_S)
    raise AssertionError(
        f"pipeline {pipe_id} did not reach terminal within {RUN_TIMEOUT_S}s; last={last_marker!r}")


# --------------------------------------------------------------------------- #
# Readback: what actually landed in the bucket
# --------------------------------------------------------------------------- #


def _read_landed_objects() -> dict[str, Any]:
    """List every blob under this run's prefix and return
    {"keys": [...], "rows_by_dir": {dir: n_jsonl_records}}.

    Counting is by parsed JSONL record, not by object: a per-object count would
    pass even if every collection wrote a single empty part-file.
    """
    script = _GCS_CLIENT_PREAMBLE + f"""
import json
keys, rows = [], {{}}
for b in _c.list_blobs("{BUCKET}", prefix="{PATH_PREFIX}/"):
    keys.append(b.name)
    leaf = b.name.rsplit("/", 1)[-1]
    if leaf.startswith("_"):
        continue  # _SUCCESS / _MANIFEST.json are markers, not data
    body = b.download_as_bytes().decode("utf-8", "replace")
    n = sum(1 for ln in body.splitlines() if ln.strip())
    # Key the count on the TABLE segment: <prefix>/<dataset>/<db>/<table>/dt=.../part
    parts = b.name.split("/")
    table = parts[3] if len(parts) > 4 else "?"
    rows[table] = rows.get(table, 0) + n
print("@@" + json.dumps({{"keys": sorted(keys), "rows_by_dir": rows}}))
"""
    out = _gcs_py(script)
    marker = [ln for ln in out.splitlines() if ln.startswith("@@")]
    assert marker, f"gcs readback produced no result line:\n{out}"
    return json.loads(marker[-1][2:])


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def mongo_source():
    """A throwaway standalone mongod seeded with three differently-sized
    collections. Batch export needs no replica set (that is a CDC requirement),
    so this stays a plain single-node mongod."""
    if not _container_running(MONGO_SRV):
        subprocess.run(["docker", "rm", "-f", MONGO_SRV],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.check_call(
            ["docker", "run", "-d", "--name", MONGO_SRV, "--network", MCP_NET,
             "mongo:6", "mongod", "--bind_ip_all"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ready = False
    for _ in range(30):
        try:
            _mongosh("db.runCommand({ping:1}).ok")
            ready = True
            break
        except subprocess.CalledProcessError:
            time.sleep(2)
    assert ready, f"mongod {MONGO_SRV} did not become ready"

    seed = [f'db = db.getSiblingDB("{MONGO_DB}");']
    for coll, n in COLLECTIONS.items():
        seed.append(f'db.{coll}.drop();')
        seed.append(
            f'db.{coll}.insertMany(Array.from({{length:{n}}},'
            f'(_,i)=>({{idx:i+1, kind:"{coll}", note:"row-"+(i+1)}})));')
    for coll, n in COLLECTIONS.items():
        seed.append(f'print("{coll}="+db.{coll}.countDocuments({{}}));')
    out = _mongosh("\n".join(seed))
    for coll, n in COLLECTIONS.items():
        assert f"{coll}={n}" in out, f"seed mismatch for {coll}: {out!r}"
    print(f"seeded {MONGO_DB}: {COLLECTIONS}")
    yield
    subprocess.run(["docker", "rm", "-f", MONGO_SRV],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


@pytest.fixture(scope="module")
def gcs_dest():
    """A throwaway fake-gcs-server plus the destination bucket. No credential of
    any kind is created or handled — the connector uses AnonymousCredentials."""
    if not _container_running(GCS_EMU):
        subprocess.run(["docker", "rm", "-f", GCS_EMU],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.check_call(
            ["docker", "run", "-d", "--name", GCS_EMU, "--network", MCP_NET,
             "fsouza/fake-gcs-server:latest", "-scheme", "http", "-port", "4443",
             "-external-url", f"http://{GCS_EMU}:4443"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ready = False
    for _ in range(20):
        try:
            _gcs_py(_GCS_CLIENT_PREAMBLE + f"""
try:
    _c.create_bucket("{BUCKET}")
except Exception as e:
    if "exist" not in str(e).lower():
        raise
print("@@ok")
""")
            ready = True
            break
        except subprocess.CalledProcessError:
            time.sleep(2)
    assert ready, f"fake-gcs-server {GCS_EMU} did not become ready"
    yield
    subprocess.run(["docker", "rm", "-f", GCS_EMU],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


@pytest.fixture(scope="module")
def auth_token() -> str:
    return _login()


@pytest.fixture(scope="module")
def mongo_source_id(auth_token: str, mongo_source) -> str:
    return _create_connection(auth_token, {
        "name": f"e2e-mongo-multi-src-{uuid.uuid4().hex[:8]}",
        "connection_type": "source",
        "connector_type": "mongodb",
        "connector_version": MONGO_CONNECTOR_VERSION,
        "config": {"host": MONGO_SRV, "port": 27017, "database": MONGO_DB},
        "sync_mode": "batch",
        "force_save": True,
    })


@pytest.fixture(scope="module")
def gcs_dest_id(auth_token: str, gcs_dest) -> str:
    return _create_connection(auth_token, {
        "name": f"e2e-gcs-multi-dest-{uuid.uuid4().hex[:8]}",
        "connection_type": "destination",
        "connector_type": "gcs",
        "connector_version": GCS_CONNECTOR_VERSION,
        "config": {
            "bucket": BUCKET,
            "endpoint_url": f"http://{GCS_EMU}:4443",
            "path_prefix": PATH_PREFIX,
            "file_format": FILE_FORMAT,
        },
        "force_save": True,
    })


@pytest.fixture(scope="module")
def pipeline_id(auth_token: str, mongo_source_id: str, gcs_dest_id: str) -> str:
    return _create_pipeline(auth_token, mongo_source_id, gcs_dest_id)


@pytest.fixture(scope="module")
def landed(auth_token: str, pipeline_id: str) -> dict[str, Any]:
    """Run the pipeline once and read back what reached the bucket. Shared by
    every assertion below so one run backs all of them."""
    _run_pipeline(auth_token, pipeline_id)
    info = _wait_for_completion(pipeline_id)
    result = _read_landed_objects()
    print(f"execution: {info}")
    print("objects under gs://%s/%s/:" % (BUCKET, PATH_PREFIX))
    for k in result["keys"]:
        print("  " + k)
    print(f"rows by table dir: {result['rows_by_dir']}")
    return {"execution": info, **result}


# --------------------------------------------------------------------------- #
# The assertions
# --------------------------------------------------------------------------- #


def test_pipeline_reaches_terminal_success(landed: dict[str, Any]) -> None:
    status = (landed["execution"].get("status") or "").lower()
    assert status in ("completed", "succeeded"), (
        f"pipeline ended non-success: {status!r} "
        f"err={landed['execution'].get('error_message')!r}")


def test_every_collection_gets_its_own_subtree(landed: dict[str, Any]) -> None:
    """The failure this catches: all collections collapsing into one prefix, or
    only the first selected table syncing. Both leave a `completed` run behind."""
    seen = {k.split("/")[3] for k in landed["keys"] if len(k.split("/")) > 4}
    assert seen == set(COLLECTIONS), (
        f"expected one object subtree per collection {sorted(COLLECTIONS)}, "
        f"got {sorted(seen)} — keys: {landed['keys']}")


def test_per_collection_document_counts_are_exact(landed: dict[str, Any]) -> None:
    """Counts are per collection and all different, so a cross-wired subtree
    cannot pass by coincidence."""
    assert landed["rows_by_dir"] == COLLECTIONS, (
        f"per-collection record counts differ from source: "
        f"expected {COLLECTIONS}, landed {landed['rows_by_dir']}")


def test_success_marker_written_per_collection(landed: dict[str, Any]) -> None:
    """`_SUCCESS` is what a downstream reader (Spark/Hive) keys completeness on;
    a partition without one is invisible to them even though the data is there."""
    markers = {k.split("/")[3] for k in landed["keys"] if k.endswith("/_SUCCESS")}
    assert markers == set(COLLECTIONS), (
        f"expected a _SUCCESS marker under every collection partition, got "
        f"{sorted(markers)}")
