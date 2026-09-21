"""
MongoDB → GCS CDC pipeline, end-to-end through the control plane, including a
delete-and-recreate on the same connections.

Why this test exists
--------------------
Two MongoDB → GCS CDC pipelines on a self-host install (2026-09-16) showed
"Running" and wrote 0 files. Every existing MongoDB CDC test drives Debezium and
the sink directly (e2e/test_mongodb_cdc_to_postgres.py), and every GCS CDC probe
is MySQL through a shell script (e2e/test_db_cdc_to_emulated_gcs_azure.sh). The
path the user actually took — POST /connections → POST /pipelines (cdc) → /run,
then DELETE the pipeline and create a new one on the same source — had no
coverage at all, so "wrote 0 files" could not be reproduced or ruled out.

What earns the test
-------------------
- Files land for the snapshot AND for a document inserted after the stream
  started, counted per collection by a marker only this run writes. A pipeline
  that reports Running but writes nothing fails here.
- The object key's database segment is the MongoDB database name, not
  `mongodb` or `default` (issue #13).
- After DELETE, the pipeline's Debezium connector (`cdc-<id8>`) is gone from
  Kafka Connect and its sink consumer group (`sink-<id8>`) has no live member.
- A SECOND pipeline on the same connections lands its own snapshot and live
  insert. This is the order of events that preceded the 0-file pipelines.

Object layout under test (kafka-sink-worker cdcObjectKey):

    <path_prefix>/<database>/<collection>/<YYYY-MM-DD>[/HH]/<ts>-<offset>.jsonl

Credential-free by design: the destination is a fake-gcs-server emulator and the
gcs connector falls back to AnonymousCredentials without a service_account_json.

Not in the deterministic merge gate (it provisions its own mongod replica set
and fake-gcs-server); listed in run_gate.sh UNGATED_TESTS. Against the isolated
CI stack:

    API_GATEWAY_URL=http://localhost:15001 CONNECT_URL=http://localhost:18083 \
    E2E_MCP_NET=rsync-ci-mcp E2E_CONNECT_NET=rsync-ci_default \
    E2E_MONGO_MCP=rsync-ci-mongodb-v1-0-0-mcp E2E_GCS_MCP=rsync-ci-gcs-v1-0-0-mcp \
    E2E_SINK_MCP=rsync-ci-kafka-mcp-sink-v1-0-0-mcp E2E_KAFKA_CONTAINER=rsync-ci-kafka \
    E2E_CONTROL_PG_CONTAINER=rsync-ci-postgres \
    pytest e2e/test_mongodb_cdc_to_gcs.py -v -s

Skips cleanly (never fails) when docker, api-gateway, Kafka Connect, or a
required MCP container is missing. The delete step also skips when the delete
response says its teardown was refused (HTTP 401/403): the stack's api-gateway
has no INTERNAL_SERVICE_SECRET, so nothing was torn down and a leak can't be judged.
"""

from __future__ import annotations

import gzip
import json
import os
import subprocess
import time
import urllib.parse
import uuid
from typing import Any

import pytest
import requests


# --------------------------------------------------------------------------- #
# Configuration (all overridable via env)
# --------------------------------------------------------------------------- #

API_GATEWAY_URL = os.getenv("API_GATEWAY_URL", "http://localhost:5001")
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")

MCP_NET = os.getenv("E2E_MCP_NET", "rsync-ai-mcp")
# Kafka Connect is not on the MCP network; the mongod fixture joins both.
CONNECT_NET = os.getenv("E2E_CONNECT_NET", "rsync-ai_default")
MONGO_MCP = os.getenv("E2E_MONGO_MCP", "rsync-ai-mongodb-v1-0-0-mcp")
GCS_MCP = os.getenv("E2E_GCS_MCP", "rsync-ai-gcs-v1-0-0-mcp")
SINK_MCP = os.getenv("E2E_SINK_MCP", "rsync-ai-kafka-mcp-sink-v1-0-0-mcp")
KAFKA_CONTAINER = os.getenv("E2E_KAFKA_CONTAINER", "kafka")
KAFKA_BOOTSTRAP = os.getenv("E2E_KAFKA_BOOTSTRAP", "localhost:9092")
MONGO_CONNECTOR_VERSION = os.getenv("E2E_MONGO_CONNECTOR_VERSION", "v1.0.0")
GCS_CONNECTOR_VERSION = os.getenv("E2E_GCS_CONNECTOR_VERSION", "v1.0.0")

RUN_TAG = uuid.uuid4().hex[:8]
MONGO_SRV = os.getenv("E2E_MONGO_SRV", "mongo-cdc-gcs-e2e")
GCS_EMU = os.getenv("E2E_GCS_EMU", "gcs-cdc-fake")
MONGO_DB = os.getenv("E2E_MONGO_DB", f"shop_{RUN_TAG}")
BUCKET = os.getenv("E2E_GCS_BUCKET", "cdc-dest")
PATH_PREFIX = f"mongo_cdc_{RUN_TAG}"

# Different sizes so a cross-wired collection cannot match by accident.
COLLECTIONS: dict[str, int] = {"orders": 6, "customers": 4}

LOGIN_EMAIL = os.getenv("E2E_USER_EMAIL", "default@rsync-ai.local")
LOGIN_PASSWORD = os.getenv("E2E_USER_PASSWORD", "password123")

# Snapshot needs connector start + snapshot + one flush (30s default).
LAND_TIMEOUT_S = int(os.getenv("E2E_LAND_TIMEOUT_S", "420"))
TEARDOWN_TIMEOUT_S = int(os.getenv("E2E_TEARDOWN_TIMEOUT_S", "120"))
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


def _reachable(url: str) -> bool:
    try:
        return requests.get(url, timeout=3).ok
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
    if not _reachable(f"{API_GATEWAY_URL}/health"):
        return f"api-gateway not reachable at {API_GATEWAY_URL}"
    if not _reachable(f"{CONNECT_URL}/connectors"):
        return f"kafka connect not reachable at {CONNECT_URL}"
    for name in (MONGO_MCP, GCS_MCP, SINK_MCP, KAFKA_CONTAINER):
        if not _container_running(name):
            return f"container '{name}' not running"
    return None


pytestmark = pytest.mark.skipif(_skip_reason() is not None, reason=_skip_reason() or "")


# --------------------------------------------------------------------------- #
# Fixture helpers
# --------------------------------------------------------------------------- #


def _mongosh(script: str) -> str:
    return subprocess.check_output(
        ["docker", "exec", MONGO_SRV, "mongosh", "--quiet", "--eval", script],
        text=True, stderr=subprocess.STDOUT,
    )


def _docker_rm(name: str) -> None:
    subprocess.run(["docker", "rm", "-f", name],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _gcs_base() -> str:
    port = subprocess.check_output(["docker", "port", GCS_EMU, "4443/tcp"], text=True)
    return "http://" + port.strip().splitlines()[0]


def _insert_docs(coll: str, tag: str, n: int) -> None:
    """Insert n docs whose `marker` is `<tag><coll><i>` — alphanumeric, so it
    survives any JSON re-encoding the sink applies."""
    docs = ",".join(
        f'{{idx:{i + 1}, marker:"{tag}{coll}{i + 1:04d}"}}' for i in range(n))
    out = _mongosh(f'db = db.getSiblingDB("{MONGO_DB}"); '
                   f'print("inserted=" + Object.keys(db.{coll}.insertMany([{docs}]).insertedIds).length)')
    assert f"inserted={n}" in out, f"insert into {coll} failed: {out!r}"


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


def _create_and_run_pipeline(token: str, source_id: str, dest_id: str, label: str) -> str:
    resp = requests.post(
        f"{API_GATEWAY_URL}/api/v1/pipelines", params={"allow_draft": "true"},
        json={
            "name": f"mongo-cdc-gcs-{label}-{RUN_TAG}",
            "request": f"Stream the {', '.join(COLLECTIONS)} collections from MongoDB "
                       f"database {MONGO_DB} into a GCS bucket",
            "source_connection_id": source_id,
            "destination_connection_id": dest_id,
            "sync_mode": "cdc",
            # Bare names, exactly as the table picker returns them for MongoDB.
            "selected_tables": sorted(COLLECTIONS),
        },
        cookies={"auth_token": token}, timeout=60)
    assert resp.ok, f"create pipeline failed: {resp.status_code} {resp.text}"
    data = resp.json()
    pipe_id = data.get("id") or (data.get("pipeline") or {}).get("id")
    assert pipe_id, f"no pipeline id in response: {data}"

    resp = requests.post(f"{API_GATEWAY_URL}/api/v1/pipelines/{pipe_id}/run",
                         params={"allow_draft": "true", "ack_warnings": "true"},
                         json={"ack_warnings": True}, cookies={"auth_token": token}, timeout=60)
    assert resp.ok, f"run pipeline failed: {resp.status_code} {resp.text}"
    print(f"[{label}] pipeline {pipe_id} started")
    return pipe_id


def _delete_pipeline(token: str, pipe_id: str) -> requests.Response:
    return requests.delete(f"{API_GATEWAY_URL}/api/v1/pipelines/{pipe_id}",
                           cookies={"auth_token": token}, timeout=120)


def _delete_warnings(resp: requests.Response) -> list[str]:
    """The delete's teardown warnings (deletePipelineResponse); [] when there are none."""
    try:
        return [str(w) for w in (resp.json().get("warnings") or [])]
    except ValueError:
        return []


def _query_control_plane(sql: str) -> str:
    return subprocess.check_output(
        ["docker", "exec", "-i", CONTROL_PLANE_DB_CONTAINER,
         "psql", "-U", CONTROL_PLANE_DB_USER, "-d", CONTROL_PLANE_DB_NAME, "-tAc", sql],
        text=True,
    ).strip()


def _pipeline_state(pipe_id: str) -> str:
    """Status + latest execution error, for failure messages only."""
    try:
        return _query_control_plane(
            "SELECT p.status || ' | exec=' || COALESCE(e.status,'-') || ' | ' || "
            "COALESCE(left(e.error_message, 300), '') FROM pipelines p "
            "LEFT JOIN LATERAL (SELECT status, error_message FROM executions "
            f"WHERE pipeline_id = p.id ORDER BY start_time DESC LIMIT 1) e ON true "
            f"WHERE p.id = '{pipe_id}';")
    except subprocess.CalledProcessError as exc:
        return f"<control-plane query failed: {exc}>"


# --------------------------------------------------------------------------- #
# Kafka-side teardown probes (names only — never connector configs)
# --------------------------------------------------------------------------- #


def _connect_connectors() -> list[str]:
    resp = requests.get(f"{CONNECT_URL}/connectors", timeout=10)
    resp.raise_for_status()
    return resp.json()


def _sink_groups_with_members(id8: str) -> dict[str, int]:
    """{group: active member count} for every consumer group naming this pipeline."""
    kcg = ["docker", "exec", KAFKA_CONTAINER, "/opt/kafka/bin/kafka-consumer-groups.sh",
           "--bootstrap-server", KAFKA_BOOTSTRAP]
    groups = [g.strip() for g in subprocess.check_output(kcg + ["--list"], text=True).splitlines()
              if id8 in g]
    counts: dict[str, int] = {}
    for g in groups:
        out = subprocess.run(kcg + ["--describe", "--group", g, "--members"],
                             capture_output=True, text=True).stdout
        # One row per member: "<group> <consumer-id> <host> <client-id> <#partitions>".
        # The consumer id is "<client-id>-<uuid>", so match the group column exactly
        # (the header row's first column is "GROUP"; an empty group prints a notice).
        counts[g] = sum(1 for ln in out.splitlines()
                        if len(ln.split()) >= 5 and ln.split()[0] == g)
    return counts


# --------------------------------------------------------------------------- #
# Readback: what actually landed in the bucket
# --------------------------------------------------------------------------- #


def _landed() -> dict[str, Any]:
    """List every object under this run's prefix. Returns
    {"keys": [...], "db_segments": {...}, "text_by_table": {table: all record text}}."""
    base = _gcs_base()
    keys: list[str] = []
    token = None
    while True:
        params = {"prefix": f"{PATH_PREFIX}/"}
        if token:
            params["pageToken"] = token
        resp = requests.get(f"{base}/storage/v1/b/{BUCKET}/o", params=params, timeout=10)
        resp.raise_for_status()
        body = resp.json()
        keys += [item["name"] for item in body.get("items", [])]
        token = body.get("nextPageToken")
        if not token:
            break

    db_segments: set[str] = set()
    text_by_table: dict[str, str] = {}
    for key in sorted(keys):
        rel = key[len(PATH_PREFIX) + 1:].split("/")
        if len(rel) < 3 or rel[-1].startswith("_"):
            continue  # markers / manifests are not data
        db_segments.add(rel[0])
        raw = requests.get(
            f"{base}/storage/v1/b/{BUCKET}/o/{urllib.parse.quote(key, safe='')}",
            params={"alt": "media"}, timeout=10).content
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        text_by_table[rel[1]] = text_by_table.get(rel[1], "") + raw.decode("utf-8", "replace")
    return {"keys": sorted(keys), "db_segments": sorted(db_segments),
            "text_by_table": text_by_table}


def _wait_for_markers(pipe_id: str, label: str, tag: str) -> dict[str, Any]:
    """Wait until every document `<tag><coll><i>` has landed for every collection."""
    want = {coll: [f"{tag}{coll}{i + 1:04d}" for i in range(n)]
            for coll, n in COLLECTIONS.items()}
    started = time.time()
    missing: dict[str, int] = {}
    while time.time() - started < LAND_TIMEOUT_S:
        got = _landed()
        missing = {coll: sum(1 for m in markers if m not in got["text_by_table"].get(coll, ""))
                   for coll, markers in want.items()}
        if not any(missing.values()):
            print(f"[{label}] all '{tag}' docs landed after {int(time.time() - started)}s "
                  f"({len(got['keys'])} objects)")
            return got
        time.sleep(POLL_INTERVAL_S)
    got = _landed()
    raise AssertionError(
        f"[{label}] '{tag}' docs did not land within {LAND_TIMEOUT_S}s; missing per "
        f"collection={missing}; objects={len(got['keys'])}; "
        f"db segments={got['db_segments']}; pipeline={_pipeline_state(pipe_id)}")


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def mongo_source():
    """A throwaway single-node replica set (change streams need one), reachable
    under one hostname from both the MCP network and Kafka Connect's network."""
    _docker_rm(MONGO_SRV)
    subprocess.check_call(
        ["docker", "run", "-d", "--name", MONGO_SRV, "--network", MCP_NET,
         "mongo:6", "mongod", "--replSet", "rs0", "--bind_ip_all"],
        stdout=subprocess.DEVNULL)
    if CONNECT_NET != MCP_NET:
        subprocess.check_call(["docker", "network", "connect", CONNECT_NET, MONGO_SRV])
    for _ in range(30):
        try:
            _mongosh("db.runCommand({ping:1}).ok")
            break
        except subprocess.CalledProcessError:
            time.sleep(2)
    else:
        pytest.fail(f"mongod {MONGO_SRV} did not become ready")
    _mongosh(f'rs.initiate({{_id:"rs0", members:[{{_id:0, host:"{MONGO_SRV}:27017"}}]}})')
    for _ in range(30):
        if "true" in _mongosh("db.hello().isWritablePrimary"):
            break
        time.sleep(2)
    else:
        pytest.fail("replica set did not elect a primary")
    for coll, n in COLLECTIONS.items():
        _insert_docs(coll, "seed", n)
    yield
    _docker_rm(MONGO_SRV)


@pytest.fixture(scope="module")
def gcs_dest():
    _docker_rm(GCS_EMU)
    subprocess.check_call(
        ["docker", "run", "-d", "--name", GCS_EMU, "--network", MCP_NET,
         "-p", "127.0.0.1::4443",
         "fsouza/fake-gcs-server:latest", "-scheme", "http", "-port", "4443",
         "-external-url", f"http://{GCS_EMU}:4443"],
        stdout=subprocess.DEVNULL)
    for _ in range(30):
        try:
            resp = requests.post(f"{_gcs_base()}/storage/v1/b", params={"project": "e2e"},
                                 json={"name": BUCKET}, timeout=5)
            if resp.ok or resp.status_code == 409:
                break
        except (requests.RequestException, subprocess.CalledProcessError):
            pass
        time.sleep(2)
    else:
        pytest.fail(f"fake-gcs-server {GCS_EMU} did not become ready")
    yield
    _docker_rm(GCS_EMU)


@pytest.fixture(scope="module")
def auth_token() -> str:
    return _login()


@pytest.fixture(scope="module")
def connections(auth_token: str, mongo_source, gcs_dest) -> tuple[str, str]:
    src = _create_connection(auth_token, {
        "name": f"e2e-mongo-cdc-src-{RUN_TAG}",
        "connection_type": "source",
        "connector_type": "mongodb",
        "connector_version": MONGO_CONNECTOR_VERSION,
        "config": {"host": MONGO_SRV, "port": 27017, "database": MONGO_DB,
                   "replica_set": "rs0"},
        "sync_mode": "cdc",
        "force_save": True,
    })
    dst = _create_connection(auth_token, {
        "name": f"e2e-gcs-cdc-dest-{RUN_TAG}",
        "connection_type": "destination",
        "connector_type": "gcs",
        "connector_version": GCS_CONNECTOR_VERSION,
        "config": {"bucket": BUCKET, "endpoint_url": f"http://{GCS_EMU}:4443",
                   "path_prefix": PATH_PREFIX, "file_format": "jsonl"},
        "force_save": True,
    })
    return src, dst


@pytest.fixture(scope="module")
def first_pipeline(auth_token: str, connections: tuple[str, str]):
    pipe_id = _create_and_run_pipeline(auth_token, *connections, label="first")
    state = {"id": pipe_id, "deleted": False}
    yield state
    if not state["deleted"]:
        _delete_pipeline(auth_token, pipe_id)


# --------------------------------------------------------------------------- #
# The assertions — ordered; each builds on the state the previous one left.
# --------------------------------------------------------------------------- #


def test_first_pipeline_lands_snapshot_and_live_insert(first_pipeline) -> None:
    pipe_id = first_pipeline["id"]
    _wait_for_markers(pipe_id, "first", "seed")
    for coll, n in COLLECTIONS.items():
        _insert_docs(coll, "livea", n)
    got = _wait_for_markers(pipe_id, "first", "livea")
    # Issue #13: the database segment is the MongoDB database, not "mongodb"/"default".
    assert got["db_segments"] == [MONGO_DB], (
        f"object keys use database segment(s) {got['db_segments']}, want [{MONGO_DB!r}]; "
        f"sample keys: {got['keys'][:5]}")


def test_delete_releases_connector_and_sink(auth_token: str, first_pipeline) -> None:
    pipe_id = first_pipeline["id"]
    id8 = pipe_id.replace("-", "")[:8].lower()
    assert f"cdc-{id8}" in _connect_connectors(), "connector missing before delete"
    before = _sink_groups_with_members(id8)
    assert any(before.values()), f"no live sink consumer before delete: {before}"

    resp = _delete_pipeline(auth_token, pipe_id)
    assert resp.ok, f"delete failed: {resp.status_code} {resp.text}"
    first_pipeline["deleted"] = True

    # The delete's source and Kafka teardown are internal api-gateway → orchestrator
    # calls. A stack whose api-gateway has no INTERNAL_SERVICE_SECRET gets them
    # refused (401/403), reported as "… did not finish (HTTP 403): …". Nothing was
    # torn down then, so a live sink member says nothing about the delete path.
    warnings = _delete_warnings(resp)
    refused = [w for w in warnings if "(HTTP 401)" in w or "(HTTP 403)" in w]
    if refused:
        pytest.skip("teardown refused by the orchestrator — this stack's api-gateway "
                    f"is not configured with INTERNAL_SERVICE_SECRET: {refused}")

    started = time.time()
    connectors, groups = _connect_connectors(), _sink_groups_with_members(id8)
    while time.time() - started < TEARDOWN_TIMEOUT_S:
        connectors, groups = _connect_connectors(), _sink_groups_with_members(id8)
        if f"cdc-{id8}" not in connectors and not any(groups.values()):
            print(f"teardown complete after {int(time.time() - started)}s; groups={groups}")
            return
        time.sleep(POLL_INTERVAL_S)
    pytest.fail(f"after delete: connector still present={f'cdc-{id8}' in connectors}, "
                f"live sink members={groups}, delete warnings={warnings}")


def test_recreated_pipeline_on_same_connections_lands_files(
        auth_token: str, connections: tuple[str, str], first_pipeline) -> None:
    assert first_pipeline["deleted"], "delete step did not run"
    # Docs written while no pipeline exists must still arrive via the new snapshot.
    for coll, n in COLLECTIONS.items():
        _insert_docs(coll, "gap", n)
    pipe_id = _create_and_run_pipeline(auth_token, *connections, label="second")
    try:
        _wait_for_markers(pipe_id, "second", "gap")
        for coll, n in COLLECTIONS.items():
            _insert_docs(coll, "liveb", n)
        _wait_for_markers(pipe_id, "second", "liveb")
    finally:
        _delete_pipeline(auth_token, pipe_id)
