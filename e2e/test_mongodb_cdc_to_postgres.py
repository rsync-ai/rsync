#!/usr/bin/env python3
"""
E2E: MongoDB -> Postgres CDC data plane (change streams -> packed _id+document JSONB).

Validates the MongoDB CDC source path end to end:
- Debezium MongoDbConnector (change_streams_update_full) emits snapshot + I/U/D
- kafka-mcp-sink decodes the JSON-string `after`/`before` into the packed
  {_id TEXT, document JSONB} shape (decodeMongoDocument / normalizeMongoID) and
  applies it to Postgres
- ObjectId `_id` lands as a 24-hex string (not a {"$oid":..} blob)
- snapshot rows land, a streamed insert lands, and a streamed delete is applied

Runs against the local e2e replica set `mongo-e2e` (rs0) by default; override the
container/DSN env vars to point at any running RS. Mirrors the mysql/pg CDC tests
(standalone, uuid/timestamp-isolated, debug-* connector name so the orchestrator
reaper leaves it alone).
"""

import json
import os
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

import requests

PFX = os.environ.get("STACK_PREFIX", "rsync-ai")
KAFKA_CONNECT = os.environ.get("CONNECT_URL", "http://localhost:8083")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
MONGO_CONTAINER = os.environ.get("MONGO_CONTAINER", f"{PFX}-mongo-e2e")
# Hostname:port Debezium uses to reach the RS (network alias), NOT the docker name.
MONGO_CONN_STR = os.environ.get("MONGO_CONN_STR", "mongodb://mongo-e2e:27017/?replicaSet=rs0")
PG_CONTAINER = os.environ.get("PG_CONTAINER", f"{PFX}-postgres-e2e")
PG_HOST = os.environ.get("PG_HOST", f"{PFX}-postgres-e2e")
PG_USER = os.environ.get("PG_USER", "e2e_user")
PG_PASS = os.environ.get("PG_PASS", "e2e_password")
PG_DB = os.environ.get("PG_DB", "e2e_db")
SINK_MCP_CONTAINER = os.environ.get("SINK_MCP_CONTAINER", f"{PFX}-kafka-mcp-sink-v1-0-0-mcp")


def resolve_current_version(connector_id: str) -> str:
    repo_root = Path(__file__).resolve().parents[1]
    public_root = repo_root / "shared" / "mcp-connectors" / "public"
    candidates: list[Path] = []
    direct = public_root / connector_id / "latest.json"
    if direct.is_file():
        candidates.append(direct)
    candidates.extend(public_root.rglob(f"{connector_id}/latest.json"))
    if not candidates:
        raise FileNotFoundError(f"latest.json not found for connector: {connector_id}")

    def score(p: Path) -> tuple[int, int]:
        s = p.as_posix()
        return (1 if "/database/" in s else 0, len(s))

    best = sorted({c.resolve() for c in candidates}, key=score)[0]
    latest = json.loads(best.read_text())
    v = (latest.get("current_version") or "").strip()
    if not v:
        path = (latest.get("path") or "").strip()
        if path.startswith("versions/"):
            v = path[len("versions/"):].strip()
    if not v:
        v = (latest.get("version") or "").strip()
    if not v:
        raise ValueError(f"missing version in {best}")
    return v if v.startswith("v") else f"v{v}"


def to_version_part(v: str) -> str:
    return v.lstrip("v").replace(".", "-")


def mcp_container(connector_id: str) -> str:
    v = resolve_current_version(connector_id)
    return f"{PFX}-{connector_id}-v{to_version_part(v)}-mcp"


PG_MCP_CONTAINER = mcp_container("postgresql")


def sh(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True)


def mongosh(js: str) -> str:
    return sh(["docker", "exec", "-i", MONGO_CONTAINER, "mongosh", "--quiet", "--eval", js])


def psql(sql: str) -> str:
    return sh(
        ["docker", "exec", "-i", PG_CONTAINER, "psql", "-U", PG_USER, "-d", PG_DB, "-t", "-A", "-c", sql]
    ).strip()


def wait_mcp_health(container: str, timeout_s: int = 90) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            out = sh(["docker", "exec", "-i", container, "/bin/sh", "-lc",
                      "curl -sSf http://localhost:8000/health >/dev/null && echo ok || true"]).strip()
            if out == "ok":
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(f"{container} did not become healthy")


def wait_kc_running(connector: str, timeout_s: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        r = requests.get(f"{KAFKA_CONNECT}/connectors/{connector}/status", timeout=10)
        if r.status_code == 200 and r.json().get("connector", {}).get("state") == "RUNNING":
            return
        time.sleep(2)
    raise TimeoutError("kafka connect connector did not reach RUNNING")


def start_sink_worker(topic: str, pg_cfg: dict[str, Any], namespace: str) -> str:
    pipeline_id = f"mongo-cdc-e2e-{uuid.uuid4()}"
    consumer_group = f"sink-mongo-cdc-e2e-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "kafka-mcp-sink_start_sink", "arguments": {"config": {
            "topics": [topic],
            "consumer_group": consumer_group,
            "kafka_bootstrap_servers": KAFKA_BOOTSTRAP,
            "sink_mode": "cdc",
            "start_offset": "earliest",
            "pipeline_id": pipeline_id,
            "execution_id": pipeline_id,
            "destination_connector": "postgresql",
            "destination_version": "v1.0.0",
            "destination_namespace": namespace,
            "destination_config": pg_cfg,
        }}},
    }
    out = sh(["docker", "exec", "-i", SINK_MCP_CONTAINER, "python", "-c", (
        "import json,urllib.request;"
        f"req={json.dumps(payload)};"
        "data=json.dumps(req).encode();"
        "r=urllib.request.Request('http://localhost:8000/mcp',data=data,headers={'Content-Type':'application/json'});"
        "print(urllib.request.urlopen(r,timeout=25).read().decode())"
    )])
    res = json.loads(out).get("result") or {}
    metrics_url = res.get("metrics_url")
    if not metrics_url:
        raise RuntimeError(f"missing metrics_url in start_sink result: {out}")
    return metrics_url


def wait_sink_at_least(metrics_url: str, min_processed: int, timeout_s: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        raw = sh(["docker", "exec", "-i", SINK_MCP_CONTAINER, "/bin/sh", "-lc",
                  f"curl -sS {metrics_url} || true"]).strip()
        if raw and int(json.loads(raw).get("processed") or 0) >= min_processed:
            return
        time.sleep(2)
    raise TimeoutError("sink worker did not process expected messages")


def main():
    ts = int(time.time())
    coll = f"orders_{ts}"
    connector = f"debug-mongo-{ts}"          # NOT cdc-* (orchestrator reaper skips debug-*)
    topic = f"{connector}.e2e_db.{coll}"     # mongo topic = <prefix>.<db>.<collection>
    namespace = f"mongo_e2e_{ts}"

    wait_mcp_health(PG_MCP_CONTAINER)
    wait_mcp_health(SINK_MCP_CONTAINER)

    # 1) Seed a MongoDB collection (2 docs, ObjectId _id + nested doc).
    mongosh(
        "db = db.getSiblingDB('e2e_db');"
        f"db.{coll}.drop();"
        f"db.{coll}.insertMany(["
        "  {item:'a', qty: 1, meta:{k:'v'}},"
        "  {item:'b', qty: 2, tags:['x','y']}"
        "]);"
    )

    # 2) Destination cleanup.
    subprocess.run(["docker", "exec", "-i", PG_CONTAINER, "psql", "-U", PG_USER, "-d", PG_DB,
                    "-c", f'DROP SCHEMA IF EXISTS {namespace} CASCADE;'], check=False)

    # 3) Debezium MongoDbConnector (change streams, full post-image).
    cfg = {
        "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
        "tasks.max": "1",
        "topic.prefix": connector,
        "mongodb.connection.string": MONGO_CONN_STR,
        "database.include.list": "e2e_db",
        "collection.include.list": f"e2e_db.{coll}",
        "capture.mode": "change_streams_update_full",
        "snapshot.mode": "initial",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter.schemas.enable": "true",
    }
    requests.delete(f"{KAFKA_CONNECT}/connectors/{connector}", timeout=10)
    r = requests.post(f"{KAFKA_CONNECT}/connectors", json={"name": connector, "config": cfg}, timeout=30)
    if r.status_code not in (200, 201, 409):
        raise RuntimeError(f"failed to create connector: {r.status_code} {r.text}")
    wait_kc_running(connector)

    # 4) Start the sink (cdc mode) into a fresh dest schema.
    pg_cfg = {"host": PG_HOST, "port": 5432, "database": PG_DB,
              "user": PG_USER, "password": PG_PASS, "sslmode": "disable"}
    metrics_url = start_sink_worker(topic, pg_cfg, namespace)

    # 5) Snapshot: 2 docs applied.
    wait_sink_at_least(metrics_url, min_processed=2, timeout_s=150)
    dest = f'{namespace}."{coll}"'
    cnt = psql(f"SELECT COUNT(*) FROM {dest};")
    assert cnt == "2", f"snapshot count: {cnt}"

    # packed shape: _id (text, 24-hex ObjectId) + document (jsonb)
    cols = psql(
        "SELECT string_agg(column_name, ',' ORDER BY column_name) "
        f"FROM information_schema.columns WHERE table_schema='{namespace}' AND table_name='{coll}';"
    )
    assert "_id" in cols and "document" in cols, f"packed columns missing: {cols}"
    idlen = psql(f"SELECT length(_id) FROM {dest} LIMIT 1;")
    assert idlen == "24", f"_id is not a 24-hex ObjectId string: {idlen}"
    docok = psql(f"SELECT document->>'item' FROM {dest} WHERE document->>'item'='a';")
    assert docok == "a", f"document JSONB not applied: {docok!r}"

    # 6) Stream an insert -> 3 rows.
    mongosh(f"db.getSiblingDB('e2e_db').{coll}.insertOne({{item:'c', qty: 3}});")
    wait_sink_at_least(metrics_url, min_processed=3, timeout_s=120)
    assert psql(f"SELECT COUNT(*) FROM {dest};") == "3", "insert not applied"

    # 7) Stream a delete of doc 'b' -> row removed.
    mongosh(f"db.getSiblingDB('e2e_db').{coll}.deleteOne({{item:'b'}});")
    wait_sink_at_least(metrics_url, min_processed=4, timeout_s=120)
    gone = psql(f"SELECT COUNT(*) FROM {dest} WHERE document->>'item'='b';")
    assert gone == "0", f"delete not applied: {gone}"

    print("OK")


if __name__ == "__main__":
    main()
