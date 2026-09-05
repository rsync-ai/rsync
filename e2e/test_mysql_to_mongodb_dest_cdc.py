#!/usr/bin/env python3
"""
E2E: MySQL -> MongoDB DESTINATION for the CDC data plane.

Same as test_postgres_to_mongodb_dest_cdc.py but with a MySQL source, so the
kafka-mcp-sink's document-DB destination lane (isDocumentDBConnector, PR #467) is
proven for a second relational source:
- Debezium MySqlConnector emits snapshot + insert/update/delete
- the sink routes them through the doc-DB lane (NO ensure_table DDL, NO synthetic
  `_rsync_row_hash` PK) into the `mongodb` MCP connector
- rows land in MongoDB as flat documents keyed on the source PK (`id`)
- snapshot rows land, a streamed insert lands, an update mutates the doc, a delete
  removes it

Mirrors test_delete_correctness_mysql_to_postgres_cdc.py (direct-sink harness,
debug-* connector name) with the destination flipped to MongoDB. Writes to the
local e2e `mongo-e2e` (rs0): pymongo does RS discovery off the seed and routes
writes to the advertised member `mongo-e2e:27017`, so the destination host is that
alias. Uses a db distinct from the mongo-*source* test's `e2e_db`.
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
MYSQL_CONTAINER = os.environ.get("MYSQL_CONTAINER", f"{PFX}-mysql-e2e")
MONGO_CONTAINER = f"{PFX}-mongo-e2e"   # docker exec target for mongosh assertions
# Write host in destination_config: the rs0-advertised member alias (pymongo routes
# writes here after topology discovery); the mongodb MCP connector resolves it.
MONGO_HOST = "mongo-e2e"
SINK_MCP_CONTAINER = f"{PFX}-kafka-mcp-sink-v1-0-0-mcp"
DEST_DB = "mongodest_e2e"


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
            v = path[len("versions/") :].strip()
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


MONGO_MCP_CONTAINER = mcp_container("mongodb")
MONGO_VERSION = resolve_current_version("mongodb")


def sh(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True)


def mysql_exec(sql: str) -> None:
    sh(["docker", "exec", "-i", MYSQL_CONTAINER, "mysql", "-uroot", "-prootpassword", "-e", sql])


def mongo_eval(js: str) -> str:
    return sh(["docker", "exec", "-i", MONGO_CONTAINER, "mongosh", "--quiet", "--eval",
               f"db = db.getSiblingDB('{DEST_DB}'); {js}"]).strip()


def mongo_count(coll: str, filt: str = "{}") -> int:
    return int(mongo_eval(f"print(db['{coll}'].countDocuments({filt}))"))


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
        if r.status_code == 200:
            js = r.json()
            if js.get("connector", {}).get("state") == "RUNNING":
                tasks = js.get("tasks") or []
                if tasks and all(t.get("state") == "RUNNING" for t in tasks):
                    return
                for t in tasks:
                    if t.get("state") == "FAILED":
                        raise RuntimeError(f"Kafka Connect task FAILED: {t.get('trace')}")
        time.sleep(2)
    raise TimeoutError("kafka connect connector did not reach RUNNING")


def start_sink_worker(topic: str, mongo_cfg: dict[str, Any]) -> str:
    pipeline_id = f"mysql-mongo-dest-e2e-{uuid.uuid4()}"
    consumer_group = f"sink-mysql-mongo-dest-e2e-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "kafka-mcp-sink_start_sink", "arguments": {"config": {
            "topics": [topic],
            "consumer_group": consumer_group,
            "kafka_bootstrap_servers": "kafka:29092",
            "sink_mode": "cdc",
            "start_offset": "earliest",
            "pipeline_id": pipeline_id,
            "execution_id": pipeline_id,
            "destination_connector": "mongodb",
            "destination_version": MONGO_VERSION,
            "destination_config": mongo_cfg,
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


def sink_metrics(metrics_url: str) -> dict[str, Any]:
    raw = sh(["docker", "exec", "-i", SINK_MCP_CONTAINER, "/bin/sh", "-lc",
              f"curl -sS {metrics_url} || true"]).strip()
    return json.loads(raw) if raw else {}


def wait_sink_at_least(metrics_url: str, min_processed: int, timeout_s: int = 150) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        m = sink_metrics(metrics_url)
        if int(m.get("processed") or 0) >= min_processed:
            return
        time.sleep(2)
    raise TimeoutError(f"sink did not process >= {min_processed} (last: {sink_metrics(metrics_url)})")


def wait_mongo_count(coll: str, want: int, filt: str = "{}", timeout_s: int = 120) -> None:
    start = time.time()
    last = None
    while time.time() - start < timeout_s:
        last = mongo_count(coll, filt)
        if last == want:
            return
        time.sleep(2)
    raise TimeoutError(f"mongo {coll} filter {filt}: want {want}, got {last}")


def main():
    ts = int(time.time())
    table = f"orders_my_{ts}"           # source table AND dest collection
    connector = f"debug-mymongo-{ts}"   # NOT cdc-* (orchestrator reaper skips debug-*)
    topic = f"{connector}.e2e_db.{table}"

    wait_mcp_health(MONGO_MCP_CONTAINER, timeout_s=90)
    wait_mcp_health(SINK_MCP_CONTAINER, timeout_s=90)

    # 1) Source table in MySQL.
    mysql_exec(
        "CREATE DATABASE IF NOT EXISTS e2e_db;"
        f"DROP TABLE IF EXISTS e2e_db.{table};"
        f"CREATE TABLE e2e_db.{table} (id INT PRIMARY KEY, name VARCHAR(50), qty INT);"
        f"INSERT INTO e2e_db.{table} VALUES (1,'a',10),(2,'b',20);"
    )

    # 2) Destination cleanup.
    mongo_eval(f"db['{table}'].drop();")

    # 3) Debezium MySQL connector.
    cfg = {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        "topic.prefix": connector,
        "database.hostname": MYSQL_CONTAINER,
        "database.port": "3306",
        "database.user": "e2e_user",
        "database.password": "e2e_password",
        "database.include.list": "e2e_db",
        "table.include.list": f"e2e_db.{table}",
        # Unique server.id base (5000): every MySQL-source gate test picks a distinct
        # base (6000/7000/8000/8200/9500) so two Debezium connectors never share a
        # server.id on the shared rsync-ai-mysql-e2e binlog under gate parallelism.
        "database.server.id": str(5000 + (ts % 1000)),
        "snapshot.mode": "initial",
        "include.schema.changes": "true",
        "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
        "schema.history.internal.kafka.topic": f"schemahistory.{connector}",
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

    # 4) Start the sink into the MongoDB dest (cdc mode, doc-DB lane).
    mongo_cfg = {
        "host": MONGO_HOST,        # rs0-advertised member; pymongo routes writes here
        "port": 27017,
        "database": DEST_DB,
        "table": table,
    }
    metrics_url = start_sink_worker(topic, mongo_cfg)

    # 5) Snapshot: 2 docs upserted (flat rows keyed on `id`).
    try:
        wait_sink_at_least(metrics_url, min_processed=2, timeout_s=150)
        wait_mongo_count(table, 2, timeout_s=120)
    except Exception:
        raise RuntimeError(f"snapshot did not land; sink={sink_metrics(metrics_url)} "
                           f"count={mongo_count(table)}")
    # Flat-doc shape (NOT packed {_id,document}): `name` top-level == 'a', no
    # top-level `document` key.
    name1 = mongo_eval(f"print(db['{table}'].findOne({{id:1}}).name)")
    assert name1 == "a", f"flat top-level `name` != 'a' (got {name1!r}); doc-DB lane regressed?"
    packed = mongo_eval(f"print(db['{table}'].findOne({{id:1}}).document === undefined)")
    assert packed == "true", "doc regressed to packed {_id,document} shape (has top-level `document`)"

    # 6) Stream an insert -> 3 docs.
    mysql_exec(f"INSERT INTO e2e_db.{table} VALUES (3,'c',30);")
    wait_sink_at_least(metrics_url, min_processed=3, timeout_s=120)
    wait_mongo_count(table, 3, timeout_s=120)

    # 7) Stream an update -> doc mutated in place (no duplicate).
    mysql_exec(f"UPDATE e2e_db.{table} SET name='A', qty=99 WHERE id=1;")
    wait_sink_at_least(metrics_url, min_processed=4, timeout_s=120)
    wait_mongo_count(table, 1, "{id:1, name:'A'}", timeout_s=120)
    assert mongo_count(table) == 3, "update must not duplicate the doc"

    # 8) Stream a delete of id=2 -> doc removed.
    mysql_exec(f"DELETE FROM e2e_db.{table} WHERE id=2;")
    wait_sink_at_least(metrics_url, min_processed=5, timeout_s=120)
    wait_mongo_count(table, 0, "{id:2}", timeout_s=120)
    assert mongo_count(table) == 2, f"delete left wrong count: {mongo_count(table)}"

    print("OK")


if __name__ == "__main__":
    main()
