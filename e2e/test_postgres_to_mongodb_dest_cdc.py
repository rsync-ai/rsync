#!/usr/bin/env python3
"""
E2E: Postgres -> MongoDB DESTINATION for the CDC data plane.

Exercises the kafka-mcp-sink's document-DB destination lane (isDocumentDBConnector,
PR #467) end to end with a RELATIONAL source:
- Debezium PostgresConnector emits snapshot + insert/update/delete
- the sink routes them through the doc-DB lane (NO ensure_table DDL, NO synthetic
  `_rsync_row_hash` PK) into the `mongodb` MCP connector's import/upsert/delete
- rows land in MongoDB as flat documents keyed on the source PK (`id`), NOT the
  packed {_id,document} shape that a Mongo *source* produces
- snapshot rows land, a streamed insert lands, a streamed update mutates the doc,
  and a streamed delete removes it

Mirrors test_delete_correctness_postgres_to_mysql_cdc.py (same direct-sink harness,
debug-* connector name so the orchestrator reaper leaves it alone) but flips the
destination to MongoDB. Writes to the local e2e `mongo-e2e` (rs0): pymongo does RS
discovery off the seed and routes writes to the RS-advertised member
`mongo-e2e:27017`, so the destination host is that alias (resolvable by the mongodb
MCP connector on the shared network). Reuses the postgres-e2e source the other PG
CDC tests use.
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
PG_CONTAINER = os.environ.get("PG_CONTAINER", f"{PFX}-postgres-e2e")
MONGO_CONTAINER = f"{PFX}-mongo-e2e"   # docker exec target for mongosh assertions
# The write host in destination_config: pymongo discovers the rs0 topology off this
# seed and routes writes to the member the RS advertises (`mongo-e2e:27017`), so use
# that alias directly — the mongodb MCP connector resolves it on the shared network.
MONGO_HOST = "mongo-e2e"
SINK_MCP_CONTAINER = f"{PFX}-kafka-mcp-sink-v1-0-0-mcp"
# The MongoDB write target: db from destination_config, collection derived from the
# topic (single-namespace dest). Keep the db distinct from the mongo-*source* test's
# `e2e_db` so the two never collide when the gate runs them together.
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


def psql(sql: str) -> None:
    sh(["docker", "exec", "-i", PG_CONTAINER, "psql", "-U", "e2e_user", "-d", "e2e_db", "-c", sql])


def mongo_eval(js: str) -> str:
    """Run mongosh against the destination RS node and return trimmed stdout."""
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
    pipeline_id = f"pg-mongo-dest-e2e-{uuid.uuid4()}"
    consumer_group = f"sink-pg-mongo-dest-e2e-{int(time.time())}-{uuid.uuid4().hex[:8]}"
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
    """The sink reports 'processed' before the async destination write necessarily
    lands; poll the actual collection instead of trusting the processed counter."""
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
    table = f"orders_pg_{ts}"          # source table AND (schema-stripped) dest collection
    connector = f"debug-pgmongo-{ts}"  # NOT cdc-* (orchestrator reaper skips debug-*)
    topic = f"{connector}.public.{table}"

    wait_mcp_health(MONGO_MCP_CONTAINER, timeout_s=90)
    wait_mcp_health(SINK_MCP_CONTAINER, timeout_s=90)

    # 1) Source table in Postgres (REPLICA IDENTITY FULL so updates/deletes carry
    #    the key + before-image reliably).
    psql(
        f'DROP TABLE IF EXISTS public."{table}" CASCADE;'
        f'CREATE TABLE public."{table}" (id BIGINT PRIMARY KEY, name TEXT, qty INT);'
        f'ALTER TABLE public."{table}" REPLICA IDENTITY FULL;'
        f"INSERT INTO public.\"{table}\" VALUES (1,'a',10),(2,'b',20);"
    )

    # 2) Destination cleanup: drop the collection in the mongo dest db.
    mongo_eval(f"db['{table}'].drop();")

    # 3) Debezium Postgres connector.
    cfg = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "topic.prefix": connector,
        "database.hostname": PG_CONTAINER,
        "database.port": "5432",
        "database.user": "e2e_user",
        "database.password": "e2e_password",
        "database.dbname": "e2e_db",
        "slot.name": f"dbz_slot_pgmongo_{ts}",
        "publication.name": f"dbz_pub_pgmongo_{ts}",
        "publication.autocreate.mode": "filtered",
        "table.include.list": f"public.{table}",
        "snapshot.mode": "initial",
        "include.schema.changes": "true",
        "plugin.name": "pgoutput",
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

    # 5) Snapshot: 2 docs upserted (flat rows keyed on the PG PK `id`).
    try:
        wait_sink_at_least(metrics_url, min_processed=2, timeout_s=150)
        wait_mongo_count(table, 2, timeout_s=120)
    except Exception:
        raise RuntimeError(f"snapshot did not land; sink={sink_metrics(metrics_url)} "
                           f"count={mongo_count(table)}")
    # Flat-doc shape (NOT the packed {_id,document} shape a Mongo *source* produces):
    # `name` must be a TOP-LEVEL field == 'a', and there must be NO top-level
    # `document` key — a substring match on the whole doc would also pass on the
    # packed shape, so assert both explicitly.
    name1 = mongo_eval(f"print(db['{table}'].findOne({{id:1}}).name)")
    assert name1 == "a", f"flat top-level `name` != 'a' (got {name1!r}); doc-DB lane regressed?"
    packed = mongo_eval(f"print(db['{table}'].findOne({{id:1}}).document === undefined)")
    assert packed == "true", "doc regressed to packed {_id,document} shape (has top-level `document`)"

    # 6) Stream an insert -> 3 docs.
    psql(f"INSERT INTO public.\"{table}\" VALUES (3,'c',30);")
    wait_sink_at_least(metrics_url, min_processed=3, timeout_s=120)
    wait_mongo_count(table, 3, timeout_s=120)

    # 7) Stream an update -> doc mutated in place (upsert keyed on id, not duplicated).
    psql(f"UPDATE public.\"{table}\" SET name='A', qty=99 WHERE id=1;")
    wait_sink_at_least(metrics_url, min_processed=4, timeout_s=120)
    wait_mongo_count(table, 1, "{id:1, name:'A'}", timeout_s=120)
    assert mongo_count(table) == 3, "update must not duplicate the doc"

    # 8) Stream a delete of id=2 -> doc removed (doc-DB delete lane).
    psql(f'DELETE FROM public."{table}" WHERE id=2;')
    wait_sink_at_least(metrics_url, min_processed=5, timeout_s=120)
    wait_mongo_count(table, 0, "{id:2}", timeout_s=120)
    assert mongo_count(table) == 2, f"delete left wrong count: {mongo_count(table)}"

    print("OK")


if __name__ == "__main__":
    main()
