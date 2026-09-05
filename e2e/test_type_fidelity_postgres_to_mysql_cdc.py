#!/usr/bin/env python3
"""
E2E: Type fidelity (Postgres -> MySQL) for CDC data plane.

Validates:
- Debezium Postgres connector emits schema-enabled envelopes
- kafka-mcp-sink derives typed DDL and coerces values (decimal bytes + timestamps)
- MySQL destination table is created with non-TEXT types and receives correct values
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
    v = v.lstrip("v")
    return v.replace(".", "-")


def mcp_container(connector_id: str) -> str:
    v = resolve_current_version(connector_id)
    return f"{PFX}-{connector_id}-v{to_version_part(v)}-mcp"


KAFKA_CONNECT = os.environ.get("CONNECT_URL", "http://localhost:8083")
PG_CONTAINER = os.environ.get("PG_CONTAINER", f"{PFX}-postgres-e2e")
MYSQL_CONTAINER = os.environ.get("MYSQL_CONTAINER", f"{PFX}-mysql-e2e")
SINK_MCP_CONTAINER = f"{PFX}-kafka-mcp-sink-v1-0-0-mcp"
MYSQL_MCP_CONTAINER = mcp_container("mysql")


def sh(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True)


def wait_mcp_health(container: str, timeout_s: int = 60) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            out = sh(
                [
                    "docker",
                    "exec",
                    "-i",
                    container,
                    "/bin/sh",
                    "-lc",
                    "curl -sSf http://localhost:8000/health >/dev/null && echo ok || true",
                ]
            ).strip()
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
                # Fail fast if any task is FAILED (won't self-heal in this test).
                for t in tasks:
                    if t.get("state") == "FAILED":
                        raise RuntimeError(f"Kafka Connect task FAILED: {t.get('trace')}")
        time.sleep(2)
    raise TimeoutError("kafka connect connector did not reach RUNNING")


def start_sink_worker(topic: str, mysql_cfg: dict[str, Any]) -> tuple[str, str]:
    pipeline_id = f"typefidelity-{uuid.uuid4()}"
    # Unique per run: the sink keys workers by consumer_group. Deriving it from a
    # bare int(time.time()) let this test and its reverse-direction sibling collide
    # within the same second at gate parallelism>=2 — the 2nd start_sink then got the
    # 1st worker's metrics_url and never started its own sink. uuid => distinct worker_id.
    consumer_group = f"sink-typefidelity-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "kafka-mcp-sink_start_sink",
            "arguments": {
                "config": {
                    "topics": [topic],
                    "consumer_group": consumer_group,
                    "kafka_bootstrap_servers": "kafka:29092",
                    "sink_mode": "cdc",
                    "start_offset": "earliest",
                    "pipeline_id": pipeline_id,
                    "execution_id": pipeline_id,
                    "destination_connector": "mysql",
                    "destination_version": "v1.0.0",
                    "destination_config": mysql_cfg,
                }
            },
        },
    }
    out = sh(
        [
            "docker",
            "exec",
            "-i",
            SINK_MCP_CONTAINER,
            "python",
            "-c",
            (
                "import json,urllib.request;"
                f"req={json.dumps(payload)};"
                "data=json.dumps(req).encode();"
                "r=urllib.request.Request('http://localhost:8000/mcp',data=data,headers={'Content-Type':'application/json'});"
                "print(urllib.request.urlopen(r,timeout=20).read().decode())"
            ),
        ]
    )
    js = json.loads(out)
    res = js.get("result") or {}
    metrics_url = res.get("metrics_url")
    if not metrics_url:
        raise RuntimeError(f"missing metrics_url in start_sink result: {js}")
    return metrics_url, consumer_group


def wait_sink_processed(metrics_url: str, timeout_s: int = 90) -> dict[str, Any]:
    start = time.time()
    while time.time() - start < timeout_s:
        raw = sh(
            [
                "docker",
                "exec",
                "-i",
                SINK_MCP_CONTAINER,
                "/bin/sh",
                "-lc",
                f"curl -sS {metrics_url} || true",
            ]
        ).strip()
        if raw:
            js = json.loads(raw)
            if int(js.get("processed") or 0) >= 2:
                return js
        time.sleep(2)
    raise TimeoutError("sink worker did not process expected messages")


def main():
    ts = int(time.time())
    table = f"type_fidelity_pg_{ts}"
    connector = f"debug-typefidelity-pg-{ts}"
    topic = f"{connector}.public.{table}"

    wait_mcp_health(MYSQL_MCP_CONTAINER, timeout_s=90)
    wait_mcp_health(SINK_MCP_CONTAINER, timeout_s=90)

    # 1) Create typed source table in Postgres e2e.
    sh(
        [
            "docker",
            "exec",
            "-i",
            PG_CONTAINER,
            "psql",
            "-U",
            "e2e_user",
            "-d",
            "e2e_db",
            "-c",
            f"""
DROP TABLE IF EXISTS public."{table}" CASCADE;
CREATE TABLE public."{table}" (
  id BIGINT PRIMARY KEY,
  amount NUMERIC(10,2),
  created_at TIMESTAMP,
  metadata JSONB
);
INSERT INTO public."{table}" VALUES
  (1, 12.34, '2026-02-02 10:11:12.123', '{{"a": 1}}'),
  (2, 99.99, '2026-02-02 10:11:13.456', '{{"b": "x"}}');
""",
        ]
    )

    # 2) Drop destination table in MySQL.
    sh(
        [
            "docker",
            "exec",
            "-i",
            MYSQL_CONTAINER,
            "mysql",
            "-uroot",
            "-prootpassword",
            "-e",
            f"""
CREATE DATABASE IF NOT EXISTS e2e_db;
DROP TABLE IF EXISTS e2e_db.{table};
""",
        ]
    )

    # 3) Create Debezium Postgres connector (schema-enabled key/value).
    cfg = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "topic.prefix": connector,
        "database.hostname": PG_CONTAINER,
        "database.port": "5432",
        "database.user": "e2e_user",
        "database.password": "e2e_password",
        "database.dbname": "e2e_db",
        "slot.name": f"dbz_slot_{ts}",
        "publication.name": f"dbz_pub_{ts}",
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

    # 4) Start sink worker to MySQL. Force destination table to be in e2e_db (avoid "public" DB in MySQL).
    mysql_cfg = {
        "host": MYSQL_CONTAINER,
        "port": 3306,
        "database": "e2e_db",
        "user": "e2e_user",
        "password": "e2e_password",
        "table": f"e2e_db.{table}",
    }
    metrics_url, _ = start_sink_worker(topic, mysql_cfg)
    status = wait_sink_processed(metrics_url, timeout_s=120)
    assert int(status.get("failed") or 0) == 0, status

    # 5) Assert destination types + values.
    ddl = sh(
        [
            "docker",
            "exec",
            "-i",
            MYSQL_CONTAINER,
            "mysql",
            "-uroot",
            "-prootpassword",
            "-N",
            "-e",
            f"""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='e2e_db' AND table_name='{table}'
ORDER BY ordinal_position;
""",
        ]
    ).strip()
    pairs = [ln.strip().split("\t") for ln in ddl.splitlines() if ln.strip()]
    got = {k: v.lower() for k, v in pairs}
    assert got["id"] in ("bigint", "int"), got
    assert got["amount"] in ("decimal", "numeric"), got
    assert got["created_at"] in ("datetime", "timestamp"), got
    assert got["metadata"] in ("json", "text", "longtext"), got

    vals = sh(
        [
            "docker",
            "exec",
            "-i",
            MYSQL_CONTAINER,
            "mysql",
            "-uroot",
            "-prootpassword",
            "-N",
            "-e",
            f"select id, amount, date_format(created_at, '%Y-%m-%d %H:%i:%s.%f') as created_at, metadata from e2e_db.{table} order by id limit 2;",
        ]
    ).strip()
    lines = [ln.strip() for ln in vals.splitlines() if ln.strip()]
    assert lines[0].startswith("1\t12.34\t2026-02-02 10:11:12.123000"), lines
    assert lines[1].startswith("2\t99.99\t2026-02-02 10:11:13.456000"), lines

    print("OK", got)


if __name__ == "__main__":
    main()

