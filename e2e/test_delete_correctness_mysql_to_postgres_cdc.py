#!/usr/bin/env python3
"""
E2E: Delete correctness (MySQL -> Postgres) for CDC data plane.

Validates:
- Debezium MySQL emits delete events
- sink applies delete deterministically using PK from Kafka key
- destination row is removed
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
MYSQL_CONTAINER = os.environ.get("MYSQL_CONTAINER", f"{PFX}-mysql-e2e")
PG_CONTAINER = os.environ.get("PG_CONTAINER", f"{PFX}-postgres-e2e")
SINK_MCP_CONTAINER = f"{PFX}-kafka-mcp-sink-v1-0-0-mcp"
PG_MCP_CONTAINER = mcp_container("postgresql")


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
                return
        time.sleep(2)
    raise TimeoutError("kafka connect connector did not reach RUNNING")


def start_sink_worker(topic: str, pg_cfg: dict[str, Any]) -> str:
    pipeline_id = f"delete-e2e-{uuid.uuid4()}"
    # Unique per run: the sink keys workers by consumer_group. Deriving it from a
    # bare int(time.time()) let this test and its reverse-direction sibling collide
    # within the same second at gate parallelism>=2 — the 2nd start_sink then got the
    # 1st worker's metrics_url and never started its own sink. uuid => distinct worker_id.
    consumer_group = f"sink-delete-e2e-{int(time.time())}-{uuid.uuid4().hex[:8]}"
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
                    "destination_connector": "postgresql",
                    "destination_version": "v1.0.0",
                    "destination_config": pg_cfg,
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
    return metrics_url


def wait_sink_at_least(metrics_url: str, min_processed: int, timeout_s: int = 120) -> None:
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
            if int(js.get("processed") or 0) >= min_processed:
                return
        time.sleep(2)
    raise TimeoutError("sink worker did not process expected messages")


def main():
    ts = int(time.time())
    table = f"delete_test_{ts}"
    connector = f"debug-delete-mysql-{ts}"
    topic = f"{connector}.e2e_db.{table}"

    wait_mcp_health(PG_MCP_CONTAINER, timeout_s=90)
    wait_mcp_health(SINK_MCP_CONTAINER, timeout_s=90)

    # 1) Source table in MySQL
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
CREATE TABLE e2e_db.{table} (
  id INT PRIMARY KEY,
  name VARCHAR(50)
);
INSERT INTO e2e_db.{table} VALUES (1, 'a'), (2, 'b');
""",
        ]
    )

    # 2) Destination cleanup
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
            f'DROP TABLE IF EXISTS public."{table}" CASCADE;',
        ]
    )

    # 3) Debezium connector
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
        "database.server.id": str(7000 + (ts % 1000)),
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

    # 4) Start sink worker
    pg_cfg = {
        "host": PG_CONTAINER,
        "port": 5432,
        "database": "e2e_db",
        "user": "e2e_user",
        "password": "e2e_password",
        "sslmode": "disable",
    }
    metrics_url = start_sink_worker(topic, pg_cfg)

    # Wait for at least 2 snapshot events applied.
    wait_sink_at_least(metrics_url, min_processed=2, timeout_s=120)

    # 5) Delete one row in MySQL
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
            f"DELETE FROM e2e_db.{table} WHERE id=2;",
        ]
    )

    # Wait for delete to apply (processed count increases).
    wait_sink_at_least(metrics_url, min_processed=3, timeout_s=120)

    # 6) Assert destination row removed.
    out = sh(
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
            "-t",
            "-A",
            "-c",
            f'SELECT COUNT(*) FROM public."{table}" WHERE id=2;',
        ]
    ).strip()
    assert out == "0", out

    print("OK")


if __name__ == "__main__":
    main()

