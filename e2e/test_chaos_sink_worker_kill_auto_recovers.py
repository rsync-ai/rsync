#!/usr/bin/env python3
"""
Chaos: kill the Go sink worker process -> supervisor restarts it -> pipeline continues.

This protects against the biggest operational failure mode: the pipeline "looks running"
but stops applying changes because the sink worker died.
"""

import json
import os
import subprocess
import time
import uuid
from typing import Any

import requests


PFX = os.environ.get("STACK_PREFIX", "rsync-ai")
KAFKA_CONNECT = os.environ.get("CONNECT_URL", "http://localhost:8083")
MYSQL_CONTAINER = os.environ.get("MYSQL_CONTAINER", f"{PFX}-mysql-e2e")
PG_CONTAINER = os.environ.get("PG_CONTAINER", f"{PFX}-postgres-e2e")
SINK_MCP_CONTAINER = f"{PFX}-kafka-mcp-sink-v1-0-0-mcp"


def sh(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True)


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


def mcp_sink_call(tool: str, args: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool, "arguments": args},
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
    return js.get("result") or js


def wait_pg_value(table: str, expected: int, timeout_s: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        proc = subprocess.run(
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
                f'SELECT v FROM public."{table}" WHERE id=1;',
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        raw = (proc.stdout or "").strip()
        if raw and raw.isdigit() and int(raw) == expected:
            return
        time.sleep(2)
    raise TimeoutError(f"destination did not reach expected v={expected}")


def main():
    ts = int(time.time())
    table = f"chaos_kill_{ts}"
    connector = f"debug-chaos-kill-{ts}"
    topic = f"{connector}.e2e_db.{table}"

    # 1) Create MySQL source table.
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
  v INT
);
INSERT INTO e2e_db.{table} VALUES (1, 1);
""",
        ]
    )

    # 2) Drop destination table for clean DDL.
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

    # 3) Start Debezium MySQL connector (schema-enabled).
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
        "database.server.id": str(8000 + (ts % 1000)),
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

    # 4) Start sink worker (auto_restart default true).
    pipeline_id = f"kill-{uuid.uuid4()}"
    consumer_group = f"sink-kill-{ts}"
    pg_cfg = {
        "host": PG_CONTAINER,
        "port": 5432,
        "database": "e2e_db",
        "user": "e2e_user",
        "password": "e2e_password",
        "sslmode": "disable",
    }
    start = mcp_sink_call(
        "kafka-mcp-sink_start_sink",
        {
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
    )
    assert start.get("success") is True, start

    # 5) Wait initial replication to destination.
    wait_pg_value(table, expected=1, timeout_s=120)

    # 6) Kill the Go worker process inside the sink MCP container.
    st0 = mcp_sink_call("kafka-mcp-sink_sink_status", {"config": {"consumer_group": consumer_group}})
    pid = int(st0.get("pid") or 0)
    assert pid > 1, st0
    sh(["docker", "exec", "-i", SINK_MCP_CONTAINER, "/bin/sh", "-lc", f"kill -9 {pid} || true"])

    # 7) Trigger an update; supervisor should restart and apply it.
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
            f"UPDATE e2e_db.{table} SET v=2 WHERE id=1;",
        ]
    )
    wait_pg_value(table, expected=2, timeout_s=120)

    # Also assert supervisor recorded at least one restart attempt.
    st = mcp_sink_call("kafka-mcp-sink_sink_status", {"config": {"consumer_group": consumer_group}})
    assert int(st.get("restart_attempts") or 0) >= 1, st

    print("OK")


if __name__ == "__main__":
    main()

