#!/usr/bin/env python3
"""
E2E: Schema evolution policy (Postgres -> MySQL) for CDC data plane.

Policy for relational sinks:
- ADD COLUMN: auto-applied (nullable) and pipeline continues
- DROP/RENAME/TYPE CHANGE: pipeline halts fail-closed
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
                tasks = js.get("tasks") or []
                if tasks and all(t.get("state") == "RUNNING" for t in tasks):
                    return
                for t in tasks:
                    if t.get("state") == "FAILED":
                        raise RuntimeError(f"Kafka Connect task FAILED: {t.get('trace')}")
        time.sleep(2)
    raise TimeoutError("kafka connect connector did not reach RUNNING")


def mcp_sink_call(tool: str, args: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool, "arguments": args},
    }
    proc = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            SINK_MCP_CONTAINER,
            "python",
            "-c",
            (
                "import sys,json,urllib.request;"
                "req=json.loads(sys.stdin.read());"
                "data=json.dumps(req).encode();"
                "r=urllib.request.Request('http://localhost:8000/mcp',data=data,headers={'Content-Type':'application/json'});"
                "print(urllib.request.urlopen(r,timeout=20).read().decode())"
            ),
        ],
        input=json.dumps(payload),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    out = proc.stdout
    js = json.loads(out)
    return js.get("result") or js


def mysql_scalar(sql: str) -> str:
    proc = subprocess.run(
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
            sql,
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return (proc.stdout or "").strip()


def wait_mysql_predicate(fn, timeout_s: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        if fn():
            return
        time.sleep(2)
    raise TimeoutError("predicate not satisfied")


def main():
    ts = int(time.time())
    table = f"schema_evo_pg_{ts}"
    connector = f"debug-schemaevo-pg-{ts}"
    topic = f"{connector}.public.{table}"

    # 1) Create Postgres source table.
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
  v INT
);
INSERT INTO public."{table}" VALUES (1, 1);
""",
        ]
    )

    # 2) Drop destination MySQL table.
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

    # 3) Start Debezium Postgres connector.
    cfg = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "topic.prefix": connector,
        "database.hostname": PG_CONTAINER,
        "database.port": "5432",
        "database.user": "e2e_user",
        "database.password": "e2e_password",
        "database.dbname": "e2e_db",
        "slot.name": f"dbz_slot_schema_{ts}",
        "publication.name": f"dbz_pub_schema_{ts}",
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

    # 4) Start sink to MySQL (disable auto-restart so halts are observable).
    pipeline_id = f"schemaevo-pg-{uuid.uuid4()}"
    consumer_group = f"sink-schemaevo-pg-{ts}"
    mysql_cfg = {
        "host": MYSQL_CONTAINER,
        "port": 3306,
        "database": "e2e_db",
        "user": "e2e_user",
        "password": "e2e_password",
        "table": f"e2e_db.{table}",
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
                "destination_connector": "mysql",
                "destination_version": "v1.0.0",
                "destination_config": mysql_cfg,
                "auto_restart": False,
            }
        },
    )
    assert start.get("success") is True, start

    # Wait initial apply.
    wait_mysql_predicate(lambda: mysql_scalar(f"SELECT v FROM e2e_db.{table} WHERE id=1;") == "1", timeout_s=180)

    # 5) ADD COLUMN should be auto-applied.
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
            f'ALTER TABLE public."{table}" ADD COLUMN extra INT NULL;',
        ]
    )
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
            f'UPDATE public."{table}" SET v=2, extra=7 WHERE id=1;',
        ]
    )

    def extra_ok() -> bool:
        typ = mysql_scalar(
            f"""
SELECT data_type
FROM information_schema.columns
WHERE table_schema='e2e_db' AND table_name='{table}' AND column_name='extra';
""".strip()
        )
        if not typ:
            return False
        val = mysql_scalar(f"SELECT extra FROM e2e_db.{table} WHERE id=1;")
        return val == "7"

    wait_mysql_predicate(extra_ok, timeout_s=240)

    # 6) DROP COLUMN should HALT the pipeline.
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
            f'ALTER TABLE public."{table}" DROP COLUMN extra;',
        ]
    )
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
            f'UPDATE public."{table}" SET v=3 WHERE id=1;',
        ]
    )

    deadline = time.time() + 30
    last = None
    while time.time() < deadline:
        st = mcp_sink_call("kafka-mcp-sink_sink_status", {"config": {"consumer_group": consumer_group}})
        last = st
        if st.get("status") == "stopped":
            print("OK")
            return
        time.sleep(2)

    raise TimeoutError(f"worker did not halt within 30s; last_status={last}")


if __name__ == "__main__":
    main()

