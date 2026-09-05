#!/usr/bin/env python3
"""
Chaos: destination DOWN, DLQ HEALTHY -> live CDC rows must NOT be dead-lettered.

This is the mirror image of test_chaos_dlq_down_fail_closed.py, and it covers the
case that regression KI-CDC-SINK-INFRA-FAULT-DLQ-COMMITS was filed for:

  test_chaos_dlq_down_fail_closed.py  -> DLQ broker unreachable  (fail-closed, correct)
  this test                           -> DESTINATION unreachable, DLQ perfectly healthy

Before the fix, the second case was silent data loss. `cdcDBBatcher.flushBatch`
retried for ~7s, then condemned `lastErr` unconditionally: every message went to
`<topic>.dlq`, the offsets were committed, and the high-water mark advanced. Kafka
never redelivers a committed offset and nothing consumes the DLQ topic, so the rows
were gone while the worker stayed up and the pipeline stayed green.

The discriminator this test asserts on is therefore NOT "did the worker survive"
(it survives either way) but "where did the rows end up":

  pre-fix : DLQ record count > 0, and rows 2..N NEVER appear at the destination
  post-fix: DLQ record count == 0, and rows 2..N appear once the destination is back

To keep shared infrastructure untouched we stop a THROWAWAY Postgres container
created for this test, never rsync-ai-postgres-e2e.

Requires the e2e DB stack (docker-compose.e2e.dbs.yml) and a kafka-mcp-sink image
built from worker-src containing infra_fault.go.
"""

import json
import os
import subprocess
import time
import uuid
from typing import Any

import requests


PFX = os.environ.get("STACK_PREFIX", "rsync-ai")
NETWORK = os.environ.get("NETWORK", f"{PFX}_default")
KAFKA_CONNECT = os.environ.get("CONNECT_URL", "http://localhost:8083")
MYSQL_CONTAINER = os.environ.get("MYSQL_CONTAINER", f"{PFX}-mysql-e2e")
KAFKA_CONTAINER = os.environ.get("KAFKA_CONTAINER", f"{PFX}-kafka")
KAFKA_BIN = os.environ.get("KAFKA_BIN", "/opt/kafka/bin")
SINK_MCP_CONTAINER = f"{PFX}-kafka-mcp-sink-v1-0-0-mcp"

# Throwaway destination Postgres. Named per-run so a crashed previous run cannot
# collide, and torn down in the finally block.
TS = int(time.time())
DEST_PG = f"{PFX}-chaos-destpg-{TS}"
DEST_DB = "destdb"
DEST_USER = "destuser"
DEST_PASS = "destpass"

# How long the destination stays down. Must be comfortably below the worker's
# infrastructure-fault budget (RSYNC_SINK_INFRA_RETRY_SECONDS, default 300s) so
# the fixed worker is still holding when we bring the destination back, and
# comfortably ABOVE the ordinary flushBatch retry budget (~7s) so the pre-fix
# code would definitely have condemned the batch by then.
OUTAGE_SECONDS = int(os.environ.get("CHAOS_OUTAGE_SECONDS", "45"))


def sh(cmd: list[str], **kw) -> str:
    return subprocess.check_output(cmd, text=True, **kw)


def quiet(cmd: list[str]) -> None:
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


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
                "print(urllib.request.urlopen(r,timeout=30).read().decode())"
            ),
        ],
        input=json.dumps(payload),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    js = json.loads(proc.stdout)
    return js.get("result") or js


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


def dlq_record_count(topic: str) -> int:
    """Records currently in <topic>.dlq.

    Uses the shipped wrapper kafka-get-offsets.sh, which execs the JAVA class
    org.apache.kafka.tools.GetOffsetShell. The Scala entry point
    kafka.tools.GetOffsetShell was REMOVED in Kafka 3.x -- calling it returns
    rc=1 with no output, which under a `2>/dev/null || true` reads as 0 for
    EVERY topic. A zero here must be earned, so an unexpected probe failure
    raises instead of being laundered into a count.
    """
    proc = subprocess.run(
        [
            "docker", "exec", KAFKA_CONTAINER, "bash", "-lc",
            f"{KAFKA_BIN}/kafka-get-offsets.sh "
            f"--bootstrap-server kafka:29092 --topic '{topic}.dlq'",
        ],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        err = (proc.stderr or "").strip()
        # A never-created topic is a legitimate 0 and reports exactly this.
        if "Could not match any topic-partitions" in err:
            return 0
        raise RuntimeError(
            f"dlq_record_count probe failed for {topic}.dlq "
            f"(rc={proc.returncode}): {err[:400]}"
        )
    total = 0
    for line in proc.stdout.splitlines():
        # format: <topic>:<partition>:<end-offset>
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[0] == f"{topic}.dlq":
            try:
                total += int(parts[2])
            except ValueError:
                pass
    return total


def probe_self_check() -> None:
    """Positive denominator for dlq_record_count.

    Proves the probe can return a NON-zero number in this environment before
    any assertion relies on it returning zero. Uses an internal Kafka Connect
    topic, which always exists and always has records by the time a connector
    is RUNNING.
    """
    proc = subprocess.run(
        [
            "docker", "exec", KAFKA_CONTAINER, "bash", "-lc",
            f"{KAFKA_BIN}/kafka-get-offsets.sh "
            f"--bootstrap-server kafka:29092 --topic '_rsync-connect-configs'",
        ],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    ok = proc.returncode == 0 and any(
        len(l.split(":")) == 3 and l.split(":")[2].strip().isdigit()
        and int(l.split(":")[2]) > 0
        for l in proc.stdout.splitlines()
    )
    if not ok:
        raise AssertionError(
            "offset probe self-check FAILED: it cannot report a non-zero count "
            "for a topic known to have records, so every 0 it returns later "
            f"would be meaningless. rc={proc.returncode} "
            f"stdout={proc.stdout!r} stderr={proc.stderr[:300]!r}"
        )


def dest_psql(sql: str) -> str:
    return subprocess.run(
        ["docker", "exec", DEST_PG, "psql", "-U", DEST_USER, "-d", DEST_DB, "-tAc", sql],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    ).stdout


def dest_rows(table: str) -> set[int]:
    """ids currently present at the throwaway destination (empty set if absent).

    Schema-agnostic on purpose: the sink derives the destination schema from the
    Debezium topic (`<prefix>.<db>.<table>` -> schema `<db>`) and the namespace
    lock may relocate it again. Pinning `public.` here would report "no rows" for
    a run that landed correctly somewhere else — a false failure that reads
    exactly like the data loss this test exists to catch.
    """
    schema = dest_psql(
        "SELECT table_schema FROM information_schema.tables "
        f"WHERE table_name = '{table}' ORDER BY table_schema LIMIT 1"
    ).strip()
    if not schema:
        return set()
    out = dest_psql(f'SELECT id FROM "{schema}"."{table}" ORDER BY id')
    return {int(x) for x in out.split() if x.strip().isdigit()}


def dest_dump() -> str:
    """Every user table at the destination — the diagnostic for a baseline miss."""
    return dest_psql(
        "SELECT table_schema||'.'||table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog','information_schema') "
        "ORDER BY 1"
    ).strip() or "(no user tables)"


def wait_dest_ready(timeout_s: int = 60) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = subprocess.run(
            ["docker", "exec", DEST_PG, "pg_isready", "-U", DEST_USER, "-d", DEST_DB],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        if r.returncode == 0:
            return
        time.sleep(2)
    raise TimeoutError(f"{DEST_PG} did not become ready")


def sink_log_tail(n: int = 60) -> str:
    return subprocess.run(
        ["docker", "logs", "--tail", str(n), SINK_MCP_CONTAINER],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    ).stdout


def main():
    table = f"chaos_destdown_{TS}"
    connector = f"debug-chaos-destdown-{TS}"
    topic = f"{connector}.e2e_db.{table}"
    worker_id = f"chaos-destdown-{uuid.uuid4()}"
    started_sink = False

    try:
        # ------------------------------------------------------------------
        # 1. Throwaway destination Postgres, on the same network the MCP
        #    connectors resolve e2e hostnames on. Never touch shared infra.
        # ------------------------------------------------------------------
        quiet(["docker", "rm", "-f", DEST_PG])
        sh([
            "docker", "run", "-d", "--name", DEST_PG, "--network", NETWORK,
            "-e", f"POSTGRES_USER={DEST_USER}",
            "-e", f"POSTGRES_PASSWORD={DEST_PASS}",
            "-e", f"POSTGRES_DB={DEST_DB}",
            "postgres:16-alpine",
        ])
        wait_dest_ready()

        # ------------------------------------------------------------------
        # 2. MySQL source + Debezium
        # ------------------------------------------------------------------
        sh([
            "docker", "exec", "-i", MYSQL_CONTAINER, "mysql", "-uroot", "-prootpassword", "-e",
            f"""
CREATE DATABASE IF NOT EXISTS e2e_db;
DROP TABLE IF EXISTS e2e_db.{table};
CREATE TABLE e2e_db.{table} (id INT PRIMARY KEY, v INT);
INSERT INTO e2e_db.{table} VALUES (1, 1);
""",
        ])

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
            "database.server.id": str(9600 + (TS % 1000)),
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
        r = requests.post(f"{KAFKA_CONNECT}/connectors",
                          json={"name": connector, "config": cfg}, timeout=30)
        if r.status_code not in (200, 201, 409):
            raise RuntimeError(f"failed to create connector: {r.status_code} {r.text}")
        wait_kc_running(connector)

        # ------------------------------------------------------------------
        # 3. Sink with a HEALTHY DLQ. This is the exact configuration in which
        #    the bug lost data: a reachable dlq_bootstrap_servers is what turned
        #    the sibling batcher's safe crash into a silent drop.
        # ------------------------------------------------------------------
        start = mcp_sink_call("kafka-mcp-sink_start_sink", {
            "config": {
                "topics": [topic],
                "consumer_group": worker_id,
                "kafka_bootstrap_servers": "kafka:29092",
                "dlq_bootstrap_servers": "kafka:29092",   # HEALTHY on purpose
                "sink_mode": "cdc",
                "start_offset": "earliest",
                "pipeline_id": worker_id,
                "execution_id": worker_id,
                "destination_connector": "postgresql",
                "destination_version": "v1.0.0",
                # No auto-restart: if the worker ever does exit we want to see
                # "stopped" rather than a supervisor masking it.
                "auto_restart": False,
                "destination_config": {
                    "host": DEST_PG,
                    "port": 5432,
                    "database": DEST_DB,
                    "user": DEST_USER,
                    "password": DEST_PASS,
                    "sslmode": "disable",
                },
            }
        })
        assert start.get("success") is True, start
        started_sink = True

        # ------------------------------------------------------------------
        # 4. Baseline: the snapshot row must land. Proves the datapath works,
        #    so a later "row missing" is attributable to the outage and not to
        #    a broken fixture.
        # ------------------------------------------------------------------
        deadline = time.time() + 180
        while time.time() < deadline:
            if 1 in dest_rows(table):
                break
            time.sleep(3)
        else:
            raise AssertionError(
                f"baseline row id=1 never landed in {DEST_PG}; fixture is broken, "
                f"not the fix.\ndest tables:\n{dest_dump()}\n"
                f"dlq={dlq_record_count(topic)}\n{sink_log_tail()}"
            )
        print(f"baseline OK: id=1 landed in {DEST_PG}.public.{table}")

        # Arm the probe before anything trusts a 0 from it: dlq_record_count
        # silently returned 0 for EVERY topic while it called the Scala class
        # Kafka 3.x removed, so all three DLQ assertions below were vacuous.
        probe_self_check()

        dlq_before = dlq_record_count(topic)
        assert dlq_before == 0, f"DLQ already non-empty before the outage: {dlq_before}"

        # ------------------------------------------------------------------
        # 5. Destination down. Live CDC rows are produced while it is down.
        # ------------------------------------------------------------------
        sh(["docker", "stop", DEST_PG])
        print(f"destination {DEST_PG} stopped")

        sh([
            "docker", "exec", "-i", MYSQL_CONTAINER, "mysql", "-uroot", "-prootpassword", "-e",
            f"INSERT INTO e2e_db.{table} VALUES (2, 2), (3, 3);",
        ])

        time.sleep(OUTAGE_SECONDS)

        # THE assertion the KI is about. Pre-fix this is > 0: every message the
        # batcher could not write was dead-lettered and its offset committed.
        dlq_during = dlq_record_count(topic)
        status_during = mcp_sink_call("kafka-mcp-sink_sink_status",
                                      {"config": {"consumer_group": worker_id}})
        if dlq_during != 0:
            raise AssertionError(
                f"FAIL: {dlq_during} record(s) dead-lettered to {topic}.dlq while the "
                f"destination was merely unreachable. An infrastructure fault must hold "
                f"offsets, not condemn rows (KI-CDC-SINK-INFRA-FAULT-DLQ-COMMITS). "
                f"status={status_during}\n{sink_log_tail()}"
            )
        print(f"during outage OK: dlq={dlq_during}, sink status={status_during.get('status')}")

        # ------------------------------------------------------------------
        # 6. Destination back. The held rows must now land.
        # ------------------------------------------------------------------
        sh(["docker", "start", DEST_PG])
        wait_dest_ready()
        print(f"destination {DEST_PG} restarted")

        deadline = time.time() + 240
        seen: set[int] = set()
        while time.time() < deadline:
            seen = dest_rows(table)
            if {1, 2, 3} <= seen:
                break
            time.sleep(5)
        else:
            raise AssertionError(
                f"FAIL: rows written during the outage never arrived after recovery. "
                f"dest ids={sorted(seen)}, expected superset of {{1,2,3}}. "
                f"dlq={dlq_record_count(topic)}\n{sink_log_tail()}"
            )

        dlq_after = dlq_record_count(topic)
        if dlq_after != 0:
            raise AssertionError(
                f"FAIL: rows landed but {dlq_after} record(s) were also dead-lettered "
                f"to {topic}.dlq.\n{sink_log_tail()}"
            )

        print(f"after recovery OK: dest ids={sorted(seen)}, dlq={dlq_after}")
        print("OK")

    finally:
        if started_sink:
            try:
                mcp_sink_call("kafka-mcp-sink_stop_sink",
                              {"config": {"consumer_group": worker_id}})
            except Exception:
                pass
        try:
            requests.delete(f"{KAFKA_CONNECT}/connectors/{connector}", timeout=10)
        except Exception:
            pass
        quiet(["docker", "rm", "-f", DEST_PG])
        quiet(["docker", "exec", "-i", MYSQL_CONTAINER, "mysql", "-uroot", "-prootpassword",
               "-e", f"DROP TABLE IF EXISTS e2e_db.{table};"])


if __name__ == "__main__":
    main()
