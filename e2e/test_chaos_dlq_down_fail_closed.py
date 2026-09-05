#!/usr/bin/env python3
"""
Chaos: destination DATA fault + DLQ unreachable -> sink halts fail-closed.

WHAT THIS TEST IS FOR
  A row the destination rejects on its own merits (constraint violation) is a
  DATA fault: the batcher is allowed to condemn it to the DLQ and commit past
  it. If the DLQ itself is unreachable, there is nowhere safe to put the row,
  so the ONLY correct behaviour is to fail closed: exit without committing.
  This test proves that halt happens, and proves it happened for the DLQ
  reason rather than for any other reason.

WHY IT IS NOT "WRONG_PASSWORD" ANY MORE
  The previous version used a wrong destination password. That text classifies
  as an INFRASTRUCTURE fault (infra_fault.go), so main.go:1680 takes the
  holdForInfraFault branch and blocks for the 300s budget
  (infra_fault.go:221) -- while the test waited 30s. It could never pass, and
  it never exercised the DLQ path at all. An auth error being classified as
  infra is a real product bug, tracked separately as
  KI-CDC-SINK-AUTH-MISCLASSIFIED-AS-INFRA; this test deliberately does not
  depend on it either way.

DESIGN
  1. baseline row lands           -> positive denominator; also makes the sink
                                     auto-create the destination table
  2. add a CHECK constraint at the destination
  3. insert a row that violates it at the source
  4. destination replies "violates check constraint" -> faultData
     (infra_fault.go:96) -> infra hold skipped -> DLQ path
  5. DLQ broker is invalid -> sendToDLQ exhausts -> sinkFailClosed
     (main.go:1741 / :1896) -> os.Exit(1)
  6. auto_restart=False -> the halt stays observable as status="stopped"

DEADLINE (HALT_DEADLINE_S)
  worst case, five stages:
    S0 debezium + flush        ~3s + 5.00s  (main.go:1259 relational override)
    S1 batch retry ladder            7.75s  (main.go:957,958,1828-1841)
    S2 per-row isolation             7.75s  (main.go:1711-1721,1781)
    S3 sendToDLQ exhaustion         12.70s  (main.go:3288 WriteTimeout=3s x4)
    S4 exit -> supervisor -> status  ~5s    (connector.py:745-765,540-553)
    total                          ~41.3s
  120s is ~2.9x that, and is deliberately BELOW the 300s infra budget so a
  misclassified fault fails this test instead of quietly passing it.

ENV KNOBS (used by the red-proof mutations; defaults are the real test)
  CHAOS_DLQ_BROKER   default "invalid-broker:29092". Set to "kafka:29092" to
                     make the DLQ healthy: the test MUST then fail.
  CHAOS_POISON       default "POISON". Set to "ok" to remove the data fault:
                     the test MUST then fail.
  CHAOS_HALT_DEADLINE_S  default 120.
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
KAFKA_CONTAINER = os.environ.get("KAFKA_CONTAINER", f"{PFX}-kafka")
KAFKA_BIN = os.environ.get("KAFKA_BIN", "/opt/kafka/bin")
SINK_MCP_CONTAINER = f"{PFX}-kafka-mcp-sink-v1-0-0-mcp"

PG_DB = os.environ.get("PG_DB", "e2e_db")
PG_USER = os.environ.get("PG_USER", "e2e_user")
PG_PASS = os.environ.get("PG_PASS", "e2e_password")

DLQ_BROKER = os.environ.get("CHAOS_DLQ_BROKER", "invalid-broker:29092")
POISON = os.environ.get("CHAOS_POISON", "POISON")
HALT_DEADLINE_S = int(os.environ.get("CHAOS_HALT_DEADLINE_S", "120"))

# main.go:8485 -- sendToDLQ genuinely exhausted its retries.
LOG_DLQ_EXHAUSTED = "DLQ publish failed after retries"
# main.go:1741 (batch) / :1896 (per-row) -- the fail-closed line itself.
LOG_DLQ_FAILCLOSED = "DLQ publish error"
# The three WRONG-REASON halts. Must be absent.
#   main.go:1703, :3862 -- fail-closed after the infra budget is exhausted
#   main.go:1878        -- per-row isolation hit an infra fault
# Deliberately NOT the bare "infrastructure fault": infra_fault.go:334 logs that
# on EVERY entry to holdForInfraFault, including a transient blip that then
# recovers (:341 logs the recovery). Matching it would red this test for a
# momentary destination hiccup that the code handled correctly. Both markers
# below appear ONLY inside sinkFailClosed calls, so they fire only on a real
# wrong-reason halt.
LOG_INFRA_MARKERS = (
    "infrastructure-fault budget",
    "hit a destination infrastructure fault at row offset",
)


def sh(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True)


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


def pg(sql: str) -> str:
    """Read/write the SHARED e2e postgres. Every object created is dropped in
    the finally block -- this container is a fixture other gated tests reuse."""
    return subprocess.run(
        ["docker", "exec", PG_CONTAINER, "psql", "-U", PG_USER, "-d", PG_DB, "-tAc", sql],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    ).stdout


def dest_schema_of(table: str) -> str:
    """Schema the sink actually created the table in.

    Schema-agnostic on purpose: the sink derives it from the Debezium topic
    (<prefix>.<db>.<table>) and the namespace lock may relocate it. Pinning
    'public.' here would report 'table missing' for a run that landed correctly
    somewhere else -- a false failure that reads exactly like a real one.
    """
    return pg(
        "SELECT table_schema FROM information_schema.tables "
        f"WHERE table_name = '{table}' ORDER BY table_schema LIMIT 1"
    ).strip()


def dest_ids(table: str) -> set[int]:
    schema = dest_schema_of(table)
    if not schema:
        return set()
    out = pg(f'SELECT id FROM "{schema}"."{table}" ORDER BY id')
    return {int(x) for x in out.split() if x.strip().isdigit()}


def dest_dump() -> str:
    return pg(
        "SELECT table_schema||'.'||table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog','information_schema') "
        "ORDER BY 1"
    ).strip() or "(no user tables)"


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


def sink_log(n: int = 6000, scope: str | None = None) -> str:
    """Sink-worker log tail, scoped to ONE run when `scope` is given.

    The sink MCP container is SHARED and long-lived: its log accumulates every
    worker of every gate run on this stack. An absence-assertion over an
    unscoped window is therefore contaminated by earlier runs -- this test
    first went red on an `infrastructure-fault budget` line emitted by a
    PREVIOUS test's worker, while its own halt was correctly the DLQ path.

    Scoping cannot hide a marker: logEvent (main.go:148-159) stamps
    pipeline_id/execution_id on every record from the globals the worker config
    sets, and sinkFailClosed logs through logf -> logEvent, so the infra
    markers below carry the same id as the DLQ ones. And a scope that matched
    NOTHING cannot pass silently -- the positive markers are asserted first, so
    an empty window fails there rather than vacuously satisfying the absence
    check below.
    """
    out = subprocess.run(
        ["docker", "logs", "--tail", str(n), SINK_MCP_CONTAINER],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    ).stdout
    if scope is None:
        return out
    return "\n".join(ln for ln in out.splitlines() if scope in ln)


def assert_halt_was_dlq_caused(logs: str, status: dict[str, Any]) -> None:
    """THE arming assertion.

    status=="stopped" alone is satisfied by ANY exit -- including an
    infrastructure-budget exhaustion or a crash loop -- so on its own it would
    make this test vacuous. These three checks pin the halt to the DLQ reason.
    """
    missing = [s for s in (LOG_DLQ_EXHAUSTED, LOG_DLQ_FAILCLOSED) if s not in logs]
    if missing:
        raise AssertionError(
            "FAIL: the worker halted but NOT for the DLQ reason this test is "
            f"about. Missing log evidence: {missing}. status={status}\n"
            f"{logs[-6000:]}"
        )
    wrong = [m for m in LOG_INFRA_MARKERS if m in logs]
    if wrong:
        raise AssertionError(
            "FAIL: the halt came from the INFRASTRUCTURE-fault path "
            f"({wrong}), not the DLQ path. A constraint violation must "
            "classify as faultData (infra_fault.go destDataFaultMarkers); if "
            "it is reaching holdForInfraFault the classifier regressed.\n"
            f"status={status}\n{logs[-6000:]}"
        )


def main():
    ts = int(time.time())
    table = f"chaos_dlq_{ts}"
    connector = f"debug-chaos-dlq-{ts}"
    topic = f"{connector}.e2e_db.{table}"
    worker_id = f"chaos-dlq-{uuid.uuid4()}"
    started_sink = False
    dest_schema = ""

    try:
        probe_self_check()

        # 1. Source table (MySQL) + a benign baseline row.
        sh([
            "docker", "exec", "-i", MYSQL_CONTAINER, "mysql", "-uroot",
            "-prootpassword", "-e",
            f"""
CREATE DATABASE IF NOT EXISTS e2e_db;
DROP TABLE IF EXISTS e2e_db.{table};
CREATE TABLE e2e_db.{table} (id INT PRIMARY KEY, payload VARCHAR(32));
INSERT INTO e2e_db.{table} VALUES (1, 'ok');
""",
        ])

        # 2. Debezium connector.
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
            "database.server.id": str(9500 + (ts % 1000)),
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

        # 3. Sink: CORRECT destination credentials (an auth error would be
        #    classified as infra and hold for 300s -- see the module docstring),
        #    UNREACHABLE DLQ, no auto-restart so the halt stays observable.
        start = mcp_sink_call("kafka-mcp-sink_start_sink", {
            "config": {
                "topics": [topic],
                "consumer_group": worker_id,
                "kafka_bootstrap_servers": "kafka:29092",
                "dlq_bootstrap_servers": DLQ_BROKER,
                "sink_mode": "cdc",
                "start_offset": "earliest",
                "pipeline_id": worker_id,
                "execution_id": worker_id,
                "destination_connector": "postgresql",
                "destination_version": "v1.0.0",
                "auto_restart": False,
                "destination_config": {
                    "host": PG_CONTAINER,
                    "port": 5432,
                    "database": PG_DB,
                    "user": PG_USER,
                    "password": PG_PASS,
                    "sslmode": "disable",
                },
            }
        })
        assert start.get("success") is True, start
        started_sink = True

        # 4. Baseline must land. Proves the datapath works, so a later halt is
        #    attributable to the poison row and not to a broken fixture. Also
        #    forces the sink to create the destination table for step 5.
        deadline = time.time() + 180
        while time.time() < deadline:
            if 1 in dest_ids(table):
                break
            time.sleep(3)
        else:
            raise AssertionError(
                f"baseline row id=1 never landed in {PG_CONTAINER}; the fixture "
                f"is broken, not the sink.\ndest tables:\n{dest_dump()}\n"
                f"{sink_log(scope=worker_id)}"
            )
        dest_schema = dest_schema_of(table)
        print(f"baseline OK: id=1 landed in {dest_schema}.{table}")

        before = dlq_record_count(topic)
        assert before == 0, f"DLQ already non-empty before the fault: {before}"

        # 5. Arm the DATA fault at the destination.
        pg(f'ALTER TABLE "{dest_schema}"."{table}" '
           f"ADD CONSTRAINT chk_chaos_poison CHECK (payload <> '{POISON}')")
        # Scoped to THIS run's table, not a bare conname lookup: an orphaned
        # constraint left by an interrupted earlier run carries the same name,
        # and would satisfy a global check while this run's table has no
        # constraint at all -- arming the test vacuously. (A global lookup can
        # also return two rows, making the compare below fail for a reason that
        # has nothing to do with the fault.) Table names are per-run unique.
        armed = pg(
            "SELECT 1 FROM pg_constraint c "
            "JOIN pg_class t ON c.conrelid = t.oid "
            "JOIN pg_namespace n ON t.relnamespace = n.oid "
            "WHERE c.conname = 'chk_chaos_poison' "
            f"AND t.relname = '{table}' AND n.nspname = '{dest_schema}'"
        ).strip()
        assert armed == "1", (
            "CHECK constraint was not created -- without it there is no data "
            f"fault and this test would pass vacuously.\n{dest_dump()}"
        )
        print(f"armed: chk_chaos_poison on {dest_schema}.{table}")

        # 6. Poison row at the source.
        sh([
            "docker", "exec", "-i", MYSQL_CONTAINER, "mysql", "-uroot",
            "-prootpassword", "-e",
            f"INSERT INTO e2e_db.{table} VALUES (2, '{POISON}');",
        ])

        # 7. The worker must halt within the derived deadline.
        deadline = time.time() + HALT_DEADLINE_S
        last: dict[str, Any] = {}
        while time.time() < deadline:
            last = mcp_sink_call("kafka-mcp-sink_sink_status",
                                 {"config": {"consumer_group": worker_id}})
            if last.get("status") in ("stopped", "crashed"):
                break
            time.sleep(2)
        else:
            raise AssertionError(
                f"FAIL: worker still running {HALT_DEADLINE_S}s after a row the "
                f"destination rejected, with the DLQ at {DLQ_BROKER}. With "
                f"nowhere safe to put the row the sink must fail closed rather "
                f"than commit past it. last_status={last}\n{sink_log(scope=worker_id)}"
            )
        started_sink = False  # it exited on its own; stop_sink would 404

        logs = sink_log(scope=worker_id)
        assert_halt_was_dlq_caused(logs, last)

        # 8. Nothing may have been dead-lettered: the DLQ was unreachable, so a
        #    non-zero count would mean the row went somewhere we cannot see.
        after = dlq_record_count(topic)
        if after != 0:
            raise AssertionError(
                f"FAIL: {after} record(s) reached {topic}.dlq although the DLQ "
                f"broker was {DLQ_BROKER}.\n{logs[-4000:]}"
            )

        print(f"halt OK: status={last.get('status')}, dlq={after}")
        print("OK")

    finally:
        # This test writes into the SHARED e2e postgres and the SHARED e2e
        # mysql. Leaving the constraint or the table behind poisons every later
        # gated test that reuses them, so teardown is unconditional and each
        # step is independently best-effort.
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
        try:
            schema = dest_schema or dest_schema_of(table)
            if schema:
                pg(f'ALTER TABLE IF EXISTS "{schema}"."{table}" '
                   f"DROP CONSTRAINT IF EXISTS chk_chaos_poison")
                pg(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE')
            # Durable CDC offset ledger (main.go:1466,1555). Guarded so a
            # missing/renamed table is a no-op rather than an error.
            pg(
                "DO $$ DECLARE r record; BEGIN "
                "FOR r IN SELECT table_schema FROM information_schema.tables "
                "WHERE table_name='_rsync_cdc_offsets' LOOP "
                "EXECUTE format('DELETE FROM %I._rsync_cdc_offsets WHERE "
                f"topic LIKE ''{connector}%%''', r.table_schema); "
                "END LOOP; END $$;"
            )
        except Exception:
            pass
        quiet(["docker", "exec", "-i", MYSQL_CONTAINER, "mysql", "-uroot",
               "-prootpassword", "-e", f"DROP TABLE IF EXISTS e2e_db.{table};"])


if __name__ == "__main__":
    main()
