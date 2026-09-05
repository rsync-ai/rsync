#!/usr/bin/env python3
"""
E2E: ClickHouse BATCH round-trip via the PRODUCT pipeline API (both roles).

ClickHouse is a batch source AND destination (supports_cdc=false); the connector
uses the HTTP clickhouse-connect driver on port 8123. This test drives the real
product path against a running stack for BOTH directions:

  Phase A (ClickHouse as DESTINATION):  Postgres source -> ClickHouse dest
      POST /connections (postgresql src + clickhouse dest) -> POST /pipelines (batch)
      -> /run ; asserts the executor auto-creates the ClickHouse database+table
      (ensure_table -> ReplacingMergeTree/MergeTree) and import_data lands the rows.

  Phase B (ClickHouse as SOURCE):       ClickHouse source -> Postgres dest
      seed a ClickHouse table -> POST /connections (clickhouse src + postgresql dest)
      -> POST /pipelines (batch) -> /run ; asserts the paginated export() reads the
      rows and the sink lands them in Postgres.

Env-GATED: skips cleanly unless CH_LIVE_HOST is set (mirrors the SS-CDC test), so
it never fails a stack without a ClickHouse fixture. On the isolated test VM set:

  CH_LIVE_HOST=clickhouse-e2e CH_LIVE_DB=ch_e2e_db CH_LIVE_USER=e2e_user \
  CH_LIVE_PWD=e2e_password CH_FIXTURE_CONTAINER=rsync-ai-clickhouse-e2e \
  PG_HOST=postgres-e2e PG_DB=e2e_db PG_USER=e2e_user PG_PWD=e2e_password \
  PG_QUERY_CONTAINER=rsync-ai-postgres-e2e GATEWAY=http://localhost:5001 \
  python3 e2e/test_clickhouse_batch_roundtrip.py

Reads/seeds go through the fixture containers' native clients (docker exec);
the PIPELINE reaches the DBs over the shared rsync-ai_default network by alias.
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

PFX = os.environ.get("STACK_PREFIX", "rsync-ai")

CH_HOST = os.environ.get("CH_LIVE_HOST", "").strip()
if not CH_HOST:
    print("SKIP: CH_LIVE_HOST not set (ClickHouse batch needs a live ClickHouse fixture).")
    sys.exit(77)  # 77 = env-gated skip; run_gate.sh counts it as SKIP, never PASS

CH_PORT = int(os.environ.get("CH_LIVE_PORT", "8123"))
CH_DB = os.environ.get("CH_LIVE_DB", "ch_e2e_db")
CH_USER = os.environ.get("CH_LIVE_USER", "e2e_user")
CH_PWD = os.environ.get("CH_LIVE_PWD", "e2e_password")
CH_SECURE = os.environ.get("CH_LIVE_SECURE", "false")
CH_FIXTURE = os.environ.get("CH_FIXTURE_CONTAINER", f"{PFX}-clickhouse-e2e")

PG_HOST = os.environ.get("PG_HOST", "postgres-e2e")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "e2e_db")
PG_USER = os.environ.get("PG_USER", "e2e_user")
PG_PWD = os.environ.get("PG_PWD", "e2e_password")
PG_QUERY_CONTAINER = os.environ.get("PG_QUERY_CONTAINER", f"{PFX}-postgres-e2e")

GATEWAY = os.environ.get("GATEWAY", os.environ.get("API_GATEWAY_URL", "http://localhost:5001"))
DEV_UID = os.environ.get("DEV_UID", "00000000-0000-0000-0000-000000000000")
RUN_TIMEOUT_S = int(os.environ.get("RUN_TIMEOUT_S", "180"))


def req(method, path, body=None, timeout=90, params=None):
    url = path if path.startswith("http") else GATEWAY + path
    if params:
        sep = "&" if "?" in url else "?"
        url = url + sep + "&".join(f"{k}={v}" for k, v in params.items())
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("X-User-ID", DEV_UID)
    r.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"_raw": raw}


def ch(sql: str) -> str:
    """Run SQL on the ClickHouse fixture via its native client (';;'-separated)."""
    out = []
    for stmt in sql.split(";;"):
        stmt = stmt.strip()
        if not stmt:
            continue
        p = subprocess.run(
            ["docker", "exec", "-i", CH_FIXTURE, "clickhouse-client",
             "--user", CH_USER, "--password", CH_PWD, "-q", stmt],
            capture_output=True, text=True,
        )
        res = (p.stdout.strip() or p.stderr.strip())
        if res:
            out.append(res)
    return "\n".join(out)


def pg(sql: str) -> str:
    p = subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=" + PG_PWD, PG_QUERY_CONTAINER,
         "psql", "-U", PG_USER, "-d", PG_DB, "-tAc", sql],
        capture_output=True, text=True,
    )
    return (p.stdout.strip() or p.stderr.strip())


def mk_conn(payload):
    st, d = req("POST", "/api/v1/connections", payload)
    assert st in (200, 201), f"create connection failed: {st} {d}"
    return d["id"]


def mk_pipeline(payload):
    st, d = req("POST", "/api/v1/pipelines", payload, params={"allow_draft": "true"})
    assert st in (200, 201), f"create pipeline failed: {st} {d}"
    return d["id"]


def run_pipeline(pid):
    st, d = req("POST", f"/api/v1/pipelines/{pid}/run",
                {"ack_warnings": True}, params={"allow_draft": "true", "ack_warnings": "true"})
    assert st == 200, f"run failed: {st} {d}"
    return d


def poll(counter, want, label):
    """Poll counter() (returns int or -1) until it reaches `want`."""
    deadline = time.time() + RUN_TIMEOUT_S
    last = None
    while time.time() < deadline:
        c = counter()
        if c != last:
            print(f"  [{label}] rows={c}")
            last = c
        if c >= want:
            return True
        time.sleep(4)
    return False


# --------------------------------------------------------------------------- #
# Phase A: Postgres source -> ClickHouse destination (batch)
# --------------------------------------------------------------------------- #
def phase_a(ts):
    print("=== Phase A: Postgres -> ClickHouse (batch dest) ===")
    src_schema = f"chsrc_{ts}"
    ch_dest_db = f"ch_dest_{ts}"
    tbl = "items"

    # 1) Seed the Postgres source.
    pg(f"DROP SCHEMA IF EXISTS {src_schema} CASCADE;; CREATE SCHEMA {src_schema}")
    pg(f'CREATE TABLE {src_schema}."{tbl}" '
       f"(id INT PRIMARY KEY, name TEXT, amount NUMERIC(10,2), updated_at TIMESTAMPTZ DEFAULT now())")
    pg(f'INSERT INTO {src_schema}."{tbl}" (id,name,amount) '
       f"VALUES (1,'one',11.11),(2,'two',22.22),(3,'three',33.33)")

    # 2) Connections: PG source + ClickHouse dest.
    src_id = mk_conn({
        "name": f"e2e-pg-src-{ts}", "connection_type": "source", "connector_type": "postgresql",
        "sync_mode": "batch",
        "config": {"host": PG_HOST, "port": PG_PORT, "database": PG_DB,
                   "user": PG_USER, "password": PG_PWD, "schema": src_schema, "sslmode": "disable"},
    })
    dst_id = mk_conn({
        "name": f"e2e-ch-dest-{ts}", "connection_type": "destination", "connector_type": "clickhouse",
        "sync_mode": "batch",
        "config": {"host": CH_HOST, "port": CH_PORT, "database": CH_DB,
                   "user": CH_USER, "password": CH_PWD, "secure": CH_SECURE in ("true", "1", True)},
    })

    # 3) Pipeline (batch) + run.
    pid = mk_pipeline({
        "name": f"e2e-pg-to-ch-{ts}", "request": "PG -> ClickHouse batch",
        "source_connection_id": src_id, "destination_connection_id": dst_id,
        "sync_mode": "batch", "selected_tables": [f"{src_schema}.{tbl}"],
        "destination_namespace": ch_dest_db,
    })
    run_pipeline(pid)

    # 4) Rows land in ClickHouse (auto-created db+table).
    def cnt():
        r = ch(f"SELECT count() FROM `{ch_dest_db}`.`{tbl}`")
        return int(r) if r.isdigit() else -1
    ok = poll(cnt, 3, "CH dest")
    rows = ch(f"SELECT concat(toString(id),':',name,':',toString(amount)) "
              f"FROM `{ch_dest_db}`.`{tbl}` ORDER BY id")
    assert ok, f"PG->CH batch did not land 3 rows: {rows!r}"
    assert ("1:one:11.11" in rows and "2:two:22.22" in rows and "3:three:33.33" in rows), \
        f"PG->CH values wrong: {rows!r}"
    print(f"  PHASE_A_ROWS:\n{rows}")

    # 5) Cleanup.
    req("POST", f"/api/v1/pipelines/{pid}/stop", {})
    pg(f"DROP SCHEMA IF EXISTS {src_schema} CASCADE")
    ch(f"DROP DATABASE IF EXISTS `{ch_dest_db}`")
    print("  PHASE_A_OK")


# --------------------------------------------------------------------------- #
# Phase B: ClickHouse source -> Postgres destination (batch)
# --------------------------------------------------------------------------- #
def phase_b(ts):
    print("=== Phase B: ClickHouse -> Postgres (batch source) ===")
    ch_tbl = f"chsrc_{ts}"
    pg_ns = f"chpg_{ts}"

    # 1) Seed the ClickHouse source (in the connection's configured database).
    ch(f"CREATE DATABASE IF NOT EXISTS `{CH_DB}`;;"
       f"DROP TABLE IF EXISTS `{CH_DB}`.`{ch_tbl}`;;"
       f"CREATE TABLE `{CH_DB}`.`{ch_tbl}` (id Int32, name String, amount Decimal(10,2)) "
       f"ENGINE=MergeTree ORDER BY id;;"
       f"INSERT INTO `{CH_DB}`.`{ch_tbl}` VALUES (1,'alpha',1.50),(2,'beta',2.50),(3,'gamma',3.50)")

    # 2) Connections: ClickHouse source + PG dest.
    src_id = mk_conn({
        "name": f"e2e-ch-src-{ts}", "connection_type": "source", "connector_type": "clickhouse",
        "sync_mode": "batch",
        "config": {"host": CH_HOST, "port": CH_PORT, "database": CH_DB,
                   "user": CH_USER, "password": CH_PWD, "secure": CH_SECURE in ("true", "1", True)},
    })
    dst_id = mk_conn({
        "name": f"e2e-pg-dest-{ts}", "connection_type": "destination", "connector_type": "postgresql",
        "sync_mode": "batch",
        "config": {"host": PG_HOST, "port": PG_PORT, "database": PG_DB,
                   "user": PG_USER, "password": PG_PWD, "schema": pg_ns, "sslmode": "disable"},
    })

    # 3) Pipeline (batch) + run. Bare table name -> resolves to the connection database.
    pid = mk_pipeline({
        "name": f"e2e-ch-to-pg-{ts}", "request": "ClickHouse -> PG batch",
        "source_connection_id": src_id, "destination_connection_id": dst_id,
        "sync_mode": "batch", "selected_tables": [ch_tbl],
        "destination_namespace": pg_ns,
    })
    run_pipeline(pid)

    # 4) Rows land in Postgres.
    dest = f'{pg_ns}."{ch_tbl}"'
    def cnt():
        r = pg(f"SELECT count(*) FROM {dest}")
        return int(r) if r.isdigit() else -1
    ok = poll(cnt, 3, "PG dest")
    rows = pg(f"SELECT id||':'||name||':'||amount FROM {dest} ORDER BY id")
    assert ok, f"CH->PG batch did not land 3 rows: {rows!r}"
    assert ("1:alpha:1.50" in rows and "2:beta:2.50" in rows and "3:gamma:3.50" in rows), \
        f"CH->PG values wrong: {rows!r}"
    print(f"  PHASE_B_ROWS:\n{rows}")

    # 5) Cleanup.
    req("POST", f"/api/v1/pipelines/{pid}/stop", {})
    ch(f"DROP TABLE IF EXISTS `{CH_DB}`.`{ch_tbl}`")
    pg(f"DROP SCHEMA IF EXISTS {pg_ns} CASCADE")
    print("  PHASE_B_OK")


def main():
    ts = int(time.time())
    phase_a(ts)
    phase_b(ts)
    print("OK")


if __name__ == "__main__":
    main()
