#!/usr/bin/env python3
"""
E2E: SQL Server (Azure SQL) -> Postgres CDC via the PRODUCT pipeline API.

Env-GATED: skips cleanly unless SS_LIVE_HOST is set, because SQL Server CDC needs a
real SQL Server / Azure SQL instance with the SQL Agent (or Azure's internal CDC
scheduler) — there is no local SQL Server in the e2e stack. Mirrors the model used
by api-gateway/internal/handlers/explorer_sqlserver_live_test.go (skip w/o SS_LIVE_HOST).

When SS_LIVE_HOST is set it drives the real product path against the running stack:
  POST /connections (sqlserver source + postgres dest) -> POST /pipelines (cdc) -> /run
and asserts the executor AUTO-PROVISIONS the capture instance (sys.sp_cdc_enable_table,
PR #396) so that snapshot + streamed I/U/D land in Postgres. This is the regression
guard for the #396 SS-CDC-auto-provision fix.

Env:
  SS_LIVE_HOST (required to run), SS_LIVE_PORT=1433, SS_LIVE_USER, SS_LIVE_PWD, SS_LIVE_DB
  GATEWAY=http://localhost:5001   DEV_UID=00000000-0000-0000-0000-000000000000
  SS_MCP_CONTAINER=rsync-ai-sqlserver-v1-0-0-mcp   (used only to run source SQL)
  PG_DEST_* (dest postgres reached by the sink)     PG_QUERY_CONTAINER=postgres
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

PFX = os.environ.get("STACK_PREFIX", "rsync-ai")

SS_HOST = os.environ.get("SS_LIVE_HOST", "").strip()
if not SS_HOST:
    print("SKIP: SS_LIVE_HOST not set (SQL Server CDC needs a live SQL Server/Azure SQL).")
    sys.exit(77)  # 77 = env-gated skip; run_gate.sh counts it as SKIP, never PASS

SS_PORT = int(os.environ.get("SS_LIVE_PORT", "1433"))
SS_USER = os.environ.get("SS_LIVE_USER", "rsynctest")
SS_PWD = os.environ.get("SS_LIVE_PWD", "")
SS_DB = os.environ.get("SS_LIVE_DB", "")
GATEWAY = os.environ.get("GATEWAY", os.environ.get("API_GATEWAY_URL", "http://localhost:5001"))
DEV_UID = os.environ.get("DEV_UID", "00000000-0000-0000-0000-000000000000")
SS_MCP = os.environ.get("SS_MCP_CONTAINER", f"{PFX}-sqlserver-v1-0-0-mcp")
PG_QUERY_CONTAINER = os.environ.get("PG_QUERY_CONTAINER") or os.environ.get("ORCH_PG_CONTAINER", "postgres")
PG_DEST_HOST = os.environ.get("PG_DEST_HOST", "postgres")
PG_DEST_DB = os.environ.get("PG_DEST_DB", "cdc_e2e_db")
PG_DEST_USER = os.environ.get("PG_DEST_USER", "user")
PG_DEST_PWD = os.environ.get("PG_DEST_PWD", "password")
# TLS for the test's OWN pyodbc probe (ss_sql). Defaults preserve Azure SQL behavior
# (Encrypt=yes, verify cert). A local self-signed fixture (docker sqlserver-e2e) must
# set SS_TRUST_CERT=yes so the probe trusts the container's self-signed cert. This does
# NOT affect the pipeline/Debezium paths — those auto-trust local hosts (connector.py
# _is_local_db_host) independently of this helper.
SS_ENCRYPT = os.environ.get("SS_ENCRYPT", "yes")
SS_TRUST_CERT = os.environ.get("SS_TRUST_CERT", "no")


def req(method, path, body=None, timeout=90):
    url = path if path.startswith("http") else GATEWAY + path
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


def ss_sql(sql: str) -> str:
    """Run SQL on the SS source via the sqlserver MCP container's pyodbc (';;'-separated)."""
    code = (
        "import pyodbc,sys\n"
        f"cs=r'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SS_HOST},{SS_PORT};DATABASE={SS_DB};"
        f"UID={SS_USER};PWD={SS_PWD};Encrypt={SS_ENCRYPT};TrustServerCertificate={SS_TRUST_CERT};Connection Timeout=20'\n"
        "cn=pyodbc.connect(cs,autocommit=True);cur=cn.cursor()\n"
        "out=[]\n"
        f"for s in r'''{sql}'''.split(';;'):\n"
        "    s=s.strip()\n"
        "    if not s: continue\n"
        "    cur.execute(s)\n"
        "    if cur.description:\n"
        "        out.append('\\n'.join('\\t'.join(str(c) for c in row) for row in cur.fetchall()))\n"
        "print('\\n'.join(out))\n"
    )
    p = subprocess.run(["docker", "exec", "-i", SS_MCP, "python3", "-c", code], capture_output=True, text=True)
    return (p.stdout.strip() or p.stderr.strip())


def pg(sql: str) -> str:
    p = subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=" + PG_DEST_PWD, PG_QUERY_CONTAINER,
         "psql", "-U", PG_DEST_USER, "-d", PG_DEST_DB, "-tAc", sql],
        capture_output=True, text=True,
    )
    return (p.stdout.strip() or p.stderr.strip())


def main():
    ts = int(time.time())
    tbl = f"e2e_sscdc_{ts}"
    ns = f"ss_cdc_e2e_{ts}"

    # 1) Source table on SQL Server (fresh, CDC NOT pre-enabled -> executor must provision it).
    ss_sql(
        f"IF OBJECT_ID('dbo.{tbl}','U') IS NOT NULL DROP TABLE dbo.{tbl};;"
        f"CREATE TABLE dbo.{tbl} (id INT PRIMARY KEY, name NVARCHAR(50), amount DECIMAL(10,2));;"
        f"INSERT INTO dbo.{tbl} (id,name,amount) VALUES (1,'one',11.11),(2,'two',22.22)"
    )

    # 2) Connections (source SS + dest PG).
    st, d = req("POST", "/api/v1/connections", {
        "name": f"e2e-ss-src-{ts}", "connection_type": "source", "connector_type": "sqlserver",
        "sync_mode": "cdc", "cdc_mode": "initial",
        "config": {"host": SS_HOST, "port": SS_PORT, "database": SS_DB,
                   "user": SS_USER, "password": SS_PWD, "schema": "dbo"},
    })
    assert st in (200, 201), f"src conn: {st} {d}"
    src_id = d["id"]
    st, d = req("POST", "/api/v1/connections", {
        "name": f"e2e-pg-dest-{ts}", "connection_type": "destination", "connector_type": "postgresql",
        "sync_mode": "cdc",
        "config": {"host": PG_DEST_HOST, "port": 5432, "database": PG_DEST_DB,
                   "user": PG_DEST_USER, "password": PG_DEST_PWD, "schema": ns},
    })
    assert st in (200, 201), f"dest conn: {st} {d}"
    dst_id = d["id"]

    # 3) Pipeline (cdc, initial) + run. Executor auto-provisions the SS capture instance (#396).
    st, d = req("POST", "/api/v1/pipelines", {
        "name": f"e2e-ss-cdc-{ts}", "request": "SS CDC regression",
        "source_connection_id": src_id, "destination_connection_id": dst_id,
        "sync_mode": "cdc", "cdc_mode": "initial",
        "selected_tables": [f"dbo.{tbl}"], "destination_namespace": ns,
    })
    assert st in (200, 201), f"pipeline: {st} {d}"
    pid = d["id"]
    st, d = req("POST", f"/api/v1/pipelines/{pid}/run?ack_warnings=true", {"ack_warnings": True})
    assert st == 200, f"run: {st} {d}"

    dest = f'{ns}."{tbl}"'
    # 4) Snapshot: 2 rows land (auto-provision can take ~40s on Azure).
    ok = False
    for _ in range(45):
        c = pg(f"SELECT count(*) FROM {dest}")
        if c.isdigit() and int(c) >= 2:
            ok = True
            break
        time.sleep(4)
    assert ok, f"snapshot did not land 2 rows: {pg(f'SELECT count(*) FROM {dest}')}"

    # 5) Stream I/U/D on the SS source.
    ss_sql(
        f"INSERT INTO dbo.{tbl} (id,name,amount) VALUES (3,'three',33.33);;"
        f"UPDATE dbo.{tbl} SET amount=99.99 WHERE id=1;;"
        f"DELETE FROM dbo.{tbl} WHERE id=2"
    )
    ok = False
    for _ in range(30):
        rows = pg(f"SELECT id||':'||amount FROM {dest} ORDER BY id")
        if ("1:99.99" in rows) and ("3:33.33" in rows) and ("2:" not in rows):
            ok = True
            break
        time.sleep(4)
    assert ok, f"streaming I/U/D not applied: {pg(f'SELECT id,amount FROM {dest} ORDER BY id')}"

    # 6) Cleanup (best-effort).
    req("POST", f"/api/v1/pipelines/{pid}/stop", {})
    ss_sql(f"IF OBJECT_ID('dbo.{tbl}','U') IS NOT NULL DROP TABLE dbo.{tbl}")
    pg(f"DROP SCHEMA IF EXISTS {ns} CASCADE")
    print("OK")


if __name__ == "__main__":
    main()
