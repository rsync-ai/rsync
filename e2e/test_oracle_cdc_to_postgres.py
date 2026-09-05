#!/usr/bin/env python3
"""
E2E: Oracle (LogMiner CDC) -> Postgres via the PRODUCT pipeline API.

Env-GATED: skips cleanly unless ORA_LIVE_HOST is set. Oracle LogMiner CDC needs a real
Oracle instance in ARCHIVELOG mode with DB-level supplemental logging and a LogMiner
common user — provisioned out-of-band by scripts/oracle_cdc_provision.sh against the
gvenzl/oracle-free fixture (CDB=FREE, PDB=FREEPDB1, app user rsynctest in the PDB).

When ORA_LIVE_HOST is set it drives the real product path against the running stack:
  POST /connections (oracle source + postgres dest) -> POST /pipelines (cdc) -> /run
and asserts snapshot + streamed I/U/D land in Postgres via the shared kafka-mcp-sink.

Multitenant wiring (verified against connector.py / oracle.go / executor.go / debezium):
  service_name=<PDB>  -> MCP discovery + Go provisioning connect INSIDE the PDB (see rsynctest tables)
  database=<CDB>      -> Debezium database.dbname (CDB root, where Debezium's JDBC connects)
  oracle_pdb_name=<PDB> -> Debezium database.pdb.name (the container it captures)
These are three distinct keys -> no conflict. The executor auto-provisions per-table
ALL-column supplemental logging (oracle.go ProvisionResources), the Oracle analog of the
SQL Server sp_cdc_enable_table auto-provision (#396).

Env:
  ORA_LIVE_HOST (required to run), ORA_LIVE_PORT=1521
  ORA_PDB=FREEPDB1  ORA_CDB=FREE
  ORA_CDC_USER=c##rsync_cdc  ORA_CDC_PWD   (LogMiner common user -> source connection)
  ORA_APP_USER=rsynctest     ORA_APP_PWD   (owns the app table -> used for DDL/DML via MCP)
  ORA_MCP_CONTAINER=rsync-ai-oracle-v1-0-0-mcp   (runs source SQL via python-oracledb)
  GATEWAY=http://localhost:5001   DEV_UID=00000000-0000-0000-0000-000000000000
  PG_DEST_* (dest postgres reached by the sink)   PG_QUERY_CONTAINER=postgres
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

PFX = os.environ.get("STACK_PREFIX", "rsync-ai")

ORA_HOST = os.environ.get("ORA_LIVE_HOST", "").strip()
if not ORA_HOST:
    print("SKIP: ORA_LIVE_HOST not set (Oracle LogMiner CDC needs a provisioned Oracle instance).")
    sys.exit(77)  # 77 = env-gated skip; run_gate.sh counts it as SKIP, never PASS

ORA_PORT = int(os.environ.get("ORA_LIVE_PORT", "1521"))
ORA_PDB = os.environ.get("ORA_PDB", "FREEPDB1")
ORA_CDB = os.environ.get("ORA_CDB", "FREE")
ORA_CDC_USER = os.environ.get("ORA_CDC_USER", "c##rsync_cdc")
ORA_CDC_PWD = os.environ.get("ORA_CDC_PWD", "")
ORA_APP_USER = os.environ.get("ORA_APP_USER", "rsynctest")
ORA_APP_PWD = os.environ.get("ORA_APP_PWD", "")
ORA_MCP = os.environ.get("ORA_MCP_CONTAINER", f"{PFX}-oracle-v1-0-0-mcp")

GATEWAY = os.environ.get("GATEWAY", os.environ.get("API_GATEWAY_URL", "http://localhost:5001"))
DEV_UID = os.environ.get("DEV_UID", "00000000-0000-0000-0000-000000000000")

PG_QUERY_CONTAINER = os.environ.get("PG_QUERY_CONTAINER") or os.environ.get("ORCH_PG_CONTAINER", "postgres")
PG_DEST_HOST = os.environ.get("PG_DEST_HOST", "postgres")
PG_DEST_DB = os.environ.get("PG_DEST_DB", "cdc_e2e_db")
PG_DEST_USER = os.environ.get("PG_DEST_USER", "user")
PG_DEST_PWD = os.environ.get("PG_DEST_PWD", "password")


def req(method, path, body=None, timeout=120, params=None):
    url = path if path.startswith("http") else GATEWAY + path
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
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


import urllib.parse  # noqa: E402  (used by req)


def ora_sql(sql: str, user: str = None, pwd: str = None, svc: str = None) -> str:
    """Run SQL on the Oracle source via the oracle MCP container's python-oracledb (';;'-separated).
    Defaults to the app owner (rsynctest) inside the PDB."""
    user = user or ORA_APP_USER
    pwd = pwd or ORA_APP_PWD
    svc = svc or ORA_PDB
    code = (
        "import oracledb\n"
        f"cn=oracledb.connect(user={user!r},password={pwd!r},dsn='{ORA_HOST}:{ORA_PORT}/{svc}')\n"
        "cn.autocommit=True\n"
        "cur=cn.cursor()\n"
        "out=[]\n"
        f"for s in {sql!r}.split(';;'):\n"
        "    s=s.strip()\n"
        "    if not s: continue\n"
        "    cur.execute(s)\n"
        "    if cur.description:\n"
        "        rows=cur.fetchall()\n"
        "        out.append('\\n'.join('\\t'.join(str(c) for c in r) for r in rows))\n"
        "print('\\n'.join(out))\n"
    )
    p = subprocess.run(["docker", "exec", "-i", ORA_MCP, "python3", "-c", code],
                       capture_output=True, text=True)
    return (p.stdout.strip() or p.stderr.strip())


def pg(sql: str) -> str:
    p = subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=" + PG_DEST_PWD, PG_QUERY_CONTAINER,
         "psql", "-U", PG_DEST_USER, "-d", PG_DEST_DB, "-tAc", sql],
        capture_output=True, text=True,
    )
    return (p.stdout.strip() or p.stderr.strip())


def find_dest_table(ns: str) -> str:
    """Oracle folds identifiers to UPPERCASE; the sink-created dest table name is unknown a
    priori. Discover whatever table landed in the destination schema."""
    out = pg(f"SELECT table_name FROM information_schema.tables WHERE table_schema='{ns}' ORDER BY table_name")
    if not out or "ERROR" in out.upper():
        return ""
    return out.splitlines()[0].strip()


def main():
    ts = int(time.time())
    tbl = f"ORA_CDC_E2E_{ts}"          # unquoted -> Oracle stores UPPERCASE
    ns = f"ora_cdc_e2e_{ts}"           # postgres dest schema (lowercase)

    # 1) Source table on Oracle (owned by rsynctest, PK present). Seed 2 rows for the snapshot.
    #    Per-table supplemental logging is left to the executor to auto-provision (oracle.go),
    #    mirroring the SQL Server #396 auto-provision regression guard.
    print("=== 1. create + seed Oracle source table ===")
    # tbl is timestamp-unique -> no pre-drop needed. All plain SQL (no PL/SQL block), so the
    # ';;' separator never collides with a statement terminator.
    r = ora_sql(
        f"CREATE TABLE {tbl} (id NUMBER(10) PRIMARY KEY, name VARCHAR2(50), amount NUMBER(10,2));;"
        f"INSERT INTO {tbl} (id,name,amount) VALUES (1,'one',11.11);;"
        f"INSERT INTO {tbl} (id,name,amount) VALUES (2,'two',22.22);;"
        f"SELECT COUNT(*) FROM {tbl}"
    )
    print("  seed result:", r)
    assert r.strip().endswith("2"), f"source seed failed: {r}"

    # 2) Connections (source Oracle + dest PG). Three-key multitenant wiring.
    print("=== 2. create connections ===")
    st, d = req("POST", "/api/v1/connections", {
        "name": f"e2e-ora-src-{ts}", "connection_type": "source", "connector_type": "oracle",
        "sync_mode": "cdc", "cdc_mode": "initial",
        "config": {"host": ORA_HOST, "port": ORA_PORT,
                   "user": ORA_CDC_USER, "password": ORA_CDC_PWD,
                   "service_name": ORA_PDB, "database": ORA_CDB,
                   "oracle_pdb_name": ORA_PDB, "schema": ORA_APP_USER},
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

    # 3) Pipeline (cdc, initial) + run. Executor auto-provisions per-table supplemental logging.
    print("=== 3. create + run pipeline ===")
    st, d = req("POST", "/api/v1/pipelines", {
        "name": f"e2e-ora-cdc-{ts}", "request": "Oracle LogMiner CDC regression",
        "source_connection_id": src_id, "destination_connection_id": dst_id,
        "sync_mode": "cdc", "cdc_mode": "initial",
        "selected_tables": [f"{ORA_APP_USER.upper()}.{tbl}"], "destination_namespace": ns,
    })
    assert st in (200, 201), f"pipeline: {st} {d}"
    pid = d["id"]
    st, d = req("POST", f"/api/v1/pipelines/{pid}/run", {"ack_warnings": True},
                params={"ack_warnings": "true"})
    assert st == 200, f"run: {st} {d}"

    # 4) Snapshot: table appears in the dest schema with >=2 rows.
    #    Oracle LogMiner startup + snapshot is slower than SQL Server -> generous window (~5 min).
    print("=== 4. await snapshot (>=2 rows) ===")
    dest_tbl = ""
    ok = False
    for _ in range(75):
        if not dest_tbl:
            dest_tbl = find_dest_table(ns)
        if dest_tbl:
            c = pg(f'SELECT count(*) FROM {ns}."{dest_tbl}"')
            if c.isdigit() and int(c) >= 2:
                snap = pg(f'SELECT * FROM {ns}."{dest_tbl}" ORDER BY 1')
                if "one" in snap and "two" in snap:
                    ok = True
                    break
        time.sleep(4)
    assert ok, f"snapshot did not land 2 rows (dest_tbl={dest_tbl!r}): {pg(f'SELECT count(*) FROM {ns}.' + chr(34) + dest_tbl + chr(34)) if dest_tbl else 'no table'}"
    print(f"  snapshot OK dest_tbl={dest_tbl}")

    # 5) Stream I/U/D on the Oracle source.
    print("=== 5. stream INSERT/UPDATE/DELETE ===")
    ora_sql(
        f"INSERT INTO {tbl} (id,name,amount) VALUES (3,'three',33.33);;"
        f"UPDATE {tbl} SET amount=99.99 WHERE id=1;;"
        f"DELETE FROM {tbl} WHERE id=2"
    )
    ok = False
    for _ in range(60):
        dump = pg(f'SELECT * FROM {ns}."{dest_tbl}" ORDER BY 1')
        cnt = pg(f'SELECT count(*) FROM {ns}."{dest_tbl}"')
        insert_ok = ("three" in dump) and ("33.33" in dump)
        update_ok = ("99.99" in dump) and ("11.11" not in dump)
        delete_ok = ("two" not in dump) and ("22.22" not in dump) and (cnt == "2")
        if insert_ok and update_ok and delete_ok:
            ok = True
            break
        time.sleep(4)
    final = pg(f'SELECT * FROM {ns}."{dest_tbl}" ORDER BY 1')
    print("EVIDENCE_DEST_ROWS:")
    print(final)
    print("EVIDENCE_COUNT:", pg(f'SELECT count(*) FROM {ns}."{dest_tbl}"'))
    print("EVIDENCE_PIPELINE:", pid)
    assert ok, f"streaming I/U/D not applied:\n{final}"

    # 6) Cleanup (best-effort).
    print("=== 6. cleanup ===")
    req("POST", f"/api/v1/pipelines/{pid}/stop", {})
    ora_sql(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {tbl}'; EXCEPTION WHEN OTHERS THEN NULL; END;")
    pg(f"DROP SCHEMA IF EXISTS {ns} CASCADE")
    print("OK")


if __name__ == "__main__":
    main()
