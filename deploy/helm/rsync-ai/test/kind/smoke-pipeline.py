#!/usr/bin/env python3
"""Move an exactly-known number of rows through a chart install on kind.

WHY THIS EXISTS
---------------
`kubectl get pods` is not evidence that this chart works: a Ready api-gateway
pod serves mock data, and a wrong Service hostname is valid YAML. The only check
that settles it is a pipeline run that moves an EXACTLY-KNOWN row count,
asserted `== N` and never `> 0`. The rest of this harness installs the chart and
stops there, so until this script existed the one criterion that can only be met
by executing the product had no executor.

WHY IT IS NOT AN e2e/ TEST
--------------------------
Every e2e/*.py test authenticates with the `X-User-ID` header. That header is a
DEV-ONLY fallback (api-gateway/internal/handlers/auth_middleware.go:194-209) and
the chart hardcodes ENVIRONMENT=production (templates/_helpers.tpl:239), which
isProductionLike() reads fail-closed (auth_middleware.go:82-90). On Kubernetes
those tests get 401 before they reach any pipeline code. The same setting turns
on CSRF double-submit enforcement (csrf_middleware.go:16-23, and the chart pins
RSYNC_CSRF_ENFORCE=true at templates/apps/api-gateway.yaml:112), so the
cookie-only auth some e2e tests use gets 403 on every write. This script takes
the real path instead: register -> session token -> matching csrf cookie+header.

WHAT IT PROVES, AND THE CONTROLS THAT KEEP IT HONEST
----------------------------------------------------
  1. The destination schema is DROPPED and its absence asserted before the run,
     so a stale artifact from an earlier attempt cannot produce a false pass.
  2. The source fingerprint is asserted == N immediately after seeding, so a
     silently-failed seed cannot make step 4 vacuous.
  3. The destination count is asserted == N exactly. A `> 0` assertion passes on
     a partial load, which is the failure this harness most needs to catch.
  4. The two md5 fingerprints must be equal, so N correct rows carrying wrong
     VALUES still fails.

N is deliberately not 1000/1500/1750 -- the counts other fixtures in this repo
use -- so a table left behind by a different test cannot satisfy step 3.

USAGE
-----
  python3 smoke-pipeline.py                 # seed, run, assert, leave artifacts
  python3 smoke-pipeline.py --cleanup       # drop both schemas afterwards

Prerequisites: the chart is installed on kind and every pod is Ready,
byo-postgres is running from postgres-up.sh, and the password file it wrote is
on disk. Nothing here prints a credential.
"""

import argparse
import json
import os
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))

KCTX = os.environ.get("KIND_CONTEXT", "kind-rsync")
NS = os.environ.get("RSYNC_NAMESPACE", "rsync")

# The BYO Postgres container this harness runs OUTSIDE the cluster. Seeding and
# verification go through its local socket (`docker exec`), which the official
# image trusts, so no password crosses this process.
PG_CONTAINER = os.environ.get("BYO_PG_CONTAINER", "byo-postgres")
PG_SUPERUSER = os.environ.get("BYO_PG_USER", "rsync")
PWFILE = os.path.join(HERE, ".byo-postgres-password")

# What the PODS must use. Not localhost: the connector runs in-cluster and
# resolves this through the headless Service + EndpointSlice broker-up.sh and
# postgres-up.sh install. Getting this wrong is precisely the silent shape the
# harness exists to catch -- the pods stay green and no rows move.
IN_CLUSTER_PG_HOST = os.environ.get(
    "BYO_PG_FQDN", "byo-postgres.{}.svc.cluster.local".format(NS)
)

SMOKE_DB = "smoke_db"
SRC_SCHEMA = "k8s_src"
DST_SCHEMA = "k8s_dst"
TABLE = "widgets"
N_ROWS = int(os.environ.get("SMOKE_ROWS", "1373"))

LOCAL_PORT = int(os.environ.get("SMOKE_LOCAL_PORT", "18080"))
RUN_TIMEOUT_S = int(os.environ.get("SMOKE_RUN_TIMEOUT_S", "900"))
# How long to keep re-reading the pipeline's status AFTER the destination row
# count is already correct, waiting for it to go terminal. Purely cosmetic: the
# pass/fail verdict is asserted on the destination, never on this.
STATUS_SETTLE_S = int(os.environ.get("SMOKE_STATUS_SETTLE_S", "60"))

FINGERPRINT_SQL = (
    "SELECT count(*)::text || ' ' || "
    "coalesce(md5(string_agg(id::text || ':' || label || ':' "
    "|| round(amount::numeric, 2)::text, ',' ORDER BY id)), 'EMPTY') "
    "FROM {schema}.{table}"
)


def die(msg, detail=None):
    print("FATAL: " + msg, file=sys.stderr)
    if detail:
        print("--- detail ---", file=sys.stderr)
        print(detail, file=sys.stderr)
    sys.exit(1)


# --------------------------------------------------------------------------- #
# Postgres, over the container's local socket
# --------------------------------------------------------------------------- #
def psql(sql, dbname=SMOKE_DB, check=True):
    """Run one statement and return stdout.

    No 2>/dev/null anywhere: a relation-does-not-exist error and an empty
    result read identically once stderr is discarded, and this script's whole
    job is telling those two apart.
    """
    p = subprocess.run(
        ["docker", "exec", "-i", PG_CONTAINER,
         "psql", "-v", "ON_ERROR_STOP=1", "-U", PG_SUPERUSER, "-d", dbname, "-tAc", sql],
        capture_output=True, text=True,
    )
    if check and p.returncode != 0:
        die("psql failed on {}: {}".format(dbname, sql[:120]),
            (p.stdout or "") + (p.stderr or ""))
    return (p.stdout or "").strip(), (p.stderr or "").strip(), p.returncode


def fingerprint(schema):
    """(count, md5) for a table, or (-1, reason) when the table is not there."""
    out, err, rc = psql(FINGERPRINT_SQL.format(schema=schema, table=TABLE), check=False)
    if rc != 0:
        return -1, (err or out or "psql rc={}".format(rc))
    parts = out.split(" ", 1)
    if len(parts) != 2:
        return -1, "unparseable fingerprint: {!r}".format(out)
    return int(parts[0]), parts[1]


# --------------------------------------------------------------------------- #
# HTTP against the gateway
# --------------------------------------------------------------------------- #
class Api:
    def __init__(self, base):
        self.base = base
        self.token = None
        self.csrf = secrets.token_hex(16)

    def call(self, method, path, body=None, params=None, timeout=60, auth=True):
        url = self.base + path
        if params:
            url += ("&" if "?" in url else "?") + "&".join(
                "{}={}".format(k, v) for k, v in params.items()
            )
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(url, data=data, method=method)
        r.add_header("Content-Type", "application/json")
        if auth and self.token:
            # Double-submit CSRF: the cookie and the header are compared to each
            # other, so any equal pair passes -- there is no server-side binding
            # (csrf_middleware.go:94-112). The session token is what actually
            # authenticates.
            r.add_header("Authorization", self.token)
            r.add_header("Cookie", "auth_token={}; csrf_token={}".format(self.token, self.csrf))
            r.add_header("X-CSRF-Token", self.csrf)
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
        except Exception as e:  # connection refused, timeout, reset
            return 0, {"_transport": repr(e)}


def wait_for_gateway(api, deadline_s=120):
    end = time.time() + deadline_s
    last = None
    while time.time() < end:
        st, d = api.call("GET", "/health", timeout=5, auth=False)
        if st:  # any HTTP status means the port-forward is up and the server answered
            return st
        last = d
        time.sleep(2)
    die("gateway never answered on {} within {}s".format(api.base, deadline_s), repr(last))


# --------------------------------------------------------------------------- #
def discover_gateway_service():
    p = subprocess.run(
        ["kubectl", "--context", KCTX, "-n", NS, "get", "svc",
         "-l", "app.kubernetes.io/component=api-gateway",
         "-o", "jsonpath={.items[*].metadata.name}"],
        capture_output=True, text=True,
    )
    names = (p.stdout or "").split()
    if p.returncode != 0 or len(names) != 1:
        die("expected exactly one api-gateway Service in namespace {}, got {}".format(NS, names),
            (p.stdout or "") + (p.stderr or ""))
    return names[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cleanup", action="store_true",
                    help="drop both smoke schemas after a PASS")
    args = ap.parse_args()

    if not os.path.exists(PWFILE):
        die("{} not found -- run ./postgres-up.sh first".format(PWFILE))
    with open(PWFILE) as fh:
        pg_password = fh.read().strip()
    if not pg_password:
        die("{} is empty".format(PWFILE))

    ts = str(int(time.time()))

    # --- 1. Seed the source, and prove the seed landed ---------------------- #
    print("=== seed {}.{} with {} rows".format(SRC_SCHEMA, TABLE, N_ROWS))
    psql("SELECT 1", dbname="postgres")  # reachability, before creating anything
    out, err, rc = psql(
        "SELECT 1 FROM pg_database WHERE datname = '{}'".format(SMOKE_DB),
        dbname="postgres", check=False)
    if rc != 0:
        die("could not query pg_database", err or out)
    if out.strip() != "1":
        psql("CREATE DATABASE {}".format(SMOKE_DB), dbname="postgres")

    psql("DROP SCHEMA IF EXISTS {} CASCADE".format(SRC_SCHEMA))
    psql("CREATE SCHEMA {}".format(SRC_SCHEMA))
    psql("CREATE TABLE {}.{} (id INT PRIMARY KEY, label TEXT NOT NULL, "
         "amount NUMERIC(12,2) NOT NULL)".format(SRC_SCHEMA, TABLE))
    psql("INSERT INTO {}.{} (id, label, amount) SELECT g, 'w-' || g, "
         "((g * 7) % 1000) / 100.0 FROM generate_series(1, {}) g"
         .format(SRC_SCHEMA, TABLE, N_ROWS))

    src_n, src_md5 = fingerprint(SRC_SCHEMA)
    if src_n != N_ROWS:
        die("seed did not land: source has {} rows, expected {}".format(src_n, N_ROWS), src_md5)
    print("    source fingerprint: rows={} md5={}".format(src_n, src_md5))

    # --- 2. Control: the destination must NOT already satisfy the assertion - #
    psql("DROP SCHEMA IF EXISTS {} CASCADE".format(DST_SCHEMA))
    pre_n, pre_reason = fingerprint(DST_SCHEMA)
    if pre_n != -1:
        die("destination {}.{} still exists with {} rows before the run -- "
            "a pass would prove nothing".format(DST_SCHEMA, TABLE, pre_n))
    print("=== control: destination absent before the run ({})".format(pre_reason.splitlines()[0][:80]))

    # --- 3. Port-forward the gateway --------------------------------------- #
    svc = discover_gateway_service()
    print("=== port-forward svc/{} {}:8080".format(svc, LOCAL_PORT))
    pf = subprocess.Popen(
        ["kubectl", "--context", KCTX, "-n", NS, "port-forward",
         "svc/" + svc, "{}:8080".format(LOCAL_PORT)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    try:
        api = Api("http://127.0.0.1:{}".format(LOCAL_PORT))
        wait_for_gateway(api)

        # --- 4. Register. The chart's ENVIRONMENT=production rules out the
        #        X-User-ID header every e2e test uses, so this is the real path.
        email = "k8s-smoke-{}@example.com".format(ts)
        st, d = api.call("POST", "/api/v1/auth/register", {
            "email": email,
            "password": secrets.token_urlsafe(24),  # never printed, never reused
            "name": "kind smoke",
        }, auth=False)
        if st not in (200, 201) or not d.get("token"):
            die("register failed: HTTP {}".format(st), json.dumps(d)[:600])
        api.token = d["token"]
        print("=== registered {} (role={})".format(email, d.get("role")))

        # --- 5. Connections ------------------------------------------------- #
        base_cfg = {
            "host": IN_CLUSTER_PG_HOST, "port": 5432, "database": SMOKE_DB,
            "user": PG_SUPERUSER, "password": pg_password, "sslmode": "disable",
        }
        src_cfg = dict(base_cfg, schema=SRC_SCHEMA)
        dst_cfg = dict(base_cfg, schema=DST_SCHEMA)

        st, d = api.call("POST", "/api/v1/connections", {
            "name": "k8s-smoke-src-" + ts, "connection_type": "source",
            "connector_type": "postgresql", "sync_mode": "batch", "config": src_cfg,
        })
        if st not in (200, 201) or not d.get("id"):
            die("create source connection failed: HTTP {}".format(st), json.dumps(d)[:600])
        src_id = d["id"]

        st, d = api.call("POST", "/api/v1/connections", {
            "name": "k8s-smoke-dst-" + ts, "connection_type": "destination",
            "connector_type": "postgresql", "sync_mode": "batch", "config": dst_cfg,
        })
        if st not in (200, 201) or not d.get("id"):
            die("create destination connection failed: HTTP {}".format(st), json.dumps(d)[:600])
        dst_id = d["id"]
        print("=== connections created")

        # --- 6. Pipeline + run ---------------------------------------------- #
        st, d = api.call("POST", "/api/v1/pipelines", {
            "name": "k8s-smoke-" + ts,
            "request": "kind BYO smoke: postgres -> postgres batch",
            "source_connection_id": src_id, "destination_connection_id": dst_id,
            "sync_mode": "batch",
            # Pre-selected on purpose: without it the executor parks at the
            # table-selection HITL step and the run looks like a hang.
            "selected_tables": ["{}.{}".format(SRC_SCHEMA, TABLE)],
            "destination_namespace": DST_SCHEMA,
        }, params={"allow_draft": "true"})
        if st not in (200, 201) or not d.get("id"):
            die("create pipeline failed: HTTP {}".format(st), json.dumps(d)[:600])
        pid = d["id"]

        st, d = api.call("POST", "/api/v1/pipelines/{}/run".format(pid),
                         {"ack_warnings": True},
                         params={"allow_draft": "true", "ack_warnings": "true"})
        if st != 200:
            die("run pipeline failed: HTTP {}".format(st), json.dumps(d)[:600])
        print("=== pipeline {} running".format(pid))

        # --- 7. Poll the DESTINATION, not the status board ------------------- #
        deadline = time.time() + RUN_TIMEOUT_S
        last = None
        dst_n, dst_md5 = -1, "never observed"
        while time.time() < deadline:
            dst_n, dst_md5 = fingerprint(DST_SCHEMA)
            if dst_n != last:
                print("    [dest] rows={}".format(dst_n))
                last = dst_n
            if dst_n >= N_ROWS:
                break
            time.sleep(5)

        # The rows landing and the pipeline being marked complete are two
        # different events, and the sink finishes first. Reading status the
        # instant the count reaches N therefore reports `running` on a run that
        # is about to succeed -- printing that next to SMOKE PASS reads as a
        # hung pipeline. Give the status a bounded settle window so the common
        # case prints the terminal value, and label what is printed either way.
        # The verdict below never depends on this: it is asserted on the
        # destination, which is the whole reason this script polls the database
        # rather than the status board.
        TERMINAL = ("completed", "failed", "cancelled", "error")
        status, settle = None, time.time() + STATUS_SETTLE_S
        while True:
            st, pdata = api.call("GET", "/api/v1/pipelines/{}".format(pid))
            status = pdata.get("status") if isinstance(pdata, dict) else None
            if status in TERMINAL or time.time() >= settle:
                break
            time.sleep(2)
        settled = "" if status in TERMINAL else "  (still non-terminal after {}s)".format(
            STATUS_SETTLE_S)

        print()
        print("=== RESULT")
        print("    pipeline status : {}{}".format(status, settled))
        print("    source          : rows={} md5={}".format(src_n, src_md5))
        print("    destination     : rows={} md5={}".format(dst_n, dst_md5))

        if dst_n != N_ROWS:
            die("destination has {} rows, expected exactly {}".format(dst_n, N_ROWS),
                "pipeline={} status={}".format(pid, status))
        if dst_md5 != src_md5:
            die("row COUNT matches but CONTENT differs: src={} dst={}".format(src_md5, dst_md5),
                "pipeline={}".format(pid))

        print()
        print("SMOKE PASS: {} rows moved, fingerprints identical".format(N_ROWS))

        if args.cleanup:
            psql("DROP SCHEMA IF EXISTS {} CASCADE".format(SRC_SCHEMA))
            psql("DROP SCHEMA IF EXISTS {} CASCADE".format(DST_SCHEMA))
            print("cleaned up both schemas")
    finally:
        pf.terminate()
        try:
            pf.wait(timeout=10)
        except Exception:
            pf.kill()


if __name__ == "__main__":
    main()
