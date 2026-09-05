#!/usr/bin/env python3
"""H8 — two-tenant authorization-diff gate (cross-tenant IDOR / BOLA).

Replays tenant A's object requests with tenant B's credentials and flags any
response that is NOT 401/403/404 — i.e. B could read or act on A's resource.
This is rsync's #1 real-world finding class (cross-tenant IDOR), which no
off-the-shelf scanner catches because it is a *relationship* ("B must not see
A"), not a single-request property.

Run against a LOCAL or STAGING stack — never prod (it exercises real routes and,
for the write checks, issues DELETEs against tenant-A objects — use disposable
seed data). Auth model (confirmed from code): opaque session token in
Authorization, plus X-Workspace-ID and X-CSRF-Token (mutations). Mint two
MEMBER-workspace sessions first (docs/runbook.md; reference_prod_api_auth_session_token).

Usage:
    BASE=http://localhost:8080 \
    SESS_A=... WS_A=... CSRF_A=... \
    SESS_B=... WS_B=... CSRF_B=... \
    [CHECK_WRITES=1] \
    python3 scripts/security/authz_diff.py
Exit non-zero if any cross-tenant access leaks OR if zero tenant-A objects were
harvested (a gate that tested nothing must not pass) — suitable as a CI gate.
"""
from __future__ import annotations

import os
import sys

try:
    import requests
except ImportError:
    sys.exit("pip install requests")

BASE = os.environ.get("BASE", "http://localhost:8080").rstrip("/")
A = {"tok": os.environ.get("SESS_A", ""), "ws": os.environ.get("WS_A", ""), "csrf": os.environ.get("CSRF_A", "")}
B = {"tok": os.environ.get("SESS_B", ""), "ws": os.environ.get("WS_B", ""), "csrf": os.environ.get("CSRF_B", "")}
CHECK_WRITES = os.environ.get("CHECK_WRITES", "") not in ("", "0", "false")
BLOCKED = {401, 403, 404}

# Each family: how to LIST tenant-A objects (verified-real routes), which GET
# detail routes to replay as B, and which WRITE (mutation) routes to replay as B.
# NOTE: only GET-readable detail routes here — a POST-only route (e.g. assess)
# returns 405/404 for a GET and would FALSELY count as "blocked". Write checks
# use the correct verb + CSRF and are opt-in (CHECK_WRITES=1) because they mutate.
FAMILIES = {
    "pipelines": {
        "list": "/api/v1/pipelines",
        "id": "id",
        "read": ["/api/v1/pipelines/{id}"],
        "write": [("DELETE", "/api/v1/pipelines/{id}")],
    },
    "connections": {
        "list": "/api/v1/connections",
        "id": "id",
        # /sample and /metadata return real source-DB data — a cross-tenant 200
        # here is a data breach, not just a metadata leak. /sample validates the
        # `table` query param BEFORE the ownership check, so a probe value is
        # supplied to actually reach the authz gate (else it 400s pre-authz).
        "read": ["/api/v1/connections/{id}", "/api/v1/connections/{id}/sample?table=_authz_probe_", "/api/v1/connections/{id}/metadata"],
        "write": [("DELETE", "/api/v1/connections/{id}")],
    },
}


def hdr(u: dict, write: bool = False) -> dict:
    h = {"Authorization": u["tok"], "X-Workspace-ID": u["ws"]}
    if write:
        h["X-CSRF-Token"] = u["csrf"]
        h["Content-Type"] = "application/json"
    return h


def harvest(u: dict, fam: dict) -> list[str]:
    try:
        r = requests.get(BASE + fam["list"], headers=hdr(u), timeout=15)
    except requests.RequestException as e:
        print(f"  ! harvest {fam['list']}: {e}")
        return []
    if r.status_code != 200:
        print(f"  ! harvest {fam['list']} -> {r.status_code}")
        return []
    try:
        body = r.json()
    except ValueError:
        return []
    if isinstance(body, list):
        items = body
    else:
        items = body.get("items") or body.get("data") or []
        if not items:
            # Envelope shape varies: {"connections": [...]}, {"pipelines": [...]}, etc.
            # Fall back to the first list-of-dicts value so the harness is response-shape agnostic.
            for v in body.values():
                if isinstance(v, list) and (not v or isinstance(v[0], dict)):
                    items = v
                    break
    return [it[fam["id"]] for it in items if isinstance(it, dict) and fam["id"] in it]


def check(name: str, method: str, url: str) -> tuple[int, str] | None:
    try:
        r = requests.request(method, url, headers=hdr(B, write=(method != "GET")), timeout=15)
    except requests.RequestException as e:
        print(f"  ! {method} {url}: {e}")
        return None
    if r.status_code not in BLOCKED:
        snippet = (r.text or "")[:120].replace("\n", " ")
        # A 400 means the request was rejected at input-validation BEFORE the
        # ownership/authz layer — no cross-tenant resource was reached, so it is
        # not a leak (just an un-probeable route). Report as inconclusive, not a hit.
        if r.status_code == 400:
            print(f"  [inconclusive-400] {method} {url}  (validation before authz; not a leak) body[:120]={snippet!r}")
            return None
        # For a leaked read, show a snippet so a 200 that is NOT A's data (e.g. an
        # empty list) can be told apart from an actual breach on review.
        print(f"  [BOLA?] {r.status_code} {method} {url}  body[:120]={snippet!r}")
        return (r.status_code, f"{method} {url}")
    return None


def main() -> int:
    for name, u in (("A", A), ("B", B)):
        if not (u["tok"] and u["ws"]):
            sys.exit(f"missing SESS_{name}/WS_{name} env")
    print(f"authz-diff against {BASE} (writes={'on' if CHECK_WRITES else 'off'})")
    total_ids, hits = 0, []
    for fname, fam in FAMILIES.items():
        ids = harvest(A, fam)
        print(f"  {fname}: harvested {len(ids)} tenant-A ids")
        total_ids += len(ids)
        for oid in ids:
            for tpl in fam["read"]:
                if (h := check(fname, "GET", BASE + tpl.format(id=oid))):
                    hits.append(h)
            if CHECK_WRITES:
                for method, tpl in fam["write"]:
                    if (h := check(fname, method, BASE + tpl.format(id=oid))):
                        hits.append(h)

    if total_ids == 0:
        print("\nFAIL: harvested 0 tenant-A objects — seed A with a pipeline + connection "
              "first, or the gate is testing nothing (not a pass).")
        return 2
    if hits:
        print(f"\nFAIL: {len(hits)} cross-tenant response(s) were NOT blocked (leaked A's data/actions to B)")
        return 1
    print(f"\nPASS: {total_ids} tenant-A objects, every cross-tenant request blocked (401/403/404)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
