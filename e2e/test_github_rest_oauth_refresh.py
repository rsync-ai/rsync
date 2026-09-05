"""
OAuth2 token-REFRESH end-to-end, through a GENERATED connector, to a real dest.

This is the OAuth companion to test_github_rest_to_postgres_batch.py (#406) and
test_github_rest_to_s3_batch.py (#412). Those prove a generated connector moves
rows with a STATIC token; they never exercise OAuth. This test proves the part
that only OAuth has: the platform detects an EXPIRED access token, performs the
`refresh_token` grant against the provider, and the refreshed token flows into a
generated connector that then lands rows.

Full chain (all live):
  login
    -> GET  /api/v1/oauth/github/authorize            (mint state)
    -> GET  /oauth/callback/github?code&state         (api-gateway exchanges the
                                                        code at the provider token
                                                        endpoint = the MOCK, stores
                                                        an AES-GCM-encrypted token)
    -> SQL  UPDATE oauth_tokens SET expires_at = past  (arm the refresh)
    -> POST /api/v1/connections (github-rest, config.oauth_token_id + base_url=mock)
            ^ enrichConfigWithOAuthToken sees <15min-to-expiry and fires
              RefreshTokenByID -> refresh_token grant to the mock -> VALID_TOKEN
    -> POST /api/v1/pipelines (allow_draft, selected_tables=[licenses])
    -> POST /api/v1/pipelines/{id}/run
    -> executor exports /licenses (mock) with the refreshed token -> sink -> Postgres

Assertions: the pipeline completes, >=1 row lands, the mock's `token_refresh_calls`
increased, and the mock served ZERO unauthorized /licenses calls (the connector
only ever presented the refreshed VALID token).

Environment gating — SKIPS (never fails) unless the stack is wired for OAuth e2e:
  - api-gateway: GITHUB_CLIENT_ID/SECRET set (github provider enabled) and
    GITHUB_TOKEN_URL pointing at the mock (docker-compose.e2e.yml). Requires the
    per-provider token-URL override fix (oauth.go resolveProviderTokenURL) so the
    override is honored on the providers.json dynamic path.
  - api-gateway + orchestrator/adapter: a shared non-empty INTERNAL_SERVICE_SECRET
    (the refresh call is service-to-service).
  - github-rest MCP: MCP_ALLOW_PRIVATE_NETWORKS=true (the mock is a private host).
The test detects each precondition during setup and skips with a pointed message.

Run directly / nightly (not part of the deterministic merge-gate):
    pytest e2e/test_github_rest_oauth_refresh.py -v -s
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from typing import Any, Optional

import pytest
import requests


# --------------------------------------------------------------------------- #
# Configuration (all overridable via env)
# --------------------------------------------------------------------------- #

# STACK_PREFIX makes container/network names isolate-able for a parallel CI
# stack; unset preserves the default names exactly.
PFX = os.environ.get("STACK_PREFIX", "rsync-ai")

API_GATEWAY_URL = os.getenv("API_GATEWAY_URL", "http://localhost:5001")
MCP_NET = os.getenv("E2E_MCP_NET") or os.getenv("NETWORK") or f"{PFX}_default"
MOCK_CONTAINER = os.getenv("E2E_MOCK_GITHUB", f"{PFX}-mock-github")
MOCK_ALIAS = os.getenv("E2E_MOCK_GITHUB_ALIAS", "mock-github")
MOCK_IMAGE = os.getenv("E2E_MOCK_GITHUB_IMAGE", "python:3.11-alpine")
GH_REST_MCP = os.getenv("E2E_GITHUB_REST_MCP", f"{PFX}-github-rest-v1-0-0-mcp")

PG_CONTAINER = os.getenv("E2E_PG_CONTAINER") or os.getenv("PG_CONTAINER") or "postgres"
PG_USER = os.getenv("E2E_PG_USER", "user")
PG_PASSWORD = os.getenv("E2E_PG_PASSWORD", "password")
CONTROL_DB = os.getenv("E2E_CONTROL_DB", "pipeline_db")
DEST_DB = os.getenv("E2E_DEST_DB", f"gh_oauth_e2e_{uuid.uuid4().hex[:8]}")
DEST_TABLE = "licenses"

LOGIN_EMAIL = os.getenv("E2E_USER_EMAIL", "default@rsync-ai.local")
LOGIN_PASSWORD = os.getenv("E2E_USER_PASSWORD", "password123")
DEFAULT_USER_ID = os.getenv("E2E_USER_ID", "00000000-0000-0000-0000-000000000000")

# Must match the mock's EXPIRED_TOKEN so the connector's stale-token calls 401.
MOCK_EXPIRED_TOKEN = os.getenv("E2E_MOCK_EXPIRED_TOKEN", "expired_token")

RUN_TIMEOUT_S = int(os.getenv("E2E_RUN_TIMEOUT_S", "300"))
POLL_INTERVAL_S = float(os.getenv("E2E_POLL_INTERVAL_S", "5"))

REPO_MOCK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mock_github_server.py")


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def _docker(*args: str, timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)


def _have_docker() -> bool:
    try:
        return _docker("version", "--format", "{{.Server.Version}}").returncode == 0
    except Exception:
        return False


def _container_running(name: str) -> bool:
    r = _docker("inspect", "-f", "{{.State.Running}}", name)
    return r.returncode == 0 and r.stdout.strip() == "true"


def _psql(db: str, sql: str, timeout: int = 20) -> tuple[str, str]:
    r = _docker("exec", PG_CONTAINER, "psql", "-U", PG_USER, "-d", db, "-tAc", sql, timeout=timeout)
    return r.stdout.strip(), r.stderr.strip()


def _mock_metrics() -> Optional[dict]:
    r = _docker("exec", MOCK_CONTAINER, "sh", "-c", "wget -qO- http://127.0.0.1:8080/metrics")
    if r.returncode != 0 or not r.stdout.strip():
        return None
    try:
        return json.loads(r.stdout)
    except Exception:
        return None


def _skip(msg: str) -> None:
    pytest.skip(msg)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="module")
def mock_github():
    """Ensure the mock OAuth+data server is running (alias `mock-github`).

    Starts it if absent (mounting the repo's mock_github_server.py) and removes
    it on teardown only if THIS fixture started it.
    """
    if not _have_docker():
        _skip("docker not available")

    started_here = False
    if not _container_running(MOCK_CONTAINER):
        if not os.path.exists(REPO_MOCK):
            _skip(f"mock server file not found at {REPO_MOCK}")
        _docker("rm", "-f", MOCK_CONTAINER)
        run = _docker(
            "run", "-d", "--name", MOCK_CONTAINER,
            "--network", MCP_NET, "--network-alias", MOCK_ALIAS,
            "-e", "PORT=8080",
            # Long exchange expiry so the create-time proactive refresh only
            # fires AFTER we force-expire the row (deterministic).
            "-e", "EXCHANGE_EXPIRES_IN_SECONDS=3600",
            "-v", f"{REPO_MOCK}:/app/mock_github_server.py:ro",
            MOCK_IMAGE, "python", "/app/mock_github_server.py",
        )
        if run.returncode != 0:
            _skip(f"could not start mock-github: {run.stderr[:200]}")
        started_here = True
        # wait for health
        for _ in range(20):
            h = _docker("exec", MOCK_CONTAINER, "sh", "-c", "wget -qO- http://127.0.0.1:8080/health")
            if h.returncode == 0 and "ok" in h.stdout:
                break
            time.sleep(0.5)

    # The mock must be the edited one (has /licenses + whoami counters).
    m = _mock_metrics()
    if m is None or "licenses_calls" not in m:
        _skip("mock-github is running an older mock_github_server.py without /licenses support")

    yield
    if started_here:
        _docker("rm", "-f", MOCK_CONTAINER)


@pytest.fixture(scope="module")
def preconditions(mock_github):
    """Skip unless the api-gateway is wired for OAuth e2e and reachable."""
    try:
        r = requests.get(f"{API_GATEWAY_URL}/health", timeout=8)
    except Exception as e:
        _skip(f"api-gateway not reachable at {API_GATEWAY_URL}: {e}")
    if r.status_code != 200:
        _skip(f"api-gateway /health returned {r.status_code}")

    # github provider must be enabled (GITHUB_CLIENT_ID set)
    try:
        provs = requests.get(f"{API_GATEWAY_URL}/api/v1/oauth/providers", timeout=8).json()
        gh = next((p for p in provs.get("providers", []) if p.get("name") == "github"), None)
    except Exception as e:
        _skip(f"could not list oauth providers: {e}")
    if not gh or not gh.get("enabled"):
        _skip("github OAuth provider not enabled — set GITHUB_CLIENT_ID/SECRET on api-gateway")

    if not _container_running(GH_REST_MCP):
        _skip(f"github-rest MCP ({GH_REST_MCP}) not running — deploy it first")


@pytest.fixture(scope="module")
def session(preconditions):
    s = requests.Session()
    r = s.post(f"{API_GATEWAY_URL}/api/v1/auth/login",
               json={"email": LOGIN_EMAIL, "password": LOGIN_PASSWORD}, timeout=15)
    if r.status_code != 200:
        _skip(f"login failed ({r.status_code}); is the api-gateway in dev mode?")
    token = None
    try:
        token = r.json().get("token") or r.json().get("access_token")
    except Exception:
        pass
    s.headers.update({"Authorization": f"Bearer {token}"} if token else {})
    return s


@pytest.fixture(scope="module")
def dest_db():
    """A throwaway destination database for isolation; dropped on teardown."""
    _psql("postgres", f'DROP DATABASE IF EXISTS "{DEST_DB}"')
    out, err = _psql("postgres", f'CREATE DATABASE "{DEST_DB}"')
    if err and "already exists" not in err:
        # CREATE DATABASE can't run in a txn wrapper on some setups; retry raw.
        _docker("exec", PG_CONTAINER, "psql", "-U", PG_USER, "-d", "postgres",
                "-c", f'CREATE DATABASE "{DEST_DB}"')
    yield DEST_DB
    _psql("postgres", f'DROP DATABASE IF EXISTS "{DEST_DB}" WITH (FORCE)')


# --------------------------------------------------------------------------- #
# Flow helpers
# --------------------------------------------------------------------------- #

def _mint_expired_token(s: requests.Session) -> str:
    """Drive authorize -> callback so the api-gateway mints + stores an
    encrypted github token via the mock. Returns the oauth_tokens id. Skips if
    the token exchange does not reach the mock (i.e. the override isn't wired)."""
    r = s.get(f"{API_GATEWAY_URL}/api/v1/oauth/github/authorize", timeout=15)
    if r.status_code != 200:
        _skip(f"oauth authorize failed ({r.status_code})")
    state = None
    try:
        j = r.json()
        state = j.get("state")
        if not state and j.get("authorization_url"):
            from urllib.parse import urlparse, parse_qs
            state = parse_qs(urlparse(j["authorization_url"]).query).get("state", [None])[0]
    except Exception:
        pass
    if not state:
        _skip("oauth authorize did not return a state")

    code = "mock_auth_code_" + uuid.uuid4().hex[:8]
    cb = s.get(f"{API_GATEWAY_URL}/oauth/callback/github",
               params={"code": code, "state": state}, allow_redirects=False, timeout=15)
    if cb.status_code not in (200, 302, 303):
        # A 500 "token exchange failed" here means GITHUB_TOKEN_URL is not being
        # honored (the exchange hit real github). That's the precondition the
        # oauth.go fix addresses — skip rather than fail.
        _skip(f"oauth callback/token-exchange failed ({cb.status_code}); "
              f"GITHUB_TOKEN_URL likely not routed to the mock. body={cb.text[:160]}")

    tid, _ = _psql(CONTROL_DB,
                   f"SELECT id FROM oauth_tokens WHERE provider='github' "
                   f"AND user_id='{DEFAULT_USER_ID}' ORDER BY created_at DESC LIMIT 1")
    if not tid:
        _skip("no oauth_tokens row was minted by the callback")
    return tid


def _create_connection(s: requests.Session, body: dict) -> Optional[str]:
    r = s.post(f"{API_GATEWAY_URL}/api/v1/connections", json=body, timeout=25)
    if r.status_code not in (200, 201):
        return None
    try:
        j = r.json()
        return j.get("id") or (j.get("connection") or {}).get("id")
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# The test
# --------------------------------------------------------------------------- #

def test_oauth_refresh_generated_connector_lands_rows(session: requests.Session, dest_db: str) -> None:
    s = session

    # 1. mint an encrypted github token via the real authorize->callback flow.
    token_id = _mint_expired_token(s)

    # 2. baseline the mock, then force the token expired so the create-time
    #    proactive refresh fires when we build the connection.
    before = _mock_metrics() or {}
    refresh_before = int(before.get("token_refresh_calls", 0))
    unauth_before = int(before.get("licenses_unauthorized", 0))
    _psql(CONTROL_DB, f"UPDATE oauth_tokens SET expires_at = now() - interval '1 hour' WHERE id='{token_id}'")

    # 3. source connection: generated github-rest, OAuth-backed, pointed at the mock.
    src_id = _create_connection(s, {
        "name": f"gh-oauth-src-{uuid.uuid4().hex[:8]}",
        "connection_type": "source",
        "connector_type": "github-rest",
        "connector_version": "v1.0.0",
        "config": {"oauth_token_id": token_id, "base_url": f"http://{MOCK_ALIAS}:8080"},
        "sync_mode": "batch",
        "force_save": True,
    })
    assert src_id, "source connection was not created"

    # the create-time refresh must have fired (expired token -> RefreshTokenByID)
    mid = _mock_metrics() or {}
    assert int(mid.get("token_refresh_calls", 0)) > refresh_before, (
        "expected a token refresh at connection-create, but the mock counter did not move — "
        "is INTERNAL_SERVICE_SECRET wired and the provider token URL routed to the mock?")

    # 4. destination connection: throwaway Postgres database (key is `user`, not `username`).
    dst_id = _create_connection(s, {
        "name": f"gh-oauth-dst-{uuid.uuid4().hex[:8]}",
        "connection_type": "destination",
        "connector_type": "postgresql",
        "connector_version": "v1.0.0",
        "config": {"host": PG_CONTAINER, "port": 5432, "database": dest_db,
                   "user": PG_USER, "password": PG_PASSWORD, "schema": "public"},
        "sync_mode": "batch",
        "force_save": True,
    })
    assert dst_id, "destination connection was not created"

    # 5. pipeline (draft connector -> allow_draft on BOTH create and run;
    #    selected_tables skips the table-selection HITL wedge).
    r = s.post(f"{API_GATEWAY_URL}/api/v1/pipelines",
               json={"name": f"gh-oauth-pipe-{uuid.uuid4().hex[:8]}",
                     "request": "Sync github licenses to postgres",
                     "source_connection_id": src_id, "destination_connection_id": dst_id,
                     "sync_mode": "batch", "selected_tables": [DEST_TABLE]},
               params={"allow_draft": "true"}, timeout=30)
    assert r.status_code in (200, 201), f"pipeline create failed: {r.status_code} {r.text[:200]}"
    pid = r.json().get("id")
    assert pid, "no pipeline id"

    # 6. run.
    run = s.post(f"{API_GATEWAY_URL}/api/v1/pipelines/{pid}/run",
                 params={"allow_draft": "true"}, timeout=30)
    if run.status_code == 422:
        # A blocking pre-flight assessment (e.g. SOURCE_UNREACHABLE) means the
        # github-rest MCP could not reach the private mock — usually
        # MCP_ALLOW_PRIVATE_NETWORKS is not set on it. Skip, don't fail.
        _skip(f"pre-flight assessment blocked the run (likely MCP_ALLOW_PRIVATE_NETWORKS "
              f"unset on {GH_REST_MCP}): {run.text[:200]}")
    assert run.status_code == 200, f"run failed: {run.status_code} {run.text[:200]}"

    # 7. poll for terminal state + landed rows.
    deadline = time.time() + RUN_TIMEOUT_S
    landed = "0"
    status = ""
    while time.time() < deadline:
        status, _ = _psql(CONTROL_DB, f"SELECT status FROM pipelines WHERE id='{pid}'")
        landed, _ = _psql(dest_db, f"SELECT count(*) FROM public.{DEST_TABLE}")
        if status.lower() in ("completed", "failed", "error", "cancelled"):
            break
        if landed.isdigit() and int(landed) > 0:
            break
        time.sleep(POLL_INTERVAL_S)

    after = _mock_metrics() or {}
    refresh_delta = int(after.get("token_refresh_calls", 0)) - refresh_before
    unauth_delta = int(after.get("licenses_unauthorized", 0)) - unauth_before

    # 8. assertions.
    assert landed.isdigit() and int(landed) >= 1, (
        f"expected >=1 landed row, got {landed!r} (pipeline status={status!r})")
    assert refresh_delta >= 1, f"expected a token refresh (delta>=1), got {refresh_delta}"
    assert unauth_delta == 0, (
        f"the connector presented a STALE token to /licenses {unauth_delta} time(s) — "
        f"the refreshed token was not used")


# --------------------------------------------------------------------------- #
# Standalone entrypoint. run_gate.sh invokes .py e2e tests as `python3 <file>`
# (not through a pytest runner), so self-invoke pytest on this module. A skip
# (OAuth wiring absent) exits 0 -> gate PASS; a real assertion failure exits
# non-zero -> gate RED. Also runnable directly via
# `pytest test_github_rest_oauth_refresh.py`. Requires pytest in the runner's
# python3 (the opt-in OAuth lane's one extra dev dep; run_gate warns + skips if
# it is missing rather than hard-failing).
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v", "-s"]))
