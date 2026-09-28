"""Two chart gaps that each wrote an error line into a healthy install's logs.

1. The bundled Postgres pods (infra/postgresql.yaml, infra/demo-warehouse.yaml)
   ran their liveness probe as `pg_isready -U <user>` with no -d. pg_isready then
   connects to a database named after the user, which does not exist, and the
   server logs `FATAL: database "<user>" does not exist` on every probe: 240
   lines an hour per pod on the rsync-v016 GKE install (2026-09-27). The probe
   still passed, so only the logs showed it.

2. llm-service, planner and tool-generator (apps/generation.yaml) got no
   REDIS_* env. The rate limiter (llm-service src/utils/ratelimit.py) then
   defaults REDIS_HOST to "redis", which no Service in the chart is named, and
   logs "Redis rate limiter unreachable ... degrading to in-memory limits" at
   startup: limits stop being shared between replicas.

3. llm-service got no DATABASE_URL either. LLM cost logging
   (src/utils/llm_cost.py) then logs "No DATABASE_URL; LLM cost logging
   disabled" and no llm_usage_events row is ever written: every call goes
   unmetered. Only llm-service calls record_usage (src/gateway/main.py).

This proves the rendered manifests, not a live install.
"""

import pathlib
import re
import shutil
import subprocess

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
CHART = REPO / "deploy" / "helm" / "rsync-ai"

RENDER_FLAGS = [
    "--set", "secrets.jwtSecret=FAKEPLACEHOLDER",
    "--set", "secrets.encryptionKey=FAKEPLACEHOLDER",
    "--set", "secrets.postgresPassword=FAKEPLACEHOLDER",
    "--set", "secrets.minioAccessKey=FAKEPLACEHOLDER",
    "--set", "secrets.minioSecretKey=FAKEPLACEHOLDER",
    "--set", "frontend.publicUrl=https://app.example.com",
    "--set", "frontend.apiUrl=https://api.example.com",
    "--set", "generation.enabled=true",
    "--set", "demo.enabled=true",
    "--set", "secrets.demoWarehousePassword=FAKEPLACEHOLDER",
    "--set", "connectors.fleet[0].id=postgresql",
    "--set", "connectors.fleet[0].version=v1.0.0",
    "--set", "connectors.fleet[0].image.repository=mcp-postgresql",
]

GENERATION = ("llm-service", "planner", "tool-generator")
# The services that already had redisEnv; the generation tier must match them.
REDIS_PEER = "api-gateway"
REDIS_VARS = ("REDIS_HOST", "REDIS_PORT", "REDIS_ADDRESS")


def _render():
    proc = subprocess.run(
        ["helm", "template", "r", str(CHART), *RENDER_FLAGS],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(REPO),
    )
    assert proc.returncode == 0, f"helm template failed:\n{proc.stderr[-3000:]}"
    docs = [d for d in yaml.safe_load_all(proc.stdout) if d]
    assert len(docs) >= 20, (
        f"only {len(docs)} documents rendered; the render is not exercising the chart"
    )
    return docs


def _containers(docs, kinds=("Deployment", "StatefulSet")):
    for d in docs:
        if d.get("kind") in kinds:
            for c in d["spec"]["template"]["spec"]["containers"]:
                yield d, c


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_every_pg_isready_probe_names_the_database():
    probes = []
    for d, c in _containers(_render()):
        for kind in ("livenessProbe", "readinessProbe", "startupProbe"):
            cmd = ((c.get(kind) or {}).get("exec") or {}).get("command") or []
            if cmd and cmd[0] == "pg_isready":
                probes.append((d["metadata"]["name"], kind, cmd))

    # Both bundled Postgres pods, liveness and readiness each; fewer means the
    # render did not reach the probes and the check below would pass on nothing.
    assert len(probes) >= 4, f"expected >= 4 pg_isready probes, found {probes}"
    missing = [p for p in probes if "-d" not in p[2]]
    assert not missing, (
        "pg_isready without -d connects to a database named after the user and "
        f"the server logs a FATAL on every probe: {missing}"
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_generation_tier_gets_the_same_redis_as_the_gateway():
    docs = _render()
    env = {}
    for d, c in _containers(docs, kinds=("Deployment",)):
        env[c["name"]] = {e["name"]: e.get("value") for e in c.get("env", [])}

    peer = {k: env[REDIS_PEER].get(k) for k in REDIS_VARS}
    assert all(peer.values()), f"{REDIS_PEER} Redis env is incomplete: {peer}"
    services = {d["metadata"]["name"] for d in docs if d.get("kind") == "Service"}
    assert peer["REDIS_HOST"] in services, (
        f"{REDIS_PEER} REDIS_HOST={peer['REDIS_HOST']!r} names no rendered Service"
    )

    for svc in GENERATION:
        assert svc in env, f"no {svc} container rendered with generation.enabled=true"
        got = {k: env[svc].get(k) for k in REDIS_VARS}
        assert got == peer, (
            f"{svc} gets {got}, {REDIS_PEER} gets {peer}. Without REDIS_HOST the "
            "rate limiter dials 'redis', which does not resolve, and falls back to "
            "per-replica in-memory limits."
        )


_VAR_REF = re.compile(r"\$\(([A-Z0-9_]+)\)")


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_llm_service_gets_the_same_database_as_the_gateway():
    envs = {}
    for d, c in _containers(_render(), kinds=("Deployment",)):
        envs[c["name"]] = c.get("env", [])

    def database_url(name):
        entries = envs[name]
        names = [e["name"] for e in entries]
        assert "DATABASE_URL" in names, (
            f"{name} renders no DATABASE_URL; llm_cost.py disables cost logging "
            "and every LLM call goes unmetered"
        )
        at = names.index("DATABASE_URL")
        url = entries[at].get("value") or ""
        # Kubernetes expands $(VAR) only from entries defined EARLIER in the
        # same list; a later or absent one stays as the literal text.
        unresolved = [v for v in _VAR_REF.findall(url) if v not in names[:at]]
        assert not unresolved, f"{name} DATABASE_URL uses {unresolved} before defining them: {url}"
        return url, {e["name"]: e for e in entries}

    peer_url, peer_env = database_url("api-gateway")
    assert _VAR_REF.findall(peer_url), f"setup: api-gateway DATABASE_URL has no $(VAR) refs: {peer_url}"
    got_url, got_env = database_url("llm-service")
    assert got_url == peer_url, f"llm-service DATABASE_URL {got_url!r} != api-gateway {peer_url!r}"
    for var in _VAR_REF.findall(peer_url):
        assert got_env[var] == peer_env[var], f"{var}: llm-service {got_env[var]} != api-gateway {peer_env[var]}"
