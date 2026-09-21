"""The prod kafka-mcp-sink container must not be capped below its siblings.

docker-compose.prod.yml capped kafka-mcp-sink-mcp at 256M. That container runs
the Python supervisor plus every Go sink worker process it spawns
(kafka-mcp-sink connector.py _spawn_worker_process), all under one cgroup, and
at 256M the kernel OOM-killed workers mid-run (demo issue #32). The quickstart
compose (mem_limit: 768m) and the Helm chart (sink limits 768Mi) already give
it 768M; prod now matches (decision D-S4).

Two layers, like test_prod_compose_postgres_tls_is_coherent.py:
  1. static: parse the prod overlay and the other two deploy shapes.
  2. render: `docker compose config` on base + prod, asserting on the merged
     project (skipped without a docker CLI; a skip is not a pass, so layer 1
     always runs).
"""

import os
import shutil
import subprocess

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
BASE = os.path.join(REPO_ROOT, "docker-compose.yml")
PROD = os.path.join(REPO_ROOT, "docker-compose.prod.yml")
QUICKSTART = os.path.join(REPO_ROOT, "docker-compose.quickstart.yml")
HELM_VALUES = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai", "values.yaml")

SINK = "kafka-mcp-sink-mcp"
MIB = 1024 * 1024
WANT_BYTES = 768 * MIB  # 805306368

# Placeholders for the ${VAR:?} guards in the merged prod render. One unset
# guard aborts interpolation for the whole project. Not secrets.
REQUIRED = {
    "INTERNAL_SERVICE_SECRET": "FAKEPLACEHOLDER",
    "MINIO_ACCESS_KEY_ID": "FAKEPLACEHOLDER",
    "MINIO_SECRET_ACCESS_KEY": "FAKEPLACEHOLDER",
    "POSTGRES_USER": "FAKEPLACEHOLDER",
    "POSTGRES_PASSWORD": "FAKEPLACEHOLDER",
    "POSTGRES_HOST": "db.example.com",
    "ENCRYPTION_KEY": "FAKEPLACEHOLDER",
    "REDIS_PASSWORD": "FAKEPLACEHOLDER",
}


def _bytes(value):
    """Compose/Kubernetes memory string (256M, 768m, 768Mi, 1G, int) -> bytes."""
    if isinstance(value, int):
        return value
    s = str(value).strip()
    units = {
        "ki": 1024, "mi": MIB, "gi": 1024 * MIB,
        "k": 1024, "m": MIB, "g": 1024 * MIB,
        "kb": 1024, "mb": MIB, "gb": 1024 * MIB, "b": 1,
    }
    for suffix in sorted(units, key=len, reverse=True):
        if s.lower().endswith(suffix):
            return int(float(s[: -len(suffix)]) * units[suffix])
    return int(s)


class _ComposeLoader(yaml.SafeLoader):
    """SafeLoader that reads through Compose's `!override` / `!reset` tags,
    which docker-compose.prod.yml uses and yaml.safe_load rejects."""


def _resolve_tag(loader, node):
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node, deep=True)
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node, deep=True)
    return loader.construct_scalar(node)


for _tag in ("!override", "!reset"):
    _ComposeLoader.add_constructor(_tag, _resolve_tag)


def _load(path):
    with open(path) as f:
        return yaml.load(f, Loader=_ComposeLoader) or {}


def _deploy_limit(svc):
    return (((svc or {}).get("deploy") or {}).get("resources") or {}).get("limits", {}).get("memory")


def test_prod_overlay_caps_the_sink_at_768m():
    svc = (_load(PROD).get("services") or {}).get(SINK)
    assert svc is not None, f"{SINK} is gone from docker-compose.prod.yml -- renamed?"
    limit = _deploy_limit(svc)
    assert limit is not None, f"{SINK} has no deploy.resources.limits.memory in prod"
    assert _bytes(limit) == WANT_BYTES, (
        f"docker-compose.prod.yml caps {SINK} at {limit}; want 768M. At 256M the "
        "kernel OOM-killed the sink's Go workers mid-run (#32), and quickstart "
        "and Helm both already give it 768M."
    )


def test_prod_matches_quickstart_and_helm():
    """The other deploy shapes are the control: if they move, re-decide prod."""
    qs = (_load(QUICKSTART).get("services") or {}).get(SINK) or {}
    qs_limit = qs.get("mem_limit") or _deploy_limit(qs)
    assert qs_limit is not None, f"quickstart {SINK} has no memory limit -- probe broken"

    helm_limit = _load(HELM_VALUES)["connectors"]["cdc"]["sink"]["resources"]["limits"]["memory"]
    prod_limit = _deploy_limit(_load(PROD)["services"][SINK])

    assert _bytes(prod_limit) == _bytes(qs_limit), (
        f"prod caps {SINK} at {prod_limit} but quickstart at {qs_limit}"
    )
    assert _bytes(prod_limit) == _bytes(helm_limit), (
        f"prod caps {SINK} at {prod_limit} but Helm at {helm_limit}"
    )


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI not installed")
def test_merged_prod_render_caps_the_sink_at_768m():
    env = dict(os.environ)
    env.update(REQUIRED)
    out = subprocess.run(
        ["docker", "compose", "-f", BASE, "-f", PROD, "config", "--format", "json"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env=env,
    )
    assert out.returncode == 0, f"docker compose config failed:\n{out.stderr[-3000:]}"
    doc = yaml.safe_load(out.stdout)
    svc = doc["services"][SINK]
    limit = _deploy_limit(svc)
    assert limit is not None, f"merged render dropped {SINK}'s memory limit"
    assert _bytes(limit) == WANT_BYTES, f"merged prod render caps {SINK} at {limit} bytes; want {WANT_BYTES}"
