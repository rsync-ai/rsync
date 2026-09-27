"""Every install path creates the object-storage bucket its services stage into.

Nothing in the application creates it. The minio connector only head_bucket()s and
put_object()s (shared/mcp-connectors/internal/minio/versions/v1.0.0/connector.py),
and the orchestrator has no MakeBucket. So each install path carries a one-shot
`mc mb` step: `minio-lifecycle-init` in docker-compose.yml, the `-minio-mb` Job in
the Helm chart, and `minio-init` in docker-compose.quickstart.yml.

The quickstart file is standalone -- it does not extend docker-compose.yml -- so a
step the base file has is not thereby in the quickstart one. Up to v0.1.5 it was
not: a fresh `install.sh` came up healthy, every container `running`, and the first
batch pipeline failed staging into a bucket that did not exist. Nothing in CI ran
a batch pipeline against the quickstart stack, so nothing noticed.

The checks read the `mc mb` CALL, one non-comment line at a time, not the bucket
name anywhere in the script: the name survives in the comment explaining the step
after the step itself is deleted.
"""

import os
import re

import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
BUCKET = "pipeline-data"

# `mc mb [--flags] <alias>/<bucket>`, quoted or not; the bucket may be a shell
# variable, resolved against the service's own environment below.
_MB_RE = re.compile(r"""^\s*mc\s+mb\b[^\n]*?\s["']?[\w.-]+/([^\s"'/]+)""")
_VAR_RE = re.compile(r"^\$\$?\{?(\w+)\}?$")
# `${NAME:-default}` / `${NAME:?msg}` -> the default, or None.
_DEFAULT_RE = re.compile(r"^\$\{\w+:-([^}]*)\}$")


def _load(name):
    with open(os.path.join(REPO_ROOT, name)) as fh:
        return yaml.safe_load(fh)


def _env(service):
    env = service.get("environment") or {}
    if isinstance(env, list):
        env = dict(item.split("=", 1) for item in env if "=" in item)
    return {k: str(v) for k, v in env.items()}


def _script(service):
    cmd = service.get("command") or ""
    return "\n".join(cmd) if isinstance(cmd, list) else str(cmd)


def _buckets_made(service):
    """-> the bucket names this service's command passes to `mc mb`."""
    env = _env(service)
    made = []
    for line in _script(service).splitlines():
        if line.lstrip().startswith("#"):
            continue
        m = _MB_RE.match(line)
        if not m:
            continue
        name = m.group(1)
        var = _VAR_RE.match(name)
        if var:
            raw = env.get(var.group(1), "")
            default = _DEFAULT_RE.match(raw)
            name = default.group(1) if default else raw
        made.append(name)
    return made


def _bucket_makers(compose):
    """-> {service: [buckets]} for every always-on service that runs `mc mb`."""
    out = {}
    for name, svc in compose["services"].items():
        if svc.get("profiles"):
            continue  # a profile nobody activates is a step nobody runs
        made = _buckets_made(svc)
        if made:
            out[name] = (svc, made)
    return out


def _assert_compose_creates_bucket(filename):
    compose = _load(filename)
    makers = _bucket_makers(compose)
    creating = {n: s for n, (s, made) in makers.items() if BUCKET in made}
    assert creating, (
        f"{filename}: no always-on service runs `mc mb .../{BUCKET}`. Nothing else "
        "creates the bucket, so the first batch pipeline fails staging on a fresh "
        f"install. Services running `mc mb` at all: {sorted(makers) or 'none'}"
    )
    # It must wait for MinIO to be healthy, or it races the server and exits.
    for name, svc in creating.items():
        dep = (svc.get("depends_on") or {}).get("minio")
        assert isinstance(dep, dict) and dep.get("condition") == "service_healthy", (
            f"{filename}: {name} creates the bucket but does not wait on "
            "minio: service_healthy"
        )


def test_quickstart_compose_creates_the_staging_bucket():
    _assert_compose_creates_bucket("docker-compose.quickstart.yml")


def test_base_compose_creates_the_staging_bucket():
    _assert_compose_creates_bucket("docker-compose.yml")


def test_quickstart_services_stage_into_the_bucket_that_is_created():
    """The bucket made is the bucket every quickstart consumer is pointed at."""
    compose = _load("docker-compose.quickstart.yml")
    consumers = {
        name: _env(svc)["MINIO_BUCKET"]
        for name, svc in compose["services"].items()
        if "MINIO_BUCKET" in _env(svc)
    }
    assert consumers, "no quickstart service sets MINIO_BUCKET; the check read nothing"
    wrong = {n: b for n, b in consumers.items() if b != BUCKET}
    assert not wrong, f"quickstart services staging into a bucket nobody creates: {wrong}"


def test_helm_chart_creates_the_configured_bucket():
    with open(os.path.join(REPO_ROOT, "deploy/helm/rsync-ai/values.yaml")) as fh:
        values = yaml.safe_load(fh)
    assert values["objectStorage"]["bucket"] == BUCKET
    with open(os.path.join(REPO_ROOT, "deploy/helm/rsync-ai/templates/infra/minio.yaml")) as fh:
        template = fh.read()
    calls = [
        line for line in template.splitlines()
        if not line.lstrip().startswith("#")
        and re.match(r"^\s*mc\s+mb\b.*\{\{\s*\.Values\.objectStorage\.bucket\s*\}\}", line)
    ]
    assert calls, "the Helm chart no longer runs `mc mb` on .Values.objectStorage.bucket"
