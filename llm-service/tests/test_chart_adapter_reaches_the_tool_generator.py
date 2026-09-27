"""The temporal-adapter is told where the tool-generator lives on Kubernetes.

toolGeneratorBaseURL() (backend-temporal-adapter/internal/workflows/
connector_check_activity.go) reads TOOL_GENERATOR_URL and falls back to the
compose name "http://tool-generator:5010". Two callers use it:

  * canGenerateConnector() GETs <base>/health when a pipeline's source or
    destination type is missing from the adapter's connector catalog. If the
    probe fails, the connector counts as not generatable and
    NLPipelineWorkflowV2 stops with "Missing connector: <type>".
  * GenerateConnectorActivityV2() POSTs <base>/v1/generate.

The chart names the Service {{ fullname }}-tool-generator and renders it only
with generation.enabled. It gave TOOL_GENERATOR_URL to the api-gateway and the
orchestrator under that guard, and not to the adapter, so on a Helm install the
probe dialled a host that does not resolve.
test_chart_sets_every_service_url_env.py missed it: its scanner recognised only
an assigned fallback literal, and this one is returned.

The address is followed through the rendered manifests (host -> Service ->
targetPort -> the container behind it), and compared with the value the other
two services get, instead of repeating the name here.

This proves the rendered manifests. It does not prove a live install reaches
the tool-generator, or what the generator answers.
"""

import pathlib
import shutil
import subprocess
import urllib.parse

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
CHART = REPO / "deploy" / "helm" / "rsync-ai"

ADAPTER = "temporal-adapter"
GENERATOR = "tool-generator"
VAR = "TOOL_GENERATOR_URL"
# The other two services that read the same variable. The adapter must get the
# value they get.
PEERS = ("api-gateway", "orchestrator")

# The documented install command's flags, so a render reaches this file's subject
# instead of stopping at an unrelated required-value check. Fakes throughout.
RENDER_FLAGS = [
    "--set", "secrets.jwtSecret=FAKEPLACEHOLDER",
    "--set", "secrets.encryptionKey=FAKEPLACEHOLDER",
    "--set", "secrets.postgresPassword=FAKEPLACEHOLDER",
    "--set", "secrets.minioAccessKey=FAKEPLACEHOLDER",
    "--set", "secrets.minioSecretKey=FAKEPLACEHOLDER",
    "--set", "frontend.publicUrl=https://app.example.com",
    "--set", "frontend.apiUrl=https://api.example.com",
]

# The chart defaults, and a release and stack prefix that appear nowhere in the
# chart, so a template that hard-codes the default name fails the second case.
NAMINGS = {
    "defaults": ("r", []),
    "custom-names": ("acme", ["--set", "global.stackPrefix=acme-stack"]),
}


def _render(release, *extra):
    proc = subprocess.run(
        ["helm", "template", release, str(CHART), *RENDER_FLAGS, *extra],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(REPO),
    )
    assert proc.returncode == 0, (
        f"helm template {' '.join(extra)} failed:\n{proc.stderr[-3000:]}"
    )
    docs = [d for d in yaml.safe_load_all(proc.stdout) if d]
    assert len(docs) >= 20, (
        f"only {len(docs)} documents rendered -- refusing to report a pass on a "
        "denominator this small; the render is not exercising the chart."
    )
    return docs


def _component(docs, kind, component):
    """Every `kind` object labelled app.kubernetes.io/component=`component`."""
    return [
        d for d in docs
        if d.get("kind") == kind
        and d["metadata"].get("labels", {}).get("app.kubernetes.io/component") == component
    ]


def _env(docs, component):
    deploys = _component(docs, "Deployment", component)
    assert len(deploys) == 1, f"expected one {component} Deployment, got {len(deploys)}"
    containers = {c["name"]: c for c in deploys[0]["spec"]["template"]["spec"]["containers"]}
    assert component in containers, f"no {component} container in {sorted(containers)}"
    return {e["name"]: e.get("value") for e in containers[component].get("env", [])}


def _route(docs, url):
    """Follow `url` the way a request does: the host names a Service, the port
    maps to its targetPort, and the Service selector matches one workload whose
    container owns that port. Returns (container, targetPort)."""
    parsed = urllib.parse.urlsplit(url)
    assert parsed.scheme == "http", f"{VAR}={url!r} is not an http URL"
    assert parsed.path in ("", "/") and not parsed.query, (
        f"{VAR}={url!r} carries a path. The adapter appends /health and "
        "/v1/generate to this base URL, so a path here is requested twice."
    )

    services = [
        d for d in docs
        if d.get("kind") == "Service" and d["metadata"]["name"] == parsed.hostname
    ]
    assert len(services) == 1, (
        f"{VAR}={url!r} names host {parsed.hostname!r}, and the chart renders "
        f"{len(services)} Services by that name. The adapter would dial a host "
        "that does not resolve."
    )
    svc = services[0]
    targets = [p["targetPort"] for p in svc["spec"]["ports"] if p["port"] == parsed.port]
    assert targets, (
        f"{VAR}={url!r} uses port {parsed.port}, but Service {parsed.hostname} "
        f"listens on {[p['port'] for p in svc['spec']['ports']]}"
    )
    target = targets[0]

    selector = svc["spec"]["selector"].items()
    pods = [
        d["spec"]["template"] for d in docs
        if d.get("kind") in ("Deployment", "StatefulSet")
        and selector <= d["spec"]["template"]["metadata"].get("labels", {}).items()
    ]
    assert len(pods) == 1, (
        f"Service {parsed.hostname} selects {len(pods)} workloads, expected one"
    )
    owners = [
        c for c in pods[0]["spec"]["containers"]
        if any(target in (p.get("name"), p.get("containerPort")) for p in c.get("ports", []))
    ]
    assert len(owners) == 1, (
        f"Service {parsed.hostname} targets port {target!r}, owned by "
        f"{[c['name'] for c in owners]} in its pod, expected exactly one container"
    )
    return owners[0], target


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
@pytest.mark.parametrize("naming", sorted(NAMINGS))
def test_generation_on_gives_the_adapter_the_real_address(naming):
    release, extra = NAMINGS[naming]
    docs = _render(release, "--set", "generation.enabled=true", *extra)
    url = _env(docs, ADAPTER).get(VAR)

    assert url, (
        f"the {ADAPTER} Deployment sets no {VAR} with generation enabled. The "
        "adapter falls back to http://tool-generator:5010, which does not resolve "
        "on Kubernetes, so every missing connector reads as 'Missing connector: "
        "<type>' without the generator being asked."
    )

    for peer in PEERS:
        assert _env(docs, peer).get(VAR) == url, (
            f"{ADAPTER} gets {VAR}={url!r} but {peer} gets "
            f"{_env(docs, peer).get(VAR)!r}; the three read the same Service"
        )

    container, target = _route(docs, url)
    assert container["name"] == GENERATOR, (
        f"{VAR}={url!r} reaches container {container['name']!r}, not {GENERATOR!r}"
    )
    # canGenerateConnector GETs <base>/health. The container's readiness probe
    # on that path and port shows the pod serves what the adapter asks for.
    ready = (container.get("readinessProbe") or {}).get("httpGet") or {}
    assert ready.get("path") == "/health" and ready.get("port") == target, (
        f"{GENERATOR}'s readiness probe is {ready}, not /health on {target!r}. "
        f"The adapter's probe of <{VAR}>/health would test a path the pod is not "
        "known to serve."
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_generation_off_gives_the_adapter_no_address():
    docs = _render("r", "--set", "generation.enabled=false")
    # First show this case removes the Service, or the absence below proves nothing.
    assert not _component(docs, "Service", GENERATOR), (
        "a tool-generator Service still renders with generation.enabled=false"
    )
    assert VAR not in _env(docs, ADAPTER), (
        f"{ADAPTER} sets {VAR} with generation disabled, pointing at a Service "
        "the chart does not render"
    )
