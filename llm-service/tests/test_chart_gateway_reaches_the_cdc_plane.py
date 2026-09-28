"""The api-gateway is told where Kafka Connect and the CDC sink live on Kubernetes.

Two gateway paths talk to the CDC plane directly, and both fall back to a
compose service name when their address is unset:

  * kafkaConnectURL() (handlers/cdc_tables.go) -> "http://kafka-connect:8083".
    findDebeziumConnectorName lists Connect's connectors on every CDC table
    edit. An unreachable Connect is deliberately NOT treated as "no connector
    yet": UpdatePipelineCDCTables answers 502 connect_unreachable and changes
    nothing. That includes the pending_provision path, the only self-service
    recovery for a pipeline that never provisioned, because telling the two
    apart needs Connect to answer.
  * kafkaSinkURL() (handlers/admin_health.go) -> "http://kafka-mcp-sink-mcp:8000",
    probed by admin/health as <base>/health.

The chart names neither Service that way. Connect is {{ fullname }}-kafka-connect
and the sink is the connectorServiceName built from global.stackPrefix, so
neither fallback resolves. The chart gave KAFKA_CONNECT_URL to the orchestrator
and to the debezium sidecar, and nothing to the gateway. Every CDC table edit on
a Helm install failed, and admin/health showed both services down once a CDC
pipeline existed.

Each address is followed through the rendered manifests (host -> Service ->
targetPort -> the container behind it) instead of repeating names, so a rename
in cdc.yaml fails here, not quietly on a cluster. The sink address
is a base URL, because the gateway appends /health itself: a value ending in
/health would be probed as /health/health.

This proves the rendered manifests. It does not prove a live install reaches
either service.
"""

import pathlib
import shutil
import subprocess
import urllib.parse

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
CHART = REPO / "deploy" / "helm" / "rsync-ai"

GATEWAY = "api-gateway"
CDC_ADDRESS_VARS = ("KAFKA_CONNECT_URL", "KAFKA_SINK_URL")

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
# chart. The two Service names derive from different values (release name vs
# stackPrefix), so a template that hard-codes either one fails the second case.
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


def _gateway_env(docs):
    deploys = _component(docs, "Deployment", GATEWAY)
    assert len(deploys) == 1, f"expected one {GATEWAY} Deployment, got {len(deploys)}"
    containers = {c["name"]: c for c in deploys[0]["spec"]["template"]["spec"]["containers"]}
    assert GATEWAY in containers, f"no {GATEWAY} container in {sorted(containers)}"
    return {e["name"]: e.get("value") for e in containers[GATEWAY].get("env", [])}


def _route(docs, url, var):
    """Follow `url` the way a request does. The URL host names a Service, the
    URL port maps to that Service's targetPort, and the Service selector
    matches a workload's pod, whose container owns that port. Returns
    (container, targetPort).

    The Connect pod is fronted by two Services: Connect's REST API and the
    debezium-mcp sidecar. Both carry component=kafka-connect, so neither a
    label nor a port number identifies what the gateway reaches. The owning
    container does.
    """
    parsed = urllib.parse.urlsplit(url)
    assert parsed.scheme == "http", f"{var}={url!r} is not an http URL"
    assert parsed.path in ("", "/") and not parsed.query, (
        f"{var}={url!r} carries a path. The gateway appends its own paths to this "
        "base URL (/connectors, /health), so a path here is requested twice."
    )

    services = [
        d for d in docs
        if d.get("kind") == "Service" and d["metadata"]["name"] == parsed.hostname
    ]
    assert len(services) == 1, (
        f"{var}={url!r} names host {parsed.hostname!r}, and the chart renders "
        f"{len(services)} Services by that name. The gateway would dial a host "
        "that does not resolve."
    )
    svc = services[0]
    targets = [p["targetPort"] for p in svc["spec"]["ports"] if p["port"] == parsed.port]
    assert targets, (
        f"{var}={url!r} uses port {parsed.port}, but Service {parsed.hostname} "
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


# Which container each address must reach, and the path admin/health probes on
# it (admin_health.go: Connect at <base>/, the sink at <base>/health). The
# container's own readiness probe on that path and port shows it serves what
# the gateway asks for.
EXPECTED = {
    "KAFKA_CONNECT_URL": ("kafka-connect", "/"),
    "KAFKA_SINK_URL": ("kafka-mcp-sink", "/health"),
}


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
@pytest.mark.parametrize("naming", sorted(NAMINGS))
def test_cdc_on_gives_the_gateway_both_real_addresses(naming):
    release, extra = NAMINGS[naming]
    docs = _render(release, *extra)
    env = _gateway_env(docs)

    missing = [v for v in CDC_ADDRESS_VARS if not env.get(v)]
    assert not missing, (
        f"the {GATEWAY} Deployment sets no {missing} with CDC enabled. The gateway "
        "falls back to compose names (kafka-connect, kafka-mcp-sink-mcp), which do "
        "not resolve on Kubernetes: every CDC table edit answers 502 "
        "connect_unreachable and admin/health reads both services down."
    )

    for var, (want_container, probe_path) in EXPECTED.items():
        container, target = _route(docs, env[var], var)
        assert container["name"] == want_container, (
            f"{var}={env[var]!r} reaches container {container['name']!r}, not "
            f"{want_container!r}"
        )
        ready = (container.get("readinessProbe") or {}).get("httpGet") or {}
        assert ready.get("path") == probe_path and ready.get("port") == target, (
            f"{want_container}'s readiness probe is {ready}, not {probe_path} on "
            f"{target!r}. The gateway's admin/health probe of <{var}>{probe_path} "
            "would test a path the pod is not known to serve."
        )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_cdc_off_keeps_the_sink_and_drops_only_connect():
    """connectors.cdc.enabled=false removes kafka-connect, never kafka-mcp-sink.

    Batch runs write through the sink, and batch pre-flight requires it whenever
    a pipeline has a destination (infra_preflight.go), so a batch-only install
    without it would fail every run pre-flight.
    """
    docs = _render("r", "--set", "connectors.cdc.enabled=false")

    # Anti-vacuity: the switch really removed Connect from this render, so the
    # absence of its address below is about the gateway, not a broken render.
    assert not _component(docs, "Service", "kafka-connect"), (
        "connectors.cdc.enabled=false still renders a kafka-connect Service"
    )
    assert _component(docs, "Service", "kafka-mcp-sink"), (
        "connectors.cdc.enabled=false dropped the kafka-mcp-sink Service; batch "
        "pre-flight requires it, so every batch run on this install would fail"
    )
    assert _component(docs, "Deployment", "kafka-mcp-sink"), (
        "connectors.cdc.enabled=false dropped the kafka-mcp-sink Deployment"
    )

    env = _gateway_env(docs)
    assert "KAFKA_CONNECT_URL" not in env, (
        f"the {GATEWAY} Deployment sets KAFKA_CONNECT_URL on an install with no "
        "Kafka Connect; admin/health would show a card for a service never run"
    )
    assert "KAFKA_SINK_URL" in env, (
        f"the {GATEWAY} Deployment lost KAFKA_SINK_URL with CDC off; the sink "
        "still runs, and kafkaSinkURL() falls back to a compose name that does "
        "not resolve here"
    )
