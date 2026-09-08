"""A default install must come up able to run the sync modes the UI offers.

Compose and Helm are two implementations of one promise, and they hid the same
defect in different ways. The compose file declared the CDC services under a
`profiles: ["cdc"]` key and nothing in install.sh ever passed `--profile`; the
chart shipped `connectors.cdc.enabled: false`. Both installs therefore started
without kafka-connect, debezium-mcp and kafka-mcp-sink, and both reported
success -- `docker compose up` skips a profiled service silently, and a chart
renders a disabled block to nothing.

There is no degraded path behind that. The orchestrator's pre-flight builds its
required-service list from `isCDC`, which is true for sync_mode `cdc` OR
`streaming` (backend-orchestrator/internal/workers/infra_preflight.go:94-104),
and requires all three together (:176-200). A streaming pipeline on such an
install does not fall back to batch: it polls three absent services, then fails
the run naming a container that was never started.

So both paths carry the plane by default, and this file holds them to it
together. Reading one path's default out of the other's file is the point --
flipping one alone is exactly the drift that produced two different symptoms
for one missing promise.
"""

import os
import re

import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
INSTALL_SH = os.path.join(REPO_ROOT, "install.sh")
COMPOSE = os.path.join(REPO_ROOT, "docker-compose.quickstart.yml")
VALUES = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai", "values.yaml")
PREFLIGHT = os.path.join(
    REPO_ROOT, "backend-orchestrator", "internal", "workers", "infra_preflight.go"
)

# The three the pre-flight requires together. Asserted against the Go source
# below rather than trusted, so a rename there fails here instead of quietly
# making this file guard a service that no longer exists.
CDC_PLANE = ("kafka-connect", "debezium-mcp", "kafka-mcp-sink")


def _installer_default_profiles():
    with open(INSTALL_SH) as fh:
        lines = [ln for ln in fh if ln.startswith("RSYNC_PROFILES=")]
    assert len(lines) == 1, f"expected one RSYNC_PROFILES= line in install.sh; got {lines}"
    m = re.search(r"\$\{RSYNC_PROFILES-([^}]*)\}", lines[0])
    assert m, f"RSYNC_PROFILES is not a `${{RSYNC_PROFILES-...}}` default: {lines[0]!r}"
    return {p for p in m.group(1).replace(",", " ").split() if p}


def _chart_cdc_default():
    with open(VALUES) as fh:
        values = yaml.safe_load(fh)
    connectors = values.get("connectors") or {}
    assert "cdc" in connectors, "connectors.cdc disappeared from the chart's values.yaml"
    return connectors["cdc"].get("enabled")


def test_the_preflight_still_requires_all_three():
    """The premise. Without it every assertion below guards a stale claim."""
    with open(PREFLIGHT) as fh:
        body = fh.read()
    missing = [n for n in CDC_PLANE if f'"{n}"' not in body]
    assert not missing, (
        f"{os.path.basename(PREFLIGHT)} no longer names {missing} -- the CDC plane "
        "was renamed or the pre-flight stopped requiring it. Re-derive CDC_PLANE "
        "before trusting the rest of this file."
    )
    assert 'sm == "cdc" || sm == "streaming"' in body, (
        "the pre-flight no longer treats `streaming` as CDC, so the blast radius "
        "this file describes has changed"
    )


def test_the_compose_file_still_parks_the_plane_behind_one_profile():
    """A denominator check: three services, one profile, no stragglers."""
    services = yaml.safe_load(open(COMPOSE))["services"]
    profiled = {
        name: set(body["profiles"])
        for name, body in services.items()
        if isinstance(body, dict) and body.get("profiles")
    }
    behind_cdc = {n for n, p in profiled.items() if "cdc" in p}
    assert behind_cdc, "no service sits behind the `cdc` profile -- the check was vacuous"

    # The two name sets are close but not equal: the pre-flight uses the MCP's
    # logical name (`kafka-mcp-sink`) while compose suffixes the container that
    # serves it (`kafka-mcp-sink-mcp`). Match by prefix, and require the match to
    # be unique in both directions so the looser comparison cannot hide a
    # service that quietly left the profile.
    matched = {}
    for wanted in CDC_PLANE:
        hits = {n for n in behind_cdc if n == wanted or n.startswith(wanted + "-")}
        assert len(hits) == 1, (
            f"the `cdc` profile should hold exactly one service for the "
            f"pre-flight's {wanted!r}; it holds {sorted(hits)} of {sorted(behind_cdc)}"
        )
        matched[wanted] = hits.pop()
    assert set(matched.values()) == behind_cdc, (
        f"the `cdc` profile carries services the pre-flight does not require: "
        f"{sorted(behind_cdc - set(matched.values()))}. Either the pre-flight "
        f"stopped requiring one, or the profile grew a service that will not be "
        f"waited for."
    )


def test_the_docker_install_activates_the_cdc_profile_by_default():
    assert "cdc" in _installer_default_profiles(), (
        "install.sh does not activate the `cdc` profile, so `docker compose up` "
        "skips all three services and any streaming pipeline fails its run two "
        "minutes in on a stack whose install printed success"
    )


def test_the_kubernetes_install_enables_the_cdc_plane_by_default():
    assert _chart_cdc_default() is True, (
        "the chart renders the CDC plane to nothing by default, so a streaming "
        "pipeline on a fresh `helm install` fails the same way the docker path "
        f"did (got connectors.cdc.enabled={_chart_cdc_default()!r})"
    )


def test_the_two_install_paths_agree():
    """The defect was one promise with two implementations, so guard the pair.

    Each side is read from the other side's file. Turning CDC off for good is a
    legitimate decision; turning it off on ONE path is the drift that gave the
    same missing plane two unrelated-looking symptoms.
    """
    docker_on = "cdc" in _installer_default_profiles()
    kubernetes_on = _chart_cdc_default() is True
    assert docker_on == kubernetes_on, (
        f"a default install starts the CDC plane on docker={docker_on} but on "
        f"kubernetes={kubernetes_on}. Both paths install the same product; if "
        "the plane is now optional, make it optional on both."
    )
