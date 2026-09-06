"""The adapter cannot start without Temporal, so the chart must wait for it -- and
the api-gateway, which can, must not.

WHAT WENT WRONG
---------------
`rsync-ai.waitForDepsInitContainer` waited for postgres and redis and stopped
there. backend-temporal-adapter dials Temporal at boot and, on the first refused
connection, called log.Fatalf. On a cold `helm install` the frontend is not
listening yet, so the pod died, backed off, died again: 3 restarts observed on a
kind cluster for a dependency that was up twenty seconds later. Nothing was
broken afterwards, which is why it survived -- a CrashLoopBackOff that resolves
itself reads as noise rather than as a missing edge in the boot order.

WHY THIS IS NOT SIMPLY "ADD TEMPORAL TO THE WAIT"
-------------------------------------------------
Two of the three callers hold a Temporal client, and only one of them may wait.

  temporal-adapter   MUST wait. Every worker it registers is constructed from
                     the client, so there is no degraded mode; missing Temporal
                     is fatal by construction.
  api-gateway        MUST NOT wait. It dials in a bounded retry and then carries
                     on with a nil client, because it serves its whole read
                     surface and the entire UI without Temporal. Gating its
                     initContainer would convert "pipeline runs are refused"
                     into "the product is down", which is a strictly worse
                     outcome than the one being fixed.
  orchestrator       Holds no Temporal client at all.

So the property is a pair, and the second half is the one that would be lost
first: a later reader sees an asymmetry, assumes it is an oversight, and
"fixes" it. Both halves are asserted below, each with its reason attached.

TWO LAYERS, as in test_chart_docker_host_features_are_off.py
------------------------------------------------------------
  1. static -- reads the template and the Go sources; runs with nothing
     installed.
  2. render -- runs `helm template`, then EXECUTES the generated wait script
     under /bin/sh with a recording stub in place of nc. A shell parameter
     expansion that splits host from port is a computation; grepping its source
     proves the characters are present, not that they split anything. It skips
     without helm, and a skip is not a pass, which is why layer 1 stands alone.
"""

import os
import pathlib
import shutil
import stat
import subprocess

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
# One slash-joined literal each, rather than a segment per path component. That is
# the shape test_ci_filter_covers_every_guard_subject.py's deriver recognises, and a
# guard it derives nothing from is one the CI-coverage census cannot check -- which
# looks exactly like a guard whose subjects are all covered.
CHART = REPO / "deploy/helm/rsync-ai"
HELPERS = REPO / "deploy/helm/rsync-ai/templates/_helpers.tpl"
APPS = REPO / "deploy/helm/rsync-ai/templates/apps"
ADAPTER_MAIN = REPO / "backend-temporal-adapter/cmd/adapter/main.go"
GATEWAY_MAIN = REPO / "api-gateway/cmd/server/main.go"

# The documented install command's flags, so a render reaches this property
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

# component -> waits for temporal, and the reason it does or does not.
WAIT_POLICY = {
    "temporal-adapter": (
        True,
        "it Fatals without a client and every worker is built from that client",
    ),
    "api-gateway": (
        False,
        "it serves the whole UI without Temporal; gating it turns a partial "
        "outage into a total one",
    ),
    "orchestrator": (False, "it holds no Temporal client at all"),
}


# ── layer 1: static ─────────────────────────────────────────────────────────

def test_the_helper_makes_the_temporal_wait_opt_in():
    """An unconditional wait would drag the api-gateway in with it."""
    src = HELPERS.read_text()
    assert "$waitTemporal := .temporal" in src, (
        "rsync-ai.waitForDepsInitContainer no longer reads a `temporal` argument. "
        "If the wait became unconditional, the api-gateway now blocks on Temporal "
        "and a Temporal outage takes the UI down with it."
    )
    assert "{{- if $waitTemporal }}" in src, (
        "the temporal wait is no longer guarded by $waitTemporal."
    )


def test_exactly_the_right_deployments_ask_for_the_temporal_wait():
    for component, (should_wait, why) in WAIT_POLICY.items():
        path = APPS / f"{component}.yaml"
        src = path.read_text()
        assert "rsync-ai.waitForDepsInitContainer" in src, (
            f"{path.name} no longer includes the ordering gate at all."
        )
        asks = '"temporal" true' in src
        if should_wait:
            assert asks, (
                f"{path.name} stopped asking for the temporal wait, and it needs "
                f"one because {why}. Without it the pod crash-loops through a "
                f"cold boot instead of waiting one out."
            )
        else:
            assert not asks, (
                f"{path.name} started asking for the temporal wait. Do not: {why}."
            )


def test_the_adapter_retries_before_it_gives_up():
    """A wait in the chart does not help compose, bare metal, or the window where
    the frontend accepts a connection just before it can serve one."""
    src = ADAPTER_MAIN.read_text()
    assert "temporalDeadline := time.Now().Add(60 * time.Second)" in src, (
        "backend-temporal-adapter no longer bounds its Temporal dial with a "
        "deadline. It went back to dying on the first refused connection, which "
        "no initContainer can prevent outside Kubernetes."
    )
    assert "for attempt := 1; ; attempt++ {" in src, (
        "the Temporal dial is no longer in a retry loop."
    )


def test_the_adapter_still_dies_when_the_budget_runs_out():
    """The retry must not become a nil-client degraded mode: this process has no
    such mode, and a Running pod that registered no workers is invisible."""
    src = ADAPTER_MAIN.read_text()
    assert "Failed to create Temporal client at %s after %d attempts" in src, (
        "backend-temporal-adapter stopped treating an exhausted retry budget as "
        "fatal. Every worker below that call is constructed from the client, so "
        "carrying on without one yields a pod that is Running, Ready, and "
        "processing nothing -- with no signal in `kubectl get pods`."
    )
    assert "log.Fatalf(\"Failed to create Temporal client at %s" in src, (
        "the exhausted-budget branch is no longer fatal."
    )


def test_the_gateways_tolerance_is_the_reason_it_is_exempt():
    """WAIT_POLICY exempts api-gateway because it survives a missing Temporal. If
    that stops being true the exemption is wrong, and this is where it shows."""
    src = GATEWAY_MAIN.read_text()
    assert "will lazily reconnect on first run" in src, (
        "api-gateway no longer carries on after failing to reach Temporal. Its "
        "exemption from the initContainer wait rested on exactly that. Either "
        "restore the tolerance or move it into the waiting set -- but do not "
        "leave a service that dies without Temporal outside the gate."
    )


# ── layer 2: render, then execute ───────────────────────────────────────────

def _render(extra=()):
    out = subprocess.run(
        ["helm", "template", "rsync-test", str(CHART), *RENDER_FLAGS, *extra],
        capture_output=True, text=True,
    )
    assert out.returncode == 0, out.stderr[-2000:]
    return [d for d in yaml.safe_load_all(out.stdout) if d]


def _wait_script(docs, component):
    for d in docs:
        if d.get("kind") != "Deployment":
            continue
        if d["metadata"]["labels"].get("app.kubernetes.io/component") != component:
            continue
        for ic in d["spec"]["template"]["spec"].get("initContainers") or []:
            if ic["name"] == "wait-for-deps":
                return ic["command"][-1], d
    raise AssertionError(f"no wait-for-deps initContainer on {component}")


def _env(dep, name):
    for e in dep["spec"]["template"]["spec"]["containers"][0].get("env") or []:
        if e["name"] == name:
            return e.get("value")
    return None


def _run_with_stub_nc(script, tmp_path):
    """Execute the rendered script with a recording stub in place of nc.

    Returns (returncode, [(host, port), ...], stderr). The stub exits 0, so every
    wait_for returns on its first attempt and the 2s sleeps never run.
    """
    log = tmp_path / "nc.log"
    stub = tmp_path / "nc"
    stub.write_text(f'#!/bin/sh\nprintf "%s %s\\n" "$2" "$3" >> {log}\nexit 0\n')
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    env = dict(os.environ, PATH=f"{tmp_path}:{os.environ['PATH']}")
    out = subprocess.run(["/bin/sh", "-c", script], capture_output=True,
                         text=True, env=env, timeout=30)
    seen = []
    if log.exists():
        seen = [tuple(line.split()) for line in log.read_text().split("\n") if line]
    return out.returncode, seen, out.stderr


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_render_only_the_adapter_waits_for_temporal():
    docs = _render()
    for component, (should_wait, why) in WAIT_POLICY.items():
        script, _ = _wait_script(docs, component)
        waits = "temporal" in script
        assert waits == should_wait, (
            f"rendered {component} {'does not wait' if should_wait else 'waits'} "
            f"for temporal. {why}."
        )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_render_the_adapter_waits_on_the_address_it_will_dial():
    """Two independent renders of the same address is drift waiting to happen:
    the wait would pass against a host the process never contacts."""
    docs = _render()
    script, dep = _wait_script(docs, "temporal-adapter")
    address = _env(dep, "TEMPORAL_ADDRESS")
    assert address, "the adapter has no TEMPORAL_ADDRESS to compare against"
    assert f'temporal_addr="{address}"' in script, (
        f"the wait-for-deps script does not target TEMPORAL_ADDRESS ({address}). "
        f"A wait on any other host is satisfied by something the adapter will "
        f"never dial."
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_executing_the_rendered_script_probes_all_three_dependencies(tmp_path):
    docs = _render()
    script, dep = _wait_script(docs, "temporal-adapter")
    address = _env(dep, "TEMPORAL_ADDRESS")
    host, _, port = address.rpartition(":")
    rc, seen, err = _run_with_stub_nc(script, tmp_path)
    assert rc == 0, f"the wait script failed: {err[-500:]}"
    assert (host, port) in seen, (
        f"the script probed {seen}, which does not include the temporal address "
        f"{host}:{port} it renders. The host/port split is not producing what the "
        f"literal suggests."
    )
    assert len(seen) == 3, (
        f"expected probes for postgres, redis and temporal; got {seen}."
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_executing_the_rendered_script_unwraps_an_ipv6_literal(tmp_path):
    """Go's host:port convention brackets an IPv6 address; nc takes it bare."""
    docs = _render([
        "--set", "temporal.enabled=false",
        "--set", "temporal.external.address=[2001:db8::1]:7233",
    ])
    script, _ = _wait_script(docs, "temporal-adapter")
    rc, seen, err = _run_with_stub_nc(script, tmp_path)
    assert rc == 0, f"the wait script failed: {err[-500:]}"
    assert ("2001:db8::1", "7233") in seen, (
        f"probed {seen}; an IPv6 literal must reach nc unbracketed, or every "
        f"attempt fails against an address that cannot resolve."
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_an_address_with_no_port_is_refused_out_loud(tmp_path):
    """Silence here would be a 120-second wait against a port named 'temporal'."""
    docs = _render([
        "--set", "temporal.enabled=false",
        "--set", "temporal.external.address=temporal.example.com",
    ])
    script, _ = _wait_script(docs, "temporal-adapter")
    rc, seen, err = _run_with_stub_nc(script, tmp_path)
    assert rc == 1, (
        f"an address with no port must fail the initContainer, not be probed. "
        f"rc={rc}, probes={seen}"
    )
    assert "has no port" in err, f"the refusal says nothing useful: {err[-500:]}"
