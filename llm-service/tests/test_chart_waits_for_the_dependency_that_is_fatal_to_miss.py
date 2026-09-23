"""A service that Fatals on a missing dependency must be made to wait for it, and a
service that tolerates one must not be. Two dependencies are covered here:
Temporal (the adapter waits, the api-gateway must not) and Kafka (the
orchestrator and the adapter wait, the api-gateway must not).

THE KAFKA HALF
--------------
`cmd/orchestrator/main.go` calls log.Fatalf the moment kafka.NewManager returns
an error, with no retry of any kind, so a broker that is not accepting
connections yet is a crash-loop and nothing else. The wait list did not include
Kafka: 1 restart on kind, and on a GKE node where the broker was itself waiting
for CPU, 7 restarts and a failed `helm --wait`. Same shape as the Temporal bug
below -- a CrashLoopBackOff that eventually resolves reads as noise rather than
as a missing edge in the boot order.

`cmd/adapter/main.go` has the same shape through a different door: it calls
createKafkaProducer, not kafka.NewManager, and log.Fatalf-s on the error. That
difference is why it was missed when the orchestrator was wired -- the guard
below grepped for the orchestrator's constructor by name, so it went on passing
while the adapter crash-looped. A fresh kind install of the 0.1.5 chart showed 4
restarts, every one "Failed to create Kafka producer ... connection refused".
The lesson is in test_no_service_fatals_on_kafka_unnoticed: match on what the
service DOES, not on the one spelling of it that was known when it was written.

It is opt-in for the same reason the Temporal wait is, and additionally skipped
for a BYO cluster: `kafka.external.bootstrapServers` is a CSV whose first entry
is not necessarily the one that is up, and a broker outside this release is not
part of its cold start.

THE TEMPORAL HALF
-----------------
The adapter cannot start without Temporal, so the chart must wait for it -- and
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
ORCHESTRATOR_MAIN = REPO / "backend-orchestrator/cmd/orchestrator/main.go"

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

# component -> waits for kafka, and the reason it does or does not.
KAFKA_WAIT_POLICY = {
    "orchestrator": (
        True,
        "kafka.NewManager failing is log.Fatalf with no retry, so a broker that "
        "is not accepting yet is a crash-loop",
    ),
    "temporal-adapter": (
        True,
        "createKafkaProducer failing is log.Fatalf with no retry, exactly as in "
        "the orchestrator, so a broker that is not accepting yet is a crash-loop",
    ),
    "api-gateway": (False, "it constructs no Kafka producer or manager at startup"),
}

# Every way a Go main has been seen to build a Kafka client at startup. The guard
# below fails on any spelling that is not already accounted for in
# KAFKA_WAIT_POLICY, because the previous version matched only the orchestrator's
# and therefore proved nothing about the adapter, which dies the same way.
KAFKA_STARTUP_CONSTRUCTORS = (
    "kafka.NewManager",
    "createKafkaProducer",
    "sarama.NewSyncProducer",
    "sarama.NewAsyncProducer",
    "sarama.NewConsumerGroup",
)


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


def test_the_helper_makes_the_kafka_wait_opt_in_and_in_chart_only():
    src = HELPERS.read_text()
    assert "$waitKafka := and (.kafka | default false) $root.Values.kafka.enabled" in src, (
        "rsync-ai.waitForDepsInitContainer no longer gates the kafka wait on BOTH an "
        "opt-in argument and kafka.enabled. Unconditional, it blocks every service on "
        "a broker only one of them needs; without the kafka.enabled half, a BYO "
        "install waits on the first entry of a bootstrapServers CSV, which is not "
        "necessarily a broker that is up."
    )
    assert "{{- if $waitKafka }}" in src, "the kafka wait is no longer guarded by $waitKafka."


def test_exactly_the_right_deployments_ask_for_the_kafka_wait():
    for component, (should_wait, why) in KAFKA_WAIT_POLICY.items():
        path = APPS / f"{component}.yaml"
        src = path.read_text()
        asks = '"kafka" true' in src
        if should_wait:
            assert asks, (
                f"{path.name} stopped asking for the kafka wait, and it needs one "
                f"because {why}. Without it the pod crash-loops through a cold boot."
            )
        else:
            assert not asks, f"{path.name} started asking for the kafka wait. Do not: {why}."


def test_the_orchestrators_intolerance_is_the_reason_it_waits():
    """KAFKA_WAIT_POLICY makes the orchestrator wait because it dies without a broker.
    If it grows a retry the wait is merely redundant -- but if this Fatal moves to a
    service that does NOT wait, that service inherits the crash-loop."""
    src = ORCHESTRATOR_MAIN.read_text()
    assert 'log.Fatalf("Failed to initialize Kafka: %v", err)' in src, (
        "backend-orchestrator no longer dies on a failed Kafka init. If it now retries, "
        "the initContainer wait is harmless and can stay; if the Fatal simply moved, "
        "find which binary owns it and make that one wait."
    )
    assert "kafka.NewManager(kafkaConfig)" in src


def test_the_adapters_intolerance_is_the_reason_it_waits():
    """The adapter's counterpart to the orchestrator assertion above. This is the
    line the live kind install produced 4 times before the broker accepted."""
    src = ADAPTER_MAIN.read_text()
    assert 'log.Fatalf("Failed to create Kafka producer: %v", err)' in src, (
        "backend-temporal-adapter no longer dies on a failed Kafka producer. If it "
        "now retries, the initContainer wait is harmless and can stay; if the Fatal "
        "moved, find which binary owns it and make that one wait."
    )
    assert "createKafkaProducer(kafkaBrokers)" in src


def test_no_service_fatals_on_kafka_unnoticed():
    """A service that builds a Kafka client at startup must be in KAFKA_WAIT_POLICY.

    This used to grep each main.go for "kafka.NewManager" -- the orchestrator's
    constructor -- and assert the other two did not contain it. The adapter builds
    its producer with createKafkaProducer, so the string was absent, the assertion
    passed, and the adapter crash-looped on every cold install regardless. Matching
    one known spelling proves nothing about a service that uses another, so match
    the whole family and let an unknown one fail loudly.
    """
    component_mains = {
        "orchestrator": ORCHESTRATOR_MAIN,
        "api-gateway": GATEWAY_MAIN,
        "temporal-adapter": ADAPTER_MAIN,
    }
    for component, main_go in component_mains.items():
        src = main_go.read_text()
        found = [c for c in KAFKA_STARTUP_CONSTRUCTORS if c in src]
        should_wait, why = KAFKA_WAIT_POLICY[component]
        if found and not should_wait:
            raise AssertionError(
                f"{main_go.name} builds a Kafka client at startup ({', '.join(found)}) "
                f"but KAFKA_WAIT_POLICY says it does not wait, because {why!r}. If it "
                f"Fatals on failure it needs the kafka wait; if it truly tolerates a "
                f"missing broker, say so here with the line that proves it."
            )
        if should_wait:
            assert found, (
                f"{main_go.name} no longer builds a Kafka client at startup, yet it "
                f"still asks for the kafka wait. Either the constructor was renamed -- "
                f"add it to KAFKA_STARTUP_CONSTRUCTORS -- or the wait is now dead "
                f"weight and the policy should say False."
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
def test_executing_the_rendered_script_probes_all_four_dependencies(tmp_path):
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
    assert len(seen) == 4, (
        f"expected probes for postgres, redis, temporal and kafka; got {seen}."
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_render_exactly_the_right_deployments_wait_for_kafka():
    docs = _render()
    for component, (should_wait, why) in KAFKA_WAIT_POLICY.items():
        script, _ = _wait_script(docs, component)
        waits = "kafka" in script
        assert waits == should_wait, (
            f"rendered {component} {'does not wait' if should_wait else 'waits'} "
            f"for kafka. {why}."
        )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_executing_the_rendered_script_probes_the_broker_the_orchestrator_dials(tmp_path):
    """The port is split out of kafka.bootstrap rather than written a second time,
    and a split is a computation: run it."""
    docs = _render()
    script, dep = _wait_script(docs, "orchestrator")
    brokers = _env(dep, "KAFKA_BROKERS")
    assert brokers, "the orchestrator has no KAFKA_BROKERS to compare against"
    host, _, port = brokers.rpartition(":")
    rc, seen, err = _run_with_stub_nc(script, tmp_path)
    assert rc == 0, f"the wait script failed: {err[-500:]}"
    assert (host, port) in seen, (
        f"the script probed {seen}, which does not include the broker {host}:{port} "
        f"the orchestrator will dial. A wait on any other address proves nothing."
    )
    assert len(seen) == 3, f"expected probes for postgres, redis and kafka; got {seen}."


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_byo_broker_is_not_waited_for(tmp_path):
    """Its first CSV entry is not necessarily the broker that is up, and a cluster
    that predates this install is not part of its cold start."""
    docs = _render([
        "--set", "kafka.enabled=false",
        "--set", "kafka.external.bootstrapServers=b1.example\\,b2.example:9093",
        "--set", "kafka.replicationFactor=2",
    ])
    # Both kafka waiters, not just the orchestrator: the adapter took the same
    # argument in #1145 and inherits the same exemption, and checking only one of
    # them would leave the other free to wait on a broker this install does not own.
    for component, expected in (("orchestrator", 2), ("temporal-adapter", 3)):
        script, _ = _wait_script(docs, component)
        assert "kafka" not in script, (
            f"{component} is waiting for a BYO broker:\n{script}"
        )
        # A directory each: the stub nc appends to a log inside it, so a shared
        # tmp_path would hand the second component the first one's probes.
        sandbox = tmp_path / component
        sandbox.mkdir()
        rc, seen, err = _run_with_stub_nc(script, sandbox)
        assert rc == 0, f"{component}: the wait script failed: {err[-500:]}"
        assert len(seen) == expected, (
            f"{component}: expected {expected} probes with no kafka; got {seen}."
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
