"""The kafka-mcp-sink sees RSYNC_SCHEMA_DRIFT_ENABLED with the orchestrator's value.

The sink worker writes the columns CDC adds on its own to rsync.healer.schema-changes,
and only when RSYNC_SCHEMA_DRIFT_ENABLED is "true" (newSchemaDriftWriter in
kafka-sink-worker/main.go). The orchestrator creates that topic, and starts the
healer that consumes it, under the same flag. If the two containers disagree:

  * sink on, orchestrator off: the sink's writer makes the broker auto-create an
    unread topic at the broker's defaults;
  * sink off, orchestrator on: CDC drift reaches no Schema changes tab.

So every file that hands the flag to the orchestrator must hand the sink the same
value, byte for byte.
"""

import os
import re

import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
FLAG = "RSYNC_SCHEMA_DRIFT_ENABLED"
SINK_SERVICE = "kafka-mcp-sink-mcp"
SINK_MAIN = os.path.join(
    REPO_ROOT,
    "shared", "mcp-connectors", "internal", "kafka-mcp-sink",
    "worker-src", "cmd", "kafka-sink-worker", "main.go",
)
# Compose files that define both services themselves: the base (dev, and prod via
# its overlay) and the standalone quickstart.
COMPOSE_FILES = ("docker-compose.yml", "docker-compose.quickstart.yml")


def _environment(path, service):
    with open(os.path.join(REPO_ROOT, path), encoding="utf-8") as fh:
        doc = yaml.safe_load(fh) or {}
    svc = (doc.get("services") or {}).get(service)
    assert isinstance(svc, dict), f"{path} has no `{service}` service"
    env = svc.get("environment") or {}
    if isinstance(env, list):
        env = dict(item.split("=", 1) if "=" in item else (item, "") for item in env)
    return env


def test_the_sink_worker_reads_the_flag():
    with open(SINK_MAIN, encoding="utf-8") as fh:
        src = fh.read()
    assert re.search(r'os\.Getenv\(\s*"' + FLAG + r'"\s*\)', src), (
        f"the sink worker no longer reads {FLAG}; this guard is pinning a mirror "
        "nothing consumes — drop it"
    )


def test_the_base_compose_passes_the_flag_to_both():
    """Arms the loop below: the base file must actually carry the flag on the
    orchestrator, or every comparison there passes by being skipped."""
    assert FLAG in _environment("docker-compose.yml", "orchestrator")


def test_every_compose_file_mirrors_the_orchestrator_value_to_the_sink():
    for path in COMPOSE_FILES:
        orch = _environment(path, "orchestrator")
        sink = _environment(path, SINK_SERVICE)
        if FLAG not in orch:
            assert FLAG not in sink, (
                f"{path}: the sink gets {FLAG} but the orchestrator does not, so the "
                "sink can write to a healer topic the orchestrator never creates"
            )
            continue
        assert FLAG in sink, (
            f"{path}: the orchestrator gets {FLAG} but {SINK_SERVICE} does not, so "
            "CDC-applied drift is never reported when the loop is on"
        )
        assert str(sink[FLAG]) == str(orch[FLAG]), (
            f"{path}: {FLAG} differs: orchestrator={orch[FLAG]!r}, "
            f"{SINK_SERVICE}={sink[FLAG]!r}"
        )


def test_the_chart_does_not_pass_the_flag_to_only_one_side():
    """The Helm chart passes the flag to neither today, which keeps both off.
    Passing it to one template and not the other reintroduces the split."""
    apps = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai", "templates")
    orch_tpl = os.path.join(apps, "apps", "orchestrator.yaml")
    sink_tpl = os.path.join(apps, "connectors", "cdc.yaml")
    assert os.path.exists(orch_tpl) and os.path.exists(sink_tpl), (orch_tpl, sink_tpl)

    def sets_flag(path):
        with open(path, encoding="utf-8") as fh:
            return bool(re.search(r"name:\s*" + FLAG + r"\b", fh.read()))

    assert sets_flag(orch_tpl) == sets_flag(sink_tpl), (
        f"{FLAG} is set on only one of orchestrator.yaml / connectors/cdc.yaml"
    )
