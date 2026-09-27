"""Every Kafka bootstrapper creates the same platform topics, with the same config.

Four shell creators pre-create rsync's platform topics, and the orchestrator's
topology provisioner (backend-orchestrator/internal/kafka/topology.go) creates
the same names at runtime:

  scripts/kafka-init-new-topics.sh                 docker-compose.yml kafka-init
  docker-compose.quickstart.yml kafka-init         the one-command install
  deploy/helm/rsync-ai/templates/jobs/kafka-init.yaml   the Helm post-install Job
  scripts/create_kafka_topics.sh                   scripts/setup.sh (dev)

No creator alters a topic that already exists, so whichever runs first fixes
the partition count and config for good. A creator that disagrees does not fail:
it just wins on some installs. And a creator that still lists a removed topic
brings back a topic nothing reads, which on the broker looks like a working one.

This test RUNS each creator's shell against a stub kafka-topics.sh (and a stub
docker for create_kafka_topics.sh) that records every --create call, then
compares what was asked for. Nothing is parsed out of the scripts by regex, so a
topic created through a loop, a function or a variable is still seen.
"""

import os
import pathlib
import shlex
import subprocess
import textwrap

import pytest
import yaml

REPO = pathlib.Path(os.environ.get("RSYNC_REPO_ROOT") or pathlib.Path(__file__).resolve().parents[2])

PREFIX = "rsync."
WANT_TOPICS = {"pipeline.domain.events", "pii.scan.request", "pii.scan.response"}
WANT_PARTITIONS = "3"
WANT_CONFIG = {
    "cleanup.policy": "delete",
    "retention.ms": "604800000",  # 7 days, pipeline.domain.events included
    "compression.type": "snappy",
}
# Removed before v0.1.6. None of these has a producer and a consumer any more.
REMOVED = (
    "agent.",
    "task.assignments",
    "task.results",
    "pipeline.failed.dlq",
    "healer.actions",
    "sentinel.audit",
    "pipeline.agent.telemetry",
)


def _stub_dir(tmp_path):
    stub = tmp_path / "stub"
    stub.mkdir()
    (stub / "kafka-topics.sh").write_text('#!/bin/bash\necho "$*" >> "$STUB_LOG"\nexit 0\n')
    (stub / "kafka-broker-api-versions.sh").write_text(
        '#!/bin/bash\necho "broker:9092 (id: 1 rack: null) -> ("\n'
    )
    # create_kafka_topics.sh drives the broker through `docker exec`.
    (stub / "docker").write_text(
        textwrap.dedent(
            """\
            #!/bin/bash
            case "$1" in
              inspect) echo true ;;
              exec) shift; shift; echo "$*" >> "$STUB_LOG" ;;
            esac
            exit 0
            """
        )
    )
    for f in stub.iterdir():
        f.chmod(0o755)
    return stub


def _run(tmp_path, script_text, extra_env=None):
    stub = _stub_dir(tmp_path)
    log = tmp_path / "calls.log"
    # The Helm and quickstart scripts write their client config to /tmp.
    script_text = script_text.replace("/tmp/kafka-client", str(tmp_path / "kafka-client"))
    script = tmp_path / "creator.sh"
    script.write_text(script_text)
    env = {
        "HOME": str(tmp_path),
        "PATH": f"{stub}:/usr/bin:/bin",
        "STUB_LOG": str(log),
        "BOOTSTRAP": "kafka:9092",
        "KAFKA_BROKERS": "kafka:9092",
        "KAFKA_BROKER": "kafka:9092",
        **(extra_env or {}),
    }
    proc = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == 0, f"creator exited {proc.returncode}:\n{proc.stdout[-2000:]}\n{proc.stderr[-2000:]}"
    calls = [shlex.split(line) for line in log.read_text().splitlines() if "--create" in line]
    assert calls, "the creator ran and asked for no topic at all -- this test would prove nothing"
    return calls


def _creates(calls):
    """topic -> (partitions, {config: value}) for every --create call."""
    out = {}
    for argv in calls:
        topic, parts, cfg = None, None, {}
        for i, a in enumerate(argv):
            if a == "--topic":
                topic = argv[i + 1]
            elif a == "--partitions":
                parts = argv[i + 1]
            elif a == "--config":
                k, _, v = argv[i + 1].partition("=")
                cfg[k] = v
        out[topic] = (parts, cfg)
    return out


def _compose_command(compose_file, service):
    doc = yaml.safe_load((REPO / compose_file).read_text())
    cmd = doc["services"][service]["command"]
    body = cmd[-1] if isinstance(cmd, list) else cmd
    return body.replace("$$", "$")  # Compose's escape for a literal $


def _helm_job_script():
    lines = (REPO / "deploy/helm/rsync-ai/templates/jobs/kafka-init.yaml").read_text().splitlines()
    command = next(i for i, l in enumerate(lines) if l.strip() == "command:")
    start = next(i for i in range(command, len(lines)) if lines[i].strip() == "- |")
    indent = len(lines[start + 1]) - len(lines[start + 1].lstrip())
    body = []
    for l in lines[start + 1 :]:
        if l.strip() and len(l) - len(l.lstrip()) < indent:
            break
        body.append(l[indent:] if l.strip() else "")
    text = "\n".join(body)
    assert "{{" not in text, "the Job's script now has template directives; render it with helm instead"
    return text


CREATORS = {
    "scripts/kafka-init-new-topics.sh": lambda: (
        (REPO / "scripts/kafka-init-new-topics.sh").read_text(),
        # exactly what docker-compose.yml's kafka-init exports
        {"PARTITIONS": "3"},
    ),
    "scripts/kafka-init-new-topics.sh (default PARTITIONS)": lambda: (
        (REPO / "scripts/kafka-init-new-topics.sh").read_text(),
        {},
    ),
    "docker-compose.quickstart.yml kafka-init": lambda: (
        _compose_command("docker-compose.quickstart.yml", "kafka-init"),
        {},
    ),
    "helm jobs/kafka-init.yaml": lambda: (_helm_job_script(), {"KAFKA_REPLICATION_FACTOR": "1"}),
    "scripts/create_kafka_topics.sh": lambda: (
        (REPO / "scripts/create_kafka_topics.sh").read_text(),
        {},
    ),
}


def test_docker_compose_kafka_init_runs_the_script_with_three_partitions():
    body = _compose_command("docker-compose.yml", "kafka-init")
    assert 'export PARTITIONS="3"' in body
    assert "/scripts/kafka-init-new-topics.sh" in body


@pytest.mark.parametrize("name", sorted(CREATORS))
def test_creator_makes_exactly_the_platform_topics_with_the_orchestrator_config(name, tmp_path):
    text, env = CREATORS[name]()
    created = _creates(_run(tmp_path, text, env))

    assert set(created) == {PREFIX + t for t in WANT_TOPICS}, (
        f"{name} creates {sorted(created)}; every creator must create exactly "
        f"{sorted(PREFIX + t for t in WANT_TOPICS)}"
    )
    for topic, (parts, cfg) in created.items():
        assert parts == WANT_PARTITIONS, f"{name}: {topic} gets {parts} partitions, not {WANT_PARTITIONS}"
        for k, v in WANT_CONFIG.items():
            assert cfg.get(k) == v, f"{name}: {topic} has {k}={cfg.get(k)!r}, want {v!r}"


@pytest.mark.parametrize("name", sorted(CREATORS))
def test_creator_asks_for_no_removed_topic(name, tmp_path):
    text, env = CREATORS[name]()
    created = _creates(_run(tmp_path, text, env))
    stale = sorted(t for t in created for r in REMOVED if t.startswith(PREFIX + r) or t == PREFIX + r)
    assert not stale, f"{name} still creates removed topic(s): {stale}"
