"""A connector container the tool-generator starts in-process gets rotated logs.

Compose writes `logging: json-file max-size/max-file` on the services it starts, but
a container created through the Docker SDK gets only the daemon default -- and a stock
daemon's default is json-file with NO rotation, so a chatty connector's log grows until
the disk is full. connector-deployer applies RSYNC_LOG_MAX_SIZE / RSYNC_LOG_MAX_FILE to
every container it creates (connector-deployer/internal/spec/spec.go); the legacy
in-process path (DEPLOYER_URL unset) must apply the same bound.
"""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

docker_errors = pytest.importorskip("docker.errors")

from src.agents.tool_generator.deployment import docker_builder as db  # noqa: E402


def _start(monkeypatch) -> dict:
    monkeypatch.delenv("DEPLOYER_URL", raising=False)
    builder = db.DockerBuilder()
    builder.client = MagicMock()
    builder.client.containers.get.side_effect = docker_errors.NotFound("absent")
    monkeypatch.setattr(builder, "_check_docker_available", lambda: True)
    monkeypatch.setattr(builder, "_is_protected_compose_container", lambda name: False)

    ok, _ = asyncio.run(builder.start_container("mcp-hubspot:v1.0.0", "rsync-ai-hubspot-v1-0-0-mcp"))
    assert ok
    assert builder.client.containers.run.call_count == 1
    return builder.client.containers.run.call_args.kwargs


def test_default_rotation_matches_compose(monkeypatch):
    monkeypatch.delenv("RSYNC_LOG_MAX_SIZE", raising=False)
    monkeypatch.delenv("RSYNC_LOG_MAX_FILE", raising=False)
    kwargs = _start(monkeypatch)
    assert kwargs["log_config"] == {"type": "json-file", "config": {"max-size": "10m", "max-file": "3"}}


def test_env_overrides_rotation(monkeypatch):
    monkeypatch.setenv("RSYNC_LOG_MAX_SIZE", "50m")
    monkeypatch.setenv("RSYNC_LOG_MAX_FILE", "5")
    kwargs = _start(monkeypatch)
    assert kwargs["log_config"] == {"type": "json-file", "config": {"max-size": "50m", "max-file": "5"}}


def test_empty_size_leaves_daemon_default(monkeypatch):
    monkeypatch.setenv("RSYNC_LOG_MAX_SIZE", "")
    kwargs = _start(monkeypatch)
    assert kwargs["log_config"] is None
