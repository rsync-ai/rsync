"""Every file the generator writes into the connector volume is readable by other uids.

The volume is shared by services that run as different users: tool-generator
(uid 1000) writes a generated connector, and the orchestrator (uid 100), the
api-gateway and the connector-deployer read it. A file only its writer can read
is a connector nobody else can find: on the 0.1.7-rc1 install the orchestrator
failed with "failed to locate connector xkcd" because latest.json was 0600 --
tempfile.NamedTemporaryFile creates its file 0600 whatever the umask is, and the
atomic rename kept that mode.

The class is "a writer into the connector volume produces a file only its own
uid can read", so the test walks everything a save, a promotion and a
deprecation leave behind rather than checking latest.json alone.
"""
from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from agents.tool_generator.deployment.version_manager import ConnectorVersionManager


@pytest.fixture
def container_umask():
    """The umask the service images run with (Docker's default)."""
    old = os.umask(0o022)
    try:
        yield
    finally:
        os.umask(old)


def _unreadable_by_others(root: Path) -> list[str]:
    bad = []
    for path in [root, *root.rglob("*")]:
        mode = stat.S_IMODE(path.stat().st_mode)
        need = 0o005 if path.is_dir() else 0o004
        if mode & need != need:
            bad.append(f"{path.relative_to(root.parent)} {oct(mode)}")
    return bad


def test_generated_connector_is_readable_by_other_uids(tmp_path: Path, container_umask):
    (tmp_path / "public").mkdir()
    vm = ConnectorVersionManager(connectors_base_path=str(tmp_path))
    name = "xkcd"
    artifacts = {
        "connector.py": "# generated\n",
        "metadata.json": json.dumps({"name": "XKCD", "connector_type": "xkcd"}),
        "requirements.txt": "requests\n",
        "Dockerfile": "FROM python:3.11-slim\n",
        "oauth/token_manager.py": "# nested artifact\n",
    }

    v1 = vm.calculate_next_version(name)
    vm.save_to_versioned_path(name, v1, artifacts)
    assert vm.promote_to_latest(name, v1, changes=["Initial version"], validation_passed=True)
    v2 = vm.calculate_next_version(name)
    vm.save_to_versioned_path(name, v2, artifacts)
    assert vm.promote_to_latest(name, v2, changes=["Regenerated"], validation_passed=True)
    assert vm.deprecate_version(name, v1, reason="superseded")

    connector_root = vm.get_connector_path(name)
    assert (connector_root / "latest.json").is_file()
    assert _unreadable_by_others(connector_root) == []
