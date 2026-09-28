"""The connector-seed init keeps the mcp_connectors volume in step with the release.

0.1.7-rc1 on a VM, two findings from one cause -- the volume is initialised once
and never reconciled:

* a fresh install left it root-owned, so tool-generator (uid 1000) could not
  save a generated connector;
* an upgrade from 0.1.6 skipped the seed ("already populated"), so the stack
  kept running 0.1.6 connector code under 0.1.7 services (mongodb/mysql
  connector.py, base_connector.py and storage_safety.py all differed).

So the seed runs on every boot and compares a stamp of the shipped catalog.
These tests run the real script on a scratch seed and target.
"""
from __future__ import annotations

import os
import re
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]  # shared/mcp-connectors
REPO = ROOT.parents[1]
SCRIPT = ROOT / "seed-connectors.sh"
STAMP = ".rsync-connector-seed"


def _write(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)


def _release(seed: Path, connector_py: str) -> None:
    """A miniature catalog in the shape Dockerfile.seed bakes."""
    _write(seed / "base_connector.py", "# base\n")
    _write(seed / "public" / "mongodb" / "latest.json", '{"current_version": "v1.0.3"}')
    _write(seed / "public" / "mongodb" / "versions" / "v1.0.3" / "connector.py", connector_py)
    _write(seed / "internal" / "debezium" / "latest.json", '{"current_version": "v1.0.0"}')
    _write(seed / "internal" / "debezium" / "versions" / "v1.0.0" / "server.py", "# dbz\n")


def _seed(seed: Path, target: Path) -> subprocess.CompletedProcess:
    env = dict(os.environ, SEED_DIR=str(seed), SEED_TARGET=str(target), CONNECTOR_OWNER="")
    return subprocess.run(["sh", str(SCRIPT)], env=env, capture_output=True, text=True, check=True)


@pytest.fixture
def dirs(tmp_path: Path):
    seed, target = tmp_path / "seed", tmp_path / "target"
    seed.mkdir()
    target.mkdir()
    return seed, target


def test_first_boot_copies_the_catalog_and_stamps_it(dirs):
    seed, target = dirs
    _release(seed, "# v0.1.7\n")
    _seed(seed, target)
    assert (target / "public/mongodb/versions/v1.0.3/connector.py").read_text() == "# v0.1.7\n"
    assert (target / "internal/debezium/versions/v1.0.0/server.py").is_file()
    assert (target / STAMP).read_text().strip()


def test_an_upgrade_refreshes_shipped_connectors_and_keeps_generated_ones(dirs):
    seed, target = dirs
    _release(seed, "# v0.1.6\n")
    _seed(seed, target)
    old_stamp = (target / STAMP).read_text()
    # What the running install adds: a generated connector, and a file the old
    # release shipped inside a version dir that the new release no longer has.
    _write(target / "public/xkcd/latest.json", '{"current_version": "v1.0.0"}')
    _write(target / "public/xkcd/versions/v1.0.0/connector.py", "# generated\n")
    _write(target / "public/mongodb/versions/v1.0.3/dropped_helper.py", "# gone in 0.1.7\n")

    _release(seed, "# v0.1.7\n")
    out = _seed(seed, target)

    assert (target / "public/mongodb/versions/v1.0.3/connector.py").read_text() == "# v0.1.7\n", out.stdout
    assert not (target / "public/mongodb/versions/v1.0.3/dropped_helper.py").exists()
    assert (target / "public/xkcd/versions/v1.0.0/connector.py").read_text() == "# generated\n"
    assert (target / STAMP).read_text() != old_stamp


def test_a_restart_on_the_same_release_changes_nothing(dirs):
    seed, target = dirs
    _release(seed, "# v0.1.7\n")
    _seed(seed, target)
    marker = target / "public/mongodb/versions/v1.0.3/connector.py"
    marker.write_text("# touched after boot\n")
    out = _seed(seed, target)
    assert marker.read_text() == "# touched after boot\n", out.stdout


def test_everything_seeded_is_readable_by_the_other_service_uids(dirs):
    seed, target = dirs
    _release(seed, "# v0.1.7\n")
    for p in seed.rglob("*"):  # a build context checked out with a tight umask
        p.chmod(0o700 if p.is_dir() else 0o600)
    _seed(seed, target)
    bad = []
    for p in [target, *target.rglob("*")]:
        need = 0o005 if p.is_dir() else 0o004
        if stat.S_IMODE(p.stat().st_mode) & need != need:
            bad.append(str(p.relative_to(target)))
    assert bad == []


def test_the_volume_is_handed_to_the_uid_tool_generator_runs_as():
    """The seed chowns to CONNECTOR_OWNER's default; that must be the uid the
    llm-service images (tool-generator) run as, or saves fail with EACCES."""
    m = re.search(r'CONNECTOR_OWNER-(\d+):(\d+)', SCRIPT.read_text())
    assert m, "seed-connectors.sh must default CONNECTOR_OWNER to uid:gid"
    uids = set()
    for dockerfile in (REPO / "llm-service").glob("Dockerfile*"):
        uids |= set(re.findall(r"useradd\b[^\n]*?-u\s+(\d+)", dockerfile.read_text()))
    assert uids, "found no useradd -u in llm-service/Dockerfile*"
    assert uids == {m.group(1)}, f"tool-generator runs as {uids}, the seed hands the volume to {m.group(1)}"


def test_every_seed_runner_uses_the_script_not_an_inline_guard():
    """The old inline guard skipped any populated volume. It lived in two compose
    files as a `command:` override; one copy left behind brings the bug back."""
    dockerfile = (ROOT / "Dockerfile.seed").read_text()
    assert "seed-connectors" in dockerfile.split("CMD", 1)[-1], "Dockerfile.seed CMD must run the script"
    for compose in ("docker-compose.quickstart.yml", "docker-compose.oss.yml"):
        text = (REPO / compose).read_text()
        block = text.split("  connector-seed:\n", 1)[1].split("\n  # ", 1)[0]
        assert "ls -A /target" not in block and "command:" not in block, (
            f"{compose} connector-seed overrides the image's seed script"
        )
