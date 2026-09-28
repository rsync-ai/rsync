"""install.sh can install what a checkout holds, not only what a release tag holds.

Before RSYNC_COMPOSE_DIR, the one-command installer could only ever fetch its
compose files from `raw.githubusercontent.com/<RSYNC_REPO>/<RSYNC_REF>` and pull
images from `ghcr.io/rsync-ai`, hard-coded fifteen times in the quickstart. A
release candidate -- a commit no tag carries, built into a registry that is not
ghcr -- could not be installed by the command operators run. Testing it meant
hand-rolling the steps the installer does, which tests those steps and not the
installer.

The cases below EXECUTE install.sh's own fetch() and check_compose_dir(), lifted
out of the file rather than restated, because what they guard is behaviour: a
checkout copy that silently falls through to the network, or an image tag that
silently defaults to the last release, both read as reasonable lines.
"""

import os
import shutil
import subprocess

import pytest

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
INSTALL_SH = os.path.join(REPO_ROOT, "install.sh")
QUICKSTART = "docker-compose.quickstart.yml"
URL = f"https://raw.githubusercontent.com/rsync-ai/rsync/v0.1.5/{QUICKSTART}"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")


def _read_install_sh():
    with open(INSTALL_SH) as fh:
        return fh.read()


def _function_body(name):
    """Brace to brace, as test_installer_rerun_upgrades_both_halves lifts them."""
    out, capturing = [], False
    for line in _read_install_sh().splitlines(keepends=True):
        if line.startswith(f"{name}()"):
            capturing = True
        if capturing:
            out.append(line)
            if line.rstrip() == "}":
                return "".join(out)
    raise AssertionError(f"{name}() not found in install.sh")


def _run(tmp_path, body, compose_dir="", version_requested="", cwd=None):
    """install.sh's function under test, with a PATH whose curl and wget only
    record that they were called -- so "copied from the checkout" and "quietly
    downloaded instead" are different outcomes, and nothing reaches the network."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    for tool in ("curl", "wget"):
        stub = bin_dir / tool
        stub.write_text(f'#!/bin/sh\necho {tool} >> "{tmp_path}/network.calls"\nexit 1\n')
        stub.chmod(0o755)
    script = tmp_path / "harness.sh"
    script.write_text(
        "set -euo pipefail\n"
        f'PATH="{bin_dir}:$PATH"\n'
        'RSYNC_REF="v0.1.5"\nRSYNC_REPO="rsync-ai/rsync"\n'
        f'COMPOSE_FILE="{QUICKSTART}"\n'
        f'RSYNC_COMPOSE_DIR="{compose_dir}"\n'
        f'RSYNC_VERSION_REQUESTED="{version_requested}"\n'
        'RSYNC_VERSION="${RSYNC_VERSION_REQUESTED:-0.1.5}"\n'
        'RSYNC_IMAGE_REGISTRY="ghcr.io/rsync-ai"\n'
        'info(){ echo "INFO $*"; }\nerror(){ echo "ERROR $*" >&2; }\n'
        + _function_body("fetch")
        + _function_body("check_compose_dir")
        + body
    )
    return subprocess.run(
        ["bash", str(script)], capture_output=True, text=True, cwd=cwd or tmp_path
    )


def _network_calls(tmp_path):
    log = tmp_path / "network.calls"
    return log.read_text().split() if log.exists() else []


def _checkout(tmp_path, content="# the checkout's quickstart\n"):
    src = tmp_path / "checkout"
    src.mkdir()
    (src / QUICKSTART).write_text(content)
    return src


def test_fetch_copies_the_checkouts_file_and_never_reaches_the_network(tmp_path):
    src = _checkout(tmp_path)
    dest = tmp_path / "install" / QUICKSTART
    dest.parent.mkdir()

    out = _run(tmp_path, f'fetch "{URL}" "{dest}"\n', compose_dir=str(src))

    assert out.returncode == 0, out.stderr
    assert dest.read_text() == "# the checkout's quickstart\n"
    assert _network_calls(tmp_path) == [], "checkout mode downloaded the file anyway"


def test_the_download_path_is_what_runs_without_a_checkout(tmp_path):
    """The control. Without it, a stub PATH that never ran would pass the case
    above just as well as a fetch() that took the checkout branch."""
    dest = tmp_path / QUICKSTART
    out = _run(tmp_path, f'fetch "{URL}" "{dest}"\n')
    assert out.returncode != 0
    assert _network_calls(tmp_path) == ["curl"]


def test_fetch_refuses_a_checkout_missing_the_file(tmp_path):
    """Not a fallback to the network: the operator asked for THIS checkout, and
    one overlay silently from a release tag would be a stack from two commits."""
    src = tmp_path / "checkout"
    src.mkdir()
    dest = tmp_path / "docker-compose.byo-kafka.yml"

    out = _run(tmp_path, f'fetch "{URL.replace(QUICKSTART, dest.name)}" "{dest}"\n', compose_dir=str(src))

    assert out.returncode == 1
    assert "missing or empty" in out.stderr
    assert not dest.exists()
    assert _network_calls(tmp_path) == []


def test_fetch_into_the_checkout_itself_is_not_an_error(tmp_path):
    """INSTALL_DIR may be the clone. `cp` of a file onto itself exits 1, which
    under `set -e` would end the install at its first compose file."""
    src = _checkout(tmp_path)
    out = _run(tmp_path, f'fetch "{URL}" "{src / QUICKSTART}"\n', compose_dir=str(src))
    assert out.returncode == 0, out.stderr
    assert (src / QUICKSTART).read_text() == "# the checkout's quickstart\n"


def test_a_checkout_without_an_explicit_version_is_refused(tmp_path):
    """The defect this precondition exists for. A checkout cannot say which tag
    its images were pushed under; defaulting to the release RSYNC_REF names runs
    the checkout's compose file against the previous release's images -- the
    pairing the top of install.sh documents as the one to avoid."""
    src = _checkout(tmp_path)
    out = _run(tmp_path, "check_compose_dir\n", compose_dir=str(src))
    assert out.returncode == 1
    assert "needs RSYNC_VERSION" in out.stderr


def test_a_checkout_with_a_version_is_accepted_and_recorded_absolute(tmp_path):
    """Relative in, absolute out: the path is recorded as RSYNC_INSTALLED_REF and
    compared on every later run, so `.` must not mean two different places."""
    _checkout(tmp_path)
    out = _run(
        tmp_path,
        'check_compose_dir\necho "DIR=${RSYNC_COMPOSE_DIR}"\n',
        compose_dir="checkout",
        version_requested="0.1.7-rc1",
    )
    assert out.returncode == 0, out.stderr
    got = [l for l in out.stdout.splitlines() if l.startswith("DIR=")]
    assert got == [f"DIR={os.path.realpath(tmp_path / 'checkout')}"]


def test_a_directory_that_is_not_a_checkout_is_refused(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    out = _run(tmp_path, "check_compose_dir\n", compose_dir=str(empty), version_requested="x")
    assert out.returncode == 1
    assert f"has no {QUICKSTART}" in out.stderr


def test_no_checkout_means_no_precondition(tmp_path):
    """The ordinary `curl | bash` install names neither variable and must pass
    straight through -- the checkout mode is opt-in, never inferred."""
    out = _run(tmp_path, "check_compose_dir\necho reached\n")
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "reached"


def test_the_requested_version_is_captured_before_the_ref_derives_one():
    """The precondition above reads RSYNC_VERSION_REQUESTED, not RSYNC_VERSION,
    because by main() the derivation from RSYNC_REF has filled RSYNC_VERSION in
    for everyone. Captured after it, the capture would always be non-empty and
    the refusal could never fire."""
    src = _read_install_sh()
    capture = src.find('RSYNC_VERSION_REQUESTED="${RSYNC_VERSION:-}"')
    derive = src.find('RSYNC_VERSION="${RSYNC_VERSION:-')
    assert capture != -1, "install.sh no longer captures the requested version"
    assert derive != -1, "install.sh no longer derives RSYNC_VERSION from RSYNC_REF"
    assert capture < derive, "the requested version is captured after the derivation fills it in"


def test_main_runs_the_precondition_before_anything_is_written():
    """check_compose_dir is only a guard if it runs before write_env and the
    compose download -- after them, the refused install has already happened."""
    src = _read_install_sh()
    main = src[src.index("\nmain() {"):]
    guard, *writers = [main.find(s) for s in ("check_compose_dir", "write_env", "download_compose")]
    assert -1 not in (guard, *writers), "main() lost a step"
    assert guard < min(writers), "check_compose_dir runs after main() starts writing"
