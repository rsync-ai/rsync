"""`install.sh` must start in the environment it documents support for.

The script has a whole branch for callers that have no terminal. `setup_tty`
probes for one, `prompt_env` announces that it is running non-interactively and
lists the variables it will read instead, and the comment above it names the
callers by hand: cloud-init, CI, a Dockerfile RUN.

Every one of those runs with `HOME` unset. So did the GCE startup script that
found this: `docker.io` installed, the external IP resolved, and then

    /root/install.sh: line 104: HOME: unbound variable

with nothing else printed and exit 1. `set -euo pipefail` on line 2 makes reading
an unset `HOME` fatal, and the read sat at the top level -- before the banner,
before `check_docker`, and a thousand lines before the no-terminal branch written
for exactly that caller. The one install path that cannot answer a prompt was
also the one path that could not start.

It survived every clean-room verification of this script because all of them ran
over an interactive ssh session, where the shell sets `HOME` on login. A terminal
was never required to reproduce it; it was required to *hide* it.

These tests execute the script rather than reading it, because the property is a
runtime one: whether the process gets past its own prologue. They stop the run at
the Docker pre-flight -- the first thing after the prologue that reports a
decision -- by handing the script a PATH with no `docker` on it. That PATH also
carries no `getent`, so the same run exercises the fallback's last branch, the
one a Mac takes, on a Linux runner that has getent in /usr/bin.
"""

import os
import pathlib
import shutil
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
INSTALL_SH = REPO / "install.sh"

# Enough of a userland for the prologue and the banner, and deliberately no more.
# `docker` is omitted so `command -v docker` fails identically on a developer Mac
# and on a CI runner that has Docker installed; `getent` is omitted so the
# passwd-database branch is the one that fails on every platform, leaving $PWD as
# the answer. Both omissions are the point of the fixture, not an oversight.
NEEDED = ("cat", "id", "cut", "uname", "sed", "grep", "tr", "head", "tail", "getconf")

# What check_docker prints when it cannot find a client. Reaching this line is the
# whole assertion: it sits on the far side of the prologue that used to abort.
PREFLIGHT_MARKER = "Docker is not installed."

# Resolved against the real PATH, because the PATH handed to the script is the
# thing under test and deliberately has no shell on it. `bash` and not `sh`: the
# script's shebang asks for bash and its `[[ ]]` and `${var:-}` forms need it.
BASH = shutil.which("bash") or "/bin/bash"


@pytest.fixture(scope="module")
def toolless_path(tmp_path_factory) -> str:
    """A PATH holding the few coreutils the prologue needs and nothing else."""
    binp = tmp_path_factory.mktemp("bin")
    found = 0
    for tool in NEEDED:
        src = shutil.which(tool)
        if src:
            (binp / tool).symlink_to(src)
            found += 1
    # A PATH that came out empty would fail every run below for an unrelated
    # reason, and the "no unbound variable" assertions would pass on a fiction.
    assert found >= 3, f"fixture built a useless PATH: {found} of {len(NEEDED)} tools found"
    assert shutil.which("docker", path=str(binp)) is None
    assert shutil.which("getent", path=str(binp)) is None
    return str(binp)


def _run(env: dict) -> str:
    proc = subprocess.run(
        [BASH, str(INSTALL_SH)],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=120,
        cwd=str(REPO),
        env=env,
    )
    return proc.stdout + proc.stderr


def _assert_reached_preflight(out: str) -> None:
    assert "unbound variable" not in out, (
        "install.sh aborted on an unset variable before it could do anything:\n"
        f"{out.strip()[-500:]}"
    )
    assert PREFLIGHT_MARKER in out, (
        "install.sh never reached its Docker pre-flight, so something in the "
        f"prologue stopped it:\n{out.strip()[-500:]}"
    )


def test_the_installer_starts_with_no_home_in_the_environment(toolless_path: str) -> None:
    """The cloud-init case: no terminal, no HOME, no getent."""
    _assert_reached_preflight(_run({"PATH": toolless_path, "LC_ALL": "C"}))


def test_an_explicit_install_dir_also_starts_without_home(toolless_path: str) -> None:
    """The documented workaround has to keep working after the fix.

    On the broken script this path already worked, by accident: the old line read
    `${RSYNC_INSTALL_DIR:-$HOME/rsync-ai}`, and bash never expands the default of
    a `:-` when the variable is set, so `$HOME` was never read. That makes it the
    one escape hatch an operator who hits the bug can reach, and a fix is not
    allowed to take it away.

    It is not a duplicate of the case above. A repair that puts a new failure in
    the HOME fallback itself breaks this path too, because the fallback runs
    before the `:-` chooses a side -- which is exactly what the first draft of
    this fix did, and what this case catches on a host that has getent.
    """
    env = {
        "PATH": toolless_path,
        "LC_ALL": "C",
        "RSYNC_INSTALL_DIR": "/tmp/rsync-install-dir",
    }
    _assert_reached_preflight(_run(env))


def test_a_normal_environment_with_home_still_works(toolless_path: str) -> None:
    """The control. If this fails alongside the others, the fixture is broken.

    It is also the only case every previous verification of this script ran,
    which is why none of them saw the defect.
    """
    env = {"PATH": toolless_path, "LC_ALL": "C", "HOME": os.path.expanduser("~")}
    _assert_reached_preflight(_run(env))


def test_the_passwd_fallback_cannot_fail_the_script_it_rescues() -> None:
    """The getent lookup has to survive its own absence.

    Its subject already documents the trap at `generate_secret`: under
    `set -o pipefail` a missing command fails the whole pipeline, and under
    `set -e` a failing command substitution takes the assignment -- and the
    script -- down with it. A first draft of this fix omitted the `|| true` and
    merely moved the silent death two lines later on every host without getent,
    which is every Mac. The behavioural tests above catch that only on a machine
    that has no getent; this one states the requirement so the guard is not
    quietly satisfied by a runner that happens to ship one.
    """
    getent_lines = [
        ln for ln in INSTALL_SH.read_text(encoding="utf-8").splitlines() if "getent passwd" in ln
    ]
    assert getent_lines, "the HOME fallback no longer consults the passwd database"
    for line in getent_lines:
        assert "|| true" in line, (
            "a getent lookup inside a command substitution must not be able to "
            f"fail the assignment under set -e:\n  {line.strip()}"
        )
