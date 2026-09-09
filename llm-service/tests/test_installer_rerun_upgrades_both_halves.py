"""A re-run of install.sh could not upgrade anything, and said that it had.

An upgrade is the only reason to run the installer a second time. Until the
block this file guards existed, both halves of an install stayed pinned to the
ref of the FIRST run, by two separate mechanisms that each look correct on their
own:

  * the compose file was re-downloaded only when MISSING -- a repair for a
    deleted file, never a version check; and
  * RSYNC_VERSION, which every image tag in that compose file interpolates from,
    is written by write_env, and the existing-.env branch skips write_env.

Deriving RSYNC_VERSION from RSYNC_REF at the top of the script does not reach
this path: install.sh never exports it, so `docker compose --env-file` reads the
value recorded on disk by the first install and nothing else.

Measured on a real host before the fix: `RSYNC_REF=main` over a v0.1.2 install
re-read the v0.1.2 compose file, re-pulled the 0.1.2 images already present,
recreated no container (`Up 8 hours` across the stack afterwards) and printed
the green success banner. Every service was still running the previous release.
An upgrade that upgrades nothing and reports success is worse than one that
fails, because nothing anywhere says so.

The cases below EXECUTE the installer's own code -- lifted out of install.sh,
not reimplemented -- because the defect was never visible in the source: both
mechanisms read as reasonable lines. It is only running them over an install
directory that shows the ref going in and the old version staying put.

The same-ref case is the control, and it is the one that makes the rest mean
something: a block that refreshed unconditionally would pass every upgrade
assertion here while silently overwriting a hand-edited compose file on every
ordinary re-run.
"""

import os
import re
import shutil
import stat
import subprocess

import pytest

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
INSTALL_SH = os.path.join(REPO_ROOT, "install.sh")

# The compose file records this in a comment so a refresh is detectable by
# content. The installer's own `[[ -f ]]` guard cannot tell these two apart,
# which is the whole bug.
PINNED = "# pinned compose from the FIRST install\n"
REFRESHED = "# compose fetched by download_compose\n"


def _read_install_sh():
    with open(INSTALL_SH) as fh:
        return fh.read()


def _function_body(name):
    """Lift a whole function out of install.sh, brace to brace.

    Copying the body into this file instead would guard a copy: the installer
    could lose the function entirely and these cases would still pass.
    """
    lines = _read_install_sh().splitlines(keepends=True)
    out, capturing = [], False
    for line in lines:
        if line.startswith(f"{name}()"):
            capturing = True
        if capturing:
            out.append(line)
            if line.rstrip() == "}":
                return "".join(out)
    raise AssertionError(f"{name}() not found in install.sh")


def _refresh_block():
    """The upgrade block, which lives inside main() and so has no name.

    Delimited by its first statement and the `fi` that closes it at main()'s own
    indentation. `local` is legal only inside a function, so the caller wraps it.
    """
    lines = _read_install_sh().splitlines(keepends=True)
    start = None
    for i, line in enumerate(lines):
        if line.rstrip() == "    local recorded_ref":
            assert start is None, "two candidate upgrade blocks in main()"
            start = i
    assert start is not None, (
        "the upgrade block is gone from install.sh main() -- a re-run can no "
        "longer tell an upgrade from an ordinary re-run"
    )
    for j in range(start + 1, len(lines)):
        if lines[j].rstrip() == "    fi":
            return "".join(lines[start : j + 1])
    raise AssertionError("upgrade block is not closed at main()'s indentation")


def _harness(tmp_path, ref, version):
    """install.sh's own env_value, set_env_value and upgrade block, nothing else.

    download_compose is stubbed to a marker write plus a call log, so the cases
    can assert both that it ran and that it did not, and never reach the network.
    """
    return (
        "set -euo pipefail\n"
        f'INSTALL_DIR="{tmp_path}"\n'
        'ENV_FILE=".env"\n'
        'COMPOSE_FILE="docker-compose.quickstart.yml"\n'
        f'RSYNC_REF="{ref}"\n'
        f'RSYNC_VERSION="{version}"\n'
        "info(){ :; }\nwarn(){ :; }\n"
        'download_compose(){\n'
        f'  printf %s "{REFRESHED}" > "${{INSTALL_DIR}}/${{COMPOSE_FILE}}"\n'
        '  echo called >> "${INSTALL_DIR}/download_compose.calls"\n'
        "}\n"
        + _function_body("env_value")
        + _function_body("set_env_value")
        + "rerun_branch() {\n"
        + _refresh_block()
        + "}\nrerun_branch\n"
    )


def _seed(tmp_path, env_body, compose=PINNED):
    env = tmp_path / ".env"
    env.write_text(env_body)
    env.chmod(0o600)
    if compose is not None:
        (tmp_path / "docker-compose.quickstart.yml").write_text(compose)
    return env


def _run(tmp_path, ref, version):
    harness = tmp_path / "harness.sh"
    harness.write_text(_harness(tmp_path, ref, version))
    out = subprocess.run(["bash", str(harness)], capture_output=True, text=True)
    assert out.returncode == 0, f"upgrade block exited {out.returncode}:\n{out.stderr}"
    return out


def _env_map(path):
    got = {}
    for line in path.read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, _, v = line.partition("=")
            got.setdefault(k.strip(), []).append(v)
    return got


def _calls(tmp_path):
    log = tmp_path / "download_compose.calls"
    return len(log.read_text().splitlines()) if log.exists() else 0


pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")


def test_an_install_predating_the_record_upgrades_both_halves(tmp_path):
    """The exact state of every install in the field when the fix landed.

    No RSYNC_INSTALLED_REF, because nothing wrote one. Read as empty, which
    differs from any requested ref, so the first re-run adopts it. This is the
    case the field needs: without it, an operator who installed v0.1.2 could
    never move forward by any documented command.
    """
    env = _seed(tmp_path, "RSYNC_VERSION=0.1.2\nNEXTAUTH_URL=http://x:3000\n")
    _run(tmp_path, ref="main", version="main")

    assert (tmp_path / "docker-compose.quickstart.yml").read_text() == REFRESHED
    assert _calls(tmp_path) == 1
    got = _env_map(env)
    assert got["RSYNC_VERSION"] == ["main"], (
        f"images still pinned to the first install: {got['RSYNC_VERSION']}"
    )
    assert got["RSYNC_INSTALLED_REF"] == ["main"]
    # Untouched keys survive. set_env_value rewrites the file wholesale, so
    # "it changed the version" and "it kept everything else" are separate facts.
    assert got["NEXTAUTH_URL"] == ["http://x:3000"]


def test_a_recorded_ref_that_changed_upgrades_both_halves(tmp_path):
    env = _seed(tmp_path, "RSYNC_VERSION=0.1.2\nRSYNC_INSTALLED_REF=v0.1.2\n")
    _run(tmp_path, ref="main", version="main")

    assert (tmp_path / "docker-compose.quickstart.yml").read_text() == REFRESHED
    got = _env_map(env)
    assert got["RSYNC_VERSION"] == ["main"]
    assert got["RSYNC_INSTALLED_REF"] == ["main"]
    # The outgoing compose file is recoverable, not merely gone.
    assert (tmp_path / "docker-compose.quickstart.yml.previous").read_text() == PINNED


def test_the_same_ref_touches_nothing(tmp_path):
    """The control, and the reason the assertions above discriminate.

    A block that refreshed unconditionally passes every upgrade case in this
    file. It also overwrites the operator's compose file and rewrites their .env
    on every ordinary re-run -- including the re-runs that exist only to restart
    a stopped stack.
    """
    env = _seed(tmp_path, "RSYNC_VERSION=0.1.2\nRSYNC_INSTALLED_REF=v0.1.2\n")
    before = env.read_text()
    _run(tmp_path, ref="v0.1.2", version="0.1.2")

    assert (tmp_path / "docker-compose.quickstart.yml").read_text() == PINNED
    assert _calls(tmp_path) == 0, "same ref re-downloaded the compose file"
    assert env.read_text() == before, "same ref rewrote the .env"
    assert not (tmp_path / "docker-compose.quickstart.yml.previous").exists()


def test_a_pinned_version_the_operator_set_by_hand_survives_a_same_ref_rerun(tmp_path):
    """Deliberately pairing one ref's compose with another ref's images stays
    available -- the top of install.sh documents it as supported. It only has to
    survive the re-run that changes nothing."""
    env = _seed(tmp_path, "RSYNC_INSTALLED_REF=v0.1.2\nRSYNC_VERSION=main\n")
    _run(tmp_path, ref="v0.1.2", version="0.1.2")
    assert _env_map(env)["RSYNC_VERSION"] == ["main"]


def test_a_missing_compose_file_is_refreshed_without_a_backup(tmp_path):
    """`cp` of a file that is not there is an error, and the script runs under
    `set -e`. An install dir whose compose file was deleted must still upgrade."""
    _seed(tmp_path, "RSYNC_VERSION=0.1.2\n", compose=None)
    _run(tmp_path, ref="main", version="main")
    assert (tmp_path / "docker-compose.quickstart.yml").read_text() == REFRESHED
    assert not (tmp_path / "docker-compose.quickstart.yml.previous").exists()


def test_set_env_value_survives_values_that_are_live_in_a_sed_replacement(tmp_path):
    """A ref may contain a slash and a value an ampersand.

    Both are live in `sed s/a/b/` -- the slash ends the expression and the
    ampersand expands to the whole match -- and inert in an awk -v variable.
    A branch ref is the ordinary case here, not a contrived one.
    """
    env = _seed(tmp_path, "RSYNC_VERSION=0.1.2\nKEEP=1\n")
    _run(tmp_path, ref="release/1.0", version="release-1.0&x")
    got = _env_map(env)
    assert got["RSYNC_INSTALLED_REF"] == ["release/1.0"]
    assert got["RSYNC_VERSION"] == ["release-1.0&x"]
    assert got["KEEP"] == ["1"]


def test_set_env_value_replaces_rather_than_appends(tmp_path):
    """env_value reads with `tail -1`, so an appended line would be live and the
    original would still be sitting in the file above it, wrong and readable.
    The .env carries a long prose block about which RSYNC_VERSION is correct;
    two of them is the confusion that block exists to prevent."""
    env = _seed(tmp_path, "RSYNC_VERSION=0.1.2\nRSYNC_INSTALLED_REF=v0.1.2\nOTHER=k\n")
    _run(tmp_path, ref="main", version="main")
    text = env.read_text()
    assert text.count("RSYNC_VERSION=") == 1, f"duplicate version lines:\n{text}"
    assert text.count("RSYNC_INSTALLED_REF=") == 1, f"duplicate ref lines:\n{text}"


def test_the_rewritten_env_is_still_mode_600(tmp_path):
    """It holds every generated secret in the install. set_env_value writes a
    new file and moves it over the old one, so the mode is its own decision --
    mktemp's default is 600 on the hosts we run on and is not a guarantee."""
    env = _seed(tmp_path, "RSYNC_VERSION=0.1.2\n")
    _run(tmp_path, ref="main", version="main")
    assert stat.S_IMODE(os.stat(env).st_mode) == 0o600


def test_write_env_records_the_ref_it_installed_from():
    """The producer half. The cases above stub write_env away, so a fresh
    install that stopped recording the ref would leave every one of them green
    while every new install came out unrecorded."""
    src = _read_install_sh()
    # Inside the interpolating heredoc, next to the version it pairs with --
    # a quoted heredoc would ship the literal text `${RSYNC_REF}` into the .env.
    m = re.search(
        r"^RSYNC_VERSION=\$\{RSYNC_VERSION\}$.*?^RSYNC_INSTALLED_REF=\$\{RSYNC_REF\}$",
        src,
        re.MULTILINE | re.DOTALL,
    )
    assert m, "write_env no longer records RSYNC_INSTALLED_REF beside RSYNC_VERSION"
    assert "EOF" not in m.group(0), (
        "a heredoc boundary sits between the two lines -- RSYNC_INSTALLED_REF is "
        "no longer in the interpolating heredoc and would be written literally"
    )
