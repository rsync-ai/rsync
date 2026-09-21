"""A re-run of `install.sh` checks what it is about to change before it changes it.

Re-running the installer is the documented upgrade, and it used to check nothing:

  * An `.env` written by an older release that lacks a variable the NEW compose
    file requires (`${VAR:?}`) got as far as `docker compose pull` and died on
    compose's interpolation error.
  * A host port some other program held got as far as `up -d` and died on
    "address already in use", after compose had already recreated part of the
    stack.
  * A fresh install asked for an admin email, defaulted it to a fixed address and
    wrote it to a variable no service reads.

These tests run the installer's own functions (install.sh sourced without its
`main "$@"` line), and then the whole of `main` with `docker` and `curl` faked
out, the way the other installer tests do. Ports are always derived at test time
from sockets the test really opens, and the required-variable set is always
computed here, independently, from the real quickstart compose file.
"""

import contextlib
import os
import pathlib
import re
import shutil
import socket
import stat
import subprocess

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
INSTALL_SH = REPO / "install.sh"
QUICKSTART = REPO / "docker-compose.quickstart.yml"
OVERLAYS = (
    "docker-compose.byo-postgres.yml",
    "docker-compose.byo-kafka.yml",
    "docker-compose.ollama.yml",
)
BASH = shutil.which("bash") or "/bin/bash"
REAL_DOCKER = shutil.which("docker")

# The userland the check functions need, and nothing that detects listeners.
# ss, netstat and lsof are deliberately absent: each test that wants one adds a
# fake of it, and the /dev/tcp test wants none at all.
BASE_TOOLS = (
    "awk", "sed", "sort", "head", "tail", "cat", "grep", "tr", "cut",
    "mktemp", "chmod", "mv", "rm", "id", "uname", "openssl", "dirname",
)

FINISHED = "__DRIVER_FINISHED__"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _lib(tmp_path: pathlib.Path) -> pathlib.Path:
    lines = INSTALL_SH.read_text(encoding="utf-8").splitlines()
    assert lines[-1].strip() == 'main "$@"', "install.sh no longer ends in its main call"
    lib = tmp_path / "install-lib.sh"
    lib.write_text("\n".join(lines[:-1]) + "\n", encoding="utf-8")
    return lib


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextlib.contextmanager
def _listener(host: str = "127.0.0.1", port: int = 0):
    """A real, foreign TCP listener: this test process, not a container."""
    s = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET)
    try:
        s.bind((host, port))
        s.listen(8)
        yield s.getsockname()[1]
    finally:
        s.close()


def _rendered(ports, project="rsync-ai") -> str:
    """`docker compose config` output, in compose v2's normalised layout.

    `ports` is a list of (service, host_ip or None, published, target). The
    parser under test is also checked against a REAL render of the quickstart
    below, so this fixture cannot drift from the real shape unnoticed.
    """
    out = [f"name: {project}", "services:"]
    for svc, hip, published, target in ports:
        out += [
            f"  {svc}:",
            f"    container_name: rsync-{svc}",
            "    environment:",
            '      PORT: "8080"',
            "      NOT_A_PORT: published",
            "    ports:",
            "      - mode: ingress",
        ]
        if hip is not None:
            out.append(f"        host_ip: {hip}")
        out += [
            f"        target: {target}",
            f'        published: "{published}"',
            "        protocol: tcp",
            "    restart: unless-stopped",
        ]
    out += [
        "  postgres:",
        "    container_name: rsync-postgres",
        "    expose:",
        '      - "5432"',
        "networks:",
        "  default:",
        f"    name: {project}_default",
        "volumes:",
        "  pgdata:",
        f"    name: {project}_pgdata",
    ]
    return "\n".join(out) + "\n"


def _toolbox(tmp_path: pathlib.Path, name: str, fakes=None) -> str:
    box = tmp_path / name
    box.mkdir()
    found = 0
    for tool in BASE_TOOLS:
        src = shutil.which(tool)
        if src:
            (box / tool).symlink_to(src)
            found += 1
    for tool, body in (fakes or {}).items():
        p = box / tool
        p.write_text(body, encoding="utf-8")
        p.chmod(p.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    assert found >= 10, f"fixture built a useless PATH: {found} of {len(BASE_TOOLS)} tools"
    for real in ("ss", "netstat", "lsof"):
        if real not in (fakes or {}):
            assert shutil.which(real, path=str(box)) is None
    return str(box)


def _run_fn(
    tmp_path,
    fn,
    *,
    config="",
    config_fails=False,
    ps="",
    env=None,
    path=None,
    compose_file=None,
    install_dir=None,
):
    """Source install.sh and call one function, with `docker` as a shell function.

    `config_fails` is True for compose's missing-variable error, or the exact
    stderr text a failed `compose config` should print.
    """
    lib = _lib(tmp_path)
    install_dir = install_dir or (tmp_path / "rsync-ai")
    install_dir.mkdir(exist_ok=True)
    cfg = tmp_path / "rendered.yml"
    cfg.write_text(config, encoding="utf-8")
    psf = tmp_path / "ps.txt"
    psf.write_text(ps, encoding="utf-8")
    compose = compose_file or (install_dir / "docker-compose.quickstart.yml")
    if config_fails:
        if config_fails is True:
            config_fails = (
                "error while interpolating services.api-gateway.environment.JWT_SECRET:"
                " required variable JWT_SECRET is missing a value\n"
            )
        errf = tmp_path / "config-stderr.txt"
        errf.write_text(config_fails, encoding="utf-8")
        config_body = f'cat "{errf}" >&2; return 15'
    else:
        config_body = f'cat "{cfg}"; return 0'
    driver = tmp_path / "drive.sh"
    driver.write_text(
        "set -euo pipefail\n"
        f'source "{lib}"\n'
        f'INSTALL_DIR="{install_dir}"\n'
        f'COMPOSE_ARGS=( -f "{compose}" )\n'
        "docker() {\n"
        f'  if [[ "${{1:-}}" == "ps" ]]; then cat "{psf}"; return 0; fi\n'
        '  if [[ "${1:-}" == "compose" && " $* " == *" config "* ]]; then\n'
        f"    {config_body}\n"
        "  fi\n"
        '  echo "unexpected: docker $*" >&2; return 99\n'
        "}\n"
        f"{fn}\n"
        f'echo "{FINISHED} API_HOST_PORT=${{API_HOST_PORT}} COMPOSE_PROJECT=${{COMPOSE_PROJECT}}"\n',
        encoding="utf-8",
    )
    run_env = {
        "PATH": path or os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(tmp_path),
        "LC_ALL": "C",
        "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
    }
    run_env.update(env or {})
    proc = subprocess.run(
        [BASH, str(driver)],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=120,
        env=run_env,
        start_new_session=True,
    )
    return proc.returncode, proc.stdout + proc.stderr, install_dir


# ---------------------------------------------------------------------------
# the port list comes from the compose model, in its real layout
# ---------------------------------------------------------------------------


def _parse_ports_with_the_installer(tmp_path, rendered: str):
    lib = _lib(tmp_path)
    src = tmp_path / "model.yml"
    src.write_text(rendered, encoding="utf-8")
    proc = subprocess.run(
        [BASH, "-c", f'set -euo pipefail; source "{lib}"; compose_published_ports < "{src}"'],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
        env={"PATH": os.environ["PATH"], "HOME": str(tmp_path), "LC_ALL": "C"},
        start_new_session=True,
    )
    assert proc.returncode == 0, proc.stderr
    rows = [line.split("\t") for line in proc.stdout.splitlines() if line]
    project = [r[1] for r in rows if r[0] == "project"]
    ports = sorted((r[1], r[2], r[3], r[4]) for r in rows if r[0] == "port")
    return project, ports


def _parse_ports_with_yaml(rendered: str):
    model = yaml.safe_load(rendered)
    ports = []
    for svc, body in (model.get("services") or {}).items():
        for p in body.get("ports") or []:
            ports.append((svc, p.get("host_ip") or "0.0.0.0", str(p["published"]), str(p["target"])))
    return [model["name"]], sorted(ports)


def test_the_fixture_layout_parses_the_same_way_yaml_reads_it(tmp_path):
    rendered = _rendered(
        [("api-gateway", "127.0.0.1", 5001, 8080), ("frontend", None, 3000, 3000)]
    )
    got = _parse_ports_with_the_installer(tmp_path, rendered)
    want = _parse_ports_with_yaml(rendered)
    assert len(want[1]) == 2
    assert got == want


@pytest.mark.skipif(REAL_DOCKER is None, reason="needs the docker CLI to render compose config")
def test_the_real_quickstart_render_yields_the_ports_it_publishes(tmp_path):
    """The parser against compose's own output for the file an install downloads."""
    required = sorted(set(re.findall(r"\$\{([A-Z_][A-Z0-9_]*):\?", QUICKSTART.read_text())))
    assert required, "no required variables found -- the render below would prove nothing"
    envf = tmp_path / ".env"
    envf.write_text("".join(f"{v}=placeholder-for-render\n" for v in required))
    proc = subprocess.run(
        [REAL_DOCKER, "compose", "-f", str(QUICKSTART), "--profile", "cdc",
         "--env-file", str(envf), "config"],
        stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, proc.stderr[-1000:]
    got = _parse_ports_with_the_installer(tmp_path, proc.stdout)
    want = _parse_ports_with_yaml(proc.stdout)
    assert len(want[1]) > 0, "the quickstart publishes no ports -- nothing was compared"
    assert got == want
    assert ("api-gateway", "127.0.0.1", "5001", "8080") in got[1]


# ---------------------------------------------------------------------------
# port pre-check
# ---------------------------------------------------------------------------


def test_a_port_held_by_a_foreign_process_stops_the_run_and_names_it(tmp_path):
    with _listener() as port:
        rc, out, _ = _run_fn(
            tmp_path,
            "check_ports_and_running_install",
            config=_rendered([("api-gateway", "127.0.0.1", port, 8080)]),
            # A fork at another ref: the skip command must name the script this
            # install actually came from, not a placeholder or the default repo.
            env={"RSYNC_REPO": "example-org/example-fork", "RSYNC_REF": "v9.8.7"},
        )
    assert rc == 1, out
    assert FINISHED not in out
    assert f"Port {port} is already in use" in out
    assert "api-gateway" in out
    assert "Nothing has been pulled or started" in out
    assert (
        "curl -sSL https://raw.githubusercontent.com/example-org/example-fork/v9.8.7/install.sh"
        " | RSYNC_SKIP_PORT_CHECK=1 bash"
    ) in out
    assert "<url>" not in out
    assert "docker-compose.quickstart.yml" in out, "the message must say where to change the port"
    if shutil.which("lsof") or shutil.which("ss"):
        assert f"pid {os.getpid()}" in out, f"the holder was not identified:\n{out}"


def test_a_port_held_by_this_installs_own_container_is_fine(tmp_path):
    """Docker's port proxy really is listening; the container owning it is ours."""
    with _listener() as port:
        install_dir = tmp_path / "rsync-ai"
        install_dir.mkdir()
        ps = f"rsync-api-gateway\trsync-ai\t{install_dir}\t127.0.0.1:{port}->8080/tcp, [::1]:{port}->8080/tcp\n"
        rc, out, _ = _run_fn(
            tmp_path,
            "check_ports_and_running_install",
            config=_rendered([("api-gateway", "127.0.0.1", port, 8080)]),
            ps=ps,
        )
    assert rc == 0, out
    assert f"{FINISHED} API_HOST_PORT={port} COMPOSE_PROJECT=rsync-ai" in out
    assert "already in use" not in out
    assert "Found 1 running containers from an existing rsync.ai install" in out
    assert "upgrades them in place" in out
    assert "different directory" not in out


def test_our_project_started_from_another_directory_is_named_but_not_fatal(tmp_path):
    with _listener() as port:
        ps = f"rsync-api-gateway\trsync-ai\t/some/source/checkout\t127.0.0.1:{port}->8080/tcp\n"
        rc, out, _ = _run_fn(
            tmp_path,
            "check_ports_and_running_install",
            config=_rendered([("api-gateway", "127.0.0.1", port, 8080)]),
            ps=ps,
        )
    assert rc == 0, out
    assert "started from a different directory" in out
    assert "/some/source/checkout" in out


@pytest.mark.parametrize("recorded", ["logical", "physical"])
def test_an_install_dir_reached_through_a_symlink_is_not_another_directory(tmp_path, recorded):
    """Compose may record either spelling of the project dir; both are this install."""
    real = tmp_path / "disk" / "rsync-ai"
    real.mkdir(parents=True)
    link = tmp_path / "rsync-ai-link"
    link.symlink_to(real, target_is_directory=True)
    physical = os.path.realpath(real)
    assert physical != str(link)
    working_dir = str(link) if recorded == "logical" else physical
    with _listener() as port:
        ps = f"rsync-api-gateway\trsync-ai\t{working_dir}\t127.0.0.1:{port}->8080/tcp\n"
        rc, out, _ = _run_fn(
            tmp_path,
            "check_ports_and_running_install",
            config=_rendered([("api-gateway", "127.0.0.1", port, 8080)]),
            ps=ps,
            install_dir=link,
        )
    assert rc == 0, out
    assert "Found 1 running containers from an existing rsync.ai install" in out
    assert "different directory" not in out


@pytest.mark.parametrize(
    "ports_column,expect_conflict",
    [
        ("0.0.0.0:{p}->80/tcp, [::]:{p}->80/tcp", True),
        ("0.0.0.0:{lo}-{hi}->8000-8004/tcp", True),
        ("0.0.0.0:{p}->80/udp", False),
        ("{p}/tcp", False),
    ],
)
def test_a_port_published_by_another_container_stops_the_run(tmp_path, ports_column, expect_conflict):
    port = _free_port()
    column = ports_column.format(p=port, lo=port - 2, hi=port + 2)
    ps = f"other-app\tother-project\t/srv/other\t{column}\n"
    rc, out, _ = _run_fn(
        tmp_path,
        "check_ports_and_running_install",
        config=_rendered([("api-gateway", "127.0.0.1", port, 8080)]),
        ps=ps,
    )
    # Another project's container is never announced as this install.
    assert "existing rsync.ai install" not in out
    assert "upgrades them in place" not in out
    if expect_conflict:
        assert rc == 1, out
        assert "Docker container other-app (compose project other-project)" in out
        assert "docker stop other-app" in out
    else:
        assert rc == 0, out
        assert "already in use" not in out


def test_only_the_taken_port_is_reported(tmp_path):
    api = _free_port()
    with _listener() as ui:
        rc, out, _ = _run_fn(
            tmp_path,
            "check_ports_and_running_install",
            config=_rendered(
                [("api-gateway", "127.0.0.1", api, 8080), ("frontend", "127.0.0.1", ui, 3000)]
            ),
        )
    assert rc == 1, out
    assert f"Port {ui} is already in use, and rsync.ai's frontend needs it" in out
    assert f"Port {api} is already in use" not in out


def test_with_no_listener_tools_the_bash_dev_tcp_probe_still_finds_the_port(tmp_path):
    box = _toolbox(tmp_path, "bare")
    config_for = lambda p: _rendered([("api-gateway", "127.0.0.1", p, 8080)])  # noqa: E731

    with _listener() as port:
        rc, out, _ = _run_fn(tmp_path, "check_ports_and_running_install", config=config_for(port), path=box)
    assert rc == 1, out
    assert f"Port {port} is already in use" in out
    assert "bash /dev/tcp" in out
    assert "command not found" not in out

    # The control: the same PATH, a port nothing holds.
    free = _free_port()
    rc, out, _ = _run_fn(tmp_path, "check_ports_and_running_install", config=config_for(free), path=box)
    assert rc == 0, out
    assert "command not found" not in out
    assert "All 1 host ports rsync.ai publishes are free" in out


def _ipv6_loopback_listener_works():
    if not socket.has_ipv6:
        return False
    try:
        with _listener("::1"):
            return True
    except OSError:
        return False


@pytest.mark.skipif(not _ipv6_loopback_listener_works(), reason="no IPv6 loopback on this host")
def test_the_dev_tcp_probe_dials_the_specific_address_the_port_is_bound_to(tmp_path):
    """A listener only on ::1 is invisible to a dial of 127.0.0.1, and still collides."""
    box = _toolbox(tmp_path, "bare")
    config_for = lambda p: _rendered([("api-gateway", "::1", p, 8080)])  # noqa: E731

    with _listener("::1") as port:
        # The same port on IPv4 loopback is free, so only a dial of ::1 can see it.
        with socket.socket() as probe:
            assert probe.connect_ex(("127.0.0.1", port)) != 0
        rc, out, _ = _run_fn(tmp_path, "check_ports_and_running_install", config=config_for(port), path=box)
    assert rc == 1, out
    assert f"Port {port} is already in use, and rsync.ai's api-gateway needs it (::1:{port})" in out
    assert "seen by bash /dev/tcp" in out

    rc, out, _ = _run_fn(
        tmp_path, "check_ports_and_running_install", config=config_for(_free_port()), path=box
    )
    assert rc == 0, out
    assert "All 1 host ports rsync.ai publishes are free" in out


def _fake_lsof(addr, port):
    return (
        "#!/bin/sh\n/bin/cat <<'EOF'\n"
        "COMMAND  PID USER   FD   TYPE DEVICE SIZE/OFF NODE NAME\n"
        f"postgres 777 me     7u  IPv4 0x1234      0t0  TCP {addr}:{port} (LISTEN)\n"
        "EOF\n"
    )


@pytest.mark.parametrize(
    "listen_addr,bind_addr,expect_conflict",
    [
        ("127.0.0.1", "127.0.0.1", True),
        ("*", "127.0.0.1", True),
        ("10.1.2.3", "127.0.0.1", False),
        ("[::1]", "127.0.0.1", False),
    ],
)
def test_lsof_alone_finds_the_listener_and_names_its_process(
    tmp_path, listen_addr, bind_addr, expect_conflict
):
    port = _free_port()
    box = _toolbox(tmp_path, "box", {"lsof": _fake_lsof(listen_addr, port)})
    rc, out, _ = _run_fn(
        tmp_path,
        "check_ports_and_running_install",
        config=_rendered([("api-gateway", bind_addr, port, 8080)]),
        path=box,
    )
    assert "command not found" not in out
    if expect_conflict:
        assert rc == 1, out
        assert f"Port {port} is already in use" in out
        assert "Held by: postgres (pid 777)" in out
    else:
        assert rc == 0, out
        assert "All 1 host ports rsync.ai publishes are free" in out


def _fake_ss(addr, port):
    return (
        "#!/bin/sh\n/bin/cat <<'EOF'\n"
        "State  Recv-Q Send-Q Local Address:Port Peer Address:Port Process\n"
        "LISTEN 0      4096   127.0.0.53%lo:53   0.0.0.0:*\n"
        f"LISTEN 0      511    127.0.0.1:1{port}  0.0.0.0:*\n"
        f'LISTEN 0      511    {addr}:{port}  0.0.0.0:*  users:(("nginx",pid=4242,fd=6))\n'
        "EOF\n"
    )


def _fake_netstat_macos(addr, port):
    return (
        "#!/bin/sh\n/bin/cat <<'EOF'\n"
        "Active Internet connections (including servers)\n"
        "Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)\n"
        f"tcp4       0      0  {addr}.{port}          *.*                    LISTEN\n"
        "tcp46      0      0  *.22                   *.*                    LISTEN\n"
        f"udp4       0      0  *.{port}               *.*\n"
        "EOF\n"
    )


def _fake_netstat_linux(addr, port):
    return (
        "#!/bin/sh\n/bin/cat <<'EOF'\n"
        "Active Internet connections (servers and established)\n"
        "Proto Recv-Q Send-Q Local Address           Foreign Address         State\n"
        f"tcp        0      0 {addr}:{port}           0.0.0.0:*               LISTEN\n"
        f"udp        0      0 0.0.0.0:{port}          0.0.0.0:*\n"
        "EOF\n"
    )


@pytest.mark.parametrize(
    "tool,listen_addr,bind_addr,expect_conflict",
    [
        ("ss", "127.0.0.1", "127.0.0.1", True),
        ("ss", "10.1.2.3", "127.0.0.1", False),
        ("ss", "127.0.0.1", "0.0.0.0", True),
        ("ss", "*", "127.0.0.1", True),
        ("ss", "[::]", "127.0.0.1", True),
        ("ss", "[::1]", "127.0.0.1", False),
        # An IPv4 address held through a dual-stack socket, as ss prints it.
        ("ss", "[::ffff:127.0.0.1]", "127.0.0.1", True),
        ("ss", "[::ffff:10.1.2.3]", "127.0.0.1", False),
        ("netstat-macos", "127.0.0.1", "127.0.0.1", True),
        ("netstat-macos", "*", "127.0.0.1", True),
        ("netstat-macos", "10.1.2.3", "127.0.0.1", False),
        ("netstat-linux", "0.0.0.0", "127.0.0.1", True),
        ("netstat-linux", ":::", "127.0.0.1", True),
        ("netstat-linux", "10.1.2.3", "127.0.0.1", False),
    ],
)
def test_listener_tables_from_linux_and_macos_decide_the_same_way(
    tmp_path, tool, listen_addr, bind_addr, expect_conflict
):
    port = _free_port()
    if tool == "ss":
        fakes = {"ss": _fake_ss(listen_addr, port)}
    elif tool == "netstat-macos":
        fakes = {"netstat": _fake_netstat_macos(listen_addr, port)}
    else:
        # `:::5001` is how net-tools prints the IPv6 wildcard; the fixture's
        # separator colon is already the third one.
        addr = "::" if listen_addr == ":::" else listen_addr
        fakes = {"netstat": _fake_netstat_linux(addr, port)}
    box = _toolbox(tmp_path, "box", fakes)
    rc, out, _ = _run_fn(
        tmp_path,
        "check_ports_and_running_install",
        config=_rendered([("api-gateway", bind_addr, port, 8080)]),
        path=box,
    )
    assert "command not found" not in out
    if expect_conflict:
        assert rc == 1, out
        assert f"Port {port} is already in use" in out
        if tool == "ss":
            assert "nginx (pid 4242)" in out
        else:
            assert "a process this user cannot identify" in out
            assert f"sudo lsof -nP -iTCP:{port} -sTCP:LISTEN" in out
    else:
        assert rc == 0, out
        assert "already in use" not in out


def test_skip_port_check_continues_past_a_conflict(tmp_path):
    with _listener() as port:
        rc, out, _ = _run_fn(
            tmp_path,
            "check_ports_and_running_install",
            config=_rendered([("api-gateway", "127.0.0.1", port, 8080)]),
            env={"RSYNC_SKIP_PORT_CHECK": "1"},
        )
    assert rc == 0, out
    assert f"Port {port} is already in use" in out
    assert "Continuing anyway because RSYNC_SKIP_PORT_CHECK=1" in out
    assert FINISHED in out


@pytest.mark.parametrize("value", ["0", "false", ""])
def test_only_skip_port_check_1_skips_it(tmp_path, value):
    with _listener() as port:
        rc, out, _ = _run_fn(
            tmp_path,
            "check_ports_and_running_install",
            config=_rendered([("api-gateway", "127.0.0.1", port, 8080)]),
            env={"RSYNC_SKIP_PORT_CHECK": value},
        )
    assert rc == 1, out
    assert f"Port {port} is already in use" in out
    assert "Continuing anyway" not in out
    assert FINISHED not in out


def test_a_model_that_publishes_no_ports_is_called_out_not_passed(tmp_path):
    config = _rendered([])
    assert "services:" in config and "ports:" not in config
    rc, out, _ = _run_fn(tmp_path, "check_ports_and_running_install", config=config)
    assert rc == 0, out
    assert "The compose files publish no host ports, so there were none to check." in out
    assert "host ports rsync.ai publishes are free" not in out
    assert FINISHED in out


def test_an_unrenderable_compose_model_skips_the_check_with_the_reason(tmp_path):
    rc, out, _ = _run_fn(tmp_path, "check_ports_and_running_install", config_fails=True)
    assert rc == 0, out
    assert "port check was skipped" in out
    assert "required variable JWT_SECRET is missing a value" in out
    assert FINISHED in out


@pytest.mark.parametrize(
    "complaint",
    [
        'line 7: unterminated quoted value "fake-value-{tag}',
        "line 7: unterminated quoted value 'fake-value-{tag}",
        'Invalid template: "${{fake-value-{tag}"',
    ],
)
def test_the_skipped_check_does_not_print_a_value_compose_quoted_from_the_env(tmp_path, complaint):
    """Compose quotes the offending .env line in these errors; the .env holds the secrets."""
    tag = "never-printed-4f1c"
    line = f"failed to read {tmp_path}/rsync-ai/.env: " + complaint.format(tag=tag)
    assert tag in line
    rc, out, _ = _run_fn(
        tmp_path,
        "check_ports_and_running_install",
        config_fails=line
        + "\nerror while interpolating services.api-gateway.environment.JWT_SECRET:"
        " required variable JWT_SECRET is missing a value\n",
    )
    assert rc == 0, out
    assert "port check was skipped" in out
    assert tag not in out
    # What stays is enough to find the line.
    assert f"failed to read {tmp_path}/rsync-ai/.env: " in out
    assert "[rest hidden: it can quote a line of the .env]" in out
    assert "required variable JWT_SECRET is missing a value" in out


PROBE_CURL = r"""#!/bin/bash
url=""
for a in "$@"; do case "$a" in http://*) url=$a ;; esac; done
printf '%s\n' "$url" >> "$PROBE_CURL_LOG"
case "$url" in
  "http://localhost:${PROBE_PORT}/ready") printf '404 page not found\n404'; exit 0 ;;
  "http://localhost:${PROBE_PORT}/health") exit 0 ;;
esac
exit 7
"""


def test_the_readiness_fallback_probes_the_published_api_port_too(tmp_path):
    """An image with no /ready falls back to /health -- on the same, moved port."""
    port = _free_port()
    while port == 5001:
        port = _free_port()
    fake_bin = tmp_path / "curlbin"
    fake_bin.mkdir()
    curl = fake_bin / "curl"
    curl.write_text(PROBE_CURL)
    curl.chmod(0o755)
    log = tmp_path / "curl.log"
    rc, out, _ = _run_fn(
        tmp_path,
        f'API_HOST_PORT={port}\n'
        'if probe_ready; then echo "PROBE=ready"; else echo "PROBE=not-ready"; fi\n'
        'echo "REASON=${READY_REASON}"',
        path=f"{fake_bin}:{os.environ.get('PATH', '/usr/bin:/bin')}",
        env={"PROBE_CURL_LOG": str(log), "PROBE_PORT": str(port)},
    )
    assert rc == 0, out
    assert "PROBE=ready" in out
    assert "REASON=this image has no /ready endpoint; fell back to /health" in out
    assert log.read_text().splitlines() == [
        f"http://localhost:{port}/ready",
        f"http://localhost:{port}/health",
    ]


# ---------------------------------------------------------------------------
# an old .env against the new compose file
# ---------------------------------------------------------------------------


def _required_by_quickstart():
    names = sorted(set(re.findall(r"\$\{([A-Z_][A-Z0-9_]*):\?", QUICKSTART.read_text())))
    assert len(names) > 0, "the quickstart requires no variables -- these tests would be vacuous"
    return names


def _old_env_lines(required):
    lines = [
        "# written by an older install.sh",
        "RSYNC_VERSION=0.1.1",
        "PUBLIC_URL=http://localhost:5001",
        "RSYNC_ADMIN_EMAILS=someone@example.com",
    ]
    lines += [f"{v}=keep-{v.lower().replace('_', '-')}" for v in required]
    lines.append('QUOTED_NOTE="a b & c/d"')
    return lines


def _install_with_env(tmp_path, lines):
    install_dir = tmp_path / "rsync-ai"
    install_dir.mkdir(exist_ok=True)
    compose = install_dir / "docker-compose.quickstart.yml"
    shutil.copy(QUICKSTART, compose)
    envf = install_dir / ".env"
    envf.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return compose, envf


def test_a_complete_old_env_passes_and_is_left_byte_for_byte(tmp_path):
    required = _required_by_quickstart()
    compose, envf = _install_with_env(tmp_path, _old_env_lines(required))
    before = envf.read_bytes()
    rc, out, _ = _run_fn(tmp_path, "check_env_required", compose_file=compose)
    assert rc == 0, out
    assert f"All {len(required)} variables the compose files require are set" in out
    assert envf.read_bytes() == before


def test_missing_generatable_secrets_are_reported_added_and_nothing_else_changes(tmp_path):
    required = _required_by_quickstart()
    for name in ("REDIS_PASSWORD", "MINIO_SECRET_KEY"):
        assert name in required, f"{name} is no longer required; pick another generatable one"
    lines = _old_env_lines(required)
    # One line gone entirely, one present but empty -- `:?` rejects both.
    lines = [ln for ln in lines if not ln.startswith("REDIS_PASSWORD=")]
    lines = ["MINIO_SECRET_KEY=" if ln.startswith("MINIO_SECRET_KEY=") else ln for ln in lines]
    compose, envf = _install_with_env(tmp_path, lines)

    rc, out, _ = _run_fn(tmp_path, "check_env_required", compose_file=compose)

    assert rc == 0, out
    assert "Added a generated REDIS_PASSWORD" in out
    assert "Added a generated MINIO_SECRET_KEY" in out
    after = envf.read_text(encoding="utf-8").splitlines()

    def value(name):
        hits = [ln.split("=", 1)[1] for ln in after if ln.startswith(name + "=")]
        assert len(hits) == 1, f"{name} appears {len(hits)} times"
        return hits[0]

    assert re.fullmatch(r"[A-Za-z0-9]{32}", value("REDIS_PASSWORD"))
    assert re.fullmatch(r"[A-Za-z0-9]{32}", value("MINIO_SECRET_KEY"))
    # Every other line is exactly what it was, in the same order.
    untouched = [ln for ln in lines if not ln.startswith("MINIO_SECRET_KEY=")]
    kept = [ln for ln in after if not ln.startswith(("REDIS_PASSWORD=", "MINIO_SECRET_KEY="))]
    assert kept == untouched
    assert len(kept) > len(required)
    # The empty line was filled where it stood, not appended as a second one.
    assert after.index(next(ln for ln in after if ln.startswith("MINIO_SECRET_KEY="))) == lines.index(
        "MINIO_SECRET_KEY="
    )
    assert stat.S_IMODE(envf.stat().st_mode) == 0o600


DATA_GUARD_ADVICE = {
    "ENCRYPTION_KEY": (
        "    ENCRYPTION_KEY — set it to the key this install has always used.",
        "      A new key makes every saved connection credential unreadable.",
    ),
    "POSTGRES_PASSWORD": (
        "    POSTGRES_PASSWORD — set it to the password your database was first created with.",
        "      Do not invent a new one: the existing database would reject every service.",
    ),
}


@pytest.mark.parametrize("name", ["ENCRYPTION_KEY", "POSTGRES_PASSWORD"])
def test_a_missing_key_that_guards_existing_data_stops_the_run_and_writes_nothing(tmp_path, name):
    required = _required_by_quickstart()
    assert name in required
    # REDIS_PASSWORD is missing too, so a check that generated before stopping
    # would change the file.
    lines = [
        ln for ln in _old_env_lines(required)
        if not ln.startswith((name + "=", "REDIS_PASSWORD="))
    ]
    compose, envf = _install_with_env(tmp_path, lines)
    before = envf.read_bytes()

    rc, out, _ = _run_fn(tmp_path, "check_env_required", compose_file=compose)

    assert rc == 1, out
    assert FINISHED not in out
    # The advice that keeps the operator from inventing a value that would lock
    # them out of their own data -- not the generic "no safe default" line.
    lines = out.splitlines()
    for advice in DATA_GUARD_ADVICE[name]:
        assert advice in lines, f"missing advice line {advice!r}:\n{out}"
    assert f"    {name} — no safe default exists" not in out
    assert "Nothing has been pulled or started" in out
    assert envf.read_bytes() == before


def test_a_required_variable_with_no_known_generator_is_reported(tmp_path):
    install_dir = tmp_path / "rsync-ai"
    install_dir.mkdir()
    compose = install_dir / "docker-compose.quickstart.yml"
    compose.write_text(
        QUICKSTART.read_text()
        + "\n# appended by the test\nx-new-in-this-release:\n  token: ${BRAND_NEW_TOKEN:?set it}\n"
    )
    required = _required_by_quickstart()
    envf = install_dir / ".env"
    envf.write_text("\n".join(_old_env_lines(required)) + "\n")
    before = envf.read_bytes()
    rc, out, _ = _run_fn(tmp_path, "check_env_required", compose_file=compose)
    assert rc == 1, out
    assert "BRAND_NEW_TOKEN" in out
    assert envf.read_bytes() == before


def test_a_value_exported_in_the_shell_satisfies_the_check_without_a_write(tmp_path):
    required = _required_by_quickstart()
    lines = [ln for ln in _old_env_lines(required) if not ln.startswith("ENCRYPTION_KEY=")]
    compose, envf = _install_with_env(tmp_path, lines)
    before = envf.read_bytes()
    rc, out, _ = _run_fn(
        tmp_path,
        "check_env_required",
        compose_file=compose,
        env={"ENCRYPTION_KEY": "exported-for-this-run-only"},
    )
    assert rc == 0, out
    assert "ENCRYPTION_KEY is set in this shell's environment but not in" in out
    assert "exported-for-this-run-only" not in out
    assert envf.read_bytes() == before


def test_a_value_exported_empty_in_the_shell_does_not_count(tmp_path):
    # compose's `:?` rejects an empty value from the environment exactly as it
    # rejects an unset one.
    required = _required_by_quickstart()
    lines = [ln for ln in _old_env_lines(required) if not ln.startswith("ENCRYPTION_KEY=")]
    compose, envf = _install_with_env(tmp_path, lines)
    before = envf.read_bytes()
    rc, out, _ = _run_fn(tmp_path, "check_env_required", compose_file=compose, env={"ENCRYPTION_KEY": ""})
    assert rc == 1, out
    assert DATA_GUARD_ADVICE["ENCRYPTION_KEY"][0] in out.splitlines()
    assert "set in this shell's environment" not in out
    assert envf.read_bytes() == before


def test_export_prefixed_env_lines_are_read_as_set_and_filled_where_they_stand(tmp_path):
    """compose reads `export KEY=value` as KEY; so must the installer."""
    required = _required_by_quickstart()
    lines = _old_env_lines(required)
    # Set, with the prefix: must count as set, or the run stops over a key it has.
    lines = [("export " + ln) if ln.startswith("ENCRYPTION_KEY=") else ln for ln in lines]
    # Empty, with the prefix: missing, and filled on that line, not appended.
    lines = ["export REDIS_PASSWORD=" if ln.startswith("REDIS_PASSWORD=") else ln for ln in lines]
    assert "export REDIS_PASSWORD=" in lines and any(ln.startswith("export ENCRYPTION_KEY=keep-") for ln in lines)
    compose, envf = _install_with_env(tmp_path, lines)

    rc, out, _ = _run_fn(tmp_path, "check_env_required", compose_file=compose)

    assert rc == 0, out
    assert "missing variables" not in out
    assert "Added a generated REDIS_PASSWORD" in out
    assert "Added a generated ENCRYPTION_KEY" not in out
    after = envf.read_text(encoding="utf-8").splitlines()
    redis = [ln for ln in after if re.match(r"\s*(export\s+)?REDIS_PASSWORD=", ln)]
    assert len(redis) == 1, f"REDIS_PASSWORD now appears {len(redis)} times"
    assert re.fullmatch(r"export REDIS_PASSWORD=[A-Za-z0-9]{32}", redis[0])
    assert after.index(redis[0]) == lines.index("export REDIS_PASSWORD=")
    others = [ln for ln in after if ln != redis[0]]
    assert others == [ln for ln in lines if ln != "export REDIS_PASSWORD="]


def test_a_shell_variable_that_is_not_exported_does_not_count(tmp_path):
    # install.sh keeps generated values in plain shell variables. compose never
    # sees those, so one of them must not stand in for a missing .env line.
    required = _required_by_quickstart()
    lines = [ln for ln in _old_env_lines(required) if not ln.startswith("ENCRYPTION_KEY=")]
    compose, envf = _install_with_env(tmp_path, lines)
    before = envf.read_bytes()
    rc, out, _ = _run_fn(
        tmp_path,
        'ENCRYPTION_KEY="only-in-this-shell"\ncheck_env_required',
        compose_file=compose,
    )
    assert rc == 1, out
    assert "    ENCRYPTION_KEY — " in out
    assert "set in this shell's environment" not in out
    assert "only-in-this-shell" not in out
    assert envf.read_bytes() == before


def test_compose_files_that_require_nothing_are_called_out_not_passed(tmp_path):
    install_dir = tmp_path / "rsync-ai"
    install_dir.mkdir()
    compose = install_dir / "docker-compose.quickstart.yml"
    compose.write_text("services:\n  web:\n    image: example/web:1\n    environment:\n      A: ${A:-x}\n")
    envf = install_dir / ".env"
    envf.write_text("A=1\n")
    rc, out, _ = _run_fn(tmp_path, "check_env_required", compose_file=compose)
    assert rc == 0, out
    assert "Found no required variables in the compose files" in out
    assert "variables the compose files require are set" not in out


# ---------------------------------------------------------------------------
# the admin email
# ---------------------------------------------------------------------------


def test_a_non_tty_configuration_needs_no_email_and_writes_none(tmp_path):
    rc, out, install_dir = _run_fn(
        tmp_path,
        'TTY_OK=0\nmkdir -p "$INSTALL_DIR"\nprompt_env\nwrite_env',
        env={"LLM_PROVIDER": "none"},
    )
    assert rc == 0, out
    assert FINISHED in out
    env_text = (install_dir / ".env").read_text()
    assert "JWT_SECRET=" in env_text, "write_env produced no .env -- nothing was checked"
    for text in (out, env_text):
        assert "admin@rsync.ai" not in text
        assert "ADMIN_EMAIL" not in text


# ---------------------------------------------------------------------------
# the whole of main, with docker and curl faked
# ---------------------------------------------------------------------------

FAKE_DOCKER = r"""#!/bin/bash
printf '%s\n' "$*" >> "$FAKE_DOCKER_LOG"
case "${1:-}" in
  info) exit 0 ;;
  version) echo 27.0.0; exit 0 ;;
  ps) cat "$FAKE_DOCKER_PS" 2>/dev/null || true; exit 0 ;;
  compose)
    if [[ "${2:-}" == "version" ]]; then echo "Docker Compose version v2.99.0"; exit 0; fi
    for a in "$@"; do
      case "$a" in
        config) exec "$REAL_DOCKER" "$@" ;;
        pull|up|ps) exit 0 ;;
      esac
    done ;;
esac
echo "fake docker: unexpected: $*" >&2
exit 99
"""

FAKE_CURL = r"""#!/bin/bash
out=""; url=""
while (( $# )); do
  case "$1" in
    -o) out=$2; shift 2 ;;
    -w|--max-time) shift 2 ;;
    -*) shift ;;
    *) url=$1; shift ;;
  esac
done
printf '%s\n' "$url" >> "$FAKE_CURL_LOG"
if [[ -n "$out" ]]; then
  src="$FAKE_SERVED/${url##*/}"
  [[ -f "$src" ]] || exit 22
  cp "$src" "$out"
  exit 0
fi
case "$url" in
  */ready) printf '{"status":"ready"}\n200'; exit 0 ;;
esac
exit 7
"""


class _Install:
    def __init__(self, tmp_path):
        if REAL_DOCKER is None:
            pytest.skip("needs the docker CLI to render compose config (no daemon is used)")
        self.tmp = tmp_path
        self.api_port = _free_port()
        self.ui_port = _free_port()
        while self.ui_port == self.api_port:
            self.ui_port = _free_port()
        served = tmp_path / "served"
        served.mkdir()
        text = QUICKSTART.read_text()
        # The real file, moved off the ports a developer's own stack may hold.
        assert text.count(':5001:8080"') == 1 and text.count(':3000:3000"') == 1
        text = text.replace(':5001:8080"', f':{self.api_port}:8080"')
        text = text.replace(':3000:3000"', f':{self.ui_port}:3000"')
        (served / QUICKSTART.name).write_text(text)
        for name in OVERLAYS:
            shutil.copy(REPO / name, served / name)
        fake_bin = tmp_path / "fakebin"
        fake_bin.mkdir()
        for name, body in (("docker", FAKE_DOCKER), ("curl", FAKE_CURL)):
            p = fake_bin / name
            p.write_text(body)
            p.chmod(0o755)
        self.install_dir = tmp_path / "rsync-ai"
        self.docker_log = tmp_path / "docker.log"
        self.curl_log = tmp_path / "curl.log"
        self.ps = tmp_path / "ps.txt"
        self.env = {
            "PATH": f"{fake_bin}:{os.environ.get('PATH', '/usr/bin:/bin')}",
            # The real HOME: Docker Desktop keeps the compose plugin under it.
            "HOME": os.environ.get("HOME", str(tmp_path)),
            "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
            "LC_ALL": "C",
            "RSYNC_INSTALL_DIR": str(self.install_dir),
            "LLM_PROVIDER": "none",
            "REAL_DOCKER": REAL_DOCKER,
            "FAKE_DOCKER_LOG": str(self.docker_log),
            "FAKE_DOCKER_PS": str(self.ps),
            "FAKE_CURL_LOG": str(self.curl_log),
            "FAKE_SERVED": str(served),
        }

    def run(self):
        for log in (self.docker_log, self.curl_log):
            log.write_text("")
        proc = subprocess.run(
            [BASH, str(INSTALL_SH)],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=300,
            env=self.env,
            cwd=str(self.tmp),
            start_new_session=True,
        )
        return proc.returncode, proc.stdout + proc.stderr

    def docker_calls(self):
        return self.docker_log.read_text().splitlines()

    def verbs(self):
        """The compose subcommand of each `docker compose` call, in order."""
        out = []
        for line in self.docker_calls():
            words = line.split()
            if words[:1] == ["compose"]:
                for w in words[1:]:
                    if w in ("config", "pull", "up", "ps", "version"):
                        out.append(w)
                        break
        return out


def test_a_fresh_unattended_install_completes_without_an_email(tmp_path):
    inst = _Install(tmp_path)
    rc, out = inst.run()
    assert rc == 0, out[-3000:]
    assert "rsync.ai is running!" in out
    assert "the first account you sign up with becomes the admin" in out
    env_text = (inst.install_dir / ".env").read_text()
    for text in (out, env_text):
        assert "admin@rsync.ai" not in text
    assert "RSYNC_ADMIN_EMAILS" not in env_text
    assert "All 2 host ports rsync.ai publishes are free" in out
    verbs = inst.verbs()
    assert "config" in verbs and "pull" in verbs and "up" in verbs
    assert verbs.index("config") < verbs.index("pull") < verbs.index("up")
    # The readiness probe follows the port the compose file actually publishes.
    assert f"http://localhost:{inst.api_port}/ready" in inst.curl_log.read_text().splitlines()


def test_a_rerun_over_a_running_install_upgrades_it_in_place(tmp_path):
    inst = _Install(tmp_path)
    rc, out = inst.run()
    assert rc == 0, out[-3000:]
    first_env = (inst.install_dir / ".env").read_text().splitlines()
    assert len(first_env) > 10

    inst.ps.write_text(
        f"rsync-api-gateway\trsync-ai\t{inst.install_dir}\t127.0.0.1:{inst.api_port}->8080/tcp\n"
        f"rsync-frontend\trsync-ai\t{inst.install_dir}\t127.0.0.1:{inst.ui_port}->3000/tcp\n"
        "rsync-postgres\trsync-ai\t" f"{inst.install_dir}\t5432/tcp\n"
    )
    # What Docker's port proxy looks like from the host while those containers run.
    with _listener(port=inst.api_port), _listener(port=inst.ui_port):
        rc, out = inst.run()
    assert rc == 0, out[-3000:]
    assert "Found 3 running containers from an existing rsync.ai install" in out
    assert "upgrades them in place" in out
    assert "already in use" not in out
    assert "rsync.ai is running!" in out
    assert "the first account you sign up with" not in out
    second_env = (inst.install_dir / ".env").read_text().splitlines()
    for line in first_env:
        assert line in second_env, "a re-run changed or dropped an existing .env line"
    assert "pull" in inst.verbs()


def test_a_rerun_with_a_foreign_process_on_the_api_port_changes_nothing(tmp_path):
    inst = _Install(tmp_path)
    rc, out = inst.run()
    assert rc == 0, out[-3000:]
    env_before = (inst.install_dir / ".env").read_bytes()

    with _listener(port=inst.api_port):
        rc, out = inst.run()
    assert rc == 1, out[-3000:]
    assert f"Port {inst.api_port} is already in use, and rsync.ai's api-gateway needs it" in out
    assert "Nothing has been pulled or started" in out
    verbs = inst.verbs()
    assert "config" in verbs
    assert "pull" not in verbs and "up" not in verbs
    assert (inst.install_dir / ".env").read_bytes() == env_before


def _edit_env(envf, name, value=None, prefix=""):
    """Drop every line for `name`; put one back in place (or at the end) when `value` is given."""
    lines = envf.read_text(encoding="utf-8").splitlines()
    at = next(
        (i for i, ln in enumerate(lines) if re.match(rf"\s*(export\s+)?{name}=", ln)), len(lines)
    )
    lines = [ln for ln in lines if not re.match(rf"\s*(export\s+)?{name}=", ln)]
    if value is not None:
        lines.insert(min(at, len(lines)), f"{prefix}{name}={value}")
    envf.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _env_line_values(envf, name):
    return [
        ln.split("=", 1)[1]
        for ln in envf.read_text(encoding="utf-8").splitlines()
        if re.match(rf"\s*(export\s+)?{name}=", ln)
    ]


def test_main_stops_an_upgrade_over_a_missing_encryption_key_before_any_pull(tmp_path):
    inst = _Install(tmp_path)
    rc, out = inst.run()
    assert rc == 0, out[-3000:]
    envf = inst.install_dir / ".env"
    assert len(_env_line_values(envf, "ENCRYPTION_KEY")) == 1
    _edit_env(envf, "ENCRYPTION_KEY")
    before = envf.read_bytes()

    rc, out = inst.run()

    assert rc == 1, out[-3000:]
    assert DATA_GUARD_ADVICE["ENCRYPTION_KEY"][0] in out.splitlines()
    verbs = inst.verbs()
    assert "pull" not in verbs and "up" not in verbs, verbs
    assert "rsync.ai is running!" not in out
    assert envf.read_bytes() == before


def test_main_fills_what_it_can_and_then_still_runs_the_port_check(tmp_path):
    """The .env check first, so the port check reads a model that renders."""
    inst = _Install(tmp_path)
    rc, out = inst.run()
    assert rc == 0, out[-3000:]
    envf = inst.install_dir / ".env"
    _edit_env(envf, "REDIS_PASSWORD")

    with _listener(port=inst.api_port):
        rc, out = inst.run()

    assert rc == 1, out[-3000:]
    assert "Added a generated REDIS_PASSWORD" in out
    assert "port check was skipped" not in out
    assert f"Port {inst.api_port} is already in use, and rsync.ai's api-gateway needs it" in out
    verbs = inst.verbs()
    assert "pull" not in verbs and "up" not in verbs, verbs
    # Filled by the .env check, which ran first, and kept: the next run needs it.
    (redis,) = _env_line_values(envf, "REDIS_PASSWORD")
    assert re.fullmatch(r"[A-Za-z0-9]{32}", redis)


def test_a_bring_your_own_database_and_broker_install_passes_the_env_check(tmp_path):
    """Several -f files at once: the required names must still come out clean."""
    inst = _Install(tmp_path)
    rc, out = inst.run()
    assert rc == 0, out[-3000:]
    envf = inst.install_dir / ".env"
    _edit_env(envf, "POSTGRES_HOST", "db.example.internal")
    _edit_env(envf, "KAFKA_BROKERS", "broker.example.internal:9092")

    rc, out = inst.run()

    assert "External PostgreSQL configured" in out
    assert "External Kafka configured" in out
    assert "missing variables" not in out
    got = re.search(r"All (\d+) variables the compose files require are set", out)
    assert got and int(got.group(1)) >= len(_required_by_quickstart()), out[-3000:]
    assert rc == 0, out[-3000:]
    assert "up" in inst.verbs()


def test_a_first_run_stopped_by_a_port_still_gets_the_admin_hint_on_the_rerun(tmp_path):
    inst = _Install(tmp_path)
    with _listener(port=inst.ui_port):
        rc, out = inst.run()
    assert rc == 1, out[-3000:]
    assert f"Port {inst.ui_port} is already in use" in out
    assert (inst.install_dir / ".env").exists(), "the first run did not get as far as the .env"
    assert "rsync.ai is running!" not in out

    # The operator freed the port. Nothing of this install is running yet.
    rc, out = inst.run()
    assert rc == 0, out[-3000:]
    assert "Existing .env found" in out
    assert "rsync.ai is running!" in out
    assert "the first account you sign up with becomes the admin" in out


def test_a_leftover_admin_email_setting_is_called_out_and_not_written(tmp_path):
    inst = _Install(tmp_path)
    inst.env["ADMIN_EMAIL"] = "ops@example.com"
    rc, out = inst.run()
    assert rc == 0, out[-3000:]
    assert "ADMIN_EMAIL is set but no longer used" in out
    env_text = (inst.install_dir / ".env").read_text()
    assert "JWT_SECRET=" in env_text
    assert "ops@example.com" not in env_text
    assert "admin@rsync.ai" not in env_text + out


def test_a_rerun_reads_export_prefixed_secrets_and_fills_an_empty_one_in_place(tmp_path):
    inst = _Install(tmp_path)
    rc, out = inst.run()
    assert rc == 0, out[-3000:]
    envf = inst.install_dir / ".env"
    for name in ("INTERNAL_SERVICE_SECRET", "JWT_SECRET"):
        (value,) = _env_line_values(envf, name)
        assert len(value) >= 32
        _edit_env(envf, name, value, prefix="export ")
    before = envf.read_bytes()

    rc, out = inst.run()
    assert rc == 0, out[-3000:]
    assert "Backfilled" not in out
    assert "Added a generated" not in out
    assert envf.read_bytes() == before, "a set `export` line was treated as missing"

    # Present but empty: filled where it stands, one line, not a second one appended.
    _edit_env(envf, "INTERNAL_SERVICE_SECRET", "")
    at = envf.read_text().splitlines().index("INTERNAL_SERVICE_SECRET=")
    rc, out = inst.run()
    assert rc == 0, out[-3000:]
    assert "Backfilled a missing INTERNAL_SERVICE_SECRET" in out
    values = _env_line_values(envf, "INTERNAL_SERVICE_SECRET")
    assert len(values) == 1, f"INTERNAL_SERVICE_SECRET now appears {len(values)} times"
    assert re.fullmatch(r"[A-Za-z0-9]{32}", values[0])
    assert envf.read_text().splitlines()[at].startswith("INTERNAL_SERVICE_SECRET=")
