"""The URL `install.sh` prints must be an address something is actually listening on.

Three surfaces have to agree for a server install to work, and none of them can
see the other two:

  * `install.sh` decides a scheme, a host and a port, and prints them.
  * `docker-compose.quickstart.yml` decides which interface those ports bind to.
  * the operator's firewall decides who can reach them.

The first two drifted, and the drift was invisible for the worst possible reason:
`wait_healthy` polls `http://localhost:5001/health`, which passes on the server
regardless of what was printed. So the install reported success, handed over
`https://<host>`, and the browser got connection refused -- with nothing in the
transcript suggesting the installer had been wrong. A check that always passes is
worse than no check, because it is *believed*.

These tests pin the agreement. The behavioural ones source `install.sh` and drive
`prompt_env` for real rather than grepping it, because the property that matters
("scheme and bind address agree") is a relationship between two variables computed
in different branches -- exactly the thing a regex reads right past.
"""

import os
import pathlib
import re
import subprocess

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
INSTALL_SH = REPO / "install.sh"
QUICKSTART = REPO / "docker-compose.quickstart.yml"
COMPOSE_FILES = ("docker-compose.yml", "docker-compose.quickstart.yml")

# The one name Docker Desktop resolves for free and Linux Docker does not. Every
# server install in the world is the second case.
HOST_ALIAS = "host.docker.internal"
GATEWAY_MAPPING = f"{HOST_ALIAS}:host-gateway"


def _load(name):
    return yaml.safe_load((REPO / name).read_text())


def _env_of(svc):
    """Compose accepts `environment:` as a map or a list; normalise to a map."""
    env = svc.get("environment") or {}
    if isinstance(env, list):
        out = {}
        for item in env:
            key, _, value = str(item).partition("=")
            out[key] = value
        return out
    return {k: ("" if v is None else str(v)) for k, v in env.items()}


def _services(name):
    return {
        k: v
        for k, v in (_load(name).get("services") or {}).items()
        if isinstance(v, dict)
    }


# ---------------------------------------------------------------------------
# host.docker.internal must resolve wherever it is used
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
def test_every_service_naming_the_host_alias_can_resolve_it(compose_file):
    """A service whose env points at host.docker.internal needs the extra_hosts entry.

    Without it the name simply does not resolve under Linux Docker, and the
    symptom is not a config error -- it is a connect failure deep inside whatever
    client happened to dial it. `install.sh` selects Ollama automatically for an
    unattended install with no `OPENAI_API_KEY`, and Ollama's default URL is this
    name, so the default path and the platform that breaks it are the same path.

    Comments naming the alias are deliberately not matched: `api-gateway` explains
    in prose why it disables the SigNoz enrichment that would otherwise dial this
    name, and that service correctly needs no mapping.
    """
    services = _services(compose_file)
    assert len(services) >= 10, (
        f"{compose_file} parsed to {len(services)} services -- too few to be a real "
        "parse. An empty census makes every assertion below vacuously true."
    )

    offenders = {}
    for name, svc in sorted(services.items()):
        uses_alias = [k for k, v in _env_of(svc).items() if HOST_ALIAS in v]
        if not uses_alias:
            continue
        declared = [str(h) for h in (svc.get("extra_hosts") or [])]
        if not any(h.replace("=", ":") == GATEWAY_MAPPING for h in declared):
            offenders[name] = uses_alias

    assert not offenders, (
        f"{compose_file}: these services point at {HOST_ALIAS} but never declare it, "
        f"so the name does not resolve under Linux Docker:\n"
        + "\n".join(f"    {n}: via {v}" for n, v in offenders.items())
        + f"\n  Add:  extra_hosts:\n          - \"{GATEWAY_MAPPING}\""
    )


def test_the_alias_census_is_not_empty():
    """Anti-vacuity: the test above only means something if it finds users of the alias."""
    users = [
        name
        for name, svc in _services("docker-compose.quickstart.yml").items()
        if any(HOST_ALIAS in v for v in _env_of(svc).values())
    ]
    assert len(users) >= 2, (
        f"Expected the quickstart's Ollama consumers (llm-service, planner) to name "
        f"{HOST_ALIAS}; found {users}. If the default moved, retarget this test rather "
        "than deleting it -- a zero-length census silently passes the guard above."
    )


# ---------------------------------------------------------------------------
# the published ports must be steerable, and must default to loopback
# ---------------------------------------------------------------------------


def test_every_published_quickstart_port_is_bind_address_parameterised():
    """A hardcoded `127.0.0.1:` here is unreachable from anywhere but the server.

    That is the correct default for a laptop and fatal for a server, so the
    interface has to be a knob rather than a literal.
    """
    published = []
    for name, svc in _services("docker-compose.quickstart.yml").items():
        for entry in svc.get("ports") or []:
            published.append((name, str(entry)))

    assert len(published) >= 2, (
        f"Only {len(published)} published ports found in the quickstart -- expected at "
        "least the api-gateway and frontend. A short list makes this test vacuous."
    )

    hardcoded = [
        (n, e) for n, e in published if e.startswith("127.0.0.1:") or e.startswith("0.0.0.0:")
    ]
    assert not hardcoded, (
        "These quickstart ports pin an interface literally, so a server install "
        "cannot publish them:\n"
        + "\n".join(f"    {n}: {e}" for n, e in hardcoded)
        + '\n  Use:  "${RSYNC_BIND_ADDR:-127.0.0.1}:<host>:<container>"'
    )

    unparameterised = [
        (n, e) for n, e in published if "RSYNC_BIND_ADDR" not in e
    ]
    assert not unparameterised, (
        "These quickstart ports do not read RSYNC_BIND_ADDR:\n"
        + "\n".join(f"    {n}: {e}" for n, e in unparameterised)
    )


def test_the_bind_address_defaults_to_loopback():
    """The knob must fail safe. An unset RSYNC_BIND_ADDR is a laptop, not a server.

    Getting this backwards publishes an unauthenticated stack to the LAN for every
    developer who runs the quickstart, which is a far worse failure than the one
    the knob exists to fix.
    """
    text = QUICKSTART.read_text()
    defaults = [
        line.strip()
        for line in text.splitlines()
        if "RSYNC_BIND_ADDR" in line and line.strip().startswith("-")
    ]
    assert len(defaults) >= 2, f"Expected >=2 parameterised ports, found {defaults}"
    for line in defaults:
        assert "${RSYNC_BIND_ADDR:-127.0.0.1}" in line, (
            f"Port entry does not default to loopback: {line}\n"
            "  Unset must mean 127.0.0.1, or a plain `docker compose up` on a laptop "
            "publishes the stack to the network."
        )


# ---------------------------------------------------------------------------
# install.sh: the URL it prints and the interface it binds must agree
# ---------------------------------------------------------------------------


def _drive_prompt_env(tmp_path, env=None, answers=None):
    """Source install.sh (minus its `main` call) and run prompt_env for real.

    Returns the resulting variables as a dict. `answers` simulates a terminal by
    feeding fd 3, which is where `ask` reads from.
    """
    lib = tmp_path / "install-lib.sh"
    lib.write_text("\n".join(INSTALL_SH.read_text().splitlines()[:-1]) + "\n")

    if answers is None:
        tty_setup = "TTY_OK=0"
    else:
        answers_file = tmp_path / "answers.txt"
        answers_file.write_text("\n".join(answers) + "\n")
        tty_setup = f'exec 3< "{answers_file}"\nTTY_OK=1'

    driver = tmp_path / "drive.sh"
    driver.write_text(
        "set -uo pipefail\n"
        "TTY_OK=0\n"
        f'source "{lib}" >/dev/null 2>&1\n'
        f"{tty_setup}\n"
        "prompt_env >/dev/null 2>&1\n"
        'printf "RSYNC_BIND_ADDR=%s\\n" "${RSYNC_BIND_ADDR:-}"\n'
        'printf "NEXTAUTH_URL=%s\\n" "${NEXTAUTH_URL:-}"\n'
        'printf "PUBLIC_URL=%s\\n" "${PUBLIC_URL:-}"\n'
        'printf "PUBLIC_WS_URL=%s\\n" "${PUBLIC_WS_URL:-}"\n'
        'printf "LLM_PROVIDER=%s\\n" "${LLM_PROVIDER:-}"\n'
        'printf "RSYNC_COOKIE_SECURE=%s\\n" "${RSYNC_COOKIE_SECURE:-}"\n'
    )

    run_env = dict(os.environ)
    for key in (
        "PUBLIC_HOST",
        "OPENAI_API_KEY",
        "LLM_PROVIDER",
        "RSYNC_BIND_ADDR",
        "RSYNC_COOKIE_SECURE",
    ):
        run_env.pop(key, None)
    run_env.update(env or {})

    proc = subprocess.run(
        ["bash", str(driver)], capture_output=True, text=True, env=run_env, timeout=60
    )
    out = {}
    for line in proc.stdout.splitlines():
        key, _, value = line.partition("=")
        out[key] = value
    assert "NEXTAUTH_URL" in out, (
        f"prompt_env produced no output -- the driver failed.\n"
        f"stdout={proc.stdout!r}\nstderr={proc.stderr[-2000:]!r}"
    )
    return out


def test_install_sh_is_syntactically_valid():
    """Cheap, and it is the failure that would make every test below unreadable."""
    proc = subprocess.run(
        ["bash", "-n", str(INSTALL_SH)], capture_output=True, text=True, timeout=60
    )
    assert proc.returncode == 0, f"install.sh has a syntax error:\n{proc.stderr}"


def test_a_localhost_install_stays_on_loopback(tmp_path):
    got = _drive_prompt_env(tmp_path, env={"PUBLIC_HOST": "localhost"})
    assert got["RSYNC_BIND_ADDR"] == "127.0.0.1"
    assert got["NEXTAUTH_URL"] == "http://localhost:3000"
    assert got["PUBLIC_URL"] == "http://localhost:5001"


def test_an_unattended_server_install_prints_a_url_that_matches_its_bind(tmp_path):
    """The headline regression: a URL nobody is listening on.

    Unattended is the case that matters, because `curl ... | bash` on a fresh cloud
    VM has no terminal, so every branch here is taken by default rather than chosen.
    """
    got = _drive_prompt_env(tmp_path, env={"PUBLIC_HOST": "rsync.example.com"})
    assert got["RSYNC_BIND_ADDR"] == "0.0.0.0", (
        "A named host with no proxy must publish the ports, or the URL below is "
        "unreachable from anywhere but the server itself."
    )
    assert got["NEXTAUTH_URL"] == "http://rsync.example.com:3000", (
        f"Got {got['NEXTAUTH_URL']!r}. Nothing in the quickstart terminates TLS or "
        "listens on 443, so an https:// URL here is a promise the stack cannot keep."
    )
    assert got["PUBLIC_URL"] == "http://rsync.example.com:5001"
    assert got["PUBLIC_WS_URL"] == "ws://rsync.example.com:5001/ws"


def test_the_proxy_answer_keeps_the_ports_on_loopback(tmp_path):
    """The opposite configuration, and it must be the opposite in BOTH variables.

    With a proxy terminating TLS on the host, loopback is reachable and correct;
    binding 0.0.0.0 as well would expose the plaintext port beside the proxied one.
    """
    got = _drive_prompt_env(
        tmp_path,
        answers=["2", "rsync.example.com", "2", "admin@example.com"],
    )
    assert got["RSYNC_BIND_ADDR"] == "127.0.0.1"
    assert got["NEXTAUTH_URL"] == "https://rsync.example.com"
    assert got["PUBLIC_WS_URL"] == "wss://rsync.example.com/ws"


@pytest.mark.parametrize(
    "case,env,answers",
    [
        ("unattended localhost", {"PUBLIC_HOST": "localhost"}, None),
        ("unattended host", {"PUBLIC_HOST": "rsync.example.com"}, None),
        ("unattended bare IP", {"PUBLIC_HOST": "203.0.113.10"}, None),
        ("tty direct", None, ["2", "rsync.example.com", "1", "admin@example.com"]),
        ("tty proxied", None, ["2", "rsync.example.com", "2", "admin@example.com"]),
    ],
)
def test_scheme_and_bind_address_never_disagree(tmp_path, case, env, answers):
    """The invariant behind all of the above, stated once.

    `https://` is only honest when something else is terminating TLS, which is
    exactly the case where the ports stay on loopback. `0.0.0.0` means this stack
    is serving directly, which it can only do over plain HTTP on its own ports.
    Any other pairing is a URL that does not answer.
    """
    got = _drive_prompt_env(tmp_path, env=env, answers=answers)
    url, bind = got["NEXTAUTH_URL"], got["RSYNC_BIND_ADDR"]

    if url.startswith("https://"):
        assert bind == "127.0.0.1", (
            f"{case}: printed {url} while publishing on {bind}. https:// implies a "
            "proxy in front, and this stack ships none -- so it must not also be "
            "publishing its plaintext ports to the network."
        )
    if bind == "0.0.0.0":
        assert url.startswith("http://") and ":3000" in url, (
            f"{case}: publishing on {bind} but printed {url}. Serving directly means "
            "plain HTTP on the published port; anything else is unreachable."
        )


def test_the_bind_address_reaches_the_env_file():
    """Computed and not written down is the same as not computed.

    Compose defaults `RSYNC_BIND_ADDR` to loopback, so an install.sh that decides
    `0.0.0.0` and forgets to persist it produces precisely the original bug, with
    the added insult that the decision was correct.
    """
    text = INSTALL_SH.read_text()
    assert "RSYNC_BIND_ADDR=${RSYNC_BIND_ADDR}" in text, (
        "write_env does not emit RSYNC_BIND_ADDR into the generated .env, so the "
        "value install.sh computed never reaches docker compose."
    )


def _drive_wait_healthy(tmp_path, curl_body, env=None):
    """Run the real `wait_healthy` with `curl` stubbed, and report its exit status.

    Same technique as `_drive_prompt_env` above, for the same reason: what matters
    is a STATUS, and a status is not visible to a regex.

    Two stubs make this take milliseconds instead of five minutes:

      * `curl` -- a shell function shadowing the binary, emitting whatever
        `curl_body` says. `wait_healthy` calls curl through `$PATH`, so a function
        of the same name intercepts every probe.
      * `sleep` -- advances bash's own `SECONDS` instead of waiting. `SECONDS` is
        assignable, and the deadline is computed from it, so time passes without
        elapsing. This is also why the deadline must be wall-clock: a loop that
        counts its own sleeps cannot be driven this way and could not be tested.
    """
    lib = tmp_path / "lib.sh"
    lib.write_text("\n".join(INSTALL_SH.read_text().splitlines()[:-1]) + "\n")
    driver = tmp_path / "drive_wait.sh"
    driver.write_text(
        "set -uo pipefail\n"
        "TTY_OK=0\n"
        f'source "{lib}" >/dev/null 2>&1\n'
        f'INSTALL_DIR="{tmp_path}"\n'
        'COMPOSE_CMD="docker compose "\n'
        f"{curl_body}\n"
        "sleep() { SECONDS=$(( SECONDS + 60 )); }\n"
        # `source` ran install.sh's own `set -euo pipefail` in THIS shell, so a
        # non-zero wait_healthy would abort the driver before it could report the
        # status -- silently, since its output is redirected. `|| rc=$?` is a
        # condition context, which errexit exempts.
        "rc=0\n"
        "wait_healthy >/dev/null 2>&1 || rc=$?\n"
        'printf "RC=%s\\n" "$rc"\n'
        'printf "REASON=%s\\n" "${READY_REASON:-}"\n'
    )
    run_env = dict(os.environ)
    run_env.update(env or {})
    proc = subprocess.run(
        ["bash", str(driver)], capture_output=True, text=True, env=run_env, timeout=120
    )
    out = {}
    for line in proc.stdout.splitlines():
        key, _, value = line.partition("=")
        out[key] = value
    assert "RC" in out, (
        f"the driver produced no status.\n"
        f"stdout={proc.stdout!r}\nstderr={proc.stderr[-2000:]!r}"
    )
    return out


# `curl` stubs. The response shape is curl's, not the gateway's: `-w '\n%{http_code}'`
# appends a newline and the status code after the body, which is what probe_ready parses.
_CURL_REFUSED = "curl() { return 7; }"
_CURL_READY = "curl() { printf '\\n200'; }"
_CURL_503 = (
    """curl() { printf '{"status":"not_ready","reason":"schema_not_migrated"}\\n503'; }"""
)


def test_wait_healthy_fails_when_nothing_ever_becomes_ready(tmp_path):
    """The bug this whole file exists for, asserted on the status rather than the text.

    `wait_healthy`'s timeout branch used to end in a bare `return`, which carries
    the status of the preceding command -- after an `echo`, zero. So a stack that
    never came up returned SUCCESS to `main`, which printed the green banner and a
    login URL. Every probe here is refused; the only correct answer is non-zero.
    """
    out = _drive_wait_healthy(tmp_path, _CURL_REFUSED)
    assert out["RC"] != "0", (
        "wait_healthy reported success after every readiness probe was refused. "
        "main() prints the 'rsync.ai is running!' banner on this status, so this "
        "is the installer certifying a stack that never started."
    )


def test_wait_healthy_succeeds_when_the_gateway_is_ready(tmp_path):
    """The control. Without it, a `wait_healthy` that always failed would pass above."""
    out = _drive_wait_healthy(tmp_path, _CURL_READY)
    assert out["RC"] == "0", (
        "wait_healthy failed against a gateway answering 200. The test above would "
        "then pass for the wrong reason, so this one is load-bearing."
    )


def test_a_gateway_that_is_up_but_has_no_database_is_not_ready(tmp_path):
    """Liveness is not readiness, and only one of the two can see this failure.

    api-gateway's `/health` is a hardcoded 200 literal, and `cmd/server/main.go`
    logs-and-continues when `db.Init()` fails ("using mock data") and when
    migrations fail. A gateway with no database therefore answers `/health` 200
    forever. `/ready` (`cmd/server/ready.go`) 503s with the reason instead, so
    polling it is the difference between a working install and a mock one.
    """
    out = _drive_wait_healthy(tmp_path, _CURL_503)
    assert out["RC"] != "0", (
        "wait_healthy accepted a gateway returning 503 from /ready. A gateway that "
        "started without its database serves /health 200, so a liveness probe "
        "cannot tell this apart from a working install."
    )
    assert "schema_not_migrated" in out["REASON"], (
        "the gateway's own diagnosis was discarded. It is the whole value of the "
        "503 body, and the failure banner has nothing else to tell the operator. "
        f"Got: {out['REASON']!r}"
    )


def test_the_health_check_also_probes_the_url_it_prints(tmp_path):
    """`wait_healthy` polling only localhost is what made the original bug silent.

    It is legitimate for the public probe to warn rather than fail -- a closed
    security group is the operator's call -- but it must happen, or success is
    reported on evidence that cannot distinguish a working install from a broken one.

    Driven, not grepped. This test used to slice `install.sh` between the literals
    "wait_healthy()" and "print_success()" and assert on the text between them --
    which broke the moment the loopback probe was factored into a `probe_ready`
    helper defined above `wait_healthy`, reporting the probe as missing when it had
    only moved. The probe is a behaviour; assert on the behaviour.

    The stub counts calls and records URLs, so "did it probe the printed URL" is
    answered by what the function DID.
    """
    stub = (
        "curl() {\n"
        '  for a in "$@"; do case "$a" in http*) echo "$a" >> "$PROBED";; esac; done\n'
        "  printf '\\n200'\n"
        "}"
    )
    probed = tmp_path / "probed.txt"
    probed.write_text("")
    out = _drive_wait_healthy(
        tmp_path,
        stub,
        env={
            "PROBED": str(probed),
            "RSYNC_BIND_ADDR": "0.0.0.0",
            "NEXTAUTH_URL": "http://rsync.example.com:3000",
        },
    )
    assert out["RC"] == "0", "sanity: a 200 on every probe should be a success"
    urls = probed.read_text().split()
    assert any("localhost:5001" in u for u in urls), (
        f"sanity: the loopback readiness probe should still happen. Probed: {urls}"
    )
    assert any("rsync.example.com" in u for u in urls), (
        "wait_healthy never probed the URL it is about to print. Its localhost poll "
        "passes on the server no matter what host was configured, so on its own it "
        f"cannot tell a reachable install from an unreachable one. Probed: {urls}"
    )


def test_the_public_probe_keys_on_a_variable_the_env_file_actually_holds(tmp_path):
    """The probe above must also run on the RE-RUN path, which is the common one.

    It used to be gated on `PUBLIC_HOST`, which `prompt_env` asks for and
    `write_env` never writes to the .env. So when `main` found an existing .env --
    the documented bring-your-own-Postgres workflow -- `PUBLIC_HOST` was empty, the
    condition was false, and the probe added to stop this script printing an
    unreachable URL did not run at all.

    Simulated by giving the driver ONLY what a re-run recovers from the .env file.
    """
    env_file = tmp_path / ".env"
    env_file.write_text(
        "RSYNC_BIND_ADDR=0.0.0.0\n"
        "NEXTAUTH_URL=http://rsync.example.com:3000\n"
    )
    persisted = dict(
        (line.split("=", 1)[0], line.split("=", 1)[1])
        for line in env_file.read_text().splitlines()
        if "=" in line
    )
    assert "PUBLIC_HOST" not in persisted, (
        "sanity: write_env does not persist PUBLIC_HOST, which is why gating on it "
        "silently disabled the probe on every re-run."
    )
    stub = (
        "curl() {\n"
        '  for a in "$@"; do case "$a" in http*) echo "$a" >> "$PROBED";; esac; done\n'
        "  printf '\\n200'\n"
        "}"
    )
    probed = tmp_path / "probed.txt"
    probed.write_text("")
    env = {"PROBED": str(probed)}
    env.update(persisted)
    env.pop("PUBLIC_HOST", None)
    _drive_wait_healthy(tmp_path, stub, env=env)
    urls = probed.read_text().split()
    assert any("rsync.example.com" in u for u in urls), (
        "with only the keys a re-run can recover from the .env, the public-URL probe "
        "did not fire. It is gated on something write_env never persists, so it is "
        f"dead on the re-run path. Probed: {urls}"
    )


# ---------------------------------------------------------------------------
# the address install.sh computed must survive all the way into the browser
# ---------------------------------------------------------------------------
#
# There are four links in that chain and each one broke independently:
#
#   install.sh computes PUBLIC_URL -> writes it to .env -> compose passes it to
#   the frontend container -> the browser reads it.
#
# The last link is the one that makes this class of bug so durable.
# `NEXT_PUBLIC_*` is substituted by the compiler at `docker build` time, so on a
# PUBLISHED image every runtime value compose passes under that name is
# discarded and the browser keeps whatever CI baked in -- `http://localhost:5001`.
# On a laptop that is accidentally correct. On a server the login POST goes to
# the operator's own machine, and nothing in any log says so.


def test_the_public_urls_reach_the_env_file():
    """Computed and not written down is the same as not computed."""
    text = INSTALL_SH.read_text()
    for var in ("PUBLIC_URL", "PUBLIC_WS_URL"):
        assert f"{var}=${{{var}}}" in text, (
            f"write_env does not emit {var} into the generated .env, so the value "
            "install.sh computed never reaches docker compose."
        )


def test_the_frontend_is_told_its_public_address_under_a_runtime_readable_name():
    """`NEXT_PUBLIC_*` alone cannot carry this, however correct the value is.

    The pair is still passed -- it is what a from-source `docker compose build`
    bakes, and that build is the one case where build time and run time share an
    environment. But a published image needs a name the compiler did not touch,
    or the container has no way to learn where it actually lives.
    """
    env = _env_of(_services("docker-compose.quickstart.yml")["frontend"])

    for var in ("PUBLIC_URL", "PUBLIC_WS_URL"):
        assert var in env, (
            f"The frontend service never receives {var}. Only the NEXT_PUBLIC_* "
            "names are passed, and those are frozen into the bundle at build "
            "time -- so a published image ignores them and every browser request "
            "goes to whatever address CI happened to build with."
        )
        assert f"${{{var}" in env[var], (
            f"frontend {var} is pinned to {env[var]!r} instead of reading the "
            "operator's value from the environment."
        )


def test_the_frontend_reads_that_name_at_runtime():
    """The compose wiring above is inert unless something in the app reads it.

    Kept deliberately shallow -- the behaviour is pinned by
    frontend/src/__tests__/runtime-config-injection.test.ts, which drives the
    real resolver. This is the cross-language half: a rename on either side of
    the boundary breaks the chain silently, and neither suite alone would notice.
    """
    runtime_env = REPO / "frontend" / "src" / "lib" / "config" / "runtime-env.ts"
    assert runtime_env.exists(), (
        "frontend/src/lib/config/runtime-env.ts is gone. Compose still passes "
        "PUBLIC_URL to the frontend, but nothing reads it, so the value is lost "
        "between the container's environment and the browser."
    )
    text = runtime_env.read_text()
    for var in ("PUBLIC_URL", "PUBLIC_WS_URL"):
        assert f'"{var}"' in text, f"runtime-env.ts no longer reads {var}"

    # Comments are stripped first. The module DOCUMENTS the NEXT_PUBLIC_ trap at
    # length, so matching raw text would fail on the explanation of the very rule
    # being enforced -- and would start passing the moment someone deleted it.
    code = re.sub(r"/\*[\s\S]*?\*/", "", text)
    code = "\n".join(l for l in code.splitlines() if not l.strip().startswith("//"))
    assert "readRuntimeConfig" in code, (
        "comment stripping ate the module body; the assertion below would be vacuous"
    )
    assert "NEXT_PUBLIC" not in code, (
        "runtime-env.ts reads a NEXT_PUBLIC_* name. The compiler substitutes "
        "those at build time, which is the exact failure this module exists to "
        "route around."
    )


# ---------------------------------------------------------------------------
# the session cookie has to survive the scheme install.sh chose
# ---------------------------------------------------------------------------


def test_the_cookie_flag_reaches_the_gateway():
    text = INSTALL_SH.read_text()
    assert "RSYNC_COOKIE_SECURE=${RSYNC_COOKIE_SECURE}" in text, (
        "write_env does not emit RSYNC_COOKIE_SECURE, so the api-gateway falls "
        "back to ENVIRONMENT=production and sets Secure on a plain-http install."
    )
    env = _env_of(_services("docker-compose.quickstart.yml")["api-gateway"])
    assert "RSYNC_COOKIE_SECURE" in env, (
        "The api-gateway service never receives RSYNC_COOKIE_SECURE; the value "
        "in .env is written and then ignored."
    )


@pytest.mark.parametrize(
    "case,env,answers",
    [
        ("unattended localhost", {"PUBLIC_HOST": "localhost"}, None),
        ("unattended host", {"PUBLIC_HOST": "rsync.example.com"}, None),
        ("unattended bare IP", {"PUBLIC_HOST": "203.0.113.10"}, None),
        ("tty direct", None, ["2", "rsync.example.com", "1", "admin@example.com"]),
        ("tty proxied", None, ["2", "rsync.example.com", "2", "admin@example.com"]),
    ],
)
def test_the_cookie_flag_never_disagrees_with_the_scheme(tmp_path, case, env, answers):
    """Secure over plain http is a login that returns 200 and loses the session.

    Browsers discard a Secure cookie that arrives over http, with one exception --
    localhost, which they treat as trustworthy. That exception is why the failure
    is invisible on a laptop and total on a server: the password is accepted, the
    cookie is dropped on the floor, and the operator is bounced back to the login
    form with nothing logged anywhere.

    The reverse pairing matters just as much. Behind a TLS-terminating proxy the
    flag must be on, or the session cookie is offered up over plaintext to
    anything that can strip the scheme.
    """
    got = _drive_prompt_env(tmp_path, env=env, answers=answers)
    url, secure = got["NEXTAUTH_URL"], got["RSYNC_COOKIE_SECURE"]

    assert secure in ("true", "false"), (
        f"{case}: RSYNC_COOKIE_SECURE={secure!r} is not a value Go's "
        "strconv.ParseBool accepts, so the gateway silently falls back to the "
        f"ENVIRONMENT default and {url} keeps the bug this flag exists to fix."
    )

    if url.startswith("https://"):
        assert secure == "true", (
            f"{case}: {url} is https but RSYNC_COOKIE_SECURE={secure}. The session "
            "cookie would be sent over plaintext to anyone who can downgrade the "
            "scheme."
        )
    else:
        assert secure == "false", (
            f"{case}: {url} is plain http but RSYNC_COOKIE_SECURE={secure}. The "
            "browser will discard the cookie and the operator can never log in."
        )
