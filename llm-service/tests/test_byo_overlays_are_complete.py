"""A bring-your-own overlay is two halves, and either half alone breaks the stack.

Compose overlays merge; they cannot delete a service. Parking the bundled one in
a profile that is never activated is the supported way to exclude it -- that is
what every docker-compose.byo-*.yml does. But a service excluded by profile is
still named by `depends_on` in the base file, and Compose then rejects the WHOLE
project with "depends on undefined service", so nothing starts at all. The
pairing fix lives in the base file: every depends_on on a parkable service
carries `required: false`.

This already cost a ~4-minute production outage once, with each half proven
separately fatal. The failure mode is nasty because it is invisible until
somebody actually layers the overlay -- the default `up` is completely healthy,
so CI, staging and every developer laptop stay green while the BYO path is dead.

The parked-service list is DERIVED from the overlay files, not hand-written, so
adding docker-compose.byo-redis.yml tomorrow tightens this test automatically
rather than leaving it quietly stale.
"""

import glob
import os
import re
import shutil
import subprocess

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
BASE = os.path.join(REPO_ROOT, "docker-compose.quickstart.yml")
INSTALL_SH = os.path.join(REPO_ROOT, "install.sh")

# Every var the base file guards with the `:?` interpolation syntax must have a
# value or `docker compose config` refuses to render anything at all.
FAKE_ENV = {
    "ENCRYPTION_KEY": "FAKEPLACEHOLDER",
    "JWT_SECRET": "FAKEPLACEHOLDER",
    "MINIO_ACCESS_KEY": "FAKEPLACEHOLDER",
    "MINIO_SECRET_KEY": "FAKEPLACEHOLDER",
    "POSTGRES_PASSWORD": "FAKEPLACEHOLDER",
    "REDIS_PASSWORD": "FAKEPLACEHOLDER",
}


def _load(path):
    with open(path) as fh:
        return yaml.safe_load(fh)


def overlay_paths():
    found = sorted(glob.glob(os.path.join(REPO_ROOT, "docker-compose.byo-*.yml")))
    assert found, "no docker-compose.byo-*.yml found -- glob is wrong or files moved"
    return found


def parked_services():
    """{service_name: overlay_path} for every service an overlay hides via profiles."""
    parked = {}
    for path in overlay_paths():
        for name, body in (_load(path).get("services") or {}).items():
            if isinstance(body, dict) and body.get("profiles"):
                parked[name] = path
    return parked


def _install_sh_assignment(name):
    """Lift a global's assignment line out of install.sh, verbatim.

    Restating it here would test the restatement. Taking the real line means the
    harness resolves the same default the installer resolves, and a change to
    that default shows up as a behaviour change in these cases.
    """
    with open(INSTALL_SH) as fh:
        lines = [ln for ln in fh if ln.startswith(name + "=")]
    assert len(lines) == 1, f"expected exactly one {name}= line in install.sh; got {lines}"
    return lines[0]


def _install_sh_int(name):
    """Read a numeric global out of install.sh rather than restating it here."""
    line = _install_sh_assignment(name)
    value = line.split("=", 1)[1].strip().strip('"')
    assert value.isdigit(), f"{name} is not a bare integer in install.sh: {line!r}"
    return int(value)


def _install_sh_default_profiles():
    """The profile set an install activates when the operator sets nothing."""
    line = _install_sh_assignment("RSYNC_PROFILES")
    m = re.search(r"\$\{RSYNC_PROFILES-([^}]*)\}", line)
    assert m, f"RSYNC_PROFILES is not a `${{RSYNC_PROFILES-...}}` default: {line!r}"
    return [p for p in m.group(1).replace(",", " ").split() if p]


def _install_sh_ram_floor_block():
    """Lift the floor resolver -- the profile default through its case -- verbatim.

    It starts at the RSYNC_PROFILES assignment because the floor is a function of
    that variable, and ends at the case's esac. Restating the logic here would
    test the restatement; running the real lines means a change to either half
    shows up as a behaviour change in the cases below.
    """
    lines = open(INSTALL_SH).read().splitlines(True)
    starts = [i for i, ln in enumerate(lines) if ln.startswith("RSYNC_PROFILES=")]
    assert len(starts) == 1, (
        f"expected exactly one RSYNC_PROFILES= line in install.sh; got {len(starts)}"
    )
    ends = [i for i, ln in enumerate(lines) if i > starts[0] and ln.rstrip() == "esac"]
    assert ends, "no esac follows the RSYNC_PROFILES assignment -- the block moved"
    block = "".join(lines[starts[0]:ends[0] + 1])
    # install.sh has one top-level esac today, so `ends` either points at this
    # case or is empty and the assertion above fires. Bound the lift anyway: a
    # second top-level case added after this one would silently widen the span,
    # and the span is executed.
    heads = [ln for ln in block.splitlines() if ln.startswith("case ")]
    assert len(heads) == 1 and "RSYNC_PROFILES" in heads[0], (
        "the lifted span is not the profile case -- install.sh's floor resolver "
        f"moved, and running this span would execute the installer:\n{block}"
    )
    assert "MIN_RAM_GB=" in block, f"the lifted block assigns no MIN_RAM_GB:\n{block}"
    return block


def _install_sh_function(name):
    """Lift a function body out of install.sh so the cases run the real code."""
    body = []
    with open(INSTALL_SH) as fh:
        capture = False
        for line in fh:
            if line.startswith(name + "()"):
                capture = True
            if capture:
                body.append(line)
                if line.rstrip() == "}":
                    break
    assert body, f"{name}() not found in install.sh"
    return "".join(body)


def _install_sh_env_backfill(key):
    """Lift the append-once block that records `key` in the generated .env.

    Two lines of logic, and both matter: the grep decides whether an operator's
    own edit survives a re-run, and the redirect decides what a hand-typed
    compose command in the install directory resolves. Running the real block
    is the only way to check either.
    """
    lines = open(INSTALL_SH).read().splitlines(True)
    starts = [i for i, ln in enumerate(lines) if f"grep -q '^{key}=' " in ln]
    assert len(starts) == 1, f"expected one {key} backfill in install.sh; got {len(starts)}"
    ends = [i for i, ln in enumerate(lines) if i > starts[0] and ln.rstrip() == "  fi"]
    assert ends, f"no `fi` closes the {key} backfill"
    block = "".join(lines[starts[0]:ends[0] + 1])
    assert f"{key}=" in block and ">>" in block, (
        f"the lifted {key} block does not append anything:\n{block}"
    )
    return block


def test_overlays_park_at_least_kafka_and_postgres():
    """A count of zero is not an error, so assert the denominator explicitly."""
    parked = parked_services()
    assert {"kafka", "postgres"} <= set(parked), (
        f"expected the kafka and postgres overlays to park their bundled service; got {parked}"
    )


def test_every_dependant_of_a_parked_service_marks_it_not_required():
    """The other half. Without this Compose refuses the entire project."""
    parked = parked_services()
    services = _load(BASE)["services"]
    checked = 0
    missing = []
    for svc_name, body in services.items():
        deps = (body or {}).get("depends_on") or {}
        if not isinstance(deps, dict):
            # list form cannot express required:false at all
            missing += [f"{svc_name} -> {d} (list-form depends_on)" for d in deps if d in parked]
            continue
        for dep_name, cond in deps.items():
            if dep_name not in parked:
                continue
            checked += 1
            if not isinstance(cond, dict) or cond.get("required") is not False:
                missing.append(f"{svc_name} -> {dep_name} (in {os.path.basename(parked[dep_name])})")
    assert checked > 0, "no depends_on referenced any parked service -- the check was vacuous"
    assert not missing, (
        "these depends_on entries name a service an overlay parks in an inactive "
        "profile, but do not carry `required: false`. Layering that overlay makes "
        "Compose reject the whole project and NOTHING starts:\n  "
        + "\n  ".join(missing)
    )


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
@pytest.mark.parametrize("overlay", [os.path.basename(p) for p in overlay_paths()])
def test_overlay_removes_exactly_its_own_service(tmp_path, overlay):
    """Render layer: proves the project still resolves, and drops only that service.

    A skip is not a pass, which is why the static checks above never depend on
    this one.
    """
    env_file = tmp_path / "env"
    env_file.write_text("".join(f"{k}={v}\n" for k, v in FAKE_ENV.items()))

    def services(*extra):
        cmd = ["docker", "compose", "--env-file", str(env_file), "-f", BASE]
        for e in extra:
            cmd += ["-f", os.path.join(REPO_ROOT, e)]
        cmd += ["--profile", "cdc", "--profile", "generate", "config", "--services"]
        out = subprocess.run(cmd, capture_output=True, text=True, cwd=REPO_ROOT)
        assert out.returncode == 0, f"`docker compose config` failed:\n{out.stderr}"
        return set(out.stdout.split())

    base = services()
    assert len(base) > 10, f"suspiciously small base render ({base}) -- refusing to trust a diff against it"
    with_overlay = services(overlay)
    expected = {n for n, p in parked_services().items() if os.path.basename(p) == overlay}
    assert base - with_overlay == expected, (
        f"{overlay} should remove exactly {expected}; it removed {base - with_overlay}"
    )
    assert with_overlay - base == set(), f"{overlay} unexpectedly ADDED {with_overlay - base}"


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")
def test_installer_overlay_selection_survives_a_default_env(tmp_path):
    """install.sh runs under `set -o pipefail`, where a no-match grep is fatal.

    Neither POSTGRES_HOST nor KAFKA_BROKERS appears in a default .env, so a bare
    `grep | tail | cut` aborts the installer on the single most common path --
    every standard install -- while the BYO paths this function exists for are
    the only ones that survive. Exactly inverted from what a smoke test would
    catch.

    The bundled-LLM overlay is selected by the same function and is checked here
    for the same reason, plus one of its own: unlike the two BYO overlays it is
    layered on a POSITIVE condition, so getting it wrong starts an extra Ollama
    and downloads several GB into it rather than merely leaving a service out.
    """
    body = []
    with open(INSTALL_SH) as fh:
        capture = False
        for line in fh:
            if line.startswith("build_compose_args()"):
                capture = True
            if capture:
                body.append(line)
                if line.rstrip() == "}":
                    break
    assert body, "build_compose_args() not found in install.sh"

    harness = tmp_path / "h.sh"
    harness.write_text(
        "set -euo pipefail\n"
        f'INSTALL_DIR="{tmp_path}"\nENV_FILE=".env"\n'
        'COMPOSE_FILE="docker-compose.quickstart.yml"\n'
        'BYO_PG_FILE="docker-compose.byo-postgres.yml"\n'
        'BYO_KAFKA_FILE="docker-compose.byo-kafka.yml"\n'
        + _install_sh_assignment("OLLAMA_FILE")
        # The bundled-LLM branch reads both of these and runs under `set -u`.
        # DETECTED_RAM_GB is pinned at the floor rather than read off this
        # machine so the cases assert overlay SELECTION and never turn red on a
        # small CI box -- the low-RAM warning is a warning, not a decision.
        + f"MIN_RAM_GB_WITH_LLM={_install_sh_int('MIN_RAM_GB_WITH_LLM')}\n"
        + f"DETECTED_RAM_GB={_install_sh_int('MIN_RAM_GB_WITH_LLM')}\n"
        + "OLLAMA_BUNDLED=0\n"
        + "COMPOSE_ARGS=(); COMPOSE_CMD=\"\"\ninfo(){ :; }\nwarn(){ :; }\n"
        # build_compose_args reads RSYNC_PROFILES, and this harness runs under
        # `set -u`, where an unset variable inside a pattern substitution aborts
        # on bash 5 while bash 3.2 quietly expands it to nothing. Lift the real
        # assignment so the two behave the same and so this case exercises the
        # installer's own default rather than an accident of the host's bash.
        + _install_sh_assignment("RSYNC_PROFILES")
        + "".join(body)
        + '\nbuild_compose_args\necho "${COMPOSE_ARGS[*]}"\necho "BUNDLED=${OLLAMA_BUNDLED}"\n'
    )

    all_overlays = (
        "docker-compose.byo-postgres.yml",
        "docker-compose.byo-kafka.yml",
        "docker-compose.ollama.yml",
    )
    for label, env_body, expect in (
        ("default (neither key)", "POSTGRES_USER=rsync\n", []),
        ("explicit bundled", "POSTGRES_HOST=postgres\nKAFKA_BROKERS=kafka:29092\n", []),
        ("external postgres", "POSTGRES_HOST=db.example.com\n", ["docker-compose.byo-postgres.yml"]),
        ("external kafka", "KAFKA_BROKERS=b-1.example.com:9096\n", ["docker-compose.byo-kafka.yml"]),
        ("empty value", "POSTGRES_HOST=\n", []),
        # What install.sh's own option 2 writes today.
        (
            "internal llm",
            "LLM_PROVIDER=ollama\nOLLAMA_URL=http://ollama:11434\n",
            ["docker-compose.ollama.yml"],
        ),
        # What every .env written before the overlay grew a pull job carries.
        # Layering here would start a second, empty Ollama beside the
        # operator's own and download several GB into it.
        (
            "host ollama",
            "LLM_PROVIDER=ollama\nOLLAMA_URL=http://host.docker.internal:11434\n",
            [],
        ),
        ("remote ollama", "LLM_PROVIDER=ollama\nOLLAMA_URL=http://203.0.113.10:11434\n", []),
        # A hand-edited .env can leave the URL off entirely; the in-code default
        # is the bundled service, so this is a bundled install.
        ("llm provider, no url", "LLM_PROVIDER=ollama\n", ["docker-compose.ollama.yml"]),
        ("openai", "LLM_PROVIDER=openai\nOLLAMA_URL=http://ollama:11434\n", []),
        # The substring must not match a host that merely ends in the same
        # characters -- `//ollama:` is the whole word, with its scheme separator.
        ("lookalike host", "LLM_PROVIDER=ollama\nOLLAMA_URL=http://myollama:11434\n", []),
    ):
        (tmp_path / ".env").write_text(env_body)
        out = subprocess.run(["bash", str(harness)], capture_output=True, text=True)
        assert out.returncode == 0, f"[{label}] build_compose_args exited {out.returncode}: {out.stderr}"
        got = out.stdout.split()
        assert "docker-compose.quickstart.yml" in " ".join(got), f"[{label}] base file dropped: {got}"
        for overlay in all_overlays:
            layered = any(overlay in g for g in got)
            assert layered == (overlay in expect), (
                f"[{label}] expected {overlay} layered={overlay in expect}, got {layered}: {got}"
            )
        # start_stack prints the multi-gigabyte-download warning off this flag,
        # so it has to track the -f list rather than merely correlate with it.
        expect_bundled = "docker-compose.ollama.yml" in expect
        assert f"BUNDLED={1 if expect_bundled else 0}" in got, (
            f"[{label}] OLLAMA_BUNDLED disagrees with the overlay list: {got}"
        )


# The compose profiles the installer is expected NOT to activate, each with the
# reason its absence is survivable. The bar is that the code DEGRADES without
# it, not that the feature is niche -- `cdc` sat in this file for its whole life
# with nothing activating it, and a streaming pipeline does not fall back to
# batch when those three services are missing, it fails the run.
_DELIBERATELY_INERT_PROFILES = {
    "generate": (
        "the connector generator probes context7-mcp with a 3s timeout inside a "
        "try/except and carries on without it (llm-service/src/agents/"
        "tool_generator/service.py), so its absence costs a documentation "
        "lookup, not a run"
    ),
}


def test_every_compose_profile_is_activated_or_documented_as_inert():
    """A profile nothing activates is a set of services no install ever starts.

    Only the base file is in scope. Profiles declared in the byo-* overlays are
    the parking mechanism itself -- never activating those is the whole point,
    and test_overlays_park_at_least_kafka_and_postgres covers them.
    """
    declared = set()
    for body in (_load(BASE).get("services") or {}).values():
        declared.update((body or {}).get("profiles") or [])
    assert declared, f"no service in {os.path.basename(BASE)} declares a profile -- vacuous"

    default = set(_install_sh_default_profiles())
    unaccounted = declared - default - set(_DELIBERATELY_INERT_PROFILES)
    assert not unaccounted, (
        "these compose profiles are declared but no install activates them, so "
        "their services have never started anywhere: "
        + ", ".join(sorted(unaccounted))
        + ". Add each to the installer's RSYNC_PROFILES default, or to "
        "_DELIBERATELY_INERT_PROFILES with the reason its absence degrades "
        "gracefully rather than failing a run."
    )

    stale = set(_DELIBERATELY_INERT_PROFILES) - declared
    assert not stale, (
        "_DELIBERATELY_INERT_PROFILES names profiles no service declares any "
        "more: " + ", ".join(sorted(stale))
    )


_PROFILE_CASES = (
    ("default", None, ["cdc"], ["generate"]),
    ("opted out", "", [], ["cdc", "generate"]),
    ("both", "cdc,generate", ["cdc", "generate"], []),
    ("whitespace separated", "cdc generate", ["cdc", "generate"], []),
)


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")
@pytest.mark.parametrize("label,value,expect,absent", _PROFILE_CASES)
def test_the_installer_activates_the_profiles_it_says_it_does(
    tmp_path, label, value, expect, absent
):
    """The default case is the regression this file now owns.

    `cdc` shipped as a profile that nothing ever activated. `docker compose up`
    starts no profiled service unless asked, so every install came up without
    kafka-connect, debezium-mcp and kafka-mcp-sink -- and reported success.
    The orchestrator's infra pre-flight requires all three together
    (backend-orchestrator/internal/workers/infra_preflight.go), so the first
    evidence anything was missing was a streaming pipeline failing its run two
    minutes in, naming a container that was never started.

    The env var is read the way an operator sets it, on the process, so these
    cases exercise install.sh's own default rather than a copy of it.
    """
    env = {k: v for k, v in os.environ.items() if k != "RSYNC_PROFILES"}
    if value is not None:
        env["RSYNC_PROFILES"] = value

    harness = tmp_path / "profiles.sh"
    harness.write_text(
        "set -euo pipefail\n"
        f'INSTALL_DIR="{tmp_path}"\nENV_FILE=".env"\n'
        'COMPOSE_FILE="docker-compose.quickstart.yml"\n'
        'BYO_PG_FILE="docker-compose.byo-postgres.yml"\n'
        'BYO_KAFKA_FILE="docker-compose.byo-kafka.yml"\n'
        'COMPOSE_ARGS=(); COMPOSE_CMD=""\ninfo(){ :; }\n'
        + _install_sh_assignment("RSYNC_PROFILES")
        + _install_sh_function("build_compose_args")
        + '\nbuild_compose_args\necho "ARGS ${COMPOSE_ARGS[*]}"\n'
        + 'echo "CMD ${COMPOSE_CMD}"\n'
    )
    (tmp_path / ".env").write_text("POSTGRES_USER=rsync\n")
    out = subprocess.run(["bash", str(harness)], capture_output=True, text=True, env=env)
    assert out.returncode == 0, f"[{label}] exited {out.returncode}: {out.stderr}"

    args_line = [ln for ln in out.stdout.splitlines() if ln.startswith("ARGS ")]
    assert args_line, f"[{label}] harness printed no ARGS line: {out.stdout}"
    args = args_line[0][len("ARGS "):].split()
    got = [args[i + 1] for i, a in enumerate(args) if a == "--profile" and i + 1 < len(args)]
    assert got == expect, f"[{label}] expected profiles {expect}, got {got} from {args}"
    for name in absent:
        assert name not in got, f"[{label}] {name} should not be activated: {got}"

    # COMPOSE_CMD is what the operator is told to run afterwards -- the status,
    # logs, retry and stop lines at the end of an install are built from it. It
    # is derived from COMPOSE_ARGS, so it inherits these flags; assert that
    # rather than assume it, because a command that starts fewer services than
    # the install did is the same defect wearing a different hat.
    cmd_line = [ln for ln in out.stdout.splitlines() if ln.startswith("CMD ")]
    assert cmd_line, f"[{label}] harness printed no CMD line: {out.stdout}"
    for name in expect:
        assert f"--profile {name}" in cmd_line[0], (
            f"[{label}] the printed compose command dropped --profile {name}:\n{cmd_line[0]}"
        )
    for name in absent:
        assert f"--profile {name}" not in cmd_line[0], (
            f"[{label}] the printed compose command invented --profile {name}:\n{cmd_line[0]}"
        )


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")
@pytest.mark.parametrize("label,value,expect", (
    ("default", None, "cdc"),
    ("opted out", "", ""),
    ("both", "cdc,generate", "cdc,generate"),
))
def test_the_resolved_profile_set_survives_the_installer_exiting(
    tmp_path, label, value, expect
):
    """A compose command typed by hand must start what the install started.

    The installer passes `--profile` flags, and those die with the process. An
    operator who later runs `docker compose up -d` in the install directory
    gets whatever the .env says, so the resolved set is recorded there. Compose
    reads .env from the project directory, which for an absolute `-f` path is
    the directory holding the compose file -- the install directory.
    """
    env = {k: v for k, v in os.environ.items() if k != "RSYNC_PROFILES"}
    if value is not None:
        env["RSYNC_PROFILES"] = value

    harness = tmp_path / "backfill.sh"
    harness.write_text(
        "set -euo pipefail\n"
        f'INSTALL_DIR="{tmp_path}"\nENV_FILE=".env"\n'
        + _install_sh_assignment("RSYNC_PROFILES")
        + _install_sh_env_backfill("COMPOSE_PROFILES").replace("\n  ", "\n").lstrip()
    )
    (tmp_path / ".env").write_text("POSTGRES_USER=rsync\n")
    out = subprocess.run(["bash", str(harness)], capture_output=True, text=True, env=env)
    assert out.returncode == 0, f"[{label}] exited {out.returncode}: {out.stderr}"

    written = [
        ln for ln in (tmp_path / ".env").read_text().splitlines()
        if ln.startswith("COMPOSE_PROFILES=")
    ]
    assert written == [f"COMPOSE_PROFILES={expect}"], (
        f"[{label}] RSYNC_PROFILES={value!r} recorded {written} in the .env; "
        f"expected exactly ['COMPOSE_PROFILES={expect}']"
    )

    # Append-once, the way INTERNAL_SERVICE_SECRET is: a re-run must not append
    # a second line, and must not overwrite an operator's edit. The cost of that
    # choice is documented in install.sh rather than hidden -- opting out on a
    # re-run leaves the earlier value in the file.
    out = subprocess.run(["bash", str(harness)], capture_output=True, text=True, env=env)
    assert out.returncode == 0, f"[{label}] re-run exited {out.returncode}: {out.stderr}"
    again = [
        ln for ln in (tmp_path / ".env").read_text().splitlines()
        if ln.startswith("COMPOSE_PROFILES=")
    ]
    assert again == written, f"[{label}] a second run changed the .env: {written} -> {again}"


_RAM_FLOOR_CASES = (
    ("default", None, "MIN_RAM_GB_CDC"),
    ("opted out", "", "MIN_RAM_GB_BATCH"),
    ("cdc named", "cdc", "MIN_RAM_GB_CDC"),
    ("both", "cdc,generate", "MIN_RAM_GB_CDC"),
    ("generate only", "generate", "MIN_RAM_GB_BATCH"),
    # A substring match would hand this one the cdc floor for a profile that is
    # not cdc, which is why the case pads with spaces.
    ("a profile whose name contains cdc", "nocdc", "MIN_RAM_GB_BATCH"),
)


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")
@pytest.mark.parametrize("label,value,expected_name", _RAM_FLOOR_CASES)
def test_the_ram_floor_tracks_the_profiles_the_install_activates(
    tmp_path, label, value, expected_name
):
    """A RAM warning must be about containers this install actually starts.

    The defect the profile cases above cover is a run that failed on a container
    nothing had started. One unconditional floor ships that same shape at install
    time from the other side: an operator who sets RSYNC_PROFILES= runs exactly
    the unprofiled services 6GB has always sized, and warning them about a JVM
    they excluded is again a message about a container that will never exist.

    README.md and docs/getting-started/quickstart.md both tell that operator the
    floor drops to 6GB. Nothing but resolving it can check that sentence.
    """
    assert _install_sh_int("MIN_RAM_GB_CDC") > _install_sh_int("MIN_RAM_GB_BATCH"), (
        "the cdc floor has to exceed the batch floor, or no case here can tell "
        "the two apart and every assertion below is vacuous"
    )
    expected = _install_sh_int(expected_name)

    script = tmp_path / "ram_floor.sh"
    script.write_text(
        "set -u\n" + _install_sh_ram_floor_block() + 'echo "FLOOR ${MIN_RAM_GB}"\n'
    )
    env = {k: v for k, v in os.environ.items() if k != "RSYNC_PROFILES"}
    if value is not None:
        env["RSYNC_PROFILES"] = value

    out = subprocess.run(["bash", str(script)], capture_output=True, text=True, env=env)
    assert out.returncode == 0, f"[{label}] exited {out.returncode}: {out.stderr}"
    got = [ln for ln in out.stdout.splitlines() if ln.startswith("FLOOR ")]
    assert len(got) == 1, f"[{label}] expected one FLOOR line, got {out.stdout!r}"
    assert int(got[0].split()[1]) == expected, (
        f"[{label}] RSYNC_PROFILES={value!r} resolved a {got[0].split()[1]}GB floor; "
        f"{expected_name} is {expected}GB"
    )
