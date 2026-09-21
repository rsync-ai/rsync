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


def added_services(overlay_path):
    """{service} an overlay DEFINES that the base file does not.

    Derived from the two files, never hand-listed, so a new one-shot in an
    overlay tightens the reachability check below instead of tripping it.
    """
    base = set(_load(BASE)["services"])
    return {n for n in (_load(overlay_path).get("services") or {}) if n not in base}


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
    introduced = added_services(os.path.join(REPO_ROOT, overlay))
    assert with_overlay - base == introduced, (
        f"{overlay} should add exactly the services it declares ({introduced}); "
        f"the render added {with_overlay - base}"
    )


def _install_sh_function(name):
    """Lift one shell function out of install.sh, verbatim.

    Copying the logic into the test instead would test the copy. Extraction
    means a change to the real function either keeps these cases passing or
    breaks them, which is the only arrangement worth having.
    """
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


def _install_sh_int(name):
    """Read a numeric global out of install.sh rather than restating it here."""
    with open(INSTALL_SH) as fh:
        m = re.search(rf"^{re.escape(name)}=(\d+)\s*$", fh.read(), re.M)
    assert m, f"{name} is not a bare integer assignment in install.sh"
    return int(m.group(1))


def _overlay_harness(tmp_path, detected_ram_gb=16):
    """A runnable install.sh fragment: the two functions plus the globals they read.

    Everything named here is a global the real script sets before calling
    build_compose_args. `set -u` turns a forgotten one into a hard failure
    rather than an empty string, which is the point -- a global added to the
    function without being added to its caller would pass silently otherwise.
    """
    harness = tmp_path / "h.sh"
    harness.write_text(
        "set -euo pipefail\n"
        f'INSTALL_DIR="{tmp_path}"\nENV_FILE=".env"\n'
        'COMPOSE_FILE="docker-compose.quickstart.yml"\n'
        'BYO_PG_FILE="docker-compose.byo-postgres.yml"\n'
        'BYO_KAFKA_FILE="docker-compose.byo-kafka.yml"\n'
        'OLLAMA_FILE="docker-compose.ollama.yml"\n'
        "COMPOSE_ARGS=(); COMPOSE_CMD=\"\"\nOLLAMA_BUNDLED=0\n"
        f"MIN_RAM_GB_WITH_LLM={_install_sh_int('MIN_RAM_GB_WITH_LLM')}\n"
        f"DETECTED_RAM_GB={detected_ram_gb}\n"
        "info(){ echo \"INFO $*\"; }\nwarn(){ echo \"WARN $*\"; }\n"
        + _install_sh_assignment("RSYNC_PROFILES")
        + _install_sh_function("write_compose_helper")
        + _install_sh_function("build_compose_args")
        + '\nbuild_compose_args\necho "ARGS ${COMPOSE_ARGS[*]}"\n'
        'echo "BUNDLED $OLLAMA_BUNDLED"\n'
    )
    return harness


# Every case below is (label, .env body, overlays expected, OLLAMA_BUNDLED expected).
# The LLM rows are the interesting ones: LLM_PROVIDER=ollama says the tier speaks
# Ollama, it does not say WHICH Ollama, and every .env written before the bundled
# overlay existed names the operator's own at host.docker.internal. Layering on
# one of those would start a second, empty server, download several GB into it,
# and leave the operator's own Ollama serving exactly as before.
_OVERLAY_CASES = (
    ("default (neither key)", "POSTGRES_USER=rsync\n", [], 0),
    ("explicit bundled", "POSTGRES_HOST=postgres\nKAFKA_BROKERS=kafka:29092\n", [], 0),
    ("external postgres", "POSTGRES_HOST=db.example.com\n", ["docker-compose.byo-postgres.yml"], 0),
    ("external kafka", "KAFKA_BROKERS=b-1.example.com:9096\n", ["docker-compose.byo-kafka.yml"], 0),
    ("empty value", "POSTGRES_HOST=\n", [], 0),
    (
        "internal llm",
        "LLM_PROVIDER=ollama\nOLLAMA_URL=http://ollama:11434\n",
        ["docker-compose.ollama.yml"],
        1,
    ),
    (
        "internal llm, url unset by hand",
        "LLM_PROVIDER=ollama\nOLLAMA_URL=\n",
        ["docker-compose.ollama.yml"],
        1,
    ),
    (
        "operator's own ollama on the host",
        "LLM_PROVIDER=ollama\nOLLAMA_URL=http://host.docker.internal:11434\n",
        [],
        0,
    ),
    (
        "operator's own ollama on another machine",
        "LLM_PROVIDER=ollama\nOLLAMA_URL=http://203.0.113.10:11434\n",
        [],
        0,
    ),
    # The condition matches `//ollama:` -- a whole word, scheme separator and
    # port colon included -- so a host that merely ends in the same characters
    # is somebody else's server and gets no overlay.
    (
        "a host whose name ends in ollama",
        "LLM_PROVIDER=ollama\nOLLAMA_URL=http://myollama:11434\n",
        [],
        0,
    ),
    # A hand-edited .env can delete the line rather than blank it. Both reach
    # the same `-z` branch, but by different routes: a grep that matches
    # nothing versus a grep whose match has an empty value.
    (
        "internal llm, url line deleted by hand",
        "LLM_PROVIDER=ollama\n",
        ["docker-compose.ollama.yml"],
        1,
    ),
    ("cloud provider", "LLM_PROVIDER=openai\nOPENAI_API_KEY=sk-FAKEPLACEHOLDER\n", [], 0),
    # What install.sh writes for "3) None": no key, and an OLLAMA_URL left empty so
    # a later switch to LLM_PROVIDER=ollama bundles the overlay. Until that switch,
    # the empty URL must not be read as a request for the bundled server.
    ("no llm", "LLM_PROVIDER=none\nLLM_MODEL=\nOLLAMA_URL=\nOPENAI_API_KEY=\n", [], 0),
    (
        "internal llm beside an external postgres",
        "LLM_PROVIDER=ollama\nOLLAMA_URL=http://ollama:11434\nPOSTGRES_HOST=db.example.com\n",
        ["docker-compose.byo-postgres.yml", "docker-compose.ollama.yml"],
        1,
    ),
)

_ALL_OVERLAYS = (
    "docker-compose.byo-postgres.yml",
    "docker-compose.byo-kafka.yml",
    "docker-compose.ollama.yml",
)


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")
@pytest.mark.parametrize("label,env_body,expect,bundled", _OVERLAY_CASES)
def test_installer_layers_exactly_the_overlays_the_env_asks_for(
    tmp_path, label, env_body, expect, bundled
):
    """install.sh runs under `set -o pipefail`, where a no-match grep is fatal.

    Neither POSTGRES_HOST nor KAFKA_BROKERS appears in a default .env, so a bare
    `grep | tail | cut` aborts the installer on the single most common path --
    every standard install -- while the BYO paths this function exists for are
    the only ones that survive. Exactly inverted from what a smoke test would
    catch. LLM_PROVIDER and OLLAMA_URL are read the same way and carry the same
    hazard, plus one of their own: the two BYO overlays layer when a value
    DIFFERS from the bundled default, while this one layers when a value
    MATCHES. Getting it wrong therefore starts an extra Ollama and downloads
    several gigabytes into it, rather than merely leaving a service out.
    """
    harness = _overlay_harness(tmp_path)
    (tmp_path / ".env").write_text(env_body)
    out = subprocess.run(["bash", str(harness)], capture_output=True, text=True)
    assert out.returncode == 0, f"[{label}] build_compose_args exited {out.returncode}: {out.stderr}"

    args_line = [ln for ln in out.stdout.splitlines() if ln.startswith("ARGS ")]
    assert args_line, f"[{label}] harness printed no ARGS line: {out.stdout}"
    got = args_line[0][len("ARGS "):].split()
    assert "docker-compose.quickstart.yml" in " ".join(got), f"[{label}] base file dropped: {got}"
    for overlay in _ALL_OVERLAYS:
        layered = any(overlay in g for g in got)
        assert layered == (overlay in expect), (
            f"[{label}] expected {overlay} layered={overlay in expect}, got {layered}: {got}"
        )
    assert f"BUNDLED {bundled}" in out.stdout, (
        f"[{label}] expected OLLAMA_BUNDLED={bundled}; start_stack reads this flag to warn "
        f"that the first `up` blocks on a multi-gigabyte download:\n{out.stdout}"
    )


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")
def test_a_small_machine_is_warned_before_it_downloads_a_model(tmp_path):
    """Three outcomes, not two: a reading we could not take must not certify the
    machine. 0 means "this platform's RAM is unreadable", never "no RAM"."""
    floor = _install_sh_int("MIN_RAM_GB_WITH_LLM")
    for stack_floor in ("MIN_RAM_GB_BATCH", "MIN_RAM_GB_CDC"):
        assert floor > _install_sh_int(stack_floor), (
            "the bundled model is resident ON TOP of the stack, so its floor must "
            f"be higher than {stack_floor} -- the stack's own floor, whichever "
            "profile set the operator chose"
        )
    env_body = "LLM_PROVIDER=ollama\nOLLAMA_URL=http://ollama:11434\n"
    for label, ram, expect_warning in (
        ("ample", floor + 4, False),
        ("too small", floor - 4, True),
        ("unreadable", 0, True),
    ):
        harness = _overlay_harness(tmp_path, detected_ram_gb=ram)
        (tmp_path / ".env").write_text(env_body)
        out = subprocess.run(["bash", str(harness)], capture_output=True, text=True)
        assert out.returncode == 0, f"[{label}] exited {out.returncode}: {out.stderr}"
        warned = any(
            ln.startswith("WARN") and str(floor) in ln for ln in out.stdout.splitlines()
        )
        assert warned == expect_warning, (
            f"[{label}] RAM={ram}GB against a {floor}GB floor: expected "
            f"warning={expect_warning}, got {warned}:\n{out.stdout}"
        )


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")
def test_the_installer_leaves_behind_a_helper_that_repeats_its_own_f_set(tmp_path):
    """The `-f` set is computed, and for a long time it was computed only inside
    install.sh. An operator who edited .env and then ran a bare `docker compose
    up -d` in the install dir got the quickstart file ALONE -- no overlay, and no
    error either, just a stack quietly missing whatever the overlays add.
    """
    harness = _overlay_harness(tmp_path)
    (tmp_path / ".env").write_text(
        "LLM_PROVIDER=ollama\nOLLAMA_URL=http://ollama:11434\nPOSTGRES_HOST=db.example.com\n"
    )
    out = subprocess.run(["bash", str(harness)], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr

    helper = tmp_path / "compose.sh"
    assert helper.exists(), f"no compose.sh written:\n{out.stdout}"
    assert os.access(helper, os.X_OK), "compose.sh is not executable"

    syntax = subprocess.run(["bash", "-n", str(helper)], capture_output=True, text=True)
    assert syntax.returncode == 0, f"compose.sh does not parse:\n{syntax.stderr}"

    text = helper.read_text()
    exec_lines = [ln for ln in text.splitlines() if ln.startswith("exec docker compose")]
    assert len(exec_lines) == 1, f"expected exactly one exec line:\n{text}"
    line = exec_lines[0]
    for overlay in ("docker-compose.quickstart.yml", "docker-compose.byo-postgres.yml", "docker-compose.ollama.yml"):
        assert overlay in line, f"compose.sh dropped {overlay}:\n{line}"
    assert "docker-compose.byo-kafka.yml" not in line, f"compose.sh invented an overlay:\n{line}"
    assert "--env-file" in line and line.rstrip().endswith('"$@"'), (
        f"compose.sh must forward its own arguments and name the .env:\n{line}"
    )

def test_a_service_an_overlay_adds_is_reachable_from_the_default_up():
    """An added one-shot nothing depends on never runs, and says nothing about it.

    `docker compose up` starts the dependency closure of the services it is
    asked for. A bootstrap job that no service names is therefore inert on the
    one path it exists to protect, while `config` still renders it and every
    static check that merely looks for the service still passes.
    """
    checked = 0
    orphans = []
    for path in overlay_paths():
        introduced = added_services(path)
        if not introduced:
            continue
        merged = dict(_load(BASE)["services"])
        merged.update(_load(path)["services"])
        for name in sorted(introduced):
            checked += 1
            depended_on = [
                s for s, b in merged.items()
                if name in ((b or {}).get("depends_on") or {})
            ]
            if not depended_on:
                orphans.append(f"{os.path.basename(path)} adds {name}, nothing depends_on it")
    assert checked > 0, "no overlay added a service -- the check was vacuous"
    assert not orphans, "\n  ".join(["services added by an overlay that nothing waits for:"] + orphans)


def test_a_completion_gate_is_never_marked_optional():
    """`required: false` turns a FAILED completion gate into a warning.

    Measured on Compose 2.x rather than read from the docs: with the flag, a
    dependency that exits non-zero logs `optional dependency "..." didn't
    complete successfully: exit 7`, the dependent starts anyway and `up` still
    exits 0. Without it, `up` fails and the dependent never runs. That flag is
    right for a service a profile can REMOVE from the project (see the parked
    services above) and wrong for a bootstrap job that is always present: it
    silently restores the very startup the gate exists to prevent.
    """
    checked = 0
    optional = []
    for path in [BASE] + overlay_paths():
        for svc, body in (_load(path).get("services") or {}).items():
            deps = (body or {}).get("depends_on") or {}
            if not isinstance(deps, dict):
                continue
            for dep, cond in deps.items():
                if not isinstance(cond, dict):
                    continue
                if cond.get("condition") != "service_completed_successfully":
                    continue
                checked += 1
                if cond.get("required") is False:
                    optional.append(f"{os.path.basename(path)}: {svc} -> {dep}")
    assert checked > 0, "no completion gate found anywhere -- the check was vacuous"
    assert not optional, (
        "these depends_on entries wait for a service to COMPLETE but mark it "
        "optional, so a failure degrades to a warning and the dependent starts "
        "anyway:\n  " + "\n  ".join(optional)
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
        "tool_generator/service.py:869-928), so its absence costs a "
        "documentation lookup, not a run"
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

    harness = _overlay_harness(tmp_path)
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

    # The resolved set has to survive the installer exiting. An operator who
    # edits .env and re-runs compose by hand gets whatever compose.sh carries,
    # and an exported COMPOSE_PROFILES would have died with the process.
    helper = (tmp_path / "compose.sh").read_text()
    exec_lines = [ln for ln in helper.splitlines() if ln.startswith("exec docker compose")]
    assert len(exec_lines) == 1, f"[{label}] expected one exec line:\n{helper}"
    for name in expect:
        assert f"--profile {name}" in exec_lines[0], (
            f"[{label}] compose.sh dropped --profile {name}:\n{exec_lines[0]}"
        )
    for name in absent:
        assert f"--profile {name}" not in exec_lines[0], (
            f"[{label}] compose.sh invented --profile {name}:\n{exec_lines[0]}"
        )


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
    the 18 unprofiled services 6GB has always sized, and warning them about a JVM
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
