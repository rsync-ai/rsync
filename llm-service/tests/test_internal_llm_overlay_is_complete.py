"""Bundling an Ollama with no model in it is a silent failure, not a loud one.

`docker-compose.ollama.yml` exists so the stack can run fully offline: no OpenAI
key, no Ollama on the operator's host. Starting a server is the easy half. The
half that used to be missing is getting a model INTO it -- and the only thing
that ever asked for that was a comment in the file's own header:

    docker exec rsync-ollama ollama pull qwen2.5:7b

Skipping it does not fail. Ollama answers every request with `model "<name>" not
found, try pulling it first`, the Python tier surfaces that as a failed
generation, and every container around it stays `running` with `/ready` 200.
There is nothing to point at. Same defect class as the manual CREATE DATABASE
this release removed from the Postgres paths: a step nothing executable knew
about, standing between a green stack and a working one.

The `ollama-pull` job closes it, and the properties below are what make it
close: it has to run, it has to finish before anything asks for a model, and a
failure has to stop the start rather than be logged past.
"""

import os
import re
import shutil
import subprocess

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
OVERLAY = os.path.join(REPO_ROOT, "docker-compose.ollama.yml")
BASE = os.path.join(REPO_ROOT, "docker-compose.quickstart.yml")
INSTALL_SH = os.path.join(REPO_ROOT, "install.sh")

PULL = "ollama-pull"
SERVER = "ollama"
# The three Python services that resolve a model name and ask Ollama for it.
ASKERS = ("llm-service", "tool-generator", "planner")

FAKE_ENV = {
    "ENCRYPTION_KEY": "FAKEPLACEHOLDER",
    "JWT_SECRET": "FAKEPLACEHOLDER",
    "MINIO_ACCESS_KEY": "FAKEPLACEHOLDER",
    "MINIO_SECRET_KEY": "FAKEPLACEHOLDER",
    "POSTGRES_PASSWORD": "FAKEPLACEHOLDER",
    "REDIS_PASSWORD": "FAKEPLACEHOLDER",
}


class _ComposeLoader(yaml.SafeLoader):
    """Compose's merge tags (`!override`, `!reset`) are not YAML the SafeLoader knows."""


_ComposeLoader.add_multi_constructor("!", lambda loader, suffix, node: None)


def _overlay():
    with open(OVERLAY) as fh:
        return yaml.load(fh, Loader=_ComposeLoader)


def _services():
    spec = _overlay()["services"]
    assert len(spec) >= 5, f"suspiciously small overlay ({list(spec)}) -- refusing to trust it"
    return spec


# ── 1. The pull job runs, and runs once ──────────────────────────────────────


def test_the_overlay_starts_a_server_and_a_puller():
    services = _services()
    assert SERVER in services, "the overlay's whole point is starting an Ollama"
    assert PULL in services, (
        "an Ollama with no model answers every prompt with 'model not found' while "
        "reading healthy -- something has to put a model in it"
    )


def test_the_puller_is_a_run_once_job():
    """`restart: always` on a job gated by service_completed_successfully is a
    deadlock: Compose restarts it forever and the gate never opens."""
    assert _services()[PULL].get("restart") == "no", (
        f"{PULL} must be restart: \"no\" -- it is a job, not a server"
    )


def test_the_puller_waits_for_a_server_that_answers():
    """`ollama list` is a round trip to the server's own API, so it goes green only
    once the HTTP listener accepts. A port check passes earlier and hands the
    puller a connection the server is not ready to serve."""
    services = _services()
    hc = services[SERVER].get("healthcheck") or {}
    assert hc.get("test"), f"{SERVER} has no healthcheck for {PULL} to wait on"
    dep = (services[PULL].get("depends_on") or {}).get(SERVER)
    assert isinstance(dep, dict) and dep.get("condition") == "service_healthy", (
        f"{PULL} must wait for {SERVER} to be healthy, got {dep!r}"
    )


def test_the_puller_does_not_write_the_model_volume_itself():
    """It holds no model bytes: both its commands are HTTP calls to the server,
    which downloads into its own volume. A second writer would race the server."""
    assert not _services()[PULL].get("volumes"), (
        f"{PULL} must mount nothing; the server owns ollama_models"
    )


def test_the_puller_points_its_cli_at_the_server_container():
    """The image bakes in OLLAMA_HOST=0.0.0.0:11434, which is a LISTEN address.
    Left alone, the CLI looks for a server inside its own container, finds
    nothing, and reports a connection error that reads like the server is down
    when it is up and healthy next door."""
    env = _services()[PULL].get("environment") or {}
    assert env.get("OLLAMA_HOST") == f"http://{SERVER}:11434", (
        f"{PULL} OLLAMA_HOST is {env.get('OLLAMA_HOST')!r}"
    )


# ── 2. Nothing asks for a model before one exists ────────────────────────────


def test_every_service_that_asks_for_a_model_waits_for_the_pull():
    services = _services()
    missing = []
    for name in ASKERS:
        dep = (services.get(name, {}).get("depends_on") or {}).get(PULL)
        if not isinstance(dep, dict) or dep.get("condition") != "service_completed_successfully":
            missing.append(f"{name} -> {dep!r}")
    assert not missing, (
        "these services resolve an Ollama model but do not wait for it to be "
        "downloaded, so a slow pull means 'model not found' on the first "
        f"prompt:\n  " + "\n  ".join(missing)
    )


def test_no_completion_gate_in_this_overlay_is_optional():
    """`required: false` reads like the house style from the byo-* overlays and
    here it would quietly undo the whole file.

    Measured on Compose 2.x: a FAILED completion dependency marked `required:
    false` degrades to `optional dependency "..." didn't complete successfully`,
    the dependent starts ANYWAY, and `up` still exits 0 -- which is exactly the
    start-on-an-empty-Ollama this overlay exists to prevent. Without the flag the
    same failure ends the `up` with a non-zero exit and the dependent never runs.

    The flag is for a service that may be absent from the project. ollama-pull is
    defined right here and never is.
    """
    offenders = []
    checked = 0
    for name, body in _services().items():
        for dep_name, cond in ((body or {}).get("depends_on") or {}).items():
            if not isinstance(cond, dict):
                continue
            if cond.get("condition") != "service_completed_successfully":
                continue
            checked += 1
            if cond.get("required") is not None:
                offenders.append(f"{name} -> {dep_name}: required={cond['required']}")
    assert checked > 0, "no completion gates found at all -- the check was vacuous"
    assert not offenders, (
        "a completion gate has to be able to fail:\n  " + "\n  ".join(offenders)
    )


def test_one_model_name_reaches_the_puller_and_every_asker():
    """The puller downloads exactly one model. Every service that later asks for
    one has to name the same model, or the download was for nothing."""
    services = _services()
    values = {}
    for name in (PULL,) + ASKERS:
        env = services[name].get("environment") or {}
        assert "OLLAMA_MODEL" in env, f"{name} gets no OLLAMA_MODEL"
        values[name] = env["OLLAMA_MODEL"]
    assert len(set(values.values())) == 1, f"services disagree on the model: {values}"


def test_every_asker_is_pointed_at_the_bundled_server():
    services = _services()
    for name in ASKERS:
        env = services[name].get("environment") or {}
        assert env.get("OLLAMA_URL") == f"http://{SERVER}:11434", (
            f"{name} OLLAMA_URL is {env.get('OLLAMA_URL')!r}; without this it keeps "
            "pointing at whatever the .env named, usually the operator's own host"
        )


# ── 3. The pull script itself ────────────────────────────────────────────────


def _pull_command():
    cmd = _services()[PULL].get("command")
    assert isinstance(cmd, list), (
        "command must be a LIST. Compose splits a bare string into argv, which "
        f"leaves `sh -c` running only the first word. Got {type(cmd).__name__}"
    )
    assert len(cmd) == 1, (
        f"`sh -c` takes one script argument; this passes {len(cmd)}, so everything "
        "after the first becomes $0, $1, ... and is never executed"
    )
    return cmd[0]


def test_the_puller_runs_a_shell_and_gets_exactly_one_script():
    assert _services()[PULL].get("entrypoint") == ["/bin/sh", "-c"]
    _pull_command()


@pytest.mark.skipif(shutil.which("sh") is None, reason="no sh")
def test_the_pull_script_parses():
    """A syntax error here surfaces as a stack that will not start, several GB
    into a download, in a container the operator has no reason to be watching."""
    script = _pull_command().replace("$$", "$")
    assert len(script) > 200, f"suspiciously short script ({len(script)} bytes) -- refusing to trust a parse of it"
    out = subprocess.run(["sh", "-n"], input=script, capture_output=True, text=True)
    assert out.returncode == 0, f"pull script does not parse:\n{out.stderr}"


def test_a_failed_pull_reports_ollamas_own_words():
    """A typo in the model name, a registry that is down, and a full disk all end
    this loop identically from outside. Discarding the error leaves the operator
    with 'it did not work' and nothing to act on."""
    script = _pull_command()
    assert re.search(r'ERR="\$\$\(ollama pull', script), (
        "the pull's stderr must be captured, not discarded"
    )
    assert "$$ERR" in script, "the captured error is never printed back"
    assert "exit 1" in script, (
        "a pull that never succeeds must fail the job; exiting 0 opens the "
        "completion gate onto an empty server"
    )


def test_the_pull_is_skipped_when_the_model_is_already_there():
    """The volume survives `down`. Re-downloading several GB on every `up` would
    make the overlay unusable for exactly the offline operator it is for."""
    assert "ollama show" in _pull_command(), "no pre-check before the download"


# ── 4. The overlay layers onto the real base file ────────────────────────────


def test_the_overlay_adds_no_new_required_variable():
    """One unsatisfied `${VAR:?}` aborts interpolation for the WHOLE merged
    config, so an overlay that introduces one breaks every path that layers it
    -- and only on a fresh host, which is why CI and every laptop stay green."""
    with open(OVERLAY) as fh:
        found = set(re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*):\?", fh.read()))
    assert not found, (
        f"this overlay demands {sorted(found)}; every operator layering it now "
        "needs those set or nothing in the project starts"
    )


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
def test_layering_the_overlay_adds_the_llm_and_keeps_what_was_there(tmp_path):
    """`depends_on` MERGES across -f files rather than replacing. If it replaced,
    adding the pull gate to tool-generator would silently drop its existing waits
    on connector-deployer and connector-seed."""
    env_file = tmp_path / "env"
    env_file.write_text("".join(f"{k}={v}\n" for k, v in FAKE_ENV.items()))

    def render(*extra):
        cmd = ["docker", "compose", "--env-file", str(env_file), "-f", BASE]
        for e in extra:
            cmd += ["-f", e]
        cmd += ["--profile", "cdc", "--profile", "generate", "config"]
        out = subprocess.run(cmd, capture_output=True, text=True, cwd=REPO_ROOT)
        assert out.returncode == 0, f"`docker compose config` failed:\n{out.stderr}"
        return yaml.load(out.stdout, Loader=_ComposeLoader)["services"]

    base = render()
    assert len(base) > 10, f"suspiciously small base render ({len(base)}) -- refusing to trust a diff"
    merged = render(OVERLAY)

    assert set(merged) - set(base) == {SERVER, PULL}, (
        f"the overlay should add exactly {SERVER} and {PULL}; it added {set(merged) - set(base)}"
    )
    assert not set(base) - set(merged), f"the overlay removed {set(base) - set(merged)}"

    for name in ASKERS:
        before = set((base[name].get("depends_on") or {}))
        after = set((merged[name].get("depends_on") or {}))
        assert after == before | {PULL}, (
            f"{name} depends_on went {sorted(before)} -> {sorted(after)}; a REPLACE "
            "here would drop waits the base file needs"
        )

    # The anchor resolves to a real model name, not to an empty string.
    model = merged[PULL]["environment"]["OLLAMA_MODEL"]
    assert model and not model.startswith("$"), f"unresolved model name: {model!r}"
    for name in ASKERS:
        assert merged[name]["environment"]["OLLAMA_MODEL"] == model


# ── 5. The installer can actually get the file ───────────────────────────────


def _install_sh():
    with open(INSTALL_SH) as fh:
        return fh.read()


def test_the_installer_downloads_the_overlay():
    """install.sh downloads compose files one by one with no repo checkout. A
    file it never fetches cannot be layered, and build_compose_args would hand
    `docker compose` a `-f` naming a path that is not on disk."""
    text = _install_sh()
    name = os.path.basename(OVERLAY)
    assert f'OLLAMA_FILE="{name}"' in text, f"install.sh does not name {name}"
    fetches = [ln for ln in text.splitlines() if "fetch " in ln and "OLLAMA_FILE" in ln]
    assert len(fetches) >= 2, (
        "expected the overlay to be fetched in download_compose AND repaired in "
        f"main(), like the byo-* overlays; found {len(fetches)} fetch line(s)"
    )
