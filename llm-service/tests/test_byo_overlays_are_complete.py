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
        "COMPOSE_ARGS=(); COMPOSE_CMD=\"\"\ninfo(){ :; }\n"
        + "".join(body)
        + '\nbuild_compose_args\necho "${COMPOSE_ARGS[*]}"\n'
    )

    for label, env_body, expect in (
        ("default (neither key)", "POSTGRES_USER=rsync\n", []),
        ("explicit bundled", "POSTGRES_HOST=postgres\nKAFKA_BROKERS=kafka:29092\n", []),
        ("external postgres", "POSTGRES_HOST=db.example.com\n", ["docker-compose.byo-postgres.yml"]),
        ("external kafka", "KAFKA_BROKERS=b-1.example.com:9096\n", ["docker-compose.byo-kafka.yml"]),
        ("empty value", "POSTGRES_HOST=\n", []),
    ):
        (tmp_path / ".env").write_text(env_body)
        out = subprocess.run(["bash", str(harness)], capture_output=True, text=True)
        assert out.returncode == 0, f"[{label}] build_compose_args exited {out.returncode}: {out.stderr}"
        got = out.stdout.split()
        assert "docker-compose.quickstart.yml" in " ".join(got), f"[{label}] base file dropped: {got}"
        for overlay in ("docker-compose.byo-postgres.yml", "docker-compose.byo-kafka.yml"):
            layered = any(overlay in g for g in got)
            assert layered == (overlay in expect), (
                f"[{label}] expected {overlay} layered={overlay in expect}, got {layered}: {got}"
            )
