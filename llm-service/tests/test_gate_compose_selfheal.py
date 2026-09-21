"""The e2e gate recovers the shared CI stack from a run cancelled mid-recreate.

Compose v2 recreates a container in three steps: it creates the new one under
the temporary name `<old id[:12]>_<name>`, stops and removes the old one, then
renames the new one. A CI run cancelled while it was rebuilding images left that
temporary container behind, and the next run's `up -d` died on "Conflict. The
container name "/<old id[:12]>_<name>" is already in use" before any test ran.
The gate only knew how to recover from a stale network, so the stack had to be
cleared by hand.

These tests run the gate's own functions, cut out of e2e/run_gate.sh, with
`docker` and compose faked: the fake daemon keeps a container table in a file,
and the fake compose fails the way the real one does while a temporary-named
container of its project exists. Nothing here can reach a real docker: the PATH
holds only the fakes and a handful of text tools.
"""

import os
import pathlib
import re
import shutil
import stat
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
GATE = REPO / "e2e" / "run_gate.sh"
# The CI runner's /bin/bash is 3.2, the oldest the gate has to run on.
BASH = "/bin/bash" if os.path.exists("/bin/bash") else shutil.which("bash")

FUNCTIONS = (
    "dc_project",
    "reclaim_orphan_recreate_containers",
    "reclaim_conflicting_recreate_containers",
    "compose_up_selfheal",
)
TOOLS = ("grep", "sed", "sort", "awk", "cat", "mv", "rm", "head", "tr")

PROJECT = "rsync-ci"
LEFTOVER = "0123456789ab_rsync-ci-api-gateway"

FAKE_DOCKER = r"""#!/bin/bash
S="$FAKE_STATE"
echo "$*" >> "$S/docker.log"
last="${@: -1}"
case "$1" in
  ps)
    proj=""
    for a in "$@"; do
      case "$a" in label=com.docker.compose.project=*) proj="${a#label=com.docker.compose.project=}" ;; esac
    done
    while read -r id name state p; do
      [ -n "$id" ] && [ "$p" = "$proj" ] && echo "$id $name $state"
    done < "$S/containers"
    ;;
  inspect)
    awk -v t="$last" '$1==t || $2==t {print $4; found=1} END {exit !found}' "$S/containers" \
      || { echo "Error: No such object: $last" >&2; exit 1; }
    ;;
  rm)
    awk -v t="$last" '$1==t || $2==t {found=1} END {exit !found}' "$S/containers" \
      || { echo "Error response from daemon: No such container: $last" >&2; exit 1; }
    awk -v t="$last" '$1!=t && $2!=t' "$S/containers" > "$S/containers.new"
    mv "$S/containers.new" "$S/containers"
    ;;
  *) echo "fake docker: unexpected call: $*" >&2; exit 2 ;;
esac
"""

# Fails like compose while a temporary-named container of its project exists;
# `fail_msg` and `stale_network` script the other failures.
FAKE_COMPOSE = r"""#!/bin/bash
S="$FAKE_STATE"
proj="$1"; shift
echo "$proj $*" >> "$S/compose.log"
if [ -f "$S/fail_msg" ]; then cat "$S/fail_msg" >&2; exit 1; fi
while read -r id name state p; do
  if [ "$p" = "$proj" ] && echo "$name" | grep -qE '^[0-9a-f]{12}_'; then
    echo "Error response from daemon: Conflict. The container name \"/$name\" is already in use by container \"${id}ffffffffffff\". You have to remove (or rename) that container to be able to reuse that name." >&2
    exit 1
  fi
done < "$S/containers"
if [ -f "$S/stale_network" ]; then
  case " $* " in
    *" --force-recreate "*) ;;
    *) echo "Error response from daemon: failed to set up container networking: network 0123456789abcdef0123 not found" >&2; exit 1 ;;
  esac
fi
echo " Container $proj  Started"
"""


def _extract(text: str, name: str) -> str:
    m = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", text, re.S | re.M)
    assert m, f"{name}() not found in {GATE}"
    return m.group(0)


@pytest.fixture
def gate(tmp_path):
    text = GATE.read_text()
    lib = tmp_path / "selfheal_lib.sh"
    lib.write_text("\n".join(_extract(text, f) for f in FUNCTIONS))

    bindir = tmp_path / "bin"
    bindir.mkdir()
    for tool in TOOLS:
        real = shutil.which(tool)
        assert real, f"{tool} missing on this host"
        (bindir / tool).symlink_to(real)
    for fname, body in (("docker", FAKE_DOCKER), ("fake-compose", FAKE_COMPOSE)):
        p = bindir / fname
        p.write_text(body)
        p.chmod(p.stat().st_mode | stat.S_IXUSR)

    state = tmp_path / "state"
    state.mkdir()

    class Gate:
        def containers(self, rows):
            (state / "containers").write_text("".join(" ".join(r) + "\n" for r in rows))

        def remaining(self):
            return [ln.split()[1] for ln in (state / "containers").read_text().splitlines() if ln.strip()]

        def calls(self, which):
            f = state / f"{which}.log"
            return f.read_text().splitlines() if f.exists() else []

        def script(self, name, body=""):
            (state / name).write_text(body)

        def run(self, snippet):
            harness = (
                "set -uo pipefail\n"
                "warn() { printf 'WARN: %s\\n' \"$*\" >&2; }\n"
                f"source '{lib}'\n"
                f"MAIN_PROJECT={PROJECT}; E2E_PROJECT={PROJECT}-e2e; MCP_PROJECT={PROJECT}-mcp\n"
                'dc_main() { fake-compose "${MAIN_PROJECT}" "$@"; }\n'
                'dc_e2e()  { fake-compose "${E2E_PROJECT}" "$@"; }\n'
                'dc_mcp()  { fake-compose "${MCP_PROJECT}" "$@"; }\n'
                f"{snippet}\n"
            )
            env = {"PATH": str(bindir), "FAKE_STATE": str(state), "HOME": str(tmp_path)}
            return subprocess.run([BASH, "-c", harness], env=env, capture_output=True, text=True, timeout=30)

    return Gate()


@pytest.mark.parametrize(
    "wrapper,project",
    [("dc_main", PROJECT), ("dc_e2e", f"{PROJECT}-e2e"), ("dc_mcp", f"{PROJECT}-mcp")],
)
def test_leftover_of_an_interrupted_recreate_is_removed_before_up(gate, wrapper, project):
    leftover = f"0123456789ab_{project}-svc"
    gate.containers([
        ("0123456789abcdef", f"{project}-svc", "exited", project),  # the old one, never removed
        ("fedcba987654", leftover, "created", project),
    ])

    r = gate.run(f"compose_up_selfheal {wrapper} --build svc")

    assert r.returncode == 0, r.stderr
    assert gate.remaining() == [f"{project}-svc"]
    # Removed BEFORE the first `up`: compose never saw the conflict.
    assert gate.calls("compose") == [f"{project} up -d --build svc"]
    assert "interrupted compose recreate" in r.stderr


def test_sweep_touches_only_stopped_temporary_names_in_its_own_project(gate):
    gate.containers([
        ("aaaaaaaaaaa1", "0123456789ab_rsync-ci-postgres", "created", PROJECT),
        ("aaaaaaaaaaa2", "0123456789ac_rsync-ci-redis", "exited", PROJECT),
        ("aaaaaaaaaaa3", "0123456789ad_rsync-ci-kafka", "dead", PROJECT),
        ("bbbbbbbbbbb1", "0123456789ae_rsync-ci-minio", "running", PROJECT),
        ("bbbbbbbbbbb2", "0123456789af_rsync-ci-planner", "restarting", PROJECT),
        ("bbbbbbbbbbb3", "rsync-ci-api-gateway", "exited", PROJECT),
        ("bbbbbbbbbbb4", "rsync-ci-orchestrator", "created", PROJECT),
        ("bbbbbbbbbbb5", "0123456789b0_rsync-ai-postgres", "exited", "rsync-ai"),
        ("bbbbbbbbbbb6", "0123456789b1_rsync-ci-mcp-mysql", "created", f"{PROJECT}-mcp"),
    ])

    r = gate.run(f"reclaim_orphan_recreate_containers {PROJECT}")

    assert r.returncode == 0, r.stderr
    assert gate.remaining() == [
        "0123456789ae_rsync-ci-minio",
        "0123456789af_rsync-ci-planner",
        "rsync-ci-api-gateway",
        "rsync-ci-orchestrator",
        "0123456789b0_rsync-ai-postgres",
        "0123456789b1_rsync-ci-mcp-mysql",
    ]
    assert sorted(c for c in gate.calls("docker") if c.startswith("rm ")) == [
        "rm -f aaaaaaaaaaa1", "rm -f aaaaaaaaaaa2", "rm -f aaaaaaaaaaa3",
    ]


def test_sweep_with_no_project_does_nothing(gate):
    gate.containers([("aaaaaaaaaaa1", "0123456789ab_rsync-ci-postgres", "created", "")])

    r = gate.run("reclaim_orphan_recreate_containers ''")

    assert r.returncode == 0, r.stderr
    assert gate.calls("docker") == []
    assert gate.remaining() == ["0123456789ab_rsync-ci-postgres"]


def test_name_conflict_removes_the_leftover_and_retries_once(gate):
    # Running, so the sweep leaves it; compose still trips over its name.
    gate.containers([("fedcba987654", LEFTOVER, "running", PROJECT)])

    r = gate.run("compose_up_selfheal dc_main api-gateway")

    assert r.returncode == 0, r.stderr
    assert gate.remaining() == []
    assert gate.calls("compose") == [f"{PROJECT} up -d api-gateway"] * 2
    assert f"rm -f {LEFTOVER}" in gate.calls("docker")
    assert "is already in use" in r.stdout  # the first failure is still shown


def test_name_conflict_on_another_projects_container_fails_without_retry(gate):
    other = "0123456789ab_rsync-ai-postgres"
    gate.containers([("fedcba987654", other, "running", "rsync-ai")])
    gate.script(
        "fail_msg",
        f'Error response from daemon: Conflict. The container name "/{other}" is already in use '
        'by container "fedcba987654ffff". You have to remove (or rename) that container to be able to reuse that name.\n',
    )

    r = gate.run("compose_up_selfheal dc_main postgres")

    assert r.returncode != 0
    assert gate.remaining() == [other]
    assert not [c for c in gate.calls("docker") if c.startswith("rm ")]
    assert gate.calls("compose") == [f"{PROJECT} up -d postgres"]
    assert "belongs to project 'rsync-ai'" in r.stderr


def test_name_conflict_on_a_final_name_is_not_ours_to_fix(gate):
    gate.containers([("fedcba987654", "rsync-ci-postgres", "exited", PROJECT)])
    gate.script(
        "fail_msg",
        'Error response from daemon: Conflict. The container name "/rsync-ci-postgres" is already in use '
        'by container "fedcba987654ffff". You have to remove (or rename) that container to be able to reuse that name.\n',
    )

    r = gate.run("compose_up_selfheal dc_main postgres")

    assert r.returncode != 0
    assert gate.remaining() == ["rsync-ci-postgres"]
    assert gate.calls("compose") == [f"{PROJECT} up -d postgres"]


def test_unrelated_failure_propagates_without_retry(gate):
    gate.containers([("fedcba987654", "rsync-ci-postgres", "exited", PROJECT)])
    gate.script("fail_msg", "Error response from daemon: pull access denied for rsync-ci-api-gateway\n")

    r = gate.run("compose_up_selfheal dc_main api-gateway")

    assert r.returncode != 0
    assert "pull access denied" in r.stdout
    assert gate.calls("compose") == [f"{PROJECT} up -d api-gateway"]
    assert not [c for c in gate.calls("docker") if c.startswith("rm ")]


def test_stale_network_still_retries_with_force_recreate(gate):
    gate.containers([("fedcba987654", "rsync-ci-postgres", "exited", PROJECT)])
    gate.script("stale_network")

    r = gate.run("compose_up_selfheal dc_main planner")

    assert r.returncode == 0, r.stderr
    assert gate.calls("compose") == [
        f"{PROJECT} up -d planner",
        f"{PROJECT} up -d --force-recreate --always-recreate-deps planner",
    ]
    assert gate.remaining() == ["rsync-ci-postgres"]
