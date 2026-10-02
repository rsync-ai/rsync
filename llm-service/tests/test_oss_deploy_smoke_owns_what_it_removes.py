"""scripts/oss-deploy-smoke.sh removes exactly what it owns -- no more, no less.

Two failures of one class, "teardown decides by NAME, not by who made it":

  * LESS: a run killed before its EXIT trap finished (runner cancel/timeout sends
    SIGKILL) left `rsync-oss-smoke-net` behind. The 2026-09-24 nightly was
    cancelled mid-deploy, and the 2026-09-25 nightly then failed on
    `docker network create` ("already exists"). The next run must reclaim its
    own leftovers before it creates them again.
  * MORE: the trap is armed before preflight and removed `$CONTAINER` and
    `$IMAGE_REF` unconditionally -- names the shared stack uses too. So the
    preflight abort that refuses to touch a shared-stack container went on to
    delete it, and its image, on the way out.

The script runs for real against a stub `docker` on PATH that keeps its state in
files and fails `docker build`, which ends each run just after preflight.
"""

import json
import os
import pathlib
import re
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "oss-deploy-smoke.sh"

NET = "rsync-oss-smoke-net"
LIFECYCLE = "rsync-oss-smoke-lifecycle"
# The seed connector is read from the script, so swapping it cannot leave this test
# naming a connector the smoke no longer deploys (or one the tree no longer has).
CONNECTOR = re.search(r'^CONNECTOR="([^"]+)"$', SCRIPT.read_text(), re.M).group(1)
_CV = json.loads((REPO / f"shared/mcp-connectors/public/{CONNECTOR}/latest.json").read_text())[
    "current_version"
]
JIT = "rsync-ai-{}-v{}-mcp".format(CONNECTOR, _CV.lstrip("v").replace(".", "-"))
IMAGE = f"mcp-{CONNECTOR}:{_CV}"

# containers: "<name> <net>[,<net>...]" per line; networks / images: one name per line.
STUB = r"""#!/usr/bin/env bash
st="$STUB_STATE"
echo "$*" >> "$st/calls"
last="${@: -1}"
case "$1" in
  info) exit 0 ;;
  ps) cut -d' ' -f1 "$st/containers"; exit 0 ;;
  inspect)
    line="$(grep "^$last " "$st/containers")" || exit 1
    for n in $(echo "$line" | cut -d' ' -f2 | tr ',' ' '); do printf '%s ' "$n"; done
    exit 0 ;;
  rm) grep -v "^$last " "$st/containers" > "$st/tmp"; mv "$st/tmp" "$st/containers"; exit 0 ;;
  network)
    if [ "$2" = rm ]; then
      cut -d' ' -f2 "$st/containers" | tr ',' '\n' | grep -qx "$3" && exit 1
      grep -vx "$3" "$st/networks" > "$st/tmp"; mv "$st/tmp" "$st/networks"
    elif [ "$2" = create ]; then
      grep -qx "$3" "$st/networks" && { echo "network with name $3 already exists" >&2; exit 1; }
      echo "$3" >> "$st/networks"
    fi
    exit 0 ;;
  image) [ "$2" = inspect ] && { grep -qx "$3" "$st/images"; exit $?; }; exit 0 ;;
  rmi) grep -vx "$last" "$st/images" > "$st/tmp"; mv "$st/tmp" "$st/images"; exit 0 ;;
  build) exit 1 ;;
esac
exit 0
"""


def _run(tmp_path, containers=(), networks=(), images=()):
    bin_dir, state = tmp_path / "bin", tmp_path / "state"
    bin_dir.mkdir()
    state.mkdir()
    (bin_dir / "docker").write_text(STUB)
    (bin_dir / "docker").chmod(0o755)
    for name, rows in (("containers", containers), ("networks", networks), ("images", images)):
        (state / name).write_text("".join(f"{r}\n" for r in rows))
    (state / "calls").write_text("")
    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "STUB_STATE": str(state)}
    proc = subprocess.run(
        ["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=60
    )
    calls = (state / "calls").read_text().splitlines()
    left = {n: (state / n).read_text().split("\n") for n in ("containers", "networks", "images")}
    return proc, calls, left


def _before_build(calls):
    builds = [i for i, c in enumerate(calls) if c.startswith("build ")]
    assert builds, f"the run never reached `docker build`; calls: {calls}"
    return calls[: builds[0]]


def test_a_killed_runs_leftovers_are_reclaimed_before_they_are_needed(tmp_path):
    proc, calls, _ = _run(
        tmp_path,
        containers=[f"{LIFECYCLE} {NET}", f"{JIT} {NET}"],
        networks=[NET],
    )
    early = _before_build(calls)
    for want in (f"rm -fv {JIT}", f"rm -fv {LIFECYCLE}", f"network rm {NET}"):
        assert want in early, (
            f"`{want}` did not run before the build. A killed earlier run leaves these "
            f"behind, and this run then fails on them (network create: already exists) "
            f"or aborts on its own JIT container.\n{proc.stdout[-2000:]}"
        )


def test_a_shared_stack_container_and_image_survive_the_abort(tmp_path):
    proc, calls, left = _run(
        tmp_path,
        containers=[f"{JIT} rsync-ai-mcp"],
        images=[IMAGE],
    )
    assert proc.returncode != 0 and "aborting to avoid touching a shared-stack container" in proc.stdout
    assert f"rm -fv {JIT}" not in calls and not any(c.startswith("rmi") for c in calls), (
        f"the preflight refused to touch {JIT} (it sits on rsync-ai-mcp), then the EXIT "
        f"trap removed it and/or {IMAGE} anyway. calls: {calls}"
    )
    assert f"{JIT} rsync-ai-mcp" in left["containers"] and IMAGE in left["images"]


@pytest.mark.parametrize("image_was_there", [False, True], ids=["image-built-here", "image-pre-existing"])
def test_the_trap_removes_the_image_only_if_this_run_made_it(tmp_path, image_was_there):
    proc, calls, left = _run(tmp_path, images=[IMAGE] if image_was_there else [])
    assert proc.returncode != 0 and "build failed" in proc.stdout, proc.stdout[-2000:]
    removed = f"rmi -f {IMAGE}" in calls
    assert removed is (not image_was_there), (
        f"image pre-existing={image_was_there}, but the trap "
        f"{'removed' if removed else 'kept'} {IMAGE}. calls: {calls}"
    )
    # The positive control for the ownership gates: this run's own resources still go.
    assert f"network rm {NET}" in calls and f"rm -fv {LIFECYCLE}" in calls
