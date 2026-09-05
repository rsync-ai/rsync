"""Docker-host features must be off in the chart, and forcing them on must fail loudly.

Two features in this platform build and start connector containers through
/var/run/docker.sock: the connector-deployer service, and tool-generator's own
"managed connectors" mode. Neither can work on Kubernetes, and mounting a node's
Docker socket into a pod hands that pod the node -- so this chart mounts none.

`connectorDeployer.enabled` was already off with a fail-fast guard.
`generation.toolGenerator.managedConnectors` was left `true`, inherited from the
compose value, where it is correct. On a cluster it opened onto a drop rather
than a shut door: the build is dispatched as a detached task
(deployment/routes.py:146), so /v1/deploy answers 200 with `building: true`
before docker.from_env() has been attempted. The orchestrator extends its poll on
that flag (mcp/server_manager.go:827-836) and waits out the entire pre-flight
timeout, while the build has already failed with a WARNING and returned False
(docker_builder.py:158). What the operator finally sees is "not reachable --
check `docker compose logs`" on a cluster that has no compose in it, with the
real cause in a tool-generator log line nobody was told to read.

Two layers, matching test_documented_install_commands_render.py:

  1. the static layer runs everywhere, including CI, which has NO helm binary.
  2. the render layer runs `helm template` and asserts the guard actually fires.
     It skips without helm -- and a skip is not a pass, which is why the whole
     property is also pinned statically above it.

The static layer is the load-bearing one precisely because CI cannot run the
other.
"""

import pathlib
import shutil
import subprocess

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
CHART = REPO / "deploy" / "helm" / "rsync-ai"
VALUES = CHART / "values.yaml"
VALIDATE = CHART / "templates" / "validate.yaml"

# Every toggle that would need a Docker socket, and the guard that must refuse it.
# Adding a third such feature without adding a row here is the drift this catches.
DOCKER_HOST_TOGGLES = [
    ("connectorDeployer.enabled", "connectorDeployer.enabled=true"),
    (
        "generation.toolGenerator.managedConnectors",
        "generation.toolGenerator.managedConnectors=true",
    ),
]

# The documented install command's flags, so a render reaches the guards instead
# of stopping at an unrelated required-value check. Fakes throughout.
RENDER_FLAGS = [
    "--set", "secrets.jwtSecret=FAKEPLACEHOLDER",
    "--set", "secrets.encryptionKey=FAKEPLACEHOLDER",
    "--set", "secrets.postgresPassword=FAKEPLACEHOLDER",
    "--set", "secrets.minioAccessKey=FAKEPLACEHOLDER",
    "--set", "secrets.minioSecretKey=FAKEPLACEHOLDER",
    "--set", "frontend.publicUrl=https://app.example.com",
    "--set", "frontend.apiUrl=https://api.example.com",
]


def _dig(mapping, dotted):
    """Resolve 'a.b.c' against nested dicts, or raise with the path that broke."""
    node = mapping
    for part in dotted.split("."):
        assert isinstance(node, dict) and part in node, (
            f"values.yaml has no key '{dotted}' (stopped at '{part}'). If the key "
            "was renamed, retarget this test -- do not delete it."
        )
        node = node[part]
    return node


def _base_values():
    return yaml.safe_load(VALUES.read_text())


def _overlay_files():
    return sorted(p for p in CHART.rglob("values*.yaml") if p != VALUES)


# ---------------------------------------------------------------------------
# static layer -- runs in CI, which has no helm
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("key,_flag", DOCKER_HOST_TOGGLES)
def test_the_docker_host_toggle_defaults_off(key, _flag):
    """A default `helm install` must not opt into a feature the chart cannot serve."""
    assert _dig(_base_values(), key) is False, (
        f"values.yaml sets {key} to something other than false. Everything it "
        "enables needs /var/run/docker.sock in the pod, and this chart mounts none."
    )


@pytest.mark.parametrize("key,_flag", DOCKER_HOST_TOGGLES)
def test_no_overlay_turns_a_docker_host_toggle_back_on(key, _flag):
    """The cloud and kind overlays inherit the base; none may re-enable these."""
    overlays = _overlay_files()
    assert len(overlays) >= 4, (
        f"Found only {len(overlays)} overlay values files -- expected the cloud "
        "and kind sets. A short list makes this test vacuous."
    )
    offenders = []
    for path in overlays:
        loaded = yaml.safe_load(path.read_text()) or {}
        node, missing = loaded, False
        for part in key.split("."):
            if not isinstance(node, dict) or part not in node:
                missing = True
                break
            node = node[part]
        if not missing and node is not False:
            offenders.append(f"{path.relative_to(REPO)} -> {key}: {node}")
    assert not offenders, "Overlays re-enable a Docker-host feature:\n  " + "\n  ".join(
        offenders
    )


@pytest.mark.parametrize("key,flag", DOCKER_HOST_TOGGLES)
def test_the_toggle_has_a_fail_fast_guard(key, flag):
    """Off-by-default is not enough -- setting it true must refuse, not degrade.

    The whole point is that these fail slowly and misleadingly at runtime. An
    install-time `fail` is the only place the operator is still holding the
    context needed to understand the message.
    """
    text = VALIDATE.read_text()
    assert f".Values.{key}" in text, (
        f"templates/validate.yaml has no guard reading .Values.{key}, so setting "
        "it true renders a manifest that comes up healthy and then does not work."
    )
    assert flag in text, (
        f"validate.yaml guards {key} but its message never names '{flag}'. The "
        "message is the entire value of the guard -- it must say what was set."
    )


def test_the_guards_premise_holds_no_template_mounts_a_docker_socket():
    """The guards exist because the socket is absent. If one appears, revisit them.

    Prose naming docker.sock is what explains its absence -- the values comment,
    the `fail` messages, the {{/* */}} rationale blocks -- and those must not
    read as violations. Only a live line counts, so both comment forms are
    stripped as multi-line regions rather than per-line prefixes.
    """
    offenders = []
    prose = 0
    for path in sorted(CHART.rglob("*.yaml")):
        in_block = False
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            stripped = line.strip()
            opened = "{{/*" in stripped
            if in_block or opened:
                closed = "*/}}" in stripped[stripped.index("{{/*") + 4 :] if opened else "*/}}" in stripped
                in_block = not closed
                prose += 1
                continue
            if stripped.startswith("#"):
                prose += 1
                continue
            # `fail` messages are prose too, and they are the point of this fix.
            if "fail " in stripped or stripped.startswith("fail"):
                prose += 1
                continue
            if "docker.sock" in stripped or "/var/run/docker" in stripped:
                offenders.append(f"{path.relative_to(REPO)}:{lineno}: {stripped}")
    assert prose > 0, (
        "Stripped zero comment lines across the chart -- the stripper is broken, "
        "so this test would pass by scanning nothing meaningful."
    )
    assert not offenders, (
        "A chart template mounts the host Docker socket, which hands the pod the "
        "node:\n  " + "\n  ".join(offenders)
    )


def test_the_env_var_is_wired_to_the_value_and_not_hardcoded():
    """A hardcoded "true" in the template would sail past every check above."""
    gen = (CHART / "templates" / "apps" / "generation.yaml").read_text()
    assert "RSYNC_MANAGED_CONNECTORS" in gen, "sanity: the env var should still be set"
    idx = gen.index("RSYNC_MANAGED_CONNECTORS")
    following = gen[idx : idx + 240]
    assert "generation.toolGenerator.managedConnectors" in following, (
        "RSYNC_MANAGED_CONNECTORS is not rendered from "
        "generation.toolGenerator.managedConnectors, so the values default and "
        "the guard both stop meaning anything."
    )


# ---------------------------------------------------------------------------
# render layer -- skips without helm, and a skip is not a pass
# ---------------------------------------------------------------------------


def _helm_template(*extra):
    return subprocess.run(
        ["helm", "template", "r", str(CHART), *RENDER_FLAGS, *extra],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(REPO),
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_default_chart_still_renders():
    """Anti-vacuity for the two tests below: the baseline must be a real render."""
    proc = _helm_template()
    assert proc.returncode == 0, f"default render failed:\n{proc.stderr[-3000:]}"
    assert proc.stdout.count("---") >= 20, (
        f"Rendered only {proc.stdout.count('---')} documents -- too few to be the "
        "whole chart, so a grep for an absent string below would pass for the "
        "wrong reason."
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_managed_connectors_renders_false():
    proc = _helm_template()
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert 'name: RSYNC_MANAGED_CONNECTORS' in proc.stdout, (
        "tool-generator no longer receives RSYNC_MANAGED_CONNECTORS at all"
    )
    idx = proc.stdout.index("name: RSYNC_MANAGED_CONNECTORS")
    assert 'value: "false"' in proc.stdout[idx : idx + 120], (
        "RSYNC_MANAGED_CONNECTORS does not render false:\n"
        + proc.stdout[idx : idx + 120]
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
@pytest.mark.parametrize("key,flag", DOCKER_HOST_TOGGLES)
def test_forcing_a_docker_host_toggle_exits_nonzero(key, flag):
    """The guard must stop the install, not warn inside a rendered manifest."""
    proc = _helm_template("--set", f"{key}=true")
    assert proc.returncode != 0, (
        f"Setting {key}=true rendered successfully. The install would come up "
        "healthy and the feature would never work."
    )
    assert flag in proc.stderr, (
        f"The failure does not name '{flag}', so the operator cannot tell which "
        f"setting caused it:\n{proc.stderr[-2000:]}"
    )
