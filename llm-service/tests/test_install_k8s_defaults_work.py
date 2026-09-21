"""`install-k8s.sh` installs on any cluster with no input, and an edited `.env` changes it.

The Helm chart is fail-closed on purpose: it renders nothing until six secrets and two
frontend URLs are set, and installs no connector pod unless `connectors.fleet` names
one. Both were found the hard way on a real GKE run -- a bare `helm install` either
refuses to render or produces a stack whose pipelines cannot reach any source. The
installer is the layer that makes "run one command" true, so what it must guarantee is
tested here against the real chart, not against a restatement of it:

  1. A run with NO `.env` and NO environment produces a chart that renders, with every
     secret generated (32 alphanumerics -- the alphabet the chart's own
     `assertDeliverableSecret` accepts), a working fleet, and files only the owner can
     read.
  2. A second run reuses every secret. ENCRYPTION_KEY is a one-way door (rotating it
     makes every saved connection undecryptable) and POSTGRES_PASSWORD must keep
     matching the database's volume; "re-run to upgrade" is only safe if this holds.
  3. A user-edited `.env` wins over the default, and a lost `.env` is rebuilt from the
     Secret the previous install left in the cluster rather than re-rolled.
  4. Nonsense input is refused up front, naming the setting, not three steps later as
     an apiserver error.
  5. The connector list embedded in the script is the tree's list. The Service name the
     orchestrator resolves contains the connector's version, so a stale version is a
     pod that exists and is never found.
  6. `--render-only` touches no cluster.

`main` is exercised end to end with `kubectl` and `helm upgrade` faked (the real helm
renders), so what is asserted is the command the installer WOULD run on a cluster.
"""

import json
import os
import pathlib
import pty
import re
import shutil
import stat
import subprocess

import pytest
import yaml

import _flip_cut

REPO = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = REPO / "install-k8s.sh"
CHART = REPO / "deploy" / "helm" / "rsync-ai"
CONNECTORS = REPO / "shared" / "mcp-connectors" / "public"
BASH = shutil.which("bash") or "/bin/bash"
REAL_HELM = shutil.which("helm")

SECRET_KEYS = (
    "JWT_SECRET",
    "ENCRYPTION_KEY",
    "INTERNAL_SERVICE_SECRET",
    "POSTGRES_PASSWORD",
    "REDIS_PASSWORD",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "RSYNC_DEMO_WAREHOUSE_PASSWORD",
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _script_var(name: str) -> str:
    m = re.search(rf'^{name}="?([^"\n]*)"?$', SCRIPT.read_text(encoding="utf-8"), re.M)
    assert m, f"install-k8s.sh no longer defines {name}"
    return m.group(1)


def _env(tmp_path, **extra):
    """A clean environment: nothing of the developer's RSYNC_* / OPENAI leaks in."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("RSYNC_")}
    env.pop("OPENAI_API_KEY", None)
    env.update(
        RSYNC_INSTALL_DIR=str(tmp_path / "inst"),
        RSYNC_CHART=str(CHART),
        HOME=str(tmp_path),
    )
    env.update({k: str(v) for k, v in extra.items()})
    return env


def _run(tmp_path, *args, stdin=None, **extra):
    return subprocess.run(
        [BASH, str(SCRIPT), *args],
        env=_env(tmp_path, **extra),
        input=stdin,
        capture_output=True,
        text=True,
        timeout=180,
    )


def _dotenv(tmp_path) -> dict:
    out = {}
    for line in (tmp_path / "inst" / ".env").read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.removeprefix("export ").strip()] = v
    return out


def _values(tmp_path) -> dict:
    return yaml.safe_load((tmp_path / "inst" / "values.generated.yaml").read_text())


def _render(tmp_path, *extra_values):
    """Render the chart the way the installer does: its generated file, then any extra."""
    cmd = [REAL_HELM, "template", "rsync", str(CHART), "-f", str(tmp_path / "inst" / "values.generated.yaml")]
    for f in extra_values:
        cmd += ["-f", str(f)]
    cp = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    assert cp.returncode == 0, cp.stderr
    return [d for d in yaml.safe_load_all(cp.stdout) if d]


def _mode(path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


# ---------------------------------------------------------------------------
# 5. the embedded connector list is the tree's list  (no helm needed)
# ---------------------------------------------------------------------------


def _tree_connectors() -> dict:
    out = {}
    for latest in CONNECTORS.rglob("latest.json"):
        if "versions" in latest.parts:
            continue
        out[latest.parent.name] = json.loads(latest.read_text())["current_version"]
    return out


def test_embedded_connector_list_equals_the_tree():
    known = dict(e.split(":", 1) for e in _script_var("KNOWN_CONNECTORS").split())
    tree = _tree_connectors()
    assert len(tree) >= 20, f"only {len(tree)} connectors found in the tree; the glob is wrong"
    # sample-data is run by the chart itself (connectors.sampleData), not listed in the fleet.
    tree.pop("sample-data", None)
    assert known == tree, (
        "KNOWN_CONNECTORS in install-k8s.sh differs from shared/mcp-connectors/public/**/latest.json.\n"
        f"only in script: {sorted(set(known) - set(tree))}\n"
        f"only in tree:   {sorted(set(tree) - set(known))}\n"
        f"version differs: {sorted(k for k in known.keys() & tree.keys() if known[k] != tree[k])}\n"
        "The version is part of the Service name the orchestrator resolves, so it must be the "
        "tree's current_version."
    )


def test_default_connectors_are_all_known():
    known = {e.split(":", 1)[0] for e in _script_var("KNOWN_CONNECTORS").split()}
    default = set(_script_var("DEFAULT_CONNECTORS").split(","))
    assert default and default <= known, f"default fleet names unknown connectors: {default - known}"
    assert "postgresql" in default, "the demo path needs a postgresql connector in the default fleet"


def test_script_stays_bash_3_compatible():
    """macOS ships bash 3.2, and this is the script people pipe into `bash`."""
    text = SCRIPT.read_text(encoding="utf-8")
    code = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("#"))
    for pattern, what in (
        (r"\bdeclare\s+-A\b|\blocal\s+-A\b", "associative arrays"),
        (r"\bmapfile\b|\breadarray\b", "mapfile/readarray"),
        (r"\$\{[A-Za-z_]+(,,|\^\^)\}", "case-modifying expansions"),
        (r"&>>|\|&", "bash 4 redirections"),
    ):
        assert not re.search(pattern, code), f"install-k8s.sh uses {what}, which bash 3.2 lacks"


def test_script_parses_and_has_a_shebang():
    assert SCRIPT.read_text().startswith("#!/usr/bin/env bash\n")
    assert subprocess.run([BASH, "-n", str(SCRIPT)], capture_output=True).returncode == 0
    assert os.access(SCRIPT, os.X_OK), "install-k8s.sh must be executable (git mode 100755)"


# ---------------------------------------------------------------------------
# 1. zero input
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_zero_input_produces_a_chart_that_renders(tmp_path):
    cp = _run(tmp_path, "--render-only")
    assert cp.returncode == 0, cp.stdout + cp.stderr

    env = _dotenv(tmp_path)
    for k in SECRET_KEYS:
        assert re.fullmatch(r"[A-Za-z0-9]{32}", env.get(k, "")), f"{k} not a 32-char alphanumeric secret"
    assert len({env[k] for k in SECRET_KEYS}) == len(SECRET_KEYS), "two secrets share a value"

    # Every file that holds a secret is owner-only, and so is the directory.
    inst = tmp_path / "inst"
    assert _mode(inst / ".env") == 0o600
    assert _mode(inst / "values.generated.yaml") == 0o600
    assert _mode(inst) == 0o700

    v = _values(tmp_path)
    assert v["frontend"] == {"apiUrl": "http://localhost:8080", "publicUrl": "http://localhost:3000"}
    fleet = [c["id"] for c in v["connectors"]["fleet"]]
    assert fleet == _script_var("DEFAULT_CONNECTORS").split(","), "default fleet is not the documented one"
    assert v["demo"]["enabled"] is True
    assert "ingress" not in v, "no hostname was given, so no ingress"

    docs = _render(tmp_path)
    names = {(d["kind"], d["metadata"]["name"]) for d in docs}
    for cid in fleet:
        assert ("Deployment", f"rsync-mcp-{cid}-v1-0-0") in names or any(
            k == "Deployment" and n.startswith("rsync-") and cid in n for k, n in names
        ), f"no Deployment renders for connector {cid}; the fleet is not reaching the chart"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_generated_secret_reaches_the_chart_secret(tmp_path):
    assert _run(tmp_path, "--render-only").returncode == 0
    env = _dotenv(tmp_path)
    secret = next(
        d for d in _render(tmp_path) if d["kind"] == "Secret" and d["metadata"]["name"] == "rsync-secrets"
    )
    data = secret.get("stringData") or {}
    assert data.get("ENCRYPTION_KEY") == env["ENCRYPTION_KEY"]
    assert data.get("JWT_SECRET") == env["JWT_SECRET"]
    assert data.get("POSTGRES_PASSWORD") == env["POSTGRES_PASSWORD"]
    assert data.get("DEMO_WAREHOUSE_PASSWORD") == env["RSYNC_DEMO_WAREHOUSE_PASSWORD"]
    # Without it the gateway cannot authenticate to the orchestrator: every
    # POST /pipelines/{id}/run is refused "authentication required".
    assert data.get("INTERNAL_SERVICE_SECRET") == env["INTERNAL_SERVICE_SECRET"]


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_piped_through_bash_stdin_works(tmp_path):
    """`curl … | bash` gives the script no $0 file and a stdin that is the script itself."""
    cp = subprocess.run(
        [BASH, "-s", "--", "--render-only"],
        input=SCRIPT.read_text(),
        env=_env(tmp_path),
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert (tmp_path / "inst" / ".env").exists()


# ---------------------------------------------------------------------------
# 2 + 3. re-runs keep secrets; the .env is the interface
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_rerun_regenerates_nothing(tmp_path):
    assert _run(tmp_path, "--render-only").returncode == 0
    first = _dotenv(tmp_path)
    cp = _run(tmp_path, "--render-only")
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert _dotenv(tmp_path) == first, "a re-run changed a secret"
    assert "Generated" not in cp.stdout, "a re-run claims to have generated secrets"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_secret_the_user_supplied_is_kept_and_the_rest_generated(tmp_path):
    inst = tmp_path / "inst"
    inst.mkdir()
    (inst / ".env").write_text('ENCRYPTION_KEY="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"\nexport JWT_SECRET=' + "j" * 40 + "\n")
    assert _run(tmp_path, "--render-only").returncode == 0
    env = _dotenv(tmp_path)
    # the quoted value is read unquoted, and left exactly as the user wrote it on disk
    assert _values(tmp_path)["secrets"]["encryptionKey"] == "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
    assert _values(tmp_path)["secrets"]["jwtSecret"] == "j" * 40
    assert env["ENCRYPTION_KEY"] == '"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"'
    assert re.fullmatch(r"[A-Za-z0-9]{32}", env["POSTGRES_PASSWORD"])
    assert "export JWT_SECRET" in (inst / ".env").read_text(), "the user's `export` was rewritten"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_openai_key_is_used_but_never_written_to_the_env_file(tmp_path):
    cp = _run(tmp_path, "--render-only", OPENAI_API_KEY="sk-test-abc123")
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert _values(tmp_path)["secrets"]["openaiApiKey"] == "sk-test-abc123"
    assert "sk-test-abc123" not in (tmp_path / "inst" / ".env").read_text()
    assert "sk-test-abc123" not in cp.stdout + cp.stderr, "the installer printed the API key"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_settings_in_the_env_file_change_the_install(tmp_path):
    inst = tmp_path / "inst"
    inst.mkdir()
    (inst / ".env").write_text(
        "RSYNC_CONNECTORS=stripe, postgresql ,stripe\nRSYNC_DEMO=false\nRSYNC_STORAGE_CLASS=fast-ssd\n"
        "RSYNC_IMAGE_REGISTRY=registry.example.com/mirror\nRSYNC_IMAGE_TAG=v9.9.9\nRSYNC_IMAGE_PULL_SECRET=regcred\n"
    )
    cp = _run(tmp_path, "--render-only")
    assert cp.returncode == 0, cp.stdout + cp.stderr
    v = _values(tmp_path)
    assert [c["id"] for c in v["connectors"]["fleet"]] == ["stripe", "postgresql"], "fleet not de-duplicated/ordered"
    assert v["demo"]["enabled"] is False
    assert v["global"]["storageClass"] == "fast-ssd"
    assert v["global"]["image"] == {
        "registry": "registry.example.com/mirror",
        "tag": "v9.9.9",
        "pullSecrets": [{"name": "regcred"}],
    }


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_environment_beats_the_env_file(tmp_path):
    inst = tmp_path / "inst"
    inst.mkdir()
    (inst / ".env").write_text("RSYNC_CONNECTORS=stripe\n")
    assert _run(tmp_path, "--render-only", RSYNC_CONNECTORS="mysql").returncode == 0
    ids = [c["id"] for c in _values(tmp_path)["connectors"]["fleet"]]
    assert "mysql" in ids and "stripe" not in ids


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_demo_pulls_in_the_postgresql_connector_it_needs(tmp_path):
    cp = _run(tmp_path, "--render-only", RSYNC_CONNECTORS="stripe", RSYNC_DEMO="true")
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert [c["id"] for c in _values(tmp_path)["connectors"]["fleet"]] == ["postgresql", "stripe"]


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_hostnames_publish_the_app_through_an_ingress(tmp_path):
    cp = _run(
        tmp_path,
        "--render-only",
        RSYNC_APP_HOST="app.example.com",
        RSYNC_API_HOST="api.example.com",
        RSYNC_INGRESS_CLASS="nginx",
        RSYNC_TLS_SECRET="rsync-tls",
    )
    assert cp.returncode == 0, cp.stdout + cp.stderr
    v = _values(tmp_path)
    assert v["frontend"] == {"apiUrl": "https://api.example.com", "publicUrl": "https://app.example.com"}
    assert v["ingress"]["enabled"] is True and v["ingress"]["className"] == "nginx"
    ingress = next(d for d in _render(tmp_path) if d["kind"] == "Ingress")
    hosts = {r["host"] for r in ingress["spec"]["rules"]}
    assert hosts >= {"app.example.com", "api.example.com"}


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_ollama_switches_the_generation_provider(tmp_path):
    cp = _run(tmp_path, "--render-only", RSYNC_LLM_PROVIDER="ollama")
    assert cp.returncode == 0, cp.stdout + cp.stderr
    v = _values(tmp_path)
    assert v["ollama"]["enabled"] is True and v["generation"]["llm"]["provider"] == "ollama"
    assert any(d["kind"] in ("Deployment", "StatefulSet") and "ollama" in d["metadata"]["name"] for d in _render(tmp_path))


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_an_extra_values_file_is_layered_on_top(tmp_path):
    extra = tmp_path / "mine.yaml"
    extra.write_text("apiGateway:\n  replicaCount: 3\n")
    cp = _run(tmp_path, "--render-only", RSYNC_EXTRA_VALUES=extra)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    gw = next(
        d for d in _render(tmp_path, extra)
        if d["kind"] == "Deployment" and d["metadata"]["name"] == "rsync-api-gateway"
    )
    assert gw["spec"]["replicas"] == 3


# ---------------------------------------------------------------------------
# 4. nonsense is refused, by name
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
@pytest.mark.parametrize(
    "extra, must_name",
    [
        ({"RSYNC_CONNECTORS": "postgresql,not-a-connector"}, "not-a-connector"),
        ({"RSYNC_NAMESPACE": "Bad_Namespace"}, "RSYNC_NAMESPACE"),
        ({"RSYNC_RELEASE": "x" * 41}, "RSYNC_RELEASE"),
        ({"RSYNC_DEMO": "maybe"}, "RSYNC_DEMO"),
        ({"RSYNC_LLM_PROVIDER": "skynet"}, "RSYNC_LLM_PROVIDER"),
        ({"RSYNC_APP_HOST": "only.example.com"}, "RSYNC_API_HOST"),
        ({"RSYNC_APP_HOST": "not a host", "RSYNC_API_HOST": "api.example.com"}, "hostname"),
        ({"RSYNC_WAIT_TIMEOUT": "soon"}, "RSYNC_WAIT_TIMEOUT"),
        ({"RSYNC_EXTRA_VALUES": "/no/such/file.yaml"}, "RSYNC_EXTRA_VALUES"),
    ],
)
def test_bad_settings_are_refused_before_anything_is_written(tmp_path, extra, must_name):
    cp = _run(tmp_path, "--render-only", **extra)
    assert cp.returncode != 0, "accepted a nonsense setting"
    assert must_name in cp.stderr, f"the error does not name {must_name!r}:\n{cp.stderr}"
    assert not (tmp_path / "inst" / ".env").exists(), "wrote secrets for an install it then refused"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_user_value_the_chart_refuses_fails_loudly(tmp_path):
    inst = tmp_path / "inst"
    inst.mkdir()
    (inst / ".env").write_text("POSTGRES_PASSWORD=has space and @ sign\n")
    cp = _run(tmp_path, "--render-only")
    assert cp.returncode != 0
    assert "chart refused" in (cp.stdout + cp.stderr).lower()


# ---------------------------------------------------------------------------
# 6 + the full flow, with a faked cluster
# ---------------------------------------------------------------------------

FAKE_KUBECTL = r"""#!/usr/bin/env bash
echo "kubectl $*" >> "$FAKE_LOG"
case "$*" in
  *"config current-context"*) echo fake-ctx ;;
  *"get --raw"*) echo ok ;;
  *"get storageclass"*) printf 'standard\ttrue\t\n' ;;
  *"get pods -A"*) printf '%b' "${FAKE_PODS:-}" ;;
  *"get nodes"*) [[ -z "${FAKE_NODES_FAIL:-}" ]] || exit 1
    if [[ -n "${FAKE_NODES:-}" ]]; then printf '%b\n' "$FAKE_NODES"; else printf '32Gi 8\n32Gi 8\n'; fi ;;
  *"get secret rsync-secrets -o"*)
    [[ -n "${FAKE_EXISTING_SECRET:-}" ]] || exit 1
    key="$(printf '%s' "$*" | sed -n 's/.*index \.data "\([A-Z_]*\)".*/\1/p')"
    var="FAKE_SECRET_${key}"
    printf '%s' "${!var:-}" | base64 ;;
  *"get secret rsync-secrets"*) [[ -n "${FAKE_EXISTING_SECRET:-}" ]] || exit 1 ;;
  *"get events"*) printf '%s\n' "${FAKE_EVENTS:-}" ;;
  *"version --client"*) echo '{"clientVersion":{"gitVersion":"v1.30.0"}}' ;;
  *) exit 0 ;;
esac
"""

FAKE_HELM = r"""#!/usr/bin/env bash
echo "helm $*" >> "$FAKE_LOG"
case "$1" in
  status)
    [[ -n "${FAKE_HELM_STATUS:-}" ]] || exit 1
    printf 'STATUS: %s\nREVISION: %s\n' "$FAKE_HELM_STATUS" "${FAKE_HELM_REVISION:-1}" ;;
  upgrade) [[ -z "${FAKE_HELM_UPGRADE_FAILS:-}" ]] || exit 1; exit 0 ;;
  uninstall) exit 0 ;;
  # an oci:// chart would be pulled from the registry; a test must not need the network
  template) case "$*" in *oci://*) exit 0 ;; esac ;;
esac
exec "$REAL_HELM" "$@"
"""


def _fake_cluster(tmp_path, **extra):
    bindir = tmp_path / "fakebin"
    bindir.mkdir(exist_ok=True)
    for name, body in (("kubectl", FAKE_KUBECTL), ("helm", FAKE_HELM)):
        p = bindir / name
        p.write_text(body)
        p.chmod(0o755)
    log = tmp_path / "calls.log"
    log.write_text("")
    env = dict(
        FAKE_LOG=str(log),
        REAL_HELM=str(REAL_HELM),
        PATH=f"{bindir}{os.pathsep}{os.environ['PATH']}",
    )
    env.update(extra)
    return env, log


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_render_only_touches_no_cluster(tmp_path):
    env, log = _fake_cluster(tmp_path)
    cp = _run(tmp_path, "--render-only", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    calls = log.read_text()
    assert "kubectl" not in calls, f"--render-only called kubectl:\n{calls}"
    assert " upgrade " not in calls and " install " not in calls


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_full_run_installs_with_the_documented_helm_command(tmp_path):
    env, log = _fake_cluster(tmp_path)
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    upgrade = next(l for l in log.read_text().splitlines() if l.startswith("helm upgrade"))
    assert "--install rsync" in upgrade
    assert str(CHART) in upgrade
    assert "--namespace rsync" in upgrade and "--create-namespace" in upgrade
    assert "--wait" in upgrade and "--timeout 15m" in upgrade
    assert f"-f {tmp_path}/inst/values.generated.yaml" in upgrade
    assert "--version" not in upgrade, "a local chart directory takes no --version"
    assert "kubectl -n rsync port-forward" in cp.stdout, "the summary does not say how to open the UI"
    assert "OPENAI_API_KEY" in cp.stdout, "no note that chat needs an LLM key"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_an_oci_chart_is_pinned_to_the_release_version(tmp_path):
    env, log = _fake_cluster(tmp_path)
    version = _script_var("RSYNC_CHART_VERSION").split(":-")[-1].rstrip("}")
    assert re.fullmatch(r"\d+\.\d+\.\d+", version), f"chart version {version!r} is not a pinned x.y.z"
    chart = "oci://ghcr.io/rsync-ai/charts/rsync-ai"
    cp = _run(tmp_path, "--no-port-forward", RSYNC_CHART=chart, **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    upgrade = next(l for l in log.read_text().splitlines() if l.startswith("helm upgrade"))
    assert f"{chart} --version {version}" in upgrade, upgrade
    # the version the script pins is the version the chart on disk declares
    chart_yaml = yaml.safe_load((CHART / "Chart.yaml").read_text())
    assert chart_yaml["version"] == version, (
        f"install-k8s.sh pins chart {version} but Chart.yaml is {chart_yaml['version']}; a release bump must move both"
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_lost_env_file_is_rebuilt_from_the_cluster_secret(tmp_path):
    """The Secret survives `helm uninstall`; it is the ground truth for the two that cannot be re-rolled."""
    env, _ = _fake_cluster(
        tmp_path,
        FAKE_EXISTING_SECRET="1",
        FAKE_SECRET_ENCRYPTION_KEY="K" * 32,
        FAKE_SECRET_POSTGRES_PASSWORD="P" * 32,
        FAKE_SECRET_DEMO_WAREHOUSE_PASSWORD="W" * 32,
    )
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    d = _dotenv(tmp_path)
    assert d["ENCRYPTION_KEY"] == "K" * 32
    assert d["POSTGRES_PASSWORD"] == "P" * 32
    assert d["RSYNC_DEMO_WAREHOUSE_PASSWORD"] == "W" * 32
    # the ones the Secret did not hold are generated, not left empty
    assert re.fullmatch(r"[A-Za-z0-9]{32}", d["JWT_SECRET"])
    assert "Reused 3 secret(s)" in cp.stdout


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_cluster_without_a_default_storage_class_is_refused_by_name(tmp_path):
    env, _ = _fake_cluster(tmp_path)
    # a StorageClass that is not the default
    kubectl = pathlib.Path(tmp_path / "fakebin" / "kubectl")
    kubectl.write_text(kubectl.read_text().replace(r"'standard\ttrue\t\n'", r"'standard\t\t\n'"))
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode != 0
    assert "default StorageClass" in cp.stderr and "RSYNC_STORAGE_CLASS" in cp.stderr
    assert not (tmp_path / "inst" / ".env").exists(), "generated secrets for an install it then refused"


# ---------------------------------------------------------------------------
# the chart change that came with the installer
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_connect_secrets_survive_a_pod_restart_by_default(tmp_path):
    """debezium-mcp writes each connector's credentials to /connect-secrets and Kafka Connect
    reads them back through `${file:…}`. Connect's own config lives in Kafka, so after a pod
    restart every connector points at a file that an emptyDir no longer has: they sit in
    RESTARTING ("Could not read properties from file") until each is deleted and recreated.
    Found on a real GKE run."""
    assert _run(tmp_path, "--render-only").returncode == 0
    docs = _render(tmp_path)
    pvc = next(
        (d for d in docs if d["kind"] == "PersistentVolumeClaim" and d["metadata"]["name"].endswith("-connect-secrets")),
        None,
    )
    assert pvc is not None, "no connect-secrets PVC renders by default"
    # The connector configs live in the Kafka volume, which survives `helm uninstall`; the
    # credential files they point at must survive with it or every connector restarts forever.
    assert pvc["metadata"].get("annotations", {}).get("helm.sh/resource-policy") == "keep"
    connect = next(
        d for d in docs
        if d["kind"] == "Deployment" and d["metadata"]["labels"].get("app.kubernetes.io/component") == "kafka-connect"
    )
    assert connect["spec"]["strategy"]["type"] == "Recreate", "an RWO claim cannot be shared by two rolling pods"
    vol = next(v for v in connect["spec"]["template"]["spec"]["volumes"] if v["name"] == "connect-secrets")
    assert vol.get("persistentVolumeClaim", {}).get("claimName") == pvc["metadata"]["name"]
    assert "emptyDir" not in vol


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_connect_secrets_can_be_switched_back_to_an_emptydir(tmp_path):
    assert _run(tmp_path, "--render-only").returncode == 0
    off = tmp_path / "off.yaml"
    off.write_text("connectors:\n  cdc:\n    kafkaConnect:\n      secretsPersistence:\n        enabled: false\n")
    docs = _render(tmp_path, off)
    assert not any(d["kind"] == "PersistentVolumeClaim" and d["metadata"]["name"].endswith("-connect-secrets") for d in docs)
    connect = next(
        d for d in docs
        if d["kind"] == "Deployment" and d["metadata"]["labels"].get("app.kubernetes.io/component") == "kafka-connect"
    )
    vol = next(v for v in connect["spec"]["template"]["spec"]["volumes"] if v["name"] == "connect-secrets")
    assert vol == {"name": "connect-secrets", "emptyDir": {}}


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
@pytest.mark.parametrize("status", ["pending-install", "failed"])
def test_an_unfinished_first_install_is_cleared_and_retried(tmp_path, status):
    """Ctrl-C during the first install leaves a `pending-install` revision 1 that
    helm will not upgrade. A one-command installer must clear it, not stop."""
    env, log = _fake_cluster(tmp_path, FAKE_HELM_STATUS=status, FAKE_HELM_REVISION="1")
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    calls = log.read_text().splitlines()
    uninstall = next(i for i, l in enumerate(calls) if l.startswith("helm uninstall rsync"))
    upgrade = next(i for i, l in enumerate(calls) if l.startswith("helm upgrade"))
    assert uninstall < upgrade, "the stale release must be cleared BEFORE the retry"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_deployed_release_is_upgraded_not_uninstalled(tmp_path):
    """Control for the test above: a healthy release must never be uninstalled."""
    env, log = _fake_cluster(tmp_path, FAKE_HELM_STATUS="deployed", FAKE_HELM_REVISION="1")
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert "helm uninstall" not in log.read_text()


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_an_interrupted_upgrade_is_reported_not_destroyed(tmp_path):
    """A pending-upgrade on revision >1 holds a working release; uninstalling it
    would delete the user's stack, so the installer must stop and say how to recover."""
    env, log = _fake_cluster(tmp_path, FAKE_HELM_STATUS="pending-upgrade", FAKE_HELM_REVISION="3")
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode != 0
    assert "helm rollback" in cp.stdout + cp.stderr
    assert "helm uninstall" not in log.read_text()


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_wrong_architecture_image_is_named_as_the_cause(tmp_path):
    """On an arm64 node the default tag's amd64-only images sit in ImagePullBackOff and helm
    only says it timed out. Someone running the one-liner with no assistant needs the cause."""
    env, _ = _fake_cluster(
        tmp_path,
        FAKE_HELM_UPGRADE_FAILS="1",
        FAKE_EVENTS='Failed to pull image "ghcr.io/rsync-ai/api-gateway:0.1.2": no match for platform in manifest',
    )
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode != 0
    out = cp.stdout + cp.stderr
    assert "no build for this cluster's CPU architecture" in out and "RSYNC_IMAGE_TAG" in out


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_an_unrelated_failure_does_not_blame_the_architecture(tmp_path):
    """Control: the hint must not appear for every failure."""
    env, _ = _fake_cluster(tmp_path, FAKE_HELM_UPGRADE_FAILS="1", FAKE_EVENTS="Back-off restarting failed container")
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode != 0
    assert "CPU architecture" not in cp.stdout + cp.stderr


# ---------------------------------------------------------------------------
# the browser has to be told where the API is, in the names the image reads
# ---------------------------------------------------------------------------


def _frontend_env(docs) -> dict:
    fe = next(d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "rsync-frontend")
    return {e["name"]: e.get("value") for e in fe["spec"]["template"]["spec"]["containers"][0]["env"] if "value" in e}


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_frontend_pod_gets_the_runtime_urls_the_image_reads(tmp_path):
    """The published frontend image is built once, with NEXT_PUBLIC_API_URL baked to
    http://localhost:5001. What overrides it per install is PUBLIC_URL / PUBLIC_WS_URL,
    read at request time (frontend/src/lib/config/runtime-env.ts) and inlined into the page
    as window.__RSYNC_RUNTIME__. The chart used to set only the NEXT_PUBLIC_ pair, which a
    running image ignores, so on every Kubernetes install the browser called localhost:5001
    and signup said "Failed to fetch"."""
    assert _run(tmp_path, "--render-only").returncode == 0
    env = _frontend_env(_render(tmp_path))
    assert env["PUBLIC_URL"] == "http://localhost:8080"
    assert env["PUBLIC_WS_URL"] == "ws://localhost:8080/ws"
    # kept for an image a user builds themselves, and must agree with the runtime pair
    assert env["NEXT_PUBLIC_API_URL"] == env["PUBLIC_URL"]
    assert env["NEXT_PUBLIC_WS_URL"] == env["PUBLIC_WS_URL"]


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_runtime_urls_follow_the_ingress_hostname(tmp_path):
    cp = _run(
        tmp_path, "--render-only", RSYNC_APP_HOST="app.example.com", RSYNC_API_HOST="api.example.com", RSYNC_TLS_SECRET="rsync-tls"
    )
    assert cp.returncode == 0, cp.stdout + cp.stderr
    env = _frontend_env(_render(tmp_path))
    assert env["PUBLIC_URL"] == "https://api.example.com"
    assert env["PUBLIC_WS_URL"] == "wss://api.example.com/ws"


# ---------------------------------------------------------------------------
# INTERNAL_SERVICE_SECRET: without it no pipeline can run
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_an_upgrade_from_a_release_that_predates_the_secret_generates_one(tmp_path):
    """A cluster installed by an earlier installer has a rsync-secrets with no
    INTERNAL_SERVICE_SECRET. Re-running must fill that key in and keep every other one."""
    env, _ = _fake_cluster(
        tmp_path,
        FAKE_EXISTING_SECRET="1",
        FAKE_SECRET_ENCRYPTION_KEY="K" * 32,
        FAKE_SECRET_POSTGRES_PASSWORD="P" * 32,
    )
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    d = _dotenv(tmp_path)
    assert d["ENCRYPTION_KEY"] == "K" * 32 and d["POSTGRES_PASSWORD"] == "P" * 32
    assert re.fullmatch(r"[A-Za-z0-9]{32}", d["INTERNAL_SERVICE_SECRET"])
    assert _values(tmp_path)["secrets"]["internalServiceSecret"] == d["INTERNAL_SERVICE_SECRET"]


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_an_existing_internal_secret_in_the_cluster_is_reused(tmp_path):
    """Changing it rolls every pod that mounts it, so a re-run must not re-roll it."""
    env, _ = _fake_cluster(tmp_path, FAKE_EXISTING_SECRET="1", FAKE_SECRET_INTERNAL_SERVICE_SECRET="I" * 32)
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert _dotenv(tmp_path)["INTERNAL_SERVICE_SECRET"] == "I" * 32


# ---------------------------------------------------------------------------
# capacity: the numbers the installer plans with are the chart's
# ---------------------------------------------------------------------------


def _qty_mib(q) -> float:
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([A-Za-z]*)", str(q))
    unit = {"": 1 / 1048576, "Ki": 1 / 1024, "Mi": 1, "Gi": 1024}[m[2]]
    return float(m[1]) * unit


def _qty_milli(q) -> float:
    q = str(q)
    return float(q[:-1]) if q.endswith("m") else float(q) * 1000


def _requested(docs) -> tuple:
    """(MiB, millicores) the scheduler must find room for: every long-running pod, plus the
    post-install hook Jobs -- they start only once every Deployment is up, which is exactly
    why a full node leaves them Pending."""
    mem = cpu = 0.0
    for d in docs:
        if d["kind"] in ("Deployment", "StatefulSet"):
            spec, reps = d["spec"]["template"]["spec"], d["spec"].get("replicas", 1)
        elif d["kind"] == "Job":
            spec, reps = d["spec"]["template"]["spec"], 1
        else:
            continue
        for c in spec.get("containers", []):
            r = (c.get("resources") or {}).get("requests") or {}
            mem += reps * _qty_mib(r.get("memory", 0))
            cpu += reps * _qty_milli(r.get("cpu", 0))
    return round(mem), round(cpu)


def _requested_with(tmp_path, *, fleet, demo, ollama=False):
    extra = tmp_path / "shape.yaml"
    extra.write_text(
        yaml.safe_dump(
            {
                "connectors": {"fleet": fleet},
                "demo": {"enabled": demo},
                "ollama": {"enabled": ollama},
            }
        )
    )
    return _requested(_render(tmp_path, extra))


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_capacity_constants_are_what_the_chart_requests(tmp_path):
    """The installer refuses to plan an install that cannot fit, using constants it carries.
    They were hard-coded once and went stale: it passed an 8.7 GiB node the chart's own
    8704 Mi could not fit beside kube-system. Re-measure them from the chart every run."""
    assert _run(tmp_path, "--render-only").returncode == 0
    fleet = _values(tmp_path)["connectors"]["fleet"]
    assert len(fleet) >= 2

    base = _requested_with(tmp_path, fleet=[], demo=False)
    assert base == (int(_script_var("BASE_MEM_MIB")), int(_script_var("BASE_CPU_M"))), (
        f"the chart with no connectors and no demo requests {base} (MiB, m); update BASE_* in install-k8s.sh"
    )

    one = _requested_with(tmp_path, fleet=fleet[:1], demo=False)
    assert (one[0] - base[0], one[1] - base[1]) == (
        int(_script_var("CONNECTOR_MEM_MIB")), int(_script_var("CONNECTOR_CPU_M"))
    ), "a connector pod's requests changed; update CONNECTOR_* in install-k8s.sh"
    two = _requested_with(tmp_path, fleet=fleet[:2], demo=False)
    assert two[0] - one[0] == one[0] - base[0], "connector pods are not all the same size any more"

    # the demo warehouse is reached through the postgresql connector, so the chart insists on one
    pg = [c for c in fleet if c["id"] == "postgresql"]
    assert pg, "the default fleet has no postgresql connector"
    demo = _requested_with(tmp_path, fleet=pg, demo=True)
    assert (demo[0] - one[0], demo[1] - one[1]) == (
        int(_script_var("DEMO_MEM_MIB")), int(_script_var("DEMO_CPU_M"))
    ), "the demo warehouse's requests changed; update DEMO_* in install-k8s.sh"

    ollama = _requested_with(tmp_path, fleet=[], demo=False, ollama=True)
    assert (ollama[0] - base[0], ollama[1] - base[1]) == (
        int(_script_var("OLLAMA_MEM_MIB")), int(_script_var("OLLAMA_CPU_M"))
    ), "the in-cluster model's requests changed; update OLLAMA_* in install-k8s.sh"


def _default_need() -> tuple:
    """(MiB, m) the DEFAULT install asks for, by the script's own constants."""
    n = len(_script_var("DEFAULT_CONNECTORS").split(","))
    mem = int(_script_var("BASE_MEM_MIB")) + n * int(_script_var("CONNECTOR_MEM_MIB")) + int(_script_var("DEMO_MEM_MIB"))
    cpu = int(_script_var("BASE_CPU_M")) + n * int(_script_var("CONNECTOR_CPU_M")) + int(_script_var("DEMO_CPU_M"))
    return mem, cpu


def _lean_need() -> tuple:
    n = len(_script_var("LEAN_CONNECTORS").split(","))
    return (
        int(_script_var("BASE_MEM_MIB")) + n * int(_script_var("CONNECTOR_MEM_MIB")),
        int(_script_var("BASE_CPU_M")) + n * int(_script_var("CONNECTOR_CPU_M")),
    )


def _nodes(mem_mib, cpu_m):
    """One node whose allocatable is exactly this, in the order the installer reads it: memory, cpu."""
    return f"{mem_mib}Mi {cpu_m}m"


def _fleet_ids(tmp_path):
    return [c["id"] for c in _values(tmp_path)["connectors"]["fleet"]]


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_cluster_that_fits_the_defaults_gets_the_defaults(tmp_path):
    mem, cpu = _default_need()
    env, _ = _fake_cluster(tmp_path, FAKE_NODES=_nodes(mem + 512, cpu + 500))
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert "Capacity:" in cp.stdout
    assert "Installing a lean set" not in cp.stdout
    assert _fleet_ids(tmp_path) == _script_var("DEFAULT_CONNECTORS").split(",")
    assert _values(tmp_path)["demo"]["enabled"] is True


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_small_cluster_gets_a_lean_install_instead_of_a_pending_one(tmp_path):
    """An 8.7 GiB node (one e2-standard-2 pair, Docker Desktop's default) cannot hold the
    default fleet: helm --wait would sit ten minutes on a Pending orchestrator. The
    connectors and demo the user did not choose are trimmed instead."""
    lean_mem, lean_cpu = _lean_need()
    mem, cpu = _default_need()
    assert lean_mem < mem
    env, log = _fake_cluster(tmp_path, FAKE_NODES=_nodes(lean_mem + 300, cpu + 500))
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert "Installing a lean set" in cp.stdout
    assert _fleet_ids(tmp_path) == _script_var("LEAN_CONNECTORS").split(",")
    assert _values(tmp_path)["demo"]["enabled"] is False
    assert any(l.startswith("helm upgrade") for l in log.read_text().splitlines()), "it stopped instead of installing"
    # a trim is not a preference: the next run on a bigger cluster must get the defaults again
    persisted = _dotenv(tmp_path)
    assert "RSYNC_CONNECTORS" not in persisted and "RSYNC_DEMO" not in persisted, "the trim was persisted"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_choice_the_user_made_is_never_trimmed(tmp_path):
    lean_mem, lean_cpu = _lean_need()
    mem, cpu = _default_need()
    env, _ = _fake_cluster(tmp_path, FAKE_NODES=_nodes(lean_mem + 300, cpu + 500))
    cp = _run(tmp_path, "--no-port-forward", RSYNC_CONNECTORS="postgresql,mongodb,mysql,stripe", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert _fleet_ids(tmp_path) == ["postgresql", "mongodb", "mysql", "stripe"]
    # only what they did not choose is trimmed: the demo goes, and the installer says so
    assert _values(tmp_path)["demo"]["enabled"] is False
    tmp2 = tmp_path / "second"
    tmp2.mkdir()
    # room for the two connectors but not for the demo warehouse beside them
    env2, _ = _fake_cluster(tmp2, FAKE_NODES=_nodes(lean_mem + 100, cpu + 500))
    cp2 = _run(tmp2, "--no-port-forward", RSYNC_CONNECTORS="postgresql,mongodb", RSYNC_DEMO="true", **env2)
    assert cp2.returncode == 0, cp2.stdout + cp2.stderr
    assert _values(tmp2)["demo"]["enabled"] is True, "an explicit RSYNC_DEMO=true was overridden"
    assert "Pending" in cp2.stdout, "nothing fits and the user was not told"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_cluster_too_small_even_for_the_lean_set_is_warned_not_refused(tmp_path):
    lean_mem, lean_cpu = _lean_need()
    env, log = _fake_cluster(tmp_path, FAKE_NODES=_nodes(lean_mem - 1024, lean_cpu + 500))
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert "Pending" in cp.stdout and "RSYNC_CONNECTORS=" in cp.stdout
    assert "Installing a lean set" not in cp.stdout
    # the defaults stay: a lean install that still does not fit is no better than the full one
    assert _fleet_ids(tmp_path) == _script_var("DEFAULT_CONNECTORS").split(",")
    assert any(l.startswith("helm upgrade") for l in log.read_text().splitlines())


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_what_other_pods_already_request_is_not_free(tmp_path):
    """The node is not empty: kube-system alone asks for ~300 MiB. Room for the chart's
    requests on an EMPTY node is not room on this one."""
    mem, cpu = _default_need()
    nodes = _nodes(mem + 100, cpu + 100)
    empty, _ = _fake_cluster(tmp_path, FAKE_NODES=nodes)
    assert "Installing a lean set" not in _run(tmp_path, "--no-port-forward", **empty).stdout

    tmp2 = tmp_path / "busy"
    tmp2.mkdir()
    busy, _ = _fake_cluster(
        tmp2,
        FAKE_NODES=nodes,
        FAKE_PODS="Running\\tkube-system\\t\\tn1\\t300Mi,100m;\\nRunning\\tkube-system\\tdns\\tn1\\t70Mi,100m;100Mi,\\n",
    )
    cp = _run(tmp2, "--no-port-forward", **busy)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert "Installing a lean set" in cp.stdout


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_this_releases_own_pods_and_finished_pods_do_not_count_against_it(tmp_path):
    """An upgrade replaces the release's pods, and a Succeeded hook Job holds no room."""
    mem, cpu = _default_need()
    env, _ = _fake_cluster(
        tmp_path,
        FAKE_NODES=_nodes(mem + 100, cpu + 100),
        FAKE_PODS=(
            "Running\\trsync\\trsync\\tn1\\t8000Mi,3000m;\\n"  # this release: replaced by the upgrade
            "Succeeded\\tother\\tjob\\tn1\\t4000Mi,2000m;\\n"  # finished
            "Pending\\tother\\tq\\t\\t4000Mi,2000m;\\n"  # not scheduled to a node
        ),
    )
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert "Installing a lean set" not in cp.stdout, cp.stdout


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_unreadable_nodes_skip_the_check_instead_of_failing_the_install(tmp_path):
    env, _ = _fake_cluster(tmp_path, FAKE_NODES_FAIL="1")
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert "skipping the capacity check" in cp.stdout + cp.stderr


# ---------------------------------------------------------------------------
# a failed install names its cause
# ---------------------------------------------------------------------------

PODS_LISTING = (
    "rsync-api-gateway-6d9-abc        1/1   Running     0   9m\\n"
    "rsync-postgres-0                 1/1   Running     0   9m\\n"
    "rsync-kafka-0                    1/1   Running     0   9m\\n"
    "rsync-orchestrator-77f-xyz       0/1   Pending     0   9m\\n"
    "rsync-kafka-init-q9x2p           0/1   Completed   0   3m\\n"
    "rsync-mcp-mongodb-v1-0-0-zzz     0/1   CrashLoopBackOff   4   9m\\n"
)


def _kubectl_lists_pods(tmp_path):
    """The fake `get pods -A` answers the capacity probe; a namespaced `get pods` is the failure listing."""
    kubectl = pathlib.Path(tmp_path / "fakebin" / "kubectl")
    kubectl.write_text(kubectl.read_text().replace(
        '  *"get events"*)', '  *"get pods --no-headers"*) printf \'%b\' "${FAKE_POD_LIST:-}" ;;\n  *"get events"*)'))


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_failed_install_lists_only_the_pods_that_are_not_ready(tmp_path):
    env, _ = _fake_cluster(tmp_path, FAKE_HELM_UPGRADE_FAILS="1", FAKE_POD_LIST=PODS_LISTING)
    _kubectl_lists_pods(tmp_path)
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode != 0
    listed = cp.stderr.split("Pods that are not ready:")[1].split("Look closer")[0]
    assert "rsync-orchestrator-77f-xyz" in listed and "rsync-mcp-mongodb-v1-0-0-zzz" in listed
    for healthy in ("rsync-api-gateway", "rsync-postgres-0", "rsync-kafka-0"):
        assert healthy not in listed, f"{healthy} is Running 1/1 and was listed as not ready:\n{listed}"
    assert "kafka-init" not in listed, "a Completed hook Job is not a failure"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_failed_install_names_an_out_of_room_cluster_as_the_cause(tmp_path):
    env, _ = _fake_cluster(
        tmp_path,
        FAKE_HELM_UPGRADE_FAILS="1",
        FAKE_POD_LIST=PODS_LISTING,
        FAKE_EVENTS=(
            "9m  Warning  FailedScheduling  pod/rsync-orchestrator-77f-xyz  "
            "0/1 nodes are available: 1 Insufficient memory. no new claims to deallocate"
        ),
    )
    _kubectl_lists_pods(tmp_path)
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode != 0
    err = cp.stderr
    assert "out of room" in err and "Insufficient" in err
    assert "rsync-orchestrator-77f-xyz" in err.split("out of room")[1], "the starved pod is not named"
    assert f"RSYNC_CONNECTORS={_script_var('LEAN_CONNECTORS')}" in err and "RSYNC_DEMO=false" in err


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_crash_loop_is_not_blamed_on_capacity(tmp_path):
    """Control: 'out of room' must only appear when the scheduler said so."""
    env, _ = _fake_cluster(
        tmp_path, FAKE_HELM_UPGRADE_FAILS="1", FAKE_POD_LIST=PODS_LISTING, FAKE_EVENTS="Back-off restarting failed container"
    )
    _kubectl_lists_pods(tmp_path)
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode != 0
    assert "out of room" not in cp.stdout + cp.stderr


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_registry_timeout_is_named_as_the_cause(tmp_path):
    """A slow ghcr.io shows up as `net/http: timeout awaiting response headers` on the pod's
    events while helm only says it timed out. The kubelet retries on its own, so the right
    advice is 'the network, re-run' -- not a hunt for a chart bug."""
    env, _ = _fake_cluster(
        tmp_path,
        FAKE_HELM_UPGRADE_FAILS="1",
        FAKE_EVENTS='Failed to pull image "ghcr.io/rsync-ai/frontend:0.1.3": '
        'Get "https://ghcr.io/v2/": net/http: timeout awaiting response headers',
    )
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode != 0
    err = cp.stderr
    assert "too slow to pull from" in err and "re-run this script" in err
    assert "CPU architecture" not in err, "a timeout is not an architecture mismatch"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_pull_that_failed_for_another_reason_is_not_called_a_timeout(tmp_path):
    """Control: `Failed to pull image` alone (denied, not found) is not the slow-registry hint."""
    env, _ = _fake_cluster(
        tmp_path,
        FAKE_HELM_UPGRADE_FAILS="1",
        FAKE_EVENTS='Failed to pull image "ghcr.io/rsync-ai/frontend:0.1.3": denied: requested access is denied',
    )
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode != 0
    assert "too slow to pull from" not in cp.stdout + cp.stderr


# ---------------------------------------------------------------------------
# Deployments must outlive a slow first pull
# ---------------------------------------------------------------------------


def _wait_seconds(spec: str) -> int:
    return int(spec[:-1]) * {"s": 1, "m": 60, "h": 3600}[spec[-1]]


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_every_deployment_outlives_the_installers_wait_timeout(tmp_path):
    """Helm 4's `--wait` fails the release the moment a Deployment reports
    ProgressDeadlineExceeded, whatever `--timeout` says. The chart set no deadline, so the
    Kubernetes default of 600s silently capped the installer's 15 minutes: one slow image
    pull and the install died at minute ten with "Progress deadline exceeded".

    Census over the rendered chart with EVERY optional workload switched on, so a Deployment
    added later without the field is this test's failure, not somebody's first install."""
    cp = _run(tmp_path, "--render-only", RSYNC_DEMO="true", RSYNC_LLM_PROVIDER="ollama")
    assert cp.returncode == 0, cp.stdout + cp.stderr
    docs = _render(tmp_path)
    deployments = [d for d in docs if d and d.get("kind") == "Deployment"]
    assert len(deployments) >= 10, f"only {len(deployments)} Deployments rendered -- the census is not seeing the chart"
    wait = _wait_seconds(_script_var_default("RSYNC_WAIT_TIMEOUT"))
    for d in deployments:
        name = d["metadata"]["name"]
        deadline = d["spec"].get("progressDeadlineSeconds")
        assert deadline is not None, f"{name} sets no progressDeadlineSeconds, so Kubernetes' 600s wins over --timeout"
        assert deadline > wait, f"{name}: progressDeadlineSeconds={deadline} <= the installer's {wait}s wait"


def _script_var_default(key: str) -> str:
    m = re.search(rf'cfg {key} ([0-9]+[smh])\)', SCRIPT.read_text(encoding="utf-8"))
    assert m, f"install-k8s.sh no longer defaults {key}"
    return m.group(1)


# ---------------------------------------------------------------------------
# the last screen: what a user is told, and whether the port-forwards really start
# ---------------------------------------------------------------------------


def _forwards(log) -> list:
    return [l for l in log.read_text().splitlines() if "port-forward" in l]


def _run_on_a_tty(tmp_path, *args, **extra):
    """`[[ -t 1 ]]` is what decides the default; a captured pipe can never make it true."""
    master, slave = pty.openpty()
    proc = subprocess.Popen(
        [BASH, str(SCRIPT), *args],
        env=_env(tmp_path, **extra),
        stdin=subprocess.DEVNULL,
        stdout=slave,
        stderr=subprocess.PIPE,
        text=True,
    )
    os.close(slave)
    out = b""
    while True:
        try:
            chunk = os.read(master, 4096)
        except OSError:  # EIO once the child closed its end
            break
        if not chunk:
            break
        out += chunk
    os.close(master)
    err = proc.stderr.read()
    proc.wait(timeout=60)
    return proc.returncode, out.decode(errors="replace"), err


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_without_a_terminal_no_forward_starts_and_the_two_commands_are_printed(tmp_path):
    """CI, cron and `ssh host 'curl … | bash'` have no tty: a forward there would either hang
    the script or die with it. The user is handed the exact commands instead."""
    env, log = _fake_cluster(tmp_path)
    cp = _run(tmp_path, **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert _forwards(log) == [], f"a port-forward started without a terminal:\n{log.read_text()}"
    assert re.search(r"kubectl -n rsync port-forward svc/rsync-frontend 3000:\d+", cp.stdout)
    assert re.search(r"kubectl -n rsync port-forward svc/rsync-api-gateway 8080:\d+", cp.stdout)


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_port_forward_flag_starts_both_forwards_without_a_terminal(tmp_path):
    env, log = _fake_cluster(tmp_path)
    cp = _run(tmp_path, "--port-forward", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    fw = _forwards(log)
    assert len(fw) == 2, fw
    assert any("svc/rsync-frontend 3000:" in l for l in fw) and any("svc/rsync-api-gateway 8080:" in l for l in fw)
    assert "unbound variable" not in cp.stderr, cp.stderr


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_on_a_terminal_both_forwards_start_by_default_and_the_script_exits_clean(tmp_path):
    """The laptop path. `pids` is local to finish(), the EXIT trap fires after it returns, and
    the script runs under `set -u` -- so this also proves the trap does not blow up on exit."""
    env, log = _fake_cluster(tmp_path)
    rc, out, err = _run_on_a_tty(tmp_path, **env)
    assert rc == 0, out + err
    assert len(_forwards(log)) == 2, log.read_text()
    assert "Opening port-forwards" in out
    assert "unbound variable" not in err, err


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_on_a_terminal_the_no_port_forward_flag_still_wins(tmp_path):
    env, log = _fake_cluster(tmp_path)
    rc, out, err = _run_on_a_tty(tmp_path, "--no-port-forward", **env)
    assert rc == 0, out + err
    assert _forwards(log) == []


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_published_install_never_forwards_and_never_calls_itself_laptop_only(tmp_path):
    """With hostnames the UI is reached through the ingress; a localhost forward would be noise
    and the 'laptop-only address' line would be false."""
    env, log = _fake_cluster(tmp_path)
    rc, out, err = _run_on_a_tty(tmp_path, RSYNC_APP_HOST="app.example.com", RSYNC_API_HOST="api.example.com", **env)
    assert rc == 0, out + err
    assert _forwards(log) == []
    assert "laptop-only" not in out


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_no_llm_warning_does_not_overstate_what_chat_needs(tmp_path):
    """Without a model the gateway still turns a short 'X to Y' request into a pipeline
    (quickParseDataSyncIntent, pinned by chat_intent_llm_not_configured_test.go), so 'chat is how
    pipelines are created' with no qualifier sent people to add a key they did not need for the
    first run. The warning must say what stops working, and the example must be one the fast
    path really parses."""
    env, _ = _fake_cluster(tmp_path)
    cp = _run(tmp_path, "--no-port-forward", **env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert "No LLM is configured" in cp.stdout + cp.stderr
    assert "free-form" in cp.stdout
    example = re.search(r'chatNoLLMExample\s*=\s*"([^"]+)"', (REPO / "api-gateway/internal/handlers/chat_nl_pipeline.go").read_text())
    assert example and f'"{example.group(1)}"' in cp.stdout, "the installer's example is not the gateway's"


def test_the_installer_ships_in_the_public_cut():
    """The documented `curl …/install-k8s.sh` is served from the PUBLIC repo, which is cut from
    this one by removing the paths in scripts/flip/excludes.txt. Parsed the way the runbook's
    `rm -rf` loop parses it (comments stripped, blanks dropped); a listed parent directory would
    drop the file just as surely as listing it.

    The subject is the cut list itself, which the cut deletes (`scripts/flip` is on it), so on
    the public tree there is nothing to read and the test skips -- keyed on that file, not on an
    env var or the repository name. The private tree is where it fails."""
    _flip_cut.require_a_pre_cut_tree()
    listed = [
        re.sub(r"[ \t]", "", l.split("#", 1)[0])
        for l in (REPO / "scripts/flip/excludes.txt").read_text().splitlines()
    ]
    listed = [p.rstrip("/") for p in listed if p]
    assert listed, "excludes.txt parsed empty -- the guard is not reading the cut list"
    assert SCRIPT.is_file()
    for p in listed:
        assert p != "install-k8s.sh" and not "install-k8s.sh".startswith(p + "/"), (
            f"scripts/flip/excludes.txt removes install-k8s.sh (via '{p}'): the public README's "
            "Kubernetes install command would 404 again"
        )
    assert SCRIPT.stat().st_mode & stat.S_IXUSR, "install-k8s.sh lost its executable bit"
