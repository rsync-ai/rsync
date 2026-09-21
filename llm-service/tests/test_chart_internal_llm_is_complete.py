"""The chart's bundled Ollama, and the model that has to be inside it.

`templates/infra/ollama.yaml` is the Kubernetes half of `docker-compose.ollama.yml`:
the deployment runs fully offline, with no OpenAI key and no Ollama on anyone's
host. Starting a server is the easy half. Getting a model INTO it is the half
that fails silently -- Ollama answers every request with
`model "<name>" not found, try pulling it first`, the Python tier surfaces that
as a failed generation, and every pod around it stays Ready with /ready 200.
Same defect class as the manual CREATE DATABASE this release removed from the
Postgres paths: a step nothing executable knew about, standing between a green
install and a working one.

`templates/jobs/ollama-pull.yaml` closes it. The properties below are what make
it close, and three of them are specific to Kubernetes rather than compose:

  * Compose gets "wait for the server" free from `condition: service_healthy`.
    Helm without --wait fires post-install hooks as soon as the main resources
    are CREATED, not once they are Ready, so the pull script carries its own
    bounded wait loop or it races a server that is still pulling its image.
  * The image sets no HOME and no USER. Under Docker the runtime supplies /root,
    which is why the compose overlay mounts /root/.ollama; under this chart's
    runAsUser: 1000 there is no such gift, and `ollama serve` calls
    initializeKeypair() FIRST and returns its error (cmd/cmd.go:2015-2016), so an
    unwritable HOME does not degrade the server, it refuses to boot.
  * A model name is one key, ollama.model, read on BOTH sides of ollama.enabled
    -- the same shape as postgresql.database naming the database whoever hosts
    it. Reading generation.llm.model instead renders OLLAMA_MODEL="gpt-4o" and
    asks a BYO Ollama for a cloud model: the exact `model not found` this whole
    file exists to eliminate, reintroduced on the arm that has no pull hook to
    make it obvious.

Two layers, matching test_chart_docker_host_features_are_off.py:

  1. the static layer runs everywhere, with or without a helm binary.
  2. the render layer runs `helm template` and asserts on real output. It skips
     without helm -- and a skip is not a pass, which is why every property that
     can be pinned statically is pinned statically as well.
"""

import pathlib
import re
import shutil
import subprocess

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
CHART = REPO / "deploy" / "helm" / "rsync-ai"
VALUES = CHART / "values.yaml"
HELPERS = CHART / "templates" / "_helpers.tpl"
SERVER_TPL = CHART / "templates" / "infra" / "ollama.yaml"
PULL_TPL = CHART / "templates" / "jobs" / "ollama-pull.yaml"
GENERATION = CHART / "templates" / "apps" / "generation.yaml"
VALIDATE = CHART / "templates" / "validate.yaml"
COMPOSE_OVERLAY = REPO / "docker-compose.ollama.yml"

# Helm comments carry the reasoning and would otherwise satisfy a grep for the
# very thing they explain. Strip them before asserting on template BODIES.
_HELM_COMMENT = re.compile(r"\{\{-?/\*.*?\*/-?\}\}", re.DOTALL)

# The two generation services that resolve a model name and ask an LLM for it.
# tool-generator is deliberately NOT one: its chart entrypoint is
# src.lifecycle.main, the connector-LIFECYCLE service, not the generation service
# (generation.yaml's header). Adding LLM env to it would be cargo cult.
ASKERS = ("llm-service", "planner")
NON_ASKER = "tool-generator"

# The documented install command's flags, so a render reaches this file's subject
# instead of stopping at an unrelated required-value check. Fakes throughout.
RENDER_FLAGS = [
    "--set", "secrets.jwtSecret=FAKEPLACEHOLDER",
    "--set", "secrets.encryptionKey=FAKEPLACEHOLDER",
    "--set", "secrets.postgresPassword=FAKEPLACEHOLDER",
    "--set", "secrets.minioAccessKey=FAKEPLACEHOLDER",
    "--set", "secrets.minioSecretKey=FAKEPLACEHOLDER",
    "--set", "frontend.publicUrl=https://app.example.com",
    "--set", "frontend.apiUrl=https://api.example.com",
]


def _body(path):
    return _HELM_COMMENT.sub("", path.read_text())


def _values():
    return yaml.safe_load(VALUES.read_text())


def _helm_template(*extra):
    return subprocess.run(
        ["helm", "template", "r", str(CHART), *RENDER_FLAGS, *extra],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(REPO),
    )


def _render(*extra):
    proc = _helm_template(*extra)
    assert proc.returncode == 0, (
        f"helm template {' '.join(extra)} failed:\n{proc.stderr[-3000:]}"
    )
    docs = [d for d in yaml.safe_load_all(proc.stdout) if d]
    assert len(docs) >= 20, (
        f"only {len(docs)} documents rendered -- refusing to report a pass on a "
        "denominator this small; the render is not exercising the chart."
    )
    return docs


def _containers(docs, kinds=("Deployment", "StatefulSet", "Job")):
    """{container name: container dict} across every workload in a render."""
    out = {}
    for d in docs:
        if d.get("kind") in kinds:
            for c in d["spec"]["template"]["spec"]["containers"]:
                out[c["name"]] = c
    return out


def _env(container):
    return {e["name"]: e.get("value") for e in container.get("env", [])}


def _named(docs, kind, suffix):
    hits = [
        d for d in docs
        if d.get("kind") == kind and d["metadata"]["name"].endswith(suffix)
    ]
    assert len(hits) == 1, (
        f"expected exactly one {kind} whose name ends in {suffix!r}, got "
        f"{[d['metadata']['name'] for d in hits]}"
    )
    return hits[0]


# ---------------------------------------------------------------------------
# static layer -- needs no helm, so it runs anywhere
# ---------------------------------------------------------------------------


def test_the_chart_ships_a_server_and_a_puller():
    """Deleting either template is the whole defect, so name both explicitly."""
    for path in (SERVER_TPL, PULL_TPL):
        assert path.is_file(), (
            f"{path.relative_to(REPO)} is gone. The bundled-Ollama path needs a "
            "server AND something that puts a model in it; one without the "
            "other is a stack that comes up green and answers every prompt "
            "'model not found'."
        )


def test_both_templates_are_gated_on_one_switch():
    """One `enabled`, or an operator gets half the component."""
    for path in (SERVER_TPL, PULL_TPL):
        assert "if .Values.ollama.enabled" in _body(path), (
            f"{path.relative_to(REPO)} is not gated on ollama.enabled. Either it "
            "renders for installs that did not ask for it, or -- worse -- only "
            "one of the two is gated and enabling the feature starts a server "
            "with no puller."
        )


def test_the_puller_is_a_hook_that_reruns_on_every_upgrade():
    """A plain Job cannot be re-applied: Job specs are immutable.

    kafka-init carries the same three annotations for the same reason. Dropping
    post-upgrade means a `helm upgrade --set ollama.model=<something else>`
    changes the env of every asker and downloads nothing.
    """
    text = _body(PULL_TPL)
    assert "helm.sh/hook: post-install,post-upgrade" in text, (
        "the pull Job is not a post-install AND post-upgrade hook"
    )
    assert "before-hook-creation" in text, (
        "without before-hook-creation the second install hits a Job that already "
        "exists, and Job specs are immutable -- the upgrade fails on the hook "
        "rather than re-pulling."
    )


def test_the_puller_waits_for_the_server_itself():
    """Compose gets this from `condition: service_healthy`. Helm does not.

    Without --wait, post-install hooks fire once the main resources are CREATED,
    not once they are Ready. A pull that assumes a live server races the image
    pull of a multi-GB runtime and fails on connection refused.
    """
    script = _body(PULL_TPL)
    assert "until ollama list" in script, (
        "the pull script does not wait for the server to answer before pulling"
    )
    assert "did not answer" in script, (
        "the wait loop does not bound itself with a message naming the server -- "
        "an unbounded wait is a hook that hangs the install with no explanation"
    )


def test_the_puller_does_not_write_the_models_volume_itself():
    """It is a CLIENT. A second writer on the volume races the server."""
    text = _body(PULL_TPL)
    assert "volumeMounts" not in text and "volumeClaimTemplates" not in text, (
        "the pull Job mounts storage. It should not: both of its commands are "
        "HTTP calls to the server container, which does the downloading into its "
        "own volume."
    )
    assert "OLLAMA_HOST" in text, (
        "the pull Job does not point its CLI at the server, so `ollama pull` "
        "looks for a server inside its own container and reports a connection "
        "error that reads like the server is down when it is up next door."
    )


def test_the_server_gets_a_writable_home_and_stores_models_there():
    """HOME is load-bearing twice, and OLLAMA_MODELS must stay unset.

    `ollama serve` calls initializeKeypair() first and returns its error
    (cmd/cmd.go:2015-2016), writing $HOME/.ollama/id_ed25519 -- so an unwritable
    HOME is a server that refuses to boot, not one that degrades. Models() then
    reads OLLAMA_MODELS first and otherwise $HOME/.ollama/models
    (envconfig/config.go:113-124), which is why setting OLLAMA_MODELS as well
    would split the two onto different volumes.
    """
    text = _body(SERVER_TPL)
    assert "name: HOME" in text, "the server sets no HOME"
    home = re.search(r"name: HOME\s*\n\s*value: (\S+)", text)
    assert home, "HOME is set from something other than a literal value"
    path = home.group(1).strip('"')
    assert f"mountPath: {path}" in text, (
        f"HOME is {path} but nothing is mounted there. The keypair write that "
        "gates `ollama serve` would land on the container filesystem, and the "
        "model would not survive a restart."
    )
    assert "OLLAMA_MODELS" not in text, (
        "OLLAMA_MODELS is set. Leave it unset: HOME already places the models, "
        "and two knobs for one path is how they end up disagreeing."
    )


def test_the_server_runs_exactly_one_replica():
    """Each StatefulSet replica gets its OWN PVC; the pull fills exactly one."""
    text = _body(SERVER_TPL)
    assert re.search(r"^\s*replicas: 1\s*$", text, re.M), (
        "the Ollama StatefulSet's replica count is not the literal 1. It must "
        "not become a values key: the pull hook fills one PVC, the headless "
        "Service load-balances across all of them, and replicaCount: 3 buys a "
        "2-in-3 failure rate that looks like an intermittent model bug."
    )


def test_the_pull_script_parses():
    """A shell typo in a hook is a failed install, discovered at install time."""
    text = PULL_TPL.read_text()
    # Everything between the `- |` block scalar and the closing {{- end }}.
    m = re.search(r"\n(\s+)- \|\n(.*?)\n\{\{- end \}\}", text, re.DOTALL)
    assert m, "could not find the pull Job's inline script block"
    indent = len(m.group(1)) + 2
    script = "\n".join(line[indent:] for line in m.group(2).split("\n"))
    assert len(script) > 400, (
        f"extracted only {len(script)} bytes of script -- refusing to report a "
        "pass on a fragment; `sh -n` prints OK for an empty string."
    )
    # The Go templating is not shell; blank it out before the syntax check.
    script = re.sub(r"\{\{.*?\}\}", "X", script)
    proc = subprocess.run(
        ["sh", "-n"], input=script, capture_output=True, text=True, timeout=30
    )
    assert proc.returncode == 0, f"pull script is not valid sh:\n{proc.stderr}"


def test_a_failed_pull_reports_ollamas_own_words():
    """A typo'd tag, a dead registry and a full disk end this loop identically."""
    script = _body(PULL_TPL)
    assert "$ERR" in script, (
        "the pull script discards ollama's stderr. Without it the operator sees "
        "only 'could not pull', and the three causes are indistinguishable."
    )
    assert "ollama.com/library" in script, (
        "the failure message does not tell the operator where to check the name"
    )


def test_one_model_key_feeds_the_puller_and_every_asker():
    """The helper is the single resolver; nothing reads the raw key twice."""
    for path in (PULL_TPL, GENERATION):
        assert 'include "rsync-ai.ollama.model"' in _body(path), (
            f"{path.relative_to(REPO)} does not resolve the model through the "
            "helper. Two resolvers is how the pull downloads one model and the "
            "pods ask for another."
        )
    gen = _body(GENERATION)
    ollama_block = gen.split('include "rsync-ai.ollama.url"', 1)[1].split("{{- end }}", 1)[0]
    assert "generation.llm.model" not in ollama_block, (
        "the Ollama env block reads generation.llm.model directly. That key "
        "defaults to gpt-4o, so this renders OLLAMA_MODEL=\"gpt-4o\" and asks "
        "Ollama for a cloud model no server has pulled."
    )


def test_the_model_key_is_read_on_both_sides_of_enabled():
    """ollama.model names the model whoever hosts the server -- like postgresql.database.

    This is the regression that motivated the test: while the helper branched on
    ollama.enabled, a BYO-Ollama install fell through to generation.llm.model and
    rendered OLLAMA_MODEL="gpt-4o". No pull hook runs on that arm, so nothing
    downloads and nothing complains until the first prompt.
    """
    helper = _HELM_COMMENT.sub("", HELPERS.read_text())
    m = re.search(
        r'\{\{- define "rsync-ai\.ollama\.model" -\}\}(.*?)\{\{- end -\}\}',
        helper,
        re.DOTALL,
    )
    assert m, "the rsync-ai.ollama.model helper is gone"
    body = m.group(1)
    assert "ollama.enabled" not in body, (
        "rsync-ai.ollama.model branches on ollama.enabled again. The model name "
        "applies to a BYO Ollama exactly as much as to the bundled one."
    )
    assert "generation.llm.model" not in body, (
        "rsync-ai.ollama.model falls back to generation.llm.model, whose default "
        "is the OpenAI model gpt-4o."
    )


def test_the_chart_and_the_compose_overlay_pin_the_same_runtime():
    """Two hand-kept files; a version skew between them is a support puzzle."""
    chart_image = _values()["ollama"]["image"]
    compose = yaml.safe_load(COMPOSE_OVERLAY.read_text())
    compose_image = compose["services"]["ollama"]["image"]
    assert chart_image == compose_image, (
        f"chart pins {chart_image}, compose overlay pins {compose_image}. They "
        "are hand-kept in step because install.sh downloads compose files with "
        "no repo checkout and the chart ships as a separate OCI artifact."
    )
    assert ":" in chart_image and not chart_image.endswith(":latest"), (
        f"{chart_image} is unpinned. A model runtime that changes under a "
        "re-pull changes inference behaviour, not just the binary."
    )


def test_the_ollama_defaults_are_off_and_sized():
    v = _values()["ollama"]
    assert v["enabled"] is False, (
        "ollama.enabled defaults on. This is the single largest thing the chart "
        "can start (~4.7 GB on disk, ~5 GB resident) and most installs bring a "
        "cloud key instead."
    )
    assert "limits" not in v["resources"], (
        "the Ollama container has a memory limit. Its working set IS the model: "
        "a cap below it does not slow inference, it OOM-kills the container "
        "mid-answer. Size the node."
    )
    assert v["model"], "ollama.model is empty -- the pull hook would have no subject"


def test_provider_ollama_with_no_ollama_is_refused_in_the_template():
    """Static half of the render-layer guard test below."""
    text = _body(VALIDATE)
    assert 'eq .Values.generation.llm.provider "ollama"' in text, (
        "validate.yaml no longer guards the Ollama provider. Without it, "
        "provider=ollama with no server anywhere resolves to the client's own "
        "default, http://host.docker.internal:11434 -- a Docker Desktop name "
        "that resolves nowhere in a pod. The pods start and pass their probes."
    )
    assert "host.docker.internal" in text, (
        "the guard does not name the address the pods would otherwise use, "
        "which is the one fact that makes the failure diagnosable."
    )


# ---------------------------------------------------------------------------
# render layer -- runs `helm template`; skips without helm, and a skip is not a
# pass, which is why every property above is pinned statically too.
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_default_chart_still_renders():
    """Anti-vacuity for everything below: the baseline must be a real render."""
    docs = _render()
    names = {d["metadata"]["name"] for d in docs}
    assert not any("ollama" in n for n in names), (
        f"the default render contains Ollama resources: {sorted(n for n in names if 'ollama' in n)}"
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_no_ollama_env_reaches_the_pods_when_there_is_no_ollama():
    """Empty is falsy to `with`: render nothing rather than a dead address."""
    containers = _containers(_render())
    for name in ASKERS:
        env = _env(containers[name])
        leaked = sorted(k for k in env if k.startswith("OLLAMA_"))
        assert not leaked, (
            f"{name} carries {leaked} on an install with no Ollama at all. An "
            "address that resolves nowhere is worse than no address: the client "
            "prefers OLLAMA_BASE_URL over its own default."
        )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_bundled_ollama_renders_a_server_a_puller_and_the_env():
    docs = _render("--set", "ollama.enabled=true", "--set", "generation.llm.provider=ollama")
    sts = _named(docs, "StatefulSet", "-ollama")
    svc = _named(docs, "Service", "-ollama")
    job = _named(docs, "Job", "-ollama-pull")

    assert svc["spec"]["clusterIP"] == "None", "the Ollama Service is not headless"
    assert sts["spec"]["serviceName"] == svc["metadata"]["name"]

    model = _values()["ollama"]["model"]
    assert _env(job["spec"]["template"]["spec"]["containers"][0])["OLLAMA_MODEL"] == model

    containers = _containers(docs)
    for name in ASKERS:
        env = _env(containers[name])
        assert env["LLM_MODEL"] == model, (
            f"{name} asks for {env['LLM_MODEL']!r} while the pull hook downloads "
            f"{model!r}. get_default_model() reads LLM_MODEL first."
        )
        assert env["OLLAMA_MODEL"] == model, (
            f"{name} leaves the other three resolvers in openai_client.py on "
            "their own defaults (llama3:latest, sqlcoder:latest) -- models the "
            "hook never downloaded, so the Data Explorer 404s beside a working "
            "chat."
        )
        # The URL is the helper's output, not a guessable prefix: fullname
        # collapses when the release name already contains the chart name, so
        # `helm template r` renders r-ollama, not r-rsync-ai-ollama.
        assert env["OLLAMA_URL"] == f"http://{svc['metadata']['name']}:11434"
        assert env["OLLAMA_BASE_URL"] == env["OLLAMA_URL"], (
            "OLLAMA_BASE_URL wins over OLLAMA_URL in the client "
            "(openai_client.py:67-76); setting only the losing one is a no-op."
        )
    assert not any(
        k.startswith("OLLAMA_") for k in _env(containers[NON_ASKER])
    ), (
        f"{NON_ASKER} carries Ollama env. Its chart entrypoint is "
        "src.lifecycle.main, the connector-LIFECYCLE service, which asks no LLM "
        "for anything."
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_byo_ollama_is_asked_for_the_model_it_actually_serves():
    """The arm with no pull hook to make a wrong name obvious.

    Nothing downloads here, so a model name the operator did not choose produces
    a 404 on the first prompt and no other signal anywhere.
    """
    docs = _render(
        "--set", "generation.llm.provider=ollama",
        "--set", "generation.llm.ollamaUrl=http://my-ollama:11434",
        "--set", "ollama.model=llama3.1:8b",
    )
    assert not [d for d in docs if "ollama" in d["metadata"]["name"]], (
        "a BYO-Ollama install rendered Ollama resources; ollama.enabled is false"
    )
    containers = _containers(docs)
    for name in ASKERS:
        env = _env(containers[name])
        assert env["OLLAMA_URL"] == "http://my-ollama:11434"
        assert env["OLLAMA_BASE_URL"] == "http://my-ollama:11434"
        assert env["LLM_MODEL"] == "llama3.1:8b", (
            f"{name} asks a BYO Ollama for {env['LLM_MODEL']!r}. If that is "
            "gpt-4o, ollama.model stopped being read when ollama.enabled is "
            "false and the chart is requesting a cloud model from Ollama."
        )
        assert env["OLLAMA_MODEL"] == "llama3.1:8b"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_a_bundled_ollama_under_a_cloud_provider_keeps_the_cloud_model():
    """Running an Ollama and using one are separate decisions.

    ollama.enabled=true with provider=openai is the documented way to pin ONLY
    the Data Explorer offline: schema metadata stays in the cluster while chat
    keeps the cloud model. LLM_MODEL must not be rewritten here.
    """
    docs = _render("--set", "ollama.enabled=true")
    containers = _containers(docs)
    cloud_model = _values()["generation"]["llm"]["model"]
    local_model = _values()["ollama"]["model"]
    for name in ASKERS:
        env = _env(containers[name])
        assert env["LLM_PROVIDER"] == "openai"
        assert env["LLM_MODEL"] == cloud_model, (
            f"{name} had its cloud model rewritten to {env['LLM_MODEL']!r} "
            "merely because an Ollama is running beside it."
        )
        assert env["OLLAMA_MODEL"] == local_model
        assert env["OLLAMA_URL"].endswith("-ollama:11434")


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_provider_ollama_with_no_ollama_fails_the_install():
    """Anti-vacuity for the guard: it must actually stop a render."""
    proc = _helm_template("--set", "generation.llm.provider=ollama")
    assert proc.returncode != 0, (
        "provider=ollama with neither ollama.enabled nor generation.llm.ollamaUrl "
        "rendered successfully. The pods would resolve their base URL to "
        "http://host.docker.internal:11434 and fail every prompt with a DNS "
        "error, on a platform whose only pipeline-creation path is /chat."
    )
    assert "ollama.enabled=true" in proc.stderr and "ollamaUrl" in proc.stderr, (
        f"the guard fires but names neither remedy:\n{proc.stderr[-2000:]}"
    )
