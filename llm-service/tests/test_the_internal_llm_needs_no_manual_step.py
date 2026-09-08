"""The bundled LLM has to work with no manual step, and it is three files wide.

The offline path -- ``LLM_PROVIDER=ollama`` with nothing on the host -- used to
come up green and answer every prompt with an error. Two independent reasons,
both silent:

1. **Nothing pulled a model.** ``docker-compose.ollama.yml`` started an empty
   Ollama and left ``docker exec rsync-ollama ollama pull ...`` to the operator,
   in a comment. Ollama answers a request for a model it does not have with
   ``model "..." not found, try pulling it first``, so every container stays
   ``running`` and ``/ready`` keeps returning 200 while nothing works.
2. **The model that got pulled was not the model the code asked for.** Four
   functions in ``src/utils/openai_client.py`` pick a model name for the Ollama
   provider, and left alone they pick three different ones -- while a pull job
   downloads exactly one. The quickstart's own ``LLM_MODEL: ${LLM_MODEL:-gpt-4o}``
   default made that worse rather than better: layered by hand, it handed the
   container an OpenAI catalog name to ask a local Ollama for.

Both are invisible to every other test in this tree, because both leave the
compose valid, the containers healthy and the imports importable. So they are
pinned here, against the real files, in the direction that matters: whatever
``ollama-pull`` downloads is what all four resolvers ask for.

This file is also named by ``install.sh``. That script fetches every compose
file from the pinned release ref except this one overlay, and the comment
explaining why cites this test by path as the thing that keeps the exception
safe -- so the scoping checks below are load-bearing, not decoration.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess

import pytest
import yaml

from src.utils.openai_client import (
    explorer_default_model,
    explorer_default_sql_model,
    get_default_model,
    rank_tables_default_model,
)

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
OVERLAY = os.path.join(REPO_ROOT, "docker-compose.ollama.yml")
QUICKSTART = os.path.join(REPO_ROOT, "docker-compose.quickstart.yml")
INSTALL_SH = os.path.join(REPO_ROOT, "install.sh")

# The run-once job that puts the model on disk, and the three services that must
# not start before it succeeds. Named rather than derived: the point of the test
# is that these three keep waiting, so deriving the list from the same file
# would make a deletion look like a pass.
PULL_JOB = "ollama-pull"
SERVER = "ollama"
DEPENDENTS = ("llm-service", "planner", "tool-generator")

# Every var the quickstart guards with `:?` must have a value or
# `docker compose config` renders nothing at all.
FAKE_ENV = {
    "ENCRYPTION_KEY": "FAKEPLACEHOLDER",
    "JWT_SECRET": "FAKEPLACEHOLDER",
    "MINIO_ACCESS_KEY": "FAKEPLACEHOLDER",
    "MINIO_SECRET_KEY": "FAKEPLACEHOLDER",
    "POSTGRES_PASSWORD": "FAKEPLACEHOLDER",
    "REDIS_PASSWORD": "FAKEPLACEHOLDER",
}

# The keys the four resolvers read, each named with the function that reads it.
# Cleared before every case so a developer's own shell cannot supply one of them
# and turn a broken render green.
MODEL_KEYS = (
    "LLM_MODEL",  # get_default_model, explorer_default_model
    "OLLAMA_MODEL",  # get_default_model, explorer_default_model, explorer_default_sql_model
    "RANK_TABLES_MODEL",  # rank_tables_default_model
    "AZURE_OPENAI_DEPLOYMENT",  # not on this path; cleared so it cannot leak in
)

# Not a resolver function and not in MODEL_KEYS: a comma-separated LIST the
# Explorer's NL->SQL retry loop walks, read straight from the environment at the
# call site rather than through openai_client.
SQL_FALLBACK_KEY = "EXPLORER_SQL_FALLBACK_MODELS"
GATEWAY_MAIN = os.path.join(REPO_ROOT, "llm-service", "src", "gateway", "main.py")

RESOLVERS = (
    ("get_default_model", get_default_model),
    ("explorer_default_model", explorer_default_model),
    ("explorer_default_sql_model", explorer_default_sql_model),
    ("rank_tables_default_model", rank_tables_default_model),
)

# What an operator can set before running the bundled path. The empty case is
# the one that matters most: it is what the documented hand-run command does.
OPERATOR_ENVS = (
    ("sets nothing", {}),
    ("sets LLM_MODEL", {"LLM_MODEL": "llama3.1:8b"}),
    ("sets OLLAMA_MODEL only", {"OLLAMA_MODEL": "mistral:7b"}),
)


def _load(path):
    with open(path) as fh:
        return yaml.safe_load(fh)


def _services(doc):
    return doc.get("services") or {}


def _expand(value, env):
    """Resolve Compose's ``${VAR:-default}`` / ``${VAR-default}``, nesting included.

    The overlay's model name is one nested expression, and the whole point of
    the cases below is which branch of it wins -- so the reader has to be real
    rather than a lookup of the literal. It is checked against Compose itself in
    ``test_compose_resolves_the_model_name_the_way_this_file_reads_it``, because
    a wrong reader here would agree with itself forever.
    """
    out = []
    i = 0
    while i < len(value):
        if not value.startswith("${", i):
            out.append(value[i])
            i += 1
            continue
        depth = 0
        j = i + 1
        while j < len(value):
            if value[j] == "{":
                depth += 1
            elif value[j] == "}":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        assert j < len(value), f"unbalanced ${{ in {value!r}"
        m = re.match(r"([A-Za-z_][A-Za-z0-9_]*)(:-|-)?(.*)$", value[i + 2 : j], re.S)
        assert m, f"unsupported interpolation {value[i:j + 1]!r}"
        name, op, default = m.group(1), m.group(2), m.group(3)
        got = env.get(name)
        if op == ":-":
            resolved = _expand(default, env) if not got else got
        elif op == "-":
            resolved = _expand(default, env) if name not in env else (got or "")
        else:
            # A bare ${VAR}; a `:?` guard also lands here, and on this path none
            # of the model keys carries one.
            resolved = got or ""
        out.append(resolved)
        i = j + 1
    return "".join(out)


def _container_env(service, operator_env):
    """The model keys a service really receives once the overlay is layered.

    Merged in compose order -- quickstart, then overlay -- because that is the
    half that bites: the quickstart supplies a default for LLM_MODEL, and the
    overlay only wins if it names the key at all.
    """
    merged = _merged_env(service)
    return {k: _expand(str(merged[k]), operator_env) for k in MODEL_KEYS if k in merged}


def _merged_env(service):
    """The raw environment mapping a service ends up with, quickstart then overlay."""
    merged = {}
    for path in (QUICKSTART, OVERLAY):
        body = _services(_load(path)).get(service) or {}
        env = body.get("environment") or {}
        assert isinstance(env, dict), (
            f"{service} in {os.path.basename(path)} uses list-form environment; "
            "this reader only handles the mapping form"
        )
        merged.update(env)
    return merged


def _pulled_model(operator_env):
    env = _services(_load(OVERLAY))[PULL_JOB]["environment"]
    return _expand(str(env["OLLAMA_MODEL"]), operator_env)


def _install_sh():
    with open(INSTALL_SH) as fh:
        return fh.read()


# ── The scoping exception install.sh takes on this one file ──────────────────


def test_the_overlay_pins_no_image_the_release_ref_controls():
    """install.sh takes this file off `main` while every other compose file comes
    from the pinned tag. That is only safe while the file has no half that can
    drift against the images RSYNC_VERSION pulls -- which means no rsync image
    and no version interpolation anywhere in it.
    """
    text = open(OVERLAY).read()
    assert "ghcr.io/rsync-ai" not in text, (
        "the overlay names an rsync image. install.sh fetches this file from an "
        "unpinned ref, so that image would float free of the pinned release."
    )
    assert "RSYNC_VERSION" not in text, (
        "the overlay interpolates RSYNC_VERSION, which install.sh derives from "
        "the PINNED ref -- an unpinned file must not consume a pinned variable."
    )
    images = [
        (name, (body or {}).get("image"))
        for name, body in _services(_load(OVERLAY)).items()
        if (body or {}).get("image")
    ]
    assert images, "no service in the overlay declares an image -- the check was vacuous"
    off = [(n, i) for n, i in images if not str(i).startswith("ollama/ollama:")]
    assert not off, f"the overlay declares images this exception does not cover: {off}"


def test_every_service_the_overlay_extends_exists_in_the_quickstart():
    """The other half of the same exception.

    A service the overlay extends but the PINNED quickstart does not define
    becomes a service with no image, and Compose refuses the whole project. The
    services the overlay *adds* carry their own image; the ones it extends must
    already be there.
    """
    quickstart = set(_services(_load(QUICKSTART)))
    extends, adds = [], []
    for name, body in _services(_load(OVERLAY)).items():
        (adds if (body or {}).get("image") else extends).append(name)
    assert extends and adds, f"expected both kinds of service; got extends={extends} adds={adds}"
    orphans = [n for n in extends if n not in quickstart]
    assert not orphans, (
        f"these services are extended by the overlay but defined nowhere: {orphans}. "
        "Layered on the pinned quickstart, Compose rejects the entire project."
    )
    collisions = [n for n in adds if n in quickstart]
    assert not collisions, (
        f"these services carry their own image AND exist in the quickstart: {collisions}. "
        "The overlay would silently rewrite a service it does not own."
    )


def test_the_installer_takes_only_this_one_file_off_the_unpinned_ref():
    """The exception must stay one file wide.

    install.sh downloads compose files from two bases -- RAW_BASE, built from
    the pinned release ref, and OLLAMA_RAW_BASE, which is not pinned. Every
    fetch from the unpinned base must name this overlay and nothing else.
    """
    fetches = re.findall(r'fetch\s+"\$\{(\w+)\}/\$\{(\w+)\}"', _install_sh())
    assert fetches, "no `fetch \"${BASE}/${FILE}\"` calls found in install.sh -- the shape changed"
    unpinned = [f for f in fetches if f[0] == "OLLAMA_RAW_BASE"]
    assert unpinned, "install.sh fetches nothing from OLLAMA_RAW_BASE -- the overlay is not delivered"
    stray = [f for f in unpinned if f[1] != "OLLAMA_FILE"]
    assert not stray, (
        f"these files are fetched off the UNPINNED ref: {stray}. Only the Ollama "
        "overlay is exempt, and only because it pins no rsync image."
    )
    assert not [f for f in fetches if f[0] == "RAW_BASE" and f[1] == "OLLAMA_FILE"], (
        "the overlay is also fetched from the pinned base; the two copies would "
        "race and the last fetch would win silently"
    )


def test_the_installers_staleness_guard_names_a_service_this_overlay_defines():
    """A re-run has to repair an install dir that holds the OLD overlay.

    `[[ -f ]]` cannot: the stale file exists, so the guard reads it as done and
    the operator is left exactly where they started. install.sh therefore greps
    the file for the pull job by name -- which silently stops detecting anything
    the moment that service is renamed here.
    """
    m = re.search(r"grep -qE '\^\[\[:space:\]\]\*(\w[\w-]*):'", _install_sh())
    assert m, "install.sh no longer greps the overlay for a service key -- the guard changed shape"
    assert m.group(1) == PULL_JOB, (
        f"install.sh's re-fetch guard looks for {m.group(1)!r}, but the job that "
        f"downloads the model is {PULL_JOB!r}. A stale overlay would survive a re-run."
    )
    assert PULL_JOB in _services(_load(OVERLAY))


# ── Nothing starts before the model is on disk ───────────────────────────────


def test_nothing_that_asks_for_a_model_starts_before_the_pull_succeeds():
    services = _services(_load(OVERLAY))
    pull = services[PULL_JOB]
    assert str(pull.get("restart")) == "no", (
        f"{PULL_JOB} has restart={pull.get('restart')!r}; a run-once job that "
        "restarts on exit 0 never reaches service_completed_successfully"
    )
    assert (pull.get("depends_on") or {}).get(SERVER, {}).get("condition") == "service_healthy", (
        f"{PULL_JOB} must wait for {SERVER} to be healthy; talking to a server "
        "whose HTTP listener is not up yet fails in a way that reads like a bad model name"
    )
    for name in DEPENDENTS:
        dep = ((services[name] or {}).get("depends_on") or {}).get(PULL_JOB)
        assert dep, f"{name} does not wait for {PULL_JOB}; it can start on an empty Ollama"
        assert dep.get("condition") == "service_completed_successfully", (
            f"{name} waits on {PULL_JOB} with condition={dep.get('condition')!r}"
        )
        assert dep.get("required") is not False, (
            f"{name} marks {PULL_JOB} `required: false`. Measured on Compose 2.x a "
            "FAILED completion dependency then degrades to a warning and the "
            "dependent starts anyway -- which is the empty-Ollama start this waits to prevent."
        )


# ── The pulled model is the model the code asks for ──────────────────────────


@pytest.mark.parametrize("label,operator_env", OPERATOR_ENVS, ids=[c[0] for c in OPERATOR_ENVS])
def test_every_ollama_resolver_asks_for_the_model_the_pull_downloads(
    monkeypatch, label, operator_env
):
    """The invariant the whole overlay exists for.

    One model is downloaded. Four functions choose what to ask for. If any of
    them chooses differently the operator gets a working /chat beside a Data
    Explorer that answers `model not found` -- with every container green.
    """
    for key in MODEL_KEYS:
        monkeypatch.delenv(key, raising=False)
    container = _container_env("llm-service", operator_env)
    for key, value in container.items():
        monkeypatch.setenv(key, value)

    want = _pulled_model(operator_env)
    assert want, f"[{label}] the pull job resolves an empty model name"
    wrong = {n: fn("ollama") for n, fn in RESOLVERS if fn("ollama") != want}
    assert not wrong, (
        f"[{label}] ollama-pull downloads {want!r}, but these resolvers ask for "
        f"something else: {wrong}. Container env was {container}."
    )


def test_the_two_services_without_their_own_model_env_still_get_one():
    """planner threads the quickstart's LLM_MODEL default; tool-generator threads
    no model variable at all. Neither can be fixed from the .env alone, which is
    why the overlay names the keys per service rather than relying on the file.
    """
    for name in ("planner", "tool-generator"):
        env = _container_env(name, {})
        assert env.get("LLM_MODEL") == "qwen2.5:7b", (
            f"{name} receives LLM_MODEL={env.get('LLM_MODEL')!r} on the bundled path; "
            "the quickstart default is an OpenAI catalog name and Ollama has never pulled it"
        )
        assert env.get("OLLAMA_MODEL") == "qwen2.5:7b", (
            f"{name} receives OLLAMA_MODEL={env.get('OLLAMA_MODEL')!r}"
        )


@pytest.mark.parametrize("label,operator_env", OPERATOR_ENVS, ids=[c[0] for c in OPERATOR_ENVS])
def test_the_sql_retry_chain_names_no_model_the_pull_did_not_download(label, operator_env):
    """The fifth model name, and the only one that is a list.

    When the Explorer's NL->SQL step returns something that is not a SELECT it
    retries against each name in EXPLORER_SQL_FALLBACK_MODELS. The code's own
    default is a three-name literal left over from when this overlay pulled
    three models, and the quickstart only passes the variable through -- so
    without a pin here every retry on a bundled install is a request for a model
    the volume does not contain, spent inside the Explorer's budget before the
    same error is raised anyway.
    """
    raw = _merged_env("llm-service").get(SQL_FALLBACK_KEY)
    assert raw is not None, (
        f"{SQL_FALLBACK_KEY} is not set on llm-service by either file, so the "
        "gateway falls back to its built-in three-name default"
    )
    want = _pulled_model(operator_env)
    names = [n.strip() for n in _expand(str(raw), operator_env).split(",") if n.strip()]
    assert names, f"[{label}] {SQL_FALLBACK_KEY} renders empty"
    stray = [n for n in names if n != want]
    assert not stray, (
        f"[{label}] ollama-pull downloads {want!r}, but the SQL retry chain would "
        f"also ask for {stray} -- models nothing downloaded"
    )


def test_the_pin_above_is_load_bearing():
    """Positive control for the guard above.

    If the gateway's built-in default ever became the pulled model on its own,
    that guard would pass for a reason that has nothing to do with the overlay.
    It does not: the default is a hard-coded literal naming models this path
    never pulls.
    """
    with open(GATEWAY_MAIN) as fh:
        body = fh.read()
    hits = re.findall(
        r'os\.getenv\("EXPLORER_SQL_FALLBACK_MODELS"\)\s*or\s*"([^"]+)"', body
    )
    assert hits, (
        "no built-in default found for EXPLORER_SQL_FALLBACK_MODELS in "
        f"{os.path.relpath(GATEWAY_MAIN, REPO_ROOT)}; if the call site moved, "
        "re-point this control rather than deleting it"
    )
    defaults = {n.strip() for h in hits for n in h.split(",") if n.strip()}
    assert len(defaults) > 1, (
        f"the built-in default is now {defaults}; the overlay pin may no longer "
        "be what keeps the retry chain on the pulled model"
    )


# ── Positive control for the interpolation reader above ──────────────────────


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
@pytest.mark.parametrize("label,operator_env", OPERATOR_ENVS, ids=[c[0] for c in OPERATOR_ENVS])
def test_compose_resolves_the_model_name_the_way_this_file_reads_it(
    tmp_path, label, operator_env
):
    """Everything above trusts `_expand`. This is what stops it being self-confirming.

    A skip is not a pass, which is why the static cases never depend on this one
    -- but a reader that disagrees with Compose makes all of them meaningless,
    so the disagreement has to be visible somewhere.
    """
    env_file = tmp_path / "env"
    env_file.write_text(
        "".join(f"{k}={v}\n" for k, v in {**FAKE_ENV, **operator_env}.items())
    )
    out = subprocess.run(
        ["docker", "compose", "--env-file", str(env_file),
         "-f", QUICKSTART, "-f", OVERLAY, "config"],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    assert out.returncode == 0, f"`docker compose config` failed:\n{out.stderr}"
    rendered = _services(yaml.safe_load(out.stdout))
    assert len(rendered) > 10, f"suspiciously small render ({list(rendered)}) -- refusing to trust it"

    disagree = {}
    for service in (PULL_JOB,) + DEPENDENTS:
        real = (rendered[service] or {}).get("environment") or {}
        for key, mine in _container_env(service, operator_env).items():
            if key in real and str(real[key]) != mine:
                disagree[f"{service}.{key}"] = (mine, real[key])
    assert not disagree, (
        f"[{label}] this file's interpolation reader disagrees with Compose "
        f"(mine, compose): {disagree}"
    )


# ── The address half: a pinned model is no use at the wrong host ─────────────


def _tolerant_load(path):
    """Parse a compose file that may carry Compose's ``!override`` / ``!reset`` tags.

    ``yaml.safe_load`` refuses those outright, which would silently reduce the
    census below to the files that happen not to use them -- and
    ``docker-compose.prod.yml``, one of the two files that pins the key this
    test is about, is exactly such a file.
    """

    class _Loader(yaml.SafeLoader):
        pass

    def _plain(loader, _suffix, node):
        if isinstance(node, yaml.ScalarNode):
            return loader.construct_scalar(node)
        if isinstance(node, yaml.SequenceNode):
            return loader.construct_sequence(node, deep=True)
        return loader.construct_mapping(node, deep=True)

    _Loader.add_multi_constructor("!", _plain)
    with open(path) as fh:
        return yaml.load(fh, Loader=_Loader) or {}


def _env_mapping(service_body):
    """A service's environment as a dict, accepting both compose spellings.

    ``docker-compose.yml`` writes tool-generator's block in list form and
    llm-service's in mapping form, so a reader that handles only one of them
    would skip the very service this test exists for.
    """
    env = (service_body or {}).get("environment") or {}
    if isinstance(env, dict):
        return {str(k): "" if v is None else str(v) for k, v in env.items()}
    out = {}
    for item in env:
        key, _, value = str(item).partition("=")
        out[key] = value
    return out


def test_the_key_the_client_reads_first_is_the_one_the_overlay_sets(monkeypatch):
    """``_ollama_base_url`` reads OLLAMA_BASE_URL before OLLAMA_URL.

    That ordering is why setting OLLAMA_URL alone is not enough: a base file
    that pins BASE_URL wins, and the bundled server never gets asked. Pinned
    from the function itself rather than from a comment, so a change to the
    precedence fails here instead of at a user's first prompt.
    """
    from src.utils.openai_client import _ollama_base_url

    for key in ("OLLAMA_BASE_URL", "OLLAMA_URL"):
        monkeypatch.delenv(key, raising=False)

    monkeypatch.setenv("OLLAMA_BASE_URL", "http://from-base-url:11434/v1")
    monkeypatch.setenv("OLLAMA_URL", "http://from-url:11434")
    assert "from-base-url" in _ollama_base_url(), (
        "OLLAMA_URL now wins over OLLAMA_BASE_URL; the overlay pins both, but "
        "the comments in docker-compose.ollama.yml explain the other order"
    )

    monkeypatch.setenv("OLLAMA_BASE_URL", "")
    assert "from-url" in _ollama_base_url(), (
        "an empty OLLAMA_BASE_URL no longer falls through to OLLAMA_URL; the "
        "quickstart ships `OLLAMA_BASE_URL: ${OLLAMA_BASE_URL:-}` and relies on it"
    )


@pytest.mark.parametrize("service", DEPENDENTS)
def test_the_overlay_points_every_service_it_extends_at_its_own_server(service):
    """Both address keys, on all three, aimed at the container this file starts."""
    from urllib.parse import urlsplit

    body = _services(_tolerant_load(OVERLAY)).get(service)
    assert body is not None, f"the overlay no longer extends {service}"
    env = _env_mapping(body)
    for key in ("OLLAMA_URL", "OLLAMA_BASE_URL"):
        assert key in env, (
            f"{service} in the overlay does not set {key}; a base file that pins "
            f"it then decides where this service looks for a model"
        )
        host = urlsplit(env[key]).hostname
        assert host == SERVER, (
            f"{service}.{key} points at {host!r}, not the bundled {SERVER!r} "
            f"service this overlay defines"
        )


def test_no_base_file_can_leave_a_bundled_service_pointing_at_the_host():
    """Every base pin of OLLAMA_BASE_URL is overridden by the overlay.

    ``docker-compose.yml`` and ``docker-compose.prod.yml`` both hard-code
    ``http://host.docker.internal:11434/v1`` on tool-generator. Compose merges
    ``environment`` key by key, so the overlay only wins on a key it names --
    and for the whole life of this overlay it named OLLAMA_URL only, which the
    client reads second. The census below is armed with its own denominator so
    it cannot pass by finding nothing.
    """
    import glob
    from urllib.parse import urlsplit

    overlay_env = {
        s: _env_mapping(_services(_tolerant_load(OVERLAY)).get(s))
        for s in DEPENDENTS
    }

    pins, stranded = [], []
    for path in sorted(glob.glob(os.path.join(REPO_ROOT, "docker-compose*.yml"))):
        if os.path.samefile(path, OVERLAY):
            continue
        for service in DEPENDENTS:
            value = _env_mapping(_services(_tolerant_load(path)).get(service)).get(
                "OLLAMA_BASE_URL"
            )
            if not value or value.startswith("${"):
                continue  # unset, or an operator-supplied default the overlay may keep
            where = f"{os.path.basename(path)}:{service}"
            pins.append(where)
            if urlsplit(value).hostname != SERVER and "OLLAMA_BASE_URL" not in overlay_env[service]:
                stranded.append(f"{where} -> {value}")

    assert pins, (
        "no base compose file pins OLLAMA_BASE_URL on any bundled service any "
        "more -- this census has lost its subject and would pass on anything"
    )
    assert not stranded, (
        "these services keep a base file's Ollama address because the overlay "
        f"does not name OLLAMA_BASE_URL for them: {stranded}"
    )
