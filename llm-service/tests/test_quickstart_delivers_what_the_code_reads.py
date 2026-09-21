"""A variable the code reads and no compose file delivers is an absence, and an
absence is valid YAML.

docker-compose.quickstart.yml is the only file install.sh downloads, and no service
in it has an ``env_file``. Every variable an operator is allowed to set therefore has
to be listed on the service by name -- the file says so itself, in the llm-service
block. Four defects of exactly this shape shipped at once:

  OPENAI_BASE_URL     absent from the whole file, so ``LLM_PROVIDER=openai`` could
                      only ever mean api.openai.com. Vertex AI, Azure, Groq,
                      OpenRouter and vLLM all speak that protocol and all need this
                      one variable; docker-compose.prod.yml already passes it.
  REDIS_*             absent from llm-service and planner, whose rate limiter builds
                      its URL from them. Host "redis" resolved by luck of the
                      default and then failed AUTH against a server this file always
                      starts with requirepass.
  OTEL_ENABLED        absent everywhere, and this bundle ships no collector, so every
                      service whose tracer defaults to on retried gRPC exports at
                      localhost:4317 forever. The cloud composes set it explicitly
                      true in 6 and 7 places; only this file was silent.
  LLM_SERVICE_TIMEOUT_SECONDS  absent, leaving the 10s code default against CPU
                      Ollama, where a first token takes 60-100s.

A fifth came later: GROQ_API_KEY and AZURE_OPENAI_* were never listed, so
``LLM_PROVIDER=groq`` or ``=azure`` in .env resolved to the Ollama fallback on every
install. The LLM check no longer names OPENAI_BASE_URL alone; it reads the variable
names out of the functions that pick the provider, key and model and build the
client, so a provider added there is required here without an edit.

The guard does not hold a list of which services need which variable -- a list is a
claim that goes stale the first time someone moves an import. It derives the answer
from the code: for each Python service it resolves the module named in the compose
``command``, walks that module's ``src.`` imports transitively, and asks whether the
reachable set contains the module that reads the variable. Move ``get_limiter`` into
the tool-generator entrypoint and this test starts requiring REDIS_* there, with no
edit here.

The Go services get the same treatment twice over. Their source trees come from
docker-compose.yml's own build stanzas rather than a name list here -- the list was
wrong on its first run, naming ``temporal-adapter`` for a directory called
``backend-temporal-adapter`` -- and each tree is then classified by which way its
flag DEFAULTS, because the three trees disagree: api-gateway and orchestrator export
unless told not to, while temporal-adapter's tracer no-ops unless the flag is
explicitly "true". Only the first kind needs a line in this file, and the guard
asserts the third kind does NOT get one: a variable that changes nothing reads as
coverage to the next person.

The other half of the OSS/cloud split rule (CLAUDE.md) is checked too: the flags must
default to the CLOUD behaviour, so no cloud compose may pin OTEL_ENABLED off. A guard
that only checked the quickstart side would pass just as happily on a patch that
disabled telemetry everywhere.

Static and cheap: pure YAML/AST/text parsing, no docker.
"""

import ast
import functools
import pathlib
import re

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
QUICKSTART = REPO / "docker-compose.quickstart.yml"
LLM_SRC = REPO / "llm-service"

# Files that describe a CLOUD deployment. The split rule says a flag defaults to the
# cloud behaviour and is set non-default only in the OSS compose, so seeing
# OTEL_ENABLED turned off in any of these is the rule being broken.
CLOUD_COMPOSE = ("docker-compose.yml", "docker-compose.prod.yml", "docker-compose.staging.yml")


def _ignore_unknown_tag(loader, suffix, node):
    """Construct a node carrying a tag SafeLoader has never heard of.

    docker-compose.prod.yml writes ``ports: !override [...]``, a Compose merge
    directive from 2.24. yaml.safe_load raises ConstructorError on it, and the two
    cloud-side assertions below would then error instead of checking anything -- a
    guard that cannot parse its subject is a guard that passes on nothing.
    """
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    return loader.construct_mapping(node)


class _ComposeLoader(yaml.SafeLoader):
    pass


_ComposeLoader.add_multi_constructor("!", _ignore_unknown_tag)


@functools.lru_cache(maxsize=None)
def _compose(path: pathlib.Path) -> dict:
    return yaml.load(path.read_text(), Loader=_ComposeLoader) or {}


def _services(path: pathlib.Path) -> dict:
    return _compose(path).get("services", {}) or {}


def _env(service: dict) -> dict:
    """Service environment as a name -> value dict, whichever syntax it uses."""
    raw = service.get("environment") or {}
    if isinstance(raw, list):
        out = {}
        for item in raw:
            name, _, value = str(item).partition("=")
            out[name.strip()] = value
        return out
    return {str(k): ("" if v is None else str(v)) for k, v in raw.items()}


def _entrypoint_module(service: dict) -> str | None:
    """``command: ["python", "-m", "src.gateway.main"]`` -> ``src.gateway.main``."""
    cmd = service.get("command")
    if isinstance(cmd, str):
        parts = cmd.split()
    elif isinstance(cmd, list):
        parts = [str(p) for p in cmd]
    else:
        return None
    if "-m" not in parts:
        return None
    idx = parts.index("-m")
    if idx + 1 >= len(parts):
        return None
    module = parts[idx + 1]
    return module if module.startswith("src.") else None


def _module_path(module: str) -> pathlib.Path | None:
    base = LLM_SRC / pathlib.Path(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


@functools.lru_cache(maxsize=None)
def _direct_imports(path: pathlib.Path) -> frozenset[str]:
    """The ``src.*`` modules this one file imports, at any indentation depth.

    Deferred imports inside a function count: a module imported lazily still reads
    the same environment when the call happens.
    """
    try:
        tree = ast.parse(path.read_text())
    except (SyntaxError, UnicodeDecodeError):  # pragma: no cover - not expected
        return frozenset()
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(a.name for a in node.names if a.name.startswith("src."))
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module and node.module.startswith("src."):
                found.add(node.module)
                # ``from src.utils import ratelimit`` names the module in the alias.
                found.update(f"{node.module}.{a.name}" for a in node.names)
    return frozenset(found)


@functools.lru_cache(maxsize=None)
def _reachable(module: str) -> frozenset[str]:
    """Every ``src.*`` module reachable from this entrypoint, transitively."""
    seen: set[str] = set()
    queue = [module]
    while queue:
        current = queue.pop()
        if current in seen:
            continue
        path = _module_path(current)
        if path is None:
            continue
        seen.add(current)
        queue.extend(_direct_imports(path))
    return frozenset(seen)


def _python_services() -> dict[str, dict]:
    """Quickstart services that run a module out of this repo's llm-service tree."""
    out = {}
    for name, svc in _services(QUICKSTART).items():
        module = _entrypoint_module(svc or {})
        if module and _module_path(module) is not None:
            out[name] = svc
    return out


# ---------------------------------------------------------------------------
# The reader modules. Each is the single place a variable is turned into behaviour;
# a service reaches the variable exactly when it reaches the module.
# ---------------------------------------------------------------------------
RATELIMIT_MODULE = "src.utils.ratelimit"
TELEMETRY_MODULE = "src.utils.telemetry"
OPENAI_CLIENT_MODULE = "src.utils.openai_client"

REDIS_VARS = ("REDIS_HOST", "REDIS_PORT", "REDIS_DB", "REDIS_PASSWORD")


def test_the_derivation_is_not_vacuous():
    """A census with an empty denominator passes every assertion it makes.

    Three separate zeros would silently disarm this whole file: no Python services
    found, no import edges walked, or a reachable set that is just the entrypoint.
    """
    services = _python_services()
    assert services, "found no quickstart service running a src.* module"

    reachable = {name: _reachable(_entrypoint_module(svc)) for name, svc in services.items()}
    for name, modules in reachable.items():
        assert len(modules) > 1, f"{name}: import walk found only the entrypoint itself"

    # And the walk must DISCRIMINATE, not just return everything. tool-generator runs
    # src.lifecycle.main, which deliberately reaches neither the limiter nor the
    # tracer; if every service came back reaching every module the guard below would
    # be asserting nothing about anything.
    reaches_limiter = {n for n, m in reachable.items() if RATELIMIT_MODULE in m}
    assert reaches_limiter, "no service reaches the rate limiter -- walk is broken"
    assert reaches_limiter != set(reachable), (
        "every service reaches the rate limiter; expected the connector-lifecycle "
        "entrypoint not to"
    )


@pytest.mark.parametrize("name", sorted(_python_services()))
def test_a_service_that_rate_limits_gets_a_redis_to_rate_limit_with(name):
    svc = _python_services()[name]
    if RATELIMIT_MODULE not in _reachable(_entrypoint_module(svc)):
        pytest.skip(f"{name} does not reach {RATELIMIT_MODULE}")
    env = _env(svc)
    missing = [v for v in REDIS_VARS if v not in env]
    assert not missing, (
        f"{name} imports {RATELIMIT_MODULE} but docker-compose.quickstart.yml never "
        f"delivers {missing}. The limiter then builds redis://redis:6379/0 with no "
        f"password and fails AUTH against a server this file always starts with "
        f"requirepass -- limits silently become per-replica."
    )


# The functions a service calls to choose its provider, key and model and to build
# its client. Everything they read -- directly or through a helper in the same
# module -- is a setting an operator can put in .env and expect to take effect.
LLM_CLIENT_ENTRYPOINTS = (
    "resolve_provider",
    "llm_configured",
    "get_default_model",
    "openai_api_key",
    "make_sync_client",
    "make_async_client",
)
# Callables whose string arguments are environment variable names.
_ENV_READERS_FIRST_ARG = {"getenv", "get", "env_bool", "_env_bool"}
_ENV_READERS_ALL_ARGS = {"_credential", "_set_credential"}
_ENV_NAME = re.compile(r"[A-Z][A-Z0-9_]+")


@functools.lru_cache(maxsize=None)
def _llm_client_env_vars() -> frozenset[str]:
    path = _module_path(OPENAI_CLIENT_MODULE)
    tree = ast.parse(path.read_text())
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    names: set[str] = set()
    seen: set[str] = set()
    queue = list(LLM_CLIENT_ENTRYPOINTS)
    while queue:
        current = queue.pop()
        if current in seen or current not in functions:
            continue
        seen.add(current)
        for node in ast.walk(functions[current]):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            called = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if isinstance(func, ast.Name) and func.id in functions:
                queue.append(func.id)
            if called in _ENV_READERS_ALL_ARGS:
                args = node.args
            elif called in _ENV_READERS_FIRST_ARG:
                args = node.args[:1]
            else:
                continue
            for arg in args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and _ENV_NAME.fullmatch(arg.value):
                    names.add(arg.value)
    return frozenset(names)


def test_the_llm_variable_census_is_not_vacuous():
    found = _llm_client_env_vars()
    # One per provider and one per way of reaching it. If the walk stops following
    # helpers, the credential names (read inside _credential calls two hops down)
    # are the first to disappear.
    for anchor in (
        "LLM_PROVIDER",
        "OPENAI_API_KEY",
        "OPENAI_BASE_URL",
        "OPENAI_API_KEY_SOURCE",
        "GROQ_API_KEY",
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_KEY",
        "LLM_MODEL",
        "OLLAMA_URL",
    ):
        assert anchor in found, f"census of {OPENAI_CLIENT_MODULE} lost {anchor}: {sorted(found)}"
    reaching = [
        n for n, svc in _python_services().items()
        if OPENAI_CLIENT_MODULE in _reachable(_entrypoint_module(svc))
    ]
    assert reaching, f"no quickstart service reaches {OPENAI_CLIENT_MODULE}"


@pytest.mark.parametrize("name", sorted(_python_services()))
def test_a_service_that_calls_an_llm_can_be_pointed_at_one(name):
    svc = _python_services()[name]
    if OPENAI_CLIENT_MODULE not in _reachable(_entrypoint_module(svc)):
        pytest.skip(f"{name} does not reach {OPENAI_CLIENT_MODULE}")
    env = _env(svc)
    missing = sorted(_llm_client_env_vars() - set(env))
    assert not missing, (
        f"{name} builds its LLM client with {OPENAI_CLIENT_MODULE}, which reads "
        f"{missing}, but docker-compose.quickstart.yml never delivers them. No service "
        f"here has an env_file, so a value an operator sets in .env for these never "
        f"reaches the container: LLM_PROVIDER=groq without GROQ_API_KEY, for one, "
        f"falls back to Ollama."
    )


@pytest.mark.parametrize("name", sorted(_python_services()))
def test_a_service_that_exports_spans_is_told_there_is_no_collector(name):
    svc = _python_services()[name]
    if TELEMETRY_MODULE not in _reachable(_entrypoint_module(svc)):
        pytest.skip(f"{name} does not reach {TELEMETRY_MODULE}")
    env = _env(svc)
    assert env.get("OTEL_ENABLED", "").strip().lower() == "false", (
        f"{name} initialises an OTLP exporter, and this bundle ships no collector. "
        f"Set OTEL_ENABLED: \"false\" -- the BatchSpanProcessor otherwise retries "
        f"every batch at localhost:4317 for the life of the process."
    )


# ---------------------------------------------------------------------------
# Go services.
#
# Which tree belongs to which service is not written down here -- a name list goes
# stale silently, and this one nearly did: the tree is backend-temporal-adapter, not
# temporal-adapter. docker-compose.yml already states the mapping in its build
# stanzas, so it is read from there.
#
# What matters is not whether a tree MENTIONS the flag but which way its default
# points, and the three trees genuinely disagree:
#
#   api-gateway     parseBoolWithDefault(os.Getenv("OTEL_ENABLED"), true)
#   orchestrator    v.SetDefault("OTEL_ENABLED", true)
#   temporal-adapter  os.Getenv("OTEL_ENABLED") != "true"  -> no-op
#
# Only a default-ON service exports to a collector that is not there, so only a
# default-ON service needs the flag in this file. Setting it on the opt-in one would
# be a line that changes nothing, which is worse than no line: it reads as coverage.
# ---------------------------------------------------------------------------
DEFAULT_ON_READS = (
    re.compile(r'parseBoolWithDefault\(\s*os\.Getenv\(\s*"OTEL_ENABLED"\s*\)\s*,\s*true\s*\)'),
    re.compile(r'SetDefault\(\s*"OTEL_ENABLED"\s*,\s*true\s*\)'),
)
OPT_IN_READ = re.compile(r'Getenv\(\s*"OTEL_ENABLED"\s*\)\s*[!=]=\s*"true"')


@functools.lru_cache(maxsize=None)
def _go_service_trees() -> dict[str, str]:
    """service name -> repo-relative source tree, taken from the dev compose."""
    out = {}
    for name, svc in _services(REPO / "docker-compose.yml").items():
        build = (svc or {}).get("build")
        dockerfile = build.get("dockerfile") if isinstance(build, dict) else None
        if not dockerfile:
            continue
        tree = str(pathlib.PurePosixPath(dockerfile).parent)
        root = REPO / tree
        if tree in {"", "."} or not root.is_dir():
            continue
        if any(root.rglob("*.go")):
            out[name] = tree
    return out


@functools.lru_cache(maxsize=None)
def _otel_default(tree: str) -> str:
    """``on``, ``opt-in`` or ``none`` -- how this tree treats an unset flag."""
    root = REPO / tree
    assert root.is_dir(), f"{tree} is not a directory"
    default_on = opt_in = False
    for path in root.rglob("*.go"):
        if path.name.endswith("_test.go"):
            continue
        text = path.read_text(errors="ignore")
        if "OTEL_ENABLED" not in text:
            continue
        if any(p.search(text) for p in DEFAULT_ON_READS):
            default_on = True
        if OPT_IN_READ.search(text):
            opt_in = True
    assert not (default_on and opt_in), (
        f"{tree} reads OTEL_ENABLED both ways; the guard cannot say which default wins"
    )
    if default_on:
        return "on"
    if opt_in:
        return "opt-in"
    return "none"


def test_the_go_census_found_all_three_trees_and_tells_them_apart():
    trees = _go_service_trees()
    for expected in ("api-gateway", "orchestrator", "temporal-adapter"):
        assert expected in trees, (
            f"docker-compose.yml no longer names a Go build for {expected}; the "
            f"OTEL assertions below would silently cover one service fewer"
        )

    defaults = {svc: _otel_default(tree) for svc, tree in trees.items()}
    unclassified = [
        svc
        for svc, tree in trees.items()
        if defaults[svc] == "none"
        and any(
            "OTEL_ENABLED" in p.read_text(errors="ignore")
            for p in (REPO / tree).rglob("*.go")
            if not p.name.endswith("_test.go")
        )
    ]
    assert not unclassified, (
        f"{unclassified} read OTEL_ENABLED in a form neither pattern recognises. A new "
        f"read style must be classified, not skipped -- update DEFAULT_ON_READS."
    )

    kinds = set(defaults.values())
    assert "on" in kinds, "no Go tree defaults telemetry ON -- the patterns match nothing"
    assert "opt-in" in kinds, (
        "no Go tree is opt-in; expected temporal-adapter, whose tracer no-ops unless "
        "OTEL_ENABLED == \"true\". Without this the guard cannot be shown to "
        "discriminate."
    )


@pytest.mark.parametrize("service", sorted(_go_service_trees()))
def test_a_go_service_that_traces_by_default_is_told_there_is_no_collector(service):
    tree = _go_service_trees()[service]
    if _otel_default(tree) != "on":
        pytest.skip(f"{tree} does not export spans unless OTEL_ENABLED is set true")
    env = _env(_services(QUICKSTART).get(service) or {})
    assert env.get("OTEL_ENABLED", "").strip().lower() == "false", (
        f"{service} exports OTLP spans by default and this bundle ships no collector, "
        f"so the gRPC exporter retries at localhost:4317 for the life of the process. "
        f"Set OTEL_ENABLED: \"false\" on it here."
    )


@pytest.mark.parametrize("service", sorted(_go_service_trees()))
def test_an_opt_in_go_service_is_not_given_a_flag_that_does_nothing(service):
    """The inverse, and the reason the classification exists.

    temporal-adapter already no-ops with the flag unset. An ``OTEL_ENABLED: "false"``
    line on it would change no behaviour while looking exactly like the lines that do,
    so a later reader would take the set of flagged services as the set of tracing
    services and be wrong.
    """
    tree = _go_service_trees()[service]
    if _otel_default(tree) != "opt-in":
        pytest.skip(f"{tree} is not opt-in")
    env = _env(_services(QUICKSTART).get(service) or {})
    assert "OTEL_ENABLED" not in env, (
        f"{service}'s tracer is opt-in ('!= \"true\"'), so setting OTEL_ENABLED here "
        f"changes nothing and misrepresents which services trace."
    )


@pytest.mark.parametrize("filename", CLOUD_COMPOSE)
def test_no_cloud_compose_turns_telemetry_off(filename):
    """The flag defaults to the cloud behaviour; only the OSS compose overrides it.

    Without this, a patch that disabled OTLP export everywhere would satisfy every
    assertion above.
    """
    path = REPO / filename
    if not path.is_file():
        pytest.skip(f"{filename} is not present")
    offenders = []
    for name, svc in _services(path).items():
        value = _env(svc or {}).get("OTEL_ENABLED")
        if value is not None and value.strip().lower() in {"false", "0", "no", "off"}:
            offenders.append(name)
    assert not offenders, (
        f"{filename} pins OTEL_ENABLED off for {offenders}. Cloud runs a collector; "
        f"per the OSS/cloud split rule in CLAUDE.md only docker-compose.quickstart.yml "
        f"sets this flag to its non-default value."
    )


# ---------------------------------------------------------------------------
# The chat deadline. api-gateway is the only caller, and the value has a ceiling it
# must stay under, which a bare presence check would not catch.
# ---------------------------------------------------------------------------
TIMEOUT_VAR = "LLM_SERVICE_TIMEOUT_SECONDS"


def test_the_chat_deadline_is_delivered_and_fits_under_the_write_timeout():
    gateway_go = REPO / "api-gateway"
    readers = [
        p
        for p in gateway_go.rglob("*.go")
        if not p.name.endswith("_test.go") and TIMEOUT_VAR in p.read_text(errors="ignore")
    ]
    assert readers, f"nothing in api-gateway reads {TIMEOUT_VAR} -- guard has no subject"

    env = _env(_services(QUICKSTART).get("api-gateway") or {})
    raw = env.get(TIMEOUT_VAR)
    assert raw, (
        f"docker-compose.quickstart.yml never delivers {TIMEOUT_VAR}, leaving the 10s "
        f"code default against CPU Ollama, where a first token takes 60-100s. Every "
        f"plain-English message then returns the canned fallback."
    )

    match = re.search(r"(\d+)", raw)
    assert match, f"{TIMEOUT_VAR} is {raw!r}, which carries no number to compare"
    seconds = int(match.group(1))
    assert seconds > 10, f"{TIMEOUT_VAR}={seconds}s does not raise the 10s code default"

    # cmd/server/main.go caps the whole response. A longer upstream deadline cannot
    # help -- it just moves the cut to a place with no error message.
    main_go = (REPO / "api-gateway" / "cmd" / "server" / "main.go").read_text()
    write_timeout = re.search(r"WriteTimeout:\s*(\d+)\s*\*\s*time\.Second", main_go)
    assert write_timeout, "could not find WriteTimeout in api-gateway/cmd/server/main.go"
    ceiling = int(write_timeout.group(1))
    assert seconds < ceiling, (
        f"{TIMEOUT_VAR}={seconds}s is not under the server's {ceiling}s WriteTimeout; "
        f"raise both together or the response is cut anyway."
    )


# ---------------------------------------------------------------------------
# Internal plumbing: the shared secret and in-network hostnames. Both fail the same
# silent way -- scheduled model runs, the dashboard's orchestrator cards, the object
# storage blob lane and plan-time topic sizing each logged a 401 or a DNS miss while
# every container reported healthy.
# ---------------------------------------------------------------------------
SECRET_VAR = "INTERNAL_SERVICE_SECRET"
# Browser-facing and host-reachable names; everything else must be a compose host.
_NON_COMPOSE_HOSTS = {"localhost", "host.docker.internal"}
# Code defaults naming a sidecar this bundle deliberately does not ship. Each feature
# stays off here: Avro is pinned false, OTEL is pinned false, and the MCP container
# listing only uses the proxy when MCP_DOCKER_API_URL is set.
_OPTIONAL_SIDECARS = {"schema-registry", "otel-collector", "docker-socket-proxy"}
_URL_HOST = re.compile(r'"(?:https?|grpc)://([A-Za-z0-9_-]+):\d+')


def _quickstart_hosts() -> set[str]:
    services = _services(QUICKSTART)
    return set(services) | {(svc or {}).get("container_name") for svc in services.values()} - {None}


def _reads(root: pathlib.Path, suffixes: tuple[str, ...], needle: str) -> bool:
    for path in root.rglob("*"):
        if path.suffix not in suffixes or path.name.endswith("_test.go") or "node_modules" in path.parts:
            continue
        if needle in path.read_text(errors="ignore"):
            return True
    return False


# Every stack that is actually run, as the file list compose merges in order. The
# quickstart is standalone; prod is an overlay on the dev base and staging on both, so
# a service the base never passes the secret to stays without it in the cloud unless
# the overlay adds it -- the planner shipped that way, ENVIRONMENT=production and no
# secret, so plan-time topic creation got 401 from the orchestrator.
SECRET_STACKS = {
    "quickstart": ("docker-compose.quickstart.yml",),
    "dev": ("docker-compose.yml",),
    "prod": ("docker-compose.yml", "docker-compose.prod.yml"),
    "staging": ("docker-compose.yml", "docker-compose.prod.yml", "docker-compose.staging.yml"),
}


@functools.lru_cache(maxsize=None)
def _secret_readers() -> frozenset[str]:
    readers = set()
    for service, tree in _go_service_trees().items():
        if _reads(REPO / tree, (".go",), SECRET_VAR):
            readers.add(service)
    for service, svc in _python_services().items():
        module = _entrypoint_module(svc)
        if any(SECRET_VAR in (_module_path(m).read_text(errors="ignore")) for m in _reachable(module)):
            readers.add(service)
    if _reads(REPO / "frontend" / "src", (".ts", ".tsx"), SECRET_VAR):
        readers.add("frontend")
    return frozenset(readers)


def _merged_env(files: tuple[str, ...]) -> dict[str, dict]:
    """service -> environment after compose merges ``files`` (later keys win)."""
    merged: dict[str, dict] = {}
    for filename in files:
        for name, svc in _services(REPO / filename).items():
            merged.setdefault(name, {}).update(_env(svc or {}))
    return merged


@pytest.mark.parametrize("stack", sorted(SECRET_STACKS))
def test_every_service_that_sends_the_internal_secret_is_given_it(stack):
    files = SECRET_STACKS[stack]
    for filename in files:
        if not (REPO / filename).is_file():
            pytest.skip(f"{filename} is not present")
    env = _merged_env(files)
    expected = sorted(s for s in _secret_readers() if s in env)
    assert {"api-gateway", "orchestrator", "temporal-adapter", "planner", "frontend"} <= set(expected), (
        f"the census found only {expected} in the {stack} stack; a reader moved or the "
        f"derivation broke"
    )
    missing = [s for s in expected if SECRET_VAR not in env[s]]
    assert not missing, (
        f"{missing} read {SECRET_VAR} but the {stack} stack ({' + '.join(files)}) never "
        f"passes it; every call they make to an internal route is refused with 401 on a "
        f"production ENVIRONMENT (503 on the gateway when its own copy is empty), and "
        f"nothing surfaces it."
    )


def test_every_in_network_url_names_a_quickstart_host():
    hosts = _quickstart_hosts()
    checked, offenders = 0, []
    for name, svc in _services(QUICKSTART).items():
        for var, raw in _env(svc or {}).items():
            value = re.sub(r"\$\{[A-Z0-9_]+:?-([^}]*)\}", r"\1", raw)
            for match in re.finditer(r"\b[a-z]+://(?:[^@/\s]*@)?([A-Za-z0-9_.-]+)", value):
                host = match.group(1)
                if "." in host or host in _NON_COMPOSE_HOSTS:
                    continue
                checked += 1
                if host not in hosts:
                    offenders.append(f"{name}.{var} -> {host}")
    assert checked > 10, f"only {checked} in-network URLs parsed; the scan is not reading the file"
    assert not offenders, f"these point at hosts this compose does not define: {offenders}"


def test_a_code_default_host_this_bundle_lacks_is_overridden():
    """The orchestrator's blob lane defaulted to ``rsync-ai-minio``, the dev compose's
    container; the quickstart's MinIO is ``minio``/``rsync-minio``, so every
    object-storage load died at DNS until MINIO_ENDPOINT_URL was passed."""
    hosts = _quickstart_hosts()
    quickstart = _services(QUICKSTART)
    found, offenders = 0, []
    for service, tree in _go_service_trees().items():
        if service not in quickstart:
            continue
        env = _env(quickstart[service] or {})
        for path in (REPO / tree).rglob("*.go"):
            if path.name.endswith("_test.go"):
                continue
            text = path.read_text(errors="ignore")
            code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("//"))
            foreign = {h for h in _URL_HOST.findall(code) if h not in hosts and h not in _NON_COMPOSE_HOSTS}
            if not foreign:
                continue
            found += len(foreign)
            if foreign <= _OPTIONAL_SIDECARS:
                continue
            read = set(re.findall(r'"([A-Z][A-Z0-9_]*(?:URL|ENDPOINT))"', text))
            if not read & set(env):
                offenders.append(f"{service}: {path.relative_to(REPO)} defaults to {sorted(foreign)}")
    assert found, "no foreign default hosts found at all -- the known rsync-ai-minio default should be one"
    assert not offenders, (
        f"code falls back to a host docker-compose.quickstart.yml does not define, and the "
        f"service is not given the variable that overrides it: {offenders}"
    )
