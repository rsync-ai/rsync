"""Every Kafka client in every compose file must get the same security profile.

The chart-side twin of this file is test_chart_kafka_security_env.py, and it exists
for the same reason: on a secured BYO cluster, a client with a bootstrap address and
no ``security.protocol`` does not fail. It speaks PLAINTEXT to a SASL listener, the
container goes healthy, and either zero rows move or the handshake BLOCKS until a
timeout -- which reads as a hung install, not a misconfiguration. The missing
variable is an absence, and an absence is valid YAML.

Three surfaces have to agree and none of them can see the other two:

  docker-compose.yml             the full stack (prod and dev)
  docker-compose.quickstart.yml  what install.sh downloads -- and the ONLY file it
                                 downloads, which is why its kafka-init builds the
                                 client.properties inline instead of calling the
                                 script
  scripts/kafka-init-new-topics.sh   the bootstrapper the full stack's kafka-init
                                 runs, which turns the same variables into the
                                 ``--command-config`` the JVM CLIs take

Drift between them is undetectable at runtime: each surface is exercised by a
different install path, so a variable dropped from one is simply never observed by
whoever tested the other.

Static and cheap on purpose -- pure YAML/text parsing, no docker, no cluster.
"""

import ast
import functools
import pathlib
import re

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
COMPOSE_FILES = ("docker-compose.yml", "docker-compose.quickstart.yml")
BOOTSTRAPPER = REPO / "scripts" / "kafka-init-new-topics.sh"

# The names a service uses to say "this is the broker I dial". A service carrying
# any of these is a Kafka client and needs a security profile.
# The bare `BOOTSTRAP_SERVERS` is not an oversight: kafka-connect's entrypoint
# hand-exports a fixed list of unprefixed names, so that is the spelling the
# compose file must use for it.
#
# APICURIO_KAFKASQL_BOOTSTRAP_SERVERS is here even though no service sets it today.
# schema-registry used to carry an *inert* KAFKA_BOOTSTRAP_SERVERS -- Apicurio 3 never
# reads that name -- so the census saw a client that was not one, and the fix was to
# delete the key. That deletion made schema-registry invisible to this census, and the
# only spelling that would make it a REAL Kafka client (apicurio.kafkasql.bootstrap.servers,
# reached by switching APICURIO_STORAGE_KIND=kafkasql) was not a BROKER_KEY. Without this
# entry, that switch would land a genuinely unsecured client with nothing failing.
APICURIO_BROKER_KEY = "APICURIO_KAFKASQL_BOOTSTRAP_SERVERS"

BROKER_KEYS = (
    "KAFKA_BROKERS",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_BROKER",
    "CONNECT_BOOTSTRAP_SERVERS",
    "BOOTSTRAP_SERVERS",
    APICURIO_BROKER_KEY,
)

# ---------------------------------------------------------------------------
# The anchor is not a universal remedy, and this is the counter-example.
#
# apicurio/apicurio-registry:3.0.6 reads NONE of the anchor's 17 KAFKA_* names.
# Measured over the image's own artifacts: all 17, in both the env spelling and
# SmallRye's dotted equivalent (34 spellings), occur 0 times in the runner jar
# (2,419 entries) and 0 times across all 356 shipped lib jars (57,339 entries) --
# against live controls in the same scan (security.protocol 3 runner / 6 lib
# entries, sasl.mechanism 7 / 3, bootstrap.servers 8 / 5). So merging
# `<<: *kafka-security` into a kafkasql registry produces the exact failure this
# file's docstring exists to prevent: a client that reads as secured and speaks
# PLAINTEXT. Requiring the anchor there would make this guard the author of the
# bug.
#
# What the image does read is declared by
# io/apicurio/registry/storage/impl/kafkasql/KafkaSqlFactory.class and defaulted in
# its application.properties. Note the HYPHENS in two of the property names; the env
# spellings below are SmallRye's mapping of dots AND dashes to `_`.
#
#   apicurio.kafkasql.security.protocol                       (no default shipped)
#   apicurio.kafkasql.security.sasl.enabled                   = false        (:22)
#   apicurio.kafkasql.security.sasl.mechanism                 = OAUTHBEARER  (:196)
#   apicurio.kafkasql.security.sasl.client-id                 = sa           (:157)
#   apicurio.kafkasql.security.sasl.client-secret             = sa           (:6)
#   apicurio.kafkasql.security.sasl.token.endpoint            = localhost:8090 (:87)
#   apicurio.kafkasql.security.sasl.login.callback.handler.class = Strimzi's   (:162)
#
# The same class also declares apicurio.kafkasql.{security.ssl.truststore.*,
# ssl.truststore.password, ssl.keystore.*, ssl.key.password}. Those are deliberately
# NOT required here: a truststore path is worthless without the file mounted next to
# it, and no static check can confirm that. Leaving them out is a decision, not an
# oversight.
#
# LATENT BY DESIGN: no service sets APICURIO_BROKER_KEY today, so the branch below is
# quantified over an empty set and proves nothing on its own. It was proven by
# mutation instead -- see the archive entry
# KI-COMPOSE-SCHEMA-REGISTRY-BOOTSTRAP-ENV-IS-INERT. Do not read its silence as a pass.
APICURIO_SECURITY_KEYS = (
    "APICURIO_KAFKASQL_SECURITY_PROTOCOL",
    "APICURIO_KAFKASQL_SECURITY_SASL_ENABLED",
    "APICURIO_KAFKASQL_SECURITY_SASL_MECHANISM",
    "APICURIO_KAFKASQL_SECURITY_SASL_CLIENT_ID",
    "APICURIO_KAFKASQL_SECURITY_SASL_CLIENT_SECRET",
    "APICURIO_KAFKASQL_SECURITY_SASL_TOKEN_ENDPOINT",
    "APICURIO_KAFKASQL_SECURITY_SASL_LOGIN_CALLBACK_HANDLER_CLASS",
)

# Services that carry a broker address and deliberately get NO x-kafka-security
# merge. Each entry is a claim about the image, and the test below fails if an
# entry stops being needed -- a stale exemption is how the next real client gets
# waved through.
#
# NOT EMPTY: the kafka-connect entries are added by the loop directly below, one per
# compose file. It starts empty here only because those are generated rather than
# spelled out twice.
#
# schema-registry was exempted here until 2026-08-29. It is not any more, and the
# reason is the good one: the KAFKA_BOOTSTRAP_SERVERS/REGISTRY_KAFKASQL_TOPIC that put
# it in the census were deleted from docker-compose.yml as provably inert, so the
# service is no longer a client the census sees, and an unreachable exemption is a
# hole. test_the_not_a_client_allowlist_has_no_stale_entries is what forced this edit.
#
# schema-registry: NOT A KAFKA CLIENT AT ALL AS SHIPPED, and its absence from every
# structure in this file is deliberate. Do not "fix" it by adding a NOT_A_CLIENT entry
# (the stale-entry test would fail it) and do not merge `<<: *kafka-security` into it
# (the image reads none of those 17 names -- see the block above). It runs the image
# default apicurio.storage.kind=sql / sql.kind=h2 over an in-memory H2 URL, names no
# broker, and therefore has no security profile to carry. It enters the census only if
# somebody sets APICURIO_BROKER_KEY, and at that moment APICURIO_SECURITY_KEYS -- not
# the anchor -- becomes its requirement.
NOT_A_CLIENT = {}

_CONNECT_EXEMPTION = (
    "Kafka Connect takes its security through CONNECT_*-prefixed keys at three "
    "client levels (worker, producer., consumer.), which the anchor's bare KAFKA_* "
    "names cannot express -- Connect would ignore them. Covered instead by its own "
    "CONNECT_* block, which test_connect_configures_all_three_client_levels pins."
)
for _f in COMPOSE_FILES:
    NOT_A_CLIENT[(_f, "kafka-connect")] = _CONNECT_EXEMPTION


def _load(name):
    return yaml.safe_load((REPO / name).read_text())


def _env_of(service):
    """Service environment as a dict, from either the map or the list form.

    ``docker compose config`` normalises both to a map, so a file may legitimately
    use either and the two must be read the same way here.
    """
    env = service.get("environment") or {}
    if isinstance(env, list):
        out = {}
        for item in env:
            key, _, value = str(item).partition("=")
            out[key] = value
        return out
    return dict(env)


def _command_text(service):
    """The service's command/entrypoint flattened to one string.

    docker-compose.yml's kafka-init sets its broker with `export KAFKA_BROKER=...`
    inside the command rather than in `environment:`, which is legitimate -- but a
    census that only reads `environment:` would call it addressless and either fail
    spuriously or, worse, get allowlisted.
    """
    parts = []
    for key in ("command", "entrypoint"):
        value = service.get(key)
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, list):
            parts.extend(str(v) for v in value)
    return "\n".join(parts)


def _anchor(name):
    doc = _load(name)
    return doc.get("x-kafka-security") or {}


def _services(name):
    doc = _load(name)
    return {k: v for k, v in (doc.get("services") or {}).items() if isinstance(v, dict)}


def _kafka_clients(name):
    """(service, env, security_keys_present) for every service that dials a broker."""
    anchor_keys = set(_anchor(name))
    found = []
    for svc_name, svc in sorted(_services(name).items()):
        env = _env_of(svc)
        has_address = any(k in env for k in BROKER_KEYS) or any(
            k in _command_text(svc) for k in BROKER_KEYS
        )
        if has_address or (anchor_keys & set(env)):
            found.append((svc_name, env, anchor_keys & set(env)))
    return found


# --------------------------------------------------------------------------
# Anti-vacuity. Every assertion below is quantified over a set this parse
# produces; if the parse produces nothing, they all pass while proving nothing.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name", COMPOSE_FILES)
def test_the_anchor_was_found_and_is_not_a_stub(name):
    anchor = _anchor(name)
    assert len(anchor) >= 10, (
        f"{name} defines x-kafka-security with {len(anchor)} keys -- the parse is "
        "broken or the anchor was gutted, and either way a green result below is "
        "meaningless"
    )


@pytest.mark.parametrize("name", COMPOSE_FILES)
def test_the_kafka_client_census_is_not_empty(name):
    clients = _kafka_clients(name)
    assert len(clients) >= 4, (
        f"{name} yielded only {len(clients)} Kafka clients "
        f"({[c[0] for c in clients]}) -- both files run at least the api-gateway, "
        "orchestrator, planner and temporal-adapter against Kafka, so this is a "
        "parser failure, not a clean file"
    )


# --------------------------------------------------------------------------
# The three surfaces agree
# --------------------------------------------------------------------------

def test_the_two_compose_files_define_the_same_security_key_set():
    """A key in one file and not the other is a per-install-path silent failure.

    install.sh ships only the quickstart file; the full stack ships only the other.
    So a variable added to one is honoured for one class of operator and ignored,
    with no error, for the rest.
    """
    root = set(_anchor("docker-compose.yml"))
    quick = set(_anchor("docker-compose.quickstart.yml"))
    assert root == quick, (
        "the x-kafka-security anchors have drifted apart.\n"
        f"  only in docker-compose.yml:            {sorted(root - quick)}\n"
        f"  only in docker-compose.quickstart.yml: {sorted(quick - root)}"
    )


def _required_security_keys(name, env):
    """The keys THIS service's image would actually read, not a blanket rule.

    Returning the anchor for every client is what made an earlier revision of this
    guard certify a kafkasql Apicurio registry as secured while it spoke PLAINTEXT:
    the anchor's names are inert to that image, so the merge satisfied the assertion
    and changed nothing about the connection.
    """
    required = set()
    if any(k in env for k in BROKER_KEYS if k != APICURIO_BROKER_KEY):
        required |= set(_anchor(name))
    if APICURIO_BROKER_KEY in env:
        required |= set(APICURIO_SECURITY_KEYS)
    return required or set(_anchor(name))


def test_the_anchor_cannot_satisfy_the_apicurio_requirement():
    """The two key families must stay disjoint, or the trap comes back.

    If any KAFKA_* anchor name ever appears in APICURIO_SECURITY_KEYS, merging
    `<<: *kafka-security` starts partially satisfying the Apicurio requirement again
    and the guard drifts back towards the false green it was rewritten to remove.
    Positive denominators asserted first: an empty tuple on either side is trivially
    disjoint and would prove nothing.
    """
    anchor = set(_anchor("docker-compose.yml"))
    assert len(anchor) >= 10, f"anchor parse yielded {len(anchor)} keys"
    assert len(APICURIO_SECURITY_KEYS) >= 6, (
        f"APICURIO_SECURITY_KEYS has {len(APICURIO_SECURITY_KEYS)} entries"
    )
    assert APICURIO_BROKER_KEY in BROKER_KEYS, (
        "the Apicurio broker spelling left BROKER_KEYS, so the census cannot see a "
        "kafkasql registry at all and the requirement below is unreachable"
    )
    overlap = anchor & set(APICURIO_SECURITY_KEYS)
    assert not overlap, (
        f"{sorted(overlap)} appear in BOTH x-kafka-security and APICURIO_SECURITY_KEYS. "
        "apicurio/apicurio-registry:3.0.6 reads none of the anchor's names (0 hits over "
        "the runner jar and all 356 lib jars, against live controls), so an anchor merge "
        "must never count towards an Apicurio client's security profile."
    )


@pytest.mark.parametrize("name", COMPOSE_FILES)
def test_every_kafka_client_carries_the_full_security_key_set(name):
    failures = []
    for svc_name, env, _present in _kafka_clients(name):
        if (name, svc_name) in NOT_A_CLIENT:
            continue
        required = _required_security_keys(name, env)
        missing = required - set(env)
        if missing:
            remedy = (
                "add the APICURIO_KAFKASQL_SECURITY_* keys -- the anchor does NOT "
                "apply to this image"
                if APICURIO_BROKER_KEY in env
                else "merge `<<: *kafka-security`"
            )
            failures.append(f"{svc_name}: missing {sorted(missing)} ({remedy})")
    assert not failures, (
        f"{name} has Kafka clients without a complete security profile. Apply the "
        "remedy named for each service below -- they are not interchangeable, because "
        "the anchor's KAFKA_* names are inert to the Apicurio image -- or add a "
        "NOT_A_CLIENT entry saying why the image can use neither:\n  "
        + "\n  ".join(failures)
    )


@pytest.mark.parametrize("name", COMPOSE_FILES)
def test_every_service_merging_the_anchor_also_declares_a_broker_address(name):
    """Credentials without an address point the right key at the wrong cluster.

    Found live in debezium-mcp: it merged the whole anchor and named no broker, so
    its schema-history client fell through to connector.py's hardcoded
    ``kafka:29092`` -- SASL-credentialed for the operator's cluster, addressed to
    the bundled one. The connector still starts; it breaks on the restart that
    replays the history topic.
    """
    anchor_keys = set(_anchor(name))
    services = _services(name)
    offenders = []
    for svc_name, svc in sorted(services.items()):
        env = _env_of(svc)
        if not anchor_keys.issubset(set(env)):
            continue
        if any(k in env for k in BROKER_KEYS):
            continue
        if any(k in _command_text(svc) for k in BROKER_KEYS):
            continue
        offenders.append(svc_name)
    assert not offenders, (
        f"{name}: {offenders} merge x-kafka-security but name no broker, so they "
        f"authenticate to whatever address their image hardcodes. Add one of "
        f"{list(BROKER_KEYS)}."
    )


@pytest.mark.parametrize("name", COMPOSE_FILES)
def test_the_not_a_client_allowlist_has_no_stale_entries(name):
    """An exemption that is no longer reachable stops being a decision and becomes
    a hole the next service falls through."""
    present = {svc for svc, _env, _keys in _kafka_clients(name)}
    stale = [
        svc for (file_name, svc) in NOT_A_CLIENT if file_name == name and svc not in present
    ]
    assert not stale, (
        f"NOT_A_CLIENT exempts {stale} in {name}, but the census no longer sees "
        "them. Delete the entries."
    )


# --------------------------------------------------------------------------
# The bootstrapper script is the third surface
# --------------------------------------------------------------------------

def test_the_bootstrapper_reads_every_key_the_anchor_delivers():
    """docker-compose.yml's kafka-init merges the anchor; the script it runs must
    actually consume it.

    Delivering a variable to a process that ignores it is worse than not delivering
    it: the compose file documents support the runtime does not have, and the
    operator's only symptom is a bootstrap that blocks.
    """
    anchor_keys = set(_anchor("docker-compose.yml"))
    src = BOOTSTRAPPER.read_text()
    missing = sorted(k for k in anchor_keys if k not in src)
    assert not missing, (
        f"{BOOTSTRAPPER.relative_to(REPO)} never mentions {missing}, which "
        "docker-compose.yml's kafka-init hands it. Either read them or stop "
        "merging the anchor into that service."
    )


def test_the_bootstrapper_passes_command_config_to_every_cli_invocation():
    """One forgotten call site falls back to PLAINTEXT, and only that call fails.

    The readiness probe is the dangerous one: it runs before ``--if-not-exists``,
    so a probe that cannot authenticate looks like a broker that is not up yet, and
    the script spends its whole retry budget before failing for the wrong reason.
    """
    src = BOOTSTRAPPER.read_text()
    calls = [
        line.strip()
        for line in src.splitlines()
        if '"$KAFKA_TOPICS"' in line or re.search(r"^\s*--bootstrap-server ", line)
    ]
    assert len(calls) >= 4, (
        f"found only {len(calls)} kafka-topics invocations in "
        f"{BOOTSTRAPPER.relative_to(REPO)} -- the parse is broken"
    )
    naked = [c for c in calls if "--bootstrap-server" in c and "$KAFKA_CC" not in c]
    assert not naked, (
        "these kafka-topics invocations dial the broker without $KAFKA_CC, so on a "
        "secured cluster they fall back to PLAINTEXT and block:\n  "
        + "\n  ".join(naked)
    )


def test_the_bootstrapper_is_a_no_op_when_nothing_is_configured():
    """The bundled-broker path must be untouched by the security block.

    Asserted structurally because it is the property that makes this change safe to
    ship to a running prod stack: the builder sits behind one emptiness check, and
    KAFKA_CC -- the only thing that reaches the CLI -- starts empty.
    """
    src = BOOTSTRAPPER.read_text()
    assert re.search(r'^KAFKA_CC=""', src, re.M), (
        "KAFKA_CC must be initialised empty, or an unconfigured stack passes a "
        "half-set --command-config"
    )
    assert re.search(r'^if \[ -n "\$_kafka_sec_any" \]; then', src, re.M), (
        "the client.properties builder must be gated on at least one security "
        "variable being set; ungated, it changes the bundled-broker path"
    )


def test_the_bootstrapper_writes_the_properties_file_privately():
    """It holds a SASL password in cleartext -- the only form the JVM CLI takes."""
    src = BOOTSTRAPPER.read_text()
    assert "umask 077" in src, (
        f"{BOOTSTRAPPER.relative_to(REPO)} writes a cleartext SASL password without "
        "umask 077 first. chmod after the fact leaves a window in which the file "
        "exists world-readable."
    )


def test_connect_configures_all_three_client_levels():
    """A green Connect worker actively hides starved tasks.

    The worker's own ``CONNECT_SECURITY_PROTOCOL`` does NOT reach the producer and
    consumer each connector task creates -- those read ``producer.``/``consumer.``-
    prefixed keys, which Connect spells CONNECT_PRODUCER_*/CONNECT_CONSUMER_*.
    Configure only the worker and every connector reports RUNNING while each task
    dials anonymously underneath it.
    """
    env = _env_of(_services("docker-compose.yml")["kafka-connect"])
    for setting in ("SECURITY_PROTOCOL", "SASL_MECHANISM", "SASL_JAAS_CONFIG"):
        for level in ("CONNECT_", "CONNECT_PRODUCER_", "CONNECT_CONSUMER_"):
            assert f"{level}{setting}" in env, (
                f"kafka-connect sets no {level}{setting}. The worker authenticates "
                "and reports every connector RUNNING; the tasks underneath it do not."
            )


@pytest.mark.parametrize(
    "prop", ["truststore", "keystore"]
)
def test_the_bootstrapper_declares_every_jvm_store_as_PEM(prop):
    """KIP-651. Without ssl.<store>.type=PEM the JVM assumes JKS, fails to parse the
    PEM, and names neither the file nor the setting."""
    src = BOOTSTRAPPER.read_text()
    if f"ssl.{prop}.location" not in src:
        pytest.skip(f"the bootstrapper writes no ssl.{prop}.location")
    assert f"ssl.{prop}.type=PEM" in src, (
        f"{BOOTSTRAPPER.relative_to(REPO)} writes ssl.{prop}.location without "
        f"ssl.{prop}.type=PEM"
    )


# --------------------------------------------------------------------------
# The second census: derived from the CODE, not from the compose file
# --------------------------------------------------------------------------
#
# `_kafka_clients()` above is ADDRESS-driven: it asks each service "do you name a
# broker?" and only inspects the ones that say yes. A service that declares no
# Kafka env at all answers no, and is therefore invisible to every assertion in
# this file -- including the ones written to catch exactly its failure mode.
#
# That is not hypothetical. llm-service, debezium-mcp and kafka-mcp-sink-mcp sat
# in docker-compose.yml with ZERO KAFKA_* keys while this whole module was green,
# because a census keyed on a service's own declaration cannot see a service that
# declares nothing. Deleting a key does not trip the guard; it removes the service
# from the guard's denominator, which looks identical to passing.
#
# So this census asks the opposite question, of the source tree rather than the
# compose file: which services RUN code that constructs a Kafka client or reads a
# KAFKA_* variable? Its answer cannot be changed by editing `environment:`, which
# is the entire point -- and
# `test_the_code_derived_census_survives_stripping_every_kafka_key` proves that
# property by re-running it against a compose document with every KAFKA_* key
# removed.

# Constructors, not mentions. A regex over the same files reports
# llm-service/src/utils/masking.py as a Kafka client, because the LLM error
# scrubber LISTS KAFKA_SASL_PASSWORD among the names it redacts -- and that one
# false positive is transitively imported by nearly every Python entrypoint in the
# repo, so a text-matching census would flag them all and mean nothing.
_PY_KAFKA_CLIENTS = frozenset(
    {"KafkaConsumer", "KafkaProducer", "KafkaAdminClient", "AIOKafkaConsumer", "AIOKafkaProducer"}
)
_KAFKA_ENV_NAME = re.compile(r"^KAFKA_[A-Z0-9_]+$")
_GO_KAFKA_CLIENT = re.compile(
    r'os\.Getenv\(\s*"KAFKA_|kafkaclient\.|sarama\.|kafka\.NewReader|kafka\.NewWriter|kgo\.NewClient'
)
_SOURCE_SUFFIXES = (".py", ".go")
_SKIP_PARTS = frozenset({".git", "node_modules", "__pycache__", ".venv", "venv", "dist", "build", ".next", "vendor"})


def _is_test_file(path):
    return path.name.startswith("test_") or path.name.endswith("_test.go") or "tests" in path.parts


@functools.lru_cache(maxsize=None)
def _py_kafka_evidence(path):
    """Why this module is a Kafka client, or None. AST, deliberately not a grep."""
    try:
        tree = ast.parse(path.read_text(errors="replace"))
    except SyntaxError:
        return None
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            called = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if called in _PY_KAFKA_CLIENTS:
                return f"constructs {called}()"
            if called in ("getenv", "get") and node.args:
                first = node.args[0]
                if isinstance(first, ast.Constant) and isinstance(first.value, str) \
                   and _KAFKA_ENV_NAME.match(first.value):
                    return f"reads env {first.value}"
        if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant) \
           and isinstance(node.slice.value, str) and _KAFKA_ENV_NAME.match(node.slice.value):
            return f"reads env {node.slice.value}"
    return None


def _module_file(root, module):
    base = root.joinpath(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


@functools.lru_cache(maxsize=None)
def _reachable_modules(root, entry):
    """Every in-tree module `python -m entry` can import, transitively.

    `ast.walk` reaches imports inside functions too, which is load-bearing here:
    llm-service's Kafka consumer is imported inside the startup coroutine at
    src/gateway/main.py, not at module top level.
    """
    seen, queue, files = set(), [entry], []
    while queue:
        module = queue.pop()
        if module in seen:
            continue
        seen.add(module)
        path = _module_file(root, module)
        if path is None:
            continue
        files.append(path)
        try:
            tree = ast.parse(path.read_text(errors="replace"))
        except SyntaxError:
            continue
        is_pkg = (root.joinpath(*module.split(".")) / "__init__.py").is_file()
        package = module if is_pkg else ".".join(module.split(".")[:-1])
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                queue.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                base = node.module or ""
                if node.level:
                    parts = package.split(".")
                    base = ".".join(parts[: len(parts) - node.level + 1] + ([base] if base else []))
                queue.append(base)
                queue.extend(f"{base}.{a.name}" if base else a.name for a in node.names)
    return tuple(files)


@functools.lru_cache(maxsize=None)
def _dir_kafka_evidence(directory):
    """First Kafka-client site under a build root, or None. Used for the Go
    services and for images whose entrypoint lives in the Dockerfile."""
    for path in sorted(directory.rglob("*")):
        if not path.is_file() or path.suffix not in _SOURCE_SUFFIXES:
            continue
        relative = path.relative_to(directory)
        if _SKIP_PARTS & set(relative.parts) or _is_test_file(relative):
            continue
        if path.suffix == ".py":
            why = _py_kafka_evidence(path)
            if why:
                return f"{path.relative_to(REPO)}: {why}"
        elif _GO_KAFKA_CLIENT.search(path.read_text(errors="replace")):
            return f"{path.relative_to(REPO)}: Go Kafka client"
    return None


def _python_entry_module(service):
    tokens = service.get("command")
    tokens = [str(t) for t in (tokens if isinstance(tokens, list) else str(tokens or "").split())]
    return tokens[tokens.index("-m") + 1] if "-m" in tokens else None


def _build_root(service):
    """The narrowest directory holding this service's source.

    The dockerfile's parent, not `build.context`: three services build from the
    repo root, and a census rooted there would call every service a Kafka client.
    """
    build = service.get("build")
    if isinstance(build, str):
        return REPO / build
    if not isinstance(build, dict):
        return None
    dockerfile, context = build.get("dockerfile"), REPO / build.get("context", ".")
    if dockerfile and "/" in dockerfile and (REPO / dockerfile).is_file():
        return (REPO / dockerfile).parent
    return context


@functools.lru_cache(maxsize=None)
def _python_source_roots():
    roots = set()
    for file_name in COMPOSE_FILES:
        for service in _services(file_name).values():
            root = _build_root(service)
            if root and (root / "src").is_dir():
                roots.add(root)
    return tuple(sorted(roots))


def _code_derived_kafka_clients(name, doc=None):
    """{service: evidence} for every service whose OWN CODE talks to Kafka.

    quickstart ships images rather than build stanzas, so build roots are taken
    from docker-compose.yml for the services it shares -- the two files describe
    the same programs, which is the premise this whole module rests on.
    """
    services = (
        {k: v for k, v in (doc.get("services") or {}).items() if isinstance(v, dict)}
        if doc is not None
        else _services(name)
    )
    base_services = _services("docker-compose.yml")
    found = {}
    for svc_name, svc in sorted(services.items()):
        entry = _python_entry_module(svc)
        if entry:
            for root in _python_source_roots():
                if _module_file(root, entry) is None:
                    continue
                for path in _reachable_modules(root, entry):
                    why = _py_kafka_evidence(path)
                    if why:
                        found[svc_name] = f"{path.relative_to(REPO)}: {why}"
                        break
                break
            else:
                continue
            continue
        root = _build_root(svc) or _build_root(base_services.get(svc_name, {}))
        if root and root.is_dir():
            why = _dir_kafka_evidence(root)
            if why:
                found[svc_name] = why
    return found


@pytest.mark.parametrize("name", COMPOSE_FILES)
def test_the_code_derived_census_is_not_empty(name):
    """A census that finds nothing certifies everything."""
    clients = _code_derived_kafka_clients(name)
    assert len(clients) >= 6, (
        f"{name}: the code-derived census found only {sorted(clients)}. Six services "
        "in this repo demonstrably speak Kafka, so a smaller answer means the "
        "resolution of build roots or entry modules broke, not that the services did."
    )


@pytest.mark.parametrize("name", COMPOSE_FILES)
def test_the_code_derived_census_discriminates_within_one_image(name):
    """tool-generator is the live control.

    It is built from the same ./llm-service source tree as llm-service and planner,
    both of which this census must flag. If the census degenerated into "any service
    with a build context" -- the easy way to write it -- tool-generator would be
    flagged too and the census would carry no information. It is excluded only
    because its `-m` entry module reaches no Kafka client.
    """
    clients = _code_derived_kafka_clients(name)
    assert "llm-service" in clients and "planner" in clients, (
        f"{name}: the census lost llm-service/planner, whose entry modules do reach "
        f"a Kafka client. Found: {sorted(clients)}"
    )
    assert "tool-generator" not in clients, (
        f"{name}: the census flagged tool-generator, which shares llm-service's image "
        f"but whose entry module reaches no Kafka client ({clients.get('tool-generator')}). "
        "A census this coarse would pass on any service with a build context."
    )


@pytest.mark.parametrize("name", COMPOSE_FILES)
def test_every_code_derived_kafka_client_declares_the_security_env(name):
    """The obligation the address-driven census structurally cannot state.

    A service reaching this assertion with nothing declared is the shape of the
    original bug: on a BYO broker it connects anonymously over PLAINTEXT, and
    because an unauthenticated client BLOCKS rather than errors, the only symptom
    is a consumer that never delivers.
    """
    anchor_keys = set(_anchor(name))
    failures = []
    for svc_name, evidence in sorted(_code_derived_kafka_clients(name).items()):
        if (name, svc_name) in NOT_A_CLIENT:
            continue
        env = _env_of(_services(name)[svc_name])
        missing = anchor_keys - set(env)
        if missing:
            failures.append(f"{svc_name} ({evidence}): missing {sorted(missing)}")
        elif not any(k in env for k in BROKER_KEYS) and not any(
            k in _command_text(_services(name)[svc_name]) for k in BROKER_KEYS
        ):
            failures.append(
                f"{svc_name} ({evidence}): has the credentials but names no broker, "
                f"so they are aimed at whatever address its image hardcodes"
            )
    assert not failures, (
        f"{name}: these services run Kafka client code and do not carry the profile "
        "for it. Merge `<<: *kafka-security` into their `environment:` (converting "
        "the list form to a map first -- YAML has no list-merge operator) and declare "
        f"one of {list(BROKER_KEYS)}:\n  " + "\n  ".join(failures)
    )


@pytest.mark.parametrize("name", COMPOSE_FILES)
def test_the_code_derived_census_survives_stripping_every_kafka_key(name):
    """The property the address-driven census lacks, asserted directly.

    Re-runs the census against a copy of the compose document with every KAFKA_*
    key deleted from every service. The answer must not move. If it shrinks, this
    census is reading the compose environment somewhere and has inherited the same
    blindness -- a service could again be removed from the denominator by deleting
    its keys, which is indistinguishable from a pass.
    """
    doc = _load(name)
    stripped = 0
    for svc in (doc.get("services") or {}).values():
        if not isinstance(svc, dict):
            continue
        env = svc.get("environment")
        if isinstance(env, dict):
            for key in [k for k in env if str(k).startswith("KAFKA_")]:
                del env[key]
                stripped += 1
        elif isinstance(env, list):
            keep = [i for i in env if not str(i).startswith("KAFKA_")]
            stripped += len(env) - len(keep)
            svc["environment"] = keep
    assert stripped > 20, (
        f"{name}: only {stripped} KAFKA_* keys were stripped, so this mutation is "
        "too weak to prove anything."
    )
    assert _code_derived_kafka_clients(name, doc=doc) == _code_derived_kafka_clients(name), (
        f"{name}: the code-derived census changed when the compose environment was "
        "emptied. It is not code-derived."
    )
