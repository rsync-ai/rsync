"""Every container the chart hands a Kafka bootstrap address must also get a security profile.

This exists because four of them did not, and nothing anywhere said so.

On a secured BYO cluster the failure is silent by construction. A client with a
bootstrap address and no `security.protocol` speaks PLAINTEXT to a SASL listener;
the pod goes Ready, Kafka Connect reports every connector `RUNNING`, and zero rows
move. `helm lint` and `helm template` both pass -- the missing variable is not a
syntax error, it is an absence, and an absence renders as valid YAML. The defect is
only decidable against a broker that actually refuses anonymous clients.

Four separate clients were affected, which is the real lesson: the variable names
were copied per template, so each new Kafka client was a fresh chance to forget.

  kafka-mcp-sink      no security env at all -- its Go worker reads exactly these
                      names and even fails closed on a half-set config
                      (kafka-mcp-sink/worker-src/.../kafka_security.go:55), so the
                      chart, not the code, is what dialled anonymously
  debezium-mcp        same
  kafka-init          same, and worse: it blocked `helm upgrade` outright, because a
                      PLAINTEXT client against a SASL listener does not error, it
                      BLOCKS -- so an unbounded retry loop hangs instead of failing
  Connect TASK clients  the worker's own `CONNECT_*` keys do NOT reach the producer
                      and consumer each connector task creates; those read
                      `producer.`/`consumer.`-prefixed keys. The worker
                      authenticated, reported RUNNING, and every task dialled
                      anonymously underneath it.

The last one is why `test_connect_configures_all_three_client_levels` is separate and
loud: a green worker actively hides a starved task.

Static and cheap on purpose -- this parses `deploy/helm` as text and needs no cluster
and no `helm` binary, matching test_chart_service_hostnames_resolve.py.
"""

import pathlib
import re

import pytest

CHART = pathlib.Path(__file__).resolve().parents[2] / "deploy" / "helm" / "rsync-ai"
TEMPLATES = CHART / "templates"
HELPERS = TEMPLATES / "_helpers.tpl"

# A container is `- name: x` whose next meaningful line is `image:`. An env entry is
# `- name: X` whose next meaningful line is `value:`/`valueFrom:`. That one-line
# lookahead is the whole discriminator, and it is why this does not need a YAML
# parser -- the templates are not valid YAML until Helm has rendered them.
# The name is matched permissively on purpose. A first version of this used a
# character class, which excluded the space in `- name: {{ $svc.name }}` and so
# silently dropped the generation services -- real Kafka clients -- from the
# census. The census still passed, because 7 of 8 is not obviously wrong. That is
# what test_the_parser_sees_every_container below exists to make impossible.
_ITEM = re.compile(r"^(?P<indent>\s*)- name: (?P<name>\S.*?)\s*$")

# Anything that hands a client a bootstrap address, under every prefix in use:
# bare (Go/Python services), `CONNECT_` (Connect worker) and the unprefixed
# `BOOTSTRAP_SERVERS` the Debezium image reads.
_BOOTSTRAP_ENV = ("KAFKA_BROKERS", "KAFKA_BOOTSTRAP_SERVERS", "BOOTSTRAP_SERVERS",
                  "CONNECT_BOOTSTRAP_SERVERS")
_BOOTSTRAP_HELPER = 'include "rsync-ai.kafka.bootstrap"'

# Satisfying the invariant: either take the shared helper (which chains the security
# profile), or take the security helper directly, or -- for Connect, whose variable
# names are prefixed and so cannot come from the shared helper -- set the protocol
# explicitly.
_SECURITY_MARKERS = (
    'include "rsync-ai.kafkaEnv"',
    'include "rsync-ai.kafkaSecurityEnv"',
    "CONNECT_SECURITY_PROTOCOL",
)

# Containers that legitimately name a bootstrap address without being a Kafka client
# themselves. Listed rather than pattern-excluded so adding one is a reviewed
# decision: the default for a new container that sees a broker address is "this is a
# client and must be secured", which is the safe direction to be wrong in.
NOT_A_KAFKA_CLIENT: dict[str, str] = {}


# The roster is PINNED, not discovered, and that distinction is the whole point.
#
# A discovered census can only inspect containers that look like Kafka clients, so
# the one mutation it cannot survive is deletion: strip `rsync-ai.kafkaEnv` from
# kafka-mcp-sink and the container stops looking like a client, drops out of the
# census, and every remaining assertion passes. That was measured, not assumed --
# against a discovered roster this file went from 14 passed to 13 passed with the
# original D3 bug reintroduced, and reported success.
#
# Which is exactly backwards: "this container has no Kafka configuration at all" IS
# the bug. So the set of containers that must be Kafka clients is written down here,
# each with the reason it cannot be dropped, and the census is compared against it.
# Adding a client means adding a line -- a reviewed decision. Losing one fails.
MUST_BE_KAFKA_CLIENTS = {
    ("apps/api-gateway.yaml", "api-gateway"):
        "publishes domain events and consumes agent results",
    ("apps/orchestrator.yaml", "orchestrator"):
        "the agent control plane runs entirely over Kafka topics",
    ("apps/temporal-adapter.yaml", "temporal-adapter"):
        "bridges Temporal workflows onto the agent command topics",
    ("apps/generation.yaml", "{{ $svc.name }}"):
        "the generation services consume agent command topics",
    ("connectors/cdc.yaml", "kafka-connect"):
        "is a Kafka client by definition",
    ("connectors/cdc.yaml", "kafka-mcp-sink"):
        "reads every row off the data topic -- this is the client that dialled "
        "anonymously and moved zero rows while reporting healthy",
    ("connectors/cdc.yaml", "debezium-mcp"):
        "produces CDC events, and its schema history is a SEPARATE client whose "
        "consumer half only fails on task restart",
    ("jobs/kafka-init.yaml", "kafka-init"):
        "creates the 14 static topics; unauthenticated it BLOCKS rather than errors, "
        "which is what made `helm upgrade` hang instead of fail",
}


def _container_blocks():
    """(file, container name, block text) for every container the chart declares."""
    out = []
    for path in sorted(TEMPLATES.rglob("*.yaml")):
        lines = path.read_text().splitlines()
        starts = []
        for i, line in enumerate(lines):
            m = _ITEM.match(line)
            if not m:
                continue
            nxt = next(
                (
                    l
                    for l in lines[i + 1 :]
                    if l.strip() and not l.strip().startswith(("{{/*", "#", "*/}}"))
                ),
                "",
            )
            if re.match(r"\s*image:", nxt):
                starts.append((i, len(m.group("indent")), m.group("name")))
        for n, (i, indent, name) in enumerate(starts):
            end = starts[n + 1][0] if n + 1 < len(starts) else len(lines)
            out.append((str(path.relative_to(TEMPLATES)), name, "\n".join(lines[i:end])))
    return out


def _kafka_client_containers():
    """Containers handed a bootstrap address, and whether they are given a profile."""
    out = {}
    for path, name, block in _container_blocks():
        if name in NOT_A_KAFKA_CLIENT:
            continue
        # `rsync-ai.kafkaEnv` hands out the bootstrap address itself, so a container
        # that includes it is a Kafka client even though the variable name never
        # appears in the template. Leaving this out is not a small miss -- it drops
        # five of the seven clients, and the census then passes by only inspecting
        # the two containers that happen to spell the variable inline.
        gets_bootstrap = (
            _BOOTSTRAP_HELPER in block
            or 'include "rsync-ai.kafkaEnv"' in block
            or any(
                re.search(rf"^\s*- name: {re.escape(v)}\s*$", block, re.M)
                for v in _BOOTSTRAP_ENV
            )
        )
        if not gets_bootstrap:
            continue
        out[(path, name)] = any(m in block for m in _SECURITY_MARKERS)
    return out


def _helper_block(name):
    """The body of one `define` in _helpers.tpl.

    Sliced at the next `define` rather than the next `end`, because these helpers
    contain nested `if`/`end` pairs and stopping at the first `end` would return a
    fragment -- which is worse than returning nothing: a substring assertion
    against a truncated block passes or fails for reasons unrelated to the code.
    """
    text = HELPERS.read_text()
    parts = re.split(rf'\{{\{{-?\s*define\s+"{re.escape(name)}"', text)
    assert len(parts) == 2, f"{name} is not defined exactly once in _helpers.tpl"
    return re.split(r'\{\{-?\s*define\s+"', parts[1])[0]


def test_the_chart_was_parsed():
    """Vacuity floor. A rename or a layout change under deploy/helm must fail here
    loudly rather than quietly reduce this whole file to zero assertions -- `0 of 0
    containers are insecure` is green and means nothing."""
    containers = _container_blocks()
    clients = _kafka_client_containers()
    assert len(containers) >= 10, f"chart containers parsed as near-empty: {containers}"
    assert len(clients) >= 5, (
        f"Kafka clients parsed as near-empty: {sorted(clients)}. Before the #836 sweep "
        f"the real number was 7; a sudden drop means the parser stopped matching, not "
        f"that the chart got simpler."
    )


def test_the_parser_sees_every_container():
    """Parser-integrity check: every `image:` in the chart must belong to a container
    this file found.

    Without this, a template the parser cannot read is indistinguishable from a
    template with nothing to report -- the census stays green while quietly covering
    less. That is not hypothetical: the first version of `_ITEM` used a character class
    that excluded the space in `- name: {{ $svc.name }}`, dropping the generation
    services from the census entirely. The count went 8 -> 7, which is not visibly
    wrong, and the vacuity floor (>= 5) happily passed.
    """
    found = len(_container_blocks())
    declared = sum(
        len(re.findall(r"^\s*image:", path.read_text(), re.M))
        for path in sorted(TEMPLATES.rglob("*.yaml"))
    )
    assert found == declared, (
        f"the chart declares {declared} container images but the parser found "
        f"{found} containers -- some template is not being read, and every container "
        f"inside it is silently exempt from the Kafka security census"
    )


def test_no_kafka_client_has_quietly_stopped_being_one():
    """The deletion check. Everything else here inspects containers that still look
    like Kafka clients; this is the only assertion that fires when one stops."""
    found = set(_kafka_client_containers())
    expected = set(MUST_BE_KAFKA_CLIENTS)
    vanished = expected - found
    assert not vanished, (
        "these containers must be Kafka clients but are no longer given any Kafka "
        "configuration at all:\n"
        + "\n".join(f"  {p}: {n} -- {MUST_BE_KAFKA_CLIENTS[(p, n)]}" for p, n in sorted(vanished))
        + "\n\nOn a secured cluster this is silent: the pod goes Ready and moves no data."
    )
    unlisted = found - expected
    assert not unlisted, (
        "new Kafka clients are not in MUST_BE_KAFKA_CLIENTS:\n"
        + "\n".join(f"  {p}: {n}" for p, n in sorted(unlisted))
        + "\n\nAdd them with the reason they need Kafka. An unpinned client is one "
        "that can later be dropped without this file noticing."
    )


def test_the_not_a_client_allowlist_has_no_stale_entries():
    """An exemption for a container that no longer exists is documentation claiming
    an exemption for nothing, and it hides the next real one."""
    declared = {name for _, name, _ in _container_blocks()}
    for name in NOT_A_KAFKA_CLIENT:
        assert name in declared, (
            f"container `{name}` is exempted from the Kafka security census but is no "
            f"longer declared in the chart -- drop the entry"
        )


@pytest.mark.parametrize(
    "path,name", sorted(_kafka_client_containers()), ids=lambda v: str(v)
)
def test_every_kafka_client_container_gets_a_security_profile(path, name):
    assert _kafka_client_containers()[(path, name)], (
        f"{path}: container `{name}` is given a Kafka bootstrap address but no security "
        f"profile. Against a SASL broker it will connect anonymously: the pod stays "
        f"Ready, the connector reports RUNNING, and no rows move. Include "
        f'`rsync-ai.kafkaEnv` (bootstrap + profile together), or `rsync-ai.'
        f"kafkaSecurityEnv` if the container sets its own bootstrap variable."
    )


def test_connect_configures_all_three_client_levels():
    """Kafka Connect runs THREE client families and the worker's keys reach only one.

    Each connector task builds its own producer and consumer, which read
    `producer.`/`consumer.`-prefixed configuration. Giving the worker credentials and
    stopping there produces the worst possible signal: the worker authenticates, the
    REST API reports every connector RUNNING, and each task dials anonymously
    underneath it. There is no error anywhere -- only zero rows.
    """
    cdc = (TEMPLATES / "connectors" / "cdc.yaml").read_text()
    for level, var in (
        ("worker", "CONNECT_SECURITY_PROTOCOL"),
        ("task producer", "CONNECT_PRODUCER_SECURITY_PROTOCOL"),
        ("task consumer", "CONNECT_CONSUMER_SECURITY_PROTOCOL"),
    ):
        assert re.search(rf"^\s*- name: {var}\s*$", cdc, re.M), (
            f"Kafka Connect's {level} client has no `{var}`. The worker's own "
            f"`CONNECT_*` keys are NOT inherited by task clients, so a secured cluster "
            f"gives you RUNNING connectors that move nothing."
        )
    # The JAAS half is asserted on `export`, not on `- name:`, because these three can
    # no longer be env vars. Their value embeds the password, and a JAAS password must
    # be escaped for two nested grammars (Java .properties, then Kafka's
    # StreamTokenizer). Helm cannot do that escaping: the chart only ever writes the
    # kubelet substitution `$(KAFKA_SASL_PASSWORD)`, and the real password is spliced
    # in AFTER rendering, downstream of anything a template could quote. So the chart
    # ships an entrypoint prelude that escapes and exports them inside the container
    # instead -- see the comment block in cdc.yaml.
    #
    # The invariant is unchanged and still the point of this test: all THREE levels, or
    # the tasks dial anonymously under a green worker.
    for level, var in (
        ("worker", "CONNECT_SASL_JAAS_CONFIG"),
        ("task producer", "CONNECT_PRODUCER_SASL_JAAS_CONFIG"),
        ("task consumer", "CONNECT_CONSUMER_SASL_JAAS_CONFIG"),
    ):
        assert re.search(rf"^\s*export {var}=", cdc, re.M), (
            f"Connect's {level} has a security protocol but nothing exports `{var}` -- "
            f"it would attempt SASL with no credentials."
        )
        # And it must NOT come back as a plain env var: that form cannot escape the
        # password, and it fails as an ordinary authentication error that points at the
        # credential rather than at the encoding.
        assert not re.search(rf"^\s*- name: {var}\s*$", cdc, re.M), (
            f"`{var}` is set as a container env var again. Helm renders "
            f"`$(KAFKA_SASL_PASSWORD)` there and the kubelet substitutes the raw value "
            f"afterwards, so any password containing a backslash or a quote is silently "
            f"corrupted before Kafka ever parses it."
        )


# --------------------------------------------------------------------------
# TLS trust material
#
# The same absence-is-valid-YAML problem, one layer down. A container can be
# handed KAFKA_SSL_CA_LOCATION and never have the file mounted: the env var
# renders, the pod goes Ready, and the client fails at the handshake naming a
# path rather than the missing volume. Nothing in `helm template` or `helm lint`
# can see it, because a volumeMount that was never written is not an error.
# --------------------------------------------------------------------------

_TLS_MOUNT = 'include "rsync-ai.kafkaTLSVolumeMount"'
_TLS_VOLUME = 'include "rsync-ai.kafkaTLSVolume"'


@pytest.mark.parametrize(
    "path,name", sorted(_kafka_client_containers()), ids=lambda v: str(v)
)
def test_every_kafka_client_container_mounts_the_tls_material(path, name):
    """A security profile without the files it points at is not a profile.

    `rsync-ai.kafkaSecurityEnv` emits KAFKA_SSL_CA_LOCATION for every client at
    once; the mount is per-container and has to be written eight times. That
    asymmetry is exactly the shape that produced the original defect -- one
    shared source of truth for the variable names, and a hand-copied block for
    everything else.

    The helper is a no-op when TLS is off, so this is safe to require
    unconditionally: on a PLAINTEXT deployment it renders nothing.
    """
    block = next(b for f, n, b in _container_blocks() if (f, n) == (path, name))
    assert _TLS_MOUNT in block, (
        f"{path}: container `{name}` is given a Kafka security profile but never "
        f"mounts the TLS material. On a SASL_SSL or SSL cluster it is handed "
        f"KAFKA_SSL_CA_LOCATION pointing at a path that does not exist, and dies "
        f"in the handshake naming the file rather than the missing volume. Add "
        f'`{{{{- include "rsync-ai.kafkaTLSVolumeMount" . | nindent N }}}}`.'
    )


@pytest.mark.parametrize(
    "path", sorted({p for p, _ in _kafka_client_containers()})
)
def test_every_template_that_mounts_tls_also_declares_the_volume(path):
    """A mount with no matching volume is the one half of this that k8s DOES
    catch -- but only at apply time, on the cluster, after the render passed.

    Pairing them here keeps the failure at the point where the file is edited.
    """
    text = (TEMPLATES / path).read_text()
    if _TLS_MOUNT not in text:
        pytest.skip("no TLS mount in this template")
    assert _TLS_VOLUME in text, (
        f"{path} mounts `kafka-tls` but never declares the volume. The render "
        f"succeeds and the API server rejects the pod with "
        f'`volume "kafka-tls" not found` -- at apply time, not at review time.'
    )


def test_the_connect_truststore_is_configured_at_all_three_client_levels():
    """The three-client-family problem again, for TLS this time.

    `CONNECT_SSL_TRUSTSTORE_LOCATION` configures the worker. The producer and
    consumer each task builds read `producer.`/`consumer.`-prefixed keys and
    inherit nothing. Give the worker a truststore and stop there and you get the
    same signal as the original defect: the worker authenticates, the REST API
    reports RUNNING, and every task fails its own handshake underneath.
    """
    cdc = (TEMPLATES / "connectors" / "cdc.yaml").read_text()
    for level, var in (
        ("worker", "CONNECT_SSL_TRUSTSTORE_LOCATION"),
        ("task producer", "CONNECT_PRODUCER_SSL_TRUSTSTORE_LOCATION"),
        ("task consumer", "CONNECT_CONSUMER_SSL_TRUSTSTORE_LOCATION"),
    ):
        assert re.search(rf"^\s*- name: {var}\s*$", cdc, re.M), (
            f"Kafka Connect's {level} client has no `{var}`. On a TLS listener it "
            f"validates against the JDK cacerts, which do not contain a private CA."
        )


def test_every_jvm_truststore_and_keystore_is_declared_PEM():
    """KIP-651 is the keystone of this whole design, and it is one word.

    A JVM defaults to JKS. Point it at a PEM bundle without `type=PEM` and it
    reports a keystore FORMAT error -- which reads as a corrupt certificate and
    sends you to inspect the file rather than the setting. Declaring the type is
    what lets ONE mounted ca.crt serve the Go clients, the Python clients, Kafka
    Connect and the Kafka CLI with no conversion step.

    Asserted as a pairing, not a presence: every `*_LOCATION` must have the
    matching `*_TYPE` beside it, so a new client level cannot be added with only
    half of the pair.
    """
    for rel in ("connectors/cdc.yaml", "jobs/kafka-init.yaml"):
        text = (TEMPLATES / rel).read_text()
        for store in ("TRUSTSTORE", "KEYSTORE"):
            # Anchored at both ends. Without the trailing `\s*$`, `_TYPE` matches
            # inside `_TYPE_ANYTHING` and a renamed variable still satisfies the
            # pairing -- which mutation testing caught: this exact assertion passed
            # with CONNECT_PRODUCER_SSL_TRUSTSTORE_TYPE renamed out of existence.
            locs = set(re.findall(
                rf"^\s*- name: (CONNECT_\w*?SSL_{store})_LOCATION\s*$", text, re.M))
            types = set(re.findall(
                rf"^\s*- name: (CONNECT_\w*?SSL_{store})_TYPE\s*$", text, re.M))
            assert locs == types, (
                f"{rel}: {store} location/type mismatch -- location for "
                f"{sorted(locs - types)} with no `_TYPE=PEM`. The JVM will assume "
                f"JKS and report a keystore format error."
            )
        # the shell-script form, used by the kafka-init AdminClient config
        for prop in ("truststore", "keystore"):
            if f"ssl.{prop}.location" in text:
                assert f"ssl.{prop}.type=PEM" in text, (
                    f"{rel} writes ssl.{prop}.location into a client properties file "
                    f"without ssl.{prop}.type=PEM"
                )


@pytest.mark.parametrize(
    "rel", ["connectors/cdc.yaml", "jobs/kafka-init.yaml"]
)
def test_every_shell_built_jaas_line_escapes_for_both_grammars(rel):
    """A SASL password reaches Kafka through two nested grammars, not one.

    Both of these templates build `sasl.jaas.config` in shell, and in both the value
    then crosses a Java .properties file before Kafka's JAAS parser sees it:

      * kafka-init writes it to the file it passes as `--command-config`;
      * cdc.yaml exports it as `CONNECT_*`, and the Debezium image's
        docker-entrypoint.sh appends every `CONNECT_*` variable to
        connect-distributed.properties and loads that.

    `Properties.load` consumes one level of backslash and the JAAS
    StreamTokenizer consumes another, so the value has to be escaped TWICE. Escaping
    it once produces, for a password containing a backslash, output byte-identical to
    not escaping it at all -- a guard that looks present and does nothing. Measured
    against kafka-clients 3.7.0 (test/kind/jaas-probe/run.sh):

        C:\\Users\\svc  once-escaped -> C:Userssvc   (wrong password, no error)
        C:\\Users\\svc  twice-escaped -> C:\\Users\\svc  (correct)

    Counting `sed` invocations is crude, but it is the property that actually broke:
    the shipped kafka-init had exactly one.
    """
    text = (TEMPLATES / rel).read_text()
    esc = re.search(r"esc\(\) \{(.+?)\n\s*\}", text, re.S)
    assert esc, f"{rel} no longer defines esc() -- nothing escapes the password"
    assert esc.group(1).count("sed") == 2, (
        f"{rel}'s esc() is not the two-pass escaper. One pass is indistinguishable "
        f"from no escaping for a backslash, and the resulting failure is an ordinary "
        f"broker authentication error that points at the credential, not the encoding."
    )


def test_the_kafka_init_admin_client_is_given_the_truststore():
    """kafka-init is a JVM client, and its failure mode is the loudest of the lot.

    An unauthenticated or untrusting AdminClient against a secured listener does
    not error, it BLOCKS -- so `helm upgrade` hangs on the pre-install hook
    instead of failing. That is what made the original SASL gap so expensive, and
    TLS reaches it by exactly the same path.
    """
    text = (TEMPLATES / "jobs" / "kafka-init.yaml").read_text()
    assert "ssl.truststore.location=$KAFKA_SSL_CA_LOCATION" in text, (
        "kafka-init builds a client properties file with no truststore. Against a "
        "private CA the AdminClient blocks rather than errors, and `helm upgrade` "
        "hangs with no message."
    )
    assert _TLS_MOUNT in text, "kafka-init names a CA path it never mounts"


def test_the_skip_verify_variable_is_the_one_spelling_both_runtimes_read():
    """There are two names for this setting and only one reaches everything.

    Go reads KAFKA_SSL_INSECURE_SKIP_VERIFY and accepts KAFKA_SSL_SKIP_VERIFY as
    an alias (shared/go/kafkaclient/config.go:80,115). Python reads
    KAFKA_SSL_SKIP_VERIFY and nothing else (kafka_security.py:49). So the
    Go-native spelling silently leaves verification ON for llm-service, planner,
    tool-generator and debezium-mcp -- half the platform, no error anywhere.
    """
    # Matched against the emitted `- name:` lines, not the raw text: the helper
    # explains this trap in a comment that names BOTH spellings, and a substring
    # search would fail on the documentation rather than on the code.
    emitted = set(re.findall(r"^\s*- name: (KAFKA_SSL\w*SKIP_VERIFY)\s*$",
                             _helper_block("rsync-ai.kafkaSecurityEnv"), re.M))
    assert "KAFKA_SSL_SKIP_VERIFY" in emitted, (
        f"rsync-ai.kafkaSecurityEnv does not emit KAFKA_SSL_SKIP_VERIFY (emits {emitted})"
    )
    assert "KAFKA_SSL_INSECURE_SKIP_VERIFY" not in emitted, (
        "rsync-ai.kafkaSecurityEnv emits the Go-only spelling "
        "KAFKA_SSL_INSECURE_SKIP_VERIFY. Python reads only KAFKA_SSL_SKIP_VERIFY, "
        "so every Python client would keep verifying while the Go ones stopped -- "
        "a split-brain TLS policy with nothing to indicate it."
    )


def test_the_tls_helpers_key_off_the_protocol_not_the_material():
    """`usesTLS` must ask what the listener speaks, not what the operator pasted.

    Deriving it from "is a caCert set" inverts the useful case: managed Kafka
    (MSK, Confluent Cloud, Aiven) chains to a public root and needs NO caCert, so
    a material-derived helper would decide those clusters are plaintext and drop
    the protocol for every client.
    """
    block = _helper_block("rsync-ai.kafka.usesTLS")
    assert "securityProtocol" in block, (
        "rsync-ai.kafka.usesTLS no longer reads securityProtocol -- if it is "
        "derived from the certificate material instead, every managed cluster "
        "with a publicly-trusted certificate silently drops to plaintext."
    )
    assert "SASL_SSL" in block and "SSL" in block


def test_the_shared_env_helper_chains_the_security_profile():
    """`rsync-ai.kafkaEnv` is the single definition every Go/Python client takes. If it
    stops chaining the profile, all of them silently lose it at once -- which is the
    failure mode the shared helper exists to make impossible."""
    text = HELPERS.read_text()
    body = re.split(r'\{\{-?\s*define\s+"rsync-ai\.kafkaEnv"', text)
    assert len(body) == 2, "rsync-ai.kafkaEnv is not defined exactly once"
    block = re.split(r"\{\{-?\s*end\s*-?\}\}", body[1])[0]
    assert "KAFKA_BROKERS" in block, "rsync-ai.kafkaEnv no longer emits a bootstrap address"
    assert 'include "rsync-ai.kafkaSecurityEnv"' in block, (
        "rsync-ai.kafkaEnv hands out a bootstrap address without chaining "
        "rsync-ai.kafkaSecurityEnv -- every Go and Python client in the chart loses "
        "its credentials in one edit, with no rendering error."
    )


def test_the_login_module_is_derived_from_the_mechanism_and_fails_closed():
    """The JAAS login module is a FUNCTION of the SASL mechanism, not a constant.

    It was a constant (`ScramLoginModule`), so `saslMechanism: PLAIN` rendered
    perfectly valid YAML and then died inside the broker handshake naming a module the
    operator never chose. An unsupported mechanism must fail at RENDER time, where the
    message can name the actual problem.
    """
    text = HELPERS.read_text()
    body = re.split(r'\{\{-?\s*define\s+"rsync-ai\.kafka\.saslLoginModule"', text)
    assert len(body) == 2, (
        "rsync-ai.kafka.saslLoginModule is not defined exactly once -- if the login "
        "module has gone back to being written inline, it is a constant again"
    )
    block = re.split(r'\{\{-?\s*define\s+"', body[1])[0]
    assert "PlainLoginModule" in block and "ScramLoginModule" in block, (
        "the login-module helper no longer distinguishes PLAIN from SCRAM"
    )
    assert re.search(r"\bfail\b", block), (
        "the login-module helper has no `fail` branch, so an unsupported mechanism "
        "renders a config that is only rejected later, inside a broker handshake"
    )


# --- OAUTHBEARER ------------------------------------------------------------------
#
# The mechanism is not "SCRAM with a different module name", and every assumption
# carried over from SCRAM produces a client that starts and then cannot authenticate:
#
#   * the credentials are JAAS OPTIONS on the login-module line, not `username=` /
#     `password=`. A credential-less `LoginModule required;` line -- the shape the
#     other mechanisms suggest -- is refused outright by the JVM with "The OAuth
#     configuration option clientId value must be non-null";
#   * the token endpoint is a SEPARATE client property, not a JAAS option;
#   * omitting the login callback handler is silent: OAuthBearerLoginModule falls
#     back to its UNSECURED default and mints a self-signed JWS, so the broker
#     rejects a token it cannot validate and the error describes the token rather
#     than the missing handler.
#
# Measured against kafka-clients 3.7.0, not inferred -- reproduce the table with
# deploy/helm/rsync-ai/test/kind/jaas-probe/OAuthProbe.java.
#
# These are static because reading the table off the templates needs no `helm`
# binary and no render, so it holds on any checkout. It used to say CI has none
# and cite the lane's Python-only setup; the setup steps are Python-only, but
# ci.yml now sets helm up for that job and asserts it before pytest, so the
# render-based chart tests run there too. See the `helm is present` step in
# .github/workflows/ci.yml.

_OAUTH_ENV = (
    "KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT",
    "KAFKA_SASL_OAUTHBEARER_CLIENT_ID",
    "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET",
    "KAFKA_SASL_OAUTHBEARER_SCOPE",
    "KAFKA_SASL_OAUTHBEARER_EXTENSIONS",
)

_REPO = CHART.parents[2]


def test_the_chart_and_both_client_runtimes_spell_the_oauth_variables_identically():
    """The chart is the only writer; a typo here is invisible on both sides.

    The Go clients and llm-service each read these names from their own constant
    tables. A misspelling in the chart does not fail to render and does not fail to
    start -- the reader simply sees an unset variable, falls back to "no OAuth
    configured", and dials with a mechanism it has no credentials for.

    Asserted in BOTH directions, because only one of them catches a typo. "Every
    name a runtime reads appears somewhere in the chart" is satisfied by a substring
    (KAFKA_SASL_OAUTHBEARER_SCOPE is a prefix of ..._SCOPES) and by any one of the
    templates that duplicate the block -- mutation testing showed the misspelling
    surviving. The direction that bites is the other one: every variable the chart
    EMITS must be one some runtime reads.
    """
    go = (_REPO / "shared" / "go" / "kafkaclient" / "config.go").read_text()
    py = (_REPO / "llm-service" / "src" / "utils" / "kafka_security.py").read_text()
    templates = {p: p.read_text() for p in TEMPLATES.rglob("*.yaml")}
    templates[HELPERS] = HELPERS.read_text()

    emitted = {}
    for path, text in templates.items():
        for name in re.findall(r"^\s*- name: (KAFKA_SASL_OAUTHBEARER_\w+)\s*$", text, re.M):
            emitted.setdefault(name, set()).add(path.name)

    # JVM-only: the login callback handler is a Java class name, so no Go client
    # reads it. llm-service does, for the Debezium schema-history properties.
    jvm_only = {"KAFKA_SASL_OAUTHBEARER_LOGIN_CALLBACK_HANDLER"}

    for name, where in sorted(emitted.items()):
        if name in jvm_only:
            assert f'"{name}"' in py, (
                f"{name} is emitted by {sorted(where)} but llm-service does not read it"
            )
            continue
        assert f'"{name}"' in go and f'"{name}"' in py, (
            f"{name} is emitted by {sorted(where)} but is not in the Go table "
            f"(shared/go/kafkaclient/config.go) and/or the Python one "
            f"(llm-service/src/utils/kafka_security.py). Neither runtime fails on an "
            "unknown variable -- it is simply unset, and the client proceeds with "
            "mechanism=OAUTHBEARER and no credentials."
        )

    missing = set(_OAUTH_ENV) - set(emitted)
    assert not missing, (
        f"{sorted(missing)} are read by the Go and Python clients but the chart "
        "emits them nowhere as a container env var"
    )


def _enclosing_conditions(text, needle):
    """The stack of `{{ if }}` conditions still open where `needle` appears.

    A "nearest enclosing if" shortcut is not enough: mutation testing showed the
    OAuth key re-nested INSIDE the saslPassword block while keeping its own `if`,
    which leaves the nearest one correct and the key still unreachable. The whole
    open stack is the thing that decides whether the key is written.
    """
    stack = []
    for m in re.finditer(
        r"\{\{-?\s*(if|else if|else|end|range|with)\b(.*?)-?\}\}|" + re.escape(needle),
        text, re.S,
    ):
        kw = m.group(1)
        if kw is None:
            return stack
        if kw in ("if", "range", "with"):
            stack.append(m.group(2).strip())
        elif kw == "end" and stack:
            stack.pop()
        elif kw == "else if" and stack:
            stack[-1] = m.group(2).strip()
    raise AssertionError(f"{needle} not found")


def test_the_oauth_client_secret_is_not_gated_on_the_scram_password():
    """The two credentials are alternatives, so one cannot guard the other.

    This is a defect that shipped for the length of one render: the OAuth client
    secret was added as a second line under
    `{{- if .Values.kafka.external.saslPassword }}`, which is never set for
    OAUTHBEARER. The key was therefore absent from the Secret, the env var
    references it with `optional: true`, and every container came up with an empty
    client secret and failed against the IdP -- pods Ready, zero rows.

    Caught by rendering the Secret, not by lint: an absent key is valid YAML.
    """
    text = (TEMPLATES / "secret.yaml").read_text()
    open_ifs = _enclosing_conditions(text, "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET:")
    assert open_ifs, "the OAuth client secret is emitted unconditionally"
    bad = [c for c in open_ifs if "saslPassword" in c]
    assert not bad, (
        "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET is enclosed by "
        f"`{bad[0]}`. OAUTHBEARER has no saslPassword, so the key would never be "
        "written -- and the secretKeyRef that reads it is `optional: true`, so "
        "nothing fails until the IdP refuses the empty secret."
    )
    assert any("oauth" in c for c in open_ifs), (
        f"the client secret is gated on {open_ifs} -- none of which mentions the "
        "oauth block, so it is written for deployments that have no OAuth config"
    )


@pytest.mark.parametrize("rel", ["connectors/cdc.yaml", "jobs/kafka-init.yaml"])
def test_every_shell_that_builds_an_oauthbearer_jaas_line_is_complete(rel):
    """Three settings, and each one is silent on its own when missing.

    A line with the module but no clientId is a hard ConfigException (loud). The
    other two are quiet: no token endpoint and the JVM cannot fetch anything; no
    callback handler and it happily mints an unsecured self-signed token instead.
    """
    text = (TEMPLATES / rel).read_text()
    assert "OAuthBearerLoginModule" in text, (
        f"{rel} builds JAAS in shell but has no OAUTHBEARER arm -- a chart that "
        "renders saslMechanism=OAUTHBEARER would hit its unsupported-mechanism "
        "branch, or worse, emit a username/password line the JVM refuses"
    )
    for prop in ("clientId", "clientSecret"):
        assert f'{prop}="' in text, (
            f"{rel} does not put {prop} on the JAAS line. OAUTHBEARER takes its "
            "credentials as JAAS options; without them the JVM fails at configure() "
            f'with "The OAuth configuration option {prop} value must be non-null"'
        )
    lowered = text.lower()
    assert "login.callback.handler" in lowered or "login_callback_handler" in lowered, (
        f"{rel} sets no OAUTHBEARER login callback handler. OAuthBearerLoginModule "
        "then falls back to its UNSECURED default and mints a self-signed JWS; the "
        "broker rejects a token it cannot validate and names the token, not the "
        "missing handler."
    )
    assert "token.endpoint.url" in lowered or "token_endpoint_url" in lowered, (
        f"{rel} never sets sasl.oauthbearer.token.endpoint.url -- it is a client "
        "property, NOT a JAAS option, so putting the endpoint on the module line "
        "does not satisfy it"
    )


# The OAUTHBEARER branch, delimited per template. Anchoring on the module name
# instead does not work: kafka-init names it in its `case` statement long before
# the emit, so the "branch" swallowed the whole file and the assertion below passed
# for the wrong reason -- which it did, on the first run.
_OAUTH_BRANCH = {
    "connectors/cdc.yaml": (
        '{{- if include "rsync-ai.kafka.isTokenMechanism" . }}',
        "{{- else }}",
    ),
    "jobs/kafka-init.yaml": (
        'if [ "$KAFKA_SASL_MECHANISM" = "OAUTHBEARER" ]; then',
        "\n                else\n",
    ),
}


@pytest.mark.parametrize("rel", sorted(_OAUTH_BRANCH))
def test_the_oauthbearer_jaas_line_carries_no_username_or_password(rel):
    """A username on an OAUTHBEARER line is not ignored -- it is a config error.

    Both templates still build a `username=`/`password=` line for PLAIN and SCRAM,
    so the assertion has to be scoped to the OAUTHBEARER arm specifically.
    """
    text = (TEMPLATES / rel).read_text()
    open_, close = _OAUTH_BRANCH[rel]
    assert open_ in text, f"{rel}: OAUTHBEARER branch opener not found -- {open_!r}"
    arm = text[text.index(open_) + len(open_):]
    assert close in arm, f"{rel}: OAUTHBEARER branch has no {close!r} -- unscoped slice"
    arm = arm[: arm.index(close)]
    # Vacuity floor: an empty or near-empty slice satisfies every `not in` below.
    assert "clientId" in arm, (
        f"{rel}: the sliced OAUTHBEARER branch does not even set clientId -- the "
        "delimiters are wrong and this test is asserting against nothing"
    )
    for forbidden in ('username="', 'password="', "KAFKA_SASL_PASSWORD"):
        assert forbidden not in arm, (
            f"{rel}'s OAUTHBEARER arm references {forbidden} -- the mechanism has "
            "neither, and the JVM rejects unknown JAAS options rather than "
            "ignoring them"
        )


def test_validate_does_not_demand_a_username_for_token_mechanisms():
    """The old check demanded saslUsername for EVERY mechanism.

    For OAUTHBEARER that is an outright lie: the operator is told to invent a
    username, and the env block then drops it, so the value they were forced to
    supply reaches no container at all.
    """
    text = (TEMPLATES / "validate.yaml").read_text()
    assert "rsync-ai.kafka.isTokenMechanism" in text, (
        "validate.yaml no longer branches on the mechanism family, so its "
        "credential requirements apply the SCRAM shape to OAUTHBEARER"
    )
    for key in ("oauth.tokenEndpoint", "oauth.clientId"):
        assert key in text, (
            f"validate.yaml does not require kafka.external.{key}. Left empty it is "
            "not a render error -- the pod starts and fails against the IdP later."
        )


def test_the_shared_env_helper_skips_the_password_for_token_mechanisms():
    """Emitting both shapes is not harmlessly redundant.

    KAFKA_SASL_PASSWORD alongside OAUTHBEARER means the client secret exists in a
    second variable under a name whose only documented meaning is "SCRAM password",
    and the Go client's ignored-settings warning would not fire because the variable
    is legitimately set for a different mechanism.
    """
    block = _helper_block("rsync-ai.kafkaSecurityEnv")
    assert "rsync-ai.kafka.isTokenMechanism" in block, (
        "kafkaSecurityEnv does not branch on the mechanism family; it emits "
        "KAFKA_SASL_USERNAME/PASSWORD for OAUTHBEARER too"
    )
    assert "KAFKA_SASL_OAUTHBEARER_CLIENT_ID" in block, (
        "kafkaSecurityEnv emits no OAuth variables, so every Go and Python client "
        "sees mechanism=OAUTHBEARER with nothing to authenticate with"
    )


def test_the_login_module_allowlist_covers_oauthbearer():
    block = _helper_block("rsync-ai.kafka.saslLoginModule")
    assert "OAuthBearerLoginModule" in block, (
        "saslLoginModule has no OAUTHBEARER branch, so validate.yaml's allowlist "
        "(which calls this helper) rejects the mechanism the rest of the chart "
        "now supports"
    )
    assert "AWS_MSK_IAM" in block, (
        "the fail message no longer names AWS_MSK_IAM -- it is the one mechanism "
        "genuinely unimplemented here, and an operator who reaches this branch "
        "needs to be told which of the two situations they are in"
    )


# --- the guard that rejected the plainest BYO config there is -------------------

USERNAME_FAIL = "kafka.external.saslMechanism is set but kafka.external.saslUsername is empty"


def test_the_missing_username_check_cannot_fire_without_a_mechanism():
    """A BYO cluster with no SASL at all must not be told to supply a SASL username.

    Adding the OAUTHBEARER branch restructured this check from

        {{- if and $isSASL $ext.saslMechanism (not $ext.saslUsername) }}{{ fail }}

    into an if/else on the mechanism family, and the rewrite dropped
    `$ext.saslMechanism` from the else-arm. The arm then read "not a token
    mechanism AND no username" -- which an UNSET mechanism satisfies. So
    `securityProtocol: PLAINTEXT` with no SASL block, the simplest BYO values file
    anyone can write and the S0 baseline of the kind harness, stopped rendering.

    It survived every stage render because S1..S4 all set a mechanism; only the
    baseline broke, and a baseline is exactly what nobody re-renders once it is
    green. The detector was rendering EVERY stage including the base, and this is
    the static half of that, because a text scan needs no helm binary.

    Asserted on the enclosing *guard*, not on the message: the message was correct
    the entire time the behaviour was wrong -- it named a mechanism that was not set.
    """
    text = (TEMPLATES / "validate.yaml").read_text()
    assert USERNAME_FAIL in text, (
        "the missing-username check is gone entirely, so a SCRAM cluster with no "
        "username now renders and fails broker-side instead"
    )
    open_ifs = _enclosing_conditions(text, USERNAME_FAIL)

    # Vacuity floor: a walker that fell off the end returns [], which satisfies
    # every `not any(...)` phrasing of the checks below.
    assert len(open_ifs) >= 2, (
        f"the missing-username fail is enclosed by {open_ifs} -- too shallow to be "
        "the BYO branch, so the walker is not reading what this test thinks it is"
    )
    assert any("kafka.enabled" in c for c in open_ifs), (
        "the fail is no longer inside the BYO-only block, so it now also applies to "
        "deployments running the chart's own broker"
    )

    # The point of the whole test: the mechanism must be REQUIRED, positively.
    # `not $ext.saslMechanism` would satisfy a bare substring check while meaning
    # the exact opposite, so the negated form is rejected explicitly.
    mech = [c for c in open_ifs if "saslMechanism" in c]
    assert mech, (
        "nothing enclosing the missing-username fail requires saslMechanism to be "
        f"set, so PLAINTEXT-with-no-SASL reaches it. Open conditions: {open_ifs}"
    )
    assert any("not $ext.saslMechanism" not in c for c in mech), (
        f"saslMechanism appears only negated ({mech}) -- the fail now fires when the "
        "mechanism is ABSENT, which is the regression this test exists for"
    )
    assert any("$isSASL" in c for c in open_ifs), (
        f"the missing-username fail is no longer gated on a SASL protocol: {open_ifs}"
    )
