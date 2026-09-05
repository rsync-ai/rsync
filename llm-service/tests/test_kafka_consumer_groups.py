"""Consumer-group namespacing for the Python half of the platform.

Topics have been funnelled through one naming function for a while. Consumer
groups were not: every call site spelled its own bare literal. That is invisible
on a broker we own and load-bearing on a customer-managed one, because the
operator writes ACLs there. Granting PREFIXED ``rsync.`` covers the topics and
NOT a bare group id, and Kafka answers the resulting join with an authorization
failure -- which surfaces as a consumer that simply never receives anything. No
crash, no error in the service log, just a queue that stops draining.

So these tests pin two things:

  * ``src/utils/kafka_topics.group`` obeys the same table as ``topic``, checked
    against the *same* fixture Go's ``kafkaclient.Group`` is checked against
    (``shared/contracts/kafka-topic-naming.json``). The two languages have to
    agree byte-for-byte; a divergence is silent for exactly the reason above.
  * every group id this service actually mints comes out namespaced -- pinned as
    concrete strings, because "it calls group()" is a claim about the code and
    "the broker sees rsync.planner-service" is a claim about the deployment.
"""

import ast
import importlib.util
import json
import sys
from pathlib import Path

import pytest

LLM_SERVICE = Path(__file__).resolve().parents[1]
REPO = LLM_SERVICE.parent
sys.path.insert(0, str(LLM_SERVICE))

from src.utils.avro_kafka import _consumer_group_id  # noqa: E402
from src.utils.kafka_topics import (  # noqa: E402
    DEFAULT_TOPIC_PREFIX,
    ENV_TOPIC_PREFIX,
    group,
    topic,
    topics,
)

CONTRACT = REPO / "shared" / "contracts" / "kafka-topic-naming.json"
PLANNER = LLM_SERVICE / "src" / "agents" / "planner" / "kafka_consumer.py"
PII_SCANNER = LLM_SERVICE / "src" / "agents" / "pii_scanner" / "kafka_consumer.py"
AVRO = LLM_SERVICE / "src" / "utils" / "avro_kafka.py"


@pytest.fixture(autouse=True)
def _clear_prefix_env(monkeypatch):
    """Every test starts from an unconfigured environment -- which is also how
    most deployments actually run, so it is the case that matters most."""
    monkeypatch.delenv(ENV_TOPIC_PREFIX, raising=False)


def _contract_cases():
    assert CONTRACT.is_file(), f"shared contract not found at {CONTRACT}"
    cases = json.loads(CONTRACT.read_text())["cases"]
    assert cases, "the shared contract has no cases; this would pass vacuously"
    return cases


def _load(path: Path, name: str):
    """Load a module from its path under a throwaway name.

    Same technique ``test_scrubber_parity.py`` uses on the connector scrubber:
    it reaches the real file without disturbing the ``sys.modules`` entry other
    tests may already hold.
    """
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _module_constant_expr(path: Path, name: str) -> ast.expr:
    """Return the AST of a module-level ``name = <expr>`` assignment."""
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return node.value
    raise AssertionError(f"{path.name} has no module-level assignment to {name}")


def _eval_planner_group() -> str:
    """Evaluate the planner's ``CONSUMER_GROUP`` expression against the real
    ``group``/``topic``.

    ``src.agents.planner.kafka_consumer`` cannot be imported on this
    interpreter -- it pulls opentelemetry's instrumentation package, which
    imports ``pkg_resources`` and dies. Rather than skip the busiest consumer in
    the service, evaluate the one assignment we care about with the genuine
    naming functions bound. A bare string literal (what this line was before)
    evaluates to itself and fails these assertions; a name we did not bind
    raises NameError. Neither can pass by accident.
    """
    expr = _module_constant_expr(PLANNER, "CONSUMER_GROUP")
    return eval(  # noqa: S307 - fixed expression from our own tracked source
        compile(ast.Expression(expr), str(PLANNER), "eval"),
        {"group": group, "topic": topic},
    )


# ---------------------------------------------------------------------------
# The contract: group() is Topic()'s rule, in both languages
# ---------------------------------------------------------------------------


def test_group_matches_cross_language_contract(monkeypatch):
    """The same fixture ``kafkaclient.Group`` is pinned to in
    ``shared/go/kafkaclient/groups_test.go``. Go creates the topics and runs the
    sink; Python names the planner's group. If these two tables ever disagree,
    the ACL an operator writes from one of them silently fails the other."""
    for case in _contract_cases():
        if case["prefix"] is None:
            monkeypatch.delenv(ENV_TOPIC_PREFIX, raising=False)
        else:
            monkeypatch.setenv(ENV_TOPIC_PREFIX, case["prefix"])
        got = group(case["input"])
        assert got == case["want"], (
            f"prefix={case['prefix']!r} group({case['input']!r}) = {got!r}, "
            f"want {case['want']!r}"
        )


@pytest.mark.parametrize("prefix", ["rsync.", "", "acme", "rs ync/co:rp", "///"])
def test_group_and_topic_apply_the_same_prefix(monkeypatch, prefix):
    """Groups deliberately share KAFKA_TOPIC_PREFIX rather than owning a second
    variable: topics and groups are granted together in one ACL set, and two
    prefixes that could drift would mean granting ``rsync.*`` on topics and
    something else on groups -- a join failure naming neither variable."""
    monkeypatch.setenv(ENV_TOPIC_PREFIX, prefix)
    for name in ("planner-service", "llm-service-pii-scanner", "cdc-sink-abc12345", "", "   "):
        assert group(name) == topic(name), f"group/topic drift at prefix={prefix!r} on {name!r}"


def test_group_qualification_is_idempotent():
    """Renaming a group is not free: a consumer that rejoins as
    ``rsync.rsync.planner-service`` is a DIFFERENT group, so it drops its
    committed offsets and re-reads from auto_offset_reset."""
    once = group("planner-service")
    assert group(once) == once
    assert group(group(once)) == once
    assert once.count(DEFAULT_TOPIC_PREFIX) == 1


def test_empty_prefix_leaves_group_ids_untouched(monkeypatch):
    """The migration lever, and it has to cover groups too. A deployment with
    live topics AND committed group offsets under bare names sets this empty to
    take the code first and rename deliberately, in one coordinated deploy."""
    monkeypatch.setenv(ENV_TOPIC_PREFIX, "")
    for name in ("planner-service", "llm-service-pii-scanner", "avro-consumer-agent.x"):
        assert group(name) == name


def test_illegal_prefix_characters_are_dropped_from_group_ids(monkeypatch):
    monkeypatch.setenv(ENV_TOPIC_PREFIX, "rs ync/co:rp")
    got = group("planner-service")
    assert not any(c in got for c in " /:"), f"{got!r} carries characters Kafka rejects"
    assert got == "rsynccorp.planner-service"


# ---------------------------------------------------------------------------
# The deployment: what the broker actually sees
# ---------------------------------------------------------------------------


def test_every_python_consumer_group_is_namespaced():
    """The concrete names, so this is a claim about the broker and not about the
    code. These are what ``kafka-consumer-groups --list`` prints and what an
    operator's PREFIXED ``rsync.`` ACL has to cover."""
    pii = _load(PII_SCANNER, "pii_scanner_kafka_consumer_under_test")

    assert _eval_planner_group() == "rsync.planner-service"
    assert pii.CONSUMER_GROUP == "rsync.llm-service-pii-scanner"
    assert _consumer_group_id(topics("agent.planner.requests"), None) == (
        "rsync.avro-consumer-rsync.agent.planner.requests"
    )


def test_planner_group_follows_the_prefix_lever(monkeypatch):
    """The planner resolves its group at import, so this evaluates the
    expression itself under each setting -- proving the constant is computed
    from the environment rather than frozen into a literal."""
    monkeypatch.setenv(ENV_TOPIC_PREFIX, "")
    assert _eval_planner_group() == "planner-service"

    monkeypatch.setenv(ENV_TOPIC_PREFIX, "acme")
    assert _eval_planner_group() == "acme.planner-service"


def test_pii_scanner_group_follows_the_prefix_lever(monkeypatch):
    monkeypatch.setenv(ENV_TOPIC_PREFIX, "acme-")
    pii = _load(PII_SCANNER, "pii_scanner_kafka_consumer_prefixed")
    assert pii.CONSUMER_GROUP == "acme-llm-service-pii-scanner"
    assert pii.REQUEST_TOPIC == "acme-pii.scan.request"


def test_group_and_topics_of_one_consumer_share_a_prefix(monkeypatch):
    """The reason both are resolved at import in the same module: a group
    computed under one environment and a topic under another is the drift the
    single-variable design exists to make unrepresentable."""
    monkeypatch.setenv(ENV_TOPIC_PREFIX, "acme.")
    pii = _load(PII_SCANNER, "pii_scanner_kafka_consumer_shared_prefix")
    assert pii.CONSUMER_GROUP.startswith("acme.")
    assert pii.REQUEST_TOPIC.startswith("acme.")
    assert pii.RESPONSE_TOPIC.startswith("acme.")


# ---------------------------------------------------------------------------
# The avro consumer's derived + caller-supplied ids
# ---------------------------------------------------------------------------


def test_caller_supplied_group_id_is_qualified_too():
    """The documented decision. An unqualified escape hatch would leave exactly
    one group outside the operator's single PREFIXED grant, and that group fails
    in the silent way. Setting KAFKA_TOPIC_PREFIX empty remains the lever, and
    idempotence means a caller who spells the namespace gets it back verbatim."""
    assert _consumer_group_id(topics("agent.planner.requests"), "planner-service") == (
        "rsync.planner-service"
    )
    assert _consumer_group_id(topics("agent.planner.requests"), "rsync.planner-service") == (
        "rsync.planner-service"
    )


def test_caller_supplied_group_id_honours_the_prefix_lever(monkeypatch):
    monkeypatch.setenv(ENV_TOPIC_PREFIX, "")
    assert _consumer_group_id(["agent.planner.requests"], "planner-service") == "planner-service"
    assert _consumer_group_id(["agent.planner.requests"], None) == (
        "avro-consumer-agent.planner.requests"
    )


def test_derived_group_id_is_identical_for_bare_and_qualified_topics():
    """Why the fallback is derived AFTER qualification even though it reads
    worse: ``AvroKafkaConsumer(["x"])`` and ``AvroKafkaConsumer([topic("x")])``
    are the same subscription spelled two ways. If they derived different group
    ids, both would receive every record instead of sharing the partitions."""
    bare = _consumer_group_id(topics("agent.planner.requests"), None)
    already = _consumer_group_id(topics(topic("agent.planner.requests")), None)
    assert bare == already


def test_derived_group_id_is_idempotent_across_restarts():
    """Group ids are recomputed on every process start; a name that grew a
    prefix each time would abandon its offsets on every restart."""
    once = _consumer_group_id(topics("agent.planner.requests"), None)
    twice = _consumer_group_id(topics("agent.planner.requests"), once)
    assert once == twice
    assert once.startswith(DEFAULT_TOPIC_PREFIX)


# ---------------------------------------------------------------------------
# The helper is wired in, not merely present
# ---------------------------------------------------------------------------


def test_avro_consumer_hands_the_namespaced_group_to_the_client():
    """Every other avro test calls ``_consumer_group_id`` directly, so all of
    them stay green if line 318 stops calling it and assigns a bare literal --
    a mutation that was demonstrated to survive. What reaches the broker is the
    ``group_id`` kwarg of ``KafkaConsumer``, so that is what is asserted, from
    a real construction of ``AvroKafkaConsumer``.

    The expected string is spelled out rather than recomputed from the helper:
    deriving it from the function under test would make this pass under the
    same mutation everything else passed under."""
    from unittest import mock

    import src.utils.avro_kafka as av

    with mock.patch.object(av, "KafkaConsumer") as KC, mock.patch.object(
        av, "get_serializer"
    ):
        c = av.AvroKafkaConsumer(topics=["agent.planner.requests"])

    assert c.group_id == "rsync.avro-consumer-rsync.agent.planner.requests"
    assert KC.call_args.kwargs["group_id"] == c.group_id, (
        "the group id the client actually joins with must be the namespaced one; "
        f"got {KC.call_args.kwargs['group_id']!r}"
    )
    assert KC.call_args.args == ("rsync.agent.planner.requests",), (
        "the subscription must be qualified alongside the group -- one PREFIXED "
        "ACL has to cover both"
    )


def test_avro_consumer_group_and_subscription_follow_the_prefix_lever(monkeypatch):
    """KAFKA_TOPIC_PREFIX="" has to disable BOTH halves together. If only the
    topic followed it, the group would keep a namespace nothing else has."""
    from unittest import mock

    import src.utils.avro_kafka as av

    monkeypatch.setenv(ENV_TOPIC_PREFIX, "")
    with mock.patch.object(av, "KafkaConsumer") as KC, mock.patch.object(
        av, "get_serializer"
    ):
        av.AvroKafkaConsumer(topics=["agent.planner.requests"], group_id="planner-service")

    assert KC.call_args.kwargs["group_id"] == "planner-service"
    assert KC.call_args.args == ("agent.planner.requests",)


# ---------------------------------------------------------------------------
# Regression guard for the next call site
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", [PLANNER, PII_SCANNER, AVRO], ids=lambda p: p.name)
def test_no_bare_consumer_group_literals_remain(path):
    """The failure mode is a NEW call site, not these three. A hard-coded group
    id does not break anything until a customer's ACLs are in front of it, at
    which point it stalls without an error -- so it has to be caught here."""
    offences = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            for t in node.targets:
                if isinstance(t, ast.Name) and "GROUP" in t.id.upper():
                    offences.append(f"{path.name}:{node.lineno} {t.id} = {node.value.value!r}")
                # ``self.group_id = "..."`` is an ast.Attribute, not an ast.Name.
                # Walking only Names is how the mutation that replaced
                # avro_kafka.py's assignment with a literal survived this guard.
                if isinstance(t, ast.Attribute) and "GROUP" in t.attr.upper():
                    offences.append(
                        f"{path.name}:{node.lineno} .{t.attr} = {node.value.value!r}"
                    )
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "group_id" and isinstance(kw.value, ast.Constant):
                    offences.append(f"{path.name}:{node.lineno} group_id={kw.value.value!r}")
    assert not offences, (
        "consumer group ids must go through kafka_topics.group(); found bare literals:\n  "
        + "\n  ".join(offences)
    )
