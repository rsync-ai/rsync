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
    "the broker sees rsync.llm-service-pii-scanner" is a claim about the
    deployment.
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

from src.utils.kafka_topics import (  # noqa: E402
    DEFAULT_TOPIC_PREFIX,
    ENV_TOPIC_PREFIX,
    group,
    topic,
)

CONTRACT = REPO / "shared" / "contracts" / "kafka-topic-naming.json"
PII_SCANNER = LLM_SERVICE / "src" / "agents" / "pii_scanner" / "kafka_consumer.py"


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


# ---------------------------------------------------------------------------
# The contract: group() is Topic()'s rule, in both languages
# ---------------------------------------------------------------------------


def test_group_matches_cross_language_contract(monkeypatch):
    """The same fixture ``kafkaclient.Group`` is pinned to in
    ``shared/go/kafkaclient/groups_test.go``. Go creates the topics and runs the
    sink; Python names its own consumers' groups. If these two tables ever disagree,
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
    for name in ("example-consumer", "llm-service-pii-scanner", "cdc-sink-abc12345", "", "   "):
        assert group(name) == topic(name), f"group/topic drift at prefix={prefix!r} on {name!r}"


def test_group_qualification_is_idempotent():
    """Renaming a group is not free: a consumer that rejoins as
    ``rsync.rsync.example-consumer`` is a DIFFERENT group, so it drops its
    committed offsets and re-reads from auto_offset_reset."""
    once = group("example-consumer")
    assert group(once) == once
    assert group(group(once)) == once
    assert once.count(DEFAULT_TOPIC_PREFIX) == 1


def test_empty_prefix_leaves_group_ids_untouched(monkeypatch):
    """The migration lever, and it has to cover groups too. A deployment with
    live topics AND committed group offsets under bare names sets this empty to
    take the code first and rename deliberately, in one coordinated deploy."""
    monkeypatch.setenv(ENV_TOPIC_PREFIX, "")
    for name in ("example-consumer", "llm-service-pii-scanner", "cdc-sink-abc12345"):
        assert group(name) == name


def test_illegal_prefix_characters_are_dropped_from_group_ids(monkeypatch):
    monkeypatch.setenv(ENV_TOPIC_PREFIX, "rs ync/co:rp")
    got = group("example-consumer")
    assert not any(c in got for c in " /:"), f"{got!r} carries characters Kafka rejects"
    assert got == "rsynccorp.example-consumer"


# ---------------------------------------------------------------------------
# The deployment: what the broker actually sees
# ---------------------------------------------------------------------------


def test_every_python_consumer_group_is_namespaced():
    """The concrete names, so this is a claim about the broker and not about the
    code. These are what ``kafka-consumer-groups --list`` prints and what an
    operator's PREFIXED ``rsync.`` ACL has to cover."""
    pii = _load(PII_SCANNER, "pii_scanner_kafka_consumer_under_test")

    assert pii.CONSUMER_GROUP == "rsync.llm-service-pii-scanner"


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
# Regression guard for the next call site
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", [PII_SCANNER], ids=lambda p: p.name)
def test_no_bare_consumer_group_literals_remain(path):
    """The failure mode is a NEW call site, not these. A hard-coded group
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
                # a group-id assignment with a literal survived this guard.
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
