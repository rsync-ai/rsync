"""Tests for src/utils/kafka_topics.py.

The same naming rule is implemented three times -- here, in Go
(shared/go/kafkaclient/topics.go) and in the Debezium connector
(shared/mcp-connectors/internal/debezium/versions/v1.0.0/connector.py) -- because
the three runtimes cannot share a library. Drift between them is silent at
runtime: a producer that qualifies and a consumer that does not simply stop
seeing each other's records, with no error on either side. So all three are
pinned to the same fixture, shared/contracts/kafka-topic-naming.json.
"""

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.kafka_topics import (  # noqa: E402
    DEFAULT_TOPIC_PREFIX,
    ENV_TOPIC_PREFIX,
    in_namespace,
    topic,
    topic_prefix,
    topics,
)

CONTRACT = (
    Path(__file__).resolve().parents[2] / "shared" / "contracts" / "kafka-topic-naming.json"
)


@pytest.fixture(autouse=True)
def _clear_prefix_env(monkeypatch):
    """Every test starts from an unconfigured environment."""
    monkeypatch.delenv(ENV_TOPIC_PREFIX, raising=False)


def _contract_cases():
    assert CONTRACT.is_file(), f"shared contract not found at {CONTRACT}"
    cases = json.loads(CONTRACT.read_text())["cases"]
    assert cases, "the shared contract has no cases; this would pass vacuously"
    return cases


def test_matches_cross_language_contract(monkeypatch):
    for case in _contract_cases():
        if case["prefix"] is None:
            monkeypatch.delenv(ENV_TOPIC_PREFIX, raising=False)
        else:
            monkeypatch.setenv(ENV_TOPIC_PREFIX, case["prefix"])
        got = topic(case["input"])
        assert got == case["want"], (
            f"{case.get('name', '')}: prefix={case['prefix']!r} "
            f"topic({case['input']!r}) = {got!r}, want {case['want']!r}"
        )


def test_unset_env_yields_rsync_prefix():
    """The reason the prefix exists: a customer listing topics on their own
    cluster must be able to tell at a glance which ones this product created.
    That has to hold with nothing configured, which is how most deploys run."""
    assert topic_prefix() == DEFAULT_TOPIC_PREFIX
    assert DEFAULT_TOPIC_PREFIX.startswith("rsync")
    assert topic("agent.chat.requests").startswith("rsync")


def test_qualification_is_idempotent():
    """Topic names are persisted (pipelines.kafka_topic, sink subscription
    config) and read back on the next run, so an already-qualified name passes
    through this function again. Doubling would point the reader at a topic
    nobody writes."""
    once = topic("cdc.abc12345")
    assert topic(once) == once
    assert topic(topic(once)) == once


def test_producer_and_consumer_resolve_identically():
    """The failure this whole file guards: producer and consumer computing
    different names is not an error, it is silence."""
    for name in ("agent.chat.requests", "pipeline.domain.events", "cdc.abc12345"):
        assert topic(name) == topic(name)
        assert topics(name) == [topic(name)]


def test_empty_prefix_leaves_names_untouched(monkeypatch):
    """The migration lever: an existing deployment with live topics and
    committed consumer-group offsets sets this empty to keep the old names."""
    monkeypatch.setenv(ENV_TOPIC_PREFIX, "")
    assert topic_prefix() == ""
    assert topic("agent.chat.requests") == "agent.chat.requests"


def test_prefix_without_separator_gains_one(monkeypatch):
    """"rsync" + "agent.x" = "rsyncagent.x" is a LEGAL topic name, so this
    mistake would not fail -- it would just create a differently-named topic."""
    monkeypatch.setenv(ENV_TOPIC_PREFIX, "acme")
    assert topic("agent.chat.requests") == "acme.agent.chat.requests"


@pytest.mark.parametrize("prefix", ["acme.", "acme-", "acme_"])
def test_existing_separator_is_not_doubled(monkeypatch, prefix):
    monkeypatch.setenv(ENV_TOPIC_PREFIX, prefix)
    assert topic("agent.chat.requests") == prefix + "agent.chat.requests"


def test_illegal_prefix_characters_are_dropped(monkeypatch):
    """Kafka topics are [a-zA-Z0-9._-]. An operator typo would otherwise make
    every derived topic illegal, and the broker error names the derived topic,
    not the prefix -- so the cause would be hidden."""
    monkeypatch.setenv(ENV_TOPIC_PREFIX, "rs ync/co:rp")
    assert topic_prefix() == "rsynccorp."
    assert topic("agent.chat.requests") == "rsynccorp.agent.chat.requests"


def test_prefix_of_only_illegal_characters_disables_qualification(monkeypatch):
    monkeypatch.setenv(ENV_TOPIC_PREFIX, "///")
    assert topic_prefix() == ""
    assert topic("agent.chat.requests") == "agent.chat.requests"


def test_empty_name_stays_empty():
    assert topic("") == ""
    assert topic("   ") == ""


def test_in_namespace_matches_prefixed_and_legacy_names():
    """Guards that classify a topic ("is this a batch topic?", "does this
    pipeline own it?") must keep matching topics created before the prefix
    existed, or a legacy topic gets misclassified and orphaned."""
    assert in_namespace("rsync.pipeline.abc12345.data", "pipeline.")
    assert in_namespace("pipeline.abc12345.data", "pipeline.")
    assert not in_namespace("rsync.cdc.abc12345", "pipeline.")
    assert not in_namespace("", "pipeline.")
    assert not in_namespace("rsync.pipeline.abc12345.data", "")


def test_in_namespace_works_with_qualification_disabled(monkeypatch):
    monkeypatch.setenv(ENV_TOPIC_PREFIX, "")
    assert in_namespace("pipeline.abc12345.data", "pipeline.")
    assert not in_namespace("cdc.abc12345", "pipeline.")
