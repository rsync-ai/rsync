"""Broker + security wiring for the async PII scanner's Kafka client.

The consumer runs as a fire-and-forget background task started in
``src/gateway/main.py`` — nothing awaits it and nothing reports its death. When
it dialed a hard-coded ``localhost:9092`` from inside a container it failed at
startup, the failure was logged at warning level, and every async PII scan
request was accepted by the API and then silently never processed.

So the client kwargs are built by one helper that goes through the same
``src/utils/kafka_security`` helpers as the Go services, and that helper is
what these tests pin.

The consumer also must never create the topic it reads. kafka-python's
KafkaConsumer defaults allow_auto_create_topics to True, so a consumer that
started before kafka-init created ``pii.scan.request`` at the broker's default
of 1 partition. The last tests pin the flag and the wait that replaces it.
"""

import asyncio
import logging
import os
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import src.agents.pii_scanner.kafka_consumer as pii_consumer  # noqa: E402
import src.agents.pii_scanner.service as pii_service  # noqa: E402
from src.agents.pii_scanner.kafka_consumer import (  # noqa: E402
    DEFAULT_KAFKA_BROKERS,
    _kafka_client_kwargs,
)
from src.utils.kafka_security import KafkaSecurityError  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_kafka_env(monkeypatch):
    for key in list(os.environ):
        if key.startswith("KAFKA_"):
            monkeypatch.delenv(key, raising=False)


def test_default_broker_is_the_in_cluster_one_not_localhost():
    """localhost:9092 inside a container is nothing at all; the consumer dies at
    startup and the PII scan queue drains nowhere."""
    assert DEFAULT_KAFKA_BROKERS == "kafka:29092"
    assert _kafka_client_kwargs()["bootstrap_servers"] == ["kafka:29092"]


def test_kafka_brokers_is_honoured():
    os.environ["KAFKA_BROKERS"] = "b1:9093"
    assert _kafka_client_kwargs()["bootstrap_servers"] == ["b1:9093"]


def test_bootstrap_servers_is_honoured_too(monkeypatch):
    """Half the compose files set the other name. Reading only KAFKA_BROKERS
    sent this client to the default broker while the rest of the service used
    the configured one."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "configured:9092")
    assert _kafka_client_kwargs()["bootstrap_servers"] == ["configured:9092"]


def test_a_multi_broker_csv_stays_multiple_brokers(monkeypatch):
    """kafka-python wants a list. An unsplit CSV is one unresolvable hostname,
    and a healthy 3-broker cluster then reads as an outage."""
    monkeypatch.setenv("KAFKA_BROKERS", "b1:9093, b2:9093 ,b3:9093")
    assert _kafka_client_kwargs()["bootstrap_servers"] == [
        "b1:9093",
        "b2:9093",
        "b3:9093",
    ]


def test_sasl_credentials_reach_the_client(monkeypatch):
    """Without these the client cannot connect to any secured cluster at all."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "rsync")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "s3cret")

    kwargs = _kafka_client_kwargs()
    assert kwargs["security_protocol"] == "SASL_SSL"
    assert kwargs["sasl_mechanism"] == "SCRAM-SHA-512"
    assert kwargs["sasl_plain_username"] == "rsync"
    assert kwargs["sasl_plain_password"] == "s3cret"


def test_plaintext_deployment_is_unchanged():
    """The kwargs an unsecured deployment gets are kafka-python's own defaults."""
    assert _kafka_client_kwargs() == {
        "bootstrap_servers": ["kafka:29092"],
        "security_protocol": "PLAINTEXT",
    }


def test_a_broken_security_profile_raises_rather_than_dialing_plaintext(monkeypatch):
    """Fail-closed: silently downgrading produces a connection error naming the
    broker, which costs an on-call cycle to tell apart from a real outage."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "rsync")  # password missing
    with pytest.raises(KafkaSecurityError):
        _kafka_client_kwargs()


# --- the consumer never creates its own topic ------------------------------


class _FakeProducer:
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs


def _fake_kafka(topic_answers, events):
    """A stand-in `kafka` module. KafkaConsumer.topics() replays topic_answers
    in order (a set, or an exception to raise); every call the code under test
    makes on the consumer is appended to `events`."""
    answers = list(topic_answers)

    class _FakeConsumer:
        instances = []

        def __init__(self, *topics, **kwargs):
            self.init_topics = topics
            self.kwargs = kwargs
            _FakeConsumer.instances.append(self)

        def topics(self):
            events.append("topics")
            answer = answers.pop(0)
            if isinstance(answer, BaseException):
                raise answer
            return answer

        def subscribe(self, topics):
            events.append(("subscribe", list(topics)))

        def __iter__(self):
            events.append("consume")
            return iter(())

    mod = types.ModuleType("kafka")
    mod.KafkaConsumer = _FakeConsumer
    mod.KafkaProducer = _FakeProducer
    return mod, _FakeConsumer


@pytest.fixture
def _no_real_sleep(monkeypatch):
    slept = []

    async def _sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(pii_consumer.asyncio, "sleep", _sleep)
    return slept


@pytest.fixture
def _no_real_service(monkeypatch):
    # The consumer builds a PIIScannerService before it connects; the real one
    # loads detectors this test has no use for.
    monkeypatch.setattr(pii_service, "PIIScannerService", lambda: object())


def _run(monkeypatch, topic_answers):
    events = []
    fake, consumer_cls = _fake_kafka(topic_answers, events)
    monkeypatch.setitem(sys.modules, "kafka", fake)
    asyncio.run(pii_consumer.run_pii_kafka_consumer())
    assert len(consumer_cls.instances) == 1, "the consumer should connect exactly once"
    return consumer_cls.instances[0], events


def test_consumer_is_built_with_auto_create_off(monkeypatch, _no_real_sleep, _no_real_service):
    """kafka-python defaults this to True, and the consumer's metadata request
    then creates the topic with the broker's defaults."""
    consumer, _ = _run(monkeypatch, [{pii_consumer.REQUEST_TOPIC}])
    assert consumer.kwargs.get("allow_auto_create_topics") is False, consumer.kwargs
    # Not subscribed at construction: the subscription waits for the topic.
    assert consumer.init_topics == ()


def test_a_missing_topic_is_waited_for_not_created(
    monkeypatch, caplog, _no_real_sleep, _no_real_service
):
    """Missing, then a failed listing, then a different topic only, then there.
    Each wait is logged, a failed listing does not end the task, and nothing
    subscribes or consumes until the topic is listed."""
    caplog.set_level(logging.WARNING, logger=pii_consumer.logger.name)
    consumer, events = _run(
        monkeypatch,
        [set(), RuntimeError("broker restarting"), {"rsync.other"}, {pii_consumer.REQUEST_TOPIC}],
    )
    assert events == [
        "topics",
        "topics",
        "topics",
        "topics",
        ("subscribe", [pii_consumer.REQUEST_TOPIC]),
        "consume",
    ], events
    assert _no_real_sleep == [5.0, 10.0, 20.0]
    waits = [r for r in caplog.records if "waiting for topic" in r.getMessage()]
    assert len(waits) == 3, [r.getMessage() for r in caplog.records]
    assert "broker restarting" in waits[1].getMessage()


def test_the_wait_backs_off_to_a_cap_and_keeps_checking(
    monkeypatch, _no_real_sleep, _no_real_service
):
    """Bounded per wait, not in count: a consumer that gave up would be dead
    for the life of the process while the API keeps accepting scans."""
    misses = 9
    _, events = _run(monkeypatch, [set()] * misses + [{pii_consumer.REQUEST_TOPIC}])
    assert events.count("topics") == misses + 1
    assert max(_no_real_sleep) == pii_consumer.TOPIC_WAIT_MAX_SECONDS
    assert _no_real_sleep == [5.0, 10.0, 20.0, 40.0, 60.0, 60.0, 60.0, 60.0, 60.0]
