"""Broker + security wiring for the async PII scanner's Kafka client.

The consumer runs as a fire-and-forget background task started in
``src/gateway/main.py`` — nothing awaits it and nothing reports its death. When
it dialed a hard-coded ``localhost:9092`` from inside a container it failed at
startup, the failure was logged at warning level, and every async PII scan
request was accepted by the API and then silently never processed.

So the client kwargs are built by one helper that goes through the same
``src/utils/kafka_security`` helpers as the planner consumer and the Go
services, and that helper is what these tests pin.
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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
