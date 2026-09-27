#!/usr/bin/env python3
"""Tests for the Kafka signal-channel consumer's security settings.

Debezium reads Reload, re-snapshot and added-table backfill signals with its own
Kafka consumer, which does NOT inherit the Kafka Connect worker's TLS/SASL
settings. On a TLS or SASL cluster that consumer looped on "Bootstrap broker ...
disconnected", so every signal was sent and none was read: the orchestrator
logged the snapshot signal while the topic offsets stayed flat (GKE, Managed
Kafka mTLS). The consumer takes its settings from the `signal.consumer.`
passthrough prefix.

Run: python3 -m pytest test_signal_consumer_security.py -q
"""
import os
import tempfile

import connector
from test_schema_history_security import _clear_env, _sasl_env
from test_signal_channel import TOPIC, _cfg

PREFIX = "signal.consumer."


def _mtls_env():
    _clear_env()
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "SSL"
    os.environ["KAFKA_SSL_CA_LOCATION"] = "/etc/rsync-ai/kafka-tls/ca.crt"
    os.environ["KAFKA_SSL_KEYSTORE_LOCATION"] = "/etc/rsync-ai/kafka-tls/client.pem"


def test_plaintext_adds_nothing():
    """An existing plaintext deployment's connector config stays byte-identical."""
    _clear_env()
    assert connector._signal_consumer_security() == {}
    assert not [k for k in _cfg(signal_kafka_topic=TOPIC) if k.startswith(PREFIX)]


def test_sasl_configures_the_signal_consumer():
    _sasl_env()
    props = connector._signal_consumer_security()
    assert props[PREFIX + "security.protocol"] == "SASL_SSL"
    assert props[PREFIX + "sasl.mechanism"] == "PLAIN"
    assert props[PREFIX + "sasl.jaas.config"].startswith(
        "org.apache.kafka.common.security.plain.PlainLoginModule required"
    )
    assert all(k.startswith(PREFIX) for k in props), props
    _clear_env()


def test_mtls_configures_the_signal_consumer():
    _mtls_env()
    props = connector._signal_consumer_security()
    assert props[PREFIX + "security.protocol"] == "SSL"
    assert props[PREFIX + "ssl.truststore.location"] == "/etc/rsync-ai/kafka-tls/ca.crt"
    assert props[PREFIX + "ssl.truststore.type"] == "PEM"
    assert props[PREFIX + "ssl.keystore.location"] == "/etc/rsync-ai/kafka-tls/client.pem"
    assert props[PREFIX + "ssl.keystore.type"] == "PEM"
    _clear_env()


def test_every_source_with_a_signal_topic_carries_them():
    _mtls_env()
    for db_type in ("postgresql", "mongodb"):
        cfg = _cfg(db_type=db_type, signal_kafka_topic=TOPIC)
        assert cfg["signal.kafka.topic"] == TOPIC, db_type
        assert cfg[PREFIX + "security.protocol"] == "SSL", db_type
        assert cfg[PREFIX + "ssl.keystore.location"] == "/etc/rsync-ai/kafka-tls/client.pem", db_type
    _clear_env()


def test_no_signal_topic_no_signal_consumer_keys():
    _mtls_env()
    for db_type in ("postgresql", "mongodb"):
        assert not [k for k in _cfg(db_type=db_type) if k.startswith(PREFIX)], db_type
    _clear_env()


def test_signal_jaas_is_externalized_and_redacted():
    """The JAAS string carries the Kafka password; it must leave the config that
    is POSTed to Connect and never appear in a response."""
    _sasl_env()
    props = connector._signal_consumer_security()
    key = PREFIX + "sasl.jaas.config"
    with tempfile.TemporaryDirectory() as d:
        os.environ["DEBEZIUM_SECRETS_DIR"] = d
        try:
            out = connector.externalize_secrets("cdc-abc12345", dict(props))
        finally:
            os.environ.pop("DEBEZIUM_SECRETS_DIR", None)
    assert out[key].startswith("${file:"), out[key]
    assert "s3cret" not in out[key]
    assert "s3cret" not in repr(connector._redact_config(props))
    _clear_env()
