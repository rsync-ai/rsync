#!/usr/bin/env python3
"""Tests for Debezium schema-history Kafka security.

Debezium's schema history is a SEPARATE Kafka client living inside the connector
task — it does NOT inherit the Kafka Connect worker's credentials. On a SASL
cluster an unconfigured history client lets the connector start and stream
normally, then fail on the next restart when the consumer half replays history.
That delay is the whole reason this is worth pinning down in tests.

Run: python3 test_schema_history_security.py
"""
import os

import connector

_KAFKA_ENV = (
    "KAFKA_SECURITY_PROTOCOL",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
    "KAFKA_SSL_CA_LOCATION",
    "KAFKA_SSL_KEYSTORE_LOCATION",
    "KAFKA_SSL_SKIP_VERIFY",
    "KAFKA_BROKERS",
    "KAFKA_BOOTSTRAP_SERVERS",
)


def _clear_env():
    for k in _KAFKA_ENV:
        os.environ.pop(k, None)


def _sasl_env():
    _clear_env()
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_SSL"
    os.environ["KAFKA_SASL_USERNAME"] = "rsync"
    os.environ["KAFKA_SASL_PASSWORD"] = "s3cret"


def test_plaintext_adds_nothing():
    """An existing deployment's connector config must be byte-identical."""
    _clear_env()
    assert connector._schema_history_security() == {}


def test_sasl_configures_both_producer_and_consumer():
    """The consumer half is what replays history on restart. Configuring only
    the producer yields a connector that works until the first restart."""
    _sasl_env()
    props = connector._schema_history_security()
    for role in ("producer", "consumer"):
        p = f"schema.history.internal.{role}."
        assert props[p + "security.protocol"] == "SASL_SSL", role
        assert props[p + "sasl.mechanism"] == "PLAIN", role
        assert props[p + "sasl.jaas.config"].startswith(
            "org.apache.kafka.common.security.plain.PlainLoginModule required"
        ), role
    _clear_env()


def test_jaas_values_are_quoted():
    """An unquoted JAAS value truncates at the first space or '=' and the broker
    rejects the credential — indistinguishable from a wrong password."""
    _clear_env()
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_PLAINTEXT"
    os.environ["KAFKA_SASL_USERNAME"] = "user name"
    os.environ["KAFKA_SASL_PASSWORD"] = "pa ss=word"
    jaas = connector._schema_history_security()[
        "schema.history.internal.producer.sasl.jaas.config"
    ]
    assert 'username="user name"' in jaas, jaas
    assert 'password="pa ss=word"' in jaas, jaas
    assert jaas.endswith(";"), jaas
    _clear_env()


def test_jaas_escapes_embedded_quotes():
    _clear_env()
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_PLAINTEXT"
    os.environ["KAFKA_SASL_USERNAME"] = "u"
    os.environ["KAFKA_SASL_PASSWORD"] = 'pa"ss'
    jaas = connector._schema_history_security()[
        "schema.history.internal.producer.sasl.jaas.config"
    ]
    assert 'password="pa\\"ss"' in jaas, jaas
    _clear_env()


def test_missing_credentials_fail_closed():
    """Falling back to an anonymous history client would surface as a broker
    outage on restart rather than as the misconfiguration it is."""
    _clear_env()
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_SSL"
    try:
        connector._schema_history_security()
    except ValueError as e:
        assert "KAFKA_SASL_PASSWORD" in str(e), e
    else:
        raise AssertionError("SASL_SSL without credentials must raise")
    _clear_env()


def test_unsupported_protocol_is_rejected_by_name():
    _clear_env()
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_TLS"  # not a real Kafka protocol
    try:
        connector._schema_history_security()
    except ValueError as e:
        assert "SASL_TLS" in str(e), e
    else:
        raise AssertionError("an unknown protocol must raise, not be passed through")
    _clear_env()


def test_relational_config_carries_the_security_properties():
    _sasl_env()
    c = connector.DebeziumConnector()
    _, cfg, _ = c._build_config(
        {
            "database_type": "postgresql",
            "connector_name": "cdc-abc12345",
            "db_host": "db.example.com",
            "db_user": "svc",
            "db_password": "pw",
            "db_name": "app",
            "tables": ["public.users"],
        }
    )
    assert cfg["schema.history.internal.consumer.security.protocol"] == "SASL_SSL"
    _clear_env()


def test_mongodb_drops_every_schema_history_key():
    """MongoDB has no relational schema history; ANY schema.history.* key left on
    the config fails Connect's validation. The old fixed 3-key pop would have
    stranded the new security properties there."""
    _sasl_env()
    c = connector.DebeziumConnector()
    _, cfg, _ = c._build_config(
        {
            "database_type": "mongodb",
            "connector_name": "cdc-mongo123",
            "db_host": "mongo",
            "db_user": "u",
            "db_password": "p",
            "db_name": "app",
            "tables": ["users"],
        }
    )
    leftovers = [k for k in cfg if k.startswith("schema.history.")]
    assert not leftovers, f"MongoDB config must carry no schema.history keys: {leftovers}"
    _clear_env()


def test_kafka_brokers_wins_over_bootstrap_servers():
    """KAFKA_BROKERS is the documented variable and what every other client
    reads; honoring only KAFKA_BOOTSTRAP_SERVERS pointed schema history at a
    different cluster than the data."""
    _clear_env()
    os.environ["KAFKA_BROKERS"] = "b1:9093,b2:9093"
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "wrong-cluster:29092"
    assert connector.DebeziumConnector().kafka_bootstrap_servers == "b1:9093,b2:9093"
    _clear_env()


def test_bootstrap_servers_still_honored_when_brokers_unset():
    """Backward compatibility for deployments that only set the old variable."""
    _clear_env()
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "legacy:29092"
    assert connector.DebeziumConnector().kafka_bootstrap_servers == "legacy:29092"
    _clear_env()


def test_jaas_config_is_externalized_not_inlined():
    """The JAAS string carries the Kafka password. externalize_secrets must move
    it out of the config that gets POSTed to Connect, or it lands in the Kafka
    config topic and in GET /connectors/<name>/config."""
    import tempfile

    _sasl_env()
    props = connector._schema_history_security()
    key = "schema.history.internal.producer.sasl.jaas.config"
    with tempfile.TemporaryDirectory() as d:
        os.environ["DEBEZIUM_SECRETS_DIR"] = d
        try:
            out = connector.externalize_secrets("cdc-abc12345", dict(props))
        finally:
            os.environ.pop("DEBEZIUM_SECRETS_DIR", None)
    assert out[key].startswith("${file:"), out[key]
    assert "s3cret" not in out[key], out[key]
    _clear_env()


def test_jaas_config_is_redacted_in_responses():
    _sasl_env()
    props = connector._schema_history_security()
    redacted = connector._redact_config(props)
    assert "s3cret" not in repr(redacted), redacted
    _clear_env()


# --------------------------------------------------------------------------
# TLS material. A JVM reads PEM directly since KIP-651 (Kafka 2.7) -- but only
# when told the store TYPE. These four exist because this file previously
# emitted ssl.truststore.location with no type, on the theory that Connect
# needed a JKS. It does not, and the omission is not inert: the JVM assumes
# JKS, fails to parse the PEM, and reports a keystore FORMAT error, which reads
# as a corrupt file rather than a missing setting.
# --------------------------------------------------------------------------


def test_a_pem_ca_is_declared_as_pem():
    _sasl_env()
    os.environ["KAFKA_SSL_CA_LOCATION"] = "/etc/rsync-ai/kafka-tls/ca.crt"
    props = connector._schema_history_security()
    for role in ("producer", "consumer"):
        p = f"schema.history.internal.{role}."
        assert props[p + "ssl.truststore.location"] == "/etc/rsync-ai/kafka-tls/ca.crt", role
        assert props[p + "ssl.truststore.type"] == "PEM", (
            f"{role}: a truststore location with no type makes the JVM assume JKS "
            "and fail with a keystore format error naming no setting"
        )
    _clear_env()


def test_mtls_keystore_is_the_single_pem_file():
    """A JVM PEM keystore is ONE file holding the chain and the key, so the two
    paths the Go and Python clients read (cert + key) cannot be used here."""
    _sasl_env()
    os.environ["KAFKA_SSL_CA_LOCATION"] = "/etc/rsync-ai/kafka-tls/ca.crt"
    os.environ["KAFKA_SSL_KEYSTORE_LOCATION"] = "/etc/rsync-ai/kafka-tls/client.pem"
    props = connector._schema_history_security()
    for role in ("producer", "consumer"):
        p = f"schema.history.internal.{role}."
        assert props[p + "ssl.keystore.location"] == "/etc/rsync-ai/kafka-tls/client.pem", role
        assert props[p + "ssl.keystore.type"] == "PEM", role
    _clear_env()


def test_skip_verify_drops_the_hostname_check_only():
    """As close as a JVM gets to skipping verification. The chain is still
    validated against the truststore, so the truststore must survive."""
    _sasl_env()
    os.environ["KAFKA_SSL_CA_LOCATION"] = "/etc/rsync-ai/kafka-tls/ca.crt"
    os.environ["KAFKA_SSL_SKIP_VERIFY"] = "true"
    props = connector._schema_history_security()
    for role in ("producer", "consumer"):
        p = f"schema.history.internal.{role}."
        assert props[p + "ssl.endpoint.identification.algorithm"] == "", role
        assert props[p + "ssl.truststore.location"], (
            f"{role}: dropping the truststore turns a hostname override into a "
            "handshake failure"
        )
    _clear_env()


def test_no_tls_properties_on_a_plaintext_listener():
    """SASL_PLAINTEXT has no certificate to verify. Emitting ssl.* there is not
    inert -- it is what makes a working plaintext deployment fail on upgrade."""
    _clear_env()
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_PLAINTEXT"
    os.environ["KAFKA_SASL_USERNAME"] = "u"
    os.environ["KAFKA_SASL_PASSWORD"] = "p"
    os.environ["KAFKA_SSL_CA_LOCATION"] = "/etc/rsync-ai/kafka-tls/ca.crt"
    os.environ["KAFKA_SSL_KEYSTORE_LOCATION"] = "/etc/rsync-ai/kafka-tls/client.pem"
    os.environ["KAFKA_SSL_SKIP_VERIFY"] = "true"
    props = connector._schema_history_security()
    assert not [k for k in props if ".ssl." in k], props
    _clear_env()


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"ERROR {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    raise SystemExit(1 if failed else 0)
