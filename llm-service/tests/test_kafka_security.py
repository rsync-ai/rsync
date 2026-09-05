"""Tests for src/utils/kafka_security.py.

The Go module shared/go/kafkaclient has an equivalent suite. Both must keep
passing: they are the contract that a customer's single Kafka configuration
means the same thing to the Go services and to the Python agents.
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.kafka_security import (  # noqa: E402
    KafkaSecurityError,
    brokers_from_env,
    describe,
    kafka_security_kwargs,
    parse_brokers,
    schema_registry_auth,
)


@pytest.fixture(autouse=True)
def _clear_kafka_env(monkeypatch):
    """Every test starts from an unconfigured environment."""
    for key in list(os.environ):
        if key.startswith("KAFKA_") or key.startswith("SCHEMA_REGISTRY_"):
            monkeypatch.delenv(key, raising=False)


# --------------------------------------------------------------------------
# Defect 1: broker-list collapse
# --------------------------------------------------------------------------

def test_parse_brokers_does_not_collapse_a_csv():
    """The whole point: a 3-broker cluster must stay 3 addresses.

    Passing the raw string to kafka-python yields one unresolvable hostname, so
    a perfectly healthy cluster is unreachable.
    """
    got = parse_brokers("b1:9093,b2:9093,b3:9093")
    assert got == ["b1:9093", "b2:9093", "b3:9093"]


def test_parse_brokers_tolerates_operator_whitespace():
    """"b1:9093, b2:9093" is what a human types. The space must not survive
    into a hostname."""
    assert parse_brokers(" b1:9093 , b2:9093 ,, ") == ["b1:9093", "b2:9093"]


def test_parse_brokers_on_empty_input():
    assert parse_brokers("") == []
    assert parse_brokers(None) == []


def test_kafka_brokers_wins_over_bootstrap_servers(monkeypatch):
    """Both names are in use across this repo; pin the precedence so a
    deployment that sets both is not silently pointed at the wrong cluster."""
    monkeypatch.setenv("KAFKA_BROKERS", "primary:9093")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "secondary:9093")
    assert brokers_from_env() == ["primary:9093"]


def test_bootstrap_servers_used_when_brokers_unset(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "secondary:9093,third:9093")
    assert brokers_from_env() == ["secondary:9093", "third:9093"]


# --------------------------------------------------------------------------
# Defect 2: plaintext-only clients
# --------------------------------------------------------------------------

def test_unset_config_is_a_strict_no_op():
    """An existing plaintext deployment must behave identically after this
    module is adopted: kafka-python's own default is PLAINTEXT, and no TLS or
    SASL kwargs may appear."""
    kwargs = kafka_security_kwargs()
    assert kwargs == {"security_protocol": "PLAINTEXT"}


def test_sasl_ssl_scram_produces_the_full_kwarg_set(monkeypatch):
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "svc-rsync")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "s3cret")

    kwargs = kafka_security_kwargs()
    assert kwargs["security_protocol"] == "SASL_SSL"
    assert kwargs["sasl_mechanism"] == "SCRAM-SHA-512"
    assert kwargs["sasl_plain_username"] == "svc-rsync"
    assert kwargs["sasl_plain_password"] == "s3cret"


def test_managed_kafka_needs_no_ca_file(monkeypatch):
    """Confluent Cloud / Aiven present a publicly-chaining certificate. Requiring
    a CA path would make the common managed case impossible to configure."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "u")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "p")

    kwargs = kafka_security_kwargs()
    assert "ssl_cafile" not in kwargs


def test_private_ca_is_passed_through(monkeypatch):
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SSL")
    monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", "/etc/kafka/ca.pem")
    assert kafka_security_kwargs()["ssl_cafile"] == "/etc/kafka/ca.pem"


def test_sasl_protocol_defaults_the_mechanism_to_plain(monkeypatch):
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "u")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "p")
    assert kafka_security_kwargs()["sasl_mechanism"] == "PLAIN"


# --------------------------------------------------------------------------
# Fail closed
# --------------------------------------------------------------------------

def test_sasl_without_credentials_is_an_error(monkeypatch):
    """Must NOT silently downgrade to an unauthenticated connection: the
    resulting error would name the broker, not the missing password."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "svc-rsync")

    with pytest.raises(KafkaSecurityError) as exc:
        kafka_security_kwargs()
    assert "PASSWORD" in str(exc.value).upper()


def test_unsupported_protocol_is_rejected(monkeypatch):
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_TLS")  # a plausible typo
    with pytest.raises(KafkaSecurityError):
        kafka_security_kwargs()


def test_unimplemented_mechanism_says_so_by_name(monkeypatch):
    """AWS_MSK_IAM needs a credential provider, not a username/password. Saying
    'unsupported' would send an operator hunting for a typo."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "AWS_MSK_IAM")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "u")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "p")

    with pytest.raises(KafkaSecurityError) as exc:
        kafka_security_kwargs()
    assert "not implemented" in str(exc.value)


def test_half_an_mtls_keypair_is_rejected(monkeypatch):
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SSL")
    monkeypatch.setenv("KAFKA_SSL_CERT_LOCATION", "/etc/kafka/client.pem")
    with pytest.raises(KafkaSecurityError):
        kafka_security_kwargs()


# --------------------------------------------------------------------------
# Credential handling
# --------------------------------------------------------------------------

def test_describe_never_leaks_the_password(monkeypatch):
    """describe() exists so operators can log the Kafka config when a connection
    fails. If it printed the password, that need would create the leak."""
    monkeypatch.setenv("KAFKA_BROKERS", "b1:9093")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "svc-rsync")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "hunter2-do-not-log")

    out = describe()
    assert "hunter2-do-not-log" not in out
    assert "[redacted]" in out
    assert "svc-rsync" in out  # the username IS useful in a log


# --------------------------------------------------------------------------
# Debezium schema-history client
# --------------------------------------------------------------------------

def test_debezium_history_security_is_empty_on_plaintext():
    """An existing plaintext deployment's connector config must not change."""
    from src.utils.kafka_security import debezium_schema_history_security

    assert debezium_schema_history_security() == {}


def test_debezium_history_security_configures_both_producer_and_consumer(monkeypatch):
    """The consumer half matters: it is what replays history on connector
    restart. Configuring only the producer yields a connector that works until
    it is restarted, which is the worst kind of bug to diagnose."""
    from src.utils.kafka_security import debezium_schema_history_security

    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "svc-rsync")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "s3cret")

    props = debezium_schema_history_security()
    for role in ("producer", "consumer"):
        prefix = f"schema.history.internal.{role}."
        assert props[prefix + "security.protocol"] == "SASL_SSL"
        assert props[prefix + "sasl.mechanism"] == "SCRAM-SHA-512"
        assert "ScramLoginModule" in props[prefix + "sasl.jaas.config"]


def test_debezium_history_trusts_a_private_ca_as_a_PEM_keystore(monkeypatch):
    """The truststore has to be declared PEM, or the JVM assumes JKS and fails.

    This is the keystone of the whole TLS design: KIP-651 (Kafka 2.7+) lets a JVM
    read a plain PEM bundle, so ONE mounted ca.crt serves the Go clients, the
    kafka-python clients, Kafka Connect and the Kafka CLI with no conversion step.
    Emit `ssl.truststore.location` without `ssl.truststore.type=PEM` and the JVM
    tries to parse the PEM as a Java keystore and reports a keystore FORMAT error
    -- which reads as a corrupt file, sending you to look at the certificate
    rather than at the missing line.
    """
    from src.utils.kafka_security import debezium_schema_history_security

    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "svc-rsync")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "s3cret")
    monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", "/etc/rsync-ai/kafka-tls/ca.crt")

    props = debezium_schema_history_security()
    for role in ("producer", "consumer"):
        prefix = f"schema.history.internal.{role}."
        assert props[prefix + "ssl.truststore.type"] == "PEM"
        assert props[prefix + "ssl.truststore.location"] == "/etc/rsync-ai/kafka-tls/ca.crt"


def test_debezium_history_presents_the_client_keypair_as_one_PEM_file(monkeypatch):
    """A JVM keystore is a SINGLE file holding the chain and the key.

    The kafka-python clients in this process read the keypair as two paths
    (ssl_certfile + ssl_keyfile). A JVM cannot: there is no way to hand it the
    halves separately. So the chart mounts a third file -- cert and key
    concatenated -- and names it in KAFKA_SSL_KEYSTORE_LOCATION, and this is the
    only consumer of that variable.

    Getting this wrong is silent in the direction that matters: with no keystore
    the Connect worker presents no certificate, the REST API still reports the
    connector RUNNING, and every task fails its handshake underneath.
    """
    from src.utils.kafka_security import debezium_schema_history_security

    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SSL")
    monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", "/etc/rsync-ai/kafka-tls/ca.crt")
    monkeypatch.setenv("KAFKA_SSL_CERT_LOCATION", "/etc/rsync-ai/kafka-tls/tls.crt")
    monkeypatch.setenv("KAFKA_SSL_KEY_LOCATION", "/etc/rsync-ai/kafka-tls/tls.key")
    monkeypatch.setenv("KAFKA_SSL_KEYSTORE_LOCATION", "/etc/rsync-ai/kafka-tls/client.pem")

    props = debezium_schema_history_security()
    for role in ("producer", "consumer"):
        prefix = f"schema.history.internal.{role}."
        assert props[prefix + "ssl.keystore.type"] == "PEM"
        assert props[prefix + "ssl.keystore.location"] == "/etc/rsync-ai/kafka-tls/client.pem"
        # The two-path form is for kafka-python and must not leak into JVM config:
        # `ssl.keystore.location` pointing at a bare certificate loads no key.
        assert prefix + "ssl.certificate.location" not in props
        assert props[prefix + "ssl.keystore.location"].endswith("client.pem")


def test_debezium_history_omits_the_keystore_when_the_chart_did_not_mount_one(monkeypatch):
    """mTLS is optional under SASL_SSL, and a keystore path that does not exist is
    worse than none: the JVM fails at startup reading a file, not at authentication.
    """
    from src.utils.kafka_security import debezium_schema_history_security

    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "u")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "p")
    monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", "/etc/rsync-ai/kafka-tls/ca.crt")
    monkeypatch.delenv("KAFKA_SSL_KEYSTORE_LOCATION", raising=False)

    props = debezium_schema_history_security()
    assert not [k for k in props if "keystore" in k], (
        "a keystore was configured with nothing mounted to back it"
    )


def test_debezium_history_skip_verify_drops_only_the_hostname_check(monkeypatch):
    """`ssl.endpoint.identification.algorithm=""` is the ONLY thing a JVM offers here.

    It is not the Go `InsecureSkipVerify`: the chain is still validated against the
    truststore. This asserts the mapping exists, and -- as the reason the chart
    refuses skip-verify without a CA -- that the truststore is still configured
    alongside it rather than being treated as redundant.
    """
    from src.utils.kafka_security import debezium_schema_history_security

    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "u")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "p")
    monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", "/etc/rsync-ai/kafka-tls/ca.crt")
    monkeypatch.setenv("KAFKA_SSL_SKIP_VERIFY", "true")

    props = debezium_schema_history_security()
    for role in ("producer", "consumer"):
        prefix = f"schema.history.internal.{role}."
        assert props[prefix + "ssl.endpoint.identification.algorithm"] == ""
        assert props[prefix + "ssl.truststore.location"], (
            "skip-verify must not suppress the truststore -- the JVM validates the "
            "chain either way, so dropping it turns a hostname override into a "
            "handshake failure"
        )


def test_debezium_history_adds_no_tls_properties_on_a_plaintext_listener(monkeypatch):
    """SASL_PLAINTEXT has no certificate to verify. Emitting ssl.* there is not
    inert -- it is what makes a working plaintext deployment fail on upgrade."""
    from src.utils.kafka_security import debezium_schema_history_security

    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "u")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "p")
    # Deliberately set: the chart does not mount these on a plaintext listener,
    # but a leftover from a previous protocol must not resurrect them.
    monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", "/etc/rsync-ai/kafka-tls/ca.crt")
    monkeypatch.setenv("KAFKA_SSL_KEYSTORE_LOCATION", "/etc/rsync-ai/kafka-tls/client.pem")

    props = debezium_schema_history_security()
    assert props, "SASL_PLAINTEXT still needs credentials"
    assert not [k for k in props if ".ssl." in k], f"ssl.* leaked onto a plaintext listener: {props}"


def test_debezium_jaas_quotes_a_password_containing_spaces(monkeypatch):
    """An unquoted JAAS value truncates at whitespace, and the resulting
    authentication failure is indistinguishable from a wrong password."""
    from src.utils.kafka_security import debezium_schema_history_security

    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "svc rsync")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "pass word=with=equals")

    jaas = debezium_schema_history_security()[
        "schema.history.internal.producer.sasl.jaas.config"
    ]
    assert 'username="svc rsync"' in jaas
    assert 'password="pass word=with=equals"' in jaas
    assert jaas.endswith(";")


# --------------------------------------------------------------------------
# Schema Registry credentials
# --------------------------------------------------------------------------


def test_schema_registry_auth_is_none_when_unset():
    """Anonymous is correct for an unsecured registry, and is what every
    existing deployment already gets."""
    assert schema_registry_auth() is None


def test_schema_registry_auth_reads_the_confluent_combined_form(monkeypatch):
    monkeypatch.setenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", "sr-user:sr-pass")
    assert schema_registry_auth() == ("sr-user", "sr-pass")


def test_schema_registry_auth_reads_the_split_pair(monkeypatch):
    monkeypatch.setenv("SCHEMA_REGISTRY_USERNAME", "sr-user")
    monkeypatch.setenv("SCHEMA_REGISTRY_PASSWORD", "sr-pass")
    assert schema_registry_auth() == ("sr-user", "sr-pass")


def test_schema_registry_combined_form_wins_over_the_split_pair(monkeypatch):
    """Confluent's own precedence. Silently merging the two would produce a
    credential the operator never wrote down."""
    monkeypatch.setenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", "combined:wins")
    monkeypatch.setenv("SCHEMA_REGISTRY_USERNAME", "split")
    monkeypatch.setenv("SCHEMA_REGISTRY_PASSWORD", "loses")
    assert schema_registry_auth() == ("combined", "wins")


def test_schema_registry_auth_fails_closed_on_half_a_pair(monkeypatch):
    """A username with no password must not degrade to anonymous: the registry
    answers 401 and the serializer reports it as a schema-registration failure
    naming the subject, sending the operator to debug the wrong thing."""
    monkeypatch.setenv("SCHEMA_REGISTRY_USERNAME", "sr-user")
    with pytest.raises(KafkaSecurityError) as exc:
        schema_registry_auth()
    assert "SCHEMA_REGISTRY_PASSWORD" in str(exc.value)


def test_schema_registry_auth_rejects_a_combined_value_with_no_colon(monkeypatch):
    monkeypatch.setenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", "userpass")
    with pytest.raises(KafkaSecurityError):
        schema_registry_auth()


def test_schema_registry_password_may_contain_a_colon(monkeypatch):
    """partition() splits on the FIRST colon; a split on the last (or a naive
    split into exactly two parts) corrupts any password containing one."""
    monkeypatch.setenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", "user:pa:ss:word")
    assert schema_registry_auth() == ("user", "pa:ss:word")


# --------------------------------------------------------------------------
# Behavioural: KAFKA_SSL_SKIP_VERIFY must not disable chain validation
# --------------------------------------------------------------------------
#
# Four places in this repo state that KAFKA_SSL_SKIP_VERIFY is *not* the Go
# InsecureSkipVerify -- that in Python it relaxes the hostname check only, and
# the certificate chain is still validated. Everything above this line asserts
# what *we* emit (ssl_check_hostname=False). Nothing asserted what kafka-python
# then *does* with it, so the claim lived only in prose.
#
# That prose already rotted once. It cited kafka/conn.py, and requirements.txt
# pins `kafka-python>=3.0.10`, which resolves to 3.0.11 -- where the code had
# moved to kafka/net/transport.py. The conclusion survived the move; the
# citation did not, and nothing failed. A future release could just as easily
# move the *behaviour* instead, and again nothing would fail: the kind rig runs
# with generation.enabled=false, so no Python Kafka client is exercised there.
#
# Hence the pair below. One asserts the security property; the other is a
# control asserting the flag still changes something, so the first cannot pass
# because the setting was ignored outright.


def _kafka_python_ssl_context_builder():
    """Locate kafka-python's SSL context factory, wherever it lives this release.

    Deliberately *not* pytest.importorskip on a single module path: the module
    moved between 3.0.10 and 3.0.11, and a skip on a moved module reads exactly
    like a pass. Skip only when kafka-python is genuinely absent -- if it is
    installed but the factory cannot be found, fail and say so.
    """
    pytest.importorskip("kafka", reason="kafka-python not installed in this lane")

    candidates = (
        ("kafka.net.transport", "KafkaSSLTransport"),  # >= 3.0.11
        ("kafka.conn", "BrokerConnection"),            # <= 3.0.10
    )
    import importlib

    for module_name, attr in candidates:
        try:
            owner = getattr(importlib.import_module(module_name), attr)
        except (ImportError, AttributeError):
            continue
        builder = getattr(owner, "_build_ssl_context", None)
        if builder is not None:
            return builder

    import kafka

    pytest.fail(
        f"kafka-python {kafka.__version__} is installed but _build_ssl_context is "
        f"not at any of {[f'{m}.{a}' for m, a in candidates]}. Find where it moved, "
        "re-verify that skip-verify still leaves verify_mode at CERT_REQUIRED, and "
        "update this list plus the four places that cite it: "
        "llm-service/src/utils/kafka_security.py's docstring, "
        "deploy/helm/rsync-ai/values.yaml, "
        "deploy/helm/rsync-ai/templates/validate.yaml, and INVENTORY.md."
    )


def _context_for(monkeypatch, skip_verify):
    """Build the SSL context kafka-python would build from our own kwargs.

    Goes through kafka_security_kwargs() rather than a hand-written dict so the
    key names are exercised too -- a kwarg we emit under a name kafka-python no
    longer accepts is a runtime crash, not a silent downgrade, but it is still a
    crash nobody would see until a real deployment.
    """
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "u")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "p")
    monkeypatch.delenv("KAFKA_SSL_CA_LOCATION", raising=False)
    if skip_verify:
        monkeypatch.setenv("KAFKA_SSL_SKIP_VERIFY", "true")
    else:
        monkeypatch.delenv("KAFKA_SSL_SKIP_VERIFY", raising=False)

    build = _kafka_python_ssl_context_builder()
    kwargs = kafka_security_kwargs()

    # No CA file: the worst case for this claim, because that is the branch
    # where kafka-python falls back to load_default_certs(). If chain validation
    # were going to be off anywhere, it would be here.
    assert "ssl_cafile" not in kwargs, "this check is only meaningful with no CA"

    config = {
        "ssl_context": None,
        "ssl_check_hostname": True,
        "ssl_cafile": None,
        "ssl_certfile": None,
        "ssl_keyfile": None,
        "ssl_password": None,
        "ssl_crlfile": None,
    }
    config.update({k: v for k, v in kwargs.items() if k in config})
    return config, build(config)


def test_skip_verify_leaves_the_certificate_chain_validated(monkeypatch):
    """The security property the docs promise, asserted against the library.

    kafka-python builds its context with ssl.PROTOCOL_TLS_CLIENT, whose
    verify_mode is CERT_REQUIRED, and reassigns only check_hostname. So a
    broker presenting a certificate from an untrusted CA is still rejected --
    unlike the Go side, where InsecureSkipVerify really does turn the chain
    check off.
    """
    import ssl

    _, ctx = _context_for(monkeypatch, skip_verify=True)
    assert ctx.verify_mode == ssl.CERT_REQUIRED, (
        f"KAFKA_SSL_SKIP_VERIFY now yields verify_mode={ctx.verify_mode!r}. If "
        "kafka-python has started disabling chain validation, the flag has "
        "become as dangerous as the Go one and every doc calling it "
        "hostname-only is now wrong -- fix them before changing this test."
    )


def test_control_skip_verify_does_change_the_hostname_check(monkeypatch):
    """Without this, the test above passes even if the flag were ignored entirely.

    A no-op setting produces a context that validates the chain too, so the
    security assertion alone cannot tell "relaxes hostname only" apart from
    "does nothing at all".
    """
    on_cfg, on_ctx = _context_for(monkeypatch, skip_verify=True)
    off_cfg, off_ctx = _context_for(monkeypatch, skip_verify=False)

    assert on_cfg["ssl_check_hostname"] is False, (
        "kafka_security_kwargs stopped emitting ssl_check_hostname=False"
    )
    assert off_cfg["ssl_check_hostname"] is True
    assert on_ctx.check_hostname is False and off_ctx.check_hostname is True, (
        "kafka-python no longer honours ssl_check_hostname; the flag is inert "
        "and the operator asking for it gets a handshake failure instead"
    )
    assert on_ctx.verify_mode == off_ctx.verify_mode, (
        "skip-verify must change the hostname check and nothing else"
    )


@pytest.mark.parametrize(
    "env",
    [
        pytest.param({"KAFKA_SECURITY_PROTOCOL": "SSL"}, id="tls-only"),
        pytest.param(
            {
                "KAFKA_SECURITY_PROTOCOL": "SSL",
                "KAFKA_SSL_CA_LOCATION": "/etc/rsync-ai/kafka-tls/ca.crt",
                "KAFKA_SSL_CERT_LOCATION": "/etc/rsync-ai/kafka-tls/client.crt",
                "KAFKA_SSL_KEY_LOCATION": "/etc/rsync-ai/kafka-tls/client.key",
            },
            id="mtls",
        ),
        pytest.param(
            {
                "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
                "KAFKA_SASL_USERNAME": "u",
                "KAFKA_SASL_PASSWORD": "p",
                "KAFKA_SSL_CA_LOCATION": "/etc/rsync-ai/kafka-tls/ca.crt",
                "KAFKA_SSL_SKIP_VERIFY": "true",
            },
            id="sasl-ssl-scram-skipverify",
        ),
        pytest.param(
            {
                "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
                "KAFKA_SASL_MECHANISM": "PLAIN",
                "KAFKA_SASL_USERNAME": "u",
                "KAFKA_SASL_PASSWORD": "p",
            },
            id="sasl-plaintext-plain",
        ),
    ],
)
def test_every_kwarg_we_emit_is_a_name_kafka_python_accepts(monkeypatch, env):
    """kafka-python rejects unknown configs outright, so a renamed kwarg is a
    hard failure at client construction -- in a code path the kind rig never
    runs (generation.enabled=false), i.e. one that would surface first in a
    customer's deployment.
    """
    kafka = pytest.importorskip("kafka", reason="kafka-python not installed in this lane")
    from kafka.admin import KafkaAdminClient

    for key, value in env.items():
        monkeypatch.setenv(key, value)

    kwargs = kafka_security_kwargs()
    assert kwargs, "kafka_security_kwargs emitted nothing -- the scan is vacuous"

    for cls in (kafka.KafkaConsumer, kafka.KafkaProducer, KafkaAdminClient):
        unknown = sorted(set(kwargs) - set(cls.DEFAULT_CONFIG))
        assert not unknown, (
            f"{cls.__name__} would raise KafkaConfigurationError on {unknown} "
            f"(kafka-python {kafka.__version__}); every Python Kafka client would "
            "fail to construct"
        )
