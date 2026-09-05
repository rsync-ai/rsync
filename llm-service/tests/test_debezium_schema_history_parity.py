"""The two Debezium schema-history security builders must not drift apart.

There are two implementations of the same rules, and there has to be:

  * ``src.utils.kafka_security.debezium_schema_history_security`` -- used by the
    planner when it generates a CDC connector config;
  * ``_schema_history_security`` inside the debezium MCP connector -- which runs
    in its own container and cannot import llm-service.

They are hand-maintained copies, so they drift silently. They already had: the
connector emitted ``ssl.truststore.location`` with no ``ssl.truststore.type``,
which makes the JVM assume JKS, fail to parse a PEM, and report a keystore
*format* error -- so the operator goes off converting a file that never needed
converting. It had no mTLS keystore handling at all. Neither gap is visible from
either file alone; both are obvious the moment the outputs are diffed.

Diffing the whole output dict, rather than testing each side against a list of
expected keys, is deliberate: a hand-written expectation list drifts in exactly
the same way the implementations do, and would have been written from whichever
copy the author was looking at.

The password shapes below include backslashes and quotes on purpose. JAAS is a
quoted-string grammar and the JVM *consumes* an unescaped backslash rather than
rejecting it, so a mismatch here is a silently wrong password rather than an
error -- see test_kafka_jaas_escaping.py for the measured behaviour.
"""

import importlib.util
import os
from pathlib import Path

import pytest

_CONNECTOR = (
    Path(__file__).resolve().parents[2]
    / "shared" / "mcp-connectors" / "internal" / "debezium"
    / "versions" / "v1.0.0" / "connector.py"
)


def _load_connector():
    """Import the connector module that actually ships.

    Pinned to versions/v1.0.0 via the same path the Docker build context uses. If
    the connector is version-bumped this fails loudly on a missing file, which is
    the correct outcome -- the new version needs to be re-pointed here and
    re-diffed, not silently skipped.
    """
    assert _CONNECTOR.is_file(), (
        f"{_CONNECTOR} is missing. If the debezium connector was version-bumped, "
        "point this test at the new versions/<current_version>/connector.py and "
        "re-run -- do not delete the test, it is the only thing comparing the two "
        "implementations."
    )
    spec = importlib.util.spec_from_file_location("debezium_connector_parity", _CONNECTOR)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Shapes chosen to cover every branch that differs between the two files: no TLS,
# TLS with a CA, mTLS, skip-verify, and both SASL mechanism families. A shape that
# exercises no branch cannot detect drift, so each is here for a named reason.
SHAPES = {
    "plaintext_no_sasl": {},
    "sasl_plaintext_plain": {
        "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "KAFKA_SASL_MECHANISM": "PLAIN",
        "KAFKA_SASL_USERNAME": "svc",
        "KAFKA_SASL_PASSWORD": "s3cr3t",
    },
    "sasl_ssl_scram_with_ca": {
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
        "KAFKA_SASL_USERNAME": "svc",
        "KAFKA_SASL_PASSWORD": "s3cr3t",
        "KAFKA_SSL_CA_LOCATION": "/etc/rsync-ai/kafka-tls/ca.crt",
    },
    "mtls_no_sasl": {
        "KAFKA_SECURITY_PROTOCOL": "SSL",
        "KAFKA_SSL_CA_LOCATION": "/etc/rsync-ai/kafka-tls/ca.crt",
        "KAFKA_SSL_CERT_LOCATION": "/etc/rsync-ai/kafka-tls/tls.crt",
        "KAFKA_SSL_KEY_LOCATION": "/etc/rsync-ai/kafka-tls/tls.key",
        "KAFKA_SSL_KEYSTORE_LOCATION": "/etc/rsync-ai/kafka-tls/client.pem",
    },
    "sasl_ssl_skip_verify": {
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_SASL_MECHANISM": "SCRAM-SHA-256",
        "KAFKA_SASL_USERNAME": "svc",
        "KAFKA_SASL_PASSWORD": "s3cr3t",
        "KAFKA_SSL_CA_LOCATION": "/etc/rsync-ai/kafka-tls/ca.crt",
        "KAFKA_SSL_SKIP_VERIFY": "true",
    },
    # OAUTHBEARER splits its settings across a JAAS line and two properties,
    # and the history client builds all three. Without a shape here the two
    # implementations could disagree completely on the token path and this
    # suite would still be green.
    "sasl_ssl_oauthbearer": {
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_SASL_MECHANISM": "OAUTHBEARER",
        "KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT": "https://idp.example/oauth2/token",
        "KAFKA_SASL_OAUTHBEARER_CLIENT_ID": "cid",
        "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET": "s3cr3t",
        "KAFKA_SASL_OAUTHBEARER_SCOPE": "kafka",
        "KAFKA_SASL_OAUTHBEARER_EXTENSIONS": "logicalCluster=lkc-1",
        "KAFKA_SSL_CA_LOCATION": "/etc/rsync-ai/kafka-tls/ca.crt",
    },
    # A client secret is as capable of holding a backslash as a password is, and
    # it travels through the same JAAS grammar.
    "oauthbearer_secret_with_backslash": {
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_SASL_MECHANISM": "OAUTHBEARER",
        "KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT": "https://idp.example/oauth2/token",
        "KAFKA_SASL_OAUTHBEARER_CLIENT_ID": "domain\\svc",
        "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET": 'C:\\Users\\p"w',
        "KAFKA_SSL_CA_LOCATION": "/etc/rsync-ai/kafka-tls/ca.crt",
    },
    # The shape that found the escaping bug.
    "password_with_backslash_and_quote": {
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_SASL_MECHANISM": "PLAIN",
        "KAFKA_SASL_USERNAME": "domain\\svc",
        "KAFKA_SASL_PASSWORD": 'C:\\Users\\p"w',
        "KAFKA_SSL_CA_LOCATION": "/etc/rsync-ai/kafka-tls/ca.crt",
    },
}


@pytest.fixture
def clean_kafka_env(monkeypatch):
    for key in list(os.environ):
        if key.startswith("KAFKA_"):
            monkeypatch.delenv(key, raising=False)
    return monkeypatch


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_both_implementations_emit_the_same_properties(shape, clean_kafka_env):
    from src.utils.kafka_security import debezium_schema_history_security

    for key, value in SHAPES[shape].items():
        clean_kafka_env.setenv(key, value)

    connector = _load_connector()._schema_history_security()
    authority = debezium_schema_history_security()

    differing = sorted(
        k for k in set(connector) | set(authority) if connector.get(k) != authority.get(k)
    )
    assert not differing, "\n".join(
        [f"schema-history security drifted on shape {shape!r}:"]
        + [
            f"  {k}\n    connector  = {connector.get(k)!r}\n    llm-service= {authority.get(k)!r}"
            for k in differing
        ]
        + [
            "",
            "These are two copies of one rule set. Fix whichever is wrong in BOTH "
            "shared/mcp-connectors/internal/debezium/versions/v1.0.0/connector.py and "
            "llm-service/src/utils/kafka_security.py.",
        ]
    )


def test_the_shapes_actually_produce_properties(clean_kafka_env):
    """Vacuity floor: two functions that both return {} agree perfectly.

    Without this, breaking both implementations identically -- or breaking the env
    plumbing so neither sees any config -- would leave every parity assertion above
    passing on empty dicts.
    """
    from src.utils.kafka_security import debezium_schema_history_security

    sizes = {}
    for shape, env in SHAPES.items():
        for key in list(os.environ):
            if key.startswith("KAFKA_"):
                clean_kafka_env.delenv(key, raising=False)
        for key, value in env.items():
            clean_kafka_env.setenv(key, value)
        sizes[shape] = len(debezium_schema_history_security())

    assert sizes["plaintext_no_sasl"] == 0, "PLAINTEXT must stay unconfigured"
    non_trivial = {s: n for s, n in sizes.items() if s != "plaintext_no_sasl"}
    assert all(n >= 6 for n in non_trivial.values()), (
        f"some shape produced almost no properties, so it proves nothing: {non_trivial}"
    )
    # Producer and consumer are configured separately; the consumer half is what
    # recovers history on restart, and omitting it fails only on the restart path.
    for prefix in ("schema.history.internal.producer.", "schema.history.internal.consumer."):
        assert any(
            k.startswith(prefix) for k in debezium_schema_history_security()
        ), f"nothing configured under {prefix}"


def test_the_jaas_builders_agree_directly(clean_kafka_env):
    """Compare the escaping functions themselves, not only their callers.

    The dict diff above only reaches _jaas_config through whatever env shapes are
    listed; this pins the function across inputs no plausible shape would cover.
    """
    from src.utils.kafka_security import _jaas_config as authority

    connector = _load_connector()._jaas_config
    nasty = ["plain", "pa\\ss", 'pa"ss', "C:\\Users\\svc", "a\\nb", "tail\\", "sp ace=x"]
    for mechanism in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
        for value in nasty:
            assert connector(mechanism, value, value) == authority(mechanism, value, value), (
                f"_jaas_config disagrees for mechanism={mechanism} value={value!r}"
            )

    # OAUTHBEARER takes its credentials as options instead of a username and
    # password, so it needs its own pass -- the loop above would emit a
    # credential-less line that both implementations refuse.
    for value in nasty:
        options = {
            "clientId": value,
            "clientSecret": value,
            "scope": value,
            "extension_lc": value,
        }
        assert connector("OAUTHBEARER", "", "", options) == authority(
            "OAUTHBEARER", "", "", options
        ), f"_jaas_config disagrees for OAUTHBEARER value={value!r}"


def test_both_implementations_refuse_a_credential_less_token_line():
    """Neither copy may emit the line the JVM rejects at configure().

    The exception types differ by module convention (KafkaSecurityError here,
    ValueError in the connector), so this pins the behaviour rather than the
    class -- what matters is that neither returns a string.
    """
    from src.utils.kafka_security import KafkaSecurityError
    from src.utils.kafka_security import _jaas_config as authority

    connector = _load_connector()._jaas_config
    for build in (authority, connector):
        for options in (None, {}, {"clientId": "c"}, {"clientSecret": "s"}):
            with pytest.raises((KafkaSecurityError, ValueError)) as excinfo:
                build("OAUTHBEARER", "", "", options)
            assert "clientId" in str(excinfo.value)
