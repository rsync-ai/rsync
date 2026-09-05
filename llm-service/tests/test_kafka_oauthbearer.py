"""OAUTHBEARER: the token path, and the JAAS shape the JVM actually accepts.

Two clients have to agree here and they are configured completely differently:

  * kafka-python authenticates with a *token provider object* -- it never sees a
    JAAS line. It also enforces the provider's base class with a real
    ``isinstance`` check, so a duck-typed provider is refused at connect time.
  * the JVM (Connect, and Debezium's schema-history client inside it)
    authenticates from a JAAS line plus two separate properties.

The JVM half is the one that cannot be reasoned out, so it was measured against
kafka-clients 3.7.0 -- deploy/helm/rsync-ai/test/kind/jaas-probe/OAuthProbe.java,
whose output is reproduced in the kind JOURNAL. The finding that matters:

    clientId / clientSecret / scope        JAAS options on the login module line
    sasl.oauthbearer.token.endpoint.url    a separate client property
    extension_<name>                       JAAS options

so the shape the other mechanisms suggest -- a credential-less
``LoginModule required;`` line, with the credentials alongside as properties --
is rejected outright:

    ConfigException: The OAuth configuration option clientId value must be non-null

That is a startup failure, not a silent one, but it is also the shape a
reasonable person writes first. test_a_credential_less_line_is_refused pins it.
"""

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils import kafka_security  # noqa: E402
from src.utils.kafka_security import (  # noqa: E402
    MECHANISM_OAUTHBEARER,
    OAUTHBEARER_LOGIN_CALLBACK_HANDLER,
    KafkaSecurityError,
    _jaas_config,
    debezium_schema_history_security,
    describe,
    kafka_security_kwargs,
    parse_sasl_extensions,
)

MODULE = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"
ENDPOINT = "https://idp.example/oauth2/token"


@pytest.fixture(autouse=True)
def _clear(monkeypatch):
    for key in list(os.environ):
        if key.startswith("KAFKA_"):
            monkeypatch.delenv(key, raising=False)
    return monkeypatch


def _configure(monkeypatch, **extra):
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "OAUTHBEARER")
    monkeypatch.setenv("KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT", ENDPOINT)
    monkeypatch.setenv("KAFKA_SASL_OAUTHBEARER_CLIENT_ID", "cid")
    monkeypatch.setenv("KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET", "s3cr3t")
    for key, value in extra.items():
        monkeypatch.setenv(key, value)


# --------------------------------------------------------------------------
# extensions parsing
# --------------------------------------------------------------------------

def test_extensions_parse_the_documented_shape():
    assert parse_sasl_extensions("a=1,b=2") == {"a": "1", "b": "2"}
    assert parse_sasl_extensions(" a = 1 , b = 2 ,, ") == {"a": "1", "b": "2"}
    assert parse_sasl_extensions(None) == {}
    assert parse_sasl_extensions("") == {}


def test_extensions_keep_an_equals_sign_in_the_value():
    """Values may contain '=' (base64 padding is the common case)."""
    assert parse_sasl_extensions("tok=abc==") == {"tok": "abc=="}


@pytest.mark.parametrize(
    "raw",
    [
        "novalue",              # no '=' at all
        "=v",                   # empty name
        "auth=Bearer x",        # reserved by the mechanism itself
        "a\x01b=v",             # separator smuggled into the name
        "a=v\x01w",             # separator smuggled into the value
    ],
)
def test_malformed_extensions_are_refused_not_silently_reshaped(raw):
    """kafka-python does no validation here, so this layer must.

    ``SaslMechanismOAuth._token_extensions`` joins pairs with 0x01 and inserts
    '=' with no checking (kafka/net/sasl/oauth.py). A bad key or value therefore
    does not raise -- it produces a *different, well-formed* extension set on the
    wire, which the broker either ignores or reads as some other extension.
    """
    with pytest.raises((KafkaSecurityError, ValueError)):
        parse_sasl_extensions(raw)


# --------------------------------------------------------------------------
# the JAAS line -- pinned to the JVM measurement
# --------------------------------------------------------------------------

def test_the_jaas_line_carries_the_credentials_as_options():
    line = _jaas_config(
        MECHANISM_OAUTHBEARER,
        "",
        "",
        {"clientId": "cid", "clientSecret": "sec", "scope": "sc", "extension_lc": "x"},
    )
    assert line == (
        f'{MODULE} required clientId="cid" clientSecret="sec" '
        'extension_lc="x" scope="sc";'
    )


def test_the_jaas_line_has_no_username_or_password():
    """A username= option is not merely useless here; OAuthBearerLoginModule
    does not define one, and its presence is the tell that a username mechanism
    was copy-pasted."""
    line = _jaas_config(
        MECHANISM_OAUTHBEARER, "", "", {"clientId": "c", "clientSecret": "s"}
    )
    assert "username=" not in line
    assert "password=" not in line


def test_a_credential_less_line_is_refused():
    """The negative control for the whole file.

    ``{MODULE} required;`` is a syntactically valid JAAS entry, so nothing local
    objects to it -- the JVM rejects it only at configure() time, with a message
    naming clientId. Refusing to emit it here turns that into an error at the
    layer that can name the missing environment variable.
    """
    for options in (None, {}, {"clientId": "c"}, {"clientSecret": "s"}):
        with pytest.raises(KafkaSecurityError) as excinfo:
            _jaas_config(MECHANISM_OAUTHBEARER, "", "", options)
        assert "clientId" in str(excinfo.value)


def test_option_values_are_escaped_like_every_other_jaas_value():
    """A client secret is as capable of holding a backslash as a password is.

    The JVM's StreamTokenizer-backed grammar eats one level of backslash inside a
    quoted string, so an unescaped one is consumed rather than rejected -- the
    same silent corruption test_kafka_jaas_escaping.py pins for passwords.
    """
    line = _jaas_config(
        MECHANISM_OAUTHBEARER, "", "", {"clientId": "c", "clientSecret": r"C:\Users\svc"}
    )
    assert r'clientSecret="C:\\Users\\svc"' in line


def test_username_mechanisms_are_byte_identical_to_before():
    """The options parameter must not perturb the path everything already uses."""
    assert _jaas_config("PLAIN", "u", "p") == (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        'username="u" password="p";'
    )


# --------------------------------------------------------------------------
# kafka-python kwargs
# --------------------------------------------------------------------------

def test_kwargs_supply_a_token_provider_and_no_password(monkeypatch):
    _configure(monkeypatch)
    kwargs = kafka_security_kwargs()
    assert kwargs["sasl_mechanism"] == "OAUTHBEARER"
    provider = kwargs["sasl_oauth_token_provider"]
    assert provider is not None
    # Passing sasl_plain_password alongside a token provider is not an error in
    # kafka-python -- it is simply ignored -- which is exactly why it must not be
    # set: it would put the client secret in a second place nobody redacts.
    assert "sasl_plain_username" not in kwargs
    assert "sasl_plain_password" not in kwargs


def test_the_provider_satisfies_kafka_pythons_isinstance_check(monkeypatch):
    """kafka-python refuses a duck-typed provider, so subclassing is load-bearing."""
    base = pytest.importorskip("kafka.net.sasl.oauth").AbstractTokenProvider
    _configure(monkeypatch)
    assert isinstance(kafka_security_kwargs()["sasl_oauth_token_provider"], base)


@pytest.mark.parametrize(
    "missing",
    [
        "KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT",
        "KAFKA_SASL_OAUTHBEARER_CLIENT_ID",
        "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET",
    ],
)
def test_incomplete_oauth_config_fails_closed(monkeypatch, missing):
    _configure(monkeypatch)
    monkeypatch.delenv(missing, raising=False)
    with pytest.raises(KafkaSecurityError):
        kafka_security_kwargs()


def test_username_and_password_are_accepted_as_id_and_secret(monkeypatch):
    """Matches the Go module, which documents the same fallback."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "OAUTHBEARER")
    monkeypatch.setenv("KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT", ENDPOINT)
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "cid")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "s3cr3t")
    assert kafka_security_kwargs()["sasl_oauth_token_provider"] is not None


def test_describe_never_prints_the_secret(monkeypatch):
    _configure(monkeypatch)
    line = describe()
    assert "s3cr3t" not in line
    assert "[redacted]" in line
    assert "cid" in line          # the id is not a secret and aids diagnosis


# --------------------------------------------------------------------------
# the token provider itself
# --------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, payload):
        self._body = json.dumps(payload).encode()

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _stub_endpoint(monkeypatch, payload, calls, exc=None):
    def fake_urlopen(request, timeout=None):
        calls.append(request)
        if exc is not None:
            raise exc
        return _FakeResponse(payload)

    monkeypatch.setattr(kafka_security.urllib.request, "urlopen", fake_urlopen)


def test_the_token_is_fetched_once_and_reused(monkeypatch):
    _configure(monkeypatch)
    calls = []
    _stub_endpoint(monkeypatch, {"access_token": "tok", "expires_in": 3600}, calls)
    provider = kafka_security_kwargs()["sasl_oauth_token_provider"]
    assert provider.token() == "tok"
    assert provider.token() == "tok"
    assert len(calls) == 1, (
        "the provider minted a token per call; kafka-python authenticates several "
        "sockets concurrently, and some IdPs rate-limit that"
    )


def test_the_post_is_a_client_credentials_grant(monkeypatch):
    _configure(monkeypatch, KAFKA_SASL_OAUTHBEARER_SCOPE="kafka")
    calls = []
    _stub_endpoint(monkeypatch, {"access_token": "tok", "expires_in": 60}, calls)
    kafka_security_kwargs()["sasl_oauth_token_provider"].token()
    request = calls[0]
    assert request.full_url == ENDPOINT
    assert request.get_method() == "POST"
    body = request.data.decode()
    assert "grant_type=client_credentials" in body
    assert "scope=kafka" in body


def test_a_token_is_refreshed_before_it_expires(monkeypatch):
    """A token cached to its exact expiry is a token that is already invalid by
    the time the broker checks it."""
    _configure(monkeypatch)
    calls = []
    _stub_endpoint(monkeypatch, {"access_token": "tok", "expires_in": 40}, calls)
    provider = kafka_security_kwargs()["sasl_oauth_token_provider"]

    clock = [1000.0]
    monkeypatch.setattr(kafka_security.time, "monotonic", lambda: clock[0])
    provider.token()
    assert len(calls) == 1
    clock[0] += 9          # inside 40 - 30 skew
    provider.token()
    assert len(calls) == 1
    clock[0] += 2          # past it
    provider.token()
    assert len(calls) == 2


def test_a_response_without_expires_in_is_refetched_every_time(monkeypatch):
    """RFC 6749 makes expires_in RECOMMENDED, not required. Caching such a token
    forever fails long after the deploy that would explain it."""
    _configure(monkeypatch)
    calls = []
    _stub_endpoint(monkeypatch, {"access_token": "tok"}, calls)
    provider = kafka_security_kwargs()["sasl_oauth_token_provider"]
    provider.token()
    provider.token()
    assert len(calls) == 2


def test_a_response_without_a_token_is_an_error_not_an_empty_token(monkeypatch):
    _configure(monkeypatch)
    _stub_endpoint(monkeypatch, {"error": "invalid_client"}, [])
    provider = kafka_security_kwargs()["sasl_oauth_token_provider"]
    with pytest.raises(KafkaSecurityError) as excinfo:
        provider.token()
    assert "access_token" in str(excinfo.value)


def test_a_failed_fetch_never_names_the_secret(monkeypatch):
    """This error reaches logs, and on the agent paths it can reach an LLM prompt."""
    _configure(monkeypatch)
    _stub_endpoint(monkeypatch, {}, [], exc=OSError("connection refused to s3cr3t"))
    provider = kafka_security_kwargs()["sasl_oauth_token_provider"]
    with pytest.raises(KafkaSecurityError) as excinfo:
        provider.token()
    message = str(excinfo.value)
    assert "s3cr3t" not in message, "the client secret leaked into an error string"
    assert ENDPOINT in message, "the endpoint is what makes this diagnosable"


def test_extensions_reach_the_provider(monkeypatch):
    _configure(monkeypatch, KAFKA_SASL_OAUTHBEARER_EXTENSIONS="logicalCluster=lkc-1")
    provider = kafka_security_kwargs()["sasl_oauth_token_provider"]
    assert provider.extensions() == {"logicalCluster": "lkc-1"}


# --------------------------------------------------------------------------
# Debezium schema history -- the KeyError path
# --------------------------------------------------------------------------

def test_schema_history_is_configured_rather_than_crashing(monkeypatch):
    """Reading sasl_plain_username here used to raise KeyError under OAUTHBEARER.

    That is worse than a missing property: the history client was unreachable
    rather than merely unconfigured, and the traceback pointed at this module
    rather than at the mechanism.
    """
    _configure(monkeypatch, KAFKA_SASL_OAUTHBEARER_SCOPE="kafka")
    props = debezium_schema_history_security()
    for role in ("producer", "consumer"):
        prefix = f"schema.history.internal.{role}."
        assert props[prefix + "sasl.mechanism"] == "OAUTHBEARER"
        assert props[prefix + "sasl.jaas.config"].startswith(MODULE)
        assert 'clientId="cid"' in props[prefix + "sasl.jaas.config"]
        assert props[prefix + "sasl.oauthbearer.token.endpoint.url"] == ENDPOINT
        assert (
            props[prefix + "sasl.login.callback.handler.class"]
            == OAUTHBEARER_LOGIN_CALLBACK_HANDLER
        )


def test_the_login_callback_handler_is_overridable(monkeypatch):
    """The class moved package between Kafka 3.5 and 4.0.

    ``...oauthbearer.secured.OAuthBearerLoginCallbackHandler`` is the only
    spelling before 3.6 and is gone in 4.0; the promoted name does not exist
    before 3.6. A hardcoded default is therefore wrong on one side or the other
    of that window, for a broker we do not control.
    """
    legacy = (
        "org.apache.kafka.common.security.oauthbearer.secured"
        ".OAuthBearerLoginCallbackHandler"
    )
    _configure(monkeypatch, KAFKA_SASL_OAUTHBEARER_LOGIN_CALLBACK_HANDLER=legacy)
    props = debezium_schema_history_security()
    assert (
        props["schema.history.internal.producer.sasl.login.callback.handler.class"]
        == legacy
    )


def test_schema_history_extensions_ride_in_the_jaas_line(monkeypatch):
    """The JVM has no sasl.oauthbearer.extensions property; extension_* options
    on the login module line are the only route."""
    _configure(monkeypatch, KAFKA_SASL_OAUTHBEARER_EXTENSIONS="logicalCluster=lkc-1")
    jaas = debezium_schema_history_security()[
        "schema.history.internal.producer.sasl.jaas.config"
    ]
    assert 'extension_logicalCluster="lkc-1"' in jaas
