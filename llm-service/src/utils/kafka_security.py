"""Kafka SASL/TLS + broker-list plumbing for every kafka-python client.

This is the Python counterpart of the Go module ``shared/go/kafkaclient``. The
two MUST stay in lockstep: same environment variable names, same defaults, same
fail-closed rule. A customer configures their cluster once, and the Go services
and the Python agents have to agree on what that configuration meant.

Two defects motivated it, both reachable from every client in this service:

1. **Collapse.** ``bootstrap_servers`` is one string here but a *list* to
   kafka-python. Passing "b1:9093,b2:9093" unsplit makes one unresolvable
   hostname, so a healthy three-broker cluster is unreachable and the failure
   reads as a broker outage rather than a config bug.

2. **Plaintext.** Every client used kafka-python's defaults: no TLS, no SASL.
   Against a cluster that requires either — which is every managed Kafka and
   every security-reviewed self-hosted one — the client cannot connect at all.

Usage — splat the kwargs into any kafka-python constructor::

    from src.utils.kafka_security import kafka_security_kwargs, parse_brokers

    KafkaConsumer(
        topic,
        bootstrap_servers=parse_brokers(KAFKA_BROKERS),
        **kafka_security_kwargs(),
    )

With nothing configured this returns ``{"security_protocol": "PLAINTEXT"}``,
which is kafka-python's own default — so an existing plaintext deployment
behaves identically.
"""

from __future__ import annotations

import collections
import ipaddress
import json
import logging
import os
import threading
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

# Environment variables. Identical to the Go module's; see its config.go.
ENV_BROKERS = "KAFKA_BROKERS"
ENV_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS"
ENV_SECURITY_PROTOCOL = "KAFKA_SECURITY_PROTOCOL"
ENV_SASL_MECHANISM = "KAFKA_SASL_MECHANISM"
ENV_SASL_USERNAME = "KAFKA_SASL_USERNAME"
ENV_SASL_PASSWORD = "KAFKA_SASL_PASSWORD"
ENV_SSL_CA_LOCATION = "KAFKA_SSL_CA_LOCATION"
ENV_SSL_CERT_LOCATION = "KAFKA_SSL_CERT_LOCATION"
ENV_SSL_KEY_LOCATION = "KAFKA_SSL_KEY_LOCATION"
ENV_SSL_SKIP_VERIFY = "KAFKA_SSL_SKIP_VERIFY"
# The same client keypair in the one shape a JVM can load: a PEM keystore is a
# SINGLE file holding the chain and the key, so Kafka Connect and the Kafka CLI
# cannot use ENV_SSL_CERT_LOCATION/ENV_SSL_KEY_LOCATION the way this module's
# own kafka-python clients do. Read only by debezium_schema_history_security();
# nothing in-process uses it.
ENV_SSL_KEYSTORE_LOCATION = "KAFKA_SSL_KEYSTORE_LOCATION"
# Where the kafka-connect image's entrypoint writes that one file when it is
# given only the two-path pair (RSYNC_CLIENT_PEM in
# shared/internal/infra/kafka-connect/connect-entrypoint.sh). A path in the
# CONNECT container, not this one: the schema-history client runs inside the
# Connect worker. The debezium connector carries the same constant, and
# test_debezium_schema_history_parity.py pins all three together.
CONNECT_IMAGE_CLIENT_PEM = "/kafka/rsync-tls/client.pem"

# OIDC client-credentials settings for KAFKA_SASL_MECHANISM=OAUTHBEARER.
# Spellings copied from the Go module (config.go EnvOAuth*) rather than invented:
# an operator configures the cluster once and both halves have to read the same
# variables, and a Python-only spelling would authenticate the Go services while
# leaving the agents anonymous -- a half-green deployment.
#
# ENV_SASL_USERNAME/ENV_SASL_PASSWORD are accepted as the client id and secret
# when the dedicated names are unset, matching Go, so there is one pair of
# variables to reach for regardless of mechanism.
ENV_OAUTH_TOKEN_ENDPOINT = "KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT"
ENV_OAUTH_CLIENT_ID = "KAFKA_SASL_OAUTHBEARER_CLIENT_ID"
ENV_OAUTH_CLIENT_SECRET = "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET"
ENV_OAUTH_SCOPE = "KAFKA_SASL_OAUTHBEARER_SCOPE"
ENV_OAUTH_EXTENSIONS = "KAFKA_SASL_OAUTHBEARER_EXTENSIONS"
# Re-permits an http:// token endpoint that is not on loopback. Exists only for
# a test rig running a throwaway IdP; see _token_endpoint_is_insecure for why
# the default is a refusal rather than a warning.
ENV_OAUTH_ALLOW_INSECURE_TOKEN_ENDPOINT = (
    "KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT"
)

# Schema Registry is a separate service with separate credentials — a cluster can
# require SASL while its registry is open, or the reverse. Confluent's own
# convention is the single "user:pass" string; the split pair is accepted because
# it is what Kubernetes secrets naturally produce.
ENV_SR_BASIC_AUTH = "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO"
ENV_SR_USERNAME = "SCHEMA_REGISTRY_USERNAME"
ENV_SR_PASSWORD = "SCHEMA_REGISTRY_PASSWORD"

PROTOCOL_PLAINTEXT = "PLAINTEXT"
PROTOCOL_SSL = "SSL"
PROTOCOL_SASL_PLAINTEXT = "SASL_PLAINTEXT"
PROTOCOL_SASL_SSL = "SASL_SSL"

_PROTOCOLS = {
    PROTOCOL_PLAINTEXT,
    PROTOCOL_SSL,
    PROTOCOL_SASL_PLAINTEXT,
    PROTOCOL_SASL_SSL,
}

MECHANISM_PLAIN = "PLAIN"
MECHANISM_SCRAM_SHA_256 = "SCRAM-SHA-256"
MECHANISM_SCRAM_SHA_512 = "SCRAM-SHA-512"
MECHANISM_OAUTHBEARER = "OAUTHBEARER"

# Split by *what a credential looks like*, not alphabetically. The two groups
# validate differently and fail differently: a username/password mechanism with
# a missing password is an incomplete pair, while OAUTHBEARER with a missing
# token endpoint has nowhere to go and ignores the pair entirely. Merging them
# into one set is how the "both must be set" check ends up demanding a password
# from a mechanism that has none.
_USERNAME_MECHANISMS = {
    MECHANISM_PLAIN,
    MECHANISM_SCRAM_SHA_256,
    MECHANISM_SCRAM_SHA_512,
}
_TOKEN_MECHANISMS = {MECHANISM_OAUTHBEARER}
_MECHANISMS = _USERNAME_MECHANISMS | _TOKEN_MECHANISMS

# Named so the error can say what is missing rather than "unsupported".
#
# AWS_MSK_IAM is the same wire mechanism as OAUTHBEARER and differs only in
# where the token comes from -- it stays here because the token source is a
# SigV4 signature over AWS credentials, which this module does not implement.
# GSSAPI needs a Kerberos keytab and a working KDC, neither of which any
# deployment of this product has.
_UNIMPLEMENTED_MECHANISMS = {"AWS_MSK_IAM", "GSSAPI"}

# The reserved key of the OAUTHBEARER initial client response, and the byte that
# separates extension pairs on the wire. Both come from RFC 7628, and kafka-python
# joins extensions with the separator without validating either -- so a value
# carrying one silently corrupts the frame into a different set of extensions.
_SASL_EXT_AUTH_KEY = "auth"
_SASL_EXT_SEPARATOR = "\x01"

# How long before expiry a cached token is considered spent. A token that
# expires between the provider handing it over and the broker validating it
# fails as an authentication error, which reads as a bad credential.
_TOKEN_REFRESH_SKEW_SECONDS = 30
_TOKEN_HTTP_TIMEOUT_SECONDS = 10

_TRUTHY = {"1", "true", "yes", "on"}


class KafkaSecurityError(ValueError):
    """Raised when the Kafka security configuration cannot be honoured.

    Deliberately fatal at the call site. Falling back to plaintext against a
    cluster the operator asked us to authenticate to produces a connection
    error that names the broker, not the misconfiguration — and that costs an
    on-call cycle to tell apart from a real outage.
    """


def parse_sasl_extensions(raw: str | None) -> Dict[str, str]:
    """Parse ``KAFKA_SASL_OAUTHBEARER_EXTENSIONS`` -- "k=v,k2=v2".

    A direct port of the Go module's ``ParseSASLExtensions`` +
    ``validateSASLExtensions``. The validation is not cosmetic: kafka-python
    builds the initial client response by joining the pairs with 0x01 and
    inserting '=' between key and value, with no checking of either
    (``kafka/net/sasl/oauth.py`` ``_token_extensions``). So a key containing '='
    or a value containing 0x01 does not raise -- it produces a *different,
    well-formed* set of extensions on the wire, and the broker rejects the
    handshake for a reason that is nowhere in the config.
    """
    raw = (raw or "").strip()
    if not raw:
        return {}
    out: Dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair:
            continue
        key, sep, value = pair.partition("=")
        if not sep:
            raise KafkaSecurityError(
                f"{ENV_OAUTH_EXTENSIONS}: {pair!r} is not key=value"
            )
        key, value = key.strip(), value.strip()
        if not key:
            raise KafkaSecurityError(
                f"{ENV_OAUTH_EXTENSIONS}: extension with an empty name"
            )
        if key.lower() == _SASL_EXT_AUTH_KEY:
            raise KafkaSecurityError(
                f"{ENV_OAUTH_EXTENSIONS}: extension {key!r} is reserved for the "
                "token itself"
            )
        if _SASL_EXT_SEPARATOR in key or _SASL_EXT_SEPARATOR in value:
            raise KafkaSecurityError(
                f"{ENV_OAUTH_EXTENSIONS}: extension {key!r} contains the "
                "reserved separator byte 0x01"
            )
        out[key] = value
    return out


def _abstract_token_provider_base() -> type:
    """Locate kafka-python's ``AbstractTokenProvider``, wherever it lives today.

    Resolved at call time rather than imported at module scope, for two reasons
    that both produce silent breakage otherwise:

    * kafka-python enforces the base class with a real ``isinstance`` check
      (``kafka/net/sasl/oauth.py``), so a duck-typed provider is refused. The
      subclass relationship is load-bearing, not documentation.
    * the module path has already moved -- ``kafka.oauth.abstract`` in older
      releases, ``kafka.net.sasl.oauth`` in 3.0.11. A hard import of either
      spelling turns a library upgrade into an ImportError at process start,
      on every client, including the ones not using OAUTHBEARER.
    """
    import importlib

    for module in ("kafka.net.sasl.oauth", "kafka.sasl.oauth", "kafka.oauth.abstract"):
        try:
            mod = importlib.import_module(module)
        except ImportError:
            continue
        base = getattr(mod, "AbstractTokenProvider", None)
        if isinstance(base, type):
            return base
    raise KafkaSecurityError(
        f"{ENV_SASL_MECHANISM}={MECHANISM_OAUTHBEARER} needs kafka-python's "
        "AbstractTokenProvider, which was not found under any known module path "
        "(kafka.net.sasl.oauth, kafka.sasl.oauth, kafka.oauth.abstract). "
        "kafka-python enforces the base class with isinstance, so a duck-typed "
        "provider will be refused -- this is an incompatible kafka-python."
    )


def _build_oidc_token_provider(
    endpoint: str,
    client_id: str,
    client_secret: str,
    scope: str,
    extensions: Dict[str, str],
):
    """A client-credentials token provider, cached and refreshed ahead of expiry.

    Built as a closure over a dynamically-resolved base class because the base
    cannot be named at import time (see ``_abstract_token_provider_base``).

    Uses ``urllib.request`` rather than ``requests``/``httpx`` deliberately:
    this module is imported by every kafka-python client in the service,
    including ones whose images do not carry an HTTP library, and a token
    provider that raises ImportError at connect time is indistinguishable from
    an outage.
    """
    base = _abstract_token_provider_base()

    class _OIDCTokenProvider(base):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            super().__init__()
            self._lock = threading.Lock()
            self._token: Optional[str] = None
            self._expires_at = 0.0

        def token(self) -> str:
            # The lock is what makes "ensure token reuse" true under the
            # multi-connection clients: KafkaProducer and KafkaConsumer both
            # authenticate several sockets concurrently, and an unlocked cache
            # mints a token per socket -- which some providers rate-limit.
            with self._lock:
                if self._token and time.monotonic() < self._expires_at:
                    return self._token
                self._token, self._expires_at = self._fetch()
                return self._token

        def extensions(self) -> Dict[str, str]:
            return dict(extensions)

        def _fetch(self):
            body = {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            }
            if scope:
                body["scope"] = scope
            request = urllib.request.Request(
                endpoint,
                data=urllib.parse.urlencode(body).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(
                    request, timeout=_TOKEN_HTTP_TIMEOUT_SECONDS
                ) as response:
                    payload = json.loads(response.read().decode())
            except Exception as exc:
                # The endpoint URL is named but the secret never is. This error
                # reaches logs, and on the LLM paths it can reach a prompt.
                raise KafkaSecurityError(
                    f"{ENV_OAUTH_TOKEN_ENDPOINT}={endpoint!r}: could not obtain "
                    f"an OAUTHBEARER token ({type(exc).__name__})"
                ) from exc

            access_token = payload.get("access_token")
            if not access_token:
                raise KafkaSecurityError(
                    f"{ENV_OAUTH_TOKEN_ENDPOINT}={endpoint!r} returned no "
                    "access_token; the response is not a client-credentials "
                    "grant"
                )
            # expires_in is RECOMMENDED, not required, by RFC 6749. Absent, a
            # single fetch would be cached forever and every client would start
            # failing the moment the token aged out -- long after the deploy
            # that would explain it. Re-fetching each time is the safe default.
            try:
                lifetime = float(payload.get("expires_in", 0))
            except (TypeError, ValueError):
                lifetime = 0.0
            deadline = time.monotonic() + max(
                0.0, lifetime - _TOKEN_REFRESH_SKEW_SECONDS
            )
            return str(access_token), deadline

    return _OIDCTokenProvider()


def _is_loopback_host(host: str) -> bool:
    """True for a host that cannot leave this machine.

    ``localhost`` and every ``*.localhost`` name are reserved for loopback by
    RFC 6761; a literal address is loopback when the IP says so (127.0.0.0/8,
    ::1), which is wider than string-matching "127.0.0.1". The trailing dot of
    a fully-qualified name is stripped first, because ``localhost.`` resolves
    to loopback and would otherwise be treated as a remote host.
    """
    host = host.strip().rstrip(".").lower()
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _token_endpoint_is_insecure(raw: str) -> bool:
    """True when fetching a token from ``raw`` would leak the client secret.

    The client-credentials grant POSTs the client secret to this URL on every
    token fetch. Over http to a remote host that is a permanent credential
    handed to anyone on the path -- worse than the replayable bearer token it
    buys, and not repairable by any broker-side setting. Loopback is exempt
    because the request never reaches a wire (Google's Workload Identity token
    server is exactly this: ``http://localhost:14293``).
    """
    parts = urllib.parse.urlsplit(raw.strip())
    if parts.scheme.lower() == "https" or not parts.hostname:
        return False
    return not _is_loopback_host(parts.hostname)


def _oauth_settings() -> Tuple[str, str, str, str, Dict[str, str]]:
    """Resolve the OIDC client-credentials settings, or fail closed."""
    endpoint = (os.getenv(ENV_OAUTH_TOKEN_ENDPOINT) or "").strip()
    if not endpoint:
        raise KafkaSecurityError(
            f"{ENV_SASL_MECHANISM}={MECHANISM_OAUTHBEARER} requires "
            f"{ENV_OAUTH_TOKEN_ENDPOINT} (the OIDC token endpoint to fetch the "
            "bearer token from)"
        )
    scheme = urllib.parse.urlsplit(endpoint).scheme.lower()
    if scheme not in ("https", "http"):
        raise KafkaSecurityError(
            f"{ENV_OAUTH_TOKEN_ENDPOINT}={endpoint!r} has scheme {scheme!r}; "
            "the client-credentials grant is an HTTP POST "
            "(want https://issuer/oauth2/token)"
        )
    allow_insecure = (
        os.getenv(ENV_OAUTH_ALLOW_INSECURE_TOKEN_ENDPOINT) or ""
    ).strip().lower() in _TRUTHY
    if not allow_insecure and _token_endpoint_is_insecure(endpoint):
        raise KafkaSecurityError(
            f"{ENV_OAUTH_TOKEN_ENDPOINT}={endpoint!r} is http and its host is "
            f"not loopback, so {ENV_OAUTH_CLIENT_SECRET} would be sent in the "
            "clear on every token fetch; use https, a loopback address, or set "
            f"{ENV_OAUTH_ALLOW_INSECURE_TOKEN_ENDPOINT}=true for a disposable "
            "test rig"
        )
    client_id = (
        os.getenv(ENV_OAUTH_CLIENT_ID) or os.getenv(ENV_SASL_USERNAME) or ""
    ).strip()
    client_secret = (
        os.getenv(ENV_OAUTH_CLIENT_SECRET) or os.getenv(ENV_SASL_PASSWORD) or ""
    )
    if not client_id or not client_secret:
        raise KafkaSecurityError(
            f"{ENV_SASL_MECHANISM}={MECHANISM_OAUTHBEARER} requires a client id "
            f"and secret: set {ENV_OAUTH_CLIENT_ID}/{ENV_OAUTH_CLIENT_SECRET}, "
            f"or {ENV_SASL_USERNAME}/{ENV_SASL_PASSWORD}"
        )
    scope = (os.getenv(ENV_OAUTH_SCOPE) or "").strip()
    extensions = parse_sasl_extensions(os.getenv(ENV_OAUTH_EXTENSIONS))
    return endpoint, client_id, client_secret, scope, extensions


def parse_brokers(raw: str | None) -> List[str]:
    """Split a comma-separated broker list, dropping blanks and whitespace.

    This is the fix for defect 1. ``[raw]`` would have made a CSV one host.
    """
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def brokers_from_env(default: str = "") -> List[str]:
    """Resolve the broker list: KAFKA_BROKERS wins over KAFKA_BOOTSTRAP_SERVERS."""
    raw = (os.getenv(ENV_BROKERS) or "").strip()
    if not raw:
        raw = (os.getenv(ENV_BOOTSTRAP_SERVERS) or "").strip()
    if not raw:
        raw = default
    return parse_brokers(raw)


def _protocol() -> str:
    p = (os.getenv(ENV_SECURITY_PROTOCOL) or "").strip().upper()
    return p or PROTOCOL_PLAINTEXT


def uses_tls(protocol: str | None = None) -> bool:
    p = protocol or _protocol()
    return p in (PROTOCOL_SSL, PROTOCOL_SASL_SSL)


def uses_sasl(protocol: str | None = None) -> bool:
    p = protocol or _protocol()
    return p in (PROTOCOL_SASL_PLAINTEXT, PROTOCOL_SASL_SSL)


def kafka_security_kwargs() -> Dict[str, Any]:
    """Build the kafka-python kwargs for the configured security profile.

    Raises KafkaSecurityError on a configuration that cannot be honoured, so a
    caller fails loudly at startup instead of silently dialing plaintext.
    """
    protocol = _protocol()
    if protocol not in _PROTOCOLS:
        raise KafkaSecurityError(
            f"{ENV_SECURITY_PROTOCOL}={protocol!r} is not supported; "
            f"expected one of {sorted(_PROTOCOLS)}"
        )

    # Every Kafka client in this service is built from these kwargs, so this is
    # the one place that guarantees the explainer is recording before a client
    # can fail. See explain_failure() for why the exception alone is not enough.
    arm_failure_explainer()

    kwargs: Dict[str, Any] = {"security_protocol": protocol}

    if uses_tls(protocol):
        ca = (os.getenv(ENV_SSL_CA_LOCATION) or "").strip()
        cert = (os.getenv(ENV_SSL_CERT_LOCATION) or "").strip()
        key = (os.getenv(ENV_SSL_KEY_LOCATION) or "").strip()
        # An empty CA path is legitimate: managed Kafka (Confluent Cloud, Aiven,
        # MSK public endpoints) presents a certificate chaining to a public root,
        # so the system trust store is the correct answer. Only a private CA
        # needs the file.
        if ca:
            kwargs["ssl_cafile"] = ca
        if bool(cert) != bool(key):
            raise KafkaSecurityError(
                f"{ENV_SSL_CERT_LOCATION} and {ENV_SSL_KEY_LOCATION} must be set "
                "together (mTLS needs both halves of the keypair)"
            )
        if cert:
            kwargs["ssl_certfile"] = cert
            kwargs["ssl_keyfile"] = key
        if (os.getenv(ENV_SSL_SKIP_VERIFY) or "").strip().lower() in _TRUTHY:
            kwargs["ssl_check_hostname"] = False

    if uses_sasl(protocol):
        mechanism = (os.getenv(ENV_SASL_MECHANISM) or "").strip().upper()
        # A SASL protocol with no mechanism is a common operator slip; default it
        # to PLAIN only when SASL was explicitly requested, matching the Go side.
        if not mechanism:
            mechanism = MECHANISM_PLAIN
        if mechanism in _UNIMPLEMENTED_MECHANISMS:
            raise KafkaSecurityError(
                f"{ENV_SASL_MECHANISM}={mechanism!r} is not implemented yet; it "
                "needs a credential provider rather than a static "
                "username/password"
            )
        if mechanism not in _MECHANISMS:
            raise KafkaSecurityError(
                f"{ENV_SASL_MECHANISM}={mechanism!r} is not supported; "
                f"expected one of {sorted(_MECHANISMS)}"
            )
        kwargs["sasl_mechanism"] = mechanism

        if mechanism in _TOKEN_MECHANISMS:
            endpoint, client_id, client_secret, scope, ext = _oauth_settings()
            # kafka-python takes the provider OBJECT, not the settings -- there
            # is no env-driven path into it, which is why this module has to
            # construct one. It also isinstance-checks the base class, so the
            # provider cannot be a simple lambda or a duck-typed shim.
            kwargs["sasl_oauth_token_provider"] = _build_oidc_token_provider(
                endpoint, client_id, client_secret, scope, ext
            )
            # Deliberately NOT setting sasl_plain_username/password: kafka-python
            # ignores them under OAUTHBEARER, and leaving them in the kwargs puts
            # the client secret into a dict that callers log.
        else:
            username = os.getenv(ENV_SASL_USERNAME) or ""
            password = os.getenv(ENV_SASL_PASSWORD) or ""
            if not username or not password:
                raise KafkaSecurityError(
                    f"{protocol} requires both {ENV_SASL_USERNAME} and "
                    f"{ENV_SASL_PASSWORD}"
                )
            kwargs["sasl_plain_username"] = username
            kwargs["sasl_plain_password"] = password

    return kwargs


# ---------------------------------------------------------------------------
# Why a connection failed
#
# kafka-python raises the WRONG exception for every auth and TLS failure there
# is. The broker does not answer "403" -- it closes the socket, or it answers a
# SaslAuthenticationException that the client swallows into its connection
# state machine and then retries. What surfaces to the caller, seconds later, is
#
#     KafkaTimeoutError: Failed to update metadata after 60.0 secs
#
# for a wrong password, an untrusted CA, a missing client certificate, an
# expired token and a broker that is genuinely down -- one message for five
# unrelated causes, none of which it names. Operators read it as "the network",
# and a Kafka security misconfiguration is then invisible for as long as it
# takes someone to run a CLI client by hand.
#
# The real verdict IS available: kafka-python logs it on the `kafka` logger at
# WARNING/ERROR before it converts the failure into retry state. So this keeps
# the last few such records and hands the most specific one back alongside the
# useless exception. Read-only -- it adds a handler, never a filter, and leaves
# propagate alone, so a service's own logging is unchanged.
#
# Adapted from the kafka-matrix probe's log capture, which is where this was
# first needed (deploy/helm/rsync-ai/test/kind/kafka-matrix/probe.py) -- and
# which is the reason it lives HERE rather than in each caller: the probe and
# the services have to agree about what "the cause" is, or the matrix cannot
# be evidence for the product.
# ---------------------------------------------------------------------------

# A record is a candidate cause only if it mentions one of these. Everything
# kafka-python logs at WARNING is otherwise routine reconnect chatter.
_CAUSE_KEYS = (
    "authenticat",        # SaslAuthenticationFailed, "authentication failed"
    "sasl",
    "ssl",
    "certificate",
    "tls",
    "handshake",
    "unknown authority",
    "verify failed",
    "token",
    "unauthor",
    "403",
    "401",
)

# Ties broken toward the most specific verdict. A broker that refuses a client
# usually ALSO races a plain "socket disconnected" onto the same logger, and
# that one is true but useless -- it is the symptom the caller already has.
_CAUSE_RANK = (
    "saslauthenticationfailed",
    "authentication failed",
    "certificate_verify_failed",
    "certificate verify failed",
    "unknown authority",
    "bad certificate",
    "certificate required",
    "handshake",
    "invalid_token",
    "invalid_client",
    "could not obtain",
)

# Bounded on purpose: this is armed for the life of a long-running service, and
# an unbounded list of every Kafka warning is a slow leak. Only the last few
# matter -- the cause is logged within milliseconds of the failure.
_CAUSE_BUFFER_SIZE = 32
_cause_records: "collections.deque[str]" = collections.deque(maxlen=_CAUSE_BUFFER_SIZE)
_cause_lock = threading.Lock()
_cause_armed = False


class _CauseRecorder(logging.Handler):
    """Keeps the text of kafka-python's own warnings, and nothing else."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001 -- a broken format string is not our failure
            return
        with _cause_lock:
            _cause_records.append(message)


def _secret_values() -> List[str]:
    """Every configured value that must never appear in a surfaced message.

    kafka-python does not log credentials today, but this text is built from a
    third party's log records and is handed to callers that log it, put it in an
    API response, or -- via the LLM call sites -- into a prompt. Redacting at
    the boundary is cheap; auditing every future kafka-python release is not.
    """
    values = []
    for name in (ENV_SASL_PASSWORD, ENV_OAUTH_CLIENT_SECRET,
                 ENV_SR_PASSWORD, ENV_SR_BASIC_AUTH):
        value = (os.getenv(name) or "").strip()
        # Below 8 characters a "secret" is more likely to be a substring of
        # ordinary words in the message than the credential itself, and
        # redacting those would corrupt the very text this exists to surface.
        if len(value) >= 8:
            values.append(value)
    return values


def _redact(text: str) -> str:
    for value in _secret_values():
        text = text.replace(value, "***")
    return text


def arm_failure_explainer() -> None:
    """Start recording kafka-python's warnings so explain_failure() can use them.

    Idempotent and safe to call from any thread; kafka_security_kwargs() calls
    it, so every client built through this module is covered without the call
    site remembering.
    """
    global _cause_armed
    with _cause_lock:
        if _cause_armed:
            return
        _cause_armed = True
    handler = _CauseRecorder(level=logging.WARNING)
    logger = logging.getLogger("kafka")
    logger.addHandler(handler)
    # Arming is a diagnostic aid and must be invisible in the service's own
    # output, so the ONLY level this touches is one nobody chose: a kafka logger
    # still at NOTSET that inherits a threshold above WARNING, where the records
    # this depends on are never created at all. A level set ON the kafka logger
    # -- including a quieter one -- is a deliberate choice about kafka's output
    # and is left exactly as found, as is propagate.
    if (
        logger.level == logging.NOTSET
        and logger.getEffectiveLevel() > logging.WARNING
    ):
        logger.setLevel(logging.WARNING)


def failure_cause() -> Optional[str]:
    """The most specific auth/TLS verdict kafka-python has logged, if any."""
    with _cause_lock:
        candidates = [m for m in _cause_records
                      if any(k in m.lower() for k in _CAUSE_KEYS)]
    if not candidates:
        return None
    # Most recent first, then most specific: a stale record from an earlier
    # reconnect must not outrank the one that explains this failure.
    candidates.reverse()
    best = min(candidates, key=lambda m: next(
        (i for i, r in enumerate(_CAUSE_RANK) if r in m.lower()), len(_CAUSE_RANK)))
    return _redact(" ".join(best.split()))[:400]


def explain_failure(exc: BaseException) -> str:
    """`exc`, plus the real cause when kafka-python logged one.

    Use this in the broad `except Exception` around any Kafka client startup:
    the exception alone is `KafkaTimeoutError: Failed to update metadata`, which
    is the same string for a wrong password and an unplugged broker.
    """
    described = _redact(f"{type(exc).__name__}: {exc}")
    cause = failure_cause()
    return f"{described} | cause: {cause}" if cause else described


def reset_failure_cause() -> None:
    """Drop recorded records. For tests, and for a caller retrying a new config."""
    with _cause_lock:
        _cause_records.clear()


def describe() -> str:
    """A log-safe one-liner describing the profile.

    The password is never included. This function exists so that logging the
    Kafka configuration — which operators genuinely need when a connection
    fails — cannot leak the credential.
    """
    protocol = _protocol()
    mechanism = (os.getenv(ENV_SASL_MECHANISM) or "").strip().upper() or "none"
    ca = (os.getenv(ENV_SSL_CA_LOCATION) or "").strip()
    if mechanism in _TOKEN_MECHANISMS:
        # The token endpoint is the one thing worth seeing here -- a wrong one
        # is the most common OAUTHBEARER misconfiguration. The client id is
        # shown because it is an identifier, not a credential; the secret is
        # not shown at all, and neither is the token.
        endpoint = (os.getenv(ENV_OAUTH_TOKEN_ENDPOINT) or "").strip()
        client_id = (
            os.getenv(ENV_OAUTH_CLIENT_ID) or os.getenv(ENV_SASL_USERNAME) or ""
        ).strip()
        return (
            f"kafka{{brokers={brokers_from_env()} protocol={protocol} "
            f"mechanism={mechanism} token_endpoint={endpoint!r} "
            f"client_id={client_id!r} client_secret=[redacted] ca={ca!r}}}"
        )
    username = os.getenv(ENV_SASL_USERNAME) or ""
    return (
        f"kafka{{brokers={brokers_from_env()} protocol={protocol} "
        f"mechanism={mechanism} user={username!r} password=[redacted] ca={ca!r}}}"
    )


# --------------------------------------------------------------------------
# Debezium / Kafka Connect
# --------------------------------------------------------------------------

_JAAS_MODULES = {
    MECHANISM_PLAIN: "org.apache.kafka.common.security.plain.PlainLoginModule",
    MECHANISM_SCRAM_SHA_256: "org.apache.kafka.common.security.scram.ScramLoginModule",
    MECHANISM_SCRAM_SHA_512: "org.apache.kafka.common.security.scram.ScramLoginModule",
    MECHANISM_OAUTHBEARER: (
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"
    ),
}

# The JVM's client-credentials handler. OAUTHBEARER splits its settings across
# two places, and the split is not guessable -- measured against kafka-clients
# 3.7.0 (deploy/helm/rsync-ai/test/kind/jaas-probe/OAuthProbe.java):
#
#   clientId / clientSecret / scope    JAAS options on the login module line
#                                      (AccessTokenRetrieverFactory:62-63, via
#                                      JaasOptionsUtils)
#   sasl.oauthbearer.token.endpoint.url   a separate client property
#                                      (ConfigurationUtils.validateUrl)
#   extension_<name>                   JAAS options, read by
#                                      OAuthBearerSaslClientCallbackHandler
#
# So a credential-less `LoginModule required;` line -- the shape the other
# mechanisms' absence of options would suggest -- is rejected outright with
# "The OAuth configuration option clientId value must be non-null".
#
# Omitting the handler class is the quieter trap: the login module then falls
# back to the *unsecured* default, which mints a self-signed JWS that any broker
# with a real validator rejects, and the rejection names the token rather than
# the missing handler.
#
# The class moved: `...oauthbearer.secured.OAuthBearerLoginCallbackHandler` on
# Kafka < 3.6, promoted out of `secured` in 3.6, and the old name removed in
# 4.0. Overridable for that reason -- a hardcoded name breaks on one side or
# the other of that window.
OAUTHBEARER_LOGIN_CALLBACK_HANDLER = (
    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler"
)
ENV_OAUTH_LOGIN_CALLBACK_HANDLER = "KAFKA_SASL_OAUTHBEARER_LOGIN_CALLBACK_HANDLER"


def _jaas_config(
    mechanism: str,
    username: str,
    password: str,
    options: Optional[Dict[str, str]] = None,
) -> str:
    """Build the JAAS line Kafka's Java clients expect.

    Values are quoted because a password containing a space or '=' would
    otherwise truncate the entry and produce an authentication failure that
    looks like a wrong password.

    Backslashes are escaped as well as quotes. That is not belt-and-braces: the
    JVM parses this with a StreamTokenizer-backed grammar that consumes one
    level of backslash inside a quoted string, so an unescaped one is *eaten*
    rather than rejected. The first version of this function escaped only '"',
    and the corruption it left is silent -- measured against Kafka's own
    JaasContext (kafka-clients 3.7.0):

        C:\\Users\\svc  ->  C:Userssvc      (wrong password, no error)
        a\\nb           ->  a<newline>b     (wrong password, no error)
        tail\\          ->  parse error "JAAS config entry not terminated
                                            by semi-colon" -- which points at
                                            the syntax, not at the password

    The escaped-once form here is correct for the value we emit, because it
    goes into the connector config as JSON and reaches sasl.jaas.config
    directly. A value that additionally passes through a Java .properties file
    needs a SECOND pass (properties eats a level of backslash too) -- that is
    what the debezium connector's _write_properties_file does on the
    externalized path, and what the chart's kafka-init and Connect containers
    have to do in shell. See test_jaas_escaping_matches_the_jvm.
    """
    module = _JAAS_MODULES[mechanism]

    def esc(v: str) -> str:
        return v.replace("\\", "\\\\").replace('"', '\\"')

    parts = [module, "required"]
    if mechanism in _TOKEN_MECHANISMS:
        # No username/password pair here -- and deliberately no
        # `unsecuredLoginStringClaim_sub`, which is the JVM's *unsecured*
        # fallback and would mint a self-signed token. The credentials arrive
        # as options; see OAUTHBEARER_LOGIN_CALLBACK_HANDLER above for why.
        if not options or not options.get("clientId") or not options.get("clientSecret"):
            raise KafkaSecurityError(
                f"{mechanism} needs clientId and clientSecret as JAAS options; "
                "emitting the line without them yields a JVM client that fails "
                "at configure() with 'The OAuth configuration option clientId "
                "value must be non-null'"
            )
    else:
        parts.append(f'username="{esc(username)}"')
        parts.append(f'password="{esc(password)}"')

    # Sorted so the line is byte-stable: it is compared across two
    # implementations by the parity suite, and dict order is not a contract.
    for key in sorted(options or {}):
        parts.append(f'{key}="{esc(options[key])}"')

    return " ".join(parts) + ";"


def debezium_schema_history_security() -> Dict[str, str]:
    """Security properties for Debezium's schema-history Kafka client.

    Debezium's schema history is a SEPARATE Kafka client living inside the
    connector task — it does not inherit the Connect worker's credentials. On a
    cluster requiring SASL, a connector whose history client is unconfigured
    starts, snapshots, and then fails when it tries to write history, which
    reads as a CDC bug rather than a missing credential.

    Both the producer and the consumer need the properties: the consumer is what
    recovers history on connector restart.

    Returns {} for PLAINTEXT, so an existing deployment's config is unchanged.
    """
    base = kafka_security_kwargs()
    protocol = base["security_protocol"]
    if protocol == PROTOCOL_PLAINTEXT:
        return {}

    props: Dict[str, str] = {}
    for role in ("producer", "consumer"):
        prefix = f"schema.history.internal.{role}."
        props[prefix + "security.protocol"] = protocol
        if uses_sasl(protocol):
            mechanism = base["sasl_mechanism"]
            props[prefix + "sasl.mechanism"] = mechanism
            if mechanism in _TOKEN_MECHANISMS:
                # base has no sasl_plain_* under a token mechanism -- reading
                # them here is what used to raise KeyError, i.e. the history
                # client was unreachable on OAUTHBEARER rather than merely
                # misconfigured.
                endpoint, client_id, client_secret, scope, ext = _oauth_settings()
                opts = {"clientId": client_id, "clientSecret": client_secret}
                if scope:
                    opts["scope"] = scope
                for name, value in ext.items():
                    opts["extension_" + name] = value
                props[prefix + "sasl.jaas.config"] = _jaas_config(
                    mechanism, "", "", opts
                )
                props[prefix + "sasl.login.callback.handler.class"] = (
                    os.getenv(ENV_OAUTH_LOGIN_CALLBACK_HANDLER) or ""
                ).strip() or OAUTHBEARER_LOGIN_CALLBACK_HANDLER
                props[prefix + "sasl.oauthbearer.token.endpoint.url"] = endpoint
            else:
                props[prefix + "sasl.jaas.config"] = _jaas_config(
                    mechanism,
                    base["sasl_plain_username"],
                    base["sasl_plain_password"],
                )
        if uses_tls(protocol):
            # A JVM reads PEM directly since KIP-651 (Kafka 2.7), so the CA
            # bundle mounted for the Go and Python clients serves this task too
            # -- no conversion step, no second copy of the material.
            #
            # `type` is not optional decoration. Without it the JVM assumes JKS,
            # fails to parse the PEM and reports a keystore FORMAT error, which
            # reads as a corrupt file rather than a missing setting and sends
            # the operator off converting a file that never needed converting.
            if "ssl_cafile" in base:
                props[prefix + "ssl.truststore.type"] = "PEM"
                props[prefix + "ssl.truststore.location"] = base["ssl_cafile"]
            # mTLS. An explicit combined file wins; with only the two-path pair
            # the Connect image has built one at a fixed path, so point there.
            # Omitting it would leave the history client without a certificate
            # on a cluster that requires one -- the connector snapshots, then
            # fails its first history write with a handshake alert.
            keystore = (os.getenv(ENV_SSL_KEYSTORE_LOCATION) or "").strip()
            if not keystore and "ssl_certfile" in base:
                keystore = CONNECT_IMAGE_CLIENT_PEM
            if keystore:
                props[prefix + "ssl.keystore.type"] = "PEM"
                props[prefix + "ssl.keystore.location"] = keystore
            if base.get("ssl_check_hostname") is False:
                # As close as a JVM gets to skipping verification: the hostname
                # check goes, the chain is still validated. Deliberately not
                # equivalent to what this module does for its own clients, and
                # the chart refuses the combination where the difference bites
                # (skip-verify with no CA bundle).
                props[prefix + "ssl.endpoint.identification.algorithm"] = ""
    return props


def schema_registry_auth() -> Optional[Tuple[str, str]]:
    """Return ``(username, password)`` for Schema Registry, or ``None`` if unset.

    ``None`` means anonymous, which is correct for an unsecured registry and is
    what every existing deployment gets.

    Half a credential pair is an error rather than a downgrade to anonymous: a
    registry that requires auth answers an unauthenticated request with 401, and
    the serializer reports that as a schema-registration failure naming the
    subject. The operator then debugs the schema, not the typo in their secret.

    SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO ("user:pass") wins over the split pair,
    matching Confluent's own precedence.
    """
    combined = os.getenv(ENV_SR_BASIC_AUTH, "").strip()
    if combined:
        if ":" not in combined:
            raise KafkaSecurityError(
                f"{ENV_SR_BASIC_AUTH} must be 'username:password', got a value with no ':'"
            )
        user, _, password = combined.partition(":")
        if not user or not password:
            raise KafkaSecurityError(
                f"{ENV_SR_BASIC_AUTH} must be 'username:password'; one half is empty"
            )
        return user, password

    user = os.getenv(ENV_SR_USERNAME, "").strip()
    password = os.getenv(ENV_SR_PASSWORD, "")
    if not user and not password:
        return None
    if not user or not password:
        raise KafkaSecurityError(
            f"Schema Registry auth needs both {ENV_SR_USERNAME} and {ENV_SR_PASSWORD}; "
            "set both or neither"
        )
    return user, password
