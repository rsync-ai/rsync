#!/usr/bin/env python3
"""
Debezium MCP Connector (CDC Engine)

This MCP server provisions Debezium source connectors in Kafka Connect via REST API.

It is intentionally GENERIC across supported databases:
- mysql, postgresql, mongodb, sqlserver, oracle

MCP JSON-RPC:
  method: "tools/call"
  params.name: "debezium_<operation>"
  params.arguments: operation arguments
"""

import hashlib
import json
import logging
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

_LOG = logging.getLogger("debezium-mcp")


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) else default


def _jsonrpc_ok(rpc_id: int, result: Dict[str, Any]) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": rpc_id, "result": result}


def _jsonrpc_err(rpc_id: int, code: int, message: str, data: Any = None) -> Dict[str, Any]:
    err: Dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": rpc_id, "error": err}


def _as_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def _normalize_db_type(db_type: str) -> str:
    s = (db_type or "").strip().lower()
    s = s.replace("-", "_")
    aliases = {
        "postgres": "postgresql",
        "mssql": "sqlserver",
        "ms_sql": "sqlserver",
        "mariadb": "mysql",
    }
    return aliases.get(s, s)


def _safe_name(s: str, max_len: int = 120) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9_-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return "rsync"
    return s[:max_len]

# --- topic namespace -------------------------------------------------------
# Third copy of a rule that also lives in shared/go/kafkaclient/topics.go and
# llm-service/src/utils/kafka_topics.py. It is duplicated rather than imported
# because this file ships inside the connector image, which has neither of those
# on its path. The three MUST agree: this process configures what Debezium
# writes, and the Go sink subscribes to it. Change one, change all three.
_TOPIC_ILLEGAL = re.compile(r"[^a-zA-Z0-9._-]")


def _topic_prefix() -> str:
    raw = os.getenv("KAFKA_TOPIC_PREFIX")
    if raw is None:
        return "rsync."
    prefix = _TOPIC_ILLEGAL.sub("", raw.strip())
    if not prefix:
        return ""
    if prefix[-1] not in "._-":
        prefix += "."
    return prefix


def _qualify_topic(name: str) -> str:
    """Put a topic in the product's namespace. Idempotent."""
    name = (name or "").strip()
    prefix = _topic_prefix()
    if not prefix or not name or name.startswith(prefix):
        return name
    return prefix + name


def _parse_tables(args: Dict[str, Any]) -> List[str]:
    # Accept "tables" (list) or "table" (string)
    tables: List[str] = []
    if isinstance(args.get("tables"), list):
        for t in args["tables"]:
            if t is None:
                continue
            ts = str(t).strip()
            if ts:
                tables.append(ts)
    if not tables:
        t = args.get("table")
        if isinstance(t, str) and t.strip():
            tables = [t.strip()]
    return tables


def _split_qualified(name: str) -> Tuple[Optional[str], Optional[str], str]:
    """
    Split a qualified identifier.
      - "db.schema.table" -> ("db", "schema", "table")
      - "schema.table" -> (None, "schema", "table")
      - "table" -> (None, None, "table")
    """
    parts = [p for p in (name or "").split(".") if p]
    if len(parts) >= 3:
        return parts[0], parts[1], parts[-1]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, None, parts[0] if parts else ""


# Kafka Connect / Debezium config keys whose VALUES are secrets. Any key that
# contains one of these substrings (case-insensitive) is masked before the config
# is echoed back in a tool result — the raw config still POSTs to Kafka Connect,
# but the password must never ride up into the orchestrator response / logs.
_SENSITIVE_KEY_MARKERS = ("password", "passwd", "secret", "token", "credential", "sasl.jaas.config", "private.key")
_REDACTED = "***REDACTED***"

# A URI can carry credentials in its userinfo (e.g. mongodb://user:pass@host) — so
# the VALUE is a secret even when the config KEY name matches none of the markers
# above (the MongoDB source password lives in "mongodb.connection.string"). Mask the
# "user:password@" segment of any URI-shaped value while keeping host/db visible.
# quote_plus-encoded passwords never contain a literal :/@ so the class is safe.
_URI_CREDENTIALS_RE = re.compile(r"(://)[^/\s:@]+:[^/\s:@]+@")


def _mask_uri_credentials(value: Any) -> Any:
    if not isinstance(value, str) or "://" not in value:
        return value
    return _URI_CREDENTIALS_RE.sub(r"\1" + _REDACTED + "@", value)


def _redact_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Return a shallow copy of a connector config with secret-bearing values masked.

    Preserves every key (so operators can still see the connector shape for
    debugging) while replacing any password/secret/token value with a placeholder.
    Two layers: (1) full-value mask when the KEY name looks sensitive; (2) userinfo
    mask for URI VALUES (catches credentials the key-name allowlist would miss).
    """
    if not isinstance(cfg, dict):
        return {}
    redacted: Dict[str, Any] = {}
    for k, v in cfg.items():
        kl = str(k).lower()
        if any(m in kl for m in _SENSITIVE_KEY_MARKERS) and v not in (None, ""):
            redacted[k] = _REDACTED
        else:
            redacted[k] = _mask_uri_credentials(v)
    return redacted


def _scrub_secrets(text: str, *secrets: str) -> str:
    """Remove known secret literals from a free-text string (e.g. an upstream error
    body) before it is returned to the caller. Best-effort — only masks values we
    actually hold, so a leak can't ride out on an echoed Kafka Connect error."""
    if not text:
        return text
    out = text
    for s in secrets:
        s = str(s or "")
        if len(s) >= 3:  # avoid masking trivially short / empty values
            out = out.replace(s, _REDACTED)
    return out


# ── Secret externalization via Kafka Connect FileConfigProvider ──────────────
# When DEBEZIUM_SECRETS_DIR is set (to a volume shared with the kafka-connect
# worker), we move secret VALUES out of the connector config into a per-connector
# .properties file and replace them with ${file:<path>:<key>} references. Kafka
# Connect resolves those at task start via its FileConfigProvider, so the plaintext
# password NEVER enters the Kafka config topic nor the GET /connectors/<name>/config
# REST response. When the env is unset (OSS/self-host, or pre-deploy) the config is
# left inline — identical to the previous behavior — so this is fully backward
# compatible. See docs: config.providers=file on the worker.

def _secrets_dir() -> str:
    return os.getenv("DEBEZIUM_SECRETS_DIR", "").strip()


def _secret_file_path(secrets_dir: str, connector_name: str) -> str:
    # One properties file per connector; name is sanitized to a filesystem-safe token.
    return os.path.join(secrets_dir, _safe_name(connector_name, 200) + ".properties")


def _is_secret_kv(key: str, value: Any) -> bool:
    """A config entry is a secret if its KEY name looks sensitive OR its VALUE is a
    URI carrying credentials in the userinfo (MongoDB's mongodb.connection.string)."""
    if not isinstance(value, str) or not value:
        return False
    kl = str(key).lower()
    if any(m in kl for m in _SENSITIVE_KEY_MARKERS):
        return True
    return "://" in value and bool(_URI_CREDENTIALS_RE.search(value))


def _write_secret_properties(path: str, kv: Dict[str, str]) -> None:
    """Write a Java .properties file. Values are single-line; backslashes escaped.
    0644 so the kafka-connect worker (uid 1001) can read what debezium-mcp (root)
    writes on the shared volume. Atomic replace so a partial file is never read."""
    lines = []
    for k, v in kv.items():
        sval = str(v).replace("\\", "\\\\").replace("\n", " ").replace("\r", " ")
        lines.append(f"{k}={sval}")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    try:
        os.chmod(tmp, 0o644)
    except OSError:
        pass
    os.replace(tmp, path)


class SecretExternalizationError(RuntimeError):
    """Externalization was requested (DEBEZIUM_SECRETS_DIR is set) but could not be
    completed. Raised instead of silently falling back, because the fallback writes the
    database password into the Kafka Connect config topic -- a durable, replicated,
    plaintext copy of a customer credential. Carries no config values, only the reason."""


def _secrets_enforced() -> bool:
    """Default ON. Setting DEBEZIUM_SECRETS_DIR is a statement that the password must
    not reach the config topic; honouring that only when it happens to be convenient
    makes the guarantee unreliable in exactly the case it exists for.

    DEBEZIUM_SECRETS_ENFORCED=false restores the old inline fallback for operators who
    would rather keep CDC provisioning up than keep the password out of Kafka. That is
    a legitimate availability trade -- it just has to be chosen, not defaulted into."""
    return os.getenv("DEBEZIUM_SECRETS_ENFORCED", "true").strip().lower() not in (
        "0", "false", "no", "off",
    )


def externalize_secrets(connector_name: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Move secret values in cfg into a shared-volume .properties file and replace
    them with ${file:...} references.

    Returns cfg UNCHANGED when the feature is off (DEBEZIUM_SECRETS_DIR unset) or the
    config holds no secrets -- neither is a failure, and inline is the documented
    behaviour for self-host. But when externalization was asked for and could not be
    done, raise SecretExternalizationError rather than quietly writing the password
    into the Kafka config topic; see _secrets_enforced for the opt-out."""
    secrets_dir = _secrets_dir()
    if not secrets_dir:
        return cfg
    secret_keys = [k for k, v in cfg.items() if _is_secret_kv(k, v)]
    if not secret_keys:
        return cfg
    try:
        os.makedirs(secrets_dir, exist_ok=True)
        path = _secret_file_path(secrets_dir, connector_name)
        _write_secret_properties(path, {k: cfg[k] for k in secret_keys})
        out = dict(cfg)
        for k in secret_keys:
            out[k] = "${file:" + path + ":" + k + "}"
        _LOG.info("externalized %d secret(s) for %s via FileConfigProvider", len(secret_keys), connector_name)
        return out
    except Exception as e:  # noqa: BLE001 — classified below; never leaks a config value
        if _secrets_enforced():
            _LOG.error(
                "secret externalization failed for %s (%s); REFUSING to fall back to "
                "inline config. Check the DEBEZIUM_SECRETS_DIR mount + volume "
                "permissions, or set DEBEZIUM_SECRETS_ENFORCED=false to accept the "
                "password being stored in the Kafka Connect config topic.",
                connector_name, e,
            )
            raise SecretExternalizationError(
                f"could not externalize {len(secret_keys)} secret(s) for {connector_name}: {e}"
            ) from e
        _LOG.warning(
            "secret externalization failed for %s (%s); falling back to inline config "
            "because DEBEZIUM_SECRETS_ENFORCED=false (password will be stored in the "
            "Kafka Connect config topic).",
            connector_name, e,
        )
        return cfg


def cleanup_secret_file(connector_name: str) -> None:
    """Best-effort removal of a connector's externalized secret file on delete."""
    secrets_dir = _secrets_dir()
    if not secrets_dir:
        return
    try:
        p = _secret_file_path(secrets_dir, connector_name)
        if os.path.exists(p):
            os.remove(p)
    except Exception as e:  # noqa: BLE001
        _LOG.warning("could not remove secret file for %s: %s", connector_name, e)


_JAAS_MODULES = {
    "PLAIN": "org.apache.kafka.common.security.plain.PlainLoginModule",
    "SCRAM-SHA-256": "org.apache.kafka.common.security.scram.ScramLoginModule",
    "SCRAM-SHA-512": "org.apache.kafka.common.security.scram.ScramLoginModule",
    "OAUTHBEARER": (
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"
    ),
}

# Mechanisms whose credential is a fetched token rather than a username/password
# pair. Kept as a set so the checks below read the same way as the llm-service
# copy this file is held in lockstep with.
_TOKEN_MECHANISMS = {"OAUTHBEARER"}

# See llm-service/src/utils/kafka_security.py for the measurement behind this:
# clientId/clientSecret/scope are JAAS *options*, the token endpoint is a
# separate property, and a credential-less `required;` line is rejected with
# "The OAuth configuration option clientId value must be non-null".
OAUTHBEARER_LOGIN_CALLBACK_HANDLER = (
    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler"
)


def _parse_sasl_extensions(raw: Optional[str]) -> Dict[str, str]:
    """Parse "k=v,k2=v2" SASL extensions, rejecting anything malformed.

    Mirrors llm-service's parse_sasl_extensions and the Go module's
    ParseSASLExtensions. Validation matters because the wire format joins pairs
    with 0x01 and inserts '=': an unchecked key or value does not raise, it
    silently produces a different, well-formed extension set.
    """
    out: Dict[str, str] = {}
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise ValueError(
                f"malformed SASL extension {part!r}: expected name=value"
            )
        name, value = part.split("=", 1)
        name, value = name.strip(), value.strip()
        if not name:
            raise ValueError(f"SASL extension {part!r} has an empty name")
        if name == "auth":
            raise ValueError(
                "SASL extension name 'auth' is reserved by the OAUTHBEARER "
                "mechanism and cannot be set"
            )
        if "\x01" in name or "\x01" in value:
            raise ValueError(
                f"SASL extension {name!r} contains the 0x01 separator byte"
            )
        out[name] = value
    return out


def _oauth_settings() -> Tuple[str, str, str, str, Dict[str, str]]:
    """Resolve OIDC client-credentials settings for the history client."""
    endpoint = _env("KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT")
    if not endpoint:
        raise ValueError(
            "KAFKA_SASL_MECHANISM=OAUTHBEARER requires "
            "KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT"
        )
    client_id = _env("KAFKA_SASL_OAUTHBEARER_CLIENT_ID") or _env("KAFKA_SASL_USERNAME")
    client_secret = os.getenv("KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET") or os.getenv(
        "KAFKA_SASL_PASSWORD", ""
    )
    if not client_id or not client_secret:
        raise ValueError(
            "KAFKA_SASL_MECHANISM=OAUTHBEARER requires a client id and secret: "
            "set KAFKA_SASL_OAUTHBEARER_CLIENT_ID/"
            "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET, or "
            "KAFKA_SASL_USERNAME/KAFKA_SASL_PASSWORD"
        )
    scope = _env("KAFKA_SASL_OAUTHBEARER_SCOPE")
    extensions = _parse_sasl_extensions(os.getenv("KAFKA_SASL_OAUTHBEARER_EXTENSIONS"))
    return endpoint, client_id, client_secret, scope, extensions


def _jaas_config(
    mechanism: str,
    username: str,
    password: str,
    options: Optional[Dict[str, str]] = None,
) -> str:
    """Build a JAAS entry. Username and password are QUOTED and escaped.

    An unquoted value truncates at the first whitespace or '=', and the broker
    then rejects the credential — an auth failure indistinguishable from a wrong
    password, which is the expensive kind of bug to chase.

    Held byte-identical to llm-service/src/utils/kafka_security.py::_jaas_config
    by tests/test_debezium_schema_history_parity.py — change both or neither.
    """
    module = _JAAS_MODULES[mechanism]

    def esc(v: str) -> str:
        return v.replace("\\", "\\\\").replace('"', '\\"')

    parts = [module, "required"]
    if mechanism in _TOKEN_MECHANISMS:
        if not options or not options.get("clientId") or not options.get("clientSecret"):
            raise ValueError(
                f"{mechanism} needs clientId and clientSecret as JAAS options; "
                "emitting the line without them yields a JVM client that fails "
                "at configure() with 'The OAuth configuration option clientId "
                "value must be non-null'"
            )
    else:
        parts.append(f'username="{esc(username)}"')
        parts.append(f'password="{esc(password)}"')

    for key in sorted(options or {}):
        parts.append(f'{key}="{esc(options[key])}"')

    return " ".join(parts) + ";"


def _schema_history_security() -> Dict[str, str]:
    """Kafka security properties for Debezium's schema-history client.

    Debezium's schema history is a SEPARATE Kafka client living inside the
    connector task — it does not inherit the Connect worker's credentials. On a
    SASL cluster, a connector whose history client is unconfigured starts, runs,
    and only fails on restart, when the consumer half replays history. Both the
    producer and the consumer need the properties for that reason.

    Mirrors llm-service/src/utils/kafka_security.py (same env vars, same
    defaults); the two must be changed together. Returns {} for PLAINTEXT, so an
    existing deployment's config is byte-identical to before.
    """
    protocol = (_env("KAFKA_SECURITY_PROTOCOL") or "PLAINTEXT").upper()
    if protocol == "PLAINTEXT":
        return {}
    if protocol not in ("SSL", "SASL_PLAINTEXT", "SASL_SSL"):
        raise ValueError(f"unsupported KAFKA_SECURITY_PROTOCOL {protocol!r}")

    props: Dict[str, str] = {}
    uses_sasl = protocol.startswith("SASL_")
    if uses_sasl:
        mechanism = (_env("KAFKA_SASL_MECHANISM") or "PLAIN").upper()
        if mechanism not in _JAAS_MODULES:
            raise ValueError(
                f"unsupported KAFKA_SASL_MECHANISM {mechanism!r} for Debezium schema history"
            )
        oauth_props: Dict[str, str] = {}
        if mechanism in _TOKEN_MECHANISMS:
            endpoint, client_id, client_secret, scope, ext = _oauth_settings()
            opts = {"clientId": client_id, "clientSecret": client_secret}
            if scope:
                opts["scope"] = scope
            for name, value in ext.items():
                opts["extension_" + name] = value
            jaas = _jaas_config(mechanism, "", "", opts)
            oauth_props["sasl.login.callback.handler.class"] = (
                _env("KAFKA_SASL_OAUTHBEARER_LOGIN_CALLBACK_HANDLER")
                or OAUTHBEARER_LOGIN_CALLBACK_HANDLER
            )
            oauth_props["sasl.oauthbearer.token.endpoint.url"] = endpoint
        else:
            username = _env("KAFKA_SASL_USERNAME")
            password = os.getenv("KAFKA_SASL_PASSWORD", "")
            if not username or not password:
                raise ValueError(
                    f"{protocol} requires both KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD"
                )
            jaas = _jaas_config(mechanism, username, password)

    ca = _env("KAFKA_SSL_CA_LOCATION")
    keystore = _env("KAFKA_SSL_KEYSTORE_LOCATION")
    skip_verify = _env("KAFKA_SSL_SKIP_VERIFY").lower() in ("1", "true", "yes", "on")
    for role in ("producer", "consumer"):
        prefix = f"schema.history.internal.{role}."
        props[prefix + "security.protocol"] = protocol
        if uses_sasl:
            props[prefix + "sasl.mechanism"] = mechanism
            props[prefix + "sasl.jaas.config"] = jaas
            for key, value in oauth_props.items():
                props[prefix + key] = value
        if protocol.endswith("SSL"):
            # A JVM reads PEM directly since KIP-651 (Kafka 2.7), so the same CA
            # bundle mounted for the Go and Python clients serves this task too.
            # The earlier comment here said Connect wanted a JKS and that a
            # private CA "shows up as a conversion step" -- it does not. It shows
            # up as `ssl.truststore.location` pointing at a PEM with no `type`,
            # which makes the JVM assume JKS, fail to parse it, and report a
            # keystore FORMAT error: an operator reading that goes off converting
            # a file that never needed converting.
            if ca:
                props[prefix + "ssl.truststore.type"] = "PEM"
                props[prefix + "ssl.truststore.location"] = ca
            # mTLS. A JVM PEM keystore is ONE file holding the chain and the key,
            # so KAFKA_SSL_CERT_LOCATION/KAFKA_SSL_KEY_LOCATION -- the two paths
            # the Go and Python clients read -- cannot be used here.
            if keystore:
                props[prefix + "ssl.keystore.type"] = "PEM"
                props[prefix + "ssl.keystore.location"] = keystore
            if skip_verify:
                # As close as a JVM gets to skipping verification: the hostname
                # check goes, the chain is still validated.
                props[prefix + "ssl.endpoint.identification.algorithm"] = ""
    return props


class DebeziumConnector:
    def __init__(self):
        self.kafka_connect_url = _env("KAFKA_CONNECT_URL", "http://kafka-connect:8083")
        # KAFKA_BROKERS first, matching every other Kafka client in the product
        # (the Go services, the sink worker, the llm-service agents). Reading only
        # KAFKA_BOOTSTRAP_SERVERS meant a deployment that sets KAFKA_BROKERS — the
        # documented variable — pointed Debezium's schema history at the wrong
        # cluster, or at a default that does not exist outside compose.
        self.kafka_bootstrap_servers = (
            _env("KAFKA_BROKERS") or _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        )

        # JSON converter defaults (GLOBAL).
        #
        # Pre-live decision: enable schemas globally so the CDC envelope includes
        # per-field type metadata for type-fidelity DDL in relational sinks.
        #
        # Message shape becomes:
        #   {"schema": {...}, "payload": {"op": "...", "before": {...}, "after": {...}, ...}}
        self.json_converters = {
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "true",
            "value.converter.schemas.enable": "true",
        }

        # Supported DB -> Debezium connector class
        self.supported = {
            "mysql": "io.debezium.connector.mysql.MySqlConnector",
            "postgresql": "io.debezium.connector.postgresql.PostgresConnector",
            "mongodb": "io.debezium.connector.mongodb.MongoDbConnector",
            "sqlserver": "io.debezium.connector.sqlserver.SqlServerConnector",
            "oracle": "io.debezium.connector.oracle.OracleConnector",
        }

    def _client(self) -> httpx.Client:
        return httpx.Client(timeout=httpx.Timeout(20.0, connect=10.0))

    def _connect_url_from_args(self, args: Dict[str, Any]) -> str:
        # Allow override via params, but keep env default for container usage.
        u = args.get("kafka_connect_url") or args.get("connect_url") or args.get("kafkaConnectUrl")
        if isinstance(u, str) and u.strip():
            return u.strip().rstrip("/")
        return self.kafka_connect_url.rstrip("/")

    def _bootstrap_from_args(self, args: Dict[str, Any]) -> str:
        bs = args.get("kafka_bootstrap_servers") or args.get("bootstrap_servers") or args.get("kafkaBootstrapServers")
        if isinstance(bs, str) and bs.strip():
            return bs.strip()
        return self.kafka_bootstrap_servers

    def _stable_server_id(self, connector_name: str) -> str:
        """Deterministic, process-independent MySQL ``database.server.id``.

        MySQL requires every replication client (binlog reader) to have a
        UNIQUE server id; two clients sharing one id makes MySQL silently kill
        / stall the colliding stream (connector shows RUNNING but never emits a
        topic or rows). The previous ``abs(hash(connector_name))`` used CPython's
        str hash, which is RANDOMIZED per process (no PYTHONHASHSEED pin) — so a
        connector got a DIFFERENT id every container restart / 409-update, and
        with orphaned connectors accumulating the id space collided. Use a
        stable SHA-1 of the connector name instead: same name → same id, always,
        across processes and restarts. Range 1..4.29e9 (MySQL server_id is a
        32-bit unsigned int; 0 is reserved).
        """
        h = hashlib.sha1((connector_name or "").encode("utf-8")).hexdigest()
        return str((int(h[:8], 16) % 4_294_967_000) + 1)

    def _map_snapshot_mode(self, snapshot_mode: str, db_type: str = "") -> str:
        s = (snapshot_mode or "").strip().lower()
        if s in ("", "initial"):
            return "initial"
        if s in ("streaming_only", "streaming-only", "no_snapshot", "nosnapshot", "never"):
            # "Changes only / no historical backfill": capture the table SCHEMA but copy
            # ZERO existing rows, then stream. In Debezium 3.x that is snapshot.mode="no_data"
            # for EVERY connector family (MySQL, PostgreSQL, SQLServer, Oracle, ...).
            #
            # We must NOT return "never" for MySQL here. On a FRESH connector (no schema
            # history topic, no stored offset) MySQL's "never" fails with "The db history
            # topic is missing" (see backend-orchestrator/.../executor/hybrid_cdc.go), and a
            # caller that then forces "initial" to recover would snapshot the WHOLE table —
            # exactly the "changes only" contract violation this maps away from.
            # "no_data" builds schema history from the live DB without snapshotting any data
            # and then streams; on a restart with a committed offset Debezium simply resumes
            # (snapshot phase is skipped), so it is safe across reruns too.
            return "no_data"
        if s in ("initial_only", "initial-only"):
            return "initial_only"
        # Hybrid batch+CDC handoff (position-anchored): the orchestrator seeds the
        # connector's offset (PG via the pre-created replication slot; MySQL via a
        # record in the connect-offsets topic) so Debezium must REBUILD schema history
        # from the live DB and RESUME streaming from that committed offset — never
        # re-snapshot data. Debezium 3.x calls this "recovery"; accept the friendlier
        # aliases the executor may send.
        if s in ("schema_recovery", "schema-recovery", "schema_only_recovery", "recovery"):
            if db_type in ("postgresql", "postgres"):
                # PostgreSQL has no schema-history topic; resuming from the slot needs
                # no schema recovery — no_data streams from the slot's confirmed_flush_lsn.
                return "no_data"
            return "recovery"
        return s

    def _build_config(self, args: Dict[str, Any]) -> Tuple[str, Dict[str, Any], str]:
        db_type = _normalize_db_type(str(args.get("database_type") or args.get("source_type") or ""))
        if not db_type:
            raise ValueError("database_type is required")
        if db_type not in self.supported:
            raise ValueError(f"unsupported database_type: {db_type}")

        connector_name = str(args.get("connector_name") or "").strip()
        if not connector_name:
            connector_name = f"cdc-{db_type}-{int(time.time())}"

        expected_class = self.supported[db_type]
        requested_class = str(args.get("connector_class") or "").strip()
        connector_class = requested_class or expected_class
        # Guardrail: ignore mismatched connector_class. This prevents callers from accidentally
        # passing e.g. PostgresConnector for MySQL and getting a hard-to-debug Kafka Connect 400.
        if requested_class and requested_class != expected_class:
            connector_class = expected_class

        db_host = str(args.get("db_host") or args.get("host") or "").strip()
        db_user = str(args.get("db_user") or args.get("user") or args.get("username") or "").strip()
        db_password = str(args.get("db_password") or args.get("password") or "").strip()
        db_name = str(args.get("db_name") or args.get("database") or "").strip()
        db_port = _as_int(args.get("db_port") or args.get("port"))

        # A MongoDB caller may supply a full connection URI (required for Atlas's
        # mongodb+srv:// form, which carries host + credentials itself) instead of
        # discrete host/user fields — don't force those in that case.
        mongo_uri_override = db_type == "mongodb" and bool(
            str(
                args.get("connection_string")
                or args.get("mongodb_connection_string")
                or args.get("mongodb_uri")
                or args.get("uri")
                or ""
            ).strip()
        )

        if not db_host and not mongo_uri_override:
            raise ValueError("db_host is required")
        # MongoDB may be an UNAUTHENTICATED deployment (SCRAM is optional), so a
        # missing user is legitimate — the mongo branch below only injects
        # credentials `if db_user`. Relational engines (mysql/postgres/sqlserver/
        # oracle) still require a user.
        if not db_user and not mongo_uri_override and db_type != "mongodb":
            raise ValueError("db_user is required")

        tables = _parse_tables(args)
        if not tables:
            raise ValueError("table or tables is required")

        # Choose include lists from tables + optional db_name overrides.
        # We keep this permissive because different DBs have different qualifiers.
        include_list = ",".join(tables)
        first_db, first_schema, first_table = _split_qualified(tables[0])

        snapshot_mode = self._map_snapshot_mode(str(args.get("snapshot_mode") or args.get("cdc_mode") or "initial"), db_type)

        kafka_bootstrap = self._bootstrap_from_args(args)
        topic_prefix = _qualify_topic(str(args.get("topic_prefix") or connector_name).strip() or connector_name)

        # Schema-history topic name.
        #
        # The orchestrator PRE-CREATES this topic before calling start_sync (see
        # executor.go schemaHistoryTopicFor), because nothing else does: this image has
        # no Kafka client, no topic.creation.* policy is set on the connector, and the
        # topic's retention/cleanup policy is a Debezium correctness requirement rather
        # than a default worth inheriting from the broker. It passes the name it created
        # in schema_history_topic so the two sides cannot drift — two independent copies
        # of _safe_name that disagree would have the orchestrator create one topic and
        # Connect write to another, and the connector would work until its first restart.
        #
        # The fallback is byte-identical to what this line computed before, so a direct
        # caller (a test, an operator hitting the MCP by hand, an older orchestrator)
        # keeps working unchanged. _qualify_topic is idempotent, so an explicit value
        # that already carries the prefix is not prefixed twice.
        schema_history_topic = _qualify_topic(
            str(args.get("schema_history_topic") or "").strip()
            or f"schemahistory.{_safe_name(connector_name, 80)}"
        )

        # Base config shared across DBs
        cfg: Dict[str, Any] = {
            "connector.class": connector_class,
            "tasks.max": str(args.get("tasks_max") or args.get("tasks.max") or "1"),
            "topic.prefix": topic_prefix,
            # Needed for schema evolution visibility (GA-min: auto-add columns, halt on others)
            "include.schema.changes": "true",
            "snapshot.mode": snapshot_mode,
            # schema history (safe default for relational connectors)
            "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap,
            "schema.history.internal.kafka.topic": schema_history_topic,
        }
        # Applied before the caller's overrides so an explicit override still wins.
        cfg.update(_schema_history_security())
        cfg.update(self.json_converters)

        # Allow explicitly passing any Debezium/Kafka Connect property overrides
        overrides = args.get("connector_config_overrides") or args.get("config_overrides") or {}
        if overrides and isinstance(overrides, dict):
            for k, v in overrides.items():
                cfg[str(k)] = v

        # DB-specific configuration
        if db_type == "mysql":
            # MySQL Debezium requires table.include.list in "database.table" format.
            # If the caller passes unqualified names (e.g. "big_table") we qualify
            # them automatically using db_name (or first_db from a qualified entry).
            # This mirrors the PostgreSQL qualification logic below.
            effective_mysql_db = db_name or first_db or ""
            if all("." not in t for t in tables) and tables and effective_mysql_db:
                mysql_include_list = ",".join([f"{effective_mysql_db}.{t}" for t in tables])
            else:
                mysql_include_list = include_list
            # Map the connection's sslmode (PG-style vocabulary) to MySQL Debezium's
            # database.ssl.mode. Hardcoding "disabled" breaks any SSL-required server
            # (e.g. Azure MySQL sets require_secure_transport=ON →
            # "Connections using insecure transport are prohibited"). Default to
            # "preferred": use TLS when the server supports it, fall back otherwise —
            # so this works for both managed (SSL-required) and local MySQL. Mirrors
            # the PostgreSQL sslmode passthrough below.
            mysql_ssl_map = {
                "disable": "disabled", "disabled": "disabled",
                "allow": "preferred", "prefer": "preferred", "preferred": "preferred", "": "preferred",
                "require": "required", "required": "required",
                "verify-ca": "verify_ca", "verify_ca": "verify_ca",
                "verify-full": "verify_identity", "verify-identity": "verify_identity",
                "verify_identity": "verify_identity",
            }
            mysql_sslmode_raw = str(args.get("sslmode") or args.get("db_sslmode") or "").strip().lower()
            mysql_ssl_mode = mysql_ssl_map.get(mysql_sslmode_raw, "preferred")
            cfg.update(
                {
                    "database.hostname": db_host,
                    "database.port": str(db_port or 3306),
                    "database.user": db_user,
                    "database.password": db_password,
                    "database.server.id": self._stable_server_id(connector_name),
                    "database.allowPublicKeyRetrieval": "true",
                    "database.ssl.mode": mysql_ssl_mode,
                    "database.include.list": effective_mysql_db,
                    "table.include.list": mysql_include_list,
                }
            )
            # Optional signaling table for incremental snapshots
            signal = args.get("signal_data_collection") or args.get("signal.table") or args.get("signal_data_collection")
            if isinstance(signal, str) and signal.strip():
                cfg["signal.data.collection"] = signal.strip()
                cfg["signal.enabled.channels"] = "source"

        elif db_type == "postgresql":
            schema = str(args.get("schema") or first_schema or "public").strip() or "public"
            cfg.update(
                {
                    "database.hostname": db_host,
                    "database.port": str(db_port or 5432),
                    "database.user": db_user,
                    "database.password": db_password,
                    "database.dbname": db_name or first_db or "",
                    "table.include.list": include_list,
                    "plugin.name": str(args.get("plugin_name") or "pgoutput"),
                    "slot.name": _safe_name(args.get("slot_name") or f"rsync_{connector_name}", 60),
                    # "disabled", never "filtered"/"all": the orchestrator creates the
                    # publication BEFORE the replication slot, and Debezium creating it
                    # itself reverses that order — pgoutput then decodes the first WAL
                    # batch without knowing which tables are included and the rows are
                    # gone with no error anywhere (CDC-02). The caller may still pass
                    # publication_autocreate_mode explicitly; the default is the safe
                    # one, so a caller that forgets fails loudly (publication does not
                    # exist) instead of losing rows quietly.
                    "publication.autocreate.mode": str(args.get("publication_autocreate_mode") or "disabled"),
                    "publication.name": _safe_name(args.get("publication_name") or f"rsync_{connector_name}", 60),
                    # Do NOT drop the replication slot when the connector stops. The slot is
                    # the durable position anchor: for the hybrid batch+CDC handoff it pins WAL
                    # at the consistent_point captured before the batch load, and in general it
                    # lets the connector resume after a restart without re-snapshotting. The slot
                    # is removed deliberately by the orchestrator's CDC cleanup (postgresql.go),
                    # not by connector lifecycle. Caller may override.
                    "slot.drop.on.stop": str(args.get("slot_drop_on_stop") or "false"),
                }
            )
            sslmode = str(args.get("sslmode") or args.get("db_sslmode") or "").strip()
            if sslmode:
                cfg["database.sslmode"] = sslmode
            # If user didn't qualify schema in tables, keep it explicit in include list.
            if all("." not in t for t in tables) and tables:
                cfg["table.include.list"] = ",".join([f"{schema}.{t}" for t in tables])

        elif db_type == "mongodb":
            # Debezium 2.x+/3.x MongoDB uses a single mongodb.connection.string (URI);
            # the legacy mongodb.hosts / mongodb.name keys were REMOVED and fail
            # validation on a modern image. topic.prefix (set in the base config)
            # now serves as the logical name. capture.mode=change_streams_update_full
            # makes every update carry a complete post-image, so the sink's packed
            # upsert (_id + document) is always correct without updateDescription
            # delta reconstruction.
            from urllib.parse import quote_plus

            mongo_db = db_name or first_db or ""

            # An explicit URI wins — required for Atlas (mongodb+srv://...) and any
            # deployment whose topology the caller already knows.
            mongo_uri = str(
                args.get("connection_string")
                or args.get("mongodb_connection_string")
                or args.get("mongodb_uri")
                or args.get("uri")
                or ""
            ).strip()
            if not mongo_uri:
                creds = ""
                if db_user:
                    creds = quote_plus(db_user)
                    if db_password:
                        creds += ":" + quote_plus(db_password)
                    creds += "@"
                opts: List[str] = []
                # Atlas and any TLS-required deployment need tls=true; honor an
                # explicit sslmode and default-on when the host looks like Atlas.
                mo_ssl = str(args.get("sslmode") or args.get("db_sslmode") or args.get("ssl") or "").strip().lower()
                want_tls = (
                    mo_ssl in ("require", "required", "true", "on", "prefer", "preferred",
                               "verify-ca", "verify_ca", "verify-full", "verify-identity", "verify_identity")
                    or "mongodb.net" in db_host.lower()
                )
                if want_tls:
                    opts.append("tls=true")
                if db_user:
                    opts.append("authSource=" + quote_plus(str(args.get("auth_source") or args.get("authSource") or "admin")))
                rs = str(args.get("replica_set") or args.get("replicaSet") or "").strip()
                if rs:
                    opts.append("replicaSet=" + quote_plus(rs))
                query = ("?" + "&".join(opts)) if opts else ""
                mongo_uri = f"mongodb://{creds}{db_host}:{db_port or 27017}/{query}"

            # collection.include.list expects db.collection; qualify bare names.
            if all("." not in t for t in tables) and tables and mongo_db:
                mongo_include = ",".join([f"{mongo_db}.{t}" for t in tables])
            else:
                mongo_include = include_list

            cfg.update(
                {
                    "mongodb.connection.string": mongo_uri,
                    "capture.mode": str(args.get("capture_mode") or "change_streams_update_full"),
                    "collection.include.list": mongo_include,
                }
            )
            if mongo_db:
                cfg["database.include.list"] = mongo_db
            # MongoDB has no relational schema history or DDL change stream; these
            # relational-only base keys make the MongoDB connector fail validation.
            # Prefix match, not a fixed list: the security properties above add
            # more schema.history.* keys, and any relational-only key left on a
            # MongoDB connector fails Connect's config validation.
            for _k in [k for k in cfg if k.startswith("schema.history.")] + ["include.schema.changes"]:
                cfg.pop(_k, None)

        elif db_type == "sqlserver":
            # SQL Server Debezium requires database.names (plural). The JDBC driver
            # needs encryption settings for Azure SQL (which mandates encryption);
            # honor an explicit sslmode/encrypt, otherwise default to encrypt=true.
            # Azure SQL presents a publicly-trusted cert (verify it → trust=false);
            # box/MI SQL Server commonly uses a self-signed cert (trust=true).
            ss_raw = str(args.get("sslmode") or args.get("db_sslmode") or args.get("encrypt") or "").strip().lower()
            ss_host = str(db_host or "").strip().lower()
            if ss_raw in ("disable", "disabled", "false", "off", "none"):
                ss_encrypt, ss_trust = "false", "false"
            elif ss_host.endswith(".database.windows.net"):
                ss_encrypt, ss_trust = "true", "false"
            else:
                ss_encrypt, ss_trust = "true", "true"
            # table.include.list is schema-qualified (schema.table); qualify
            # unqualified names with the source schema (default dbo), mirroring PG.
            ss_schema = str(args.get("schema") or first_schema or "dbo").strip() or "dbo"
            if tables and all("." not in t for t in tables):
                ss_include_list = ",".join([f"{ss_schema}.{t}" for t in tables])
            else:
                ss_include_list = include_list
            cfg.update(
                {
                    "database.hostname": db_host,
                    "database.port": str(db_port or 1433),
                    "database.user": db_user,
                    "database.password": db_password,
                    "database.names": db_name or first_db or "",
                    "table.include.list": ss_include_list,
                    # driver.* props pass through to the mssql JDBC driver
                    # (Debezium strips the prefix). database.encrypt is NOT a
                    # recognized SqlServerConnector property — it must be driver.*.
                    "driver.encrypt": ss_encrypt,
                    "driver.trustServerCertificate": ss_trust,
                    # repeatable_read (Debezium default) holds range locks during the
                    # snapshot; read_committed is the low-impact default. Override via
                    # snapshot_isolation_mode (use "snapshot" only when the DB has
                    # ALLOW_SNAPSHOT_ISOLATION ON).
                    "snapshot.isolation.mode": str(args.get("snapshot_isolation_mode") or "read_committed"),
                }
            )

        elif db_type == "oracle":
            # Oracle LogMiner CDC (io.debezium.connector.oracle.OracleConnector).
            # database.dbname is the CDB / service name; a multitenant PDB is set
            # via database.pdb.name. table.include.list is <schema>.<table> where
            # the schema is the OWNER — Oracle folds unquoted identifiers to
            # UPPERCASE, so qualify unqualified names with the source owner and
            # upper-case them (mirrors the SQL Server schema-qualification), or the
            # regex never matches a conventionally-uppercase table and streaming
            # captures nothing. Overrides via connector_config_overrides.
            ora_owner = str(
                args.get("schema") or first_schema or (db_user or "")
            ).strip().upper()
            if tables and ora_owner and all("." not in t for t in tables):
                ora_include_list = ",".join([f"{ora_owner}.{str(t).upper()}" for t in tables])
            else:
                ora_include_list = include_list
            cfg.update(
                {
                    "database.hostname": db_host,
                    "database.port": str(db_port or 1521),
                    "database.user": db_user,
                    "database.password": db_password,
                    # Required by Debezium; in many setups this is the service name.
                    "database.dbname": db_name or first_db or "",
                    "table.include.list": ora_include_list,
                    "database.connection.adapter": str(args.get("oracle_connection_adapter") or "logminer"),
                    # online_catalog = low-overhead LogMiner dictionary (fast, but
                    # canNOT capture DDL / schema changes mid-stream). Override to
                    # redo_log_catalog for schema-evolution parity with PG/SQL Server
                    # (needs adequately-sized redo). Deliberate v1 default: online.
                    "log.mining.strategy": str(args.get("oracle_log_mining_strategy") or "online_catalog"),
                }
            )
            pdb = args.get("oracle_pdb_name") or args.get("pdb_name")
            if isinstance(pdb, str) and pdb.strip():
                cfg["database.pdb.name"] = pdb.strip()

        # ── Incremental (chunked) snapshot strategy ──────────────────────────────
        # When snapshot_strategy=incremental, SKIP Debezium's blocking initial snapshot
        # (snapshot.mode=no_data → build schema history only and start streaming at once)
        # and backfill history via a resumable, lock-free INCREMENTAL snapshot triggered
        # out-of-band over the Kafka signal channel. read.only=true keeps the snapshot's
        # low/high watermarks in the DB transaction log (PG LSN / MySQL GTID), so NOTHING
        # is written to the customer's source database (no signal table). Only PostgreSQL
        # and MySQL are wired; any other engine keeps its blocking snapshot.
        #
        # STAGING-VALIDATE: PostgreSQL read-only incremental support is version-sensitive.
        # Kafka Connect ignores an unknown property, so a mismatch degrades to "no
        # backfill" (streaming still works) rather than a broken connector — confirm that
        # snapshot 'r' events flow on staging before relying on this for PG.
        _snapshot_strategy = str(args.get("snapshot_strategy") or "").strip().lower()
        if _snapshot_strategy in ("incremental", "incremental_snapshot", "chunked") and db_type in ("postgresql", "mysql"):
            cfg["snapshot.mode"] = "no_data"
            signal_topic = str(
                args.get("signal_kafka_topic") or _qualify_topic(f"signals.{_safe_name(connector_name, 60)}")
            ).strip()
            cfg["signal.enabled.channels"] = "kafka"
            cfg["signal.kafka.topic"] = signal_topic
            cfg["signal.kafka.bootstrap.servers"] = kafka_bootstrap
            cfg["signal.kafka.groupId"] = f"{_safe_name(connector_name, 60)}-signal"
            # Watermark via the transaction log instead of a source signal table.
            cfg["read.only"] = "true"
            _chunk = str(args.get("incremental_snapshot_chunk_size") or "").strip()
            if _chunk.isdigit() and int(_chunk) > 0:
                cfg["incremental.snapshot.chunk.size"] = _chunk

        # Validate a few derived requirements
        if db_type in ("mysql", "postgresql", "sqlserver", "oracle") and not (db_name or first_db):
            # We still allow fully-qualified tables, but warn if db name is missing.
            pass

        # Best-effort topic calculation (used by orchestrator sink subscription)
        kafka_topic = ""
        if db_type == "mysql":
            db = db_name or first_db or ""
            kafka_topic = f"{topic_prefix}.{db}.{first_table}"
        elif db_type == "postgresql":
            schema = first_schema or "public"
            kafka_topic = f"{topic_prefix}.{schema}.{first_table}"
        elif db_type == "mongodb":
            # If tables were provided as db.collection, Debezium emits <topic.prefix>.<db>.<collection>
            db = first_db or db_name or ""
            kafka_topic = f"{topic_prefix}.{db}.{first_table}"
        elif db_type == "sqlserver":
            # Debezium SQL Server ALWAYS includes the database segment (driven by
            # database.names): the emitted topic is
            #   <topic.prefix>.<database>.<schema>.<table>
            # NOT <topic.prefix>.<schema>.<table>. Dropping the database segment
            # makes the orchestrator subscribe the sink to a topic that never
            # receives events (snapshot + streaming both land on the 4-segment
            # topic), so the destination stays empty.
            schema = first_schema or "dbo"
            db = db_name or first_db or ""
            kafka_topic = (
                f"{topic_prefix}.{db}.{schema}.{first_table}"
                if db
                else f"{topic_prefix}.{schema}.{first_table}"
            )
        elif db_type == "oracle":
            # Debezium Oracle emits <topic.prefix>.<SCHEMA>.<TABLE> with the OWNER
            # as the schema segment (uppercase). Match the table.include.list
            # qualification above, or the sink subscribes to a topic that never
            # receives events and the destination stays empty (same failure class
            # as the SQL Server 4-segment note).
            schema = str(args.get("schema") or first_schema or (db_user or "")).strip().upper()
            table_seg = str(first_table or "").upper()
            kafka_topic = f"{topic_prefix}.{schema}.{table_seg}" if schema else f"{topic_prefix}.{table_seg}"

        return connector_name, cfg, kafka_topic

    # -----------------------
    # MCP tool implementations
    # -----------------------
    def debezium_get_capabilities(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "success": True,
            "connector_type": "debezium",
            "supports_cdc": True,
            "supported_databases": list(self.supported.keys()),
            "kafka_connect_url": self._connect_url_from_args(args),
        }

    def debezium_test_connection(self, args: Dict[str, Any]) -> Dict[str, Any]:
        url = self._connect_url_from_args(args)
        with self._client() as client:
            r = client.get(f"{url}/")
            r.raise_for_status()
            return {"success": True, "message": "Kafka Connect reachable", "connect_url": url, "info": r.json()}

    def debezium_validate_config(self, args: Dict[str, Any]) -> Dict[str, Any]:
        errors: List[str] = []
        warnings: List[str] = []
        try:
            self._build_config(args)
        except Exception as e:
            errors.append(str(e))
        db_type = _normalize_db_type(str(args.get("database_type") or args.get("source_type") or ""))
        if db_type and db_type not in self.supported:
            errors.append(f"unsupported database_type: {db_type}")
        if db_type == "oracle":
            warnings.append("Oracle CDC often needs extra setup (LogMiner/XStream, supplemental logging, permissions). Use connector_config_overrides as needed.")
        return {"success": len(errors) == 0, "valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    def debezium_list_connectors(self, args: Dict[str, Any]) -> Dict[str, Any]:
        url = self._connect_url_from_args(args)
        with self._client() as client:
            r = client.get(f"{url}/connectors")
            r.raise_for_status()
            return {"success": True, "connectors": r.json()}

    def debezium_get_status(self, args: Dict[str, Any]) -> Dict[str, Any]:
        connector_name = str(args.get("connector_name") or "").strip()
        if not connector_name:
            return {"success": False, "error": "connector_name is required"}
        url = self._connect_url_from_args(args)
        with self._client() as client:
            r = client.get(f"{url}/connectors/{connector_name}/status")
            if r.status_code == 404:
                return {"success": False, "error": "not_found", "connector_name": connector_name}
            r.raise_for_status()
            return {"success": True, "connector_name": connector_name, "status": r.json()}

    def debezium_stop_sync(self, args: Dict[str, Any]) -> Dict[str, Any]:
        connector_name = str(args.get("connector_name") or "").strip()
        if not connector_name:
            return {"success": False, "error": "connector_name is required"}
        url = self._connect_url_from_args(args)
        with self._client() as client:
            r = client.delete(f"{url}/connectors/{connector_name}")
            if r.status_code in (200, 202, 204):
                cleanup_secret_file(connector_name)
                return {"success": True, "connector_name": connector_name, "message": "deleted"}
            if r.status_code == 404:
                cleanup_secret_file(connector_name)
                return {"success": True, "connector_name": connector_name, "message": "already absent"}
            return {"success": False, "connector_name": connector_name, "error": f"delete failed: HTTP {r.status_code}", "body": r.text[:2000]}

    def debezium_start_sync(self, args: Dict[str, Any]) -> Dict[str, Any]:
        connector_name, cfg, kafka_topic = self._build_config(args)
        url = self._connect_url_from_args(args)

        # Incremental-snapshot orchestration hints for the caller. When the config uses
        # the Kafka signal channel (snapshot_strategy=incremental → snapshot.mode=no_data),
        # the orchestrator must send an execute-snapshot signal to `signal_topic` for
        # `data_collections` AFTER the connector reaches RUNNING; otherwise history is
        # never backfilled. Blocking-snapshot connectors report incremental_snapshot=False.
        _incr_signal_topic = str(cfg.get("signal.kafka.topic", "") or "")
        _incr_info = {
            "incremental_snapshot": bool(_incr_signal_topic),
            "signal_topic": _incr_signal_topic,
            "data_collections": [t for t in str(cfg.get("table.include.list", "") or "").split(",") if t.strip()],
        }

        # Move secret values (database.password, mongodb.connection.string, …) out of
        # the config into a shared-volume file, replacing them with ${file:...} refs so
        # they never land in the Kafka config topic / REST config dump. No-op (inline)
        # when DEBEZIUM_SECRETS_DIR is unset. Must run BEFORE the POST so the file
        # exists when Kafka Connect resolves the references at task start.
        try:
            cfg = externalize_secrets(connector_name, cfg)
        except SecretExternalizationError as e:
            # Fail the provisioning call rather than the guarantee. Returned in the
            # standard shape (not raised) so the orchestrator gets a classifiable
            # error instead of an HTTP 500 with a stack trace; the wording avoids
            # "timeout"/"connection refused" so the healer's rule-based diagnoser
            # does not read it as transient and retry into the same wall.
            return {
                "success": False,
                "connector_name": connector_name,
                "error": f"secret externalization required but failed: {e}",
            }

        payload = {"name": connector_name, "config": cfg}

        with self._client() as client:
            # Create
            r = client.post(f"{url}/connectors", json=payload)
            if r.status_code in (200, 201):
                return {
                    "success": True,
                    "connector_name": connector_name,
                    "kafka_topic": kafka_topic,
                    "config": _redact_config(cfg),
                    **_incr_info,
                }
            # Already exists. Do NOT blindly PUT the freshly-built config over a
            # live connector: that can mutate snapshot.mode / database.server.id
            # mid-stream and silently turn a healthy connector into a stalled one
            # (a prime "ran last time, fails this time" trigger). Instead fetch
            # the existing config, PRESERVE its database.server.id (the running
            # binlog identity), and only PUT when something actually changed.
            if r.status_code == 409:
                existing = {}
                try:
                    g = client.get(f"{url}/connectors/{connector_name}/config")
                    if g.status_code == 200:
                        existing = g.json() or {}
                except Exception:
                    existing = {}
                # Keep the already-running server id to avoid a re-roll/collision.
                if existing.get("database.server.id"):
                    cfg["database.server.id"] = existing["database.server.id"]
                if existing == cfg:
                    return {
                        "success": True,
                        "connector_name": connector_name,
                        "kafka_topic": kafka_topic,
                        "config": _redact_config(cfg),
                        "message": "connector already running with identical config (no-op)",
                        "already_running": True,
                        **_incr_info,
                    }
                u = client.put(f"{url}/connectors/{connector_name}/config", json=cfg)
                u.raise_for_status()
                return {
                    "success": True,
                    "connector_name": connector_name,
                    "kafka_topic": kafka_topic,
                    "config": _redact_config(cfg),
                    "message": "updated existing connector (server.id preserved)",
                    **_incr_info,
                }

            # Scrub any secret literal from the echoed upstream body — a Kafka Connect
            # validation error can quote submitted config values back. Cover both the
            # relational database.password and any URI-embedded credential (MongoDB's
            # mongodb.connection.string carries the password in its userinfo).
            safe_body = _scrub_secrets(r.text[:2000], cfg.get("database.password"), cfg.get("database.user"))
            safe_body = _mask_uri_credentials(safe_body)
            return {"success": False, "connector_name": connector_name, "error": f"create failed: HTTP {r.status_code}", "body": safe_body}


def _dispatch_tool(server: DebeziumConnector, tool: str, args: Dict[str, Any]) -> Dict[str, Any]:
    tool = (tool or "").strip()
    if not tool:
        return {"success": False, "error": "missing tool name"}

    # Orchestrator calls tools as "<connector_type>_<operation>"
    # e.g. "debezium_start_sync".
    if not tool.startswith("debezium_"):
        return {"success": False, "error": f"unsupported tool namespace: {tool}"}

    fn_name = tool
    if not hasattr(server, fn_name):
        return {"success": False, "error": f"unknown tool: {tool}"}

    fn = getattr(server, fn_name)
    return fn(args or {})


def create_http_app():
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel

    class MCPRequest(BaseModel):
        method: str
        params: Optional[dict] = None

    app = FastAPI(title="Debezium MCP Connector", version="v1.0.0")
    server = DebeziumConnector()

    @app.get("/health")
    async def health():
        return {"status": "healthy", "connector": "debezium", "version": "v1.0.0"}

    @app.post("/mcp")
    async def mcp(req: Dict[str, Any]):
        # Accept raw JSON-RPC request (orchestrator sends JSON-RPC directly)
        try:
            rpc_id = int(req.get("id", 1))
            if req.get("method") != "tools/call":
                return _jsonrpc_err(rpc_id, -32601, "Method not found")
            params = req.get("params") or {}
            tool = params.get("name")
            args = params.get("arguments") or {}
            result = _dispatch_tool(server, tool, args)
            return _jsonrpc_ok(rpc_id, result)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/invoke/{tool_name}")
    async def invoke(tool_name: str, params: Optional[dict] = None):
        try:
            return _dispatch_tool(server, tool_name, params or {})
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    return app


def run_stdio():
    server = DebeziumConnector()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            rpc_id = int(req.get("id", 1))
            if req.get("method") != "tools/call":
                resp = _jsonrpc_err(rpc_id, -32601, "Method not found")
            else:
                params = req.get("params") or {}
                tool = params.get("name")
                args = params.get("arguments") or {}
                result = _dispatch_tool(server, tool, args)
                resp = _jsonrpc_ok(rpc_id, result)
        except Exception as e:
            resp = _jsonrpc_err(1, -32603, "Internal error", {"error": str(e)})
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    http_mode = os.getenv("MCP_HTTP_MODE", "false").lower() == "true" or bool(os.getenv("DOCKER_CONTAINER"))
    port = int(os.getenv("MCP_PORT", os.getenv("PORT", "8000")))

    if http_mode:
        import uvicorn

        app = create_http_app()
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        run_stdio()


