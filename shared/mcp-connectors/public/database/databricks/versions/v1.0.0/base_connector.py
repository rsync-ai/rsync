#!/usr/bin/env python3
"""
Base MCP Connector Class
All connectors should inherit from this to ensure consistent interface for AI agents.
Includes built-in optimization helpers for performance.
Includes automatic trace_id propagation for distributed tracing.

GENERIC DESIGN:
- No hardcoded connector types anywhere
- All capabilities declared via get_capabilities()
- All operations discoverable via list_tools()
- Planner/Validator/Executor query capabilities dynamically
- trace_id automatically extracted and propagated for all operations

USAGE:
    from base_connector import BaseMCPConnector
    
    class MyConnectorMCPServer(BaseMCPConnector):
        def __init__(self):
            super().__init__()
            self.connector_type = "my-connector"
            self.connector_category = "relational_db"  # or document_db, api_saas, etc.
        
        # Implement abstract methods...

VERSION: 2.1.0 - Added Category Handler Contracts (ApiHandler/StorageHandler/DatabaseHandler)
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime
import ipaddress
import logging
import re
import socket
import time
import json
import os
import sys
import threading
import io
from urllib.parse import urlparse, urljoin


# =============================================================================
# SSRF guard (T1-4)
# =============================================================================
#
# Generated REST + GraphQL connectors accept a per-connection `base_url`
# override and pass it straight to `requests.request(url=...)` with the
# connector's auth header attached. Without an SSRF guard, any
# authenticated user can:
#   1. Set base_url=http://169.254.169.254/... → cloud IMDS exfiltration
#   2. Set base_url=http://attacker.com/log    → vendor credential theft
#      (the auth Bearer / API key reaches attacker.com in the
#      Authorization header)
#   3. Probe internal services on the docker network (postgres:5432,
#      kafka:9092, temporal:7233, redis:6379) — credentials silently
#      land in those services' connection logs.
#
# The guard rejects any URL whose resolved hostname maps to:
#   - Loopback (127.0.0.0/8, ::1)
#   - Link-local (169.254.0.0/16, fe80::/10) — blocks IMDS
#   - RFC1918 private nets (10/8, 172.16/12, 192.168/16)
#   - The hardcoded internal-service names of the docker stack
#
# Opt-out for dev: set MCP_ALLOW_PRIVATE_NETWORKS=true (use ONLY when
# testing against a localhost mock server; never in production).

_SSRF_INTERNAL_HOSTNAMES = frozenset([
    "localhost",
    # Docker compose service names — outbound calls from generated
    # connectors should NEVER target these. If a real connector ever
    # needs them, add an explicit per-connector allowlist instead of
    # weakening this list.
    "postgres", "mysql", "redis", "kafka", "kafka-connect",
    "temporal", "minio", "llm-service", "api-gateway",
    "backend-orchestrator", "backend-temporal-adapter",
    # K8s default service host names that show up in cluster deploys
    "kubernetes", "kube-dns",
])


# Credential-bearing query params that must never reach a log line. Request
# exceptions (urllib3 ConnectionError/MaxRetryError) embed the full request
# URL including the query string — for api_key_query connectors (pipedrive
# ?api_token=…) that is the live credential.
_URL_SECRET_PARAMS = re.compile(
    r"(?i)\b(api_token|api_key|apikey|token|access_token|auth|key|secret|"
    r"client_secret|password|passwd|sig|signature)=([^&\s'\"]+)"
)


def _scrub_url_secrets(text: Any) -> str:
    """Mask credential query-param values in free-form text before logging.
    Lossy by design: losing part of an error message costs debuggability;
    logging a credential is an incident."""
    try:
        return _URL_SECRET_PARAMS.sub(r"\1=***", str(text))
    except Exception:
        return "(unloggable error)"


# Header names that carry credentials and must NOT follow a redirect to a
# different host (a validated vendor endpoint could 30x to an attacker-
# controlled public host — SSRF checks pass, but the API key/token would leak).
_SENSITIVE_HEADER_SUBSTRINGS = ("authorization", "auth", "api-key", "apikey", "token", "secret", "password", "cookie")


def _headers_safe_for_redirect(headers: dict, from_url: str, to_url: str) -> dict:
    """Return `headers` unchanged for a same-host redirect; for a cross-host
    redirect, return a copy with credential-bearing headers stripped so the
    connector never sends its auth to a third-party host it was redirected to.
    """
    try:
        from_host = (urlparse(from_url).hostname or "").lower()
        to_host = (urlparse(to_url).hostname or "").lower()
    except Exception:
        from_host, to_host = "", ""
    if from_host and to_host and from_host == to_host:
        return headers
    return {
        k: v for k, v in (headers or {}).items()
        if not any(s in k.lower() for s in _SENSITIVE_HEADER_SUBSTRINGS)
    }


def _ssrf_check_url(url: str) -> Optional[str]:
    """Return None when `url` is safe; otherwise return a rejection
    reason. Used by BaseMCPConnector._make_request_v2 before every
    outbound `requests.request` call.

    Resolution + check happens on every request (not cached) because
    DNS rebinding is a known SSRF bypass: the first resolve might
    return a public IP, the second a private one. The cost is one
    `socket.getaddrinfo` per outbound call.
    """
    if os.getenv("MCP_ALLOW_PRIVATE_NETWORKS", "").lower() in ("true", "1", "yes"):
        return None

    try:
        parsed = urlparse(url)
    except Exception as e:
        return f"unparseable URL: {e}"

    scheme = (parsed.scheme or "").lower()
    if scheme not in ("http", "https"):
        return f"only http/https allowed; got scheme={scheme!r}"

    host = (parsed.hostname or "").lower()
    if not host:
        return "missing hostname"

    # Reject internal service names BEFORE resolution. This is a
    # belt-and-suspenders check — even if DNS resolves the name to a
    # public IP (which shouldn't happen, but might in misconfigured
    # split-horizon DNS), the name itself shouldn't be reachable.
    if host in _SSRF_INTERNAL_HOSTNAMES:
        return f"hostname {host!r} is an internal service; outbound calls forbidden"

    # Resolve and check every A/AAAA record. Connector hostnames are
    # vendor endpoints (api.stripe.com, *.myshopify.com, etc.) so
    # the resolve should be fast.
    try:
        addrs = socket.getaddrinfo(host, None)
    except socket.gaierror as e:
        # Don't leak DNS failures as "blocked"; let the actual
        # requests.request raise the underlying error. This keeps
        # the connector's existing 5xx handling on DNS issues.
        return None

    for fam, _, _, _, sockaddr in addrs:
        try:
            ip = ipaddress.ip_address(sockaddr[0])
        except (ValueError, IndexError):
            continue
        if ip.is_loopback:
            return f"hostname {host!r} resolves to loopback IP {ip}"
        if ip.is_link_local:
            return f"hostname {host!r} resolves to link-local IP {ip} (blocks cloud IMDS exfiltration)"
        if ip.is_private:
            return f"hostname {host!r} resolves to private IP {ip}"
        if ip.is_multicast or ip.is_reserved or ip.is_unspecified:
            return f"hostname {host!r} resolves to unusable IP {ip} ({'multicast' if ip.is_multicast else 'reserved/unspecified'})"
    return None


# =============================================================================
# SHARED RESULT TYPES (Contract-Driven)
# =============================================================================

@dataclass
class ErrorItem:
    """Structured error for operations with partial failures"""
    code: str
    message: str
    resource: Optional[str] = None
    page: Optional[int] = None
    detail: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "resource": self.resource,
            "page": self.page,
            "detail": self.detail,
        }


@dataclass
class ExportResult:
    """Standard result for export operations across all categories"""
    success: bool
    records: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[Union[ErrorItem, Dict[str, Any]]] = field(default_factory=list)
    next_cursor: Optional[Union[str, int]] = None
    stats: Dict[str, Any] = field(default_factory=dict)
    # True ONLY when a cap (max_pages / max_records) truncated the result, so the
    # caller can tell "complete" apart from "more rows exist, fetch again from
    # next_cursor". A complete fetch leaves this False. Prevents silent tail-drop.
    has_more: bool = False
    # Blob (raw-bytes) passthrough envelopes — parallel to `records`, used only by the
    # blob move modality (universal-blob-passthrough plan §2). Each entry is a
    # {object_key, content_type, size, data_ref, sha256, source_file_modified} pointer
    # to bytes staged byte-identical in the claim-check store. Empty for the default
    # structured (row) lane, so existing pipelines are unaffected.
    blobs: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "records": self.records,
            "errors": [e.to_dict() if isinstance(e, ErrorItem) else e for e in self.errors],
            "next_cursor": self.next_cursor,
            "stats": self.stats,
            "has_more": self.has_more,
            "blobs": self.blobs,
        }


@dataclass
class ImportResult:
    """Standard result for import_data operations across all categories"""
    success: bool
    written: int = 0
    failed: int = 0
    errors: List[Union[ErrorItem, Dict[str, Any]]] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "written": self.written,
            "failed": self.failed,
            "errors": [e.to_dict() if isinstance(e, ErrorItem) else e for e in self.errors],
            "stats": self.stats,
        }

# =============================================================================
# DESTINATION LOAD STRATEGY (Universal stage-and-merge abstraction)
# =============================================================================
# A destination connector declares HOW it best loads a batch by setting a
# DestinationLoadSpec on self.load_spec. The shared DestinationLoadMixin then
# orchestrates a bulk "stage -> bulk-load -> merge -> drop" path, replacing the
# slow per-row upsert loop (one network round-trip per row). Adding a new
# destination = declare a spec + override the four staging hooks. See
# docs/connectors/destination-load-strategies.md for the full design.

# How a batch is physically written into the staging area / target.
LOAD_METHODS = (
    "copy",          # Postgres COPY / Redshift COPY-from-stage (fastest bulk)
    "stage_copy",    # warehouse: PUT to internal stage then COPY INTO (Snowflake)
    "load_api",      # warehouse load API (BigQuery load jobs)
    "multi_values",  # single multi-row INSERT ... VALUES (...),(...),... (MySQL)
    "native_insert", # executemany / batched parameterized INSERT
    "row_by_row",    # last-resort one-INSERT-per-row (legacy fallback)
)

# How staged rows are reconciled into the target on the key columns.
MERGE_METHODS = (
    "on_conflict",       # Postgres INSERT ... ON CONFLICT DO UPDATE
    "on_duplicate_key",  # MySQL INSERT ... ON DUPLICATE KEY UPDATE
    "merge",             # ANSI MERGE (Snowflake, BigQuery, SQL Server)
    "replacing",         # ClickHouse ReplacingMergeTree (insert + dedup on read)
    "delete_insert",     # generic: DELETE matching keys then INSERT (no native upsert)
)


@dataclass
class DestinationLoadSpec:
    """Declarative description of how a destination best loads a batch.

    A destination connector sets ``self.load_spec = DestinationLoadSpec(...)``;
    the mixin reads it to choose the bulk path and the merge dialect. Defaults
    are the universally-safe stage-and-merge configuration.
    """
    load_method: str = "native_insert"
    merge_method: str = "delete_insert"
    supports_staging: bool = True
    # Rows above this are chunked before staging to bound memory / statement size.
    max_batch_rows: int = 10_000

    def to_dict(self) -> Dict[str, Any]:
        return {
            "load_method": self.load_method,
            "merge_method": self.merge_method,
            "supports_staging": self.supports_staging,
            "max_batch_rows": self.max_batch_rows,
        }


class DestinationLoadMixin:
    """Shared orchestration for the stage-and-merge upsert path.

    A destination connector mixes this in, sets ``self.load_spec`` and overrides
    the four ``_stage_*`` hooks for its dialect. ``staged_upsert`` runs
    stage -> bulk-load -> merge -> drop on a caller-supplied cursor and returns
    the number of rows written, raising on any failure so the caller (which owns
    the connection) can roll back and fall back to its per-row loop.
    """

    # Connectors override this; default is the safe generic spec.
    load_spec: "DestinationLoadSpec" = DestinationLoadSpec()

    def supports_staged_load(self) -> bool:
        spec = getattr(self, "load_spec", None) or DestinationLoadSpec()
        return bool(spec.supports_staging and spec.load_method != "row_by_row")

    def staged_upsert(self, cursor, target_table: str, columns: List[str],
                      rows: List[Dict[str, Any]], key_fields: List[str],
                      col_types: Optional[Dict[str, Any]] = None) -> int:
        """Run the bulk stage-and-merge path. Returns the rows the merge WROTE
        (``cursor.rowcount``, clamped to the input count), falling back to the
        input count only when the driver reports none.

        Raises on any failure; the caller owns the connection and is responsible
        for rollback + per-row fallback so transaction state stays consistent.
        """
        if not rows:
            return 0
        col_types = col_types or {}
        n_input = len(rows)
        keys = self._resolve_conflict_keys(columns, key_fields)
        # Collapse duplicate keys within the batch (last write wins). A single
        # INSERT ... ON CONFLICT / MERGE cannot touch the same target row twice,
        # so duplicates that the per-row loop would resolve sequentially must be
        # deduped before staging.
        staged_rows = self._dedupe_by_key(rows, keys)
        staging = self._stage_create(cursor, target_table, columns)
        merged = n_input
        try:
            self._stage_bulk_load(cursor, staging, columns, staged_rows, col_types)
            self._stage_merge(cursor, target_table, staging, columns, keys,
                              getattr(self, "load_spec", DestinationLoadSpec()).merge_method)
            # Rows the merge actually WROTE to the target. Capture BEFORE
            # _stage_drop (in the finally) overwrites cursor.rowcount. Returning
            # len(rows) unconditionally (the old behavior) reported a merge that
            # persisted 0 rows as a full success — the same silent-data-loss
            # shape KI-WRITE-COUNT-1 closed on import_data.
            #
            # Clamped to n_input because the merge cannot write more TARGET rows
            # than were staged, while MySQL's INSERT ... ON DUPLICATE KEY UPDATE
            # reports rowcount 2 per UPDATED row — an unclamped value would
            # over-report there. The clamp is a no-op on PostgreSQL-protocol and
            # ClickHouse drivers, where rowcount <= n_input already.
            #
            # Fall back to the input count only when the driver reports none
            # (rowcount None or -1, as some warehouse DBAPIs do).
            rc = getattr(cursor, "rowcount", None)
            if rc is not None and rc >= 0:
                merged = min(rc, n_input)
        finally:
            self._stage_drop(cursor, staging)
        # Diagnostic: resolved target + connected DB + input-vs-merged. A shortfall
        # is the silent-data-loss signature (success reported, rows didn't land).
        # Metadata only — no row values, per the log/LLM privacy rule.
        try:
            _conn = getattr(cursor, "connection", None)
            _db = getattr(getattr(_conn, "info", None), "dbname", "?")
        except Exception:
            _db = "?"
        logger.info(
            "staged_upsert: target=%s db=%s input=%d merged=%d",
            target_table, _db, n_input, merged,
        )
        return merged

    @staticmethod
    def _resolve_conflict_keys(columns: List[str], key_fields: List[str]) -> List[str]:
        """Resolve the effective conflict/merge key columns, with the same
        fallback the per-row path uses: requested keys present in the row, else
        ``id``, else the first column."""
        keys = [k for k in (key_fields or []) if k in columns]
        if not keys:
            keys = ["id"] if "id" in columns else ([columns[0]] if columns else [])
        return keys

    @staticmethod
    def _dedupe_by_key(rows: List[Dict[str, Any]], keys: List[str]) -> List[Dict[str, Any]]:
        """Return rows deduped on ``keys`` keeping the last occurrence, order
        preserved. No-op when there are no keys."""
        if not keys:
            return rows

        def _hashable(v):
            if isinstance(v, (dict, list)):
                try:
                    return json.dumps(v, default=str, sort_keys=True)
                except (TypeError, ValueError):
                    return str(v)
            return v

        deduped: Dict[Any, Dict[str, Any]] = {}
        order: List[Any] = []
        for row in rows:
            k = tuple(_hashable(row.get(c)) for c in keys)
            if k not in deduped:
                order.append(k)
            deduped[k] = row  # last write wins
        return [deduped[k] for k in order]

    # ---- dialect hooks: override per destination -------------------------
    def _stage_create(self, cursor, target_table: str, columns: List[str]) -> str:
        """Create the staging area mirroring target columns; return its name."""
        raise NotImplementedError(f"{type(self).__name__} must implement _stage_create")

    def _stage_bulk_load(self, cursor, staging_table: str, columns: List[str],
                         rows: List[Dict[str, Any]], col_types: Dict[str, Any]) -> None:
        """Bulk-load rows into the staging area (COPY / multi-row INSERT / load job)."""
        raise NotImplementedError(f"{type(self).__name__} must implement _stage_bulk_load")

    def _stage_merge(self, cursor, target_table: str, staging_table: str,
                     columns: List[str], key_fields: List[str], merge_method: str) -> None:
        """Reconcile staged rows into the target on the key columns."""
        raise NotImplementedError(f"{type(self).__name__} must implement _stage_merge")

    def _stage_drop(self, cursor, staging_table: str) -> None:
        """Tear down the staging area (no-op if it auto-drops)."""
        raise NotImplementedError(f"{type(self).__name__} must implement _stage_drop")

    def load_strategy_capability(self) -> Dict[str, Any]:
        """Capability block describing this destination's load strategy."""
        spec = getattr(self, "load_spec", None) or DestinationLoadSpec()
        return {**spec.to_dict(), "staged_load_available": self.supports_staged_load()}


# =============================================================================
# TRACE ID HANDLING (Automatic for all connectors)
# =============================================================================

# Thread-local storage for trace_id - ensures thread-safety for concurrent requests
_trace_local = threading.local()

def get_trace_id() -> str:
    """
    Get current trace_id from thread-local storage.
    Returns 'no-trace-id' if not set.
    """
    return getattr(_trace_local, 'trace_id', 'no-trace-id')

def set_trace_id(trace_id: str) -> None:
    """
    Set trace_id in thread-local storage.
    Called automatically by handle_request() - no manual call needed.
    """
    _trace_local.trace_id = trace_id if trace_id else 'no-trace-id'

def clear_trace_id() -> None:
    """Clear the trace_id from thread-local storage."""
    _trace_local.trace_id = 'no-trace-id'


class TraceContextFilter(logging.Filter):
    """
    Logging filter that automatically adds trace_id to all log records.
    Applied globally to ensure all logs include trace context.
    """
    def filter(self, record):
        record.trace_id = get_trace_id()
        return True


def setup_traced_logging(logger_instance: logging.Logger, connector_name: str) -> None:
    """
    Configure a logger to include trace_id in all log messages.
    Call this in __init__ of your connector if you want custom logging.
    
    Args:
        logger_instance: The logger to configure
        connector_name: Name of the connector for log prefix
    """
    # Add trace context filter
    logger_instance.addFilter(TraceContextFilter())
    logger_instance.addFilter(SensitiveDataScrubbingFilter())
    
    # Update formatter to include trace_id
    log_format = f'%(asctime)s [{connector_name}] [trace_id=%(trace_id)s] %(message)s'
    for handler in logger_instance.handlers:
        handler.setFormatter(logging.Formatter(log_format))
    
    # If using root logger handlers
    if not logger_instance.handlers:
        logging.basicConfig(format=log_format, level=logging.INFO)


# Setup default logger with trace support
# >>> RSYNC_LOG_SCRUBBER_BEGIN — canonical source. Propagated to every versioned
# base_connector.py copy by scripts/mcp-connectors/sync_log_scrubber.py. Keep the
# regex set in lockstep with llm-service masking.py (scrub_error_for_llm) and
# backend-orchestrator pkg/llmscrub/scrub.go (a parity test guards this).
# =============================================================================
# Sensitive-data scrubbing for logs
# -----------------------------------------------------------------------------
# Self-contained imports so this block also works in older connector snapshots
# that don't import re/logging at module scope (idempotent — no-op where the
# base already imports them at the top).
import logging  # noqa: E402
import re  # noqa: E402
# -----------------------------------------------------------------------------
# Privacy contract: connector logs must not leak customer row values,
# credentials, or PII to the log backend (SigNoz). DB drivers embed failing-row
# values in error text — e.g. Postgres "Key (email)=(a@b.com)" or "Failing row
# contains (...)" — which base_connector logs verbatim at the Query/Import leak
# sites. This scrubber redacts those before the record is emitted.
#
# The regex set below is kept in lockstep with the Go scrubber
# (backend-orchestrator/pkg/llmscrub/scrub.go) and the llm-service Python
# scrubber (llm-service/src/utils/masking.py :: scrub_error_for_llm). Change one
# → change all three (a parity test guards this).
# =============================================================================

_SCRUB_REDACTED = "[redacted]"

_SCRUB_PATTERNS = [
    # Postgres not-null/check DETAIL dumps the ENTIRE row — greedy to end of
    # string so a truncated dump can't leak a partial row.
    (re.compile(r"\bFailing row contains\b.*", re.IGNORECASE | re.DOTALL), f"Failing row contains ({_SCRUB_REDACTED})"),
    # Everything after a SQL VALUES keyword is row data (multi-tuple included).
    (re.compile(r"\bVALUES\b\s*\(.*", re.IGNORECASE | re.DOTALL), f"VALUES ({_SCRUB_REDACTED})"),
    # Postgres duplicate-key detail: Key (email)=(user@x.com) — keep column names.
    (re.compile(r"(\bKey\s*\([^)]*\)=)\((?:[^()]|\([^)]*\))*\)", re.IGNORECASE), rf"\1({_SCRUB_REDACTED})"),
    # Credentials embedded in URLs: scheme://user:pass@host
    (re.compile(r"(\w+://)[^/\s:@]+:[^/\s@]*@"), rf"\1{_SCRUB_REDACTED}@"),
    # HTTP credential headers — must run before the KV rule.
    (re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=\-]+", re.IGNORECASE), f"Bearer {_SCRUB_REDACTED}"),
    # key=value / key: value pairs with credential-bearing key names.
    (
        re.compile(
            r"\b(password|passwd|pwd|secret|token|api[_-]?key|apikey|authorization|bearer|access[_-]?key|private[_-]?key|client[_-]?secret)\b\s*[=:]\s*\S+",
            re.IGNORECASE,
        ),
        rf"\1={_SCRUB_REDACTED}",
    ),
    # The same key=value shape when the credential word is fused into a longer
    # key name. The rule above is anchored with \b and an underscore is a word
    # character, so KAFKA_SASL_PASSWORD, SASL_PASSWORD, sasl_plain_password and
    # KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET had no word boundary before the
    # credential word and matched nothing at all. Connectors log the env they
    # were handed on connect failure, and a broker credential is shared across
    # all tenants rather than scoped to one, so a single connect error leaks a
    # cluster-wide secret. AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN ride the
    # same shape on the MSK IAM and S3 paths.
    #
    # The key name is kept: it names the variable an operator has to rotate.
    # Bare "key" is deliberately NOT a suffix — it would eat the Postgres
    # "Key (email)=" detail and schema metadata like partition_key. Character
    # classes are spelled out rather than \w because Python's \w is
    # Unicode-aware and Go's is not.
    # MUST stay lockstep with masking.py + Go llmscrub + the sink's scrubLog.
    (
        re.compile(
            r"\b([A-Za-z0-9_.\-]*(?:password|passwd|pwd|secret|token|api[_.\-]?key|access[_.\-]?key|private[_.\-]?key))\b\s*[=:]\s*\S+",
            re.IGNORECASE,
        ),
        rf"\1={_SCRUB_REDACTED}",
    ),
    # Bare JWTs (base64url evades the base64 rule): eyJ = {"
    (re.compile(r"\beyJ[A-Za-z0-9_\-]{4,}(?:\.[A-Za-z0-9_\-]+){1,2}"), _SCRUB_REDACTED),
    # Double-quoted string directly after a colon — Postgres quotes offending
    # VALUES this way and it catches JSON string values.
    (re.compile(r'(:\s*)"(?:[^"\\]|\\.)*"'), rf'\1"{_SCRUB_REDACTED}"'),
    # JSON numeric values ("age": 41) — quoted key + colon + number.
    (re.compile(r'("[\w\-]+"\s*:\s*)-?\d[\d.eE+\-]*'), r"\1[num-redacted]"),
    # Single-quoted literals — SQL string values (fail-closed on identifiers).
    # `(?:'|\Z)` also covers a literal left open by upstream log truncation, and
    # the leading `(^|[^A-Za-z0-9_])` keeps an apostrophe inside a word ("couldn't",
    # "the user's") from being read as an opening quote — that misread consumed the
    # prose as the literal and left the real value in the clear. One rule, not two
    # passes: a separate `'…$` pass re-read this rule's own `'[redacted]'` output.
    # MUST stay lockstep with masking.py + Go llmscrub.
    (re.compile(r"(^|[^A-Za-z0-9_])'(?:[^'\\]|\\.)*(?:'|\Z)", re.DOTALL), rf"\1'{_SCRUB_REDACTED}'"),
    # Email addresses appearing outside quotes.
    (re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"), "[email-redacted]"),
    # IPv4 addresses — client/host IPs are personal data under GDPR and aren't
    # needed for diagnosis. Octets validated 0-255 so version strings (8.0.32)
    # and ISO dates survive. MUST stay lockstep with masking + Go llmscrub.
    (
        re.compile(r"\b(?:(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\b"),
        "[ip-redacted]",
    ),
    # Long base64-ish blobs (tokens, keys). 40+ keeps 32-hex trace ids.
    (re.compile(r"\b[A-Za-z0-9+/]{40,}={0,2}\b"), _SCRUB_REDACTED),
    # Dashed SSN / US-phone shapes (ISO dates don't match — timestamps survive).
    (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "[num-redacted]"),
    (re.compile(r"\b\d{3}[-.]\d{3}[-.]\d{4}\b"), "[num-redacted]"),
    # Digit runs of 7+ — likely record ids / SSNs / phones; ports/counts survive.
    (re.compile(r"\d{7,}"), "[num-redacted]"),
]


def scrub_sensitive(text):
    """Redact likely customer data (row values, credentials, PII) from a string.

    Lossy by design: losing a fragment of a log line costs diagnosis quality;
    leaking a row value breaks the privacy contract. Mirror of
    masking.scrub_error_for_llm — keep the two rule sets in lockstep.
    """
    if not text:
        return text if isinstance(text, str) else ""
    result = text
    for pattern, replacement in _SCRUB_PATTERNS:
        result = pattern.sub(replacement, result)
    return result


class SensitiveDataScrubbingFilter(logging.Filter):
    """Redact customer row values / credentials / PII from every log record —
    message, %-args, exception traceback, and stack info — before emission.
    Attached wherever base_connector configures a logger, so the DB-driver error
    text at the Query/Import leak sites never reaches the log backend verbatim.
    Scrubbing must never break logging, so all work is wrapped defensively.
    """

    def filter(self, record):
        try:
            if record.args:
                # Collapse %-args into the message so nothing leaks via args.
                record.msg = record.getMessage()
                record.args = None
            if isinstance(record.msg, str):
                record.msg = scrub_sensitive(record.msg)
            if record.exc_info:
                # DB tracebacks carry row values; render, scrub, and pin so the
                # handler formatter emits the scrubbed text.
                record.exc_text = scrub_sensitive(
                    logging.Formatter().formatException(record.exc_info)
                )
                record.exc_info = None
            if getattr(record, "stack_info", None):
                record.stack_info = scrub_sensitive(record.stack_info)
        except Exception:
            pass
        return True


def _install_root_log_scrubber():
    """Attach the scrubber to the root logger's handlers so DB-driver errors
    logged via a CONNECTOR's OWN module logger (logging.getLogger(__name__),
    which propagates to the root handler) are scrubbed too — not only
    base_connector's loggers. Idempotent."""
    _root = logging.getLogger()
    for _h in list(_root.handlers):
        if not any(isinstance(_f, SensitiveDataScrubbingFilter) for _f in _h.filters):
            _h.addFilter(SensitiveDataScrubbingFilter())


# Wrap logging.basicConfig so a connector's own basicConfig(...) — which creates
# the root handler AFTER base_connector is imported — gets the scrubber attached
# to the handler it just made. Without this a connector's module-logger DB errors
# (e.g. postgres "Key (col)=(value)" row data) reach the log backend unscrubbed.
# Idempotent (guarded on the wrapper name).
if getattr(logging.basicConfig, "__name__", "") != "_rsync_scrubbing_basic_config":
    _rsync_orig_basic_config = logging.basicConfig

    def _rsync_scrubbing_basic_config(*args, **kwargs):
        _rsync_orig_basic_config(*args, **kwargs)
        _install_root_log_scrubber()

    logging.basicConfig = _rsync_scrubbing_basic_config

_install_root_log_scrubber()
# >>> RSYNC_LOG_SCRUBBER_END


logger = logging.getLogger(__name__)
logger.addFilter(SensitiveDataScrubbingFilter())
logger.addFilter(TraceContextFilter())
for handler in logger.handlers:
    handler.setFormatter(logging.Formatter('%(asctime)s [MCP-Base] [trace_id=%(trace_id)s] %(message)s'))


# =============================================================================
# PARAMETER NORMALIZATION (Standalone function - use in ANY connector!)
# =============================================================================
# This is the KEY to making ALL connectors generic. Plan generators can use
# any terminology (table, tables, entity, entities, collection, etc.)
# and this normalizes to connector-specific terminology.
#
# USAGE in your connector:
#   from base_connector import normalize_params_for_connector
#   normalized = normalize_params_for_connector(params, "relational_db")
# =============================================================================

def normalize_params_for_connector(params: Dict[str, Any], connector_category: str) -> Dict[str, Any]:
    """
    Normalize parameters to connector-specific terminology.
    
    This is a STANDALONE function that can be called by ANY connector,
    whether or not it inherits from BaseMCPConnector.
    
    Args:
        params: The raw parameters from plan/executor
        connector_category: One of "relational_db", "document_db", "cloud_storage", 
                           "data_warehouse", "streaming", "api_saas"
    
    Returns:
        Normalized parameters with correct terminology for the connector category
    
    Example:
        # Plan sends: {"tables": ["users", "orders"]}
        # After normalization for relational_db: {"table": "users", "tables": ["users", "orders"]}
        
        # Plan sends: {"entities": ["contacts"]}
        # After normalization for document_db: {"collection": "contacts", "collections": ["contacts"]}
    """
    if not params:
        return params
    
    # IMPORTANT:
    # This normalization must be NON-DESTRUCTIVE.
    #
    # We *alias* across terminologies instead of renaming via pop(), because
    # different connector versions may expect different dialect keys (e.g.
    # "table" vs "endpoint"). Destroying the original key breaks older
    # connectors at runtime.
    normalized = params.copy()
    
    # Get category info (defined later in this file)
    # Using a local import to avoid circular dependency
    category_info = CATEGORY_OPERATIONS.get(connector_category, CATEGORY_OPERATIONS.get("relational_db", {}))
    terminology = category_info.get("terminology", {"entity": "table", "record": "row", "field": "column"})
    
    # Get connector-specific entity name (e.g., "table", "collection", "bucket")
    entity_singular = terminology.get("entity", "table")
    entity_plural = entity_singular + "s"  # e.g., "tables", "collections"
    
    # =========================================================================
    # RULE 1: Convert generic 'entity'/'entities' to specific terminology
    # =========================================================================
    if "entity" in normalized and entity_singular != "entity":
        if entity_singular not in normalized:
            normalized[entity_singular] = normalized["entity"]
        logger.debug(f"[normalize] alias 'entity' -> '{entity_singular}'")
    
    if "entities" in normalized and entity_plural != "entities":
        if entity_plural not in normalized:
            normalized[entity_plural] = normalized["entities"]
        logger.debug(f"[normalize] alias 'entities' -> '{entity_plural}'")
    
    # =========================================================================
    # RULE 2: Convert plural array to singular (use first element)
    # =========================================================================
    # This is the critical fix for "tables vs table" issue!
    if entity_plural in normalized and entity_singular not in normalized:
        plural_value = normalized[entity_plural]
        if isinstance(plural_value, list) and len(plural_value) > 0:
            normalized[entity_singular] = plural_value[0]
            normalized["_selected_" + entity_plural] = plural_value  # Keep for multi-entity ops
            logger.info(f"[normalize] ⚠️ '{entity_plural}' array -> '{entity_singular}' = {plural_value[0]}")
    
    # =========================================================================
    # RULE 3: Cross-terminology normalization
    # =========================================================================
    # Handle when plan uses wrong terminology for connector type
    all_entity_names = ["table", "collection", "bucket", "topic", "endpoint", "object"]
    all_entity_plurals = [n + "s" for n in all_entity_names]
    
    # If connector expects 'collection' but params have 'table', convert
    for other_entity in all_entity_names:
        if other_entity != entity_singular and other_entity in normalized:
            if entity_singular not in normalized:
                normalized[entity_singular] = normalized[other_entity]
            logger.info(f"[normalize] ⚠️ Cross-terminology alias: '{other_entity}' -> '{entity_singular}'")
            break
    
    # Same for plurals
    for other_plural in all_entity_plurals:
        if other_plural != entity_plural and other_plural in normalized:
            if entity_plural not in normalized:
                normalized[entity_plural] = normalized[other_plural]
            if entity_singular not in normalized:
                plural_value = normalized[entity_plural]
                if isinstance(plural_value, list) and len(plural_value) > 0:
                    normalized[entity_singular] = plural_value[0]
            logger.info(f"[normalize] ⚠️ Cross-terminology alias: '{other_plural}' -> '{entity_plural}'")
            break
    
    return normalized


def detect_incremental_candidates(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Detect candidate columns/fields for incremental sync from a discovered schema.
    
    Returns a list of incremental candidates with their confidence scores.
    Each candidate includes:
    - table: table/collection name
    - field: field/column name
    - type: field data type
    - recommended: bool indicating if this is a high-confidence match
    
    Args:
        schema: Output from discover_schema containing tables/collections and columns
    
    Returns:
        List of incremental candidate dicts
    """
    candidates = []
    
    # Common timestamp field name patterns (ordered by preference)
    timestamp_patterns = [
        'updated_at', 'modified_at', 'modified_date', 'last_modified',
        'updated_date', 'changed_at', 'created_at', 'timestamp',
        'modifieddate', 'lastmodified', '_updated', 'update_time',
        'modified_time', 'mod_time'
    ]
    
    # Common timestamp type keywords
    timestamp_types = [
        'datetime', 'timestamp', 'timestamptz', 'date', 'time',
        'datetime64', 'datetime2', 'datetimeoffset'
    ]
    
    tables = schema.get('tables', [])
    for table_info in tables:
        table_name = table_info.get('name')
        columns = table_info.get('columns', [])
        
        table_candidates = []
        for col in columns:
            field_name = col.get('name', '')
            field_type = col.get('type', '')
            field_name_lower = field_name.lower()
            field_type_lower = field_type.lower()
            
            # Check if field name matches timestamp patterns
            is_name_match = any(pattern in field_name_lower for pattern in timestamp_patterns)
            
            # Check if field type is timestamp-like
            is_type_match = any(t in field_type_lower for t in timestamp_types)
            
            if is_name_match or is_type_match:
                # High confidence if both name and type match well-known patterns
                recommended = (
                    field_name_lower in ['updated_at', 'modified_at', 'last_modified']
                    and is_type_match
                )
                
                table_candidates.append({
                    "field": field_name,
                    "type": field_type,
                    "recommended": recommended
                })
        
        if table_candidates:
            candidates.append({
                "table": table_name,
                "candidates": table_candidates
            })
    
    return candidates


# Load optimization templates
TEMPLATES_PATH = os.path.join(os.path.dirname(__file__), 'optimization_templates.json')
OPTIMIZATION_TEMPLATES = {}
if os.path.exists(TEMPLATES_PATH):
    with open(TEMPLATES_PATH, 'r') as f:
        OPTIMIZATION_TEMPLATES = json.load(f)

# =============================================================================
# CONNECTOR CATEGORY DEFINITIONS (Extensible - no hardcoded connector lists!)
# =============================================================================
# Categories define operation mappings. New connectors declare their category,
# and automatically inherit the correct operations.

CATEGORY_OPERATIONS = {
    "relational_db": {
        "source_operations": ["export", "query", "discover_schema"],
        "destination_operations": ["import", "execute"],
        "default_source_op": "export",
        "default_dest_op": "import",
        "terminology": {"entity": "table", "record": "row", "field": "column"}
    },
    "document_db": {
        "source_operations": ["export", "query", "discover_schema"],
        "destination_operations": ["import", "upsert"],
        "default_source_op": "export",
        "default_dest_op": "import",
        "terminology": {"entity": "collection", "record": "document", "field": "field"}
    },
    "cloud_storage": {
        "source_operations": ["read", "list", "discover_schema"],
        "destination_operations": ["import_data", "write"],
        "default_source_op": "read",
        "default_dest_op": "import_data",
        "terminology": {"entity": "bucket", "record": "file", "field": "key"}
    },
    "data_warehouse": {
        "source_operations": ["query", "export", "discover_schema"],
        "destination_operations": ["load", "merge"],
        "default_source_op": "query",
        "default_dest_op": "load",
        "terminology": {"entity": "table", "record": "row", "field": "column"}
    },
    "streaming": {
        "source_operations": ["consume", "subscribe"],
        "destination_operations": ["produce", "publish"],
        "default_source_op": "consume",
        "default_dest_op": "produce",
        "terminology": {"entity": "topic", "record": "message", "field": "field"}
    },
    "api_saas": {
        "source_operations": ["fetch", "list", "discover_schema"],
        "destination_operations": ["push", "create", "update"],
        "default_source_op": "fetch",
        "default_dest_op": "push",
        "terminology": {"entity": "endpoint", "record": "record", "field": "field"}
    }
}

# Descriptions for the operation names the categories above declare, phrased with each
# category's own terminology ("table"/"row" for a database, "bucket"/"file" for storage).
# A connector that declares an operation but no description gets the text from here, so
# tools/list never publishes a name an agent has to guess the meaning of. Wording follows
# what the hand-written connectors already say for the same verbs.
STANDARD_OPERATION_DESCRIPTIONS = {
    "test_connection": "Test connectivity to {connector}",
    "validate_config": "Validate configuration without connecting",
    "discover_schema": "Discover available {entity}s and their schemas",
    "get_capabilities": "Return connector capabilities",
    "export": "Export {record}s from a {entity}",
    "query": "Run a read query against a {entity}",
    "read": "Read {record}s from a {entity}",
    "list": "List available {entity}s",
    "fetch": "Fetch {record}s from a {entity}",
    "consume": "Consume {record}s from a {entity}",
    "subscribe": "Subscribe to {record}s on a {entity}",
    "execute": "Execute a statement against the destination",
    "import_data": "Import {record}s into a {entity}",
    "import": "Import {record}s into a {entity}",
    "write": "Write {record}s to a {entity}",
    "load": "Bulk load {record}s into a destination {entity}",
    "merge": "Merge {record}s into a destination {entity} (upsert semantics)",
    "upsert": "Upsert {record}s (CDC insert/update events)",
    "upsert_data": "Upsert {record}s (CDC insert/update events)",
    "push": "Push {record}s to a {entity}",
    "create": "Create a {record} on a {entity}",
    "update": "Update a {record} on a {entity}",
    "produce": "Produce {record}s to a {entity}",
    "publish": "Publish {record}s to a {entity}",
    "ensure_table": "Ensure the destination {entity} exists (DDL)",
    "drop_table": "Drop the destination {entity} (used by run_mode=reload to rebuild schema)",
}


class BaseMCPConnector(ABC):
    """
    Abstract base class for all MCP connectors.
    Ensures uniform interface for AI agent interaction.
    Includes built-in optimization helpers.
    Includes automatic trace_id propagation.
    Includes OAuth support for API connectors.
    
    GENERIC DESIGN:
    - Subclasses declare their category via self.connector_category
    - Operations are derived from category, not hardcoded
    - Capabilities discoverable via get_capabilities()
    - trace_id automatically handled - no manual code needed!
    
    TRACE_ID:
    - Automatically extracted from requests in handle_request()
    - Available via get_trace_id() function
    - Included in all log messages automatically
    
    OAUTH SUPPORT:
    - Set self.auth_type = "oauth" to enable OAuth
    - Set self.oauth_provider = "hubspot" (or other provider name)
    - Use self._get_access_token() to get the current token
    - Use self._get_auth_headers() to get auth headers for API requests
    - Falls back to API key if OAuth token not available
    """
    
    def __init__(self):
        self.connector_name = self.__class__.__name__
        self.connector_type = "unknown"  # Override in subclass (e.g., "mysql", "aws-s3")
        self.connector_category = "relational_db"  # Override: relational_db, document_db, cloud_storage, etc.
        self.supported_operations = []  # Override with actual supported operations
        self.supports_source = True  # Can be used as a source
        self.supports_destination = False  # Can be used as a destination (override if true)
        self.optimization_category = None  # Auto-detected or manually set
        self._performance_target_ms = 2000  # Target: < 2 seconds
        
        # Capability Stats (standardized for Smart Planner)
        self.max_batch_size = 10000
        self.supported_formats = ["json"] # default, override in subclass
        # Move-capability modalities (universal-blob-passthrough plan §2). Distinct
        # from supported_formats (serialization encodings): this declares WHAT KIND of
        # data the connector can move. Default = structured-only; object-storage
        # connectors override to ["structured", "blob"]. The orchestrator capability
        # gate keys off this field; a connector with only "structured" is never
        # offered a blob (raw-bytes) move.
        self.supported_modalities = ["structured"]
        self.supports_cdc = False

        # =========================================================================
        # OAUTH SUPPORT (Generic for all API connectors)
        # =========================================================================
        self.auth_type = "api_key"  # Override: "oauth", "api_key", "basic", "none"
        self.oauth_provider = None  # Override: "hubspot", "salesforce", etc.
        self.connection_id = None  # Set at runtime from config/env
        self._oauth_token = None  # Cached OAuth token
        self._token_manager = None  # Lazy-loaded TokenManager instance
        
        # API key fallback (used if OAuth not available)
        self.api_key = os.getenv('MCP_API_KEY', '')

        # =========================================================================
        # RATE LIMITING (Generic for API connectors; safe defaults)
        # =========================================================================
        try:
            rps = float(os.getenv("MCP_RATE_LIMIT_RPS", "10"))
        except Exception:
            rps = 10.0
        try:
            max_retries = int(os.getenv("MCP_RATE_LIMIT_MAX_RETRIES", "3"))
        except Exception:
            max_retries = 3
        self._rate_limiter = RateLimitHandler(requests_per_second=rps, max_retries=max_retries)
        
        # Setup traced logging for this connector instance
        self._logger = logging.getLogger(f"mcp.{self.connector_type}")
        setup_traced_logging(self._logger, self.connector_name)
        
        logger.info(f"{self.connector_name} initialized")
    
    # =========================================================================
    # OAUTH HELPER METHODS (Generic for all connectors)
    # =========================================================================
    
    def _get_token_manager(self):
        """
        Lazy-load the TokenManager instance.
        Only loaded when OAuth is actually used.
        """
        if self._token_manager is None:
            try:
                from oauth.token_manager import TokenManager
                self._token_manager = TokenManager()
                self.log("TokenManager loaded for OAuth support")
            except ImportError:
                # oauth module is optional — connectors that manage their own auth
                # (e.g. Google Sheets, Shopify) handle tokens via their own helpers
                # and never call _get_token_manager() in a hot path.
                # Log at DEBUG so the noise doesn't pollute production logs.
                self.log("OAuth module not available - using API key fallback", level="debug")
                return None
        return self._token_manager
    
    def _get_access_token(self) -> Optional[str]:
        """
        Get the current access token for API requests.
        
        Priority:
        1. OAuth token from TokenManager (if auth_type == "oauth")
        2. API key from environment variable (fallback)
        
        Returns:
            Access token string or None if not available
        """
        # If not using OAuth, return API key
        if self.auth_type != "oauth":
            return self.api_key or None
        
        # Try to get OAuth token
        if self.connection_id:
            manager = self._get_token_manager()
            if manager:
                token = manager.get_valid_token(
                    self.connection_id,
                    refresh_callback=self._create_refresh_callback()
                )
                if token:
                    self._oauth_token = token
                    return token.access_token
        
        # Check for cached token
        if self._oauth_token and not self._oauth_token.is_expired():
            return self._oauth_token.access_token
        
        # Fallback to API key
        if self.api_key:
            self.log("OAuth token not available, falling back to API key", level="warning")
            return self.api_key
        
        return None
    
    def _create_refresh_callback(self):
        """
        Create a refresh callback for the TokenManager.
        Uses the oauth_registry to get provider-specific refresh logic.
        """
        if not self.oauth_provider:
            return None
        
        try:
            from oauth.token_manager import create_refresh_callback
            return create_refresh_callback(self.oauth_provider)
        except ImportError:
            return None
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """
        Get authentication headers for API requests.
        
        Supports:
        - OAuth Bearer token
        - API key (Bearer)
        - Basic auth
        - No auth
        
        Returns:
            Dictionary of HTTP headers
        """
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        if self.auth_type == "none":
            return headers
        
        token = self._get_access_token()
        if not token:
            self.log("No access token available for authentication", level="error")
            return headers
        
        if self.auth_type in ["oauth", "bearer", "api_key"]:
            headers["Authorization"] = f"Bearer {token}"
        elif self.auth_type == "basic":
            import base64
            # For basic auth, api_key is expected to be "username:password"
            encoded = base64.b64encode(token.encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"
        elif self.auth_type == "header":
            # Custom header-based auth (e.g., X-API-Key)
            headers["X-API-Key"] = token
        
        return headers

    # =========================================================================
    # HTTP REQUEST HELPERS (Backward compatible + enhanced v2 signature)
    # =========================================================================

    def _make_request_v2(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0,
        form_encoded: bool = False,
    ) -> Tuple[bool, int, Any, Dict[str, str]]:
        """
        New request helper for generated connectors.

        Returns: (success, status_code, data, headers)
        - success: True for HTTP 2xx
        - data: parsed JSON when possible, else text/None
        - headers: response headers (lower/upper as provided by requests)

        Behaviors:
        - Rate limiting: default 10 req/s (configurable by env)
        - 429: respects Retry-After and retries boundedly
        - OAuth: on 401, attempts refresh via TokenManager refresh callback and retries once
        """
        import requests

        # Robustness: a generated connector whose base_url resolves EMPTY
        # (e.g. a Dockerfile bakes `ENV MCP_BASE_URL=""` and the connector uses
        # `os.getenv('MCP_BASE_URL', default)`, which returns "" — not the
        # default — for a set-but-empty var) builds a RELATIVE request URL like
        # "/pokemon". That is neither an SSRF attempt (no host to reach) nor a
        # URL `requests` can send (it raises "No scheme supplied"). Surface it
        # as a clear, actionable CONFIGURATION error so the operator fixes
        # base_url, instead of a misleading SSRF `scheme=''` block or an opaque
        # requests exception. This runs BEFORE the SSRF check because a relative
        # URL can never reach the network — classifying it as SSRF is wrong.
        try:
            _req_parts = urlparse(url if isinstance(url, str) else "")
        except Exception:
            _req_parts = None
        if _req_parts is None or not _req_parts.scheme or not _req_parts.netloc:
            self.log(
                "request URL is not absolute (no scheme/host) — base_url is "
                "unset or empty; set 'base_url' in the connection config or "
                "the MCP_BASE_URL environment variable",
                level="error",
            )
            return False, 0, {
                "error": (
                    "base_url is not configured: the resolved request URL is "
                    "relative (no scheme/host). Set 'base_url' in the connection "
                    "config or the MCP_BASE_URL environment variable."
                )
            }, {}

        # SSRF guard (T1-4): refuse to send the connector's auth
        # header to internal services / private IPs / loopback /
        # link-local. The check runs on EVERY request (not cached)
        # because DNS rebinding is a known bypass — the first
        # resolve may return a public IP and a subsequent one a
        # private. See _ssrf_check_url docstring for the rationale.
        ssrf_reason = _ssrf_check_url(url)
        if ssrf_reason is not None:
            self.log(f"🚫 SSRF guard blocked outbound call: {ssrf_reason} (url redacted)", level="error")
            # Don't log the full URL — it may contain query-string
            # secrets. The hostname is in the reason above.
            return False, 0, {"error": f"SSRF guard: {ssrf_reason}"}, {}

        req_headers = dict(headers or {})
        # If caller didn't provide auth header, add generic auth headers from base connector.
        if "Authorization" not in req_headers and self.auth_type in ["oauth", "bearer", "api_key", "basic", "header", "none"]:
            req_headers = {**self._get_auth_headers(), **req_headers}

        attempt = 0
        max_attempts = (getattr(self, "_rate_limiter", None).max_retries if getattr(self, "_rate_limiter", None) else 3) + 1
        refreshed = False

        while attempt < max_attempts:
            attempt += 1
            # Rate limiting (sync)
            try:
                if getattr(self, "_rate_limiter", None):
                    self._rate_limiter.wait_if_needed_sync()
            except Exception:
                pass

            try:
                # allow_redirects=False + manual bounded follow: the default
                # requests behavior would follow a 30x from a validated public
                # host to http://169.254.169.254/… (IMDS) or an internal IP
                # WITH the connector's auth header attached, bypassing the
                # _ssrf_check_url() gate above. We re-run the SSRF check on
                # every Location before following it. (See _ssrf_guarded_get
                # pattern in tool_generator/discoverer.py.)
                # form_encoded: send the body as application/x-www-form-urlencoded
                # (requests form-encodes a dict passed to data= and sets the header)
                # for APIs that require it and ignore a JSON body (Twilio classic,
                # Stripe). Default False → json= (unchanged for every other caller).
                _body_kwargs = (
                    {"data": json_data} if (form_encoded and json_data is not None)
                    else {"json": json_data}
                )
                resp = requests.request(
                    method=method,
                    url=url,
                    headers=req_headers,
                    params=params,
                    timeout=timeout,
                    allow_redirects=False,
                    **_body_kwargs,
                )

                redirect_hops = 0
                _MAX_REDIRECTS = 5
                while resp.is_redirect and redirect_hops < _MAX_REDIRECTS:
                    location = resp.headers.get("Location")
                    if not location:
                        break
                    # Resolve relative Locations against the current URL.
                    next_url = urljoin(resp.url, location)
                    redirect_reason = _ssrf_check_url(next_url)
                    if redirect_reason is not None:
                        self.log(
                            f"🚫 SSRF guard blocked redirect target: {redirect_reason} (url redacted)",
                            level="error",
                        )
                        return False, 0, {"error": f"SSRF guard (redirect): {redirect_reason}"}, {}
                    redirect_hops += 1
                    # params/json belong to the original request only; a 30x
                    # Location is a fully-formed URL, so re-issue a bare GET-style
                    # follow with the (already SSRF-validated) headers. Strip
                    # credential headers on a cross-host redirect so auth never
                    # leaks to a third-party host.
                    follow_headers = _headers_safe_for_redirect(req_headers, resp.url, next_url)
                    resp = requests.request(
                        method=method,
                        url=next_url,
                        headers=follow_headers,
                        timeout=timeout,
                        allow_redirects=False,
                    )
                if resp.is_redirect:
                    self.log("HTTP redirect budget exhausted; refusing to follow further", level="error")
                    return False, 0, {"error": "too many redirects"}, {}

                # 429 handling
                if resp.status_code == 429 and getattr(self, "_rate_limiter", None):
                    wait_time = self._rate_limiter.handle_429_response(dict(resp.headers)) or (2 ** (attempt - 1))
                    if attempt < max_attempts:
                        time.sleep(min(float(wait_time), 60.0))
                        continue

                # 401 handling (OAuth refresh retry once)
                if resp.status_code == 401 and self.auth_type == "oauth" and not refreshed and self.connection_id:
                    manager = self._get_token_manager()
                    refresh_cb = self._create_refresh_callback()
                    if manager and refresh_cb:
                        try:
                            old_token = manager.get_token(self.connection_id)
                            if old_token and getattr(old_token, "refresh_token", None):
                                self.log("401 received; forcing OAuth refresh + retry", level="warning")
                                new_token = refresh_cb(old_token)
                                manager.store_token(self.connection_id, new_token)
                                self._oauth_token = new_token
                                req_headers["Authorization"] = f"Bearer {new_token.access_token}"
                                refreshed = True
                                continue
                        except Exception as e:
                            self.log(f"OAuth refresh failed: {_scrub_url_secrets(e)}", level="warning")

                data: Any = None
                try:
                    if resp.content:
                        data = resp.json()
                except Exception:
                    try:
                        data = resp.text
                    except Exception:
                        data = None

                success = 200 <= resp.status_code < 300
                return success, int(resp.status_code), data, dict(resp.headers)
            except requests.exceptions.Timeout:
                return False, 0, None, {}
            except Exception as e:
                # requests exception text embeds the full URL incl. query-string
                # credentials (api_key_query auth) — scrub before logging.
                self.log(f"HTTP request error: {_scrub_url_secrets(e)}", level="error")
                return False, 0, None, {}

        return False, 0, None, {}

    def _make_request(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0,
    ) -> Any:
        """
        Legacy request helper (kept for backward compatibility).
        Returns data, raises on non-2xx.
        """
        success, status_code, data, _headers = self._make_request_v2(
            method,
            url,
            headers=headers,
            params=params,
            json_data=json_data,
            timeout=timeout,
        )
        if not success:
            raise RuntimeError(f"HTTP {status_code}: {data}")
        return data
    
    def set_oauth_token(self, token_data: Dict[str, Any]) -> None:
        """
        Set OAuth token directly (e.g., from connection config).
        
        Args:
            token_data: Token dictionary with access_token, refresh_token, expires_at, etc.
        """
        try:
            from oauth.token_manager import OAuthToken
            self._oauth_token = OAuthToken.from_dict(token_data)
            self._oauth_token.provider = self.oauth_provider
            self.auth_type = "oauth"
            self.log(f"OAuth token set for {self.oauth_provider}")
        except ImportError:
            self.log("OAuth module not available", level="error")
    
    # Per-connection auth/endpoint state. configure_from_connection() overwrites
    # these, but only conditionally, so they must be reset between connections.
    _CONNECTION_STATE_FIELDS = ('api_key', 'auth_type', 'base_url', '_oauth_token')

    def _reset_connection_state(self) -> None:
        """Restore per-connection auth/endpoint state to its pristine defaults.

        create_http_app() builds ONE connector instance per process and
        _handle_tool_call() re-runs configure_from_connection() on that shared
        instance for every request. Every assignment in that method is guarded,
        so without this reset a connection that omits a field keeps the PREVIOUS
        connection's value -- one tenant's api_key / auth_type / _oauth_token /
        base_url surviving into the next tenant's request.

        The pristine values are snapshotted lazily on first use rather than in
        __init__ so the snapshot reflects what __init__ actually produced -- base
        AND subclass. That matters: the base seeds api_key from MCP_API_KEY, but
        base_url is resolved by the GENERATED subclass's __init__ (from
        MCP_BASE_URL, falling back to its spec endpoint), which has not run yet
        when the base __init__ does. A blind reset to None would break every
        env-configured deployment and blank the spec endpoint, which is why this
        is snapshot-and-restore rather than a wipe.
        """
        snapshot = self.__dict__.get('_pristine_connection_state')
        if snapshot is None:
            snapshot = {f: getattr(self, f, None) for f in self._CONNECTION_STATE_FIELDS}
            self._pristine_connection_state = snapshot
        for field, value in snapshot.items():
            setattr(self, field, value)

    def configure_from_connection(self, connection_config: Dict[str, Any]) -> None:
        """
        Configure connector from a connection configuration.
        Automatically detects and sets up OAuth if present.
        
        Args:
            connection_config: Configuration dict from connections table
        """
        # Set connection ID
        self.connection_id = connection_config.get('id') or connection_config.get('connection_id')

        # Drop the previous connection's auth/endpoint state before applying this
        # one's. Everything below is conditional -- the if/elif chain has no
        # else, and base_url is written only when present -- so on this
        # process-global instance an omitted field would otherwise inherit the
        # last connection's value. See KI-RESTMCP-STICKY-BASE-URL.
        self._reset_connection_state()

        # Check for OAuth token in config
        if 'oauth_token' in connection_config:
            self.set_oauth_token(connection_config['oauth_token'])
        elif 'access_token' in connection_config:
            self.set_oauth_token(connection_config)
        elif 'api_key' in connection_config:
            self.api_key = connection_config['api_key']
            self.auth_type = "api_key"
        
        # Set other config values
        if 'base_url' in connection_config:
            self.base_url = connection_config['base_url']
        
        self.log(f"Configured from connection: {self.connection_id}, auth_type: {self.auth_type}")
    
    def is_oauth_configured(self) -> bool:
        """Check if OAuth is properly configured."""
        if self.auth_type != "oauth":
            return False
        
        if self._oauth_token and not self._oauth_token.is_expired():
            return True
        
        if self.connection_id:
            manager = self._get_token_manager()
            if manager:
                token = manager.get_token(self.connection_id)
                return token is not None and not token.is_expired()
        
        return False
    
    def log(self, message: str, level: str = "info") -> None:
        """
        Log a message with automatic trace_id inclusion.
        
        Usage in subclass:
            self.log("Operation completed successfully")
            self.log("Error occurred", level="error")
        
        Args:
            message: Log message
            level: Log level (debug, info, warning, error)
        """
        log_func = getattr(self._logger, level, self._logger.info)
        log_func(message)
    
    def get_current_trace_id(self) -> str:
        """
        Get the current trace_id for this request.
        Useful for including in response payloads or external API calls.
        
        Returns:
            Current trace_id or 'no-trace-id' if not set
        """
        return get_trace_id()
    
    # ============================================================================
    # OPTIMIZATION HELPERS (Generic for all connectors)
    # ============================================================================
    
    def _measure_performance(self, operation_name: str):
        """Context manager to measure operation performance"""
        class PerformanceMeasurer:
            def __init__(self, connector, op_name):
                self.connector = connector
                self.op_name = op_name
                self.start_time = None
            
            def __enter__(self):
                self.start_time = time.time()
                return self
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                duration_ms = (time.time() - self.start_time) * 1000
                logger.info(f"[{self.connector.connector_name}] {self.op_name} took {duration_ms:.2f}ms")
                
                # Warn if slow
                if duration_ms > self.connector._performance_target_ms:
                    logger.warning(
                        f"⚠️ {self.op_name} is SLOW ({duration_ms:.2f}ms > target {self.connector._performance_target_ms}ms). "
                        f"Consider optimization! Check {self.connector.connector_type} in optimization_templates.json"
                    )
        
        return PerformanceMeasurer(self, operation_name)
    
    def _apply_limits(self, params: Dict, defaults: Dict = None) -> Dict:
        """
        Apply sensible limits to prevent slow queries on large systems.
        Generic for all connector types.
        """
        defaults = defaults or {}
        
        # Get category-specific defaults from templates
        if self.optimization_category and self.optimization_category in OPTIMIZATION_TEMPLATES:
            template_params = OPTIMIZATION_TEMPLATES[self.optimization_category].get('parameters', {})
            defaults.update(template_params)
        
        # Apply defaults
        limits = {
            'max_tables': params.get('max_tables', defaults.get('max_tables', 100)),
            'max_collections': params.get('max_collections', defaults.get('max_collections', 100)),
            'max_buckets': params.get('max_buckets', defaults.get('max_buckets', 10)),
            'max_files': params.get('max_files', defaults.get('max_files', 50)),
            'sample_size': params.get('sample_size', defaults.get('sample_size', 20)),
            'include_row_counts': params.get('include_row_counts', True)
        }
        
        return limits
    
    def _get_optimization_tips(self) -> List[str]:
        """Get optimization tips for this connector category"""
        if not self.optimization_category:
            return ["Set self.optimization_category in __init__ for category-specific tips"]
        
        if self.optimization_category not in OPTIMIZATION_TEMPLATES:
            return [f"No optimization template found for category: {self.optimization_category}"]
        
        template = OPTIMIZATION_TEMPLATES[self.optimization_category]
        return template.get('principles', [])
    
    def _check_anti_patterns(self, code_snippet: str = None) -> List[str]:
        """
        Check for common anti-patterns that slow down schema discovery.
        Can be used during development/testing.
        """
        if not self.optimization_category or self.optimization_category not in OPTIMIZATION_TEMPLATES:
            return []
        
        template = OPTIMIZATION_TEMPLATES[self.optimization_category]
        anti_patterns = template.get('anti_patterns', [])
        
        return [f"⚠️ Avoid: {pattern}" for pattern in anti_patterns]
    
    @abstractmethod
    def test_connection(self, params: Dict = None) -> Dict[str, Any]:
        """
        Test connectivity to the data source.
        
        Returns:
            {
                "success": bool,
                "message": str,
                "error": str (optional),
                "version": str (optional)
            }
        """
        pass
    
    @abstractmethod
    def discover_schema(self, params: Dict = None) -> Dict[str, Any]:
        """
        Discover tables/collections and their schemas.
        
        PERFORMANCE TARGET: < 2 seconds for 100+ tables
        
        Use self._measure_performance() and self._apply_limits() helpers!
        
        Example:
            with self._measure_performance("discover_schema"):
                limits = self._apply_limits(params)
                # Your optimized discovery code here...
        
        Returns:
            {
                "success": bool,
                "tables": [
                    {
                        "name": str,
                        "schema": str (optional),
                        "columns": [
                            {
                                "name": str,
                                "type": str,
                                "nullable": bool
                            }
                        ],
                        "row_count": int (optional)
                    }
                ],
                "total_tables": int,
                "error": str (optional)
            }
        """
        pass
    
    @abstractmethod
    def validate_config(self, params: Dict = None) -> Dict[str, Any]:
        """
        Validate configuration without connecting.
        
        Returns:
            {
                "success": bool,
                "valid": bool,
                "errors": List[str],
                "warnings": List[str]
            }
        """
        pass
    
    @abstractmethod
    def export(self, params: Dict) -> Dict[str, Any]:
        """
        Export data from the source.
        
        Returns:
            {
                "success": bool,
                "data": List[Dict],
                "columns": List[str],
                "row_count": int,
                "has_more": bool,
                "error": str (optional)
            }
        """
        pass
    
    def import_data(self, params: Dict) -> Dict[str, Any]:
        """
        Import data into the destination (optional for sources).
        
        Returns:
            {
                "success": bool,
                "rows_inserted": int,
                "rows_updated": int,
                "error": str (optional)
            }
        """
        return {
            "success": False,
            "error": f"{self.connector_type} does not support import (source-only connector)"
        }
    
    def get_schema(self, params: Dict) -> Dict[str, Any]:
        """
        Get detailed schema for a specific table.
        Default implementation calls discover_schema and filters.
        """
        table_name = params.get('table')
        if not table_name:
            return {"success": False, "error": "Missing 'table' parameter"}
        
        # Get all schemas
        all_schemas = self.discover_schema(params)
        if not all_schemas.get('success'):
            return all_schemas
        
        # Filter for requested table
        for table in all_schemas.get('tables', []):
            if table.get('name') == table_name:
                return {
                    "success": True,
                    "table": table_name,
                    "schema": table.get('columns', [])
                }
        
        return {
            "success": False,
            "error": f"Table '{table_name}' not found"
        }
    
    def get_capabilities(self, params: Dict = None) -> Dict[str, Any]:
        """
        Return full capability metadata for this connector.
        Used by Planner/Validator/Executor to understand what this connector can do.
        
        THIS IS THE KEY METHOD FOR GENERIC AGENT INTERACTION.
        Agents should NEVER hardcode connector types - they query capabilities instead.
        """
        category_info = CATEGORY_OPERATIONS.get(self.connector_category, CATEGORY_OPERATIONS["relational_db"])
        
        # Build operation list based on category and actual implementation
        operations = []
        
        # Always include core operations
        core_ops = ["test_connection", "discover_schema", "validate_config"]
        for op in core_ops:
            operations.append({
                "name": op,
                "method": f"{self.connector_type}_{op}",
                "description": f"{op.replace('_', ' ').title()} for {self.connector_type}",
                "type": "core"
            })
        
        # Add source operations if supported
        if self.supports_source:
            for op in category_info.get("source_operations", []):
                if op not in ["discover_schema"]:  # Already added
                    operations.append({
                        "name": op,
                        "method": f"{self.connector_type}_{op}",
                        "description": f"{op.replace('_', ' ').title()} from {self.connector_type}",
                        "type": "source"
                    })
        
        # Add destination operations if supported
        if self.supports_destination:
            for op in category_info.get("destination_operations", []):
                operations.append({
                    "name": op,
                    "method": f"{self.connector_type}_{op}",
                    "description": f"{op.replace('_', ' ').title()} to {self.connector_type}",
                    "type": "destination"
                })
        
        # Runtime version is supplied by deployer (tool-generator / orchestrator) via env.
        runtime_version = (os.getenv("MCP_CONNECTOR_VERSION") or os.getenv("CONNECTOR_VERSION") or "").strip()
        if runtime_version and not runtime_version.startswith("v"):
            runtime_version = f"v{runtime_version}"
        
        return {
            "success": True,
            "connector_type": self.connector_type,
            "connector_category": self.connector_category,
            "connector_version": runtime_version or None,
            "supports_source": self.supports_source,
            "supports_destination": self.supports_destination,
            "default_source_operation": category_info.get("default_source_op", "export"),
            "default_destination_operation": category_info.get("default_dest_op", "import"),
            "terminology": category_info.get("terminology", {}),
            "operations": operations,
            "optimization_category": self.optimization_category,
            "performance_target_ms": self._performance_target_ms,
            "capabilities": {
                "max_batch_size": self.max_batch_size,
                "supported_formats": self.supported_formats,
                "supported_modalities": self.supported_modalities,
                "supports_cdc": self.supports_cdc
            }
        }
    
    def _resolves_to_handler(self, method: str) -> bool:
        """Would handle_invoke find a handler for this tool name?

        Mirrors handle_invoke exactly - strip the "{connector_type}_" prefix, refuse an
        underscore-prefixed remainder, then look for a callable attribute - so that
        "advertised" and "callable" cannot drift apart.
        """
        handler = method or ""
        prefix = f"{self.connector_type}_"
        if handler.startswith(prefix):
            handler = handler[len(prefix):]
        if not handler or handler.startswith("_"):
            return False
        return callable(getattr(self, handler, None))

    def _default_operation_description(self, op: Dict[str, Any]) -> str:
        """Description for a declaration that omitted one.

        Six connectors (bigquery, clickhouse, databricks, snowflake, redshift and
        shopify-admin-graphql) declare their operations with no description at all, so
        this is the only text an agent gets for them. Standard verbs are answered from
        STANDARD_OPERATION_DESCRIPTIONS in this connector's own terminology; anything
        genuinely custom falls back to its name, which is still better than nothing.
        """
        name = str(op.get("name") or op.get("method") or "operation")
        prefix = f"{self.connector_type}_"
        if name.startswith(prefix):
            name = name[len(prefix):]

        terminology = CATEGORY_OPERATIONS.get(
            getattr(self, "connector_category", None) or "", {}
        ).get("terminology", {})
        template = STANDARD_OPERATION_DESCRIPTIONS.get(name)
        if template:
            return template.format(
                connector=self.connector_type,
                entity=terminology.get("entity", "resource"),
                record=terminology.get("record", "record"),
            )
        # Deliberately terminology-free: a connector that mis-declares its category
        # (shopify-admin-graphql calls itself relational_db) would otherwise publish the
        # wrong noun to a planner for every custom operation it exposes.
        return f"{name.replace('_', ' ').title()} operation exposed by {self.connector_type}"

    def _advertisable_operations(self, operations) -> List[Dict[str, Any]]:
        """The subset of declared operations fit to publish as MCP tools.

        Every connector hand-builds its own `operations` list, and two failures came out
        of that which tools/list used to pass straight through to the caller:

        - an entry missing "description"/"type" raised KeyError out of list_tools, so a
          single malformed entry failed the ENTIRE tool surface rather than itself;
        - an entry naming a method the connector does not implement (a wrong prefix, a
          renamed handler) was published anyway, and handle_invoke then answered the
          call with "Unknown tool" - an agent plans around a tool that cannot run.

        Publishing is therefore gated on the same predicate handle_invoke dispatches on.
        A withheld tool is logged, never silently dropped.
        """
        advertisable = []
        unresolved = []
        for op in operations or []:
            if not isinstance(op, dict) or not op.get("method"):
                unresolved.append(repr(op)[:60])
            elif self._resolves_to_handler(op["method"]):
                advertisable.append(op)
            else:
                unresolved.append(op["method"])

        if unresolved:
            log = getattr(self, "_logger", logger)
            log.warning(
                "%s: withholding %d declared operation(s) from tools/list - no handler "
                "resolves for: %s", self.connector_type, len(unresolved),
                ", ".join(sorted(unresolved)),
            )
        return advertisable

    def list_tools(self, params: Dict = None) -> Dict[str, Any]:
        """
        List available tools for this connector (MCP protocol standard).
        Enhanced to include full capability metadata for generic agent interaction.
        """
        capabilities = self.get_capabilities()
        
        tools = []
        for op in self._advertisable_operations(capabilities.get("operations", [])):
            tools.append({
                "name": op["method"],
                "description": op.get("description") or self._default_operation_description(op),
                "operation_type": op.get("type", "core"),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "config": {"type": "object", "description": "Connection configuration"},
                        "params": {"type": "object", "description": "Operation parameters"}
                    }
                }
            })
        
        return {
            "tools": tools,
            "capabilities": capabilities  # Include full capabilities for agents
        }
    
    # ============================================================================
    # STAGING HELPERS (Claim Check Pattern)
    # ============================================================================
    
    def _get_staging_client(self, config: Dict):
        """Get S3 client for staging operations"""
        try:
            import boto3
            from botocore.config import Config as BotoConfig
        except ImportError:
            self.log("boto3 not installed - staging not supported", level="error")
            return None
        
        endpoint = config.get('endpoint') or config.get('endpoint_url')
        access_key = config.get('access_key') or config.get('aws_access_key_id')
        secret_key = config.get('secret_key') or config.get('aws_secret_access_key')
        region = config.get('region') or config.get('aws_region', 'us-east-1')

        # BOTH credentials absent means workload identity -- not a misconfiguration.
        #
        # On EKS/GKE/AKS the chart deliberately emits no MINIO_ACCESS_KEY_ID /
        # MINIO_SECRET_ACCESS_KEY at all: _helpers.tpl:659 gates that pair on
        # objectStorage.external.accessKeyId being set, and values.yaml:375-378
        # documents leaving both empty as the way to use IRSA / Workload Identity /
        # AAD Pod Identity, so boto3 reads a projected token instead. The
        # orchestrator honors that by leaving both empty in the staging config it
        # sends (blob_lane.go buildStagingConfig defaults them only against the
        # bundled MinIO endpoint). Rejecting "empty" here turned that supported
        # path into a hard failure one layer down: _stage_blob raises "blob
        # passthrough requires a claim-check staging client"
        # (object_storage_source.py:509-514) and every blob run on a
        # workload-identity install died before staging a single object.
        #
        # Exactly ONE of the pair set is still an error, and a louder one than
        # before: that is a half-written secret, and falling through to the default
        # chain would re-surface it later as a confusing permission denial.
        if bool(access_key) != bool(secret_key):
            self.log(
                "Staging credentials are half-configured: "
                + ("access_key" if access_key else "secret_key")
                + " is set and the other is not. Set both, or neither to use "
                "workload identity (IRSA / Workload Identity / AAD Pod Identity).",
                level="error",
            )
            return None

        is_minio = bool(endpoint) and ('localhost' in endpoint or 'minio' in endpoint or ':9000' in endpoint)

        client_kwargs = {'region_name': region}
        if access_key and secret_key:
            client_kwargs['aws_access_key_id'] = access_key
            client_kwargs['aws_secret_access_key'] = secret_key
        else:
            self.log("No static staging credentials in the staging config; using boto3's "
                     "default credential chain (workload identity / instance profile)",
                     level="info")

        # Pass the configured endpoint whether or not it looks like MinIO.
        #
        # The non-MinIO branch used to drop endpoint_url entirely. That is invisible
        # against real AWS -- boto3 rebuilds the same regional endpoint from
        # region_name -- and wrong everywhere else: objectStorage.mode=gcs|azure
        # renders MINIO_ENDPOINT_URL=https://storage.googleapis.com (or the Azure S3
        # gateway), neither of which matches the is_minio substring test, so the
        # client silently addressed AWS S3 instead of the provider the operator
        # configured. is_minio decides addressing style, not whether to honor the
        # endpoint.
        if endpoint:
            client_kwargs['endpoint_url'] = endpoint
        if is_minio:
            client_kwargs['config'] = BotoConfig(signature_version='s3v4',
                                                 s3={'addressing_style': 'path'})
        else:
            client_kwargs['config'] = BotoConfig(signature_version='s3v4')

        return boto3.client('s3', **client_kwargs)

    def upload_to_staging(self, data: Any, staging_config: Dict) -> Dict[str, Any]:
        """
        Upload data to staging (MinIO/S3) and return a reference.
        Used by connectors to offload large payloads.
        """
        client = self._get_staging_client(staging_config)
        if not client:
            return {"success": False, "error": "Staging client initialization failed"}
            
        bucket = staging_config.get('bucket', 'staging')
        prefix = staging_config.get('prefix', 'exports')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        key = f"{prefix}/{self.connector_type}/{timestamp}.json"
        
        try:
            # Ensure serialization
            if not isinstance(data, (str, bytes)):
                content = json.dumps(data, default=str)
            else:
                content = data
                
            if isinstance(content, str):
                content = content.encode('utf-8')
                
            # Create bucket if valid (ignore errors for AWS)
            try:
                client.create_bucket(Bucket=bucket)
            except:
                pass
                
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=content,
                ContentType='application/json'
            )
            
            url = f"s3://{bucket}/{key}"
            self.log(f"Upload to staging successful: {url}")
            
            return {
                "success": True,
                "data_ref": url,
                "bucket": bucket,
                "key": key,
                "size": len(content)
            }
        except Exception as e:
            self.log(f"Staging upload failed: {e}", level="error")
            return {"success": False, "error": str(e)}

    def read_from_staging(self, data_ref: str, staging_config: Dict) -> Dict[str, Any]:
        """
        Read data from staging reference.
        Expects format: s3://bucket/key
        """
        if not data_ref.startswith("s3://"):
             return {"success": False, "error": "Invalid data_ref format (must be s3://)"}
             
        try:
            parts = data_ref[5:].split('/', 1)
            if len(parts) != 2:
                return {"success": False, "error": "Invalid s3 url"}
                
            bucket, key = parts
            
            client = self._get_staging_client(staging_config)
            if not client:
                return {"success": False, "error": "Staging client initialization failed"}
                
            response = client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()
            
            return {
                "success": True,
                "data": json.loads(content),
                "size": len(content)
            }
        except Exception as e:
            self.log(f"Staging read failed: {e}", level="error")
            return {"success": False, "error": str(e)}

    # ============================================================================
    # GENERIC SOURCE HELPERS (For ALL source connectors)
    # ============================================================================
    # These methods standardize parameter handling for source (export) operations.
    # All source connectors should use these to ensure consistent behavior for:
    # - entity name resolution (table/collection/bucket/topic)
    # - prefix handling for storage connectors
    # - config merging (plan params > connection config > defaults)
    # - staging upload (Claim Check pattern)
    # ============================================================================

    # ============================================================================
    # CONFIGURATION ENFORCEMENT
    #Keys in this list, if present in connection config, MUST override plan params.
    # ============================================================================
    PROTECTED_CONFIG_KEYS = [
        # Format & Encoding
        "format", "file_format", "output_format", "compression", "delimiter", "encoding", "header",
        # Cloud / Region
        "region", "zone", "endpoint", "endpoint_url",
        # Storage
        "bucket", "bucket_name", "container", "container_name",
        # Database / Warehouse
        "database", "schema", "warehouse", "account", "role", "ssl", "sslmode", "verify_ssl",
        # Network
        "host", "hostname", "port",
        # Auth
        "user", "username", "api_key", "token", "client_id"
    ]

    def _enforce_config_precedence(self, params: Dict) -> Dict:
        """
        Enforce connection config precedence for protected keys.
        Returns modified params with config values overwriting param values for protected keys.
        """
        config = params.get('config', {})
        if not config:
            return params
            
        for key in self.PROTECTED_CONFIG_KEYS:
            if key in config and config[key]:
                val = config[key]
                # If param has a conflicting value, log warning (only if different)
                if key in params and str(params[key]) != str(val):
                    _kl = str(key).lower()
                    if any(_s in _kl for _s in ("password", "passwd", "secret", "token", "credential", "api_key", "apikey", "access_key", "private_key")):
                        # SECURITY: never log credential values here — connector stdout ships to SigNoz.
                        # Mirrors the Go twin's security.IsSensitiveKey guard (backend-orchestrator/internal/mcp/client.go).
                        self.log(f"⚠️  Enforcing config for '{key}': overriding plan value with connection config (sensitive value masked)")
                    else:
                        self.log(f"⚠️  Enforcing config for '{key}': overriding plan value '{params[key]}' with '{val}'")
                
                # Overwrite/Set in params
                params[key] = val
        
        # 2. Enforce Aliases (Config Alias -> Param Canonical)
        # format/file_format/output_format -> format
        fmt = config.get('output_format') or config.get('file_format') or config.get('format')
        if fmt:
            if 'format' in params and str(params['format']) != str(fmt):
                self.log(f"⚠️  Enforcing config for 'format': overriding plan value '{params['format']}' with '{fmt}'")
            params['format'] = fmt

        # bucket/bucket_name -> bucket
        bucket = config.get('bucket_name') or config.get('bucketName') or config.get('bucket')
        if bucket:
            if 'bucket' in params and str(params['bucket']) != str(bucket):
                self.log(f"⚠️  Enforcing config for 'bucket': overriding plan value '{params['bucket']}' with '{bucket}'")
            params['bucket'] = bucket

        return params

    def prepare_source_params(self, params: Dict) -> Dict:
        """
        Prepare parameters for source operations (export, read, query, etc.).
        
        Merges connection config with plan params using proper precedence:
        1. Explicit plan params (params.table, params.prefix, etc.)
        2. Connection config (params.config.database, params.config.prefix)
        3. Connector defaults
        
        Returns dict with normalized keys:
        - config: original connection config for auth
        - table/collection/bucket: entity name based on connector category
        - limit, offset: pagination parameters
        - prefix: for storage connectors
        - where/filter: query conditions
        - staging_config: if staging is requested
        - columns: optional column selection
        """
        # PHASE 0: ENFORCE PROTECTED CONFIG
        params = self._enforce_config_precedence(params)

        config = params.get('config', {}) or {}
        # Normalize common UI/API aliases to canonical names
        # Frontend/API often uses: bucket_name, path_prefix, output_format
        if isinstance(config, dict):
            if 'bucket' not in config and 'bucket_name' in config:
                config['bucket'] = config.get('bucket_name')
            if 'prefix' not in config and 'path_prefix' in config:
                config['prefix'] = config.get('path_prefix')
            if 'file_format' not in config and 'output_format' in config:
                config['file_format'] = config.get('output_format')
            # Normalize port typing for DB connectors
            if 'port' in config and isinstance(config.get('port'), str):
                try:
                    config['port'] = int(config['port'])
                except Exception:
                    pass

        if 'port' in params and isinstance(params.get('port'), str):
            try:
                params['port'] = int(params['port'])
            except Exception:
                pass
            # Normalize port typing for DB connectors (common source of runtime errors).
            # Many DB drivers require an int port, but connection configs are often stored as strings.
            if 'port' in config and isinstance(config.get('port'), str):
                try:
                    config['port'] = int(config['port'])
                except Exception:
                    pass

        # Also normalize top-level port if present
        if 'port' in params and isinstance(params.get('port'), str):
            try:
                params['port'] = int(params['port'])
            except Exception:
                pass
        
        # Get category-specific terminology
        category_info = CATEGORY_OPERATIONS.get(
            getattr(self, 'connector_category', 'relational_db'),
            CATEGORY_OPERATIONS['relational_db']
        )
        terminology = category_info.get('terminology', {'entity': 'table'})
        entity_name = terminology.get('entity', 'table')
        
        # 1. Resolve entity name (table/collection/bucket/topic)
        # Priority: params.{entity} > params.table > config.{entity} > config.table
        entity_value = (
            params.get(entity_name) or
            params.get('table') or
            params.get('collection') or
            params.get('bucket') or
            params.get('topic') or
            config.get(entity_name) or
            config.get('table') or
            config.get('collection') or
            config.get('bucket')
        )
        
        # 2. Handle array of entities (use first one for single-entity operations)
        entities_key = entity_name + 's'  # tables, collections, buckets
        if not entity_value:
            entities = params.get(entities_key) or params.get('tables') or params.get('entities')
            if isinstance(entities, list) and len(entities) > 0:
                entity_value = entities[0]
        
        # 3. Pagination parameters
        limit = params.get('limit')
        if limit is None:
            limit = config.get('default_limit', 10000)
        offset = params.get('offset', 0)
        
        # 4. For storage connectors: handle prefix/key
        prefix = (
            params.get('prefix') or
            params.get('path_prefix') or
            params.get('pathPrefix') or
            config.get('prefix', '')
        )
        key = (
            params.get('key') or
            params.get('object_key') or
            params.get('objectKey') or
            config.get('key', '') or
            config.get('object_key') or
            config.get('objectKey', '')
        )
        
        # If prefix is set but no key, apply prefix to key pattern
        if prefix and not key:
            key = prefix.rstrip('/') + '/'
        
        # 5. Filter/where conditions
        where = params.get('where') or params.get('filter') or config.get('where', '')
        
        # 6. Column selection (optional)
        columns = params.get('columns') or config.get('columns', [])
        
        # 7. Staging config (for Claim Check pattern)
        staging_config = params.get('staging_config')
        
        # 8. Database-specific: database name
        database = params.get('database') or config.get('database')
        
        # Build result with entity using the correct terminology
        result = {
            'config': config,
            entity_name: entity_value,  # e.g., 'table': 'users'
            'table': entity_value,      # Also include as 'table' for backward compat
            'limit': limit,
            'offset': offset,
            'where': where,
            'filter': where,            # Alias
            'columns': columns,
            'staging_config': staging_config,
            'database': database,
            'prefix': prefix,
            'key': key,
            'success': True  # Mark as successfully prepared
        }
        
        # Log what we resolved
        self.log(f"📋 Source params: {entity_name}={entity_value}, limit={limit}, offset={offset}")
        
        return result
    
    def prepare_export_data(self, params: Dict) -> Dict:
        """
        Alias for prepare_source_params() - use for export operations.
        
        This is the recommended entry point for export() methods in source connectors.
        It handles:
        - Parameter normalization (table/collection/bucket terminology)
        - Config merging (plan params > connection config)
        - Staging setup (Claim Check pattern)
        
        Usage in connector:
            def export(self, params: Dict) -> Dict:
                prepared = self.prepare_export_data(params)
                if not prepared.get('success'):
                    return prepared
                
                table = prepared['table']
                limit = prepared['limit']
                config = prepared['config']
                # ... perform export ...
        """
        return self.prepare_source_params(params)
    
    def finalize_export_result(self, data: List[Dict], params: Dict, columns: List[str] = None) -> Dict:
        """
        Finalize export result, handling staging upload if requested.
        
        Call this at the end of export() to handle:
        - Staging upload (Claim Check pattern) if staging_config is present
        - Proper result formatting
        
        Args:
            data: The exported data (list of dicts)
            params: Original params (to check for staging_config)
            columns: Optional column names
        
        Returns:
            Properly formatted export result dict
        """
        staging_config = params.get('staging_config')
        table = params.get('table') or params.get('collection') or params.get('bucket')
        
        # If staging requested and we have data, upload to staging
        if staging_config and len(data) > 0:
            self.log(f"📤 Uploading {len(data)} rows to staging (Claim Check)...")
            staging_result = self.upload_to_staging(data, staging_config)
            
            if staging_result['success']:
                return {
                    "success": True,
                    "table": table,
                    "data_ref": staging_result['data_ref'],
                    "data": [],  # Don't return data inline when staged
                    "columns": columns or [],
                    "row_count": len(data),
                    "staged": True,
                    "staging_bucket": staging_result.get('bucket'),
                    "staging_key": staging_result.get('key')
                }
            else:
                # Staging failed, return data inline with warning
                self.log(f"⚠️ Staging failed: {staging_result.get('error')}, returning inline data", level="warning")
        
        # No staging, return data inline
        return {
            "success": True,
            "table": table,
            "data": data,
            "columns": columns or [],
            "row_count": len(data),
            "has_more": len(data) >= params.get('limit', 10000)
        }

    # ============================================================================
    # GENERIC DESTINATION HELPERS (For ALL destination connectors)
    # ============================================================================
    # These methods standardize parameter handling for destination operations.
    # All connectors should use these to ensure consistent behavior for:
    # - prefix/key generation
    # - file format (json, csv, jsonl, parquet)
    # - compression (none, gzip, snappy)
    # ============================================================================

    def prepare_destination_params(self, params: Dict) -> Dict:
        """
        Prepare final parameters for destination operations (import_data, write, etc.).
        
        Merges connection config with plan params using proper precedence:
        1. Explicit plan params (params.key, params.prefix, params.format)
        2. Connection config (params.config.prefix, params.config.file_format)
        3. Connector defaults
        
        Returns dict with normalized keys:
        - bucket: final bucket/container name
        - key: final object/file key (includes all prefixes)
        - format: output format (json, csv, jsonl, parquet)
        - compression: compression codec (none, gzip, snappy)
        - data: the actual data to write
        - config: original connection config for auth
        """
        # PHASE 0: ENFORCE PROTECTED CONFIG
        params = self._enforce_config_precedence(params)

        config = params.get('config', {}) or {}
        # Normalize common UI/API aliases to canonical names
        # Frontend/API often uses: bucket_name, path_prefix, output_format
        if isinstance(config, dict):
            if 'bucket' not in config and 'bucket_name' in config:
                config['bucket'] = config.get('bucket_name')
            if 'prefix' not in config and 'path_prefix' in config:
                config['prefix'] = config.get('path_prefix')
            if 'file_format' not in config and 'output_format' in config:
                config['file_format'] = config.get('output_format')
        
        # 1. Extract data from various possible locations
        # NOTE: empty lists are valid payloads (e.g., table exists but has 0 rows).
        # Do NOT use `or` here because it treats [] as falsy and incorrectly falls back to None.
        data = params.get('data', None)
        if data is None:
            data = params.get('rows', None)
        if data is None:
            data = params.get('source_data', None)
        if isinstance(data, dict) and 'data' in data:
            # Handle nested data from previous step output
            data = data.get('data', [])
        
        # 2. Determine bucket/container (plan param overrides config)
        bucket = (
            params.get('bucket') or
            params.get('bucket_name') or
            params.get('bucketName') or
            config.get('bucket') or
            config.get('bucket_name') or
            config.get('bucketName')
        )
        
        # 3. Determine format: plan.format > config.file_format > default
        format_val = (
            params.get('format') or
            params.get('file_format') or
            params.get('output_format') or
            params.get('outputFormat') or
            config.get('file_format') or
            config.get('output_format') or
            config.get('outputFormat') or
            config.get('format') or
            'json'
        ).lower()
        
        # Normalize format names
        format_map = {
            'json': 'json',
            'jsonl': 'jsonl', 
            'json_lines': 'jsonl',
            'ndjson': 'jsonl',
            'csv': 'csv',
            'parquet': 'parquet'
        }
        format_val = format_map.get(format_val, 'json')
        
        # 4. Determine compression: plan > config > default
        compression = (
            params.get('compression') or 
            config.get('compression') or 
            'none'
        ).lower()
        
        # 5. Build the full key/path with prefix chain
        key = self.generate_destination_key(params, config, format_val, compression)
        
        return {
            'bucket': bucket,
            'key': key,
            'format': format_val,
            'compression': compression,
            'data': data,
            'config': config,
            'content_type': self._get_content_type(format_val, compression)
        }
    
    def generate_destination_key(self, params: Dict, config: Dict, format_val: str, compression: str) -> str:
        """
        Generate the full destination key/path including all prefixes.
        
        Key structure: {config.prefix}{params.prefix}{filename}.{format}[.{compression}]
        
        Examples:
        - config.prefix='test-s3/', params.prefix='pipelines/abc/' 
          -> test-s3/pipelines/abc/data_20231213_120000.csv
        - config.prefix='', params.prefix='exports/'
          -> exports/data_20231213_120000.json.gz
        """
        from datetime import datetime
        
        # If explicit key provided, use it directly
        explicit_key = (
            params.get('key') or
            params.get('object_key') or
            params.get('objectKey') or
            config.get('key') or
            config.get('object_key') or
            config.get('objectKey')
        )
        if explicit_key:
            return explicit_key
        
        # Build prefix chain (config prefix + plan prefix)
        config_prefix = (config.get('prefix') or config.get('path_prefix') or config.get('pathPrefix') or '').strip()
        params_prefix = (params.get('prefix') or params.get('path_prefix') or params.get('pathPrefix') or '').strip()
        
        # Normalize prefixes (ensure they end with / if non-empty)
        if config_prefix and not config_prefix.endswith('/'):
            config_prefix += '/'
        if params_prefix and not params_prefix.endswith('/'):
            params_prefix += '/'
            
        full_prefix = config_prefix + params_prefix
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Determine file extension
        ext_map = {
            'json': 'json',
            'jsonl': 'jsonl',
            'csv': 'csv',
            'tsv': 'tsv',
            'parquet': 'parquet',
            'avro': 'avro',
            'orc': 'orc',
            'arrow': 'arrow',
            'xlsx': 'xlsx',
        }
        ext = ext_map.get(format_val, format_val)
        
        if compression and compression != 'none':
            compression_ext_map = {
                'gzip': 'gz',
                'bzip2': 'bz2',
                'snappy': 'snappy',
                'lz4': 'lz4',
                'zstd': 'zst',
            }
            ext = f"{ext}.{compression_ext_map.get(compression, compression)}"
        
        filename = f"data_{timestamp}.{ext}"
        
        return f"{full_prefix}{filename}"
    
    def _get_content_type(self, format_val: str, compression: str) -> str:
        """Get appropriate content type for format and compression."""
        content_types = {
            'json': 'application/json',
            'jsonl': 'application/x-ndjson',
            'csv': 'text/csv',
            'tsv': 'text/tab-separated-values',
            'parquet': 'application/vnd.apache.parquet',
            'avro': 'application/avro',
            'orc': 'application/orc',
            'arrow': 'application/vnd.apache.arrow.file',
            'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        }
        content_type = content_types.get(format_val, 'application/octet-stream')
        
        if compression and compression != 'none':
            if compression == 'gzip':
                content_type = 'application/gzip'
            elif compression == 'bzip2':
                content_type = 'application/x-bzip2'
        
        return content_type
    
    def convert_data_to_format(self, data: List, format_val: str, compression: str = 'none') -> bytes:
        """
        Convert data to the specified format.
        
        STRICT MODE: Raises ValueError for unsupported formats/compressions.
        No silent fallbacks to prevent data corruption.
        
        Args:
            data: List of dictionaries to convert
            format_val: 'json', 'jsonl', 'csv', 'tsv', 'parquet', 'avro', 'orc', 'arrow', 'xlsx'
            compression: 'none', 'gzip', 'bzip2', 'snappy', 'lz4', 'zstd'
        
        Returns:
            bytes: Serialized and optionally compressed data
        
        Raises:
            ValueError: If format or compression is not supported
            ImportError: If required library is not installed for the format
        """
        if not data:
            data = []
        
        # Handle case where data is a single dict (wrap in list)
        if isinstance(data, dict):
            data = [data]
        
        # Convert to bytes based on format
        if format_val == 'json':
            content = json.dumps(data, indent=2, default=str).encode('utf-8')
        
        elif format_val == 'jsonl':
            lines = [json.dumps(row, default=str) for row in data]
            content = '\n'.join(lines).encode('utf-8')
        
        elif format_val == 'csv':
            content = self._convert_to_csv(data)
        
        elif format_val == 'tsv':
            content = self._convert_to_tsv(data)
        
        elif format_val == 'parquet':
            content = self._convert_to_parquet(data)
        
        elif format_val == 'avro':
            content = self._convert_to_avro(data)
        
        elif format_val == 'orc':
            content = self._convert_to_orc(data)
        
        elif format_val == 'arrow':
            content = self._convert_to_arrow(data)
        
        elif format_val == 'xlsx':
            content = self._convert_to_xlsx(data)
        
        else:
            raise ValueError(
                f"Unsupported format: '{format_val}'. "
                f"Supported formats: json, jsonl, csv, tsv, parquet, avro, orc, arrow, xlsx. "
                f"Check connector's metadata.json for advertised formats."
            )
        
        # Apply compression if requested
        if compression and compression != 'none':
            content = self._compress_data(content, compression)
        
        return content
    
    def _convert_to_csv(self, data: List) -> bytes:
        """Convert list of dicts to CSV bytes."""
        import csv
        
        if not data:
            return b''
        
        output = io.StringIO()
        
        # Get all unique keys across all rows for headers
        all_keys = set()
        for row in data:
            if isinstance(row, dict):
                all_keys.update(row.keys())
        headers = sorted(all_keys)
        
        writer = csv.DictWriter(output, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()
        
        for row in data:
            if isinstance(row, dict):
                writer.writerow(row)
        
        return output.getvalue().encode('utf-8')
    
    def _convert_to_tsv(self, data: List) -> bytes:
        """Convert list of dicts to TSV (tab-separated) bytes."""
        import csv
        
        if not data:
            return b''
        
        output = io.StringIO()
        
        # Get all unique keys across all rows for headers
        all_keys = set()
        for row in data:
            if isinstance(row, dict):
                all_keys.update(row.keys())
        headers = sorted(all_keys)
        
        writer = csv.DictWriter(output, fieldnames=headers, delimiter='\t', extrasaction='ignore')
        writer.writeheader()
        
        for row in data:
            if isinstance(row, dict):
                writer.writerow(row)
        
        return output.getvalue().encode('utf-8')
    
    def _convert_to_parquet(self, data: List) -> bytes:
        """
        Convert list of dicts to Parquet bytes.
        
        Raises:
            ImportError: If pyarrow is not installed
        """
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError(
                "pyarrow is required for Parquet format. "
                "Install it with: pip install pyarrow>=14.0.0"
            )
        
        if not data:
            # Return empty parquet
            table = pa.table({})
        else:
            table = pa.Table.from_pylist(data)
        
        buffer = io.BytesIO()
        # Don't force codec here - let compression be handled separately
        pq.write_table(table, buffer, compression=None)
        return buffer.getvalue()
    
    def _convert_to_avro(self, data: List) -> bytes:
        """
        Convert list of dicts to Avro bytes.
        
        Raises:
            ImportError: If fastavro is not installed
        """
        try:
            from fastavro import writer, parse_schema
        except ImportError:
            raise ImportError(
                "fastavro is required for Avro format. "
                "Install it with: pip install fastavro>=1.8.0"
            )
        
        if not data:
            data = []
        
        # Infer schema from first record
        if data:
            first_record = data[0]
            fields = []
            for key, value in first_record.items():
                # Simple type inference
                if isinstance(value, bool):
                    avro_type = "boolean"
                elif isinstance(value, int):
                    avro_type = "long"
                elif isinstance(value, float):
                    avro_type = "double"
                elif value is None:
                    avro_type = ["null", "string"]
                else:
                    avro_type = "string"
                
                fields.append({"name": key, "type": avro_type})
            
            schema = {
                "type": "record",
                "name": "Record",
                "fields": fields
            }
        else:
            # Empty schema
            schema = {
                "type": "record",
                "name": "Record",
                "fields": []
            }
        
        parsed_schema = parse_schema(schema)
        
        buffer = io.BytesIO()
        writer(buffer, parsed_schema, data)
        return buffer.getvalue()
    
    def _convert_to_orc(self, data: List) -> bytes:
        """
        Convert list of dicts to ORC bytes.
        
        Raises:
            ImportError: If pyarrow is not installed
        """
        try:
            import pyarrow as pa
            import pyarrow.orc as orc
        except ImportError:
            raise ImportError(
                "pyarrow is required for ORC format. "
                "Install it with: pip install pyarrow>=14.0.0"
            )
        
        if not data:
            table = pa.table({})
        else:
            table = pa.Table.from_pylist(data)
        
        buffer = io.BytesIO()
        orc.write_table(table, buffer)
        return buffer.getvalue()
    
    def _convert_to_arrow(self, data: List) -> bytes:
        """
        Convert list of dicts to Arrow IPC (Feather) bytes.
        
        Raises:
            ImportError: If pyarrow is not installed
        """
        try:
            import pyarrow as pa
            import pyarrow.feather as feather
        except ImportError:
            raise ImportError(
                "pyarrow is required for Arrow format. "
                "Install it with: pip install pyarrow>=14.0.0"
            )
        
        if not data:
            table = pa.table({})
        else:
            table = pa.Table.from_pylist(data)
        
        buffer = io.BytesIO()
        feather.write_feather(table, buffer)
        return buffer.getvalue()
    
    def _convert_to_xlsx(self, data: List) -> bytes:
        """
        Convert list of dicts to Excel (XLSX) bytes.
        
        Raises:
            ImportError: If openpyxl is not installed
        """
        try:
            from openpyxl import Workbook
        except ImportError:
            raise ImportError(
                "openpyxl is required for XLSX format. "
                "Install it with: pip install openpyxl>=3.1.0"
            )
        
        wb = Workbook()
        ws = wb.active
        
        if data:
            # Get all unique keys for headers
            all_keys = set()
            for row in data:
                if isinstance(row, dict):
                    all_keys.update(row.keys())
            headers = sorted(all_keys)
            
            # Write headers
            ws.append(headers)
            
            # Write data rows
            for row in data:
                if isinstance(row, dict):
                    ws.append([row.get(h) for h in headers])
        
        buffer = io.BytesIO()
        wb.save(buffer)
        return buffer.getvalue()
    
    def _compress_data(self, data: bytes, compression: str) -> bytes:
        """
        Compress data using the specified codec.
        
        STRICT MODE: Raises ValueError for unsupported compression types.
        
        Args:
            data: Raw bytes to compress
            compression: Compression type ('gzip', 'bzip2', 'snappy', 'lz4', 'zstd')
        
        Returns:
            Compressed bytes
        
        Raises:
            ValueError: If compression type is not supported
            ImportError: If required library is not installed for the compression
        """
        if compression == 'gzip':
            import gzip
            return gzip.compress(data)
        
        elif compression == 'bzip2':
            import bz2
            return bz2.compress(data)
        
        elif compression == 'snappy':
            try:
                import snappy
                return snappy.compress(data)
            except ImportError:
                raise ImportError(
                    "python-snappy is required for Snappy compression. "
                    "Install it with: pip install python-snappy>=0.6.0"
                )
        
        elif compression == 'lz4':
            try:
                import lz4.frame
                return lz4.frame.compress(data)
            except ImportError:
                raise ImportError(
                    "lz4 is required for LZ4 compression. "
                    "Install it with: pip install lz4>=4.3.0"
                )
        
        elif compression == 'zstd':
            try:
                import zstandard as zstd
                cctx = zstd.ZstdCompressor()
                return cctx.compress(data)
            except ImportError:
                raise ImportError(
                    "zstandard is required for ZSTD compression. "
                    "Install it with: pip install zstandard>=0.21.0"
                )
        
        else:
            raise ValueError(
                f"Unsupported compression: '{compression}'. "
                f"Supported compressions: gzip, bzip2, snappy, lz4, zstd. "
                f"Check connector's metadata.json for advertised compressions."
            )
        
        return data

    # ============================================================================
    # GENERIC IMPORT HELPERS (For Database/Row-based Connectors)
    # ============================================================================
    # These methods standardize parameter handling for row-based import operations.
    # Database connectors (MySQL, PostgreSQL, MongoDB, etc.) should use these.
    # Storage connectors (S3, MinIO) should use prepare_destination_params() instead.
    # ============================================================================

    def prepare_import_data(self, params: Dict) -> Dict:
        """
        Prepare parameters for row-based import operations (databases).
        
        This method handles:
        1. Claim Check pattern - fetches data from staging if data_ref is present
        2. Parameter normalization - handles table/collection/entity variations
        3. Config extraction - properly extracts connection config
        4. Data extraction - gets data from various param locations
        
        ALL database connectors should call this at the start of import_data():
        
            def import_data(self, params: Dict) -> Dict[str, Any]:
                prepared = self.prepare_import_data(params)
                if not prepared['success']:
                    return prepared  # Return error if data fetch failed
                
                table = prepared['table']
                data = prepared['data']
                config = prepared['config']
                # ... do actual insert ...
        
        Returns:
            {
                'success': bool,
                'config': dict,          # Connection configuration
                'table': str,            # Table/collection/entity name
                'data': list,            # List of dicts to insert
                'mode': str,             # 'append', 'replace', 'upsert'
                'schema': str,           # Schema name (for DBs that support it)
                'database': str,         # Database name (for DBs that support it)
                'error': str (optional)  # Error message if success=False
            }
            }
        """
        # PHASE 0: ENFORCE PROTECTED CONFIG
        params = self._enforce_config_precedence(params)

        config = params.get('config', {}) or {}
        
        # 1. Handle Claim Check pattern - fetch data from staging if needed
        data = params.get('data') or params.get('rows') or params.get('source_data')
        data_ref = params.get('data_ref')
        
        if data_ref and (not data or len(data) == 0):
            self.log(f"📥 Claim Check: Fetching data from staging: {data_ref}")
            staging_config = params.get('staging_config') or config
            staging_result = self.read_from_staging(data_ref, staging_config)
            
            if not staging_result.get('success'):
                return {
                    'success': False,
                    'error': f"Failed to fetch data from staging: {staging_result.get('error', 'Unknown error')}"
                }
            
            data = staging_result.get('data', [])
            self.log(f"✅ Retrieved {len(data) if data else 0} records from staging")
        
        # Handle nested data (from previous step output)
        if isinstance(data, dict):
            if 'data' in data:
                data = data.get('data', [])
            elif 'rows' in data:
                data = data.get('rows', [])
        
        # Ensure data is a list
        if data is None:
            data = []
        elif not isinstance(data, list):
            data = [data]
        
        # 2. Normalize table/collection/entity parameter
        # Support multiple naming conventions used by different plan generators
        table = (
            params.get('table') or
            params.get('collection') or
            params.get('entity') or
            config.get('table') or
            config.get('collection') or
            config.get('entity')
        )
        
        # Also check for plural forms and take first element
        if not table:
            tables = params.get('tables') or params.get('collections') or params.get('entities')
            if tables:
                if isinstance(tables, list) and len(tables) > 0:
                    table = tables[0]
                elif isinstance(tables, str):
                    table = tables
        
        # 3. Extract other common parameters
        mode = params.get('mode') or config.get('mode', 'append')  # append, replace, upsert
        schema = params.get('schema') or config.get('schema')  # For PostgreSQL, SQL Server, etc.
        database = params.get('database') or config.get('database')  # For MongoDB, MySQL, etc.
        
        return {
            'success': True,
            'config': config,
            'table': table,
            'data': data,
            'mode': mode,
            'schema': schema,
            'database': database,
            'row_count': len(data) if data else 0
        }

    
    def handle_request(self, request: Dict) -> Dict:
        """
        Handle JSON-RPC request.
        GENERIC IMPLEMENTATION: Automatically dispatches to methods based on tool name.
        """
        request_id = request.get('id')
        method = request.get('method')
        # MCP JSON-RPC allows omitting params or setting it to null.
        # Normalize to a dict so downstream handlers can safely call .get().
        params = request.get('params') or {}
        
        # Extract and set trace_id (automatic for all requests)
        trace_id = None
        if method == 'tools/call':
            # For tool calls, trace_id is inside arguments
            arguments = params.get('arguments', {})
            trace_id = arguments.get('trace_id') or arguments.get('_trace_id')
        else:
            # For other calls, it might be at top level
            trace_id = params.get('trace_id') or params.get('_trace_id')
            
        set_trace_id(trace_id)
        
        try:
            if method == 'tools/list':
                result = self.list_tools(params)
                return {"jsonrpc": "2.0", "id": request_id, "result": result}
            
            elif method == 'tools/call':
                result = self._handle_tool_call(params)
                return {"jsonrpc": "2.0", "id": request_id, "result": result}
            
            elif method == 'ping':
                return {"jsonrpc": "2.0", "id": request_id, "result": "pong"}
                
            else:
                # Direct method call (legacy or internal)
                # Map method name to function
                # e.g. "test_connection" -> self.test_connection
                if hasattr(self, method) and callable(getattr(self, method)):
                    func = getattr(self, method)
                    # Check if it's a valid exposed method (not private)
                    if not method.startswith('_'):
                        # Extract config/params
                        call_params = params.get('params', params)
                        result = func(call_params)
                        return {"jsonrpc": "2.0", "id": request_id, "result": result}

                # Compatibility: allow direct method names of the form "<connector_type>_<op>".
                #
                # Some older clients used this legacy shape (e.g. "aws-s3_import_data") instead of
                # MCP tool invocation ("method":"tools/call", "params":{"name":"aws-s3_import_data", ...}).
                #
                # This matters especially for connector types containing '-' which are not valid Python
                # attribute names; we strip the prefix and dispatch to the underlying operation.
                mapped_method = None
                if isinstance(method, str) and method:
                    prefixes = []
                    if getattr(self, "connector_type", ""):
                        prefixes.append(str(self.connector_type))
                        prefixes.append(str(self.connector_type).replace("-", "_"))
                    for pfx in prefixes:
                        pfx = (pfx or "").strip()
                        if not pfx:
                            continue
                        needle = f"{pfx}_"
                        if method.startswith(needle) and len(method) > len(needle):
                            mapped_method = method[len(needle):]
                            break

                if mapped_method and hasattr(self, mapped_method) and callable(getattr(self, mapped_method)):
                    if not str(mapped_method).startswith("_"):
                        call_params = params.get('params', params)
                        result = getattr(self, mapped_method)(call_params)
                        return {"jsonrpc": "2.0", "id": request_id, "result": result}
                
                return {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": f"Method not found: {method}"}}
                
        except Exception as e:
            self.log(f"Request failed: {e}", level="error")
            import traceback
            self.log(traceback.format_exc(), level="debug")
            return {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32000, "message": str(e)}}
        finally:
            # Clean up trace_id
            clear_trace_id()

    def _handle_tool_call(self, params: Dict) -> Dict:
        """
        Generic tool call handler.
        Dispatches to methods based on naming convention:
        {connector_type}_{method_name} -> self.{method_name}
        """
        tool_name = params.get('name', '')
        tool_args = params.get('arguments', {})

        # 0) Configure auth context from connection config (OAuth/API key/etc).
        # Many connectors rely on per-connection tokens stored in the encrypted `config`.
        # Doing this here makes OAuth connectors (HubSpot, Salesforce, etc.) work generically
        # without each connector re-implementing token plumbing.
        try:
            cfg = tool_args.get("config")
            if isinstance(cfg, dict) and cfg:
                self.configure_from_connection(cfg)
        except Exception:
            # Best-effort: never fail the tool call due to auth-context hydration.
            pass
        
        # 1. Normalize tool name to method name
        # Remove connector prefix if present (e.g., "mysql_query" -> "query")
        method_name = tool_name
        if tool_name.startswith(f"{self.connector_type}_"):
            method_name = tool_name[len(self.connector_type)+1:]
            
            
        # Security: never dispatch to private/internal methods. A crafted tool name
        # like "<type>__cleanup_worker" strips to "_cleanup_worker"; without this guard
        # getattr would resolve it and expose internals to any unauthenticated tools/call.
        # Tool handlers are always public — reject underscore-prefixed names.
        if not method_name or method_name.startswith('_'):
            return {"success": False, "error": f"Unknown tool: {tool_name}"}

        # 2. Look for handler method
        if hasattr(self, method_name) and callable(getattr(self, method_name)):
            handler = getattr(self, method_name)

            # 3. Normalize parameters (generic -> specific)
            # This handles table/tables, collection/collections differences
            normalized_args = normalize_params_for_connector(tool_args, self.connector_category)

            # 4. Execute
            return handler(normalized_args)
            
        # 5. Fallback for special/meta tools
        if method_name == "get_capabilities":
            return self.get_capabilities(tool_args)
            
        return {"success": False, "error": f"Unknown tool: {tool_name}"}
    
    # =========================================================================
    # PARAMETER NORMALIZATION (Generic for ALL connectors)
    # =========================================================================
    # This is the KEY to making connectors truly generic. Plan generators can
    # use any terminology (table, tables, entity, entities, collection, etc.)
    # and this layer normalizes to connector-specific terminology.
    # =========================================================================
    
    def normalize_params(self, params: Dict) -> Dict:
        """
        Normalize parameters to connector-specific terminology.
        
        This method is called automatically before any operation.
        Plan generators can use generic terms, and this converts them.
        
        Conversions (based on connector_category):
        - 'entity' -> 'table' (relational_db) / 'collection' (document_db) / 'bucket' (cloud_storage)
        - 'entities' -> 'tables' / 'collections' / 'buckets'
        - 'tables' array -> 'table' string (uses first element)
        - 'collections' array -> 'collection' string (uses first element)
        
        This ensures:
        1. New connectors work automatically with existing plan generators
        2. Plan generators can use consistent terminology
        3. No individual connector patching required
        """
        if not params:
            return params
            
        normalized = params.copy()
        category_info = CATEGORY_OPERATIONS.get(self.connector_category, CATEGORY_OPERATIONS["relational_db"])
        terminology = category_info.get("terminology", {})
        
        # Get connector-specific entity name (e.g., "table", "collection", "bucket")
        entity_singular = terminology.get("entity", "table")
        entity_plural = entity_singular + "s"  # e.g., "tables", "collections"
        
        # ===================================================================
        # RULE 1: Convert generic 'entity'/'entities' to specific terminology
        # ===================================================================
        # 'entity' -> 'table' / 'collection' / 'bucket' etc.
        if "entity" in normalized and entity_singular != "entity":
            if entity_singular not in normalized:
                normalized[entity_singular] = normalized["entity"]
            logger.debug(f"Normalized alias: 'entity' -> '{entity_singular}'")
        
        # 'entities' -> 'tables' / 'collections' / 'buckets' etc.
        if "entities" in normalized and entity_plural != "entities":
            if entity_plural not in normalized:
                normalized[entity_plural] = normalized["entities"]
            logger.debug(f"Normalized alias: 'entities' -> '{entity_plural}'")
        
        # ===================================================================
        # RULE 2: Convert plural array to singular (use first element)
        # ===================================================================
        # This is the critical fix for the "tables vs table" issue!
        # If we have 'tables' array but no 'table', extract first element
        
        # Handle the specific entity plural (e.g., 'tables', 'collections')
        if entity_plural in normalized and entity_singular not in normalized:
            plural_value = normalized[entity_plural]
            if isinstance(plural_value, list) and len(plural_value) > 0:
                normalized[entity_singular] = plural_value[0]
                # Keep the plural for reference (multi-entity operations)
                normalized["_selected_" + entity_plural] = plural_value
                logger.info(f"⚠️ Normalized: '{entity_plural}' array -> '{entity_singular}' = {plural_value[0]}")
        
        # ===================================================================
        # RULE 3: Cross-terminology normalization
        # ===================================================================
        # Handle when plan uses wrong terminology for connector type
        # e.g., plan says 'table' but connector is document_db expecting 'collection'
        
        # Map of all possible entity names to check
        all_entity_names = ["table", "collection", "bucket", "topic", "endpoint", "object"]
        all_entity_plurals = [n + "s" for n in all_entity_names]
        
        # If connector expects 'collection' but params have 'table', convert
        for other_entity in all_entity_names:
            if other_entity != entity_singular and other_entity in normalized:
                if entity_singular not in normalized:
                    normalized[entity_singular] = normalized[other_entity]
                logger.info(f"⚠️ Cross-terminology alias: '{other_entity}' -> '{entity_singular}'")
                break
        
        # Same for plurals
        for other_plural in all_entity_plurals:
            if other_plural != entity_plural and other_plural in normalized:
                if entity_plural not in normalized:
                    normalized[entity_plural] = normalized[other_plural]
                if entity_singular not in normalized:
                    plural_value = normalized[entity_plural]
                    if isinstance(plural_value, list) and len(plural_value) > 0:
                        normalized[entity_singular] = plural_value[0]
                logger.info(f"⚠️ Cross-terminology alias: '{other_plural}' -> '{entity_plural}'")
                break
        
        return normalized
    
    
    def run(self):
        """
        Run JSON-RPC server (stdio transport).
        
        This is the main entry point when running as a standalone process.
        Reads JSON-RPC requests from stdin and writes responses to stdout.
        """
        logger.info(f"🚀 {self.connector_name} starting...")
        logger.info(f"   Type: {self.connector_type}")
        logger.info(f"   Category: {self.connector_category}")
        logger.info("   Ready for JSON-RPC requests on stdin")
        
        for line in sys.stdin:
            try:
                request = json.loads(line.strip())
                response = self.handle_request(request)
                # Ensure JSON-safe output for common Python types (datetime, Decimal, bytes, etc.)
                print(json.dumps(response, default=str), flush=True)
            except json.JSONDecodeError as e:
                logger.error(f"JSON parse error: {e}")
                error_response = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": "Parse error"}
                }
                print(json.dumps(error_response, default=str), flush=True)
            except Exception as e:
                logger.error(f"Unexpected error: {e}", exc_info=True)
                error_response = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32603, "message": str(e)}
                }
                print(json.dumps(error_response, default=str), flush=True)


# =============================================================================
# RESPONSE VALIDATION (Optional - for connectors that want strict validation)
# =============================================================================
# Import from schemas module for response validation
# Usage:
#   from base_connector import validate_response
#   from schemas import DiscoverSchemaResponse
#   
#   @validate_response(DiscoverSchemaResponse)
#   def discover_schema(self, params):
#       return {"success": True, "tables": [...]}
# =============================================================================

try:
    from schemas.response_schemas import (
        validate_response,
        BaseResponse,
        TestConnectionResponse,
        ValidateConfigResponse,
        DiscoverSchemaResponse,
        ExportResponse,
        ImportResponse,
        CapabilitiesResponse,
    )
except ImportError:
    # Schemas module not available - provide no-op decorator
    def validate_response(model):
        """No-op decorator when schemas module is not available"""
        def decorator(func):
            return func
        return decorator
    
    logger.debug("schemas module not available - response validation disabled")


# =============================================================================
# HTTP REQUEST HELPERS (Rate Limiting + OAuth Refresh + Enhanced Error Surface)
# =============================================================================

class RateLimitHandler:
    """
    Rate limit handler for API connectors.
    
    Features:
    - Token bucket algorithm
    - Respects 429 Retry-After header
    - Exponential backoff
    - Bounded retries
    """
    
    def __init__(self, requests_per_second: float = 10.0, max_retries: int = 3):
        self.requests_per_second = requests_per_second
        self.max_retries = max_retries
        self.last_request_time = 0.0
        self.min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0
    
    async def wait_if_needed(self):
        """Wait if rate limit requires it (async version)."""
        import asyncio
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_interval:
            wait_time = self.min_interval - time_since_last
            await asyncio.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    def wait_if_needed_sync(self):
        """Wait if rate limit requires it (sync version)."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_interval:
            wait_time = self.min_interval - time_since_last
            time.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    def handle_429_response(self, headers: Dict[str, str]) -> Optional[float]:
        """
        Parse Retry-After header and return wait time in seconds.
        
        Returns:
            Wait time in seconds, or None if no Retry-After header
        """
        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        if not retry_after:
            return None
        
        try:
            # Try as integer (seconds)
            return float(retry_after)
        except ValueError:
            # Try as HTTP date
            try:
                from email.utils import parsedate_to_datetime
                retry_time = parsedate_to_datetime(retry_after)
                return max(0, (retry_time - datetime.now()).total_seconds())
            except Exception:
                return None


class OAuthHandler:
    """
    OAuth token refresh handler.
    
    Features:
    - Detect 401 responses
    - Refresh access token using refresh token
    - Retry request once after refresh
    - Thread-safe token storage
    """
    
    def __init__(self, token_url: str = None, refresh_callback = None):
        self.token_url = token_url
        self.refresh_callback = refresh_callback
        self._token_lock = threading.Lock()
    
    def should_refresh(self, status_code: int) -> bool:
        """Check if status code indicates token refresh is needed."""
        return status_code == 401
    
    def refresh_token(self, refresh_token: str, client_id: str = None, client_secret: str = None) -> Optional[Dict]:
        """
        Refresh OAuth access token.
        
        Args:
            refresh_token: The refresh token
            client_id: OAuth client ID
            client_secret: OAuth client secret
        
        Returns:
            New token data, or None if refresh failed
        """
        if not self.token_url or not refresh_token:
            return None
        
        with self._token_lock:
            try:
                import requests
                
                data = {
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token
                }
                
                if client_id:
                    data["client_id"] = client_id
                if client_secret:
                    data["client_secret"] = client_secret
                
                response = requests.post(self.token_url, data=data, timeout=10)
                
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.warning(f"Token refresh failed: {response.status_code}")
                    return None
            except Exception as e:
                logger.error(f"Token refresh error: {e}")
                return None


class HTTPRequestHelper:
    """
    Enhanced HTTP request helper with rate limiting, OAuth refresh, and structured responses.
    
    This is backward-compatible: existing connectors continue to work unchanged.
    New generated connectors can use _http_request_v2() for enhanced error surface.
    """
    
    def __init__(
        self,
        rate_limiter: RateLimitHandler = None,
        oauth_handler: OAuthHandler = None
    ):
        self.rate_limiter = rate_limiter or RateLimitHandler()
        self.oauth_handler = oauth_handler
    
    def _http_request_v2(
        self,
        method: str,
        url: str,
        headers: Dict = None,
        params: Dict = None,
        json_data: Dict = None,
        timeout: float = 30.0,
        oauth_config: Dict = None
    ) -> Tuple[bool, int, Any, Dict]:
        """
        Make HTTP request with enhanced error surface.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            headers: Request headers
            params: Query parameters
            json_data: JSON body
            timeout: Request timeout
            oauth_config: OAuth config for token refresh (optional)
        
        Returns:
            Tuple of (success, status_code, data, headers)
            - success: True if request succeeded (2xx status)
            - status_code: HTTP status code
            - data: Parsed JSON response body (or None)
            - headers: Response headers dict
        """
        import requests
        
        # Rate limiting
        self.rate_limiter.wait_if_needed_sync()
        
        attempt = 0
        max_attempts = self.rate_limiter.max_retries + 1
        
        while attempt < max_attempts:
            attempt += 1
            
            try:
                # SSRF guard on the initial URL (this helper previously had
                # none) plus manual bounded redirect-following: the default
                # requests behavior follows a 30x to an internal IP / cloud
                # IMDS with the auth header attached. Re-check every hop.
                initial_reason = _ssrf_check_url(url)
                if initial_reason is not None:
                    logger.error(f"🚫 SSRF guard blocked outbound call: {initial_reason} (url redacted)")
                    return (False, 0, {"error": f"SSRF guard: {initial_reason}"}, {})

                response = requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=json_data,
                    timeout=timeout,
                    allow_redirects=False,
                )

                redirect_hops = 0
                _MAX_REDIRECTS = 5
                while response.is_redirect and redirect_hops < _MAX_REDIRECTS:
                    location = response.headers.get("Location")
                    if not location:
                        break
                    next_url = urljoin(response.url, location)
                    redirect_reason = _ssrf_check_url(next_url)
                    if redirect_reason is not None:
                        logger.error(f"🚫 SSRF guard blocked redirect target: {redirect_reason} (url redacted)")
                        return (False, 0, {"error": f"SSRF guard (redirect): {redirect_reason}"}, {})
                    redirect_hops += 1
                    follow_headers = _headers_safe_for_redirect(headers, response.url, next_url)
                    response = requests.request(
                        method=method,
                        url=next_url,
                        headers=follow_headers,
                        timeout=timeout,
                        allow_redirects=False,
                    )
                if response.is_redirect:
                    logger.error("HTTP redirect budget exhausted; refusing to follow further")
                    return (False, 0, {"error": "too many redirects"}, {})

                # Handle 429 (rate limit)
                if response.status_code == 429:
                    wait_time = self.rate_limiter.handle_429_response(dict(response.headers))
                    if wait_time and attempt < max_attempts:
                        logger.warning(f"Rate limited (429), waiting {wait_time}s before retry")
                        time.sleep(min(wait_time, 60))  # Cap at 60s
                        continue
                    else:
                        # No Retry-After or out of retries
                        return (False, response.status_code, None, dict(response.headers))
                
                # Handle 401 (OAuth token expired)
                if response.status_code == 401 and self.oauth_handler and oauth_config:
                    if attempt == 1:  # Only try refresh once
                        logger.info("Access token expired (401), attempting refresh")
                        new_tokens = self.oauth_handler.refresh_token(
                            refresh_token=oauth_config.get("refresh_token"),
                            client_id=oauth_config.get("client_id"),
                            client_secret=oauth_config.get("client_secret")
                        )
                        
                        if new_tokens and "access_token" in new_tokens:
                            # Update headers with new token
                            if headers is None:
                                headers = {}
                            headers["Authorization"] = f"Bearer {new_tokens['access_token']}"
                            logger.info("Token refreshed successfully, retrying request")
                            continue
                
                # Parse response body
                data = None
                try:
                    if response.content:
                        data = response.json()
                except Exception:
                    data = response.text
                
                # Success for 2xx
                success = 200 <= response.status_code < 300
                return (success, response.status_code, data, dict(response.headers))
                
            except requests.exceptions.Timeout:
                logger.error(f"Request timeout after {timeout}s")
                return (False, 0, None, {})
            except Exception as e:
                logger.error(f"Request error: {_scrub_url_secrets(e)}")
                return (False, 0, None, {})
        
        # All retries exhausted
        return (False, 429, None, {})
    
    def _http_request(
        self,
        method: str,
        url: str,
        headers: Dict = None,
        params: Dict = None,
        json_data: Dict = None,
        timeout: float = 30.0
    ) -> Any:
        """
        Legacy HTTP request method (backward compatible).
        
        Raises exception on error, returns parsed body on success.
        Existing connectors continue to use this unchanged.
        """
        success, status_code, data, _ = self._http_request_v2(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json_data=json_data,
            timeout=timeout
        )
        
        if not success:
            raise Exception(f"HTTP {status_code}: Request failed")
        
        return data


# =============================================================================
# PAGINATION HANDLER (Universal pagination for all strategies)
# =============================================================================

# Well-known response keys (priority order) under which REST/JSON APIs nest
# their list payloads. Single source of truth shared by the runtime paginator
# (PaginationHandler._extract_records) and the generated connectors' schema
# discovery (_extract_first_row in connector.py.j2, which imports this) so the
# two never drift. Append-only: new keys go at the END to preserve precedence.
RESPONSE_DATA_KEYS = (
    "data", "results", "items", "records", "rows",
    "members", "objects", "value", "list",
)


class PaginationHandler:
    """
    Universal pagination handler supporting offset, page, cursor, and Link header strategies.
    
    Safety features:
    - Caps on max pages and max records
    - Non-paginated API detection (stops if no pagination markers)
    - Duplicate detection (stops if same data returned twice)
    - Partial failure handling (returns partial results + errors)
    - Consecutive failure limit (stops after 3 consecutive failures)
    """
    
    DEFAULT_MAX_PAGES = 10
    DEFAULT_MAX_RECORDS = 10000
    MAX_CONSECUTIVE_FAILURES = 3
    
    def __init__(
        self,
        pagination_type: str = "offset",
        pagination_param: str = None,
        limit_param: str = "limit",
        max_page_size: int = 100,
        http_helper: HTTPRequestHelper = None,
        # Response-shape hints injected by the deterministic REST architect.
        # When set these override the generic auto-detect logic, eliminating
        # hallucinated key-names from the LLM pipeline.
        response_data_key: str = "",          # e.g. "channels", "results", "data"
        cursor_path: str = "",                # dot-path: "paging.next.after"
        cursor_mode: str = "response",        # "response" | "last_item_id" (Stripe)
        id_field: str = "id",                 # record field used as cursor in last_item_id mode
        record_is_object: bool = False,       # 2xx body IS one record → wrap [obj]
    ):
        """
        Initialize pagination handler.

        Args:
            pagination_type: One of "offset", "page", "cursor", "link", "none"
            pagination_param: Parameter name for pagination (e.g., "offset", "page", "cursor")
            limit_param: Parameter name for limit/page size
            max_page_size: Maximum records per page
            http_helper: HTTP request helper (uses default if not provided)
            response_data_key: Known key in response where records live ('' = auto-detect)
            cursor_path: Dot-path to next cursor in response body ('' = auto-detect)
            cursor_mode: "response" reads cursor from response; "last_item_id" uses last record's id
            id_field: Field name used as cursor when cursor_mode == "last_item_id"
            record_is_object: True when the endpoint returns a single entity whose
                whole body IS the record (Twilio /Balance.json, Jira /myself). When
                no records array is found, _extract_records wraps the object as a
                one-row list instead of returning 0 rows.
        """
        self.pagination_type = pagination_type.lower()
        self.pagination_param = pagination_param or self._default_param_name()
        self.limit_param = limit_param
        self.max_page_size = max_page_size
        self.http_helper = http_helper or HTTPRequestHelper()
        self.response_data_key = (response_data_key or "").strip()
        self.cursor_path = (cursor_path or "").strip()
        self.cursor_mode = (cursor_mode or "response").strip().lower()
        self.id_field = (id_field or "id").strip()
        self.record_is_object = bool(record_is_object)

        # State tracking
        self.seen_cursors = set()
        self.seen_urls = set()
        self.last_data_hash = None
        # Set by fetch_all_pages: True when the last run was truncated by a cap
        # (max_pages / max_records). Initialized here so it's safe to read
        # before the first fetch.
        self.last_run_hit_cap = False

    def _default_param_name(self) -> str:
        """Get default pagination parameter name for type."""
        defaults = {
            "offset": "offset",
            "page": "page",
            "cursor": "cursor",
            "link": None  # No param, uses Link header
        }
        return defaults.get(self.pagination_type, "offset")
    
    def fetch_all_pages(
        self,
        fetch_page_fn,
        max_pages: int = None,
        max_records: int = None,
        initial_params: Dict = None
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Fetch all pages using the provided fetch function.
        
        Args:
            fetch_page_fn: Function(params) -> (success, status_code, data, headers)
                          Should call API and return tuple from _http_request_v2
            max_pages: Maximum pages to fetch (defaults to DEFAULT_MAX_PAGES)
            max_records: Maximum total records (defaults to DEFAULT_MAX_RECORDS)
            initial_params: Initial request parameters
        
        Returns:
            Tuple of (all_records, errors)
            - all_records: List of all fetched records
            - errors: List of error dicts (page number, error message)
        """
        max_pages = max_pages or self.DEFAULT_MAX_PAGES
        max_records = max_records or self.DEFAULT_MAX_RECORDS
        initial_params = initial_params or {}
        
        all_records = []
        errors = []
        page_num = 0
        consecutive_failures = 0
        next_cursor = None
        next_url = None
        # Cap-stop tracking (read by ApiHandler.export_resource to set
        # ExportResult.has_more). Stays False on a natural end (cursor/link
        # exhausted, short last page, empty page, duplicate, error stop) and
        # flips True only when a cap truncated the fetch: the max_records break
        # below, or the `while ... else` (loop ran out of allowed pages).
        hit_cap = False

        # Allow callers to start from a non-zero offset/page/cursor
        base_offset = 0
        base_page = 1
        if self.pagination_type in {"offset", "page"} and self.pagination_param:
            try:
                if self.pagination_type == "offset":
                    base_offset = int(initial_params.get(self.pagination_param) or 0)
                if self.pagination_type == "page":
                    base_page = int(initial_params.get(self.pagination_param) or 1)
            except Exception:
                base_offset = 0
                base_page = 1
        base_cursor = initial_params.get(self.pagination_param) if self.pagination_type == "cursor" else None

        while page_num < max_pages and len(all_records) < max_records:
            page_num += 1
            
            # Build pagination params
            params = dict(initial_params)
            
            if self.pagination_type == "offset":
                params[self.pagination_param] = base_offset + (page_num - 1) * self.max_page_size
                if self.limit_param:
                    params[self.limit_param] = self.max_page_size
            elif self.pagination_type == "page":
                params[self.pagination_param] = base_page + (page_num - 1)
                if self.limit_param:
                    params[self.limit_param] = self.max_page_size
            elif self.pagination_type == "cursor":
                _cur = None
                if page_num == 1 and base_cursor:
                    _cur = base_cursor
                elif page_num > 1:
                    if not next_cursor:
                        break  # No more pages
                    _cur = next_cursor
                # Some APIs return a next-page URL as the cursor rather than an
                # opaque token — ABSOLUTE (modern Twilio meta.next_page_url) or
                # ROOT-RELATIVE (legacy Twilio's top-level next_page_uri,
                # Salesforce nextRecordsUrl). Follow it via _override_url —
                # sending a URL as a query-param value 400s. Relative URLs are
                # resolved against the connector's base (and every followed URL
                # host-pinned) in ApiHandler's fetch_page_fn. Token cursors ride
                # the pagination query param (with the page-size limit) as before.
                if isinstance(_cur, str) and _cur.startswith(("http://", "https://", "/")):
                    params["_override_url"] = _cur
                    # A resume cursor arrives in initial_params under the
                    # pagination param (copied into params above). Drop it —
                    # otherwise every request of a resumed run re-sends the
                    # stale URL as ?PageToken=https://…, the exact 400 this
                    # branch exists to avoid.
                    if self.pagination_param:
                        params.pop(self.pagination_param, None)
                else:
                    if _cur:
                        params[self.pagination_param] = _cur
                    if self.limit_param:
                        params[self.limit_param] = self.max_page_size
            elif self.pagination_type == "link":
                if page_num > 1 and next_url:
                    params["_override_url"] = next_url  # Signal to use this URL instead
                elif self.limit_param:
                    params[self.limit_param] = self.max_page_size
            
            # Fetch page
            success, status_code, data, headers = fetch_page_fn(params)
            
            if not success:
                consecutive_failures += 1
                # Capture error body (truncated) for debuggability.
                # This prevents opaque failures like "Operation failed" when the real cause is
                # an invalid endpoint (404) or missing credentials (401).
                preview = None
                try:
                    if data is None:
                        preview = None
                    elif isinstance(data, (dict, list)):
                        preview = json.dumps(data, default=str)[:800]
                    else:
                        preview = str(data)[:800]
                except Exception:
                    preview = None
                errors.append({
                    "page": page_num,
                    "status_code": status_code,
                    "error": f"Request failed with status {status_code}",
                    "data": preview,
                })
                
                if consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES:
                    logger.warning(f"Stopping after {consecutive_failures} consecutive failures")
                    break
                
                continue  # Try next page
            
            # Reset failure counter on success
            consecutive_failures = 0
            
            # Extract records from response
            records = self._extract_records(data)
            
            if not records:
                # No records - might be end of pagination or non-paginated API
                if page_num == 1:
                    # First page empty - might be valid empty result
                    pass
                logger.info(f"No records on page {page_num}, stopping pagination")
                break
            
            # Duplicate detection (non-paginated API returning same data)
            data_hash = hash(json.dumps(records, sort_keys=True, default=str))
            if data_hash == self.last_data_hash:
                logger.warning(f"Duplicate data detected on page {page_num}, stopping (non-paginated API)")
                break
            self.last_data_hash = data_hash
            
            # Add records
            all_records.extend(records)
            
            # Check record limit
            if len(all_records) >= max_records:
                logger.info(f"Reached max_records limit ({max_records}), stopping")
                all_records = all_records[:max_records]
                hit_cap = True  # truncated by record cap → more rows may exist
                break
            
            # Extract next page marker
            if self.pagination_type == "cursor":
                if self.cursor_mode == "last_item_id":
                    # Stripe-style: has_more flag + starting_after = last record id
                    has_more = bool(data.get("has_more")) if isinstance(data, dict) else False
                    if not has_more:
                        logger.info("No more pages (has_more=false)")
                        break
                    last_rec = records[-1] if records else {}
                    next_cursor = str(last_rec.get(self.id_field) or "") if isinstance(last_rec, dict) else ""
                    if not next_cursor or next_cursor in self.seen_cursors:
                        logger.info("No more pages (last_item_id cursor exhausted or empty)")
                        break
                    self.seen_cursors.add(next_cursor)
                else:
                    next_cursor = self._extract_next_cursor(data)
                    if not next_cursor or next_cursor in self.seen_cursors:
                        logger.info("No more pages (cursor exhausted)")
                        break
                    self.seen_cursors.add(next_cursor)
            elif self.pagination_type == "link":
                next_url = self._extract_next_link(headers)
                if not next_url or next_url in self.seen_urls:
                    logger.info("No more pages (Link header exhausted)")
                    break
                self.seen_urls.add(next_url)
            else:
                # Offset/page: check if we got fewer records than page size
                if len(records) < self.max_page_size:
                    logger.info(f"Last page (received {len(records)} < {self.max_page_size})")
                    break
        else:
            # The while-condition went false without any natural-end break
            # firing — i.e. page_num reached max_pages while pages were still
            # being consumed. The fetch was truncated by the page cap.
            hit_cap = True

        # Record whether a cap truncated this run so export_resource can set
        # ExportResult.has_more. Instance attr (not a return value) keeps the
        # tuple signature stable for the many existing callers.
        self.last_run_hit_cap = hit_cap

        # Return final cursor for resumption (if cursor-based pagination)
        final_cursor = next_cursor if self.pagination_type == "cursor" else None
        return (all_records, errors, final_cursor)
    
    def _extract_records(self, data: Any) -> List[Dict]:
        """Extract records array from API response.

        Resolution order:
          1. ``self.response_data_key`` when explicitly configured (deterministic path)
          2. Common well-known keys: data, results, items, records, rows, members, objects
          3. Dynamic fallback: first dict value that is a non-empty list (handles Slack's
             per-method response keys like ``channels``, ``bots``, ``users``)
          4. Single-entity wrap: when ``record_is_object`` is set and no records array
             was found by 1–3, the whole object IS the record → return ``[data]``.
             Placed LAST so it can only rescue an otherwise-empty result — a real
             collection (its array found by 1–3) is never overridden.
        """
        if isinstance(data, list):
            return data

        if not isinstance(data, dict):
            return []

        # 1. Explicit key from vendor registry / resource config
        if self.response_data_key:
            v = data.get(self.response_data_key)
            if isinstance(v, list):
                return v

        # 2. Well-known common keys (in priority order) — shared constant
        for key in RESPONSE_DATA_KEYS:
            if key in data and isinstance(data[key], list):
                return data[key]

        # 3. Dynamic fallback: first list-valued key that isn't pagination/meta
        _META_KEYS = frozenset({
            "ok", "error", "errors", "warnings", "has_more", "more",
            "paging", "pagination", "meta", "metadata", "response_metadata",
            "next_cursor", "cursor", "next", "previous", "total", "total_count",
            "count", "page", "pages", "links", "offset", "limit",
        })
        for key, val in data.items():
            if key in _META_KEYS:
                continue
            if isinstance(val, list) and val:
                return val

        # 4. Single-entity read (Twilio /Balance.json, Jira /myself): no records
        # array anywhere, so the whole object IS the one record. Only when the
        # generator flagged this resource — an unflagged single object still
        # yields [] (unchanged), so this can never wrap a pagination envelope.
        if self.record_is_object and data:
            return [data]

        return []

    def _extract_next_cursor(self, data: Any) -> Optional[str]:
        """Extract next cursor from API response.

        Resolution order:
          1. ``self.cursor_path`` when explicitly configured (dot-path traversal)
          2. Extended list of well-known cursor locations across common REST APIs

        Covers:
          - HubSpot: paging.next.after
          - Slack:   response_metadata.next_cursor
          - Notion:  next_cursor (root)
          - Salesforce: nextPageUrl / nextRecordsUrl
          - GitHub REST: Link header is handled by _extract_next_link
          - Generic: cursor, next_cursor, pagination.next_cursor, paging.cursor
        """
        if not isinstance(data, dict):
            return None

        # 1. Explicit cursor path from vendor registry (dot-path)
        if self.cursor_path:
            parts = self.cursor_path.split(".")
            value = data
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    value = None
                    break
            if value and isinstance(value, str):
                return value

        # 2. Extended well-known paths (ordered from most-specific to least)
        _CURSOR_PATHS = [
            # HubSpot CRM v3
            ["paging", "next", "after"],
            # Slack Web API
            ["response_metadata", "next_cursor"],
            # Generic / Notion
            ["next_cursor"],
            # Generic short
            ["cursor"],
            # HubSpot (older)
            ["paging", "next", "cursor"],
            ["paging", "cursors", "after"],
            # Asana — opaque offset token returned under next_page.offset. The
            # spec documents this only in the param prose (not the 2xx schema),
            # so it can't be inferred statically; discovered here at runtime.
            ["next_page", "offset"],
            ["next_page", "cursor"],
            # Spotify-style cursor object
            ["cursors", "after"],
            # Box — opaque marker token
            ["next_marker"],
            # Google / generic page-token
            ["next_page_token"],
            ["nextPageToken"],
            # Azure / generic continuation token
            ["continuation_token"],
            # GraphQL-over-REST page info (endCursor token)
            ["page_info", "end_cursor"],
            ["pageInfo", "endCursor"],
            # Generic pagination wrappers
            ["pagination", "next_cursor"],
            ["pagination", "cursor"],
            ["meta", "next_cursor"],
            ["meta", "cursor"],
            # Twilio — meta.next_page_uri/url is a FULL next-page URL (not a token);
            # the cursor loop follows URL cursors via _override_url.
            ["meta", "next_page_uri"],
            ["meta", "next_page_url"],
            # Twilio legacy (2010-04-01) — same fields but TOP-LEVEL, and the
            # value is a ROOT-RELATIVE URI ("/2010-04-01/…?PageToken=…").
            ["next_page_uri"],
            ["next_page_url"],
            # Salesforce-style full-URL pagination
            ["nextRecordsUrl"],
            ["nextPageUrl"],
        ]
        for path in _CURSOR_PATHS:
            value = data
            for key in path:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    value = None
                    break
            if value and isinstance(value, str):
                return value

        return None
    
    def _extract_next_link(self, headers: Dict) -> Optional[str]:
        """Extract next page URL from Link header."""
        link_header = headers.get("Link") or headers.get("link")
        if not link_header:
            return None
        
        # Parse Link header: <url>; rel="next"
        import re
        for link in link_header.split(","):
            match = re.match(r'<([^>]+)>;\s*rel="next"', link.strip())
            if match:
                return match.group(1)
        
        return None


class GraphQLPaginationHandler:
    """
    Relay-style cursor pagination handler for GraphQL connectors.

    GraphQL connections expose pagination via ``pageInfo { hasNextPage endCursor }``
    and accept a ``$first`` page-size + ``$after`` cursor. A single page is capped
    server-side (Shopify: 250). This helper loops every page inside one ``export()``
    call, accumulating rows so the connector returns a full result set (plus a
    correct watermark computed over all rows) in one response — no orchestrator
    round-trips.

    Safety features mirror PaginationHandler:
    - Caps on max pages and max records.
    - Cycle guard: stops if ``endCursor`` is empty or repeats.
    - On a cap-stop it reports ``has_more=True`` + the final cursor so a caller can
      resume via ``$after`` on the next call.
    """

    DEFAULT_MAX_PAGES = 200
    DEFAULT_MAX_RECORDS = 10000
    DEFAULT_PAGE_SIZE = 250

    def fetch_all_pages(
        self,
        execute_page_fn,
        base_variables: Dict = None,
        *,
        page_size: int = None,
        max_pages: int = None,
        max_records: int = None,
        start_after: str = None,
        row_extractor=None,
    ) -> Tuple[List[Dict], Optional[str], bool]:
        """
        Loop every page of a Relay connection.

        Args:
            execute_page_fn: Function(variables) -> raw GraphQL ``data`` dict for one page.
            base_variables: Base GraphQL variables; ``first``/``after`` are injected per page.
            page_size: Records per page (defaults to DEFAULT_PAGE_SIZE, clamped to it).
            max_pages: Hard cap on page count (defaults to DEFAULT_MAX_PAGES).
            max_records: Hard cap on accumulated rows (defaults to DEFAULT_MAX_RECORDS).
            start_after: Resume cursor seeded into the first page's ``after``.
            row_extractor: Function(data) -> List[Dict] turning a page's raw data into rows.

        Returns:
            Tuple of (all_rows, final_end_cursor, has_more)
            - all_rows: every row across all fetched pages
            - final_end_cursor: cursor to resume from when has_more is True, else None
            - has_more: True only when a cap (records/pages) stopped the loop early
        """
        if row_extractor is None:
            raise ValueError("GraphQLPaginationHandler.fetch_all_pages requires row_extractor")

        page_size = min(int(page_size or self.DEFAULT_PAGE_SIZE), self.DEFAULT_PAGE_SIZE)
        page_size = max(1, page_size)
        max_pages = max_pages or self.DEFAULT_MAX_PAGES
        max_records = max_records or self.DEFAULT_MAX_RECORDS

        all_rows: List[Dict] = []
        cursor = start_after or None
        seen_cursors = set()
        page_num = 0
        has_more = False
        final_cursor = None

        while page_num < max_pages:
            page_num += 1
            variables = dict(base_variables or {})
            variables["first"] = page_size
            if cursor:
                variables["after"] = cursor
            else:
                variables.pop("after", None)

            data = execute_page_fn(variables)
            rows = row_extractor(data) or []
            all_rows.extend(rows)

            page_info = self._extract_page_info(data)
            has_next = bool(page_info.get("hasNextPage"))
            end_cursor = page_info.get("endCursor")

            # Record cap: stop and report resumable state.
            if len(all_rows) >= max_records:
                has_more = has_next
                final_cursor = end_cursor if has_next else None
                break

            # Natural exhaustion.
            if not has_next:
                break

            # Cycle guard: empty or repeating cursor would loop forever.
            if not end_cursor or end_cursor in seen_cursors:
                break
            seen_cursors.add(end_cursor)
            cursor = end_cursor
        else:
            # Page cap reached without exhausting the connection.
            has_more = True
            final_cursor = cursor

        return all_rows, final_cursor, has_more

    def _extract_page_info(self, data: Any, _depth: int = 0) -> Dict:
        """
        Depth-first search for the ``pageInfo`` dict carrying hasNextPage/endCursor.

        GraphQL responses nest the connection under a query-specific field
        (e.g. ``data.products.pageInfo``); this finds it without hardcoding the path.
        """
        if _depth > 4 or not isinstance(data, dict):
            return {}
        # Direct hit: this dict IS a pageInfo.
        if "hasNextPage" in data or "endCursor" in data:
            return data
        # Nested pageInfo key.
        page_info = data.get("pageInfo")
        if isinstance(page_info, dict):
            return page_info
        for value in data.values():
            if isinstance(value, dict):
                found = self._extract_page_info(value, _depth + 1)
                if found:
                    return found
        return {}


# =============================================================================
# CATEGORY HANDLERS (Contract-Driven)
# =============================================================================

class ApiHandler:
    """
    Handler for API/SaaS connectors (category: api_saas).
    
    Provides standardized export_resource that uses:
    - PaginationHandler for multi-page fetching
    - RateLimitHandler for API throttling
    - OAuthHandler for token refresh
    - Partial failure handling with structured errors
    """
    
    def __init__(
        self,
        connector: 'BaseMCPConnector',
        pagination_type: str = "offset",
        pagination_param: str = "offset",
        limit_param: str = "limit",
        max_page_size: int = 100,
        rate_limit_rps: float = 10.0,
        oauth_handler: OAuthHandler = None
    ):
        self.connector = connector
        self.http_helper = HTTPRequestHelper(
            rate_limiter=RateLimitHandler(requests_per_second=rate_limit_rps),
            oauth_handler=oauth_handler
        )
        self.pagination_handler = PaginationHandler(
            pagination_type=pagination_type,
            pagination_param=pagination_param,
            limit_param=limit_param,
            max_page_size=max_page_size
        )
    
    def export_resource(
        self,
        *,
        connector: 'BaseMCPConnector',
        config: Dict[str, Any],
        resource: str,
        endpoint: str,
        params: Dict[str, Any],
        max_pages: int = 10,
        max_records: int = 10000,
        incremental_field: Optional[str] = None,
        resource_config: Optional[Dict[str, Any]] = None,
        prior_watermark: Optional[Dict[str, Any]] = None,
    ) -> ExportResult:
        """
        Export data from an API resource with pagination.

        Args:
            connector: The connector instance (for _make_request_v2)
            config: Connection configuration
            resource: API resource name (for logging/errors)
            endpoint: API endpoint path to fetch
            params: Additional query parameters
            max_pages: Maximum pages to fetch
            max_records: Maximum total records
            incremental_field: Field name for watermark computation (e.g., 'updated_at')
            resource_config: Optional per-resource pagination/shape config dict with keys:
                pagination_type, pagination_param, limit_param, max_page_size,
                response_data_key, cursor_path, cursor_mode, id_field.
                When provided, overrides the handler-level defaults for this call,
                ensuring each resource uses the correct settings (critical for
                multi-resource connectors like Stripe/HubSpot/Slack).
            prior_watermark: The watermark from the previous incremental run
                ({"field": ..., "value": ...}). Echoed back on a 0-row re-run so
                the executor keeps the checkpoint instead of resetting it.

        Returns:
            ExportResult with records, errors, next_cursor, has_more, and stats.
            stats carries `watermark`/`max_watermark` (MAX of incremental_field
            over ALL records, not just the last) when applicable; has_more is
            True only when a cap truncated the fetch.
        """
        logger.info(
            "ApiHandler.export_resource: %s (%s), max_pages=%d, max_records=%d, incremental_field=%s",
            resource, endpoint, max_pages, max_records, incremental_field,
        )

        # Build a per-call PaginationHandler when resource_config overrides are supplied.
        # This is critical for multi-resource connectors where different resources use
        # different pagination strategies (e.g., Stripe cursor vs HubSpot paging.next.after).
        if resource_config:
            pag_handler = PaginationHandler(
                pagination_type=str(resource_config.get("pagination_type") or self.pagination_handler.pagination_type),
                pagination_param=str(resource_config.get("pagination_param") or self.pagination_handler.pagination_param),
                # Finding C: an explicit empty limit_param means the resource
                # declares NO limit query param — it must NOT be coerced back to
                # the connector-wide default (built from resources[0], which may
                # declare pagination). A plain `or` turns "" into "limit" and
                # re-injects an undeclared limit=100 → strict APIs 400 (NASA APOD
                # "incorrect field passed"). Use the per-resource value verbatim
                # when present; fall back to the connector default only when the
                # key is absent/None.
                limit_param=(
                    str(resource_config["limit_param"])
                    if resource_config.get("limit_param") is not None
                    else str(self.pagination_handler.limit_param)
                ),
                max_page_size=int(resource_config.get("max_page_size") or self.pagination_handler.max_page_size),
                response_data_key=str(resource_config.get("response_data_key") or ""),
                cursor_path=str(resource_config.get("cursor_path") or ""),
                cursor_mode=str(resource_config.get("cursor_mode") or "response"),
                id_field=str(resource_config.get("id_field") or "id"),
                record_is_object=bool(resource_config.get("record_is_object")),
            )
        else:
            pag_handler = self.pagination_handler

        # F10 — HTTP verb for the list read. Defaults to GET; a JSON-RPC / POST-list
        # API (Outline /documents.list) declares 'POST', and the pagination/filter
        # params then ride in the request BODY instead of the query string.
        list_method = str((resource_config or {}).get("list_method") or "GET").upper()

        # PaginationHandler expects: fetch_page_fn(params) -> (success, status_code, data, headers)
        def fetch_page_fn(params_for_page: Dict) -> Tuple[bool, int, Any, Dict]:
            override = params_for_page.get("_override_url")
            if isinstance(override, str) and override:
                # Cursor URLs come from the RESPONSE BODY. Resolve root-relative
                # ones (legacy Twilio next_page_uri, Salesforce nextRecordsUrl)
                # against the connector's own base URL — same resolution order
                # as the generated _make_request_v2 — and pin EVERY followed URL
                # to that host: a hostile/compromised API response must not be
                # able to point the next page (sent with auth headers) at a
                # third-party host. Fail the page (recorded as a page error by
                # the pagination loop) rather than send a credentialed request
                # elsewhere.
                from urllib.parse import urljoin, urlparse
                base = str(
                    (config or {}).get("base_url")
                    or (config or {}).get("url")
                    or getattr(connector, "base_url", "")
                    or ""
                )
                if override.startswith("/"):
                    if not base:
                        return (
                            False, 0,
                            {"error": "relative cursor URL with no base_url to resolve against — refusing to follow"},
                            {},
                        )
                    override = urljoin(base, override)
                    params_for_page = dict(params_for_page)
                    params_for_page["_override_url"] = override
                def _host_port(u: str):
                    p = urlparse(u)
                    default = {"http": 80, "https": 443}.get((p.scheme or "").lower())
                    return ((p.hostname or "").lower(), p.port or default)

                base_hp = _host_port(base) if base else ("", None)
                cursor_hp = _host_port(override)
                if base_hp[0] and cursor_hp != base_hp:
                    return (
                        False, 0,
                        {"error": (
                            f"cursor URL host '{cursor_hp[0]}:{cursor_hp[1]}' does not match "
                            f"connector host '{base_hp[0]}:{base_hp[1]}' — refusing to follow"
                        )},
                        {},
                    )
            if hasattr(connector, "_make_request_v2"):
                # Generated API connectors implement _make_request_v2(method, endpoint, config, params, data)
                if list_method == "POST":
                    # POST-list (JSON-RPC style): pagination/filter params ride in
                    # the BODY, not the query string. Drop the internal override key.
                    body = {k: v for k, v in params_for_page.items() if k != "_override_url"}
                    return connector._make_request_v2(
                        method="POST",
                        endpoint=endpoint,
                        config=config,
                        params=None,
                        data=body,
                    )
                return connector._make_request_v2(
                    method="GET",
                    endpoint=endpoint,
                    config=config,
                    params=params_for_page,
                    data=None,
                )
            raise NotImplementedError("Connector must implement _make_request_v2 for ApiHandler")

        # Use PaginationHandler to fetch all pages
        records, errors, final_cursor = pag_handler.fetch_all_pages(
            fetch_page_fn=fetch_page_fn,
            max_pages=max_pages,
            max_records=max_records,
            initial_params=params or {},
        )
        
        # Compute watermark as the MAX of incremental_field over ALL records.
        # Using records[-1] is wrong whenever the API returns results in DESC or
        # unsorted order — it would persist a too-low checkpoint and silently
        # re-export or skip rows on the next incremental run. We scan every row
        # and keep the maximum comparable value (ISO timestamps and ints both
        # order correctly under max()).
        watermark = None
        if incremental_field and records:
            best = None
            for rec in records:
                if not isinstance(rec, dict) or incremental_field not in rec:
                    continue
                val = rec[incremental_field]
                if val is None:
                    continue
                if best is None:
                    best = val
                else:
                    try:
                        if val > best:
                            best = val
                    except TypeError:
                        # Mixed/incomparable types — keep the first seen rather
                        # than crash; better a stable checkpoint than none.
                        pass
            if best is not None:
                watermark = {"field": incremental_field, "value": best}

        # On a 0-row incremental re-run, echo the caller's prior watermark so the
        # executor does not reset the checkpoint to null and re-scan from the
        # beginning next time.
        if watermark is None and not records and incremental_field and prior_watermark:
            watermark = prior_watermark

        # has_more reflects whether a cap (max_pages / max_records) truncated the
        # fetch. Read it off the handler that just ran (set in fetch_all_pages).
        has_more = bool(getattr(pag_handler, "last_run_hit_cap", False))

        # Build result
        success = len(records) > 0 or len(errors) == 0
        stats = {
            "total_records": len(records),
            "total_errors": len(errors),
            "max_pages": max_pages,
            "max_records": max_records,
            "has_more": has_more,
        }

        if watermark:
            stats["watermark"] = watermark
            # max_watermark mirrors watermark for callers that read either key.
            stats["max_watermark"] = watermark

        return ExportResult(
            success=success,
            records=records,
            errors=errors,
            next_cursor=final_cursor,
            stats=stats,
            has_more=has_more,
        )


class StorageHandler:
    """
    Handler for cloud storage connectors (category: cloud_storage).
    
    Provides standardized export that:
    - Lists objects/files in bucket/prefix
    - Downloads file content (REQUIRED for validation)
    - Handles pagination for large listings
    - Provides partial failure handling
    """
    
    def __init__(self, connector: 'BaseMCPConnector'):
        self.connector = connector
    
    def export(
        self,
        *,
        config: Dict[str, Any],
        bucket: str,
        prefix: str = "",
        include_patterns: List[str] = None,
        max_files: int = 1000,
        max_bytes: int = 100 * 1024 * 1024,  # 100MB default
        max_pages: int = 10,
        max_records: int = 10000,
        modified_since: Optional[str] = None
    ) -> ExportResult:
        """
        Export files from cloud storage with incremental sync support.
        
        MUST perform at least one download operation (get_object/download_fileobj)
        to pass category validation.
        
        Args:
            config: Connection configuration (credentials, region, etc.)
            bucket: Bucket/container name
            prefix: Path prefix to filter
            include_patterns: File patterns to include (e.g., ["*.csv"])
            max_files: Maximum files to download
            max_bytes: Maximum total bytes to download
            max_pages: Maximum pages for listing
            max_records: Maximum records to return
            modified_since: ISO8601 timestamp - only fetch objects modified after this time
        
        Returns:
            ExportResult with file records, errors, and stats (including watermark)
        """
        logger.info(f"StorageHandler.export: bucket={bucket}, prefix={prefix}, max_files={max_files}, modified_since={modified_since}")
        
        records = []
        errors = []
        total_bytes = 0
        max_last_modified = None
        
        # Connector must implement list_objects and download_file
        if not hasattr(self.connector, 'list_objects'):
            errors.append(ErrorItem(
                code="MISSING_LIST_OBJECTS",
                message="Connector does not implement list_objects method"
            ))
            return ExportResult(success=False, records=[], errors=errors, stats={})
        
        if not hasattr(self.connector, 'download_file'):
            errors.append(ErrorItem(
                code="MISSING_DOWNLOAD_FILE",
                message="Connector does not implement download_file method"
            ))
            return ExportResult(success=False, records=[], errors=errors, stats={})
        
        # List objects
        try:
            objects = self.connector.list_objects(
                config=config,
                bucket=bucket,
                prefix=prefix,
                max_keys=max_files
            )
        except Exception as e:
            logger.error(f"Failed to list objects: {e}")
            errors.append(ErrorItem(
                code="LIST_FAILED",
                message=str(e),
                resource=f"{bucket}/{prefix}"
            ))
            return ExportResult(success=False, records=[], errors=errors, stats={})
        
        # Parse modified_since timestamp if provided
        modified_since_dt = None
        if modified_since:
            try:
                from dateutil import parser as date_parser
                modified_since_dt = date_parser.parse(modified_since)
            except Exception as e:
                logger.warning(f"Could not parse modified_since '{modified_since}': {e}")
        
        # Download files (required for validation) with incremental filtering
        for obj in objects[:max_files]:
            if total_bytes >= max_bytes:
                logger.info(f"Reached max_bytes limit ({max_bytes})")
                break
            
            # Incremental filter: skip objects not modified since cutoff
            obj_last_modified = obj.get('last_modified') or obj.get('LastModified')
            if modified_since_dt and obj_last_modified:
                try:
                    from dateutil import parser as date_parser
                    obj_dt = date_parser.parse(obj_last_modified) if isinstance(obj_last_modified, str) else obj_last_modified
                    if hasattr(obj_dt, 'replace') and hasattr(obj_dt, 'tzinfo') and obj_dt.tzinfo is None:
                        # Make naive datetime timezone-aware (assume UTC)
                        import pytz
                        obj_dt = pytz.utc.localize(obj_dt)
                    if obj_dt <= modified_since_dt:
                        continue  # Skip - not modified since cutoff
                except Exception as e:
                    logger.debug(f"Could not parse last_modified for {obj.get('key')}: {e}")
            
            try:
                file_data = self.connector.download_file(
                    config=config,
                    bucket=bucket,
                    key=obj.get('key') or obj.get('Key')
                )
                
                file_size = len(file_data) if isinstance(file_data, (bytes, str)) else 0
                total_bytes += file_size
                
                records.append({
                    "key": obj.get('key') or obj.get('Key'),
                    "size": file_size,
                    "last_modified": obj_last_modified,
                    "content": file_data if file_size < 1024 * 1024 else f"<{file_size} bytes>"
                })
                
                # Track max last_modified for watermark
                if obj_last_modified:
                    try:
                        from dateutil import parser as date_parser
                        obj_dt = date_parser.parse(obj_last_modified) if isinstance(obj_last_modified, str) else obj_last_modified
                        if max_last_modified is None:
                            max_last_modified = obj_last_modified
                        else:
                            max_dt = date_parser.parse(max_last_modified) if isinstance(max_last_modified, str) else max_last_modified
                            if obj_dt > max_dt:
                                max_last_modified = obj_last_modified
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"Failed to download {obj.get('key')}: {e}")
                errors.append(ErrorItem(
                    code="DOWNLOAD_FAILED",
                    message=str(e),
                    resource=obj.get('key')
                ))
                # Continue on single-file failure (partial failure policy)
        
        success = len(records) > 0
        stats = {
            "total_files": len(records),
            "total_bytes": total_bytes,
            "total_errors": len(errors)
        }
        
        # Include watermark if we have a max_last_modified
        if max_last_modified:
            stats["watermark"] = {
                "field": "last_modified",
                "value": max_last_modified
            }
        
        return ExportResult(
            success=success,
            records=records,
            errors=errors,
            next_cursor=None,
            stats=stats
        )


class DatabaseHandler:
    """
    Handler for database connectors (categories: relational_db, document_db, 
    data_warehouse, wide_column_db).
    
    Provides standardized export/import_data operations with:
    - Query execution for export
    - Batch inserts for import
    - Upsert/delete for CDC destinations
    - Partial failure handling
    """
    
    def __init__(self, connector: 'BaseMCPConnector'):
        self.connector = connector
    
    def export(
        self,
        *,
        connector: 'BaseMCPConnector',
        config: Dict[str, Any],
        entity: str,
        query: Optional[str] = None,
        max_pages: int = 10,
        max_records: int = 10000
    ) -> ExportResult:
        """
        Export data from a database entity (table/collection).
        
        Args:
            connector: The connector instance
            config: Connection configuration
            entity: Table/collection name
            query: Optional custom query/filter
            max_pages: Maximum pages (for pagination)
            max_records: Maximum total records
        
        Returns:
            ExportResult with records, errors, and stats
        """
        logger.info(f"DatabaseHandler.export: entity={entity}, max_records={max_records}")
        
        records = []
        errors = []
        
        # Connector must implement execute_query or similar
        if not hasattr(connector, 'execute_query'):
            errors.append(ErrorItem(
                code="MISSING_EXECUTE_QUERY",
                message="Connector does not implement execute_query method"
            ))
            return ExportResult(success=False, records=[], errors=errors, stats={})
        
        try:
            # Execute query
            if query:
                result = connector.execute_query(config=config, query=query)
            else:
                # Default: SELECT * FROM entity LIMIT max_records
                result = connector.execute_query(
                    config=config,
                    query=f"SELECT * FROM {entity} LIMIT {max_records}"
                )
            
            records = result if isinstance(result, list) else []
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            errors.append(ErrorItem(
                code="QUERY_FAILED",
                message=str(e),
                resource=entity
            ))
        
        success = len(records) > 0 or len(errors) == 0
        stats = {
            "total_records": len(records),
            "total_errors": len(errors)
        }
        
        return ExportResult(
            success=success,
            records=records,
            errors=errors,
            next_cursor=None,
            stats=stats
        )
    
    def import_data(
        self,
        *,
        connector: 'BaseMCPConnector',
        config: Dict[str, Any],
        entity: str,
        data: List[Dict[str, Any]],
        mode: str = "insert"
    ) -> ImportResult:
        """
        Import data into a database entity.
        
        Args:
            connector: The connector instance
            config: Connection configuration
            entity: Table/collection name
            data: Records to insert
            mode: Import mode ("insert", "upsert", "replace")
        
        Returns:
            ImportResult with written/failed counts and errors
        """
        logger.info(f"DatabaseHandler.import_data: entity={entity}, records={len(data)}, mode={mode}")
        
        written = 0
        failed = 0
        errors = []
        
        # Connector must implement insert_data or similar
        if not hasattr(connector, 'insert_data'):
            errors.append(ErrorItem(
                code="MISSING_INSERT_DATA",
                message="Connector does not implement insert_data method"
            ))
            return ImportResult(success=False, written=0, failed=len(data), errors=errors, stats={})
        
        try:
            # Batch insert
            result = connector.insert_data(config=config, entity=entity, data=data, mode=mode)
            written = result.get('written', len(data))
            failed = result.get('failed', 0)
        except Exception as e:
            logger.error(f"Import failed: {e}")
            failed = len(data)
            errors.append(ErrorItem(
                code="IMPORT_FAILED",
                message=str(e),
                resource=entity
            ))
        
        success = written > 0
        stats = {
            "total_written": written,
            "total_failed": failed
        }
        
        return ImportResult(
            success=success,
            written=written,
            failed=failed,
            errors=errors,
            stats=stats
        )



