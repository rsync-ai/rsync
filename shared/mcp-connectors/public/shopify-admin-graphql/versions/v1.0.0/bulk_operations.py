#!/usr/bin/env python3
"""
Shopify Bulk Operations helper (SHOPIFY-BulkOps).

Encapsulates the submit -> poll -> download -> parse contract for
Shopify's `bulkOperationRunQuery` GraphQL mutation. This is the
high-throughput export path: instead of paginating server-side at
~250 rows / 3 seconds (rate-limited), we submit one query, Shopify
processes it asynchronously, and we download a JSONL file when ready.

Order-of-magnitude speedup for large stores (~3.3 hr -> ~20 min for 1M rows).

Design constraints
------------------
* Pure module — no class state, no shared globals beyond constants. All
  callers pass their own HTTP transport (a ``post_graphql`` callable)
  plus a ``get_url`` callable for the JSONL download. This keeps the
  connector's auth + endpoint logic in one place and makes mocking
  trivial in unit tests.
* Bulk-ops failures NEVER raise. Every path returns a
  ``BulkOperationResult`` whose ``status`` is one of
  {"completed","failed","timeout","userError","downloadError",
   "noOpResource","cancelled","expired"}. The caller decides what to
  do — typically: log a [BulkOps] warning and fall back to regular
  pagination.

Shopify wire contract (Admin GraphQL 2025-10)
---------------------------------------------
1. Submit:
   mutation {
     bulkOperationRunQuery(query: "<INNER QUERY>") {
       bulkOperation { id status }
       userErrors { field message }
     }
   }
   Constraint: the inner query MUST have a single top-level
   connection field with NO arguments (no `first`, no `after`,
   no `query`).
2. Poll every ~5s:
   query { currentBulkOperation { id status errorCode objectCount
                                  fileSize url partialDataUrl } }
   States: CREATED -> RUNNING -> COMPLETED | FAILED | CANCELED | EXPIRED.
3. Download:
   GET <url> -> body is JSONL (one record per line). For nested
   connections, child records carry ``__parentId`` linking back to
   their parent.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_POLL_INTERVAL_SEC = 5.0
DEFAULT_TIMEOUT_SEC = 3600  # 1 hour
DEFAULT_DOWNLOAD_TIMEOUT_SEC = 300  # 5 minutes for the actual GCS fetch
DEFAULT_DOWNLOAD_MAX_BYTES = 0  # 0 == unlimited; tests/cap if needed

LOG_PREFIX = "[BulkOps]"

# Terminal statuses from Shopify's BulkOperationStatus enum.
_TERMINAL_OK = {"COMPLETED"}
_TERMINAL_FAIL = {"FAILED", "CANCELED", "CANCELLED", "EXPIRED"}


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class BulkOperationResult:
    """Outcome of a single bulk-op submit -> poll -> download cycle.

    ``status`` discriminates:
      - "completed"      -> success; ``rows`` populated, ``row_count`` accurate.
      - "userError"      -> bulkOperationRunQuery returned userErrors.
      - "failed"         -> poll saw status=FAILED or non-empty errorCode.
      - "cancelled"      -> poll saw status=CANCELED / CANCELLED.
      - "expired"        -> poll saw status=EXPIRED.
      - "timeout"        -> elapsed > timeout_sec before terminal status.
      - "downloadError"  -> COMPLETED but download failed / body invalid.
      - "noOpResource"   -> caller passed an unrecognized resource key.
    """

    status: str
    rows: List[Dict[str, Any]] = field(default_factory=list)
    row_count: int = 0
    error: Optional[str] = None
    operation_id: Optional[str] = None
    object_count: Optional[int] = None
    file_size: Optional[int] = None
    download_url: Optional[str] = None
    elapsed_sec: float = 0.0

    @property
    def ok(self) -> bool:
        return self.status == "completed"


# ---------------------------------------------------------------------------
# Query strip — turn paginated query into bulk-op-compatible query
# ---------------------------------------------------------------------------

# Shopify's bulkOperationRunQuery REQUIRES that the inner query's
# top-level connection field has NO arguments. Our existing per-resource
# queries declare `($first: Int, $after: String, $query: String)` and
# pass them through, so we need to strip those before wrapping. We do
# NOT strip arguments from nested connections — the docs allow nested
# `first: N` and we don't currently use any.
#
# This regex matches the OUTERMOST `<resource>(first: $first, after: $after, ...)`
# call and replaces it with bare `<resource>`. It also removes the
# `query <Name>($first: Int, $after: String, ...) {` operation header
# variables so the wrapping mutation doesn't need to forward them.

_OUTER_OP_HEADER_RE = re.compile(
    r"^\s*query\s+\w+\s*\([^)]*\)\s*\{",
    re.IGNORECASE,
)
_TOP_CONNECTION_CALL_RE = re.compile(
    # `<field>(<args>)` — first occurrence after the opening `{`. The
    # arguments list cannot itself contain unbalanced parens in a valid
    # GraphQL doc, so a non-greedy match is safe.
    r"(\b[a-zA-Z_]\w*)\s*\(([^()]*)\)",
)


def strip_query_for_bulk_ops(query: str) -> str:
    """Strip pagination arguments so a paginated query becomes bulk-op-safe.

    Transforms:
        query GetOrders($first: Int, $after: String, $query: String) {
          orders(first: $first, after: $after, query: $query) { edges ... }
        }
    into:
        {
          orders { edges ... }
        }

    The transform is conservative: if the regex can't confidently
    rewrite the query, we return the original string and let Shopify's
    validator reject it (which is the safe failure — userError path).
    """
    if not isinstance(query, str) or not query.strip():
        return query

    # 1. Drop the operation header (and its variables); we'll start with `{`.
    body = _OUTER_OP_HEADER_RE.sub("{", query, count=1)

    # 2. Strip args from the FIRST connection-style field call inside the body.
    #    We only modify the first match — nested connections (variants(first: 5),
    #    lineItems(first: 10), inventoryLevels(first: 5)) MUST be stripped too
    #    because bulk ops forbids any arguments on connection fields. So
    #    actually we strip ALL `<field>(...)` calls that look like connection
    #    paginators (first / after / last / before / query).
    def _maybe_strip(match: "re.Match[str]") -> str:
        field_name = match.group(1)
        args = match.group(2)
        # Only strip if the args list looks like a connection paginator —
        # presence of any of these signals it. Otherwise (e.g. a real
        # `quantities(names: [...])` arg list) we leave the call alone.
        if re.search(r"\b(first|after|last|before|query)\s*:", args, re.IGNORECASE):
            return field_name
        return match.group(0)

    body = _TOP_CONNECTION_CALL_RE.sub(_maybe_strip, body)
    return body


# ---------------------------------------------------------------------------
# Mutation / poll query builders
# ---------------------------------------------------------------------------


def build_submit_mutation(inner_query: str) -> str:
    """Wrap the (already-stripped) inner query in bulkOperationRunQuery."""
    # Use triple-quote escaping inside GraphQL via a JSON-encoded string.
    # Shopify accepts `query: """..."""` block strings, but escaping them
    # safely with arbitrary user queries is brittle. JSON-escape and let
    # the GraphQL parser unwrap it.
    escaped = json.dumps(inner_query)
    return (
        "mutation BulkOpsSubmit {\n"
        "  bulkOperationRunQuery(query: " + escaped + ") {\n"
        "    bulkOperation { id status }\n"
        "    userErrors { field message }\n"
        "  }\n"
        "}\n"
    )


POLL_QUERY = (
    "query BulkOpsPoll {\n"
    "  currentBulkOperation {\n"
    "    id\n"
    "    status\n"
    "    errorCode\n"
    "    objectCount\n"
    "    fileSize\n"
    "    url\n"
    "    partialDataUrl\n"
    "    createdAt\n"
    "    completedAt\n"
    "  }\n"
    "}\n"
)


# ---------------------------------------------------------------------------
# JSONL parsing
# ---------------------------------------------------------------------------


def parse_jsonl(body: str) -> List[Dict[str, Any]]:
    """Parse a Shopify bulk-op JSONL response into a row list.

    Records carry their GraphQL nesting via the ``__parentId`` field:
      {"id": "gid://shopify/Order/1", ...}
      {"id": "gid://shopify/LineItem/2", "__parentId": "gid://shopify/Order/1"}

    For flat queries (no nested connections), every line is a root row.
    For nested queries, we re-attach children to their parents under a
    synthesized ``__children`` list keyed by the child's resource type.

    NOTE: for the simple per-resource queries we currently use (orders,
    products, customers, ...), nested connections are pre-selected with
    `first: N` — bulk ops will flatten those into separate JSONL lines.
    Re-assembly preserves the structure the regular paginated path
    would have produced.
    """
    lines = [ln for ln in (body or "").splitlines() if ln.strip()]
    if not lines:
        return []

    by_id: Dict[str, Dict[str, Any]] = {}
    roots: List[Dict[str, Any]] = []
    orphans: List[Dict[str, Any]] = []

    for ln in lines:
        try:
            rec = json.loads(ln)
        except json.JSONDecodeError:
            # Skip malformed lines rather than failing the whole import.
            logger.warning("%s skipping malformed JSONL line", LOG_PREFIX)
            continue
        if not isinstance(rec, dict):
            continue

        rec_id = rec.get("id")
        parent_id = rec.pop("__parentId", None)

        if isinstance(rec_id, str):
            by_id[rec_id] = rec

        if parent_id is None:
            roots.append(rec)
        else:
            parent = by_id.get(parent_id)
            if parent is None:
                # Bulk ops can emit children before parents in some edge
                # cases (e.g. malformed query). Park them and we'll
                # re-attempt at the end.
                orphans.append({"__parent": parent_id, "_rec": rec})
            else:
                parent.setdefault("__children", []).append(rec)

    # Second pass for orphans (parent might have appeared later).
    for o in orphans:
        parent = by_id.get(o["__parent"])
        if parent is not None:
            parent.setdefault("__children", []).append(o["_rec"])
        else:
            # Truly orphaned — surface it as a root rather than dropping.
            roots.append(o["_rec"])

    return roots


# ---------------------------------------------------------------------------
# Public API: run_bulk_export
# ---------------------------------------------------------------------------


PostGraphQLFn = Callable[[str, Optional[Dict[str, Any]]], Dict[str, Any]]
GetUrlFn = Callable[[str, int], Tuple[int, str]]


def run_bulk_export(
    *,
    inner_query: str,
    post_graphql: PostGraphQLFn,
    get_url: GetUrlFn,
    poll_interval_sec: float = DEFAULT_POLL_INTERVAL_SEC,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    download_timeout_sec: int = DEFAULT_DOWNLOAD_TIMEOUT_SEC,
    download_max_bytes: int = DEFAULT_DOWNLOAD_MAX_BYTES,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock_fn: Callable[[], float] = time.monotonic,
) -> BulkOperationResult:
    """Submit a bulk operation, poll until terminal, download & parse JSONL.

    Parameters
    ----------
    inner_query :
        The original GraphQL query (with pagination args). We'll strip
        the args before submission.
    post_graphql :
        ``(query, variables) -> data`` callable that the caller wires up
        with its auth + endpoint. Must return the GraphQL ``data``
        payload (post-error envelope) or raise on transport/HTTP error.
    get_url :
        ``(url, timeout) -> (status_code, body)`` callable for the JSONL
        download. The download is from a Google Cloud Storage signed URL,
        NOT the Shopify Admin endpoint, so auth headers must NOT be added.
    poll_interval_sec / timeout_sec :
        Polling cadence and total budget.

    Returns
    -------
    A ``BulkOperationResult``. On any failure ``ok`` is False and the
    caller should fall back to regular pagination.
    """

    start = clock_fn()

    # 1. Strip pagination args and wrap.
    stripped = strip_query_for_bulk_ops(inner_query)
    submit_mutation = build_submit_mutation(stripped)

    # 2. Submit.
    try:
        submit_data = post_graphql(submit_mutation, None)
    except Exception as e:
        return BulkOperationResult(
            status="userError",
            error="submit transport error: " + _scrub(str(e)),
            elapsed_sec=clock_fn() - start,
        )

    payload = (submit_data or {}).get("bulkOperationRunQuery") or {}
    user_errors = payload.get("userErrors") or []
    if user_errors:
        msg = "; ".join(
            str((ue or {}).get("message") or "")
            for ue in user_errors[:3]
            if isinstance(ue, dict)
        )
        return BulkOperationResult(
            status="userError",
            error="bulkOperationRunQuery userErrors: " + msg,
            elapsed_sec=clock_fn() - start,
        )

    bulk_op = payload.get("bulkOperation") or {}
    op_id = bulk_op.get("id")
    if not op_id:
        return BulkOperationResult(
            status="userError",
            error="bulkOperationRunQuery returned no bulkOperation",
            elapsed_sec=clock_fn() - start,
        )

    logger.info("%s submitted bulk op %s", LOG_PREFIX, op_id)

    # 3. Poll.
    last_status: Optional[str] = None
    last_op: Dict[str, Any] = {}
    while True:
        elapsed = clock_fn() - start
        if elapsed > timeout_sec:
            return BulkOperationResult(
                status="timeout",
                error="bulk op did not complete within %ds (last_status=%s)" % (timeout_sec, last_status),
                operation_id=op_id,
                elapsed_sec=elapsed,
            )

        try:
            poll_data = post_graphql(POLL_QUERY, None)
        except Exception as e:
            # Transient poll errors retry; permanent errors caller can
            # decide via timeout. Sleep and loop.
            logger.warning("%s poll transport error (will retry): %s", LOG_PREFIX, _scrub(str(e)))
            sleep_fn(poll_interval_sec)
            continue

        op = (poll_data or {}).get("currentBulkOperation") or {}
        last_op = op
        status = (op.get("status") or "").upper()
        last_status = status

        if status in _TERMINAL_OK:
            break
        if status in _TERMINAL_FAIL:
            terminal_kind = {
                "FAILED": "failed",
                "CANCELED": "cancelled",
                "CANCELLED": "cancelled",
                "EXPIRED": "expired",
            }.get(status, "failed")
            return BulkOperationResult(
                status=terminal_kind,
                error="bulk op terminated in %s (errorCode=%s)" % (status, op.get("errorCode")),
                operation_id=op_id,
                object_count=_as_int(op.get("objectCount")),
                elapsed_sec=clock_fn() - start,
            )

        err_code = op.get("errorCode")
        if err_code:
            # Sometimes Shopify keeps RUNNING but sets errorCode mid-flight.
            return BulkOperationResult(
                status="failed",
                error="bulk op errorCode=%s while status=%s" % (err_code, status),
                operation_id=op_id,
                elapsed_sec=clock_fn() - start,
            )

        sleep_fn(poll_interval_sec)

    # 4. Download.
    url = last_op.get("url")
    object_count = _as_int(last_op.get("objectCount"))
    file_size = _as_int(last_op.get("fileSize"))

    if not url:
        # COMPLETED with no URL means zero rows matched (Shopify
        # convention). Return success with empty rows.
        return BulkOperationResult(
            status="completed",
            rows=[],
            row_count=0,
            operation_id=op_id,
            object_count=object_count or 0,
            file_size=file_size,
            elapsed_sec=clock_fn() - start,
        )

    try:
        status_code, body = get_url(url, download_timeout_sec)
    except Exception as e:
        return BulkOperationResult(
            status="downloadError",
            error="download transport error: " + _scrub(str(e)),
            operation_id=op_id,
            object_count=object_count,
            file_size=file_size,
            download_url=_redact_url(url),
            elapsed_sec=clock_fn() - start,
        )

    if status_code != 200:
        return BulkOperationResult(
            status="downloadError",
            error="download returned HTTP %d" % status_code,
            operation_id=op_id,
            object_count=object_count,
            file_size=file_size,
            download_url=_redact_url(url),
            elapsed_sec=clock_fn() - start,
        )

    if download_max_bytes and isinstance(body, (str, bytes)) and len(body) > download_max_bytes:
        return BulkOperationResult(
            status="downloadError",
            error="download exceeded max bytes (%d > %d)" % (len(body), download_max_bytes),
            operation_id=op_id,
            object_count=object_count,
            file_size=file_size,
            download_url=_redact_url(url),
            elapsed_sec=clock_fn() - start,
        )

    if isinstance(body, bytes):
        try:
            body = body.decode("utf-8")
        except UnicodeDecodeError as e:
            return BulkOperationResult(
                status="downloadError",
                error="download body not utf-8: " + str(e),
                operation_id=op_id,
                elapsed_sec=clock_fn() - start,
            )

    try:
        rows = parse_jsonl(body)
    except Exception as e:
        return BulkOperationResult(
            status="downloadError",
            error="JSONL parse error: " + _scrub(str(e)),
            operation_id=op_id,
            elapsed_sec=clock_fn() - start,
        )

    return BulkOperationResult(
        status="completed",
        rows=rows,
        row_count=len(rows),
        operation_id=op_id,
        object_count=object_count,
        file_size=file_size,
        download_url=_redact_url(url),
        elapsed_sec=clock_fn() - start,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# Scrub anything that looks like a Shopify access token / OAuth token
# from error strings before logging. Shopify tokens are typically:
#   shpat_<32-hex>, shpca_<...>, shpss_<...>, or generic 32+ char hex.
_SECRET_RE = re.compile(
    r"(shp(at|ca|ss|pa)_[A-Za-z0-9]+|"
    r"X-Shopify-Access-Token\s*[:=]\s*\S+|"
    r"access_token\s*[:=]\s*\S+|"
    r"Bearer\s+\S+)",
    re.IGNORECASE,
)


def _scrub(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return _SECRET_RE.sub("<redacted>", s)


def _redact_url(url: str) -> str:
    """Return a download URL with query params (signed token) stripped."""
    if not isinstance(url, str):
        return ""
    return url.split("?", 1)[0] + "?<signed>"


# ---------------------------------------------------------------------------
# Env-config helpers (used by the caller in connector.py)
# ---------------------------------------------------------------------------


def env_threshold(default: int = 10000) -> int:
    raw = os.environ.get("SHOPIFY_BULK_OPS_THRESHOLD")
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def env_timeout(default: int = DEFAULT_TIMEOUT_SEC) -> int:
    raw = os.environ.get("SHOPIFY_BULK_OPS_TIMEOUT")
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def env_poll_interval(default: float = DEFAULT_POLL_INTERVAL_SEC) -> float:
    raw = os.environ.get("SHOPIFY_BULK_OPS_POLL_INTERVAL")
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default
