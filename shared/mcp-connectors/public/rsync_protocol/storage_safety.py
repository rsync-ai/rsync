"""
INV-1 isolation guard for hand-built object-storage connectors.

A user-facing object-storage connection (aws-s3, gcs, azure-blob) must NEVER be
allowed to resolve to rsync-ai's *internal* MinIO — the env-driven infra store
used only for claim-check overflow. The two are different systems: the internal
MinIO is selected by env (MINIO_ENDPOINT_URL, default http://rsync-ai-minio:9000)
and reached only via the internal `minio-mcp`; the user connector is driven by a
connection record. The only crossover risk is a user typing the internal endpoint
(or its `minio` alias) into the optional S3-compatible `endpoint_url` field.

This module centralizes that check so all three storage connectors enforce it
identically. It denies the *internal host specifically*, not all private IPs —
self-hosted operators legitimately run their own MinIO on a private address. A
hardened, hosted-only mode (STORAGE_DENY_PRIVATE_ENDPOINTS=true) additionally
denies private/loopback/link-local endpoints to also cover the SSRF surface.
"""
from __future__ import annotations

import ipaddress
import os
from urllib.parse import urlparse


class StorageEndpointDenied(ValueError):
    """Raised when a user-supplied storage endpoint resolves to rsync-ai's own
    internal MinIO (or, in hardened mode, any private/loopback endpoint)."""


def _host_of(endpoint: str) -> str:
    """Extract a lowercase hostname from a URL or bare host[:port] string."""
    s = (endpoint or "").strip()
    if not s:
        return ""
    if "://" not in s:
        s = "//" + s  # let urlparse populate .hostname for bare host[:port]
    try:
        host = urlparse(s).hostname or ""
    except Exception:
        host = ""
    return host.strip().lower().rstrip(".")


def _internal_minio_hosts() -> set:
    """The set of hostnames that ARE the internal infra MinIO. Always includes
    the `minio` alias and the `rsync-ai-minio` service name (the split-brain
    footgun), plus whatever MINIO_ENDPOINT_URL points at, plus any operator
    overrides in STORAGE_INTERNAL_MINIO_HOSTS (comma/space separated)."""
    hosts = {"minio", "rsync-ai-minio"}
    for env_key in ("MINIO_ENDPOINT_URL", "MINIO_ENDPOINT"):
        h = _host_of(os.getenv(env_key) or "")
        if h:
            hosts.add(h)
    extra = os.getenv("STORAGE_INTERNAL_MINIO_HOSTS") or ""
    for part in extra.replace(",", " ").split():
        h = _host_of(part) or part.strip().lower()
        if h:
            hosts.add(h)
    return hosts


def _hardened() -> bool:
    return (os.getenv("STORAGE_DENY_PRIVATE_ENDPOINTS") or "").strip().lower() in (
        "1", "true", "yes", "on",
    )


def _is_private_ip(host: str) -> bool:
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False  # a hostname, not an IP literal
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_unspecified
    )


def assert_external_endpoint(endpoint_url, *, connector_type: str = "storage") -> None:
    """
    INV-1 guard. Raise StorageEndpointDenied if `endpoint_url` points at the
    internal MinIO. An empty endpoint means the real cloud provider (AWS S3,
    GCS, Azure) and always passes. In hardened mode, private/loopback IP
    endpoints are additionally denied.
    """
    host = _host_of(endpoint_url)
    if not host:
        return  # no endpoint => managed cloud provider; nothing to guard
    if host in _internal_minio_hosts():
        raise StorageEndpointDenied(
            f"Endpoint host '{host}' is rsync-ai's internal MinIO and cannot be "
            f"used as a {connector_type} connection. Internal MinIO is reserved "
            f"for claim-check staging. Point this connector at your own "
            f"S3-compatible endpoint (or leave endpoint_url empty for AWS S3)."
        )
    if _hardened() and _is_private_ip(host):
        raise StorageEndpointDenied(
            f"Endpoint host '{host}' is a private/loopback address, which is "
            f"denied in hardened mode (STORAGE_DENY_PRIVATE_ENDPOINTS). Use a "
            f"publicly routable S3-compatible endpoint."
        )
