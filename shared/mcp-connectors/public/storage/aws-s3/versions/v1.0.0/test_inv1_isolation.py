"""
INV-1 isolation contract for the aws-s3 (and template for gcs/azure-blob)
storage connector. A user S3 connection must NEVER be wired to rsync-ai's
internal MinIO claim-check store.

Three guarantees, three tests:
  1. Static: the connector module reads no MINIO_* env var (only AWS_S3_* /
     config). The denylist lives in rsync_protocol.storage_safety, not here.
  2. Config provenance: even with MINIO_* env present, _get_config() picks up
     no MinIO credentials/endpoint — config stays AWS-only.
  3. Endpoint deny-guard: pointing endpoint_url at the internal MinIO (or its
     `minio` alias) is rejected via every client entry point.
"""
import os
import re
import sys

# Bootstrap imports the same way connector.py does (base_connector + rsync_protocol).
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
_d = _HERE
while _d != os.path.dirname(_d):
    if os.path.isdir(os.path.join(_d, "rsync_protocol")):
        sys.path.insert(0, _d)
        break
    _d = os.path.dirname(_d)


def test_connector_source_has_no_minio_env_reference():
    """The connector must never read MINIO_* — that is the internal store."""
    with open(os.path.join(_HERE, "connector.py"), "r", encoding="utf-8") as f:
        src = f.read()
    hits = re.findall(r"MINIO_[A-Z_]+", src)
    assert not hits, f"connector.py must not reference MINIO_* env vars; found {hits}"


def test_get_config_ignores_minio_env(monkeypatch):
    """With MINIO_* set but no aws creds, config must stay empty (never MinIO)."""
    import connector as C
    monkeypatch.setenv("MINIO_ENDPOINT_URL", "http://rsync-ai-minio:9000")
    monkeypatch.setenv("MINIO_ROOT_USER", "minioadmin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "minioadmin")
    monkeypatch.delenv("AWS_S3_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_S3_SECRET_ACCESS_KEY", raising=False)
    srv = C.AwsS3MCPServer()
    cfg = srv._get_config({"config": {}})
    assert cfg.get("access_key_id") == "", cfg
    assert cfg.get("secret_access_key") == "", cfg
    # No MinIO endpoint ever leaks in from env.
    assert not cfg.get("endpoint_url") and not cfg.get("endpoint"), cfg


def test_internal_minio_endpoint_is_denied():
    """endpoint_url pointing at the internal MinIO / alias must be refused."""
    from rsync_protocol.storage_safety import (
        assert_external_endpoint,
        StorageEndpointDenied,
    )
    for ep in (
        "http://rsync-ai-minio:9000",
        "minio:9000",
        "https://minio",
        "rsync-ai-minio",
    ):
        try:
            assert_external_endpoint(ep, connector_type="aws-s3")
        except StorageEndpointDenied:
            continue
        raise AssertionError(f"internal endpoint not denied: {ep}")


def test_external_and_empty_endpoints_pass():
    """Real AWS (empty) and a user's own S3-compatible host must pass."""
    from rsync_protocol.storage_safety import assert_external_endpoint
    assert_external_endpoint("", connector_type="aws-s3")  # AWS S3
    assert_external_endpoint("https://s3.amazonaws.com", connector_type="aws-s3")
    # A genuinely different MinIO (the connector-facing one, or a customer's) is allowed.
    assert_external_endpoint("http://rsync-ai-minio-v1-0-0-mcp:9000", connector_type="aws-s3")
    assert_external_endpoint("https://minio.customer.example.com:9000", connector_type="aws-s3")


def test_export_surfaces_denied_endpoint_as_error():
    """The deny-guard must fail the export cleanly, not crash the worker.

    endpoint_url MUST sit inside `config` — that is the only place the connector
    reads it (every client-build site + export use `config.get("endpoint_url")`;
    `_get_config` does not hoist a top-level key). At the top level it would be
    ignored, the guard would never fire, and export would make a real network call.
    Matches the gcs / azure-blob INV-1 suites, whose export tests nest it in config.
    """
    import connector as C
    srv = C.AwsS3MCPServer()
    res = srv.export({
        "config": {"bucket": "b", "access_key_id": "k", "secret_access_key": "s",
                   "endpoint_url": "http://rsync-ai-minio:9000"},
        "table": "data",
    })
    assert res.get("success") is False
    assert "internal MinIO" in (res.get("error") or "")


if __name__ == "__main__":
    # Allow running standalone (no pytest) for a quick contract check.
    test_connector_source_has_no_minio_env_reference()
    test_internal_minio_endpoint_is_denied()
    test_external_and_empty_endpoints_pass()
    print("INV-1 static + endpoint contract checks PASS")
