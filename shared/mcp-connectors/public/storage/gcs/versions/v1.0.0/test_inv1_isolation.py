"""
INV-1 isolation contract for the gcs storage connector. A user GCS connection
must NEVER be wired to rsync-ai's internal MinIO claim-check store.

Three guarantees, three tests (mirrors aws-s3's test_inv1_isolation.py):
  1. Static: the connector module reads no MINIO_* env var (only GCS_* / config).
  2. Config provenance: even with MINIO_* env present, _get_config() picks up no
     MinIO credentials/endpoint — config stays GCS-only.
  3. Endpoint deny-guard: pointing a custom endpoint at the internal MinIO (or
     its `minio` alias) is rejected via the shared storage_safety guard.

Tests (1) and (3, via storage_safety) run standalone with stdlib only. Test (2)
and the export-level guard instantiate GcsMCPServer (needs base_connector +
google-cloud-storage) and run under pytest in the connector image.
"""
import os
import re
import sys

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
    """With MINIO_* set but no GCS creds, config must stay GCS-only (never MinIO)."""
    import connector as C
    monkeypatch.setenv("MINIO_ENDPOINT_URL", "http://rsync-ai-minio:9000")
    monkeypatch.setenv("MINIO_ROOT_USER", "minioadmin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "minioadmin")
    monkeypatch.delenv("GCS_SERVICE_ACCOUNT_JSON", raising=False)
    monkeypatch.delenv("GCS_PROJECT_ID", raising=False)
    srv = C.GcsMCPServer()
    cfg = srv._get_config({"config": {}})
    assert cfg.get("service_account_json") == "", cfg
    # No MinIO endpoint ever leaks in from env.
    assert not cfg.get("endpoint_url") and not cfg.get("endpoint"), cfg


def test_internal_minio_endpoint_is_denied():
    """A custom endpoint at the internal MinIO / alias must be refused."""
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
            assert_external_endpoint(ep, connector_type="gcs")
        except StorageEndpointDenied:
            continue
        raise AssertionError(f"internal endpoint not denied: {ep}")


def test_external_and_empty_endpoints_pass():
    """Google Cloud (empty) and a customer's own/emulator endpoint must pass."""
    from rsync_protocol.storage_safety import assert_external_endpoint
    assert_external_endpoint("", connector_type="gcs")  # managed GCS
    assert_external_endpoint("https://storage.googleapis.com", connector_type="gcs")
    # fake-gcs-server emulator on the MCP network is allowed (not internal MinIO).
    assert_external_endpoint("http://fake-gcs-server:4443", connector_type="gcs")


def test_export_surfaces_denied_endpoint_as_error():
    """The deny-guard must fail export cleanly, not crash the worker."""
    import connector as C
    srv = C.GcsMCPServer()
    res = srv.export({
        "config": {"bucket": "b", "endpoint_url": "http://rsync-ai-minio:9000"},
        "table": "data",
    })
    assert res.get("success") is False
    assert "internal MinIO" in (res.get("error") or "")


if __name__ == "__main__":
    test_connector_source_has_no_minio_env_reference()
    test_internal_minio_endpoint_is_denied()
    test_external_and_empty_endpoints_pass()
    print("INV-1 static + endpoint contract checks PASS")
