"""
INV-1 isolation contract for the azure-blob storage connector. A user Azure Blob
connection must NEVER be wired to rsync-ai's internal MinIO claim-check store.

Three guarantees, three tests (mirrors aws-s3/gcs test_inv1_isolation.py):
  1. Static: the connector module reads no MINIO_* env var (only AZURE_STORAGE_* / config).
  2. Config provenance: even with MINIO_* env present, _get_config() picks up no
     MinIO credentials/endpoint — config stays Azure-only.
  3. Endpoint deny-guard: pointing a custom endpoint at the internal MinIO (or
     its `minio` alias) is rejected via the shared storage_safety guard.

Tests (1) and (3, via storage_safety) run standalone with stdlib only. Test (2)
and the export-level guard instantiate AzureBlobMCPServer (needs base_connector +
azure-storage-blob) and run under pytest in the connector image.
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
    """With MINIO_* set but no Azure creds, config must stay Azure-only (never MinIO)."""
    import connector as C
    monkeypatch.setenv("MINIO_ENDPOINT_URL", "http://rsync-ai-minio:9000")
    monkeypatch.setenv("MINIO_ROOT_USER", "minioadmin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "minioadmin")
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_NAME", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_KEY", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_SAS_TOKEN", raising=False)
    srv = C.AzureBlobMCPServer()
    cfg = srv._get_config({"config": {}})
    assert cfg.get("connection_string") == "", cfg
    assert cfg.get("account_key") == "", cfg
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
            assert_external_endpoint(ep, connector_type="azure-blob")
        except StorageEndpointDenied:
            continue
        raise AssertionError(f"internal endpoint not denied: {ep}")


def test_external_and_empty_endpoints_pass():
    """Azure (empty) and a customer's own/emulator endpoint must pass."""
    from rsync_protocol.storage_safety import assert_external_endpoint
    assert_external_endpoint("", connector_type="azure-blob")  # managed Azure
    assert_external_endpoint("https://myacct.blob.core.windows.net", connector_type="azure-blob")
    # Azurite emulator on the MCP network is allowed (not internal MinIO).
    assert_external_endpoint("http://azurite:10000/devstoreaccount1", connector_type="azure-blob")


def test_export_surfaces_denied_endpoint_as_error():
    """The deny-guard must fail export cleanly, not crash the worker."""
    import connector as C
    srv = C.AzureBlobMCPServer()
    res = srv.export({
        "config": {"bucket": "b", "account_name": "x", "account_key": "y",
                   "endpoint_url": "http://rsync-ai-minio:9000"},
        "table": "data",
    })
    assert res.get("success") is False
    assert "internal MinIO" in (res.get("error") or "")


if __name__ == "__main__":
    test_connector_source_has_no_minio_env_reference()
    test_internal_minio_endpoint_is_denied()
    test_external_and_empty_endpoints_pass()
    print("INV-1 static + endpoint contract checks PASS")
