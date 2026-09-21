#!/usr/bin/env python3
"""
Azure Blob Storage MCP Connector — first-class SOURCE + destination.

HAND-BUILT (not tool-generator output). The provider-agnostic source pipeline
(file selection, single/per_table/dynamic table mapping, sampling, schema
inference, cursor-paged export, provenance columns, the INV-1 endpoint guard)
is inherited verbatim from rsync_protocol.object_storage_source.
ObjectStorageSourceMixin — shared with aws-s3/gcs so there is one source
implementation, no per-connector drift. This file supplies ONLY the Azure-specific
primitives: auth/client, list_blobs, download_blob, and a basic object upload.

Object storage as a SOURCE = batch + last-modified incremental, NO CDC (no
transaction log; matches Fivetran/Airbyte). Category: cloud_storage.

Azure terminology note: a "container" is the unit the mixin calls `bucket`. The
config accepts either `bucket` or `container` (normalized in _get_config).
"""

import sys
import os
import logging
from typing import Dict, Any, List

# Add parent dir for base_connector; walk up to public/ for rsync_protocol.
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
_d = os.path.dirname(os.path.abspath(__file__))
while _d != os.path.dirname(_d):
    if os.path.isdir(os.path.join(_d, "rsync_protocol")):
        sys.path.insert(0, _d)
        break
    _d = os.path.dirname(_d)

from base_connector import (
    BaseMCPConnector,
    StorageHandler,
    ExportResult,
)
from rsync_protocol.object_storage_source import ObjectStorageSourceMixin

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AzureBlobMCPServer(ObjectStorageSourceMixin, BaseMCPConnector):
    """MCP Server for Azure Blob Storage."""

    def __init__(self):
        super().__init__()

        self.connector_type = "azure-blob"
        self.connector_category = "cloud_storage"
        self.supports_source = True
        self.supports_destination = True
        self.supports_cdc = False

        self.supported_formats = ['csv', 'tsv', 'json', 'jsonl', 'parquet']
        # Move-capability modalities (universal-blob-passthrough plan §2): object
        # storage can move structured records AND opaque bytes (blobs). Distinct from
        # supported_formats (serialization encodings); the orchestrator capability gate
        # reads this to allow/deny a blob (raw-bytes) move into this destination.
        self.supported_modalities = ['structured', 'blob']
        self.max_batch_size = 10000

        self._storage_handler = StorageHandler(connector=self)
        self.log("Azure Blob MCP Server initialized")

    # =========================================================================
    # CONFIGURATION + CLIENT (Azure-specific)
    # =========================================================================
    def _get_config(self, params: Dict) -> Dict:
        """Extract config. Reads only AZURE_STORAGE_* env (never MINIO_* — that is
        the internal claim-check store, INV-1). Normalizes `container` -> `bucket`
        so the shared source mixin (which keys on `bucket`) works unchanged."""
        config = params.get('config', params) if params else {}
        if not config.get('connection_string'):
            config['connection_string'] = os.getenv('AZURE_STORAGE_CONNECTION_STRING', '')
        if not config.get('account_name'):
            config['account_name'] = os.getenv('AZURE_STORAGE_ACCOUNT_NAME', '')
        if not config.get('account_key'):
            config['account_key'] = os.getenv('AZURE_STORAGE_ACCOUNT_KEY', '')
        if not config.get('sas_token'):
            config['sas_token'] = os.getenv('AZURE_STORAGE_SAS_TOKEN', '')
        if not config.get('bucket') and config.get('container'):
            config['bucket'] = config.get('container')
        return config

    def _blob_endpoint(self, config: Dict):
        """A custom blob endpoint (private Azure or an Azurite emulator). Empty
        means real Azure (https://<account>.blob.core.windows.net)."""
        return (config.get('endpoint_url') or config.get('blob_endpoint')
                or config.get('account_url') or config.get('endpoint') or None)

    def _get_blob_service_client(self, config: Dict):
        """Build an azure-storage-blob BlobServiceClient.

        Auth precedence: connection_string → account_name+account_key →
        account_name+sas_token → anonymous (public/emulator). A custom
        `endpoint_url`/`blob_endpoint` (private Azure or an Azurite emulator) is
        allowed but FIRST passes the INV-1 guard so it can never resolve to
        rsync-ai's internal MinIO. account_key auth is routed through a built
        connection string so it works identically against real Azure and Azurite."""
        from azure.storage.blob import BlobServiceClient

        endpoint = self._blob_endpoint(config)
        self._assert_safe_endpoint(endpoint)

        conn = config.get('connection_string') or ''
        account_name = config.get('account_name') or ''
        account_key = config.get('account_key') or ''
        sas_token = config.get('sas_token') or config.get('sas') or ''

        if conn:
            return BlobServiceClient.from_connection_string(conn)

        if account_key:
            if endpoint:
                proto = 'https' if str(endpoint).lower().startswith('https') else 'http'
                conn_str = (
                    f"DefaultEndpointsProtocol={proto};"
                    f"AccountName={account_name or 'devstoreaccount1'};"
                    f"AccountKey={account_key};"
                    f"BlobEndpoint={endpoint};"
                )
            else:
                if not account_name:
                    raise ValueError("account_key requires account_name (or a custom endpoint)")
                conn_str = (
                    f"DefaultEndpointsProtocol=https;AccountName={account_name};"
                    f"AccountKey={account_key};EndpointSuffix=core.windows.net"
                )
            return BlobServiceClient.from_connection_string(conn_str)

        if sas_token:
            base = endpoint or (f"https://{account_name}.blob.core.windows.net" if account_name else None)
            if not base:
                raise ValueError("sas_token requires account_name or a custom endpoint")
            return BlobServiceClient(account_url=base, credential=sas_token)

        if endpoint:
            # Anonymous / public container on a custom endpoint (e.g. Azurite).
            return BlobServiceClient(account_url=endpoint)

        raise ValueError(
            "azure-blob requires one of: connection_string, account_name + account_key, "
            "account_name + sas_token, or a custom endpoint"
        )

    # ---- provider primitives required by ObjectStorageSourceMixin -----------
    def list_objects(
        self,
        *,
        config: Dict,
        bucket: str,
        prefix: str = "",
        max_keys: int = 1000,
    ) -> List[Dict[str, Any]]:
        """List blobs under container/prefix → [{key,size,last_modified}]."""
        client = self._get_blob_service_client(config)
        container = client.get_container_client(bucket)
        out: List[Dict[str, Any]] = []
        for blob in container.list_blobs(name_starts_with=prefix or None):
            lm = getattr(blob, "last_modified", None)
            out.append({
                "key": blob.name,
                "size": getattr(blob, "size", None),
                "last_modified": lm.isoformat() if hasattr(lm, "isoformat") else (str(lm) if lm else ""),
            })
            if len(out) >= max_keys:
                break
        return out

    def download_file(self, *, config: Dict, bucket: str, key: str) -> bytes:
        """Download one blob's bytes."""
        client = self._get_blob_service_client(config)
        blob_client = client.get_container_client(bucket).get_blob_client(key)
        return blob_client.download_blob().readall()

    # =========================================================================
    # CORE OPERATIONS
    # =========================================================================
    def get_capabilities(self, params: Dict = None) -> Dict[str, Any]:
        return {
            "success": True,
            "connector_type": self.connector_type,
            "connector_category": self.connector_category,
            "supports_source": self.supports_source,
            "supports_destination": self.supports_destination,
            "supports_cdc": self.supports_cdc,
            "operations": [
                {"name": "test_connection", "method": "azure-blob_test_connection", "type": "core",
                 "description": "Test connectivity to the data source"},
                {"name": "validate_config", "method": "azure-blob_validate_config", "type": "core",
                 "description": "Validate configuration without connecting"},
                {"name": "discover_schema", "method": "azure-blob_discover_schema", "type": "core",
                 "description": "Discover logical tables and their schemas by sampling objects"},
                {"name": "get_capabilities", "method": "azure-blob_get_capabilities", "type": "core",
                 "description": "Return connector capabilities for agent interaction"},
                {"name": "read", "method": "azure-blob_read", "type": "source",
                 "description": "Read a file (or prefix) from storage"},
                {"name": "import_data", "method": "azure-blob_import_data", "type": "destination",
                 "description": "Write data to storage"},
                {"name": "delete_prefix", "method": "azure-blob_delete_prefix", "type": "destination",
                 "description": "Delete all objects under a container prefix (guardrailed)"},
                {"name": "get_cdc_offsets", "method": "azure-blob_get_cdc_offsets", "type": "destination",
                 "description": "Durable CDC high-water offsets read from this pipeline's blob metadata"},
            ],
            "capabilities": {
                "max_batch_size": self.max_batch_size,
                "supported_formats": self.supported_formats,
                "supported_modalities": self.supported_modalities,
                "supports_cdc": self.supports_cdc,
            },
        }

    def validate_config(self, params: Dict = None) -> Dict[str, Any]:
        if not params:
            return {"valid": False, "errors": ["No configuration provided"]}
        config = self._get_config(params)
        errors: List[str] = []
        warnings: List[str] = []
        if not config.get('bucket') and not config.get('bucket_name') and not config.get('container'):
            errors.append("Missing required field: bucket (a.k.a. container)")
        has_auth = bool(
            config.get('connection_string')
            or (config.get('account_name') and (config.get('account_key') or config.get('sas_token')))
        )
        has_endpoint = bool(self._blob_endpoint(config))
        if not has_auth and not has_endpoint:
            errors.append(
                "Missing auth: provide connection_string, or account_name + account_key, "
                "or account_name + sas_token (or a custom endpoint for anonymous access)"
            )
        return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    def test_connection(self, params: Dict = None) -> Dict[str, Any]:
        config = self._get_config(params)
        try:
            client = self._get_blob_service_client(config)
            bucket = config.get('bucket') or config.get('container') or config.get('bucket_name')
            if bucket:
                # list_blobs is the minimal permission a SOURCE needs; an empty
                # container returns no blobs without raising → still "connected".
                container = client.get_container_client(bucket)
                next(iter(container.list_blobs(results_per_page=1)), None)
                return {"success": True, "message": f"Connection successful (container: {bucket})"}
            next(iter(client.list_containers(results_per_page=1)), None)
            return {"success": True, "message": "Connection successful"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # discover_schema + export are inherited from ObjectStorageSourceMixin.

    def read(self, params: Dict = None) -> Dict[str, Any]:
        """Read a single object (when `key` given) else behave like export."""
        params = params or {}
        prepared = self.prepare_export_data(params)
        if not prepared.get('success'):
            return prepared
        config = prepared.get('config') or {}
        bucket = (prepared.get('table') or params.get('bucket')
                  or (config.get('bucket') if isinstance(config, dict) else None))
        key = params.get("key") or params.get("object_key") or params.get("path")
        if key:
            try:
                content = self.download_file(config=config, bucket=bucket, key=key)
                rec = {"key": key, "size": len(content) if hasattr(content, "__len__") else None,
                       "content": content}
                return ExportResult(success=True, records=[rec], errors=[],
                                    stats={"total_files": 1}).to_dict()
            except Exception as e:
                return {"success": False, "error": str(e)}
        return self.export(params)

    # =========================================================================
    # DESTINATION OPERATIONS (basic; full partitioned dest is Phase 4 in the Go sink)
    # =========================================================================
    def import_data(self, params: Dict) -> Dict[str, Any]:
        params = params or {}

        # Blob (raw-bytes passthrough) write — universal-blob-passthrough plan §3.
        # Fetch the staged bytes by data_ref, verify integrity, write byte-identical
        # with the source content-type carried verbatim (bypasses the row path).
        if (params.get("blob") or params.get("is_blob")) and params.get("data_ref"):
            try:
                from azure.storage.blob import ContentSettings
                body, container, key, content_type = self._prepare_blob_write(params)
                config = self._get_config({"config": params.get("config") or {}})
                client = self._get_blob_service_client(config)
                client.get_container_client(container).get_blob_client(key).upload_blob(
                    body, overwrite=True,
                    content_settings=ContentSettings(content_type=content_type),
                )
                return {
                    "success": True, "bytes_written": len(body),
                    "metadata": {"container": container, "key": key,
                                 "content_type": content_type, "modality": "blob"},
                }
            except Exception as e:
                return {"success": False, "error": str(e)}

        data = params.get("data") or params.get("rows") or params.get("source_data")
        data_ref = params.get("data_ref")
        if data_ref and (data is None or (isinstance(data, list) and len(data) == 0)):
            staging_config = params.get("staging_config") or params.get("config") or {}
            staged = self.read_from_staging(data_ref, staging_config)
            if not staged.get("success"):
                return {"success": False,
                        "error": f"Failed to fetch data from staging: {staged.get('error', 'Unknown error')}"}
            data = staged.get("data")

        params_for_dest = dict(params)
        if data is not None:
            params_for_dest["data"] = data
        prepared = self.prepare_destination_params(params_for_dest)

        config = self._get_config({"config": prepared.get("config") or {}})
        bucket = prepared.get("bucket") or config.get("bucket")
        key = prepared.get("key")
        payload = prepared.get("data")
        fmt = prepared.get("format") or "json"
        compression = prepared.get("compression") or "none"
        content_type = prepared.get("content_type") or "application/octet-stream"

        if not bucket or not key:
            return {"success": False, "error": "Bucket (container) and key are required"}
        if payload is None:
            return {"success": False, "error": "Missing 'data' payload"}

        try:
            from azure.storage.blob import ContentSettings
            client = self._get_blob_service_client(config)
            blob_client = client.get_container_client(bucket).get_blob_client(key)

            raw = bool(params.get("raw") or params.get("raw_data") or params.get("raw_bytes"))
            if raw:
                if isinstance(payload, (bytes, bytearray)):
                    body = bytes(payload)
                else:
                    body = str(payload).encode("utf-8")
            else:
                body = self.convert_data_to_format(payload, fmt, compression)

            # CDC provenance (Tier C): the kafka-mcp-sink stamps each CDC object with its
            # pipeline + Kafka coordinates so get_cdc_offsets can recover the durable
            # high-water mark by listing. Sent with the upload so it lands atomically with
            # the bytes — a blob can never exist without its offsets.
            object_metadata = self._clean_object_metadata(params.get("object_metadata"))
            blob_client.upload_blob(
                body, overwrite=True,
                content_settings=ContentSettings(content_type=content_type),
                metadata=object_metadata or None,
            )
            return {
                "success": True,
                "rows_inserted": len(payload) if isinstance(payload, list) else 1,
                "bytes_written": len(body),
                "metadata": {"bucket": bucket, "key": key, "format": fmt, "compression": compression},
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    # Blob metadata keys the kafka-mcp-sink writes on every CDC object
    # (cdcObjectMetadata in kafka-sink-worker main.go). Keep the two in lockstep.
    CDC_META_PIPELINE_ID = "rsync_pipeline_id"
    CDC_META_TOPIC = "rsync_topic"
    CDC_META_PARTITION = "rsync_partition"
    CDC_META_LAST_OFFSET = "rsync_last_offset"

    @staticmethod
    def _clean_object_metadata(raw: Any) -> Dict[str, str]:
        """Azure blob metadata is a flat str→str map; drop anything else."""
        if not isinstance(raw, dict):
            return {}
        out: Dict[str, str] = {}
        for k, v in raw.items():
            key = str(k).strip()
            if not key or v is None or isinstance(v, (dict, list)):
                continue
            out[key] = str(v)
        return out

    def get_cdc_offsets(self, params: Dict) -> Dict[str, Any]:
        """Durable CDC high-water marks for a pipeline (Tier C, cdc-exactly-once-offsets.md §5).

        Same contract as gcs: every CDC blob the sink writes carries metadata
        {rsync_pipeline_id, rsync_topic, rsync_partition, rsync_last_offset}; this lists
        the pipeline's root (`prefix`, sent by the sink) with metadata included and
        returns the max last offset per (topic, partition) in the §2.3 shape.

        Never an error (§2.3): first run, a missing prefix/pipeline, or any listing
        failure returns {"success": True, "offsets": []}, which the sink treats as
        "nothing to skip".
        """
        params = params or {}
        empty: Dict[str, Any] = {"success": True, "offsets": []}
        pipeline_id = str(params.get("pipeline_id") or "").strip()
        try:
            config = self._get_config(params)
        except Exception as e:  # pragma: no cover - _get_config does not raise today
            logger.warning("azure-blob get_cdc_offsets: bad config (%s); returning no offsets", e)
            return empty
        container = str(params.get("container") or params.get("bucket")
                        or config.get("bucket") or config.get("container") or "").strip()
        prefix = str(params.get("prefix") or "").strip().lstrip("/")
        if not pipeline_id or not container or not prefix:
            # Refuse to scan a whole container: without the pipeline root there is no
            # bounded listing, and a wrong answer is worse than no answer.
            return {**empty, "note": "pipeline_id, container and prefix are required to derive offsets"}

        max_objects = 0
        try:
            max_objects = int(params.get("max_objects") or 0)
        except (TypeError, ValueError):
            max_objects = 0

        best: Dict[tuple, int] = {}
        scanned = 0
        truncated = False
        try:
            client = self._get_blob_service_client(config)
            listing = client.get_container_client(container).list_blobs(
                name_starts_with=prefix, include=["metadata"])
            for blob in listing:
                if max_objects and scanned >= max_objects:
                    truncated = True
                    break
                scanned += 1
                meta = getattr(blob, "metadata", None) or {}
                if str(meta.get(self.CDC_META_PIPELINE_ID) or "").strip() != pipeline_id:
                    continue
                topic = str(meta.get(self.CDC_META_TOPIC) or "").strip()
                if not topic:
                    continue
                try:
                    partition = int(meta.get(self.CDC_META_PARTITION))
                    last = int(meta.get(self.CDC_META_LAST_OFFSET))
                except (TypeError, ValueError):
                    continue
                k = (topic, partition)
                if k not in best or last > best[k]:
                    best[k] = last
        except Exception as e:
            logger.warning("azure-blob get_cdc_offsets: listing %s/%s failed (%s); returning no offsets",
                           container, prefix, e)
            return empty

        offsets = [{"topic": t, "partition": p, "offset": o} for (t, p), o in sorted(best.items())]
        return {"success": True, "offsets": offsets, "objects_scanned": scanned, "truncated": truncated}

    def delete_prefix(self, params: Dict) -> Dict[str, Any]:
        """Delete every blob under a folder prefix, failing closed.

        Contract (pinned by shared/delete_prefix_contract_golden.json; aws-s3 and
        gcs return the same shape):
          {success, deleted, failed, complete, bucket, prefix, errors[, error]}
        - success is True only when complete is True and failed == 0.
        - complete is True only when the listing was walked to its end.
        - The prefix is a folder: 'a/b' is normalised to 'a/b/', so 'a/bc/...'
          is never listed or deleted.
        - Pages are bounded: at most 1000 blobs per listed page, and at most
          max_pages pages. Stopping at max_pages or max_objects with blobs still
          listed is complete=False, success=False -- never a silent truncation.
        - A blob the store refuses to delete is counted in failed, never in
          deleted; a blob already gone (404) counts as deleted.

        Guardrails: prefix must name a folder (refuse deleting the entire container);
        refuse deletes outside a configured path_prefix/prefix folder."""
        params = params or {}
        config = self._get_config(params)
        bucket = params.get("bucket") or params.get("container") or config.get("bucket")
        raw_prefix = params.get("prefix") or params.get("key_prefix") or params.get("path") or ""
        result: Dict[str, Any] = {"success": False, "deleted": 0, "failed": 0, "complete": False,
                                  "bucket": bucket, "prefix": raw_prefix, "errors": []}
        if not bucket or str(bucket).strip() == "":
            result["error"] = "Bucket (container) is required"
            return result
        if str(raw_prefix).strip().strip("/") == "":
            result["error"] = "prefix is required (refusing to delete entire container)"
            return result

        # Folder boundary: 'a/b' must never match 'a/bc/...'.
        prefix = str(raw_prefix).strip()
        if not prefix.endswith("/"):
            prefix += "/"
        result["prefix"] = prefix

        base_prefix = (config.get("path_prefix") or config.get("prefix") or "").strip().strip("/")
        if base_prefix and not prefix.lstrip("/").startswith(base_prefix + "/"):
            result["error"] = (f"Refusing to delete outside configured path_prefix/prefix '{base_prefix}' "
                               f"(requested '{raw_prefix}')")
            return result

        page_size = 1000
        try:
            max_pages = int(params.get("max_pages") or 100000)
            max_objects = int(params.get("max_objects") or 0) or None
        except (TypeError, ValueError):
            result["error"] = "max_pages and max_objects must be integers"
            return result

        def _fail_key(key: Any, err: Any) -> None:
            result["failed"] += 1
            if len(result["errors"]) < 20:
                result["errors"].append({"key": key, "error": str(err)[:300]})

        def _already_gone(exc: Exception) -> bool:
            return (getattr(exc, "code", None) == 404
                    or getattr(exc, "status_code", None) == 404
                    or type(exc).__name__ == "ResourceNotFoundError")

        try:
            client = self._get_blob_service_client(config)
            container = client.get_container_client(bucket)
            page_iter = iter(container.list_blobs(name_starts_with=prefix,
                                                  results_per_page=page_size).by_page())
            pages = 0
            while True:
                try:
                    page = next(page_iter)
                except StopIteration:
                    result["complete"] = True
                    break
                pages += 1
                if pages > max_pages:
                    result["error"] = f"stopped after max_pages={max_pages} before the listing ended"
                    return result
                blobs = [b for b in page if str(getattr(b, "name", "") or "").startswith(prefix)]

                over_bound = False
                if max_objects is not None:
                    room = max(max_objects - (result["deleted"] + result["failed"]), 0)
                    if len(blobs) > room:
                        blobs = blobs[:room]
                        over_bound = True

                for blob in blobs:
                    try:
                        container.delete_blob(blob.name)
                        result["deleted"] += 1
                    except Exception as e:
                        if _already_gone(e):
                            result["deleted"] += 1
                        else:
                            _fail_key(blob.name, e)

                if over_bound:
                    result["error"] = f"stopped at max_objects={max_objects} with blobs left under the prefix"
                    return result
        except Exception as e:
            result["error"] = str(e)
            return result

        if result["failed"]:
            result["error"] = f"{result['failed']} blob(s) under the prefix could not be deleted"
        result["success"] = result["complete"] and result["failed"] == 0
        return result


# =============================================================================
# HTTP SERVER (Docker) — mirrors the aws-s3/gcs entrypoint
# =============================================================================
def create_http_app():
    from fastapi import FastAPI, HTTPException
    # Decimal crosses this boundary as a STRING, never a float. FastAPI's
    # jsonable_encoder maps Decimal -> float (ENCODERS_BY_TYPE), which silently
    # destroys precision on the way to the sink: a numeric 123456789012345678.5
    # arrives as 1.2345678901234568e+17, and the orchestrator writes that back
    # out as 123456789012345680 -- a changed value, with no error raised anywhere.
    # The stdio path already serialises with json.dumps(..., default=str), so
    # HTTP mode was the only lossy leg -- and it is the leg every containerised
    # deployment uses, which is why no stdio-based test could ever see it.
    from decimal import Decimal as _Decimal
    from fastapi.encoders import ENCODERS_BY_TYPE as _ENCODERS_BY_TYPE
    _ENCODERS_BY_TYPE[_Decimal] = str

    app = FastAPI(title="Azure Blob MCP Connector")
    server = AzureBlobMCPServer()

    @app.get("/health")
    async def health():
        return {"status": "healthy", "connector": server.connector_type}

    @app.post("/mcp")
    async def mcp(request: dict):
        return server.handle_request(request)

    @app.post("/invoke/{tool_name}")
    async def invoke(tool_name: str, params: dict = {}):
        try:
            method_name = tool_name.replace("-", "_")
            # Security: this route bypasses _handle_tool_call, so it needs its own
            # copy of that dispatcher's guard. Without it a crafted name like
            # "_cleanup_worker" resolves via getattr and exposes internals to any
            # caller that can reach the container. Tool handlers are always public.
            if method_name.startswith("_"):
                raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
            if hasattr(server, method_name):
                return getattr(server, method_name)(params)
            prefixed = f"azure_blob_{method_name}"
            if hasattr(server, prefixed):
                return getattr(server, prefixed)(params)
            raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/capabilities")
    async def capabilities():
        return server.get_capabilities()

    @app.post("/test_connection")
    async def test_connection(params: dict = {}):
        return server.test_connection(params)

    @app.post("/validate_config")
    async def validate_config(params: dict = {}):
        return server.validate_config(params)

    @app.post("/discover_schema")
    async def discover_schema(params: dict = {}):
        return server.discover_schema(params)

    return app


if __name__ == "__main__":
    http_mode = os.getenv("MCP_HTTP_MODE", "false").lower() == "true"
    port = int(os.getenv("MCP_PORT", os.getenv("PORT", "8000")))
    if http_mode or os.getenv("DOCKER_CONTAINER"):
        import uvicorn
        app = create_http_app()
        logger.info(f"🚀 Starting Azure Blob MCP Server in HTTP mode on port {port}")
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        server = AzureBlobMCPServer()
        server.run()
