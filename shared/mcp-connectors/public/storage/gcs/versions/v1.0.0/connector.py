#!/usr/bin/env python3
"""
Google Cloud Storage (GCS) MCP Connector — first-class SOURCE + destination.

HAND-BUILT (not tool-generator output). The provider-agnostic source pipeline
(file selection, single/per_table/dynamic table mapping, sampling, schema
inference, cursor-paged export, provenance columns, the INV-1 endpoint guard)
is inherited verbatim from rsync_protocol.object_storage_source.
ObjectStorageSourceMixin — shared with aws-s3/azure-blob so there is one source
implementation, no per-connector drift. This file supplies ONLY the GCS-specific
primitives: auth/client, list_blobs, download_blob, and a basic object upload.

Object storage as a SOURCE = batch + last-modified incremental, NO CDC (no
transaction log; matches Fivetran/Airbyte). Category: cloud_storage.
"""

import sys
import os
import logging
import json
from typing import Dict, Any, List, Optional

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


class GcsMCPServer(ObjectStorageSourceMixin, BaseMCPConnector):
    """MCP Server for Google Cloud Storage."""

    def __init__(self):
        super().__init__()

        self.connector_type = "gcs"
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
        self.log("GCS MCP Server initialized")

    # =========================================================================
    # CONFIGURATION + CLIENT (GCS-specific)
    # =========================================================================
    def _get_config(self, params: Dict) -> Dict:
        """Extract config. Reads only GCS_* env (never MINIO_* — that is the
        internal claim-check store, INV-1)."""
        config = params.get('config', params) if params else {}
        if not config.get('service_account_json'):
            config['service_account_json'] = os.getenv('GCS_SERVICE_ACCOUNT_JSON', '')
        if not config.get('project_id'):
            config['project_id'] = os.getenv('GCS_PROJECT_ID', '')
        return config

    def _service_account_info(self, config: Dict) -> Optional[Dict]:
        sa = config.get('service_account_json') or config.get('service_account') or ''
        if not sa:
            return None
        if isinstance(sa, dict):
            return sa
        try:
            return json.loads(sa)
        except Exception as e:
            raise ValueError(f"service_account_json is not valid JSON: {e}")

    def _get_gcs_client(self, config: Dict):
        """Build a google-cloud-storage Client.

        Auth precedence: explicit service-account JSON → ADC. A custom
        `endpoint_url`/`api_endpoint` (private GCS or a fake-gcs-server emulator)
        is allowed but FIRST passes the INV-1 guard so it can never resolve to
        rsync-ai's internal MinIO. With an endpoint and no service account we use
        anonymous credentials (the emulator case)."""
        from google.cloud import storage

        endpoint = (config.get('endpoint_url') or config.get('api_endpoint')
                    or config.get('endpoint') or None)
        self._assert_safe_endpoint(endpoint)

        project = config.get('project_id') or config.get('project') or None
        client_options = {"api_endpoint": endpoint} if endpoint else None
        info = self._service_account_info(config)

        if info:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_info(info)
            return storage.Client(
                project=project or info.get('project_id'),
                credentials=creds,
                client_options=client_options,
            )

        if endpoint:
            # Emulator / anonymous endpoint (fake-gcs-server).
            from google.auth.credentials import AnonymousCredentials
            return storage.Client(
                project=project or "emulator",
                credentials=AnonymousCredentials(),
                client_options=client_options,
            )

        # No SA, no endpoint → Application Default Credentials.
        return storage.Client(project=project) if project else storage.Client()

    # ---- provider primitives required by ObjectStorageSourceMixin -----------
    def list_objects(
        self,
        *,
        config: Dict,
        bucket: str,
        prefix: str = "",
        max_keys: int = 1000,
    ) -> List[Dict[str, Any]]:
        """List blobs under bucket/prefix → [{key,size,last_modified}]."""
        client = self._get_gcs_client(config)
        out: List[Dict[str, Any]] = []
        for blob in client.list_blobs(bucket, prefix=prefix or None, max_results=max_keys):
            lm = getattr(blob, "updated", None) or getattr(blob, "time_created", None)
            out.append({
                "key": blob.name,
                "size": blob.size,
                "last_modified": lm.isoformat() if hasattr(lm, "isoformat") else (str(lm) if lm else ""),
            })
            if len(out) >= max_keys:
                break
        return out

    def download_file(self, *, config: Dict, bucket: str, key: str) -> bytes:
        """Download one blob's bytes."""
        client = self._get_gcs_client(config)
        blob = client.bucket(bucket).blob(key)
        return blob.download_as_bytes()

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
                {"name": "test_connection", "method": "gcs_test_connection", "type": "core",
                 "description": "Test connectivity to the data source"},
                {"name": "validate_config", "method": "gcs_validate_config", "type": "core",
                 "description": "Validate configuration without connecting"},
                {"name": "discover_schema", "method": "gcs_discover_schema", "type": "core",
                 "description": "Discover logical tables and their schemas by sampling objects"},
                {"name": "get_capabilities", "method": "gcs_get_capabilities", "type": "core",
                 "description": "Return connector capabilities for agent interaction"},
                {"name": "read", "method": "gcs_read", "type": "source",
                 "description": "Read a file (or prefix) from storage"},
                {"name": "import_data", "method": "gcs_import_data", "type": "destination",
                 "description": "Write data to storage"},
                {"name": "delete_prefix", "method": "gcs_delete_prefix", "type": "destination",
                 "description": "Delete all objects under a bucket prefix (guardrailed)"},
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
        if not config.get('bucket') and not config.get('bucket_name'):
            errors.append("Missing required field: bucket")
        has_sa = bool(config.get('service_account_json') or config.get('service_account'))
        has_endpoint = bool(config.get('endpoint_url') or config.get('api_endpoint') or config.get('endpoint'))
        if not has_sa and not has_endpoint:
            warnings.append("No service_account_json provided; falling back to Application Default Credentials")
        return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    def test_connection(self, params: Dict = None) -> Dict[str, Any]:
        config = self._get_config(params)
        try:
            client = self._get_gcs_client(config)
            bucket = config.get('bucket') or config.get('bucket_name')
            if bucket:
                # objects.list is the minimal permission a SOURCE needs; an empty
                # bucket returns no blobs without raising → still "connected".
                next(iter(client.list_blobs(bucket, max_results=1)), None)
                return {"success": True, "message": f"Connection successful (bucket: {bucket})"}
            next(iter(client.list_buckets(max_results=1)), None)
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
                body, bucket, key, content_type = self._prepare_blob_write(params)
                config = self._get_config({"config": params.get("config") or {}})
                client = self._get_gcs_client(config)
                client.bucket(bucket).blob(key).upload_from_string(body, content_type=content_type)
                return {
                    "success": True, "bytes_written": len(body),
                    "metadata": {"bucket": bucket, "key": key,
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
            return {"success": False, "error": "Bucket and key are required"}
        if payload is None:
            return {"success": False, "error": "Missing 'data' payload"}

        try:
            client = self._get_gcs_client(config)
            blob = client.bucket(bucket).blob(key)

            raw = bool(params.get("raw") or params.get("raw_data") or params.get("raw_bytes"))
            if raw:
                if isinstance(payload, (bytes, bytearray)):
                    body = bytes(payload)
                else:
                    body = str(payload).encode("utf-8")
            else:
                body = self.convert_data_to_format(payload, fmt, compression)

            blob.upload_from_string(body, content_type=content_type)
            return {
                "success": True,
                "rows_inserted": len(payload) if isinstance(payload, list) else 1,
                "bytes_written": len(body),
                "metadata": {"bucket": bucket, "key": key, "format": fmt, "compression": compression},
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete_prefix(self, params: Dict) -> Dict[str, Any]:
        """Delete all blobs under a prefix. Guardrails: prefix required; refuse
        deletes outside a configured path_prefix/prefix."""
        params = params or {}
        config = self._get_config(params)
        bucket = params.get("bucket") or config.get("bucket")
        prefix = params.get("prefix") or params.get("key_prefix") or params.get("path") or ""
        if not bucket or str(bucket).strip() == "":
            return {"success": False, "error": "Bucket is required"}
        if not prefix or str(prefix).strip() == "":
            return {"success": False, "error": "prefix is required (refusing to delete entire bucket)"}

        base_prefix = (config.get("path_prefix") or config.get("prefix") or "").strip().strip("/")
        if base_prefix:
            target = str(prefix).strip().lstrip("/")
            if not target.startswith(base_prefix):
                return {"success": False,
                        "error": f"Refusing to delete outside configured path_prefix/prefix '{base_prefix}' (requested '{prefix}')"}

        max_objects = int(params.get("max_objects") or 100000)
        deleted = 0
        truncated = False
        try:
            client = self._get_gcs_client(config)
            for blob in client.list_blobs(bucket, prefix=str(prefix)):
                blob.delete()
                deleted += 1
                if deleted >= max_objects:
                    truncated = True
                    break
            return {"success": True, "bucket": bucket, "prefix": prefix,
                    "deleted": deleted, "truncated": truncated}
        except Exception as e:
            return {"success": False, "error": str(e)}


# =============================================================================
# HTTP SERVER (Docker) — mirrors the aws-s3 entrypoint
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

    app = FastAPI(title="GCS MCP Connector")
    server = GcsMCPServer()

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
            prefixed = f"gcs_{method_name}"
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
        logger.info(f"🚀 Starting GCS MCP Server in HTTP mode on port {port}")
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        server = GcsMCPServer()
        server.run()
