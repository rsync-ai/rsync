#!/usr/bin/env python3
"""
MinIO MCP Connector (Internal) - Versioned runtime

Implements claim-check staging for large batch payloads:
  - upload_for_staging: writes JSON to MinIO (S3 API) and returns s3://bucket/key
  - read_from_staging: reads JSON back (debug/smoke tests)
  - delete_from_staging: deletes staged object
"""

import gzip
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Tuple

logger = logging.getLogger(__name__)


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def _sse_enabled() -> bool:
    # Default-ON server-side encryption at rest for claim-check objects.
    # Opt out with RSYNC_MINIO_SSE=false (e.g. a backend that rejects SSE).
    return _env("RSYNC_MINIO_SSE", "true").strip().lower() != "false"


def _gzip_enabled() -> bool:
    # Default-off, like the other data-plane flags (RSYNC_PG_BULK_COPY etc.).
    return _env("RSYNC_CLAIM_CHECK_GZIP", "").strip().lower() == "true"


def _maybe_gzip(body: bytes) -> Tuple[bytes, bool]:
    """Gzip the claim-check body when RSYNC_CLAIM_CHECK_GZIP=true; else passthrough.

    Snappy already covers the Kafka *message*, but the claim-check object in MinIO
    is uncompressed JSON — this is the one structured payload on the data plane
    that ships raw. ~10x smaller for typical batches (see e2e/test_claim_check_gzip).
    """
    if _gzip_enabled():
        return gzip.compress(body), True
    return body, False


def _gunzip_if_needed(raw: bytes) -> bytes:
    """Transparently decompress gzip-magic (0x1f 0x8b) bodies.

    Flag-INDEPENDENT autodetect so the reader handles BOTH legacy plain-JSON and
    new gzip objects — the writer flag can flip with nothing in flight stranded.
    """
    if len(raw) >= 2 and raw[0] == 0x1F and raw[1] == 0x8B:
        return gzip.decompress(raw)
    return raw


def _parse_s3_url(url: str) -> Tuple[str, str]:
    u = (url or "").strip()
    if not u.startswith("s3://"):
        raise ValueError("claim_check_url must start with s3://")
    rest = u[len("s3://") :]
    if "/" not in rest:
        raise ValueError("claim_check_url must be s3://<bucket>/<key>")
    bucket, key = rest.split("/", 1)
    bucket = bucket.strip()
    key = key.strip()
    if not bucket or not key:
        raise ValueError("claim_check_url must be s3://<bucket>/<key>")
    return bucket, key


class MinioMCPServer:
    def __init__(self):
        self.connector_type = "minio"

    def _resolve_config(self, cfg: Dict[str, Any]) -> Dict[str, str]:
        cfg = cfg or {}

        def pick(key: str, env_name: str, default: str = "") -> str:
            v = cfg.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
            return _env(env_name, default).strip()

        endpoint_url = pick("endpoint_url", "MINIO_ENDPOINT_URL", "")
        access_key = pick("access_key_id", "MINIO_ACCESS_KEY_ID", "")
        secret_key = pick("secret_access_key", "MINIO_SECRET_ACCESS_KEY", "")
        region = pick("region", "MINIO_REGION", "us-east-1") or "us-east-1"
        bucket = pick("bucket", "MINIO_BUCKET", "pipeline-data") or "pipeline-data"
        prefix = pick("prefix", "MINIO_PREFIX", "staging") or "staging"

        if endpoint_url and not (endpoint_url.startswith("http://") or endpoint_url.startswith("https://")):
            endpoint_url = "http://" + endpoint_url

        return {
            "endpoint_url": endpoint_url,
            "access_key_id": access_key,
            "secret_access_key": secret_key,
            "region": region,
            "bucket": bucket,
            "prefix": prefix,
        }

    def _client(self, cfg: Dict[str, str]):
        import boto3
        from botocore.config import Config

        return boto3.client(
            "s3",
            endpoint_url=cfg.get("endpoint_url") or None,
            aws_access_key_id=cfg.get("access_key_id") or None,
            aws_secret_access_key=cfg.get("secret_access_key") or None,
            region_name=cfg.get("region") or "us-east-1",
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    def test_connection(self, params: Dict = None) -> Dict[str, Any]:
        params = params or {}
        cfg = self._resolve_config(params.get("config", {}) or {})
        try:
            c = self._client(cfg)
            c.head_bucket(Bucket=cfg["bucket"])
            return {"success": True, "bucket": cfg["bucket"], "endpoint_url": cfg["endpoint_url"]}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def upload_for_staging(self, params: Dict = None) -> Dict[str, Any]:
        params = params or {}
        cfg = self._resolve_config(params.get("config", {}) or {})
        data = params.get("data")
        if data is None:
            return {"success": False, "error": "Missing required param: data"}

        key = params.get("key")
        if not isinstance(key, str) or not key.strip():
            pipeline_id = ""
            table = ""
            if isinstance(data, dict):
                pipeline_id = str(data.get("pipeline_id") or "")
                table = str(data.get("table") or "")
            pipeline_id = pipeline_id.strip() or "pipeline"
            table = table.strip() or "table"
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            key = f"{cfg['prefix'].strip().strip('/')}/{pipeline_id}/{table}/{ts}-{uuid.uuid4().hex}.json"

        try:
            body = json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            body, gzipped = _maybe_gzip(body)
            extra = {"ContentEncoding": "gzip"} if gzipped else {}
            c = self._client(cfg)
            # Server-side encryption at rest (SSE-S3 / AES256). boto3 S3 API
            # takes ServerSideEncryption on put_object (NOT the minio SDK's
            # SseS3 object). Default-on; degrade gracefully if the backend
            # rejects SSE so claim-check writes never fail-open on a bucket
            # that isn't SSE-capable.
            if _sse_enabled():
                try:
                    c.put_object(
                        Bucket=cfg["bucket"], Key=key, Body=body,
                        ContentType="application/json",
                        ServerSideEncryption="AES256", **extra,
                    )
                except Exception as sse_err:
                    logger.warning(
                        "MinIO SSE (AES256) rejected for %s/%s (%s); retrying without SSE",
                        cfg["bucket"], key, sse_err,
                    )
                    c.put_object(Bucket=cfg["bucket"], Key=key, Body=body, ContentType="application/json", **extra)
            else:
                c.put_object(Bucket=cfg["bucket"], Key=key, Body=body, ContentType="application/json", **extra)
            claim = f"s3://{cfg['bucket']}/{key}"
            return {"success": True, "claim_check_url": claim, "bucket": cfg["bucket"], "key": key, "bytes": len(body), "gzip": gzipped}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def read_from_staging(self, params: Dict = None) -> Dict[str, Any]:
        params = params or {}
        cfg = self._resolve_config(params.get("config", {}) or {})
        url = params.get("claim_check_url")
        if not isinstance(url, str) or not url.strip():
            return {"success": False, "error": "Missing required param: claim_check_url"}

        try:
            bucket, key = _parse_s3_url(url)
            c = self._client(cfg)
            obj = c.get_object(Bucket=bucket, Key=key)
            raw = obj["Body"].read()
            stored_bytes = len(raw)  # actual object size moved (compressed if gzip)
            raw = _gunzip_if_needed(raw)
            data = json.loads(raw.decode("utf-8"))
            return {"success": True, "data": data, "bucket": bucket, "key": key, "bytes": stored_bytes}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete_from_staging(self, params: Dict = None) -> Dict[str, Any]:
        params = params or {}
        cfg = self._resolve_config(params.get("config", {}) or {})
        url = params.get("claim_check_url")
        if not isinstance(url, str) or not url.strip():
            return {"success": False, "error": "Missing required param: claim_check_url"}

        try:
            bucket, key = _parse_s3_url(url)
            c = self._client(cfg)
            c.delete_object(Bucket=bucket, Key=key)
            return {"success": True, "bucket": bucket, "key": key}
        except Exception as e:
            return {"success": False, "error": str(e)}


def _jsonrpc_error(req_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def _jsonrpc_result(req_id: Any, result: Dict[str, Any]) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def serve_http() -> None:
    server = MinioMCPServer()
    port = int(_env("MCP_PORT", _env("PORT", "8000")) or "8000")

    class Handler(BaseHTTPRequestHandler):
        def _send_json(self, status: int, obj: Dict[str, Any]) -> None:
            body = json.dumps(obj).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            if self.path == "/health":
                self._send_json(200, {"status": "ok"})
            else:
                self._send_json(404, {"error": "not found"})

        def do_POST(self):  # noqa: N802
            if self.path != "/mcp":
                self._send_json(404, {"error": "not found"})
                return

            try:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length > 0 else b"{}"
                req = json.loads(raw.decode("utf-8") or "{}")
            except Exception:
                self._send_json(200, _jsonrpc_error(None, -32700, "parse error"))
                return

            req_id = req.get("id")
            if req.get("jsonrpc") != "2.0":
                self._send_json(200, _jsonrpc_error(req_id, -32600, "invalid request"))
                return
            if req.get("method") != "tools/call":
                self._send_json(200, _jsonrpc_error(req_id, -32601, "method not found"))
                return

            params = req.get("params") or {}
            tool = params.get("name") or ""
            args = params.get("arguments") or {}
            if not isinstance(tool, str) or not tool:
                self._send_json(200, _jsonrpc_error(req_id, -32602, "missing tool name"))
                return
            if not isinstance(args, dict):
                self._send_json(200, _jsonrpc_error(req_id, -32602, "arguments must be an object"))
                return

            if not tool.startswith("minio_"):
                self._send_json(200, _jsonrpc_error(req_id, -32601, "tool not found"))
                return

            op = tool[len("minio_") :].strip()
            try:
                if op == "upload_for_staging":
                    res = server.upload_for_staging(args)
                elif op == "read_from_staging":
                    res = server.read_from_staging(args)
                elif op == "delete_from_staging":
                    res = server.delete_from_staging(args)
                elif op == "test_connection":
                    res = server.test_connection(args)
                elif op == "health":
                    res = {"success": True, "status": "ok"}
                else:
                    self._send_json(200, _jsonrpc_error(req_id, -32601, "tool not found"))
                    return
            except Exception as e:
                res = {"success": False, "error": str(e)}

            self._send_json(200, _jsonrpc_result(req_id, res))

        def log_message(self, format: str, *args):  # noqa: A002
            return

    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    httpd.serve_forever()


if __name__ == "__main__":
    serve_http()
