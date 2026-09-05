from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ManifestObject:
    key: str
    size_bytes: Optional[int] = None
    etag: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"key": self.key, "size_bytes": self.size_bytes, "etag": self.etag}


def pending_prefix(base_prefix: str, stream_id: str, batch_id: str) -> str:
    base = (base_prefix or "").rstrip("/")
    return f"{base}/rsync/{stream_id}/pending/batch_id={batch_id}".lstrip("/")


def manifest_key(base_prefix: str, stream_id: str, batch_id: str) -> str:
    base = (base_prefix or "").rstrip("/")
    return f"{base}/rsync/{stream_id}/batch_id={batch_id}/manifest.json".lstrip("/")


def manifest_success_key(base_prefix: str, stream_id: str, batch_id: str) -> str:
    base = (base_prefix or "").rstrip("/")
    return f"{base}/rsync/{stream_id}/batch_id={batch_id}/_SUCCESS".lstrip("/")


def _schema_hash(schema: Any) -> str:
    try:
        b = json.dumps(schema, sort_keys=True, separators=(",", ":")).encode("utf-8")
    except Exception:
        b = str(schema).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def build_batch_manifest(
    *,
    stream_id: str,
    batch_id: str,
    file_format: str,
    compression: str,
    schema: Optional[Any],
    objects: List[ManifestObject],
    stats: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Manifest used for idempotency + auditing. Presence of a manifest implies the batch is committed.
    """
    now = datetime.now(timezone.utc).isoformat()
    manifest = {
        "schema_version": "1.0",
        "stream_id": stream_id,
        "batch_id": batch_id,
        "created_at": now,
        "file_format": file_format,
        "compression": compression,
        "schema_hash": _schema_hash(schema) if schema is not None else None,
        "schema": schema,
        "objects": [o.to_dict() for o in (objects or [])],
        "stats": stats or {},
        "extra": extra or {},
    }
    return manifest

