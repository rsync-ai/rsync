"""
Shared helpers for repo integration tests.

Intent:
- Default to **real AWS S3** when credentials + bucket are provided (internal env).
- Fall back to **MinIO (S3-compatible)** for local dev (no AWS creds needed).

These helpers return connection payloads compatible with the existing `aws-s3` MCP connector.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional


def _truthy(v: Optional[str]) -> bool:
    if v is None:
        return False
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def use_real_aws_s3() -> bool:
    """
    Decide whether tests should target real AWS S3.
    """
    if _truthy(os.getenv("RSYNC_TEST_USE_REAL_AWS_S3")):
        return True

    # Autodetect based on env presence.
    bucket = os.getenv("RSYNC_TEST_S3_BUCKET") or os.getenv("AWS_S3_BUCKET") or os.getenv("S3_BUCKET")
    access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_S3_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_S3_SECRET_ACCESS_KEY")
    return bool(bucket and access_key and secret_key)


def s3_bucket() -> str:
    b = os.getenv("RSYNC_TEST_S3_BUCKET") or os.getenv("AWS_S3_BUCKET") or os.getenv("S3_BUCKET")
    if b:
        return b
    # Local fallback (MinIO init creates this by default in docker-compose.e2e.yml)
    return os.getenv("MINIO_BUCKET", "pipeline-data")


def s3_region() -> str:
    return (
        os.getenv("RSYNC_TEST_S3_REGION")
        or os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or "us-east-1"
    )


def s3_destination_config() -> Dict[str, Any]:
    """
    Config for the `aws-s3` connector.
    - For AWS: requires access_key_id/secret_access_key/region/bucket.
    - For MinIO fallback: same + endpoint_url (docker network).
    """
    if use_real_aws_s3():
        return {
            "access_key_id": os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_S3_ACCESS_KEY_ID") or "",
            "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_S3_SECRET_ACCESS_KEY") or "",
            "region": s3_region(),
            "bucket": s3_bucket(),
        }

    # Local MinIO (S3-compatible) fallback
    return {
        "endpoint_url": os.getenv("RSYNC_TEST_MINIO_ENDPOINT_URL") or "http://minio:9000",
        "access_key_id": os.getenv("MINIO_ACCESS_KEY_ID", "minioadmin"),
        "secret_access_key": os.getenv("MINIO_SECRET_ACCESS_KEY", "minioadmin"),
        "region": s3_region(),
        "bucket": s3_bucket(),
    }


def destination_connection_payload(name: str, ts: int) -> Dict[str, Any]:
    """
    Payload for POST /api/v1/connections (destination).
    """
    return {
        "name": f"{name}_{ts}",
        "connector_type": "aws-s3",
        "connection_type": "destination",
        "config": s3_destination_config(),
    }

