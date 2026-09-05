#!/usr/bin/env python3
"""
E2E Test: Full Pipeline Execution with Real Connections
Tests: MySQL → S3 (CSV) and MySQL → Postgres
"""

import requests
import json
import time
import sys
import gzip
import boto3
from datetime import datetime, timezone
from typing import Dict, Optional

# Colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"

# Config
# NOTE: In docker-compose, API Gateway is exposed on host port 5001.
API_BASE = "http://localhost:5001/api/v1"
USER_ID = "00000000-0000-0000-0000-000000000001"  # Dev default user
HEADERS = {"X-User-ID": USER_ID, "Content-Type": "application/json"}

# E2E Database configs (from docker-compose.e2e.yml)
# NOTE: Use Docker service names from orchestrator's perspective (runs in Docker network)
# Use container_name hostnames for maximum reliability on the docker network.
MYSQL_E2E_CONFIG = {
    "host": "rsync-ai-mysql-e2e",
    "port": 3306,
    "database": "e2e_db",
    "user": "e2e_user",
    "password": "e2e_password"
}

POSTGRES_E2E_CONFIG = {
    "host": "rsync-ai-postgres-e2e",
    "port": 5432,
    "database": "e2e_db",
    "user": "e2e_user",
    "password": "e2e_password"
}

MINIO_E2E_CONFIG = {
    "access_key_id": "minioadmin",
    "secret_access_key": "minioadmin",
    "region": "us-east-1",
    "bucket": "e2e-bucket",
    "endpoint_url": "http://minio:9000",  # S3-compatible endpoint (internal Docker)
    "path_prefix": "test-output",
    "file_format": "csv",
    "compression": "gzip"
}


def slugify_pipeline_name(name: str) -> str:
    """Convert a pipeline name to a URL/filesystem-safe slug (matches Go implementation)"""
    name = name.strip().lower()
    name = name.replace(" ", "-").replace("_", "-")
    # Keep only alphanumeric and hyphens
    result = []
    last_was_hyphen = False
    for char in name:
        if char.isalnum():
            result.append(char)
            last_was_hyphen = False
        elif char == "-" and not last_was_hyphen and result:
            result.append("-")
            last_was_hyphen = True
    slug = "".join(result).rstrip("-")
    if len(slug) > 63:
        slug = slug[:63].rstrip("-")
    return slug


def log(message: str, color: str = RESET):
    """Print colored log message"""
    print(f"{color}{message}{RESET}")


def create_connection(name: str, connector_type: str, conn_type: str, config: Dict) -> Optional[str]:
    """Create a connection and return its ID"""
    # Best-effort idempotency: if a previous run left connections behind, reuse them.
    def find_existing_id() -> Optional[str]:
        try:
            resp = requests.get(f"{API_BASE}/connections", headers=HEADERS, timeout=10)
            if resp.status_code != 200:
                return None
            payload = resp.json()
            conns = payload.get("connections", []) or []
            for c in conns:
                if isinstance(c, dict) and c.get("name") == name:
                    return c.get("id")
            return None
        except Exception:
            return None

    payload = {
        "name": name,
        "connection_type": conn_type,
        "connector_type": connector_type,
        "connector_version": "latest",
        "sync_mode": "batch",
        "description": f"E2E test connection: {name}",
        "config": config
    }
    
    response = requests.post(f"{API_BASE}/connections", headers=HEADERS, json=payload)
    if response.status_code == 409:
        existing_id = find_existing_id()
        if existing_id:
            log(f"♻️  Reusing existing connection '{name}' (ID: {existing_id})", YELLOW)
            return existing_id
    if response.status_code != 201:
        log(f"❌ Failed to create connection '{name}': {response.status_code}", RED)
        log(f"   Response: {response.text}", RED)
        return None
    
    conn_id = response.json().get("id")
    log(f"✅ Connection '{name}' created (ID: {conn_id})", GREEN)
    return conn_id


def create_pipeline(
    nl_request: str,
    source_conn_id: str,
    dest_conn_id: str,
    *,
    default_run_mode: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    """Create a pipeline and return its ID and name as a tuple (pipeline_id, pipeline_name)."""
    pipeline_name = f"E2E Test Pipeline - {int(time.time())}"
    payload = {
        "name": pipeline_name,
        "description": "Automated e2e test pipeline",
        "request": nl_request,
        "source_connection_id": source_conn_id,
        "destination_connection_id": dest_conn_id,
    }
    if default_run_mode:
        payload["default_run_mode"] = default_run_mode

    response = requests.post(f"{API_BASE}/pipelines", headers=HEADERS, json=payload)
    if response.status_code not in (200, 201):
        log(f"❌ Failed to create pipeline: {response.status_code}", RED)
        log(f"   Response: {response.text}", RED)
        return None, None
    
    pipeline_id = response.json().get("id")
    log(f"✅ Pipeline created (ID: {pipeline_id}, name: {pipeline_name})", GREEN)
    return pipeline_id, pipeline_name


def run_pipeline(pipeline_id: str) -> bool:
    """Start pipeline execution"""
    response = requests.post(f"{API_BASE}/pipelines/{pipeline_id}/run", headers=HEADERS)
    if response.status_code != 200:
        log(f"❌ Failed to start pipeline: {response.status_code}", RED)
        return False
    
    log(f"✅ Pipeline {pipeline_id} started", GREEN)
    return True


def select_tables(pipeline_id: str, tables: list) -> bool:
    """Send table selection (HITL resume)"""
    payload = {"selected_tables": tables}
    response = requests.post(f"{API_BASE}/pipelines/{pipeline_id}/hitl/tables", headers=HEADERS, json=payload)
    if response.status_code != 200:
        log(f"❌ Failed to select tables: {response.status_code}", RED)
        return False
    
    log(f"✅ Tables selected: {tables}", GREEN)
    return True


def wait_for_pipeline(pipeline_id: str, timeout: int = 600) -> Dict:
    """Poll pipeline state until completion or timeout"""
    start = time.time()
    last_stage = None
    
    while time.time() - start < timeout:
        response = requests.get(f"{API_BASE}/pipelines/{pipeline_id}/state", headers=HEADERS)
        if response.status_code != 200:
            log(f"⚠️  Failed to fetch pipeline state: {response.status_code}", YELLOW)
            time.sleep(5)
            continue
        
        state = response.json()
        status = state.get("status", "unknown")
        current_stage = state.get("current_stage", "unknown")
        progress = state.get("progress", {}).get("percent", 0)
        
        if current_stage != last_stage:
            log(f"⏳ Stage: {current_stage} | Status: {status} | Progress: {progress}%", BLUE)
            last_stage = current_stage
        
        if status == "completed":
            log(f"✅ Pipeline completed successfully!", GREEN)
            return state
        elif status == "failed":
            error_msg = state.get("error_message", "Unknown error")
            log(f"❌ Pipeline failed: {error_msg}", RED)
            return state
        elif status in ("hitl_required", "waiting_for_user"):
            # V2 state uses "waiting_for_user" + blocking_reason for HITL.
            br = state.get("blocking_reason") or {}
            br_type = br.get("type") or br.get("details", {}).get("action_needed")
            details = br.get("details") or {}
            available = details.get("available_tables") or details.get("tables") or []
            if br_type == "table_selection":
                selected: list[str] = []

                # Preferred: choose from server-provided list when available
                if available:
                    table_names = [
                        t.get("name") for t in available
                        if isinstance(t, dict) and t.get("name")
                    ]
                    selected = ["big_table"] if "big_table" in table_names else table_names[:1]
                else:
                    # Fallback: manual entry mode (some environments don't populate available_tables)
                    if details.get("allow_manual_entry"):
                        db_name = MYSQL_E2E_CONFIG.get("database") or "e2e_db"
                        selected = [f"{db_name}.big_table"]

                if selected:
                    log(f"📋 HITL: Selecting tables: {selected}", BLUE)
                    if not select_tables(pipeline_id, selected):
                        return state
        
        time.sleep(5)
    
    log(f"❌ Pipeline timed out after {timeout}s", RED)
    return {"status": "timeout"}


def _minio_client_host() -> "boto3.client":
    """Create a MinIO S3 client from the host machine."""
    return boto3.client(
        "s3",
        aws_access_key_id=MINIO_E2E_CONFIG["access_key_id"],
        aws_secret_access_key=MINIO_E2E_CONFIG["secret_access_key"],
        endpoint_url="http://localhost:9000",  # External MinIO endpoint from host
        region_name="us-east-1",
    )


def ensure_bucket_exists(bucket: str) -> bool:
    """Ensure bucket exists in MinIO."""
    try:
        s3 = _minio_client_host()
        try:
            s3.head_bucket(Bucket=bucket)
            return True
        except Exception:
            # MinIO typically doesn't require LocationConstraint
            s3.create_bucket(Bucket=bucket)
            log(f"✅ Created MinIO bucket: {bucket}", GREEN)
            return True
    except Exception as e:
        log(f"❌ Failed to ensure bucket exists '{bucket}': {e}", RED)
        return False


def clear_s3_prefix(bucket: str, prefix: str) -> bool:
    """Delete all objects under a prefix (best-effort) to make tests idempotent."""
    try:
        s3 = _minio_client_host()
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        objs = resp.get("Contents", []) or []
        if not objs:
            return True
        to_delete = [{"Key": o["Key"]} for o in objs if isinstance(o, dict) and o.get("Key")]
        # Batch delete (MinIO supports it)
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
        log(f"🧹 Cleared {len(to_delete)} object(s) under s3://{bucket}/{prefix}", BLUE)
        return True
    except Exception as e:
        log(f"⚠️  Failed to clear prefix s3://{bucket}/{prefix}: {e}", YELLOW)
        return False


def _count_csv_rows_bytes(content: bytes, key: str) -> int:
    """Return number of data rows in a CSV object (excluding header)."""
    if key.endswith(".gz"):
        content = gzip.decompress(content)
    text = content.decode("utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if not lines:
        return 0
    # First line is header (best-effort)
    return max(0, len(lines) - 1)


def _list_s3_keys(bucket: str, prefix: str) -> list[str]:
    s3 = _minio_client_host()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objs = resp.get("Contents", []) or []
    return [
        o["Key"]
        for o in objs
        if isinstance(o, dict) and isinstance(o.get("Key"), str)
    ]


def _put_s3_object(bucket: str, key: str, body: bytes) -> bool:
    try:
        s3 = _minio_client_host()
        s3.put_object(Bucket=bucket, Key=key, Body=body)
        return True
    except Exception as e:
        log(f"⚠️  Failed to put object s3://{bucket}/{key}: {e}", YELLOW)
        return False


def verify_s3_prefix(bucket: str, prefix: str, expected_rows: int, *, require_markers: bool = True) -> bool:
    """Verify we wrote objects under prefix, markers exist, and total CSV row count matches expected."""
    try:
        s3 = _minio_client_host()
        # Poll a bit: destination writes and marker writes are async across services.
        deadline = time.time() + 90
        objs = []
        all_keys = []
        has_manifest = False
        has_success = False
        while time.time() < deadline:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            objs = resp.get("Contents", []) or []
            if not objs:
                time.sleep(5)
                continue

            all_keys = [
                o["Key"]
                for o in objs
                if isinstance(o, dict) and isinstance(o.get("Key"), str)
            ]

            has_manifest = any(k.endswith("/_MANIFEST.json") for k in all_keys)
            has_success = any(k.endswith("/_SUCCESS") for k in all_keys)
            if require_markers and (not has_manifest or not has_success):
                time.sleep(5)
                continue
            break

        if not objs:
            log(f"❌ No objects found under s3://{bucket}/{prefix}", RED)
            return False
        if require_markers and (not has_manifest or not has_success):
            log(
                f"❌ Missing markers under s3://{bucket}/{prefix} "
                f"(manifest={has_manifest}, success={has_success})",
                RED,
            )
            return False

        # Only consider CSV outputs
        keys = [
            o["Key"] for o in objs
            if isinstance(o, dict) and isinstance(o.get("Key"), str)
            and (o["Key"].endswith(".csv") or o["Key"].endswith(".csv.gz"))
        ]
        if not keys:
            log(f"❌ No CSV objects found under s3://{bucket}/{prefix}", RED)
            return False

        # Best-effort: validate manifest references the uploaded keys (if present)
        manifest_keys = [k for k in all_keys if k.endswith("/_MANIFEST.json")]
        if manifest_keys:
            mk = sorted(manifest_keys)[-1]
            try:
                mobj = s3.get_object(Bucket=bucket, Key=mk)
                mbody = mobj["Body"].read()
                manifest = json.loads(mbody.decode("utf-8", errors="replace"))
                uploaded = manifest.get("uploaded_keys") or []
                if isinstance(uploaded, list) and uploaded:
                    missing = [k for k in keys if k not in uploaded]
                    if missing:
                        log(f"⚠️  Manifest does not include {len(missing)} CSV key(s)", YELLOW)
            except Exception as e:
                log(f"⚠️  Failed to validate manifest content: {e}", YELLOW)

        total = 0
        for key in sorted(keys):
            obj = s3.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read()
            rows = _count_csv_rows_bytes(body, key)
            total += rows
            log(f"   - s3://{bucket}/{key}: {rows} rows", RESET)

        if total == expected_rows:
            log(f"✅ S3 row count verified across prefix: {total}", GREEN)
            return True

        log(f"❌ S3 row count mismatch across prefix: expected {expected_rows}, got {total}", RED)
        return False
    except Exception as e:
        log(f"❌ S3 verification failed: {e}", RED)
        return False


def verify_postgres_table(table: str, expected_rows: int) -> bool:
    """Verify Postgres table has expected row count"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost",  # From host perspective
            port=5433,  # External port
            database="e2e_db",
            user="e2e_user",
            password="e2e_password"
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        if count == expected_rows:
            log(f"✅ Postgres row count verified: {count}", GREEN)
            return True
        else:
            log(f"❌ Postgres row count mismatch: expected {expected_rows}, got {count}", RED)
            return False
    except Exception as e:
        log(f"❌ Postgres verification failed: {e}", RED)
        return False


def ensure_postgres_table_exists(table: str) -> bool:
    """Create destination table if it doesn't exist (so DB-to-DB import doesn't fail)."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="e2e_db",
            user="e2e_user",
            password="e2e_password"
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
              id INTEGER,
              payload TEXT NOT NULL,
              created_at TIMESTAMP NULL
            );
            """
        )
        cur.close()
        conn.close()
        return True
    except Exception as e:
        log(f"❌ Failed to ensure Postgres table exists '{table}': {e}", RED)
        return False


def truncate_postgres_table(table: str) -> bool:
    """TRUNCATE destination table to make test runs idempotent."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="e2e_db",
            user="e2e_user",
            password="e2e_password"
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {table};")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        log(f"⚠️  Failed to truncate Postgres table '{table}': {e}", YELLOW)
        return False


def cleanup_connection(conn_id: str):
    """Delete a connection"""
    if not conn_id:
        return
    try:
        resp = requests.delete(f"{API_BASE}/connections/{conn_id}", headers=HEADERS, timeout=10)
        if resp.status_code not in (200, 204):
            # Not fatal; we often keep e2e connections around between runs.
            log(f"⚠️  Failed to delete connection {conn_id}: {resp.status_code} {resp.text}", YELLOW)
    except Exception as e:
        log(f"⚠️  Failed to delete connection {conn_id}: {e}", YELLOW)


def test_mysql_to_s3():
    """Test MySQL → S3 pipeline"""
    log("\n" + "="*80, BLUE)
    log("🧪 TEST 1: MySQL → S3 (CSV + Gzip)", BLUE)
    log("="*80, BLUE)
    
    # Ensure MinIO bucket exists before running pipeline
    if not ensure_bucket_exists(MINIO_E2E_CONFIG["bucket"]):
        return False

    # Make this test idempotent by clearing previous outputs for this table under the configured prefix.
    base_prefix = (MINIO_E2E_CONFIG.get("path_prefix") or "").strip().rstrip("/") + "/"
    clear_s3_prefix(MINIO_E2E_CONFIG["bucket"], base_prefix)

    # Create connections
    mysql_conn_id = create_connection("E2E MySQL Source", "mysql", "source", MYSQL_E2E_CONFIG)
    s3_conn_id = create_connection("E2E S3 Destination", "aws-s3", "destination", MINIO_E2E_CONFIG)
    
    if not mysql_conn_id or not s3_conn_id:
        return False
    
    try:
        # Create and run pipeline
        pipeline_id, pipeline_name = create_pipeline(
            "Move big_table from MySQL to S3 as CSV",
            mysql_conn_id,
            s3_conn_id,
            default_run_mode="reload",
        )
        if not pipeline_id:
            return False
        
        if not run_pipeline(pipeline_id):
            return False
        
        # Wait for completion
        state = wait_for_pipeline(pipeline_id, timeout=600)
        if state.get("status") != "completed":
            return False
        
        # NEW LAYOUT: Verify outputs under {path_prefix}/{dataset}/{db}/{table}/dt=YYYY-MM-DD/
        # The dataset is derived from the pipeline name (slugified)
        dataset = slugify_pipeline_name(pipeline_name)
        # Source is MySQL database "e2e_db", table "big_table"
        db_name = MYSQL_E2E_CONFIG["database"]
        table_name = "big_table"
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Require the new layout (human-readable + stable) with markers.
        new_prefix = f"{base_prefix}{dataset}/{db_name}/{table_name}/dt={today}/"
        
        log(f"📂 Checking new layout prefix: {new_prefix}", BLUE)
        if verify_s3_prefix(MINIO_E2E_CONFIG["bucket"], new_prefix, 100000, require_markers=True):
            log(f"✅ Data found in new layout: {new_prefix}", GREEN)
            # Reload-mode validation: create a junk object under the table prefix and ensure the next run deletes it.
            junk_key = f"{base_prefix}{dataset}/{db_name}/{table_name}/junk.txt"
            if _put_s3_object(MINIO_E2E_CONFIG["bucket"], junk_key, b"junk"):
                log(f"🧪 Created junk object for reload test: s3://{MINIO_E2E_CONFIG['bucket']}/{junk_key}", BLUE)

            # Run the same pipeline again (should invoke reload cleanup)
            if not run_pipeline(pipeline_id):
                return False
            state2 = wait_for_pipeline(pipeline_id, timeout=600)
            if state2.get("status") != "completed":
                return False

            # Verify junk object is gone under table prefix
            table_prefix = f"{base_prefix}{dataset}/{db_name}/{table_name}/"
            # Poll a bit: deletions and follow-on writes are async across services.
            deadline = time.time() + 60
            keys_after: list[str] = []
            while time.time() < deadline:
                keys_after = _list_s3_keys(MINIO_E2E_CONFIG["bucket"], table_prefix)
                if junk_key not in keys_after:
                    break
                time.sleep(5)
            if junk_key in keys_after:
                log(f"❌ Reload did not remove junk object: s3://{MINIO_E2E_CONFIG['bucket']}/{junk_key}", RED)
                return False
            log("✅ Reload removed junk object under table prefix", GREEN)
            return True

        log(f"❌ New layout not found or missing markers under: {new_prefix}", RED)
        return False
    
    finally:
        # Keep connections for faster repeat runs.
        pass


def test_mysql_to_postgres():
    """Test MySQL → Postgres pipeline"""
    log("\n" + "="*80, BLUE)
    log("🧪 TEST 2: MySQL → Postgres (Direct DB-to-DB)", BLUE)
    log("="*80, BLUE)
    
    # Ensure destination table exists (pipeline typically imports into the same table name)
    if not ensure_postgres_table_exists("big_table"):
        return False
    truncate_postgres_table("big_table")

    # Create connections
    mysql_conn_id = create_connection("E2E MySQL Source 2", "mysql", "source", MYSQL_E2E_CONFIG)
    pg_conn_id = create_connection("E2E Postgres Dest", "postgresql", "destination", POSTGRES_E2E_CONFIG)
    
    if not mysql_conn_id or not pg_conn_id:
        return False
    
    try:
        # Create and run pipeline
        pipeline_id, _ = create_pipeline(
            "Copy big_table from MySQL to Postgres",
            mysql_conn_id,
            pg_conn_id
        )
        if not pipeline_id:
            return False
        
        if not run_pipeline(pipeline_id):
            return False
        
        # Wait for completion
        state = wait_for_pipeline(pipeline_id, timeout=600)
        if state.get("status") != "completed":
            return False
        
        return verify_postgres_table("big_table", 100000)
    
    finally:
        # Keep connections for faster repeat runs.
        pass


def main():
    log("\n" + "="*80, GREEN)
    log("🚀 E2E Pipeline Execution Test Suite", GREEN)
    log("="*80, GREEN)
    
    tests = [
        ("MySQL → S3", test_mysql_to_s3),
        ("MySQL → Postgres", test_mysql_to_postgres),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, success))
        except Exception as e:
            log(f"❌ Test '{name}' crashed: {e}", RED)
            import traceback
            traceback.print_exc()
            results.append((name, False))
    
    # Summary
    log("\n" + "="*80, BLUE)
    log("📊 TEST SUMMARY", BLUE)
    log("="*80, BLUE)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for name, success in results:
        status = f"{GREEN}✅ PASS{RESET}" if success else f"{RED}❌ FAIL{RESET}"
        log(f"  {name}: {status}", RESET)
    
    log(f"\n🎯 Result: {passed}/{total} tests passed", GREEN if passed == total else RED)
    
    if passed == total:
        log("\n🎉 ALL TESTS PASSED! System is production-ready.", GREEN)
        sys.exit(0)
    else:
        log(f"\n⚠️  {total - passed} test(s) failed. Review logs above.", RED)
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\n\n⚠️  Test suite interrupted by user", YELLOW)
        sys.exit(1)

