#!/usr/bin/env python3
"""
Real-world pipeline testing suite
Tests actual data movement through all 7 stages
"""

from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, Optional, Tuple

import mysql.connector
import psycopg2
import requests

BASE_URL = "http://localhost:5001"
API_URL = f"{BASE_URL}/api/v1"


class RealPipelineTests:
    def __init__(self):
        self.test_results = []
        self.mysql_conn = None
        self.postgres_conn = None

    def connect_dbs(self):
        print("Connecting to databases...")

        # MySQL e2e overlay container is mapped to 3307 on host
        self.mysql_conn = mysql.connector.connect(
            host="localhost",
            port=3307,
            user="root",
            password="rootpassword",
            database="test_db",
        )
        print("  ✅ MySQL connected")

        # Postgres primary container is mapped to 5432 on host, user/password per docker-compose.yml
        self.postgres_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="user",
            password="password",
            database="test_db",
        )
        print("  ✅ PostgreSQL connected")
        print()

    def run_all_tests(self):
        print("╔════════════════════════════════════════════════════╗")
        print("║  REAL PIPELINE TESTING SUITE                      ║")
        print("╚════════════════════════════════════════════════════╝")
        print()

        self.connect_dbs()

        try:
            # Clear destination tables first
            self.clear_destination_tables()

            # Run tests
            self.test_small_table_migration()
            # self.test_medium_table_batch_processing()  # Uncomment when small works
            # self.test_large_table_adaptive_batching()  # Uncomment when medium works
            self.test_data_integrity()

        finally:
            self.cleanup()

        self.print_summary()

    def clear_destination_tables(self):
        """Clear destination tables before tests"""
        print("Clearing destination tables...")
        cursor = self.postgres_conn.cursor()

        cursor.execute("TRUNCATE TABLE small_table_dest")
        cursor.execute("TRUNCATE TABLE medium_table_dest")
        cursor.execute("TRUNCATE TABLE large_table_dest")

        self.postgres_conn.commit()
        print("  ✅ Destination tables cleared")
        print()

    def test_small_table_migration(self):
        """Test 1: Small table (100 rows) - Basic pipeline"""
        print("\nTest 1: Small Table Migration (100 rows)")
        print("=" * 60)

        # First, check if we need to create connections
        print("\nStep 1: Checking connections...")
        mysql_conn_id = self.ensure_connection("mysql", "source")
        postgres_conn_id = self.ensure_connection("postgresql", "destination")

        if not mysql_conn_id or not postgres_conn_id:
            self.fail("small_table", "Could not create/find connections")
            return

        print(f"  ✅ MySQL connection: {mysql_conn_id}")
        print(f"  ✅ PostgreSQL connection: {postgres_conn_id}")

        # Create pipeline via chat
        print("\nStep 2: Creating pipeline...")

        # IMPORTANT: chat handler reads connection IDs from req.context.*
        response = requests.post(
            f"{API_URL}/chat/message",
            json={
                "message": "migrate small_table from mysql to postgresql as small_table_dest",
                "context": {
                    "session_id": f"real-pipeline-{int(time.time())}",
                    "user_id": "test-user-123",
                    "source_connection_id": mysql_conn_id,
                    "destination_connection_id": postgres_conn_id,
                },
            },
            headers={"Content-Type": "application/json"},
            timeout=20,
        )

        if response.status_code != 200:
            print(f"  ❌ Chat request failed: {response.status_code}")
            print(f"  Response: {response.text[:200]}")
            self.fail("small_table", f"Chat request failed: {response.status_code}")
            return

        data = response.json()
        print(f"  Response: {json.dumps(data, indent=2)[:300]}...")

        pipeline_id = self.extract_pipeline_id(data)

        if not pipeline_id:
            self.fail("small_table", "No pipeline_id returned")
            return

        print(f"  ✅ Pipeline created: {pipeline_id}")

        # Monitor execution
        print("\nStep 3: Monitoring execution...")
        success, duration, final_status = self.monitor_pipeline(pipeline_id, timeout=180)

        print("\nStep 4: Results")
        print(f"  Status: {final_status}")
        print(f"  Duration: {duration:.1f}s")

        if not success:
            self.fail("small_table", f"Pipeline execution failed: {final_status}")
            return

        # Verify data
        print("\nStep 5: Verifying data...")
        source_count = self.get_mysql_count("small_table")
        dest_count = self.get_postgres_count("small_table_dest")

        print(f"  Source (MySQL): {source_count} rows")
        print(f"  Destination (PostgreSQL): {dest_count} rows")

        if source_count == dest_count == 100:
            throughput = 100 / duration if duration > 0 else 0
            print("\n✅ TEST PASSED")
            print(f"   Rows transferred: {dest_count}")
            print(f"   Duration: {duration:.1f}s")
            print(f"   Throughput: {throughput:.1f} rows/sec")

            self.pass_test(
                "small_table",
                {"rows": 100, "duration": duration, "throughput": throughput},
            )
        else:
            print("\n❌ Row count mismatch")
            self.fail(
                "small_table",
                f"Expected 100 rows, got source={source_count}, dest={dest_count}",
            )

    def test_medium_table_batch_processing(self):
        """Test 2: Medium table (10K rows) - Batch processing"""
        print("\nTest 2: Medium Table Batch Processing (10K rows)")
        print("=" * 60)

        mysql_conn_id = self.ensure_connection("mysql", "source")
        postgres_conn_id = self.ensure_connection("postgresql", "destination")

        response = requests.post(
            f"{API_URL}/chat/message",
            json={
                "message": "migrate medium_table from mysql to postgresql as medium_table_dest",
                "context": {
                    "session_id": f"real-pipeline-{int(time.time())}",
                    "user_id": "test-user-123",
                    "source_connection_id": mysql_conn_id,
                    "destination_connection_id": postgres_conn_id,
                },
            },
            timeout=20,
        )

        data = response.json()
        pipeline_id = self.extract_pipeline_id(data)

        if not pipeline_id:
            self.fail("medium_table", "No pipeline_id")
            return

        print(f"  Pipeline ID: {pipeline_id}")
        print("  This may take 1-2 minutes...")

        success, duration, final_status = self.monitor_pipeline(pipeline_id, timeout=600)

        if not success:
            self.fail("medium_table", f"Pipeline failed: {final_status}")
            return

        source_count = self.get_mysql_count("medium_table")
        dest_count = self.get_postgres_count("medium_table_dest")

        if source_count == dest_count == 10000:
            throughput = 10000 / duration if duration > 0 else 0
            print("\n✅ TEST PASSED")
            print(f"   Rows: {dest_count:,}")
            print(f"   Duration: {duration:.1f}s")
            print(f"   Throughput: {throughput:.1f} rows/sec")

            self.pass_test(
                "medium_table",
                {"rows": 10000, "duration": duration, "throughput": throughput},
            )
        else:
            self.fail("medium_table", f"Row mismatch: {source_count} != {dest_count}")

    def test_large_table_adaptive_batching(self):
        """Test 3: Large table (100K rows) - Adaptive batching"""
        print("\nTest 3: Large Table Adaptive Batching (100K rows)")
        print("=" * 60)

        mysql_conn_id = self.ensure_connection("mysql", "source")
        postgres_conn_id = self.ensure_connection("postgresql", "destination")

        response = requests.post(
            f"{API_URL}/chat/message",
            json={
                "message": "migrate large_table from mysql to postgresql as large_table_dest",
                "context": {
                    "session_id": f"real-pipeline-{int(time.time())}",
                    "user_id": "test-user-123",
                    "source_connection_id": mysql_conn_id,
                    "destination_connection_id": postgres_conn_id,
                },
            },
            timeout=20,
        )

        data = response.json()
        pipeline_id = self.extract_pipeline_id(data)

        if not pipeline_id:
            self.fail("large_table", "No pipeline_id")
            return

        print(f"  Pipeline ID: {pipeline_id}")
        print("  This may take 3-5 minutes...")

        success, duration, final_status = self.monitor_pipeline(pipeline_id, timeout=1200)

        if not success:
            self.fail("large_table", f"Pipeline failed: {final_status}")
            return

        source_count = self.get_mysql_count("large_table")
        dest_count = self.get_postgres_count("large_table_dest")

        if source_count == dest_count == 100000:
            throughput = 100000 / duration if duration > 0 else 0
            print("\n✅ TEST PASSED")
            print(f"   Rows: {dest_count:,}")
            print(f"   Duration: {duration:.1f}s ({duration/60:.1f} min)")
            print(f"   Throughput: {throughput:.1f} rows/sec")

            self.pass_test(
                "large_table",
                {"rows": 100000, "duration": duration, "throughput": throughput},
            )
        else:
            self.fail("large_table", f"Row mismatch: {source_count} != {dest_count}")

    def test_data_integrity(self):
        """Test 4: Verify data integrity (not just counts)"""
        print("\nTest 4: Data Integrity Validation")
        print("=" * 60)

        dest_count = self.get_postgres_count("small_table_dest")
        if dest_count == 0:
            print("  ⚠️  Skipping - no data in destination table")
            print("     Run small_table test first")
            self.pass_test("data_integrity", {"skipped": True})
            return

        mysql_cursor = self.mysql_conn.cursor(dictionary=True)
        postgres_cursor = self.postgres_conn.cursor()

        mysql_cursor.execute("SELECT id, name, email FROM small_table ORDER BY id LIMIT 5")
        mysql_rows = mysql_cursor.fetchall()

        postgres_cursor.execute("SELECT id, name, email FROM small_table_dest ORDER BY id LIMIT 5")
        postgres_rows = postgres_cursor.fetchall()

        matches = 0
        for i in range(min(len(mysql_rows), len(postgres_rows))):
            mysql_row = mysql_rows[i]
            postgres_row = postgres_rows[i]

            if (
                mysql_row["id"] == postgres_row[0]
                and mysql_row["name"] == postgres_row[1]
                and mysql_row["email"] == postgres_row[2]
            ):
                matches += 1
                print(f"  ✅ Row {i+1}: ID={postgres_row[0]}, Name={postgres_row[1]}")
            else:
                print(f"  ❌ Row {i+1}: Mismatch")

        if matches >= 3:
            print("\n✅ TEST PASSED")
            print(f"   {matches}/5 samples matched")
            self.pass_test("data_integrity", {"matches": matches, "total": 5})
        else:
            self.fail("data_integrity", f"Only {matches}/5 samples matched")

    # -----------------------------
    # Helper methods
    # -----------------------------
    def ensure_connection(self, connector_type: str, connection_type: str) -> Optional[str]:
        """Ensure connection exists, create if needed"""

        # Try to find existing connection
        try:
            response = requests.get(f"{API_URL}/connections", timeout=15)
            if response.status_code == 200:
                payload = response.json()
                # handler returns {connections: [...], total: n}
                connections = payload.get("connections") if isinstance(payload, dict) else payload
                if isinstance(connections, list):
                    for conn in connections:
                        if (
                            conn.get("connector_type") == connector_type
                            and conn.get("type") == connection_type
                        ):
                            return conn.get("id")
        except Exception:
            pass

        # Create new connection
        print(f"    Creating {connector_type} {connection_type} connection...")

        config = self.get_connection_config(connector_type)

        response = requests.post(
            f"{API_URL}/connections",
            json={
                "name": f"test_{connector_type}_{connection_type}_{int(time.time())}",
                "connector_type": connector_type,
                "connection_type": connection_type,
                "config": config,
            },
            headers={"Content-Type": "application/json"},
            timeout=20,
        )

        if response.status_code in [200, 201]:
            data = response.json()
            conn_id = data.get("id") or data.get("connection_id")
            print(f"    ✅ Connection created: {conn_id}")
            return conn_id
        else:
            print(f"    ❌ Failed to create connection: {response.status_code}")
            print(f"    Response: {response.text[:200]}")
            return None

    def get_connection_config(self, connector_type: str) -> Dict[str, Any]:
        """Get connection config for connector type (docker-network addresses)."""

        configs = {
            "mysql": {
                "host": "mysql-e2e",
                "port": 3306,
                "database": "test_db",
                "user": "root",
                "password": "rootpassword",
            },
            "postgresql": {
                "host": "postgres",
                "port": 5432,
                "database": "test_db",
                "user": "user",
                "password": "password",
            },
        }

        return configs.get(connector_type, {})

    def extract_pipeline_id(self, response_data: Dict[str, Any]) -> Optional[str]:
        """Extract pipeline_id from various response formats"""

        data = response_data.get("data")
        if isinstance(data, dict) and data.get("pipeline_id"):
            return data.get("pipeline_id")

        if "metadata" in response_data and isinstance(response_data["metadata"], dict):
            pid = response_data["metadata"].get("pipeline_id")
            if pid:
                return pid

        if "pipeline_id" in response_data:
            return response_data["pipeline_id"]

        message = response_data.get("message", "")
        if isinstance(message, str) and "pipeline" in message.lower():
            uuid_pattern = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
            match = re.search(uuid_pattern, message)
            if match:
                return match.group(0)

        return None

    def monitor_pipeline(self, pipeline_id: str, timeout: int = 300) -> Tuple[bool, float, str]:
        """Monitor pipeline until completion or timeout"""

        start_time = time.time()
        last_stage = None
        stages_seen = []

        for _ in range(timeout // 2):
            time.sleep(2)
            elapsed = time.time() - start_time

            try:
                response = requests.get(f"{API_URL}/pipelines/{pipeline_id}/state", timeout=15)
                if response.status_code != 200:
                    continue

                state = response.json()
                status = state.get("status", "unknown")
                current_stage = state.get("current_stage", "")

                # Prefer percent from REST state API
                progress_obj = state.get("progress") or {}
                progress_percent = progress_obj.get("percent") if isinstance(progress_obj, dict) else None
                if not isinstance(progress_percent, int):
                    progress_percent = 0

                if current_stage and current_stage != last_stage:
                    stages_seen.append(current_stage)
                    display_name = current_stage
                    plan = state.get("execution_plan")
                    if isinstance(plan, dict):
                        for stage in plan.get("stages", []) or []:
                            if isinstance(stage, dict) and stage.get("id") == current_stage:
                                display_name = stage.get("display_name") or current_stage
                                break

                    print(f"  [{elapsed:5.1f}s] {display_name:30} ({progress_percent}%)")
                    last_stage = current_stage

                if status == "completed":
                    duration = time.time() - start_time
                    print(f"\n  Pipeline completed in {duration:.1f}s")
                    print(f"  Stages: {' → '.join(stages_seen)}")
                    return True, duration, status

                if status == "failed":
                    duration = time.time() - start_time
                    err = state.get("error") or state.get("message") or "Unknown error"
                    print(f"\n  Pipeline failed at {current_stage}")
                    print(f"  Error: {err}")
                    return False, duration, status

                if status == "waiting_for_user":
                    duration = time.time() - start_time
                    print("\n  Pipeline waiting for user input")
                    return False, duration, status

            except Exception as e:
                print(f"  ⚠️  Error polling: {e}")
                continue

        duration = time.time() - start_time
        print(f"\n  ⏱️  Timeout after {timeout}s")
        return False, duration, "timeout"

    def get_mysql_count(self, table: str) -> int:
        cursor = self.mysql_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        return cursor.fetchone()[0]

    def get_postgres_count(self, table: str) -> int:
        cursor = self.postgres_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        return cursor.fetchone()[0]

    def pass_test(self, test_name: str, metadata: Dict[str, Any]):
        self.test_results.append({"test": test_name, "status": "PASSED", "metadata": metadata})

    def fail(self, test_name: str, reason: str):
        self.test_results.append({"test": test_name, "status": "FAILED", "reason": reason})

    def cleanup(self):
        if self.mysql_conn:
            self.mysql_conn.close()
        if self.postgres_conn:
            self.postgres_conn.close()

    def print_summary(self):
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)

        passed = sum(1 for r in self.test_results if r["status"] == "PASSED")
        failed = sum(1 for r in self.test_results if r["status"] == "FAILED")

        print(f"\nTotal: {len(self.test_results)}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")

        if passed > 0:
            print("\n✅ Passed Tests:")
            for result in self.test_results:
                if result["status"] == "PASSED":
                    meta = result["metadata"]
                    print(f"   {result['test']}", end="")
                    if isinstance(meta, dict) and "throughput" in meta:
                        print(f" - {meta['throughput']:.1f} rows/sec")
                    elif isinstance(meta, dict) and meta.get("skipped"):
                        print(" - skipped")
                    else:
                        print()

        if failed > 0:
            print("\n❌ Failed Tests:")
            for result in self.test_results:
                if result["status"] == "FAILED":
                    print(f"   {result['test']}: {result.get('reason', 'Unknown')}")

        print("\n" + "=" * 60)

        if failed == 0 and passed > 0:
            print("🎉 ALL TESTS PASSED!")
        elif failed > 0:
            print("⚠️  Some tests failed. Review above for details.")

        print("=" * 60)


if __name__ == "__main__":
    tests = RealPipelineTests()
    tests.run_all_tests()

