#!/usr/bin/env python3
"""
Tests for HITL Table Selection and Validation

Run with: python -m pytest test_cdc_table_selector.py -v
"""

import os
import sys
import pytest

# Add repo root so `src.*` imports work (matches other planner tests)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from src.agents.planner.cdc_table_selector import (
    PIIDetector,
    PIIFinding,
    PIISeverity,
    TableValidator,
    TableSelector,
    TableSelectionDiff,
    TableValidationResult,
)
from src.agents.planner.cdc_pipeline_model import SourceType
from src.agents.planner.cdc_size_estimator import format_size_for_display, estimate_snapshot_time


class TestPIIDetector:
    """Test PII detection"""
    
    def test_detect_email(self):
        """Test email PII detection"""
        detector = PIIDetector()
        
        columns = [
            {"name": "user_email", "type": "varchar"},
            {"name": "contact_email_address", "type": "text"},
            {"name": "id", "type": "int"},
        ]
        
        findings = detector.scan_table_columns(columns)
        
        assert len(findings) >= 2
        email_findings = [f for f in findings if f.pii_type == "email"]
        assert len(email_findings) == 2
        assert all(f.severity == PIISeverity.HIGH for f in email_findings)
    
    def test_detect_critical_pii(self):
        """Test critical PII detection (SSN, passwords, CC)"""
        detector = PIIDetector()
        
        columns = [
            {"name": "ssn", "type": "varchar"},
            {"name": "password_hash", "type": "varchar"},
            {"name": "credit_card_number", "type": "varchar"},
        ]
        
        findings = detector.scan_table_columns(columns)
        
        assert len(findings) == 3
        assert all(f.severity == PIISeverity.CRITICAL for f in findings)
        
        pii_types = {f.pii_type for f in findings}
        assert "ssn" in pii_types
        assert "password" in pii_types
        assert "credit_card" in pii_types
    
    def test_no_pii(self):
        """Test table with no PII"""
        detector = PIIDetector()
        
        columns = [
            {"name": "id", "type": "int"},
            {"name": "product_name", "type": "varchar"},
            {"name": "quantity", "type": "int"},
            {"name": "created_at", "type": "timestamp"},
        ]
        
        findings = detector.scan_table_columns(columns)
        
        assert len(findings) == 0


class TestTableValidator:
    """Test table validation"""
    
    def test_validate_table_with_pk(self):
        """Test validating table with primary key"""
        validator = TableValidator(require_pk=True)
        
        table_schema = {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "varchar"},
            ],
            "primary_key": ["id"],
            "estimated_rows": 1000,
            "data_size_mb": 5.0,
        }
        
        result = validator.validate_table(
            "mydb.users",
            table_schema,
            SourceType.MYSQL,
        )
        
        assert result.is_valid
        assert result.has_primary_key
        assert result.primary_key_columns == ["id"]
        assert not result.should_block
        assert len(result.errors) == 0
    
    def test_validate_table_without_pk(self):
        """Test validating table without primary key (should block)"""
        validator = TableValidator(require_pk=True)
        
        table_schema = {
            "columns": [
                {"name": "log_message", "type": "text"},
                {"name": "timestamp", "type": "timestamp"},
            ],
            "primary_key": [],
            "estimated_rows": 10000,
        }
        
        result = validator.validate_table(
            "mydb.logs",
            table_schema,
            SourceType.MYSQL,
        )
        
        assert not result.is_valid
        assert not result.has_primary_key
        assert result.should_block
        assert result.block_reason == "No primary key or replica identity"
        assert len(result.errors) > 0
    
    def test_validate_table_with_critical_pii(self):
        """Test table with critical PII (should block if policy enabled)"""
        validator = TableValidator(
            require_pk=True,
            enable_pii_detection=True,
            block_critical_pii=True,
        )
        
        table_schema = {
            "columns": [
                {"name": "user_id", "type": "int"},
                {"name": "ssn", "type": "varchar"},
                {"name": "credit_card_number", "type": "varchar"},
            ],
            "primary_key": ["user_id"],
            "estimated_rows": 5000,
        }
        
        result = validator.validate_table(
            "mydb.sensitive_data",
            table_schema,
            SourceType.MYSQL,
        )
        
        assert result.has_primary_key
        assert result.has_critical_pii
        assert result.should_block
        assert "Critical PII detected" in result.block_reason
        assert len(result.pii_findings) >= 2
    
    def test_large_table_warnings(self):
        """Test warnings for large tables"""
        validator = TableValidator()
        
        table_schema = {
            "columns": [{"name": "id", "type": "int"}],
            "primary_key": ["id"],
            "estimated_rows": 150_000_000,  # 150M rows
            "data_size_mb": 15_000.0,  # 15 GB
        }
        
        result = validator.validate_table(
            "mydb.huge_table",
            table_schema,
            SourceType.MYSQL,
        )
        
        assert result.is_valid
        assert len(result.warnings) >= 2
        assert any("Large table" in w for w in result.warnings)
        assert any("Large data size" in w for w in result.warnings)


class TestTableSelector:
    """Test HITL table selection"""
    
    def test_create_diff_add_tables(self):
        """Test creating diff for adding tables"""
        selector = TableSelector()
        
        current_tables = ["mydb.users"]
        requested_tables = ["mydb.users", "mydb.orders", "mydb.products"]
        
        table_schemas = {
            "mydb.orders": {
                "columns": [{"name": "order_id", "type": "int"}],
                "primary_key": ["order_id"],
                "estimated_rows": 50000,
                "data_size_mb": 100.0,
            },
            "mydb.products": {
                "columns": [{"name": "product_id", "type": "int"}],
                "primary_key": ["product_id"],
                "estimated_rows": 1000,
                "data_size_mb": 5.0,
            },
        }
        
        diff = selector.create_table_diff(
            current_tables,
            requested_tables,
            table_schemas,
            SourceType.MYSQL,
        )
        
        assert diff.total_new_tables == 2
        assert "mydb.orders" in diff.tables_to_add
        assert "mydb.products" in diff.tables_to_add
        assert diff.total_removed_tables == 0
        assert diff.requires_approval
        assert diff.total_estimated_rows == 51000
        assert diff.total_size_mb == 105.0
    
    def test_create_diff_with_blocked_table(self):
        """Test diff with a table that should be blocked"""
        selector = TableSelector()
        
        current_tables = []
        requested_tables = ["mydb.sensitive"]
        
        table_schemas = {
            "mydb.sensitive": {
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "ssn", "type": "varchar"},
                    {"name": "password", "type": "varchar"},
                ],
                "primary_key": ["id"],
                "estimated_rows": 100,
            },
        }
        
        diff = selector.create_table_diff(
            current_tables,
            requested_tables,
            table_schemas,
            SourceType.MYSQL,
        )
        
        assert diff.total_new_tables == 1
        assert len(diff.blocked_tables) == 1
        assert "mydb.sensitive" in diff.blocked_tables
        assert diff.has_critical_issues()
    
    def test_diff_summary(self):
        """Test diff summary generation"""
        selector = TableSelector()
        
        current_tables = ["mydb.users"]
        requested_tables = ["mydb.users", "mydb.orders"]
        
        table_schemas = {
            "mydb.orders": {
                "columns": [
                    {"name": "order_id", "type": "int"},
                    {"name": "customer_email", "type": "varchar"},
                ],
                "primary_key": ["order_id"],
                "estimated_rows": 10000,
                "data_size_mb": 50.0,
            },
        }
        
        diff = selector.create_table_diff(
            current_tables,
            requested_tables,
            table_schemas,
            SourceType.MYSQL,
        )
        
        summary = diff.get_summary()
        
        assert "Adding 1 table" in summary
        assert "mydb.orders" in summary
        assert "10,000 rows" in summary
        assert "50.0 MB" in summary
        assert "PII detected: email" in summary


class TestSizeEstimation:
    """Test size estimation utilities"""
    
    def test_format_size_bytes(self):
        """Test size formatting"""
        assert format_size_for_display(500) == "500 B"
        assert format_size_for_display(2048) == "2.0 KB"
        assert format_size_for_display(5 * 1024 * 1024) == "5.0 MB"
        assert format_size_for_display(2 * 1024 * 1024 * 1024) == "2.00 GB"
    
    def test_snapshot_time_estimation(self):
        """Test snapshot time estimation"""
        # Small table
        assert "seconds" in estimate_snapshot_time(5000)
        
        # Medium table (still seconds at default 10k rows/sec)
        assert "seconds" in estimate_snapshot_time(500_000)
        
        # Large table
        assert "hours" in estimate_snapshot_time(50_000_000)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

