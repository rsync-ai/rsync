#!/usr/bin/env python3
"""
Tests for CDC Pipeline Configuration Model

Run with: python -m pytest test_cdc_pipeline_model.py -v
"""

import os
import sys
import pytest

# Add repo root so `src.*` imports work (matches other planner tests)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from src.agents.planner.cdc_pipeline_model import (
    CDCPipelineConfig,
    SourceConfig,
    DestinationConfig,
    TopicConfig,
    CDCSafetyPolicy,
    TableMetadata,
    SourceType,
    DestinationType,
    SnapshotMode,
    TopicStrategy,
    DeleteHandlingMode,
)


class TestCDCPipelineModel:
    """Test CDC pipeline configuration model"""
    
    def test_create_mysql_pipeline_config(self):
        """Test creating a MySQL CDC pipeline configuration"""
        source = SourceConfig(
            source_type=SourceType.MYSQL,
            connection_id="conn-mysql-prod-1",
            host="mysql.prod.internal",
            port=3306,
            username="debezium_user",
            password="secret",
            database="production_db",
            server_id=1001,
            gtid_enabled=True,
        )
        
        destination = DestinationConfig(
            destination_type=DestinationType.DATABASE,
            connection_id="conn-snowflake-1",
            merge_strategy="upsert",
            delete_handling=DeleteHandlingMode.PROPAGATE,
        )
        
        topic_config = TopicConfig(
            strategy=TopicStrategy.HYBRID,
            default_partitions=6,
        )
        
        config = CDCPipelineConfig(
            pipeline_id="pipeline-001",
            pipeline_name="Production MySQL to Snowflake",
            tenant_id="tenant-acme",
            created_by="user@acme.com",
            source=source,
            destination=destination,
            selected_tables=["production_db.users", "production_db.orders"],
            snapshot_mode=SnapshotMode.INITIAL,
            topic_config=topic_config,
        )
        
        assert config.pipeline_id == "pipeline-001"
        assert config.source.source_type == SourceType.MYSQL
        assert config.connector_name == "cdc-tenant-acme-pipeline-001"
        assert len(config.selected_tables) == 2
        assert config.snapshot_mode == SnapshotMode.INITIAL
    
    def test_table_metadata_topic_decision(self):
        """Test table metadata determines topic strategy"""
        # Small dimension table
        dim_table = TableMetadata(
            name="dim_products",
            estimated_rows=50_000,
            data_size_mb=10.0,
            change_rate_per_hour=50,
            has_primary_key=True,
            primary_key_columns=["product_id"],
        )
        
        thresholds = {
            "max_rows_for_unified": 10_000_000,
            "max_change_rate_for_unified": 1000,
            "max_size_mb_for_unified": 1024,
        }
        
        # Should be eligible for unified topic
        assert not dim_table.should_use_per_table_topic(thresholds)
        
        # Large fact table
        fact_table = TableMetadata(
            name="fact_orders",
            estimated_rows=50_000_000,
            data_size_mb=5000.0,
            change_rate_per_hour=5000,
            has_primary_key=True,
            primary_key_columns=["order_id"],
        )
        
        # Should get its own topic
        assert fact_table.should_use_per_table_topic(thresholds)
    
    def test_config_serialization(self):
        """Test config to_dict and from_dict round-trip"""
        source = SourceConfig(
            source_type=SourceType.POSTGRESQL,
            connection_id="conn-pg-1",
            host="postgres.internal",
            port=5432,
            username="repl_user",
            password="secret",
            database="mydb",
            publication_name="dbz_pub_pipeline1",
            slot_name="dbz_slot_pipeline1",
        )
        
        destination = DestinationConfig(
            destination_type=DestinationType.OBJECT_STORAGE,
            connection_id="conn-s3-1",
            bucket="cdc-bronze",
            path_template="{tenant_id}/{source_id}/{schema}/{table}/date={date}",
            file_format="parquet",
            partition_by_event_time=True,
            delete_handling=DeleteHandlingMode.APPEND_ONLY,
        )
        
        config = CDCPipelineConfig(
            pipeline_id="pipeline-pg-001",
            pipeline_name="Postgres to S3 Bronze",
            tenant_id="tenant-xyz",
            created_by="admin@xyz.com",
            source=source,
            destination=destination,
            selected_tables=["public.events", "public.logs"],
            snapshot_mode=SnapshotMode.INITIAL,
        )
        
        # Serialize
        config_dict = config.to_dict()
        assert config_dict["pipeline_id"] == "pipeline-pg-001"
        assert config_dict["source"]["source_type"] == "postgresql"
        assert config_dict["destination"]["destination_type"] == "object_storage"
        
        # Deserialize
        config_restored = CDCPipelineConfig.from_dict(config_dict)
        assert config_restored.pipeline_id == config.pipeline_id
        assert config_restored.source.source_type == SourceType.POSTGRESQL
        assert config_restored.destination.bucket == "cdc-bronze"
        
    def test_json_serialization(self):
        """Test JSON serialization"""
        source = SourceConfig(
            source_type=SourceType.MYSQL,
            connection_id="conn-1",
            host="localhost",
            port=3306,
            username="user",
            password="pass",
            database="testdb",
        )
        
        destination = DestinationConfig(
            destination_type=DestinationType.DATABASE,
            connection_id="dest-1",
        )
        
        config = CDCPipelineConfig(
            pipeline_id="test-001",
            pipeline_name="Test Pipeline",
            tenant_id="test-tenant",
            created_by="test@test.com",
            source=source,
            destination=destination,
        )
        
        # To JSON
        json_str = config.to_json()
        assert isinstance(json_str, str)
        assert "test-001" in json_str
        
        # From JSON
        config_restored = CDCPipelineConfig.from_json(json_str)
        assert config_restored.pipeline_id == "test-001"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

