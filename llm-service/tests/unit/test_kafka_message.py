"""
Unit tests for Kafka Message and Partition Key Builder.
"""

import pytest
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from models.kafka_message import (
    KafkaMessage,
    Operation,
    ConnectorCategory,
    CONNECTOR_CATEGORIES,
    hash_to_partition,
    topic_name,
    cdc_topic_name,
    protected_topic_name,
)
from models.entity_stats import (
    EntityStats,
    EntityStatsRegistry,
    HotThresholds,
    DEFAULT_HOT_THRESHOLDS,
)
# Imported via the same path src/models/kafka_message.py uses, so this test
# reads the identical module object the code under test does. A bare
# `utils.` import resolves to a different package once another test in the
# suite has bound that name.
from src.utils.kafka_topics import topic_prefix
from models.partition_key import (
    PartitionKeyBuilder,
    FieldMapping,
    DEFAULT_FIELD_MAPPINGS,
)


class TestKafkaMessage:
    """Tests for KafkaMessage model."""
    
    def test_create_message(self):
        """Test creating a basic KafkaMessage."""
        msg = KafkaMessage(
            connection="prod_mysql",
            connector_type="mysql",
            namespace="public",
            entity="users",
            record_id="12345",
            operation=Operation.INSERT,
            pipeline_id="pipeline_123",
            request_id="req_456",
        )
        
        assert msg.connection == "prod_mysql"
        assert msg.connector_type == "mysql"
        assert msg.namespace == "public"
        assert msg.entity == "users"
        assert msg.record_id == "12345"
        assert msg.operation == Operation.INSERT
    
    def test_base_partition_key(self):
        """Test base partition key generation."""
        msg = KafkaMessage(
            connection="prod_mysql",
            connector_type="mysql",
            namespace="public",
            entity="users",
            record_id="12345",
            operation=Operation.INSERT,
            pipeline_id="pipeline_123",
            request_id="req_456",
        )
        
        assert msg.base_partition_key == "prod_mysql.public.users"
    
    def test_full_partition_key(self):
        """Test full partition key generation."""
        msg = KafkaMessage(
            connection="prod_mysql",
            connector_type="mysql",
            namespace="public",
            entity="users",
            record_id="12345",
            operation=Operation.INSERT,
            pipeline_id="pipeline_123",
            request_id="req_456",
        )
        
        assert msg.full_partition_key == "prod_mysql.public.users.12345"
    
    def test_partition_key_with_sub_entity(self):
        """Test partition key with sub_entity."""
        msg = KafkaMessage(
            connection="mongo_prod",
            connector_type="mongodb",
            namespace="mydb",
            entity="users",
            sub_entity="preferences",
            record_id="507f1f77",
            operation=Operation.INSERT,
            pipeline_id="pipeline_123",
            request_id="req_456",
        )
        
        assert msg.base_partition_key == "mongo_prod.mydb.users.preferences"
        assert msg.full_partition_key == "mongo_prod.mydb.users.preferences.507f1f77"
    
    def test_get_partition_key_normal(self):
        """Test partition key for normal entity."""
        msg = KafkaMessage(
            connection="prod_mysql",
            connector_type="mysql",
            namespace="public",
            entity="users",
            record_id="12345",
            operation=Operation.INSERT,
            pipeline_id="pipeline_123",
            request_id="req_456",
        )
        
        # Normal entity: should return base key
        assert msg.get_partition_key(is_hot_entity=False) == "prod_mysql.public.users"
    
    def test_get_partition_key_hot(self):
        """Test partition key for hot entity."""
        msg = KafkaMessage(
            connection="prod_mysql",
            connector_type="mysql",
            namespace="public",
            entity="orders",
            record_id="99999",
            operation=Operation.INSERT,
            pipeline_id="pipeline_123",
            request_id="req_456",
        )
        
        # Hot entity: should return full key with record_id
        assert msg.get_partition_key(is_hot_entity=True) == "prod_mysql.public.orders.99999"
    
    def test_get_connector_category(self):
        """Test connector category detection."""
        mysql_msg = KafkaMessage(
            connection="prod",
            connector_type="mysql",
            namespace="public",
            entity="users",
            record_id="1",
            operation=Operation.INSERT,
            pipeline_id="p",
            request_id="r",
        )
        assert mysql_msg.get_connector_category() == ConnectorCategory.DATABASE
        
        mongo_msg = KafkaMessage(
            connection="prod",
            connector_type="mongodb",
            namespace="mydb",
            entity="users",
            record_id="1",
            operation=Operation.INSERT,
            pipeline_id="p",
            request_id="r",
        )
        assert mongo_msg.get_connector_category() == ConnectorCategory.NOSQL
        
        stripe_msg = KafkaMessage(
            connection="prod",
            connector_type="stripe",
            namespace="payments",
            entity="charges",
            record_id="ch_1",
            operation=Operation.INSERT,
            pipeline_id="p",
            request_id="r",
        )
        assert stripe_msg.get_connector_category() == ConnectorCategory.API
    
    def test_serialization(self):
        """Test JSON serialization."""
        msg = KafkaMessage(
            connection="prod_mysql",
            connector_type="mysql",
            namespace="public",
            entity="users",
            record_id="12345",
            operation=Operation.INSERT,
            pipeline_id="pipeline_123",
            request_id="req_456",
            after={"id": 12345, "name": "John"},
        )
        
        json_str = msg.to_json()
        assert "prod_mysql" in json_str
        assert "users" in json_str
        
        # Deserialize
        msg2 = KafkaMessage.from_json(json_str)
        assert msg2.connection == msg.connection
        assert msg2.entity == msg.entity


class TestHashToPartition:
    """Tests for partition hashing."""
    
    def test_consistent_hashing(self):
        """Test that same key always maps to same partition."""
        key = "prod_mysql.public.users"
        
        partition1 = hash_to_partition(key, 10)
        partition2 = hash_to_partition(key, 10)
        partition3 = hash_to_partition(key, 10)
        
        assert partition1 == partition2 == partition3
    
    def test_distribution(self):
        """Test that different keys distribute across partitions."""
        partitions = set()
        
        for i in range(100):
            key = f"prod_mysql.public.table_{i}"
            partition = hash_to_partition(key, 10)
            partitions.add(partition)
        
        # Should use multiple partitions
        assert len(partitions) > 5
    
    def test_zero_partitions(self):
        """Test handling of zero partitions."""
        partition = hash_to_partition("key", 0)
        assert partition == 0


class TestEntityStats:
    """Tests for entity statistics."""
    
    def test_create_stats(self):
        """Test creating entity stats."""
        stats = EntityStats(
            entity_id="prod_mysql.public.users",
            connector_type="mysql",
        )
        
        assert stats.entity_id == "prod_mysql.public.users"
        assert stats.connector_type == "mysql"
        assert stats.message_count == 0
    
    def test_record_message(self):
        """Test recording messages."""
        stats = EntityStats(
            entity_id="prod_mysql.public.users",
            connector_type="mysql",
        )
        
        stats.record_message(1000)
        assert stats.message_count == 1
        assert stats.size_bytes == 1000
        
        stats.record_message(2000)
        assert stats.message_count == 2
        assert stats.size_bytes == 3000
    
    def test_hot_entity_detection(self):
        """Test hot entity detection."""
        stats = EntityStats(
            entity_id="prod_mysql.public.orders",
            connector_type="mysql",
        )
        
        # Initially not hot
        assert not stats.is_hot_entity()
        
        # Set high record count
        stats.unique_records = 5_000_000
        assert stats.is_hot_entity()
    
    def test_hot_thresholds(self):
        """Test custom hot thresholds."""
        stats = EntityStats(
            entity_id="prod_mysql.public.orders",
            connector_type="mysql",
        )
        stats.unique_records = 500
        
        # Low thresholds
        low_thresholds = HotThresholds(
            messages_per_second=10,
            record_count=100,
            size_gb=0.001,
        )
        
        assert stats.is_hot_with_thresholds(low_thresholds)


class TestEntityStatsRegistry:
    """Tests for entity stats registry."""
    
    def test_get_or_create(self):
        """Test get or create behavior."""
        registry = EntityStatsRegistry()
        
        stats1 = registry.get_or_create("prod_mysql.public.users", "mysql")
        stats2 = registry.get_or_create("prod_mysql.public.users", "mysql")
        
        assert stats1 is stats2  # Same instance
    
    def test_record_message(self):
        """Test recording messages through registry."""
        registry = EntityStatsRegistry()
        
        registry.record_message("prod_mysql.public.users", "mysql", 1000)
        registry.record_message("prod_mysql.public.users", "mysql", 2000)
        
        stats = registry.get("prod_mysql.public.users")
        assert stats.message_count == 2
    
    def test_get_hot_entities(self):
        """Test getting hot entities."""
        registry = EntityStatsRegistry()
        
        # Create stats with high record count
        stats = registry.get_or_create("prod_mysql.public.orders", "mysql")
        stats.unique_records = 10_000_000
        
        hot = registry.get_hot_entities()
        assert "prod_mysql.public.orders" in hot


class TestTopicNaming:
    """Tests for topic naming functions."""
    
    def test_topic_name(self):
        """Test connection topic name."""
        assert topic_name("prod_mysql") == "rsync.conn.prod-mysql"
        assert topic_name("my connection") == "rsync.conn.my-connection"

    def test_cdc_topic_name(self):
        """Test CDC topic name."""
        assert cdc_topic_name("prod_mysql") == "rsync.cdc.prod-mysql"

    def test_protected_topic_name(self):
        """Test PII-protected topic name."""
        assert protected_topic_name("prod_mysql") == "rsync.protected.conn.prod-mysql"

    def test_every_helper_is_namespaced(self):
        """Every name-building helper in this module must go through topic().

        Asserted as a property rather than three literals because the failure
        mode is a *stranded* helper: the namespacing change routed
        cdc_topic_name() through topic() and left topic_name() and
        protected_topic_name() building bare strings. Nothing raised -- they
        simply returned a name outside the prefix an operator granted ACLs on,
        which on a BYO cluster is an authorization failure that reads as a
        consumer that never receives anything.
        """
        prefix = topic_prefix()
        assert prefix, "default prefix must be non-empty for this assertion to mean anything"
        for helper in (topic_name, cdc_topic_name, protected_topic_name):
            assert helper("prod_mysql").startswith(prefix), (
                f"{helper.__name__} does not route through topic()"
            )

    def test_empty_prefix_disables_qualification(self, monkeypatch):
        """The documented migration lever: KAFKA_TOPIC_PREFIX="" -> bare names.

        A deployment with live topics and committed offsets under the unprefixed
        names needs this to stay reachable.
        """
        monkeypatch.setenv("KAFKA_TOPIC_PREFIX", "")
        assert topic_name("prod_mysql") == "conn.prod-mysql"
        assert cdc_topic_name("prod_mysql") == "cdc.prod-mysql"
        assert protected_topic_name("prod_mysql") == "protected.conn.prod-mysql"


class TestPartitionKeyBuilder:
    """Tests for partition key builder."""
    
    def test_default_field_mappings(self):
        """Test default field mappings exist for all categories."""
        for category in ConnectorCategory:
            assert category in DEFAULT_FIELD_MAPPINGS
    
    def test_build_partition_key_database(self):
        """Test building partition key for database."""
        builder = PartitionKeyBuilder(connectors_dir="/nonexistent")
        
        key = builder.build_partition_key(
            connector_type="mysql",
            connection_name="prod_mysql",
            data={"schema": "public", "table": "users", "id": 12345},
        )
        
        assert key == "prod_mysql.public.users"
    
    def test_build_partition_key_api(self):
        """Test building partition key for API."""
        builder = PartitionKeyBuilder(connectors_dir="/nonexistent")
        
        key = builder.build_partition_key(
            connector_type="stripe",
            connection_name="stripe_prod",
            data={"module": "payments", "object_type": "charges", "id": "ch_123"},
        )
        
        assert key == "stripe_prod.payments.charges"
    
    def test_build_partition_key_nosql(self):
        """Test building partition key for NoSQL."""
        builder = PartitionKeyBuilder(connectors_dir="/nonexistent")
        
        key = builder.build_partition_key(
            connector_type="mongodb",
            connection_name="mongo_prod",
            data={"database": "analytics", "collection": "events", "_id": "507f"},
        )
        
        assert key == "mongo_prod.analytics.events"
    
    def test_build_kafka_message(self):
        """Test building complete KafkaMessage."""
        builder = PartitionKeyBuilder(connectors_dir="/nonexistent")
        
        msg = builder.build_kafka_message(
            connector_type="mysql",
            connection_name="prod_mysql",
            operation=Operation.INSERT,
            data={"schema": "public", "table": "users", "id": 12345, "name": "John"},
            pipeline_id="pipeline_123",
            request_id="req_456",
        )
        
        assert msg.connection == "prod_mysql"
        assert msg.namespace == "public"
        assert msg.entity == "users"
        assert msg.record_id == "12345"
        assert msg.after["name"] == "John"


class TestConnectorCategories:
    """Tests for connector category mappings."""
    
    def test_database_connectors(self):
        """Test database connector categories."""
        assert CONNECTOR_CATEGORIES["mysql"] == ConnectorCategory.DATABASE
        assert CONNECTOR_CATEGORIES["postgresql"] == ConnectorCategory.DATABASE
        assert CONNECTOR_CATEGORIES["oracle"] == ConnectorCategory.DATABASE
        assert CONNECTOR_CATEGORIES["sqlserver"] == ConnectorCategory.DATABASE
    
    def test_nosql_connectors(self):
        """Test NoSQL connector categories."""
        assert CONNECTOR_CATEGORIES["mongodb"] == ConnectorCategory.NOSQL
        assert CONNECTOR_CATEGORIES["cassandra"] == ConnectorCategory.NOSQL
        assert CONNECTOR_CATEGORIES["redis"] == ConnectorCategory.NOSQL
    
    def test_api_connectors(self):
        """Test API connector categories."""
        assert CONNECTOR_CATEGORIES["stripe"] == ConnectorCategory.API
        assert CONNECTOR_CATEGORIES["salesforce"] == ConnectorCategory.API
        assert CONNECTOR_CATEGORIES["hubspot"] == ConnectorCategory.API
    
    def test_storage_connectors(self):
        """Test storage connector categories."""
        assert CONNECTOR_CATEGORIES["aws-s3"] == ConnectorCategory.STORAGE
        assert CONNECTOR_CATEGORIES["minio"] == ConnectorCategory.STORAGE


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
