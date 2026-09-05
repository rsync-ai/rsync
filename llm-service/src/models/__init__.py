"""
Kafka Message Models and Partition Key Builder.

This module provides a universal message format and partition key generation
for all MCP connector types.
"""

from .kafka_message import (
    KafkaMessage,
    Operation,
    ConnectorCategory,
    CONNECTOR_CATEGORIES,
    hash_to_partition,
    topic_name,
    cdc_topic_name,
    protected_topic_name,
    transformed_topic_name,
)

from .entity_stats import (
    EntityStats,
    EntityStatsRegistry,
    HotThresholds,
    DEFAULT_HOT_THRESHOLDS,
    get_global_registry,
)

from .partition_key import (
    FieldMapping,
    PartitionKeyBuilder,
    get_partition_key_builder,
    build_partition_key,
    build_kafka_message,
)

__all__ = [
    # Kafka Message
    "KafkaMessage",
    "Operation",
    "ConnectorCategory",
    "CONNECTOR_CATEGORIES",
    "hash_to_partition",
    "topic_name",
    "cdc_topic_name",
    "protected_topic_name",
    "transformed_topic_name",
    
    # Entity Stats
    "EntityStats",
    "EntityStatsRegistry",
    "HotThresholds",
    "DEFAULT_HOT_THRESHOLDS",
    "get_global_registry",
    
    # Partition Key
    "FieldMapping",
    "PartitionKeyBuilder",
    "get_partition_key_builder",
    "build_partition_key",
    "build_kafka_message",
]
