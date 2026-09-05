"""
Universal Partition Key Builder for ALL MCP Connector Types.

This module builds partition keys automatically based on connector metadata,
ensuring consistent key generation across all connector types.
"""

import os
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path

from .kafka_message import (
    KafkaMessage, 
    Operation, 
    ConnectorCategory, 
    CONNECTOR_CATEGORIES,
    hash_to_partition,
)
from .entity_stats import (
    EntityStatsRegistry, 
    HotThresholds, 
    DEFAULT_HOT_THRESHOLDS,
    get_global_registry,
)


@dataclass
class FieldMapping:
    """Defines how to extract partition key fields from connector data."""
    namespace_field: str
    entity_field: str
    record_id_field: str


# Default field mappings by category
DEFAULT_FIELD_MAPPINGS: Dict[ConnectorCategory, FieldMapping] = {
    ConnectorCategory.DATABASE: FieldMapping(
        namespace_field="schema",
        entity_field="table",
        record_id_field="id",
    ),
    ConnectorCategory.NOSQL: FieldMapping(
        namespace_field="database",
        entity_field="collection",
        record_id_field="_id",
    ),
    ConnectorCategory.API: FieldMapping(
        namespace_field="module",
        entity_field="object_type",
        record_id_field="id",
    ),
    ConnectorCategory.STORAGE: FieldMapping(
        namespace_field="bucket",
        entity_field="prefix",
        record_id_field="key",
    ),
    ConnectorCategory.STREAMING: FieldMapping(
        namespace_field="database",
        entity_field="table",
        record_id_field="id",
    ),
    ConnectorCategory.QUEUE: FieldMapping(
        namespace_field="topic",
        entity_field="partition",
        record_id_field="offset",
    ),
}


@dataclass
class KafkaPartitionConfig:
    """Partition configuration from metadata.json."""
    field_mapping: FieldMapping
    hot_thresholds: HotThresholds


class PartitionKeyBuilder:
    """
    Builds partition keys for ANY MCP connector type.
    
    Reads connector metadata to understand field mappings,
    then generates consistent partition keys.
    """
    
    def __init__(self, connectors_dir: Optional[str] = None):
        self.connectors_dir = connectors_dir or os.getenv(
            "MCP_CONNECTORS_PATH",
            "/app/shared/mcp-connectors"
        )
        self._metadata_cache: Dict[str, Dict] = {}
        self._stats_registry = get_global_registry()
        
        # Load all metadata on startup
        self._load_all_metadata()
    
    def _load_all_metadata(self) -> None:
        """Load metadata from all connector directories."""
        connectors_path = Path(self.connectors_dir)
        if not connectors_path.exists():
            return
        
        for connector_dir in connectors_path.iterdir():
            if connector_dir.is_dir():
                metadata_path = connector_dir / "metadata.json"
                if metadata_path.exists():
                    try:
                        with open(metadata_path) as f:
                            self._metadata_cache[connector_dir.name] = json.load(f)
                    except Exception:
                        pass
    
    def get_field_mapping(self, connector_type: str) -> FieldMapping:
        """Get field mapping for a connector type."""
        metadata = self._metadata_cache.get(connector_type, {})
        kafka_config = metadata.get("kafka_partition", {})
        field_config = kafka_config.get("field_mapping", {})
        
        # If metadata has field mapping, use it
        if field_config.get("namespace_field"):
            return FieldMapping(
                namespace_field=field_config.get("namespace_field", "namespace"),
                entity_field=field_config.get("entity_field", "entity"),
                record_id_field=field_config.get("record_id_field", "id"),
            )
        
        # Fall back to default based on category
        category = CONNECTOR_CATEGORIES.get(
            connector_type.lower(),
            ConnectorCategory.API
        )
        return DEFAULT_FIELD_MAPPINGS.get(
            category,
            DEFAULT_FIELD_MAPPINGS[ConnectorCategory.API]
        )
    
    def get_hot_thresholds(self, connector_type: str) -> HotThresholds:
        """Get hot thresholds for a connector type."""
        metadata = self._metadata_cache.get(connector_type, {})
        kafka_config = metadata.get("kafka_partition", {})
        threshold_config = kafka_config.get("hot_thresholds", {})
        
        # If metadata has thresholds, use them
        if threshold_config.get("messages_per_second"):
            return HotThresholds(
                messages_per_second=threshold_config.get("messages_per_second", 1000),
                record_count=threshold_config.get("record_count", 1_000_000),
                size_gb=threshold_config.get("size_gb", 10),
            )
        
        # Fall back to default based on category
        category = CONNECTOR_CATEGORIES.get(
            connector_type.lower(),
            ConnectorCategory.API
        )
        return DEFAULT_HOT_THRESHOLDS.get(
            category,
            DEFAULT_HOT_THRESHOLDS[ConnectorCategory.API]
        )
    
    def build_partition_key(
        self,
        connector_type: str,
        connection_name: str,
        data: Dict[str, Any],
    ) -> str:
        """
        Build partition key from raw connector data.
        
        Args:
            connector_type: MCP connector type (mysql, stripe, mongodb, etc.)
            connection_name: User's connection name
            data: Raw data record from connector
        
        Returns:
            Partition key string
        """
        mapping = self.get_field_mapping(connector_type)
        
        # Extract values
        namespace = self._get_string_field(data, mapping.namespace_field, "default")
        entity = self._get_string_field(data, mapping.entity_field, "unknown")
        record_id = self._get_string_field(data, mapping.record_id_field, "")
        
        # Build base key
        base_key = f"{connection_name}.{namespace}.{entity}"
        
        # Check if entity is hot
        if self._stats_registry.is_hot_entity(base_key) and record_id:
            return f"{base_key}.{record_id}"
        
        return base_key
    
    def build_kafka_message(
        self,
        connector_type: str,
        connection_name: str,
        operation: Operation,
        data: Dict[str, Any],
        pipeline_id: str,
        request_id: str,
    ) -> KafkaMessage:
        """
        Build a complete KafkaMessage from raw connector data.
        
        Args:
            connector_type: MCP connector type
            connection_name: User's connection name
            operation: Operation type
            data: Raw data record
            pipeline_id: Pipeline identifier
            request_id: Request identifier
        
        Returns:
            KafkaMessage instance
        """
        mapping = self.get_field_mapping(connector_type)
        
        namespace = self._get_string_field(data, mapping.namespace_field, "default")
        entity = self._get_string_field(data, mapping.entity_field, "unknown")
        record_id = self._get_string_field(data, mapping.record_id_field, "")
        
        return KafkaMessage(
            connection=connection_name,
            connector_type=connector_type,
            namespace=namespace,
            entity=entity,
            record_id=record_id,
            operation=operation,
            pipeline_id=pipeline_id,
            request_id=request_id,
            after=data,
        )
    
    def get_partition(self, msg: KafkaMessage, num_partitions: int) -> int:
        """Calculate partition for a message."""
        is_hot = self._stats_registry.is_hot_entity(msg.entity_id)
        return msg.get_partition(is_hot, num_partitions)
    
    def record_message(self, msg: KafkaMessage, size_bytes: int = 0) -> None:
        """Record a message in the stats registry."""
        self._stats_registry.record_message(
            entity_id=msg.entity_id,
            connector_type=msg.connector_type,
            size_bytes=size_bytes,
        )
    
    @property
    def stats_registry(self) -> EntityStatsRegistry:
        """Get the entity stats registry."""
        return self._stats_registry
    
    @staticmethod
    def _get_string_field(data: Dict[str, Any], field: str, default: str) -> str:
        """Extract string field from data dictionary."""
        value = data.get(field)
        if value is None:
            return default
        return str(value)


# Global builder instance
_global_builder: Optional[PartitionKeyBuilder] = None


def get_partition_key_builder() -> PartitionKeyBuilder:
    """Get the global partition key builder."""
    global _global_builder
    if _global_builder is None:
        _global_builder = PartitionKeyBuilder()
    return _global_builder


# Convenience functions

def build_partition_key(
    connector_type: str,
    connection_name: str,
    data: Dict[str, Any],
) -> str:
    """Build partition key using the global builder."""
    return get_partition_key_builder().build_partition_key(
        connector_type, connection_name, data
    )


def build_kafka_message(
    connector_type: str,
    connection_name: str,
    operation: Operation,
    data: Dict[str, Any],
    pipeline_id: str,
    request_id: str,
) -> KafkaMessage:
    """Build KafkaMessage using the global builder."""
    return get_partition_key_builder().build_kafka_message(
        connector_type, connection_name, operation, data, pipeline_id, request_id
    )
