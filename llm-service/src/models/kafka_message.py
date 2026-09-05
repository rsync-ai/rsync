"""
Universal Kafka Message Format for ALL MCP Connector Types.

This module provides a generic message format that works with databases,
APIs, file storage, and any future connector type.
"""

from enum import Enum
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field
from datetime import datetime
import hashlib
import json
from src.utils.kafka_topics import topic


class ConnectorCategory(str, Enum):
    """Categories of MCP connectors for partition strategy."""
    DATABASE = "database"
    NOSQL = "nosql"
    API = "api"
    STORAGE = "storage"
    QUEUE = "queue"
    STREAMING = "streaming"


class Operation(str, Enum):
    """Type of data operation."""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    UPSERT = "UPSERT"
    READ = "READ"


# Connector type to category mapping
CONNECTOR_CATEGORIES: Dict[str, ConnectorCategory] = {
    # Databases
    "mysql": ConnectorCategory.DATABASE,
    "postgresql": ConnectorCategory.DATABASE,
    "oracle": ConnectorCategory.DATABASE,
    "sqlserver": ConnectorCategory.DATABASE,
    "db2": ConnectorCategory.DATABASE,
    "mariadb": ConnectorCategory.DATABASE,
    "sqlite": ConnectorCategory.DATABASE,
    
    # NoSQL
    "mongodb": ConnectorCategory.NOSQL,
    "cassandra": ConnectorCategory.NOSQL,
    "redis": ConnectorCategory.NOSQL,
    "dynamodb": ConnectorCategory.NOSQL,
    "elasticsearch": ConnectorCategory.NOSQL,
    
    # APIs/SaaS
    "stripe": ConnectorCategory.API,
    "salesforce": ConnectorCategory.API,
    "hubspot": ConnectorCategory.API,
    "zoho-crm": ConnectorCategory.API,
    "shopify": ConnectorCategory.API,
    "twilio": ConnectorCategory.API,
    "jira": ConnectorCategory.API,
    "freshdesk": ConnectorCategory.API,
    "pipedrive": ConnectorCategory.API,
    
    # File Storage
    "aws-s3": ConnectorCategory.STORAGE,
    "minio": ConnectorCategory.STORAGE,
    "sftp": ConnectorCategory.STORAGE,
    "gcs": ConnectorCategory.STORAGE,
    
    # Streaming/CDC
    "debezium": ConnectorCategory.STREAMING,
    "kafka-mcp-sink": ConnectorCategory.QUEUE,
}


class KafkaMessage(BaseModel):
    """
    Universal message format for ALL MCP connector types.
    
    This model works with databases, APIs, file storage, and any future connector.
    """
    
    # ===== UNIVERSAL ROUTING (Required for ALL connectors) =====
    connection: str = Field(..., description="Connection name: prod_mysql, stripe_api, s3_data")
    connector_type: str = Field(..., description="MCP connector type: mysql, stripe, aws-s3, mongodb")
    
    # ===== HIERARCHICAL LOCATION (Generic naming) =====
    namespace: str = Field(..., description="Logical grouping: schema, database, bucket, org")
    entity: str = Field(..., description="Primary object: table, collection, object_type, prefix")
    sub_entity: Optional[str] = Field(None, description="Optional nested entity")
    
    # ===== RECORD IDENTIFICATION =====
    record_id: str = Field(..., description="Unique record identifier")
    
    # ===== OPERATION DETAILS =====
    operation: Operation = Field(..., description="INSERT, UPDATE, DELETE, UPSERT, READ")
    timestamp: int = Field(default_factory=lambda: int(datetime.now().timestamp() * 1000))
    
    # ===== PAYLOAD =====
    before: Optional[Dict[str, Any]] = Field(None, description="Previous state (UPDATE/DELETE)")
    after: Optional[Dict[str, Any]] = Field(None, description="New state (INSERT/UPDATE)")
    
    # ===== METADATA =====
    pipeline_id: str = Field(..., description="Pipeline identifier")
    request_id: str = Field(..., description="Request identifier")
    source_metadata: Optional[Dict[str, Any]] = Field(None, description="Connector-specific metadata")
    
    class Config:
        use_enum_values = True
    
    @property
    def base_partition_key(self) -> str:
        """
        Base partition key WITHOUT record_id.
        Use for small/normal entities to keep all records together.
        """
        key = f"{self.connection}.{self.namespace}.{self.entity}"
        if self.sub_entity:
            key = f"{key}.{self.sub_entity}"
        return key
    
    @property
    def full_partition_key(self) -> str:
        """
        Full partition key WITH record_id.
        Use for large/hot entities to distribute across partitions.
        """
        return f"{self.base_partition_key}.{self.record_id}"
    
    def get_partition_key(self, is_hot_entity: bool = False) -> str:
        """
        Get appropriate partition key based on entity characteristics.
        
        Args:
            is_hot_entity: True if entity has high throughput
        """
        if is_hot_entity:
            return self.full_partition_key
        return self.base_partition_key
    
    def get_partition(self, is_hot_entity: bool, num_partitions: int) -> int:
        """Calculate partition number using consistent hashing."""
        key = self.get_partition_key(is_hot_entity)
        return hash_to_partition(key, num_partitions)
    
    def get_connector_category(self) -> ConnectorCategory:
        """Get the category for the connector type."""
        return CONNECTOR_CATEGORIES.get(
            self.connector_type.lower(),
            ConnectorCategory.API
        )
    
    @property
    def entity_id(self) -> str:
        """Unique identifier for the entity (for stats tracking)."""
        return self.base_partition_key
    
    def to_json(self) -> str:
        """Serialize to JSON string."""
        return self.model_dump_json()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return self.model_dump()
    
    @classmethod
    def from_json(cls, data: str) -> "KafkaMessage":
        """Deserialize from JSON string."""
        return cls.model_validate_json(data)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KafkaMessage":
        """Create from dictionary."""
        return cls.model_validate(data)


def hash_to_partition(key: str, num_partitions: int) -> int:
    """Calculate partition number from a key string using consistent hashing."""
    if num_partitions <= 0:
        return 0
    
    hash_bytes = hashlib.sha256(key.encode()).digest()
    hash_int = int.from_bytes(hash_bytes[:8], byteorder='big')
    
    return hash_int % num_partitions


def topic_name(connection_name: str) -> str:
    """Generate topic name for a connection."""
    normalized = connection_name.lower().replace(" ", "-").replace("_", "-")
    return topic(f"conn.{normalized}")


def cdc_topic_name(connection_name: str) -> str:
    """Generate CDC topic name for a connection."""
    normalized = connection_name.lower().replace(" ", "-").replace("_", "-")
    return topic(f"cdc.{normalized}")


def protected_topic_name(connection_name: str) -> str:
    """Generate PII-protected topic name."""
    normalized = connection_name.lower().replace(" ", "-").replace("_", "-")
    return topic(f"protected.conn.{normalized}")


def transformed_topic_name(pipeline_id: str, destination: str) -> str:
    """Generate transformed data topic name."""
    return f"transformed.{pipeline_id}.{destination}"
