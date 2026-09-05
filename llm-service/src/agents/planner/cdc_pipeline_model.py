#!/usr/bin/env python3
"""
CDC Pipeline Configuration Model
Canonical model for CDC pipelines with HITL-first design.

This model represents the complete CDC pipeline configuration including:
- Source database connection and tables
- Snapshot mode and CDC settings
- Destination configuration
- Topic strategy (per-table vs unified)
- Safety policies (PII, prerequisites, DLQ)
- Multi-tenant isolation
"""

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
import json


class SnapshotMode(Enum):
    """Debezium snapshot modes"""
    INITIAL = "initial"  # Full snapshot + CDC
    SCHEMA_ONLY = "schema_only"  # Schema snapshot only, then CDC
    NEVER = "never"  # CDC only, no snapshot
    WHEN_NEEDED = "when_needed"  # Snapshot if no offset exists
    

class SourceType(Enum):
    """Supported source database types"""
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    POSTGRES = "postgres"  # Alias
    MONGODB = "mongodb"
    ORACLE = "oracle"
    SQLSERVER = "sqlserver"
    DB2 = "db2"
    CASSANDRA = "cassandra"


class DestinationType(Enum):
    """Supported destination types"""
    DATABASE = "database"  # Any DB destination (Postgres, MySQL, Snowflake, etc.)
    OBJECT_STORAGE = "object_storage"  # S3, MinIO, GCS, Azure Blob
    KAFKA = "kafka"  # Kafka topics for downstream consumers


class TopicStrategy(Enum):
    """Topic routing strategy"""
    PER_TABLE = "per_table"  # Debezium default: one topic per table
    UNIFIED = "unified"  # All tables to one topic per pipeline
    HYBRID = "hybrid"  # Rule-based: dimensions unified, facts per-table


class DeleteHandlingMode(Enum):
    """How to handle delete events at destination"""
    PROPAGATE = "propagate"  # Execute DELETE (default for DB destinations)
    SOFT_DELETE = "soft_delete"  # Add deleted_at column
    DROP = "drop"  # Ignore delete events
    APPEND_ONLY = "append_only"  # Keep delete events as records (for Bronze)


@dataclass
class TableMetadata:
    """Metadata about a source table"""
    name: str
    estimated_rows: int = 0
    data_size_mb: float = 0.0
    change_rate_per_hour: int = 0
    has_primary_key: bool = False
    primary_key_columns: List[str] = field(default_factory=list)
    has_pii: bool = False
    pii_columns: List[str] = field(default_factory=list)
    
    def should_use_per_table_topic(self, thresholds: Dict[str, Any]) -> bool:
        """
        Determine if this table should get its own topic based on thresholds.
        """
        return (
            self.estimated_rows > thresholds.get("max_rows_for_unified", 10_000_000)
            or self.change_rate_per_hour > thresholds.get("max_change_rate_for_unified", 1000)
            or self.data_size_mb > thresholds.get("max_size_mb_for_unified", 1024)
        )


@dataclass
class SourceConfig:
    """Source database configuration"""
    source_type: SourceType
    connection_id: str  # Reference to connection in connection manager
    host: str
    port: int
    username: str
    password: str  # Will be encrypted/vaulted
    database: str
    
    # Optional overrides for DB-specific settings
    extra_config: Dict[str, Any] = field(default_factory=dict)
    
    # Postgres-specific
    publication_name: Optional[str] = None
    slot_name: Optional[str] = None
    
    # MySQL-specific
    server_id: Optional[int] = None  # Stable, deterministic server ID
    gtid_enabled: bool = False


@dataclass
class DestinationConfig:
    """Destination configuration"""
    destination_type: DestinationType
    connection_id: str  # Reference to destination connection
    
    # DB destination specific
    merge_strategy: Optional[str] = None  # "upsert", "merge", "append"
    delete_handling: DeleteHandlingMode = DeleteHandlingMode.PROPAGATE
    
    # Object storage specific
    bucket: Optional[str] = None
    path_template: str = "{tenant_id}/{source_id}/{schema}/{table}/date={date}"  # S3 path
    file_format: str = "parquet"  # parquet, orc, avro, csv
    partition_by_event_time: bool = True  # vs ingestion time
    
    # Extra config
    extra_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TopicConfig:
    """Kafka topic configuration"""
    strategy: TopicStrategy = TopicStrategy.HYBRID
    
    # Topic naming
    topic_prefix: str = ""  # e.g., "cdc.{tenant}.{source}.{pipeline}"
    
    # Per-table or unified
    unified_topic_name: Optional[str] = None
    
    # Partitioning
    default_partitions: int = 1  # Conservative default
    default_replication_factor: int = 3
    
    # Thresholds for hybrid strategy
    hybrid_thresholds: Dict[str, Any] = field(default_factory=lambda: {
        "max_rows_for_unified": 10_000_000,
        "max_change_rate_for_unified": 1000,
        "max_size_mb_for_unified": 1024,
    })
    
    # Schema Registry
    schema_registry_url: str = "http://schema-registry:8080"
    subject_name_strategy: str = "TopicNameStrategy"  # or TopicRecordNameStrategy for unified


@dataclass
class CDCSafetyPolicy:
    """Safety policies for CDC pipeline"""
    # Table validation
    require_primary_key: bool = True
    block_tables_without_pk: bool = True
    
    # PII detection
    enable_pii_detection: bool = True
    block_critical_pii: bool = True  # Block tables with SSN, CC, passwords
    
    # Prerequisites validation
    validate_prerequisites: bool = True  # Check binlog, WAL level, etc.
    
    # Postgres-specific
    max_slot_lag_mb: int = 1000  # Circuit break at 1GB
    max_slot_lag_seconds: int = 3600  # Circuit break at 1 hour
    
    # DLQ
    enable_dlq: bool = True
    dlq_topic_prefix: str = "dlq"
    
    # Restart policy
    enable_circuit_breaker: bool = True
    max_restart_attempts: int = 5
    restart_backoff_seconds: List[int] = field(default_factory=lambda: [1, 2, 4, 8, 16])
    
    # Per-tenant limits
    max_pipelines_per_source: int = 10
    max_tables_per_pipeline: int = 500


@dataclass
class CDCPipelineConfig:
    """
    Canonical CDC Pipeline Configuration Model.
    
    This is the single source of truth for CDC pipeline configuration
    that gets translated into:
    - Debezium connector config (via MCP)
    - Kafka topic creation
    - Sink connector config
    - Monitoring/alerting rules
    """
    # Identity
    pipeline_id: str
    pipeline_name: str
    tenant_id: str
    created_by: str
    
    # Source
    source: SourceConfig

    # Destination
    destination: DestinationConfig

    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Tables (HITL-approved)
    selected_tables: List[str] = field(default_factory=list)
    table_metadata: Dict[str, TableMetadata] = field(default_factory=dict)
    
    # CDC settings
    snapshot_mode: SnapshotMode = SnapshotMode.INITIAL
    
    # Topic strategy
    topic_config: TopicConfig = field(default_factory=TopicConfig)
    
    # Safety policies
    safety_policy: CDCSafetyPolicy = field(default_factory=CDCSafetyPolicy)
    
    # Debezium connector name (generated or explicit)
    connector_name: Optional[str] = None
    
    # State
    status: str = "pending"  # pending, creating, running, paused, failed, stopped
    
    # Optional overrides for advanced users
    debezium_config_overrides: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate and normalize configuration after initialization"""
        if not self.connector_name:
            self.connector_name = f"cdc-{self.tenant_id}-{self.pipeline_id}"
        
        if not self.topic_config.topic_prefix:
            self.topic_config.topic_prefix = f"cdc.{self.tenant_id}.{self.source.database}.{self.pipeline_id}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "tenant_id": self.tenant_id,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "source": {
                "source_type": self.source.source_type.value,
                "connection_id": self.source.connection_id,
                "host": self.source.host,
                "port": self.source.port,
                "username": self.source.username,
                "database": self.source.database,
                "publication_name": self.source.publication_name,
                "slot_name": self.source.slot_name,
                "server_id": self.source.server_id,
                "gtid_enabled": self.source.gtid_enabled,
                "extra_config": self.source.extra_config,
            },
            "selected_tables": self.selected_tables,
            "table_metadata": {
                name: {
                    "name": meta.name,
                    "estimated_rows": meta.estimated_rows,
                    "data_size_mb": meta.data_size_mb,
                    "change_rate_per_hour": meta.change_rate_per_hour,
                    "has_primary_key": meta.has_primary_key,
                    "primary_key_columns": meta.primary_key_columns,
                    "has_pii": meta.has_pii,
                    "pii_columns": meta.pii_columns,
                }
                for name, meta in self.table_metadata.items()
            },
            "snapshot_mode": self.snapshot_mode.value,
            "destination": {
                "destination_type": self.destination.destination_type.value,
                "connection_id": self.destination.connection_id,
                "merge_strategy": self.destination.merge_strategy,
                "delete_handling": self.destination.delete_handling.value,
                "bucket": self.destination.bucket,
                "path_template": self.destination.path_template,
                "file_format": self.destination.file_format,
                "partition_by_event_time": self.destination.partition_by_event_time,
                "extra_config": self.destination.extra_config,
            },
            "topic_config": {
                "strategy": self.topic_config.strategy.value,
                "topic_prefix": self.topic_config.topic_prefix,
                "unified_topic_name": self.topic_config.unified_topic_name,
                "default_partitions": self.topic_config.default_partitions,
                "default_replication_factor": self.topic_config.default_replication_factor,
                "hybrid_thresholds": self.topic_config.hybrid_thresholds,
                "schema_registry_url": self.topic_config.schema_registry_url,
                "subject_name_strategy": self.topic_config.subject_name_strategy,
            },
            "safety_policy": asdict(self.safety_policy),
            "connector_name": self.connector_name,
            "status": self.status,
            "debezium_config_overrides": self.debezium_config_overrides,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CDCPipelineConfig":
        """Create instance from dictionary"""
        # Parse source
        source_data = data["source"]
        source = SourceConfig(
            source_type=SourceType(source_data["source_type"]),
            connection_id=source_data["connection_id"],
            host=source_data["host"],
            port=source_data["port"],
            username=source_data["username"],
            password=source_data.get("password", ""),
            database=source_data["database"],
            publication_name=source_data.get("publication_name"),
            slot_name=source_data.get("slot_name"),
            server_id=source_data.get("server_id"),
            gtid_enabled=source_data.get("gtid_enabled", False),
            extra_config=source_data.get("extra_config", {}),
        )
        
        # Parse destination
        dest_data = data["destination"]
        destination = DestinationConfig(
            destination_type=DestinationType(dest_data["destination_type"]),
            connection_id=dest_data["connection_id"],
            merge_strategy=dest_data.get("merge_strategy"),
            delete_handling=DeleteHandlingMode(dest_data.get("delete_handling", "propagate")),
            bucket=dest_data.get("bucket"),
            path_template=dest_data.get("path_template", "{tenant_id}/{source_id}/{schema}/{table}/date={date}"),
            file_format=dest_data.get("file_format", "parquet"),
            partition_by_event_time=dest_data.get("partition_by_event_time", True),
            extra_config=dest_data.get("extra_config", {}),
        )
        
        # Parse topic config
        topic_data = data.get("topic_config", {})
        topic_config = TopicConfig(
            strategy=TopicStrategy(topic_data.get("strategy", "hybrid")),
            topic_prefix=topic_data.get("topic_prefix", ""),
            unified_topic_name=topic_data.get("unified_topic_name"),
            default_partitions=topic_data.get("default_partitions", 1),
            default_replication_factor=topic_data.get("default_replication_factor", 3),
            hybrid_thresholds=topic_data.get("hybrid_thresholds", {}),
            schema_registry_url=topic_data.get("schema_registry_url", "http://schema-registry:8080"),
            subject_name_strategy=topic_data.get("subject_name_strategy", "TopicNameStrategy"),
        )
        
        # Parse table metadata
        table_metadata = {}
        for name, meta_data in data.get("table_metadata", {}).items():
            table_metadata[name] = TableMetadata(
                name=meta_data["name"],
                estimated_rows=meta_data.get("estimated_rows", 0),
                data_size_mb=meta_data.get("data_size_mb", 0.0),
                change_rate_per_hour=meta_data.get("change_rate_per_hour", 0),
                has_primary_key=meta_data.get("has_primary_key", False),
                primary_key_columns=meta_data.get("primary_key_columns", []),
                has_pii=meta_data.get("has_pii", False),
                pii_columns=meta_data.get("pii_columns", []),
            )
        
        # Parse safety policy
        safety_data = data.get("safety_policy", {})
        safety_policy = CDCSafetyPolicy(**safety_data) if safety_data else CDCSafetyPolicy()
        
        return cls(
            pipeline_id=data["pipeline_id"],
            pipeline_name=data["pipeline_name"],
            tenant_id=data["tenant_id"],
            created_by=data["created_by"],
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            source=source,
            selected_tables=data.get("selected_tables", []),
            table_metadata=table_metadata,
            snapshot_mode=SnapshotMode(data.get("snapshot_mode", "initial")),
            destination=destination,
            topic_config=topic_config,
            safety_policy=safety_policy,
            connector_name=data.get("connector_name"),
            status=data.get("status", "pending"),
            debezium_config_overrides=data.get("debezium_config_overrides", {}),
        )
    
    def to_json(self) -> str:
        """Serialize to JSON string"""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_json(cls, json_str: str) -> "CDCPipelineConfig":
        """Deserialize from JSON string"""
        return cls.from_dict(json.loads(json_str))

