"""
Avro Serialization Utilities for RSync Platform.

This module provides Avro serialization/deserialization with Confluent Schema Registry
integration. It supports all MCP connector message types and ensures schema evolution
compatibility.

Usage:
    from src.utils.avro_serializer import AvroSerializer, get_serializer

    serializer = get_serializer()
    
    # Serialize
    data = {"trace_id": "abc123", "pipeline_id": "pipe1", ...}
    encoded = serializer.serialize("topic", "com.rsync.pipeline.PipelineRequest", data)
    
    # Deserialize
    decoded = serializer.deserialize(encoded)
"""

import os

from .kafka_security import schema_registry_auth
import json
import struct
import logging
from typing import Dict, Any, Optional, Union, Tuple
from functools import lru_cache
import requests
from io import BytesIO

logger = logging.getLogger(__name__)

# Try to import fastavro for binary encoding, fallback to JSON-in-wire-format
try:
    import fastavro
    from fastavro.schema import parse_schema
    FASTAVRO_AVAILABLE = True
except ImportError:
    FASTAVRO_AVAILABLE = False
    logger.warning("fastavro not installed (binary Avro disabled)")


class SchemaRegistryClient:
    """Client for Confluent Schema Registry."""
    
    def __init__(self, url: str, auth: Optional[Tuple[str, str]] = None):
        self.url = url.rstrip('/')
        self.auth = auth
        self.schema_cache: Dict[int, Dict] = {}  # id -> schema
        self.subject_cache: Dict[str, int] = {}  # subject -> latest id
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/vnd.schemaregistry.v1+json',
            'Accept': 'application/vnd.schemaregistry.v1+json',
        })
        if auth:
            self.session.auth = auth
    
    def get_schema_by_id(self, schema_id: int) -> Dict:
        """Get schema by global ID."""
        if schema_id in self.schema_cache:
            return self.schema_cache[schema_id]
        
        try:
            resp = self.session.get(f"{self.url}/schemas/ids/{schema_id}")
            resp.raise_for_status()
            data = resp.json()
            schema = json.loads(data['schema'])
            self.schema_cache[schema_id] = schema
            return schema
        except Exception as e:
            logger.error(f"Failed to get schema ID {schema_id}: {e}")
            raise
    
    def register_schema(self, subject: str, schema: Dict) -> int:
        """Register a schema and return its ID."""
        cache_key = f"{subject}:{json.dumps(schema, sort_keys=True)}"
        if cache_key in self.subject_cache:
            return self.subject_cache[cache_key]
        
        try:
            resp = self.session.post(
                f"{self.url}/subjects/{subject}/versions",
                json={"schema": json.dumps(schema), "schemaType": "AVRO"}
            )
            resp.raise_for_status()
            schema_id = resp.json()['id']
            self.subject_cache[cache_key] = schema_id
            self.schema_cache[schema_id] = schema
            logger.info(f"Registered schema '{subject}' with ID: {schema_id}")
            return schema_id
        except Exception as e:
            logger.error(f"Failed to register schema '{subject}': {e}")
            raise
    
    def get_latest_schema(self, subject: str) -> Optional[Tuple[int, Dict]]:
        """Get latest schema version for a subject."""
        try:
            resp = self.session.get(f"{self.url}/subjects/{subject}/versions/latest")
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            schema_id = data['id']
            schema = json.loads(data['schema'])
            self.schema_cache[schema_id] = schema
            return schema_id, schema
        except Exception as e:
            logger.error(f"Failed to get latest schema for '{subject}': {e}")
            raise
    
    def check_compatibility(self, subject: str, schema: Dict) -> bool:
        """Check if schema is compatible with latest version."""
        try:
            resp = self.session.post(
                f"{self.url}/compatibility/subjects/{subject}/versions/latest",
                json={"schema": json.dumps(schema), "schemaType": "AVRO"}
            )
            if resp.status_code == 404:
                return True  # No existing schema
            resp.raise_for_status()
            return resp.json().get('is_compatible', True)
        except Exception as e:
            logger.warning(f"Compatibility check failed for '{subject}': {e}")
            return True  # Assume compatible on error


class AvroSerializer:
    """
    Avro serializer with Schema Registry integration.
    
    Supports Confluent wire format: [0][4-byte schema ID][payload]
    """
    
    # Built-in schemas
    SCHEMAS = {
        "com.rsync.pipeline.PipelineRequest": {
            "namespace": "com.rsync.pipeline",
            "type": "record",
            "name": "PipelineRequest",
            "fields": [
                {"name": "trace_id", "type": "string"},
                {"name": "pipeline_id", "type": "string"},
                {"name": "pipeline_name", "type": ["null", "string"], "default": None},
                {"name": "source", "type": {
                    "type": "record",
                    "name": "ConnectionRef",
                    "fields": [
                        {"name": "connection_id", "type": "string"},
                        {"name": "connector_type", "type": "string"},
                        {"name": "config", "type": ["null", {"type": "map", "values": "string"}], "default": None}
                    ]
                }},
                {"name": "destination", "type": "ConnectionRef"},
                {"name": "sync_mode", "type": {"type": "enum", "name": "SyncMode", "symbols": ["FULL", "INCREMENTAL", "CDC", "APPEND", "UPSERT"]}, "default": "FULL"},
                {"name": "entities", "type": ["null", {"type": "array", "items": {
                    "type": "record",
                    "name": "EntitySelection",
                    "fields": [
                        {"name": "name", "type": "string"},
                        {"name": "namespace", "type": ["null", "string"], "default": None},
                        {"name": "filter", "type": ["null", "string"], "default": None},
                        {"name": "columns", "type": ["null", {"type": "array", "items": "string"}], "default": None}
                    ]
                }}], "default": None},
                {"name": "pii_handling", "type": ["null", {
                    "type": "record",
                    "name": "PIIConfig",
                    "fields": [
                        {"name": "mode", "type": {"type": "enum", "name": "PIIMode", "symbols": ["NONE", "DETECT", "MASK", "HASH", "ENCRYPT", "REDACT"]}, "default": "NONE"},
                        {"name": "fields", "type": ["null", {"type": "array", "items": "string"}], "default": None}
                    ]
                }], "default": None},
                {"name": "schedule", "type": ["null", {
                    "type": "record",
                    "name": "Schedule",
                    "fields": [
                        {"name": "cron", "type": ["null", "string"], "default": None},
                        {"name": "interval_minutes", "type": ["null", "int"], "default": None},
                        {"name": "timezone", "type": "string", "default": "UTC"}
                    ]
                }], "default": None},
                {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": None},
                {"name": "created_at", "type": "long"},
                {"name": "user_id", "type": ["null", "string"], "default": None}
            ]
        },
        "com.rsync.agent.AgentMessage": {
            "namespace": "com.rsync.agent",
            "type": "record",
            "name": "AgentMessage",
            "fields": [
                {"name": "trace_id", "type": "string"},
                {"name": "message_id", "type": "string"},
                {"name": "correlation_id", "type": ["null", "string"], "default": None},
                {"name": "source_agent", "type": "string"},
                {"name": "target_agent", "type": ["null", "string"], "default": None},
                {"name": "message_type", "type": {"type": "enum", "name": "MessageType", "symbols": ["REQUEST", "RESPONSE", "EVENT", "ERROR", "HEARTBEAT", "COMMAND"]}},
                {"name": "action", "type": "string"},
                {"name": "payload", "type": ["null", "bytes"], "default": None},
                {"name": "payload_schema", "type": ["null", "string"], "default": None},
                {"name": "status", "type": ["null", {"type": "enum", "name": "Status", "symbols": ["PENDING", "PROCESSING", "SUCCESS", "FAILED", "TIMEOUT", "CANCELLED"]}], "default": None},
                {"name": "error", "type": ["null", {
                    "type": "record",
                    "name": "ErrorInfo",
                    "fields": [
                        {"name": "code", "type": "string"},
                        {"name": "message", "type": "string"},
                        {"name": "details", "type": ["null", "string"], "default": None},
                        {"name": "retryable", "type": "boolean", "default": False}
                    ]
                }], "default": None},
                {"name": "context", "type": ["null", {"type": "map", "values": "string"}], "default": None},
                {"name": "timestamp", "type": "long"},
                {"name": "expires_at", "type": ["null", "long"], "default": None},
                {"name": "priority", "type": "int", "default": 0},
                {"name": "retry_count", "type": "int", "default": 0}
            ]
        },
        "com.rsync.mcp.MCPMessage": {
            "namespace": "com.rsync.mcp",
            "type": "record",
            "name": "MCPMessage",
            "fields": [
                {"name": "trace_id", "type": "string"},
                {"name": "message_id", "type": "string"},
                {"name": "pipeline_id", "type": "string"},
                {"name": "connection_id", "type": "string"},
                {"name": "connector_type", "type": "string"},
                {"name": "connector_category", "type": {"type": "enum", "name": "ConnectorCategory", "symbols": ["DATABASE", "CLOUD_STORAGE", "DATA_WAREHOUSE", "STREAMING", "API", "FILE", "CDC"]}},
                {"name": "operation", "type": {"type": "enum", "name": "MCPOperation", "symbols": ["EXTRACT", "LOAD", "QUERY", "WRITE", "UPSERT", "DELETE", "SCHEMA_DISCOVERY", "TEST_CONNECTION", "STREAM_START", "STREAM_STOP"]}},
                {"name": "entity", "type": ["null", {
                    "type": "record",
                    "name": "EntityReference",
                    "fields": [
                        {"name": "namespace", "type": ["null", "string"], "default": None},
                        {"name": "name", "type": "string"},
                        {"name": "alias", "type": ["null", "string"], "default": None}
                    ]
                }], "default": None},
                {"name": "partition_key", "type": "string"},
                {"name": "batch_info", "type": ["null", {
                    "type": "record",
                    "name": "BatchInfo",
                    "fields": [
                        {"name": "batch_id", "type": "string"},
                        {"name": "batch_index", "type": "int"},
                        {"name": "batch_total", "type": ["null", "int"], "default": None},
                        {"name": "record_count", "type": "int"},
                        {"name": "is_first", "type": "boolean", "default": False},
                        {"name": "is_last", "type": "boolean", "default": False}
                    ]
                }], "default": None},
                {"name": "records", "type": ["null", "bytes"], "default": None},
                {"name": "record_schema", "type": ["null", "string"], "default": None},
                {"name": "record_count", "type": "int", "default": 0},
                {"name": "config", "type": ["null", {"type": "map", "values": "string"}], "default": None},
                {"name": "filters", "type": ["null", {"type": "array", "items": {
                    "type": "record",
                    "name": "Filter",
                    "fields": [
                        {"name": "field", "type": "string"},
                        {"name": "operator", "type": {"type": "enum", "name": "FilterOperator", "symbols": ["EQ", "NE", "GT", "GTE", "LT", "LTE", "IN", "NOT_IN", "LIKE", "IS_NULL", "IS_NOT_NULL"]}},
                        {"name": "value", "type": ["null", "string"], "default": None}
                    ]
                }}], "default": None},
                {"name": "timestamp", "type": "long"},
                {"name": "watermark", "type": ["null", "long"], "default": None},
                {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": None}
            ]
        },
        "com.rsync.cdc.ChangeEvent": {
            "namespace": "com.rsync.cdc",
            "type": "record",
            "name": "ChangeEvent",
            "fields": [
                {"name": "trace_id", "type": "string"},
                {"name": "pipeline_id", "type": "string"},
                {"name": "connection_id", "type": "string"},
                {"name": "connector_type", "type": "string"},
                {"name": "entity", "type": {
                    "type": "record",
                    "name": "EntityRef",
                    "fields": [
                        {"name": "namespace", "type": ["null", "string"], "default": None},
                        {"name": "name", "type": "string"},
                        {"name": "sub_entity", "type": ["null", "string"], "default": None}
                    ]
                }},
                {"name": "operation", "type": {"type": "enum", "name": "ChangeOperation", "symbols": ["INSERT", "UPDATE", "DELETE", "UPSERT", "SNAPSHOT", "TRUNCATE", "DDL"]}},
                {"name": "key", "type": ["null", {"type": "map", "values": ["null", "string", "long", "int", "double", "boolean", "bytes"]}], "default": None},
                {"name": "partition_key", "type": "string"},
                {"name": "before", "type": ["null", {"type": "map", "values": ["null", "string", "long", "int", "double", "boolean", "bytes"]}], "default": None},
                {"name": "after", "type": ["null", {"type": "map", "values": ["null", "string", "long", "int", "double", "boolean", "bytes"]}], "default": None},
                {"name": "diff", "type": ["null", {"type": "array", "items": "string"}], "default": None},
                {"name": "source_ts", "type": "long"},
                {"name": "processed_ts", "type": "long"},
                {"name": "sequence", "type": ["null", "long"], "default": None},
                {"name": "is_snapshot", "type": "boolean", "default": False},
                {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": None}
            ]
        }
    }
    
    def __init__(
        self,
        schema_registry_url: str = None,
        auto_register: bool = True,
        use_binary_encoding: bool = True,
        subject_name_strategy: str = "TopicRecordNameStrategy"
    ):
        """
        Initialize Avro serializer.
        
        Args:
            schema_registry_url: Schema Registry URL
            auto_register: Auto-register schemas
            use_binary_encoding: Use binary Avro encoding (requires fastavro)
            subject_name_strategy: Subject naming strategy
        """
        self.schema_registry_url = schema_registry_url or os.getenv(
            "SCHEMA_REGISTRY_URL", "http://schema-registry:8080"
        )
        self.auto_register = auto_register
        self.allow_json_payload = bool(os.getenv("AVRO_ALLOW_JSON_PAYLOAD", "").strip())
        if use_binary_encoding and not FASTAVRO_AVAILABLE and not self.allow_json_payload:
            raise RuntimeError(
                "fastavro is required for binary Avro encoding. "
                "Install fastavro or set AVRO_ALLOW_JSON_PAYLOAD=1 to permit legacy JSON payloads."
            )
        self.use_binary_encoding = use_binary_encoding and FASTAVRO_AVAILABLE
        self.subject_name_strategy = subject_name_strategy
        
        # A secured registry answers an unauthenticated request with 401, which
        # surfaces as a schema-registration failure naming the subject — so the
        # credentials have to arrive here, not be debugged from the symptom.
        self.registry = SchemaRegistryClient(self.schema_registry_url, auth=schema_registry_auth())
        self.parsed_schemas: Dict[str, Any] = {}
        self.schema_id_cache: Dict[str, int] = {}
        
        # Pre-parse built-in schemas
        if self.use_binary_encoding:
            for name, schema in self.SCHEMAS.items():
                try:
                    self.parsed_schemas[name] = parse_schema(schema)
                except Exception as e:
                    logger.warning(f"Failed to parse schema {name}: {e}")
        
        logger.info(
            f"AvroSerializer initialized (binary={self.use_binary_encoding}, "
            f"allow_json_payload={self.allow_json_payload}, registry={self.schema_registry_url})"
        )
    
    def register_schema(self, name: str, schema: Dict):
        """Register a custom schema."""
        self.SCHEMAS[name] = schema
        if self.use_binary_encoding:
            try:
                self.parsed_schemas[name] = parse_schema(schema)
            except Exception as e:
                logger.warning(f"Failed to parse schema {name}: {e}")
    
    def get_subject_name(self, topic: str, schema_name: str, is_key: bool = False) -> str:
        """Get subject name based on strategy."""
        suffix = "-key" if is_key else "-value"
        
        if self.subject_name_strategy == "TopicNameStrategy":
            return f"{topic}{suffix}"
        elif self.subject_name_strategy == "RecordNameStrategy":
            return schema_name
        else:  # TopicRecordNameStrategy
            return f"{topic}-{schema_name}"
    
    def serialize(
        self,
        topic: str,
        schema_name: str,
        data: Dict[str, Any],
        is_key: bool = False
    ) -> bytes:
        """
        Serialize data to Avro wire format.
        
        Wire format: [magic byte 0][4-byte schema ID big-endian][payload]
        
        Args:
            topic: Kafka topic
            schema_name: Full schema name (e.g., com.rsync.pipeline.PipelineRequest)
            data: Data to serialize
            is_key: Whether this is a key (default: value)
            
        Returns:
            Serialized bytes in Confluent wire format
        """
        schema = self.SCHEMAS.get(schema_name)
        if not schema:
            raise ValueError(f"Unknown schema: {schema_name}")
        
        # Get or register schema ID
        subject = self.get_subject_name(topic, schema_name, is_key)
        cache_key = f"{subject}:{schema_name}"
        
        if cache_key in self.schema_id_cache:
            schema_id = self.schema_id_cache[cache_key]
        elif self.auto_register:
            try:
                schema_id = self.registry.register_schema(subject, schema)
                self.schema_id_cache[cache_key] = schema_id
            except Exception as e:
                if self.allow_json_payload:
                    logger.warning(f"Schema registration failed, using legacy JSON payload fallback: {e}")
                    schema_id = 0  # Legacy fallback ID (discouraged)
                else:
                    raise
        else:
            result = self.registry.get_latest_schema(subject)
            if result:
                schema_id, _ = result
                self.schema_id_cache[cache_key] = schema_id
            else:
                if self.allow_json_payload:
                    schema_id = 0
                else:
                    raise ValueError(f"Schema not found in registry for subject '{subject}'")
        
        # Serialize payload
        if self.use_binary_encoding and schema_name in self.parsed_schemas:
            # Binary Avro encoding
            buffer = BytesIO()
            fastavro.schemaless_writer(buffer, self.parsed_schemas[schema_name], data)
            payload = buffer.getvalue()
        else:
            if self.allow_json_payload:
                # Legacy JSON encoding fallback (NOT Avro)
                payload = json.dumps(data).encode('utf-8')
            else:
                raise RuntimeError(
                    "Binary Avro encoding unavailable for this schema/runtime. "
                    "Install fastavro / ensure schema is parsed, or set AVRO_ALLOW_JSON_PAYLOAD=1 to permit legacy JSON payloads."
                )
        
        # Build wire format
        # Magic byte (0) + 4-byte schema ID (big-endian) + payload
        result = struct.pack('>bI', 0, schema_id) + payload
        
        return result
    
    def deserialize(self, data: bytes, schema_name: str = None) -> Dict[str, Any]:
        """
        Deserialize Avro wire format data.
        
        Args:
            data: Serialized bytes
            schema_name: Optional schema name hint
            
        Returns:
            Deserialized data dictionary
        """
        if len(data) < 5:
            raise ValueError("Invalid Avro data: too short")
        
        # Parse wire format
        magic, schema_id = struct.unpack('>bI', data[:5])
        
        if magic != 0:
            # Not Avro wire format: fail-closed.
            # We intentionally do NOT fall back to parsing arbitrary JSON here, because this
            # serializer is used for Confluent wire format messages and tests expect strictness.
            raise ValueError(f"Invalid magic byte: {magic}")
        
        payload = data[5:]
        
        # Get schema
        if schema_id > 0:
            try:
                schema = self.registry.get_schema_by_id(schema_id)
            except Exception as e:
                logger.warning(f"Failed to get schema {schema_id}, using hint: {e}")
                schema = self.SCHEMAS.get(schema_name) if schema_name else None
        else:
            schema = self.SCHEMAS.get(schema_name) if schema_name else None
        
        # Deserialize payload
        if self.use_binary_encoding and schema:
            try:
                parsed = parse_schema(schema) if schema not in self.parsed_schemas.values() else None
                if not parsed:
                    for name, ps in self.parsed_schemas.items():
                        if self.SCHEMAS.get(name) == schema:
                            parsed = ps
                            break
                
                if parsed:
                    buffer = BytesIO(payload)
                    return fastavro.schemaless_reader(buffer, parsed)
            except Exception as e:
                if self.allow_json_payload:
                    logger.warning(f"Binary deserialization failed, trying legacy JSON payload: {e}")
                else:
                    raise
        
        if self.allow_json_payload:
            # Legacy fallback to JSON
            try:
                return json.loads(payload.decode('utf-8'))
            except Exception as e:
                raise ValueError("Failed to deserialize payload") from e

        raise ValueError("Binary Avro deserialization unavailable and AVRO_ALLOW_JSON_PAYLOAD is not set")
    
    def is_avro_format(self, data: bytes) -> bool:
        """Check if data is in Avro wire format."""
        return len(data) >= 5 and data[0] == 0


# Singleton instance
_serializer: Optional[AvroSerializer] = None


def get_serializer() -> AvroSerializer:
    """Get singleton AvroSerializer instance."""
    global _serializer
    if _serializer is None:
        _serializer = AvroSerializer()
    return _serializer


def serialize_pipeline_request(topic: str, data: Dict[str, Any]) -> bytes:
    """Convenience function to serialize a pipeline request."""
    return get_serializer().serialize(topic, "com.rsync.pipeline.PipelineRequest", data)


def serialize_agent_message(topic: str, data: Dict[str, Any]) -> bytes:
    """Convenience function to serialize an agent message."""
    return get_serializer().serialize(topic, "com.rsync.agent.AgentMessage", data)


def serialize_mcp_message(topic: str, data: Dict[str, Any]) -> bytes:
    """Convenience function to serialize an MCP message."""
    return get_serializer().serialize(topic, "com.rsync.mcp.MCPMessage", data)


def serialize_cdc_event(topic: str, data: Dict[str, Any]) -> bytes:
    """Convenience function to serialize a CDC event."""
    return get_serializer().serialize(topic, "com.rsync.cdc.ChangeEvent", data)


def deserialize_message(data: bytes, schema_hint: str = None) -> Dict[str, Any]:
    """Convenience function to deserialize any message."""
    return get_serializer().deserialize(data, schema_hint)
