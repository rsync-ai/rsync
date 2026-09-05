#!/usr/bin/env python3
"""
Generic MCP Avro Schema Generator.

Automatically generates Avro schemas for any MCP connector based on its metadata.json.
This ensures all MCP connectors use consistent Avro serialization.

Usage:
    from schema_generator import MCPSchemaGenerator
    
    generator = MCPSchemaGenerator()
    schema = generator.generate_schema("mysql", "users")
    
    # Or generate for all connectors
    generator.generate_all_schemas()
"""

import os
import json
import re
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

# Base namespace for all MCP schemas
MCP_NAMESPACE = "com.rsync.mcp"

# Avro type mappings from common data types
TYPE_MAPPINGS = {
    # SQL types
    "varchar": "string",
    "char": "string",
    "text": "string",
    "longtext": "string",
    "mediumtext": "string",
    "tinytext": "string",
    "string": "string",
    "uuid": "string",
    "json": "string",
    "jsonb": "string",
    "xml": "string",
    
    # Numeric types
    "int": "int",
    "integer": "int",
    "smallint": "int",
    "tinyint": "int",
    "mediumint": "int",
    "bigint": "long",
    "long": "long",
    "serial": "long",
    "bigserial": "long",
    
    # Floating point
    "float": "float",
    "real": "float",
    "double": "double",
    "double precision": "double",
    "decimal": "double",
    "numeric": "double",
    "money": "double",
    
    # Boolean
    "bool": "boolean",
    "boolean": "boolean",
    "bit": "boolean",
    
    # Binary
    "blob": "bytes",
    "binary": "bytes",
    "varbinary": "bytes",
    "bytea": "bytes",
    "bytes": "bytes",
    
    # Date/Time - stored as longs with logical types
    "date": {"type": "int", "logicalType": "date"},
    "time": {"type": "long", "logicalType": "time-micros"},
    "datetime": {"type": "long", "logicalType": "timestamp-millis"},
    "timestamp": {"type": "long", "logicalType": "timestamp-millis"},
    "timestamptz": {"type": "long", "logicalType": "timestamp-millis"},
    
    # Arrays
    "array": {"type": "array", "items": "string"},
    
    # Default fallback
    "unknown": "string",
}

# Connector categories for specialized handling
CONNECTOR_CATEGORIES = {
    "databases": ["mysql", "postgresql", "postgres", "mongodb", "redis", "oracle",
                  "sqlserver", "sqlite", "mariadb", "cassandra", "dynamodb", "cockroachdb",
                  "clickhouse", "timescaledb", "influxdb", "neo4j", "elasticsearch", "db2"],
    "cloud_storage": ["aws-s3", "s3", "gcs", "azure_blob", "minio"],
    "data_warehouses": ["snowflake", "bigquery", "redshift", "databricks"],
    "streaming": ["kafka", "kinesis", "pubsub", "eventhub"],
    "apis": ["stripe", "hubspot", "salesforce", "slack", "shopify", "zendesk",
             "jira", "github", "gitlab", "notion", "freshdesk", "zoho-crm", "pipedrive"],
    "files": ["json", "csv", "parquet", "avro"],
}


class MCPSchemaGenerator:
    """Generates Avro schemas for MCP connectors."""
    
    def __init__(self, connectors_dir: str = None, output_dir: str = None):
        """
        Initialize the schema generator.
        
        Args:
            connectors_dir: Path to MCP connectors directory
            output_dir: Path to output generated schemas
        """
        self.base_dir = Path(__file__).parent.parent.parent
        self.connectors_dir = Path(connectors_dir) if connectors_dir else self.base_dir / "mcp-connectors"
        self.output_dir = Path(output_dir) if output_dir else self.base_dir / "avro-schemas" / "generated"
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def get_connector_category(self, connector_name: str) -> str:
        """Get the category for a connector."""
        connector_lower = connector_name.lower()
        for category, connectors in CONNECTOR_CATEGORIES.items():
            if connector_lower in connectors:
                return category.upper()
        return "API"  # Default category
    
    def map_type(self, data_type: str) -> Any:
        """Map a data type to Avro type."""
        if not data_type:
            return "string"
        
        data_type_lower = data_type.lower().strip()
        
        # Check for array types
        if data_type_lower.startswith("array<") or data_type_lower.endswith("[]"):
            inner_type = re.sub(r'array<|>|\[\]', '', data_type_lower)
            return {"type": "array", "items": self.map_type(inner_type)}
        
        # Check for nullable types (e.g., "string?")
        is_nullable = data_type_lower.endswith("?")
        if is_nullable:
            data_type_lower = data_type_lower[:-1]
        
        # Get base type
        base_type = TYPE_MAPPINGS.get(data_type_lower, "string")
        
        if is_nullable:
            return ["null", base_type]
        
        return base_type
    
    def generate_entity_schema(
        self,
        connector_name: str,
        entity_name: str,
        fields: List[Dict[str, Any]] = None,
        namespace: str = None
    ) -> Dict[str, Any]:
        """
        Generate an Avro schema for an entity (table/collection).
        
        Args:
            connector_name: MCP connector name
            entity_name: Entity name (table, collection, etc.)
            fields: List of field definitions [{"name": "id", "type": "int", "pk": true}]
            namespace: Optional namespace/schema
            
        Returns:
            Avro schema dictionary
        """
        # Build record name
        safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', entity_name)
        record_name = f"{safe_name.title().replace('_', '')}Record"
        
        # Build namespace
        full_namespace = f"{MCP_NAMESPACE}.{connector_name}"
        if namespace:
            full_namespace = f"{full_namespace}.{namespace}"
        
        # Generate fields
        avro_fields = [
            # Always include metadata fields
            {"name": "_rsync_trace_id", "type": ["null", "string"], "default": None, "doc": "RSync trace ID"},
            {"name": "_rsync_timestamp", "type": "long", "logicalType": "timestamp-millis", "doc": "Processing timestamp"},
            {"name": "_rsync_operation", "type": ["null", "string"], "default": None, "doc": "Operation type (INSERT/UPDATE/DELETE)"},
        ]
        
        # Add entity fields
        if fields:
            for field in fields:
                field_name = field.get("name", "unknown")
                field_type = field.get("type", "string")
                is_nullable = field.get("nullable", True) and not field.get("pk", False)
                doc = field.get("description", "")
                
                avro_type = self.map_type(field_type)
                
                # Make nullable if needed
                if is_nullable and not isinstance(avro_type, list):
                    avro_type = ["null", avro_type]
                
                avro_field = {
                    "name": field_name,
                    "type": avro_type,
                }
                
                # Add default for nullable
                if is_nullable:
                    avro_field["default"] = None
                
                if doc:
                    avro_field["doc"] = doc
                
                avro_fields.append(avro_field)
        else:
            # No fields defined - use generic data map
            avro_fields.append({
                "name": "data",
                "type": {"type": "map", "values": ["null", "string", "long", "double", "boolean"]},
                "doc": "Record data fields"
            })
        
        return {
            "namespace": full_namespace,
            "type": "record",
            "name": record_name,
            "doc": f"Avro schema for {connector_name}.{entity_name}",
            "fields": avro_fields
        }
    
    def generate_connector_envelope_schema(self, connector_name: str) -> Dict[str, Any]:
        """
        Generate the envelope schema for a connector's messages.
        
        This wraps individual records with metadata for CDC/batch operations.
        """
        category = self.get_connector_category(connector_name)
        
        return {
            "namespace": f"{MCP_NAMESPACE}.{connector_name}",
            "type": "record",
            "name": "MessageEnvelope",
            "doc": f"Envelope for {connector_name} messages",
            "fields": [
                {"name": "trace_id", "type": "string", "doc": "Distributed trace ID"},
                {"name": "pipeline_id", "type": "string", "doc": "Pipeline identifier"},
                {"name": "connection_id", "type": "string", "doc": "Connection identifier"},
                {"name": "connector_type", "type": "string", "default": connector_name},
                {"name": "connector_category", "type": {
                    "type": "enum",
                    "name": "ConnectorCategory",
                    "symbols": ["DATABASE", "CLOUD_STORAGE", "DATA_WAREHOUSE", "STREAMING", "API", "FILE", "CDC"]
                }, "default": category},
                {"name": "entity", "type": {
                    "type": "record",
                    "name": "EntityRef",
                    "fields": [
                        {"name": "namespace", "type": ["null", "string"], "default": None},
                        {"name": "name", "type": "string"},
                        {"name": "sub_entity", "type": ["null", "string"], "default": None}
                    ]
                }},
                {"name": "operation", "type": {
                    "type": "enum",
                    "name": "Operation",
                    "symbols": ["INSERT", "UPDATE", "DELETE", "UPSERT", "SNAPSHOT", "READ", "WRITE"]
                }},
                {"name": "partition_key", "type": "string"},
                {"name": "key", "type": ["null", {"type": "map", "values": "string"}], "default": None},
                {"name": "before", "type": ["null", "bytes"], "default": None, "doc": "Serialized record before change"},
                {"name": "after", "type": ["null", "bytes"], "default": None, "doc": "Serialized record after change"},
                {"name": "source_ts", "type": "long", "logicalType": "timestamp-millis"},
                {"name": "processed_ts", "type": "long", "logicalType": "timestamp-millis"},
                {"name": "batch_info", "type": ["null", {
                    "type": "record",
                    "name": "BatchInfo",
                    "fields": [
                        {"name": "batch_id", "type": "string"},
                        {"name": "batch_index", "type": "int"},
                        {"name": "batch_total", "type": ["null", "int"], "default": None},
                        {"name": "is_first", "type": "boolean", "default": False},
                        {"name": "is_last", "type": "boolean", "default": False}
                    ]
                }], "default": None},
                {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": None}
            ]
        }
    
    def generate_from_metadata(self, connector_name: str) -> Dict[str, Dict[str, Any]]:
        """
        Generate schemas for a connector based on its metadata.json.
        
        Returns:
            Dictionary of schema_name -> schema
        """
        metadata_path = self.connectors_dir / connector_name / "metadata.json"
        
        if not metadata_path.exists():
            return {}
        
        try:
            with open(metadata_path) as f:
                metadata = json.load(f)
        except Exception as e:
            print(f"Error loading metadata for {connector_name}: {e}")
            return {}
        
        schemas = {}
        
        # Generate envelope schema
        envelope = self.generate_connector_envelope_schema(connector_name)
        schemas[f"{connector_name}.MessageEnvelope"] = envelope
        
        # Generate schemas for known entities if available
        entities = metadata.get("entities", metadata.get("tables", []))
        for entity in entities:
            if isinstance(entity, str):
                entity_schema = self.generate_entity_schema(connector_name, entity)
            elif isinstance(entity, dict):
                entity_schema = self.generate_entity_schema(
                    connector_name,
                    entity.get("name", "unknown"),
                    entity.get("fields", entity.get("columns", [])),
                    entity.get("namespace", entity.get("schema"))
                )
            else:
                continue
            
            schema_name = f"{connector_name}.{entity_schema['name']}"
            schemas[schema_name] = entity_schema
        
        return schemas
    
    def generate_all_schemas(self) -> Dict[str, Dict[str, Any]]:
        """
        Generate schemas for all MCP connectors.
        
        Returns:
            Dictionary of all generated schemas
        """
        all_schemas = {}
        
        if not self.connectors_dir.exists():
            print(f"Connectors directory not found: {self.connectors_dir}")
            return all_schemas
        
        for connector_dir in self.connectors_dir.iterdir():
            if not connector_dir.is_dir():
                continue
            if connector_dir.name.startswith(('.', '_')):
                continue
            
            connector_name = connector_dir.name
            schemas = self.generate_from_metadata(connector_name)
            
            if schemas:
                all_schemas.update(schemas)
                
                # Save to output directory
                connector_output = self.output_dir / connector_name
                connector_output.mkdir(parents=True, exist_ok=True)
                
                for schema_name, schema in schemas.items():
                    safe_name = schema_name.replace('.', '_')
                    output_file = connector_output / f"{safe_name}.avsc"
                    with open(output_file, 'w') as f:
                        json.dump(schema, f, indent=2)
                
                print(f"✓ Generated {len(schemas)} schemas for {connector_name}")
        
        # Generate index file
        self._generate_index(all_schemas)
        
        return all_schemas
    
    def _generate_index(self, schemas: Dict[str, Dict[str, Any]]):
        """Generate an index file of all schemas."""
        index = {
            "generated_at": datetime.now().isoformat(),
            "total_schemas": len(schemas),
            "schemas": list(schemas.keys()),
            "connectors": {}
        }
        
        for schema_name in schemas:
            parts = schema_name.split(".")
            if len(parts) >= 2:
                connector = parts[0]
                if connector not in index["connectors"]:
                    index["connectors"][connector] = []
                index["connectors"][connector].append(schema_name)
        
        with open(self.output_dir / "index.json", 'w') as f:
            json.dump(index, f, indent=2)
    
    def get_schema_registry_subjects(self, topic: str, connector_name: str) -> Dict[str, str]:
        """
        Get Schema Registry subject names for a connector's schemas.
        
        Uses TopicRecordNameStrategy: {topic}-{namespace}.{recordName}
        
        Returns:
            Dictionary of schema_name -> subject_name
        """
        schemas = self.generate_from_metadata(connector_name)
        subjects = {}
        
        for schema_name, schema in schemas.items():
            namespace = schema.get("namespace", "")
            record_name = schema.get("name", "")
            
            if namespace and record_name:
                subject = f"{topic}-{namespace}.{record_name}"
            else:
                subject = f"{topic}-{schema_name}"
            
            subjects[schema_name] = subject
        
        return subjects


def main():
    """Generate schemas for all connectors."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate Avro schemas for MCP connectors")
    parser.add_argument("--connectors-dir", help="Path to MCP connectors directory")
    parser.add_argument("--output-dir", help="Path to output schemas")
    parser.add_argument("--connector", help="Generate for specific connector only")
    
    args = parser.parse_args()
    
    generator = MCPSchemaGenerator(
        connectors_dir=args.connectors_dir,
        output_dir=args.output_dir
    )
    
    if args.connector:
        schemas = generator.generate_from_metadata(args.connector)
        print(f"Generated {len(schemas)} schemas for {args.connector}")
        for name, schema in schemas.items():
            print(f"  - {name}")
            print(json.dumps(schema, indent=2))
    else:
        schemas = generator.generate_all_schemas()
        print(f"\nTotal: {len(schemas)} schemas generated")


if __name__ == "__main__":
    main()
