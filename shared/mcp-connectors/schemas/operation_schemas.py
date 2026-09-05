#!/usr/bin/env python3
"""
Operation Schema Registry

Defines standard operations for each connector category.
Used by:
1. LLM to generate correct methods
2. Validator to check method presence
3. Base class to auto-generate handlers

VERSION: 1.0.0
"""

from typing import Dict, List, Any, Optional
from enum import Enum


class ConnectorCategory(str, Enum):
    """Valid connector categories"""
    RELATIONAL_DB = "relational_db"
    DOCUMENT_DB = "document_db"
    CLOUD_STORAGE = "cloud_storage"
    API_SAAS = "api_saas"
    STREAMING = "streaming"
    DATA_WAREHOUSE = "data_warehouse"
    WIDE_COLUMN_DB = "wide_column_db"


# =============================================================================
# OPERATION DEFINITIONS BY CATEGORY
# =============================================================================

OPERATION_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "relational_db": {
        "description": "Relational database connectors (MySQL, PostgreSQL, SQLite, etc.)",
        "required_methods": [
            "test_connection",
            "validate_config",
            "discover_schema",
            "export",
            "get_capabilities"
        ],
        "optional_methods": [
            "import_data",
            "query",
            "execute",
            "get_schema"
        ],
        "operations": {
            "discover_schema": {
                "description": "List all tables and their schemas",
                "params": {
                    "include_row_counts": {"type": "bool", "default": True},
                    "max_tables": {"type": "int", "default": 100}
                },
                "returns": "DiscoverSchemaResponse"
            },
            "export": {
                "description": "Export data from a table",
                "params": {
                    "table": {"type": "str", "required": True},
                    "limit": {"type": "int", "default": 10000},
                    "offset": {"type": "int", "default": 0},
                    "where": {"type": "str", "default": ""},
                    "columns": {"type": "List[str]", "default": []}
                },
                "returns": "ExportResponse"
            },
            "import_data": {
                "description": "Import data into a table",
                "params": {
                    "table": {"type": "str", "required": True},
                    "data": {"type": "List[Dict]", "required": True},
                    "mode": {"type": "str", "default": "append", "enum": ["append", "replace", "upsert"]}
                },
                "returns": "ImportResponse"
            },
            "query": {
                "description": "Execute SQL query",
                "params": {
                    "sql": {"type": "str", "required": True}
                },
                "returns": "ExportResponse"
            }
        },
        "terminology": {
            "entity": "table",
            "entityPlural": "tables",
            "record": "row",
            "recordPlural": "rows",
            "field": "column",
            "fieldPlural": "columns"
        }
    },

    "document_db": {
        "description": "Document database connectors (MongoDB, CouchDB, etc.)",
        "required_methods": [
            "test_connection",
            "validate_config",
            "discover_schema",
            "export",
            "get_capabilities"
        ],
        "optional_methods": [
            "import_data",
            "query",
            "aggregate"
        ],
        "operations": {
            "discover_schema": {
                "description": "List all collections and infer schemas from sample documents",
                "params": {
                    "sample_size": {"type": "int", "default": 20},
                    "max_collections": {"type": "int", "default": 100}
                }
            },
            "export": {
                "description": "Export documents from a collection",
                "params": {
                    "collection": {"type": "str", "required": True},
                    "limit": {"type": "int", "default": 10000},
                    "filter": {"type": "Dict", "default": {}}
                }
            }
        },
        "terminology": {
            "entity": "collection",
            "entityPlural": "collections",
            "record": "document",
            "recordPlural": "documents",
            "field": "field",
            "fieldPlural": "fields"
        }
    },

    "cloud_storage": {
        "description": "Cloud storage connectors (S3, GCS, MinIO, etc.)",
        "required_methods": [
            "test_connection",
            "validate_config",
            "discover_schema",
            "read",
            "get_capabilities"
        ],
        "optional_methods": [
            "write",
            "delete",
            "list_files",
            "import_data"
        ],
        "operations": {
            "discover_schema": {
                "description": "List buckets and infer schemas from files",
                "params": {
                    "max_buckets": {"type": "int", "default": 10},
                    "max_files": {"type": "int", "default": 50}
                }
            },
            "read": {
                "description": "Read file from storage",
                "params": {
                    "bucket": {"type": "str", "required": True},
                    "key": {"type": "str", "required": True}
                }
            },
            "write": {
                "description": "Write file to storage",
                "params": {
                    "bucket": {"type": "str", "required": True},
                    "key": {"type": "str", "required": True},
                    "data": {"type": "Any", "required": True},
                    "content_type": {"type": "str", "default": "application/octet-stream"}
                }
            },
            "import_data": {
                "description": "Import data to storage (alias for write with JSON/CSV)",
                "params": {
                    "bucket": {"type": "str", "required": True},
                    "key": {"type": "str", "required": True},
                    "data": {"type": "List[Dict]", "required": True},
                    "format": {"type": "str", "default": "json", "enum": ["json", "csv", "parquet"]}
                }
            }
        },
        "terminology": {
            "entity": "bucket",
            "entityPlural": "buckets",
            "record": "file",
            "recordPlural": "files",
            "field": "key",
            "fieldPlural": "keys"
        }
    },

    "api_saas": {
        "description": "API/SaaS connectors (HubSpot, Salesforce, Stripe, etc.)",
        "required_methods": [
            "test_connection",
            "validate_config",
            "discover_schema",
            "export",
            "get_capabilities"
        ],
        "optional_methods": [],  # Object-specific methods are defined in API_OBJECTS
        "operations": {
            "discover_schema": {
                "description": "List available API objects/endpoints",
                "params": {}
            },
            "export": {
                "description": "Export data from an object/endpoint",
                "params": {
                    "object": {"type": "str", "required": True},
                    "limit": {"type": "int", "default": 100}
                }
            }
        },
        "object_operations": {
            # Template for object-specific methods (generated per object)
            "list_{object}": {
                "description": "List all {object}",
                "params": {
                    "limit": {"type": "int", "default": 100},
                    "offset": {"type": "int", "default": 0}
                }
            },
            "get_{object}": {
                "description": "Get single {object} by ID",
                "params": {
                    "id": {"type": "str", "required": True}
                }
            },
            "create_{object}": {
                "description": "Create new {object}",
                "params": {
                    "data": {"type": "Dict", "required": True}
                }
            },
            "update_{object}": {
                "description": "Update existing {object}",
                "params": {
                    "id": {"type": "str", "required": True},
                    "data": {"type": "Dict", "required": True}
                }
            },
            "delete_{object}": {
                "description": "Delete {object}",
                "params": {
                    "id": {"type": "str", "required": True}
                }
            }
        },
        "terminology": {
            "entity": "object",
            "entityPlural": "objects",
            "record": "record",
            "recordPlural": "records",
            "field": "property",
            "fieldPlural": "properties"
        }
    },

    "streaming": {
        "description": "Streaming connectors (Kafka, Kinesis, etc.)",
        "required_methods": [
            "test_connection",
            "validate_config",
            "discover_schema",
            "get_capabilities"
        ],
        "optional_methods": [
            "consume",
            "produce",
            "list_topics"
        ],
        "operations": {
            "discover_schema": {
                "description": "List topics and get schemas from registry"
            },
            "consume": {
                "description": "Consume messages from topic",
                "params": {
                    "topic": {"type": "str", "required": True},
                    "limit": {"type": "int", "default": 100},
                    "from_beginning": {"type": "bool", "default": False}
                }
            },
            "produce": {
                "description": "Produce message to topic",
                "params": {
                    "topic": {"type": "str", "required": True},
                    "message": {"type": "Any", "required": True},
                    "key": {"type": "str", "default": None}
                }
            }
        },
        "terminology": {
            "entity": "topic",
            "entityPlural": "topics",
            "record": "message",
            "recordPlural": "messages",
            "field": "field",
            "fieldPlural": "fields"
        }
    },

    "data_warehouse": {
        "description": "Data warehouse connectors (Snowflake, BigQuery, Redshift, etc.)",
        "required_methods": [
            "test_connection",
            "validate_config",
            "discover_schema",
            "query",
            "get_capabilities"
        ],
        "optional_methods": [
            "export",
            "load",
            "merge"
        ],
        "operations": {
            "discover_schema": {
                "description": "List schemas, tables and views",
                "params": {
                    "include_views": {"type": "bool", "default": True},
                    "include_row_counts": {"type": "bool", "default": False}
                }
            },
            "query": {
                "description": "Execute SQL query",
                "params": {
                    "sql": {"type": "str", "required": True},
                    "limit": {"type": "int", "default": 10000}
                }
            },
            "load": {
                "description": "Load data into table",
                "params": {
                    "table": {"type": "str", "required": True},
                    "data": {"type": "List[Dict]", "required": True},
                    "mode": {"type": "str", "default": "append"}
                }
            }
        },
        "terminology": {
            "entity": "table",
            "entityPlural": "tables",
            "record": "row",
            "recordPlural": "rows",
            "field": "column",
            "fieldPlural": "columns"
        }
    },

    "wide_column_db": {
        "description": "Wide column database connectors (Cassandra, ScyllaDB, etc.)",
        "required_methods": [
            "test_connection",
            "validate_config",
            "discover_schema",
            "export",
            "get_capabilities"
        ],
        "optional_methods": [
            "import_data",
            "query"
        ],
        "operations": {
            "discover_schema": {
                "description": "List keyspaces and tables",
                "params": {
                    "keyspace": {"type": "str", "default": None}
                }
            },
            "export": {
                "description": "Export data from table",
                "params": {
                    "keyspace": {"type": "str", "required": True},
                    "table": {"type": "str", "required": True},
                    "limit": {"type": "int", "default": 10000}
                }
            }
        },
        "terminology": {
            "entity": "table",
            "entityPlural": "tables",
            "record": "row",
            "recordPlural": "rows",
            "field": "column",
            "fieldPlural": "columns"
        }
    }
}


# =============================================================================
# API-SPECIFIC OBJECT DEFINITIONS (DEPRECATED)
# =============================================================================
# Use Context7 or OpenAI for object discovery.
API_OBJECTS: Dict[str, Dict[str, Any]] = {}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_category_schema(category: str) -> Dict[str, Any]:
    """Get operation schema for a connector category."""
    return OPERATION_SCHEMAS.get(category, OPERATION_SCHEMAS["relational_db"])


def get_required_methods(category: str) -> List[str]:
    """Get required methods for a connector category."""
    schema = get_category_schema(category)
    return schema.get("required_methods", [])


def get_optional_methods(category: str) -> List[str]:
    """Get optional methods for a connector category."""
    schema = get_category_schema(category)
    return schema.get("optional_methods", [])


def get_terminology(category: str) -> Dict[str, str]:
    """Get terminology mapping for a connector category."""
    schema = get_category_schema(category)
    return schema.get("terminology", {})


def get_api_config(api_name: str) -> Optional[Dict[str, Any]]:
    """Get API configuration for a known API."""
    # Normalize name (e.g., "zoho-crm" -> "zoho_crm")
    normalized = api_name.lower().replace("-", "_")
    return API_OBJECTS.get(normalized)


def get_api_objects(api_name: str) -> List[str]:
    """Get list of objects for a known API."""
    config = get_api_config(api_name)
    if config:
        return list(config.get("objects", {}).keys())
    return []


def get_operations_for_connector(connector_name: str) -> Dict[str, Any]:
    """
    Get all operations that should be generated for a connector.
    
    For known APIs, includes object-specific methods.
    For other connectors, returns category-based operations.
    """
    # Check if it's a known API with objects
    api_config = get_api_config(connector_name)
    
    if api_config:
        category = api_config.get("category", "api_saas")
        base_schema = get_category_schema(category)
        
        operations = {
            "required_methods": base_schema.get("required_methods", []).copy(),
            "object_methods": []
        }
        
        # Generate object-specific methods
        for obj_name, obj_config in api_config.get("objects", {}).items():
            for op in obj_config.get("supports", []):
                # Singularize object name for method (contacts -> contact)
                obj_singular = obj_name.rstrip("s") if obj_name.endswith("s") else obj_name
                method_name = f"{op}_{obj_singular}" if op != "list" else f"list_{obj_name}"
                
                operations["object_methods"].append({
                    "name": method_name,
                    "object": obj_name,
                    "operation": op,
                    "endpoint": obj_config["endpoint"],
                    "id_field": obj_config["id_field"]
                })
        
        return operations
    
    # Infer category from connector name
    name_lower = connector_name.lower()
    
    if any(kw in name_lower for kw in ["mongo", "couch", "firestore"]):
        return get_category_schema("document_db")
    elif any(kw in name_lower for kw in ["s3", "gcs", "minio", "azure-blob"]):
        return get_category_schema("cloud_storage")
    elif any(kw in name_lower for kw in ["kafka", "kinesis", "pubsub"]):
        return get_category_schema("streaming")
    elif any(kw in name_lower for kw in ["snowflake", "bigquery", "redshift"]):
        return get_category_schema("data_warehouse")
    elif any(kw in name_lower for kw in ["cassandra", "scylla"]):
        return get_category_schema("wide_column_db")
    elif any(kw in name_lower for kw in ["mysql", "postgres", "sqlite", "sqlserver", "oracle", "db2"]):
        return get_category_schema("relational_db")
    else:
        # Default to API/SaaS for unknown connectors
        return get_category_schema("api_saas")


def generate_method_signature(method_name: str, category: str) -> str:
    """
    Generate Python method signature for a given operation.
    
    Used by LLM and code generators.
    """
    schema = get_category_schema(category)
    operations = schema.get("operations", {})
    
    if method_name in operations:
        op = operations[method_name]
        params = op.get("params", {})
        
        param_strs = ["self", "params: Dict = None"]
        signature = f"def {method_name}({', '.join(param_strs)}) -> Dict[str, Any]:"
        
        docstring = f'"""{op.get("description", method_name)}"""'
        
        return f"{signature}\n        {docstring}"
    
    return f"def {method_name}(self, params: Dict = None) -> Dict[str, Any]:"


# =============================================================================
# EXPORT FOR LLM PROMPTS
# =============================================================================

def get_schema_prompt_for_category(category: str) -> str:
    """
    Generate a prompt fragment describing required operations for a category.
    
    Used in LLM prompts for connector generation.
    """
    schema = get_category_schema(category)
    
    lines = [
        f"## Connector Category: {category}",
        f"Description: {schema.get('description', '')}",
        "",
        "### Required Methods:",
    ]
    
    for method in schema.get("required_methods", []):
        op_info = schema.get("operations", {}).get(method, {})
        desc = op_info.get("description", method)
        lines.append(f"- {method}(): {desc}")
    
    lines.append("")
    lines.append("### Optional Methods:")
    
    for method in schema.get("optional_methods", []):
        op_info = schema.get("operations", {}).get(method, {})
        desc = op_info.get("description", method)
        lines.append(f"- {method}(): {desc}")
    
    lines.append("")
    lines.append("### Terminology:")
    terminology = schema.get("terminology", {})
    for key, value in terminology.items():
        lines.append(f"- {key}: {value}")
    
    return "\n".join(lines)


def get_api_prompt_for_connector(api_name: str) -> Optional[str]:
    """
    Generate a prompt fragment for a known API connector.
    
    Used in LLM prompts for API connector generation.
    """
    config = get_api_config(api_name)
    if not config:
        return None
    
    lines = [
        f"## API: {api_name}",
        f"Base URL: {config.get('base_url', 'unknown')}",
        f"Auth Type: {config.get('auth_type', 'bearer')}",
        "",
        "### Objects and Operations:",
    ]
    
    for obj_name, obj_config in config.get("objects", {}).items():
        supports = obj_config.get("supports", [])
        lines.append(f"- {obj_name}: {', '.join(supports)}")
        lines.append(f"  Endpoint: {obj_config.get('endpoint', '')}")
    
    return "\n".join(lines)
