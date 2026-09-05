# MCP Connector Parameter Normalization

## Overview

MCP Connectors use a **generic parameter normalization system** that ensures any terminology used by plan generators (agents) works correctly with any connector type.

## The Problem We Solved

Different components use different terminology:

| Component | Uses | Example |
|-----------|------|---------|
| Plan generators | `tables: ["users"]` | Array of table names |
| Relational DBs | `table: "users"` | Singular table name |
| Document DBs | `collection: "users"` | Collection name |
| Cloud Storage | `bucket: "data"` | Bucket/prefix |

Without normalization, plans using `tables` would fail when connectors expect `table`.

## The Solution: Centralized Normalization

### 1. Standalone Function (for any connector)

```python
from base_connector import normalize_params_for_connector

# In your export/import methods:
params = normalize_params_for_connector(params, "relational_db")
table = params.get('table')  # Now always works!
```

### 2. Automatic Normalization (BaseMCPConnector)

If your connector inherits from `BaseMCPConnector`, params are normalized automatically in `handle_request()`.

### 3. Go Executor Normalization

The Go executor also normalizes params before calling MCP connectors:

```go
resolved = a.normalizeParamsForMCP(resolved)
```

## Normalization Rules

### Rule 1: Generic → Specific
```
entity  → table (relational_db)
        → collection (document_db)
        → bucket (cloud_storage)
        → topic (streaming)

entities → tables / collections / buckets / topics
```

### Rule 2: Plural → Singular
```
tables: ["users", "orders"] → table: "users"
collections: ["contacts"]   → collection: "contacts"
```

### Rule 3: Cross-Terminology
```
table → collection (if connector is document_db)
collection → table (if connector is relational_db)
```

## Category Definitions

```python
CATEGORY_OPERATIONS = {
    "relational_db": {
        "terminology": {"entity": "table", "record": "row", "field": "column"}
    },
    "document_db": {
        "terminology": {"entity": "collection", "record": "document", "field": "field"}
    },
    "cloud_storage": {
        "terminology": {"entity": "bucket", "record": "file", "field": "key"}
    },
    "streaming": {
        "terminology": {"entity": "topic", "record": "message", "field": "field"}
    },
    "api_saas": {
        "terminology": {"entity": "endpoint", "record": "record", "field": "field"}
    }
}
```

## Usage in Connectors

### For NEW Connectors (using BaseMCPConnector)

```python
from base_connector import BaseMCPConnector

class MyConnectorMCPServer(BaseMCPConnector):
    def __init__(self):
        super().__init__()
        self.connector_category = "relational_db"  # Set your category
    
    def export(self, params: Dict) -> Dict[str, Any]:
        # Params are already normalized by BaseMCPConnector
        table = params.get('table')  # Works regardless of input format
        ...
```

### For EXISTING Connectors (not inheriting)

```python
from base_connector import normalize_params_for_connector

class MyLegacyConnector:
    def __init__(self):
        self.connector_category = "relational_db"
    
    def export(self, params: Dict) -> Dict[str, Any]:
        # Call normalization explicitly
        params = normalize_params_for_connector(params, self.connector_category)
        table = params.get('table')
        ...
```

## Why This Matters

1. **New connectors work automatically** - Just set `connector_category` and inherit from `BaseMCPConnector`
2. **Plan generators can use any terminology** - The normalization layer handles translation
3. **No connector patching required** - Central fix applies everywhere
4. **Generated connectors are correct** - Template uses normalization by default

## Defense in Depth

We have THREE layers of normalization:

```
Plan Generator → Go Executor → MCP Connector
     ↓              ↓              ↓
  (creates)    (normalizes)   (normalizes)
  tables: []   tables→table   tables→table
```

Even if one layer fails, the others catch it.
