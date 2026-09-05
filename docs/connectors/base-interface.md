# Generic MCP Connector Interface

## Overview
All MCP connectors MUST implement this standard interface to enable AI agents to interact with any data source uniformly.

## Required Methods

### 1. `test_connection(params: Dict) -> Dict`
**Purpose**: Verify connectivity to the data source

**Input**:
```json
{
  "config": {
    "host": "...",
    "user": "...",
    "password": "...",
    ...
  }
}
```

**Output**:
```json
{
  "success": true|false,
  "message": "Connection successful",
  "error": "Error message (if failed)",
  "version": "Data source version (optional)"
}
```

---

### 2. `discover_schema(params: Dict) -> Dict`
**Purpose**: Discover tables, collections, or objects and their schemas

**Input**:
```json
{
  "config": {...},
  "include_row_counts": true|false,
  "include_samples": true|false,
  "max_tables": 100
}
```

**Output**:
```json
{
  "success": true|false,
  "tables": [
    {
      "name": "table_name",
      "schema": "schema_name (optional)",
      "columns": [
        {
          "name": "column_name",
          "type": "data_type",
          "nullable": true|false,
          "primary_key": true|false (optional),
          "description": "..." (optional)
        }
      ],
      "row_count": 12345,
      "size_bytes": 1024000 (optional),
      "last_modified": "2025-01-01T00:00:00Z" (optional)
    }
  ],
  "total_tables": 10,
  "error": "Error message (if failed)"
}
```

---

### 3. `validate_config(params: Dict) -> Dict`
**Purpose**: Validate configuration without connecting

**Input**:
```json
{
  "config": {...}
}
```

**Output**:
```json
{
  "success": true|false,
  "valid": true|false,
  "errors": ["Missing field: host", ...],
  "warnings": ["Password not provided", ...]
}
```

---

### 4. `export(params: Dict) -> Dict`
**Purpose**: Extract data from the source

**Input**:
```json
{
  "config": {...},
  "table": "table_name",
  "query": "SELECT * FROM ...", (optional, overrides table)
  "limit": 10000,
  "offset": 0,
  "where": "filter_condition",
  "columns": ["col1", "col2"] (optional)
}
```

**Output**:
```json
{
  "success": true|false,
  "data": [
    {"col1": "val1", "col2": "val2"},
    ...
  ],
  "columns": ["col1", "col2"],
  "row_count": 100,
  "has_more": true|false,
  "next_offset": 100,
  "error": "Error message (if failed)"
}
```

---

### 5. `import_data(params: Dict) -> Dict`
**Purpose**: Load data into the destination

**Input**:
```json
{
  "config": {...},
  "table": "table_name",
  "data": [
    {"col1": "val1", "col2": "val2"},
    ...
  ],
  "mode": "append|replace|upsert",
  "create_table": true|false
}
```

**Output**:
```json
{
  "success": true|false,
  "rows_inserted": 100,
  "rows_updated": 0,
  "rows_failed": 0,
  "error": "Error message (if failed)"
}
```

---

### 6. `get_schema(params: Dict) -> Dict`
**Purpose**: Get detailed schema for a specific table

**Input**:
```json
{
  "config": {...},
  "table": "table_name"
}
```

**Output**:
```json
{
  "success": true|false,
  "table": "table_name",
  "schema": [
    {
      "name": "column_name",
      "type": "data_type",
      "nullable": true|false,
      "key": "PRI|UNI|MUL",
      "default": "default_value",
      "extra": "auto_increment"
    }
  ],
  "indexes": [...] (optional),
  "constraints": [...] (optional),
  "error": "Error message (if failed)"
}
```

---

## Connector-Specific Adaptations

### Databases (MySQL, PostgreSQL, MongoDB)
- `discover_schema`: Returns tables/collections with columns/fields
- `row_count`: Actual row counts via COUNT(*)

### Cloud Storage (S3, GCS, Azure Blob)
- `discover_schema`: Returns buckets/containers and file listings
- `tables`: Each "table" is a bucket or prefix
- `columns`: Inferred from file format (JSON, CSV, Parquet)
- `row_count`: File count or estimated from sampling

### Data Warehouses (Snowflake, BigQuery, Redshift)
- `discover_schema`: Returns databases, schemas, and tables
- `columns`: Full type information including precision/scale
- `row_count`: From metadata tables

### APIs/SaaS (Salesforce, HubSpot, Stripe)
- `discover_schema`: Returns available API endpoints/objects
- `tables`: Each "table" is an API endpoint
- `columns`: Fields returned by API

### Streaming (Kafka, Kinesis, Pub/Sub)
- `discover_schema`: Returns topics/streams
- `tables`: Each "table" is a topic
- `columns`: Schema from schema registry or inferred
- `row_count`: Message count (if available)

---

## Performance Optimization Guidelines

**Target**: `discover_schema` should complete in < 2 seconds for 100+ tables/collections

### Universal Optimization Patterns

#### 1. **Use Metadata Catalogs (Not Actual Data Scans)**
✅ **Good**:
```python
# MySQL: Use INFORMATION_SCHEMA (instant)
SELECT TABLE_NAME, TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES

# PostgreSQL: Use pg_stat_user_tables (instant)
SELECT relname, n_live_tup FROM pg_stat_user_tables

# MongoDB: Use estimated_document_count() (instant)
collection.estimated_document_count()
```

❌ **Bad** (Slow for large tables):
```python
# DON'T do this for each table - takes 1-5 seconds per table!
SELECT COUNT(*) FROM table_name  
```

#### 2. **Batch Queries Instead of Per-Table Loops**
✅ **Good** (1-2 queries for all tables):
```sql
-- Get ALL columns for ALL tables in ONE query
SELECT table_name, column_name, data_type 
FROM information_schema.columns 
WHERE table_schema = 'mydb'
ORDER BY table_name, ordinal_position
```

❌ **Bad** (N queries for N tables):
```python
for table in tables:
    cursor.execute(f"DESCRIBE {table}")  # Separate query per table!
```

#### 3. **Set Reasonable Limits for Large Systems**
```python
def discover_schema(self, params):
    max_tables = params.get('max_tables', 100)       # Limit tables to scan
    sample_size = params.get('sample_size', 20)       # Sample docs/records
    max_files = params.get('max_files', 50)           # Limit file sampling
```

#### 4. **Use Estimated Counts (Not Exact)**
- **MySQL**: `TABLE_ROWS` from `INFORMATION_SCHEMA.TABLES`
- **PostgreSQL**: `n_live_tup` from `pg_stat_user_tables`
- **MongoDB**: `estimated_document_count()` not `count_documents()`
- **S3**: File count, not reading all files

### Connector-Specific Optimization Tips

| Connector | Key Optimization | Expected Time |
|-----------|------------------|---------------|
| **MySQL** | 2 queries: `INFORMATION_SCHEMA.TABLES` + `INFORMATION_SCHEMA.COLUMNS` | < 1 sec for 100 tables |
| **PostgreSQL** | JOIN `information_schema.tables` + `pg_stat_user_tables` | < 1 sec for 100 tables |
| **MongoDB** | `estimated_document_count()` + sample 20 docs per collection | < 2 sec for 50 collections |
| **S3** | Limit to 10 buckets, 50 files each, infer schema from 1 file | < 3 sec for 10 buckets |
| **Snowflake** | Use `INFORMATION_SCHEMA`, not `SHOW TABLES` | < 2 sec for 100 tables |
| **BigQuery** | Use `INFORMATION_SCHEMA.TABLES` API | < 2 sec for 100 tables |

### Example: Optimized MySQL `discover_schema`

```python
def discover_schema(self, params):
    database = config.get('database')
    
    # STEP 1: Get all tables with estimated row counts (instant)
    cursor.execute("""
        SELECT TABLE_NAME, TABLE_ROWS 
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
    """, (database,))
    tables_info = cursor.fetchall()
    row_count_map = {row[0]: row[1] for row in tables_info}
    
    # STEP 2: Get ALL columns for ALL tables in ONE query
    cursor.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """, (database,))
    
    # Group columns by table
    columns_by_table = {}
    for col in cursor.fetchall():
        table_name = col[0]
        if table_name not in columns_by_table:
            columns_by_table[table_name] = []
        columns_by_table[table_name].append({
            "name": col[1], "type": col[2], "nullable": col[3] == "YES"
        })
    
    # Build result (no additional queries!)
    return {"success": True, "tables": [...]}
```

**Result**: 77 tables discovered in < 1 second (vs 35 seconds with COUNT(*) per table)

---

## AI Agent Usage Pattern

```python
# 1. Test connection
result = connector.test_connection(params)
if not result['success']:
    raise ConnectionError(result['error'])

# 2. Discover schema
schema_result = connector.discover_schema(params)
tables = schema_result['tables']

# 3. AI analyzes and selects tables to replicate
selected_tables = ai_agent.select_tables(tables)

# 4. Export data from source
for table in selected_tables:
    data = source_connector.export({
        'config': source_config,
        'table': table['name'],
        'limit': 10000
    })
    
    # 5. Import data to destination
    dest_connector.import({
        'config': dest_config,
        'table': table['name'],
        'data': data['data'],
        'mode': 'append'
    })
```

---

## Implementation Checklist

For each new connector, implement:
- [x] `test_connection` - Verify connectivity
- [x] `discover_schema` - Auto-discover tables/schema
- [x] `validate_config` - Config validation
- [x] `export` - Data extraction
- [x] `import` - Data loading (for destinations)
- [x] `get_schema` - Detailed schema info
- [ ] Error handling with descriptive messages
- [ ] Retry logic for transient failures
- [ ] Rate limiting (for APIs)
- [ ] Pagination support (for large datasets)

---

## Testing

Each connector must pass:
1. Connection test with valid credentials
2. Schema discovery returns expected format
3. Export handles pagination correctly
4. Import creates tables if needed
5. Handles network errors gracefully

---

## Distributed Tracing (trace_id)

### Automatic Trace ID Handling

All connectors that inherit from `BaseMCPConnector` automatically get trace_id support:

```python
from base_connector import BaseMCPConnector, get_trace_id, setup_traced_logging

class MyConnector(BaseMCPConnector):
    def __init__(self):
        super().__init__()  # Sets up traced logging automatically
        self.connector_type = "my-connector"
    
    def some_operation(self, params):
        # Use self.log() for traced logging
        self.log("Starting operation")
        
        # Access current trace_id if needed
        trace_id = self.get_current_trace_id()
        
        # Include in external API calls
        headers = {"X-Trace-ID": trace_id}
```

### How It Works

1. **Request Handling**: `handle_request()` in `BaseMCPConnector` automatically extracts `trace_id` or `_trace_id` from request params
2. **Thread-Local Storage**: trace_id is stored in thread-local storage, making it available anywhere
3. **Logging**: All logs via `self.log()` or the configured logger include trace_id
4. **Propagation**: Use `self.get_current_trace_id()` to propagate to external calls

### For New Connectors

Connectors generated by the tool-generator service inherit trace_id support from
`BaseMCPConnector` automatically — there is nothing to wire up. See
[developer-guide.md](developer-guide.md#1-generate-a-new-connector) for the generation
path.

### For Existing Connectors (Not Using Base Class)

If your connector doesn't inherit from `BaseMCPConnector`, add this boilerplate:

```python
import threading
from base_connector import get_trace_id, set_trace_id, TraceContextFilter, setup_traced_logging

# In handle_request():
def handle_request(self, request):
    method = request.get('method')
    params = request.get('params', {})
    
    # Extract trace_id
    if method == 'tools/call':
        trace_id = params.get('arguments', {}).get('trace_id')
    else:
        trace_id = params.get('trace_id') or params.get('_trace_id')
    
    set_trace_id(trace_id)
    # ... rest of handler
```

### Trace ID Flow

```
Frontend → API Gateway → Orchestrator → MCP Connector
   │           │              │              │
   └── X-Trace-ID header ─────┴── trace_id in params ──┘
                                      │
                              Thread-local storage
                                      │
                              All logs include trace_id
```

