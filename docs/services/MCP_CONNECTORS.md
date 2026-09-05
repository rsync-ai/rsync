# MCP Connectors Documentation

**Technology**: Python 3.12, Docker
**Interface**: Model Context Protocol (MCP)
**Directory**: `/shared/mcp-connectors`

---

## Overview

MCP (Model Context Protocol) Connectors are the unified interface layer that enables rsync-ai's AI agents to work with any data source generically. Each connector implements a standardized interface, allowing the same operations (test, discover, export, import) across every connected system.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    MCP Connector Architecture                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                   BaseMCPConnector                         │  │
│  │                   (Abstract Base)                          │  │
│  │                                                            │  │
│  │  + test_connection(config) → bool                         │  │
│  │  + discover_schema(config) → Schema                        │  │
│  │  + validate_config(config) → ValidationResult              │  │
│  │  + export(config, table, limit) → Data                     │  │
│  │  + import_data(config, table, data) → Result               │  │
│  │  + list_tools() → List[Tool]                               │  │
│  │  + get_capabilities() → Capabilities                       │  │
│  │                                                            │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              △                                   │
│                              │ inherits                          │
│      ┌───────────────────────┼───────────────────────┐          │
│      │                       │                       │          │
│  ┌───────────┐          ┌───────────┐          ┌───────────┐   │
│  │  MySQL    │          │   S3      │          │  Stripe   │   │
│  │ Connector │          │ Connector │          │ Connector │   │
│  └───────────┘          └───────────┘          └───────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Why MCP Matters

### The Problem with Traditional Connectors

Traditional ETL tools require connector-specific code:
```python
# Without MCP - different code for each connector
mysql_data = mysql_client.query("SELECT * FROM users")
s3_data = s3_client.get_object(bucket, key)
stripe_data = stripe_client.Customer.list()
```

### The MCP Solution

With MCP, AI agents use a unified interface:
```python
# With MCP - same code for ANY connector
data = connector.export(config, table="users")
# Works for MySQL, S3, Stripe, or any other connector
```

**Key Benefit**: AI can work with ALL connectors — pre-built and generated alike — using the same interface, with no connector-specific knowledge needed.

---

## Connector Interface

### BaseMCPConnector (Abstract)

Every connector implements this interface:

```python
class BaseMCPConnector(ABC):
    """Base class for all MCP connectors"""

    @abstractmethod
    def test_connection(self, config: Dict) -> TestResult:
        """
        Verify connectivity to the data source.

        Returns:
            TestResult with success status and latency
        """
        pass

    @abstractmethod
    def discover_schema(self, config: Dict) -> Schema:
        """
        Auto-discover tables and columns.

        Returns:
            Schema with tables, columns, types, relationships
        """
        pass

    @abstractmethod
    def validate_config(self, config: Dict) -> ValidationResult:
        """
        Validate configuration without connecting.

        Returns:
            ValidationResult with errors if invalid
        """
        pass

    @abstractmethod
    def export(self, config: Dict, table: str, limit: int = None) -> ExportResult:
        """
        Read/export data from source.

        Returns:
            ExportResult with rows and metadata
        """
        pass

    @abstractmethod
    def import_data(self, config: Dict, table: str, data: List[Dict]) -> ImportResult:
        """
        Write/import data to destination.

        Returns:
            ImportResult with success count and errors
        """
        pass

    def list_tools(self) -> List[Tool]:
        """Return available operations/tools"""
        pass

    def get_capabilities(self) -> Capabilities:
        """Declare connector capabilities"""
        pass
```

### Schema Types

```python
class Schema(BaseModel):
    tables: List[Table]

class Table(BaseModel):
    name: str
    columns: List[Column]
    row_count: Optional[int]
    primary_key: Optional[str]

class Column(BaseModel):
    name: str
    type: str  # "STRING", "INTEGER", "TIMESTAMP", etc.
    nullable: bool
    description: Optional[str]
```

### Result Types

```python
class ExportResult(BaseModel):
    rows: List[Dict]
    total_count: int
    has_more: bool
    cursor: Optional[str]

class ImportResult(BaseModel):
    success: bool
    rows_imported: int
    errors: List[ErrorItem]

class TestResult(BaseModel):
    success: bool
    latency_ms: int
    message: str
    server_version: Optional[str]
```

---

## Connector Catalog

**17 pre-built public connectors, plus any connector the tool-generator writes on demand.**
The list below is the on-disk inventory — every entry has a `latest.json` under
`shared/mcp-connectors/`. Anything not listed here is a *generation target*, not a shipped
connector (see [Connector Generation](#connector-generation) below).

> Verify at any time: `find shared/mcp-connectors -name latest.json`

### Relational Databases

| Connector | Directory | CDC Support |
|-----------|-----------|-------------|
| **PostgreSQL** | `public/postgresql/` | Yes (Debezium, logical replication) |
| **MySQL** | `public/database/mysql/` | Yes (Debezium, binlog) |
| **Oracle** | `public/database/oracle/` | Yes (Debezium, LogMiner) |
| **SQL Server** | `public/database/sqlserver/` | Yes (Debezium) |
| **MongoDB** | `public/database/mongodb/` | Yes (Debezium) |

CDC family per [ARCHITECTURE.md](../../ARCHITECTURE.md) — MySQL / Postgres / Mongo / SQL Server / Oracle.
Provisioning order for the Postgres family is non-negotiable — see the publication-before-slot
rule in [CDC source prerequisites](../connectors/cdc-source-prerequisites.md).

### Data Warehouses

| Connector | Directory |
|-----------|-----------|
| **Snowflake** | `public/database/snowflake/` |
| **BigQuery** | `public/database/bigquery/` |
| **Redshift** | `public/redshift/` |
| **Databricks** | `public/database/databricks/` |
| **ClickHouse** | `public/database/clickhouse/` |

### Cloud Storage

| Connector | Directory |
|-----------|-----------|
| **AWS S3** | `public/storage/aws-s3/` |
| **Azure Blob** | `public/storage/azure-blob/` |
| **Google Cloud Storage** | `public/storage/gcs/` |

Config reference → [docs/connectors/cloud-storage-config.md](../connectors/cloud-storage-config.md).

### SaaS APIs

| Connector | Directory | Protocol |
|-----------|-----------|----------|
| **Stripe** | `public/stripe/` | REST |
| **GitHub** | `public/github-rest/` | REST |
| **Shopify** | `public/shopify-admin-graphql/` | GraphQL (Admin API) |
| **Google Sheets** | `public/google-sheets/` | REST |

### Internal / Infrastructure

Not user-selectable sources — these are platform plumbing.

| Connector | Directory | Role |
|-----------|-----------|------|
| **Debezium** | `internal/debezium/` | CDC capture on Kafka Connect |
| **Kafka MCP Sink** | `internal/kafka-mcp-sink/` | Applies CDC events to the destination |
| **MinIO** | `internal/minio/` | Claim-check payload store for batch |
| **Context7** | `internal/context7/` | API-doc lookup for the tool-generator |

### Test fixtures (not products)

`public/petstore/`, `public/sample-data/`, `public/widgets-graphql/` exist for tests and demos of
the generation flow. Do not present these as customer-facing connectors.

### Generation targets with OAuth preconfigured

`shared/mcp-connectors/oauth/providers.json` ships OAuth app config for **18 providers**, so a
generated connector for any of them skips the auth plumbing:

`dropbox`, `freshdesk`, `github`, `google`, `hubspot`, `intercom`, `jira`, `linear`, `mailchimp`,
`notion`, `petstore`, `pipedrive`, `salesforce`, `shopify`, `slack`, `stripe`, `zendesk`, `zoho-crm`

**These are not pre-built connectors.** They are sources the generator can target with auth already
wired. Saying "we support Salesforce" is only true after the generator has produced and deployed
that connector.

---

## Connector Details

> **Paths below point at the versioned directory, which is the code that actually runs.**
> A connector root holds only `latest.json` (the version pointer) plus the `versions/` tree —
> there are **no root copies** of `connector.py`/`metadata.json`/`Dockerfile`. The Docker build
> context is `versions/<current_version>/`, where `<current_version>` comes from
> `latest.json.current_version` (**not** the highest-numbered directory). Resolve it in code via
> `resolve_current_dir()` (Python) or `connectorpaths.ResolveVersionedMetadataPath()` (Go) —
> never by string-joining the connector root. See the [connector developer guide](../connectors/developer-guide.md).

### MySQL Connector

**Location**: `shared/mcp-connectors/public/database/mysql/versions/<current_version>/`

**Configuration**:
```json
{
  "host": "mysql.example.com",
  "port": 3306,
  "database": "production",
  "username": "reader",
  "password": "***",
  "ssl": true
}
```

**Operations**:
- `test_connection` - Verify MySQL connectivity
- `discover_schema` - List databases, tables, columns
- `export` - SELECT with pagination
- `import_data` - INSERT/UPSERT

**CDC Support**: Debezium + MySQL Binlog

---

### PostgreSQL Connector

**Location**: `shared/mcp-connectors/public/postgresql/versions/<current_version>/`

**Configuration**:
```json
{
  "host": "postgres.example.com",
  "port": 5432,
  "database": "analytics",
  "username": "reader",
  "password": "***",
  "schema": "public",
  "ssl_mode": "require"
}
```

**Operations**:
- Same as MySQL
- Additional: `execute_query` for custom SQL

**CDC Support**:
- Debezium
- Native Logical Replication

---

### AWS S3 Connector

**Location**: `shared/mcp-connectors/public/storage/aws-s3/versions/<current_version>/`

**Configuration**:
```json
{
  "bucket": "data-warehouse",
  "region": "us-east-1",
  "access_key_id": "AKIA***",
  "secret_access_key": "***",
  "prefix": "exports/"
}
```

**Supported Formats**:
- CSV (with gzip compression)
- JSON (newline-delimited)
- Parquet

**Operations**:
- `test_connection` - List bucket contents
- `discover_schema` - Infer from file headers
- `export` - Read files
- `import_data` - Write files

---

### Stripe Connector

**Location**: `shared/mcp-connectors/public/stripe/versions/<current_version>/`

**Configuration** (both fields optional; `auth_type: bearer`):
```json
{
  "base_url": "https://api.stripe.com/v1",
  "access_token": "***"
}
```

**Resources Exposed** (from `metadata.json` `operations`):
- `customers`, `invoices`, `charges`, `payment_intents`
- `subscriptions`, `products`, `prices`, `refunds`

**Operations**:
- `test_connection`, `validate_config`, `discover_schema`, `get_capabilities`
- `export`, `import_data`, `upsert_data`, `delete_data`

**Direction**: source ✅ · destination ✅ · CDC ❌ (`supports_cdc: false` — REST APIs have no
change log; use batch with an incremental cursor)

---

## Connector Generation

When a connector doesn't exist, the Tool Generator creates one:

```
User: "Create a connector for Notion API"

Tool Generator Pipeline:
1. DocResearcher → Fetch Notion API docs
2. Researcher → Analyze endpoints
3. Architect → Design ConnectorSpec
4. Builder → Generate Python code
5. QA Agent → Validate
6. Deployment → Docker + Registry

Output:
notion/
├── connector.py      # 500+ lines of generated code
├── metadata.json     # Operation definitions
├── requirements.txt  # notion-client, etc.
├── Dockerfile        # Container definition
└── spec.json         # Full specification
```

**Template**: `/llm-service/src/agents/tool_generator/templates/connector.py.j2`

---

## Connector Metadata

Each connector includes a `metadata.json`:

```json
{
  "name": "mysql",
  "display_name": "MySQL",
  "version": "1.0.0",
  "category": "relational_db",
  "description": "MySQL database connector",
  "logo_url": "/connectors/mysql/logo.png",

  "operations": [
    {
      "name": "test_connection",
      "type": "core",
      "description": "Verify database connectivity"
    },
    {
      "name": "discover_schema",
      "type": "core",
      "description": "List databases, tables, and columns"
    },
    {
      "name": "export",
      "type": "source",
      "description": "Export data from tables"
    },
    {
      "name": "import_data",
      "type": "destination",
      "description": "Import data to tables"
    }
  ],

  "capabilities": {
    "supports_source": true,
    "supports_destination": true,
    "supports_cdc": true,
    "max_batch_size": 10000,
    "supports_schema_discovery": true
  },

  "config_fields": [
    {"name": "host", "type": "string", "required": true},
    {"name": "port", "type": "integer", "default": 3306},
    {"name": "database", "type": "string", "required": true},
    {"name": "username", "type": "string", "required": true},
    {"name": "password", "type": "string", "required": true, "secret": true},
    {"name": "ssl", "type": "boolean", "default": false}
  ]
}
```

---

## Directory Structure

```
shared/mcp-connectors/
├── base_connector.py          # BaseMCPConnector abstract class
├── handlers/
│   ├── api_handler.py         # HTTP API handling
│   ├── database_handler.py    # SQL database handling
│   └── storage_handler.py     # Cloud storage handling
│
├── oauth/
│   └── providers.json         # OAuth app config for 18 generation targets
│
├── public/                    # 17 pre-built connectors (+ 3 test fixtures)
│   ├── postgresql/
│   │   ├── latest.json        # {"current_version": "v1.0.0", "all_versions": [...]}
│   │   └── versions/
│   │       └── v1.0.0/        # <- the Docker build context; the code that RUNS
│   │           ├── connector.py
│   │           ├── base_connector.py
│   │           ├── metadata.json
│   │           ├── spec.json
│   │           ├── requirements.txt
│   │           ├── Dockerfile
│   │           └── logo.svg
│   ├── database/              # bigquery, clickhouse, databricks, mongodb,
│   │                          # mysql, oracle, snowflake, sqlserver
│   ├── storage/               # aws-s3, azure-blob, gcs
│   ├── redshift/
│   ├── stripe/
│   ├── github-rest/
│   ├── google-sheets/
│   ├── shopify-admin-graphql/
│   └── petstore/ sample-data/ widgets-graphql/   # test fixtures, not products
│
├── internal/                  # Infrastructure connectors
│   ├── debezium/
│   ├── kafka-mcp-sink/
│   ├── minio/
│   └── context7/
│
└── generated/                 # AI-generated connectors
    └── (dynamically created)
```

---

## Running Connectors

### Standalone (Development)

```bash
cd shared/mcp-connectors/public/database/mysql/versions/v1.0.0   # = latest.json.current_version
python connector.py
```

### Docker (Production)

```bash
# Build
docker build -t rsync-ai/mysql-connector:latest .

# Run
docker run -p 5020:5020 rsync-ai/mysql-connector:latest
```

### Docker Compose

```yaml
services:
  mysql-connector:
    image: rsync-ai/mysql-connector:latest
    ports:
      - "5020:5020"
    environment:
      - LOG_LEVEL=INFO
```

---

## Testing Connectors

### Unit Tests

```python
def test_mysql_connection():
    connector = MySQLConnector()
    config = {
        "host": "localhost",
        "port": 3306,
        "database": "test",
        "username": "root",
        "password": "test"
    }
    result = connector.test_connection(config)
    assert result.success == True

def test_schema_discovery():
    connector = MySQLConnector()
    schema = connector.discover_schema(config)
    assert len(schema.tables) > 0
```

### E2E Tests

```bash
# Run E2E tests
cd tests
python -m pytest test_mysql_to_s3.py -v
```

---

## Demo Highlights

1. **Schema Discovery** - Show automatic table/column detection
2. **Universal Interface** - Same `export()` call for MySQL, S3, Stripe
3. **Connector Catalog** - Browse the catalog: 17 pre-built, more generated on demand
4. **Generated Connector** - Show AI-generated code structure
5. **CDC Integration** - Show Debezium connector in action

---

## Adding a New Connector

### Manual (Developer)

1. Create directory in `public/`
2. Implement `connector.py` extending `BaseMCPConnector`
3. Create `metadata.json`
4. Add `requirements.txt`
5. Create `Dockerfile`
6. Test with unit tests

### AI-Generated

```bash
curl -X POST http://localhost:5010/v1/generate \
  -H "Content-Type: application/json" \
  -d '{
    "api_name": "notion",
    "description": "Notion API connector for pages and databases",
    "docs_url": "https://developers.notion.com/"
  }'
```

---

## Troubleshooting

### Connection failed
```bash
# Test connectivity directly
docker-compose exec mysql-connector python -c "
from connector import MySQLConnector
c = MySQLConnector()
print(c.test_connection({'host': 'mysql', ...}))
"
```

### Schema discovery empty
```bash
# Check permissions
docker-compose exec mysql mysql -u reader -p -e "SHOW DATABASES"
```

### Import failed
```bash
# Check destination permissions
docker-compose exec postgres psql -U writer -d db -c "\dt"
```

---

*For more details, see the codebase at `/shared/mcp-connectors`*
