# LLM Service & AI Agents Documentation

**Technology**: Python 3.12, FastAPI, LangChain, OpenAI, Ollama
**Ports**: 5010 (Tool Generator), 5011 (Planner)
**Directory**: `/llm-service`

---

## Overview

The LLM Service is the AI brain of rsync-ai. It contains multiple specialized agents that handle natural language understanding, intelligent planning, connector generation, and data exploration. Each agent is designed for a specific task and can work independently or as part of a larger pipeline.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                       LLM Service                              │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  ┌──────────────────┐  ┌──────────────────┐                    │
│  │   Intent Agent   │  │  Planner Agent   │                    │
│  │  (NL → Intent)   │  │ (Intent → Plan)  │                    │
│  └──────────────────┘  └──────────────────┘                    │
│                                                                │
│  ┌──────────────────┐  ┌──────────────────┐                    │
│  │ Tool Generator   │  │ Explorer Agent   │                    │
│  │(Generate Connectors)│ │   (NL2SQL)      │                    │
│  └──────────────────┘  └──────────────────┘                    │
│                                                                │
│  ┌──────────────────┐  ┌──────────────────┐                    │
│  │Connector Resolver│  │  DAG Planner     │                    │
│  │ (Name Matching)  │  │ (Dependency)     │                    │
│  └──────────────────┘  └──────────────────┘                    │
│                                                                │
│  ┌──────────────────┐  ┌──────────────────┐                    │
│  │  PII Scanner     │  │ Suggestions      │                    │
│  │(Sensitive Data)  │  │ (Next Steps)     │                    │
│  └──────────────────┘  └──────────────────┘                    │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

---

## AI Agents Deep Dive

### 1. Intent Agent

> ⚠️ **The Python `/llm-service/src/agents/intent/` FastAPI module was removed (PR #167) — it was dead scaffolding that nothing launched.** Live NL→intent parsing now happens in the Go orchestrator's `IntentWorker` (`backend-orchestrator/internal/agents/...`). The conceptual flow below still describes what intent parsing does, but the implementation is no longer this Python module.

**Purpose**: Parse natural language requests into structured pipeline intent.

**Location**: ~~`/llm-service/src/agents/intent/`~~ **removed** — see banner above; live path is the Go `IntentWorker`.

**How It Works**:
```
User: "Sync all customer data from MySQL to S3 every day"
                    │
                    ▼
            ┌───────────────┐
            │ Intent Agent  │
            │               │
            │ 1. Identify   │ ──► "sync" = data movement
            │    action     │
            │               │
            │ 2. Extract    │ ──► source: MySQL
            │    source     │     tables: [customers]
            │               │
            │ 3. Extract    │ ──► destination: S3
            │    destination│
            │               │
            │ 4. Parse      │ ──► cron: "0 0 * * *"
            │    schedule   │
            │               │
            │ 5. Calculate  │ ──► 0.95
            │    confidence │
            └───────────────┘
                    │
                    ▼
            Structured Intent
```

**Output Schema**:
```python
class PipelineIntent(BaseModel):
    action: str  # "sync", "replicate", "export", "backup"
    source: SourceIntent
    destination: DestinationIntent
    tables: List[str]
    schedule: Optional[str]
    transformations: List[TransformSpec]
    confidence: float
```

**Demo Point**: Show how the agent handles variations:
- "Sync customers from MySQL" → same as "Move customer table from my mysql database"
- Handles typos: "postgress" → "postgresql"
- Handles abbreviations: "S3" → "aws-s3"

---

### 2. Connector Resolver Agent

**Purpose**: AI-powered connector name matching with fuzzy resolution.

**Location**: `/llm-service/src/agents/connector_resolver/`

**Pattern**: ReAct (Reasoning + Acting)

**Available Tools**:
```python
@tool
def discover_connectors() -> List[str]:
    """List all available connectors"""

@tool
def get_connector_metadata(name: str) -> Dict:
    """Get details about a specific connector"""

@tool
def search_by_category(category: str) -> List[str]:
    """Filter connectors by category"""

@tool
def search_by_description(query: str) -> List[str]:
    """Semantic search in connector descriptions"""
```

**Reasoning Example**:
```
User Input: "postgress"

Agent Reasoning:
  Thought: User likely means PostgreSQL with a typo
  Action: discover_connectors()
  Observation: [..., "postgresql", "postgres-cdc", ...]
  Thought: Found "postgresql" which matches the typo pattern
  Action: get_connector_metadata("postgresql")
  Observation: {"name": "postgresql", "category": "relational_db", ...}
  Thought: High confidence match, returning result
  Final Answer: {"match": "postgresql", "confidence": 0.98}
```

**Demo Point**: Type intentional typos and watch the agent resolve them correctly.

---

### 3. Planner Agent

**Purpose**: Generate optimal execution plans from pipeline intent.

**Location**: `/llm-service/src/agents/planner/`

**Port**: 5011

**Strategy Selection**:
```
Intent Analysis
     │
     ├── Is real-time needed?
     │        │
     │        ├── Yes ──► CDC Strategy
     │        │              │
     │        │              ├── Debezium (MySQL, PostgreSQL, Oracle)
     │        │              ├── PostgreSQL Logical Replication
     │        │              └── MongoDB Change Streams
     │        │
     │        └── No ──► Batch Strategy
     │                       │
     │                       ├── Small data (<100K rows) → Full load
     │                       ├── Large data → Incremental
     │                       └── Very large → Partitioned
     │
     └── Is initial load needed for CDC?
              │
              └── Yes ──► Hybrid Strategy (batch + CDC)
```

**CDC Provider Selection Logic**:
```python
def select_cdc_provider(source_type: str, requirements: Dict) -> str:
    if source_type == "mysql":
        if requirements.get("latency") == "low":
            return "mysql-binlog"  # Native, lowest latency
        return "debezium-mysql"    # More features

    if source_type == "postgresql":
        if requirements.get("simplicity"):
            return "postgres-logical"  # Native
        return "debezium-postgresql"   # More features

    # Default to Debezium for other databases
    return f"debezium-{source_type}"
```

**Output Plan Schema**:
```python
class ExecutionPlan(BaseModel):
    strategy: str  # "batch", "cdc", "hybrid"
    cdc_provider: Optional[str]
    stages: List[PlanStage]
    kafka_topics: List[str]
    estimated_duration: int

class PlanStage(BaseModel):
    id: str
    operation: str  # "export", "transform", "import"
    source: str
    destination: str
    table: str
    batch_size: int
    dependencies: List[str]
```

**Demo Point**: Ask for real-time sync and watch the agent choose CDC with appropriate provider.

---

### 4. Tool Generator Agent

**Purpose**: Generate new connectors on-demand from natural language.

**Location**: `/llm-service/src/agents/tool_generator/`

**Port**: 5010

**Pipeline Architecture**:
```
User Request
     │
     ▼
┌─────────────────┐
│ DocResearcher   │ ──► Fetch API documentation (Context7)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Researcher    │ ──► Analyze docs → MockProfile
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Architect     │ ──► Design connector → ConnectorSpec
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Builder      │ ──► Jinja2 templates → Python code
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    QA Agent     │ ──► Validate with mock server
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Deployment    │ ──► Docker build + registry
└─────────────────┘
```

**ConnectorSpec Schema**:
```python
class ConnectorSpec(BaseModel):
    name: str                    # "stripe"
    display_name: str            # "Stripe"
    version: str                 # "1.0.0"
    description: str
    category: ConnectorCategory  # api_saas, relational_db, etc.
    connector_type: str          # "stripe" (kebab-case)
    base_url: Optional[str]      # "https://api.stripe.com"
    auth: AuthConfig
    operations: List[OperationConfig]
    config_fields: List[ConfigField]
    resources: List[ResourceConfig]
    supports_source: bool
    supports_destination: bool
    supports_cdc: bool
```

**Template Files**:
- `connector.py.j2` - Main API/SaaS connector (73KB)
- `connector_database.py.j2` - Database connector (126KB)
- `metadata.json.j2` - Connector metadata
- `requirements.txt.j2` - Dependencies
- `Dockerfile.j2` - Container definition

**Generated Connector Structure**:
```
stripe/
├── connector.py        # Generated Python implementation
├── metadata.json       # Operation definitions
├── requirements.txt    # Dependencies
├── Dockerfile          # Container definition
├── logo.png            # Downloaded logo
└── spec.json           # Full specification
```

**Demo Point**: Generate a connector live - "Create a connector for Notion API"

---

### 5. Explorer Agent (NL2SQL)

**Purpose**: Convert natural language questions to SQL queries.

**Location**: `/llm-service/src/agents/explorer/`

**Pipeline**:
```
Natural Language Question
         │
         ▼
┌─────────────────┐
│ Table Linker    │ ──► Identify relevant tables
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Column Mapper   │ ──► Select relevant columns
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Join Planner    │ ──► Determine table relationships
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ SQL Generator   │ ──► Build query
└────────┬────────┘
         │
         ▼
      SQL Query
```

**Example**:
```
User: "Top 10 customers by total orders this quarter"

Table Linker Output:
  - customers (confidence: 0.95)
  - orders (confidence: 0.90)

Column Mapper Output:
  - customers.id, customers.name
  - orders.customer_id, orders.total, orders.created_at

Join Planner Output:
  - customers JOIN orders ON customers.id = orders.customer_id

SQL Generator Output:
  SELECT c.name, COUNT(o.id) as order_count, SUM(o.total) as total_value
  FROM customers c
  JOIN orders o ON c.id = o.customer_id
  WHERE o.created_at >= DATE_TRUNC('quarter', CURRENT_DATE)
  GROUP BY c.id, c.name
  ORDER BY total_value DESC
  LIMIT 10
```

**Supported Databases**:
- PostgreSQL
- Redshift
- Databricks
- MySQL (with dialect adaptation)

**Offline Mode**:
- Uses Ollama with Llama3 model
- No internet required
- Lower latency, privacy-preserving

**Demo Point**: Ask a complex question and show the generated SQL.

---

### 6. DAG Planner Agent

**Purpose**: Optimize execution order and detect dependencies.

**Location**: `/llm-service/src/agents/dag_planner/`

**Dependency Detection**:
```
Tables to sync: [customers, orders, order_items]

Analysis:
  - orders.customer_id → customers.id (FK)
  - order_items.order_id → orders.id (FK)

DAG:
  customers ──► orders ──► order_items

Execution Order:
  1. customers (no dependencies)
  2. orders (depends on customers)
  3. order_items (depends on orders)
```

---

### 7. PII Scanner Agent

**Purpose**: Detect personally identifiable information in data.

**Location**: `/llm-service/src/agents/pii/`

**Detection Patterns**:
- Email addresses
- Phone numbers
- Social Security Numbers
- Credit card numbers
- Names (using NER)
- Addresses

**Output**:
```python
class PIIDetectionResult(BaseModel):
    column: str
    pii_type: str  # "email", "phone", "ssn", etc.
    confidence: float
    sample_match: str  # Redacted sample
    recommendation: str  # "mask", "encrypt", "exclude"
```

---

### 8. Suggestions Agent

**Purpose**: Provide intelligent next-step recommendations.

**Location**: `/llm-service/src/agents/suggestions/`

**Context-Aware Suggestions**:
```
Context: User just created MySQL → S3 pipeline

Suggestions:
  1. "Add a schedule to run this pipeline hourly"
  2. "Enable CDC for real-time updates"
  3. "Add column filtering to reduce data volume"
  4. "Set up monitoring alerts for failures"
```

---

## LLM Providers

All providers are built by one factory —
[`utils/openai_client.py`](../../llm-service/src/utils/openai_client.py). There is no
provider-specific code anywhere else. Four providers: `openai` · `azure` · `groq` · `ollama`,
selected by `LLM_PROVIDER`.

Resolution when `LLM_PROVIDER` is unset (`resolve_provider`, `:77-115`): Azure endpoint → `azure`,
else `OPENAI_API_KEY` → `openai`, else `ollama`. `LLM_PROVIDER=openai` with no key **fails closed
to `ollama`** (`:101-103`), and Groq is never auto-selected — it requires an explicit opt-in so a
stray `GROQ_API_KEY` cannot silently route prompts to an undisclosed external LLM
(`:98-99`, `:113-115`).

### Model selection

One rule, two levels (`get_default_model`, `:118-136`):

1. `LLM_MODEL` — overrides everything, for every provider. On Azure this is a **deployment name**.
2. Otherwise the provider default: `gpt-4o-mini` (openai/azure), `llama-3.3-70b-versatile` (groq),
   `OLLAMA_MODEL` or `qwen2.5:7b` (ollama).

Individual agents may override: `RSYNC_TOOL_GENERATOR_MODEL`, `RANK_TABLES_MODEL`, and the
`EXPLORER_*_MODEL` family.

Two deliberate exceptions to "`LLM_MODEL` overrides everything", both in
[openai_client.py](../../llm-service/src/utils/openai_client.py): the Explorer resolves through
`explorer_default_model` (`:162`) / `explorer_default_sql_model` (`:185`), and `/agents/rank-tables`
through `rank_tables_default_model` (`:197`) — which ignores `LLM_MODEL` on OpenAI on purpose, so a
stack-wide upgrade to `gpt-4o` doesn't silently multiply the cost of a bulk metadata task. Set
`RANK_TABLES_MODEL` to move it.

**Configuration**:
```env
LLM_PROVIDER=azure          # or openai | groq | ollama
OPENAI_API_KEY=sk-xxx
LLM_MODEL=gpt-4o-mini       # Azure: the deployment name
```

> There is no `OPENAI_MODEL_PLANNING` or `OPENAI_MODEL_INTENT` variable — earlier revisions of this
> doc invented both. Per-agent model selection uses the override names listed above.

### Ollama (Offline)

Used by the Explorer when `EXPLORER_OFFLINE_ONLY=true` (default **`false`**), and by every agent
when `LLM_PROVIDER=ollama`.

Offline defaults: `llama3:latest` (table/column linking), `sqlcoder:latest` (NL→SQL),
`qwen2.5:7b` (general agents).

**Benefits**: no API costs · data privacy (on-premise) · works without internet.

**Caveat**: the api-gateway enforces a 30 s timeout on the Explorer's LLM call
([`explorer.go:3395`](../../api-gateway/internal/handlers/explorer.go#L3395)), so Explorer-on-Ollama
needs a GPU. Batch and CDC are unaffected.

**Configuration**:
```env
LLM_PROVIDER=ollama
OLLAMA_BASE_URL=http://ollama:11434   # /v1 is appended automatically
OLLAMA_MODEL=qwen2.5:7b
```

Full sizing and deployment options → [docs/deployment/ollama.md](../deployment/ollama.md).

---

## API Endpoints

### Tool Generator (Port 5010)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/v1/generate` | Generate new connector |
| POST | `/v1/validate` | Validate connector name |
| GET | `/v1/connectors` | List generated connectors |
| DELETE | `/v1/connectors/{name}` | Delete connector |
| GET | `/v1/capabilities` | List supported categories |
| POST | `/v1/oauth/authorize_url` | Get OAuth URL |
| POST | `/v1/oauth/exchange` | Exchange OAuth code |
| GET | `/health` | Health check |

### Planner (Port 5011)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/v1/plan` | Generate execution plan |
| POST | `/v1/plan/validate` | Validate plan |
| GET | `/v1/strategies` | List available strategies |
| GET | `/v1/cdc/providers` | List CDC providers |
| GET | `/health` | Health check |

### Internal (Called by Orchestrator)

| Agent | Endpoint | Description |
|-------|----------|-------------|
| ~~Intent~~ | ~~`/agents/intent/parse`~~ | **removed (PR #167)** — intent parsing is now the Go `IntentWorker`, not an llm-service endpoint |
| Resolver | `/agents/resolver/resolve` | Resolve connector name |
| Explorer | `/agents/explorer/nl2sql` | Convert NL to SQL |
| Explorer | `/agents/explorer/tables` | Resolve tables |
| Explorer | `/agents/explorer/columns` | Resolve columns |
| PII | `/agents/pii/scan` | Scan for PII |
| Suggestions | `/agents/suggestions/next` | Get suggestions |

---

## Configuration

```env
# Server
PORT=5010
PLANNER_PORT=5011

# OpenAI
OPENAI_API_KEY=sk-xxx
OPENAI_MODEL_PLANNING=gpt-4
OPENAI_MODEL_INTENT=gpt-3.5-turbo
OPENAI_TIMEOUT=60

# Ollama (offline)
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=llama3
USE_OLLAMA_FOR_EXPLORER=true

# Context7 (documentation service)
CONTEXT7_URL=http://localhost:5012
USE_CONTEXT7=true

# Tool Generator
CONNECTOR_OUTPUT_DIR=/app/connectors
DOCKER_REGISTRY=localhost:5000

# Logging
LOG_LEVEL=INFO
```

---

## Demo Highlights

1. **Intent Parsing** - Show NL understanding
2. **Typo Handling** - Type "postgress" and watch it resolve
3. **Connector Generation** - Create a new connector live
4. **NL2SQL** - Ask a business question, get SQL
5. **Offline Mode** - Show it works without internet

---

## Troubleshooting

### Agent timeout
```bash
# Check OpenAI API status
curl https://status.openai.com/api/v2/status.json

# Check Ollama
curl http://localhost:11434/api/tags
```

### Connector generation failed
```bash
# Check tool generator logs
docker-compose logs -f tool-generator

# Verify Context7 is running
curl http://localhost:5012/health
```

### NL2SQL incorrect
```bash
# Enable debug logging
export LOG_LEVEL=DEBUG

# Check Ollama model
ollama list
ollama pull llama3
```

---

*For more details, see the codebase at `/llm-service`*
