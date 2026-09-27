# Backend Orchestrator Service Documentation

**Technology**: Go 1.24, Kafka, Redis, PostgreSQL
**Port**: 8081
**Directory**: `/backend-orchestrator`

---

## Overview

The Backend Orchestrator is the brain of rsync-ai - it manages pipeline state machines, coordinates worker execution, and ensures reliable data movement. Agent stages hand off through the Redis correlation store: the Temporal adapter writes one request per stage and each worker polls for its own. Kafka carries pipeline domain events and batch/CDC data, not agent requests.

---

## Architecture

```
                     ┌─────────────────────┐
                     │   Temporal adapter  │
                     │  (workflow stages)  │
                     └──────────┬──────────┘
                                │ one request per stage
                                ▼
                     ┌─────────────────────┐
                     │ Redis correlation   │
                     │ store               │
                     └──────────┬──────────┘
                                │ polled by
    ┌────────┬──────────┬───────┴──┬─────────┬─────────┬─────────┐
    ▼        ▼          ▼          ▼         ▼         ▼         ▼
┌──────┐ ┌──────────┐ ┌─────────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌────────┐
│Intent│ │Capability│ │Connect- │ │Planner│ │Valid- │ │Cost   │ │Executor│
│      │ │resolver  │ │ion val. │ │       │ │ator   │ │estim. │ │        │
└──────┘ └──────────┘ └─────────┘ └───────┘ └───────┘ └───────┘ └────────┘

Kafka (events + batch/CDC data) · PostgreSQL (persistence)
```

---

## Key Features

### 1. Stage Hand-off

Each worker polls the Redis correlation store for requests of its own agent type
(the `*_redis_polling.go` files in `backend-orchestrator/internal/workers/`) and
claims one before working on it. No worker joins a Kafka consumer group: the agent
Kafka bus (`task.assignments`, `task.results`, `pipeline.agent.telemetry`) was removed in
[#1227](https://github.com/rsync-ai/rsync-ai/pull/1227).

**Kafka Topics** the orchestrator still uses:
- `pipeline.domain.events` - UI real-time updates
- `rsync.notifications` - Slack and email alerts
- per-pipeline batch and CDC data topics

Full catalogue: [kafka-topics.md](../architecture/kafka-topics.md).

### 2. Stateless Worker Architecture

Workers are stateless and horizontally scalable:

| Worker | Responsibility | LLM Dependency |
|--------|----------------|----------------|
| **Intent** | Parse NL to structured intent | Yes |
| **Capability resolver** | Resolve connector types and the workspace | No |
| **Connection validator** | Select and probe saved connections | No |
| **Planner** | Generate execution plans | Yes |
| **Validator** | Validate configurations | No |
| **Cost estimator** | Estimate run cost | No |
| **Executor** | Move data between systems | No |

There is no separate Resolver or Discovery worker. Live schema discovery is the executor
agent's `DiscoverSchema`, served over HTTP as `POST /agent/discover-schema`.

### 3. State Management

**Redis State Store**:
- Pipeline execution state
- Correlation IDs for request/reply
- Temporary data caching

**PostgreSQL Persistence**:
- Pipeline definitions
- Execution history
- Connection configurations

### 4. Sentinel Agent

Self-healing infrastructure component:
- Monitors worker health
- Restarts failed processes
- Alerts on anomalies
- Tracks resource usage

### 5. Domain Event Publishing

Real-time updates for UI:
```go
event := DomainEvent{
    Type:       "pipeline.stage.completed",
    PipelineID: "pipe_123",
    Stage:      "discovery",
    Timestamp:  time.Now(),
    Payload: map[string]interface{}{
        "tables_found": 15,
        "duration_ms":  1250,
    },
}
kafka.Publish("pipeline.domain.events", event)
```

---

## Worker Details

### Intent Worker

**Purpose**: Parse natural language into structured pipeline intent.

**Input**:
```json
{
  "user_message": "Sync customers from MySQL to S3 hourly",
  "context": {
    "user_id": "user_123",
    "available_connections": ["mysql_prod", "s3_warehouse"]
  }
}
```

**Output**:
```json
{
  "intent": {
    "action": "sync",
    "source": {
      "connector_type": "mysql",
      "connection_hint": "mysql_prod",
      "tables": ["customers"]
    },
    "destination": {
      "connector_type": "s3",
      "connection_hint": "s3_warehouse"
    },
    "schedule": "0 * * * *",
    "confidence": 0.95
  }
}
```

**Demo Point**: Show how typos and variations are handled (e.g., "postgress" → "postgresql")

---

### Resolver Worker

> There is no `resolver.go` worker any more: this work is split between
> `capability_resolver.go` and `connection_validator.go` (see the table above).

**Purpose**: Validate and resolve connection references.

**Input**:
```json
{
  "intent": {
    "source": {"connection_hint": "mysql_prod"},
    "destination": {"connection_hint": "s3_warehouse"}
  }
}
```

**Output**:
```json
{
  "resolved": {
    "source_connection_id": "conn_123",
    "source_connector_type": "mysql",
    "destination_connection_id": "conn_456",
    "destination_connector_type": "s3"
  },
  "validation": {
    "source_reachable": true,
    "destination_writable": true
  }
}
```

---

### Discovery Worker

> There is no `discovery.go` worker any more: schema discovery is the executor agent's
> `DiscoverSchema`, served as `POST /agent/discover-schema`.

**Purpose**: Auto-discover schemas from data sources.

**Input**:
```json
{
  "connection_id": "conn_123",
  "connector_type": "mysql"
}
```

**Output**:
```json
{
  "schema": {
    "tables": [
      {
        "name": "customers",
        "columns": [
          {"name": "id", "type": "INT", "primary_key": true},
          {"name": "email", "type": "VARCHAR(255)"},
          {"name": "created_at", "type": "TIMESTAMP"}
        ],
        "row_count": 50000
      }
    ]
  }
}
```

**Demo Point**: Show real-time schema discovery animation

---

### Planner Worker

**Purpose**: Generate optimal execution plans.

**Input**:
```json
{
  "intent": {...},
  "schema": {...},
  "selected_tables": ["customers", "orders"]
}
```

**Output**:
```json
{
  "plan": {
    "strategy": "batch",
    "stages": [
      {
        "id": "stage_1",
        "operation": "export",
        "source": "mysql",
        "table": "customers",
        "batch_size": 10000
      },
      {
        "id": "stage_2",
        "operation": "import",
        "destination": "s3",
        "table": "customers",
        "format": "csv",
        "compression": "gzip"
      }
    ],
    "estimated_duration_seconds": 120
  }
}
```

**Strategies**:
- **Batch** - Traditional extract-load for static data
- **CDC** - Real-time streaming for live data
- **Hybrid** - Initial batch + ongoing CDC

---

### Validator Worker

**Purpose**: Validate plan feasibility before execution.

**Checks**:
- Source connection accessible
- Destination connection writable
- Required permissions present
- Resource limits not exceeded
- No circular dependencies

---

### Executor Worker

**Purpose**: Execute data movement operations.

**Process**:
1. Initialize source connection
2. Initialize destination connection
3. For each batch:
   - Extract from source
   - Transform if needed
   - Load to destination
4. Verify row counts
5. Emit completion event

**Progress Tracking**:
```json
{
  "stage": "execution",
  "table": "customers",
  "rows_processed": 25000,
  "rows_total": 50000,
  "percentage": 50,
  "throughput_rows_per_sec": 5000
}
```

---

## Pipeline Execution Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    Pipeline Lifecycle                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  User Request                                               │
│       │                                                     │
│       ▼                                                     │
│  ┌─────────┐                                                │
│  │ PENDING │ ─────► Pipeline created, waiting to start     │
│  └────┬────┘                                                │
│       │                                                     │
│       ▼                                                     │
│  ┌─────────┐                                                │
│  │ INTENT  │ ─────► AI parsing natural language            │
│  └────┬────┘                                                │
│       │                                                     │
│       ▼                                                     │
│  ┌──────────┐                                               │
│  │RESOLUTION│ ─────► Validating connections                │
│  └────┬─────┘                                               │
│       │                                                     │
│       ▼                                                     │
│  ┌──────────┐                                               │
│  │DISCOVERY │ ─────► Fetching schemas                      │
│  └────┬─────┘                                               │
│       │                                                     │
│       ▼                                                     │
│  ┌────────────────┐                                         │
│  │ HITL: TABLES   │ ─────► User selects tables (optional)  │
│  └───────┬────────┘                                         │
│          │                                                  │
│          ▼                                                  │
│  ┌──────────┐                                               │
│  │ PLANNING │ ─────► AI generating execution plan          │
│  └────┬─────┘                                               │
│       │                                                     │
│       ▼                                                     │
│  ┌────────────────┐                                         │
│  │ HITL: PLAN     │ ─────► User approves plan (optional)   │
│  └───────┬────────┘                                         │
│          │                                                  │
│          ▼                                                  │
│  ┌───────────┐                                              │
│  │VALIDATION │ ─────► Final checks before execution        │
│  └────┬──────┘                                              │
│       │                                                     │
│       ▼                                                     │
│  ┌──────────┐                                               │
│  │EXECUTION │ ─────► Moving data                           │
│  └────┬─────┘                                               │
│       │                                                     │
│       ▼                                                     │
│  ┌──────────┐                                               │
│  │ COMPLETE │ ─────► Pipeline finished successfully        │
│  └──────────┘                                               │
│                                                             │
│  At any point: ─────► FAILED (with retry) or CANCELLED     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/metrics` | Prometheus metrics |
| GET | `/workers` | List active workers |
| POST | `/workers/:type/scale` | Scale worker count |
| GET | `/pipelines/:id/state` | Get pipeline state |
| POST | `/pipelines/:id/retry` | Retry failed stage |

---

## Configuration

```env
# Server
PORT=8081

# Kafka
KAFKA_BROKERS=localhost:9092

# Redis
REDIS_URL=redis://localhost:6379

# Database
DATABASE_URL=postgres://user:pass@localhost:5432/rsync_db

# LLM Service
LLM_SERVICE_URL=http://localhost:5010

# Workers: all 7 run in-process; there is no per-worker count variable

# Observability
OTEL_EXPORTER_ENDPOINT=localhost:14317
```

---

## Horizontal Scaling

```
                   Load Balancer
                        │
        ┌───────────────┼───────────────┐
        │               │               │
        ▼               ▼               ▼
   ┌─────────┐     ┌─────────┐     ┌─────────┐
   │Orch #1  │     │Orch #2  │     │Orch #3  │
   └────┬────┘     └────┬────┘     └────┬────┘
        │               │               │
        └───────────────┼───────────────┘
                        │
              Redis correlation store
                        │
        ┌───────────────┼───────────────┐
        │               │               │
        ▼               ▼               ▼
   ┌─────────┐     ┌─────────┐     ┌─────────┐
   │Worker   │     │Worker   │     │Worker   │
   │Pool #1  │     │Pool #2  │     │Pool #3  │
   └─────────┘     └─────────┘     └─────────┘
```

**Scaling Rules**:
- Add orchestrator instances for API capacity
- Add workers for processing capacity
- Each request in the Redis correlation store is claimed by one poller

---

## Demo Highlights

1. **Stage Visualization** - Watch pipeline progress through stages
2. **Worker Activity** - Show workers processing tasks
3. **Real-time Events** - Domain events updating UI
4. **Failure Recovery** - Simulate failure and auto-retry
5. **Scaling Demo** - Show adding workers dynamically

---

## Troubleshooting

### Pipeline stuck
```bash
# Check worker logs
docker-compose logs -f backend-orchestrator

# Check the stage pollers started (workers poll Redis, not Kafka)
docker-compose logs backend-orchestrator | grep "Redis poller started"
```

### Workers not processing
```bash
# Verify Kafka connectivity
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check Redis state
docker-compose exec redis redis-cli KEYS "pipeline:*"
```

### Stage timeout
```bash
# Check LLM service for intent/planner
curl http://localhost:5010/health

# Check MCP connectors for discovery/execution
docker-compose ps | grep mcp
```

---

*For more details, see the codebase at `/backend-orchestrator`*
