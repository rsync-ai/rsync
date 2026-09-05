# Retention Manager Agent (Go)

High-performance data lifecycle management for Kafka topics with intelligent cleanup and archival.

## Overview

The Retention Manager Agent manages the lifecycle of bulk data in Kafka topics. It provides:

- **Bulk Load Detection**: Automatically detect and track large data loads
- **Consumer Progress Tracking**: Monitor consumer offset progress per topic
- **Safety Checks**: Comprehensive safety checks before cleanup
- **Intelligent Cleanup**: Reduce retention, delete records, or archive to S3
- **Automatic Restoration**: Restore original retention after cleanup

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Retention Manager Agent                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Offset       │  │ Bulk Load    │  │ Safety       │      │
│  │ Tracker      │  │ Detector     │  │ Checker      │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│          │                │                 │               │
│          └────────────────┼─────────────────┘               │
│                           │                                 │
│              ┌────────────▼────────────┐                   │
│              │  Cleanup Manager        │                   │
│              │  (Retention/Archive)    │                   │
│              └─────────────────────────┘                   │
│                           │                                 │
│              ┌────────────▼────────────┐                   │
│              │  Retention Agent        │                   │
│              │  (Orchestrator)         │                   │
│              └─────────────────────────┘                   │
│                           │                                 │
│              ┌────────────▼────────────┐                   │
│              │  HTTP Handlers          │                   │
│              │  (REST API)             │                   │
│              └─────────────────────────┘                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Components

### 1. Types (`types.go`)

Core data structures:
- `BulkLoadStatus`: Lifecycle states (active, monitoring, completed, cleaned, archived)
- `BulkLoadInfo`: Full information about a bulk data load
- `ConsumerProgress`: Progress tracking for consumer groups
- `TopicRetention`: Retention configuration for topics
- `CleanupResult`: Result of cleanup operations
- `SafetyCheckResult`: Result of safety checks

### 2. Config (`config.go`)

Configuration with environment variable overrides:
- `RetentionConfig`: Default and cleanup retention settings
- `SafetyConfig`: Safety check thresholds
- `ArchiveConfig`: S3 archival settings
- `MonitoringConfig`: Monitoring intervals

### 3. Offset Tracker (`offset_tracker.go`)

Tracks consumer group offsets:
- Get consumer group offsets
- Calculate consumer progress
- Check if all consumers caught up
- Simulate offsets when Kafka REST proxy unavailable

### 4. Bulk Load Detector (`bulk_detection.go`)

Detects and manages bulk loads:
- Register bulk loads manually
- Auto-detect from offset changes
- Track progress and status
- Cleanup old bulk load records

### 5. Safety Checker (`safety.go`)

Performs safety checks before cleanup:
- Minimum wait time after load
- All consumers caught up
- No consumers with high lag
- Grace period elapsed
- Minimum consumers caught up

### 6. Cleanup Manager (`cleanup.go`)

Executes cleanup operations:
- Reduce retention (default)
- Delete records (if supported)
- Archive to S3 (if enabled)
- Compact topic
- Restore original retention

### 7. Agent (`agent.go`)

Main orchestrator:
- Monitoring loop for active bulk loads
- Cleanup check loop
- Retention restore loop
- Public API for management

### 8. Handlers (`handlers.go`)

HTTP API handlers:
- Agent control (start/stop)
- Bulk load management
- Consumer progress queries
- Cleanup operations

## API Endpoints

All endpoints available at `/api/v1/retention/`:

### Agent Control

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/status` | Get agent status |
| POST | `/start` | Start the agent |
| POST | `/stop` | Stop the agent |

### Bulk Load Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/bulk-loads` | Register a bulk load |
| POST | `/bulk-loads/detect` | Auto-detect bulk load |
| GET | `/bulk-loads` | List all bulk loads |
| GET | `/bulk-loads/active` | List active bulk loads |
| GET | `/bulk-loads/:id` | Get specific bulk load |
| GET | `/bulk-loads/:id/progress` | Get progress |
| GET | `/bulk-loads/:id/safety-check` | Check cleanup readiness |
| POST | `/bulk-loads/:id/cleanup` | Trigger cleanup |

### Consumer Progress

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/progress/:group/:topic` | Get consumer progress |

### Cleanup History

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/history` | Get cleanup history |
| GET | `/history/:topic` | Get history for topic |

### Modified Topics

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/modified-topics` | Get modified topics |
| POST | `/restore/:topic` | Restore retention |
| POST | `/restore-all` | Restore all retentions |

## Configuration

Environment variables:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Retention
RETENTION_DEFAULT_MS=604800000        # 7 days
RETENTION_CLEANUP_MS=3600000          # 1 hour
RETENTION_BULK_THRESHOLD=100000       # 100K messages

# Safety
RETENTION_MIN_WAIT_MINS=30
RETENTION_GRACE_PERIOD_MINS=15
RETENTION_MAX_LAG=1000

# Archive (optional)
RETENTION_ARCHIVE_ENABLED=false
RETENTION_S3_BUCKET=my-archive-bucket
RETENTION_S3_PREFIX=kafka-archive

# Monitoring
RETENTION_CHECK_INTERVAL=60
RETENTION_CLEANUP_CHECK_INTERVAL=300

# Agent
ENABLE_RETENTION_AGENT=true
```

## Usage Examples

### Register a Bulk Load

```bash
curl -X POST http://localhost:8080/api/v1/retention/bulk-loads \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "cdc.prod-mysql",
    "pipeline_id": "pipeline-123",
    "start_offset": 0,
    "end_offset": 1000000,
    "expected_consumers": ["consumer-group-1", "consumer-group-2"]
  }'
```

### Check Cleanup Readiness

```bash
curl http://localhost:8080/api/v1/retention/bulk-loads/bulk-abc123/safety-check
```

### Trigger Cleanup

```bash
curl -X POST http://localhost:8080/api/v1/retention/bulk-loads/bulk-abc123/cleanup \
  -H "Content-Type: application/json" \
  -d '{"action": "reduce_retention"}'
```

### Get Consumer Progress

```bash
curl http://localhost:8080/api/v1/retention/progress/my-consumer-group/cdc.prod-mysql?target_offset=1000000
```

## Safety Checks

Before any cleanup, the following checks are performed:

1. **Minimum Wait Time**: Ensure enough time has passed since bulk load registered
2. **All Consumers Caught Up**: All expected consumers have processed the data
3. **No High Lag**: No consumers have lag exceeding threshold
4. **Grace Period**: Wait after all consumers caught up for late-joining consumers
5. **Minimum Consumers**: At least N consumers must be caught up

## Cleanup Actions

| Action | Description | Use Case |
|--------|-------------|----------|
| `reduce_retention` | Reduce retention to 1 hour | Default, safe |
| `delete_records` | Delete records up to offset | Fast cleanup |
| `archive_to_s3` | Archive then cleanup | Compliance |
| `compact` | Enable log compaction | Key-based data |

## Testing

Run tests:

```bash
cd backend-orchestrator
go test -v ./internal/agents/retention/...
```

## Integration

The retention agent is automatically started when the backend-orchestrator runs:

```go
// In main.go
retentionAgent := retention.NewAgent(retentionConfig)
retentionAgent.Start(ctx)
defer retentionAgent.Stop()
```

## Data Lifecycle Flow

```
[1] Bulk Data Load Starts
    - Historical data loaded to Kafka topic
    - E.g., 10 million rows from MySQL
    ↓
[2] Bulk Load Registered
    - Either manually or auto-detected
    - Start/end offsets recorded
    - Expected consumers specified
    ↓
[3] Monitoring Phase
    - Agent checks consumer progress every 60s
    - Tracks which consumers have caught up
    ↓
[4] All Consumers Caught Up
    - All consumers processed past end_offset
    - Status changes to "monitoring"
    ↓
[5] Grace Period
    - Wait 15 minutes for late-joining consumers
    - Status changes to "completed"
    ↓
[6] Safety Checks
    - All 5 safety checks must pass
    ↓
[7] Cleanup Triggered
    - Retention reduced to 1 hour
    - Kafka auto-deletes old segments
    ↓
[8] Retention Restored
    - After cleanup, restore to 7 days
    - Topic ready for future CDC events
```

## Future Enhancements

- [ ] Kafka Admin API integration (instead of REST proxy)
- [ ] Real S3 archival implementation
- [ ] Prometheus metrics export
- [ ] LLM-enhanced cleanup decisions
- [ ] Multi-tenant isolation
