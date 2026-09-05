# Temporal Orchestration Documentation

**Technology**: Temporal.io, Go (34 `.go` files, no TypeScript)
**Ports**: 7233 (Server), 8233 (UI), 8082 (Adapter)
**Directory**: `backend-temporal-adapter/`

---

## Overview

Temporal provides the durable workflow orchestration layer for rsync-ai. It ensures pipelines execute reliably, survive failures, support long-running operations, and enable human-in-the-loop approval workflows. Temporal is used by companies like Netflix, Uber, and Stripe for mission-critical workflows.

---

## Architecture

```
                           ┌─────────────────────┐
                           │   API Gateway       │
                           │   (Go)              │
                           └──────────┬──────────┘
                                      │
                                      ▼
                           ┌─────────────────────┐
                           │  Temporal Adapter   │
                           │  (Port 8082)        │
                           │                     │
                           │  - Workflow Client  │
                           │  - Activity Worker  │
                           │  - Signal Handler   │
                           └──────────┬──────────┘
                                      │
                           ┌──────────┴──────────┐
                           │                     │
                           ▼                     ▼
              ┌─────────────────────┐  ┌─────────────────────┐
              │   Temporal Server   │  │   Temporal UI       │
              │   (Port 7233)       │  │   (Port 8233)       │
              │                     │  │                     │
              │  - Workflow Engine  │  │  - Workflow Monitor │
              │  - Task Queues      │  │  - Activity History │
              │  - Event History    │  │  - Debugging Tools  │
              └──────────┬──────────┘  └─────────────────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │    PostgreSQL       │
              │  (Workflow State)   │
              └─────────────────────┘
```

---

## Key Features

### 1. Durable Workflows

Workflows survive crashes and resume automatically:

```
Pipeline Execution
     │
     ▼
[Intent Stage] ──── Server crashes ────┐
     │                                 │
     │ <── Server restarts ────────────┘
     │
     ▼
[Discovery Stage] ── Continues from checkpoint
     │
     ▼
[Execution Stage]
     │
     ▼
  Complete
```

**How It Works**:
- Temporal persists workflow state to PostgreSQL
- Activities are recorded in event history
- On restart, workflow replays from last checkpoint
- No data loss, no manual intervention needed

### 2. Human-in-the-Loop (HITL)

Workflows can pause for user input:

```go
// Workflow code
func PipelineWorkflow(ctx workflow.Context, input PipelineInput) error {
    // ... discovery stage ...

    // Wait for user to select tables
    var selectedTables []string
    selector := workflow.NewSelector(ctx)
    selector.AddReceive(tableSelectionChannel, func(c workflow.ReceiveChannel, more bool) {
        c.Receive(ctx, &selectedTables)
    })
    selector.Select(ctx)

    // Continue with selected tables
    // ... planning stage ...
}
```

**Frontend Integration**:
```typescript
// Send signal from API
await temporal.workflow.signal(workflowId, 'tableSelection', {
  selectedTables: ['customers', 'orders']
});
```

### 3. Scheduling

Cron-based recurring pipelines:

```go
// Create scheduled workflow
scheduleHandle, err := client.ScheduleClient().Create(ctx, client.ScheduleOptions{
    ID: "daily-sync-pipeline-123",
    Spec: client.ScheduleSpec{
        CronExpressions: []string{"0 0 * * *"}, // Daily at midnight
    },
    Action: &client.ScheduleWorkflowAction{
        Workflow: PipelineWorkflow,
        Args:     []interface{}{pipelineInput},
    },
})
```

### 4. Retry Policies

Configurable retry with exponential backoff:

```go
activityOptions := workflow.ActivityOptions{
    StartToCloseTimeout: 5 * time.Minute,
    RetryPolicy: &temporal.RetryPolicy{
        InitialInterval:    time.Second,
        BackoffCoefficient: 2.0,
        MaximumInterval:    time.Minute,
        MaximumAttempts:    5,
    },
}
```

### 5. Long-Running Operations

Workflows can run for hours, days, or longer:

```go
// Heartbeat for long operations
for batch := range batches {
    activity.RecordHeartbeat(ctx, progressInfo{
        BatchNumber: batch.Number,
        RowsProcessed: batch.RowsProcessed,
    })
    processBatch(batch)
}
```

---

## Workflow: NLPipelineWorkflowV2

The main pipeline workflow:

```
┌─────────────────────────────────────────────────────────────────┐
│                   NLPipelineWorkflowV2                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Input: { user_message, context, options }                      │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Stage 1: Parse Intent                                    │   │
│  │                                                          │   │
│  │ Activity: ParseIntentActivity                            │   │
│  │ Input: user_message                                      │   │
│  │ Output: PipelineIntent                                   │   │
│  │ Timeout: 30s                                             │   │
│  │ Retries: 3                                               │   │
│  └─────────────────────────────────────────────────────────┘   │
│                          │                                      │
│                          ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Stage 2: Resolve Connections                             │   │
│  │                                                          │   │
│  │ Activity: ResolveConnectionsActivity                     │   │
│  │ Input: PipelineIntent                                    │   │
│  │ Output: ResolvedConnections                              │   │
│  │ Timeout: 15s                                             │   │
│  │ Retries: 3                                               │   │
│  └─────────────────────────────────────────────────────────┘   │
│                          │                                      │
│                          ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Stage 3: Discover Schema                                 │   │
│  │                                                          │   │
│  │ Activity: DiscoverSchemaActivity                         │   │
│  │ Input: source_connection_id                              │   │
│  │ Output: Schema (tables, columns)                         │   │
│  │ Timeout: 2m                                              │   │
│  │ Retries: 3                                               │   │
│  └─────────────────────────────────────────────────────────┘   │
│                          │                                      │
│                          ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ HITL Checkpoint: Table Selection                         │   │
│  │                                                          │   │
│  │ Signal: tableSelection                                   │   │
│  │ Wait: Until signal received                              │   │
│  │ Timeout: 24h (workflow continues waiting)                │   │
│  └─────────────────────────────────────────────────────────┘   │
│                          │                                      │
│                          ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Stage 4: Generate Plan                                   │   │
│  │                                                          │   │
│  │ Activity: GeneratePlanActivity                           │   │
│  │ Input: intent, schema, selected_tables                   │   │
│  │ Output: ExecutionPlan (stages, dependencies)             │   │
│  │ Timeout: 1m                                              │   │
│  │ Retries: 3                                               │   │
│  └─────────────────────────────────────────────────────────┘   │
│                          │                                      │
│                          ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ HITL Checkpoint: Plan Approval (optional)                │   │
│  │                                                          │   │
│  │ Signal: planApproval                                     │   │
│  │ Skip if: options.auto_approve = true                     │   │
│  └─────────────────────────────────────────────────────────┘   │
│                          │                                      │
│                          ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Stage 5: Validate Plan                                   │   │
│  │                                                          │   │
│  │ Activity: ValidatePlanActivity                           │   │
│  │ Input: ExecutionPlan                                     │   │
│  │ Output: ValidationResult                                 │   │
│  │ Timeout: 30s                                             │   │
│  │ Retries: 3                                               │   │
│  └─────────────────────────────────────────────────────────┘   │
│                          │                                      │
│                          ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Stage 6: Execute Plan                                    │   │
│  │                                                          │   │
│  │ Activity: ExecutePlanActivity                            │   │
│  │ Input: ExecutionPlan                                     │   │
│  │ Output: ExecutionResult                                  │   │
│  │ Timeout: 10h (long-running)                              │   │
│  │ Retries: 3 (per stage)                                   │   │
│  │ Heartbeat: Every 30s                                     │   │
│  └─────────────────────────────────────────────────────────┘   │
│                          │                                      │
│                          ▼                                      │
│  Output: { success, rows_synced, duration, errors }             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Activities

Activities encapsulate side effects:

### ParseIntentActivity

```go
func ParseIntentActivity(ctx context.Context, input ParseIntentInput) (*PipelineIntent, error) {
    // Call LLM Service
    resp, err := llmClient.ParseIntent(ctx, input.UserMessage)
    if err != nil {
        return nil, temporal.NewApplicationError("intent parsing failed", "INTENT_ERROR", err)
    }
    return resp, nil
}
```

### DiscoverSchemaActivity

```go
func DiscoverSchemaActivity(ctx context.Context, connectionID string) (*Schema, error) {
    // Call MCP connector
    connector := connectorRegistry.Get(connectionID)
    schema, err := connector.DiscoverSchema(ctx)
    if err != nil {
        return nil, err
    }
    return schema, nil
}
```

### ExecutePlanActivity

```go
func ExecutePlanActivity(ctx context.Context, plan ExecutionPlan) (*ExecutionResult, error) {
    result := &ExecutionResult{}

    for _, stage := range plan.Stages {
        // Heartbeat for long operations
        activity.RecordHeartbeat(ctx, StageProgress{
            StageName: stage.Name,
            Status:    "running",
        })

        // Execute stage
        stageResult, err := executeStage(ctx, stage)
        if err != nil {
            return nil, err
        }
        result.AddStageResult(stageResult)
    }

    return result, nil
}
```

---

## Temporal Adapter

The adapter bridges rsync-ai with Temporal:

**Location**: `/temporal-adapter`

**Responsibilities**:
- Workflow client management
- Activity worker registration
- Signal handling for HITL
- Configuration encryption/decryption (shared key with API Gateway)

### Configuration

```env
# Temporal
TEMPORAL_ADDRESS=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=rsync-pipelines

# Encryption (shared with API Gateway)
ENCRYPTION_KEY=***REMOVED***

# Kafka (for domain events)
KAFKA_BROKERS=localhost:9092
```

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/workflows/start` | Start new workflow |
| GET | `/workflows/:id` | Get workflow status |
| POST | `/workflows/:id/signal` | Send signal (HITL) |
| POST | `/workflows/:id/cancel` | Cancel workflow |
| GET | `/workflows/:id/history` | Get event history |
| POST | `/schedules` | Create schedule |
| DELETE | `/schedules/:id` | Delete schedule |

---

## Temporal UI

The Temporal UI (port 8233) provides:

### Workflow Monitoring

- List all workflows
- Filter by status, type, time range
- Search by workflow ID

### Workflow Detail

- Event history timeline
- Activity execution details
- Signal history
- Error messages

### Debugging

- Stack traces
- Input/output inspection
- Retry controls
- Termination options

**Demo Point**: Show the Temporal UI during demos - it's visually impressive.

---

## Signals (HITL)

### Table Selection Signal

```go
// Workflow receives signal
channel := workflow.GetSignalChannel(ctx, "tableSelection")
selector.AddReceive(channel, func(c workflow.ReceiveChannel, more bool) {
    var selection TableSelectionSignal
    c.Receive(ctx, &selection)
    selectedTables = selection.Tables
})
```

```typescript
// Frontend sends signal
await temporalClient.signal(workflowId, 'tableSelection', {
  tables: ['customers', 'orders', 'products']
});
```

### Plan Approval Signal

```go
// Workflow waits for approval
channel := workflow.GetSignalChannel(ctx, "planApproval")
var approval PlanApprovalSignal
channel.Receive(ctx, &approval)

if !approval.Approved {
    return nil, errors.New("plan rejected by user")
}
```

---

## Scheduling

### Create Schedule

```bash
# Via API
curl -X POST http://localhost:8082/schedules \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_id": "pipe_123",
    "cron": "0 * * * *",
    "timezone": "America/New_York"
  }'
```

### Common Cron Patterns

| Pattern | Description |
|---------|-------------|
| `0 * * * *` | Every hour |
| `0 0 * * *` | Daily at midnight |
| `0 0 * * 0` | Weekly on Sunday |
| `0 0 1 * *` | Monthly on the 1st |
| `*/15 * * * *` | Every 15 minutes |

### Pause/Resume

```bash
# Pause schedule
curl -X POST http://localhost:8082/schedules/sched_123/pause

# Resume schedule
curl -X POST http://localhost:8082/schedules/sched_123/resume
```

---

## Error Handling

### Activity Failure

```go
// Custom error types
temporal.NewApplicationError("Connection timeout", "TIMEOUT_ERROR", originalErr)
temporal.NewNonRetryableApplicationError("Invalid config", "CONFIG_ERROR", nil)
```

### Workflow Failure

```go
// Workflow-level error handling
defer func() {
    if r := recover(); r != nil {
        workflow.GetLogger(ctx).Error("Workflow panic", "error", r)
    }
}()
```

### Retry Configuration

```go
// Activity with custom retry
activityOptions := workflow.ActivityOptions{
    RetryPolicy: &temporal.RetryPolicy{
        InitialInterval:        time.Second,
        BackoffCoefficient:     2.0,
        MaximumInterval:        time.Minute,
        MaximumAttempts:        5,
        NonRetryableErrorTypes: []string{"CONFIG_ERROR"},
    },
}
```

---

## Monitoring

### Prometheus Metrics

Temporal exposes Prometheus metrics:

```
# Workflow metrics
temporal_workflow_completed_total
temporal_workflow_failed_total
temporal_workflow_active_count

# Activity metrics
temporal_activity_execution_latency_seconds
temporal_activity_failed_total

# Schedule metrics
temporal_schedule_run_total
```

### Health Check

```bash
curl http://localhost:7233/health
```

---

## Demo Highlights

1. **Temporal UI** - Show workflow visualization
2. **Event History** - Walk through activity execution
3. **HITL Signal** - Demonstrate approval workflow
4. **Failure Recovery** - Simulate crash and auto-resume
5. **Scheduling** - Create and view recurring pipeline

---

## Troubleshooting

### Workflow stuck waiting

```bash
# Check for pending signals
temporal workflow describe --workflow-id <id>

# Send signal manually
temporal workflow signal --workflow-id <id> --name tableSelection --input '{"tables":["users"]}'
```

### Activity timeout

```bash
# Check activity logs
docker-compose logs temporal-adapter | grep "activity"

# Increase timeout
# Edit workflow code, redeploy
```

### Temporal server not starting

```bash
# Check PostgreSQL connectivity
docker-compose logs temporal

# Verify database exists
docker-compose exec postgres psql -U temporal -d temporal -c "\dt"
```

### Workflows not executing

```bash
# Check worker is running
docker-compose logs temporal-adapter

# Verify task queue
temporal task-queue describe --task-queue rsync-pipelines
```

---

## Advanced Configuration

### Namespace Isolation

```bash
# Create namespace for production
temporal operator namespace create --namespace production

# Use in config
TEMPORAL_NAMESPACE=production
```

### Worker Tuning

```go
// High-throughput worker
workerOptions := worker.Options{
    MaxConcurrentActivityExecutionSize:     100,
    MaxConcurrentWorkflowTaskExecutionSize: 100,
    MaxConcurrentLocalActivityExecutionSize: 100,
}
```

### Encryption at Rest

Connection configs are encrypted:

```go
// Encrypt before storing
encryptedConfig := encrypt(connectionConfig, encryptionKey)

// Decrypt when needed
decryptedConfig := decrypt(encryptedConfig, encryptionKey)
```

---

*For more details, see the codebase at `/temporal-adapter` and the Temporal documentation at https://docs.temporal.io*
