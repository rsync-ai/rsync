# Consumer Registry Agent (Go)

High-performance Kafka consumer management with auto-scaling and auto-restart capabilities.

## Overview

The Consumer Registry Agent is a production-grade Go implementation for dynamic Kafka consumer lifecycle management. It provides:

- **Dynamic Consumer Spawning**: Spawn consumers as Docker containers or simulate for testing
- **Health Monitoring**: Track heartbeats, lag, throughput, and error rates
- **Auto-Scaling**: Scale consumers up/down based on lag and throughput
- **Auto-Restart**: Automatically restart failed or dead consumers
- **REST API**: Full HTTP API for management and monitoring

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Consumer Registry Agent                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Health       │  │ Scaling      │  │ Docker       │      │
│  │ Monitor      │  │ Rules        │  │ Spawner      │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│          │                │                 │               │
│          └────────────────┼─────────────────┘               │
│                           │                                 │
│              ┌────────────▼────────────┐                   │
│              │  Consumer Registry      │                   │
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
- `ConsumerState`: Lifecycle states (starting, running, unhealthy, stopped, failed)
- `HealthStatus`: Health states (healthy, degraded, unhealthy, dead)
- `ConsumerHealth`: Health metrics for a consumer
- `ConsumerInfo`: Full consumer information
- `ScalingDecision`: Scaling recommendations

### 2. Config (`config.go`)

Configuration with environment variable overrides:
- `KafkaConfig`: Kafka connection settings
- `ScalingConfig`: Auto-scaling thresholds
- `HealthConfig`: Health monitoring settings
- `DockerConfig`: Container deployment settings

### 3. Health Monitor (`health.go`)

Continuous health monitoring:
- Registers/unregisters consumers
- Records heartbeats and failures
- Updates metrics (lag, throughput)
- Triggers callbacks on status changes

### 4. Scaling Rules (`scaling.go`)

Intelligent scaling decisions:
- Rule-based evaluation
- Cooldown management
- Decision history tracking

### 5. Spawner (`spawner.go`)

Consumer lifecycle management:
- Docker API integration via HTTP
- Simulated mode when Docker unavailable
- Container create/start/stop/remove

### 6. Registry (`registry.go`)

Main orchestrator:
- Coordinates all components
- Background scaling loop
- Background restart loop
- Consumer management API

### 7. Handlers (`handlers.go`)

HTTP API handlers:
- Agent control (start/stop)
- Consumer management (spawn/terminate/restart)
- Health queries
- Scaling operations

## API Endpoints

### Agent Control

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/consumers/status` | Get agent status |
| POST | `/api/v1/consumers/start` | Start the agent |
| POST | `/api/v1/consumers/stop` | Stop the agent |

### Consumer Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/consumers/consumers` | List all consumers |
| GET | `/api/v1/consumers/consumers/:id` | Get specific consumer |
| POST | `/api/v1/consumers/consumers/spawn` | Spawn new consumers |
| POST | `/api/v1/consumers/consumers/terminate` | Terminate consumers |
| POST | `/api/v1/consumers/consumers/restart` | Restart a consumer |

### Health

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/consumers/health/summary` | Get health summary |
| GET | `/api/v1/consumers/health/:id` | Get consumer health |

### Scaling

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/consumers/scaling/:topic` | Get scaling recommendation |
| POST | `/api/v1/consumers/scaling/:topic/apply` | Apply scaling |
| POST | `/api/v1/consumers/scaling/manual` | Manual scale |
| GET | `/api/v1/consumers/scaling/history` | Get scaling history |
| GET | `/api/v1/consumers/scaling/cooldowns` | Get topics in cooldown |

### Topics

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/consumers/topics` | List managed topics |
| GET | `/api/v1/consumers/topics/:topic/consumers` | Get topic consumers |

## Configuration

Environment variables:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Scaling
CONSUMER_LAG_SCALE_UP=50000
CONSUMER_LAG_SCALE_DOWN=1000
CONSUMER_MAX_PER_TOPIC=10
CONSUMER_MIN_PER_TOPIC=1
CONSUMER_SCALE_COOLDOWN=300

# Health
CONSUMER_HEALTH_INTERVAL=30
CONSUMER_AUTO_RESTART=true
CONSUMER_MAX_RESTARTS=5

# Docker
DOCKER_NETWORK=rsync-network
CONSUMER_IMAGE=rsync-ai/consumer:latest

# Agent
ENABLE_CONSUMER_AGENT=true
CONSUMER_AUTO_SCALE=true
CONSUMER_GROUP_PREFIX=rsync-pipeline
```

## Usage Examples

### Spawn Consumers

```bash
curl -X POST http://localhost:8080/api/v1/consumers/consumers/spawn \
  -H "Content-Type: application/json" \
  -d '{
    "group_id": "pipeline-group",
    "topic": "cdc.prod-mysql",
    "pipeline_id": "pipeline-123",
    "count": 3
  }'
```

### Get Scaling Recommendation

```bash
curl http://localhost:8080/api/v1/consumers/scaling/cdc.prod-mysql
```

### Manual Scale

```bash
curl -X POST http://localhost:8080/api/v1/consumers/scaling/manual \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "cdc.prod-mysql",
    "target_count": 5
  }'
```

### Get Health Summary

```bash
curl http://localhost:8080/api/v1/consumers/health/summary
```

## Scaling Rules

The scaling engine evaluates based on:

1. **Replace Unhealthy**: Dead/unhealthy consumers are replaced first
2. **Scale Up for Lag**: If `lag > LagScaleUpThreshold`
3. **Scale Down for Low Lag**: If `lag < LagScaleDownThreshold`
4. **Partition Matching**: Optionally match consumer count to partitions

### Cooldown

After any scaling action, the topic enters a cooldown period (default 5 minutes) to prevent thrashing.

## Testing

Run tests:

```bash
cd backend-orchestrator
go test -v ./internal/agents/consumer/...
```

## Performance

The Go implementation provides:
- **Low latency**: Sub-millisecond health checks
- **High throughput**: Handle thousands of consumers
- **Efficient memory**: Minimal allocations
- **Concurrent safety**: Thread-safe with fine-grained locking

## Integration

The consumer registry is automatically started when the backend-orchestrator runs:

```go
// In main.go
consumerRegistry, err := consumer.NewRegistry(
    consumerConfig,
    true,  // auto-scale
    true,  // auto-restart
)
consumerRegistry.Start(ctx)
defer consumerRegistry.Stop()
```

## Future Enhancements

- [ ] Kubernetes pod spawner
- [ ] Prometheus metrics export
- [ ] LLM-enhanced scaling decisions
- [ ] Predictive scaling based on historical patterns
