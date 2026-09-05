## Kafka (Event Bus) — HLD

### Purpose
Kafka is the **message bus** for rsync-ai:
- agent command + result topics,
- pipeline domain events and CDC topics,
- (optionally) schema-registry backed Avro payloads.

### Runtime Interface
- **Compose service**: `kafka`
- **Image**: `confluentinc/cp-kafka:7.6.1`
- **Host ports**:
  - `9092` (client access from host)
  - `9101` (JMX)
- **Mode**: KRaft (no ZooKeeper)

### Dependencies
Used by:
- API Gateway
- Orchestrator
- Temporal Adapter
- Planner (optional)
- Kafka Connect (CDC)

### Key Reliability Settings (dev compose)
- group rebalance delay + session timeouts tuned to reduce consumer thrash
- listeners bound to `0.0.0.0` to avoid i/o timeouts


