## Schema Registry — HLD

### Purpose
**Apicurio Registry** (Confluent-compatible API) is an **optional dependency** used when rsync-ai is configured to serialize Kafka messages using Avro. It replaces Confluent Schema Registry (CCL-licensed) — rsync-ai never uses the Confluent image.

### Runtime Interface
- **Compose service**: `schema-registry`
- **Image**: `apicurio/apicurio-registry:3.0.6`
- **Profile**: `schema-registry` (not started by default)
- **Host port**: `8085` → container `8080`

### Dependencies
- Kafka broker
- Used by:
  - API Gateway and Orchestrator when `KAFKA_USE_AVRO=true`


