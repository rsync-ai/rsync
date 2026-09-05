## Kafka Connect (Debezium) — HLD

### Purpose
The `kafka-connect` service provides **CDC ingestion** using Debezium connectors, with custom SMTs to route events into rsync-ai topic conventions.

In this repo, `kafka-connect` is used for:
- MySQL/Postgres/MongoDB/SQL Server/Oracle CDC connectors (Debezium),
- optional schema registry integration,
- topic routing and partition-key header injection via RSync SMTs.

### Runtime Interface
- **Container**: `kafka-connect`
- **Port**: `8083` (Kafka Connect REST API)
- **Profile**: enabled behind docker-compose `demo` profile in `docker-compose.yml`

### Responsibilities
- Runs Debezium connector tasks (snapshot + streaming).
- Routes events to rsync-ai topics using SMTs:
  - topic routing to a connection-level topic
  - partition key headers for downstream ordering and “hot entity” distribution.

### Dependencies
- Kafka broker(s)
- (Optional) Schema Registry (when Avro enabled)
- Source databases (MySQL/Postgres/etc.) in E2E/demo environments

### Observability
- Kafka Connect REST health checks
- Logs via docker logging pipeline


