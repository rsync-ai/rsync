## Kafka Connect (Debezium) — LLD

### Repo Location
- `shared/internal/infra/kafka-connect/`

### Image Build
- Dockerfile: `shared/internal/infra/kafka-connect/Dockerfile`
  - multi-stage:
    - builds SMT JAR with Maven
    - installs Debezium connector plugins (via Maven Central)
    - installs Java 17 for Debezium 3.4 compatibility

### Custom SMTs
- `shared/internal/infra/kafka-connect/smt/src/main/java/com/rsync/kafka/smt/TopicRouter.java`
  - routes table-level Debezium topics → `cdc.<connection>`
  - adds headers like `rsync.original.topic`, `rsync.table`, etc.
- `shared/internal/infra/kafka-connect/smt/src/main/java/com/rsync/kafka/smt/PartitionKeyHeader.java`
  - adds headers for partition key strategy

### Config Surface
Kafka Connect is configured via env vars in docker-compose (examples):
- `CONNECT_BOOTSTRAP_SERVERS`
- `CONNECT_*` topic names and converter settings
- `CONNECT_PLUGIN_PATH`

### Health
- Docker healthcheck: `curl -f http://localhost:8083/`


