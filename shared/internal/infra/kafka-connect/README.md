# RSync Kafka Connect SMT

Custom Single Message Transforms (SMT) for Debezium CDC routing.

## Overview

This module provides custom Kafka Connect SMTs that route all CDC events from multiple tables to a single connection-level topic, enabling:

- **Simplified Topic Management**: One topic per connection instead of one per table
- **Consistent Partition Keys**: Headers for downstream ordering guarantees
- **Hot Entity Detection**: Partition key format supports both base and full keys

## Components

### 1. TopicRouter (`com.rsync.kafka.smt.TopicRouter`)

Routes all Debezium CDC events to a single topic per connection.

**Configuration:**

```json
{
  "transforms": "route",
  "transforms.route.type": "com.rsync.kafka.smt.TopicRouter",
  "transforms.route.topic.format": "cdc.${connection}",
  "transforms.route.connection.name": "prod-mysql",
  "transforms.route.preserve.original.topic": "true"
}
```

**Headers Added:**
- `rsync.original.topic` - Original Debezium topic name
- `rsync.connection` - Connection name
- `rsync.database` - Database name
- `rsync.schema` - Schema name
- `rsync.table` - Table name

### 2. PartitionKeyHeader (`com.rsync.kafka.smt.PartitionKeyHeader`)

Adds partition key information as headers for downstream consumers.

**Configuration:**

```json
{
  "transforms": "addPartitionKey",
  "transforms.addPartitionKey.type": "com.rsync.kafka.smt.PartitionKeyHeader",
  "transforms.addPartitionKey.pk.fields": "id",
  "transforms.addPartitionKey.pk.separator": "_",
  "transforms.addPartitionKey.key.format": "${connection}.${schema}.${table}"
}
```

**Headers Added:**
- `rsync.partition.key.base` - Base key: `{connection}.{schema}.{table}`
- `rsync.partition.key.full` - Full key: `{connection}.{schema}.{table}.{record_id}`
- `rsync.record.id` - Primary key value
- `rsync.operation` - Operation type (INSERT, UPDATE, DELETE)

## Building

```bash
cd smt
mvn clean package
```

The JAR will be created at `smt/target/rsync-kafka-smt-1.0.0.jar`.

## Docker Deployment

### Build Custom Image

```bash
docker build -t rsync-kafka-connect:latest .
```

### Docker Compose

```yaml
services:
  kafka-connect:
    image: rsync-kafka-connect:latest
    # ... see docker-compose.kafka-connect.yml
```

## Usage Example

### Complete CDC Pipeline with SMT

```python
from shared.mcp_connectors.debezium.smt_config import generate_debezium_config

config = generate_debezium_config(
    connection_name="prod-mysql",
    database_type="mysql",
    db_host="mysql",
    db_port=3306,
    db_user="root",
    db_password="password",
    db_name="ecommerce",
    tables=["users", "orders", "products"],
    use_single_topic=True,
    pk_fields=["id"],
)

# This routes ALL tables to: cdc.prod-mysql
# With partition key headers for ordering
```

### Topic Naming Convention

| Topic Pattern | Description |
|---------------|-------------|
| `cdc.{connection}` | CDC events from all tables |
| `conn.{connection}` | Batch/polling events |
| `protected.conn.{connection}` | PII-protected data |
| `transformed.{pipeline}.{dest}` | Transformed output |

## Partition Key Strategy

### Base Key (Normal Entities)
```
{connection}.{schema}.{table}
Example: prod-mysql.public.users
```

Use for: Small tables, need full table ordering

### Full Key (Hot Entities)
```
{connection}.{schema}.{table}.{record_id}
Example: prod-mysql.public.orders.12345
```

Use for: Large tables, high throughput, need record-level distribution

The downstream consumer uses `EntityStats` to determine which entities are "hot" and should use the full partition key.

## Schema Registry Integration

The SMT configures Kafka Connect to use `TopicRecordNameStrategy`, which:
- Allows multiple schemas per topic
- Names schemas based on record name, not topic
- Enables schema evolution per table within a single topic

## Testing

```bash
cd smt
mvn test
```

## Files

```
shared/internal/infra/kafka-connect/
├── smt/
│   ├── src/main/java/com/rsync/kafka/smt/
│   │   ├── TopicRouter.java        # Topic routing SMT
│   │   └── PartitionKeyHeader.java # Partition key SMT
│   ├── src/test/java/...           # Unit tests
│   └── pom.xml                     # Maven build
├── Dockerfile                      # Custom Kafka Connect image
├── docker-compose.kafka-connect.yml
└── README.md
```

## Requirements

- Java 11+
- Maven 3.6+
- Kafka Connect 3.x
- Schema Registry (for Avro)
