# Avro Schemas for RSync Data Pipeline

This directory contains all Avro schemas used across the RSync platform for Kafka message serialization.

## Overview

These `.avsc` files are the canonical **reference definitions** for the platform's
Kafka message schemas. Avro serialization is **optional and currently disabled** in
production (`KAFKA_USE_AVRO=false` → JSON wire format). When enabled it uses the
**Confluent-compatible wire format** registered against the **Apicurio** registry
(see the `schema-registry` compose profile). The schemas provide:
- **Schema Validation**: Ensure message structure conformance
- **Schema Evolution**: Backward/forward compatibility
- **Compression**: ~50-70% smaller than JSON
- **Type Safety**: Strong typing across services

> These files are reference definitions, **not loaded at runtime** — the previous Go
> loader (`shared/go/avro`) was removed. Live (un)marshalling lives in the services
> listed under **Usage**.

## Directory Structure

```
avro-schemas/
├── core/                    # Core platform schemas
│   ├── pipeline.avsc        # Pipeline request/response
│   ├── agent.avsc           # Agent communication
│   └── trace.avsc           # Distributed tracing
├── cdc/                     # CDC-specific schemas
│   ├── envelope.avsc        # Debezium envelope format
│   └── change_event.avsc    # Generic change event
├── mcp/                     # MCP connector schemas
│   ├── message.avsc         # Generic MCP message
│   ├── batch.avsc           # Batch operation
│   └── record.avsc          # Single record
└── generated/               # Auto-generated connector schemas
    └── {connector}/         # Per-connector schemas
```

## Schema Naming Convention

- **Namespace**: `com.rsync.{category}.{subcategory}`
- **Topic-to-Schema**: Uses `TopicRecordNameStrategy`
- **Subject Format**: `{topic}-{record_name}`

## Usage

Avro (un)marshalling lives in the services, gated by `KAFKA_USE_AVRO`:

- **Produce (API Gateway)**: `api-gateway/internal/kafka/avro_producer.go` (via `unified_producer.go`)
- **Consume (Orchestrator)**: `backend-orchestrator/internal/kafka/manager.go` — `SmartDeserialize` auto-detects Avro vs JSON
- **Python (Planner)**: `llm-service/src/utils/avro_serializer.py` + `avro_kafka.py`
- **Registry admin API (API Gateway)**: `api-gateway/internal/handlers/schema_registry.go`

## Schema Registry

- **Implementation**: Apicurio Registry 3.0.6 — Confluent-compatible REST API (replaces Confluent Schema Registry, CCL-licensed)
- **Compose**: `schema-registry` service, `profiles: [schema-registry]` (not started by default)
- **Host port**: `8085` → container `8080`
- **Compatibility**: BACKWARD (default)
- **Auto-register**: Enabled for development

## Evolution Rules

1. **Add fields**: Must have default value
2. **Remove fields**: Only if previously optional
3. **Rename fields**: Not allowed (use aliases)
4. **Change types**: Use union types with null
