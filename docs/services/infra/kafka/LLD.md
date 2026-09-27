## Kafka (Event Bus) — LLD

### Compose Definition
Source: `docker-compose.yml` service `kafka`
- advertised listeners:
  - internal: `PLAINTEXT://kafka:29092`
  - host: `PLAINTEXT_HOST://localhost:9092`
- healthcheck: `nc -z localhost 9092`

### Topic Conventions (high level)
Examples (not exhaustive):
- platform (created at orchestrator startup):
  - `pipeline.domain.events`
  - `rsync.notifications`
  - `pii.scan.request` / `pii.scan.response`
  - `rsync.healer.*` (only with `RSYNC_SCHEMA_DRIFT_ENABLED=true`)
- batch:
  - `pipeline.<id8>.data`
- CDC:
  - `cdc-<id8>.<db>.<table>`

The `agent.control.*` and `agent.*.response(s)` topics were removed with the agent Kafka
bus in [#1227](https://github.com/rsync-ai/rsync-ai/pull/1227).

See also: [kafka-topics.md](../../../architecture/kafka-topics.md).


