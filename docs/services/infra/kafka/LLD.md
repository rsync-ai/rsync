## Kafka (Event Bus) — LLD

### Compose Definition
Source: `docker-compose.yml` service `kafka`
- advertised listeners:
  - internal: `PLAINTEXT://kafka:29092`
  - host: `PLAINTEXT_HOST://localhost:9092`
- healthcheck: `nc -z localhost 9092`

### Topic Conventions (high level)
Examples (not exhaustive):
- agent control:
  - `agent.control.commands`
  - `agent.control.results`
- planner:
  - `agent.planner.response`
- pipeline:
  - `pipeline.domain.events`
- CDC:
  - `cdc.<connection>` (when SMT routes to single topic per connection)

See also: `docs/architecture/kafka-topics.md` (may be archived during doc cleanup if superseded).


