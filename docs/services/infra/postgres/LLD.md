## Postgres (Platform DB) — LLD

### Compose Definition
Source: `docker-compose.yml` service `postgres`
- `POSTGRES_USER=user`
- `POSTGRES_PASSWORD=password`
- `POSTGRES_DB=pipeline_db`
- Command flags:
  - `wal_level=logical`
  - `max_wal_senders=1`
  - `max_replication_slots=1`

### Health
- `pg_isready -U user -d pipeline_db`

### Data Model Location
Migrations:
- API Gateway migrations: `api-gateway/migrations/`
- Orchestrator migrations: `backend-orchestrator/migrations/`
- Temporal uses its own internal schema (managed by `temporalio/auto-setup`)


