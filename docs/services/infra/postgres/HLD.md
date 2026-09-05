## Postgres (Platform DB) — HLD

### Purpose
Postgres is the **primary metadata store** for rsync-ai:
- pipelines, pipeline progress, decisions/events (depending on modules),
- connections + encrypted configs,
- OAuth tokens (when enabled),
- Temporal persistence (Temporal auto-setup uses the same Postgres service in dev compose).

### Runtime Interface
- **Compose service**: `postgres`
- **Image**: `postgres:16-alpine`
- **Port**: `5432` (host mapped)
- **DB**: `pipeline_db` (dev default)
- **Special config**: logical replication enabled (`wal_level=logical`) for CDC support.

### Dependencies
- Used by: API Gateway, Orchestrator, Temporal, Temporal Adapter

### Persistence
- Volume: `postgres_data`


