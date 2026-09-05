## Temporal — LLD

### Compose Definition
Source: `docker-compose.yml`

Temporal server:
- image: `temporalio/auto-setup:1.22.4`
- env uses Postgres (`DB=postgresql`, seeds `postgres`)
- dynamic config mounted from: `deploy/temporal/`

Temporal UI:
- image: `temporalio/ui:2.21.3`
- points to `TEMPORAL_ADDRESS=temporal:7233`
- CORS origins include frontend host

Admin tools:
- image: `temporalio/admin-tools:1.22.4`

### Task Queue (rsync-ai)
Temporal Adapter registers workflows/activities on:
- task queue: `pipeline-workflows`


