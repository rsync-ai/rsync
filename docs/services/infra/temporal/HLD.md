## Temporal — HLD

### Purpose
Temporal provides **deterministic workflow orchestration** for rsync-ai.

It is used for:
- stage-by-stage execution of NL-driven pipelines (V2),
- retries/timeouts/cancellation semantics,
- visibility and debugging via Temporal UI.

### Runtime Interface
- **Temporal Server**
  - compose service: `temporal`
  - port: `7233`
- **Temporal UI**
  - compose service: `temporal-ui`
  - host port: `8233`
- **Temporal Admin Tools**
  - compose service: `temporal-admin-tools`
  - interactive debug container

### Dependencies
- Postgres (dev compose uses Postgres as Temporal persistence)
- Temporal Adapter (runs the worker for rsync-ai task queues)


