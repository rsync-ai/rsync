## Redis — HLD

### Purpose
Redis is used as a **state and correlation store** for rsync-ai V2 request/reply activities:
- prevents duplicate claims,
- allows Temporal activities to wait on agent replies deterministically.

### Runtime Interface
- **Compose service**: `redis`
- **Image**: `redis:7-alpine`
- **Port**: `6379`
- **Persistence**: append-only enabled (AOF)

### Dependencies
Used by:
- Orchestrator (correlation client)
- API Gateway (V2 workflows)
- Temporal Adapter (correlation store init)


