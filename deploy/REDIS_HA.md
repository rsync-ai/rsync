# Redis High Availability

This document describes the Redis topology options for rsync-ai deployments.
Redis is used in three distinct ways by the platform, and the right HA choice
depends on which of those usages matters most to your deployment.

## What Redis is used for

| Usage | Owner | Loss tolerance |
|-------|-------|---------------|
| Correlation request/reply store (V2 Temporal activities) | `backend-temporal-adapter`, `backend-orchestrator` | Temporal retries on timeout — total Redis loss = some workflows pause for ~2 minutes then resume cleanly |
| Schema cache (5-minute TTL) | `api-gateway` | Cold cache, transparent re-fetch from connector |
| Conversation cache (30-minute TTL) for multi-turn chat | `api-gateway` | Loss = active chat sessions reset to StateIdle (single-turn behavior until user restarts conversation) |
| Explorer schema index (10-minute TTL) | `api-gateway` | Cold cache, re-derived on next query |

**No Redis usage is authoritative.** The source of truth lives in PostgreSQL
(persisted state) and Temporal (workflow state). Redis is a cache plus an
in-flight correlation channel.

## Topology options

### Option A — Single-node Redis with AOF persistence (default; recommended for self-hosted launch)

This is what `docker-compose.yml` already configures:

```yaml
redis:
  image: redis:7-alpine
  command: ["redis-server", "--appendonly", "yes", "--maxmemory", "256mb", "--maxmemory-policy", "allkeys-lru"]
  restart: unless-stopped
```

**Properties:**
- AOF (`appendonly yes`) flushes writes to disk every second by default
  (`appendfsync everysec`). Worst-case data loss on crash: ~1 second.
- `restart: unless-stopped` means Docker brings Redis back automatically.
- LRU eviction on 256 MB ceiling prevents runaway memory.

**Behavior on Redis restart:**
- Schema/conversation/explorer caches: cold (rebuilt on next request).
- In-flight correlation requests: lost. Temporal activity polling Redis
  hits its 5-minute timeout (with 30s heartbeats keeping the activity
  alive), then Temporal retries the activity. The retry generates a new
  correlation ID and the worker processes it normally.
- **Net visible effect:** workflows in flight when Redis dies pause for
  up to 5 minutes, then resume successfully. No data loss. No manual
  intervention.

**When to use:** all single-node self-hosted deployments. This is the
default and is appropriate unless you have hard RTO commitments.

### Option B — Redis Sentinel (3 sentinels + 1 master + 2 replicas)

Six containers instead of one. Automatic failover in ~10 seconds when the
master dies.

**When to use:**
- You have strict uptime SLAs (e.g. <1 minute RTO).
- You have ops capacity to monitor sentinel quorum and manage network
  partitions.
- Customer-managed deployments where Redis restart is unacceptable.

**When NOT to use:**
- Self-hosted single-tenant launch — the complexity isn't justified
  given Temporal's built-in retry on timeout.
- Resource-constrained hosts (Sentinel needs 3× memory for replicas).

A reference compose file would live at `deploy/docker-compose.redis-ha.yml`
(not yet shipped). Implementation outline:

```yaml
# Outline only — not production-tested.
services:
  redis-master:
    image: redis:7-alpine
    command: ["redis-server", "--appendonly", "yes"]
  redis-replica-1:
    image: redis:7-alpine
    command: ["redis-server", "--replicaof", "redis-master", "6379", "--appendonly", "yes"]
  redis-replica-2:
    image: redis:7-alpine
    command: ["redis-server", "--replicaof", "redis-master", "6379", "--appendonly", "yes"]
  sentinel-1:
    image: redis:7-alpine
    command: ["redis-sentinel", "/etc/redis-sentinel.conf"]
    volumes: [./sentinel-1.conf:/etc/redis-sentinel.conf]
  sentinel-2: {...}  # Same as sentinel-1
  sentinel-3: {...}  # Same as sentinel-1
```

Application services connect via `redis-sentinel://sentinel-1:26379,sentinel-2:26379,sentinel-3:26379/mymaster`.
The Go and Python Redis clients used in this project (go-redis, redis-py)
both support sentinel discovery natively.

### Option C — Managed Redis (cloud SaaS deployments)

Out of scope for self-hosted. If you offer a SaaS tier, point services at
Upstash, Redis Cloud, AWS ElastiCache, or GCP Memorystore. Set the
`REDIS_HOST` / `REDIS_PASSWORD` / `REDIS_TLS` environment variables
appropriately.

## Decision guide

Use this table to pick:

| Constraint | Choose |
|---|---|
| Self-hosted, ops simplicity matters more than RTO | **Option A** |
| Self-hosted, customer demands <1min RTO | Option B |
| Cloud SaaS deployment | Option C |
| Multiple regions or geographic redundancy needed | Out of scope; talk to platform team |

## Operational notes

- `REDIS_PASSWORD` must be set in production. The value is read by all
  services (api-gateway, backend-orchestrator, backend-temporal-adapter,
  llm-service) — keep it consistent across services.
- Schema / conversation / explorer caches use deterministic keys, so a
  cold cache after restart simply incurs the next-request fetch cost. There
  is no cache stampede protection — if 1000 requests hit at the moment Redis
  cold-starts, all 1000 will independently miss and re-fetch. This has not
  been a problem at current load levels but is worth monitoring as traffic
  grows.
- The correlation store uses TTLs of `2 × request timeout` (default 10 minutes)
  and the orchestrator runs a background `correlation_claim_renewer` worker
  to keep claims fresh. Total Redis memory used by correlation entries is
  bounded by the number of in-flight Temporal activities, which is tiny.
