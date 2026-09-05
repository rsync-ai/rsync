## Redis — LLD

### Compose Definition
Source: `docker-compose.yml` service `redis`
- command:
  - `redis-server --appendonly yes --maxmemory 256mb --maxmemory-policy allkeys-lru`
- healthcheck:
  - `redis-cli ping`
- volume:
  - `redis_data:/data`

### Primary Usage Pattern (V2)
- Temporal activity sends a request (Kafka or direct)
- agent writes response to Redis under a correlation key
- activity polls/blocks on Redis until response arrives or TTL expires


