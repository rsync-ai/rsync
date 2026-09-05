# Rsync AI Scripts

Utility scripts for development, testing, and deployment.

## 📋 Script Reference

### 🚀 setup.sh
**Initial setup and installation**

```bash
./scripts/setup.sh
```

**What it does:**
- ✅ Checks Docker and Docker Compose
- ✅ Creates `.env` from `.env.example`
- ✅ Creates necessary directories
- ✅ Makes all scripts executable
- ✅ Pulls Docker images
- ✅ Builds services
- ✅ Starts database and Kafka
- ✅ Runs database migrations
- ✅ Creates Kafka topics

**When to use:** First time setup

---

### 🔧 dev.sh
**Development workflow management**

```bash
# Start all services
./scripts/dev.sh start

# Stop all services
./scripts/dev.sh stop

# Restart all services
./scripts/dev.sh restart

# Rebuild specific service
./scripts/dev.sh rebuild frontend
./scripts/dev.sh rebuild api-gateway

# View logs
./scripts/dev.sh logs                # All services
./scripts/dev.sh logs frontend       # Specific service

# Check status
./scripts/dev.sh ps
```

**When to use:** Daily development

---

### 📊 migrate.sh
**Database migration management**

```bash
./scripts/migrate.sh
```

**What it does:**
- ✅ Waits for PostgreSQL to be ready
- ✅ Runs all SQL migrations in order
- ✅ API Gateway migrations (001-006)
- ✅ Orchestrator migrations (if any)
- ✅ Verifies migration success
- ✅ Shows database state

**When to use:**
- After pulling new changes
- When database schema changes
- When setting up fresh environment

---

### 🧪 test.sh
**Test runner**

```bash
# Test API endpoints
./scripts/test.sh api

# Run UI tests (Playwright)
./scripts/test.sh ui

# Run integration tests
./scripts/test.sh integration

# Run all tests
./scripts/test.sh all
```

**API tests include:**
- ✅ Login endpoint
- ✅ Connections CRUD
- ✅ Connectors listing
- ✅ Pipelines operations

**When to use:**
- Before committing changes
- After making API changes
- CI/CD pipeline

---

### 📨 create_kafka_topics.sh
**Kafka topic creation — local dev only**

```bash
./scripts/create_kafka_topics.sh
```

Runs `kafka-topics` *inside* the broker container, so Docker must be up. The container
name and the CLI script name are both probed at runtime (`rsync-ai-kafka` /
`rsync-kafka` / `kafka`; `kafka-topics.sh` / `kafka-topics`); override with
`KAFKA_CONTAINER=<name>`.

Not wired into any compose file — only [setup.sh](setup.sh) calls it, so it does **not**
run on a deployed stack. The deployed equivalent is the `kafka-init` one-shot service in
`docker-compose.yml`, which runs [kafka-init-new-topics.sh](kafka-init-new-topics.sh).

**Topics created** (8, each prefixed with `$KAFKA_TOPIC_PREFIX`, default `rsync.`):
- `agent.planner.requests` · `agent.executor.requests` — the two request topics a service actually consumes
- `agent.{intent,resolver,discovery,planner,validator,executor}.responses` — consumed by the api-gateway WebSocket bridge

Every name has a matching produce or subscribe call in a service. Fourteen names that
had neither were removed in the `rsync.` cutover — a pre-created topic nobody reads is
indistinguishable on the broker from a working one.

Full catalogue of who produces, consumes and creates each topic:
[docs/architecture/kafka-topics.md](../docs/architecture/kafka-topics.md).

**When to use:**
- First time setup
- After Kafka container reset
- When topics are missing

---

### 🧨 prod-teardown-pipelines.sh
**Wipes every pipeline and clears Kafka topic state** — Phase 0 steps 1-3 of the
BYO-Kafka / EKS migration. Destructive by design; dry-run by default.

```bash
# Report only. Changes nothing. This is the default.
sudo bash scripts/prod-teardown-pipelines.sh --dry-run

# Feed it over ssh stdin so the SQL quoting survives (nothing is pulled to the host):
ssh -i <key> azureuser@<host> \
  'sudo ENV_FILE=/root/rsync-ai/.env.prod bash -s -- --dry-run' \
  < scripts/prod-teardown-pipelines.sh

# Execute. Needs BOTH flags; the token embeds the target database name, so a
# token minted against staging cannot execute against prod.
sudo bash scripts/prod-teardown-pipelines.sh \
  --execute --confirm=DELETE-ALL-PIPELINES-<dbname> --kafka=volume
```

What it does, in this order (do not reshuffle):
1. Deletes Kafka Connect connectors, while Connect is still up — a connector that
   outlives its pipeline resumes against a slot and topics that no longer exist.
2. Deletes every `pipelines` row. 22 of the 23 FK children cascade;
   **`cdc_resources` is `ON DELETE SET NULL`**, so its rows survive with a null
   `pipeline_id` and the publications/slots they name leak on the *source*
   databases. The script refuses to run while that table is non-empty unless you
   pass `--cdc-dropped-on-source` to assert the source-side drop is done.
3. Stops the Kafka-touching containers in one `docker stop`, producers before the
   broker, so nothing re-creates a topic mid-teardown.
4. Clears topic state. `--kafka=volume` (default) drops the broker's data volume —
   the **only** moment `offsets.topic.num.partitions` can be changed, and it also
   clears Connect's `_rsync-connect-*` trio. `--kafka=topics` deletes topics and
   consumer groups in place instead, leaving the offset topic's partition count.

Safety properties (each one exists because testing broke it):
- The target volume is resolved from the **running** broker's mount table, never by
  name match and never from a stopped container. With several compose projects on
  one host, `docker volume ls | grep kafka_data` matches many volumes; with no
  broker running the script refuses rather than guessing (`KAFKA_VOLUME_OVERRIDE`
  is the deliberate escape hatch).
- Deletions are verified by **re-listing**, not by exit code:
  `kafka-consumer-groups.sh --delete` exits 0 even when it fails.
- `--max-pipelines=N` (default 25) is a wrong-database tripwire.
- The DB credential is forwarded through the environment, never in an argv element.
- Idempotent: a second run against cleared state is a no-op.

**When to run**: only during the Kafka namespace migration. Never as routine
maintenance. See [docs/architecture/kafka-topics.md](../docs/architecture/kafka-topics.md).

---

### 🛑 stop-all.sh
**Emergency stop**

```bash
./scripts/stop-all.sh
```

**What it does:**
- Stops all Docker containers
- Removes containers
- Cleans up volumes (optional)

**When to use:**
- Clean shutdown needed
- Before system maintenance
- Troubleshooting

---

### 🔍 test_mcp_generation_all_scenarios.sh
**MCP connector generation tests**

```bash
./scripts/test_mcp_generation_all_scenarios.sh
```

**Tests:**
- Generate MySQL connector
- Generate S3 connector
- Generate Stripe connector
- Verify logo downloads
- Validate metadata

**When to use:**
- Testing tool generator
- Validating connector system
- CI/CD pipeline

---

### 📡 setup_signoz.sh
**Observability setup (optional)**

```bash
./scripts/setup_signoz.sh
```

**What it does:**
- Installs SigNoz for observability
- Sets up tracing
- Configures metrics

**When to use:**
- Production deployments
- Advanced monitoring needed

---

## 🎯 Common Workflows

### First Time Setup
```bash
./scripts/setup.sh
# Update .env with your values
./scripts/dev.sh start
```

### Daily Development
```bash
./scripts/dev.sh start          # Start services
./scripts/dev.sh logs frontend  # Watch logs
# ... make changes ...
./scripts/dev.sh rebuild frontend  # Rebuild if needed
./scripts/test.sh api           # Test changes
```

### After Git Pull
```bash
./scripts/migrate.sh            # Run new migrations
./scripts/dev.sh restart        # Restart services
```

### Troubleshooting
```bash
./scripts/dev.sh stop           # Stop everything
./scripts/dev.sh start          # Fresh start
./scripts/dev.sh logs api-gateway  # Check logs
./scripts/test.sh api           # Verify APIs
```

### Before Committing
```bash
./scripts/test.sh all           # Run all tests
./scripts/dev.sh ps             # Check all services running
```

---

## 🔧 Advanced Usage

### Environment Variables

Scripts respect these environment variables:

```bash
# Database
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=rsyncdb
export POSTGRES_USER=rsync
export POSTGRES_PASSWORD=rsyncpass

# Services
export NEXT_PUBLIC_API_URL=http://localhost:5001
export ORCHESTRATOR_URL=http://localhost:8081
export LLM_SERVICE_URL=http://localhost:5011
```

### Custom Docker Compose

All scripts use `docker-compose` by default. To use a custom compose file:

```bash
# docker-compose.override.yml is LOCAL-ONLY (gitignored) — copy the template:
cp docker-compose.override.yml.example docker-compose.override.yml
# edit it with your dev ports/origins, then (compose auto-loads override.yml):
docker-compose -f docker-compose.yml -f docker-compose.override.yml up
```

### Running Individual Services

```bash
# Start only database
docker-compose up -d postgres

# Start only frontend
docker-compose up -d frontend

# Start backend services
docker-compose up -d api-gateway orchestrator llm-service
```

---

## 🐛 Troubleshooting

### Script not executable
```bash
chmod +x scripts/*.sh
```

### Docker not found
```bash
# Install Docker Desktop or Docker Engine
# macOS: brew install --cask docker
# Linux: curl -fsSL https://get.docker.com | sh
```

### PostgreSQL connection refused
```bash
# Wait for PostgreSQL to start
docker-compose logs postgres
# Or restart
./scripts/dev.sh restart
```

### Kafka topics not created
```bash
./scripts/create_kafka_topics.sh
# Or recreate Kafka
docker-compose rm -sf kafka
./scripts/dev.sh start
```

### Port already in use
```bash
# Find process using port
lsof -i :3000  # or :5001, :8081, etc.
# Kill process
kill -9 <PID>
# Or change port in .env
```

---

## 📝 Script Maintenance

### Adding a New Script

1. Create script in `scripts/`
2. Add shebang: `#!/bin/bash`
3. Add description comment
4. Make executable: `chmod +x scripts/your-script.sh`
5. Document in this README

### Modifying Existing Scripts

1. Test changes locally
2. Update documentation
3. Commit with clear message

---

## 🔗 Related Documentation

- [Services (HLD/LLD)](../docs/services/INDEX.md)
- [Changelog](../CHANGELOG.md)
- [Main README](../README.md)

---

**Last Updated:** December 2025
