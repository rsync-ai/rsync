# Quick Start

## Self-hosting (recommended for evaluation)

The fastest way to run rsync.ai is the one-command installer — no source code needed.

```bash
curl -sSL https://raw.githubusercontent.com/rsync-ai/rsync/main/install.sh | bash
```

The installer will prompt for an OpenAI API key, your domain or IP, and an admin email. It generates all secrets, pulls Docker images, and starts the full stack. Open `http://localhost:3000` when it finishes.

For detailed self-hosting instructions (TLS, secrets management, backup, upgrades) see [deployment/self-hosting.md](../deployment/self-hosting.md).

---

## Local development

**Requirements:** Docker Desktop 4.x+, Go 1.24+, Python 3.11+, Node 20+

```bash
# 1. Clone and configure
git clone https://github.com/rsync-ai/rsync.git
cd rsync
cp .env.example .env          # fill in OPENAI_API_KEY and secrets
cp llm-service/.env.example llm-service/.env

# 2. Start the full stack
docker compose -p rsync-ai up -d

# 3. Verify health (~30s startup)
curl http://localhost:5001/health   # api-gateway
curl http://localhost:8081/health          # orchestrator
```

**Key URLs:**
| Service | URL |
|---|---|
| Frontend | http://localhost:3000 |
| API Gateway | http://localhost:5001 |
| Temporal UI | http://localhost:8233 |
| Orchestrator | http://localhost:8081 |
| MinIO console (e2e overlay) | http://localhost:9001 |

---

## Try it in 5 minutes, with no credentials

You do not need a database to see rsync.ai move rows. The quickstart stack ships
a credential-free `sample-data` source and a throwaway `demo-warehouse` Postgres
to land it in.

1. Open http://localhost:3000 and sign in.
2. On the first-run checklist, click **Start with sample data**.
3. Two connections appear in your workspace — `sample-data` (source) and
   `demo-warehouse` (destination). Both are connection-tested before they are
   saved, so if the button succeeds, they work.
4. You land in `/chat`. Ask for something like **"sync customers and orders from
   sample data to the demo warehouse"**, pick the tables, and confirm.

The demo warehouse is a separate database from the one holding your pipelines
and credentials, it has its own password, and it is safe to throw away. When you
are done, delete the two connections; to remove the container as well, drop the
`demo-warehouse` service and its `demo_warehouse_data` volume.

To turn the demo off entirely, unset `RSYNC_DEMO_DESTINATION_DSN` on the
api-gateway. Nothing else keys off it — with the variable unset the endpoints
report unavailable and the card never renders.

On Kubernetes the same path is opt-in. See
[deployment/self-hosting.md](../deployment/self-hosting.md) and the chart's
`demo.enabled` value; enabling it requires `connectors.sampleData.enabled`, a
`postgresql` entry in `connectors.fleet`, and `secrets.demoWarehousePassword`,
and the chart refuses to install rather than come up healthy and fail later.

---

## Create your first pipeline

1. Open http://localhost:3000/chat
2. Type a pipeline description, e.g. **"sync MySQL to S3"**
3. The AI will ask for your source and destination credentials
4. Select the tables you want to sync
5. Confirm — the pipeline deploys and runs

The UI shows real-time progress as the agent works through each stage: intent classification → planning → provisioning → execution.

---

## Architecture mental model

```
User (natural language)
  → Frontend (Next.js)
    → API Gateway (Go, REST + WebSocket)
      → Orchestrator (Go workers)
        → Temporal (workflow engine)
          → Temporal Adapter (activities)
            → Kafka (commands / results)
              → MCP Connector containers (read/write data)
```

Each pipeline is a Temporal workflow. The orchestrator runs workers that execute activities. Sources and destinations are versioned MCP connector containers — each exposes a standard tool interface.

---

## Optional: E2E overlay (MySQL + MinIO)

```bash
docker compose -p rsync-ai -f docker-compose.yml -f docker-compose.e2e.yml up -d
```

This adds a pre-seeded MySQL instance and a local MinIO (S3-compatible) bucket — useful for testing a real MySQL → S3 pipeline without external credentials.

---

## Running tests

```bash
# API integration tests
cd api-gateway && go test ./...

# Frontend E2E (Playwright)
cd frontend && npx playwright test

# MySQL → S3 batch smoke test
bash tests/test_mysql_to_s3.sh

# MySQL CDC → MinIO
bash e2e/test_mysql_cdc_debezium.sh
```

---

## Troubleshooting

**Services not starting:**
```bash
docker compose -p rsync-ai logs api-gateway
docker compose -p rsync-ai logs orchestrator
```

**Reset and start clean (deletes all pipeline data):**
```bash
docker compose -p rsync-ai down -v && docker compose -p rsync-ai up -d
```

**Kafka issues:**
```bash
docker compose -p rsync-ai restart kafka
```

For more detail see [architecture/overview.md](../architecture/overview.md) and the per-service docs in [services/](../services/).
