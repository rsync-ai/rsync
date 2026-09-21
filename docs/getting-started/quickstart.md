# Quick Start

## Self-hosting (recommended for evaluation)

The fastest way to run rsync.ai is the one-command installer — no source code needed.

```bash
curl -sSL https://raw.githubusercontent.com/rsync-ai/rsync/main/install.sh | bash
```

The installer asks which LLM you want and your domain or IP. Everything else it does itself — generates every secret, pulls the images, starts the full stack. For the LLM you can bring an OpenAI key, pick the bundled Ollama, which needs no key and no manual `ollama pull` (the stack runs its own Ollama container and downloads the model before any service that would ask for one starts), or pick none and set one up later. Without an LLM, pipelines, raw SQL in the Data Explorer and the shipped connectors work; chat beyond pipeline commands, natural-language SQL, pipeline diagnosis and connector generation say `Set up an LLM first`. An OpenAI key is used when you give one, and Ollama only when you choose it — see [which LLM is used](../deployment/self-hosting.md#which-llm-is-used). Open `http://localhost:3000` when it finishes and sign up straight away: the first account created on a new install becomes its admin, and later accounts are regular users until an admin changes their role in the admin panel. The installer does not ask for an admin email, because nothing reads one.

Run without a terminal (for example from a provisioning script), the installer takes the LLM from the environment on the `bash` side of the pipe — `OPENAI_API_KEY=sk-...`, `LLM_PROVIDER=ollama` or `LLM_PROVIDER=none` — and with none of them set it installs without an LLM:

```bash
curl -sSL https://raw.githubusercontent.com/rsync-ai/rsync/main/install.sh | LLM_PROVIDER=ollama bash
```

The full stack includes the change-data-capture services, so streaming pipelines work
on a fresh install with nothing extra to run. Those three containers reserve 2816 MB
between them, which is why the installer asks for 8 GB. To leave them out on a machine
that will only ever run batch syncs, put an empty `RSYNC_PROFILES` on the `bash` side
of the pipe — before `curl` it would set the variable for `curl`, which never reads it:

```bash
curl -sSL https://raw.githubusercontent.com/rsync-ai/rsync/main/install.sh | RSYNC_PROFILES= bash
```

The host floor drops back to 6 GB, and the installer writes the resolved set into the
`compose.sh` it leaves behind, so running compose by hand later keeps the same choice.

Running the same command again on a machine that already has rsync.ai upgrades it in place. It keeps your existing `.env` and, before it pulls or starts anything, checks two things:

- **Settings.** Every variable the new compose file requires must have a value in `.env`. A missing internal secret that the installer generates on a fresh install (`INTERNAL_SERVICE_SECRET`, `JWT_SECRET`, `REDIS_PASSWORD`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`) is generated and added, and the installer says so. `POSTGRES_PASSWORD` and `ENCRYPTION_KEY` are never generated, because a new value would lock you out of the existing database or make saved connection credentials unreadable: if either is missing the installer stops and asks you to add the original value. Values already in `.env` are never changed. A line with nothing after the `=` counts as missing, and a line written as `export NAME=value` counts as set, the same way compose reads it.
- **Ports.** Each host port the compose file publishes (5001 and 3000 by default) must be free or already held by this install's own containers. If another program or container holds one, the installer stops, names the port and what holds it, and explains how to free it or move rsync.ai to another port. Containers from the existing install are recreated only where their image or settings changed, and the data volumes are kept. If you are sure a reported port is free, put `RSYNC_SKIP_PORT_CHECK=1` on the `bash` side of the pipe to skip that check.

For detailed self-hosting instructions (TLS, secrets management, backup, upgrades) see [deployment/self-hosting.md](../deployment/self-hosting.md).

---

## Local development

**Requirements:** Docker Desktop 4.x+, Go 1.24+, Python 3.11+, Node 20+

```bash
# 1. Clone and configure
git clone https://github.com/rsync-ai/rsync.git
cd rsync
cp .env.example .env          # secrets; OPENAI_API_KEY only if you have one
cp llm-service/.env.example llm-service/.env
# The LLM is optional. In llm-service/.env either set a real OPENAI_API_KEY, or
# set LLM_PROVIDER=none to run without one (the shipped placeholder key is not a key).

# 2. Start the full stack
docker compose -p rsync-ai up -d

#    ...or without an OpenAI key, adding the bundled LLM (and LLM_PROVIDER=ollama
#    in llm-service/.env, which the overlay does not set for you):
#    docker compose -p rsync-ai -f docker-compose.yml -f docker-compose.ollama.yml up -d
#    (naming files explicitly also stops compose picking up docker-compose.override.yml)

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
