# Self-Hosted Deployment Guide

Deploy rsync-ai on your own infrastructure with production-grade security, TLS, and proper secret management.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Start (Evaluation)](#quick-start-evaluation)
3. [Production Deployment](#production-deployment)
4. [TLS/HTTPS with Traefik](#tlshttps-with-traefik)
5. [LLM Provider Configuration](#llm-provider-configuration)
6. [Bring Your Own Kafka](#bring-your-own-kafka)
7. [Bring Your Own PostgreSQL](#bring-your-own-postgresql)
8. [Connector Setup](#connector-setup)
9. [Monitoring (Optional)](#monitoring-optional)
10. [Admin Panel Setup](#admin-panel-setup)
11. [Backup & Restore](#backup--restore)
12. [Upgrading](#upgrading)
13. [Troubleshooting](#troubleshooting)

---


---

## Prerequisites

### Hardware

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU       | 4 cores | 8+ cores    |
| RAM       | 8 GB    | 16+ GB      |
| Disk      | 40 GB SSD | 100+ GB SSD |

If running Ollama locally for the Explorer feature, add an additional 8 GB RAM.

### Software

| Dependency | Version | Notes |
|------------|---------|-------|
| Docker Engine | 24.0+ | [Install](https://docs.docker.com/engine/install/) |
| Docker Compose | v2.24+ | Required for `!override` syntax in prod compose |
| Git | 2.30+ | To clone the repository |

Verify your versions:

```bash
docker --version        # Docker Engine 24.0+
docker compose version  # Docker Compose v2.24+
```

### Domain & DNS (for TLS)

For production with HTTPS, you need:
- A domain name (e.g., `rsync.example.com`)
- An A record pointing to your server's public IP
- Ports 80 and 443 open on your firewall

---

## Quick Start (Evaluation)

Get rsync-ai running locally in under 5 minutes for evaluation. This uses dev defaults (insecure passwords, no TLS).

```bash
# 1. Clone the repository
git clone https://github.com/rsync-ai/rsync.git
cd rsync

# 2. Set up LLM provider (minimum: OpenAI API key)
cp llm-service/.env.example llm-service/.env
# Edit llm-service/.env and set OPENAI_API_KEY=sk-...

# 3. Start the stack
docker compose up -d

# 4. Wait for services to be healthy (~2-3 minutes)
docker compose ps

# 5. Open the UI
open http://localhost:3000
```

The API is available at `http://localhost:5001`.

> **Warning**: The quick-start configuration uses hardcoded passwords and is not suitable for production. See [Production Deployment](#production-deployment) for a secure setup.

---

## Production Deployment

### 1. Generate Secrets

Generate all required secrets before configuring the environment:

```bash
# Encryption key (32+ chars, used to encrypt stored connection credentials)
# WARNING: Do NOT change this after initial setup — existing connections become unreadable
echo "ENCRYPTION_KEY=$(openssl rand -base64 32)"

# JWT signing secret
echo "JWT_SECRET=$(openssl rand -base64 32)"

# Database password
echo "POSTGRES_PASSWORD=$(openssl rand -base64 24)"

# Redis password
echo "REDIS_PASSWORD=$(openssl rand -base64 24)"
```

### 2. Configure Environment

```bash
# Copy the production environment template
cp .env.prod.example .env.prod

# Edit with your values — at minimum, fill in:
#   DOMAIN, ACME_EMAIL, ENCRYPTION_KEY, JWT_SECRET,
#   POSTGRES_PASSWORD, REDIS_PASSWORD
nano .env.prod
```

See [`.env.prod.example`](../../.env.prod.example) for all available options with descriptions.

**External managed database (Azure PostgreSQL, AWS RDS, etc.):**
Set `DATABASE_URL` in `.env.prod` to point to your managed DB instead of the bundled container:
```env
DATABASE_URL=postgresql://<user>:<password>@<host>:5432/pipeline_db?sslmode=require
```
Create the database first if it doesn't exist:
```bash
psql "postgresql://<user>:<password>@<host>:5432/postgres?sslmode=require" \
  -c "CREATE DATABASE pipeline_db;"
```

### 3. Configure LLM Provider

```bash
# Copy the LLM service env template
cp llm-service/.env.example llm-service/.env
nano llm-service/.env
```

**Option A — Azure OpenAI (recommended for production):**
```env
LLM_PROVIDER=azure
AZURE_OPENAI_ENDPOINT=https://<resource>.openai.azure.com
AZURE_OPENAI_API_KEY=<key>
AZURE_OPENAI_API_VERSION=2025-01-01-preview
AZURE_OPENAI_DEPLOYMENT=gpt-4o
LLM_MODEL=
```

**Option B — OpenAI:**
```env
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-...
LLM_MODEL=gpt-4o-mini
```

### 4. Configure Frontend Environment

```bash
cp frontend/.env.prod.example frontend/.env
nano frontend/.env
```

Replace `YOUR_SERVER_IP` with your VM's public IP or domain. Example for IP-only (no domain):
```env
NEXT_PUBLIC_API_URL=http://203.0.113.10
NEXT_PUBLIC_BACKEND_ORCHESTRATOR_URL=http://203.0.113.10
NEXT_PUBLIC_LLM_SERVICE_URL=http://203.0.113.10
NEXT_PUBLIC_WS_URL=ws://203.0.113.10/ws
NEXT_PUBLIC_WS_ORCHESTRATOR_URL=ws://203.0.113.10/ws
NEXT_PUBLIC_APP_NAME=Rsync AI
NEXT_PUBLIC_ENVIRONMENT=production
API_GATEWAY_INTERNAL_URL=http://api-gateway:8080
ORCHESTRATOR_INTERNAL_URL=http://orchestrator:8080
NEXTAUTH_URL=http://203.0.113.10
NEXTAUTH_SECRET=$(openssl rand -base64 32)
```

**Critical rules for the frontend env:**

1. **No explicit ports** — all traffic goes through Traefik on port 80/443. Do NOT use `:5001`, `:8081`, `:3000` — those ports are not exposed on the host in production.
2. **Internal URLs use container names and internal ports** — `api-gateway:8080` (not `api-gateway:5001`), `orchestrator:8080` (not `orchestrator:8081`). The `:5001`/`:8081` ports are dev-only host mappings.
3. **NEXTAUTH_SECRET must be a real value** — run `openssl rand -base64 32` first and paste the output. The literal string `$(openssl rand -base64 32)` is NOT evaluated inside a heredoc or cat redirect.
4. **Rebuild required after any change** — `NEXT_PUBLIC_*` vars are baked into the Next.js bundle at build time. After editing `frontend/.env`, always rebuild: `docker compose ... up -d --build frontend`

> **Important:** `frontend/.env` must exist before starting the stack or Docker Compose will fail with `env file not found`.

### 5. Open Firewall Ports

Ensure these ports are reachable from the internet on your VM:

| Port | Service |
|------|---------|
| 3000 | Frontend UI |
| 5001 | API Gateway |
| 80 / 443 | Traefik (if using TLS) |

```bash
# UFW example
sudo ufw allow 3000/tcp
sudo ufw allow 5001/tcp
```

For Azure VMs: add inbound port rules in Azure portal → VM → Networking.

### 6. Start the Stack

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
  --env-file .env.prod up -d --build
```

The `--build` flag is required on first run to build all images. Subsequent restarts can omit it.

### 7. Verify

```bash
# Check all services are healthy (~3-5 minutes after start)
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
  --env-file .env.prod ps

# Check logs for errors
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
  --env-file .env.prod logs --tail=50

# Quick health check
curl http://localhost:5001/health
curl http://localhost:8081/health
```

Once healthy, the application is available at:
- With TLS: `https://<your-domain>`
- IP-only: `http://<your-ip>:3000` (frontend) and `http://<your-ip>:5001` (API)

---

## TLS/HTTPS with Traefik

The production compose file includes a [Traefik](https://traefik.io/) reverse proxy that automatically provisions Let's Encrypt TLS certificates.

### How It Works

1. Traefik listens on ports 80 and 443
2. HTTP requests on port 80 are redirected to HTTPS
3. Certificates are automatically obtained and renewed via Let's Encrypt HTTP challenge
4. Frontend is served at `https://<DOMAIN>/`
5. API Gateway is served at `https://<DOMAIN>/api/`
6. WebSocket connections at `wss://<DOMAIN>/ws/`

### DNS Requirements

Create an A record pointing your domain to the server:

```
rsync.example.com  →  A  →  203.0.113.10
```

### Firewall

Ensure ports 80 and 443 are open:

```bash
# UFW
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# firewalld
sudo firewall-cmd --permanent --add-service=http
sudo firewall-cmd --permanent --add-service=https
sudo firewall-cmd --reload
```

### Configuration

The Traefik configuration lives in `deploy/traefik/`:

| File | Purpose |
|------|---------|
| `traefik.yml` | Entrypoints, Let's Encrypt resolver, provider config |
| `dynamic.yml` | TLS options (min version, cipher suites), security headers |

Set your domain and ACME email in `.env.prod`:

```env
DOMAIN=rsync.example.com
ACME_EMAIL=admin@example.com
```

> **Cloudflare proxy users — set SSL/TLS encryption mode to `Full (Strict)`.** With the orange-cloud proxy on, Cloudflare's **Off** or **Flexible** modes cause an infinite redirect loop (`ERR_TOO_MANY_REDIRECTS`) that takes the **whole site dark**: Traefik's `web` entrypoint always 308-redirects HTTP→HTTPS, while Cloudflare in those modes either fetches the origin over plain HTTP or redirects the visitor back to HTTP — the two redirects fight forever. This is an edge *setting*, not code: a deploy can neither cause nor fix it, and a rollback won't help. Fix it in the Cloudflare dashboard under **SSL/TLS → Overview → Full (Strict)** (Traefik's Let's Encrypt cert satisfies strict; if you see a `526`, drop to **Full**). To prove the origin is healthy while the edge loops, bypass Cloudflare with `curl --resolve <your-domain>:443:127.0.0.1 -skI https://<your-domain>/` — a `200`/`30x` there means the loop is entirely the Cloudflare mode.

> **Cloudflare proxy users — if in-app navigation feels slow or flaky, check Security → Events for `?_rsc=`.** Next.js fetches React Server Component payloads as `GET <path>?_rsc=<hash>`, several in parallel on hover-prefetch, which can look automated to Bot Fight Mode, a rate-limiting rule, or a WAF managed rule. On a previous hosted deployment (since decommissioned) those requests were observed intermittently failing at the edge — the same URL returning both 200 and 503 in one session — while Traefik, configured to log every non-2xx, recorded **zero** matching rows and `/api/v1/*` through the same proxy was clean. **The cause was never confirmed** and that environment no longer exists, so treat this as a place to look rather than a known Cloudflare bug: filter Security → Events on the `?_rsc=` pattern and, if the matching rule is yours, scope it to exclude same-origin document/prefetch requests. A failed prefetch degrades to a normal full navigation, so the symptom is latency, not a hard error.

### Running Without TLS

If you're behind another reverse proxy (e.g., Cloudflare, AWS ALB) or testing locally, you can skip Traefik by using the base compose only:

```bash
docker compose --env-file .env.prod up -d
```

Override the public URLs in `.env.prod`:

```env
NEXT_PUBLIC_API_URL=http://localhost:5001
NEXT_PUBLIC_WS_URL=ws://localhost:5001/ws
NEXTAUTH_URL=http://localhost:3000
```

---

## LLM Provider Configuration

rsync-ai uses an OpenAI-compatible API interface. You can use OpenAI directly or run models locally with Ollama.

### Option A: OpenAI API (Default)

The simplest setup. Edit `llm-service/.env`:

```env
OPENAI_API_KEY=sk-proj-...
LLM_PROVIDER=openai
LLM_MODEL=gpt-4o-mini
```

### Option B: Ollama (Local / Air-Gapped)

For environments without internet access or to avoid API costs.

1. **Install Ollama** on the host machine: [ollama.com/download](https://ollama.com/download)

2. **Pull required models:**

```bash
ollama pull llama3:latest     # General-purpose (used by planner/executor)
ollama pull sqlcoder:latest   # SQL generation (used by Explorer)
ollama pull mistral:7b        # Code generation (used by tool-generator)
```

3. **Configure** `llm-service/.env`:

```env
LLM_PROVIDER=ollama
OLLAMA_URL=http://host.docker.internal:11434
LLM_MODEL=llama3:latest
```

### Explorer Feature

The Explorer uses the same `LLM_PROVIDER` as the rest of the stack by default. No separate Ollama setup is required when using Azure OpenAI or OpenAI.

For air-gapped / on-prem deployments where schema metadata must not leave the system, set `EXPLORER_OFFLINE_ONLY=true` in `llm-service/.env` to force Ollama for Explorer only while keeping the main pipeline LLM on a cloud provider.

---

## Bring Your Own Kafka

The stack ships a single-node Kafka so evaluation works out of the box. If you
already run a cluster — MSK, Confluent, Redpanda, or your own brokers — point
the stack at it and drop the bundled broker.

Every variable below is optional and **unset means the bundled broker**: an
unset `KAFKA_SECURITY_PROTOCOL` is `PLAINTEXT`, which is correct for the
container on your own bridge network and wrong for anything else. Nothing fails
when you forget one — the client connects in the clear, anonymously. The whole
set is reproduced, commented, in `.env.example`, `.env.prod.example` and
`.env.staging.example`; the per-variable reference is
[Environment Variables § Kafka](env-vars.md).

**1. Set `KAFKA_BROKERS` in your `.env`:**

```bash
KAFKA_BROKERS=b-1.mycluster.kafka.eu-west-1.amazonaws.com:9096,b-2.mycluster.kafka.eu-west-1.amazonaws.com:9096
```

A comma-separated list is supported and preserved end to end. Collapsing a
multi-broker cluster to one hostname gives you a single point of failure that
looks like it works.

The port must match the protocol you pick in step 4. MSK exposes each on its
own listener — **9092** PLAINTEXT, **9094** TLS, **9096** SASL/SCRAM, **9098**
IAM — so a `SASL_SSL` client aimed at `:9092` fails with a timeout or a torn
connection rather than an authentication error.

**2. Add the BYO overlay so the bundled broker is not started:**

```bash
docker compose -f docker-compose.quickstart.yml -f docker-compose.byo-kafka.yml up -d
```

Without the overlay the app tier still honours `KAFKA_BROKERS`, but the bundled
`kafka` container starts anyway and holds its 1 GB memory limit — you pay for a
broker nothing talks to.

**3. Size the topics for a real cluster.** The bundled broker can only serve
RF=1, and RF=1 topics go unavailable during routine rolling maintenance:

```bash
KAFKA_REPLICATION_FACTOR=3
KAFKA_MIN_INSYNC_REPLICAS=2
```

> `KAFKA_MIN_INSYNC_REPLICAS` **must be ≤** `KAFKA_REPLICATION_FACTOR`. Inverted,
> the topic is created *successfully* and then rejects every write — the stack
> comes up healthy and pipelines report `dispatched N rows … no acks were
> recorded`. The `kafka-init` job clamps it and logs a warning rather than
> creating an unwritable topic.

The `kafka-init` job still runs and creates the prefixed topics on **your**
cluster with the settings above. If your ACLs forbid topic creation, create them
out of band — the required permissions are in [Kafka ACLs](kafka-acls.md) — and
exclude `kafka-init` the same way `docker-compose.byo-kafka.yml` excludes the
broker.

### 4. Authentication and TLS

Any managed cluster reachable from outside its own subnet enforces one of these.
Pick the row your broker is configured for and set **both** the protocol and the
mechanism.

| Broker listener | `KAFKA_SECURITY_PROTOCOL` | `KAFKA_SASL_MECHANISM` | Also set |
|---|---|---|---|
| Plaintext (bundled broker, private network) | *unset* / `PLAINTEXT` | — | — |
| TLS, no client auth | `SSL` | — | `KAFKA_SSL_CA_LOCATION` if the CA is private |
| mTLS | `SSL` | — | `KAFKA_SSL_CERT_LOCATION` + `KAFKA_SSL_KEY_LOCATION` |
| SASL over TLS — the usual managed default | `SASL_SSL` | `SCRAM-SHA-512` (or `-256`, or `PLAIN`) | `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` |
| SASL, unencrypted listener | `SASL_PLAINTEXT` | as above | as above |
| MSK IAM | `SASL_SSL` | `AWS_MSK_IAM` | `KAFKA_AWS_REGION` |
| OIDC / Confluent Cloud OAuth | `SASL_SSL` | `OAUTHBEARER` | the `KAFKA_SASL_OAUTHBEARER_*` set below |

**MSK / Confluent with SCRAM — the common case:**

```bash
KAFKA_BROKERS=b-1.mycluster.kafka.eu-west-1.amazonaws.com:9096,b-2.mycluster.kafka.eu-west-1.amazonaws.com:9096
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=rsync-app
KAFKA_SASL_PASSWORD=<the secret stored in your AWS Secrets Manager SCRAM secret>
```

> **Setting a username and password without `KAFKA_SECURITY_PROTOCOL` does not
> authenticate.** The credentials are discarded and the connection is anonymous
> (`shared/go/kafkaclient/config.go:290-297` logs it once as `sasl-ignored`). A
> broker that enforces ACLs then rejects you with an error naming the *broker*,
> not the missing setting; a broker that does not enforce them accepts you, and
> the credentials were never checked. This is the single most common BYO slip —
> the protocol is the load-bearing variable, not the password.

`PLAIN` / `SCRAM-SHA-256` / `SCRAM-SHA-512` all require both the username and
the password; either alone is rejected at startup (`config.go:424-425`).

**MSK IAM:**

```bash
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=AWS_MSK_IAM
KAFKA_AWS_REGION=eu-west-1
```

There is no static secret: the token is signed from the ambient AWS credential
chain (instance profile, IRSA, or `AWS_*` environment variables), so the task or
node needs an IAM policy granting `kafka-cluster:Connect` plus the topic and
group actions in [Kafka ACLs](kafka-acls.md). `KAFKA_AWS_REGION` falls back to
`AWS_REGION` and then `AWS_DEFAULT_REGION`. `SASL_SSL` is mandatory and enforced
— the token is a bearer credential, so an unencrypted listener is refused
outright rather than warned about (`config.go:461-464`).

> **IAM covers the Go services only.** The Python tier raises
> `KafkaSecurityError` for `AWS_MSK_IAM`
> (`llm-service/src/utils/kafka_security.py:123,422-427`) and the JVM Kafka
> Connect image ships no IAM login module, so an IAM-only cluster runs the Go
> data plane and leaves the CDC profile unable to authenticate. **If you use
> CDC, use SCRAM.**

**OAUTHBEARER (OIDC client credentials):**

```bash
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=OAUTHBEARER
KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT=https://issuer.example.com/oauth2/token
KAFKA_SASL_OAUTHBEARER_CLIENT_ID=rsync-kafka-client
KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET=<client secret>
KAFKA_SASL_OAUTHBEARER_SCOPE=kafka
# Confluent Cloud identity pools, if your provider needs them:
KAFKA_SASL_OAUTHBEARER_EXTENSIONS=logicalCluster=lkc-00000,identityPoolId=pool-00000
```

`CLIENT_ID` / `CLIENT_SECRET` fall back to `KAFKA_SASL_USERNAME` /
`KAFKA_SASL_PASSWORD` when unset. `KAFKA_SASL_OAUTHBEARER_LOGIN_CALLBACK_HANDLER`
is JVM-only (Kafka Connect and Debezium's schema-history client) and should stay
empty unless your broker's Kafka version needs a specific handler class.

### 5. Mounting the TLS material

**`KAFKA_SSL_CA_LOCATION`, `KAFKA_SSL_CERT_LOCATION`, `KAFKA_SSL_KEY_LOCATION`
and `KAFKA_SSL_KEYSTORE_LOCATION` are paths *inside the container*.** Compose
cannot guess a host path, so nothing mounts them for you: pointing them at a
file on the host and restarting gives you a client that cannot find its CA.

You only need these when the broker presents a certificate from a private CA.
MSK and Confluent Cloud use publicly-trusted CAs, and the images already carry
the system trust store — leave all four unset there.

Put the files in one host directory and bind it read-only into every service
that talks to Kafka, via an overlay of your own:

```yaml
# docker-compose.kafka-certs.yml
x-kafka-certs: &kafka-certs
  - /etc/rsync/kafka-certs:/etc/rsync/kafka:ro

services:
  orchestrator:
    volumes: *kafka-certs
  api-gateway:
    volumes: *kafka-certs
  temporal-adapter:
    volumes: *kafka-certs
  planner:
    volumes: *kafka-certs
  kafka-init:
    volumes: *kafka-certs
  llm-service:
    volumes: *kafka-certs
  # The three below belong to the `cdc` profile — a default `up` renders without
  # them, and naming a service that is not in the active profile is harmless.
  debezium-mcp:
    volumes: *kafka-certs
  kafka-mcp-sink-mcp:
    volumes: *kafka-certs
  # Kafka Connect additionally reads its own CONNECT_*_TRUSTSTORE settings.
  kafka-connect:
    volumes: *kafka-certs
```

```bash
docker compose -f docker-compose.quickstart.yml \
               -f docker-compose.byo-kafka.yml \
               -f docker-compose.kafka-certs.yml up -d
```

Confirm the mount reached every service before you restart anything —
`config` is read-only and needs no daemon-side changes:

```bash
docker compose -f docker-compose.quickstart.yml -f docker-compose.byo-kafka.yml \
               -f docker-compose.kafka-certs.yml --profile cdc config \
  | grep -c '/etc/rsync/kafka'
```

Then the paths in `.env` are the *container-side* ones:

```bash
KAFKA_SSL_CA_LOCATION=/etc/rsync/kafka/ca.pem
# mTLS — both or neither. Half a pair is rejected at startup rather than
# silently downgrading to server-only TLS (config.go:445-447).
KAFKA_SSL_CERT_LOCATION=/etc/rsync/kafka/client.crt
KAFKA_SSL_KEY_LOCATION=/etc/rsync/kafka/client.key
```

`KAFKA_SSL_KEYSTORE_LOCATION` is **the same keypair in the one shape a JVM can
load: a single file holding the chain and the key.** It is not derivable from
the two paths above, so build it explicitly and set it *in addition* whenever
you run `kafka-init` or the CDC profile against an mTLS cluster:

```bash
cat client.crt client.key > /etc/rsync/kafka-certs/client.pem
```

```bash
KAFKA_SSL_KEYSTORE_LOCATION=/etc/rsync/kafka/client.pem
```

> **Certificate verification has two spellings for one setting.**
> `KAFKA_SSL_SKIP_VERIFY` is the documented name and the only one the Python
> services read (`kafka_security.py:54`); the Go services read
> `KAFKA_SSL_INSECURE_SKIP_VERIFY` as well (`config.go:80,115`). The shipped
> compose files default each to the other so setting *either* reaches every
> service — but if you write them into a `.env` by hand, write both. Turning
> verification off is for a self-signed lab only; mounting the CA is the
> production answer.

### 6. Verify

```bash
docker compose logs orchestrator api-gateway temporal-adapter | grep -i kafka
```

A healthy BYO connection logs the resolved settings with the password redacted.
Two log lines mean it is *not* working the way you think:

- `SASL settings are configured … they are IGNORED and this connection is
  anonymous` — you set credentials but not `KAFKA_SECURITY_PROTOCOL` (step 4).
- a connection that establishes against `:9092` while you set `SASL_SSL` — the
  port belongs to a different listener (step 1).

Do not treat "the stack came up" as proof: with the bundled broker excluded and
a misconfigured client, the API still serves and pipelines still accept work.
The failure surfaces as a pipeline that reports `completed` with nothing at the
destination.

On Kubernetes this is `kafka.enabled: false` plus `kafka.external.*`, including
SASL and TLS — see [Kubernetes](kubernetes.md#kafka-you-already-run).

---

## Bring Your Own PostgreSQL

The stack ships a Postgres container so evaluation works with no external
dependencies. If you already run a managed instance — RDS, Cloud SQL, Azure
Database, Neon, or your own server — point the stack at it and drop the bundled
one. Backups, HA, and patching then belong to your provider instead of to a
container on one box.

This is the **metadata** database: pipeline definitions, run history, schedules,
and the encrypted connection credentials. The rows your pipelines move never
land here — they go to whatever destination the pipeline names.

**1. Set the connection in your `.env`:**

```bash
POSTGRES_HOST=my-instance.abc123.us-east-1.rds.amazonaws.com
POSTGRES_PORT=5432
POSTGRES_USER=rsync
POSTGRES_PASSWORD=<your password>
POSTGRES_DB=pipeline_db
POSTGRES_SSLMODE=require
POSTGRES_TLS_ENABLED=true
```

**2. Create the database and a role that can create tables in it.** Three
components bootstrap themselves on first boot and each needs DDL:

| Component | What it creates |
|---|---|
| `api-gateway` | its own tables in `POSTGRES_DB`, via migrations at startup |
| `orchestrator` | its own tables in `POSTGRES_DB`, via migrations at startup |
| `temporal` | the **separate** `temporal` and `temporal_visibility` databases |

```sql
CREATE ROLE rsync LOGIN PASSWORD '<your password>';
CREATE DATABASE pipeline_db OWNER rsync;
```

You must create Temporal's two databases yourself. This is **not** a
managed-instance caveat — it applies to every external database:

```sql
CREATE DATABASE temporal OWNER rsync;
CREATE DATABASE temporal_visibility OWNER rsync;
```

`docker-compose.byo-postgres.yml` sets `SKIP_DB_CREATE=true`, so auto-setup will
not create them for you, and the two halves go together. Leave the create
enabled and it runs whether or not the databases exist — its only guard is the
name test `${DBNAME} != ${POSTGRES_USER}` — so it fails with `pq: permission
denied to create database` on any role without `CREATEDB`, which is what a
managed instance normally hands you. Pre-creating does not help there, because
the create is still attempted. Skip it without creating them and the next step
fails instead, with `pq: database "temporal" does not exist`.

Either way the container does not degrade, it dies: the image entrypoint is
`auto-setup.sh && start-temporal.sh` under `set -e`, so the server is never
reached, the pod crash-loops, and every pipeline hangs with no workflow engine.

**2b. Make sure the two extensions exist.** The api-gateway migrations issue
`CREATE EXTENSION` at startup, and on a locked-down managed instance the
application role is usually not allowed to:

| Extension | Created by | Needed for |
|---|---|---|
| `uuid-ossp` | `001_init_schema.sql:5`, `021_add_audit_logs.sql:4` | `uuid_generate_v4()`, the `DEFAULT` on the primary key of `users` and ~24 other migrations' tables |
| `pg_trgm` | `020b_workspace_connection_refs.sql:12` | the `gin_trgm_ops` indexes at `020b:47-51`, behind fuzzy connection lookup ("production", "staging") |

**`IF NOT EXISTS` does not make these optional.** It suppresses the error when
the extension is *already installed*; when it is absent and your role lacks the
privilege, it still raises — `permission denied to create extension "uuid-ossp"`
on RDS/Cloud SQL, or `extension "uuid-ossp" is not allow-listed` on Azure.

The most portable fix is to **pre-create both as an admin**, which turns the
migrations' statements into the no-op that `IF NOT EXISTS` is meant to be. Run
this once against `POSTGRES_DB`, as your instance's superuser-equivalent
(`rds_superuser` on RDS/Aurora, `azure_pg_admin` on Azure, `postgres` on Cloud
SQL):

```sql
\c pipeline_db
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

Granting the privilege instead also works, and on PostgreSQL 13+ both of these
are *trusted* extensions, so `CREATE` on the database is enough — no superuser:

```sql
GRANT CREATE ON DATABASE pipeline_db TO rsync;
```

Provider caveats, because the grant alone is not always sufficient:

- **Azure Database for PostgreSQL** keeps a server-level allow-list. Add
  `UUID-OSSP` and `PG_TRGM` to the `azure.extensions` server parameter first —
  no role-level grant overrides it.
- **Cloud SQL** requires `cloudsqlsuperuser`; the default user has it, a
  least-privilege role you created does not.
- **RDS / Aurora** allow trusted extensions to any role with `CREATE` on the
  database. Confirm the version is 13+; on 12 and older both need
  `rds_superuser`.

> **This failure is silent, so check for it rather than waiting to notice.**
> The migration runner stops at the first failing file
> (`api-gateway/internal/db/migrate.go:127-129`), and `001_init_schema.sql` is
> the first file — so a privilege error there means **no** migration applies at
> all. `main.go:311-313` then logs `❌ Database migration failed` and **starts
> serving anyway** (there is no `return`), and the container healthcheck hits
> `/health`, which is an unconditional `200` that never touches the database
> (`main.go:614-621`). The result is a stack that comes up "healthy" against an
> empty schema, and every request fails with `relation "..." does not exist`.
>
> Verify explicitly after the first boot — `/ready` is the endpoint that knows,
> and it is not what the healthcheck uses:
>
> ```bash
> docker compose exec api-gateway curl -sf localhost:8080/ready
> # ready         => {"status":"ready"}
> # NOT migrated  => {"status":"not_ready","reason":"schema_not_migrated"}
>
> docker compose logs api-gateway | grep -i "migration failed"
> ```

**3. Start with the BYO overlay so the bundled database is not started:**

```bash
docker compose -f docker-compose.quickstart.yml -f docker-compose.byo-postgres.yml up -d
```

If you installed with `install.sh`, skip the flag — set `POSTGRES_HOST` in
`~/rsync-ai/.env` and re-run the installer. It layers the overlay for you when
the host is not the bundled `postgres`, and does the same for `KAFKA_BROKERS`.

Without the overlay the app tier still honours `POSTGRES_HOST`, but the bundled
`postgres` container starts anyway and keeps its volume — you pay for a database
nothing reads, and its stale contents are one `POSTGRES_HOST` typo away from
looking like your real metadata.

### TLS is two switches, not one

`POSTGRES_SSLMODE` covers the three Go services — the api-gateway DSN, the
orchestrator's `DB_SSLMODE`, and the temporal-adapter's `POSTGRES_SSLMODE`.
**Temporal ignores it entirely**, and Temporal is really two programs: the
`auto-setup` image runs `temporal-sql-tool` to create the schemas and *then*
starts the server, and the two read different names for the same fact. The
compose file feeds both from one key each, so you set only the left column:

| `.env` key | Default | Server | Schema tool |
|---|---|---|---|
| `POSTGRES_TLS_ENABLED` | `false` \| `true` † | `SQL_TLS_ENABLED` | `SQL_TLS` |
| `POSTGRES_TLS_CA_FILE` | *(empty)* | `SQL_CA` | `SQL_TLS_CA_FILE` |
| `POSTGRES_TLS_SERVER_NAME` | *(empty)* | `SQL_HOST_NAME` | `SQL_TLS_SERVER_NAME` |
| `POSTGRES_TLS_VERIFY_HOST` | `false` | `SQL_HOST_VERIFICATION` | — |
| `POSTGRES_TLS_SKIP_HOST_VERIFY` | `true` | — | `SQL_TLS_DISABLE_HOST_VERIFICATION` |

Host verification is the one pair you set twice, because the schema tool states
it *inverted* and Compose cannot negate a value. To verify hostnames, set
`POSTGRES_TLS_VERIFY_HOST=true` **and** `POSTGRES_TLS_SKIP_HOST_VERIFY=false`.

† **The default differs by which compose file you run, and so does the name of
the sslmode knob.** The two paths are not interchangeable:

| | `docker-compose.quickstart.yml` (+ BYO overlay) | `docker-compose.yml` + `docker-compose.prod.yml` |
|---|---|---|
| sslmode key | `POSTGRES_SSLMODE` | `DB_SSLMODE` |
| sslmode default | `disable` | `require` |
| `POSTGRES_TLS_ENABLED` default | `false` | `true` |

The quickstart path defaults to plaintext because it ships a bundled Postgres on
the same Docker network, where that is correct. The prod path has no bundled
database — `POSTGRES_HOST` is required and every service asserts
`RSYNC_REQUIRE_REMOTE_DB=true` — so it defaults to TLS on both halves.

To point the **prod** path at a database that speaks plaintext, turn off both:
`DB_SSLMODE=disable` and `POSTGRES_TLS_ENABLED=false`. Setting one and not the
other is what this pairing exists to prevent; it is asserted by
`llm-service/tests/test_prod_compose_postgres_tls_is_coherent.py`.

> Set `POSTGRES_TLS_ENABLED=true` alongside `POSTGRES_SSLMODE` against a
> database that requires TLS. With sslmode alone, the three Go services connect
> fine and the schema step fails before the server ever binds — so the symptom
> is not a TLS error, it is that no workflow engine starts and every pipeline
> hangs, with nothing in the logs naming TLS.

Conversely, do not leave `POSTGRES_SSLMODE` unset against a database that
*permits* plaintext. An omitted `sslmode` does not error — it negotiates an
unencrypted connection, and your database password crosses the network in the
clear while everything looks healthy.

`POSTGRES_TLS_CA_FILE` names a path **inside the Temporal container**; mount
your CA bundle there yourself if your provider requires a private root.

The two overlays are independent — combine them freely:

```bash
docker compose -f docker-compose.quickstart.yml \
  -f docker-compose.byo-postgres.yml \
  -f docker-compose.byo-kafka.yml up -d
```

On Kubernetes this is `postgresql.enabled: false` plus `postgresql.external.*`;
`postgresql.external.sslMode` drives all four consumers, and Temporal's TLS
switches are derived from it — see [Kubernetes](kubernetes.md#postgres-you-already-run).

---

## Connector Setup

rsync-ai uses MCP (Model Context Protocol) connectors to interface with external services. The core stack includes several internal connectors:

| Connector | Purpose | Included by Default |
|-----------|---------|---------------------|
| `debezium-mcp` | CDC change capture | Yes |
| `minio-mcp` | Staging data (claim-check) | Yes |
| `kafka-mcp-sink-mcp` | Kafka sink operations | Yes |
| `context7-mcp` | Documentation lookup | Yes |

### External Connectors

External/user-facing connectors (Stripe, Shopify, etc.) are defined in `docker-compose.mcp.yml`. To enable them:

```bash
docker compose -f docker-compose.mcp.yml --env-file .env.prod up -d
```

Start this file **on its own**, without the base and prod files. It declares its own
`name: rsync-ai-mcp` so the connectors show up as a separate group, and when several `-f`
files each declare a name the last one wins — stacking it on top of `docker-compose.yml`
would re-create every base and prod service a second time under the `rsync-ai-mcp`
project, alongside the stack already running.

### Generated Connectors

The tool-generator can create new connectors on-the-fly. To enable auto-deployment of generated connectors, set in `.env.prod`:

```env
RSYNC_MANAGED_CONNECTORS=true
```

This requires mounting Docker socket to the tool-generator container (see the base `docker-compose.yml` for the volume configuration).

### Updating connector code (redeploying a connector)

**Important:** `scripts/deploy-service.sh` deploys only the core services (api-gateway, frontend, orchestrator, llm-service, …). It does **NOT** touch MCP connectors — they run as a separate compose project (`rsync-ai-mcp`). When you change a connector's code (`connector.py`, `metadata.json`, etc.) you must rebuild that connector's image yourself.

How connectors map to containers:
- Each connector **version** is its own image, built from `shared/mcp-connectors/public/<connector>/versions/<current_version>/`.
- `docker-compose.mcp.yml` is **auto-generated** by `scripts/mcp_generate_compose.py`, which reads each connector's `latest.json → current_version` to decide which version dir to build and names the service/container `rsync-ai-<id>-vX-Y-Z-mcp`. **Do not edit `docker-compose.mcp.yml` by hand** — regenerate it.
- The orchestrator routes a connection's `connector_version` (or `"latest"`) to the matching container.

Deploy a connector code change on the prod VM:

```bash
cd ~/rsync-ai
git pull origin main                                   # 1. get the updated connector code + latest.json
python3 scripts/mcp_generate_compose.py                # 2. regenerate the MCP compose from latest.json
# 3. rebuild + restart ONLY the changed connector (use the versioned service name):
docker compose -f docker-compose.mcp.yml build <connector>-v<x-y-z>-mcp
docker compose -f docker-compose.mcp.yml up -d --no-deps <connector>-v<x-y-z>-mcp
```

Example — the Shopify connector (active version v1.0.1):

```bash
docker compose -f docker-compose.mcp.yml build shopify-admin-graphql-v1-0-1-mcp
docker compose -f docker-compose.mcp.yml up -d --no-deps shopify-admin-graphql-v1-0-1-mcp
# verify the new code is in the image:
docker exec rsync-ai-shopify-admin-graphql-v1-0-1-mcp grep -n config_keys /app/connector.py
```

Notes:
- A connector code change is **not** picked up until you `build` — Docker reuses the cached image otherwise. Always `build` then `up -d`.
- **In-place fix (no version bump):** the service name is unchanged; just rebuild the existing `vX-Y-Z` image (the active version per `latest.json`).
- **New version bump:** step 2 regenerates the compose with the new service name (from the bumped `latest.json.current_version`); build + `up -d` that new service. See the [connector developer guide](../connectors/developer-guide.md) for when to bump vs patch in place.

---

## Monitoring (Optional)

rsync-ai ships with OpenTelemetry instrumentation. For full observability, deploy [SigNoz](https://signoz.io/):

### SigNoz Setup

> **Use the in-repo vendored config, NOT a vanilla upstream clone.** rsync-ai ships a
> customized SigNoz stack under [`deploy/signoz/`](../../deploy/signoz/) with deviations the
> platform depends on: UI on **3301** (host 8080 is Traefik's), ClickHouse HTTP exposed on
> **8123** (api-gateway diagnose log-enrichment), and a **ClickHouse memory bound**. A
> plain `git clone` of upstream SigNoz has none of these and will OOM its ClickHouse under
> load (exit 137).

SigNoz runs as a separate Docker Compose stack (project name `signoz`). Launch it with the
guarded helper, which always runs from the main checkout so ClickHouse's bind-mounts stay
stable (never launch it from a `.claude/worktrees/` path — a pruned worktree leaves dangling
mounts that block ClickHouse from restarting after an OOM):

```bash
# From the rsync-ai repo root on the prod VM:
scripts/signoz-up.sh                       # up -d (UI: http://<server-ip>:3301)
scripts/signoz-up.sh status                # ps + ClickHouse ingest-lag check
```

Equivalent raw command (the script just resolves the main root and runs this):

```bash
docker compose -p signoz -f deploy/signoz/docker/docker-compose.yaml up -d
```

**Size ClickHouse to the VM.** The compose defaults to `mem_limit: 3g` (tuned for the
staging Docker VM). On a larger prod VM, set it to ~40% of host RAM and keep the
`max_server_memory_usage_to_ram_ratio=0.7` in
[`deploy/signoz/common/clickhouse/config.xml`](../../deploy/signoz/common/clickhouse/config.xml):

```bash
CLICKHOUSE_MEM_LIMIT=8g scripts/signoz-up.sh    # e.g. on a ~20 GB VM
```

ClickHouse is `restart: unless-stopped`, so once launched from a stable path it self-heals
after a crash. rsync-ai services are pre-configured to send traces and logs to the OTel
Collector, which forwards to SigNoz.

### SigNoz on a Linux prod VM — two mandatory first-time steps

SigNoz was validated on the staging Mac (Docker Desktop). On a **native Linux prod VM**
two things that "just work" on the Mac must be done explicitly the first time you bring
SigNoz up. Skip either and **nothing ingests** — the UI stays empty with no obvious error.

**1. Create the organization (first-run UI setup).** SigNoz's otel-collector fetches its
pipeline config from the SigNoz server over OpAMP, and the server refuses to register the
collector until an org exists. Symptom in `docker logs signoz`:
`cannot create agent without orgId` (every 30s); the collector never starts its `4317`
receiver, so `docker logs signoz-otel-collector` has **no** `Starting GRPC server ... 4317`
line. Fix: tunnel to the UI and create the admin account.
```bash
# from your laptop (the UI must NOT be publicly exposed):
ssh -L 3301:localhost:3301 <user>@<prod-vm>
# then open http://localhost:3301 and create the admin email/password (creates the org)
```

**2. Wire the rsync-ai collector to SigNoz over the shared network.** On Linux the default
`host.docker.internal:4317` exporter endpoint is refused (see the macOS-vs-Linux callout in
[deploy/signoz/README.md](../../deploy/signoz/README.md)). Put the rsync-ai collector on
`signoz-net` and target SigNoz's collector by container name:
```bash
P="-f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod"
# attach the app collector to SigNoz's network
docker network connect signoz-net rsync-ai-otel-collector
# point the exporter at the container name instead of host.docker.internal
sed -i 's#host.docker.internal:4317#signoz-otel-collector:4317#' deploy/otel-collector-config.yaml
docker compose $P restart otel-collector
```

> **⚠️ The `docker network connect` is EPHEMERAL.** A future `docker compose up` (i.e. your
> next core deploy, step 4 above) recreates `rsync-ai-otel-collector` and silently drops the
> `signoz-net` attachment — breaking ingestion again. Until this is folded into compose
> (declare `signoz-net` as an external network on the `otel-collector` service and set the
> endpoint to `signoz-otel-collector:4317` in `deploy/otel-collector-config.yaml`), you must
> **re-run the two commands above after every core redeploy**. The `sed` edit to
> `deploy/otel-collector-config.yaml` also makes `git status` dirty — commit it or keep it as
> a tracked deviation; do not leave it as an unexplained local change.

### Verify Traces

0. Confirm the two steps above are done: `docker logs signoz-otel-collector --tail=20 | grep 4317`
   should show `Starting GRPC server ... [::]:4317`, and `docker logs rsync-ai-otel-collector
   --since 2m | grep refused` should be empty.
1. Open SigNoz at `http://localhost:3301` (over the SSH tunnel)
2. Go to **Services** — you should see `api-gateway`, `orchestrator`, `temporal-adapter`, etc.
3. Go to **Logs** — verify structured JSON logs are flowing
4. Sanity-check ingest freshness from the host (should be a few seconds; a value in the
   billions means ZERO rows — i.e. nothing has ingested yet, re-check the two steps above):
   ```bash
   curl -s http://localhost:8123/ --data \
     "SELECT dateDiff('second', toDateTime(max(timestamp)/1e9), now()) AS lag_s FROM signoz_logs.logs_v2"
   ```

---

## Admin Panel Setup

The admin panel provides operator-level access to pipeline management, system health, and configuration.

Configure admin access by setting `RSYNC_ADMIN_EMAILS` in `.env.prod`:

```env
RSYNC_ADMIN_EMAILS=alice@company.com,bob@company.com
```

Only authenticated users whose email matches this allowlist will see the admin panel.

---

## Backup & Restore

### PostgreSQL

```bash
# Backup
docker compose exec postgres pg_dump -U ${POSTGRES_USER:-rsync} pipeline_db \
  | gzip > backup_postgres_$(date +%Y%m%d_%H%M%S).sql.gz

# Restore
gunzip -c backup_postgres_YYYYMMDD_HHMMSS.sql.gz \
  | docker compose exec -T postgres psql -U ${POSTGRES_USER:-rsync} pipeline_db
```

### Redis

```bash
# Backup (triggers RDB snapshot)
docker compose exec redis redis-cli -a "${REDIS_PASSWORD}" BGSAVE
docker cp $(docker compose ps -q redis):/data/dump.rdb backup_redis_$(date +%Y%m%d).rdb

# Restore
docker compose stop redis
docker cp backup_redis_YYYYMMDD.rdb $(docker compose ps -q redis):/data/dump.rdb
docker compose start redis
```

### Kafka

```bash
# Backup Kafka data volume
docker compose stop kafka
docker run --rm -v rsync-ai_kafka_data:/data -v $(pwd):/backup \
  alpine tar czf /backup/backup_kafka_$(date +%Y%m%d).tar.gz -C /data .
docker compose start kafka
```

### Full Volume Backup

```bash
# Stop all services
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
  --env-file .env.prod down

# Backup all volumes
for vol in rsync-ai_postgres_data rsync-ai_kafka_data rsync-ai_redis_data; do
  docker run --rm -v ${vol}:/data -v $(pwd)/backups:/backup \
    alpine tar czf /backup/${vol}_$(date +%Y%m%d).tar.gz -C /data .
done

# Restart
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
  --env-file .env.prod up -d
```

---

## Upgrading

### Moving code to the prod VM (standard upgrade)

Run from the rsync-ai repo root on the prod VM. **Order matters and every step is
required** unless marked optional. The two steps people miss — and the two that have
caused real prod issues — are step 3 (rebuild **all** MCP connectors, not just core)
and the (non-existent) migrate step (see the callouts below).

```bash
export DOCKER_BUILDKIT=1            # REQUIRED — connector images build with BuildKit named contexts

# 0. Capture the current commit as your rollback point, and confirm a clean tree
git rev-parse HEAD                  # SAVE THIS hash — it's your rollback target (see Rolling Back)
git status -s                       # must be EMPTY; stash/commit any local edits before pulling

# 1. Pull the new code
git fetch origin
git checkout main && git pull origin main      # expect a clean "Fast-forward"; if it isn't, STOP

# 2. Rebuild the CORE stack (Go services + frontend). NEXT_PUBLIC_* vars are baked
#    into the frontend bundle at build time, so the frontend MUST be rebuilt here.
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
  --env-file .env.prod build --pull

# 3. Rebuild the MCP CONNECTORS — REQUIRED, and the easiest step to forget.
#    The core build in step 2 does NOT touch MCP connectors (they are a separate
#    compose project, rsync-ai-mcp). A change to the shared connector library,
#    base_connector.py, or the BuildKit build context affects EVERY connector, so
#    rebuild the whole set — not just the one connector you think changed. Skipping
#    this leaves stale connector code running against new core services.
python3 scripts/mcp_generate_compose.py        # regenerate compose from latest.json (idempotent)
docker compose -f docker-compose.mcp.yml --env-file .env.prod build

# 4. Restart both stacks. Database migrations apply AUTOMATICALLY here — api-gateway
#    runs them on boot (see callout). There is no separate migrate command to run.
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
  --env-file .env.prod up -d
docker compose -f docker-compose.mcp.yml --env-file .env.prod up -d
```

> **There is NO `migrate` step — do not run `... run --rm api-gateway /app/migrate`.**
> That binary does not exist; the api-gateway image's only entrypoint is `./server`,
> which applies migrations on boot via `db.Migrate("migrations")`
> ([api-gateway/cmd/server/main.go](../../api-gateway/cmd/server/main.go)). The old
> `run --rm` form fails with `stat /app/migrate: no such file or directory` **and**
> half-starts dependency containers as a side effect. Migrations run automatically in
> step 4's `up -d` — confirm via the log check below.

> **Connector versions are usually patched in place (no `latest.json` bump).** Bug
> fixes patch `versions/<current_version>/` and keep the same version, so step 3's
> `mcp_generate_compose.py` regenerates the same service names and a plain rebuild
> picks up the new code. A deliberate `vX.Y.(Z+1)` bump changes the service name —
> step 3 still handles it (it reads `latest.json`), but the old-version container
> keeps running until you `down` it.

### Verify the upgrade

```bash
P="-f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod"

# (a) all core services healthy; MCP connectors flip "health: starting" -> healthy in ~30-60s
docker compose $P ps
docker ps --format '{{.Names}}\t{{.Status}}' | grep mcp

# (b) migrations applied cleanly on api-gateway boot
docker compose $P logs api-gateway --tail=120 | grep -iE "Running database migrations|❌"
#     want "🔄 Running database migrations..." with NO "❌ Database migration failed"
#     (a release with no new migration files is a clean no-op — that's expected)

# (c) no errors across the app stack AND the connector stack
docker compose $P logs --since 10m 2>&1 | grep -iE "panic|fatal|level=error|❌" | tail -40
docker compose -f docker-compose.mcp.yml --env-file .env.prod logs --since 10m 2>&1 \
  | grep -iE "panic|fatal|error|❌" | tail -40
```

For any change that touched the orchestrator or temporal-adapter, re-run checks (a)–(c)
above and drive one pipeline end to end before calling the deploy successful. If any of
them fails, do **not** call it successful — diagnose first.

### Rolling Back

```bash
export DOCKER_BUILDKIT=1
git checkout <rollback-hash>        # the HEAD hash you saved in step 0

# rebuild + restart BOTH stacks (core first, then connectors)
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod build
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up -d
python3 scripts/mcp_generate_compose.py
docker compose -f docker-compose.mcp.yml --env-file .env.prod build
docker compose -f docker-compose.mcp.yml --env-file .env.prod up -d
```

> **A rollback that crosses a connector version bump needs a manual `down`.** If the
> forward deploy bumped a connector to a new `vX.Y.Z`, `git checkout` restores the old
> `latest.json` and `mcp_generate_compose.py` regenerates the old service name — but the
> new-version container keeps running. Check `docker ps | grep mcp` after rollback and
> `docker rm -f` any stranded newer-version container.

---

## Troubleshooting

### Services fail to start

```bash
# Check which services are unhealthy
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
  --env-file .env.prod ps

# View logs for a specific service
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
  --env-file .env.prod logs <service-name> --tail=100
```

### "connection refused" errors between services

Services communicate over the Docker network. Ensure:
- All services are on the same Docker Compose project (started together)
- No custom `network_mode` overrides
- Services reference each other by service name (e.g., `postgres`, not `localhost`)

### Kafka consumer rebalancing / timeouts

If you see frequent rebalancing in orchestrator logs:
- Ensure only 1 replica of the orchestrator is running (see the scaling warning in `docker-compose.yml`)
- Check Kafka health: `docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list`

### TLS certificate not provisioning

- Verify DNS: `dig rsync.example.com` should return your server's IP
- Ensure port 80 is open (required for HTTP challenge)
- Check Traefik logs: `docker compose logs traefik`
- Verify ACME email is set: check `ACME_EMAIL` in `.env.prod`
- Let's Encrypt has [rate limits](https://letsencrypt.org/docs/rate-limits/) — avoid restarting Traefik repeatedly

### Explorer not working

The Explorer uses `LLM_PROVIDER` from `llm-service/.env`. Check:

1. **Azure/OpenAI**: Verify `AZURE_OPENAI_DEPLOYMENT` matches the deployment name exactly (case-sensitive). Check llm-service logs:
   ```bash
   docker compose logs rsync-ai-llm-service | grep -i "azure\|deployment\|error"
   ```

2. **Ollama (air-gapped)**: If `EXPLORER_OFFLINE_ONLY=true`, verify Ollama is running:
   ```bash
   curl http://localhost:11434/api/tags
   ollama list
   ```

### Database migrations failed

Migrations run automatically when **api-gateway boots** (`db.Migrate("migrations")` in
`cmd/server/main.go`) — there is no separate migrate binary. To inspect or re-run, work
through api-gateway:

```bash
P="-f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod"

# Check what api-gateway logged on its last boot
docker compose $P logs api-gateway --tail=200 | grep -iE "migration|❌"

# Check applied migration versions in the DB
docker compose exec postgres psql -U rsync -d pipeline_db \
  -c "SELECT * FROM schema_migrations ORDER BY version DESC LIMIT 5;"

# Re-run migrations = restart api-gateway (it re-applies on boot, idempotently)
docker compose $P restart api-gateway
```

### Out of memory (exit code 137)

If containers are killed with exit code 137, increase memory limits in `docker-compose.prod.yml` or add more RAM to your server. Check which service is affected:

```bash
docker inspect <container-id> --format='{{.State.OOMKilled}}'
```

### Redis authentication errors

If services can't connect to Redis after enabling password:

- Verify `REDIS_PASSWORD` is set in `.env.prod`
- Ensure orchestrator has `REDIS_PASSWORD` in its environment
- Check: `docker compose exec redis redis-cli -a "${REDIS_PASSWORD}" ping`
