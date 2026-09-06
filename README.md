# rsync.ai

[![License: ELv2](https://img.shields.io/badge/license-ELv2-3b82f6)](LICENSE)
[![Deploy: Docker Compose](https://img.shields.io/badge/deploy-Docker%20Compose-2496ED?logo=docker&logoColor=white)](#docker--one-command)
[![Deploy: Helm](https://img.shields.io/badge/deploy-Helm%20chart-0F1689?logo=helm&logoColor=white)](#kubernetes)
[![Connectors](https://img.shields.io/badge/connectors-21-16a34a)](docs/connectors/reference.md)
[![Docs](https://img.shields.io/badge/docs-read%20the%20guides-64748b)](docs/README.md)

> **Describe a data pipeline in plain English. rsync plans it, asks you when it needs a
> decision, then runs it on durable infrastructure you host yourself.**

rsync.ai is a self-hosted data platform for moving data between databases, warehouses,
object stores and APIs. You describe the job in a sentence; an agent turns it into an
explicit, staged plan, pauses for you when something is ambiguous, and executes it on
Temporal so a long sync survives restarts. Batch and change-data-capture are both
first-class. Twenty-one connectors ship in the box.

It is **source-available** under the [Elastic License 2.0](LICENSE): run it, modify it,
and use it internally for free — you just cannot resell it as a hosted service. The
[full summary is below](#license).

**Try it without a single credential of your own.** The stack bundles a
`sample-data` source and a throwaway `demo-warehouse` Postgres, so you can build and run
a real pipeline end to end on the first-run checklist —
[Try it in 5 minutes](docs/getting-started/quickstart.md#try-it-in-5-minutes-with-no-credentials).

## Contents

- [Install](#install) — [Docker](#docker--one-command) · [Kubernetes](#kubernetes)
- [What you get](#what-you-get)
- [Connectors](#connectors)
- [The Data Explorer](#the-data-explorer)
- [How it works](#how-it-works)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Documentation](#documentation)
- [Development](#development)
- [Project status](#project-status)
- [Community and support](#community-and-support)
- [License](#license)

---

## Install

### Docker — one command

```bash
curl -sSL https://raw.githubusercontent.com/rsync-ai/rsync/main/install.sh | bash
```

Requires Docker. The installer prompts for an OpenAI API key, generates every other
secret, and starts the full stack. Open `http://localhost:3000` when it finishes. If the
stack does not come up, the installer says so and exits non-zero — it does not print a
success banner over a dead stack.

> **Which code you get.** `v0.1.2`, the current release. Both halves of the install come
> from that one tag: the compose file is fetched from `RSYNC_REF` and the images are
> pulled at a tag derived from it, so the file and the containers it starts are the same
> commit. Every image the default compose starts is published at that tag and pullable
> anonymously — a test pins that, so a release cannot ship half-built.
>
> Pass `RSYNC_REF=main` to track the branch instead. That install is not reproducible:
> the compose file comes from the branch tip and changes with every commit, while `main`
> images track the last publish rather than the newest commit, so the two halves move at
> different rates.

### Kubernetes

```bash
git clone https://github.com/rsync-ai/rsync.git && cd rsync
helm install rsync ./deploy/helm/rsync-ai \
  --namespace rsync --create-namespace \
  --set secrets.jwtSecret="$(openssl rand -base64 32)" \
  --set secrets.encryptionKey="$(openssl rand -base64 32)" \
  --set secrets.postgresPassword="$(openssl rand -hex 24)" \
  --set secrets.minioAccessKey="$(openssl rand -hex 16)" \
  --set secrets.minioSecretKey="$(openssl rand -base64 32)" \
  --set frontend.publicUrl=https://app.example.com \
  --set frontend.apiUrl=https://api.example.com
```

That is the **evaluation** footprint — in-chart Postgres, Redis, Kafka, MinIO and
Temporal, one replica each, no backups. The chart runs the same images as the compose
stack and can point at managed Postgres, Redis, Kafka and object storage instead;
per-provider value files ship for EKS, GKE and AKS. See the
[Kubernetes guide](docs/deployment/kubernetes.md) for a production install.

> [!IMPORTANT]
> **Save `secrets.encryptionKey`.** It encrypts every stored connection credential. Read
> it back with
> `kubectl -n rsync get secret rsync-secrets -o jsonpath='{.data.ENCRYPTION_KEY}' | base64 -d`
> and keep it somewhere you will still have it after the cluster is gone — reinstalling
> with a different key makes every saved connection permanently undecryptable.

> [!TIP]
> The chart is also published to the registry, so you can install without cloning:
>
> ```bash
> helm install rsync oci://ghcr.io/rsync-ai/charts/rsync-ai --version 0.1.2 \
>   --namespace rsync --create-namespace \
>   --set secrets.jwtSecret="$(openssl rand -base64 32)" \
>   --set secrets.encryptionKey="$(openssl rand -base64 32)" \
>   --set secrets.postgresPassword="$(openssl rand -hex 24)" \
>   --set secrets.minioAccessKey="$(openssl rand -hex 16)" \
>   --set secrets.minioSecretKey="$(openssl rand -base64 32)"
> ```
>
> Both paths pull images at `.Chart.AppVersion` (**0.1.2**), and every image the chart
> names is published at that tag.

---

## What you get

| | |
|---|---|
| **Pipelines from a sentence** | Type *"sync MySQL orders to S3 every hour"*. An agent resolves it into named stages you can read before anything runs. |
| **Batch and CDC, both first-class** | Batch loads for anything, plus Debezium-backed change data capture on five databases — PostgreSQL, MySQL, SQL Server, Oracle and MongoDB. |
| **It asks instead of guessing** | When the source is ambiguous — which tables, which schema, which key — the run pauses on a human-in-the-loop gate rather than picking for you. |
| **Durable execution** | Stages run as Temporal workflows, so a multi-hour sync survives a restart, a redeploy, or a crashed worker. |
| **You can answer "why did it do that?"** | Every run emits domain events carrying stage state, row counts and a trace id, and the UI shows them stage by stage. |
| **A SQL and NL query surface** | The [Data Explorer](#the-data-explorer) queries the systems you connected — no second BI tool to stand up first. |
| **Your infrastructure, your keys** | One Docker command or one Helm chart. Credentials are encrypted at rest with a key you hold; point the LLM at OpenAI or at a local [Ollama](docs/deployment/ollama.md). |

## Connectors

**21 connectors ship in the box** — every one is a source, 17 are also destinations, and
five support change data capture. Each runs as its own versioned container, so you can
upgrade or pin one without touching the rest.

| Category | Connectors | CDC |
|---|---|---|
| **Relational** | PostgreSQL, MySQL, SQL Server, Oracle, ClickHouse, Amazon Redshift | PostgreSQL, MySQL, SQL Server, Oracle |
| **Data warehouse** | Snowflake, Google BigQuery, Databricks | — |
| **Document** | MongoDB | MongoDB |
| **Object storage** | AWS S3, Google Cloud Storage, Azure Blob Storage | — |
| **APIs** | Stripe, Shopify, GitHub, Notion, Google Sheets | — |
| **Demo and reference** | Sample Data (credential-free demo source), Petstore (OpenAPI example), Widgets-GraphQL (GraphQL example) | — |

The [connector reference](docs/connectors/reference.md) is generated from the connector
tree itself and lists exact ids, versions and per-connector source/destination support —
CI fails if it drifts, and a second guard fails if the table above stops matching it. To
add your own, start with the
[connector developer guide](docs/connectors/developer-guide.md).

## The Data Explorer

Once data has landed somewhere, you can query it without leaving rsync. Ask a question in
English and get SQL back, or write the SQL yourself; browse the schema; then keep the
useful ones — as a saved query with versions and diffs, or as a **model**: a table that
rebuilds itself on a cron, an interval, or after a given pipeline finishes. Results export
to CSV, TSV and JSON. See the [Data Explorer guide](docs/explorer/README.md) and the deep
dive on [saved queries, models and schedules](docs/explorer/saved-queries-and-models.md).

## How it works

1. **Describe.** You type *"sync MySQL orders table to S3 every hour"* into `/chat`. An
   agent reads it and drafts a staged plan.
2. **Decide.** Where the request is under-specified — which tables, which schema, which
   primary key, which credentials — the plan stops at a human-in-the-loop gate and asks.
   Nothing runs until you answer. This is the single most common reason a run is waiting
   rather than broken.
3. **Provision.** Connections are validated and stored encrypted; for CDC the publication
   and replication slot are created in the required order before Debezium is told to
   stream.
4. **Run.** Each stage is a Temporal activity, so progress is checkpointed and a restart
   resumes rather than starts over.
5. **Watch.** Row counts, stage state and a trace id are emitted as domain events and
   rendered stage by stage in the UI.

## Architecture

```
User (natural language)
  → Frontend (Next.js)
    → API Gateway (Go)
      → Orchestrator (Go workers)
        → Temporal (workflow engine)
          → MCP Connectors (versioned containers per source/destination)
```

[ARCHITECTURE.md](ARCHITECTURE.md) explains the stack and why each piece was chosen;
[docs/architecture/overview.md](docs/architecture/overview.md) has the component and
data-flow diagrams.

## Requirements

- Docker 24+ and Docker Compose v2 — or, for the Helm path, Kubernetes 1.25+ and Helm 3.8+
- 8 GB RAM minimum (16 GB recommended)
- An OpenAI API key (or a self-hosted Ollama instance — see [docs/deployment/ollama.md](docs/deployment/ollama.md))

## Documentation

| | |
|---|---|
| [Quick start](docs/getting-started/quickstart.md) | Local dev setup and first pipeline |
| [Self-hosting](docs/deployment/self-hosting.md) | Production deployment with TLS |
| [Kubernetes](docs/deployment/kubernetes.md) | Helm chart install on EKS, GKE, AKS, or any cluster |
| [Oracle Cloud (free)](docs/deployment/oracle-cloud.md) | Free 4 OCPU / 24 GB VM |
| [Connector reference](docs/connectors/reference.md) | Every shipped source and destination |
| [Connector developer guide](docs/connectors/developer-guide.md) | Build a new connector |
| [Data Explorer](docs/explorer/README.md) | SQL, natural-language queries, saved models and schedules |
| [Architecture](docs/architecture/overview.md) | System design and data flows |
| [API reference](docs/api/README.md) | REST + WebSocket endpoints |
| [Environment variables](docs/deployment/env-vars.md) | Full configuration reference |
| [Errors](docs/errors/README.md) | What each error code means and what to do about it |
| [All docs](docs/README.md) | Full documentation index |

## Development

```bash
git clone https://github.com/rsync-ai/rsync.git
cd rsync
cp .env.example .env           # add your OPENAI_API_KEY
cp llm-service/.env.example llm-service/.env
docker compose -p rsync-ai up -d
open http://localhost:3000
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for building individual services, running the test
suites, and the PR process.

## Project status

rsync.ai is young and self-hosted. It runs, it has been driven end to end, and the
connector and deployment claims on this page are checked by tests rather than asserted —
but you are early, and the two release-tag gaps called out under [Install](#install) are
the current rough edges. There is no hosted offering: every install is yours.

What that means in practice: pin a tag rather than tracking `main` if you want
reproducibility, keep `ENCRYPTION_KEY` somewhere durable before you store a credential,
and read [CHANGELOG.md](CHANGELOG.md) before upgrading. Bugs and gaps are tracked as
[GitHub issues](https://github.com/rsync-ai/rsync/issues) — that list is the register.

## Community and support

- **Questions and help** — [SUPPORT.md](SUPPORT.md) points at the right place for each kind of question
- **Bugs and feature requests** — [open an issue](https://github.com/rsync-ai/rsync/issues)
- **Contributing** — [CONTRIBUTING.md](CONTRIBUTING.md) and the [Code of Conduct](CODE_OF_CONDUCT.md)
- **Security** — report privately, never in a public issue: [SECURITY.md](SECURITY.md)
- **Changes between versions** — [CHANGELOG.md](CHANGELOG.md)

## License

rsync.ai is **source-available** under the [Elastic License 2.0](LICENSE) (ELv2) — not an
OSI "open source" license.

The `LICENSE` file is the binding text; the following is a plain-English summary (not
legal advice):

**You can:**
- Download, install, run, and modify rsync.ai on your own infrastructure
- Use it for your own internal business data pipelines
- Distribute it and your modifications under these same terms
- Contribute back to the project (see [CONTRIBUTING.md](CONTRIBUTING.md))

**You cannot:**
- Offer rsync.ai (or a modified version) to third parties as a hosted or managed service
- Move, change, disable, or circumvent any license-key functionality
- Remove or obscure the licensing, copyright, or other notices

The rsync.ai name and logo are trademarks — see [TRADEMARK.md](TRADEMARK.md). Licenses of
bundled third-party dependencies are listed in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
