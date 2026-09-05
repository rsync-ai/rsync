# rsync.ai Documentation

rsync.ai is an agentic data pipeline platform. Users describe what data they want to move in plain English — the AI builds and executes the pipeline.

---

## Start here — pick your path

| I want to… | Go to |
|------------|-------|
| **Understand what rsync-ai is + the stack & why** | [ARCHITECTURE.md](../ARCHITECTURE.md) |
| **Run it locally** | [Quick Start](getting-started/quickstart.md) |
| **Build / add a connector** | [Connector developer guide](connectors/developer-guide.md) |
| **Deploy to production** | [Self-hosting](deployment/self-hosting.md) (Docker Compose) · [Kubernetes](deployment/kubernetes.md) (EKS/GKE/AKS) |
| **Understand a single service in depth** | [Services HLD/LLD](services/INDEX.md) |
| **Query data / use the Data Explorer** | [Data Explorer](explorer/README.md) |
| **Contribute (setup, PR process, style)** | [CONTRIBUTING.md](../CONTRIBUTING.md) |

> Root-level canonical docs: [ARCHITECTURE.md](../ARCHITECTURE.md) (design + rationale) · [CONTRIBUTING.md](../CONTRIBUTING.md) (setup, PR process, style).

---

## Getting started

| | |
|---|---|
| [Quick Start](getting-started/quickstart.md) | One-command install, local dev setup, first pipeline |
| [Contributing](../CONTRIBUTING.md) | Dev environment, build, test, PR process (canonical local-setup) |
| [Self-hosting](deployment/self-hosting.md) | Production deployment with TLS, secrets, backups |
| [Kubernetes](deployment/kubernetes.md) | Helm chart install on EKS, GKE, AKS, or any cluster |
| [Cloud options](deployment/cloud-options.md) | Choosing between Oracle, AWS, Azure, and other providers |

---

## Architecture

| | |
|---|---|
| [ARCHITECTURE.md](../ARCHITECTURE.md) | **Front door** — what it is, the stack, and why each choice |
| [System overview](architecture/overview.md) | Component diagram + data-flow sequence diagrams |
| [Kafka topics](architecture/kafka-topics.md) | Topic naming, partitioning, message contracts |
| [Services (HLD/LLD)](services/INDEX.md) | Per-service high-level and low-level design |

---

## Connectors

| | |
|---|---|
| [Connector reference](connectors/reference.md) | All supported sources and destinations |
| [Developer guide](connectors/developer-guide.md) | **How to build a new MCP connector** (canonical) |
| [Base interface](connectors/base-interface.md) | Standard connector tool interface spec |
| [Add a CDC database type](connectors/add-cdc-database.md) | Checklist for adding a new database to Debezium CDC |
| [CDC exactly-once offsets](connectors/cdc-exactly-once-offsets.md) | Per-destination (Tier A/B/C) offset-tracking guide — replaces Redis dedup |
| [Optimization](connectors/optimization.md) | Schema-discovery + throughput tuning |
| [Parameter normalization](connectors/parameter-normalization.md) | Schema/param normalization rules |

> Before fixing a hand-curated connector, read the [connector developer guide](connectors/developer-guide.md): a fix must be propagated to the Jinja template + regenerated.

---

## Data Explorer

| | |
|---|---|
| [Data Explorer overview](explorer/README.md) | **Start here** — the whole feature: running SQL, NL→SQL, schema intelligence, HITL resolution, export/share, Metabase, data protection, and every route |
| [Saved queries, models & schedules](explorer/saved-queries-and-models.md) | Deep dive on the largest subsystem — materialization modes, the run authorization path, cron/interval/after-pipeline triggers, version control, and the approval gate on scheduled SQL |

> `validators.ValidateExplorerStatement` is a security boundary — it is what stops stacked SQL running under a schedule creator's authority. See [§3](explorer/saved-queries-and-models.md#3-authorization--the-part-to-change-carefully) before touching either run path.

---

## Deployment

| | |
|---|---|
| [Self-hosting](deployment/self-hosting.md) | Generic production deploy (TLS, secrets, backups, upgrades) |
| [Kubernetes (EKS/GKE/AKS)](deployment/kubernetes.md) | Helm chart install — OCI chart, per-provider values, managed data stores |
| [AWS production](deployment/aws.md) | EC2 MVP → ECS Fargate |
| [Oracle Cloud (free tier)](deployment/oracle-cloud.md) | 4 OCPU / 24 GB ARM VM, free forever |
| [CI/CD (GitHub Actions)](deployment/ci-cd.md) | Build, push, and deploy pipeline |
| [Environment variables](deployment/env-vars.md) | Full reference for all env vars |
| [Kafka ACLs](deployment/kafka-acls.md) | ACLs required to run against a customer-managed (BYO) Kafka cluster |
| [Ollama (local LLM)](deployment/ollama.md) | Running without OpenAI / air-gapped |

---

## API

| | |
|---|---|
| [API reference](api/README.md) | All REST + WebSocket endpoints, auth, error codes |
| [Port reference](api/PORT_REFERENCE.md) | Which service runs on which port |

---

## Development & Testing

| | |
|---|---|
| [Contributing](../CONTRIBUTING.md) | Canonical dev setup, build, PR process |
| [Test strategy](testing/strategy.md) | Unit, integration, and E2E test approach |
| [UI testing](testing/ui-testing.md) | Playwright E2E guide |
