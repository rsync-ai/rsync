# What you get when you self-host

This page answers one question: **if I run rsync on my own server or cluster, what
works, what needs me to bring something, and what is not there?**

Every claim below is cited to the file that decides it. There is **no edition gate** in
the code — self-host and the hosted service run the *same* api-gateway, orchestrator,
adapter and frontend images. The boundary is drawn in three places only:

| Mechanism | Where | What it decides |
|---|---|---|
| Which compose file you run | `docker-compose.quickstart.yml` (22 services) vs `docker-compose.yml` (27) | which containers exist |
| Which image artifact ships | `llm-service/oss-strip-list.txt` (20 entries) | which Python packages are inside the image |
| Feature flags, defaulting to the hosted behavior | `api-gateway/internal/config/features.go`, `plan_quota.go:73` | runtime behavior |

There is no `if edition == …` branch anywhere. `RSYNC_EDITION` is cosmetic and has zero
functional reads.

## A. Works self-hosted, no extra setup

Everything the product is for. These are on by default in
`docker-compose.quickstart.yml`.

| Capability | Notes |
|---|---|
| Batch pipelines (source → destination) | full executor, Temporal-backed |
| **CDC / streaming replication** | 5 source families: PostgreSQL, MySQL, SQL Server, Oracle, MongoDB — see [connector reference](../connectors/reference.md#cdc-prerequisites) |
| The 21-connector catalogue | databases, warehouses, object storage, SaaS APIs — [full table](../connectors/reference.md#catalogue) |
| Schema drift detection | `RSYNC_SCHEMA_DRIFT_ENABLED`, off by default in both editions |
| CDC self-heal / sentinel | detection always on; the two autonomy flags default off in both editions |
| Data explorer + NL→SQL | needs an LLM provider — see section B |
| Workspaces, members, roles, invites | **ships and functions.** Not withheld, but self-host is not tested against multi-tenant use — treat it as single-team |
| Direct SQL queries | unlimited and **not metered** in any edition |
| Unlimited pipelines | see the quota section below |
| Bring-your-own Postgres | `docker-compose.byo-postgres.yml`, full TLS (`POSTGRES_TLS_ENABLED`, `SQL_TLS_CA_FILE`) |
| Bring-your-own Kafka | `docker-compose.byo-kafka.yml`, SASL/TLS |
| Kubernetes | `deploy/helm/rsync-ai` — AKS values at `values-aks.yaml` |

## B. Works, but you supply the credential or endpoint

Nothing here is disabled. The stack needs a value only you can provide.

| Capability | You supply | Where |
|---|---|---|
| LLM features (explorer, NL→SQL, planner) | an API key, **or** a local model | `LLM_PROVIDER` (default `openai`), `AZURE_OPENAI_ENDPOINT`, or `docker-compose.ollama.yml` for a fully offline stack |
| OAuth connectors (Google, GitHub, Slack, Salesforce, HubSpot) | **your own** client ID + secret, registered with that vendor | `${GOOGLE_CLIENT_ID:-}` etc. in `docker-compose.quickstart.yml` — all default empty, so the stack starts without them |
| Outbound email (invites, notifications) | SMTP or Resend credentials | unset in quickstart; invites render a URL but do not send |
| Object-storage destinations (S3, GCS, Azure Blob) | your bucket + credentials | [cloud-storage-config.md](../connectors/cloud-storage-config.md) |

**On vendor credentials:** self-hosting means you register your own OAuth app with each
vendor. The hosted service's client IDs are not shipped and would not work for your
domain anyway, since the redirect URI is bound to the host.

## C. Not in a self-hosted stack

Short list, and none of it is product function withheld on purpose — it is the hosted
service's own operational plumbing.

| Absent | Why | Decided at |
|---|---|---|
| **Connector *generation*** (build a new connector from an OpenAPI/GraphQL spec) | the generator package is stripped from the image | `llm-service/oss-strip-list.txt` |
| Monitoring UI (overview, infra, traces) | backed by a SigNoz install that self-host does not deploy | `features.go:35-41` — default `false` |
| OpenTelemetry export | no `otel-collector` container in quickstart | `docker-compose.quickstart.yml` |
| Log shipping (`fluent-bit`), Avro `schema-registry`, Temporal web UI + admin tools, MinIO lifecycle init, Docker socket proxy, connector FS init | hosted-only plumbing | 8 services quickstart omits, `otel-collector` included |
| Internal connectors as pipeline endpoints (MinIO, Debezium, Kafka sink) | blocked by default in **both** editions; the hosted compose opts in with `RSYNC_ALLOW_INTERNAL_CONNECTORS=true` | `connections.go:56-60` |
| Billing and plan quotas | `RSYNC_BILLING_ENFORCED=false` | `docker-compose.quickstart.yml:1081` |

### Connector generation: what exactly is missing

This matters because two docs used to point at an endpoint that is not in the image.

In a self-hosted stack the container named `tool-generator` runs a **different program**:
`ghcr.io/rsync-ai/connector-lifecycle` with entrypoint `python -m src.lifecycle.main`,
not `src.agents.tool_generator.service`. It serves exactly three routes:

| Route | Self-hosted |
|---|---|
| `GET /health` | ✅ |
| `GET /version` | ✅ |
| `POST /v1/deploy` (build + start a connector container) | ✅ — requires `X-Internal-Secret` |
| `POST /v1/generate` | ❌ **not present** |

`POST /v1/deploy` is deliberately kept: the Go data plane calls it on every self-heal and
just-in-time connector build, so stripping it would break CDC recovery. Only the
*generation* half is absent. (A second, separate service also serves a deploy route —
`connector-deployer:5011`, `POST /v1/deploy` and `/v1/undeploy`, the BuildKit-backed
builder. It ships self-hosted too.)

The api-gateway route `POST /api/v1/connectors/generate`
(`api-gateway/cmd/server/main.go:947`) **does** exist self-hosted — same image, no edition
gate — but it proxies to a service that no longer serves that path. Adding a connector
self-hosted means writing it by hand: see the
[connector developer guide](../connectors/developer-guide.md).

## Quotas and limits

**Self-hosted, every quota is off.** Two independent reasons, either sufficient:

1. `RSYNC_BILLING_ENFORCED=false` in `docker-compose.quickstart.yml:1081` →
   `billingEnforced()` returns false → `resolvePlanQuota` returns `unlimitedQuota`
   (`billingEnforced()` at `plan_quota.go:73`, the early return at `:95`).
2. Even with billing on, `loadPlans` reads the `plans` table; when it is empty the code
   returns `unlimitedQuota` (`plan_quota.go:105-106`).

The flag **fails closed to the hosted behavior**: unset or unparseable → enforced. Never
set it `false` in `docker-compose.yml` or `docker-compose.prod.yml`.

For reference, these are the hosted plan rows — they live in migrations, not in Go:

| Dimension | trial | free | starter | pro | Actually enforced? |
|---|---|---|---|---|---|
| Pipelines (`pipeline_limit`) | 1 | 2 | 5 | unlimited | **Yes** — creation is blocked |
| Data transfer (`included_gb`/mo) | 10 | 10 | 100 | 500 | **No** — metered only |
| NL queries (`included_queries`/mo) | 1000 | 1000 | 3000 | 10000 | **No** — metered only |
| LLM spend | $50/mo, per **user**, not per plan | | | | **Yes** |
| Plan duration (`duration_days`) | 14 | 30 | — | — | expiry cascades trial → free |

Sources: `api-gateway/migrations/060_user_plans.sql:37-41`,
`071_starter_plan_and_workspace_plans.sql:27-28`, `072_plan_gb_allowances.sql:18-21`,
`075_nl_query_usage.sql:20-23`, `057_llm_usage_quotas.sql:35`.

⚠️ **Read the last column.** Data transfer and NL-query counts are recorded but never
refused: `charge_workspace_bytes()` never refuses, and the NL-query charger is documented
in-migration as "enforcement is Phase 2". Only the pipeline count and the per-user LLM
spend actually block. Direct SQL is neither limited nor metered.

## Where self-host is stricter than hosted

Worth stating, because the usual assumption runs the other way:

| Setting | Hosted | Self-hosted |
|---|---|---|
| `RSYNC_CSRF_ENFORCE` | `false` by default | **`true`** |
| `RSYNC_TRUST_CLOUDFLARE` | on (runs behind Cloudflare) | `false` |
| `ENVIRONMENT` | `development` in the base compose | `production` |
| Internal connectors | selectable | blocked |

## See also

- [self-hosting.md](self-hosting.md) — the actual install procedure
