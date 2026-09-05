# Rsync AI API (Local Docker) — Reference

This repo runs as a **multi-service stack**. The two HTTP APIs you'll call most are:

- **API Gateway (external)**: `http://localhost:5001`
  - REST base: `http://localhost:5001/api/v1`
  - WebSocket: `ws://localhost:5001/ws`
- **Orchestrator (control plane / internal helpers)**: `http://localhost:8081`
  - REST base: `http://localhost:8081/api/v1`

> This reference is generated from the route registrations in code and verified
> against them. Source of truth:
> `api-gateway/cmd/server/main.go` (+ `internal/handlers/transforms.go`,
> `internal/handlers/pii.go`) and
> `backend-orchestrator/cmd/orchestrator/main.go` (+ the orchestrator
> `RegisterRoutes` files for consumers / retention / topology / assessment /
> health). When in doubt, the router code wins over this doc.

## Auth model

Most API Gateway `/api/v1` endpoints are **user-scoped and authenticated**. The
whole `/api/v1` group (except `/features`) is protected by, in order:
session auth → email-verified gate → CSRF → rate limiting
(`AuthRequiredMiddleware`, `EmailVerifiedMiddleware`, `CSRFMiddleware`,
`APIRateLimitMiddleware` — see `api-gateway/cmd/server/main.go`).

- **Production**: authenticate via `/api/v1/auth/*` (session cookie + CSRF).
- **Dev only**: `X-User-ID: <uuid>` is accepted **only when `ENVIRONMENT != production`**
  (the CORS layer adds it to allowed headers only outside prod). It is **not** a
  production auth path. The seeded dev user is
  `00000000-0000-0000-0000-000000000000`.

> **The Orchestrator API (port 8081) has no user-auth layer** and is intended to
> be **internal-only** — do not expose it publicly. Several orchestrator helpers
> (`/agent/test-connection`, `/agent/sample-rows`) accept raw connection
> `config` (credentials) in the body.

### Common headers

- **Content-Type**: `application/json`
- **X-User-ID**: `00000000-0000-0000-0000-000000000000` (dev only; ignored in prod)
- **Authorization / Cookie**: session credentials in prod

## Health checks (quick sanity)

- API Gateway: `GET http://localhost:5001/health` · `GET /version` · `GET /ready`
- Orchestrator: `GET http://localhost:8081/health` · `GET /version` · `GET /ready`
- Frontend: `GET http://localhost:3000/api/health`
- Temporal UI: `http://localhost:8233`
- Kafka Connect: `http://localhost:8083`

---

## API Gateway (`http://localhost:5001`)

### Top-level (no `/api/v1` prefix)

- `GET /metrics` (Prometheus)
- `GET /health`
- `GET /version`
- `GET /ready` (DB ping)
- `GET /ws` (WebSocket; token via `?token=` or `auth_token` cookie)
- `GET /oauth/callback/:provider`

### Auth (public) — `/api/v1/auth`

- `POST /login` *(rate-limited)*
- `POST /register` *(rate-limited)*
- `POST /logout`
- `GET /me`
- `PATCH /me`
- `PATCH /password`
- `GET /invite/:token`
- `GET /verify-email`
- `POST /resend-verification` *(rate-limited)*

### Feature flags

- `GET /api/v1/features` (public within the group)

### Chat (agentic pipeline flow)

- `POST /api/v1/chat/message` *(rate-limited)*

### Pipelines — `/api/v1/pipelines`

**Lifecycle**

- `GET /pipelines`
- `GET /pipelines/stats`
- `POST /pipelines`
- `GET /pipelines/:id`
- `PATCH /pipelines/:id`
- `DELETE /pipelines/:id`
- `POST /pipelines/:id/run` *(rate-limited)*
- `POST /pipelines/:id/assess`
- `POST /pipelines/:id/stop`
- `POST /pipelines/:id/pause`
- `POST /pipelines/:id/resume`

**Tables / CDC tables**

- `POST /pipelines/:id/tables`
- `GET /pipelines/:id/table-stats`
- `POST /pipelines/:id/cdc/tables`
- `POST /pipelines/:id/cdc/backfill`

**Observability / state**

- `GET /pipelines/:id/state`
- `GET /pipelines/:id/runtime`
- `GET /pipelines/:id/events`
- `POST /pipelines/:id/events/raw` *(power-user/admin + justification)*
- `GET /pipelines/:id/events/stream` (WebSocket)
- `GET /pipelines/:id/compare`
- `GET /pipelines/:id/trends`
- `GET /pipelines/:id/monitoring/overview`
- `GET /pipelines/:id/checkpoints`
- `POST /pipelines/:id/diagnose`
- `GET /pipelines/:id/executions`
- `GET /pipelines/:id/executions/:execId`

**HITL resume**

- `POST /pipelines/:id/hitl/connections`
- `POST /pipelines/:id/hitl/connectors`
- `POST /pipelines/:id/hitl/tables`
- `POST /pipelines/:id/hitl/node-input`

**CDC controls**

- `GET /pipelines/:id/cdc/status`
- `POST /pipelines/:id/cdc/restart`
- `POST /pipelines/:id/cdc/recover`
- `POST /pipelines/:id/cdc/pause`
- `POST /pipelines/:id/cdc/resume`

**Schema evolution**

- `GET /pipelines/:id/schema-changes`
- `POST /pipelines/:id/schema-changes/:changeId/approve`
- `POST /pipelines/:id/schema-changes/:changeId/reject`

**Schedules (Temporal schedules)**

- `POST /pipelines/:id/schedules`
- `GET /pipelines/:id/schedules`
- `PATCH /pipelines/:id/schedules/:schedule_id`
- `PATCH /pipelines/:id/schedules/:schedule_id/pause`
- `PATCH /pipelines/:id/schedules/:schedule_id/resume`
- `POST /pipelines/:id/schedules/:schedule_id/trigger`
- `DELETE /pipelines/:id/schedules/:schedule_id`

### Drafts — `/api/v1/drafts`

- `POST /drafts`
- `GET /drafts/:id`
- `PUT /drafts/:id`
- `DELETE /drafts/:id`

### Executions (flat) — `/api/v1/executions`

- `GET /executions`
- `GET /executions/:id`
- `GET /executions/:id/transforms`
- `POST /executions/:id/cancel`
- `GET /executions/:id/diagnose`

### Connectors (MCP registry) — `/api/v1/connectors`

- `GET /connectors`
- `GET /connectors/:name`
- `GET /connectors/:name/logo`
- `POST /connectors/detect-category`
- `POST /connectors/validate` *(power-user/admin)*
- `POST /connectors/generate` *(power-user/admin)*
- `ANY /api/v1/tool-generator/*proxyPath` *(power-user/admin; proxies internal tool-generator)*

### Workspaces — `/api/v1/workspaces`

- `GET /workspaces`
- `POST /workspaces`
- `GET /workspaces/:id`
- `PATCH /workspaces/:id`
- `GET /workspaces/:id/members`

### Connections — `/api/v1/connections`

- `GET /connections`
- `POST /connections`
- `GET /connections/:id`
- `PUT /connections/:id`
- `PATCH /connections/:id` (connector version upgrades)
- `DELETE /connections/:id`
- `POST /connections/test`
- `POST /connections/:id/test`
- `GET /connections/:id/sample`
- `GET /connections/:id/metadata` (schema discovery: tables/columns/counts)

### Transforms — `/api/v1/transforms`

- `POST /transforms/parse`
- `POST /transforms/preview`
- `GET /transforms/pipeline/:pipeline_id`
- `POST /transforms/pipeline/:pipeline_id`
- `DELETE /transforms/pipeline/:pipeline_id`
- `PUT /transforms/:id`
- `DELETE /transforms/:id`

### PII — `/api/v1/pii` + `/api/v1/hash-functions`

- `GET /pii/scan/results`
- `GET /pii/scan/results/:pipeline_id`
- `POST /pii/scan`
- `GET /pii/scan/jobs/:id`
- `GET /pii/approvals`
- `POST /pii/approvals`
- `POST /pii/approvals/:id/decide`
- `GET /pii/policies` — storage only, see the note below
- `POST /pii/policies` — storage only, see the note below
- `PUT /pii/policies/:id` — storage only, see the note below
- `DELETE /pii/policies/:id` — storage only, see the note below

> **`/pii/policies` stores policies; it does not enforce them.** These four endpoints are a
> CRUD surface over the `pii_policies` table, and nothing in the pipeline runtime reads that
> table. A policy you create here will not mask, hash, or block anything. To actually mask a
> column, configure the `mask_pii` transform on the pipeline via `/transforms/pipeline/:id`.
> The endpoints are documented rather than removed because the stored rows are real and the
> scan side of PII *is* wired end to end — but do not build a compliance story on this table.
- `GET /hash-functions`
- `POST /hash-functions`
- `POST /hash-functions/:name/test`

### Suggestions (LLM proxy)

- `POST /api/v1/suggestions/generate`

### Schema Registry proxy — `/api/v1/schemas`

- `GET /schemas`
- `GET /schemas/info`
- `GET /schemas/config`
- `PUT /schemas/config`
- `GET /schemas/:subject`
- `GET /schemas/:subject/versions/:version`
- `POST /schemas/:subject`
- `POST /schemas/:subject/compatibility`
- `DELETE /schemas/:subject`
- `GET /schemas/:subject/config`
- `PUT /schemas/:subject/config`

### OAuth — `/api/v1/oauth`

- `GET /oauth/providers`
- `GET /oauth/:provider/authorize`
- `GET /oauth/tokens`
- `POST /oauth/tokens/:token_id/refresh`
- `DELETE /oauth/tokens/:token_id`
- Callback (non-`/api/v1`): `GET /oauth/callback/:provider`

### Monitoring (Sentinel) — `/api/v1/monitoring`

- `GET /monitoring/sentinel/health`
- `GET /monitoring/sentinel/issues`

### Explorer (NL → SQL) — `/api/v1`

- `POST /sql/generate`
- `POST /explorer/query`
- `POST /explorer/connections/:id/tables/recommend`
- `POST /explorer/metabase/dashboard`
- `GET /explorer/connections/:id/schema-index`
- `POST /explorer/connections/:id/schema-index/refresh`
- `POST /explorer/tables/retrieve`
- `POST /explorer/nl/resolve-tables`
- `POST /explorer/nl/resolve-columns`
- `POST /explorer/nl/next-steps`
- `GET /explorer/export.csv` (legacy)
- `POST /explorer/export`
- `POST /explorer/share/slack`
- `POST /explorer/share/email`

### Admin — `/api/v1/admin` *(admin role required)*

- `GET /admin/overview`
- `GET /admin/pipelines`
- `GET /admin/executions`
- `POST /admin/pipelines/:id/events/raw` *(rate-limited)*
- `GET /admin/users`
- `GET /admin/users/:id`
- `PATCH /admin/users/:id/role`
- `PATCH /admin/users/:id/status`
- `DELETE /admin/users/:id`
- `POST /admin/invitations`
- `GET /admin/invitations`
- `DELETE /admin/invitations/:id`
- `GET /admin/audit-logs`
- `GET /admin/settings`
- `PATCH /admin/settings`
- `GET /admin/health`
- `GET /admin/drift`
- `POST /admin/encryption/rotate`

### Internal (service-to-service) — `/api/v1/internal` *(service secret, no user auth)*

- `POST /internal/oauth/tokens/:token_id/refresh`

---

## Orchestrator (`http://localhost:8081`) — internal control plane

### Top-level (no `/api/v1` prefix)

- `GET /version`
- `GET /health`
- `GET /ready`
- `GET /metrics` (Prometheus)
- `GET /workers`
- `GET /scheduler` — **deprecated, returns HTTP 410 Gone** (use api-gateway `/api/v1/pipelines/:id/schedules`)

### Deprecated

- `POST /api/v1/pipelines/:id/run` — **HTTP 410 Gone** (use the api-gateway route)

### Pipeline shapes — `/api/v1`

- `GET /pipeline/shapes`

**Connector metadata is not served here.** `GET /mcp/connectors`, `/mcp/connectors/:name` and
`/mcp/connectors/:name/capabilities` were **deleted 2026-08-29**. Their handler read a connector
layout that no longer exists, so the listing always returned `{"connectors":[],"total":0}` and both
by-name routes always 404'd — and repairing it would have published the connector catalog, config
schemas included, on an orchestrator group that carries no auth middleware while the default compose
publishes port 8081. Use the api-gateway catalog above (`GET /api/v1/connectors`), which is
authenticated. A guard test keeps them gone.

### Agent HTTP helpers — `/api/v1/agent`

- `POST /agent/discover-schema`
- `POST /agent/test-connection`
- `POST /agent/sample-rows`

### CDC controls — `/api/v1/cdc`

- `GET /cdc/data-pipelines`
- `GET /cdc/pipelines/:pipeline_id/status`
- `POST /cdc/pipelines/:pipeline_id/restart`
- `PUT /cdc/pipelines/:pipeline_id/pause`
- `PUT /cdc/pipelines/:pipeline_id/resume`
- `POST /cdc/pipelines/:pipeline_id/backfill`
- `POST /cdc/pipelines/:pipeline_id/recover`
- `POST /cdc/pipelines/:pipeline_id/sink/restart`
- `POST /cdc/provision`
- `POST /cdc/cleanup`
- `PUT /cdc/tables`

### Transform preview

- `POST /api/v1/transforms/preview`

### Assessment — `/api/v1`

- `POST /pipelines/:id/assess`
- `GET /pipelines/:id/assess/latest`
- `GET /pipelines/:id/assess/:run_id`
- `GET /assess/supported-types`

### Connector health watchdog — `/api/v1/health`

- `GET /health/connector-versions`
- `POST /health/connector-versions/refresh`

### Consumers — `/api/v1/consumers` *(only registered when `ENABLE_CONSUMER_AGENT=true`, the default)*

- `GET /consumers/status` · `POST /consumers/start` · `POST /consumers/stop`
- `POST /consumers/consumers/spawn` · `POST /consumers/consumers/terminate` · `POST /consumers/consumers/restart`
- `GET /consumers/consumers` · `GET /consumers/consumers/:id`
- `GET /consumers/health/summary` · `GET /consumers/health/:id`
- `GET /consumers/scaling/:topic` · `POST /consumers/scaling/:topic/apply` · `POST /consumers/scaling/manual`
- `GET /consumers/scaling/history` · `GET /consumers/scaling/cooldowns`
- `GET /consumers/topics` · `GET /consumers/topics/:topic/consumers`

  *(The doubled `consumers/consumers/...` segment is real — group prefix `/consumers` + route `/consumers/...`.)*

### Retention — `/api/v1/retention` *(only registered when `ENABLE_RETENTION_AGENT=true`)*

> **Disabled by default.** The retention agent is currently non-functional and is
> off unless `ENABLE_RETENTION_AGENT=true` is set explicitly, so these routes are
> normally absent.

- `GET /retention/status` · `POST /retention/start` · `POST /retention/stop`
- `POST /retention/bulk-loads` · `POST /retention/bulk-loads/detect`
- `GET /retention/bulk-loads` · `GET /retention/bulk-loads/active` · `GET /retention/bulk-loads/:id`
- `GET /retention/bulk-loads/:id/progress` · `GET /retention/bulk-loads/:id/safety-check` · `POST /retention/bulk-loads/:id/cleanup`
- `GET /retention/progress/:group/:topic`
- `GET /retention/history` · `GET /retention/history/:topic` · `GET /retention/modified-topics`
- `POST /retention/restore/:topic` · `POST /retention/restore-all`

### Topology — `/api/v1/topology` *(only registered when the topology manager is enabled)*

- `POST /topology/topics` · `POST /topology/topics/pipeline`
- `GET /topology/topics` · `GET /topology/topics/:name` · `DELETE /topology/topics/:name`
- `PUT /topology/topics/:name/partitions`
- `GET /topology/calculate-partitions`

> **Note:** Orchestrator `/connections*` routes were **removed (2026-05-22)** for
> security (an unauthenticated duplicate handler whose mask list leaked
> `access_token` / `refresh_token` / `client_secret` in responses). Connection
> CRUD lives **only** on the api-gateway (user-scoped, authenticated).

---

## OpenAPI/AsyncAPI specs

Legacy OpenAPI/AsyncAPI specs were removed to avoid drift. **Current source of
truth** for routes is the code + Postman:

- API Gateway router: `api-gateway/cmd/server/main.go`
- Orchestrator router: `backend-orchestrator/cmd/orchestrator/main.go`
- Postman: `postman/rsync-ai.local.postman_collection.json`

## Postman

Import:

- Collection: `postman/rsync-ai.local.postman_collection.json`
- Environment: `postman/rsync-ai.local.postman_environment.json`

Set variables (at least):

- `apiGatewayUrl` = `http://localhost:5001`
- `orchestratorUrl` = `http://localhost:8081`
- `userId` = `00000000-0000-0000-0000-000000000000`
