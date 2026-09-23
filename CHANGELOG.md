# Changelog

All notable changes to Rsync AI are documented in this file.

<!-- Every `## [x.y.z]` heading below must name a tag that actually exists.
     Guarded by llm-service/tests/test_changelog_versions_name_real_tags.py --
     this file used to open with `## [1.0.0] - December 2025`, a version no tag
     has ever pointed at, and nothing in the repo could disagree with it. -->

## [0.1.5] - 2026-09-23

Everything since v0.1.4.

### Kubernetes
- A single 4-vCPU node installs: the installer trims to a set that fits instead of leaving
  pods `Pending`, and the orchestrator waits for Kafka rather than crash-looping past it.

### Change data capture
- A CDC data topic is created with `min(3, brokers)` partitions instead of 1, so its traffic
  is not pinned to one leader on a multi-broker cluster. `KAFKA_CDC_TOPIC_PARTITIONS` sets it
  wider.

## [0.1.4] - 2026-09-21

Everything since v0.1.3.

### Kubernetes
- **One-command installer** (`install-k8s.sh`) that fits the cluster it lands on, waits for a
  slow image pull instead of failing, and keeps Kafka Connect credentials across a pod
  restart. The chart's default image tag is multi-arch (`amd64` and `arm64`).

### Change data capture
- Auto-pickup: a new source table joins a running "whole database" CDC pipeline.
- MongoDB: a stalled source now fails loudly instead of reporting healthy; the heartbeat
  topic is named with the key Debezium reads; editing a pipeline's tables updates
  `collection.include.list`; a standalone `mongod` is refused before `start_sync`; MongoDB
  CDC/streaming runs that carry an enabled `mask_pii` are refused.
- Fixes for wrong row counts, counters that reset on restart, and cooldowns that never blocked.
- The assessor warns about PostgreSQL tables without a primary key that sit outside a CDC pipeline.

### Connections and storage
- A Scope step, server-level MySQL, MongoDB and ClickHouse connections, multi-database CDC and
  mirror mapping; namespace listing (`GET /connections/:id/namespaces`).
- Table discovery lists up to 5000 tables with totals.
- Object-storage layout v2 for GCS, S3 and Azure Blob (no pipeline id in the path).
- Deleting a pipeline closes six cleanup gaps and no longer un-owns destination data.

### Security
- Plain-`http` OAuth token endpoints are refused for Kafka in all four runtimes.
- The connector deploy gate fails closed when `ENVIRONMENT` is unset; MongoDB URI aliases are masked.
- Compose Kafka Connect honours `KAFKA_*` security settings.

### Interface
- Monitoring Overview shows freshness, backlog, Kafka lag and failures; the pipeline page has an
  Assessment tab; Activity replaces Trace and Live events; Data flow reads top-down.
- Explorer: a model page shows its lineage graph, its schedule in words and who runs it.
- Stage durations come from one formatter, so a single transition no longer reads as a retry.

### Removed
- The bundled observability-backend stack. Telemetry still exports over OTLP to whichever
  collector you configure; the seeded `sentinel_config` keys are now backend-neutral
  (migration `100`).

## Earlier changes

Entries from before the release tags; they are not tied to a version.

### 📚 Documentation

#### Added
- **Solutions guides** under `docs/solutions/`: self-hosted PostgreSQL CDC, PostgreSQL to
  MySQL sync, Shopify to PostgreSQL, scheduled SQL models with dependency triggers, and
  data lineage and pipeline observability. Each states its prerequisites, what is and is
  not supported, and how far it has been verified.
- **Public release checklist** (`docs/deployment/public-release-checklist.md`): the manual
  GitHub steps for a release, and what each download and usage number does and does not
  measure.

#### Changed
- README and docs index lead with the same positioning: a self-hosted data platform for
  batch pipelines, CDC, scheduled data models and lineage.

### ✅ Authentication & Security

#### Added
- **User Authentication System**
  - Email/password login and registration
  - Session-based auth with UUID tokens
  - bcrypt password hashing (cost 10)
  - 24-hour session expiration
  - Protected routes via Next.js middleware
  - Logout functionality

- **Security Enhancements**
  - Sensitive data masking in logs
  - Trace ID propagation across all services
  - Structured logging with context
  - Input validation on all endpoints
  - CORS configuration

#### Changed
- **BREAKING — a plain-`http://` Kafka OAuth token endpoint is now refused at start-up**
  (it used to log a warning and connect anyway). The client-credentials grant POSTs
  `KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET` to that URL on *every* token fetch, so one
  plain-http hop hands a credential that never expires to anyone on the path — worse
  than the short-lived bearer token it buys, and not repairable by any broker-side
  setting. Enforced identically by the Go tier, the Python tier, the Debezium
  connector, the Kafka Connect image, the `kafka-init` scripts and the Helm chart.
  - **Exception:** a loopback host — `localhost`, `*.localhost`, `127.0.0.0/8`, `::1` —
    is still allowed, because a token helper on the same host never puts the secret on
    a network. GCP Workload Identity's `http://localhost:14293` is exactly that case.
  - **Opt-out, for disposable test rigs only:**
    `KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT=true`
    (Helm: `kafka.external.oauth.allowInsecureTokenEndpoint=true`).
  - **Action required** only if you point `KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT` at a
    non-loopback `http://` URL: switch the IdP to https. A bundled-broker deployment
    sets no token endpoint and is unaffected.
- Kafka connection failures in the Python tier now name the real cause (rejected SASL
  credentials, untrusted CA, missing client certificate) instead of surfacing only
  `KafkaTimeoutError: Failed to update metadata after 60.0 secs`, which every one of
  those causes used to collapse into.
- Removed all hardcoded user IDs
- All API calls now use authenticated user's ID
- Database cleaned to single admin user

#### Files Modified
- `api-gateway/internal/handlers/auth.go` - Auth endpoints
- `api-gateway/migrations/006_sessions_table.sql` - Sessions table
- `frontend/src/lib/auth.ts` - Client-side auth helpers
- `frontend/src/middleware.ts` - Route protection
- `frontend/src/app/(auth)/login/page.tsx` - Login page
- `frontend/src/app/(auth)/signup/page.tsx` - Signup page
- `frontend/src/components/layout/Header.tsx` - User dropdown with logout

### ✅ API Integration Fixes

#### Added
- **Centralized API Configuration**
  - `frontend/src/lib/config/api.ts` - Single source of truth
  - All service URLs and endpoints defined in one place
  - Environment variable support

#### Changed
- **Fixed API Endpoints Across All Pages**
  - Connections API: `localhost:8082` → `localhost:8081`
  - Pipelines API: `localhost:8000` → `localhost:5001`
  - Executions API: `localhost:8082` → `localhost:5001`
  - Endpoint paths: `/cdc-connections/*` → `/connections/*`

- **Updated API Libraries**
  - `frontend/src/lib/api/connections.ts` - Fixed ports and endpoints
  - `frontend/src/lib/api/pipelines.ts` - Uses API Gateway + auth
  - `frontend/src/lib/api/executions.ts` - Added auth headers

- **Updated Pages**
  - `/connections/[id]` - Fixed endpoints and user ID
  - `/connections/page` - Proper auth integration
  - `/connections/new` - Proper auth integration
  - `/pipelines/page` - Fixed navigation and API port

#### Removed
- Local `getUserId()` implementations in API files
- Hardcoded user IDs throughout frontend

### ✅ Logo System

#### Added
- **High-Quality Logo Download System**
  - Multi-source fallback: Clearbit → Google High-Res → Frontend fallback
  - Automated logo download during connector generation
  - 512x512 PNG format standardization
  - Local storage in `shared/mcp-connectors/:name/logo.png`

#### Changed
- `llm-service/src/utils/logo_downloader.py` - Simplified to 133 lines
- Removed PIL dependency
- Removed placeholder generation
- Frontend handles fallback rendering with emojis

#### Removed
- `scripts/download_connector_logos.py` - Old low-quality script
- `scripts/update_connector_logos.py` - Redundant CDN URL script
- Logo URL fields from connector metadata

#### Files Added
- `shared/mcp-connectors/*/logo.png` - High-quality logos for all connectors
- `docs/LOGO_SYSTEM.md` - Logo system documentation

### ✅ Pipeline Navigation Fix

#### Changed
- **Fixed Pipeline Creation Navigation**
  - `/pipelines` page: "New Pipeline" button now goes to `/pipelines/new`
  - Empty state: "Create Your First Pipeline" goes to `/pipelines/new`
  - Fixed API port: `8082` → `8081` for CDC pipelines

#### Clarified
- `/pipelines/new` - General pipeline builder (AI + Manual modes)
- `/cdc/new` - CDC-specific conversational AI chat

#### Files Modified
- `frontend/src/app/(dashboard)/pipelines/page.tsx` - Fixed navigation links

### ✅ Tracing & Observability

#### Added
- **Distributed Tracing**
  - `X-Trace-ID` header propagation across all services
  - Consistent logging with trace context
  - Request/response tracing

#### Changed
- **Masking Utilities**
  - Go: `api-gateway/internal/security/masking.go`
  - Python: `llm-service/src/utils/masking.py`
  - Masks: password, secret, api_key, token, access_key, etc.

#### Files Modified
- `api-gateway/internal/telemetry/middleware.go` - Trace middleware
- `backend-orchestrator/internal/kafka/manager.go` - Kafka trace propagation
- `llm-service/src/agents/*/service.py` - Added trace logging

### 🗄️ Database

#### Added
- **Migration Scripts**
  - `001_init_schema.sql` - Initial database schema
  - `002_agentic_enhancements.sql` - Agent tracking tables
  - `003_oauth_tables.sql` - OAuth provider support
  - `004_genericity_enhancements.sql` - Generic connector support
  - `005_insert_default_user.sql` - Default development users
  - `006_sessions_table.sql` - Session management

- **Migration Runner**
  - `scripts/run_all_migrations.sh` - Runs all migrations in order
  - Idempotent migrations with `CREATE IF NOT EXISTS`
  - Connection verification before running

#### Changed
- Database cleaned to single admin user (`admin@rsync.ai`)
- Foreign key constraints properly configured
- UUID-OSSP extension enabled

### 📝 Documentation

#### Added
- `docs/services/INDEX.md` - Per-service HLD/LLD documentation index
- `CHANGELOG.md` - This file
- Inline documentation improvements

#### Consolidated
- Combined multiple technical documents into single source
- Organized by topic and service

#### Removed
- Individual fix documentation files (consolidated into changelog)
- Redundant setup guides

### 🧹 Cleanup

#### Removed Redundant Files
- `FRONTEND_API_AUDIT.md` - Temporary audit file
- `api-gateway/migrations/README.md` - Redundant documentation
- Old logo scripts (download_connector_logos.py, update_connector_logos.py)

#### Organized
- Scripts consolidated in `/scripts/`
- Documentation consolidated in `/docs/`
- All migrations properly versioned

---

## Migration Guide

### From Previous Version

1. **Update Environment Variables**
   ```bash
   # Ensure correct service ports in .env
   NEXT_PUBLIC_API_URL=http://localhost:5001
   NEXT_PUBLIC_BACKEND_ORCHESTRATOR_URL=http://localhost:8081
   NEXT_PUBLIC_LLM_SERVICE_URL=http://localhost:5011
   ```

2. **Run Database Migrations**
   ```bash
   ./scripts/migrate.sh
   ```

3. **Clear Old Sessions**
   ```bash
   # If upgrading from version without auth
   docker compose exec postgres psql -U rsync -d rsyncdb -c "DELETE FROM sessions"
   ```

4. **Rebuild Frontend**
   ```bash
   docker compose build --no-cache frontend
   docker compose up -d
   ```

5. **Login with New Auth**
   - Email: `admin@rsync.ai` (override with `ADMIN_EMAIL`)
   - Password: set during setup — `install.sh` generates a unique admin password; never rely on a shared default

---

## Known Issues

Open issues are tracked at
[github.com/rsync-ai/rsync/issues](https://github.com/rsync-ai/rsync/issues) — that
list is the register, and it cannot go stale the way a hand-maintained count here
does. This heading used to read "None at this time", which was untrue when it was
written and had no way of noticing.

---

## Upcoming Features

- [ ] Webhook integrations
- [ ] Advanced pipeline monitoring dashboard
- [ ] SSO/SAML authentication
- [ ] Pipeline templates library
- [ ] Connector marketplace

---

## Support

For issues or questions:
- Review: `docs/services/INDEX.md`
- Check: `docker compose logs <service>`
- GitHub: [rsync-ai/rsync issues](https://github.com/rsync-ai/rsync/issues)
- Email: support@rsync.ai
