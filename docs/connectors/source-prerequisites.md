# Universal Source Readiness Pre-flight

**Purpose**: Document the **read-only, in-product** validation framework that checks — for **every** source type — whether the source is configured well enough for a pipeline to run, and surfaces an **actionable remediation message** to the user when it is not.

**Audience**: anyone debugging "why did my pipeline get blocked / warned before it ran", or adding readiness coverage for a new source type.

**Key property — read only**: this layer **never changes or enables** any source configuration. It connects, runs catalog/`SHOW` queries (DBs) or a `test_connection` round-trip (connectors), classifies the result, and reports. The user (or their DBA / SaaS admin) performs any remediation. There is no Claude agent on the prod server doing this — it is plain product code.

Related:
- [cdc-source-prerequisites.md](cdc-source-prerequisites.md) — the **deeper, CDC-only** `internal/cdc/<db>.go ValidatePrerequisites` path that runs on the executor right before a Debezium connector is created. That layer is a hard gate inside the run; this doc covers the broader read-only assessor that runs **up-front at setup and again at run** for *all* source families.

---

## Why a universal pre-flight

Different source families fail for completely different reasons:

| Family | Typical "not ready" cause |
|---|---|
| MySQL / PostgreSQL (CDC) | `binlog_format`/`wal_level` not set, missing replication grant, binlog retention too short |
| MySQL / PostgreSQL (batch) | unreachable host, TLS required but not negotiated, schema not visible to the user |
| SaaS / REST connectors (Shopify, Stripe, HubSpot, …) | invalid/expired token, missing API scope, incomplete config |

A single hard-coded "is MySQL ready" check can't cover SaaS, and a SaaS-only check can't cover a database. The framework solves this with a **pluggable assessor registry** plus a **generic connector fallback**, so coverage is *universal by construction*: any source type resolves to *some* assessor, and unknown types degrade to a non-blocking warning rather than a crash.

### When it runs (surfaced twice)

1. **Up-front, at setup** — the api-gateway's pipeline-run gate calls the orchestrator assess endpoint and merges the result into the pre-migration assessment the user already sees. A source-readiness blocker shows up *before* the user commits to a run.
2. **At run** — `POST /api/v1/pipelines/:id/run` re-runs the gate. Errors block the run (HTTP 422 with the assessment payload); warnings require a per-finding acknowledgement; info notes are shown but need no action.

---

## Architecture

```
api-gateway RunPipeline
  └─ buildPipelineAssessment
       └─ fetchSourceReadiness ──POST {}──▶ orchestrator  POST /api/v1/pipelines/:id/assess
                                                              └─ RunAssessment
                                                                   ├─ loadPipelineSource (JOIN connections → connector_type, config→selected_tables)
                                                                   └─ assessmentRegistry.Resolve(sourceType).Assess(ctx, Input)
                                                                        ├─ MySQLAssessor       (sourceType "mysql")
                                                                        ├─ PostgresAssessor    (sourceType "postgresql")
                                                                        └─ ConnectorAssessor   (DEFAULT — any other type)
       └─ merge readiness checks as a synthetic "(source readiness)" table
  └─ if !ack_warnings && (blocking || warnings) ⇒ HTTP 422 { assessment }
```

### The registry (`backend-orchestrator/internal/assessor/assessor.go`)

```go
type SourceAssessor interface {
    SourceType() string
    Assess(ctx, Input) (*Result, error)
}
```

- `Registry.Register(a)` keys an assessor by `a.SourceType()`.
- `Registry.SetDefault(a)` installs the **fallback** used for any unregistered type.
- `Registry.Resolve(sourceType)` therefore **always** returns an assessor (ok=true) once a default is set → universal coverage.

Wired at startup in `cmd/orchestrator/main.go`:
```go
assessmentRegistry.Register(assessor.NewMySQLAssessor())
assessmentRegistry.Register(assessor.NewPostgresAssessor())
assessmentRegistry.SetDefault(assessor.NewConnectorAssessor(mcp.NewClient(mcpServerManager)))
```

### Check / Result shape

```go
type Check struct {
    Code        string         // stable machine code, e.g. MYSQL_BINLOG_FORMAT_NOT_ROW
    Severity    Severity       // "info" | "warning" | "error"
    Passed      bool
    Message     string         // human-readable finding
    Remediation *diagnose.Remediation
}

type Remediation struct {  // pkg/diagnose
    Steps            []string  // "what to do", ordered
    SQLToRun         []string  // copy-paste SQL (DB sources)
    DocURL           string
    EstimatedMinutes int
}
```

- `Severity == "error"` ⇒ `blocks_start = true` (run is gated).
- `Severity == "warning"` ⇒ surfaced; requires user ack to proceed.
- `Severity == "info"` + `Passed` ⇒ a passing check; the api-gateway drops these from the merged table to reduce noise.

`Result` aggregates `[]Check`, a `Status`, `BlocksStart`, and pass/warn/error counts. JSON tags: `code`, `severity`, `passed`, `message`, `remediation`; remediation: `steps`, `sql_to_run`, `doc_url`, `estimated_minutes`. The frontend renders these directly (see `FindingRemediation` in `PreMigrationAssessmentModal.tsx`).

### Batch vs CDC gating

`Input.IsCDC()` (true when `sync_mode` is a CDC mode) gates the CDC-only checks so a **batch** pipeline is never false-blocked by a CDC requirement. Concretely, in `mysql.go` the binlog/grant/retention checks (`checkMySQLLogBin`, `BinlogFormat`, `BinlogRowImage`, `BinlogExpire`, `ReplicationGrants`, `TablePrimaryKeys`) run **only** under `in.IsCDC()`. A batch MySQL pipeline only gets the reachability/connection check.

---

## Per-source checks

### MySQL — `internal/assessor/mysql.go` (`SourceType() == "mysql"`, also `mariadb`→`mysql`)

| Check | Code | Severity | Gated to CDC? |
|---|---|---|---|
| Reachable + auth (TLS-aware DSN) | `MYSQL_CONNECTION` | error | no (always) |
| `log_bin` enabled | `MYSQL_LOG_BIN_DISABLED` | error | yes |
| `binlog_format = ROW` | `MYSQL_BINLOG_FORMAT_NOT_ROW` | error | yes |
| `binlog_row_image = FULL` | `MYSQL_BINLOG_ROW_IMAGE_NOT_FULL` | warning | yes |
| binlog retention not too short | `MYSQL_BINLOG_EXPIRE_TOO_SHORT` | warning | yes |
| `REPLICATION CLIENT` + `REPLICATION SLAVE` grants | `MYSQL_USER_LACKS_REPLICATION` | error | yes |
| selected table exists in source | `CDC_TABLE_NOT_FOUND_IN_SOURCE` | error | yes |

> **TLS**: `openMySQL` picks the TLS mode host-aware (`assessorMySQLTLSMode`): explicit `ssl_mode`/`sslmode`/`tls` config wins; otherwise local hosts → no TLS, remote hosts → `skip-verify`. This avoids a false `MYSQL_CONNECTION` failure against managed MySQL that enforces `require_secure_transport=ON` (e.g. Azure Database for MySQL, which returns Error 3159 on a plaintext connection).

### PostgreSQL — `internal/assessor/postgresql.go` (`SourceType() == "postgresql"`, also `postgres`→`postgresql`)

| Check | Code | Severity | Gated to CDC? |
|---|---|---|---|
| Reachable + auth (`sslmode` defaults to `prefer`) | `POSTGRES_CONNECTION` | error | no (always) |
| `wal_level = logical` | `POSTGRES_WAL_LEVEL_NOT_LOGICAL` | error | yes |
| `max_replication_slots >= 1` | `POSTGRES_MAX_REPLICATION_SLOTS_LOW` | warning | yes |
| `max_wal_senders >= 1` | `POSTGRES_MAX_WAL_SENDERS_LOW` | warning | yes |
| role has REPLICATION | `POSTGRES_USER_LACKS_REPLICATION` | error | yes |
| selected schema visible to user | `POSTGRES_SCHEMA_NOT_VISIBLE` | error | conditionally |

### Any other source (SaaS / REST / GraphQL / new DBs) — `internal/assessor/connector.go` (DEFAULT)

The `ConnectorAssessor` delegates to the connector's own MCP `test_connection` — the **same** credential + scope round-trip the connector uses at runtime — and classifies the outcome (`classifyConnectorFailure`):

| Outcome | Code | Severity | Meaning |
|---|---|---|---|
| `test_connection` succeeds | (passing info check) | info | source is reachable + authorized |
| auth rejected (401/403, expired/invalid token, missing scope/permission) | `CONNECTOR_AUTH_FAILED` | error | re-auth or grant the required API scope |
| config missing/invalid | `CONNECTOR_CONFIG_INCOMPLETE` | error | fill in required connection fields |
| reachable but call failed | `CONNECTOR_CONNECTION_FAILED` | error | endpoint/network problem |
| connector exposes no `test_connection` | `CONNECTOR_ASSESS_UNAVAILABLE` | warning | can't pre-validate; proceeds with ack |
| unknown connector type | `CONNECTOR_TYPE_UNKNOWN` | warning | no specific assessor; proceeds with ack |

Auth keywords matched: `401`, `403`, `unauthorized`, `forbidden`, `authentication`, `invalid token`, `token expired`, `invalid api key`, `invalid_grant`, `access denied`, `permission`, `scope`, `insufficient`, `not authorized`. Each error attaches remediation `Steps` + a `DocURL` + an effort estimate. The `DocURL` is built by `diagnose.ErrorDocURL` (never hand-written) and anchors into the [error reference](../errors/README.md) — one section per code: `connector-config-incomplete`, `connector-auth-failed`, `connector-connection-failed`.

**This is what makes coverage universal**: a brand-new SaaS connector nobody wrote a bespoke assessor for still gets a real readiness check (its own `test_connection`), and a truly unknown type degrades to a non-blocking warning rather than crashing the gate.

---

## What the user sees

`PreMigrationAssessmentModal.tsx` renders the merged report generically:
- **Errors** (`CONNECTOR_AUTH_FAILED`, `MYSQL_LOG_BIN_DISABLED`, …) block the Proceed button outright — "Resolve errors first".
- **Warnings** (`MYSQL_BINLOG_ROW_IMAGE_NOT_FULL`, `CONNECTOR_TYPE_UNKNOWN`, …) require a per-finding "I understand" checkbox; Proceed enables once all are acked → re-issues the run with `ack_warnings: true`.
- Each finding's `details` renders the remediation block: ordered **What to do** steps (with `~N min`), copy-paste **SQL**, and a **View documentation →** link.

---

## Verified end-to-end (staging, 2026-06-03)

| Source | Mode | Result |
|---|---|---|
| MySQL (Azure) | batch | passed — no false CDC block; TLS negotiated |
| MySQL (Azure) | CDC | warning — real `binlog_row_image=MINIMAL` + retention warnings, `blocks_start=false` |
| PostgreSQL (Azure) | batch | passed |
| Shopify (`shopify-admin-graphql`) | — | ConnectorAssessor authenticated via live `test_connection` → passed |
| MySQL CDC run-gate | run | `POST /run` without ack ⇒ HTTP 422 with `(source readiness)` table merged in |

`CONNECTOR_AUTH_FAILED` / config / connection classification verified by code review of `classifyConnectorFailure`.

Two real bugs were caught and fixed by this live testing (both would otherwise have shipped):
1. `loadPipelineSource` referenced non-existent `source_connector`/`tables` columns → rewritten to JOIN `connections.connector_type` and read `config->'selected_tables'`.
2. `openMySQL` connected without TLS → false `MYSQL_CONNECTION` failure against Azure MySQL (`require_secure_transport=ON`) → host-aware TLS added.

---

## Adding readiness coverage for a new source

- **Bespoke DB/source check**: implement a `SourceAssessor` in `internal/assessor/<type>.go`, return `[]Check` with appropriate codes/severities/remediation, and `assessmentRegistry.Register(...)` it in `main.go`. Gate CDC-only checks behind `in.IsCDC()`.
- **SaaS/REST/GraphQL connector**: nothing to do — the `ConnectorAssessor` default already covers it via `test_connection`. Improve specificity only if you need finer error classification (extend `classifyConnectorFailure`).
- Always keep it **read-only** — connect, query, classify, report. Never mutate source config.
