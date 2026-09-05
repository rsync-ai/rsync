# CDC Source Pre-flight Prerequisites

**Purpose**: Reference for the server-side configuration each Debezium CDC **source** must have, and how rsync-ai validates it *before* a pipeline starts.

**Audience**: anyone adding a new CDC source type, or debugging why a CDC pipeline failed fast at validation.

Related:
- [add-cdc-database.md](add-cdc-database.md) (full-stack checklist for wiring a new DB into Debezium) — this doc covers only the **pre-flight validation** layer.
- [source-prerequisites.md](source-prerequisites.md) — the **broader, read-only universal readiness assessor** (`internal/assessor/`) that runs up-front at setup and again at run for **all** source families (incl. SaaS/REST connectors), surfacing actionable remediation in the UI. The CDC validator documented here is the deeper hard-gate that runs on the executor right before a Debezium connector is created; the assessor is the early, user-facing pre-flight.

---

## Why a pre-flight check

Debezium fails *deep* and *slow* when a source server is misconfigured: the connector is created, registers with Kafka Connect, attempts to read the WAL/binlog, and only then errors — often retrying on a backoff loop and leaving an orphaned connector behind.

rsync-ai instead validates the source server **before any Debezium connector is created**. A misconfigured source produces a fast, actionable failure and creates **no** Kafka Connect resources.

### Where it runs

| Layer | File | Role |
|---|---|---|
| Per-DB validator | `backend-orchestrator/internal/cdc/<db>.go` → `ValidatePrerequisites(ctx, connectionID)` | Connects to the source, runs `SHOW`/catalog queries, returns `[]ValidationError`. |
| Pre-flight gate | `backend-orchestrator/internal/agents/executor/executor.go` → `validateCDCSourcePrerequisites(...)` | Called on the CDC path before the PK check and before connector creation. Blocks only on `Severity=="error"`; logs warnings. |
| Healer classification | `backend-orchestrator/internal/workers/executor.go` → `suggestRecoveryAction` | Config errors match `"not configured for change data capture"` (+ specific tokens) → returns `"fail"` (non-retryable, no backoff loop). |
| Healer (newer path) | `pkg/diagnose/diagnose.go` → `RuleBasedDiagnoser` | Maps `wal_level`/`binlog_format`/`binlog_row_image`/`publication does not exist` → `ActionEscalate`. |

A `ValidationError` has: `Code`, `Severity` (`"error"` blocks, `"warning"` logs only), `Message`, `Action` (the remediation string surfaced to the user).

---

## Currently supported CDC sources

### MySQL  (`io.debezium.connector.mysql.MySqlConnector`)

Validator: `internal/cdc/mysql.go` → `ValidatePrerequisites`

| Check | Required value | Code | Severity | Remediation |
|---|---|---|---|---|
| `binlog_format` | `ROW` | `MYSQL_BINLOG_FORMAT` | error | `SET GLOBAL binlog_format='ROW';` (managed: change the server parameter + restart) |
| `binlog_row_image` | `FULL` | `MYSQL_BINLOG_ROW_IMAGE` | error | `SET GLOBAL binlog_row_image='FULL';` (managed: change the server parameter + restart) |
| Grants | `REPLICATION CLIENT` + `REPLICATION SLAVE` | `MYSQL_NO_REPLICATION_GRANT` | error | `GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '<user>';` |

> ⚠️ **Managed MySQL caveat**: Azure Database for MySQL / RDS expose `binlog_row_image` as a server parameter; on some shared/Basic tiers it is pinned to `MINIMAL` and cannot be changed. Such a source **cannot** do Debezium CDC — the pre-flight surfaces this cleanly instead of letting Debezium fail later. (This is exactly the Azure `rsync-test` MySQL situation.)

### PostgreSQL  (`io.debezium.connector.postgresql.PostgresConnector`)

Validator: `internal/cdc/postgresql.go` → `ValidatePrerequisites`

| Check | Required value | Code | Severity | Remediation |
|---|---|---|---|---|
| `wal_level` | `logical` | `PG_WAL_LEVEL_INCORRECT` | error | Set `wal_level='logical'` + restart (managed: `rds.logical_replication` / `azure.replication_support`) |
| `max_replication_slots` | `>= 1` (rec. 10) | `PG_MAX_REPLICATION_SLOTS` | error | Set `max_replication_slots >= 1` + restart |
| `max_wal_senders` | `>= 1` (rec. 10) | `PG_MAX_WAL_SENDERS` | error | Set `max_wal_senders >= 1` + restart |
| Replication privilege | `usesuper OR userepl` | `PG_NO_REPLICATION_PERMISSION` | error | `ALTER ROLE <user> REPLICATION;` |

> **Provisioning order reminder** (separate from pre-flight, enforced in `cdc/postgresql.go ProvisionResources`): publication → `REPLICA IDENTITY FULL` → replication slot. Debezium `publication.autocreate.mode` stays `disabled`.

---

## Future / planned CDC sources

These have Debezium connectors but are **not yet wired** into rsync-ai pre-flight. When adding one, implement `ValidatePrerequisites` for the items below and add a `case` to `validateCDCSourcePrerequisites` in `executor.go`, plus matching healer tokens.

### Oracle  (`io.debezium.connector.oracle.OracleConnector`) — position: SCN

| Check | Required value | Notes |
|---|---|---|
| ARCHIVELOG mode | enabled | `SELECT log_mode FROM v$database;` → `ARCHIVELOG` |
| Supplemental logging | `ALL COLUMNS` (min: PK) | `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;` |
| LogMiner / XStream | user privileges | LogMiner: grants on `V_$LOGMNR_*`, `EXECUTE_CATALOG_ROLE`; XStream requires a licensed option |
| Redo log accessibility | reachable | online + archived redo logs readable by the capture user |

### SQL Server  (`io.debezium.connector.sqlserver.SqlServerConnector`) — position: LSN

| Check | Required value | Notes |
|---|---|---|
| SQL Server Agent | running | CDC capture/cleanup jobs run under the Agent |
| Database CDC | enabled | `EXEC sys.sp_cdc_enable_db;` |
| Table CDC | enabled per table | `EXEC sys.sp_cdc_enable_table ...` |
| Capture user | `db_owner` or CDC role | needs read on the change tables |

### MongoDB  (`io.debezium.connector.mongodb.MongoDbConnector`) — position: oplog/change-stream

| Check | Required value | Notes |
|---|---|---|
| Deployment | replica set or sharded | standalone `mongod` has no oplog → no CDC |
| Change streams | available | requires WiredTiger + replica set |
| User roles | `read` on DBs + `read` on `local` (oplog) | or change-stream privileges |

### MariaDB  (`io.debezium.connector.mariadb.MariaDbConnector`) — position: GTID/binlog

Same requirements as MySQL (`binlog_format=ROW`, `binlog_row_image=FULL`, replication grants). Can often reuse the MySQL validator with a different connector class.

> Debezium's authoritative per-connector setup pages: https://debezium.io/documentation/reference/stable/connectors/

---

## Adding pre-flight validation for a new source

1. **Implement the validator** in `internal/cdc/<db>.go`:
   ```go
   func (m *<DB>Manager) ValidatePrerequisites(ctx context.Context, connectionID string) ([]ValidationError, error) {
       // connect to the source, run SHOW / catalog queries,
       // append a ValidationError{Code, Severity:"error", Message, Action} per failed check.
   }
   ```
   Use `Severity:"error"` for anything Debezium hard-requires; `"warning"` for advisory items.

2. **Wire the gate** in `executor.go` → `validateCDCSourcePrerequisites`, add a `case`:
   ```go
   case "<db>":
       vErrs, err = cdc.New<DB>Manager(a.db).ValidatePrerequisites(ctx, sourceConnID)
   ```

3. **Healer tokens**: the wrapper message already starts with `"CDC source (<db>) is not configured for change data capture"`, which the `workers/executor.go` rule matches → `"fail"`. If you want finer-grained matching, add the new config token(s) to that rule and to `pkg/diagnose/diagnose.go`'s escalation list.

4. **Test**: point a deliberately-misconfigured source at a CDC pipeline and confirm (a) the pipeline fails fast with the remediation message, (b) the healer logs `recovery_action":"fail"`, (c) `curl localhost:8083/connectors` shows **no** connector was created.

5. **Docs**: add the source to the tables above and follow the full-stack steps in [add-cdc-database.md](add-cdc-database.md).
