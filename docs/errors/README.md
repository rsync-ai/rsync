# Error reference

Every failure rsync surfaces carries a stable `code` and a `remediation` block. When a
remediation has a `doc_url`, it points at a section of **this page** — the anchor is the
error code in lower-kebab form, so `POSTGRES_WAL_LEVEL_NOT_LOGICAL` links to
[`#postgres-wal-level`](#postgres-wal-level).

The URL is built in one place, `backend-orchestrator/pkg/diagnose/structured_error.go`
(`DocsBaseURL` + `ErrorDocURL`). If you self-host and mirror these docs, set
`RSYNC_DOCS_BASE_URL` on the orchestrator and every error payload will point at your copy
instead. **Never hand-write a `DocURL`** — a literal is how the previous docs host went
dead without anything failing.

Two sources feed this page, and both are worth reading when a section is too terse:

- **Pre-flight readiness checks** run *before* a pipeline starts —
  [`docs/connectors/source-prerequisites.md`](../connectors/source-prerequisites.md).
- **CDC prerequisites per source family** —
  [`docs/connectors/cdc-source-prerequisites.md`](../connectors/cdc-source-prerequisites.md)
  and the summary table in [`docs/connectors/reference.md`](../connectors/reference.md).

Codes are permanent. Once a code has been written to a run event or a notification it is
never reused for a different meaning; if the meaning changes, a new code is minted.

---

## Contents

**Connections and credentials** — [auth-token-expired](#auth-token-expired) ·
[auth-scope-insufficient](#auth-scope-insufficient) ·
[connector-config-incomplete](#connector-config-incomplete) ·
[connector-auth-failed](#connector-auth-failed) ·
[connector-connection-failed](#connector-connection-failed) ·
[postgres-connection-failed](#postgres-connection-failed) ·
[mysql-connection-failed](#mysql-connection-failed)

**Tables and schema** — [cdc-missing-pk](#cdc-missing-pk) ·
[cdc-table-not-found](#cdc-table-not-found) ·
[mysql-invisible-primary-key](#mysql-invisible-primary-key) ·
[schema-drift](#schema-drift) ·
[postgres-schema-not-visible](#postgres-schema-not-visible)

**PostgreSQL CDC** — [postgres-wal-level](#postgres-wal-level) ·
[postgres-publication-missing](#postgres-publication-missing) ·
[postgres-slot-conflict](#postgres-slot-conflict) ·
[postgres-user-replication](#postgres-user-replication) ·
[postgres-max-replication-slots](#postgres-max-replication-slots) ·
[postgres-max-wal-senders](#postgres-max-wal-senders) ·
[postgres-max-slot-wal-keep-size](#postgres-max-slot-wal-keep-size) ·
[postgres-wal-sender-timeout](#postgres-wal-sender-timeout) ·
[postgres-logical-decoding-work-mem](#postgres-logical-decoding-work-mem) ·
[postgres-publication-privilege](#postgres-publication-privilege) ·
[postgres-keyless-tables-outside-pipeline](#postgres-keyless-tables-outside-pipeline)

**MySQL CDC** — [mysql-log-bin](#mysql-log-bin) ·
[mysql-binlog-format](#mysql-binlog-format) ·
[mysql-binlog-row-image](#mysql-binlog-row-image) ·
[mysql-binlog-expire](#mysql-binlog-expire) ·
[mysql-replication-grants](#mysql-replication-grants)

**SQL Server CDC** — [sqlserver-cdc-tier-unsupported](#sqlserver-cdc-tier-unsupported) ·
[sqlserver-cdc-not-enabled](#sqlserver-cdc-not-enabled) ·
[sqlserver-agent-not-running](#sqlserver-agent-not-running) ·
[sqlserver-capture-instance](#sqlserver-capture-instance)

**MongoDB CDC** — [mongodb-not-replica-set](#mongodb-not-replica-set) ·
[mongodb-resume-token-invalid](#mongodb-resume-token-invalid) ·
[mongodb-change-stream-access](#mongodb-change-stream-access) ·
[mongodb-oplog-window-short](#mongodb-oplog-window-short)

**Pipeline, destination and rsync-side** — [user-config-invalid](#user-config-invalid) ·
[dest-capacity](#dest-capacity) ·
[rsync-bug-silent-drop](#rsync-bug-silent-drop) ·
[connector-version-regressions](#connector-version-regressions)

---

# Connections and credentials

## auth-token-expired

**Code:** `AUTH_TOKEN_EXPIRED` · config error · shown to the user · ~2 minutes.

The credentials for this connection have expired, so the pipeline can no longer reach the
source. This is the ordinary end of an OAuth token's life, not a fault.

**Fix**

1. Open the connection settings for this pipeline.
2. Click **Re-authenticate** to refresh credentials.
3. Re-run the pipeline once the connection is green.

## auth-scope-insufficient

**Code:** `AUTH_SCOPE_INSUFFICIENT` · config error · shown to the user · ~5 minutes.

The credentials authenticate correctly but lack permission for one or more of the resources
this pipeline reads. Typically the OAuth app or API key was created with a narrower scope
set than the tables/objects later selected.

**Fix**

1. Review the missing scope in the error details.
2. In the source app (e.g. Shopify Admin, Stripe Dashboard), grant the required permission
   to the API key / OAuth app.
3. Click **Re-authenticate** in the connection settings.

## connector-config-incomplete

**Code:** `CONNECTOR_CONFIG_INCOMPLETE` · pre-flight error, blocks the run · ~5 minutes.

Raised by the generic `ConnectorAssessor` when a source's own `test_connection` reports a
missing or empty required field (`missing required config`, `required field`, `is required`).
The message names the connector type and quotes the connector's own complaint.

**Fix**

1. Open the source connection and fill in every required field.
2. Required fields are listed in the message above.
3. Re-test the connection after saving.

Which fields a connector requires is declared in its own `spec.json`; the catalogue of
connectors is in [`docs/connectors/reference.md`](../connectors/reference.md), and
object-storage sources have their own guide in
[`docs/connectors/cloud-storage-config.md`](../connectors/cloud-storage-config.md).

## connector-auth-failed

**Code:** `CONNECTOR_AUTH_FAILED` · pre-flight error, blocks the run · ~10 minutes.

The connector's `test_connection` was rejected on credentials or permissions. Matched on the
source's own error text: `401`, `403`, `unauthorized`, `forbidden`, `authentication`,
`invalid token`, `token expired`, `invalid api key`, `invalid_grant`, `access denied`,
`permission`, `scope`, `insufficient`, `not authorized`.

**Fix**

1. Verify the API token / key / OAuth grant for this source is valid and not expired.
2. Confirm the credential carries the read scopes/permissions this connector needs.
3. Re-authorize or paste a fresh token, then re-test the connection.

Because the check runs the *same* credential round-trip the connector uses at runtime, a
pass here means the runtime call will authenticate too. See
[`docs/connectors/source-prerequisites.md`](../connectors/source-prerequisites.md).

## connector-connection-failed

**Code:** `CONNECTOR_CONNECTION_FAILED` · pre-flight error, blocks the run · ~10 minutes.

The connector was reachable enough to try, but the call failed for a reason that is neither
missing config nor an auth rejection — usually the endpoint, port, or network path.

**Fix**

1. Verify the host/endpoint, port and credentials in the source connection.
2. Confirm the source is reachable from rsync (network / firewall / IP allow-list).
3. Re-test the connection after correcting it.

## postgres-connection-failed

**Code:** `POSTGRES_CONNECTION` · pre-flight error, blocks the run · ~5 minutes.

The PostgreSQL assessor could not open a connection, so none of the deeper server-config
checks could run. Reported as a check finding rather than a crash, so the UI can say
"we couldn't assess" instead of "we assessed and found nothing".

**Fix**

1. Verify host, port, user, password in connection config.
2. Verify the database user has `CONNECT` privilege.
3. Check network reachability from rsync to PostgreSQL.

## mysql-connection-failed

**Code:** `MYSQL_CONNECTION` · pre-flight error, blocks the run · ~5 minutes.

The MySQL assessor could not open a connection, so the binlog and grant checks below never
ran.

**Fix**

1. Verify host, port, user, password in connection config.
2. Verify the user has `CONNECT` privilege.
3. Check network reachability and that MySQL is bound on a routable interface.

---

# Tables and schema

## cdc-missing-pk

**Code:** `CDC_TABLE_MISSING_PRIMARY_KEY` · severity depends on the destination.

A selected table has no `PRIMARY KEY`. What happens next depends on where the data is going:

- **CDC into a database destination — this BLOCKS the run.** The executor validates a
  `PRIMARY KEY` *declared on the source table*. Neither the content-hash surrogate key nor a
  user column nomination reaches that validator, so the run would fail at start with
  `CDC requires PRIMARY KEY for DB destinations; missing PK on: <table>`.
- **Otherwise — a warning, and the run proceeds.** rsync loads keyless tables using a
  content-hash surrogate key (`_rsync_row_hash`). Because the hash covers all columns, a
  later change to any column is written as a **new row** and the prior version is retained,
  so updates accumulate duplicates rather than applying in place.

**Fix (blocking case — pick one)**

1. Add a `PRIMARY KEY` on the source table, or promote an existing unique `NOT NULL` index.
   This is the only fix that lets CDC stream it.
2. Or deselect this table and stream the keyed tables only.
3. Or switch this pipeline to a batch (full-refresh) sync, which *does* load keyless tables
   via the content-hash surrogate key.

```sql
-- Required for CDC to a database destination (replace 'id' with the natural key):
ALTER TABLE schema_name.table_name ADD PRIMARY KEY (id);
```

**Fix (warning case)**

No action is needed to run. For correct in-place updates, nominate the column(s) that
uniquely identify a row as the key (recommended), or declare a real primary key on the
source.

## cdc-table-not-found

**Code:** `CDC_TABLE_NOT_FOUND_IN_SOURCE` · error, blocks the run · ~5 minutes.

A table selected for this pipeline does not exist in the source. The `CDC_` prefix is
historical — a plain batch pipeline into a relational destination raises this too, and it
blocks in both cases.

Note that "does not exist" and "not visible to this user" are indistinguishable from the
outside: a missing `SELECT` grant looks exactly like a dropped table.

**Fix**

1. Check the source database for the listed table; verify the name and the schema/database.
2. Confirm the connection user has `USAGE` on the schema and `SELECT` on the table.
3. If you renamed or dropped it, update this pipeline's table selection.
4. If you use a non-default schema, verify it is set on the connection.

## mysql-invisible-primary-key

**Code:** `MYSQL_TABLE_PRIMARY_KEY_INVISIBLE` · warning, the run proceeds · no fix required.

MySQL 8.0.30+ with `sql_generate_invisible_primary_key=ON` — the default on Azure Database
for MySQL Flexible Server — auto-creates an **invisible** primary key column (usually
`my_row_id`) for any table declared without one. `information_schema.statistics` still
reports a `PRIMARY` index, so a naive "does a PRIMARY index exist?" check passes, but
invisible columns are excluded from `SELECT *` and are not propagated to the destination.
The replicated table therefore arrives with no key.

rsync handles this: the table loads via the content-hash surrogate key (`_rsync_row_hash`)
and the run succeeds. The caveat is the same as [cdc-missing-pk](#cdc-missing-pk) — because
the hash covers all columns, a later change to any column is written as a new row, so
updates can accumulate duplicates.

**Fix (optional, for true in-place updates)**

Nominate the column(s) that uniquely identify a row as the key (recommended), or declare an
explicit primary key on a real column:

```sql
-- Replace 'id' with the natural key:
ALTER TABLE `your_database`.`your_table` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`);
```

## schema-drift

**Code:** `SCHEMA_DRIFT_DETECTED` · config error · shown to the user · ~10 minutes.

The source schema has changed — a column was added, removed, or retyped — and the
destination is out of sync.

**Fix**

1. Open the pipeline's **Schema** tab to see the detected drift.
2. Approve the proposed schema migration, **or**
3. Run the pipeline in `reload` mode to recreate destination tables.

## postgres-schema-not-visible

**Code:** `POSTGRES_SCHEMA_NOT_VISIBLE` · error, blocks the run · ~5 minutes.

The schema configured on the connection (defaulting to `public`) does not exist, or is not
visible to the connection user.

**Fix**

1. Verify the schema name in your connection config.
2. Grant `USAGE` on the schema to the connection user.
3. Re-run this assessment.

```sql
GRANT USAGE ON SCHEMA "your_schema" TO <connection_user>;
```

---

# PostgreSQL CDC

The provisioning order for PostgreSQL CDC is fixed — publication, then
`REPLICA IDENTITY FULL`, then the replication slot — and Debezium's
`publication.autocreate.mode` is held at `disabled` so it cannot create these itself and
reverse that order. See [`docs/connectors/reference.md`](../connectors/reference.md).

## postgres-wal-level

**Code:** `POSTGRES_WAL_LEVEL_NOT_LOGICAL` · error, blocks the run · ~10 minutes.

`wal_level` must be `logical` for CDC. Anything lower blocks replication-slot creation
outright: the WAL does not carry enough information to decode row-level changes.

**Fix**

1. As a PostgreSQL superuser, run the SQL below.
2. **Restart PostgreSQL** for the setting to take effect — it is not reloadable.
3. Re-run this pipeline.

```sql
ALTER SYSTEM SET wal_level = 'logical';
-- Then restart PostgreSQL (sudo systemctl restart postgresql, or your platform's equivalent)
```

On managed PostgreSQL this is a parameter-group / server-parameter change plus a reboot
rather than `ALTER SYSTEM`.

## postgres-publication-missing

**Code:** `POSTGRES_PUBLICATION_DOES_NOT_EXIST` · config error · ~2 minutes.

The PostgreSQL publication for this pipeline does not exist. rsync provisions it
automatically, so this usually means it was dropped manually.

**Fix**

1. Re-run the pipeline — rsync will recreate the publication.
2. If it fails again, ensure the database user has `CREATE` on the database.

## postgres-slot-conflict

**Code:** `POSTGRES_REPLICATION_SLOT_CONFLICT` · warning · routed to the operator.

A PostgreSQL replication slot for this pipeline already exists from a previous run. This is
auto-healable: rsync's healer attempts to reuse or recreate the slot, and only escalates if
healing fails.

**Fix**

1. No action required — rsync's healer is attempting auto-recovery.
2. If this error persists after 5 minutes, contact support.

## postgres-user-replication

**Code:** `POSTGRES_USER_LACKS_REPLICATION` · error, blocks the run · ~2 minutes.

The connection user does not have the `REPLICATION` role attribute (`pg_roles.rolreplication`
is false). CDC cannot open a replication connection without it.

**Fix**

1. As a PostgreSQL superuser, run the SQL below.
2. Re-run this assessment.

```sql
ALTER USER "your_user" WITH REPLICATION;
```

On AWS RDS, grant `rds_replication` to the user instead of setting the role attribute.

## postgres-max-replication-slots

**Code:** `POSTGRES_MAX_REPLICATION_SLOTS_LOW` · warning · ~10 minutes.

`max_replication_slots` is below 10. Each CDC pipeline consumes a slot, so a low ceiling
starts refusing new pipelines once the existing ones have claimed theirs.

**Fix**

1. As a PostgreSQL superuser, run the SQL below.
2. Restart PostgreSQL for the setting to take effect.

```sql
ALTER SYSTEM SET max_replication_slots = 10;
```

**Code:** `POSTGRES_REPLICATION_SLOTS_EXHAUSTED` · blocking · ~10 minutes.

Every replication slot is already taken and none belongs to this pipeline, so it cannot
create its own and CDC cannot start. Find a slot nothing uses any more and drop it, or
raise `max_replication_slots` as above. Dropping a slot discards the changes it has not
yet delivered — only drop one no pipeline or tool still reads from.

```sql
SELECT slot_name, plugin, active, restart_lsn FROM pg_replication_slots ORDER BY active, slot_name;
SELECT pg_drop_replication_slot('<slot_name>');
```

## postgres-max-wal-senders

**Code:** `POSTGRES_MAX_WAL_SENDERS_LOW` · warning · ~10 minutes.

`max_wal_senders` is below 10. This caps how many replication connections can stream at
once, so it constrains concurrent CDC pipelines the same way slots do.

**Fix**

1. As a PostgreSQL superuser, run the SQL below.
2. Restart PostgreSQL for the setting to take effect.

```sql
ALTER SYSTEM SET max_wal_senders = 10;
```

## postgres-max-slot-wal-keep-size

**Code:** `POSTGRES_MAX_SLOT_WAL_KEEP_SIZE_UNLIMITED` · advisory · ~10 minutes.

`max_slot_wal_keep_size` is `-1` (unlimited). A replication slot holds WAL until its
reader confirms it, so a pipeline that stops reading — paused, failing, or deleted without
its slot — keeps WAL on the source without limit until the disk fills. A limit caps that:
past it PostgreSQL invalidates the slot instead, and the pipeline re-snapshots.

**Fix**

1. As a superuser, run the SQL below (PostgreSQL 13+). No restart is needed.
2. On Cloud SQL, RDS or Azure, set the `max_slot_wal_keep_size` flag or parameter instead.

```sql
ALTER SYSTEM SET max_slot_wal_keep_size = '50GB';
SELECT pg_reload_conf();
```

## postgres-wal-sender-timeout

**Code:** `POSTGRES_WAL_SENDER_TIMEOUT_LOW` · advisory · ~5 minutes.

`wal_sender_timeout` is under 10 seconds. The server drops a replication connection that
has not answered within it, so a slow write or a long snapshot can cut the CDC stream and
restart it.

**Fix**

1. As a superuser, run the SQL below. No restart is needed.
2. On Cloud SQL, RDS or Azure, set the `wal_sender_timeout` flag or parameter instead.

```sql
ALTER SYSTEM SET wal_sender_timeout = '60s';
SELECT pg_reload_conf();
```

## postgres-logical-decoding-work-mem

**Code:** `POSTGRES_LOGICAL_DECODING_WORK_MEM_LOW` · advisory · ~5 minutes.

`logical_decoding_work_mem` is below the 64 MB default. A transaction larger than it spills
to disk on the source while it is decoded, which slows CDC.

**Fix**

1. As a superuser, run the SQL below. No restart is needed.
2. On Cloud SQL, RDS or Azure, set the `logical_decoding_work_mem` flag or parameter instead.

```sql
ALTER SYSTEM SET logical_decoding_work_mem = '64MB';
SELECT pg_reload_conf();
```

## postgres-publication-privilege

**Code:** `POSTGRES_PUBLICATION_PRIVILEGE` · advisory · ~5 minutes.

rsync creates each pipeline's publication with `CREATE PUBLICATION … FOR ALL TABLES`, which
PostgreSQL allows only to a superuser — or, on a managed service, to a member of its admin
role. The connection's user is neither. This is advisory: some platforms grant the right in
ways the catalog does not show, so the pipeline may still start.

**Fix**

If the pipeline fails to start, grant the role for your platform:

```sql
GRANT cloudsqlsuperuser TO "<user>";  -- Cloud SQL
GRANT rds_superuser TO "<user>";      -- Amazon RDS / Aurora
ALTER USER "<user>" WITH SUPERUSER;   -- self-managed
```

## postgres-keyless-tables-outside-pipeline

**Code:** `POSTGRES_UNSELECTED_TABLE_MISSING_PRIMARY_KEY` · warning · ~5 minutes per table.

rsync's publication covers every table in the database (`FOR ALL TABLES`), and PostgreSQL
requires every table in a publication to have a replica identity before it accepts UPDATE or
DELETE on it. A table with no primary key has none, so once the pipeline starts, your
application's UPDATE and DELETE on that table fail, even though the pipeline does not copy it:

```
ERROR: cannot update table "<table>" because it does not have a replica identity and publishes updates
```

Tables the pipeline copies are not affected: rsync sets `REPLICA IDENTITY FULL` on them.
rsync does not change your other tables; the assessment lists them, and the scheduled recheck
of a running pipeline reports new ones.

**Fix**

Add a primary key to each listed table before starting the pipeline. Replace `id` with the
column(s) that identify a row:

```sql
ALTER TABLE "<schema>"."<table>" ADD PRIMARY KEY (id);
```

---

# MySQL CDC

## mysql-log-bin

**Code:** `MYSQL_LOG_BIN_DISABLED` · error, blocks the run · ~15 minutes.

`log_bin` is not `ON`, so MySQL is not writing a binary log at all and there is nothing for
CDC to read.

**Fix** — this one needs a server restart; it cannot be set at runtime.

1. Add `log_bin = mysql-bin` (or a similar path) under `[mysqld]` in `my.cnf`.
2. For RDS / Aurora: enable binary logging via the DB parameter group, then reboot.
3. Restart MySQL and re-run this assessment.

## mysql-binlog-format

**Code:** `MYSQL_BINLOG_FORMAT_NOT_ROW` · error, blocks the run · ~10 minutes.

`binlog_format` must be `ROW`. Under `STATEMENT` (or `MIXED`) the binary log records the SQL
that ran rather than the rows it changed, and Debezium cannot decode row-level changes from
that.

**Fix**

1. As a MySQL admin, run the SQL below.
2. For RDS / Aurora: set `binlog_format` to `ROW` in the DB parameter group and reboot.
3. Re-run this pipeline.

```sql
SET GLOBAL binlog_format = 'ROW';
-- For permanence across a MySQL restart, also update my.cnf:
-- [mysqld]
-- binlog_format = ROW
```

## mysql-binlog-row-image

**Code:** `MYSQL_BINLOG_ROW_IMAGE_NOT_FULL` · warning · ~5 minutes.

`binlog_row_image` should be `FULL`. `MINIMAL` / `NOBLOB` log only the changed columns, which
produces incomplete `UPDATE` events — correct for MySQL's own replication, lossy for a
downstream copy.

**Fix**

1. As a MySQL admin, run the SQL below.
2. Re-run this pipeline.

```sql
SET GLOBAL binlog_row_image = 'FULL';
```

## mysql-binlog-expire

**Code:** `MYSQL_BINLOG_EXPIRE_TOO_SHORT` · warning · ~2 minutes.

Binary logs are being purged sooner than one day. If rsync is offline longer than the
retention window, the binlog position it stopped at is gone and the affected tables must be
re-snapshotted. At least 1 day is recommended so Debezium can resume after brief outages.

**Fix** — as a MySQL admin, run whichever variable your server has:

```sql
-- MySQL 8+:
SET GLOBAL binlog_expire_logs_seconds = 86400;
-- Older MySQL:
SET GLOBAL expire_logs_days = 1;
```

## mysql-replication-grants

**Code:** `MYSQL_USER_LACKS_REPLICATION` · error, blocks the run · ~3 minutes.

The connection user is missing `REPLICATION CLIENT`, `REPLICATION SLAVE`, or both. The
message names which. Both are required: `REPLICATION CLIENT` to read binlog metadata,
`REPLICATION SLAVE` to stream the binlog itself.

**Fix**

1. As a MySQL admin, run the SQL below.
2. On MySQL 8.0.22+ you may need `REPLICATION REPLICA` instead of `REPLICATION SLAVE`.
3. Re-run this assessment.

```sql
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'your_user'@'%';
FLUSH PRIVILEGES;
```

---

# SQL Server CDC

## sqlserver-cdc-tier-unsupported

**Code:** `SQLSERVER_CDC_TIER_UNSUPPORTED` · config error · ~10 minutes.

This Azure SQL Database service tier does not support change data capture. CDC requires at
least Standard S3 (DTU model) or a General Purpose vCore tier — Basic / S0 / S1 / S2 and the
smallest vCore sizes are rejected.

**Fix**

1. Scale the Azure SQL Database up to a CDC-capable service tier (S3+ DTU, or any General
   Purpose / Business Critical vCore tier).
2. Wait for the scale operation to finish.
3. Re-run this pipeline.

## sqlserver-cdc-not-enabled

**Code:** `SQLSERVER_CDC_NOT_ENABLED` · config error · ~5 minutes.

Change data capture is not enabled on this database. rsync enables it automatically, but
that requires a `sysadmin` or `db_owner` login — the runtime login usually is not one.

**Fix**

1. As a `sysadmin` / `db_owner`, run the SQL below against the source database.
2. Ensure the runtime login can `SELECT` from the `cdc` schema.
3. Re-run this pipeline.

```sql
USE [your_database];
EXEC sys.sp_cdc_enable_db;
```

## sqlserver-agent-not-running

**Code:** `SQLSERVER_AGENT_NOT_RUNNING` · config error · ~5 minutes.

The SQL Server Agent is not running. CDC capture jobs run under the Agent; without it no
change data is produced at all and `fn_cdc_get_max_lsn` stays `NULL` — capture appears
enabled but silently never advances.

**Fix**

1. On **Azure SQL Database**, CDC capture is auto-scheduled — there is no Agent to start.
   Confirm CDC is enabled and give the internal scheduler a minute.
2. On **Azure SQL Managed Instance** the Agent is managed for you.
3. On **box / on-prem SQL Server** only: start the SQL Server Agent service on the source
   instance.
4. Re-run this pipeline once change data begins flowing.

## sqlserver-capture-instance

**Code:** `SQLSERVER_CAPTURE_INSTANCE_ERROR` · config error · ~10 minutes.

A capture instance could not be created, or was not found. SQL Server allows at most **two**
capture instances per source table, so a table that has been re-provisioned twice has no room
for a third.

**Fix**

1. List existing capture instances:

   ```sql
   SELECT capture_instance, source_schema, source_name FROM cdc.change_tables;
   ```

2. If the table already has two, disable an unused one with `sys.sp_cdc_disable_table`.
3. Re-run this pipeline.

---

# MongoDB CDC

## mongodb-not-replica-set

**Code:** `MONGODB_NOT_REPLICA_SET` · config error · ~10 minutes.

MongoDB CDC requires a replica set (or a sharded cluster). Debezium reads the change stream,
and a standalone `mongod` does not expose one. The pre-migration assessment reports this
before the pipeline runs, and a run on a standalone source fails before CDC starts.

A single-node replica set is enough — you do not need extra members.

**Fix**

1. Start `mongod` with `--replSet rs0` (or set `replication.replSetName` in `mongod.conf`).
2. Initialize the replica set once, from a `mongosh` shell connected to the node:

   ```javascript
   rs.initiate()
   ```

3. Ensure the CDC user has `read` + `changeStream` privileges and can read the `admin`
   database.
4. Re-run this pipeline.

## mongodb-resume-token-invalid

**Code:** `MONGODB_RESUME_TOKEN_INVALID` · warning · ~10 minutes.

The change-stream resume token is no longer in the oplog, so streaming cannot continue from
where it left off. rsync re-snapshots the affected collections from a fresh position — no
data is lost, but the snapshot re-reads them.

**Fix**

1. No manual command is needed; rsync re-snapshots from a fresh change-stream position.
2. To prevent recurrence, size the oplog so tokens survive longer downtime:

   ```javascript
   // Inspect the current oplog window (mongosh):
   db.getReplicationInfo()
   // Increase the oplog size (example: 16 GB):
   db.adminCommand({ replSetResizeOplog: 1, size: 16384 })
   ```

3. Re-run or resume this pipeline.

## mongodb-change-stream-access

**Code:** `MONGODB_CHANGE_STREAM_UNAUTHORIZED` · blocking when every collection is in one
database, a warning otherwise · ~5 minutes.

The assessment opened a change stream where CDC will watch — the one database every
selected collection is in, or the whole deployment when they span databases — and MongoDB
refused it. Opening a change stream needs the `changeStream` and `find` actions, which the
built-in `read` role carries.

**Fix**

1. Grant the connection's user `read` on the database (Atlas: Database Access → edit the
   user → add the role):

   ```javascript
   db.getSiblingDB("admin").grantRolesToUser("<user>", [{ role: "read", db: "<database>" }])
   ```

2. For collections in more than one database, grant `readAnyDatabase` instead — or select
   the collections as `<database>.<collection>` from a single database:

   ```javascript
   db.getSiblingDB("admin").grantRolesToUser("<user>", [{ role: "readAnyDatabase", db: "admin" }])
   ```

3. Re-run the assessment.

## mongodb-oplog-window-short

**Code:** `MONGODB_OPLOG_WINDOW_SHORT` · advisory · ~10 minutes.

The oplog holds less than 24 hours of changes. CDC resumes from a position in the oplog, so
a pipeline stopped or behind for longer than the window finds that position overwritten
and must re-snapshot (see [mongodb-resume-token-invalid](#mongodb-resume-token-invalid)).

**Fix**

1. Keep at least a day of oplog. On MongoDB 4.4+, run on each replica set member:

   ```javascript
   db.adminCommand({ replSetResizeOplog: 1, minRetentionHours: 48 })
   ```

2. On Atlas, edit the cluster → Additional Settings → set a minimum oplog window.
3. Re-run the assessment.

---

# Pipeline, destination and rsync-side

## user-config-invalid

**Code:** `USER_CONFIG_INVALID` · config error · ~5 minutes.

The generic configuration error — the pipeline configuration is incomplete or invalid, and
the failure did not match any of the more specific CDC-provisioning patterns above. Whenever
a more precise code applies (a binlog setting, a missing publication, a keyless table), that
code is emitted instead.

**Fix**

1. Open the pipeline configuration.
2. Review the field flagged in the error details.
3. Save changes and re-run.

## dest-capacity

**Code:** `DESTINATION_CAPACITY_EXCEEDED` · config error · ~30 minutes.

The destination is out of disk, memory, or quota.

**Fix**

1. Free space on the destination (drop unused tables, increase disk).
2. Or reduce the pipeline's table set / time window.
3. Re-run the pipeline once capacity is available.

## rsync-bug-silent-drop

**Code:** `RSYNC_BUG_SILENT_DROP` · **system error** — this one is on rsync, not you.

rsync detected that a connector behaved incorrectly: it read rows from the source but wrote
none to the destination. This is a bug in the connector, which is why it is classified as a
system error and routed to the developer audience rather than shown as something to fix.

**What happens**

1. No action is required from you.
2. rsync engineering investigates the connector logs.
3. You are notified when a fix is deployed.

If you are self-hosting, this is the point to open an issue with the pipeline id and the
connector type and version.

## connector-version-regressions

**Code:** `RSYNC_CONNECTOR_VERSION_REGRESSION` · warning · routed to the **operator**.

Not a pipeline failure — an ops-level alert. The health watchdog rolls up `executions` hourly
and flags any `(connector_type, version)` whose 24-hour success rate has dropped more than
the regression threshold below the previous version, with at least the minimum sample size.
The usual cause is a bad template release.

**Fix**

1. Review recent commits to that connector's template / connector code.
2. Inspect the failed executions for the regressed version:

   ```sql
   SELECT * FROM executions
   WHERE status = 'failed'
     AND connection_id IN (
       SELECT id FROM connections
       WHERE connector_type = 'your_connector' AND connector_version = 'x.y.z'
     )
   ORDER BY start_time DESC
   LIMIT 20;
   ```

3. Consider rolling back to the previous version via the connector version manifest.
