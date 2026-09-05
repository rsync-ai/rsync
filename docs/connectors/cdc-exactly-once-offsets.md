# CDC Exactly-Once Offset Tracking — Per-Destination Implementation Guide

**Status:** Tier A implemented (PostgreSQL, MySQL, relational template) · Tier B reference impl (BigQuery adapter) · Tier C documented (implement when first object-store destination lands)
**Author:** AI-assisted (rahulv8)
**Audience:** Any agent adding a **new destination connector** (data warehouse, REST/API sink, or cloud object store) that must participate in CDC exactly-once recovery.
**Replaces:** Redis-based CDC dedup. Redis is removed from CDC correctness — the destination is the sole durable source of truth for replication progress.

---

## 0. TL;DR — what you must do for a new destination

When you add a new CDC **destination** connector, you must make it durably remember
**"the highest Kafka offset I have already applied, per (topic, partition)"** so that
after a crash/restart the sink can skip messages it already wrote. Pick the tier that
matches your destination's transactional model:

| Tier | Destination class | Mechanism | Guarantee |
|---|---|---|---|
| **A** | Transactional relational (PostgreSQL, MySQL, Oracle, SQL Server) | Write `(topic, partition, offset)` into `_rsync_cdc_offsets` **in the SAME transaction** as the data batch | True exactly-once, even for non-idempotent ops |
| **B** | Idempotent non-transactional (BigQuery, Snowflake, Redshift, Databricks, MongoDB) | Idempotent `MERGE`/upsert by PK + soft-delete; write offset row **best-effort AFTER** the data merge | Effectively-once (replay duplicates absorbed by idempotency) |
| **C** | Object stores (S3, GCS, Azure Blob, MinIO) | **Deterministic object keys** (`pipeline/topic/partition/offset-range`) — Kafka redelivery overwrites the same object | Effectively-once (idempotent by key) |

Every destination must also expose a **read path** — a `get_cdc_offsets` operation — so the
sink can seed its in-memory high-water map on startup. (Tier C destinations may instead
derive the high-water mark by listing object keys; see §4.)

> **Industry context.** This matches how Fivetran / Airbyte / AWS DMS actually behave:
> "exactly-once delivery" across a network is a myth; you achieve **effectively-once**
> with idempotent upserts + deterministic PKs + versioning by LSN/offset, and you only
> advance the source cursor *after* the destination write succeeds. Tier A is the one
> case where a true transaction lets us reach genuine exactly-once for non-idempotent ops.

---

## 1. The universal invariant (all tiers)

```
For each (topic, partition) the destination holds a durable high-water offset H.
The sink commits the Kafka group offset ONLY AFTER the destination write succeeds.
On startup the sink calls <dest>_get_cdc_offsets, builds an in-memory map
  highWater[(topic, partition)] = H,
and SKIPS any message where msg.Offset <= H.
```

This is what makes Redis unnecessary: the thing Redis used to remember (which offsets
were already processed) now lives durably **in the destination itself**, written
atomically (Tier A) or idempotently (Tier B/C) with the data.

---

## 2. Data contracts (identical across tiers)

### 2.1 The `kafka_offset` parameter

The sink passes a `kafka_offset` value into every write operation (`upsert_data`,
`delete_data`, `import_data`, `merge`, object `load`). It is either a single object or a
list of objects of this shape:

```jsonc
{
  "pipeline_id": "pl_abc123",   // scopes offsets to one pipeline
  "topic":       "dbserver1.public.orders",
  "partition":   0,             // Kafka partition (int)
  "offset":      948213         // Kafka offset of the LAST message in this batch (int)
}
```

A batch spanning multiple partitions yields a **list** — one entry per partition, each
carrying that partition's highest offset in the batch. A connector must take the
**max** per (pipeline_id, topic, partition); never blindly overwrite, or an
out-of-order/retried batch could move the high-water mark backwards.

### 2.2 The `_rsync_cdc_offsets` table (Tier A & B)

One row per `(pipeline_id, topic, partition)`. Created lazily by the connector
(`CREATE TABLE IF NOT EXISTS` / `create_table(exists_ok=True)`) so no migration step is
required.

| Column | Type (logical) | Notes |
|---|---|---|
| `pipeline_id` | string, part of PK | |
| `topic` | string, part of PK | |
| `kafka_partition` | int, part of PK | named `kafka_partition` not `partition` (reserved word in several engines) |
| `last_offset` | int / int64 | the high-water mark; **monotonic** — only ever increases |
| `updated_at` | timestamp, nullable | audit only |

PK = (`pipeline_id`, `topic`, `kafka_partition`). The merge must be monotonic:
`last_offset = GREATEST(existing, incoming)`.

### 2.3 The `get_cdc_offsets` operation (read path)

Returns the durable high-water marks for one pipeline:

```jsonc
// request params: { "pipeline_id": "pl_abc123", ...connection config }
// response:
{ "success": true,
  "offsets": [ { "topic": "...", "partition": 0, "offset": 948213 }, ... ] }
```

If the offsets table/keys do not exist yet (first run), return
`{ "success": true, "offsets": [] }` — never an error. The sink treats an empty list as
"start from the beginning / from the Kafka committed offset."

### 2.4 Advertising the operation (reflective dispatch)

Tools are dispatched by `BaseMCPConnector.handle_request()`, which maps
`"<connector_type>_<op>"` → the connector's `<op>` method (hyphen + underscore variants
are both accepted). For the sink to discover the read path, add an entry to the
`operations` array returned by `get_capabilities()`:

```python
{
    "name": "get_cdc_offsets",
    "method": "<connector_type>_get_cdc_offsets",
    "type": "destination",
    "description": "Return durable per-partition Kafka high-water offsets for CDC exactly-once recovery"
},
```

The **generated** relational template relies purely on this entry + reflective dispatch
(it does *not* emit an explicit `<type>_get_cdc_offsets` entrypoint method). The
**hand-curated** PostgreSQL/MySQL connectors additionally define an explicit
`postgresql_get_cdc_offsets` / `mysql_get_cdc_offsets` entrypoint — both styles work; match
whatever the connector you are editing already does.

---

## 3. Tier A — Transactional relational (REFERENCE: PostgreSQL, MySQL, template)

**This tier is fully implemented.** Use it as the copy-paste model for any new
transactional relational destination (Oracle, SQL Server, ClickHouse-with-txn, etc.).

**Reference files:**
- Template (authoritative, regenerates all DB connectors):
  `llm-service/src/agents/tool_generator/templates/connector_database.py.j2`
- PostgreSQL: `shared/mcp-connectors/public/postgresql/versions/v1.0.0/connector.py`
- MySQL: `shared/mcp-connectors/public/database/mysql/versions/v1.0.0/connector.py`

**Required pieces:**

1. `_CDC_OFFSETS_TABLE = "_rsync_cdc_offsets"` class constant.

2. `_write_cdc_offsets(self, cursor, params)` — runs on the **same cursor/connection** as
   the data write, so it commits atomically with the batch. It:
   - no-ops if `params` has no `kafka_offset`;
   - lazily creates the offsets table (dialect-correct DDL);
   - upserts each offset monotonically. Dialect branches (detected via
     `self.driver_pattern.get("module", "")`):
     - **psycopg2 / asyncpg** → `INSERT … ON CONFLICT (pipeline_id, topic, kafka_partition) DO UPDATE SET last_offset = GREATEST(EXCLUDED.last_offset, t.last_offset)`
     - **mysql / pymysql** → backtick DDL + `ON DUPLICATE KEY UPDATE last_offset = GREATEST(last_offset, VALUES(last_offset))`
     - **oracledb / cx_Oracle** → `CREATE TABLE` (swallow "already exists") + `MERGE … WHEN MATCHED THEN UPDATE SET last_offset = GREATEST(...)` with `:1` placeholders
     - **generic ANSI fallback** → DELETE-where-`last_offset <= ?` then INSERT, to emulate monotonic upsert
   - iterates the offset list, skipping non-dict entries or any missing topic/partition/offset.

3. **Call it inside the transaction, immediately before `conn.commit()`** in every
   mutating write path (`upsert_data` staged + per-row, `delete_data`, and any
   `import_data`):

   ```python
   # Record the Kafka high-water offset in the SAME transaction as the batch so
   # data + progress are atomic (replaces Redis dedup).
   self._write_cdc_offsets(cursor, params)
   conn.commit()
   ```

4. `get_cdc_offsets(self, params)` — opens a read connection and `SELECT`s the rows for
   `pipeline_id`, returning the §2.3 shape (dialect-correct placeholders/quoting; empty
   list if the table is missing).

5. The §2.4 `operations` entry.

**Why same-transaction matters:** if the process dies after `conn.commit()` but before
the Kafka offset is committed, on restart the sink reads `H` from the destination and
skips the already-applied batch — no double-apply, even for non-idempotent statements.

---

## 4. Tier B — Idempotent non-transactional warehouses / document stores

**REFERENCE (implemented): the BigQuery adapter in**
`shared/mcp-connectors/public/warehouse_adapters.py`. Use it as the model for Snowflake,
Redshift, Databricks, and MongoDB adapters.

Warehouse destination connectors delegate `load`/`merge`/`discover_schema`/`get_cdc_offsets`
to `self._warehouse_adapter` (obtained via
`from warehouse_adapters import get_data_warehouse_adapter`). The `ADAPTERS` registry in
`warehouse_adapters.py` maps a destination type → its adapter class. **Adding a new
warehouse = adding an adapter class + a registry entry**, and giving that adapter the
three offset members below.

Because these engines have no cheap multi-statement transaction spanning the data write
and an offset write, Tier B is **at-least-once + idempotency**:

1. The data `merge()` is an idempotent `MERGE` keyed on the row PK (soft-delete instead
   of hard delete), so replaying a batch is a no-op.

2. **After** the data merge succeeds, write the offset **best-effort**:

   ```python
   # Tier B (idempotent): record the Kafka high-water offset AFTER the data MERGE.
   # Best-effort — duplicates on replay are absorbed by the idempotent MERGE above,
   # so a failed offset write only costs reprocessing, never data.
   try:
       self._write_cdc_offsets(client, project_id, dataset_id, params)
   except Exception:
       pass
   ```

3. Adapter members to implement (mirror the BigQuery adapter):
   - `_CDC_OFFSETS_TABLE = "_rsync_cdc_offsets"`
   - `_offsets_fq(self, config, ...)` → fully-qualified offsets table name (e.g.
     `project.dataset._rsync_cdc_offsets`), or `None` if not derivable.
   - `_write_cdc_offsets(self, client, …, params)` → no-op unless `params["kafka_offset"]`;
     `create_table(exists_ok=True)` with schema = (pipeline_id, topic, kafka_partition,
     last_offset, updated_at); per offset run a parameterized `MERGE … last_offset =
     GREATEST(T.last_offset, S.last_offset)`.
   - `get_cdc_offsets(self, config, params)` → `SELECT topic, kafka_partition, last_offset
     WHERE pipeline_id = @pid`; return §2.3 shape; **return empty list on ANY exception**
     (table not provisioned yet).

4. The connector's `get_cdc_offsets` delegates to the adapter (the relational template
   already does this so generated connectors that load a warehouse adapter get it for free):

   ```python
   if getattr(self, "_warehouse_adapter", None) and hasattr(self._warehouse_adapter, "get_cdc_offsets"):
       config = self._get_config(params) if hasattr(self, "_get_config") else (params.get("config") or {})
       return self._warehouse_adapter.get_cdc_offsets(config, params)
   ```

**Correctness note:** the offset write being best-effort is safe *only because* the data
merge is idempotent. If you add a warehouse whose load path is **not** idempotent (e.g. a
blind append), you must either make it idempotent (dedup by PK + version on `last_offset`)
or treat it as Tier A with a real transaction — never leave a non-idempotent append with a
best-effort offset.

---

## 5. Tier C — Object stores (S3 / GCS / Azure Blob / MinIO) — *implement when first added*

No object-store CDC destination exists yet. When you add one, **do not** create an
`_rsync_cdc_offsets` "table" — object stores have no transactions and no cheap upsert.
Instead make the **object key itself** the idempotency token:

```
s3://bucket/<path_prefix>/<db_or_schema>/<table>/<YYYY-MM-DD>/<YYYYMMDD-HHMMSSmmm>[-p<n>]-<offset>.<fmt>
```

> Implemented layout (sink `cdcObjectKey`) — a DMS-style table root with a plain date
> folder and an event-timestamp leaf. The trailing Kafka `<offset>` (and `-p<n>` for a
> multi-partition topic) makes the key unique + deterministic, so a redelivered message
> overwrites the same object (idempotency) instead of a `partition=<p>/` folder or a
> collision-prone bare timestamp.

- A batch writes exactly one object whose key encodes its partition + offset range.
  Kafka redelivery of the same batch writes the **same key** → an overwrite, not a
  duplicate. That is the idempotency guarantee (effectively-once).
- **Read path (`get_cdc_offsets`):** there is no offsets table to query. Derive the
  high-water mark by **listing keys** under `<path_prefix>/<dataset>/<db_or_schema>/<table>/`
  and taking the max `offset_end`. Return the same §2.3 shape so the sink is tier-agnostic.
  (The shipped sink instead uses an in-memory high-water tracker + deterministic keys for
  idempotency; key-listing recovery is the fallback design if durable recovery is needed.)
- Make the writer's key derivation **deterministic** (no timestamps, no UUIDs, no random
  suffixes in the key) — otherwise a replay produces a *new* object instead of
  overwriting, and dedup is lost.
- If the format requires manifest/commit files (Iceberg, Delta), the manifest commit is
  the atomic point; encode the offset range in the committed manifest entry and read it
  back the same way.

---

## 6. Checklist for a new destination connector

- [ ] Pick the tier (A/B/C) from the table in §0.
- [ ] **Tier A:** add `_CDC_OFFSETS_TABLE`, `_write_cdc_offsets` (same cursor), call it
      before every `conn.commit()`, add `get_cdc_offsets`. **If it is a DB type covered by
      the template, patch the template too** (`connector_database.py.j2`) and regenerate.
- [ ] **Tier B:** add the adapter class + `ADAPTERS` registry entry in
      `warehouse_adapters.py`; implement `_offsets_fq`, `_write_cdc_offsets` (best-effort,
      after merge), `get_cdc_offsets` (empty list on error); ensure the data merge is
      idempotent by PK with soft-delete.
- [ ] **Tier C:** deterministic offset-encoding object keys; `get_cdc_offsets` derives
      high-water by listing keys.
- [ ] Add the `get_cdc_offsets` entry to `get_capabilities()` `operations` (§2.4).
- [ ] Honor the `kafka_offset` param contract (§2.1); take **max** per partition.
- [ ] `get_cdc_offsets` returns `{ "success": true, "offsets": [] }` (never an error) on
      first run.
- [ ] If this is a connector-version-bumpable change, follow the MCP versioning rule in the
      [connector developer guide](developer-guide.md): patch the active version in place —
      there are no root copies — and cut a new `vX.Y.(Z+1)` only for a deliberate
      behavior change you want separately pinnable.

---

## 7. Sink-side contract (backend-orchestrator)

The Go sink (`backend-orchestrator`, CDC consumer/`main.go` write path) is the other half:

1. **On startup**, for each destination call `<dest>_get_cdc_offsets(pipeline_id)` and build
   `highWater[(topic, partition)] = last_offset`.
2. **Skip** any consumed message with `msg.Offset <= highWater[(topic, partition)]`.
3. **Pass `kafka_offset`** (the §2.1 shape, per-partition max for the batch) into every
   destination write call.
4. **Commit the Kafka group offset only after** the destination write returns success.
5. The control-plane Postgres ledger (`persistCDCAckToPostgres`) is kept as **best-effort
   audit/telemetry only** — it is no longer required for correctness and must never be
   fatal.
6. **No Redis** in the CDC correctness path (no SetNX ack loop, no Redis-based
   schema-evolution fail-closed `os.Exit`). Redis may remain only for non-correctness
   concerns if any.

---

## 8. Cross-references

- Destination load/merge strategies (staged upsert, idempotent merge) → [destination-load-strategies.md](destination-load-strategies.md)
- Adding a DB to Debezium CDC (source side) → [add-cdc-database.md](add-cdc-database.md)
