# Destination Load Strategies — Design

**Status:** Proposed (design review)
**Author:** AI-assisted (rahulv8)
**Related:** BUG-10 pagination fix (PR #118), Shopify throughput investigation
**Audience:** Anyone adding a destination database/warehouse, or touching the sink write path.

---

## 1. Problem & goals

Every destination connector today writes rows with a **per-row `cursor.execute()` loop**
(`postgresql/connector.py:2561`, MySQL/Oracle equivalents, and the generated
`connector_database.py.j2` template). On a remote destination (e.g. Azure managed
PostgreSQL) every row is a separate network round-trip. Measured cost: ~1,850 rows in a
Shopify sync spent **~40–60s purely in serial INSERT round-trips**, and the cost scales
linearly — ~10,000 orders would be >4 minutes of DB latency alone, which blocks the
"10,000 orders in minutes" target.

Separately, we want to add more destinations (Snowflake, BigQuery, Redshift, ClickHouse,
Databricks). Each warehouse has a *different* fastest load path (COPY, load jobs,
PUT+COPY INTO, native bulk insert). We do **not** want to hand-write a bespoke loader per
destination.

**Goals**

1. **Throughput** — collapse N per-row round-trips into O(1) per batch using each
   destination's best-supported bulk method.
2. **Minimal code per new destination** — adding a destination should be a small
   *declaration*, not a hand-written write loop. Generated connectors inherit it for free.
3. **Backward compatible** — no change to the sink→destination wire contract; no executor
   changes. A destination that declares nothing keeps working via the existing per-row path.
4. **Always correct** — any bulk path failure falls back to the proven per-row upsert.

---

## 2. Current architecture (what already exists — we build on it, not replace it)

| Piece | Location | State |
|---|---|---|
| Sink → destination call | `kafka-mcp-sink/.../main.go:920` | **Already generic.** Tool name built dynamically: `{destType}_upsert_data` (or `_import_data` if no keys). Payload: `config, table, data[], key_fields[]`. The sink never knows the load method. |
| Capability discovery | `base_connector.py:1127` `get_capabilities()` | Sink reads `supports_ddl`, `connector_category`, `max_batch_size` at startup. **This is our channel to declare a load strategy.** |
| Category → operations | `base_connector.py:464` `CATEGORY_OPERATIONS` | `relational_db → import/execute`, `data_warehouse → load/merge`. |
| DDL | sink `ensureDestinationTable` → `{destType}_ensure_table` | Per-destination `ensure_table` tool. Postgres/MySQL implement it; warehouses don't yet. |
| Postgres upsert | `postgresql/connector.py:2489` `upsert_data()` | Per-row `cursor.execute()`. Has type-aware binding (`_pg_bind_value`), quoted identifiers, synthetic-PK hashing, auto-create-on-missing-table retry. **All of these must be preserved.** |
| Warehouse adapter Protocol | `warehouse_adapters.py` | `import_data` + `merge`; only BigQuery partially implemented. |
| DB connector template | `tool_generator/templates/connector_database.py.j2:2356+` | Generates `import_data()` (`executemany`) + `upsert_data()` (per-row). |

**Key insight:** the *wire contract is already uniform and dynamic*. The only missing piece
is a way for a destination to **declare and use its best bulk method internally**. The
abstraction therefore lives entirely inside the destination connector + a shared base
mixin. The sink and executor need **near-zero changes**.

---

## 3. Design overview — capability-driven "stage-and-merge"

Almost every fast destination upsert is the **same shape**:

```
1. ensure a transient STAGING table shaped like the target
2. BULK-LOAD the batch into staging via the destination's fastest ingest
3. MERGE staging → target ON key_fields
4. drop/truncate staging
```

For a pure insert (no key_fields) you skip the merge and bulk-load straight into the target.

We implement this orchestration **once** in a shared `DestinationLoadMixin`. Each
destination supplies only a small **dialect descriptor** + (rarely) one ingest hook. The
mixin selects the path and guarantees a fallback.

```
                         upsert_data(params)            ← unchanged wire entry point
                                │
                    DestinationLoadMixin.load()
                                │
        ┌───────────────────────┼─────────────────────────┐
   keys + staging?         keys, no staging?            no keys?
        │                       │                          │
  stage → bulk → merge    in-place bulk upsert        bulk insert
        │                       │                          │
        └────────── on ANY exception ──────────────────────┘
                                │
                    per-row upsert (today's loop)         ← always-correct fallback
```

---

## 4. The dialect descriptor — the minimal-code surface

A destination declares its capabilities. This is *all* a simple new destination must provide:

```python
class DestinationLoadSpec:
    load_method:   str   # "copy" | "stage_copy" | "load_api" | "native_insert"
                         #   | "multi_values" | "row_by_row"
    merge_method:  str   # "on_conflict" | "on_duplicate_key" | "merge"
                         #   | "replacing" | "delete_insert"
    supports_staging: bool = True
    max_batch_rows:   int = 10_000   # connector may override per destination limits
```

- **`load_method`** — how a batch is physically ingested into a (staging or target) table.
- **`merge_method`** — how staging rows are reconciled into the target on `key_fields`.
- **`supports_staging`** — whether a transient staging table is available/cheaper than
  in-place upsert. Warehouses: yes. Tiny/edge OLTP: may be no.

The mixin maps `(load_method, merge_method)` to concrete SQL/clients. A destination only
writes custom code when its **ingest primitive is genuinely novel** (e.g. a cloud-stage
PUT). Everything else — staging lifecycle, batching, type binding, transaction boundaries,
merge-SQL generation, fallback — is inherited.

---

## 5. Shared `DestinationLoadMixin` (in `shared/mcp-connectors/base_connector.py`)

Responsibilities (implemented once, reused by every DB connector):

1. **`load(conn, table, data, key_fields, column_types, spec)`** — the orchestrator that
   chooses stage-and-merge vs in-place vs insert vs fallback per §3.
2. **Staging lifecycle** — `CREATE TEMP TABLE <stage> (LIKE <target> INCLUDING DEFAULTS)`,
   guaranteed `DROP`/auto-drop on commit, collision-safe stage names.
3. **Bulk-load primitives** keyed by `load_method`:
   - `copy` — `COPY <stage> (...) FROM STDIN` (psycopg2 `copy_expert` / `copy_from`).
   - `multi_values` — `execute_values(cur, "INSERT ... VALUES %s", rows)`.
   - `native_insert` — driver-native bulk insert (ClickHouse `client.insert`).
   - `stage_copy` / `load_api` — call a connector-provided hook (cloud stage / load job).
   - `row_by_row` — the existing per-row loop (fallback + last resort).
4. **Merge-SQL generation** keyed by `merge_method`:
   - `on_conflict` → `INSERT INTO target SELECT * FROM stage ON CONFLICT (keys) DO UPDATE SET col = EXCLUDED.col …`
   - `on_duplicate_key` → `INSERT INTO target SELECT … ON DUPLICATE KEY UPDATE …`
   - `merge` → ANSI `MERGE INTO target USING stage ON (keys) WHEN MATCHED … WHEN NOT MATCHED …`
   - `replacing` → plain insert (ClickHouse ReplacingMergeTree dedups on merge).
   - `delete_insert` → `DELETE FROM target WHERE key IN (stage keys); INSERT … SELECT` (generic fallback for engines with no native upsert).
5. **One transaction per batch**, single commit (preserves today's atomicity).
6. **Preserve existing per-row behaviors**: type-aware binding (`_pg_bind_value`), quoted
   identifiers, synthetic-PK hashing (`_rsync_row_hash`), auto-create-on-missing-table
   retry. These move into shared helpers so the bulk path and the fallback share them.
7. **Fallback contract** — any exception in the bulk path logs a warning and re-runs the
   batch through `row_by_row`. Bulk failure must NEVER fail the write.

---

## 6. PostgreSQL reference implementation (Phase 1) — COPY + temp + ON CONFLICT

This is the chosen aggressive path and the proof-of-pattern. Per batch:

```sql
-- 1. staging shaped like target (temp, auto-dropped at COMMIT)
CREATE TEMP TABLE "_rsync_stage_<table>_<n>" (LIKE <target> INCLUDING DEFAULTS) ON COMMIT DROP;

-- 2. bulk ingest: one COPY for the whole batch (psycopg2 copy_expert, type-aware encoding)
COPY "_rsync_stage_..." ("col1","col2",...) FROM STDIN;

-- 3. merge staging → target on key_fields
INSERT INTO <target> ("col1","col2",...)
SELECT "col1","col2",... FROM "_rsync_stage_..."
ON CONFLICT ("<key1>","<key2>") DO UPDATE
SET "col2" = EXCLUDED."col2", ...;        -- DO NOTHING when no non-key cols

-- 4. (temp table dropped automatically on COMMIT)
```

`DestinationLoadSpec(load_method="copy", merge_method="on_conflict", supports_staging=True)`.

**Round-trips:** today = N (one per row). New = ~3 statements per batch (create/copy/merge),
independent of N. Expected ~20–50× fewer round-trips; the Shopify 917-row run should drop
from ~91s toward ~15–25s once combined with the sink-flush tuning (§10).

**Behaviors preserved (must verify in review):**
- Type-aware value encoding — COPY needs correct text/binary encoding per `column_types`;
  reuse `_pg_bind_value` semantics in the COPY encoder.
- Quoted, case-sensitive identifiers (`createdAt`) — staging `LIKE target` inherits exact
  column casing; merge uses quoted idents.
- Synthetic PK (`_rsync_row_hash`, `synthetic_pk`) — computed before load, becomes the
  conflict key, same as `postgresql/connector.py:2523`.
- Auto-create-on-missing-table — if `LIKE target` fails because the target doesn't exist,
  trigger the existing `ensure_table` path then retry (mirrors line 2585 retry-once).

---

## 7. Capability wire (small, additive)

`get_capabilities()` gains an optional block so operators/observability can see the chosen
strategy; the sink does **not** need to branch on it (the connector self-selects internally):

```python
"capabilities": {
    "max_batch_size": ...,
    "supported_formats": ...,
    "supports_cdc": ...,
    "load_strategy": {            # NEW, optional, informational
        "load_method": "copy",
        "merge_method": "on_conflict",
        "supports_staging": True
    }
}
```

The sink already reads capabilities at startup (`main.go:4501`); it may optionally log the
strategy and size batches against `max_batch_size`. No required sink code change.

---

## 8. Per-destination strategy matrix

| Destination | `load_method` | `merge_method` | New code to add it |
|---|---|---|---|
| **PostgreSQL** | `copy` | `on_conflict` | descriptor only (COPY in shared mixin) — **Phase 1** |
| **MySQL** | `multi_values` | `on_duplicate_key` | descriptor only — **Phase 2** |
| **Generic SQL** | `multi_values` | `delete_insert` | descriptor only (fallback engine) — **Phase 2** |
| **Snowflake** | `stage_copy` (PUT → `COPY INTO`) | `merge` | descriptor + stage hook — Phase 3 |
| **BigQuery** | `load_api` (load job → staging) | `merge` | descriptor + adapter (started) — Phase 3 |
| **Redshift** | `stage_copy` (`COPY` from S3) | `merge` | descriptor + S3 stage hook — Phase 3 |
| **ClickHouse** | `native_insert` | `replacing` | descriptor only — Phase 3 |
| **anything new** | `row_by_row` | `delete_insert` | **zero — works out of the box** |

---

## 9. Generation vs hand-curation (scope decision)

**Decision:** the tool generator stays focused on **api/saas source connectors**
(`connector.py.j2` REST, `connector_graphql.py.j2` GraphQL). It is **not** used to generate
destination databases or data warehouses, which are **hand-curated**. Rationale:

- **Databases** are few and long-lived (PostgreSQL, MySQL, Oracle, SQL Server). Hand-curating
  each is cheap because the heavy lifting lives in the shared mixin.
- **Data warehouses use entirely different protocols/SDKs** (Snowflake stage PUT+COPY,
  BigQuery REST load jobs, Redshift COPY-from-S3, ClickHouse native client). There is no
  single SQL/DBAPI template that captures them — a generator would have to special-case each
  one, which is more work and harder to test than just writing them.

**Why this is still minimal-code:** the abstraction lives in the **base**
(`base_connector.py`), not in a template. Every hand-curated destination — DB *or*
warehouse — inherits `DestinationLoadMixin` and only declares a `DestinationLoadSpec`
(+ one ingest hook for warehouses). So "hand-curated" here means *a few lines per
destination*, not a bespoke write loop.

**Tool-generator impact:** none for this work. The DB template
(`connector_database.py.j2`) and the api/saas templates are untouched; the repo's
"fix the Jinja template when you fix a hand-curated connector" rule does not apply because
destination DB/WH connectors are no longer treated as generated artifacts for the load path.
If we ever revisit auto-generating databases, the mixin is already in the base, so the
template change would then be a 3-line descriptor emit — but that is explicitly out of scope.

---

## 10. Sink/executor impact + batch/flush tuning (separate but complementary)

- **Sink/executor code:** effectively unchanged. Same `{destType}_upsert_data` call, same
  payload. The strategy is internal to the connector.
- **Sink flush timer (separate fix, recommended alongside):** small batches today dead-wait
  up to the 30s `flushInterval` (`kafka-sink-worker/main.go:512`, no flush-on-idle). Adding
  **flush-on-partition-drained** removes up to ~30–60s of fixed latency on small/medium
  syncs. This is independent of the load-strategy work and can land separately.
- **Batch size:** bulk paths benefit from larger batches. `maxEvents` (1000) and
  `max_batch_size` can be raised once bulk is in place; tune against `maxBytes` (10MB) and
  the destination's `max_batch_rows`.

---

## 11. How to add a new destination (worked examples)

**ClickHouse (descriptor-only):**
```python
class ClickHouseConnector(BaseMCPConnector, DestinationLoadMixin):
    load_spec = DestinationLoadSpec(
        load_method="native_insert",   # client.insert(table, rows)
        merge_method="replacing",      # ReplacingMergeTree dedups on key
        supports_staging=False,
    )
    # bulk insert + dedup inherited; no write loop written by hand.
```

**Snowflake (descriptor + one stage hook):**
```python
class SnowflakeConnector(BaseMCPConnector, DestinationLoadMixin):
    load_spec = DestinationLoadSpec(load_method="stage_copy", merge_method="merge")

    def _bulk_stage_load(self, conn, stage_table, columns, rows):
        # write rows to an internal stage, then COPY INTO stage_table
        ...   # ~15 lines: PUT file to @%stage, COPY INTO
    # MERGE staging→target generated by the mixin from merge_method="merge".
```

A new OLTP engine with native upsert: descriptor only. A new warehouse: descriptor + one
ingest hook. Nothing reimplements staging, batching, merge SQL, transactions, or fallback.

---

## 12. Correctness & safety guarantees

- **Idempotent** — merge on `key_fields` (or synthetic `_rsync_row_hash`) → re-running a
  batch is safe, same as today.
- **Atomic per batch** — one transaction, single commit.
- **Never-fail-on-bulk** — any bulk-path exception falls back to the per-row loop; the batch
  still completes.
- **Schema-evolution preserved** — `ensure_table` still runs before load; staging `LIKE
  target` inherits the evolved schema.
- **No data widening** — staging is `ON COMMIT DROP` / dropped explicitly; no residue.

---

## 13. Phasing & rollout

| Phase | Scope | Outcome |
|---|---|---|
| **1** | `DestinationLoadMixin` in base + Postgres `copy`/`on_conflict` (hand-curated) + this doc; rebuild Postgres connector; re-measure 917-row run | Immediate throughput fix + pattern proven |
| **2** | MySQL (`multi_values`/`on_duplicate_key`) + generic `delete_insert` fallback (hand-curated) | OLTP coverage |
| **3** | Snowflake, BigQuery, Redshift, ClickHouse descriptors/hooks (hand-curated) | Warehouse coverage |
| (later) | Sink flush-on-idle + batch-size tuning | Removes fixed-latency dead-wait |

All destination DB/WH connectors are **hand-curated** (§9) — the tool generator (api/saas
only) is not touched.

---

## 14. Testing plan

- **Unit** — merge-SQL generator per `merge_method`; staging name collision; synthetic-PK
  path; fallback triggers on injected bulk failure.
- **Integration (Phase 1)** — real Azure PostgreSQL: correctness (row counts, upsert
  idempotency on re-run, case-sensitive columns, NULL/type binding) + timing vs the per-row
  baseline on the Shopify 917-row run.
- **10k benchmark (fix-first, seed-later)** — after Phase 1 lands, seed ~10k orders into the
  Shopify dev store and measure end-to-end against the "10k in minutes" target.
- **Driver + Reviewer** for the integration pass per repo testing convention.

---

## Open questions for review

1. Staging type — `TEMP … ON COMMIT DROP` (simplest, session-scoped) vs `UNLOGGED` real
   table (survives across batches, reusable). Proposal: TEMP per batch for Phase 1.
2. Should `load_strategy` in `get_capabilities()` be informational only (proposed) or should
   the sink actively size batches from it in Phase 1?
3. Generic `delete_insert` fallback — acceptable for engines with no native upsert, or
   require explicit opt-in per destination?
