# SaaS / API Destination Contract

> Scope: **API/SaaS REST + GraphQL connectors** acting as a pipeline **destination**.
> Databases and warehouses follow [base-interface.md](base-interface.md) +
> [destination-load-strategies.md](destination-load-strategies.md); they are *not*
> covered here. SaaS connectors are **not** CDC sinks — `ensure_table` and
> `get_cdc_offsets` are **not** required of them (they have no DDL and no offset
> store). This doc pins exactly what a generated SaaS connector must implement to
> be selectable as a destination and to load data **idempotently**.

The reference implementation for every rule below is the database template
(`llm-service/src/agents/tool_generator/templates/connector_database.py.j2`):
`import_data` ≈ L2460, `upsert_data` ≈ L2577. SaaS connectors mirror its
**parameter handling and return shape**, not its SQL.

---

## 1. Operations a SaaS destination implements

| Method | Required when | Returns (success) |
|---|---|---|
| `import_data(params)` | `supports_destination=true` (append) | `{"success": true, "rows_inserted": <int>}` |
| `upsert_data(params)` | resource exposes an update/PATCH/PUT verb **or** `synthetic_pk` | `{"success": true, "rows_upserted": <int>}` |
| `delete_data(params)` | resource exposes a DELETE verb | `{"success": true, "rows_deleted": <int>}` |

`ensure_table`, `drop_table`, `get_cdc_offsets` are **DB/warehouse-only**. A SaaS
connector MUST NOT advertise `supports_cdc=true`.

### Honest capability advertising (`metadata.json`)
- `supports_destination` = `true` **iff** at least one resource has a write verb
  (`supports_create`/`supports_update`/`supports_delete`) **or** `synthetic_pk`.
- `destination_modes` = subset of `["append", "upsert", "delete"]` actually
  implemented. `append` ⇒ `import_data`; `upsert` ⇒ `upsert_data`; `delete` ⇒
  `delete_data`. Never list a mode whose method isn't implemented — the planner
  trusts this list when wiring a pipeline.

---

## 2. Parameters every write method receives

All write methods take a single `params: dict`. The executor / sink populates:

| Key | Meaning | Handling rule |
|---|---|---|
| `data` | `List[dict]` — the batch of source rows | Empty/absent ⇒ return success with count `0` (no-op, never error). |
| `namespace` / `db_or_schema` | per-pipeline target qualifier | Forward to qualify the target resource. Treat the literal `"default"` as **unset** (skip it) — it's the planner's placeholder. |
| `key_fields` (aka `keys`, `primary_key_fields`) | columns identifying a row | Required for `upsert_data`/`delete_data`. See §3 for the empty case. |
| `synthetic_pk` | `bool` | When `true` **and** `key_fields` empty, compute `_rsync_row_hash` (see §3). |

A write method that silently ignores `namespace` or `key_fields` is a **contract
violation** — it causes cross-tenant writes or non-idempotent reloads.

---

## 3. Idempotency (the core destination rule)

A pipeline **re-runs**. Loading the same batch twice MUST NOT create duplicates
when the destination supports identity.

1. **`upsert_data` is the idempotent path.** It matches existing rows on
   `key_fields` and updates-or-creates (API PATCH/PUT on the key, or
   create-then-update fallback). Re-running with the same batch is a no-op delta.
2. **`key_fields` empty + `synthetic_pk=true`:** compute
   `_rsync_row_hash = sha256(canonical_json(row))` over **all source columns**,
   and upsert on that hash. This gives keyless sources a stable identity so a
   reload de-dupes. (Same primitive the DB template uses for GIPK/keyless tables.)
3. **`key_fields` empty + no `synthetic_pk`:** `import_data` (append) is the only
   honest option; the connector MUST NOT claim `upsert` in `destination_modes`.

`import_data` (append) is **not** required to be idempotent, but it MUST return an
accurate `rows_inserted` count so the executor can reconcile.

---

## 4. Batching & counts

- Write in **batches**, not one network round-trip per row. Respect the API's
  bulk endpoint when one exists; otherwise chunk and bound concurrency.
- Every write method MUST return an **explicit integer** row count
  (`rows_inserted` / `rows_upserted` / `rows_deleted`) reflecting rows actually
  persisted — never echo the input length blindly. Partial failures go in
  `errors: [...]` with the count reflecting only successes.

---

## 5. Source side (for completeness — see ExportResult)

A SaaS **source** returns an `ExportResult` (`base_connector.py`). Destination
authors don't implement this, but the same connector is often both, so the source
contract is summarized here:

- `records` — the page(s) of rows.
- `next_cursor` — resume token for cursor pagination.
- `has_more` — **`true` only when a cap (`max_pages`/`max_records`) truncated the
  result.** Lets the executor distinguish "complete" from "truncated, fetch more"
  instead of silently dropping the tail.
- `stats.watermark` / `stats.max_watermark` — the incremental checkpoint, computed
  as the **MAX** of the incremental field over **all** returned rows (never just
  the last row — APIs may return DESC or unsorted).
- On a **0-row incremental re-run**, the connector echoes the **input** watermark
  back in `stats.watermark` so the executor doesn't reset the checkpoint.

---

## 6. Conformance

Generated connectors are checked against this contract by the destination-contract
QA assertions (see the tool-generator QA scaffold). A connector that advertises a
mode it can't honor, ignores `namespace`/`key_fields`, or returns an inaccurate
count fails QA and is not shipped.
