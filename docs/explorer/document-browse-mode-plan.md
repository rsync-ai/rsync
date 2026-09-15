# Data Explorer: MongoDB Document Browse Mode — Implementation Plan

> **Status:** proposed, not started. Written 2026-09-15 against `main` @ `5f798550`.
> Line references are as of that commit — re-grep before editing.
> Replaces the one-line deferral in [saved-queries-and-models.md:1004](saved-queries-and-models.md).

## 1. Why this, and not SQL-over-Mongo

The MongoDB BI Connector (`mongosqld`) reaches end-of-life in **September 2026**, and its
successor (MongoDB SQL Interface) requires Atlas or Enterprise Advanced, is JDBC/ODBC-only,
and needs a separately maintained SQL schema. Neither can ship in the OSS images or serve
Community-edition users. Document browse mode needs only `pymongo`, which the connector
already uses, and works on every MongoDB edition. SQL analytics over Mongo data remain
available the rsync way: sync the collection into a warehouse and query it there.

## 2. Goal and non-goals

**Goal (v1):** a member picks a MongoDB connection in the Data Explorer, sees its
collections with sampled fields, runs a read-only `find` (filter / projection / sort /
limit), pages through results, and inspects nested documents as a collapsible JSON tree.

**Not in v1** (see §9): NL→filter, `aggregate` pipelines, saved document queries,
export, models/schedules, any write.

## 3. What exists today (verified)

| Fact | Where |
|---|---|
| Mongo resolves `Supported: false`; the test pins it | [explorer_capability.go:136-140](../../api-gateway/internal/handlers/explorer_capability.go), [explorer_capability_test.go:92-97](../../api-gateway/internal/handlers/explorer_capability_test.go) |
| `/explorer/query` runs the SQL validator **before** loading the connection, clamps limit to 500 | [explorer.go:466-493](../../api-gateway/internal/handlers/explorer.go) |
| Delegated reads always call the connector's **`export`** tool with `{query, sql, limit, config}` — no way to pass a find spec | [executor.go:7624-7675](../../backend-orchestrator/internal/agents/executor/executor.go), route [main.go:1456-1500](../../backend-orchestrator/cmd/orchestrator/main.go) |
| Mongo `export` ignores `query`, requires `collection`, has no filter (`_id` keyset only) | [connector.py:522-579](../../shared/mcp-connectors/public/database/mongodb/versions/v1.0.0/connector.py) |
| Mongo `discover_schema` lists collections + fields sampled from 20 docs | [connector.py:369-441](../../shared/mcp-connectors/public/database/mongodb/versions/v1.0.0/connector.py) |
| Tools are advertised from `get_capabilities().operations`; `mongodb_<op>` dispatches to `self.<op>` | [connector.py:488-509](../../shared/mcp-connectors/public/database/mongodb/versions/v1.0.0/connector.py), `base_connector.py` `_handle_tool_call` |
| `SchemaStrategy` is never read: `buildSchemaIndex` returns an empty `"unsupported"` index for everything except pg/mysql/databricks/sqlserver — **BigQuery and ClickHouse schema panels are empty today too** | [explorer.go:2203-2218](../../api-gateway/internal/handlers/explorer.go) |
| Orchestrator already has a generic discover endpoint the gateway calls for connection metadata | [main.go:1308-1360](../../backend-orchestrator/cmd/orchestrator/main.go), caller [connections.go:3029-3057](../../api-gateway/internal/handlers/connections.go) |
| PII redaction on delegated reads is **top-level column name only** — a nested `customer.email` would not be redacted | [explorer.go:430-441](../../api-gateway/internal/handlers/explorer.go), [connections.go:1961-1975](../../api-gateway/internal/handlers/connections.go) |
| `CreateSavedQuery` has **no connector-type check** — only statement class + viewer on the connection | [saved_queries.go:352-364](../../api-gateway/internal/handlers/saved_queries.go) |
| Materialization already rejects Mongo (`modelDialect` has no case) | `saved_query_models.go` `modelDialect`, `saved_query_schedules.go` SetSavedQueryMaterialization |
| Frontend reads only `supports_explorer`; `explorer_mode` is typed but unused; page is 3,374 lines | [page.tsx:104-117, 499-505](../../frontend/src/app/(dashboard)/explorer/page.tsx) |
| Results grid `JSON.stringify`s objects into one truncated line; no JSON viewer component exists; `@codemirror/lang-json` not installed | `page.tsx` `formatCellValue`, `frontend/package.json` |
| Mongo connector is hand-curated — no Jinja template to sync | `connector_database.py.j2:61` |
| A local replica-set Mongo exists for testing | [docker-compose.e2e.dbs.yml:243](../../docker-compose.e2e.dbs.yml) (`mongo-e2e`, `mongo:6`, `rs0`) |

## 4. Design decisions

**D1 — A separate endpoint, not `/explorer/query`.** The query path is SQL all the way
down: statement validation and role classes, `ClassifyStatementSQL`, write audit, saved-query
`sql_text`. Bending it to carry a find spec would weaken those gates. New route:
`POST /api/v1/explorer/documents/find`.

**D2 — A new `find` connector tool and orchestrator operation.** Don't overload `export`:
pipelines depend on its `_id`-keyset contract.

**D3 — Allowlist query operators, enforced twice.** The gateway (Go) rejects before any
network hop, and the connector (Python) re-validates, so the connector is safe even if it's
called directly. Allowlist:

- Comparison: `$eq $ne $gt $gte $lt $lte $in $nin`
- Logical: `$and $or $nor $not`
- Element / array: `$exists $type $elemMatch $size $all`
- Other: `$regex $options $mod`
- Extended-JSON value wrappers: `$oid $date $numberLong $numberDecimal`

Everything else is rejected, **explicitly including** `$where $function $accumulator $expr`
(server-side JavaScript / arbitrary expressions), plus `$text`, `$jsonSchema` and geo
operators (deferred). Other limits:

- Filter: JSON object only, depth ≤ 20, ≤ 64 KB serialized.
- Projection: `field: 0|1` only, no `$` keys.
- Sort: ≤ 5 keys, each `1|-1`. Accepted as an object **or** an ordered list of `[field, direction]` pairs; the gateway must send the list form, because a Go `map[string]any` does not preserve key order and sort order *is* key order.
- Collection name: 1–120 bytes, no `$` or NUL, and not a `system.*` collection.

**D4 — Hard limits.**

- Documents: default limit 50, max 500. Fetch `limit+1` to compute `has_more`.
- Server time: `maxTimeMS` 15 000 on every find.
- Response size: capped at 5 MB serialized; past that, truncate and set `truncated_bytes: true`.
- Gateway → orchestrator timeout: 60 s, the same as `queryViaOrchestrator`.

**D5 — Paging.**

- Default sort `{_id: 1}` (or `{_id: -1}`): keyset paging. The filter becomes `{$and: [userFilter, {_id: {$gt: cursor}}]}` (`$lt` when descending), so it stays stable under concurrent writes. The cursor is **opaque**: canonical Extended JSON of `{_id: last}`, so the `_id`'s BSON type survives — a 24-hex *string* `_id` resumes as a string, which `export`'s `str()` cursor cannot do. Clients pass it back verbatim and never parse it. If the projection hides `_id`, it is still fetched for the cursor and stripped from the output.
- Custom sort: `skip`-based "Load more", with skip capped at 10 000 and a UI hint to narrow the filter.

**D6 — Results as Relaxed Extended JSON.** Serialize with
`bson.json_util.dumps(..., json_options=RELAXED_JSON_OPTIONS)` rather than `_json_safe`,
which flattens `ObjectId` and dates to plain strings. That way a copied value (`{"$oid": …}`)
pastes straight back into a filter, and the tree can show type badges. `export` keeps
`_json_safe`; nothing about pipelines changes.

**D7 — Recursive redaction.** Add `redactDocument(v any) any` in the gateway. It walks maps
and arrays and applies `shouldRedactColumnName` to **every key at every depth**. Browse
responses always redact, matching SQL previews.

**D8 — Capability and guards.** Add `langDocument = "document"`. Mongo resolves to
`{Supported: true, QueryLanguage: "document", ExecStrategy: execDelegated,
SchemaStrategy: schemaMCPDiscover, SupportsMaterialization: false}`. Every SQL-shaped
handler must then reject `QueryLanguage != "sql"` with a 400 that points at the new
endpoint. Those handlers are:

- `ExecuteExplorerQuery` (explorer.go:528)
- both export handlers (explorer.go:3616, :3787)
- `GenerateSQL`
- `CreateSavedQuery`: this check doesn't exist today, so add it

**D9 — Collections panel through the existing discover path.** In `buildSchemaIndex`,
branch on `SchemaStrategy == schemaMCPDiscover`. It calls orchestrator
`/api/v1/agent/discover-schema` and maps `tables[].columns[]` onto
`cache.ExplorerTableIndex`, keeping the existing cache. Pass `include_row_counts: false`,
since Mongo's `countDocuments` is exact and slow on large collections. The branch is
written generically but **enabled for Mongo only**; turning it on for BigQuery and
ClickHouse is a separate, separately verified change (§9).

**D10 — Authorization.** A find is a read, so any workspace member may run it, the same as
SQL reads. The connection is loaded with `WHERE id=$1 AND workspace_id=$2`, as
explorer.go:507-511 does. Reads aren't audited, which also matches the SQL path.

## 5. API contract

```http
POST /api/v1/explorer/documents/find
{
  "connection_id": "uuid",
  "collection": "orders",
  "filter":     {"status": "paid", "customer_id": {"$oid": "65f1c0..."}},
  "projection": {"items": 0},
  "sort":       {"created_at": -1},
  "limit": 50,
  "cursor": null,          // keyset cursor (default sort only)
  "skip": 0                // custom sort only
}

200 {
  "documents": [ { ... relaxed extended JSON ... } ],
  "columns": ["_id", "status", "created_at", "customer"],   // top-level key union, _id first
  "returned": 50,
  "has_more": true,
  "next_cursor": "{\"_id\": {\"$oid\": \"65f1c0...\"}}",   // opaque; or null
  "next_skip": null,            // or 50 for custom sort
  "execution_time_ms": 41,
  "truncated_bytes": false,
  "warnings": []
}

400 {"error": "...", "error_code": "operator_not_allowed", "path": "filter.$or[1].$where"}
404 connection not in active workspace
504 {"error_code": "query_timeout", "hint": "Add a filter on an indexed field"}
```

The orchestrator side is `POST /api/v1/agent/explorer-find` with body
`{connector_type, config, connection_id, collection, filter, projection, sort, limit, cursor, skip}`.
It maps to `Operation: "find"`.

## 6. Delivery: three PRs

> **As delivered (2026-09-15):** the three layers below shipped together in **one PR**, so the interim
> PR 2 hide filter was never needed. The UI is simpler than the PR 3 sketch: a single
> `frontend/src/components/explorer/DocumentExplorer.tsx` (grid ⇄ raw JSON, row expand, Load more) plus
> `frontend/src/lib/explorer/documentSpec.ts`; `SchemaBrowser` is reused with `itemLabel="collections"`.
> No `JsonTree`, CodeMirror JSON inputs or document history yet. The gateway validator is
> `validators/document_find.go`.

Order matters: nothing may become visible in the UI before it works.

### PR 1 — Connector `find` tool (size: S)

Patch `versions/v1.0.0/` in place. This is additive and follows the destination-write
precedent in `latest.json`'s changelog.

- `connector.py`:
  - `def find(self, params)`: validate collection, filter, projection and sort (same allowlist as D3, shared module-level constants).
  - A small dedicated decoder converts only the four allowlisted wrappers (`$oid $date $numberLong $numberDecimal`). **Not** `json_util.loads`, which would also decode `$code` (JavaScript), `$binary`, `$regularExpression` and legacy `{$regex, $options}` pairs.
  - Run `coll.find(filter, projection).sort(...).skip(...).limit(limit+1).max_time_ms(ms)`.
  - Serialize per D6 and cap bytes per D4.
  - Map `ExecutionTimeout` to `error_code: query_timeout` and `OperationFailure` to a short message. Never echo filter values into errors.
  - As implemented, the connector's `error_code` set is: `invalid_config`, `invalid_collection`, `invalid_filter`, `operator_not_allowed`, `filter_too_deep`, `filter_too_large`, `invalid_projection`, `invalid_sort`, `invalid_limit`, `invalid_skip`, `invalid_cursor`, `invalid_max_time_ms`, `query_timeout`, `query_failed` (plus `mongo_code` = the server's `codeName`), `connection_failed`. Proposed for PR 2 (not yet built): the gateway maps `query_timeout` → 504, `connection_failed` → 502, the rest → 400.
- Register `{"name": "find", "method": "mongodb_find", "type": "source", ...}` in `get_capabilities().operations` (connector.py:488-509) and document it in `metadata.json` `operations`.
- `latest.json`: add a changelog line under v1.0.0.
- Tests in `test_mongodb_connector.py`, extending `_mongo_fakes.py` so `FakeCursor` supports `skip`, arbitrary sort keys, projection and general filter matching:
  - Allowlist accepts and rejects correctly, including a nested `$where` inside `$or` and `$expr`.
  - `$oid` and `$date` conversion.
  - Keyset and skip paging, and `has_more`.
  - Byte cap.
  - Timeout mapping.
  - No write method reachable from `find`.
- Run offline: `PYTHONPATH=…; cd <connector dir> && pytest -q`.

### PR 2 — Orchestrator + gateway (size: M)

- **Orchestrator:** add `Agent.ExplorerFind(ctx, connectorType, config, spec)` next to `ExplorerQuery` (executor.go:~7624) and the `/agent/explorer-find` route with `requirePrincipal`. Log only shape metadata, never filter values. Unit-test the params it sends.
- **Gateway:**
  - `validators/document_filter.go` holds the D3 allowlist, with table tests. Keep it in lockstep with the Python constants: add a test that loads both lists, or at least a comment pointer in each.
  - `handlers/explorer_documents.go` holds `FindExplorerDocuments`: auth → workspace-scoped connection load → capability must be `langDocument` → validate → orchestrator → `redactDocument` → response. Register it in `cmd/server/main.go` beside `/explorer/query` (~:1105).
  - `explorer_capability.go`: D8 resolver change. Update `explorer_capability_test.go` so Mongo is supported, document mode, delegated, materialization false. Add the guards on the handlers listed in D8, each with a handler test proving a Mongo connection gets a 400.
  - `buildSchemaIndex`: D9 branch, with a test using a stubbed orchestrator.
  - `redactDocument` with a nested and array test (`{"customer": {"email": …}, "contacts": [{"phone": …}]}`).
- **Frontend (one line, in this same PR):** tighten the connection filter at page.tsx:499-505 to `supports_explorer && (explorer_mode ?? "sql") === "sql"`, so Mongo stays hidden until PR 3 ships the UI.

### PR 3 — Frontend document mode (size: M–L)

`page.tsx` is already 3,374 lines, so it only gets a branch. The new UI lives in
`frontend/src/components/explorer/document/`:

| File | Role |
|---|---|
| `DocumentExplorer.tsx` | State + fetch for one connection; rendered by page.tsx in place of the NL box, `SqlEditor`, results card and Saved/Steps tabs when `selectedConnection.explorer_mode === "document"` |
| `FilterBar.tsx` | Three compact CodeMirror JSON inputs (filter / projection / sort) using `@codemirror/lang-json` (new dependency), a limit select (25/50/100/500), Run, and ⌘/Ctrl+Enter |
| `DocumentResults.tsx` | Toggles between **Documents** (a card per doc with `JsonTree`) and **Table** (top-level `columns`, nested values as expandable JSON); "Load more" with `next_cursor` / `next_skip`; banners for `truncated_bytes` and `warnings` |
| `JsonTree.tsx` | Dependency-free collapsible tree: type badges for `$oid`/`$date`/numbers, "copy value", "copy as filter" (`{path: value}`), auto-collapse past depth 2 |
| `documentSpec.ts` | Client mirror of the D3 validator for inline errors (the server stays authoritative), plus history (de)serialization |

- The collections list reuses `SchemaBrowser` if its props fit the mapped index. If they don't, add a thin `CollectionList`. Caption it "fields sampled from 20 documents".
- History is stored under a separate key, `rsync_explorer_doc_history_v1:<connectionId>`, with entries `{id, collection, filter, projection, sort, limit, timestamp, returned}`. That keeps the SQL history loader (which drops entries without `sql`) untouched.
- Hidden in document mode: Export, Send to BI Tool, Saved tab, Steps timeline, NL box.
- Remove PR 2's `explorer_mode` hide filter in this PR.
- Tests:
  - Vitest for `JsonTree`, `documentSpec`, and `DocumentExplorer` with mocked fetch: renders, paging, 400 path error shown inline.
  - A Playwright stub spec modeled on `e2e/explorer_nl2sql_corner_cases.spec.ts`, with a connection stub carrying `explorer_mode: "document"`.

## 7. Verification (evidence required before calling it done)

Run a live check on the local stack plus `mongo-e2e`, seeded with a collection that has
nested objects, arrays, `ObjectId`s, dates, a nested `email` field, and ≥ 1 500 documents.
Capture each item as output, not a description:

1. A collection list appears, with sampled fields.
2. A filter by `{"_id": {"$oid": …}}` returns exactly that document.
3. Custom sort + Load more returns disjoint pages. Default sort pages by keyset: no duplicates across 3 pages.
4. `{"$or": [{"a": 1}, {"$where": "sleep(1000)"}]}` → gateway 400 with `path`. The **same spec sent straight to the connector tool** is also rejected (proves D3's second layer).
5. The nested `customer.email` value comes back redacted.
6. A connection ID from another workspace → 404.
7. `/explorer/query`, export, and saved-query create against the Mongo connection → 400.
8. An unindexed regex over a large seeded collection with a lowered `maxTimeMS` → 504 `query_timeout`.
9. **Mutation check:** disable the gateway allowlist locally. Item 4 must still fail at the connector. Restore it.

### Evidence captured (2026-09-15, local stack, real remote MongoDB `shop`)

The grid was compared cell-by-cell with `GET /connections/:id/sample` — a different code path (orchestrator `SampleRows`, MCP `export`) — matching 50/50 documents by `_id` per collection:

| Collection | Cells compared | Differences |
|---|---|---|
| customers | 750 | `email` only — redacted by `redactDocument`, by design |
| orders | 850 | none after the display fix (nested `$numberDecimal` inside `items` was shown as a raw wrapper; now `"unit_price":35.10`) |
| payments | 700 | none |
| products | 700 | none |
| reviews | 650 | none |

Also observed in the GUI: Load more 50 → 100 with 0 duplicate `_id`s; `{"$where": …}` → gateway 400 shown as “…at filter.$where”; malformed JSON flagged client-side (`aria-invalid`, no request sent); JSON view parses as a 50-element array; one find request per collection switch; header “Collections (5)”.
Offline: connector pytest 63/63 (+ mutation run, 11/12 killed, survivor equivalent), live connector 19/19 vs mongo:7, gateway + orchestrator `go test` green, frontend vitest + `tsc` clean.
Not yet run live: items 6 (cross-workspace 404 — covered by handler test only), 8 (real `maxTimeMS` via the gateway) and 9 (gateway-allowlist mutation end to end).

## 8. Definition of done

- Tests and the §7 evidence above.
- `docs/explorer/saved-queries-and-models.md`: replace "deferred" with a link to this doc.
- The §9 items are tracked as follow-ups.
- Connector: no template sync (hand-curated) and no Patch Sync Ledger entry. The change is a new tool, not a bug fix.

## 9. Follow-ups (not in these PRs)

- **int64 precision in results:** the orchestrator's MCP client decodes the connector reply into `map[string]interface{}` with plain `json.Unmarshal` (`backend-orchestrator/internal/mcp/client.go:547`, no `UseNumber`), so an integer above 2^53 *inside a returned document* is rounded before it reaches the gateway. Filters are unaffected (forwarded verbatim). Fix: `json.Decoder.UseNumber()` on that path, or pass the documents through as `json.RawMessage`.
- **`/connections/:id/sample` returns unredacted values** (pre-existing, found while verifying this feature — the find path redacts `email`, the sample path does not). Needs its own triage.

- **NL → filter:** an llm-service endpoint whose prompt holds collection and sampled field names/types only (metadata-only rule). Output is validated by the same D3 allowlist. Errors pass `scrub_error_for_llm`.
- **Read-only `aggregate`:** stage allowlist (`$match $project $group $sort $limit $skip $unwind $count $addFields`, `$lookup` within the same database). Always blocks `$out $merge $function $accumulator $where`.
- **Saved document queries:** needs a `query_language` + JSON spec column on saved queries; they are SQL-text-only today.
- **Export** of the current result as JSON/NDJSON.
- **Schema panel for BigQuery/ClickHouse** by enabling the D9 branch for them (empty today per explorer.go:2208).

## 10. Open questions

1. **Result format:** Relaxed Extended JSON (D6) vs the flattened strings `export` uses? *Proposed: Extended JSON.*
2. **Viewer access:** may workspace viewers browse documents? *Proposed: yes, parity with SQL reads.*
3. **`aggregate` in v1?** *Proposed: no, follow-up.*
4. **Read preference:** honor an optional `read_preference` connection setting (e.g. `secondaryPreferred`) to keep browse load off the primary? *Proposed: honor it if set, driver default otherwise.*
5. **Scope of D9:** also light up the BigQuery/ClickHouse schema panels in PR 2, or keep that separate? *Proposed: separate.*
