# The Data Explorer

Ask a question in English, get SQL, run it against a customer warehouse, then keep the
useful ones — as a saved query, a version-controlled artifact, or a table that rebuilds
itself on a schedule.

This page is the map of the whole feature. The one subsystem large enough to need its own
document has one: [Saved queries, models & schedules](saved-queries-and-models.md).

| For | Read |
|---|---|

---

## 1. The shape of it

```
        ┌── you type English ──┐
        │                      ▼
        │        ┌─────────────────────────┐
        │        │  schema index (cached)  │──► top-K table retrieval
        │        └─────────────────────────┘         │
        │                      │                     ▼
        │                      │            NL → tables → columns
        │                      │           (LLM, with HITL escape hatch)
        │                      ▼                     │
        │              /explorer/query  ◄────────── SQL
        │                      │
        └── or you just type SQL ──┘
                               │
              role gate ──► engine ──► rows (PII-redacted)
                               │
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
    export / share         save it            make it a model
    (csv/tsv/json,      (versions, diff,     (rebuild on cron,
     Slack, email)       restore, approval)   interval, or pipeline)
```

Two entry points converge on one execution path. Everything downstream — export, sharing,
saving, scheduling — hangs off the same validated SQL.

## 2. Feature map

| Feature | Where |
|---|---|
| Run SQL against a connection | [§3](#3-running-sql) |
| Natural language → SQL | [§4](#4-natural-language--sql) |
| Schema browser, index, and table retrieval | [§5](#5-schema-intelligence) |
| Human-in-the-loop table/column resolution | [§6](#6-human-in-the-loop-resolution) |
| Suggested next actions after a result | [§6](#6-human-in-the-loop-resolution) |
| **Save a query; versions, diff, restore, approval** | [§7](#7-saving-versioning-and-scheduling) → [deep dive](saved-queries-and-models.md) |
| **Models: materialize a query as a table** | [§7](#7-saving-versioning-and-scheduling) → [deep dive](saved-queries-and-models.md) |
| **Schedules: cron, interval, after-pipeline** | [§7](#7-saving-versioning-and-scheduling) → [deep dive](saved-queries-and-models.md) |
| Export to CSV / TSV / JSON | [§8](#8-export-and-sharing) |
| Share to Slack / email | [§8](#8-export-and-sharing) |
| Create a Metabase dashboard | [§9](#9-metabase-dashboards) |
| PII redaction, egress and LLM data rules | [§10](#10-data-protection) |
| SQL editor, completions, statement splitting | [§11](#11-frontend) |

---

## 3. Running SQL

`POST /api/v1/explorer/query` →
[`ExecuteExplorerQuery`](../../api-gateway/internal/handlers/explorer.go:453).

**Role gate first, always.** `validators.ValidateExplorerStatement` runs before anything
touches a connection ([:478](../../api-gateway/internal/handlers/explorer.go:478)):

| Class | Verbs | Minimum role |
|---|---|---|
| `read` | `SELECT`, `WITH` | any member |
| `dml_write` | `INSERT`, `UPDATE`, `DELETE`, `MERGE` | admin |
| `ddl` | `CREATE`, `ALTER` | admin |
| `destructive` | `DROP`, `TRUNCATE`, `ALTER … DROP` | **owner** |
| `blocked` | `GRANT`/`REVOKE`/`CALL`/`EXEC`/`SET` | nobody |

Two status codes, deliberately distinguished
([:480-486](../../api-gateway/internal/handlers/explorer.go:480)): a role shortfall is
**403** (you are authenticated but not permitted); malformed SQL, a blocked class, or
**more than one statement** is **400**. The single-statement rule is a security boundary —
see [the deep dive's §3](saved-queries-and-models.md#3-authorization--the-part-to-change-carefully)
for why stacked SQL is the thing it exists to stop.

**Then routing.** [`ResolveExplorerCapability`](../../api-gateway/internal/handlers/explorer_capability.go:86)
is the single source of truth; unsupported connectors 400 here rather than failing deeper.

| Engine | Strategy | Reads | Writes |
|---|---|---|---|
| PostgreSQL / Redshift | direct driver | `executePostgresQuery` | `executeDirectWrite` |
| MySQL / MariaDB | direct driver | `executeMySQLQuery` | `executeDirectWrite` |
| SQL Server | direct driver | `executeSQLServerQuery` | `executeDirectWrite` |
| Databricks | direct (REST) | `executeDatabricksQuery` | `executeDatabricksWrite` |
| BigQuery, ClickHouse | **delegated** | [`queryViaOrchestrator`](../../api-gateway/internal/handlers/explorer.go:346) | same, via MCP execute |
| MongoDB | unsupported | — | — |

Reads scan rows; writes take a no-rows path and report an affected-row count.

**Errors are classified, not blanket-500'd**
([:585-596](../../api-gateway/internal/handlers/explorer.go:585)). A typo or a stale table
reference is the user's, not the server's: `syntax_error`/`missing_table_or_column` → 400,
`permission_denied` → 403, `timeout` → 504, `network_or_unavailable` → 502.

**Writes are audited.** [`auditExplorerWrite`](../../api-gateway/internal/handlers/explorer.go:624)
records who ran what, against which connection, with the row count and the statement
truncated to 2000 chars. Best-effort — an audit failure never fails the query.

> **Known defect — `LIMIT` is silently downgraded.**
> [`ensureLimit`](../../api-gateway/internal/handlers/explorer.go:1664) replaces a user's
> explicit `LIMIT` with the server cap whenever theirs is larger
> ([:1674-1676](../../api-gateway/internal/handlers/explorer.go:1674)), and says nothing.
> Preview caps at 500 rows, export at 10 000. Tracked as **DX-LimitDowngrade**; the fix is to honour `min(user, cap)` *and* return a
> "limited to N" flag.

---

## 4. Natural language → SQL

`POST /api/v1/sql/generate` →
[`GenerateSQL`](../../api-gateway/internal/handlers/explorer.go:100). Note the path has
**no `/explorer` prefix** ([main.go:1095](../../api-gateway/cmd/server/main.go:1095)) — it
predates the Explorer namespace and is shared with other NL→SQL callers. Calls the
`TEXT2SQL_ENDPOINT` service (falling back to `LLM_SERVICE_URL`), 120s context timeout.

Four things happen around the LLM call that are easy to miss:

- **The quota gate runs first** ([:112](../../api-gateway/internal/handlers/explorer.go:112)).
  A workspace over its monthly NL→SQL allowance gets **402** *before* the request is built,
  so a blocked workspace pays no LLM cost. Fail-open: no workspace, nil DB, or a query
  error all read as unlimited.
- **The dialect is derived from the connection, not trusted from the client**
  ([:123-137](../../api-gateway/internal/handlers/explorer.go:123)). The UI sends the right
  one, but a caller that omits it used to get PostgreSQL SQL for a MySQL warehouse.
- **Missing schema context produces a warning, not silence**
  ([:148-151](../../api-gateway/internal/handlers/explorer.go:148)). Without schema the
  generator guesses column names — `active` vs `is_active` — and fails at execution. The
  warning makes the cause visible instead of the SQL merely being wrong.
- **Metering counts one success, not one attempt**
  ([`recordNLQueryUsage`](../../api-gateway/internal/handlers/explorer.go:277)). It sits
  after the non-empty-SQL guard, so failed generations bill nothing, and the gateway sees
  one request/response regardless of the llm-service's internal retry fan-out. Direct SQL
  via `/explorer/query` involves no LLM and is never metered.

> **Known gap:** no timeout-plus-backoff on the explorer's NL→SQL calls, so a transient
> 429 or cold start shows as a hung spinner (~70s observed). **DX-SqlGenResilience**.

---

## 5. Schema intelligence

| Route | Handler |
|---|---|
| `GET /explorer/connections/:id/schema-index` | [`GetSchemaIndex`](../../api-gateway/internal/handlers/explorer.go:2020) |
| `POST /explorer/connections/:id/schema-index/refresh` | [`RefreshSchemaIndex`](../../api-gateway/internal/handlers/explorer.go:2120) |
| `POST /explorer/tables/retrieve` | [`RetrieveTables`](../../api-gateway/internal/handlers/explorer.go:3029) |
| `POST /explorer/connections/:id/tables/recommend` | [`GetRecommendedTablesForExplorer`](../../api-gateway/internal/handlers/explorer.go:1965) |

[`buildSchemaIndex`](../../api-gateway/internal/handlers/explorer.go:2199) fans out to a
per-engine fetcher (Postgres, MySQL, SQL Server, Databricks) and collects tables, columns
and **foreign keys** — the FK graph is what lets NL resolution suggest join keys rather
than guessing them. Cached in Redis with a **10-minute TTL**
([main.go:431](../../api-gateway/cmd/server/main.go:431)) and stamped with a `SchemaHash`,
so a client can tell whether an answer was computed against the schema it is looking at.

Retrieval is **lexical, not vector**: `cache.ExtractSearchTerms` on the question, then
`cache.RetrieveTopTables` for the top K (default 20, hard cap 50 —
[:3041-3046](../../api-gateway/internal/handlers/explorer.go:3041)). Its job is to fit a
large warehouse into a prompt, not to answer the question.

**Internal tables never surface.**
[`isInternalExplorerTable`](../../api-gateway/internal/handlers/explorer.go:1131) hides
rsync's own bookkeeping and pipeline-staging tables (`_rsync*`, `rsync_*`, `flat_mysql_*`,
`flat_pg_*`, `flat_postgres_*`) from the index, NL resolution, and HITL. It tolerates a
schema qualifier, and is mirrored on the client by
[`internalTables.ts`](../../frontend/src/lib/explorer/internalTables.ts) — **the pair must
stay in lockstep**; a table hidden in one and shown in the other is the bug this shape
exists to prevent.

---

## 6. Human-in-the-loop resolution

| Route | Handler | Answers |
|---|---|---|
| `POST /explorer/nl/resolve-tables` | [`ResolveExplorerTables`](../../api-gateway/internal/handlers/explorer.go:3143) | which tables does this question mean? |
| `POST /explorer/nl/resolve-columns` | [`ResolveExplorerColumns`](../../api-gateway/internal/handlers/explorer.go:3329) | which columns are the metrics/filters? |
| `POST /explorer/nl/next-steps` | [`GetExplorerNextSteps`](../../api-gateway/internal/handlers/explorer.go:3484) | now that there are results, what next? |

Both resolvers return ranked candidates with a confidence and a **reason string**, plus a
`needs_hitl` flag. When it is set the UI stops and asks — [HITLTablePicker](../../frontend/src/components/explorer/HITLTablePicker.tsx)
or [HITLMetricPicker](../../frontend/src/components/explorer/HITLMetricPicker.tsx) — rather
than proceeding on a low-confidence guess.

**The gateway does not own the HITL threshold.** It passes through the LLM service's own
`needs_hitl` ([:3286](../../api-gateway/internal/handlers/explorer.go:3286),
[:3452](../../api-gateway/internal/handlers/explorer.go:3452)). If you go looking for a
tunable confidence cutoff in the gateway, there isn't one — it lives in the LLM service.

`next-steps` degrades to a static list (create a dashboard, download CSV) when the LLM is
unavailable ([:3538](../../api-gateway/internal/handlers/explorer.go:3538)), so the panel
never renders empty.

> **Known gap:** the HITL picker flow and the SQL re-qualification regex have never been
> driven live. **DX-CornerCaseCoverage**.

---

## 7. Saving, versioning, and scheduling

This is the largest subsystem and has its **own document**:
**[Saved queries, models & schedules](saved-queries-and-models.md)**.

In brief:

- **Saving** — private or workspace-visible, one name per workspace.
- **Version control** — every edit snapshots the prior version in the same transaction, so
  history can never disagree with what ran. History, an in-repo line diff (no new
  dependency), and restore. **Restore is a normal edit**, not a privileged path, so it
  inherits the approval gate automatically.
- **Approval gate** — editing a *scheduled* query does not change it. The SQL becomes a
  proposal an admin must approve, because `saved_queries.sql_text` stays the approved text
  and is the column the run reads.
- **Models** — a saved query with `materialization` set rebuilds a warehouse table
  (`table` mode) or runs its statement as written (`statement` mode).
- **Schedules** — `cron`, `interval`, or `after_pipeline` (fires when an upstream pipeline
  completes). One live schedule per query: a model has a clock **or** an upstream, never
  both.

---

## 8. Export and sharing

| Route | Handler | Notes |
|---|---|---|
| `POST /explorer/export` | [`ExportQueryHandler`](../../api-gateway/internal/handlers/explorer.go:3720) | csv / tsv / json |
| `GET /explorer/export.csv` | [`ExportCSVHandler`](../../api-gateway/internal/handlers/explorer.go:3572) | legacy, kept for deep links |
| `POST /explorer/share/slack` | [`ShareToSlack`](../../api-gateway/internal/handlers/explorer.go:3930) | via webhook |
| `POST /explorer/share/email` | [`ShareViaEmail`](../../api-gateway/internal/handlers/explorer.go:4047) | via platform SMTP |

Export re-validates the SQL server-side and clamps the limit to 10 000 regardless of what
the client asked for ([:3732-3738](../../api-gateway/internal/handlers/explorer.go:3732)) —
"client sends 10000; we trust nothing".

**The redaction asymmetry is deliberate.** The preview path masks PII by column name; the
export path does **not** (`executePostgresQueryUnredacted` and siblings,
[:748-755](../../api-gateway/internal/handlers/explorer.go:748)). A data owner exporting
their own rows should get their own data — and the export endpoint has already enforced
workspace-scoped ownership on the connection before it gets there. Read those two functions
together or the unredacted one looks like a hole.

> ### Email sharing is a hardened endpoint
> It previously accepted an arbitrary `to[]` and sent through platform SMTP credentials —
> an open phishing relay that would have burned the platform's SPF/DKIM reputation within
> hours ([:4059-4063](../../api-gateway/internal/handlers/explorer.go:4059)). Three gates
> now: **max 5 recipients** per send, **max 10 sends per user per hour** (sliding window,
> [`shareViaEmailRateLimit`](../../api-gateway/internal/handlers/explorer.go:4026)), and a
> **domain allowlist** — `SHARE_EMAIL_ALLOWED_DOMAINS`, and when it is unset the only
> accepted recipient is the caller's own address, read from the session. Default-safe;
> operators opt into broader use.

---

## 9. Metabase dashboards

`POST /explorer/metabase/dashboard` →
[`CreateMetabaseDashboard`](../../api-gateway/internal/handlers/explorer.go:1710). Creates a
saved question (card) and a dashboard from Explorer SQL, returning the dashboard URL so the
UI can link straight to it.

---

## 10. Data protection

- **PII redaction on preview** — masked by column-name match before rows leave the gateway
  ([:993](../../api-gateway/internal/handlers/explorer.go:993)), on every engine including
  the delegated path ([:430](../../api-gateway/internal/handlers/explorer.go:430)). Bypassed
  only for data-owner export (§8).
- **Outbound TLS verifies by default.**
  [`resolvePostgresSSLMode`](../../api-gateway/internal/handlers/explorer.go:808) defaults
  remote hosts to `verify-full` — encrypt *and* check the certificate and hostname. The
  previous `require` default encrypted without verifying, leaving a MITM window on customer
  DB traffic. Local/docker hosts default to `disable`; a server whose CA isn't in the image
  trust store opts out with `ssl_mode=require`.
- **Local-host classification is address-based**, not a textual dot heuristic, so an IPv6
  or public IPv4 literal is correctly treated as remote
  ([`isLocalDBHost`](../../api-gateway/internal/handlers/explorer.go:767),
  [`isPrivateOrLoopbackIP`](../../api-gateway/internal/handlers/explorer.go:787)). One
  residual is documented in the code: a dotless single-label hostname resolving to a public
  address is still treated as local.
- **Identifiers injected into `SET search_path` are guarded and quoted**
  ([`isSafeSchemaName`](../../api-gateway/internal/handlers/explorer.go:1148),
  [`quotePGIdent`](../../api-gateway/internal/handlers/explorer.go:1154)).
- **LLM prompts carry metadata only** — schema names, column names, types, FK edges, and the
  user's own question. **Never row values, query results, credentials, or PII.** This is a
  platform rule, not an Explorer-local one.
- **Audit logs are admin-only and are never sent to an LLM**, which is why the raw statement
  is stored unscrubbed ([:619-623](../../api-gateway/internal/handlers/explorer.go:619)).

---

## 11. Frontend

`frontend/src/components/explorer/` — [SqlEditor](../../frontend/src/components/explorer/SqlEditor.tsx),
[SchemaBrowser](../../frontend/src/components/explorer/SchemaBrowser.tsx),
[HITLTablePicker](../../frontend/src/components/explorer/HITLTablePicker.tsx),
[HITLMetricPicker](../../frontend/src/components/explorer/HITLMetricPicker.tsx),
[ExplorerStepTimeline](../../frontend/src/components/explorer/ExplorerStepTimeline.tsx),
[NlExamplePrompts](../../frontend/src/components/explorer/NlExamplePrompts.tsx), plus the
saved-query dialogs covered in the [deep dive](saved-queries-and-models.md#11-frontend).

`frontend/src/lib/explorer/` — [sqlCompletions](../../frontend/src/lib/explorer/sqlCompletions.ts) /
[sqlCompletionSources](../../frontend/src/lib/explorer/sqlCompletionSources.ts) (schema-aware
autocomplete), [schemaTree](../../frontend/src/lib/explorer/schemaTree.ts),
[runShortcut](../../frontend/src/lib/explorer/runShortcut.ts),
[examplePrompts](../../frontend/src/lib/explorer/examplePrompts.ts),
[internalTables](../../frontend/src/lib/explorer/internalTables.ts).

> ### The editor holds many queries; the server accepts one
> The editor is a scratchpad, but the whole buffer used to be sent — so the server's
> single-statement rule 400'd it, and selecting the one query you wanted changed nothing
> because the selection was never read.
> [`sqlStatements.ts`](../../frontend/src/lib/explorer/sqlStatements.ts) is a real lexer
> (`''` doubling, `E'…'` escapes, quoted and backticked identifiers, `--` and block
> comments, `$$`/`$tag$` dollar quoting — so a `;` inside a literal never splits) and
> `resolveRunTarget` picks **selection → statement under the caret → whole buffer**.
>
> This is a safety property, not a convenience: **every client-side gate reads the exact
> string the fetch will send.** Classifying the buffer while executing one statement is the
> shape that once let a declined `DROP` run. The destructive-confirm dialog captures its own
> target, so moving the caret behind the modal cannot swap the statement out from under it.
> The classifier lives in [`statementClass.ts`](../../frontend/src/lib/explorer/statementClass.ts)
> so the gate and the splitter are testable together. **The client gate is UX; the server
> gate is the boundary.**

---

## 12. Every route

Auth-required and workspace-scoped ([main.go:1095–1182](../../api-gateway/cmd/server/main.go:1095)).

| Method | Path | Role |
|---|---|---|
| `POST` | `/explorer/query` | member; write classes escalate |
| `POST` | `/sql/generate` — *no `/explorer` prefix* | member (quota-gated) |
| `POST` | `/explorer/connections/:id/tables/recommend` | member |
| `GET` | `/explorer/connections/:id/schema-index` | member |
| `POST` | `/explorer/connections/:id/schema-index/refresh` | member |
| `POST` | `/explorer/tables/retrieve` | member |
| `POST` | `/explorer/nl/resolve-tables` | member |
| `POST` | `/explorer/nl/resolve-columns` | member |
| `POST` | `/explorer/nl/next-steps` | member |
| `GET` | `/explorer/export.csv` | member (legacy) |
| `POST` | `/explorer/export` | member |
| `POST` | `/explorer/share/slack` | member |
| `POST` | `/explorer/share/email` | member (capped + rate-limited) |
| `POST` | `/explorer/metabase/dashboard` | member |
| `GET`·`POST` | `/explorer/saved` | member |
| `GET`·`PATCH`·`DELETE` | `/explorer/saved/:id` | creator or admin |
| `GET` | `/explorer/saved/:id/versions` | member |
| `POST` | `/explorer/saved/:id/pending/approve` · `/reject` | **admin** |
| `GET` | `/explorer/schedules` | viewer |
| `PUT` | `/explorer/saved/:id/materialization` | admin |
| `POST` | `/explorer/saved/:id/run` | admin |
| `GET` | `/explorer/saved/:id/runs` | admin |
| `GET`·`POST`·`PUT`·`DELETE` | `/explorer/saved/:id/schedule` | admin |
| `POST` | `/explorer/saved/:id/schedule/pause` · `/resume` | admin |
| `GET` | `/explorer/saved/:id/upstreams` | viewer |
| `POST` | `/internal/explorer/models/:id/run` | S2S only |

---

## 13. Known gaps

Open items — none are regressions, all are known:

| ID | Gap | Est. |
|---|---|---|
| **DX-LimitDowngrade** | An explicit `LIMIT` above the cap is silently downgraded with no notice (§3) | ~2h |
| **DX-SqlGenResilience** | No timeout/backoff on NL→SQL; a transient 429 or cold start hangs the spinner (§4) | ~half-day |
| **DX-CornerCaseCoverage** | HITL picker flow and SQL re-qualification regex never driven live (§6) | ~half-day |

**Closed in-repo, not yet deployed.** `DX-VersionRace` and `DX-VersionRetention` are
implemented and passing locally but have not run on prod, so treat them as fixed in `main` and
unproven in production until PRODUCT_STATUS says otherwise. Concurrent edits are now refused
with a 409 rather than a 500, and version history has an opt-in per-workspace retention policy
that defaults to keep-forever — design and the trap to avoid are in
[§6](saved-queries-and-models.md#6-version-history-diff-and-restore).

Deliberately not built — recorded so they aren't re-litigated as oversights:
`incremental` materialization, a stored dependency graph, an LLM fallback for table
extraction, two triggers on one model, and enforced second-person approval. Reasons in the
[deep dive's §12](saved-queries-and-models.md#12-deliberately-not-built).
