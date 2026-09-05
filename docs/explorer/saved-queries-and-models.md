# Data Explorer: saved queries, models, and schedules

How a query someone typed in the Explorer becomes a table the warehouse rebuilds on its
own — and what stops that from becoming a way to run unreviewed SQL under somebody
else's authority.

This document describes **mechanism**: the data model, the authorization path, the three
triggers, and the API. It deliberately does not duplicate status.

> This is a deep dive on one subsystem. For the rest of the Data Explorer — running SQL,
> NL→SQL, schema intelligence, HITL resolution, export and sharing — start at the
> **[Data Explorer overview](README.md)**.

| For | Read |
|---|---|
| The Explorer's place in the platform | [ARCHITECTURE.md](../../ARCHITECTURE.md) |

---

## 1. The three things a saved query can be

A saved query carries a `materialization` mode. The mode is not a preference — it decides
what the SQL *is*, and the run path authorizes each mode differently.

| Mode | What it means | Needs `target_table` | Runnable on a schedule |
|---|---|---|---|
| `none` | A plain bookmark. Stored SQL, nothing else. | no | no |
| `table` | `CREATE TABLE <target> AS <sql>` — a rebuild. | **yes** | yes |
| `statement` | Run the stored SQL exactly as written. | no | yes |

`statement` mode exists because the shape people schedule most often — a `MERGE`, an
`UPDATE`, an `INSERT … SELECT` — already names its own destination. Before it, the model
dialog asked users to invent a target table before it would let them schedule anything at
all. See the rationale in
[`088_saved_query_statement_materialization.sql`](../../api-gateway/migrations/088_saved_query_statement_materialization.sql).

`incremental` is **absent on purpose**. It needs a merge key and a watermark, and shipping
the enum value before the behaviour would let a user pick a mode that silently does
something else.

The two runnable modes want opposite things, and each is incoherent with the other's SQL
rather than merely unusual — wrapping a `DELETE` in `CREATE TABLE AS` is meaningless, and a
scheduled `SELECT` burns warehouse time to deliver its rows nowhere. Both directions are
refused with a message naming the other mode
([saved_query_models.go:483](../../api-gateway/internal/handlers/saved_query_models.go:483)).

---

## 2. Data model

Six migrations, each additive:

| Migration | Adds |
|---|---|
| [084](../../api-gateway/migrations/084_saved_queries.sql) | `saved_queries`, `saved_query_versions` |
| [085](../../api-gateway/migrations/085_saved_query_models.sql) | `materialization`, `target_table`, `target_owned`, `last_run_*`; `saved_query_schedules` |
| [086](../../api-gateway/migrations/086_saved_query_runs.sql) | per-attempt run history |
| [088](../../api-gateway/migrations/088_saved_query_statement_materialization.sql) | `materialization = 'statement'` |
| [095](../../api-gateway/migrations/095_saved_query_after_pipeline_trigger.sql) | `schedule_type = 'after_pipeline'`, `trigger_pipeline_id` |
| [096](../../api-gateway/migrations/096_saved_query_pending_edits.sql) | `saved_query_pending_edits` — the approval gate |

Two constraints in 085/088 are worth knowing because they shape the API's error messages:

- **Target required only for `table`.** Written positively —
  `CHECK (materialization <> 'table' OR target_table IS NOT NULL)` — so a fourth mode has
  to opt *in* to needing a target rather than inheriting the requirement by accident.
- **One live schedule per query** (`idx_sq_schedules_unique_query`, 085). A consequence
  worth stating plainly: **a model has a clock or an upstream, never both.** Two
  independent triggers on one target table race each other into the same `DROP`/`CREATE`,
  and the advisory lock would turn that race into a silently skipped rebuild rather than
  an error anyone sees.

---

## 3. Authorization — the part to change carefully

Every run, manual or scheduled, goes through
[`authorizeModelRun`](../../api-gateway/internal/handlers/saved_query_models.go:435). It
re-derives everything from the **current** SQL and the run-as user's **current** role on
every single run. Nothing is trusted from create time.

The order of its four checks is the security property:

1. **Resolve the run-as user's role now.** A user removed from the workspace stops the
   schedule; a demoted user's schedule fails rather than keeps its old power.
2. **Meet the minimum role** — `modelRunMinRole = WSAdmin`
   ([:426](../../api-gateway/internal/handlers/saved_query_models.go:426)).
3. **`validators.ValidateExplorerStatement` — for every class, before any mode question**
   ([:470](../../api-gateway/internal/handlers/saved_query_models.go:470)).
4. **Only then** does the mode get a say.

> ### Why step 3 must stay where it is
>
> The statement gate used to sit *last*, behind an early "refuse anything that isn't a
> read". That was safe only for as long as no class but read could get past it.
> `statement` mode ended that — a `MERGE` now legitimately runs.
>
> `MERGE INTO invoices …; DROP TABLE customers` classifies as `dml_write` **on its leading
> verb**. A class cannot describe a second statement. The plan hands the whole string to a
> parameter-free `Exec`, which the driver runs under the simple query protocol — executing
> *every* statement in it under the schedule creator's authority.
>
> `ValidateExplorerStatement` is where the single-statement rule lives. It is a **security
> boundary**; any proposal to relax it must say so loudly.
>
> The save path is not a second line of defence here. It classifies the SQL and refuses
> `blocked` ([saved_queries.go:204](../../api-gateway/internal/handlers/saved_queries.go:204)),
> but it never applies the single-statement rule — storing SQL mutates nothing, so saving is
> deliberately cheap. Every stacked-statement check happens at run time, which is also the
> only place it *can* be correct: what matters is the run-as user's role **now**, not the
> saver's role then.

Running the full validator (not just the single-statement check) also puts a model's SQL
through the same policy an interactive query gets
([statement_policy.go](../../api-gateway/internal/validators/statement_policy.go)):

| Class | Verbs | Minimum role |
|---|---|---|
| `read` | `SELECT`, `WITH` | viewer |
| `dml_write` | `INSERT`, `UPDATE`, `DELETE`, `MERGE` | admin |
| `ddl` | `CREATE`, `ALTER` | admin |
| `destructive` | `DROP`, `TRUNCATE` | **owner** |
| `blocked` | `GRANT`/`REVOKE`/`CALL`/`EXEC`/`SET`/… | never |
| `unknown` | unrecognized | fails closed → owner |

Two classifier subtleties that exist because they were once bugs: an `ALTER` that drops an
object escalates to `destructive`, and a `WITH`-led statement is classified on the write it
performs rather than on `WITH`
([:134](../../api-gateway/internal/validators/statement_policy.go:134),
[:143](../../api-gateway/internal/validators/statement_policy.go:143)).

---

## 4. Triggers

### `cron` and `interval`

Registered with Temporal
([`createTemporalModelSchedule`](../../api-gateway/internal/handlers/saved_query_schedules.go:1453)).
`temporal_schedule_id` is mandatory for these and forbidden for event triggers — 095 splits
085's blanket `NOT NULL` into one conditional constraint per direction, so a violation names
which half is wrong.

The "next run" shown in the UI is computed **locally**
([`nextScheduleRun`](../../api-gateway/internal/handlers/saved_query_schedules.go:1306))
rather than by asking Temporal, because `Describe` is a network call per schedule and this
runs once per row of a list. Two consequences:

- It reproduces Temporal's tick arithmetic, so both branches must keep matching
  `createTemporalModelSchedule`. Notably **interval ticks align to the Unix epoch**, not to
  creation time — the next tick is the next epoch multiple.
- When the answer is unknowable (unparseable cron, unknown timezone) it returns **nil, not a
  guess**: a wrong time on a schedule page is worse than a blank one, because a user reads
  "next run 03:00" as a promise and stops checking.

### `after_pipeline`

A model reads tables a pipeline writes, so the honest trigger is "when that pipeline
finishes". The only way to say that with a cron is to guess a time far enough after the
pipeline usually lands — and that guess is wrong in both directions, neither of which
announces itself: too early and the model rebuilds from yesterday's data **and reports
success**; too late and the dashboard is stale for hours the user paid for by padding the
gap.

Path: `PIPELINE_COMPLETED` →
[event_projector.go:213](../../api-gateway/internal/projector/event_projector.go:213) →
`OnPipelineCompleted` (wired at [main.go:498](../../api-gateway/cmd/server/main.go:498)) →
[`FireModelsAfterPipeline`](../../api-gateway/internal/handlers/saved_query_schedules.go:1125).

Four properties of that path, each load-bearing:

- **Fires only on first storage of the event.** The hook is guarded by `stored`, so the
  same row that dedupes the event store dedupes the rebuild. Without it, an offset replay,
  a partition reassignment, or a projector started at `FirstOffset` would re-fire every
  downstream model — a `DROP`/`CREATE` of a user's table, on an event from weeks ago.
- **Tenancy is re-checked at fire time, not trusted from create time.** The lookup joins
  `sq.workspace_id = p.workspace_id`, because a pipeline can be moved after the schedule
  was authorized. Without it, moving a pipeline into workspace B would keep rebuilding
  workspace A's model on B's data, and the history would show a normal successful rebuild.
- **`status = 'active'` *is* the pause.** An event trigger has no Temporal schedule to
  pause, and the predicate is re-read on every completion.
- **Sequential within one completion, capped at 4 concurrent fan-outs globally**
  (`eventFireSlots`,
  [:1110](../../api-gateway/internal/handlers/saved_query_schedules.go:1110)). Models
  downstream of one pipeline usually read the same tables; running them one at a time keeps
  a thirty-table nightly load from opening thirty warehouse sessions at once.
- **No retry.** The next completion of the upstream pipeline *is* the retry, and it carries
  fresher data than a replay would.

---

## 5. The approval gate on scheduled SQL

A scheduled saved query is production infrastructure — it rebuilds a table other people
read. Editing one used to be a single `PUT`, and the next run silently used the new SQL.

Now [`UpdateSavedQuery`](../../api-gateway/internal/handlers/saved_queries.go:515) branches
on whether the query is scheduled:

- **Not scheduled** → snapshot the prior version and apply, in one transaction. A version
  row without its edit would make the history lie about what ran.
- **Scheduled** →
  [`proposeScheduledSQLEdit`](../../api-gateway/internal/handlers/saved_queries.go:973).
  Name, description and visibility apply immediately; **the SQL becomes a proposal**.

The enforcement is structural, not procedural:

> `saved_queries.sql_text` remains **the approved text, always**. The run path reads that
> column directly, so a proposed edit cannot reach a scheduled run — it is not in the
> column the run reads. An approval gate enforced in a handler could be bypassed by any
> future code path that writes `sql_text`; this one cannot, because there is nothing to
> bypass.

Other design notes:

- **At most one open proposal per query** (partial unique index in 096). A second returns
  **409, not 400** — the request was well formed, the *state* conflicts
  ([saved_queries.go:1012](../../api-gateway/internal/handlers/saved_queries.go:1012)). There
  is no sensible automatic merge of two SQL rewrites, so the two authors need to talk.
- **Staleness is surfaced.** `base_sql_text` records the live SQL at proposal time;
  [`loadOpenPendingEdit`](../../api-gateway/internal/handlers/saved_queries.go:1052) sets
  `Stale` when the query moved underneath. Without it, approving a stale proposal silently
  reverts whatever landed in between, and the diff the approver read would not be the
  change the approval made.
- **Rejected rows are kept.** "Who tried to change this, and who said no" is exactly what
  an audit asks.
- **The response is 200, not 202**, and says plainly what happened to the SQL rather than
  leaving the status code to imply it — part of the request *did* apply.

**Who may approve:** a workspace admin — deliberately *not* "an admin who is not the
proposer". Requiring a second person reads like the stronger control, and in a team it is,
but many workspaces here have exactly one admin; for them it would make a scheduled query
permanently uneditable, and the way out is to delete the schedule, which is strictly worse.
The teeth survive self-approval: **a plain member can propose but cannot approve**, so no
member can unilaterally change what a scheduled table means, and every approval names its
approver in the audit log. Requiring two pairs of eyes is a workspace-policy question.

---

## 6. Version history, diff, and restore

[`ListSavedQueryVersions`](../../api-gateway/internal/handlers/saved_queries.go:847) returns
the edit history **and any open proposal** in one response, so the panel can show a pending
change beside the history it would join.

The diff is a line differ written in-repo with **no new dependency**
([sqlDiff.ts](../../frontend/src/components/explorer/sqlDiff.ts), rendered by
[SqlDiffView.tsx](../../frontend/src/components/explorer/SqlDiffView.tsx)).

**Restore is not a separate privileged path** — it is a normal edit that happens to carry an
older version's text. That means a restore on a *scheduled* query goes through the same
approval gate as any other SQL change, automatically, with no extra code to keep in sync.

### Concurrent edits, and the "fix" that would have made it worse

`existing` is still read outside any transaction, at
[saved_queries.go:525](../../api-gateway/internal/handlers/saved_queries.go:525) — that
read is the request's view of the row, and it can be stale by the time the write runs. What
changed is that it is no longer *trusted*. Inside the transaction the row is re-read under a
lock and compared before anything is written
([:660](../../api-gateway/internal/handlers/saved_queries.go:660)):

```sql
SELECT name, sql_text, statement_class, updated_at FROM saved_queries WHERE id = $1 FOR UPDATE
```

If the locked `updated_at` no longer matches the one the request was built from, the edit is
refused with **409** and `code: "stale_write"`
([:685](../../api-gateway/internal/handlers/saved_queries.go:685)). `updated_at` costs nothing
to use as a concurrency token — a trigger already maintains it (migration 084) — and
`ListSavedQueryVersions` was already handing clients `current.updated_at`, so no API contract
had to change. Clients that want to guard a *long-open editor* — the window the server cannot
see, where someone opened the query an hour ago — may additionally send
`expected_updated_at` ([:176](../../api-gateway/internal/handlers/saved_queries.go:176));
omitting it preserves existing behaviour exactly.

The same lock and the same snapshot-from-locked-values rule apply on the review path
([:1157](../../api-gateway/internal/handlers/saved_queries.go:1157)), which takes
`pending_edits` then `saved_queries` — it is the only path that takes both, so there is no
deadlock against the edit path.

> **Do not "fix" this by changing how version numbers are allocated.** A sequence or a retry
> loop removes the error and introduces silent data loss: T1 snapshots the original and writes
> `A`; T2, holding a stale `existing`, snapshots the original *again* and writes `B`. Final
> state is `B`, history reads `[original, original]`, and T1's change is gone with no record it
> existed — on SQL a scheduled model runs. **`UNIQUE (saved_query_id, version)` was doing
> double duty as a lost-update guard**, and the old 500 was a bad message wrapped around
> correct, protective behaviour. The fix removes the racy read, not the constraint, which still
> stands as a backstop and now maps to 409 rather than 500.

Re-reading under the lock also closed a second hole the constraint never caught: an edit that
changes only metadata takes the ungated path — no SQL change, no approval — but used to write
its own stale `sqlText` back. Two concurrent edits, one renaming and one changing SQL, could
therefore silently revert reviewed SQL on a scheduled query. The snapshot and the write now
both use the locked values, so a metadata edit cannot carry stale SQL with it.

### Retention

History is no longer unconditionally unbounded, but **the default is still keep-forever**.
Migration 097 adds a per-workspace policy with two axes that are ANDed — a version is deleted
only if it is both older than `retention_days` *and* outside the newest `min_versions`. Age
alone would wipe the history of a query edited twice a year, which is the history most worth
keeping; count alone leaves an unbounded ancient tail on a query edited hourly. Neither is
offered on its own, and `min_versions` has a schema-level floor of 5
([saved_query_retention.go:44](../../api-gateway/internal/handlers/saved_query_retention.go:44)),
because a policy that can erase the previous version destroys the feature it configures.

`retention_days` is NULL out of the migration, so applying it deletes nothing anywhere until an
admin calls `PUT /api/v1/explorer/version-retention`
([main.go:1136](../../api-gateway/cmd/server/main.go:1136)). Reading the policy is member-level;
setting it is admin-only and audited, because the deletions it causes happen later on an
unrelated request and the audit record is the only thing tying them back to a decision.

**The prune is deliberately not load-bearing.** It runs after the edit's transaction has
committed, in its own statement, and a failure is logged and swallowed
([:133](../../api-gateway/internal/handlers/saved_query_retention.go:133)). Housekeeping must
never be able to fail someone's save — including in an environment where 097 has not been
applied yet. Pruning loses old SQL *content* and never accountability: `audit_logs` records
that an edit happened independently, and `saved_query_pending_edits.base_sql_text` stores its
text inline, so a prune cannot orphan an open proposal.

---

## 7. Upstream suggestion

The schedule dialog answers "which pipeline produces the tables this model reads?" so the
user does not have to remember one
([`SuggestSavedQueryUpstreams`](../../api-gateway/internal/handlers/saved_query_upstreams.go:77)).

**It is a suggestion, and nothing is written.** No dependency-edge table exists — the
answer is recomputed per request. That is the design, not a limitation: an inferred edge
that re-derives itself whenever someone edits the SQL is a schedule that changes without
anyone asking for it. A person picks from the list, and what they picked stays picked.

Table references come from a **deterministic parser**, not an LLM
([`ExtractTableReferences`](../../api-gateway/internal/validators/table_references.go:93)) —
a real lexer that tracks CTE names so a `WITH` alias is not reported as a table.

Two matching rules that are easy to get backwards
([saved_query_upstreams.go:29-38](../../api-gateway/internal/handlers/saved_query_upstreams.go:29)):

1. It matches `destination_qualified_name`, **never** `qualified_name`. A model's SQL runs
   against the warehouse and names *destination* tables; `qualified_name` is the source-side
   name. Matching on it would answer with whichever pipeline happens to *read* a table of
   that name — for a MySQL→Postgres pipeline, a different pipeline than the one that wrote it.
2. It is scoped to the model's own connection and workspace. A pipeline landing
   `analytics.orders` into a different destination produces a different table that merely
   shares a name.

The response reports `references`, `unresolved`, `candidates` and `ambiguous`, so the dialog
can say "3 of 4 inputs have a known producer" rather than silently showing what it happened
to find. When `ambiguous` is true the UI **must not pre-select anything**.

`destination_qualified_name` is NULL for object-storage destinations and sinks older than
migration 089, so those cannot be suggested. That is a miss — and a miss is the right way to
be wrong here: the dialog falls back to the manual picker, whereas a confident wrong answer
hangs a schedule off an unrelated pipeline.

---

## 8. Engine support

[`ResolveExplorerCapability`](../../api-gateway/internal/handlers/explorer_capability.go:86)
is the single source of truth for both the connections API and query/schema/export dispatch.
Adding a warehouse means editing **only** that table.

| Connector | Dialect | Execution | Explorer | Can be a model |
|---|---|---|---|---|
| MySQL / MariaDB | `mysql` | direct | ✅ | ✅ |
| PostgreSQL / Redshift | `postgresql` | direct | ✅ | ✅ |
| SQL Server | `tsql` | direct | ✅ | ✅ |
| Databricks | `databricks` | direct | ✅ | ❌ |
| BigQuery | `bigquery` | delegated | ✅ | ❌ |
| ClickHouse | `clickhouse` | delegated | ✅ | ❌ |
| MongoDB | — | — | ❌ | ❌ |

**`SupportsMaterialization` is deliberately NOT derived from `ExecStrategy == direct`.**
Databricks executes directly and queries perfectly well, but
[`modelDialect`](../../api-gateway/internal/handlers/saved_query_models.go:177) has no case
for it, so a rebuild would refuse at run time. Being direct is necessary, not sufficient:
the materialization path needs a driver that can execute DDL, a narrower set than the read
path. The flag and `modelDialect` are pinned in lockstep by
`TestSupportsMaterializationMatchesModelDialect` — if you add a warehouse, that test is the
one that tells you you have only done half the job.

The UI disables the materialization control **with a reason** rather than hiding it, so a
BigQuery user learns why instead of hunting for a missing button.

---

## 9. API

All routes are workspace-scoped and auth-required
([main.go:1123–1163](../../api-gateway/cmd/server/main.go:1123)).

**Saved queries**

| Method | Path | Role |
|---|---|---|
| `GET` | `/api/v1/explorer/saved` | member — workspace-visible + own private |
| `POST` | `/api/v1/explorer/saved` | member |
| `GET` | `/api/v1/explorer/saved/:id` | member |
| `PATCH` | `/api/v1/explorer/saved/:id` | creator or admin — **proposes** if scheduled |
| `DELETE` | `/api/v1/explorer/saved/:id` | creator or admin |
| `GET` | `/api/v1/explorer/saved/:id/versions` | history + any open proposal |
| `POST` | `/api/v1/explorer/saved/:id/pending/approve` | **admin** |
| `POST` | `/api/v1/explorer/saved/:id/pending/reject` | **admin** |

**Models and schedules** (admin — `modelRunMinRole`)

| Method | Path | Notes |
|---|---|---|
| `PUT` | `/api/v1/explorer/saved/:id/materialization` | set/clear the target table |
| `POST` | `/api/v1/explorer/saved/:id/run` | materialize once, now, **as the caller** |
| `GET` | `/api/v1/explorer/saved/:id/runs` | attempt history (086) |
| `GET`·`POST`·`PUT`·`DELETE` | `/api/v1/explorer/saved/:id/schedule` | detaching leaves the table |
| `POST` | `/api/v1/explorer/saved/:id/schedule/pause` · `/resume` | |
| `GET` | `/api/v1/explorer/saved/:id/upstreams` | viewer — read-only suggestion |
| `GET` | `/api/v1/explorer/schedules` | viewer — all schedules in the workspace |

**Internal (S2S)** — `POST /api/v1/internal/explorer/models/:id/run`
([main.go:1182](../../api-gateway/cmd/server/main.go:1182)), behind
`InternalServiceMiddleware`. This is the route Temporal calls; the user-facing run route
fail-closes without a session.

---

## 10. Operational notes

- **Run timeout: 30 minutes** (`modelRunTimeout`,
  [saved_query_models.go:70](../../api-gateway/internal/handlers/saved_query_models.go:70)).
  Event-driven runs get `timeout + 1 minute` **per run, not per batch**, so one slow model
  cannot eat the budget of the models queued behind it.
- **One rebuild at a time per model** — a Postgres advisory lock
  ([`acquireModelRunLock`](../../api-gateway/internal/handlers/saved_query_models.go:711)).
- **Auto-pause is distinguishable from an operator pause.**
  [`autoPauseModelSchedule`](../../api-gateway/internal/handlers/saved_query_schedules.go:1236)
  writes `auto_paused_*`, not `paused_*`, because the resume path treats them differently.
  A machine pause means "this cannot succeed as configured" — a dropped connection, a
  demoted run-as user.
- **The schedule paths are Temporal-optional.** An `after_pipeline` schedule can be created,
  paused, resumed and deleted with no Temporal client at all, because nothing is registered
  with it.
- **Deleting a trigger pipeline cascades** (`ON DELETE CASCADE` on `trigger_pipeline_id`).
  The alternative is worse than it looks: `SET NULL` would leave a row the CHECK forbids, so
  Postgres would refuse the pipeline `DELETE` with a constraint error naming a saved query
  the operator has never heard of. With CASCADE the pipeline stays deletable and the model
  simply shows as unscheduled, which is what it is.

---

## 11. Frontend

| File | Role |
|---|---|
| [SavedQueries.tsx](../../frontend/src/components/explorer/SavedQueries.tsx) | the list |
| [SavedQueryModelDialog.tsx](../../frontend/src/components/explorer/SavedQueryModelDialog.tsx) | materialization + all three triggers + upstream picker |
| [SavedQueryEditDialog.tsx](../../frontend/src/components/explorer/SavedQueryEditDialog.tsx) | edit; surfaces the approval notice |
| [SavedQueryHistoryDialog.tsx](../../frontend/src/components/explorer/SavedQueryHistoryDialog.tsx) | version history, diff, restore |
| [SqlDiffView.tsx](../../frontend/src/components/explorer/SqlDiffView.tsx) · [sqlDiff.ts](../../frontend/src/components/explorer/sqlDiff.ts) | the differ |
| [savedQueryUpdate.ts](../../frontend/src/components/explorer/savedQueryUpdate.ts) | pure update/response logic |
| [explorer/schedules/page.tsx](../../frontend/src/app/%28dashboard%29/explorer/schedules/page.tsx) | workspace-wide schedules view |

---

## 12. Deliberately not built

Recording these so they are not re-litigated as oversights:

- **`incremental` materialization** — needs a merge key and watermark (085).
- **A stored dependency graph** — the upstream answer is a live suggestion by design (§7).
- **An LLM fallback for table extraction** — the deterministic parser was kept after
  measurement.
- **Two triggers on one model** — one live schedule per query, to avoid a rebuild race.
- **Second-person approval** — a workspace-policy question, not a hard-coded rule (§5).
- **Document (MongoDB) browsing** — designed, deferred to a future Document Explorer.

Open Data Explorer items — `DX-LimitDowngrade`, `DX-SqlGenResilience`,
`DX-CornerCaseCoverage` — are tracked separately and are **not** part of
this subsystem.
