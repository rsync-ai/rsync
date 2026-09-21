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
([saved_query_models.go:484](../../api-gateway/internal/handlers/saved_query_models.go:484)).

---

## 2. Data model

Seven migrations. Six are additive; 100 is the one that moves data and drops a column.

| Migration | Adds |
|---|---|
| [084](../../api-gateway/migrations/084_saved_queries.sql) | `saved_queries`, `saved_query_versions` |
| [085](../../api-gateway/migrations/085_saved_query_models.sql) | `materialization`, `target_table`, `target_owned`, `last_run_*`; `saved_query_schedules` |
| [086](../../api-gateway/migrations/086_saved_query_runs.sql) | per-attempt run history |
| [088](../../api-gateway/migrations/088_saved_query_statement_materialization.sql) | `materialization = 'statement'` |
| [095](../../api-gateway/migrations/095_saved_query_after_pipeline_trigger.sql) | `schedule_type = 'after_pipeline'`, `trigger_pipeline_id` |
| [096](../../api-gateway/migrations/096_saved_query_pending_edits.sql) | `saved_query_pending_edits` — the approval gate |
| [100](../../api-gateway/migrations/100_saved_query_upstream_fan_in.sql) | `saved_query_schedule_upstreams`; renames the trigger to `after_upstream`; drops `trigger_pipeline_id` |

Two constraints in 085/088 are worth knowing because they shape the API's error messages:

- **Target required only for `table`.** Written positively —
  `CHECK (materialization <> 'table' OR target_table IS NOT NULL)` — so a fourth mode has
  to opt *in* to needing a target rather than inheriting the requirement by accident.
- **One live schedule per query** (`idx_sq_schedules_unique_query`, 085). A consequence
  worth stating plainly: **a model has a clock or a set of upstreams, never both.** The
  schedule row is the model's single wake-up policy; 100 moved the upstream *set* into a
  child table without touching that index, so every route that addresses a schedule by
  saved-query id kept working. 095's header explained the rule by saying two triggers
  would race into the same `DROP`/`CREATE`; migration 100's header retracts that —
  `acquireModelRunLock` takes an advisory lock keyed on the model before any DDL, so a
  second concurrent rebuild is *skipped*, not interleaved. The real cost of a second
  trigger is a dropped rebuild, which is why fan-in is expressed as one schedule with
  many producers rather than many schedules.

---

## 3. Authorization — the part to change carefully

Every run, manual or scheduled, goes through
[`authorizeModelRun`](../../api-gateway/internal/handlers/saved_query_models.go:436). It
re-derives everything from the **current** SQL and the run-as user's **current** role on
every single run. Nothing is trusted from create time.

The order of its four checks is the security property:

1. **Resolve the run-as user's role now.** A user removed from the workspace stops the
   schedule; a demoted user's schedule fails rather than keeps its old power.
2. **Meet the minimum role** — `modelRunMinRole = WSAdmin`
   ([:427](../../api-gateway/internal/handlers/saved_query_models.go:427)).
3. **`validators.ValidateExplorerStatement` — for every class, before any mode question**
   ([:471](../../api-gateway/internal/handlers/saved_query_models.go:471)).
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

### Authorizing an upstream set

Naming a producer is a second authorization question, asked at write time on the create and
update paths. Every entry in a proposed set is checked as a resource in its own right —
viewer on the pipeline, or viewer on the model
([`authorizeUpstreams`](../../api-gateway/internal/handlers/saved_query_schedules.go:2418)).
Without that, the field is a cross-tenant probe: the run history of a model the caller *does*
own would report, run by run, when another tenant's pipeline finishes.

**One refusal refuses the whole request.** The tempting alternative — store the upstreams the
caller can see, drop the rest — produces a model that quietly waits on fewer producers than
the operator asked for, and nothing distinguishes that from a producer that simply has not run
yet.

A model upstream is also checked for a ring
([`checkUpstreamCycle`](../../api-gateway/internal/handlers/saved_query_schedules.go:2522)),
which walks *up* from each proposed model and asks whether the model being scheduled is
already an ancestor. Only model edges can close a ring — a pipeline is never downstream of a
model. Two details are load-bearing: the recursive term uses `UNION`, not `UNION ALL`, so a
ring already stored by some other path terminates the walk instead of hanging the request; and
a failed check **refuses** rather than allows, because the check cannot be re-run after the
write and a stored ring rebuilds on every upstream completion until the depth bound catches it.

---

## 4. Triggers

### `cron` and `interval`

Registered with Temporal
([`createTemporalModelSchedule`](../../api-gateway/internal/handlers/saved_query_schedules.go:3068)).
`temporal_schedule_id` is mandatory for these and forbidden for event triggers — 095 splits
085's blanket `NOT NULL` into one conditional constraint per direction, so a violation names
which half is wrong.

The "next run" shown in the UI is computed **locally**
([`nextScheduleRun`](../../api-gateway/internal/handlers/saved_query_schedules.go:2916))
rather than by asking Temporal, because `Describe` is a network call per schedule and this
runs once per row of a list. Two consequences:

- It reproduces Temporal's tick arithmetic, so both branches must keep matching
  `createTemporalModelSchedule`. Notably **interval ticks align to the Unix epoch**, not to
  creation time — the next tick is the next epoch multiple.
- When the answer is unknowable (unparseable cron, unknown timezone) it returns **nil, not a
  guess**: a wrong time on a schedule page is worse than a blank one, because a user reads
  "next run 03:00" as a promise and stops checking.

**How a cron is shown.** The schedule pages and the Model dialog put a cron into words
where one short sentence is exact: *Every day at 03:00 (UTC)*, *Every 15 minutes, Monday to
Friday (Asia/Kuala_Lumpur)*. Anything else is shown as typed
([`describeCron`](../../frontend/src/components/explorer/cronSentence.ts:234)). The same
rule as the next run applies: a sentence that is nearly right is worse than the expression,
because nobody re-reads the expression after reading the sentence.

- It reads the gateway's grammar, not generic cron: robfig/cron v1.2.0, five fields,
  weekdays 0-6, three-letter names, no `@daily`. That is what
  [`validateScheduleSpec`](../../api-gateway/internal/handlers/pipeline_schedules.go:917)
  accepts and `nextScheduleRun` parses.
- **The two day fields get no sentence when neither is `*`.** Temporal, which fires the
  schedule, needs a day to match both fields. robfig, which computes *Next run*, fires on a
  day matching either one when neither is starred. So `0 9 1 * 1` gets no sentence, because
  the two disagree on what it means. Whether the *Next run* column is then wrong for such a
  schedule has not been checked against a live Temporal.
- The expression stays in reach: under the sentence on the model page, and as the hover
  text of the Cadence cell on the schedules list.
- It describes the pattern only. When the next run happens still comes from the server.

Tests: [cronSentence.test.ts](../../frontend/src/components/explorer/__tests__/cronSentence.test.ts).

### `after_upstream`

A model reads tables something else writes, so the honest trigger is "when that thing
finishes". The only way to say that with a cron is to guess a time far enough after the
producer usually lands — and that guess is wrong in both directions, neither of which
announces itself: too early and the model rebuilds from yesterday's data **and reports
success**; too late and the dashboard is stale for hours the user paid for by padding the
gap.

Migration 095 spelled this `after_pipeline` and stored one pipeline id on the schedule row.
Migration 100 renamed it because neither half of that name survived: an upstream can be
another model, and there can be more than one
([:142](../../api-gateway/internal/handlers/saved_query_schedules.go:142)). A staging model
feeding a reporting model no longer needs a cron guess about when the first one finishes,
and a model reading from three pipelines can name all three instead of being wired to
whichever one someone picked.

**Fan-in is a per-schedule choice: `upstream_policy` `any` (the default) or `all`**
(migration 104, [`normalizeUpstreamPolicy`](../../api-gateway/internal/handlers/saved_query_schedules.go:163)).
Under `any` a completion fires every schedule that lists it, so a model naming three nightly
pipelines rebuilds three times a night, and coalescing — one refresh workflow per model — is
what keeps a burst to one rebuild. That is wrong for a real join: the model rebuilt the moment
the first producer finished, reading the second one's previous output, and then again when the
second finished. Under `all` a completion rebuilds the model only when every other upstream
has also succeeded since this model's last successful rebuild **started**
([`unsatisfiedUpstreams`](../../api-gateway/internal/handlers/saved_query_schedules.go:2105));
otherwise it writes a `skipped` run with `skip_reason = waiting_on_upstreams` saying how many
are stale, and the last sibling to finish does the one rebuild. The message gives a count, not
names: run history is readable by anyone who can see this model, and a stale upstream may be
another member's private model. Both sides of the comparison are on the database clock — a
run's `started_at` is written as `NOW()` less its elapsed time — so a gateway whose clock is
ahead cannot make a fresh sibling look stale. The firing upstream is excluded by id, so an
adapter that sends an unrecognised `kind` loses the provenance (with a warning) but not the
exclusion. A pipeline's completions are read from `pipeline_run_events`, which retention
deletes after `PIPELINE_RUN_RETENTION_DAYS` (90 by default): a pipeline upstream that last
completed before that reads as never completed and holds the model until it runs again. `all` is still not a barrier over a
*night*: deciding when a batch is over is a freshness question, answered by a deadline (§12).
Both schedule reads carry the policy — the one-model route and the workspace list
([`ScheduledQuerySummary.UpstreamPolicy`](../../api-gateway/internal/handlers/saved_query_schedules.go:696)) —
because the list is where the cadence sentence is drawn, and a client that is not told
falls back to "After any of…" for a model that waits on all of them. The policy also decides
the Next run an active `after_upstream` schedule shows, on the list and on the model's own page
([`describeUpstreamNextRun`](../../frontend/src/components/explorer/scheduledModel.tsx:102)): a
fan-in on `all` reads "When all its upstreams have run", because the next landing alone does
not rebuild it. With one upstream the policy changes nothing, so that model keeps "When an
upstream runs".

Two doors reach the same path:

- A pipeline finishing — `PIPELINE_COMPLETED` →
  [event_projector.go:213](../../api-gateway/internal/projector/event_projector.go:213) →
  `OnPipelineCompleted` (wired at [main.go:498](../../api-gateway/cmd/server/main.go:498)) →
  [`FireModelsAfterPipeline`](../../api-gateway/internal/handlers/saved_query_schedules.go:1837).
- A model finishing —
  [`fireDownstreamModelsAfterRun`](../../api-gateway/internal/handlers/saved_query_schedules.go:1859),
  called from all three doors a model run comes through: the manual route, the internal
  endpoint Temporal drives, and the in-process fallback below. A model is an upstream
  whatever caused it to rebuild, and a chain that continued from only some of those would be
  a rule nobody could hold in their head. Only a **succeeded** run propagates.

Both converge on
[`fireDownstreamModels`](../../api-gateway/internal/handlers/saved_query_schedules.go:2092),
which reads the child table by upstream — the opposite direction from every other reader of
it, and the reason `idx_sq_upstreams_by_pipeline` and `idx_sq_upstreams_by_model` exist.

From there the whole set is handed over in **one** call. The gateway starts a single
[`UpstreamFanOutWorkflow`](../../backend-temporal-adapter/internal/workflows/upstream_fanout_workflow.go:123) per completion
([`temporalUpstreamFanOutDispatch`](../../api-gateway/internal/handlers/saved_query_schedules.go:1577)) carrying every waiting model as its argument,
and that workflow starts one [`ModelRefreshDispatchWorkflow`](../../backend-temporal-adapter/internal/workflows/upstream_fanout_workflow.go:215)
child per model. Each child runs one activity,
[`SignalModelRefreshActivity`](../../backend-temporal-adapter/internal/workflows/upstream_fanout_workflow.go:245), which signal-with-starts
that model's [`ModelRefreshWorkflow`](../../backend-temporal-adapter/internal/workflows/model_refresh_workflow.go:112) on the id
`model-refresh:<saved_query_id>` — and that workflow calls the same internal run endpoint the
clock path calls, with `trigger=after_upstream`.

**Why the fan-out is a workflow and not a loop.** Until this shape, only the *rebuild* was
durable. The gateway looked up the models and then signalled them one at a time from a
detached goroutine, so a gateway that restarted after the third of nine signals left six
models unrebuilt with nothing recording that they were owed anything — the goroutine died
with the process, the in-process fallback died with it, and the next rebuild of those six was
whenever their upstream happened to run again. Now the gateway makes one call and the fan-out
is Temporal's problem the moment it returns; the gateway can die immediately afterwards and
all nine models are still signalled.

**Why the child is not `ModelRefreshWorkflow` itself.** That is the shape this looks like it
should have and it does not work. A refresh loop has to be reached by *signal-with-start*, and
that atomicity is the whole reason a completion arriving mid-rebuild coalesces instead of
losing the model's run lock. `ExecuteChildWorkflow` cannot express signal-with-start; a child
keyed on the model would collide across parents; and a child started without a signal would
block on its signal channel forever, because the loop has no run timeout. Signal-with-start is
a client-side primitive, so the only place a workflow can perform one is inside an activity.

With no Temporal client — or when the fan-out fails to start — every model in the batch falls
back to the in-process goroutine this path has always used. The fallback is now
all-or-nothing rather than per-model, because there is one call to fail instead of N. Both
routes end at the same endpoint, so a deployment without Temporal loses durability, not the
trigger.

Thirteen properties of that path, each load-bearing:

- **Fires only on first storage of the event.** The pipeline door is guarded by `stored`, so
  the same row that dedupes the event store dedupes the rebuild. Without it, an offset
  replay, a partition reassignment, or a projector started at `FirstOffset` would re-fire
  every downstream model — a `DROP`/`CREATE` of a user's table, on an event from weeks ago.
- **Tenancy is re-checked at fire time, not trusted from create time.** The lookup joins
  `sq.workspace_id = up.workspace_id`, because a producer can be moved after the schedule
  was authorized. Without it, moving a pipeline into workspace B would keep rebuilding
  workspace A's model on B's data, and the history would show a normal successful rebuild.
  The Temporal route re-checks at run time
  ([`modelRunScheduleLookup`](../../api-gateway/internal/handlers/saved_query_schedules.go:1468)),
  because a workflow can rebuild long after the lookup that queued it. That second check is
  written as "**no** upstream is outside this model's workspace" rather than as a check on
  the one that fired, because the signal does not name which one that was and a fan-in
  schedule has several. The strict reading fails in the safe direction: a set split across
  workspaces stops rebuilding, which an operator sees.
- **`status = 'active'` *is* the pause.** An event trigger has no Temporal schedule to
  pause, and the predicate is re-read on every completion.
- **The fire path is the last guard that an event trigger has an upstream.** 095 could state
  that as a CHECK; a cross-table requirement cannot be one. Three weaker things replace it —
  the single transaction each write path uses, `validateModelScheduleSpec` refusing an empty
  set, and this lookup, which matches *by* upstream and so cannot see a schedule that has
  none. Only the last is beyond the reach of a row written by some future path.
- **Completions arriving during a rebuild are kept, not dropped.** One workflow per model
  means a second completion — of the same producer or a different one in the set — is
  delivered to the run already working and becomes one further rebuild. The fallback route
  does the same in-process
  ([`rebuildLocally`](../../api-gateway/internal/handlers/saved_query_schedules.go:2257)):
  it used to lose the model's advisory run lock and drop that completion, which was found
  live on a policy-`any` fan-in with Temporal down — woken by the root and by its sibling one
  hop later, on two goroutines. Only a run held by *another* process (a second replica, or
  the Temporal route) still refuses through the lock, and that holder wakes the same
  consumers when it finishes. Coalescing is sound because a rebuild replaces the target wholesale rather than applying a
  delta. A rerun is a new trigger, so under `all` it meets the policy again rather than
  rebuilding against a sibling that has not finished. One call runs at most
  `maxLocalRebuildRuns` = 5 times; past that the next rebuild is requeued for a fire slot
  instead of holding one for as long as completions keep arriving, and a panic is recovered
  and logged so the claim is released
  ([`rebuildLocallyAbsorbing`](../../api-gateway/internal/handlers/saved_query_schedules.go:2261)).
- **A redelivered completion does not fan out twice.** The fan-out is named after the
  completion that caused it and started under `ALLOW_DUPLICATE_FAILED_ONLY`, so Kafka
  redelivering `PIPELINE_COMPLETED` is refused by Temporal rather than rebuilding everything
  a second time. That refusal is read as success
  ([`isFanOutAlreadyStarted`](../../api-gateway/internal/handlers/saved_query_schedules.go:1772)); falling back on it would run the whole batch on the
  one route that has no coalescing. The name is the pipeline execution id, or a fresh uuid
  for a model hop, which has no execution of its own
  ([`fanOutOccurrence`](../../api-gateway/internal/handlers/saved_query_schedules.go:1638)). A hop must **not** reuse the execution id it carries —
  that id names the pipeline that started the chain, so every hop would collide with the
  first and the chain would stop after one, silently and as a duplicate.
- **One model that cannot be signalled does not cost the others.** Every child is started
  before any is awaited, so a dispatch stuck behind a rate-limited frontend delays nothing.
  Children are `ParentClosePolicy: ABANDON`, so one still retrying outlives a parent that
  failed or was terminated — cancelling a fan-out cannot half-deliver it. A child retries for
  ten minutes (`modelRefreshDispatchHorizon`) and then stops: past that the honest answer is
  that this completion did not reach that model, and the parent's history names which ones.
- **The fallback route is sequential within one completion, capped at 4 concurrent
  fan-outs globally** (`eventFireSlots`,
  [:1785](../../api-gateway/internal/handlers/saved_query_schedules.go:1785)). Models
  downstream of one pipeline usually read the same tables; running them one at a time keeps
  a thirty-table nightly load from opening thirty warehouse sessions at once. Rebuilds
  Temporal accepted are bounded by the worker instead and never take a slot.
- **A chain is bounded at `maxTriggerChainDepth` = 8**
  ([:186](../../api-gateway/internal/handlers/saved_query_schedules.go:186)). The depth
  rides on the fan-out argument, then on each child, then on the signal payload, as well as
  on the in-process call — so a chain that hops through Temporal is bounded by the same
  number as one that does not. A hop that dropped it would report a chain eight deep as depth
  zero, and the bound would never fire. It is a backstop, not the
  cycle check: a ring is refused at write time by `checkUpstreamCycle`
  ([:2522](../../api-gateway/internal/handlers/saved_query_schedules.go:2522)), which is the
  only place a person can be told about it. It walks only schedules that are not `deleted`:
  a deleted schedule keeps its upstream rows, and counting them refused a legitimate edge
  forever. A chain that hits the bound writes a `skipped` run with
  `skip_reason = chain_depth_exceeded` on the model it did not wake. Neither replaces the other — a depth bound alone
  would let a two-model ring rebuild eight times before stopping, and a write-time check
  alone cannot bound a chain that is long without being circular.
- **Retry belongs to the workflow.** It retries a rebuild three times with backoff, and
  spent retries do not fail the run — the completions buffered behind it would go down
  with it. A query the engine rejected on its merits is not retried at all, because the
  endpoint answers 200 with status `failed`. The fallback route still has no retry: there,
  the next completion of an upstream *is* the retry, and it carries fresher data than a
  replay would.
- **Every triggered run says what woke it, and a model that was not rebuilt says why.**
  Migration 104 adds provenance to `saved_query_runs` — `upstream_kind`/`upstream_id`,
  `upstream_run_id` (the upstream model's own run row), `origin_execution_id` (the pipeline
  execution at the root, when there is one), `trigger_depth` (hops from the root) and
  `coalesced_count` — written by
  [`recordModelRunOutcome`](../../api-gateway/internal/handlers/saved_query_models.go:1151).
  A model below an upstream whose run failed gets a `skipped` row with `upstream_failed`, and
  everything below that `upstream_skipped`, each linked to the row above it
  ([`recordSkippedDownstream`](../../api-gateway/internal/handlers/saved_query_schedules.go:1914)).
  A transient "already in progress" skip does not cascade — its holder wakes the same models.
  The schedules page renders these through
  [runProvenance.ts](../../frontend/src/components/explorer/runProvenance.ts).
- **A schedule cannot be left waiting on nothing.** Deleting a pipeline or model pauses every
  active schedule whose *only* upstream it was, inside the delete's transaction and before the
  rows cascade
  ([`pauseTriggersOrphanedBy`](../../api-gateway/internal/handlers/saved_query_schedules.go:2585)),
  with an `auto_paused_reason` saying an upstream was deleted (not which one, for the same
  private-model reason as the skip message); a saved-query delete reports the count
  as `paused_downstream_schedules`. Only active schedules are marked: one the user had already
  paused stays paused with no reason and no upstreams, and nothing reports it until a resume
  is refused (409, "choose new upstreams"). A plain saved query (materialization `none`) is refused as
  an upstream at write time
  ([`refuseUnbuildableUpstreams`](../../api-gateway/internal/handlers/saved_query_schedules.go:2621)),
  because its runs never succeed and so it could never wake anything.
- **A Temporal that just failed is not re-dialled on every hop.** One failed fan-out start opens
  a one-minute breaker ([`fanOutTemporalBreaker`](../../api-gateway/internal/handlers/saved_query_schedules.go:1700));
  while it is open each hop goes straight to the in-process route instead of paying the dial
  deadline again, so a chain with Temporal down no longer costs ~10 s per hop. After the
  window exactly one dispatch is let through as the probe; the rest keep taking the in-process
  route until it answers, and a probe that never answers expires after another window. A dial failure inside the lazily-created client is
  not observed by the breaker — only a failed start is.

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

The schedule dialog answers "which pipelines and models produce the tables this model
reads?" so the user does not have to remember
([`SuggestSavedQueryUpstreams`](../../api-gateway/internal/handlers/saved_query_upstreams.go:98)).

The answer lists every table the query's SQL reads, so the endpoint loads the query through
[`loadSavedQuery`](../../api-gateway/internal/handlers/saved_queries.go:412), the same loader
as `GET`: another member's private query is a 404 here too. Workspace membership alone, which
is all the role gate proves, would disclose it.

**It suggests both kinds of upstream.** A pipeline is found through the tables it was
observed writing (`pipeline_run_table_stats`). A model is found through the tables it writes,
read through [`modelProducedTables`](../../api-gateway/internal/handlers/table_producers.go:112),
the same definition the asset graph uses:

- a `materialization='table'` model writes its `target_table`;
- a `statement` model writes the tables its own SQL names (`INSERT INTO`, `MERGE INTO`,
  `UPDATE`, …, via `ExtractWriteTargets`). A leftover `target_table` from an earlier `table`
  life is ignored, and SQL holding more than one statement produces nothing — the runner
  refuses it, so it never writes a row.

Neither is an observation, so a model is offered before it has ever run; the schedule being
set up may be what makes it run. In the asset graph the first is a `declared` edge and the
second `inferred`, because nobody typed the statement's target — it was parsed out of SQL.

Two kinds of model are never offered:

- **The model itself.** A model that reads the table it builds is an incremental pattern, not
  its own upstream, and the cycle check would refuse it one click after the dialog offered it.
  It is removed *before* ambiguity is counted, so its own target cannot make a reference look
  uncertain.
- **Another user's private model.** The visibility predicate is the saved-query list's own;
  naming a private model as a producer would disclose it.

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
   A model's `target_table` needs no such care: it is already a name in the warehouse.
2. It is scoped to the model's own connection and workspace, for both kinds. A pipeline
   landing `analytics.orders` into a different destination — or a model building it on a
   different connection — produces a different table that merely shares a name.

The response reports `references`, `unresolved`, `candidates` and `ambiguous`, so the dialog
can say "3 of 4 inputs have a known producer" rather than silently showing what it happened
to find. Each candidate carries its `kind` (`pipeline` or `model`), `id` and `name`.

`ambiguous` means **one name in the SQL could be more than one table** — a bare `orders`
when both `analytics.orders` and `staging.orders` are produced. It does not mean one table
has several producers: a pipeline and a model that both write `analytics.orders` are fan-in,
and every one of them is a real upstream. The rule is the asset graph's, counted on distinct
tables rather than producers. When `ambiguous` is true the UI **must not pre-select
anything**; it never pre-selects in any case.

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
[`modelDialect`](../../api-gateway/internal/handlers/saved_query_models.go:178) has no case
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
| `GET` | `/api/v1/explorer/saved/:id/runs` | attempt history (086), newest first. `?limit=` 1–200 (default 50) · `?status=` `succeeded`/`failed`/`skipped` · `?before=<next_cursor>`; anything else is a 400 |
| `GET`·`POST`·`PUT`·`DELETE` | `/api/v1/explorer/saved/:id/schedule` | detaching leaves the table |
| `POST` | `/api/v1/explorer/saved/:id/schedule/pause` · `/resume` | |
| `GET` | `/api/v1/explorer/saved/:id/upstreams` | viewer — read-only suggestion |
| `PUT` | `/api/v1/explorer/saved/:id/freshness` | declare or withdraw a deadline (§12) |
| `GET` | `/api/v1/explorer/schedules` | viewer — all schedules in the workspace; `?saved_query_id=<uuid>` narrows it to one model (a non-uuid is a 400) |
| `GET` | `/api/v1/explorer/freshness` | viewer — the workspace's freshness misses (§12) |
| `GET` | `/api/v1/explorer/running` | viewer — what the refresh loops are doing right now (§13) |

An `after_upstream` schedule carries its producers in `upstreams`, a list of
`{kind, id}` with `kind` one of `pipeline` or `model`. It is required for that type,
forbidden for the two clock types, and bounded at `maxScheduleUpstreams = 16`
([:147](../../api-gateway/internal/handlers/saved_query_schedules.go:147)). An empty set, a
set over the cap, a duplicate within one request, and upstreams sent alongside a `cron` or
`interval` type are each refused by name
([`validateModelScheduleSpec`](../../api-gateway/internal/handlers/saved_query_schedules.go:2766));
a duplicate is refused rather than deduplicated, because collapsing it would hide that the
caller and the handler disagree about what the set is. Responses echo the set back with each
producer's display name joined server-side, so the dialog and the schedules page can name a
trigger without a second round trip.

**Internal (S2S)** — `POST /api/v1/internal/explorer/models/:id/run`
([main.go:1215](../../api-gateway/cmd/server/main.go:1215)) and
`POST /api/v1/internal/explorer/freshness/sweep`
([main.go:1222](../../api-gateway/cmd/server/main.go:1222)), behind
`InternalServiceMiddleware`. These are the routes Temporal calls; the user-facing run route
fail-closes without a session.

---

## 10. Operational notes

- **Run timeout: 30 minutes** (`modelRunTimeout`,
  [saved_query_models.go:71](../../api-gateway/internal/handlers/saved_query_models.go:71)).
  Event-driven runs get `timeout + 1 minute` **per run, not per batch**, so one slow model
  cannot eat the budget of the models queued behind it.
- **One rebuild at a time per model** — a Postgres advisory lock
  ([`acquireModelRunLock`](../../api-gateway/internal/handlers/saved_query_models.go:712)).
  The refresh workflow already serializes the event route's rebuilds of one model, so what
  the lock still buys is the other doors: a manual run, or a clock tick, overlapping one.
- **Auto-pause is distinguishable from an operator pause.**
  [`autoPauseModelSchedule`](../../api-gateway/internal/handlers/saved_query_schedules.go:2384)
  writes `auto_paused_*`, not `paused_*`, because the resume path treats them differently.
  A machine pause means "this cannot succeed as configured" — a dropped connection, a
  demoted run-as user.
- **The schedule paths are Temporal-optional.** An `after_upstream` schedule can be created,
  paused, resumed and deleted with no Temporal client at all, because nothing is registered
  with it. Firing degrades rather than breaking: with no client every rebuild runs on the
  in-process route, losing durability and coalescing but not the trigger.
- **Deploy the adapter before the gateway.** The fan-out is the one place where "no Temporal"
  and "Temporal that does not know this workflow" are different failures. With no client the
  gateway rebuilds in-process and the models still update. With a client but a worker that
  predates `UpstreamFanOutWorkflow`, the start call **succeeds** — so the gateway takes the
  Temporal route and skips the fallback — and the workflow task then fails on repeat against
  a queue where no worker can pick it up. Nothing rebuilds and nothing logs an error on the
  gateway side. Shipping `backend-temporal-adapter` first makes the window empty; shipping
  the gateway first makes it as long as the gap. Rolling *back* has the same shape in
  reverse, so roll the gateway back before the adapter.
- **Deleting an upstream cascades to the upstream row, not to the schedule.** Both foreign
  keys in `saved_query_schedule_upstreams` are `ON DELETE CASCADE`, for the reason 095 gave
  for the column they replace: `SET NULL` would leave a row that names nothing, and
  `RESTRICT` would make deleting a pipeline fail with a constraint error naming a saved query
  the operator has never heard of. What changed is the blast radius. Under 095 the cascade
  reached `saved_query_schedules` itself, so deleting the pipeline deleted the schedule; now
  it deletes one producer out of the set and the rest keep firing. **Delete the last one and
  the schedule stays `active` with an empty set** — nothing fires it, because the lookup
  matches by upstream, and nothing auto-pauses it either. The schedules page renders it as
  "After an upstream runs", so the model reads as scheduled and never rebuilds until someone
  edits the trigger.

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
| [explorer/schedules/page.tsx](../../frontend/src/app/%28dashboard%29/explorer/schedules/page.tsx) | workspace-wide schedules view, including the live **Now** column ([§13](#on-the-schedules-page)) |
| [explorer/schedules/[id]/page.tsx](../../frontend/src/app/%28dashboard%29/explorer/schedules/%5Bid%5D/page.tsx) | one model: every run, filtered and paged, beside its details and controls ([§13](#the-per-model-page)) |
| [scheduledModel.tsx](../../frontend/src/components/explorer/scheduledModel.tsx) | types and badges both schedule pages share, `describeCadence` (also used by the Model dialog), and `formatDuration` |
| [cronSentence.ts](../../frontend/src/components/explorer/cronSentence.ts) | pure: a cron in words, or null where no short sentence is exact ([§4](#cron-and-interval)) |
| [ModelDetailRows.tsx](../../frontend/src/components/explorer/ModelDetailRows.tsx) | the model page's *Run as* and *Lineage* values, each with its own request ([§13](#the-per-model-page)) |
| [modelLineage.ts](../../frontend/src/components/explorer/modelLineage.ts) | pure: the chain around one model, what each node may say, the layout ([§13](#the-graph-tab)) |
| [ModelLineageGraph.tsx](../../frontend/src/components/explorer/ModelLineageGraph.tsx) | the Graph tab: its requests, the canvas, the list, the notices |
| [getJson.ts](../../frontend/src/components/explorer/getJson.ts) | a GET whose deadline covers the body, for the Graph tab and its run panel |
| [modelRunGrid.ts](../../frontend/src/components/explorer/modelRunGrid.ts) | pure: which run of each drawn node belongs in each column of the run grid ([§13](#the-run-panel-and-the-run-grid)) |
| [ModelRunGridTable.tsx](../../frontend/src/components/explorer/ModelRunGridTable.tsx) | the run grid, drawn |
| [modelRunSql.ts](../../frontend/src/components/explorer/modelRunSql.ts) | pure: which SQL a run executed, from the model's edit history |
| [ModelRunPanel.tsx](../../frontend/src/components/explorer/ModelRunPanel.tsx) | the panel a node opens: now, recent runs, the run, its SQL |
| [liveState.ts](../../frontend/src/components/explorer/liveState.ts) | pure: what a row's Now cell may say, given `/explorer/running` and `/explorer/freshness` |
| [ModelLiveState.tsx](../../frontend/src/components/explorer/ModelLiveState.tsx) | the visible-only, non-overlapping poll; the Now cell, the Overdue badge, the expanded detail and the banner |

---

## 12. Freshness deadlines

Every trigger in §4 fires on the **presence** of an event: a clock tick, a pipeline
finishing, a model finishing. None of them can fire on an event that never arrives, and
that is the failure this subsystem is most exposed to. A paused schedule, an
`after_upstream` set someone emptied (§10), a rebuild that fails the same way every hour —
each leaves a model reading as configured while its table stops moving. Nothing in the
Explorer notices, because nothing happened.

A **durable timer** is the only construct that fires on an absence. This section is that
timer.

### What a deadline is

`saved_queries.freshness_deadline_seconds` is a nullable opt-in
([101_saved_query_freshness.sql](../../api-gateway/migrations/101_saved_query_freshness.sql)):
*this table should never be more than N seconds behind*. It sits on the **saved query**, not
on the schedule, for three reasons.

- A deadline is a promise about the **table**, and the table outlives the schedule. Detach
  the trigger and the table is still there, still read, and now certain to go stale.
- A model with no schedule at all can carry a deadline. That is the one case a
  schedule-attached deadline could never express, and it is the case most worth reporting.
- Widening a deadline and changing a trigger are different acts under different
  circumstances, and storing them together would make one look like the other.

`NULL` means no promise and the model is never reported. The range is `60 … 31536000`,
enforced by `saved_queries_freshness_deadline_range` in the migration and again in Go for
a 400 that names the range instead of a 500 that names a constraint. The floor equals the
sweep interval below: a deadline shorter than the gap between two checks would report on how
often we look, not on the data. The ceiling catches a caller who sent milliseconds — 6 hours
as `21600000` would silently never fire, which is wrong in the quiet direction.

### The reference point is the last **succeeded** run

`saved_queries.last_run_at` is the obvious column and the wrong one.
[`stampLastRunOutcome`](../../api-gateway/internal/handlers/saved_query_models.go:1133)
writes it on every outcome **including failures**, so a model whose rebuild fails hourly
would carry a reference point an hour old and read as fresh forever — precisely the model
this feature exists to find.

The reference point is `MAX(finished_at)` over `saved_query_runs` where `status =
'succeeded'`, falling back to `saved_queries.created_at`
([`modelFreshnessFactsQuery`](../../api-gateway/internal/handlers/saved_query_freshness.go:247)).
The fallback is what makes a model that has **never** succeeded go stale on schedule rather
than compare `NULL` against every deadline and become the one model in the workspace that
can never be reported.

### The sweep

A singleton Temporal workflow in the adapter
([`ModelFreshnessWorkflow`](../../backend-temporal-adapter/internal/workflows/model_freshness_workflow.go:92)),
started at boot under a fixed workflow id with `USE_EXISTING`
([`startModelFreshnessSweep`](../../backend-temporal-adapter/cmd/adapter/main.go:404)), so
restarts and multiple adapter replicas converge on one monitor rather than N.

It **sleeps first**, then sweeps. On the first run the adapter has just booted, which
usually means the gateway is still binding its port beside it; sweeping immediately would
spend all three activity retries on connection refusals before the first interval elapsed.
Default interval 60s (`ModelFreshnessSweepInterval`,
[:50](../../backend-temporal-adapter/internal/workflows/model_freshness_workflow.go:50)),
overridable with `MODEL_FRESHNESS_SWEEP_SECONDS`. The variable is read in `main.go` and
passed **as a workflow argument** — reading the environment inside workflow code would be
non-deterministic — and carried across continue-as-new, which happens every 500 ticks to
bound history.

An activity failure is **logged, not returned**. The sweep is the thing that notices when
nothing is happening, so it has to be the last thing to stop; a gateway that is down for
ten minutes costs ten minutes of detection latency and nothing else, because every tick
re-derives its answer from current state. Nothing is queued and nothing is caught up.

The activity is one S2S call to
`POST /api/v1/internal/explorer/freshness/sweep`
([`SweepModelFreshnessInternal`](../../api-gateway/internal/handlers/saved_query_freshness.go:587)),
which loads facts, evaluates them, applies the decisions, and returns the counts. The
evaluator
([`evaluateModelFreshness`](../../api-gateway/internal/handlers/saved_query_freshness.go:147))
is pure — facts in, decisions out, no clock and no database — which is why its tests run in
the **default** `go test ./...` suite rather than behind the `integration_pg` tag CI never
passes.

### What a breach records, and why it is a row

`saved_query_freshness_breaches` holds one row per miss. A read-time computation would be
simpler and would lose the only thing worth having: a model that goes stale for six hours
every night and recovers by morning is invisible to any check made on read, and is a column
of closed rows here.

Two staleness numbers are returned, deliberately.  `stale_seconds` is how overdue the table
was **when the breach was detected** and never changes; `stale_seconds_now` is computed as
the response is written. One number for both is how a breach that opened an hour ago and one
that opened last week become indistinguishable.

Each breach carries a **cause**
([`classifyFreshnessCause`](../../api-gateway/internal/handlers/saved_query_freshness.go:214)),
because "stale" alone does not tell an operator what to do:

| Cause | What it means |
|---|---|
| `no_schedule` | a deadline with no trigger — the promise was never wired to anything |
| `schedule_auto_paused` | the machine paused it: a dropped connection, a demoted run-as user (§10) |
| `schedule_paused` | a person paused it and the deadline outlived their intent |
| `no_upstreams` | an `after_upstream` schedule whose producer set is empty — active, fires never (§10) |
| `overdue` | the trigger is wired and live; the rebuild is simply not landing |

The order is load-bearing. `autoPauseModelSchedule` writes `status = 'paused'` **and**
`auto_paused_at` together, so testing the human pause first would file every machine pause
as a human decision and send the operator to look for a person who never touched it.

Resolutions are equally separated. `rebuilt` means the reference point moved forward;
`deadline_widened` means it did not and the deadline did — someone moved the goalposts, and
that is not the same event as a fix; `no_longer_tracked` means the deadline was cleared or
the model stopped being a table, and the promise was withdrawn rather than met.

### Two writers would be one writer too many

The sweep is the only thing that opens or closes a breach. `PUT …/freshness` deliberately
leaves an open breach alone; the next sweep re-derives it. Deduplication lives in the
database, not the application — a read-then-write would race two adapters into two open
rows for one model, after which the partial unique index could never be created at all.

`idx_sq_freshness_one_open_per_query` is `UNIQUE … WHERE resolved_at IS NULL`: at most one
open breach per model, any number of closed ones. The insert names that exact conflict target
rather than a bare `ON CONFLICT DO NOTHING`
([`openFreshnessBreachStmt`](../../api-gateway/internal/handlers/saved_query_freshness.go:326)),
so a foreign-key or CHECK violation still surfaces instead of being swallowed as a duplicate.
Decisions are applied one at a time rather than in one transaction: a single bad row costs
its own write and is counted, and the next sweep tries again.

### It reports; it does not rebuild

A breach never triggers a rebuild. This is the call Airflow and Dagster both converged on
from opposite directions — Airflow replaced SLAs in 3.1 after the defect that they were only
evaluated when a run *finished*, so a run that never completed was never flagged, and Dagster
shipped `FreshnessPolicy` as an **observation** primitive while deprecating the coupling that
let freshness drive auto-rematerialization.

The reason is the same reason the cause column exists. Four of the five causes are
configuration problems, and rebuilding on a breach would paper over them: the auto-paused
schedule would keep rebuilding under the credentials that made the machine pause it, and the
`after_upstream` schedule with an empty producer set would look like it was working. The
fifth, `overdue`, is a rebuild that is already failing — running it again on a timer is the
retry loop the run history already has. Reporting a breach leaves the decision where the
information is.

---

## 13. What is running right now

Every other surface in this subsystem reports something that already finished.
`saved_query_runs` cannot hold an unfinished rebuild: its status `CHECK` admits only
`succeeded`, `failed` and `skipped`, `finished_at` is `NOT NULL`, and the row is written
once at the end with `started_at` and `finished_at` together. That is the right shape for
an attempt history and the wrong shape for an incident, because the three questions asked
during one — is this model rebuilding, what is queued behind it, which upstream woke it —
are all about a rebuild that has not finished.

Those three facts exist. They live in the refresh loop's own memory, in
`ModelRefreshWorkflow`, and until now nothing could read them.

### Queries, not search attributes

The obvious mechanism is a Temporal **search attribute**: upsert `ModelPhase` and
`SavedQueryID` onto each run and list them. It cannot work on any surface this repo ships,
and the way it fails is worth recording, because it fails green.

This deployment runs Temporal with `DB=postgresql`
([docker-compose.yml:346](../../docker-compose.yml:346),
[docker-compose.prod.yml:177](../../docker-compose.prod.yml:177),
[docker-compose.quickstart.yml:546](../../docker-compose.quickstart.yml:546),
[helm infra/temporal.yaml:90](../../deploy/helm/rsync-ai/templates/infra/temporal.yaml:90)), which selects
**standard visibility**. Its `executions_visibility` table has twelve columns and no
`search_attributes` column at all — custom attributes on SQL visibility need
`DB=postgres12`, which is **advanced** visibility and requires the `btree_gin` extension.
That extension is blocked by default on Azure Flexible Server, Cloud SQL and RDS, which is
the whole of where this product is meant to run.

The trap is that none of that is reported as an error. Registering a custom attribute on
standard visibility exits 0, prints "Search attributes have been added", and the attribute
then appears in `temporal operator search-attribute list`. Every filter naming it fails.
Verified on the running local stack: a filter on a **registered** `CustomKeywordField` was
refused with "not supported for standard visibility", while a control filter on a built-in
attribute returned the live `ModelFreshnessWorkflow` row. A registration guard would have
been vacuous — it would have asserted the half that works.

So the state is exposed with `workflow.SetQueryHandler` instead, which is a better fit for
a second reason: **a query emits no command and writes no history event**. That is what
makes it safe to add a handler to a workflow that is already running. The contrast is
`workflow.UpsertTypedSearchAttributes`, which *is* a command — adding one to a running
`ModelRefreshWorkflow` would fail replay with `[TMPRL1100]`, and the default
`WorkflowPanicPolicy` is `BlockWorkflow`, so that failure has no floor: the task fails, is
ignored on later attempts, times out, and the server retries it forever with the run still
`RUNNING`. No dead-letter queue, no failure callback, nothing alerts. A model would simply
stop refreshing. [`explorer_workflow_queries_replay_test.go`](../../backend-temporal-adapter/internal/workflows/explorer_workflow_queries_replay_test.go)
replays the real pre-change history of the running singleton against the new code to hold
that property.

Handler placement is load-bearing: each one is registered at the **top** of its workflow,
before any blocking call, or a query arriving during the first `Await` finds no handler.

### The three handlers

| Query | Workflow | Answers |
|---|---|---|
| `model_refresh_state` | [`ModelRefreshWorkflow`:147](../../backend-temporal-adapter/internal/workflows/model_refresh_workflow.go:147) | phase, the completion being rebuilt now, how many are queued behind it, refreshes used against this run's budget |
| `freshness_sweep_state` | [`ModelFreshnessWorkflow`:112](../../backend-temporal-adapter/internal/workflows/model_freshness_workflow.go:112) | effective interval, ticks used, whether the last sweep succeeded and what it found |
| `fanout_state` | [`UpstreamFanOutWorkflow`:144](../../backend-temporal-adapter/internal/workflows/upstream_fanout_workflow.go:144) | the completion being fanned out, how many children are dispatched of how many targets |

`QueuedCompletions` is `ch.Len() + len(pending)` rather than the channel length alone,
because a completion the loop has taken off the channel but not yet rebuilt is
structurally invisible from the channel — and a rebuild that is three deep in a burst is
exactly the situation somebody is asking about.

After `continue-as-new` the in-memory counters reset, which is why the sweep's fields are
named `TicksThisRun` and `SweptThisRun`: they are honest about their scope rather than
implying a lifetime total the workflow does not have.

### The route, and why it takes no id

`GET /api/v1/explorer/running` ([main.go:1194](../../api-gateway/cmd/server/main.go:1194))
is workspace-scoped, viewer-gated and read-only. It takes **no path parameter**, and that
is the security property rather than an omission.

A workflow id is not a workspace-scoped object. Temporal knows nothing about this
product's tenancy, so `GET /explorer/saved/:id/running` would answer perfectly well for
`model-refresh:<somebody else's model>` — an IDOR with extra steps. This route reads no id
from the request at all. Every id it can query is derived by `modelRefreshWorkflowID` from
a row the caller's own workspace and visibility predicate already returned, so there is
nothing to validate because there is nothing to supply.
`ListWorkflowExecutions` is never called; it would answer across the whole namespace and
make the filtering this handler's problem, which is the arrangement the previous paragraph
exists to avoid.

The same property keeps the freshness singleton out of reach: its id is the bare constant
`model-freshness-sweep`, which is not in the image of `modelRefreshWorkflowID` for **any**
input, because every derived id carries the `model-refresh:` prefix. The singleton is
cross-workspace by nature and is reported on the admin health route instead
([`checkFreshnessSweep`:453](../../api-gateway/internal/handlers/saved_query_explorer_running.go:453)),
behind the admin gate, where it belongs. It earns a row there because it fails the same
way the infrastructure checks beside it do and nothing else reports it — the local stack's
sweep had been failing every tick for hours with `INTERNAL_SERVICE_SECRET` unset, and no
surface said so.

Only `after_upstream` schedules are listed. A clock schedule never produces a
`ModelRefreshWorkflow`, so it would report idle by construction, and a column that is
always idle teaches an operator to ignore the column.

### Four answers, because there are four different facts

| State | Means | What to do |
|---|---|---|
| `idle` | no open refresh loop — either none ever started (`NotFound`) or the last one closed (query rejected by `QUERY_REJECT_CONDITION_NOT_OPEN`) | nothing; this is the common case |
| `running` | a loop is open; `detail_available` says whether it answered | if detail is missing, the worker predates the handler — finish the deploy |
| `unreachable` | the question could not be asked | look at Temporal |
| `unknown` | nothing was asked, because this gateway has no Temporal client | look at this gateway |

Collapsing any pair of these into "not running" is the failure the whole route exists to
prevent, and it is invisible when it happens: the response still renders and every row
still has a state. `idle` is the reassuring answer, so the two faults that most want to
decay into it — an unreachable Temporal and an absent client — are the two the tests pin
hardest.

The rejection matters more than it looks. Temporal will answer a query against a **closed**
run by replaying its history, returning real but stale state, so without
`QUERY_REJECT_CONDITION_NOT_OPEN` a model that finished rebuilding an hour ago renders as
running with an hour-old batch underneath it. Errors are classified by **type** through
`errors.As`, never by message: the SDK wraps service errors on the way out, and a string
match would pass a unit test and then report a workspace of idle models as an outage.

⚠️ **The cost is one query per model per ask**, and those queries land on
`pipeline-workflows` — the same task queue that carries real rebuilds. The fan-out is
bounded at eight concurrent and fifty models per ask, with a two-second per-query timeout,
but nothing in the backend test suite can detect the load itself. There is no server-side
cache. The route's one caller polls it, under three limits that exist for this cost and
nothing else: every 15 s, **only while the tab is visible**, only when at least one row on
the page is triggered by an upstream, and **never while its previous ask is still out** —
the next ask is scheduled from when the last one landed, so a slow answer (about 14 s at
the cap, worst case) stretches the interval rather than stacking a second fan-out behind
the first. Refresh obeys the same limit: pressed while an ask is out, it waits on that ask
instead of starting another, and a timer tick that meets a Refresh's ask waits on it too,
timing the next ask from when it lands. A tab shown again while a Refresh's ask is out
still resumes polling after it.

### On the schedules page

`/explorer/schedules` has a **Now** column. It is read-only; nothing on it starts, stops or
signals a workflow.

| The server said | The row reads |
|---|---|
| a clock schedule (`cron`, `interval`) | `—` — never asked, so never `Idle`, even if an answer names it |
| `running`, phase `rebuilding` | **Rebuilding**, with the queued count when there is one |
| `running`, phase `waiting` | **Waiting…** |
| `running` with no detail, or a phase this page does not know | **Running · no detail** |
| `idle` | **Idle** |
| `unreachable` / `unknown` / a state this page does not know | **Unreachable** / **Unknown** |
| `temporal_available: false`, a failed or unreadable check | **Unavailable**, plus a banner |
| a model the answer left out | **Not checked** — the banner names the 50-model cap when it was hit |
| the last answer is older than one poll (a tab shown again), while the page asks again | **Checking…**, with that answer's age in the tooltip and the expanded row |
| `detail.last_error` set, beside Rebuilding or Waiting | a red **Rebuild failed this run** — the loop keeps it for the rest of the run, so it says a rebuild failed, not that one is failing now |

The rule the table encodes is the route's own: `idle` is the reassuring answer, so this
page shows it **only** when the server asked a refresh loop and was told there was none.
`temporal_available` is checked **before** any row, because a body with it `false` still
carries rows. A body missing it is treated as unreadable rather than defaulted, and a
failed check **replaces** the previous answer instead of leaving it on screen — a
fifteen-second-old `Rebuilding` shown beside an error is the same lie as a false `Idle`.
For the same reason an answer is set aside once it is older than one poll plus five seconds
of slack, which only happens when the tab was hidden: a tab shown again after ten minutes
reads **Checking…** until the new answer lands (up to about 14 s at the cap), rather than
showing a ten-minute-old `Rebuilding` as current. A tab away for a few seconds keeps its
rows. A failed check is not set aside, because **Unavailable** claims nothing. The Overdue
badges are not set aside either: a freshness check that is out shows no badge, which reads
as on time, so withdrawing them would be the calmer claim, not the safer one.

Rows also carry an **Overdue** badge from `GET /explorer/freshness` ([§12](#12-freshness-deadlines)):
open breaches only, by `stale_seconds_now` where the server sent one. That route reads
Postgres, not Temporal, so it has its own 60 s poll under the same visibility and overlap
limits. A breach for a model with no row on this page is named in the banner rather than
dropped, and a failed freshness check says so, because a row without a badge is otherwise
indistinguishable from one that is on time. Expanding a row shows, above its run history,
when the rebuild started and which upstream woke it (by name, from the schedule's own
upstreams), the queue and per-run counters, the loop's last failure this run, and the
breach's deadline, reference point and cause.

The freshness sweep's admin health row renders its `detail` as key/value pairs and gives
`unknown` — nothing was asked — a hollow dot of its own instead of the fallback grey. A
caveat on an `up` row renders grey, not red, since it qualifies a healthy answer.

Tests: [liveState.test.ts](../../frontend/src/components/explorer/__tests__/liveState.test.ts)
(the rules), [explorer-schedules-live-state.test.tsx](../../frontend/src/__tests__/explorer-schedules-live-state.test.tsx)
(what the page says and what it costs — cadence, hidden tab, overlap, unmount, an answer
from before the tab was hidden, and Refresh meeting the timer) and
[admin-health-detail.test.tsx](../../frontend/src/__tests__/admin-health-detail.test.tsx).

### The per-model page

Each name on `/explorer/schedules` links to `/explorer/schedules/<saved_query_id>`. The
row's inline history stays; it shows the last 50 runs and nothing older. The page is
for one model at a time and reaches all of its runs.

**Paging is keyset, not offset.** Runs keep landing while someone pages back, and an
offset would then repeat or skip rows. The route orders by `(finished_at, run_id)`,
asks for one row past the page to learn whether another exists, and returns
`next_cursor` only when one does. `run_id` breaks ties, because a fan-out burst can
finish several runs in the same microsecond. The page keeps a stack of the cursors it
has used, so **Newer** is a pop and never a second query shape. A filter change or a
Run now starts again from the newest run.

**The duration chart** sits above the table: one bar per run on the current page, oldest
on the left, green for succeeded, red for failed, amber for skipped
([RunDurationChart.tsx](../../frontend/src/components/explorer/RunDurationChart.tsx)). It
draws the same runs the table lists, so a filter or a page change redraws it without a
second request, and the two cannot show different runs. A model that has started taking
three times as long, or fails every other night, shows as a shape before anyone reads a
row. It covers the page, not the whole history: at most 25 bars, and **Older** moves the
window.

- Height is the run's duration over the longest run on the page that did work; the
  caption names that duration.
- A skip did no work, and its recorded duration is about 0 ms (-1 ms under clock jitter).
  It is drawn as a fixed amber stub and never sets the scale, so a streak of skips stays
  visible and cannot pass for a streak of very fast rebuilds.
- A run that did work keeps a 3% floor, so 5 ms next to two minutes is still a bar.
- A status this client does not know is drawn grey rather than dropped; a missing bar
  would read as a run that never happened.
- Pointing at a bar names the run under the chart (start time, status, duration); each
  bar carries the same sentence as its accessible name.
- A duration under a second reads in milliseconds, here and in the table's Duration
  column ([`formatDuration`](../../frontend/src/components/explorer/scheduledModel.tsx:119)).
  In tenths of a second, runs of 16 ms and 181 ms both read "0s" while their bars stood
  at 9% and 100%.

**What the page refuses to blur:**

- A failed request reads *Could not load run history (HTTP n)*, never *No runs recorded
  yet*.
- A skip shows `—` for duration, and its message is amber, not red; a skip is not a
  failure.
- A succeeded table rebuild reads *Table rebuilt* in Result, with the table's name on
  hover. Its statements are DDL, and only a DML write reports a row count
  (`reportsRowsAffected`), so the cell used to be empty, as if the run did nothing. A DML
  write keeps its *n rows*; a statement model has no target table and shows nothing.
- A query with no live schedule is not a dead end. Deleting a schedule keeps every run it
  made, so when the schedule lookup returns nothing the page asks for the query itself
  (`GET /explorer/saved/:id`,
  [page.tsx:299](../../frontend/src/app/%28dashboard%29/explorer/schedules/%5Bid%5D/page.tsx:299)).
  If that answers, the page shows the query's name, a *Not scheduled* notice and the full
  runs table, with filters and paging. Run now, Pause/Resume and Edit schedule are left
  off, and the Details card keeps what the query row itself knows (Last run, Does,
  Created, Updated) plus Lineage.
- A query the route answers 404 for (one that does not exist, or another member's
  private one — the route does not tell those apart, and neither does the page) or a bad
  id reads *This query could not be found*, and no runs are requested.

**Controls** are the ones the Model dialog already has, with the same gates:

- **Run now**, **Pause/Resume** and **Edit schedule** are disabled without the
  `schedule_query` permission (admin and above). **Edit query** follows `canEditSavedQuery`.
- **Run now** is also disabled when the engine cannot materialize, when a run would do
  nothing (`runsDoSomething`), or while this page's own Run now request is out. It does
  not know whether a scheduled run is already in progress.
- An automatic pause is explained by its reason and time above the table.

The details card reads the same live state as the list: `/explorer/running`, polled
for `after_upstream` models and while the Graph tab is open, and `/explorer/freshness`.
Each upstream model links to its own page.

**Trigger, Run as and Lineage** in the Details card:

- **Trigger** shows a cron as a sentence with the expression under it in monospace
  ([`TriggerCadence`](../../frontend/src/app/%28dashboard%29/explorer/schedules/%5Bid%5D/page.tsx:153)).
  A cron with no exact sentence is printed once, as typed ([§4](#cron-and-interval)).
- **Run as** names the member every run of this schedule is authorized as
  ([`RunAsValue`](../../frontend/src/components/explorer/ModelDetailRows.tsx:34)). It reads
  *you*, their email, *a former member* when the workspace roster no longer lists them, or
  *another member* when the roster could not be read.
  - `run_as_user_id` is whoever attached the schedule
    ([saved_query_schedules.go:596](../../api-gateway/internal/handlers/saved_query_schedules.go:596)).
    Editing the schedule does not change it.
  - Each run that the schedule or an upstream starts re-reads that member's role
    ([`authorizeModelRun`](../../api-gateway/internal/handlers/saved_query_models.go:436)).
    If they have left the workspace, are below admin, or are below what the model's SQL
    needs, the run is refused and the schedule pauses. *Run now* runs as whoever clicks
    it. The row's hover text says both.
  - The workspace schedule list leaves the id out, so the row reads
    `GET /explorer/saved/:id/schedule`, then `GET /workspaces/:id/members` only when the
    id is not yours.
  - The read waits until the signed-in user and the workspace have loaded. Started
    earlier, it was cancelled and sent again as soon as they arrived, once per page load,
    and Chrome's network log lists a cancelled request as a 503.
  - It is left off for a query with no schedule.
- **Lineage** reads *4 upstream · 5 downstream* and opens the Graph tab
  ([`LineageCountValue`](../../frontend/src/components/explorer/ModelDetailRows.tsx:132)).
  - The numbers count everything the model runs after, or that runs after it, directly or
    through another model. That includes nodes past the graph's 50-node cap.
  - They come from the same `GET /explorer/schedules` and `buildModelLineage` as the Graph
    tab's header, so the two cannot disagree. The cost is one more request for that list on
    each load and each Refresh.
  - When the list comes back at its 500-row limit, each number reads *at least*, because a
    schedule left out can only hide a link.
  - It is shown for a query with no schedule too, since other models can still run after
    it.

Each of these two rows loads on its own. A failure reads in its row, for example *Could not
load who runs this schedule (HTTP 500).*, and leaves the rest of the page alone.

**Does** names the table a table model rebuilds. The name wraps after its dot, never
inside a word ([`QualifiedName`](../../frontend/src/app/%28dashboard%29/explorer/schedules/%5Bid%5D/page.tsx:124)); the column is narrow, and
`public.swipes_total` used to break as `public.swipes_tota` / `l`.

Tests: [explorer-schedule-detail.test.tsx](../../frontend/src/__tests__/explorer-schedule-detail.test.tsx)
(the page) and [run_history_paging_test.go](../../api-gateway/internal/handlers/run_history_paging_test.go)
(the cursor, the filter, the 400s and the one-model lookup).

### The Graph tab

The page has two tabs, **Runs** (the default) and **Graph**. The tab is kept in the address
as `?tab=graph`, so a link can open on it and Back returns to it. The graph shows the chain
around this one model: the models and pipelines it runs after, the models that run after
it, and so on in both directions. Each node is coloured by its own status: usually its last
outcome, but a model the live check lists as rebuilding or running is blue, a CDC pipeline is
grey unless its last execution failed, and a node whose status is hidden or unavailable is
grey. It is not a workspace-wide map. A sibling that waits on the same upstream is not drawn, and neither is
another upstream of a downstream: the downstream says *+1 more upstream not shown* instead.

**Where it comes from.** The tab calls no graph route. It does not use the workspace-wide
`GET /explorer/asset-graph` ([main.go:1243](../../api-gateway/cmd/server/main.go:1243)) either.
The chain is built in the browser from `GET /explorer/schedules`, the same visibility-checked
list the schedules page reads
([`buildModelLineage`](../../frontend/src/components/explorer/modelLineage.ts:122)). It walks up
through each `after_upstream` schedule's upstreams and down through the schedules that name
this model. A pipeline is a leaf, because nothing wakes a pipeline. A ring ends where it meets
a node already drawn, and a node that is both upstream and downstream is drawn once. Then
each node is looked up on its own route, six at a time: `GET /explorer/saved/:id/runs?limit=25`
for a model, `GET /pipelines/:id` for a pipeline
([`lookupNode`](../../frontend/src/components/explorer/ModelLineageGraph.tsx:82)). The result
is set once every lookup is in. A half-drawn graph would move under the reader, and it
would show names before the lookups that decide them.

Each request has a deadline, 10 seconds for a lookup and 15 for the schedule list, and the
deadline covers reading the body, not only the headers
([`withDeadline`](../../frontend/src/components/explorer/getJson.ts:12)). The
timeout `authFetch` offers stops counting once the headers arrive, so a body that stalled
after them would have left the tab on *Loading the graph…* for good. The run panel's SQL
request uses the same helper. At the deadline the
download is aborted: a lookup reads *Status unavailable*, and the schedule list reads
*Could not reach the server to load the graph*, with Retry.

**What a node may say.** A schedule row's `upstreams` carries the upstream's name even
when the caller cannot open that upstream (see *Known gap, outside the tab* below). So a name on another
schedule's upstream list is never enough to print
([`describeLineageNode`](../../frontend/src/components/explorer/modelLineage.ts:518)):

| What vouched for the node | It reads |
|---|---|
| This model | its name, marked *This model*, not linked |
| Its own schedule row is in the caller's list | its name, linked, even if its runs failed to load |
| Its own route answered 2xx | its name, linked |
| Its own route answered 403 or 404 | *A model you can't open* or *A pipeline you can't open* · *Status hidden*, no link, no Retry |
| A pipeline's route answered 404 with `pipeline_not_found` | *A pipeline* · *Status unavailable*, no link, with Retry |
| Anything else (timeout, 401, 429, 5xx, a body it cannot read) | *A model* or *A pipeline* · *Status unavailable*, no link, with Retry |

The first two rows keep the name whatever the lookup says. When the runs could not be read,
such a model reads *Status unavailable*: with Retry after a failure, without it after a 403
or 404. Retry sits beside *Some statuses could not be loaded.* above the graph.

Pipelines follow the same rules, with one exception. The pipeline route's gate refuses with
`{"error":"not found"}`. After the gate, its own query answers 404 `pipeline_not_found` only
when the row is gone, a pipeline deleted after the gate let the caller in; a failed read there,
a dropped database connection for one, is a 500 `pipeline_fetch_failed`
([pipelines.go](../../api-gateway/internal/handlers/pipelines.go)). So that 404 says nothing about
access, and it is read as unknown ([`lookupFromResponse`](../../frontend/src/components/explorer/modelLineage.ts:300)).

A model link opens that model's page on its Graph tab. The canvas node ids are `n0`, `n1`
and so on, never a saved-query id.

**A model's status** is its last outcome, how long that run took, and its age, as in
*Succeeded in 97ms · 21h ago*. A skip did no work, so it gets no duration
([`runDurationMs`](../../frontend/src/components/explorer/modelLineage.ts:400)).
Two kinds of skipped run are not outcomes
([`modelRunStatus`](../../frontend/src/components/explorer/modelLineage.ts:422)), and the status looks past them to the newest run that is:

- *waiting for other upstreams* (`skip_reason = waiting_on_upstreams`), written when an
  upstream of an `all` model finishes while at least one other upstream has not finished
  since the model's last successful run. Completions coalesced into one rebuild request
  write one row, and the completion that leaves no upstream waiting rebuilds the model
  instead;
- *a run of this model is already in progress*, written with no `skip_reason` when another
  run held the model's lock. It is matched by those words, so if the words change such a
  row reads as a plain *Skipped* again.

When all 25 newest runs are such skips and the runs route says there are more, the lookup
asks once for up to 200 older runs, from the `next_cursor` it was given. If that page fails,
the 25 it has stand. When nothing in hand is an outcome, the node reads *Waiting for other
upstreams*, or *Skipped: a run was already in progress*, grey, with the newest skip's age. A
model with no runs reads *Never run*.

A model the live check lists as rebuilding, or running with no detail, reads *Rebuilding now*
or *Running* instead. Its new run row is written before the live state leaves that phase
(`model_refresh_workflow.go`: `RunModelActivity` records the run, then the phase returns to
waiting). So when a live poll stops listing a drawn model as running, the graph loads again,
once, and shows that run. If the live-state request fails, or is still loading, the graph
does not reload: that says nothing about whether the model stopped. But if an answer arrives
that could not tell what the model is doing, the model is no longer listed as running, so
the graph still reloads once. That happens when orchestration is unavailable, when the
model's refresh loop could not be asked (unreachable or unknown), or when the model is
missing from the answer.

The line under the status says how the model is scheduled: *Not scheduled*, *Paused* or
*Paused automatically*, *Waiting on upstreams* from the live state, *Waiting for other
upstreams*, or *Waits on all N* for an `all` policy with more than one upstream. An active
model that fits none of these, such as an `all` model with a single upstream, gets no schedule
line. When some of its upstreams are not
drawn, that follows: *Paused · +1 more upstream not shown*. A card cuts the line short, so its
whole text is also the tooltip.

**Limits it says out loud.**

- At most 50 nodes, this model included. Nearer nodes come first. At the same distance,
  upstreams and downstreams take turns, so 60 direct upstreams cannot push out the one
  direct downstream. A larger chain reads *Showing 49 of N linked models and pipelines,
  nearest first. K more are not drawn.* The list's headings then say *Upstream (48 of 60)*.
- The schedule list stops at 500 rows (`LIMIT 500`, newest-updated first). At 500 the graph
  says links from the rest may be missing. An unlinked model then reads *As far as this page
  can see, …*. Any other model with no row whose runs loaded reads *Schedule not loaded
  (500+ schedules)* instead of *Not scheduled*. The model the page is about still reads
  *Not scheduled*, because the page loaded its schedule by id.
- If the schedule list comes back with an HTTP error, the graph reads *Could not load the
  models linked to this one (HTTP n)*. A 2xx response it can't read reads *Could not read
  the models linked to this one.* A network error, or no answer within 15 s, reads *Could
  not reach the server to load the graph.* Each shows Retry, and none reads as a model with
  no links.

**An unlinked model** gets a sentence that fits how it runs. For no schedule: *This query
has no schedule*. For a clock schedule: *No model or pipeline feeds this model*. For
`after_upstream` with nothing chosen: *No model or pipeline is chosen to trigger this model*.
Each goes on *and no model you can see waits on it*, and the clock-schedule text then adds
*It runs on its own schedule.* The clause is there because another member's private model
can wait on this one, and the schedule list leaves that model out. A second line says how to
link it. Someone without `schedule_query` is told that an admin can.

**Canvas and list.** On a screen at least 640 px wide with a fine pointer, the chain is
drawn left to right with `@xyflow/react` and dagre
([`LineageCanvas`](../../frontend/src/components/explorer/ModelLineageGraph.tsx:545)). It uses a
Graph/List toggle and Zoom in, Zoom out and Fit buttons. A dashed edge points into a model
whose own schedule is not active (paused), so that upstream finishing won't trigger it. The
canvas is `aria-hidden`, its nodes cannot be dragged (dragging the canvas pans the view), and
nothing in it takes keyboard focus. The Zoom in, Zoom out and Fit buttons sit outside the
hidden canvas, so they can be focused. The list ([`LineageList`](../../frontend/src/components/explorer/ModelLineageGraph.tsx:717))
has the same nodes with links, under *Upstream (n)*, *This model* and *Downstream (n)*, each
with its distance (*Upstream, 2 hops away*). A model whose own schedule is not active also
reads *won't trigger (paused)* when a drawn edge points into it. The text goes on the paused
model itself, not on the upstream that would have woken it. The dashed edge into that model
says the same thing on the canvas. A
narrow or touch screen gets only the list. The initial viewport fits the whole chain when
that keeps nodes at 75% or larger. Otherwise it centres on this model at full size.

For a screen reader, a polite status region says *Loading the graph.*, then *Graph loaded:
2 upstream, 1 downstream.*, adding *Some statuses could not be loaded.* when that is so. An
error is an alert. Retry moves focus to the tab's *Graph* heading. The error's Retry is
replaced by the loading spinner at once, so focus would otherwise fall back to the page. The
Retry beside *Some statuses could not be loaded.* stays on screen, dimmed, until the reload
finishes.

The page's **Refresh** reloads the graph, and the previous graph stays on screen, dimmed,
until the new one is in.

Tests: [modelLineage.test.ts](../../frontend/src/components/explorer/__tests__/modelLineage.test.ts)
(the walk, the cap and its turn-taking, the lookups, the pipeline 404, older-run paging,
both kinds of skip, every placeholder, the live keys, the layout and the viewport) and
[model-lineage-graph.test.tsx](../../frontend/src/__tests__/model-lineage-graph.test.tsx)
(names only after the node's own route answers, the canvas hidden from assistive tech, the
list, the notices, Retry and focus, the status region, six lookups at a time, a body that
stalls, the older page, and the reload when a rebuild ends).

**Known gap, outside the tab.** `upstreamRowsQuery`
([saved_query_schedules.go](../../api-gateway/internal/handlers/saved_query_schedules.go)) attaches
upstream names without the visibility predicate. A shared model that waits on another
member's private model therefore carries that model's name and id in its schedule row.
The Graph tab prints that name only once the model's own runs route answers 2xx, which a
caller who cannot open it never gets, but the page's Details card and the API response still
show it.

### The run panel and the run grid

**What opens the panel.** Clicking a card on the canvas, a *Runs and SQL* button in the list,
or a square in the run grid opens a side panel for that node. Only this model and a node
with a link can be opened: a node whose own route answered 2xx
([`isClickable`](../../frontend/src/components/explorer/ModelLineageGraph.tsx:176)). A placeholder (*A model you can't open*,
or a node whose status is unavailable) opens nothing. It has no button in the list, and its
squares in the grid are pictures, not buttons. The canvas stays `aria-hidden`, so the list's
buttons and the grid's buttons are how a keyboard or a screen reader gets to the panel.

**The panel** ([`ModelRunPanel`](../../frontend/src/components/explorer/ModelRunPanel.tsx:83)) is a dialog. Escape, the
backdrop or Close shuts it, and focus goes back to what opened it. The header gives the
node's kind and where it sits (*Direct upstream*, *Upstream, 2 hops away*, *This model*),
its name, and *Open model* (that model's page, on its Graph tab) or *Open pipeline*. This
model gets no link. A model's panel has four parts:

- *Now*: for an `after_upstream` model, its live state from `/explorer/running`, the same
  text as on the schedules page, with *Show its SQL* while it rebuilds. A model on a clock
  schedule, or with none, is not asked, and the panel says so.
- *Recent runs*: the first 25 runs the graph's lookup loaded, newest first, each with its
  status, how long it took and its age. When a grid square picked an older run, that run is
  added at the end. The panel opens on the run clicked, else on the rebuild in flight, else
  on the newest run.
- *This run*: status, start, how long it took, trigger (manual, schedule, after a model,
  after a pipeline), skip reason, rows affected, and the run's error.
- The SQL, below.

A pipeline's panel shows its last execution (status, started, finished, how long, error)
and says a pipeline has no SQL of its own here.

**Which SQL a run executed.** A run row stores no copy of the SQL. The run reads the
model's SQL just before it executes (`runSavedQueryModel`,
[saved_query_models.go:821](../../api-gateway/internal/handlers/saved_query_models.go:821)).
So the panel asks `GET /explorer/saved/:id/versions` once
([saved_queries.go:922](../../api-gateway/internal/handlers/saved_queries.go:922)). That route
checks access again and returns the text as it was before each edit, newest first, at most
100 versions. From that ([`sqlForRun`](../../frontend/src/components/explorer/modelRunSql.ts:34)):

| What the history says | The panel shows |
|---|---|
| No edit since the run started | *SQL when this run started*: the current text |
| Edited since | *SQL when this run started*: the text before the first edit after the start, with *The model has been edited since.* and *Show current SQL* |
| Edited since, but the versions kept no longer reach back to the run | *Current SQL*, saying it may not be what ran |
| An edit saved within a minute of the start | the text as above, plus a note that the run may have read the text from either side of that edit |
| The rebuild running now | the text as of the start time the refresh loop reported; with no start time, *Current SQL*, saying so |

*Copy* puts the text shown on the clipboard. A 403 or 404 reads *You can't open this
model's SQL.*, with no Copy. Any other HTTP error, a body it can't read, a network error, or
no whole answer within 10 seconds is an alert with Retry. The deadline covers the body
([`getJson`](../../frontend/src/components/explorer/getJson.ts:39)).

**The run grid** ([`buildRunGrid`](../../frontend/src/components/explorer/modelRunGrid.ts:158), drawn by
[`ModelRunGridTable`](../../frontend/src/components/explorer/ModelRunGridTable.tsx:77)) sits under the graph, headed *Runs
across the chain*. Each row is a drawn node: upstreams farthest first, then this model, then
downstreams nearest first. Each column is one of this model's newest runs, at most 15, oldest
on the left. A bar above each column shows how long that run took, on the same scale as the
Runs tab's chart. A fan-in model writes a *waiting for other upstreams* row each time an
upstream lands, then its rebuild row. The rebuild and the waiting rows before it are one
column ([`runRounds`](../../frontend/src/components/explorer/modelRunGrid.ts:76)). A column still waiting has an outlined
square. When the live check lists a drawn model as running, a *Now* column is added on the
right.

Squares are lined up only by what the run history records, never by time: two runs that
merely overlap are not linked. A run woken by a model stores that model's run id
(`upstream_run_id`, [migration 104](../../api-gateway/migrations/104_saved_query_run_provenance.sql)).
So an upstream's square is the run that a row of the column names, walking up one hop at a
time. A downstream's square is its newest run that names this model's run for the column;
when other rebuilds of it name the same run, its label says how many. A run square has a
colour and a shape (a tick, a cross or a dash), and its label gives the status, how long it
took, and when it started. The other squares:

| Square | Means |
|---|---|
| *Woke this run; no run of its own is shown* | a pipeline woke it, or an upstream is named without a run id (a row from before migration 104) or with a run that is no longer recorded |
| *No run linked to this one* | nothing in the history links a run of this node to the column (a failed run wakes nothing) |
| *Older than the runs loaded* | the link may be in runs older than the ones the lookup loaded |
| *Runs hidden: you can't open it* | the node's own route answered 403 or 404 |
| *Runs could not be loaded* | the node's own route failed |
| *Not known: linked only through a model whose runs are hidden or not loaded* | the only way to the column goes through such a node |

A run square is a button when its row can be opened, and it opens the panel on that run. The
running square in the *Now* column opens the panel on the rebuild in flight. The legend lists
only the kinds the grid shows. With more than 15 runs, or more on the route, the grid adds
*Older runs are on the Runs tab.*

**Gaps the grid shows as blanks** (backend follow-ups, not built):

- A rebuild row names one upstream run. When a fan-in model's rebuild absorbed several
  upstream completions (`coalesced_count`), the others are named by no row. Their squares
  read *No run linked to this one*, even though those upstreams ran.
- The grid reaches back only as far as each node's lookup: 25 runs, plus up to 200 more when
  all 25 are rows that are not outcomes. No route returns older runs for a whole chain.

Tests: [modelRunGrid.test.ts](../../frontend/src/components/explorer/__tests__/modelRunGrid.test.ts) (rounds, linking by run id
and never by time, a fan-in upstream that was never recorded, a round still waiting, hidden
and unknown nodes, links older than the runs loaded, a pipeline, several rebuilds from one
run, the *Now* column, the column cap),
[modelRunSql.test.ts](../../frontend/src/components/explorer/__tests__/modelRunSql.test.ts) (the text at a run's start, an edit
near the start, pruned history, no start time, the route's body) and
[model-lineage-graph.test.tsx](../../frontend/src/__tests__/model-lineage-graph.test.tsx)
(durations on the cards, a card, the list button and a grid square each opening the panel,
the SQL at the run with Show current SQL, a 403, a 500 with Retry, a body that stalls, a
pipeline's panel, focus returning on close, and a placeholder that opens nothing).

---

## 14. Deliberately not built

Recording these so they are not re-litigated as oversights:

- **`incremental` materialization** — needs a merge key and watermark (085).
- **A stored dependency graph** — the *suggested* upstream is recomputed per request by
  design (§7). A chosen upstream is stored, and only what a person picked.
- **An LLM fallback for table extraction** — the deterministic parser was kept after
  measurement.
- **Two triggers on one model** — one live schedule per query. Fan-in is expressed as one
  schedule with many producers instead (100). Not for the race 095 claimed — see §2.
- **A fan-in barrier** — "rebuild once all three producers have landed" needs a definition of
  when a batch is over. That is a freshness deadline, and it is not this trigger's job (§4,
  and the deadline itself is §12).
- **Rebuilding a model because it went stale** — the sweep reports, it never triggers (§12).
- **Second-person approval** — a workspace-policy question, not a hard-coded rule (§5).
- **Document (MongoDB) browsing** — shipped as the Explorer's document browse mode ([document-browse-mode-plan.md](document-browse-mode-plan.md)); saving a document query is still a follow-up there.

Open Data Explorer items — `DX-LimitDowngrade`, `DX-SqlGenResilience`,
`DX-CornerCaseCoverage` — are tracked separately and are **not** part of
this subsystem.
