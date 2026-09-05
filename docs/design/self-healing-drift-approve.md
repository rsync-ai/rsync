# Self-Healing Schema Drift — Implementation Plan (drift-detect → diff → human-approve)

**Status:** Build doc / design proposal. NOT YET IMPLEMENTED. Every step cites `file:line` against the current tree.
**Provenance:** authored from a code-grounded extraction pass, then revised against an adversarial design review. The 8 review findings (2 critical, 3 major, 3 minor) are folded in below and marked `[RT-fix]` where they changed the plan.
**Author intent:** ship the proactive schema-drift loop by *activating machinery that already exists* — the dormant `healer.Agent`, the live `schema_change_approvals` store + approval API, and the batch `DiscoverSchema` seam. Exactly **one** genuinely new subsystem (the detector + baseline store). Everything else is wiring.

---

## 1. Summary & non-goals

rsync detects schema drift between a pipeline's source and its destination, renders a human-readable **diff**, and routes it through an **approve / reject** decision before any DDL touches the destination. We ship **propose → approve**.

**`[RT-fix #4]` For the shipped phases (P0–P3), *all* drift classes — including additive `add_column` / `create_table` — surface as records/proposals; nothing auto-mutates the destination as a result of detection.** Additive auto-apply is a *separate, later, product-signed-off* phase, not on this critical path. (The sink already applies `add_column` silently today via `ensure_table` (`kafka-sink-worker/main.go:6476-6568`); detection makes that *auditable*, but the detection→auto-DDL path stays gated and off by default.) Every destructive class — `drop_column`, `drop_table`, `modify_column` (retype), `rename`, connector-regen, CDC re-provision — is **approval-only**, with the existing `applyMigration` DROP/TRUNCATE substring guard (`healer.go:869-874`) kept as a non-negotiable backstop even on the post-approval apply path.

**Non-goals (this build):**
- **No autonomous destructive DDL. Ever.**
- **No additive auto-apply on the critical path** `[RT-fix #4]` — deferred to a separately-approved phase; `RSYNC_SCHEMA_DRIFT_AUTOAPPLY` stays off and out of P0–P3.
- **No CDC real-time drift in P0–P3.** Debezium schema-history (`include.schema.changes=true` → `schemahistory.<name>`, `debezium/versions/v1.0.0/connector.py:250-254`) currently has **no consumer**; CDC drift is **P4**. The batch detector (P1) does **not** cover CDC.
- **No Slack/email delivery.** Notifier producers exist but **no consumer subscribes** (`healer.go:33-41`). User-visible surface is the in-app `/schema-changes` poll + deep-link only.
- **No DDL apply for object-storage / warehouse / blob dests.** `applyMigration` supports **only** `postgres` + `mysql` (`healer.go:904-910`). Non-RDBMS dests can only ever go `pending_approval`; the UI must not present "Approve" as auto-appliable for them.
- **No confidence-band recalibration / eval loop.** AutoBand 0.85 / HITLBand 0.50 (`heal/heal.go:33-166`) shipped as-is, flagged uncalibrated (§8).

---

## 2. Architecture decision: ONE spine

Three distinct "healer" types exist. We commit to exactly one for schema drift and keep the other two in their lanes. **We do NOT fork a 4th.**

| Type | Package / file | Responsibility | Touched? |
|---|---|---|---|
| **`healer.Agent`** | `backend-orchestrator/internal/agents/healer/healer.go:116` | Schema-change consume → analysis → `schema_change_approvals` write → apply on approve. **DORMANT** (`.Start()` has zero callers). | **YES — the spine.** |
| `sentinel.Healer` | `internal/agents/sentinel/healer.go:28`, started `sentinel.go:117` | CDC Kafka-Connect task watchdog. Different struct/topics/job. | **NO.** |
| `heal.HealWorker` | `internal/agents/heal/worker.go:70`, wired `cmd/orchestrator/main.go:743` | Reactive execution-failure healing (AutoBand/HITLBand). | **NO** `[RT-fix #2]` (CDC-hook fix removed from this build — see below). |

**The spine = `healer.Agent` + `schema_change_approvals` table + the live approval API.** The store (`api-gateway/migrations/051_schema_evolution.sql`), the owner-gated GET/approve/reject handlers (`schema_evolution.go:40-192`), the routes (`cmd/server/main.go:443,669-671`), and the approve→`ApprovedChangeTopic`→`handleApprovedChange`→`applyMigration` round-trip **already exist and are wired**. We activate a built-but-dormant path; we do not build approval plumbing.

---

## 3. The event contract

### 3.1 Wire struct (reuse verbatim — `healer.go:68-91`)

```go
type SchemaChangeEvent struct {
    EventType    string                 `json:"event_type"`
    PipelineID   string                 `json:"pipeline_id"`
    Timestamp    string                 `json:"timestamp"`
    SchemaChange SchemaChange           `json:"schema_change"`
    Context      map[string]interface{} `json:"context"`       // auto_apply_enabled / skip_destructive_enabled
    ActionNeeded bool                   `json:"action_needed"`
}
type SchemaChange struct {
    ChangeType string `json:"change_type"` // EXACT: add_column|drop_column|modify_column|drop_table|create_table
    Table      string `json:"table"`
    Database   string `json:"database"`
    SchemaName string `json:"schema_name"`
    ColumnName string `json:"column_name"`
    ColumnType string `json:"column_type"`
    DDL        string `json:"ddl"`        // flows into applyMigration
    RiskLevel  string `json:"risk_level"`
    DetectedAt string `json:"detected_at"`
    Applied    bool   `json:"applied"`
    Error      string `json:"error"`
}
```

**Hard contract rules:**
- `ChangeType` **must** be one of the five exact strings the `fallbackAnalysis` switch consumes (`healer.go:768`). Any other value → default branch → `RequiresApproval=true` (safe, but loses the diff label).
- `Context` **must** carry `auto_apply_enabled` + `skip_destructive_enabled` — `buildAnalysisPrompt` reads them (`healer.go:760-761`).
- Encode with plain `json.Marshal` — **never Avro.** `handleSchemaChangeMessage` uses `kafka.SmartDeserialize` (`healer.go:247`); Apicurio/Avro is OFF in prod.

### 3.2 Topics

| Topic | Const | Producer | Consumer |
|---|---|---|---|
| `rsync.healer.schema-changes` | `HealerTopic` (`healer.go:31`) | **NEW** detector (P1); optional sink (P2) | `handleSchemaChangeMessage` (`healer.go:239`) — activated P0 |
| `rsync.healer.approved-changes` | `ApprovedChangeTopic` (`healer.go:131`) | approve API (`schema_evolution.go:128`, live) | `handleApprovedChange` (`healer.go:172-231`) — activated P0 |
| `rsync.notifications` / `rsync.healer.results` | `NotifyTopic`/`ResultsTopic` | healer (live producer) | **none** (dead end) |

---

## 4. Phased plan

### P0 — Activate the back half (spine wakes up)

**Goal:** make the existing schema-change + approved-change consumers run, **without** activating the DLQ reactive path (avoids double-processing).

**`[RT-fix #8]` The Start()-split is verified collision-free.** `executeWithHealer` (`workers/executor.go:758-828`) calls `w.executorAgent.ExecuteTask` + `w.suggestRecoveryAction` (a *worker* method, `:831`) — it never calls a `healer.Agent` method. The healer's DLQ consumers are a *separate* dormant path that would double-react to executor failures if the full `Start()` ran. **`handleSchemaChangeMessage` has no synchronous twin**, so subscribing only the two schema topics is safe. Add `StartSchemaOnly()` that subscribes to **only** `HealerTopic` + `ApprovedChangeTopic` and drops the two DLQ subscriptions.

**`[RT-fix #2]` The CDC-cleanup hook fix is REMOVED from P0.** It is orthogonal to schema drift (the schema path never touches `HealWorker`), the original sketch used a non-existent field, and there is a documented import-cycle constraint. See "Task T-hook" below — do not bundle it here.

**Files:** `healer.go` (add `StartSchemaOnly`), `workers/executor.go` (call it after `NewAgent` at `:63`).

```go
// healer.go — alongside Start() at :133. Subscribes ONLY the schema topics; omits the two DLQ ConsumeWithContext calls.
func (a *Agent) StartSchemaOnly() error {
    if err := a.kafkaManager.ConsumeWithContext(HealerTopic, a.handleSchemaChangeMessage); err != nil {
        return fmt.Errorf("start consumer %s: %w", HealerTopic, err)
    }
    if err := a.kafkaManager.ConsumeWithContext(ApprovedChangeTopic, a.handleApprovedChange); err != nil {
        log.Warnf("[HealerAgent] approved-changes consumer: %v", err)
    }
    return nil
}
```
```go
// workers/executor.go — right after :63 healerAgent := healer.NewAgent(kafkaManager, db)
if os.Getenv("RSYNC_SCHEMA_DRIFT_ENABLED") == "true" {
    if err := healerAgent.StartSchemaOnly(); err != nil {
        log.WithError(err).Warn("healer schema consumer failed to start (non-fatal)")
    }
}
```

**Migrations:** none. **Flag:** `RSYNC_SCHEMA_DRIFT_ENABLED` (default `false`). Off → not called → zero behavior change.

**Tests:** unit — assert `StartSchemaOnly` registers exactly 2 consumers and **never** subscribes `agent.executor.requests.dlq` / `agent.planner.responses.dlq`. Integration — produce a hand-crafted `add_column` event; assert a `healing_history` row and (for an approval class) a `schema_change_approvals` row.

**Verification evidence (gate 4):** orchestrator log `Listening for schema changes on rsync.healer.schema-changes` + `…approved-changes`, with **no** DLQ-topic line; consumer-group membership on `rsync.healer.schema-changes`.

**Rollback:** `RSYNC_SCHEMA_DRIFT_ENABLED=false` + restart.

#### Task T-hook (optional, separate from P0) `[RT-fix #2]`
Wire the inert CDC cleanup hook. The real struct is `heal.AutoHealHooks{ CleanupCDCResourcesFn func(ctx, pipelineID) error; RepairOwnershipFn func(ctx, pipelineID) error }` (`worker.go:58-61`) — **not** `CDCCleanup`. The real cleanup lives in `internal/cdc` which **can't be imported into `internal/agents/heal` without a cycle** (`worker.go:49-57`). Wire it by constructing the function values in `cmd/orchestrator/main.go` (where `internal/cdc` IS importable) and passing them to `NewHealWorkerWithHooks`, replacing the empty `AutoHealHooks{}` at `:743`. This is its own task; it does not block schema drift.

---

### P1 — The detector (THE one real new build)

**Goal:** persist a per-pipeline schema **baseline**, diff it against the live `DiscoverSchema` result each batch run, and emit `SchemaChangeEvent`s. This is the only net-new subsystem.

**Files:** `api-gateway/migrations/068_schema_baselines.sql` (NEW — 067 is current highest, confirmed); `executor.go` (snapshot + diff at the `DiscoverSchema` call-site `:3310`; helpers near `loadNominatedKeys` `:178`).

**Migration (068_schema_baselines.sql):**
```sql
CREATE TABLE IF NOT EXISTS schema_baselines (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id  UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    schema_name  TEXT NOT NULL DEFAULT '',
    table_name   TEXT NOT NULL,
    baseline     JSONB NOT NULL,            -- TableMetadata projection: columns(name/type/nullable/is_pk) + PKs
    source_type  TEXT,
    execution_id UUID,
    captured_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_id, schema_name, table_name)
);
CREATE INDEX IF NOT EXISTS idx_schema_baselines_pipeline ON schema_baselines(pipeline_id);
```

**`[RT-fix #1]` Execution ID:** `ExecutorTask` (`executor.go:378-386`) has **no `ExecutionID` field** — `task.ExecutionID` does not compile. Thread the local var derived in `executeBatchDataTransfer` (~`:3170`: `executionID, _ := task.Params["execution_id"].(string)`) into `storeSchemaBaseline`.

**`[RT-fix #3]` Scope to the pipeline's SELECTED tables.** `DiscoverSchema` returns the **entire source database** (no table filter — `executor.go:6474`). A pipeline syncing 2 of 50 tables would baseline all 50 and emit spurious proposals for the other 48. Before `storeSchemaBaseline` and before diffing, **filter `discovered` to the pipeline's selected-table set** (the same set persisted by the source `table_selection` HITL / plan steps). Add a unit case proving an out-of-scope table change emits no event.

**Helpers (mirror `loadNominatedKeys` `:178-195` for READ; per-table UPSERT for WRITE):**
```go
func storeSchemaBaseline(ctx context.Context, db *sql.DB, pipelineID, executionID string, discovered []TableMetadata) {
    if db == nil || strings.TrimSpace(pipelineID) == "" { return }
    for _, t := range discovered { // discovered already filtered to selected tables [RT-fix #3]
        b, err := json.Marshal(t); if err != nil { continue }
        if _, err := db.ExecContext(ctx, `
            INSERT INTO schema_baselines (pipeline_id, schema_name, table_name, baseline, source_type, execution_id, captured_at)
            VALUES ($1,$2,$3,$4::jsonb,$5,$6,NOW())
            ON CONFLICT (pipeline_id, schema_name, table_name)
            DO UPDATE SET baseline=EXCLUDED.baseline, source_type=EXCLUDED.source_type,
                          execution_id=EXCLUDED.execution_id, captured_at=NOW()`,
            pipelineID, t.Schema, t.Name, string(b), srcType, executionID); err != nil {
            log.Warnf("[drift] persist baseline (best-effort) %s.%s: %v", t.Schema, t.Name, err)
        }
    }
}
func loadSchemaBaseline(ctx context.Context, db *sql.DB, pipelineID string) map[string]TableMetadata { /* QueryContext; key schema.table; json.Unmarshal */ }
```

**Snapshot + diff insertion:** the batch metadata pass is `executor.go:3310` (`discovered, err := a.DiscoverSchema(...)`); the success branch (`else {`) iterates tables building `pkByTable`/`colTypesByTable` (`:3315-3324`). Insert **inside the `else` (discovery-success) branch only** — never the error branch (`:3311-3312`), which continues without PKs/types and would synthesize false drop-everything deltas.

1. **DIFF + EMIT** at `:3313` (before the existing `for _, tbl := range discovered`):
   - `prior := loadSchemaBaseline(ctx, a.db, task.PipelineID)`; if `prior==nil` (first run) skip diff (baseline only).
   - Else diff vs the **selected-table-filtered** `discovered`, **normalizing table keys** identically to existing code (key by qualified `schema.table` AND bare `table`, `:3315-3324`) so a schema-prefix omission doesn't phantom `create_table`/`drop_table`.
   - For each delta build a `SchemaChangeEvent` with the exact `ChangeType`, `Context["auto_apply_enabled"]=false` (P0–P3) / `Context["skip_destructive_enabled"]=true`, and a **deterministic** `DDL` (so `UNIQUE(pipeline_id, ddl)` dedup is idempotent).
   - Emit via `a.kafkaManager.ProduceWithContext(ctx, healer.HealerTopic, []byte(task.PipelineID), b)` (`kafka/manager.go:189`), keyed by `pipeline_id`.
2. **SNAPSHOT** at `:3343` (after the `for _, tbl := range discovered` loop, before the PR-D nominated-keys override `:3347`): `storeSchemaBaseline(ctx, a.db, task.PipelineID, executionID, discovered)`.

**`[RT-fix #6]` ONE writer — avoid the double-write race.** Do **not** also direct-INSERT into `schema_change_approvals` from the detector. Emit the event only and let the now-active P0 consumer own the INSERT (it also enriches `reasoning`/`risks`). The consumer's `storePendingApproval` uses `ON CONFLICT(pipeline_id,ddl) DO UPDATE SET status='pending'` (`healer.go:996`); a second writer racing it could reset a just-approved row back to `pending`. (If a UI-latency fast-path direct-write is ever wanted, first change that `ON CONFLICT` to `... WHERE schema_change_approvals.status='pending'` so it never clobbers an actioned row.)

**Delta classification `[RT-fix #4]` (shipped routing for P0–P3):**

| Delta | `ChangeType` | Routing (P0–P3) |
|---|---|---|
| Column added | `add_column` | **Pending/informational record** — NOT auto-applied |
| Column removed | `drop_column` | **APPROVE-only** |
| Type changed | `modify_column` | **APPROVE-only** |
| Table removed | `drop_table` | **APPROVE-only** |
| Table added | `create_table` | **Pending/informational record** — NOT auto-applied |

**Flag:** entire detector behind `RSYNC_SCHEMA_DRIFT_ENABLED`. (`RSYNC_SCHEMA_DRIFT_AUTOAPPLY` exists but stays OFF and out of P0–P3 — §6.)

**Tests:** unit `diffSchemas(prior,current) []SchemaChange` table-driven (add/drop/retype/create/drop-table + schema-prefix-normalization + **out-of-scope-table-no-event** `[RT-fix #3]`); `store/loadSchemaBaseline` round-trip (UPSERT idempotency). Integration: run twice, `ADD COLUMN`+`DROP COLUMN` between runs, assert one informational `add_column` + one `drop_column` pending row, third unchanged run emits nothing. e2e: `e2e/test_schema_drift_batch.sh`.

**Verification (gate 4):** `SELECT … FROM schema_baselines WHERE pipeline_id=…` one row per **selected** table after run 1; after mutate+run 2 the expected `drop_column`/`pending` row; `count(*)` unchanged on run 3 (idempotency).

**Rollback:** `RSYNC_SCHEMA_DRIFT_ENABLED=false`. Migration 068 additive — safe to leave.

---

### P2 — Reroute the sink destructive `os.Exit` into a proposal (conditional)

**Goal:** when the sink detects a destructive column-shape change (`failOnMissingColumns`), emit a `drop_column` event **before** failing closed.

**Dependency check (confirmed):** the sink holds a producer (`eventsWriter *kafka.Writer`, `main.go:2729-2736`); it knows `pipeline_id` (`WorkerConfig.PipelineID:104`, `SinkMessage.PipelineID:236`) but **not** `user_id` (resolved downstream by api-gateway joining `pipeline_id→pipelines`). So the proposal carries `pipeline_id` only — sufficient.

**P2 is an enhancement, not a prerequisite:** P1 already catches the same drop on the next batch run. P2 only adds intra-run / CDC immediacy.

**Approach:** thread a **new, separate** `*kafka.Writer` pinned to `Topic="rsync.healer.schema-changes"` (cannot reuse `eventsWriter` — its fixed `Topic` conflicts with per-message Topic) into `ensureDestinationTable` (`:6404`) from the `failOnMissingColumns=true` callers (`writeCDCToDestination:4323`, `cdcDBBatcher.flush:1257`). At `main.go:6456-6461`, inside `if _, present := curSet[c]; !present {`, **before** `return fatalError{…}`, call `emitSchemaChangeEvent(...)`.

```go
func emitSchemaChangeEvent(ctx context.Context, w *kafka.Writer, pipelineID, table, changeType, column string) error {
    event := SchemaChangeEvent{
        EventType: "schema_change_detected", PipelineID: pipelineID,
        Timestamp: time.Now().UTC().Format(time.RFC3339),
        SchemaChange: SchemaChange{ChangeType: changeType, Table: table, ColumnName: column, RiskLevel: "high"},
        Context: map[string]interface{}{"auto_apply_enabled": false, "skip_destructive_enabled": true},
        ActionNeeded: true,
    }
    b, _ := json.Marshal(event)
    return w.WriteMessages(ctx, kafka.Message{Key: []byte(pipelineID), Value: b}) // RequireAll → synchronous
}
```

**Critical ordering:** `os.Exit(1)` (`main.go:3094-3099`) runs **no deferred flush**. `WriteMessages` must return (synchronous ack, RequireAll) **before** the `fatalError` bubbles to `os.Exit`. Ship **propose-then-halt** (stay fail-closed; the destructive change is real).

**Caveat to encode:** the sink detector is column-**presence** only — can't distinguish drop vs rename vs incompatible-retype, doesn't catch pure same-name retype, and `ddl.Ensured` is process-lived (a drop during downtime is undetectable post-restart). Label `drop_column` as "best-guess destructive change, needs human decision." **Do NOT** route the intentional reload-mode DROP (`main.go:3453-3492`, gated `run_mode==reload`) into a proposal.

**Flag:** ~~`RSYNC_SINK_DRIFT_EMIT` (sink, default `false`).~~ **CORRECTED 2026-07-31 — no such flag was ever implemented.** `RSYNC_SINK_DRIFT_EMIT` is read by no Go code and set by no compose file; it survived here as a spec for a gate nobody built. When the sink's drift emission actually landed it was wired **unconditionally** (`main.go:3069`, `reportAppliedSchemaDrift` at `:7421`) — there is no off switch, and the fail-closed/fail-open behaviour below is decided by the change class, not by a flag.

**Verification (gate 4):** a `rsync.healer.schema-changes` message with `change_type=drop_column` observed **before** the sink's `fatal cdc error` log; the fail-closed exit preserved; a `schema_change_approvals` row via the P0 consumer.

---

### P3 — Approve UX (mirror PR-D column-nomination HITL)

**Goal:** render the drift-approval timeline + approve/reject modal at the deep-link the notifier already emits (`/pipelines/:id/schema-changes`, `healer.go:1063-1064`).

**Files (mirror the exact PR-D pattern):**
- `frontend/src/app/(dashboard)/pipelines/[id]/schema-changes/page.tsx` — NEW route.
- `frontend/src/components/pipeline/SchemaDriftApprovalModal.tsx` — NEW, modeled on `PreMigrationAssessmentModal.tsx:212-425` (Dialog + per-finding ack checkbox `acked` `:219-266`).
- `frontend/src/lib/api/pipelines.ts` — add `listSchemaChanges/approveSchemaChange/rejectSchemaChange` + a `SchemaChangeApproval` interface. Endpoints **already exist** unused in `frontend/src/lib/config/api.ts:267-271`.
- `frontend/src/components/pipeline/PipelineActions.tsx` — host the modal + an "N pending" badge (mirror `assessmentReport` host `:40-41,324-338`).

**Modal UX (the rule P3 enforces):**
- Destructive `change_type` requires an explicit **ack checkbox** before Approve enables (reuse `acked`).
- `add_column` / `create_table` render **informational** (no auto-apply; consistent with `[RT-fix #4]`).
- Non-RDBMS dests: **disable Approve** + "not auto-appliable" (`applyMigration` supports only postgres+mysql).
- Standalone page/modal (like `PreMigrationAssessmentModal`), **not** routed through `HITLPanel`.
- Treat 404 on approve/reject as "already actioned" (UPDATEs guarded `AND status='pending'` → `RowsAffected=0` → 404; `schema_evolution.go:112,180`): re-fetch, no hard error.

**`[RT-fix #5]` Honest verification — an approved `drop_column` does NOT apply.** The DROP/TRUNCATE substring guard (`healer.go:869-874`) refuses any DDL containing `drop`/`truncate` on the **post-approval** apply path too. So an approved `drop_column` terminates as `status='failed'` with the guard error — it never alters the destination. **The UI must surface this as "approved — not auto-applicable; apply manually,"** and gate-4 evidence for destructive classes is *"row → `failed` with guard error"*, NOT *"column altered."* (Only additive/`modify`-without-drop DDL can actually apply via the guard today.) True destructive auto-apply would require replacing the substring guard with a parsed-DDL classifier — a **separate, scoped change**, out of P3.

**`[RT-fix #7]` `suggested_ddl` is a dead column** — `storePendingApproval` never writes it (`healer.go:992-1006`) and `ListPipelineSchemaChanges` never selects it (`schema_evolution.go:56-59`). If product wants the LLM's safer rewrite shown, extend **both** the INSERT (`healer.go`, add `suggested_ddl`+`user_message` to columns/VALUES) **and** the SELECT+struct (`schema_evolution.go`). Until then the UI shows the raw detected DDL only.

**Auth:** every handler owner-gated via `requirePipelineOwner` (`pipeline_ownership.go:29-44`) → 404 (not 403) to non-owners. `reviewed_by` from `c.Get("user_email")`.

**Verification (gate 4):** DOM of `/pipelines/:id/schema-changes` rendering a `pending` `drop_column` with ack-gated Approve disabled until checked; after Approve → `status=approved`, a `rsync.healer.approved-changes` message, and **for destructive: row → `failed` with the guard error** `[RT-fix #5]` (for an applicable additive/modify: the destination DDL actually changed).

---

### P4 — CDC real-time drift + non-DB dest apply (deferred, L)

(a) NEW consumer for `schemahistory.<name>` (Debezium emits it; no consumer today) → map DDL → emit `SchemaChangeEvent`. **Separate build from P1's batch detector — P1 does NOT cover CDC.** (b) Extend `applyMigration` (`:859-912`) beyond postgres/mysql to delegate to the dest connector's structured `ensure_table`/`alter_table` MCP tools for object-storage/warehouse dests. Defer until P0–P3 proven in prod.

---

## 5. AUTO vs APPROVE matrix

The hard line: **destructive == approval-only**, and the DROP/TRUNCATE backstop (`healer.go:869-874`) refuses destructive DDL on **every** apply path including post-approval `[RT-fix #5]`.

| Drift class | Shipped routing (P0–P3) | Existing component |
|---|---|---|
| `add_column` | **Informational pending record; NOT auto-applied** `[RT-fix #4]` | sink `ensure_table` already additive (`main.go:6476-6568`) — detection path stays gated |
| `create_table` | **Informational pending record; NOT auto-applied** | sink `ensure_table` |
| `drop_column` | **APPROVE-only** → approve API → `applyMigration` → **refused by guard → `failed`** `[RT-fix #5]` | `schema_change_approvals` round-trip |
| `drop_table` | **APPROVE-only** (same guard behavior) | same |
| `modify_column` (retype) | **APPROVE-only** (applies if DDL has no drop/truncate token) | same |
| rename | **APPROVE-only** (indistinguishable from drop at sink) | same |
| connector-regen | **APPROVE-only** | `diagnose.go:215-230` → ActionRegenerateConnector |
| CDC re-provision | **APPROVE-only** | sentinel / cdc provisioning |

**Backstop:** any DDL containing `drop`/`truncate` tokens is refused regardless of approval. Known false-positive: a column literally named `drop_date` in `ADD COLUMN` trips it — safe-fail for now (§8).

---

## 6. Feature flags + safe rollout

| Flag | Scope | Default | Effect |
|---|---|---|---|
| `RSYNC_SCHEMA_DRIFT_ENABLED` | orchestrator | `false` | Gates `StartSchemaOnly` (P0) + detector (P1). Off → fully dormant. |
| `RSYNC_SCHEMA_DRIFT_AUTOAPPLY` | orchestrator | `false` | **Off and OUT OF P0–P3** `[RT-fix #4]`. Controls additive auto-apply only; do NOT enable without explicit product sign-off. Destructive classes ignore it entirely. |
| ~~`RSYNC_SINK_DRIFT_EMIT`~~ | ~~sink~~ | — | **NEVER IMPLEMENTED — do not look for it.** Read by no Go code, set by no compose file (verified 2026-07-31). The sink's drift emit shipped ungated; see the correction under P2. |

**Rollout:** (1) `RSYNC_SCHEMA_DRIFT_ENABLED=true`, autoapply off → detector populates `schema_baselines` + (via consumer) `schema_change_approvals`; **nothing applies**; soak; confirm zero false positives (esp. selected-table scoping + schema-prefix edge). (2) Build the approve UX (P3); humans drive all destructive decisions. (3) Only after a separate product decision, consider additive auto-apply. (4) ~~`RSYNC_SINK_DRIFT_EMIT=true` for CDC immediacy.~~ — superseded: the sink emit is unconditional, so there is no step 4 to perform.

---

## 7. Sequencing, effort, dependencies

| Phase | Effort | Depends on | Blocking? |
|---|---|---|---|
| **P0** activate spine | **S** | — | Yes |
| **P1** detector + baseline | **M–L** (the one real build) | P0 | Yes — the value |
| **P2** sink reroute | **M** | P0 | No — P1 covers batch on next run |
| **P3** approve UX | **M** | P1 | Yes for HITL value |
| **P4** CDC + non-DB apply | **L** | P0–P3 proven | No — deferred |
| **T-hook** CDC cleanup wiring | **S** | — | No — orthogonal `[RT-fix #2]` |

Critical path: **P0 → P1 → P3.** P2, P4, T-hook are parallelizable/deferrable.

---

## 8. Risks & open questions

1. **Uncalibrated confidence constants (no eval).** AutoBand 0.85 / HITLBand 0.50 + the LLM `SafeToAutoMigrate` gate are hand-set. Mitigation: the NOTIFY-only soak (§6) is the de-facto eval. Build a labeled drift corpus before ever trusting auto-apply.
2. **Sink `os.Exit` ordering (P2).** A fire-and-forget publish is lost if the process dies before ack → synchronous `WriteMessages` (RequireAll), check error before Exit. `ddl.Ensured` is process-lived → **P1's persisted baseline is the durable net the sink path lacks.**
3. **Additive auto-apply vs brand promise `[RT-fix #4]`.** The sink already applies `add_column` silently. For P0–P3 additive stays an *informational record*, not auto-DDL from detection. Flipping to auto-apply is a deliberate product decision, not a default.
4. **Destructive apply is structurally blocked today `[RT-fix #5]`.** The substring guard refuses approved drops → `failed`. Surface honestly; a parsed-DDL classifier is the separate prerequisite for real destructive auto-apply.
5. **Selected-table scoping is mandatory `[RT-fix #3]`.** `DiscoverSchema` returns the whole DB; without filtering, drift on unrelated tables spams the timeline and breaks the "zero false positives" bar.
6. **`suggested_ddl` dead column `[RT-fix #7]`** — needs both writer + reader extended if surfaced.
7. **Notifier delivery is a dead end** — no consumer on `rsync.notifications`; only the in-app poll/deep-link is real. Don't promise Slack/email.
8. **Multi-healer overlap `[RT-fix #8]`** — `StartSchemaOnly` deliberately drops the DLQ subscriptions to avoid double-processing vs `executeWithHealer`; if the full reactive DLQ path is ever activated, audit that overlap first.

---

## Key file references

- Spine: `backend-orchestrator/internal/agents/healer/healer.go` (struct `:68-91`, `Start` `:133-169`, `processAnalysis` `:817-857`, `applyMigration` `:859-912`, guard `:869-874`, `storePendingApproval` `:990-1012`)
- Activation: `backend-orchestrator/internal/workers/executor.go:63`
- Detector seam: `backend-orchestrator/internal/agents/executor/executor.go:3310` (snapshot `:3343`, diff `:3313`), helpers near `:178`, `executionID` ~`:3170`, `DiscoverSchema` `:6474`, `ExecutorTask` `:378-386`
- Migration: `api-gateway/migrations/068_schema_baselines.sql` (NEW; 067 current highest)
- Sink: `shared/mcp-connectors/internal/kafka-mcp-sink/worker-src/cmd/kafka-sink-worker/main.go` (drop detect `:6442-6465`, emit point `:6456-6461`, callers `:4323`+`:1257`, exit `:3094-3099`, producer `:2729-2736`)
- Approval API (live): `api-gateway/internal/handlers/schema_evolution.go:40-192`; routes `cmd/server/main.go:443,669-671`; table `migrations/051_schema_evolution.sql:4-38`
- Frontend (PR-D pattern): `frontend/src/components/pipeline/PreMigrationAssessmentModal.tsx:212-425`, `PipelineActions.tsx:40-41,324-338`, `frontend/src/lib/api/pipelines.ts:268-329`, `frontend/src/lib/config/api.ts:267-271`
- CDC (P4): `shared/mcp-connectors/internal/debezium/versions/v1.0.0/connector.py:250-254`
- Hook (T-hook): `backend-orchestrator/internal/agents/heal/worker.go:49-61`; wire in `cmd/orchestrator/main.go:743`
