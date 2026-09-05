import type { ExecutionPlanStage } from "./DAGVisualization"
import { stageDurationMs } from "./dagHelpers"

/**
 * Pure derivations behind the execution detail dialog.
 *
 * Split out from the component for two reasons. First, the interesting
 * questions — "did this run move any data?", "where did the wall-clock go?" —
 * are decidable without a DOM, so they get real unit tests instead of render
 * assertions. Second, the row-count derivation must exist in exactly ONE place
 * on the client: a dialog and the list row it opened from answering the same
 * question differently is the recurring defect this file exists to prevent
 * (#741's dashboard, #742's records column).
 */

export type TableStatRow = {
  qualified_name: string
  table_name?: string
  /**
   * Source-side schema. For CDC the sink derives the table identity from the
   * Debezium envelope, so this names the SOURCE database — not where the rows
   * landed. `destination_qualified_name` answers that; it is absent when the
   * emitter cannot name a destination namespace (object-storage destination, no
   * configured namespace, or a sink predating migration 089).
   */
  schema_name?: string
  destination_schema?: string
  destination_qualified_name?: string
  mode?: string
  status?: string
  read_rows?: number | null
  inserted_rows?: number | null
  applied_inserts?: number | null
  applied_updates?: number | null
  applied_deletes?: number | null
  dlq_rows?: number | null
  bytes_read?: number | null
  bytes_committed?: number | null
  started_at?: string | null
  completed_at?: string | null
}

/**
 * Rows this table actually landed at the destination.
 *
 * Mirrors the server's derivation byte-for-byte — `GREATEST(inserted_rows,
 * applied_inserts + applied_updates + applied_deletes)` in ListExecutions
 * (`pipelines.go:3728`) and GetExecution (`pipelines.go:4003`). A batch table
 * populates the first family and a CDC table the second; GREATEST picks whichever
 * one this table is using without needing to branch on mode. Keep this in
 * lockstep with the SQL: if the server's expression changes, change this too, or
 * the dialog starts disagreeing with the row that opened it.
 */
export function rowsWrittenForTable(t: TableStatRow): number {
  const batch = t.inserted_rows ?? 0
  const cdc = (t.applied_inserts ?? 0) + (t.applied_updates ?? 0) + (t.applied_deletes ?? 0)
  return Math.max(batch, cdc)
}

export type TableStatsRollup = {
  tableCount: number
  /**
   * Null — not 0 — when no table reported a read count at all. "Nothing was
   * read" and "reads were never measured" lead to opposite conclusions about a
   * zero-row run, so they must not collapse into the same number.
   */
  rowsRead: number | null
  rowsWritten: number
  dlqRows: number
  bytesCommitted: number | null
  failedTables: number
  degradedTables: number
  runningTables: number
}

export function rollupTableStats(tables: TableStatRow[]): TableStatsRollup {
  let rowsRead: number | null = null
  let bytesCommitted: number | null = null
  let rowsWritten = 0
  let dlqRows = 0
  let failedTables = 0
  let degradedTables = 0
  let runningTables = 0

  for (const t of tables) {
    rowsWritten += rowsWrittenForTable(t)
    dlqRows += t.dlq_rows ?? 0

    if (typeof t.read_rows === "number") rowsRead = (rowsRead ?? 0) + t.read_rows
    if (typeof t.bytes_committed === "number") bytesCommitted = (bytesCommitted ?? 0) + t.bytes_committed

    if (t.status === "failed") failedTables++
    else if (t.status === "degraded") degradedTables++
    else if (t.status === "running") runningTables++
  }

  return {
    tableCount: tables.length,
    rowsRead,
    rowsWritten,
    dlqRows,
    bytesCommitted,
    failedTables,
    degradedTables,
    runningTables,
  }
}

/** The server-computed `summary` object from GET /pipelines/:id/table-stats. */
export type TableStatsSummaryPayload = {
  total_tables?: number
  tables_failed?: number
  tables_degraded?: number
  tables_running?: number
  total_read_rows?: number
  total_inserted_rows?: number
  total_applied_inserts?: number
  total_applied_updates?: number
  total_applied_deletes?: number
  total_dlq_rows?: number
}

/**
 * Roll up from the server's summary rather than the returned page of tables.
 *
 * `/table-stats` paginates — a client-side sum over the first page silently
 * undercounts a pipeline with more tables than the limit, and would then
 * disagree with the Records value in the row that opened this dialog. The
 * summary is computed across every table for the run, so it is the only
 * paging-proof source.
 *
 * Caveat, deliberate: the server sums GREATEST *per table*, while this can only
 * take GREATEST of the already-summed families. The two agree exactly for a run
 * whose tables are all batch or all CDC, and can differ for a mixed-mode run —
 * which is why the caller prefers `metrics.records_processed` (the server's own
 * per-table sum) for the headline and uses this for read/DLQ counts.
 */
export function rollupFromSummary(s: TableStatsSummaryPayload | null | undefined): TableStatsRollup | null {
  if (!s) return null

  const applied =
    (s.total_applied_inserts ?? 0) + (s.total_applied_updates ?? 0) + (s.total_applied_deletes ?? 0)

  return {
    tableCount: s.total_tables ?? 0,
    rowsRead: typeof s.total_read_rows === "number" ? s.total_read_rows : null,
    rowsWritten: Math.max(s.total_inserted_rows ?? 0, applied),
    dlqRows: s.total_dlq_rows ?? 0,
    bytesCommitted: null,
    failedTables: s.tables_failed ?? 0,
    degradedTables: s.tables_degraded ?? 0,
    runningTables: s.tables_running ?? 0,
  }
}

/**
 * What the run did to the data, as a decision rather than a number.
 *
 * The bug this replaces: the Records column rendered "—" for a run that copied
 * nothing AND for a run whose statistics were never projected. Those demand
 * opposite responses from an operator — one is a correct no-op, the other means
 * you have no idea what happened — so they get distinct verdicts here and
 * distinct sentences on screen.
 *
 * `read-not-written` is the one worth waking up for: the source produced rows
 * and none of them landed. It was previously indistinguishable from both of the
 * above.
 */
export type DataMovement =
  | { kind: "unmeasured" }
  | { kind: "empty-source"; tables: number }
  | { kind: "read-not-written"; read: number; tables: number }
  | { kind: "moved"; written: number; read: number | null; tables: number }

export function dataMovementVerdict(rollup: TableStatsRollup | null): DataMovement {
  if (!rollup || rollup.tableCount === 0) return { kind: "unmeasured" }
  if (rollup.rowsWritten > 0) {
    return {
      kind: "moved",
      written: rollup.rowsWritten,
      read: rollup.rowsRead,
      tables: rollup.tableCount,
    }
  }
  if ((rollup.rowsRead ?? 0) > 0) {
    return { kind: "read-not-written", read: rollup.rowsRead as number, tables: rollup.tableCount }
  }
  return { kind: "empty-source", tables: rollup.tableCount }
}

export type MovementHeadline = {
  title: string
  detail: string
  /** Drives colour only. "unknown" is deliberately not "ok" — see below. */
  tone: "ok" | "alarm" | "unknown"
}

const plural = (n: number, word: string) => `${n.toLocaleString()} ${word}${n === 1 ? "" : "s"}`

/**
 * The one-line answer to "what did this run do?".
 *
 * Kept out of JSX so the wording of the three previously-identical cases is
 * asserted by tests rather than by looking at a screenshot. The `unknown` tone
 * matters: an operator who reads "0 rows" acts differently than one who reads
 * "we don't know", and the old dialog gave both of them the same em dash.
 */
export function movementHeadline(v: DataMovement): MovementHeadline {
  switch (v.kind) {
    case "unmeasured":
      return {
        tone: "unknown",
        title: "No table statistics were recorded",
        detail:
          "This run reported no per-table statistics, so how much data moved is unknown — which is not the same as zero.",
      }
    case "empty-source":
      return {
        tone: "ok",
        title: "Nothing to move",
        detail: `${plural(v.tables, "table")} reported no rows at the source, so nothing needed copying.`,
      }
    case "read-not-written":
      return {
        tone: "alarm",
        title: `Read ${plural(v.read, "row")}, wrote none`,
        detail: `The source produced rows across ${plural(v.tables, "table")} and none of them landed at the destination.`,
      }
    case "moved": {
      const readPart =
        v.read !== null && v.read !== v.written
          ? ` from ${plural(v.read, "row")} read`
          : ""
      return {
        tone: "ok",
        title: `Moved ${plural(v.written, "row")}`,
        detail: `Written to the destination across ${plural(v.tables, "table")}${readPart}.`,
      }
    }
  }
}

type EventLike = {
  event_type?: string
  seq?: number | null
  occurred_at?: string | null
  received_at?: string
  payload?: Record<string, unknown> | null
}

/**
 * Recover the execution plan for a FINISHED run from its event stream.
 *
 * The Steps/DAG tab reads `execution_plan` off the pipeline *state* response,
 * which only ever describes the current run — there is no state row to read for
 * a run that ended two days ago. Every terminal event carries a snapshot of the
 * plan under `payload.metadata.execution_plan`, so the newest snapshot in the
 * stream is the run's final shape.
 *
 * Ordering is computed here rather than trusted from the caller: the stream
 * carries two independent `seq` numbering schemes (adapter counts 1..n, the
 * orchestrator emits snowflake ids), so `seq` is not comparable across
 * producers. Timestamps are.
 */
export function planStagesFromEvents(events: EventLike[]): ExecutionPlanStage[] {
  let best: { at: number; stages: ExecutionPlanStage[] } | null = null

  for (const e of events) {
    const metadata = e?.payload?.metadata as Record<string, unknown> | undefined
    const plan = metadata?.execution_plan as Record<string, unknown> | undefined
    const stages = plan?.stages
    if (!Array.isArray(stages) || stages.length === 0) continue

    const stamp = e.occurred_at || e.received_at
    const at = stamp ? new Date(stamp).getTime() : NaN
    // An unparseable timestamp still beats having no plan at all, but never
    // displaces one we could actually order.
    const score = Number.isFinite(at) ? at : -Infinity
    if (!best || score >= best.at) best = { at: score, stages: stages as ExecutionPlanStage[] }
  }

  return best?.stages ?? []
}

export type PhaseRow = {
  id: string
  label: string
  /** Null when the stage was never timed — distinct from a measured zero. */
  ms: number | null
  status?: string
  /** True for the synthetic remainder row, which is inferred rather than reported. */
  inferred?: boolean
}

export type RunTimeline = {
  phases: PhaseRow[]
  totalMs: number | null
  /**
   * Wall-clock the stages do not account for: queueing, workflow startup,
   * teardown. Null when we cannot compute it honestly (no end time, or no
   * stage was timed). Negative remainders are clamped away — overlapping
   * stages can sum past the wall clock, and a negative bar is a lie.
   */
  unaccountedMs: number | null
}

/**
 * Where the run's wall-clock actually went.
 *
 * Durations come from `stageDurationMs`, the same helper the DAG uses, so the
 * two screens cannot disagree about how long a stage took — including the
 * legacy seconds-vs-milliseconds fallback it encapsulates.
 */
export function buildRunTimeline(
  stages: ExecutionPlanStage[],
  startTime?: string | null,
  endTime?: string | null,
): RunTimeline {
  const phases: PhaseRow[] = stages.map((s) => ({
    id: s.id,
    label: s.display_name || s.id,
    ms: stageDurationMs(s),
    status: s.status,
  }))

  const start = startTime ? new Date(startTime).getTime() : NaN
  const end = endTime ? new Date(endTime).getTime() : NaN
  const totalMs = Number.isFinite(start) && Number.isFinite(end) ? end - start : null

  const timed = phases.filter((p) => p.ms !== null)
  const accounted = timed.reduce((sum, p) => sum + (p.ms as number), 0)

  let unaccountedMs: number | null = null
  if (totalMs !== null && timed.length > 0) {
    unaccountedMs = Math.max(0, totalMs - accounted)
  }

  return { phases, totalMs, unaccountedMs }
}

export type RunDelta = {
  previousId: string
  /** Null when either run's row count is unmeasured — a delta against an unknown is not a number. */
  rowsDelta: number | null
  durationDeltaMs: number | null
  /** True when both runs moved the same (measured) number of rows. */
  sameRows: boolean
}

type RunLike = {
  id: string
  start_time?: string | null
  end_time?: string | null
}

/**
 * Compare this run to the one before it.
 *
 * A single execution in isolation rarely tells you whether anything is wrong —
 * "0 rows in 55s" is either steady state or a total outage depending entirely on
 * what the previous run did. That comparison is what makes a *history* tab worth
 * opening, and the list is already in client state, so it costs nothing.
 */
export function computeRunDelta(
  currentRows: number | null,
  current: RunLike,
  previousRows: number | null,
  previous: RunLike | null | undefined,
): RunDelta | null {
  if (!previous) return null

  const durationOf = (r: RunLike): number | null => {
    if (!r.start_time || !r.end_time) return null
    const a = new Date(r.start_time).getTime()
    const b = new Date(r.end_time).getTime()
    return Number.isFinite(a) && Number.isFinite(b) ? b - a : null
  }

  const curDur = durationOf(current)
  const prevDur = durationOf(previous)

  const rowsKnown = currentRows !== null && previousRows !== null

  return {
    previousId: previous.id,
    rowsDelta: rowsKnown ? (currentRows as number) - (previousRows as number) : null,
    durationDeltaMs: curDur !== null && prevDur !== null ? curDur - prevDur : null,
    sameRows: rowsKnown && currentRows === previousRows,
  }
}

/**
 * Format a byte count. Returns null — not "0 B" — when the metric was never
 * populated, matching the read-count rule above.
 */
export function formatBytes(bytes: number | null | undefined): string | null {
  if (typeof bytes !== "number" || !Number.isFinite(bytes)) return null
  if (bytes < 1024) return `${bytes} B`
  const units = ["KB", "MB", "GB", "TB"]
  let value = bytes / 1024
  let unit = 0
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024
    unit++
  }
  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unit]}`
}
