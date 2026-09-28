import type { PipelineRuntime } from "@/lib/hooks/usePipelineRuntime"

export type LoadStatusTone = "ok" | "info" | "warn" | "error"

export interface LoadStatus {
  text: string
  tone: LoadStatusTone
  // The recorded reason a load failed or was not confirmed, for a tooltip.
  title?: string
  // When the load started, or finished once it has.
  since?: { label: "started" | "finished"; at: string }
}

// Phases in which the pipeline is being set up (again): after a Reload or a
// Start the gateway reports one of these (pipeline_runtime.go
// computeRuntimePhase) while runtime.load still holds the previous run's row.
const SETUP_PHASES: ReadonlySet<string> = new Set(["initializing", "planning", "validating", "syncing"])

/** Whether the pipeline is being set up, so no stream is live to be "ongoing" or "caught up". */
export function isSettingUp(phase: string | null | undefined): boolean {
  return SETUP_PHASES.has(String(phase ?? ""))
}

const tables = (n: number) => `${n.toLocaleString()} ${n === 1 ? "table" : "tables"}`
const rows = (n: number) => `${n.toLocaleString("en-US")} ${n === 1 ? "row" : "rows"}`

/**
 * Where a CDC pipeline is, the way AWS DMS says it: "Full load in progress ·
 * 3 / 5 tables", then "Load completed, replication ongoing".
 *
 * It reads the initial load the orchestrator recorded (runtime.load), never an
 * inference from row counts, so a pipeline without a record gets null: the page
 * must not claim a load finished that nobody saw. The pipeline's phase and
 * health only decide whether replication is ongoing, paused or stalled.
 *
 * The recorded load completes when the source has been read to the end, so a
 * completed load whose rows the destination has not all written yet
 * (snapshot_rows_waiting) reads "Full load read · writing N rows", not
 * "Load completed".
 */
export function describeLoadStatus(
  runtime: Pick<PipelineRuntime, "mode" | "phase" | "health" | "load"> | null | undefined,
): LoadStatus | null {
  const load = runtime?.mode === "cdc" ? runtime.load : undefined
  if (!runtime || !load) return null

  // userStopped: the user pressed Stop; the connector and its position are kept
  // and Start resumes (U-14 — it used to read "replication paused"). stopped is a
  // failure.
  const userStopped = runtime.phase === "stopped"
  const paused = runtime.phase === "paused"
  const stopped = runtime.phase === "failed"
  const total = Number(load.tables_total) || 0
  const done = Number(load.tables_done) || 0
  const started = load.started_at ? { since: { label: "started" as const, at: load.started_at } } : {}
  const reason = load.last_error?.trim() ? { title: load.last_error.trim() } : {}

  switch (load.status) {
    case "sent":
    case "started": {
      // Debezium marks no table done in an incremental snapshot, so a done
      // count would sit at 0 and read as stuck: name only the tables covered.
      const count = total <= 0 ? "" : load.mode === "incremental" ? ` · ${tables(total)}` : ` · ${done} / ${tables(total)}`
      const verb = stopped ? "stopped" : userStopped ? "on hold, pipeline stopped" : paused ? "paused" : "in progress"
      return { text: `Full load ${verb}${count}`, tone: stopped ? "error" : "info", ...started }
    }
    case "completed": {
      // A Reload or Start is setting the pipeline up again: the completed row is
      // the previous load's, and reloading_tables stays 0 until the re-snapshot
      // request exists, so nothing here says replication is running (item 32).
      if (isSettingUp(runtime.phase)) {
        return { text: "Setting up · previous load completed", tone: "info", ...(load.completed_at ? { since: { label: "finished" as const, at: load.completed_at } } : {}) }
      }
      const finished = load.completed_at ? { since: { label: "finished" as const, at: load.completed_at } } : {}
      const reloading = Number(load.reloading_tables) || 0
      const waiting = Math.max(0, Number(load.snapshot_rows_waiting) || 0)
      // A re-snapshot's rows land in the same counters; the re-loading line
      // below already says that load is under way.
      if (waiting > 0 && reloading === 0) {
        const state = stopped
          ? "stopped"
          : userStopped
            ? "pipeline stopped"
            : paused
              ? "paused"
              : runtime.health === "unhealthy"
                ? "stalled"
                : null
        if (!state) return { text: `Full load read · writing ${rows(waiting)}`, tone: "info", ...started }
        const tone = stopped ? "error" : state === "stalled" ? "warn" : "info"
        return { text: `Full load read · ${rows(waiting)} to write, ${state}`, tone, ...started }
      }
      if (stopped) return { text: "Load completed, replication stopped", tone: "error", ...finished }
      if (userStopped) return { text: "Load completed, pipeline stopped", tone: "info", ...finished }
      if (paused) return { text: "Load completed, replication paused", tone: "info", ...finished }
      if (runtime.health === "unhealthy") return { text: "Load completed, replication stalled", tone: "warn", ...finished }
      if (reloading > 0) return { text: `Replication ongoing · re-loading ${tables(reloading)}`, tone: "info", ...finished }
      return { text: "Load completed, replication ongoing", tone: "ok", ...finished }
    }
    case "failed":
      return { text: "Full load failed", tone: "error", ...reason, ...started }
    case "unconfirmed":
      return { text: "Full load not confirmed", tone: "warn", ...reason, ...started }
    default:
      // A status this client does not know yet: say it rather than guess.
      return { text: `Full load: ${String(load.status)}`, tone: "info", ...reason, ...started }
  }
}

/**
 * Whether a CDC pipeline's backlog may still hold its initial load's rows, so
 * the page must count it in rows rather than "changes".
 *
 * liveness.pending_events and the sink's Kafka lag both count snapshot rows
 * alongside changes: during a 75,200-row load the header read "67.2K changes
 * waiting" and Delivery "34,233 changes". "Rows" is never wrong; "changes" is
 * right only once the recorded load has finished and written every row, with no
 * table being loaded again.
 */
export function backlogIncludesLoadRows(
  runtime: Pick<PipelineRuntime, "mode" | "load"> | null | undefined,
): boolean {
  const load = runtime?.mode === "cdc" ? runtime.load : undefined
  if (!load) return false
  if (load.status !== "completed") return true
  return (Number(load.snapshot_rows_waiting) || 0) > 0 || (Number(load.reloading_tables) || 0) > 0
}
