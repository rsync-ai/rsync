// Subtitle helpers for a *running* stage in PipelineAccordionView.
//
// Two bugs lived in the inline code these replace:
//  - A running stage carries `actual_duration_ms: 0` (the workflow writes the
//    field when the stage starts and fills it on completion). formatDuration(0)
//    is "0ms", which is truthy, so it beat the live elapsed time:
//    "Executing Pipeline — Preparing… (0ms)" for the whole run.
//  - The executor only said "Syncing" when *its own* metadata carried the
//    connection names, but only the connection-validation stage emits them, so
//    a batch run that was moving rows said "Preparing…" until it finished.

type StageLike = {
  id: string
  status?: string
  progress?: number
  metadata?: Record<string, unknown>
}

/** A measured duration only counts when it is a positive number. */
export function positiveDurationMs(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : null
}

/**
 * Time to show next to a running stage: the live elapsed time first, then a
 * positive measured duration. A zero is "not measured yet", never "took 0ms".
 */
export function runningStageTimeLabel(
  elapsedLabel: string | null,
  durationLabel: string | null,
): string | null {
  const isZero = (s: string | null) => !s || s === "0s" || s === "0ms"
  if (!isZero(elapsedLabel)) return elapsedLabel
  if (!isZero(durationLabel)) return durationLabel
  return null
}

/** Connection names from whichever stage emitted them (connection validation). */
export function findConnectionNames(
  stage: StageLike,
  allStages: StageLike[],
): { srcName: string | null; dstName: string | null } {
  const pick = (key: string): string | null => {
    for (const s of [stage, ...allStages]) {
      const v = s.metadata?.[key]
      if (typeof v === "string" && v.trim()) return v
    }
    return null
  }
  return { srcName: pick("source_connection_name"), dstName: pick("destination_connection_name") }
}

/**
 * Executor subtitle. "Syncing" needs evidence the transfer is past prep,
 * because table selection happens inside this stage and saying "Syncing"
 * before tables are picked is untrue. Evidence: stage progress or rows, or
 * the worker's per-table progress message ("Transferred N of M tables",
 * executor.go buildExecutorTableProgressEvent). Without it the label still
 * names the route when the connection-validation stage reported it.
 */
export function executorRunningMessage(args: {
  currentStage?: string
  stateMessage?: string
  stage: StageLike
  srcName: string | null
  dstName: string | null
}): string {
  const { currentStage, stateMessage, stage, srcName, dstName } = args
  if (currentStage === "infra_preflight") return "Waiting for infrastructure…"
  const meta = stage.metadata || {}
  const rows = Number(meta.rows_synced ?? meta.written_rows ?? meta.inserted_rows ?? 0)
  const transferring =
    (typeof stage.progress === "number" && stage.progress > 0 && stage.progress < 100) ||
    (Number.isFinite(rows) && rows > 0) ||
    /^Transferred \d+ of \d+ tables/.test(stateMessage || "")
  const route = srcName && dstName ? ` "${srcName}" → "${dstName}"` : ""
  if (transferring) return route ? `Syncing${route}…` : "Syncing data…"
  return route ? `Preparing${route}…` : "Preparing…"
}
