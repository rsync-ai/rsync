export type StrategyMode = "batch" | "cdc"

export interface DataLoadingStrategy {
  mode?: string
  initial_load?: string
  ongoing_sync?: string
  rerun_default?: string
  dataset?: string
  explanation_steps?: string[]
  evidence?: Record<string, unknown>
  selected_tables?: string[]
  schedule_status?: string
  schedule_type?: string
  effective_cdc_mode?: string
  effective_sync_mode?: string
}

export function normalizeStrategyMode(input?: string | null): StrategyMode {
  const s = String(input || "").trim().toLowerCase()
  return s === "cdc" ? "cdc" : "batch"
}

export function strategyTitle(mode: StrategyMode, cdcMode?: string | null): string {
  if (mode === "cdc") {
    const cm = String(cdcMode || "").trim().toLowerCase()
    if (cm === "streaming_only") return "CDC (changes only)"
    return "CDC (snapshot + changes)"
  }
  return "Batch sync"
}

export function strategyBadge(mode: StrategyMode): { label: string; className: string } {
  if (mode === "cdc") {
    return {
      label: "CDC",
      className:
        "bg-violet-50 text-violet-700 dark:bg-violet-900/30 dark:text-violet-300 border-violet-200 dark:border-violet-800",
    }
  }
  return {
    label: "BATCH",
    className:
      "bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border-blue-200 dark:border-blue-800",
  }
}

export function computeStrategySteps(strategy?: DataLoadingStrategy | null): string[] {
  if (strategy?.explanation_steps && Array.isArray(strategy.explanation_steps) && strategy.explanation_steps.length > 0) {
    return strategy.explanation_steps.filter((s) => Boolean(String(s || "").trim()))
  }

  const mode = normalizeStrategyMode(strategy?.mode || strategy?.effective_sync_mode)
  const dataset = String(strategy?.dataset || "").trim()
  const cdcMode = String(strategy?.effective_cdc_mode || "").trim().toLowerCase()
  const scheduleStatus = String(strategy?.schedule_status || "").trim().toLowerCase()
  const rerunDefault = String(strategy?.rerun_default || "").trim().toLowerCase()

  const steps: string[] = []
  if (mode === "cdc") {
    if (cdcMode === "streaming_only") steps.push("Backfill: Skip historical backfill (start streaming new changes only)")
    else steps.push("Backfill: Take an initial snapshot of selected tables")
    steps.push("Keep in sync: Stream INSERT/UPDATE/DELETE continuously (CDC)")
    steps.push("Reruns: CDC is continuous; use Restart CDC to recover if needed")
  } else {
    steps.push("Backfill: Copy selected tables in batch runs (historical snapshot)")
    steps.push(
      scheduleStatus === "active" ? "Keep in sync: Runs automatically on schedule" : "Keep in sync: Runs when you click Run"
    )
    steps.push(
      rerunDefault === "reload"
        ? "Reruns: Default is Reload (rebuild from scratch)"
        : "Reruns: Default is Resume (continue from last checkpoint)"
    )
  }

  if (dataset) steps.push(`Destination layout: Write under dataset "${dataset}"`)
  return steps
}

export function strategyCompactLabel(strategy?: DataLoadingStrategy | null): string {
  const mode = normalizeStrategyMode(strategy?.mode || strategy?.effective_sync_mode)
  if (mode === "cdc") {
    const cdcMode = String(strategy?.effective_cdc_mode || "").trim().toLowerCase()
    return cdcMode === "streaming_only" ? "CDC (changes only)" : "Backfill+CDC"
  }
  const scheduleStatus = String(strategy?.schedule_status || "").trim().toLowerCase()
  return scheduleStatus === "active" ? "Batch (scheduled)" : "Batch (manual)"
}

