"use client"

import { useEffect, useMemo, useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion"
import { cn } from "@/lib/utils"
import { getPipeline } from "@/lib/api/pipelines"
import { onPipelineSchedulesChanged } from "@/lib/events/scheduleCreate"
import {
  type DataLoadingStrategy,
  computeStrategySteps,
  normalizeStrategyMode,
  strategyBadge,
  strategyCompactLabel,
  strategyTitle,
} from "@/lib/pipeline/dataLoadingStrategy"

function isMeaningfulEvidenceValue(v: unknown): boolean {
  if (v === null || v === undefined) return false
  if (typeof v === "string") return v.trim().length > 0
  if (typeof v === "number") return Number.isFinite(v)
  if (typeof v === "boolean") return v === true
  if (Array.isArray(v)) return v.length > 0
  if (typeof v === "object") return Object.keys(v as Record<string, unknown>).length > 0
  return true
}

function formatEvidenceValue(v: unknown): string {
  if (v === null || v === undefined) return ""
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") return String(v)
  try {
    return JSON.stringify(v)
  } catch {
    return String(v)
  }
}

export const STRATEGY_FETCH_TIMEOUT_MS = 15_000
export const STRATEGY_FETCH_MAX_RETRIES = 2
export const STRATEGY_FETCH_RETRY_DELAY_MS = 3_000

interface DataLoadingStrategyCardProps {
  pipelineId?: string
  strategy?: DataLoadingStrategy | null
  title?: string
  className?: string
  showWhy?: boolean
}

export function DataLoadingStrategyCard({
  pipelineId,
  strategy: strategyProp,
  title = "Data loading strategy",
  className,
  showWhy = true,
}: DataLoadingStrategyCardProps) {
  const shouldFetch = Boolean(pipelineId && !strategyProp)
  const [strategy, setStrategy] = useState<DataLoadingStrategy | null>(strategyProp ?? null)
  // Avoid initial "batch" fallback flicker while we fetch strategy (especially noticeable for CDC pipelines).
  const [loading, setLoading] = useState<boolean>(shouldFetch)
  const [error, setError] = useState<string | null>(null)

  // The strategy label is derived from schedule status — "Batch (scheduled) · Runs
  // automatically on schedule" vs "Batch (manual)" — but this card fetched the pipeline
  // once on mount and never again, so it kept advertising a schedule that had just been
  // deleted until a hard reload. Re-fetch when any schedule mutation broadcasts.
  const [scheduleRevision, setScheduleRevision] = useState(0)
  // "Loading strategy…" used to be permanent when the one GET never settled:
  // authFetch has no default timeout and nothing retried, so a request that
  // stalled while the run finished left the Overview spinning until a reload.
  // Bound each attempt and retry a few times before saying it is unavailable.
  const [retryAttempt, setRetryAttempt] = useState(0)
  useEffect(() => {
    if (!pipelineId) return
    return onPipelineSchedulesChanged((pid) => {
      if (pid !== pipelineId) return
      setScheduleRevision((n) => n + 1)
    })
  }, [pipelineId])

  useEffect(() => {
    let cancelled = false
    if (!pipelineId) return
    if (strategyProp) return // explicit beats fetch

    let retryTimer: ReturnType<typeof setTimeout> | undefined
    const run = async () => {
      setLoading(true)
      setError(null)
      try {
        const p = await getPipeline(pipelineId, { timeoutMs: STRATEGY_FETCH_TIMEOUT_MS })
        const s = p?.data_loading_strategy as DataLoadingStrategy | undefined
        if (!cancelled) setStrategy(s || null)
        if (!cancelled) setLoading(false)
      } catch (e: any) {
        if (cancelled) return
        if (retryAttempt < STRATEGY_FETCH_MAX_RETRIES) {
          // Keep the loading label through the retry; a transient stall is not an error.
          retryTimer = setTimeout(() => setRetryAttempt((n) => n + 1), STRATEGY_FETCH_RETRY_DELAY_MS)
          return
        }
        setError(e?.message || "Failed to load strategy")
        setLoading(false)
      }
    }

    void run()
    return () => {
      cancelled = true
      if (retryTimer) clearTimeout(retryTimer)
    }
  }, [pipelineId, strategyProp, scheduleRevision, retryAttempt])

  const hasStrategy = Boolean(strategy)
  const mode = useMemo(
    () => (hasStrategy ? normalizeStrategyMode(strategy?.mode || strategy?.effective_sync_mode) : "batch"),
    [hasStrategy, strategy?.effective_sync_mode, strategy?.mode]
  )
  const steps = useMemo(() => (hasStrategy ? computeStrategySteps(strategy) : []), [hasStrategy, strategy])
  const badge = useMemo(
    () =>
      hasStrategy
        ? strategyBadge(mode)
        : {
            label: "…",
            className:
              "bg-zinc-50 text-zinc-700 dark:bg-zinc-900/30 dark:text-zinc-300 border-zinc-200 dark:border-zinc-800",
          },
    [hasStrategy, mode]
  )
  const subtitle = useMemo(
    () => {
      if (hasStrategy) return strategyTitle(mode, strategy?.effective_cdc_mode)
      if (loading) return "Loading strategy…"
      return "Strategy unavailable"
    },
    [hasStrategy, loading, mode, strategy?.effective_cdc_mode]
  )

  const selectedTables = useMemo(() => {
    const raw = strategy?.selected_tables
    if (!raw || !Array.isArray(raw)) return []
    return raw.map((t) => String(t || "").trim()).filter(Boolean)
  }, [strategy?.selected_tables])

  const dataset = String(strategy?.dataset || "").trim()
  const compact = hasStrategy ? strategyCompactLabel(strategy) : "—"
  const evidenceEntries = useMemo(() => {
    const raw = strategy?.evidence
    if (!raw || typeof raw !== "object") return []
    return Object.entries(raw)
      .filter(([, v]) => isMeaningfulEvidenceValue(v))
      .map(([k, v]) => ({
        key: k,
        label: k.replace(/_/g, " "),
        value: formatEvidenceValue(v),
      }))
      .filter((e) => e.value.trim().length > 0)
  }, [strategy?.evidence])

  return (
    <Card className={cn("border-zinc-200 dark:border-zinc-800", className)}>
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <CardTitle className="text-base">{title}</CardTitle>
            <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400">
              <Badge variant="outline" className={cn("text-xs", badge.className)}>
                {badge.label}
              </Badge>
              <span className="truncate">{subtitle}</span>
              <Badge variant="secondary" className="text-xs">
                {compact}
              </Badge>
            </div>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        {loading ? (
          <div className="text-sm text-zinc-500 dark:text-zinc-400">Loading strategy…</div>
        ) : error ? (
          <div className="text-sm text-red-600">{error}</div>
        ) : steps.length === 0 ? (
          <div className="text-sm text-zinc-500 dark:text-zinc-400">Strategy unavailable.</div>
        ) : (
          <ol className="space-y-2">
            {steps.slice(0, 5).map((s, idx) => (
              <li key={`${idx}-${s}`} className="flex items-start gap-3">
                <div className="mt-0.5 flex h-6 w-6 items-center justify-center rounded-full bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-200 text-xs font-medium">
                  {idx + 1}
                </div>
                <div className="text-sm text-zinc-800 dark:text-zinc-200">{s}</div>
              </li>
            ))}
          </ol>
        )}

        {(dataset || selectedTables.length > 0) && (
          <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 p-3 text-sm">
            {dataset && (
              <div className="flex items-center justify-between gap-3">
                <span className="text-zinc-500 dark:text-zinc-400">Dataset</span>
                <span className="font-mono text-xs text-zinc-900 dark:text-zinc-100">{dataset}</span>
              </div>
            )}
            {selectedTables.length > 0 && (
              <div className={cn("flex items-start justify-between gap-3", dataset ? "mt-2" : "")}>
                <span className="text-zinc-500 dark:text-zinc-400">Tables</span>
                <span className="text-right text-xs text-zinc-700 dark:text-zinc-300">
                  {selectedTables.length} selected
                </span>
              </div>
            )}
          </div>
        )}

        {showWhy && evidenceEntries.length > 0 && (
          <Accordion type="single" collapsible>
            <AccordionItem value="why">
              <AccordionTrigger className="text-sm">Why this strategy?</AccordionTrigger>
              <AccordionContent>
                <div className="space-y-2 text-xs">
                  {evidenceEntries.map((e) => (
                    <div key={e.key} className="flex items-center justify-between gap-3">
                      <span className="text-zinc-500 dark:text-zinc-400">{e.label}</span>
                      <span className="font-mono text-zinc-900 dark:text-zinc-100 truncate max-w-[60%]">
                        {e.value}
                      </span>
                    </div>
                  ))}
                </div>
              </AccordionContent>
            </AccordionItem>
          </Accordion>
        )}
      </CardContent>
    </Card>
  )
}

