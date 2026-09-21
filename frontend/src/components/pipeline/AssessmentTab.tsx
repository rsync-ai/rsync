"use client"

/**
 * Assessment tab: the pipeline's pre-migration assessment as a DMS-style list
 * of graded checks (Critical / High / Medium / Low, passes included), what to
 * do about each one, and the history of saved runs (manual, the run gate and
 * the scheduled re-check of running CDC pipelines). Only a failed Critical
 * check blocks the start — the same rule the run gate applies.
 */

import { Fragment, useCallback, useEffect, useMemo, useState } from "react"
import {
  AlertCircle,
  AlertTriangle,
  Check,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  ClipboardCopy,
  Info,
  Loader2,
  PlayCircle,
  ShieldCheck,
} from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { LocalDateTime } from "@/components/ui/local-date-time"
import { cn } from "@/lib/utils"
import { getConnectorDisplayName } from "@/lib/types/mcp-connector"
import {
  assessPipeline,
  getPipelineAssessment,
  listPipelineAssessments,
  type AssessmentCheck,
  type AssessmentCheckCategory,
  type AssessmentCheckResult,
  type AssessmentCounts,
  type AssessmentLevel,
  type AssessmentReport,
  type AssessmentRemediation,
  type AssessmentRunSummary,
} from "@/lib/api/pipelines"

/** Fired after a run is saved so the tab badge re-reads the newest counts. */
export const ASSESSMENT_UPDATED_EVENT = "rsync:assessment_updated"

function emitAssessmentUpdated(pipelineId: string) {
  if (typeof window === "undefined") return
  window.dispatchEvent(new CustomEvent(ASSESSMENT_UPDATED_EVENT, { detail: { pipelineId } }))
}

const LEVELS: AssessmentLevel[] = ["critical", "high", "medium", "low"]

const LEVEL_LABEL: Record<AssessmentLevel, string> = {
  critical: "Critical",
  high: "High",
  medium: "Medium",
  low: "Low",
}

const LEVEL_BADGE: Record<AssessmentLevel, string> = {
  critical: "border-red-200 bg-red-50 text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300",
  high: "border-orange-200 bg-orange-50 text-orange-700 dark:border-orange-900 dark:bg-orange-950/40 dark:text-orange-300",
  medium: "border-amber-200 bg-amber-50 text-amber-800 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-300",
  low: "border-sky-200 bg-sky-50 text-sky-700 dark:border-sky-900 dark:bg-sky-950/40 dark:text-sky-300",
}

const RESULT_LABEL: Record<AssessmentCheckResult, string> = {
  failed: "Failed",
  warning: "Warning",
  info: "Note",
  passed: "Passed",
}

const CATEGORY_LABEL: Record<AssessmentCheckCategory, string> = {
  source: "Source",
  tables: "Tables",
  destination: "Destination",
}

const TRIGGER_LABEL: Record<string, string> = {
  manual: "Run by hand",
  run_gate: "Before a run",
  scheduled: "Scheduled re-check",
}

// Issues first (by level), then notes, then passes — the order a reader works
// through them in. The server sorts by level first, which would put a passed
// Critical check above a failed High one.
const RESULT_RANK: Record<AssessmentCheckResult, number> = { failed: 0, warning: 0, info: 1, passed: 2 }
const LEVEL_RANK: Record<AssessmentLevel, number> = { critical: 0, high: 1, medium: 2, low: 3 }

export function sortChecks(checks: AssessmentCheck[]): AssessmentCheck[] {
  return [...checks].sort(
    (a, b) =>
      (RESULT_RANK[a.result] ?? 3) - (RESULT_RANK[b.result] ?? 3) ||
      (LEVEL_RANK[a.level] ?? 4) - (LEVEL_RANK[b.level] ?? 4) ||
      a.title.localeCompare(b.title),
  )
}

type LevelFilter = "all" | AssessmentLevel
type ResultFilter = "all" | "issues" | AssessmentCheckResult
type CategoryFilter = "all" | AssessmentCheckCategory

export interface CheckFilters {
  search: string
  level: LevelFilter
  result: ResultFilter
  category: CategoryFilter
}

export function filterChecks(checks: AssessmentCheck[], f: CheckFilters): AssessmentCheck[] {
  const q = f.search.trim().toLowerCase()
  return checks.filter((c) => {
    if (f.level !== "all" && c.level !== f.level) return false
    if (f.result === "issues" && c.result !== "failed" && c.result !== "warning") return false
    if (f.result !== "all" && f.result !== "issues" && c.result !== f.result) return false
    if (f.category !== "all" && c.category !== f.category) return false
    if (!q) return true
    const haystack = [c.title, c.code, c.message, ...(c.objects ?? []).map((o) => `${o.name} ${o.message ?? ""}`)]
      .join(" ")
      .toLowerCase()
    return haystack.includes(q)
  })
}

function isIssue(c: AssessmentCheck) {
  return c.result === "failed" || c.result === "warning"
}

function ResultIcon({ check }: { check: AssessmentCheck }) {
  if (check.result === "passed") return <CheckCircle2 className="h-4 w-4 text-green-600 dark:text-green-400" aria-hidden />
  if (check.result === "info") return <Info className="h-4 w-4 text-sky-500" aria-hidden />
  if (check.level === "critical") return <AlertCircle className="h-4 w-4 text-red-600 dark:text-red-400" aria-hidden />
  return <AlertTriangle className="h-4 w-4 text-amber-500" aria-hidden />
}

function LevelBadge({ check }: { check: AssessmentCheck }) {
  // A pass keeps its level (what it would have been had it failed) but greyed
  // out, so a green row never reads as an open Critical.
  const muted = !isIssue(check)
  return (
    <Badge
      variant="outline"
      className={cn(
        "font-medium",
        muted ? "border-zinc-200 text-zinc-500 dark:border-zinc-700 dark:text-zinc-400" : LEVEL_BADGE[check.level],
      )}
    >
      {LEVEL_LABEL[check.level] ?? check.level}
    </Badge>
  )
}

function CopyBlock({ label, lines }: { label: string; lines: string[] }) {
  const [copied, setCopied] = useState(false)
  const text = lines.join("\n")
  return (
    <div>
      <div className="flex items-center justify-between">
        <p className="text-[11px] font-medium uppercase tracking-wide text-zinc-500 dark:text-zinc-400">{label}</p>
        <button
          type="button"
          className="inline-flex items-center gap-1 text-xs text-zinc-500 dark:text-zinc-400 hover:text-zinc-800 dark:hover:text-zinc-200"
          onClick={async () => {
            try {
              await navigator.clipboard.writeText(text)
              setCopied(true)
              setTimeout(() => setCopied(false), 1500)
            } catch {
              // Clipboard blocked (insecure origin, permissions): the text is
              // still selectable in the block below.
            }
          }}
          aria-label={`Copy ${label.toLowerCase()}`}
        >
          {copied ? <Check className="h-3.5 w-3.5" aria-hidden /> : <ClipboardCopy className="h-3.5 w-3.5" aria-hidden />}
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
      <pre className="mt-1 overflow-x-auto rounded bg-zinc-100 p-2 font-mono text-[11px] text-zinc-800 dark:bg-zinc-900 dark:text-zinc-200">
        {text}
      </pre>
    </div>
  )
}

function Remediation({ remediation }: { remediation?: AssessmentRemediation }) {
  if (!remediation) return null
  const steps = remediation.steps ?? []
  const sql = remediation.sql_to_run ?? []
  const commands = remediation.commands_to_run ?? []
  if (steps.length === 0 && sql.length === 0 && commands.length === 0 && !remediation.doc_url) return null
  const minutes = remediation.estimated_minutes ?? 0
  return (
    <div className="space-y-2">
      {steps.length > 0 && (
        <div>
          <p className="text-[11px] font-medium uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            What to do{minutes > 0 ? ` · about ${minutes} min` : ""}
          </p>
          <ol className="mt-1 list-inside list-decimal space-y-0.5 text-sm text-zinc-700 dark:text-zinc-300">
            {steps.map((s, i) => (
              <li key={i}>{s}</li>
            ))}
          </ol>
        </div>
      )}
      {sql.length > 0 && <CopyBlock label="SQL to run" lines={sql} />}
      {commands.length > 0 && <CopyBlock label="Commands to run" lines={commands} />}
      {remediation.doc_url && (
        <a
          href={remediation.doc_url}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-block text-sm text-blue-600 underline underline-offset-2 dark:text-blue-400"
        >
          Read the documentation →
        </a>
      )}
    </div>
  )
}

function CheckDetails({ check }: { check: AssessmentCheck }) {
  const objects = check.objects ?? []
  return (
    <div className="space-y-3 px-4 py-3">
      <p className="text-sm text-zinc-700 dark:text-zinc-300">{check.message}</p>
      {objects.length > 0 && (
        <div>
          <p className="text-[11px] font-medium uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            Affected {objects.length === 1 ? "object" : `objects (${objects.length})`}
          </p>
          <ul className="mt-1 space-y-1">
            {objects.map((o) => (
              <li key={o.name} className="text-sm">
                <span className="font-mono text-xs text-zinc-900 dark:text-zinc-100">{o.name}</span>
                {o.message && o.message !== check.message && (
                  <span className="text-zinc-600 dark:text-zinc-400"> — {o.message}</span>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}
      {isIssue(check) && <Remediation remediation={check.remediation} />}
      <p className="font-mono text-[11px] text-zinc-400">{check.code}</p>
    </div>
  )
}

const COUNT_CARDS: {
  key: keyof AssessmentCounts
  label: string
  tone: string
  filter: Partial<CheckFilters>
}[] = [
  { key: "critical", label: "Critical", tone: "text-red-600 dark:text-red-400", filter: { level: "critical", result: "issues" } },
  { key: "high", label: "High", tone: "text-orange-600 dark:text-orange-400", filter: { level: "high", result: "issues" } },
  { key: "medium", label: "Medium", tone: "text-amber-600 dark:text-amber-400", filter: { level: "medium", result: "issues" } },
  { key: "low", label: "Low", tone: "text-sky-600 dark:text-sky-400", filter: { level: "low", result: "issues" } },
  { key: "passed", label: "Passed", tone: "text-green-600 dark:text-green-400", filter: { level: "all", result: "passed" } },
]

const DEFAULT_FILTERS: CheckFilters = { search: "", level: "all", result: "all", category: "all" }

const selectClass =
  "h-9 rounded-md border border-zinc-300 bg-white px-2 text-sm dark:border-zinc-700 dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-blue-500"

function CountsBar({
  counts,
  onPick,
}: {
  counts: AssessmentCounts
  onPick: (f: Partial<CheckFilters>) => void
}) {
  return (
    <div className="grid grid-cols-2 gap-2 sm:grid-cols-5">
      {COUNT_CARDS.map((c) => (
        <button
          key={c.key}
          type="button"
          onClick={() => onPick(c.filter)}
          className="rounded-lg border border-zinc-200 px-3 py-2 text-left transition-colors hover:bg-zinc-50 dark:border-zinc-800 dark:hover:bg-zinc-900"
          aria-label={`${counts[c.key]} ${c.label.toLowerCase()} — show these checks`}
        >
          <div className={cn("text-2xl font-semibold tabular-nums", counts[c.key] > 0 ? c.tone : "text-zinc-400")}>
            {counts[c.key]}
          </div>
          <div className="text-xs text-zinc-500 dark:text-zinc-400">{c.label}</div>
        </button>
      ))}
    </div>
  )
}

function RunHistory({
  runs,
  selectedId,
  onSelect,
}: {
  runs: AssessmentRunSummary[]
  selectedId: string | undefined
  onSelect: (id: string) => void
}) {
  if (runs.length === 0) return null
  return (
    <section aria-labelledby="assessment-history-heading" className="space-y-2">
      <h3 id="assessment-history-heading" className="text-sm font-semibold">
        History
      </h3>
      <ul className="divide-y divide-zinc-200 rounded-lg border border-zinc-200 dark:divide-zinc-800 dark:border-zinc-800">
        {runs.map((r, i) => (
          <li key={r.id}>
            <button
              type="button"
              onClick={() => onSelect(r.id)}
              aria-current={r.id === selectedId ? "true" : undefined}
              className={cn(
                "flex w-full flex-wrap items-center gap-x-3 gap-y-1 px-3 py-2 text-left text-sm hover:bg-zinc-50 dark:hover:bg-zinc-900",
                r.id === selectedId && "bg-zinc-100 dark:bg-zinc-800/60",
              )}
            >
              <span className="min-w-[10rem] text-zinc-700 dark:text-zinc-300">
                <LocalDateTime value={r.created_at} />
                {i === 0 && <span className="ml-1 text-xs text-zinc-400">(latest)</span>}
              </span>
              <span className="text-xs text-zinc-500 dark:text-zinc-400">{TRIGGER_LABEL[r.trigger] ?? r.trigger}</span>
              <span className="ml-auto flex items-center gap-2 text-xs tabular-nums">
                {r.counts.critical > 0 && <span className="text-red-600 dark:text-red-400">{r.counts.critical} critical</span>}
                {r.counts.high > 0 && <span className="text-orange-600 dark:text-orange-400">{r.counts.high} high</span>}
                {r.counts.medium > 0 && <span className="text-amber-600 dark:text-amber-400">{r.counts.medium} medium</span>}
                {r.counts.low > 0 && <span className="text-sky-600 dark:text-sky-400">{r.counts.low} low</span>}
                <span className="text-green-600 dark:text-green-400">{r.counts.passed} passed</span>
              </span>
            </button>
          </li>
        ))}
      </ul>
    </section>
  )
}

export function AssessmentTab({
  pipelineId,
  pipelineType,
}: {
  pipelineId: string
  pipelineType: "etl" | "cdc"
}) {
  const [runs, setRuns] = useState<AssessmentRunSummary[]>([])
  const [current, setCurrent] = useState<{ run: AssessmentRunSummary; report: AssessmentReport } | null>(null)
  const [selectedId, setSelectedId] = useState<string>("latest")
  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [running, setRunning] = useState(false)
  const [runError, setRunError] = useState<string | null>(null)
  const [filters, setFilters] = useState<CheckFilters>(DEFAULT_FILTERS)
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})

  const load = useCallback(
    async (runId: string) => {
      setLoading(true)
      try {
        const [detail, history] = await Promise.all([
          getPipelineAssessment(pipelineId, runId),
          listPipelineAssessments(pipelineId),
        ])
        setCurrent(detail)
        setRuns(history)
        setLoadError(null)
      } catch (e) {
        setLoadError(e instanceof Error ? e.message : "Failed to load the assessment")
      } finally {
        setLoading(false)
      }
    },
    [pipelineId],
  )

  useEffect(() => {
    void load(selectedId)
  }, [load, selectedId])

  const runAssessment = useCallback(async () => {
    setRunning(true)
    setRunError(null)
    try {
      await assessPipeline(pipelineId)
      setExpanded({})
      emitAssessmentUpdated(pipelineId)
      if (selectedId === "latest") await load("latest")
      else setSelectedId("latest")
    } catch (e) {
      setRunError(e instanceof Error ? e.message : "Assessment failed")
    } finally {
      setRunning(false)
    }
  }, [pipelineId, selectedId, load])

  const checks = useMemo(() => sortChecks(current?.report.checks ?? []), [current])
  const visible = useMemo(() => filterChecks(checks, filters), [checks, filters])
  const counts = current?.report.counts ?? current?.run.counts
  const selectedRunId = current?.run.id

  const runButton = (
    <Button onClick={() => void runAssessment()} disabled={running} size="sm">
      {running ? <Loader2 className="h-4 w-4 animate-spin" /> : <PlayCircle className="h-4 w-4" />}
      {running ? "Running assessment…" : "Run assessment"}
    </Button>
  )

  const intro = (
    <p className="text-sm text-zinc-600 dark:text-zinc-400">
      Checks the source, each table and the destination before data moves. Only a failed{" "}
      <span className="font-medium text-red-600 dark:text-red-400">Critical</span> check stops the pipeline from
      starting; fix High and Medium issues before you rely on the data.
      {pipelineType === "cdc" &&
        " A running CDC pipeline is re-checked automatically, and a new Critical or High issue notifies the owner."}
    </p>
  )

  if (loading && !current && runs.length === 0) {
    return (
      <div className="flex items-center gap-2 py-8 text-sm text-zinc-500 dark:text-zinc-400">
        <Loader2 className="h-4 w-4 animate-spin" /> Loading assessment…
      </div>
    )
  }

  if (loadError && !current) {
    return (
      <div className="space-y-3 py-4">
        <p className="text-sm text-red-600 dark:text-red-400">{loadError}</p>
        <Button variant="outline" size="sm" onClick={() => void load(selectedId)}>
          Try again
        </Button>
      </div>
    )
  }

  if (!current) {
    return (
      <div className="space-y-4">
        {intro}
        <div className="flex flex-col items-center gap-3 rounded-lg border border-dashed border-zinc-300 py-10 text-center dark:border-zinc-700">
          <ShieldCheck className="h-8 w-8 text-zinc-400" aria-hidden />
          <p className="text-sm text-zinc-600 dark:text-zinc-400">This pipeline has not been assessed yet.</p>
          {runButton}
          {runError && <p className="text-sm text-red-600 dark:text-red-400">{runError}</p>}
        </div>
      </div>
    )
  }

  const { run, report } = current
  const isLatest = runs.length === 0 || runs[0]?.id === run.id

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 flex-1 space-y-1">
          <h2 className="text-base font-semibold">Pre-migration assessment</h2>
          {intro}
        </div>
        {runButton}
      </div>
      {runError && <p className="text-sm text-red-600 dark:text-red-400">{runError}</p>}

      <div
        className={cn(
          "flex flex-wrap items-center gap-x-3 gap-y-1 rounded-lg border px-3 py-2 text-sm",
          run.blocking
            ? "border-red-200 bg-red-50 text-red-800 dark:border-red-900 dark:bg-red-950/30 dark:text-red-200"
            : "border-green-200 bg-green-50 text-green-800 dark:border-green-900 dark:bg-green-950/30 dark:text-green-200",
        )}
        role="status"
      >
        <span className="font-medium">
          {run.blocking ? "Start blocked — fix the Critical checks below" : "Nothing blocks the start"}
        </span>
        <span className="text-xs opacity-80">
          {isLatest ? "Latest run" : "Older run"} · <LocalDateTime value={run.created_at} /> ·{" "}
          {TRIGGER_LABEL[run.trigger] ?? run.trigger}
          {report.source_connector_type && report.destination_connector_type && (
            <>
              {" · "}
              <span className="font-mono">
                {getConnectorDisplayName(report.source_connector_type)} → {getConnectorDisplayName(report.destination_connector_type)}
              </span>
            </>
          )}
        </span>
        {!isLatest && (
          <button
            type="button"
            className="ml-auto text-xs underline underline-offset-2"
            onClick={() => setSelectedId("latest")}
          >
            Show the latest run
          </button>
        )}
      </div>

      {counts && <CountsBar counts={counts} onPick={(f) => setFilters((prev) => ({ ...prev, ...f }))} />}

      {checks.length === 0 ? (
        <p className="text-sm text-zinc-600 dark:text-zinc-400">
          This run was saved without graded checks. Run the assessment again to see them.
        </p>
      ) : (
        <section aria-labelledby="assessment-checks-heading" className="space-y-3">
          <h3 id="assessment-checks-heading" className="sr-only">
            Checks
          </h3>
          <div className="flex flex-wrap items-center gap-2">
            <Input
              value={filters.search}
              onChange={(e) => setFilters((prev) => ({ ...prev, search: e.target.value }))}
              placeholder="Search checks, tables or codes"
              aria-label="Search checks"
              className="h-9 w-full sm:w-64"
            />
            <select
              aria-label="Filter by level"
              className={selectClass}
              value={filters.level}
              onChange={(e) => setFilters((prev) => ({ ...prev, level: e.target.value as LevelFilter }))}
            >
              <option value="all">All levels</option>
              {LEVELS.map((l) => (
                <option key={l} value={l}>
                  {LEVEL_LABEL[l]}
                </option>
              ))}
            </select>
            <select
              aria-label="Filter by result"
              className={selectClass}
              value={filters.result}
              onChange={(e) => setFilters((prev) => ({ ...prev, result: e.target.value as ResultFilter }))}
            >
              <option value="all">All results</option>
              <option value="issues">Issues only</option>
              <option value="failed">Failed</option>
              <option value="warning">Warning</option>
              <option value="info">Note</option>
              <option value="passed">Passed</option>
            </select>
            <select
              aria-label="Filter by category"
              className={selectClass}
              value={filters.category}
              onChange={(e) => setFilters((prev) => ({ ...prev, category: e.target.value as CategoryFilter }))}
            >
              <option value="all">All categories</option>
              <option value="source">Source</option>
              <option value="tables">Tables</option>
              <option value="destination">Destination</option>
            </select>
            {(filters.search || filters.level !== "all" || filters.result !== "all" || filters.category !== "all") && (
              <Button variant="ghost" size="sm" onClick={() => setFilters(DEFAULT_FILTERS)}>
                Clear filters
              </Button>
            )}
            <span className="ml-auto text-xs text-zinc-500 dark:text-zinc-400">
              {visible.length} of {checks.length} checks
            </span>
          </div>

          <div className="overflow-x-auto rounded-lg border border-zinc-200 dark:border-zinc-800">
            <table className="w-full min-w-[640px] text-sm">
              <thead className="bg-zinc-50 text-left text-xs text-zinc-500 dark:text-zinc-400 dark:bg-zinc-900">
                <tr>
                  <th className="w-8 px-3 py-2" aria-label="Expand" />
                  <th className="px-3 py-2 font-medium">Check</th>
                  <th className="px-3 py-2 font-medium">Level</th>
                  <th className="px-3 py-2 font-medium">Result</th>
                  <th className="px-3 py-2 font-medium">Category</th>
                  <th className="px-3 py-2 font-medium">Objects</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-200 dark:divide-zinc-800">
                {visible.length === 0 && (
                  <tr>
                    <td colSpan={6} className="px-3 py-6 text-center text-zinc-500 dark:text-zinc-400">
                      No checks match these filters.
                    </td>
                  </tr>
                )}
                {visible.map((c) => {
                  const key = `${c.code}|${c.result}|${c.level}|${c.category}`
                  const open = !!expanded[key]
                  return (
                    <Fragment key={key}>
                      <tr
                        className="cursor-pointer hover:bg-zinc-50 dark:hover:bg-zinc-900/60"
                        onClick={() => setExpanded((prev) => ({ ...prev, [key]: !open }))}
                      >
                        <td className="px-3 py-2 align-top">
                          <button
                            type="button"
                            aria-expanded={open}
                            aria-label={`${open ? "Hide" : "Show"} details for ${c.title}`}
                            onClick={(e) => {
                              e.stopPropagation()
                              setExpanded((prev) => ({ ...prev, [key]: !open }))
                            }}
                            className="text-zinc-500 dark:text-zinc-400"
                          >
                            {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                          </button>
                        </td>
                        <td className="px-3 py-2 align-top">
                          <div className="flex items-start gap-2">
                            <ResultIcon check={c} />
                            <div className="min-w-0">
                              <div className="font-medium text-zinc-900 dark:text-zinc-100">{c.title}</div>
                              {!open && (
                                <div className="line-clamp-1 text-xs text-zinc-500 dark:text-zinc-400">{c.message}</div>
                              )}
                            </div>
                          </div>
                        </td>
                        <td className="px-3 py-2 align-top">
                          <LevelBadge check={c} />
                        </td>
                        <td className="px-3 py-2 align-top text-zinc-700 dark:text-zinc-300">
                          {RESULT_LABEL[c.result] ?? c.result}
                        </td>
                        <td className="px-3 py-2 align-top text-zinc-700 dark:text-zinc-300">
                          {CATEGORY_LABEL[c.category] ?? c.category}
                        </td>
                        <td className="px-3 py-2 align-top tabular-nums text-zinc-700 dark:text-zinc-300">
                          {c.objects?.length ?? 0}
                        </td>
                      </tr>
                      {open && (
                        <tr className="bg-zinc-50/60 dark:bg-zinc-900/40">
                          <td />
                          <td colSpan={5}>
                            <CheckDetails check={c} />
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  )
                })}
              </tbody>
            </table>
          </div>
        </section>
      )}

      <RunHistory runs={runs} selectedId={selectedRunId} onSelect={(id) => setSelectedId(id)} />
    </div>
  )
}

/**
 * The tab trigger's count: the newest run's open Critical issues (red), or its
 * High issues (amber) when nothing is Critical. Reads the history list (counts
 * only), not the full report.
 */
export function AssessmentTabBadge({ pipelineId }: { pipelineId: string }) {
  const [counts, setCounts] = useState<AssessmentCounts | null>(null)

  const refresh = useCallback(async () => {
    try {
      const [latest] = await listPipelineAssessments(pipelineId, 1)
      setCounts(latest?.counts ?? null)
    } catch {
      setCounts(null)
    }
  }, [pipelineId])

  useEffect(() => {
    void refresh()
    if (typeof window === "undefined") return
    const onUpdate = (evt: Event) => {
      const pid = String((evt as CustomEvent).detail?.pipelineId ?? "")
      if (pid === pipelineId) void refresh()
    }
    window.addEventListener(ASSESSMENT_UPDATED_EVENT, onUpdate)
    return () => window.removeEventListener(ASSESSMENT_UPDATED_EVENT, onUpdate)
  }, [pipelineId, refresh])

  if (!counts) return null
  if (counts.critical > 0) {
    return (
      <span
        className="ml-1.5 rounded-full bg-red-600 px-1.5 text-[10px] font-semibold leading-4 text-white"
        aria-label={`${counts.critical} critical`}
      >
        {counts.critical}
      </span>
    )
  }
  if (counts.high > 0) {
    return (
      <span
        className="ml-1.5 rounded-full bg-amber-500 px-1.5 text-[10px] font-semibold leading-4 text-amber-950"
        aria-label={`${counts.high} high`}
      >
        {counts.high}
      </span>
    )
  }
  return null
}
