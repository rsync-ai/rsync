"use client"

import { useCallback, useEffect, useId, useMemo, useState } from "react"
import Link from "next/link"
import { toast } from "sonner"
import { authGet } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"
import { meetsRole } from "@/lib/workspace/roles"
import {
  deleteTransform,
  deIdentifyWarning,
  idsOf,
  isDeIdentifying,
  partialFailureMessage,
  setTransformEnabled,
} from "@/lib/pipeline/transformMutations"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Switch } from "@/components/ui/switch"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import { TransformExecutionLog, TransformExecutionLogsPanel } from "@/components/transforms/TransformExecutionLogsPanel"
import { TransformMonitoringPanel } from "@/components/transforms/TransformMonitoringPanel"
import {
  ChevronDown,
  ChevronRight,
  CircleCheck,
  Search,
  Copy,
  Check,
  Loader2,
  ShieldAlert,
  Trash2,
} from "lucide-react"
import {
  transformTypeLabel,
  transformTypeBlurb,
  transformTypeTone,
  transformTypeVerb,
  describeTransform,
} from "@/lib/transform-display"

type TransformDefinition = {
  id: string
  pipeline_id: string
  transform_type: string
  transform_order: number
  transform_config: Record<string, any>
  enabled: boolean
  created_at?: string
  updated_at?: string
}

type PipelineTransformsResponse = {
  pipeline_id: string
  producer_transforms: TransformDefinition[]
  consumer_transforms: TransformDefinition[]
}

type ExecutionsResponse = {
  executions: Array<{ id: string }>
}

// A single user-facing transform after merging the producer (batch) and
// consumer (CDC) rows. The creation flow persists every transform twice with a
// byte-identical config (SuggestionsReviewDialog.handleApply), so we collapse
// the two side-specific rows into one logical entry keyed by config and surface
// which execution paths it runs on via `appliesTo`.
type LogicalTransform = {
  key: string
  operation: string
  config: Record<string, any>
  appliesTo: Array<"batch" | "cdc">
  enabled: boolean
  order: number
  ids: { batch?: string; cdc?: string }
}

type TransformGroup = {
  operation: string
  items: LogicalTransform[]
  enabledCount: number
}

// Deterministic JSON stringify (sorted keys) so two configs that differ only in
// key order still hash identically — the producer/consumer pair must collapse.
function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`
  const obj = value as Record<string, unknown>
  const keys = Object.keys(obj).sort()
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(",")}}`
}

function operationOf(t: TransformDefinition): string {
  return (t.transform_config?.operation as string) || "transform"
}

// Merge producer + consumer rows into one logical list. Rows with an identical
// config collapse into a single entry whose `appliesTo` records every side it
// was found on (batch from producer, cdc from consumer).
function mergeTransforms(resp: PipelineTransformsResponse | null): LogicalTransform[] {
  const byKey = new Map<string, LogicalTransform>()

  const ingest = (rows: TransformDefinition[], side: "batch" | "cdc") => {
    for (const t of rows) {
      const operation = operationOf(t)
      const key = `${operation}::${stableStringify(t.transform_config ?? {})}`
      const existing = byKey.get(key)
      if (existing) {
        if (!existing.appliesTo.includes(side)) existing.appliesTo.push(side)
        existing.enabled = existing.enabled && t.enabled
        existing.order = Math.min(existing.order, t.transform_order)
        existing.ids[side] = t.id
      } else {
        byKey.set(key, {
          key,
          operation,
          config: t.transform_config ?? {},
          appliesTo: [side],
          enabled: t.enabled,
          order: t.transform_order,
          ids: { [side]: t.id },
        })
      }
    }
  }

  ingest(resp?.producer_transforms ?? [], "batch")
  ingest(resp?.consumer_transforms ?? [], "cdc")

  return Array.from(byKey.values()).sort((a, b) => a.order - b.order)
}

function appliesToLabel(appliesTo: Array<"batch" | "cdc">): string {
  const hasBatch = appliesTo.includes("batch")
  const hasCdc = appliesTo.includes("cdc")
  if (hasBatch && hasCdc) return "Batch + CDC"
  if (hasBatch) return "Batch only"
  if (hasCdc) return "CDC only"
  return ""
}

export function PipelineTransformsTab({ pipelineId }: { pipelineId: string }) {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [transforms, setTransforms] = useState<PipelineTransformsResponse | null>(null)
  const [latestExecutionId, setLatestExecutionId] = useState<string | null>(null)
  const [logs, setLogs] = useState<TransformExecutionLog[]>([])
  const [query, setQuery] = useState("")
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({})
  const [reloadKey, setReloadKey] = useState(0)

  // Mirrors requireTransformWorkspaceRole(..., security.WSMember) on both
  // PUT /transforms/:id and DELETE /transforms/:id (transforms.go:551, :614).
  // `role` is "" while the workspace context is loading, and meetsRole fails
  // closed on that, so the controls never flash enabled for a viewer.
  const { role } = useWorkspaceRole()
  const canEdit = meetsRole(role, "member")

  // Every mutation refetches rather than patching local state: the two rows
  // behind one logical transform are merged on read, so the honest way to show
  // the result of a half-applied change is to re-read it.
  const refetch = useCallback(() => setReloadKey((k) => k + 1), [])

  useEffect(() => {
    let cancelled = false
    const run = async () => {
      setLoading(true)
      setError(null)
      try {
        // Transforms and the latest-execution lookup are independent — fetch them
        // in parallel; only the per-run logs depend on the resolved execution id.
        const [t, ex] = await Promise.all([
          authGet<PipelineTransformsResponse>(API_ENDPOINTS.TRANSFORMS.PIPELINE(pipelineId)),
          authGet<ExecutionsResponse>(`${API_ENDPOINTS.EXECUTIONS.LIST}?pipeline_id=${encodeURIComponent(pipelineId)}&limit=1`),
        ])
        if (cancelled) return
        setTransforms(t)
        const execId = ex?.executions?.[0]?.id || null
        setLatestExecutionId(execId)

        if (execId) {
          const res = await authGet<{ logs: TransformExecutionLog[] }>(API_ENDPOINTS.EXECUTIONS.TRANSFORMS(execId))
          if (cancelled) return
          setLogs(Array.isArray(res?.logs) ? res.logs : [])
        } else {
          setLogs([])
        }
      } catch (e: any) {
        if (cancelled) return
        setError(String(e?.message || e))
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    run()
    return () => {
      cancelled = true
    }
  }, [pipelineId, reloadKey])

  // Merge producer/consumer into one logical list, apply the search filter, then
  // group by operation while preserving execution order.
  const merged = useMemo(() => mergeTransforms(transforms), [transforms])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return merged
    return merged.filter((lt) => {
      const { primary, secondary } = describeTransform(lt.operation, lt.config)
      const haystack = `${transformTypeLabel(lt.operation)} ${lt.operation} ${primary} ${secondary ?? ""}`.toLowerCase()
      return haystack.includes(q)
    })
  }, [merged, query])

  const groups = useMemo<TransformGroup[]>(() => {
    const order: string[] = []
    const byOp = new Map<string, LogicalTransform[]>()
    for (const lt of filtered) {
      if (!byOp.has(lt.operation)) {
        byOp.set(lt.operation, [])
        order.push(lt.operation)
      }
      byOp.get(lt.operation)!.push(lt)
    }
    return order.map((operation) => {
      const items = byOp.get(operation)!
      return { operation, items, enabledCount: items.filter((i) => i.enabled).length }
    })
  }, [filtered])

  // One-line impact summary across all logical transforms (pre-filter, so the
  // banner always reflects the full pipeline, not the current search).
  const summary = useMemo(() => {
    const counts = new Map<string, number>()
    let disabled = 0
    let batchAndCdc = true
    for (const lt of merged) {
      counts.set(lt.operation, (counts.get(lt.operation) || 0) + 1)
      if (!lt.enabled) disabled++
      if (!(lt.appliesTo.includes("batch") && lt.appliesTo.includes("cdc"))) batchAndCdc = false
    }
    return { total: merged.length, counts, disabled, batchAndCdc }
  }, [merged])

  const allCollapsed = groups.length > 0 && groups.every((g) => collapsed[g.operation])
  const toggleAll = () => {
    if (allCollapsed) {
      setCollapsed({})
    } else {
      const next: Record<string, boolean> = {}
      for (const g of groups) next[g.operation] = true
      setCollapsed(next)
    }
  }

  // When searching, force-open any group with matches so results are visible.
  const isOpen = (operation: string) => (query.trim() ? true : !collapsed[operation])

  const hasConfigured = merged.length > 0

  return (
    <div className="space-y-6">
      {/* Configured transforms + latest-run detail come from one fetch. Its
          loading/error is isolated to this block so a hiccup here can't blank the
          self-fetching run-level monitoring panel rendered below. */}
      {loading ? (
        <div role="status" aria-live="polite" className="text-sm text-zinc-500 dark:text-zinc-400">
          Loading transforms…
        </div>
      ) : error ? (
        <div role="alert" className="rounded-lg border border-red-200 dark:border-red-900/50 bg-red-50 dark:bg-red-950/20 p-4">
          <div className="text-sm text-red-700 dark:text-red-300">Couldn&apos;t load configured transforms.</div>
          <div className="mt-1 text-xs text-red-600/80 dark:text-red-400/80 break-all">{error}</div>
          <Button variant="outline" size="sm" className="mt-2" onClick={() => setReloadKey((k) => k + 1)}>
            Try again
          </Button>
        </div>
      ) : (
        <>
          <div className="space-y-3">
            <div className="flex items-center justify-between gap-3">
              <div className="text-sm font-semibold text-zinc-900 dark:text-white">Configured transforms</div>
              {/* This tab shows what is configured and what each run did with it;
                  the builder is the only place a rule can be authored. Without this
                  link the two pages had nothing pointing at each other, and an
                  operator who wanted to add a transform had no route from here. */}
              {canEdit ? (
                <Link
                  href={`/transforms?pipeline=${encodeURIComponent(pipelineId)}`}
                  className="shrink-0 text-xs text-blue-600 hover:underline dark:text-blue-400"
                >
                  Edit in Transform Builder
                </Link>
              ) : null}
            </div>

            {!hasConfigured ? (
              <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-4 text-sm text-zinc-500 dark:text-zinc-400">
                No transforms configured. rsync.ai didn&apos;t detect any columns that need shaping.
              </div>
            ) : (
              <>
                <SummaryBanner summary={summary} />

                <div className="flex items-center gap-2">
                  <div className="relative flex-1">
                    <Search aria-hidden="true" className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-zinc-400" />
                    <Input
                      value={query}
                      onChange={(e) => setQuery(e.target.value)}
                      placeholder="Find a column or transform…"
                      className="pl-8 h-9"
                    />
                  </div>
                  <Button variant="outline" size="sm" onClick={toggleAll}>
                    {allCollapsed ? "Expand all" : "Collapse all"}
                  </Button>
                </div>

                {groups.length === 0 ? (
                  <div className="text-sm text-zinc-500 dark:text-zinc-400 px-1">No transforms match &ldquo;{query}&rdquo;.</div>
                ) : (
                  <div className="space-y-2">
                    {groups.map((group) => (
                      <TransformGroupCard
                        key={group.operation}
                        group={group}
                        open={isOpen(group.operation)}
                        canEdit={canEdit}
                        onChanged={refetch}
                        onToggle={() =>
                          setCollapsed((prev) => ({ ...prev, [group.operation]: !prev[group.operation] }))
                        }
                      />
                    ))}
                  </div>
                )}
              </>
            )}
          </div>

          <div className="space-y-2">
            <div className="flex items-center justify-between gap-3">
              <div className="min-w-0">
                <div className="text-sm font-semibold text-zinc-900 dark:text-white">Latest run · per-table</div>
                <div className="text-xs text-zinc-500 dark:text-zinc-400">
                  Per-table transform results for the most recent run. Run-level totals are in Transform run history below.
                </div>
              </div>
              {latestExecutionId ? (
                <Link
                  href={`/executions/${latestExecutionId}`}
                  className="shrink-0 font-mono text-xs text-blue-600 hover:underline dark:text-blue-400"
                  title={`Open execution ${latestExecutionId}`}
                >
                  {latestExecutionId.slice(0, 8)}
                </Link>
              ) : (
                <div className="shrink-0 text-xs text-zinc-500 dark:text-zinc-400">No executions yet</div>
              )}
            </div>

            <TransformExecutionLogsPanel logs={logs} emptyLabel="No transform execution logs for the latest run." />
          </div>
        </>
      )}

      {/* Run-level rollup + configuration history — self-fetching, always mounted. */}
      <TransformMonitoringPanel pipelineId={pipelineId} />
    </div>
  )
}

function SummaryBanner({
  summary,
}: {
  summary: { total: number; counts: Map<string, number>; disabled: number; batchAndCdc: boolean }
}) {
  const parts = Array.from(summary.counts.entries()).map(
    ([op, n]) => `${n} ${transformTypeVerb(op).toLowerCase()}`,
  )
  const enabled = summary.total - summary.disabled
  return (
    <div className="flex flex-wrap items-center gap-3 rounded-lg border border-teal-200 dark:border-teal-900/60 bg-teal-50 dark:bg-teal-950/30 px-4 py-3">
      <CircleCheck className="h-5 w-5 text-teal-600 dark:text-teal-400 shrink-0" />
      <div className="min-w-0 flex-1">
        <div className="text-sm font-medium text-teal-900 dark:text-teal-100">
          Auto-applied on import — no action needed
        </div>
        <div className="text-xs text-teal-700 dark:text-teal-300">
          {enabled} {enabled === 1 ? "transform" : "transforms"} active
          {summary.batchAndCdc ? " · applied to batch + CDC" : ""}
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-1.5">
        {parts.map((p, i) => (
          <Badge key={i} variant="secondary" className="font-normal">
            {p}
          </Badge>
        ))}
        {summary.disabled > 0 && (
          <Badge className="bg-amber-100 text-amber-800 dark:bg-amber-950/40 dark:text-amber-300 font-normal">
            {summary.disabled} disabled
          </Badge>
        )}
      </div>
    </div>
  )
}

function TransformGroupCard({
  group,
  open,
  onToggle,
  canEdit,
  onChanged,
}: {
  group: TransformGroup
  open: boolean
  onToggle: () => void
  canEdit: boolean
  onChanged: () => void
}) {
  const tone = transformTypeTone(group.operation)
  const blurb = transformTypeBlurb(group.operation)
  const contentId = useId()
  const groupAppliesTo = useMemo(() => {
    const sides = new Set<"batch" | "cdc">()
    for (const i of group.items) for (const s of i.appliesTo) sides.add(s)
    return appliesToLabel(Array.from(sides))
  }, [group.items])

  return (
    <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 overflow-hidden">
      <button
        type="button"
        onClick={onToggle}
        aria-expanded={open}
        aria-controls={contentId}
        className="flex w-full items-center gap-3 p-3 text-left hover:bg-zinc-50 dark:hover:bg-zinc-900/40"
      >
        {open ? (
          <ChevronDown aria-hidden="true" className="h-4 w-4 text-zinc-500 dark:text-zinc-400 shrink-0" />
        ) : (
          <ChevronRight aria-hidden="true" className="h-4 w-4 text-zinc-500 dark:text-zinc-400 shrink-0" />
        )}
        <span aria-hidden="true" className={`h-2 w-2 rounded-full shrink-0 ${tone.dot}`} />
        <div className="min-w-0 flex-1">
          <div className="text-sm font-medium text-zinc-900 dark:text-white">
            {transformTypeLabel(group.operation)}
          </div>
          {blurb && <div className="text-xs text-zinc-500 dark:text-zinc-400 truncate">{blurb}</div>}
        </div>
        <Badge variant="secondary" className="font-normal">
          {group.items.length}
        </Badge>
        <Badge className="bg-teal-50 text-teal-700 dark:bg-teal-950/40 dark:text-teal-300 font-normal">
          Auto
        </Badge>
        {groupAppliesTo && (
          <Badge variant="outline" className="font-normal">
            {groupAppliesTo}
          </Badge>
        )}
      </button>

      {open && (
        <div id={contentId} className="border-t border-zinc-200 dark:border-zinc-800">
          {group.items.map((lt) => (
            <TransformRow key={lt.key} lt={lt} tone={tone} canEdit={canEdit} onChanged={onChanged} />
          ))}
        </div>
      )}
    </div>
  )
}

function TransformRow({
  lt,
  tone,
  canEdit,
  onChanged,
}: {
  lt: LogicalTransform
  tone: { badge: string; dot: string }
  canEdit: boolean
  onChanged: () => void
}) {
  const [open, setOpen] = useState(false)
  const [copied, setCopied] = useState(false)
  const [busy, setBusy] = useState(false)
  // Which confirmation is open, if any. A masking rule gets one for BOTH
  // actions; everything else only for delete, which is not recoverable here.
  const [confirm, setConfirm] = useState<null | "disable" | "delete">(null)
  const { primary, secondary } = describeTransform(lt.operation, lt.config)
  const sideLabel = appliesToLabel(lt.appliesTo)
  const rowCount = idsOf(lt.ids).length
  const sensitive = isDeIdentifying(lt.operation)

  const copyId = () => {
    const id = lt.ids.batch || lt.ids.cdc || ""
    if (!id) return
    void navigator.clipboard?.writeText(id).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    })
  }

  const applyEnabled = async (enabled: boolean) => {
    setBusy(true)
    try {
      const out = await setTransformEnabled(lt.ids, enabled)
      if (out.ok) {
        toast.success(
          enabled
            ? `Enabled — ${primary} runs again from the next execution.`
            : `Disabled — ${primary} is skipped from the next execution.`
        )
        onChanged()
      } else {
        toast.error(partialFailureMessage(out.applied, rowCount, out.error))
        // Re-read even on failure: a partial fan-out changed real state.
        if (out.applied > 0) onChanged()
      }
    } finally {
      setBusy(false)
      setConfirm(null)
    }
  }

  const applyDelete = async () => {
    setBusy(true)
    try {
      const out = await deleteTransform(lt.ids)
      if (out.ok) {
        toast.success(`Deleted — ${primary} is gone from this pipeline.`)
        onChanged()
      } else {
        toast.error(partialFailureMessage(out.applied, rowCount, out.error))
        if (out.applied > 0) onChanged()
      }
    } finally {
      setBusy(false)
      setConfirm(null)
    }
  }

  // A masking rule being switched OFF is the only toggle that needs asking
  // about; switching one back on restores protection and goes straight through.
  const onSwitch = (next: boolean) => {
    if (!next && sensitive) {
      setConfirm("disable")
      return
    }
    void applyEnabled(next)
  }

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <div className="flex items-center gap-3 px-3 py-2.5 pl-9 border-b border-zinc-100 dark:border-zinc-900 last:border-b-0">
        <div className="min-w-0 flex-1">
          <span className="text-sm font-medium text-zinc-900 dark:text-white">{primary}</span>
          {secondary && (
            <span className="ml-2 text-xs font-mono text-zinc-500 dark:text-zinc-400">{secondary}</span>
          )}
        </div>
        <Badge className={`font-normal ${tone.badge}`}>{transformTypeVerb(lt.operation)}</Badge>
        {sideLabel && (
          <span className="text-[11px] text-zinc-500 dark:text-zinc-400">{sideLabel}</span>
        )}
        {canEdit ? (
          <div className="flex items-center gap-2">
            {busy && <Loader2 aria-hidden="true" className="h-3 w-3 animate-spin text-zinc-400" />}
            <Switch
              checked={lt.enabled}
              disabled={busy}
              onCheckedChange={onSwitch}
              aria-label={`${lt.enabled ? "Disable" : "Enable"} ${primary}`}
            />
            <Button
              variant="ghost"
              size="sm"
              className="h-7 w-7 p-0 text-zinc-400 hover:text-red-600 dark:hover:text-red-400"
              disabled={busy}
              onClick={() => setConfirm("delete")}
              aria-label={`Delete ${primary}`}
            >
              <Trash2 className="h-3.5 w-3.5" />
            </Button>
          </div>
        ) : (
          <span
            className={`text-xs ${lt.enabled ? "text-emerald-600 dark:text-emerald-400" : "text-zinc-400"}`}
          >
            {lt.enabled ? "enabled" : "disabled"}
          </span>
        )}
        <CollapsibleTrigger asChild>
          <Button variant="ghost" size="sm" className="h-7 px-2">
            {open ? <ChevronDown aria-hidden="true" className="h-4 w-4" /> : <ChevronRight aria-hidden="true" className="h-4 w-4" />}
            <span className="ml-1 text-xs">Details</span>
          </Button>
        </CollapsibleTrigger>
      </div>
      <CollapsibleContent className="px-3 pb-3 pl-9">
        <div className="flex items-center justify-between gap-2 mt-2 mb-1">
          <div className="text-[11px] text-zinc-400">
            {sideLabel} · runs as {lt.appliesTo.length === 2 ? "2 rows (producer + consumer)" : "1 row"}
          </div>
          <Button variant="ghost" size="sm" className="h-6 px-2 text-xs" onClick={copyId}>
            {copied ? <Check className="h-3.5 w-3.5 mr-1" /> : <Copy className="h-3.5 w-3.5 mr-1" />}
            {copied ? "Copied" : "Copy ID"}
          </Button>
        </div>
        <pre className="max-h-[260px] overflow-auto rounded-md bg-zinc-50 dark:bg-zinc-900 p-3 text-xs text-zinc-800 dark:text-zinc-200">
          {JSON.stringify(lt.config ?? {}, null, 2)}
        </pre>
      </CollapsibleContent>

      <AlertDialog open={confirm !== null} onOpenChange={(o) => !o && setConfirm(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2">
              {sensitive && <ShieldAlert className="h-4 w-4 text-red-600 dark:text-red-400" />}
              {confirm === "delete" ? "Delete this transform?" : "Turn off this masking rule?"}
            </AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-2">
                <div className="font-medium text-zinc-900 dark:text-zinc-100">
                  {primary}
                  {secondary ? <span className="ml-2 font-mono text-xs">{secondary}</span> : null}
                </div>
                {sensitive && (
                  <div className="rounded border border-red-200 bg-red-50 p-2 text-sm text-red-800 dark:border-red-900/60 dark:bg-red-950/30 dark:text-red-300">
                    {deIdentifyWarning(confirm === "delete" ? "delete" : "disable", lt.appliesTo)}
                  </div>
                )}
                <div>
                  {confirm === "delete"
                    ? `This removes ${rowCount === 2 ? "both rows (producer and consumer)" : "the row"} behind this transform. It cannot be undone from here.`
                    : `This changes ${rowCount === 2 ? "both rows (producer and consumer)" : "the row"}. You can switch it back on at any time.`}
                </div>
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={busy}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              disabled={busy}
              className={sensitive || confirm === "delete" ? "bg-red-600 hover:bg-red-700 text-white" : undefined}
              onClick={(e) => {
                e.preventDefault()
                if (confirm === "delete") void applyDelete()
                else void applyEnabled(false)
              }}
            >
              {busy && <Loader2 className="mr-2 h-3 w-3 animate-spin" />}
              {confirm === "delete" ? "Delete transform" : "Turn off masking"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </Collapsible>
  )
}
