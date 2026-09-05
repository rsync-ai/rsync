"use client"

import { useCallback, useEffect, useState } from "react"
import { AlertTriangle, Check, ChevronDown, ChevronUp, RefreshCw, X } from "lucide-react"
import { toast } from "sonner"
import { cn } from "@/lib/utils"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetchOrThrow } from "@/lib/api/auth-fetch"
import { classifyError } from "@/lib/utils/error-handling"
// SchemaChange is owned by the api client so this panel and the dedicated
// approval page (SchemaDriftApprovalList) can't drift on the shape.
import type { SchemaChange } from "@/lib/api/schema-changes"

interface SchemaEvolutionPanelProps {
  pipelineId: string
}

const RISK_COLORS: Record<string, string> = {
  drop_column: "text-red-600 bg-red-50 border-red-200 dark:text-red-400 dark:bg-red-950/30 dark:border-red-900",
  drop_table: "text-red-600 bg-red-50 border-red-200 dark:text-red-400 dark:bg-red-950/30 dark:border-red-900",
  modify_column: "text-amber-600 bg-amber-50 border-amber-200 dark:text-amber-400 dark:bg-amber-950/30 dark:border-amber-900",
  add_column: "text-emerald-600 bg-emerald-50 border-emerald-200 dark:text-emerald-400 dark:bg-emerald-950/30 dark:border-emerald-900",
  create_table: "text-emerald-600 bg-emerald-50 border-emerald-200 dark:text-emerald-400 dark:bg-emerald-950/30 dark:border-emerald-900",
}

const CHANGE_LABELS: Record<string, string> = {
  add_column: "Add Column",
  drop_column: "Drop Column",
  modify_column: "Modify Column",
  create_table: "Create Table",
  drop_table: "Drop Table",
}

export function SchemaEvolutionPanel({ pipelineId }: SchemaEvolutionPanelProps) {
  const [changes, setChanges] = useState<SchemaChange[]>([])
  const [loading, setLoading] = useState(true)
  const [actioning, setActioning] = useState<Record<string, boolean>>({})
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})
  const [dismissed, setDismissed] = useState(false)
  const [loadError, setLoadError] = useState<string | null>(null)

  const fetchChanges = useCallback(async () => {
    try {
      const res = await authFetchOrThrow(API_ENDPOINTS.PIPELINES.SCHEMA_CHANGES(pipelineId), {
        cache: "no-store",
      })
      const data = await res.json()
      setChanges(data.schema_changes ?? [])
      setLoadError(null)
    } catch (e) {
      // NOT best-effort: this panel is the only surface that says a pending
      // schema change is waiting on a human. Rendering nothing when the read
      // failed is indistinguishable from "there is nothing pending", which is
      // the answer that makes an operator walk away.
      setLoadError(classifyError(e, "general").message)
    } finally {
      setLoading(false)
    }
  }, [pipelineId])

  useEffect(() => {
    fetchChanges()
    const iv = setInterval(fetchChanges, 30_000)
    return () => clearInterval(iv)
  }, [fetchChanges])

  const handleAction = async (changeId: string, action: "approve" | "reject") => {
    setActioning((prev) => ({ ...prev, [changeId]: true }))
    try {
      const url =
        action === "approve"
          ? API_ENDPOINTS.PIPELINES.SCHEMA_CHANGE_APPROVE(pipelineId, changeId)
          : API_ENDPOINTS.PIPELINES.SCHEMA_CHANGE_REJECT(pipelineId, changeId)
      await authFetchOrThrow(url, { method: "POST" })
      toast.success(action === "approve" ? "Migration approved" : "Schema change rejected")
      await fetchChanges()
    } catch (e) {
      // A refused approval that re-renders the same pending card reads as "the
      // click didn't register" — so the operator clicks again, and again.
      const err = classifyError(e, "general")
      toast.error(
        action === "approve" ? "Could not approve migration" : "Could not reject schema change",
        { description: err.hint ?? err.message }
      )
    } finally {
      setActioning((prev) => ({ ...prev, [changeId]: false }))
    }
  }

  const pending = changes.filter((c) => c.status === "pending")
  const resolved = changes.filter((c) => c.status !== "pending")

  if (loading) return null
  // A failed read gets its own pixels. Falling through to the `changes.length
  // === 0` return below would render the exact same nothing as "no pending
  // schema changes" — the answer that ends the investigation.
  if (loadError && changes.length === 0) {
    return (
      <div className="rounded-lg border border-red-200 dark:border-red-900/60 bg-red-50/60 dark:bg-red-950/20 p-3 flex items-center gap-2">
        <AlertTriangle className="h-4 w-4 text-red-600 dark:text-red-400 shrink-0" />
        <span className="text-xs text-red-800 dark:text-red-300 flex-1 min-w-0">
          Could not load schema changes — {loadError}
        </span>
        <button
          type="button"
          onClick={fetchChanges}
          className="shrink-0 text-[11px] font-medium px-2 py-1 rounded border border-red-200 dark:border-red-900 text-red-700 dark:text-red-300 hover:bg-red-100 dark:hover:bg-red-900/40"
        >
          Retry
        </button>
      </div>
    )
  }
  if (changes.length === 0) return null
  if (dismissed && pending.length === 0) return null

  return (
    <div className="rounded-lg border border-amber-200 dark:border-amber-900/60 bg-amber-50/60 dark:bg-amber-950/20 p-3 space-y-2">
      {/* Header */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <AlertTriangle className="h-4 w-4 text-amber-600 dark:text-amber-400 shrink-0" />
          <span className="text-sm font-semibold text-amber-900 dark:text-amber-200">
            Schema Evolution
          </span>
          {pending.length > 0 && (
            <span className="text-[10px] font-bold px-1.5 py-0.5 rounded-full bg-amber-600 text-white">
              {pending.length} pending
            </span>
          )}
        </div>
        <div className="flex items-center gap-1">
          <button
            type="button"
            onClick={fetchChanges}
            className="p-1 rounded text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-900/40"
            title="Refresh"
          >
            <RefreshCw className="h-3.5 w-3.5" />
          </button>
          {pending.length === 0 && (
            <button
              type="button"
              onClick={() => setDismissed(true)}
              className="p-1 rounded text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-900/40"
              title="Dismiss"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      </div>

      {/* Pending changes */}
      {pending.length > 0 && (
        <div className="space-y-2">
          {pending.map((c) => (
            <SchemaChangeCard
              key={c.id}
              change={c}
              expanded={!!expanded[c.id]}
              actioning={!!actioning[c.id]}
              onToggleExpand={() => setExpanded((p) => ({ ...p, [c.id]: !p[c.id] }))}
              onApprove={() => handleAction(c.id, "approve")}
              onReject={() => handleAction(c.id, "reject")}
            />
          ))}
        </div>
      )}

      {/* Recently resolved (collapsed summary) */}
      {resolved.length > 0 && (
        <div className="text-[10px] text-zinc-500 pt-1 border-t border-amber-200 dark:border-amber-900/40">
          {resolved.filter((c) => c.status === "applied").length} applied ·{" "}
          {resolved.filter((c) => c.status === "rejected").length} rejected ·{" "}
          {resolved.filter((c) => c.status === "failed").length} failed
        </div>
      )}
    </div>
  )
}

function SchemaChangeCard({
  change,
  expanded,
  actioning,
  onToggleExpand,
  onApprove,
  onReject,
}: {
  change: SchemaChange
  expanded: boolean
  actioning: boolean
  onToggleExpand: () => void
  onApprove: () => void
  onReject: () => void
}) {
  const colorClass = RISK_COLORS[change.change_type] ?? "text-zinc-600 bg-zinc-50 border-zinc-200 dark:text-zinc-400 dark:bg-zinc-900 dark:border-zinc-700"
  const label = CHANGE_LABELS[change.change_type] ?? change.change_type

  return (
    <div className="rounded-md border border-amber-200 dark:border-amber-900/60 bg-white dark:bg-zinc-900 overflow-hidden">
      <div className="flex items-center gap-2 px-3 py-2">
        {/* Change type badge */}
        <span className={cn("text-[9px] font-bold uppercase px-1.5 py-0.5 rounded border shrink-0", colorClass)}>
          {label}
        </span>
        <div className="flex-1 min-w-0">
          <div className="text-xs font-medium text-zinc-900 dark:text-zinc-100 truncate">
            {change.table_name}
          </div>
          {change.user_message && (
            <div className="text-[10px] text-zinc-500 truncate">{change.user_message}</div>
          )}
        </div>
        <button
          type="button"
          onClick={onToggleExpand}
          className="shrink-0 p-0.5 text-zinc-400 hover:text-zinc-600"
        >
          {expanded ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
        </button>
      </div>

      {expanded && (
        <div className="px-3 pb-3 space-y-2 border-t border-zinc-100 dark:border-zinc-800">
          {/* DDL */}
          <div>
            <div className="text-[9px] uppercase tracking-wide text-zinc-400 mb-1 mt-2">Proposed DDL</div>
            <pre className="text-[10px] font-mono bg-zinc-50 dark:bg-zinc-800 rounded p-2 overflow-x-auto text-zinc-800 dark:text-zinc-200 whitespace-pre-wrap break-all">
              {change.ddl}
            </pre>
          </div>

          {/* Reasoning */}
          {change.reasoning && (
            <div>
              <div className="text-[9px] uppercase tracking-wide text-zinc-400 mb-0.5">AI reasoning</div>
              <div className="text-[10px] text-zinc-600 dark:text-zinc-400 leading-snug">
                {change.reasoning}
              </div>
            </div>
          )}

          {/* Risks */}
          {change.risks && change.risks !== "[]" && (
            <div>
              <div className="text-[9px] uppercase tracking-wide text-amber-500 mb-0.5">Risks</div>
              <div className="text-[10px] text-amber-700 dark:text-amber-400 leading-snug">
                {change.risks.replace(/[\[\]"]/g, "").replace(/,/g, " · ")}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Action buttons */}
      <div className="flex items-center justify-end gap-2 px-3 py-2 bg-zinc-50 dark:bg-zinc-800/50 border-t border-zinc-100 dark:border-zinc-800">
        <button
          type="button"
          onClick={onReject}
          disabled={actioning}
          className="inline-flex items-center gap-1 text-[11px] font-medium px-2 py-1 rounded border border-zinc-200 dark:border-zinc-700 hover:bg-zinc-100 dark:hover:bg-zinc-700 text-zinc-700 dark:text-zinc-300 disabled:opacity-50"
        >
          <X className="h-3 w-3" />
          Reject
        </button>
        <button
          type="button"
          onClick={onApprove}
          disabled={actioning}
          className="inline-flex items-center gap-1 text-[11px] font-medium px-2 py-1 rounded bg-emerald-600 hover:bg-emerald-700 text-white disabled:opacity-50"
        >
          <Check className="h-3 w-3" />
          Apply migration
        </button>
      </div>
    </div>
  )
}
