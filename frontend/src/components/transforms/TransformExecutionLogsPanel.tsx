"use client"

import { useMemo, useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import { cn } from "@/lib/utils"
import { ChevronDown, ChevronRight } from "lucide-react"
import { formatCount, formatDuration } from "@/lib/transform-format"

export type TransformExecutionLog = {
  id: string
  pipeline_id: string
  execution_id: string
  table_name: string
  transform_id?: string | null
  transform_order: number
  transform_type: string
  status: string
  error_message?: string | null
  input_rows: number
  output_rows: number
  duration_ms: number
  config_snapshot: any
  created_at?: string
  updated_at?: string
}

function statusBadgeVariant(status: string) {
  const s = String(status || "").toLowerCase()
  if (s === "failed" || s === "error") return "destructive" as const
  if (s === "success") return "success" as const
  return "secondary" as const
}

export function TransformExecutionLogsPanel(props: {
  logs: TransformExecutionLog[]
  className?: string
  emptyLabel?: string
}) {
  const { logs, className, emptyLabel } = props
  const grouped = useMemo(() => {
    const m = new Map<string, TransformExecutionLog[]>()
    for (const l of logs || []) {
      const key = l?.table_name || "unknown"
      const arr = m.get(key) || []
      arr.push(l)
      m.set(key, arr)
    }
    for (const [k, arr] of m.entries()) {
      arr.sort((a, b) => (a.transform_order ?? 0) - (b.transform_order ?? 0))
      m.set(k, arr)
    }
    return Array.from(m.entries()).sort(([a], [b]) => a.localeCompare(b))
  }, [logs])

  if (!logs || logs.length === 0) {
    return <div className={cn("text-sm text-zinc-500", className)}>{emptyLabel || "No transform execution logs yet."}</div>
  }

  return (
    <div className={cn("space-y-5", className)}>
      {grouped.map(([table, arr]) => (
        <div key={table} className="space-y-2">
          <div className="flex items-center justify-between">
            <div className="font-semibold text-zinc-900 dark:text-white">{table}</div>
            <div className="text-xs text-zinc-500">{arr.length} transforms</div>
          </div>

          <div className="space-y-2">
            {arr.map((l) => (
              <TransformLogRow key={l.id} log={l} />
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}

function TransformLogRow({ log }: { log: TransformExecutionLog }) {
  const [open, setOpen] = useState(false)

  const durLabel = formatDuration(log.duration_ms)

  // A transform "dropped" rows whenever it output fewer than it took in — measured
  // from the actual row counts, not a transform-type allow-list (which drifts from
  // the engine and misses null-handling / validation that also remove rows).
  const dropped = (log.input_rows ?? 0) - (log.output_rows ?? 0)
  const showDrop = dropped > 0

  return (
    <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950">
      <div className="flex items-start justify-between gap-3 p-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline" className="font-mono">
              #{log.transform_order}
            </Badge>
            <div className="font-medium text-zinc-900 dark:text-white truncate">{log.transform_type}</div>
            <Badge variant={statusBadgeVariant(log.status)}>{String(log.status || "unknown")}</Badge>
          </div>
          <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-zinc-500">
            <span>
              Rows: {formatCount(log.input_rows)} <span aria-hidden="true">→</span> {formatCount(log.output_rows)} • Duration: {durLabel}
            </span>
            {showDrop && (
              <Badge
                variant="outline"
                className="border-amber-300 text-amber-700 dark:border-amber-800 dark:text-amber-400"
                title={`${log.transform_type} removed ${formatCount(dropped)} rows (input − output)`}
              >
                −{formatCount(dropped)} rows
              </Badge>
            )}
          </div>
          {log.error_message && (
            <div className="mt-2 text-xs text-red-600 dark:text-red-400 whitespace-pre-wrap">
              {log.error_message}
            </div>
          )}
        </div>

        <Collapsible open={open} onOpenChange={setOpen}>
          <CollapsibleTrigger asChild>
            <Button variant="outline" size="sm" className="shrink-0">
              {open ? (
                <>
                  <ChevronDown className="h-4 w-4 mr-1" />
                  Hide config
                </>
              ) : (
                <>
                  <ChevronRight className="h-4 w-4 mr-1" />
                  View config
                </>
              )}
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent className="px-3 pb-3">
            <pre className="mt-3 max-h-[320px] overflow-auto rounded-md bg-zinc-50 dark:bg-zinc-900 p-3 text-xs text-zinc-800 dark:text-zinc-200">
              {JSON.stringify(log.config_snapshot ?? {}, null, 2)}
            </pre>
          </CollapsibleContent>
        </Collapsible>
      </div>
    </div>
  )
}

