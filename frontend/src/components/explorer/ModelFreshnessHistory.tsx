"use client"

// Every freshness miss a model has had, open or ended. The api-gateway has kept them all
// since the deadline shipped (GET /explorer/freshness?include_resolved=true); until this
// tab, the only one ever shown was the open miss, as a badge. A model that missed its
// deadline every night last week and is on time today read as a model that is fine.

import { useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import { authFetch } from "@/lib/api/auth-fetch"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { formatAbsoluteTime } from "@/components/explorer/scheduleTime"
import {
  FRESHNESS_LIST_LIMIT,
  describeFreshnessCause,
  describeFreshnessResolution,
  formatSpan,
  overdueAtClose,
  overdueSeconds,
  parseBreaches,
  type ModelFreshnessBreach,
} from "@/components/explorer/liveState"

const HISTORY_URL = "/api/v1/explorer/freshness?include_resolved=true"

type LoadResult =
  | { key: string; breaches: ModelFreshnessBreach[]; capped: boolean }
  | { key: string; error: string }

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1)
}

export function ModelFreshnessHistory({ savedQueryId, reloadTick }: { savedQueryId: string; reloadTick: number }) {
  const [retryTick, setRetryTick] = useState(0)
  const [result, setResult] = useState<LoadResult | null>(null)
  const loadKey = `${savedQueryId}|${reloadTick}|${retryTick}`

  useEffect(() => {
    let cancelled = false
    const settle = (r: LoadResult) => {
      if (!cancelled) setResult(r)
    }
    void (async () => {
      try {
        const res = await authFetch(HISTORY_URL, { cache: "no-store" })
        if (!res.ok) {
          settle({ key: loadKey, error: `Could not load the freshness history (HTTP ${res.status}).` })
          return
        }
        const all = parseBreaches(await res.json())
        if (!all) {
          settle({ key: loadKey, error: "The freshness history came back in a form this page could not read." })
          return
        }
        settle({
          key: loadKey,
          // The route answers for the whole workspace; there is no per-model filter.
          breaches: all.filter((b) => b.saved_query_id === savedQueryId),
          capped: all.length >= FRESHNESS_LIST_LIMIT,
        })
      } catch {
        settle({ key: loadKey, error: "Could not reach the server to load the freshness history." })
      }
    })()
    return () => {
      cancelled = true
    }
  }, [loadKey, savedQueryId])

  const loading = result?.key !== loadKey
  const shown = result && "breaches" in result ? result : null

  return (
    <Card className="flex h-full flex-col overflow-hidden p-0" data-testid="model-freshness-card">
      <div className="border-b px-4 py-3 dark:border-zinc-800">
        <h2 className="text-sm font-semibold text-zinc-900 dark:text-white">Freshness misses</h2>
        <p className="mt-0.5 text-xs text-zinc-500 dark:text-zinc-400">
          Each time the table went past its freshness deadline, and how that ended.
        </p>
      </div>

      {result && "error" in result && !loading ? (
        <div className="flex flex-col items-center gap-2 px-4 py-10 text-center">
          <p className="text-sm text-red-600 dark:text-red-400">{result.error}</p>
          <Button size="sm" variant="outline" onClick={() => setRetryTick((t) => t + 1)}>
            Retry
          </Button>
        </div>
      ) : !shown ? (
        <div className="flex items-center justify-center gap-2 py-10 text-sm text-zinc-500 dark:text-zinc-400">
          <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
          Loading freshness history…
        </div>
      ) : shown.breaches.length === 0 ? (
        <div className="space-y-1 px-4 py-10 text-center text-sm text-zinc-500 dark:text-zinc-400">
          <p>No freshness misses recorded.</p>
          <p className="text-xs">
            A miss is recorded when a model that builds a table goes past the freshness deadline in Details.
          </p>
          {shown.capped && <CapNote />}
        </div>
      ) : (
        <div className={loading ? "opacity-60" : ""} aria-busy={loading}>
          <Table aria-label="Freshness misses">
            <TableHeader>
              <TableRow>
                <TableHead className="whitespace-nowrap">Detected</TableHead>
                <TableHead>Why</TableHead>
                <TableHead className="whitespace-nowrap">Overdue by</TableHead>
                <TableHead>Ended</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {shown.breaches.map((b) => (
                <BreachRow key={b.breach_id} b={b} />
              ))}
            </TableBody>
          </Table>
          {shown.capped && (
            <div className="border-t px-4 py-2 dark:border-zinc-800">
              <CapNote />
            </div>
          )}
        </div>
      )}
    </Card>
  )
}

function CapNote() {
  return (
    <p className="text-xs text-zinc-500 dark:text-zinc-400">
      The server returns only the workspace&apos;s {FRESHNESS_LIST_LIMIT} most recent misses across all models, so older
      misses of this one may not be listed.
    </p>
  )
}

function BreachRow({ b }: { b: ModelFreshnessBreach }) {
  const closedBy = overdueAtClose(b)
  const ending = describeFreshnessResolution(b.resolution)
  return (
    <TableRow>
      <TableCell className="whitespace-nowrap align-top text-xs text-zinc-600 dark:text-zinc-400">
        {formatAbsoluteTime(b.detected_at)}
      </TableCell>
      <TableCell className="min-w-[200px] align-top text-xs">
        <div>{capitalize(describeFreshnessCause(b.cause))}.</div>
        <div className="text-zinc-500 dark:text-zinc-400">
          {`Deadline ${formatSpan(b.deadline_seconds)}; `}
          {b.never_succeeded
            ? "never rebuilt successfully"
            : `last rebuilt successfully ${formatAbsoluteTime(b.reference_at)}`}
        </div>
      </TableCell>
      <TableCell className="whitespace-nowrap align-top text-xs tabular-nums">
        {closedBy === null ? `${formatSpan(overdueSeconds(b))} so far` : formatSpan(closedBy)}
      </TableCell>
      <TableCell className="min-w-[160px] align-top text-xs">
        {b.resolved_at ? (
          <>
            <div>
              <span className={b.resolution === "rebuilt" ? "text-emerald-700 dark:text-emerald-400" : "text-zinc-700 dark:text-zinc-300"}>
                {ending.label}
              </span>
              <span className="text-zinc-500 dark:text-zinc-400"> · {formatAbsoluteTime(b.resolved_at)}</span>
            </div>
            {ending.detail && <div className="text-zinc-500 dark:text-zinc-400">{ending.detail}</div>}
          </>
        ) : (
          <span className="font-medium text-amber-700 dark:text-amber-400">Still open</span>
        )}
      </TableCell>
    </TableRow>
  )
}
