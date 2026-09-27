"use client"

/**
 * Per-table resume positions: "where did this pipeline get to on each table?"
 *
 * GET /api/v1/pipelines/:id/checkpoints has been served (and workspace-gated)
 * since migration 026 with no UI calling it. Without this card the answer to
 * "will a restart replay from the beginning, and from where?" was only in the
 * database.
 *
 * `position` is free-form JSONB by design — a Postgres source stores an LSN, a
 * MongoDB source a resume token, an API connector a `watermark.value` timestamp
 * — so the rendering has to stay shape-agnostic. It prints the leaf key/value
 * pairs it finds rather than assuming any one engine's vocabulary.
 *
 * Only the batch export loop writes this table (cdc.SaveCheckpoint, called
 * once per batch in executor.go). A CDC stream resumes from Kafka offsets
 * instead: the Debezium connector's, which Kafka Connect keeps in its offsets
 * topic, and the sink's, committed by its consumer group. So
 * on a CDC pipeline the list is normally empty, and the batch wording — "a
 * restart would start from the beginning" — was telling a streaming pipeline's
 * operator the opposite of what a restart does.
 */

import { useCallback, useEffect, useState } from "react"
import { RefreshCw } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"

type Checkpoint = {
  id: string
  pipeline_id: string
  connection_id: string
  source_table: string
  position: Record<string, unknown> | null
  created_at: string
  updated_at: string
}

// Keys whose raw name misreads on a per-table row. The batch executor stamps the
// whole run's running totals into each table's checkpoint (executor.go, the
// shared totalRows/totalBytes), so "rows_so_far: 1200" beside public.orders
// read as that table's count when it was every table's (#13).
const POSITION_LABELS: Record<string, string> = {
  rows_so_far: "run rows so far (all tables)",
  bytes_so_far: "run bytes so far (all tables)",
}

/**
 * Flatten `position` into "key: value" pairs, one level deep into nested
 * objects so `{"watermark":{"value":"2026-09-01T…"}}` reads as
 * `watermark.value: 2026-09-01T…` instead of as raw JSON.
 */
export function formatPosition(position: Record<string, unknown> | null | undefined): string {
  if (!position || typeof position !== "object") return "—"
  const parts: string[] = []
  for (const [k, v] of Object.entries(position)) {
    if (v && typeof v === "object" && !Array.isArray(v)) {
      for (const [k2, v2] of Object.entries(v as Record<string, unknown>)) {
        parts.push(`${k}.${k2}: ${String(v2)}`)
      }
    } else {
      parts.push(`${POSITION_LABELS[k] ?? k}: ${Array.isArray(v) ? JSON.stringify(v) : String(v)}`)
    }
  }
  // An empty object is a real state — the row exists but carries no position —
  // and is not the same as having no row at all.
  return parts.length > 0 ? parts.join(" · ") : "(empty)"
}

function fmtWhen(iso: string): string {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return d.toLocaleString()
}

export function PipelineCheckpointsCard({
  pipelineId,
  pipelineType,
}: {
  pipelineId: string
  pipelineType?: "etl" | "cdc"
}) {
  const isCdc = pipelineType === "cdc"
  const [checkpoints, setCheckpoints] = useState<Checkpoint[] | null>(null)
  const [loading, setLoading] = useState(true)
  // A failed read is a THIRD outcome, not a variant of "no checkpoints yet":
  // an empty list is a claim that the pipeline has not committed a position
  // anywhere, which is exactly the wrong thing to say when the read failed.
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const res = await authFetch(API_ENDPOINTS.PIPELINES.CHECKPOINTS(pipelineId), { cache: "no-store" })
      if (!res.ok) {
        setError(`Could not load checkpoints (HTTP ${res.status})`)
        return
      }
      const data = (await res.json().catch(() => null)) as { checkpoints?: Checkpoint[] } | null
      setCheckpoints(Array.isArray(data?.checkpoints) ? data!.checkpoints : [])
      setError(null)
    } catch {
      setError("Could not load checkpoints — the API is unreachable")
    } finally {
      setLoading(false)
    }
  }, [pipelineId])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <Card>
      <CardHeader className="flex flex-row items-start justify-between gap-4 space-y-0">
        <div>
          <CardTitle className="text-sm font-medium">Checkpoints</CardTitle>
          <CardDescription className="text-xs">
            {isCdc
              ? "Positions written by batch runs only. A CDC stream resumes from Kafka offsets — the connector's in Kafka Connect, the sink's in its consumer group — not from this list."
              : "The last position committed per table — where a restart resumes from. Row and byte counts are the whole run's running total when this table last committed; per-table counts are in Table statistics."}
          </CardDescription>
        </div>
        <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
          <RefreshCw className={`h-3 w-3 mr-2 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </CardHeader>
      <CardContent>
        {loading && checkpoints === null ? (
          <div className="text-sm text-muted-foreground">Loading…</div>
        ) : error ? (
          <div className="rounded border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300">
            {error}
          </div>
        ) : !checkpoints || checkpoints.length === 0 ? (
          isCdc ? (
            <div className="text-sm text-muted-foreground">
              No batch checkpoints — expected for a CDC pipeline. This does not mean a restart starts over: the stream
              resumes from the Kafka offsets its connector and sink have committed.
            </div>
          ) : (
            <div className="text-sm text-muted-foreground">
              No checkpoint recorded yet. A restart would start this pipeline&apos;s tables from the beginning.
            </div>
          )
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-xs text-muted-foreground">
                  <th className="py-2 pr-4 font-medium">Table</th>
                  <th className="py-2 pr-4 font-medium">Position</th>
                  <th className="py-2 font-medium">Updated</th>
                </tr>
              </thead>
              <tbody>
                {checkpoints.map((cp) => (
                  <tr key={cp.id} className="border-b last:border-0 align-top">
                    <td className="py-2 pr-4 font-medium">{cp.source_table}</td>
                    <td className="py-2 pr-4 font-mono text-xs break-all">{formatPosition(cp.position)}</td>
                    <td className="py-2 whitespace-nowrap text-xs text-muted-foreground">{fmtWhen(cp.updated_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
