/**
 * Row counters read from DATA_PLANE_METRICS events for the pipeline monitoring panel.
 */

import type { PipelineRunEvent } from "@/lib/pipeline/eventNormalizer"

/**
 * CDC_STATUS_POLL_SOURCE marks the metrics event the orchestrator writes each time a CDC
 * pipeline's status is polled. It has never carried a row count: until the orchestrator
 * stopped, its `rows_processed` was the sink's Kafka lag times ten, so a pipeline with 1,280
 * events of lag showed "written 12800" beside 72,670 rows actually applied, plus a
 * "mismatch" warning. Those events stay in run history, so readers must skip them.
 */
export const CDC_STATUS_POLL_SOURCE = "cdc_status_poll"

// Same parsing as the panel's asObject: metadata can arrive as a JSON string.
function asObject(v: unknown): Record<string, unknown> | null {
  if (!v) return null
  if (typeof v === "object" && !Array.isArray(v)) return v as Record<string, unknown>
  if (typeof v === "string") {
    const s = v.trim()
    if (s.startsWith("{")) {
      try {
        const parsed: unknown = JSON.parse(s)
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed as Record<string, unknown>
      } catch {
        // ignore
      }
    }
  }
  return null
}

function asNumber(v: unknown): number | undefined {
  const n = typeof v === "number" ? v : typeof v === "string" ? Number(v) : undefined
  return typeof n === "number" && Number.isFinite(n) ? n : undefined
}

/**
 * extractLatestRowMetrics returns the highest rows read / written the data-plane metrics
 * events report. The counters are cumulative, so the highest one is the latest.
 */
export function extractLatestRowMetrics(events: PipelineRunEvent[]): { read?: number; written?: number } {
  const metricEvents: Record<string, unknown>[] = []
  for (const e of events) {
    if (e.event_type !== "DATA_PLANE_METRICS") continue
    const p = asObject(e.payload) || {}
    const meta = asObject(p["metadata"]) || {}
    if (meta["source"] === CDC_STATUS_POLL_SOURCE) continue
    metricEvents.push(p)
  }

  let read: number | undefined = undefined
  let written: number | undefined = undefined
  for (const p of metricEvents) {
    const meta = asObject(p["metadata"]) || {}
    const m = asObject(meta["metrics"]) || asObject(p["metrics"]) || {}

    const pr = asNumber(m["records_read"] ?? m["rows_read"] ?? meta["records_read"] ?? meta["rows_read"])
    const pw = asNumber(m["records_written"] ?? m["rows_written"] ?? meta["records_written"] ?? meta["rows_written"])
    if (pr !== undefined) read = read === undefined ? pr : Math.max(read, pr)
    if (pw !== undefined) written = written === undefined ? pw : Math.max(written, pw)
  }
  // Back-compat fallback: some payloads only provide rows_processed (ambiguous). Treat as written.
  if (read === undefined && written === undefined) {
    let rowsProcessed: number | undefined
    for (const p of metricEvents) {
      const meta = asObject(p["metadata"]) || {}
      const n = asNumber(meta["rows_processed"] ?? p["rows_processed"])
      if (n !== undefined) rowsProcessed = rowsProcessed === undefined ? n : Math.max(rowsProcessed, n)
    }
    if (rowsProcessed !== undefined) written = rowsProcessed
  }

  return { read, written }
}
