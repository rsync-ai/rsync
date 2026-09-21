import { describe, expect, it } from "vitest"
import { extractLatestRowMetrics } from "@/lib/pipeline/dataPlaneRowMetrics"
import type { PipelineRunEvent } from "@/lib/pipeline/eventNormalizer"

function metricsEvent(metadata: Record<string, unknown>, n: number): PipelineRunEvent {
  return {
    pipeline_id: "p1",
    event_id: `e${n}`,
    event_type: "DATA_PLANE_METRICS",
    received_at: "2026-09-17T10:00:00Z",
    payload: { schema_version: 2, metadata },
  }
}

// The shape the orchestrator's CDC status poll wrote: rows_processed was the sink's Kafka lag
// times ten, not a row count.
const statusPoll = (rowsProcessed: number, n: number) =>
  metricsEvent({ source: "cdc_status_poll", rows_processed: rowsProcessed, cdc_lag_ms: rowsProcessed }, n)

describe("extractLatestRowMetrics", () => {
  it("does not report a CDC status poll's lag estimate as rows written", () => {
    // Pipeline page: "written 12800" beside 72,670 rows applied.
    expect(extractLatestRowMetrics([statusPoll(12800, 1), statusPoll(900, 2)])).toEqual({
      read: undefined,
      written: undefined,
    })
  })

  it("keeps a real rows_processed-only payload as written", () => {
    const legacy = metricsEvent({ source: "executor_batch", rows_processed: 500 }, 1)
    expect(extractLatestRowMetrics([legacy, statusPoll(12800, 2)])).toEqual({ read: undefined, written: 500 })
  })

  it("keeps real counters when status polls are mixed in", () => {
    const batch = metricsEvent({ source: "executor_batch", metrics: { records_read: 70 }, rows_processed: 70 }, 1)
    expect(extractLatestRowMetrics([statusPoll(99999, 2), batch])).toEqual({ read: 70, written: undefined })
  })

  it("takes the highest cumulative counter, and reads metadata sent as a JSON string", () => {
    const a = metricsEvent({ metrics: { records_written: 40 } }, 1)
    const b = {
      ...metricsEvent({}, 2),
      payload: { metadata: JSON.stringify({ metrics: { records_written: 90, records_read: 95 } }) },
    }
    expect(extractLatestRowMetrics([a, b])).toEqual({ read: 95, written: 90 })
  })

  it("ignores events that are not data-plane metrics", () => {
    const other = { ...metricsEvent({ rows_processed: 7 }, 1), event_type: "STAGE_COMPLETED" }
    expect(extractLatestRowMetrics([other])).toEqual({ read: undefined, written: undefined })
  })
})
