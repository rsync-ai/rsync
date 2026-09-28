/**
 * Item 34: a CDC pipeline's Monitoring header read "Latest data-plane rows: read
 * 75230" right above a Table statistics tile reading "Total Rows Read 0". The
 * header took the highest count across every run; the tile reads the run on
 * screen. Bug class: two widgets on one page counting over different run scopes.
 * The header now reads the run the Table statistics panel reads, in both modes.
 */

import { readFileSync } from "node:fs"
import { join } from "node:path"
import { describe, expect, it } from "vitest"

import { headerRowLine } from "@/lib/pipeline/dataPlaneRowMetrics"
import type { PipelineRunEvent } from "@/lib/pipeline/eventNormalizer"

const RELOAD = "05ec379f"
const CURRENT = "e0afbc01"

function run(executionId: string, read: number, n: number): PipelineRunEvent {
  return {
    pipeline_id: "p1",
    event_id: `e${n}`,
    execution_id: executionId,
    event_type: "DATA_PLANE_METRICS",
    received_at: "2026-09-27T10:00:00Z",
    payload: { schema_version: 2, metadata: { source: "executor_batch", metrics: { records_read: read, records_written: read } } },
  } as PipelineRunEvent
}

describe("header row line", () => {
  it("does not show an earlier run's rows for the run on screen", () => {
    // The CDC pipeline's Reload read 75,230; the current run has no count yet.
    expect(headerRowLine([run(RELOAD, 75230, 1)], CURRENT)).toBeNull()
  })

  it("shows the current run's rows, whatever an earlier run read", () => {
    expect(headerRowLine([run(RELOAD, 75230, 1), run(CURRENT, 12, 2)], CURRENT)).toEqual({
      label: "Rows this run:",
      read: 12,
      written: 12,
    })
  })

  it("control: the Reload's own rows show while the Reload is the run on screen", () => {
    expect(headerRowLine([run(RELOAD, 75230, 1)], RELOAD)?.read).toBe(75230)
  })

  it("says nothing before any run is known", () => {
    expect(headerRowLine([run(RELOAD, 75230, 1)], undefined)).toBeNull()
  })

  it("the panel uses it for both modes — no CDC-only cross-run branch", () => {
    const src = readFileSync(join(__dirname, "../components/pipeline/PipelineMonitoringPanel.tsx"), "utf8")
    expect(src).toContain("headerRowLine(events, state?.execution_id)")
    expect(src).not.toContain("Latest data-plane rows")
  })
})
