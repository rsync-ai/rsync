/**
 * The CDC header must not say "Load completed, replication ongoing · caught up"
 * while a Reload is setting the pipeline up again (item 32).
 *
 * Bug class: a status line built from the previous run's record, without the
 * current phase. After Reload the gateway reports phase "initializing" or
 * "syncing" (pipeline_runtime.go computeRuntimePhase), while runtime.load still
 * holds the last load's "completed" row and reloading_tables stays 0 until the
 * re-snapshot request row exists. Likewise pending_events 0 is not "caught up"
 * while a table is being loaded again or load rows are still waiting.
 *
 * The streaming cases are the controls: a live stream keeps both phrases.
 */

import { describe, expect, it, vi } from "vitest"
import type { PipelineRuntime, RuntimeLoad } from "@/lib/hooks/usePipelineRuntime"

vi.mock("@/components/pipeline/DiagnosePanel", () => ({ DiagnosePanel: () => null }))

import { describeLoadStatus } from "@/lib/pipeline/loadStatus"
import { backlogVital } from "@/components/pipeline/PipelineHealthHeader"
import { backlogTile, kafkaTile } from "@/components/pipeline/MonitoringOverviewTab"

const done: RuntimeLoad = {
  status: "completed",
  mode: "blocking",
  tables_total: 5,
  tables_done: 5,
  started_at: "2026-09-25T09:00:00Z",
  completed_at: "2026-09-25T09:05:00Z",
  reloading_tables: 0,
}

const rt = (over: Partial<PipelineRuntime> = {}): PipelineRuntime => ({
  pipeline_id: "p1",
  mode: "cdc",
  phase: "streaming",
  health: "healthy",
  dependencies: [],
  updated_at: "2026-09-25T10:00:00Z",
  load: done,
  liveness: { pending_events: 0 } as PipelineRuntime["liveness"],
  ...over,
})

describe("load status while the pipeline is set up again", () => {
  it.each(["initializing", "planning", "validating", "syncing"] as const)(
    "%s: never claims replication is ongoing",
    (phase) => {
      const s = describeLoadStatus(rt({ phase }))
      expect(s?.text).not.toMatch(/replication ongoing/i)
      expect(s?.text).toMatch(/setting up/i)
      expect(s?.tone).toBe("info")
    },
  )

  it("streaming (control): a finished load with a live stream keeps its wording", () => {
    expect(describeLoadStatus(rt())?.text).toBe("Load completed, replication ongoing")
  })
})

describe("caught up needs a live stream with nothing loading", () => {
  it.each(["initializing", "syncing"] as const)("%s: 0 pending is not caught up", (phase) => {
    expect(backlogVital(rt({ phase }))).toBeNull()
  })

  it("a table being re-loaded: 0 pending is not caught up", () => {
    expect(backlogVital(rt({ load: { ...done, reloading_tables: 2 } }))).toBeNull()
  })

  it("load rows still to write: 0 pending is not caught up", () => {
    expect(backlogVital(rt({ load: { ...done, snapshot_rows_waiting: 40 } }))).toBeNull()
  })

  it("streaming (control): 0 pending reads caught up", () => {
    expect(backlogVital(rt())?.text).toBe("caught up")
  })
})

// Sibling: the Overview tab's Backlog and Kafka tiles said the same green
// "Caught up" at zero with no phase check.

describe("Overview tiles: zero during set-up or a load is not caught up", () => {
  it("backlog tile", () => {
    expect(backlogTile(0, true, "row", true)).toMatchObject({ value: "Nothing waiting yet", tone: "neutral" })
    expect(backlogTile(0, true, "change", false)).toMatchObject({ value: "Caught up", tone: "ok" }) // control
  })

  it("kafka tile", () => {
    expect(kafkaTile(0, { capture: "capturing", settling: true })).toMatchObject({ value: "Nothing waiting yet", tone: "neutral" })
    expect(kafkaTile(0, { capture: "capturing" })).toMatchObject({ value: "Caught up", tone: "ok" }) // control
    // A stopped capture still wins: it is the more urgent reading.
    expect(kafkaTile(0, { capture: "stopped", settling: true }).value).toBe("Capture stopped")
  })
})
