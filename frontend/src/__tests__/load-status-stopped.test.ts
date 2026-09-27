import { describe, expect, it } from "vitest"

import type { PipelineRuntime, RuntimeLoad } from "@/lib/hooks/usePipelineRuntime"
import { describeLoadStatus } from "@/lib/pipeline/loadStatus"

// U-14 (prod 2026-09-26): a CDC pipeline the user stopped read
// "Paused · Load completed, replication paused". Stop keeps the connector and its
// position and Start resumes it, so the page says stopped, in the info tone a
// user's own action gets, not the error tone of a failure.

const completed: RuntimeLoad = {
  status: "completed",
  mode: "blocking",
  tables_total: 6,
  tables_done: 6,
  completed_at: "2026-09-26T16:00:00Z",
  reloading_tables: 0,
}

function rt(phase: PipelineRuntime["phase"], load: RuntimeLoad = completed) {
  return { mode: "cdc" as const, phase, health: "degraded" as const, load }
}

describe("describeLoadStatus for a stopped CDC pipeline", () => {
  it("says stopped, not paused, once the load completed", () => {
    expect(describeLoadStatus(rt("stopped"))).toMatchObject({ text: "Load completed, pipeline stopped", tone: "info" })
  })

  it("keeps paused for a paused pipeline", () => {
    expect(describeLoadStatus(rt("paused"))?.text).toBe("Load completed, replication paused")
  })

  it("keeps a failure distinct from a user stop", () => {
    expect(describeLoadStatus(rt("failed"))).toMatchObject({ text: "Load completed, replication stopped", tone: "error" })
  })

  it("names a load the stop interrupted as on hold", () => {
    const s = describeLoadStatus(rt("stopped", { ...completed, status: "started", tables_done: 2 }))
    expect(s).toMatchObject({ text: "Full load on hold, pipeline stopped · 2 / 6 tables", tone: "info" })
  })

  it("names rows still to write under a stop", () => {
    const s = describeLoadStatus(rt("stopped", { ...completed, snapshot_rows_waiting: 1000 }))
    expect(s?.text).toMatch(/to write, pipeline stopped$/)
    expect(s?.tone).toBe("info")
  })
})
