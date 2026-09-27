import { describe, expect, it } from "vitest"
import type { PipelineRuntime, RuntimeLoad } from "@/lib/hooks/usePipelineRuntime"
import { backlogIncludesLoadRows, describeLoadStatus } from "@/lib/pipeline/loadStatus"

// The pipeline page says where a CDC pipeline is, the way AWS DMS does: "Full
// load in progress", then "Load completed, replication ongoing". It reads the
// initial load the orchestrator recorded (runtime.load). No record is no chip:
// the page must not claim a load finished that nobody saw.

type Rt = Pick<PipelineRuntime, "mode" | "phase" | "health" | "load">

const load = (over: Partial<RuntimeLoad> = {}): RuntimeLoad => ({
  status: "started",
  mode: "blocking",
  tables_total: 5,
  tables_done: 3,
  started_at: "2026-09-25T09:00:00Z",
  reloading_tables: 0,
  ...over,
})

const rt = (over: Partial<Rt> = {}): Rt => ({
  mode: "cdc",
  phase: "streaming",
  health: "healthy",
  load: load(),
  ...over,
})

describe("describeLoadStatus", () => {
  it("says nothing without a recorded load, or for a batch pipeline", () => {
    expect(describeLoadStatus(null)).toBeNull()
    expect(describeLoadStatus(rt({ load: undefined }))).toBeNull()
    expect(describeLoadStatus(rt({ mode: "batch" }))).toBeNull()
  })

  it("counts tables while a blocking load runs", () => {
    expect(describeLoadStatus(rt())).toEqual({
      text: "Full load in progress · 3 / 5 tables",
      tone: "info",
      since: { label: "started", at: "2026-09-25T09:00:00Z" },
    })
    expect(describeLoadStatus(rt({ load: load({ status: "sent", tables_done: 0 }) }))?.text).toBe(
      "Full load in progress · 0 / 5 tables",
    )
  })

  // Debezium marks no table done in an incremental snapshot: "0 / 5" would read
  // as stuck, so it names only how many tables the load covers.
  it("names the table count, not a done count, for an incremental load", () => {
    expect(describeLoadStatus(rt({ load: load({ mode: "incremental", tables_done: 0 }) }))?.text).toBe(
      "Full load in progress · 5 tables",
    )
    expect(describeLoadStatus(rt({ load: load({ mode: "incremental", tables_total: 1 }) }))?.text).toBe(
      "Full load in progress · 1 table",
    )
  })

  it("does not print a count it does not have", () => {
    expect(describeLoadStatus(rt({ load: load({ tables_total: 0, tables_done: 0 }) }))?.text).toBe(
      "Full load in progress",
    )
  })

  it("says a load is paused or stopped rather than in progress", () => {
    expect(describeLoadStatus(rt({ phase: "paused" }))).toMatchObject({
      text: "Full load paused · 3 / 5 tables",
      tone: "info",
    })
    expect(describeLoadStatus(rt({ phase: "failed", health: "unhealthy" }))).toMatchObject({
      text: "Full load stopped · 3 / 5 tables",
      tone: "error",
    })
  })

  it("reads load completed, replication ongoing once the load finished", () => {
    const done = load({ status: "completed", tables_done: 5, completed_at: "2026-09-25T10:00:00Z" })
    expect(describeLoadStatus(rt({ load: done }))).toEqual({
      text: "Load completed, replication ongoing",
      tone: "ok",
      since: { label: "finished", at: "2026-09-25T10:00:00Z" },
    })
    // Waiting for data only means no change has arrived yet; the stream is up.
    expect(describeLoadStatus(rt({ phase: "waiting_for_data", load: done }))?.text).toBe(
      "Load completed, replication ongoing",
    )
  })

  // Debezium's last snapshot marker completes the load when the SOURCE has been
  // read to the end, while the sink can still be writing those rows: a live
  // 17,819-row load read "Load completed" with every row still in Kafka.
  it("says a load is still being written while its rows wait for the destination", () => {
    const read = load({
      status: "completed",
      tables_done: 5,
      completed_at: "2026-09-25T10:00:00Z",
      snapshot_rows_waiting: 17819,
    })
    expect(describeLoadStatus(rt({ load: read }))).toEqual({
      text: "Full load read · writing 17,819 rows",
      tone: "info",
      since: { label: "started", at: "2026-09-25T09:00:00Z" },
    })
    expect(describeLoadStatus(rt({ load: { ...read, snapshot_rows_waiting: 1 } }))?.text).toBe(
      "Full load read · writing 1 row",
    )
    expect(describeLoadStatus(rt({ phase: "paused", load: read }))).toMatchObject({
      text: "Full load read · 17,819 rows to write, paused",
      tone: "info",
    })
    expect(describeLoadStatus(rt({ health: "unhealthy", load: read }))).toMatchObject({
      text: "Full load read · 17,819 rows to write, stalled",
      tone: "warn",
    })
    expect(describeLoadStatus(rt({ phase: "failed", health: "unhealthy", load: read }))).toMatchObject({
      text: "Full load read · 17,819 rows to write, stopped",
      tone: "error",
    })
    // Control: every row written (or an older gateway that sends no count) is
    // load completed.
    expect(describeLoadStatus(rt({ load: { ...read, snapshot_rows_waiting: 0 } }))?.text).toBe(
      "Load completed, replication ongoing",
    )
    expect(describeLoadStatus(rt({ load: { ...read, snapshot_rows_waiting: undefined } }))?.text).toBe(
      "Load completed, replication ongoing",
    )
  })

  // A re-snapshot's rows are counted in the same columns; the re-loading line
  // already says that load is under way.
  it("leaves a re-snapshot's waiting rows to the re-loading line", () => {
    const done = load({ status: "completed", reloading_tables: 2, snapshot_rows_waiting: 500 })
    expect(describeLoadStatus(rt({ load: done }))?.text).toBe("Replication ongoing · re-loading 2 tables")
  })

  it("says which tables a re-snapshot is loading again", () => {
    const done = load({ status: "completed", reloading_tables: 2 })
    expect(describeLoadStatus(rt({ load: done }))).toMatchObject({
      text: "Replication ongoing · re-loading 2 tables",
      tone: "info",
    })
    expect(describeLoadStatus(rt({ load: { ...done, reloading_tables: 1 } }))?.text).toBe(
      "Replication ongoing · re-loading 1 table",
    )
  })

  it("does not call a stopped stream ongoing", () => {
    const done = load({ status: "completed" })
    expect(describeLoadStatus(rt({ phase: "paused", load: done }))).toMatchObject({
      text: "Load completed, replication paused",
      tone: "info",
    })
    expect(describeLoadStatus(rt({ health: "unhealthy", load: done }))).toMatchObject({
      text: "Load completed, replication stalled",
      tone: "warn",
    })
    expect(describeLoadStatus(rt({ phase: "failed", health: "unhealthy", load: done }))).toMatchObject({
      text: "Load completed, replication stopped",
      tone: "error",
    })
    // A paused pipeline is not re-loading anything, whatever is queued.
    expect(describeLoadStatus(rt({ phase: "paused", load: { ...done, reloading_tables: 3 } }))?.text).toBe(
      "Load completed, replication paused",
    )
  })

  it("carries the recorded reason on a failed or unconfirmed load", () => {
    expect(
      describeLoadStatus(rt({ load: load({ status: "failed", last_error: "the batch historical load failed" }) })),
    ).toEqual({
      text: "Full load failed",
      tone: "error",
      title: "the batch historical load failed",
      since: { label: "started", at: "2026-09-25T09:00:00Z" },
    })
    expect(describeLoadStatus(rt({ load: load({ status: "unconfirmed", last_error: "no rows for 1h" }) }))).toMatchObject(
      { text: "Full load not confirmed", tone: "warn", title: "no rows for 1h" },
    )
  })

  it("says a status it does not know rather than guessing", () => {
    expect(
      describeLoadStatus(rt({ load: load({ status: "archived" as RuntimeLoad["status"] }) }))?.text,
    ).toBe("Full load: archived")
  })
})

// A backlog read during the initial load is mostly snapshot rows, and the page
// called them "changes" (live on 2026-09-25: "67.2K changes waiting" while the
// load's rows were still being written). "Rows" is never wrong; "changes" is
// right only once the load's rows are all written.
describe("backlogIncludesLoadRows", () => {
  it("is true while the load reads, and while its rows are still being written", () => {
    expect(backlogIncludesLoadRows(rt())).toBe(true)
    expect(backlogIncludesLoadRows(rt({ load: load({ status: "sent" }) }))).toBe(true)
    expect(backlogIncludesLoadRows(rt({ load: load({ status: "completed", snapshot_rows_waiting: 67_200 }) }))).toBe(true)
  })

  it("is true while a re-snapshot reloads tables", () => {
    expect(backlogIncludesLoadRows(rt({ load: load({ status: "completed", reloading_tables: 1 }) }))).toBe(true)
  })

  it("is false once the load's rows are all written", () => {
    expect(backlogIncludesLoadRows(rt({ load: load({ status: "completed", snapshot_rows_waiting: 0 }) }))).toBe(false)
    expect(backlogIncludesLoadRows(rt({ load: load({ status: "completed" }) }))).toBe(false)
  })

  it("is false without a recorded load, and for batch", () => {
    expect(backlogIncludesLoadRows(rt({ load: undefined }))).toBe(false)
    expect(backlogIncludesLoadRows(rt({ mode: "batch" }))).toBe(false)
    expect(backlogIncludesLoadRows(null)).toBe(false)
  })
})
