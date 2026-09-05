import { describe, expect, it } from "vitest"
import {
  buildRunTimeline,
  computeRunDelta,
  dataMovementVerdict,
  formatBytes,
  movementHeadline,
  planStagesFromEvents,
  rollupFromSummary,
  rollupTableStats,
  rowsWrittenForTable,
} from "../executionSummary"

describe("rowsWrittenForTable", () => {
  it("reads the batch family", () => {
    expect(rowsWrittenForTable({ qualified_name: "t", inserted_rows: 120 })).toBe(120)
  })

  it("reads the CDC family as the sum of the three operations", () => {
    expect(
      rowsWrittenForTable({
        qualified_name: "t",
        applied_inserts: 5,
        applied_updates: 3,
        applied_deletes: 2,
      }),
    ).toBe(10)
  })

  it("takes the larger family when both are populated, matching the server's GREATEST", () => {
    expect(
      rowsWrittenForTable({
        qualified_name: "t",
        inserted_rows: 4,
        applied_inserts: 9,
      }),
    ).toBe(9)
  })

  it("treats absent counters as zero rather than NaN", () => {
    expect(rowsWrittenForTable({ qualified_name: "t" })).toBe(0)
  })
})

describe("dataMovementVerdict — the three cases the old dialog collapsed into one em dash", () => {
  it("distinguishes 'no statistics recorded' from any row count", () => {
    expect(dataMovementVerdict(null)).toEqual({ kind: "unmeasured" })
    expect(dataMovementVerdict(rollupTableStats([]))).toEqual({ kind: "unmeasured" })
  })

  it("calls a run that read nothing and wrote nothing an empty source, not a failure", () => {
    const rollup = rollupTableStats([
      { qualified_name: "public.demo_customers", read_rows: 0, inserted_rows: 0, dlq_rows: 0 },
    ])
    expect(dataMovementVerdict(rollup)).toEqual({
      kind: "empty-source",
      tables: 1,
    })
  })

  it("flags rows read but none written — the silent-drop shape", () => {
    const rollup = rollupTableStats([
      { qualified_name: "public.orders", read_rows: 4200, inserted_rows: 0 },
    ])
    expect(dataMovementVerdict(rollup)).toEqual({
      kind: "read-not-written",
      read: 4200,
      tables: 1,
    })
  })

  it("reports a successful move with both sides of the count", () => {
    const rollup = rollupTableStats([
      { qualified_name: "public.orders", read_rows: 100, inserted_rows: 100 },
      { qualified_name: "public.items", read_rows: 50, inserted_rows: 50 },
    ])
    expect(dataMovementVerdict(rollup)).toEqual({
      kind: "moved",
      written: 150,
      read: 150,
      tables: 2,
    })
  })

  it("keeps 'never reported a read' as null rather than folding it into zero", () => {
    // A CDC table reports applied counts and no read count. Summing it as 0 would
    // make a healthy run look like it read nothing.
    const rollup = rollupTableStats([{ qualified_name: "public.orders", applied_inserts: 12 }])
    expect(rollup.rowsRead).toBeNull()
    expect(dataMovementVerdict(rollup)).toEqual({
      kind: "moved",
      written: 12,
      read: null,
      tables: 1,
    })
  })
})

describe("movementHeadline", () => {
  it("does not describe an unmeasured run as a zero-row run", () => {
    const h = movementHeadline({ kind: "unmeasured" })
    expect(h.tone).toBe("unknown")
    expect(h.title).toMatch(/No table statistics/i)
    expect(h.detail).toMatch(/not the same as zero/i)
  })

  it("gives the silent-drop case the alarm tone", () => {
    const h = movementHeadline({ kind: "read-not-written", read: 4200, tables: 1 })
    expect(h.tone).toBe("alarm")
    expect(h.title).toBe("Read 4,200 rows, wrote none")
  })

  it("does not alarm on a correctly empty source", () => {
    const h = movementHeadline({ kind: "empty-source", tables: 1 })
    expect(h.tone).toBe("ok")
    expect(h.detail).toBe("1 table reported no rows at the source, so nothing needed copying.")
  })

  it("mentions the read count only when it differs from what was written", () => {
    expect(movementHeadline({ kind: "moved", written: 100, read: 100, tables: 2 }).detail).toBe(
      "Written to the destination across 2 tables.",
    )
    expect(movementHeadline({ kind: "moved", written: 90, read: 100, tables: 2 }).detail).toBe(
      "Written to the destination across 2 tables from 100 rows read.",
    )
  })
})

describe("rollupFromSummary — paging-proof totals", () => {
  it("uses the server's cross-table aggregates, not the returned page", () => {
    const rollup = rollupFromSummary({
      total_tables: 300,
      total_read_rows: 9_000,
      total_inserted_rows: 9_000,
      total_dlq_rows: 4,
      tables_failed: 1,
    })
    expect(rollup).toMatchObject({ tableCount: 300, rowsWritten: 9_000, dlqRows: 4, failedTables: 1 })
  })

  it("returns null when the response carried no summary", () => {
    expect(rollupFromSummary(null)).toBeNull()
  })
})

describe("planStagesFromEvents", () => {
  const stage = (id: string) => ({ id, display_name: id, status: "complete" })

  it("takes the newest plan snapshot by timestamp, not by seq", () => {
    // The stream carries two independent seq schemes (adapter 1..n vs orchestrator
    // snowflake ids), so a higher seq does not mean a later event.
    const events = [
      {
        seq: 9_000_000_000_000,
        occurred_at: "2026-08-05T14:01:20Z",
        payload: { metadata: { execution_plan: { stages: [stage("early")] } } },
      },
      {
        seq: 3,
        occurred_at: "2026-08-05T14:02:09Z",
        payload: { metadata: { execution_plan: { stages: [stage("final")] } } },
      },
    ]
    expect(planStagesFromEvents(events).map((s) => s.id)).toEqual(["final"])
  })

  it("falls back to received_at when occurred_at is absent", () => {
    const events = [
      {
        received_at: "2026-08-05T14:02:09Z",
        payload: { metadata: { execution_plan: { stages: [stage("only")] } } },
      },
    ]
    expect(planStagesFromEvents(events).map((s) => s.id)).toEqual(["only"])
  })

  it("returns an empty plan rather than throwing when no event carries one", () => {
    expect(planStagesFromEvents([{ event_type: "LOG", payload: {} }])).toEqual([])
    expect(planStagesFromEvents([])).toEqual([])
  })
})

describe("buildRunTimeline", () => {
  it("accounts for the wall clock no stage claims", () => {
    // Prod execution 32b9dba8: 14:01:13.624 → 14:02:09.180 = 55.6s wall clock,
    // of which the timed stages report 54s. The remainder is queueing/teardown
    // and was invisible on every screen before this.
    const timeline = buildRunTimeline(
      [
        { id: "infra_preflight", display_name: "Preflight", status: "complete", actual_duration_ms: 9_000 },
        { id: "executor", display_name: "Executor", status: "complete", actual_duration_ms: 45_000 },
      ],
      "2026-08-05T14:01:13.624Z",
      "2026-08-05T14:02:09.180Z",
    )
    expect(timeline.totalMs).toBe(55_556)
    expect(timeline.unaccountedMs).toBe(1_556)
  })

  it("reads legacy seconds through the same helper the DAG uses", () => {
    const timeline = buildRunTimeline(
      [{ id: "executor", display_name: "Executor", status: "complete", actual_duration: 44 }],
      "2026-08-05T14:01:13Z",
      "2026-08-05T14:02:09Z",
    )
    expect(timeline.phases[0].ms).toBe(44_000)
  })

  it("clamps a negative remainder — overlapping stages can sum past the wall clock", () => {
    const timeline = buildRunTimeline(
      [
        { id: "a", display_name: "A", status: "complete", actual_duration_ms: 40_000 },
        { id: "b", display_name: "B", status: "complete", actual_duration_ms: 40_000 },
      ],
      "2026-08-05T14:01:13Z",
      "2026-08-05T14:02:09Z",
    )
    expect(timeline.unaccountedMs).toBe(0)
  })

  it("leaves an untimed stage null instead of claiming it took zero", () => {
    const timeline = buildRunTimeline(
      [{ id: "a", display_name: "A", status: "pending" }],
      "2026-08-05T14:01:13Z",
      null,
    )
    expect(timeline.phases[0].ms).toBeNull()
    expect(timeline.totalMs).toBeNull()
    expect(timeline.unaccountedMs).toBeNull()
  })
})

describe("computeRunDelta", () => {
  const run = (id: string, start: string, end: string) => ({
    id,
    start_time: start,
    end_time: end,
  })

  it("compares rows and duration against the previous run", () => {
    const delta = computeRunDelta(
      1_200,
      run("cur", "2026-08-05T14:00:00Z", "2026-08-05T14:01:00Z"),
      1_000,
      run("prev", "2026-08-04T14:00:00Z", "2026-08-04T14:00:40Z"),
    )
    expect(delta).toEqual({
      previousId: "prev",
      rowsDelta: 200,
      durationDeltaMs: 20_000,
      sameRows: false,
    })
  })

  it("refuses to compute a row delta against an unmeasured run", () => {
    const delta = computeRunDelta(
      1_200,
      run("cur", "2026-08-05T14:00:00Z", "2026-08-05T14:01:00Z"),
      null,
      run("prev", "2026-08-04T14:00:00Z", "2026-08-04T14:00:40Z"),
    )
    expect(delta?.rowsDelta).toBeNull()
    expect(delta?.sameRows).toBe(false)
    expect(delta?.durationDeltaMs).toBe(20_000)
  })

  it("returns null when there is no previous run", () => {
    expect(
      computeRunDelta(1, run("cur", "2026-08-05T14:00:00Z", "2026-08-05T14:01:00Z"), 1, null),
    ).toBeNull()
  })
})

describe("formatBytes", () => {
  it("returns null — not '0 B' — for a metric that was never populated", () => {
    expect(formatBytes(null)).toBeNull()
    expect(formatBytes(undefined)).toBeNull()
  })

  it("still renders a real zero", () => {
    expect(formatBytes(0)).toBe("0 B")
  })

  it("scales up", () => {
    expect(formatBytes(2_048)).toBe("2.0 KB")
    expect(formatBytes(5 * 1024 * 1024)).toBe("5.0 MB")
  })
})
