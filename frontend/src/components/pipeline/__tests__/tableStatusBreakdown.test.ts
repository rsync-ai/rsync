import { describe, expect, it } from "vitest"

import {
  dataMovementVerdict,
  rollupFromSummary,
  rollupTableStats,
  tableStatusBreakdown,
  TABLE_STATUS_WAITING_FOR_DATA,
  type TableStatusBreakdown,
} from "../executionSummary"

// The shape api-gateway's computeCDCSummary returns for a pipeline that just
// finished setting up: every table is counted in total_tables, and a table with
// no stats row yet is counted in tables_waiting_for_data — not tables_running.
const NEW_CDC_PIPELINE = {
  total_tables: 5,
  tables_completed: 1,
  tables_failed: 0,
  tables_running: 1,
  tables_degraded: 1,
  tables_waiting_for_data: 2,
}

const shown = (b: TableStatusBreakdown) => b.completed + b.rows.reduce((sum, r) => sum + r.count, 0)

describe("tableStatusBreakdown — the Monitor tab's table counts add up", () => {
  it("accounts for every table, including ones with no data yet", () => {
    const b = tableStatusBreakdown(NEW_CDC_PIPELINE)
    expect(b.total).toBeGreaterThan(0)
    expect(shown(b)).toBe(b.total)
    expect(b.rows).toEqual([
      { key: "degraded", label: "Tables degraded", count: 1 },
      { key: "running", label: "Tables running", count: 1 },
      { key: TABLE_STATUS_WAITING_FOR_DATA, label: "Tables with no data yet", count: 2 },
    ])
  })

  it("control: the old footer (completed, failed, running) left tables unaccounted for", () => {
    const s = NEW_CDC_PIPELINE
    const oldFooter = s.tables_completed + s.tables_failed + s.tables_running
    expect(oldFooter).toBeLessThan(s.total_tables)
  })

  it("puts a status this client does not name into 'another state' rather than dropping it", () => {
    // e.g. a newer gateway that counts a status the breakdown has no bucket for.
    const b = tableStatusBreakdown({ total_tables: 4, tables_completed: 2, tables_running: 1 })
    expect(shown(b)).toBe(4)
    expect(b.rows.find((r) => r.key === "other")).toEqual({
      key: "other",
      label: "Tables in another state",
      count: 1,
    })
  })

  it("reads a gateway that predates tables_waiting_for_data as 'another state'", () => {
    const b = tableStatusBreakdown({ total_tables: 3, tables_completed: 0, tables_running: 0 })
    expect(shown(b)).toBe(3)
    expect(b.rows.map((r) => r.key)).toEqual(["other"])
  })

  it("shows no zero rows, and nothing extra when all tables completed", () => {
    const b = tableStatusBreakdown({
      total_tables: 3,
      tables_completed: 3,
      tables_failed: 0,
      tables_running: 0,
      tables_degraded: 0,
      tables_waiting_for_data: 0,
    })
    expect(b.rows).toEqual([])
    expect(shown(b)).toBe(3)
  })
})

describe("waiting-for-data tables in the execution rollups", () => {
  it("counts them separately from running tables", () => {
    const r = rollupTableStats([
      { qualified_name: "db.a", status: "running", applied_inserts: 3 },
      { qualified_name: "db.b", status: TABLE_STATUS_WAITING_FOR_DATA },
      { qualified_name: "db.c", status: TABLE_STATUS_WAITING_FOR_DATA },
    ])
    expect(r).toMatchObject({ tableCount: 3, runningTables: 1, waitingForDataTables: 2 })
  })

  it("reads tables_waiting_for_data from the server summary", () => {
    expect(rollupFromSummary(NEW_CDC_PIPELINE)).toMatchObject({ tableCount: 5, runningTables: 1, waitingForDataTables: 2 })
    expect(rollupFromSummary({ total_tables: 2 })).toMatchObject({ waitingForDataTables: 0 })
  })

  it("does not call tables that reported nothing an empty source", () => {
    const allWaiting = rollupTableStats([
      { qualified_name: "db.a", status: TABLE_STATUS_WAITING_FOR_DATA },
      { qualified_name: "db.b", status: TABLE_STATUS_WAITING_FOR_DATA },
    ])
    expect(dataMovementVerdict(allWaiting)).toEqual({ kind: "unmeasured" })

    // Control: the same tables having reported zero rows really are an empty source.
    const reportedZero = rollupTableStats([
      { qualified_name: "db.a", status: "completed", read_rows: 0, inserted_rows: 0 },
      { qualified_name: "db.b", status: "completed", read_rows: 0, inserted_rows: 0 },
    ])
    expect(dataMovementVerdict(reportedZero)).toEqual({ kind: "empty-source", tables: 2 })
  })

  it("does not call a run an empty source while any table has not reported", () => {
    // A large first sync: one collection finished empty, the next has not been reached.
    const partlyReported = rollupTableStats([
      { qualified_name: "db.a", status: "completed", read_rows: 0, inserted_rows: 0 },
      { qualified_name: "db.b", status: TABLE_STATUS_WAITING_FOR_DATA },
    ])
    expect(partlyReported.tableCount).toBeGreaterThan(partlyReported.waitingForDataTables)
    expect(dataMovementVerdict(partlyReported)).toEqual({ kind: "unmeasured" })
  })

  it("control: running tables that reported zero rows are measured, so they are an empty source", () => {
    const runningZero = rollupTableStats([
      { qualified_name: "db.a", status: "running", read_rows: 0, applied_inserts: 0 },
      { qualified_name: "db.b", status: "running", read_rows: 0, applied_inserts: 0 },
    ])
    expect(runningZero).toMatchObject({ tableCount: 2, runningTables: 2, waitingForDataTables: 0 })
    expect(dataMovementVerdict(runningZero)).toEqual({ kind: "empty-source", tables: 2 })
  })

  it("keeps 'moved' when some tables moved rows and others are still waiting", () => {
    const mixed = rollupTableStats([
      { qualified_name: "db.a", status: "running", applied_inserts: 3 },
      { qualified_name: "db.b", status: TABLE_STATUS_WAITING_FOR_DATA },
    ])
    expect(mixed.waitingForDataTables).toBe(1)
    expect(dataMovementVerdict(mixed)).toEqual({ kind: "moved", written: 3, read: null, tables: 2 })
  })

  it("keeps the read-but-not-written alarm when other tables are still waiting", () => {
    const readNotWritten = rollupTableStats([
      { qualified_name: "db.a", status: "running", read_rows: 5, applied_inserts: 0 },
      { qualified_name: "db.b", status: TABLE_STATUS_WAITING_FOR_DATA },
    ])
    expect(dataMovementVerdict(readNotWritten)).toEqual({ kind: "read-not-written", read: 5, tables: 2 })
  })

  it("lets rows the server counted win over tables that all read as waiting", () => {
    // ExecutionDetailDialog replaces rowsWritten with the run's records_processed.
    const summary = rollupFromSummary({ total_tables: 2, tables_waiting_for_data: 2 })
    expect(summary).not.toBeNull()
    expect(summary?.waitingForDataTables).toBe(summary?.tableCount)
    expect(dataMovementVerdict(summary)).toEqual({ kind: "unmeasured" })
    expect(dataMovementVerdict(summary && { ...summary, rowsWritten: 7 })).toEqual({
      kind: "moved",
      written: 7,
      read: null,
      tables: 2,
    })
  })
})
