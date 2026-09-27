/**
 * Table-stats rows as the Tables card and "Edit tables" read them: a table no
 * longer selected (`status: "removed"`) is split out of the selection (#5), and
 * a CDC row's counts are never read as run counts (#9). Each rule has the
 * control it differs from.
 */

import { describe, expect, it } from "vitest"

import { makeTableMatcher, parseTableStatsRows } from "@/lib/pipeline/tableStatsRows"

describe("parseTableStatsRows", () => {
  it("splits removed tables out of the selection, and keeps a streaming-only one in it", () => {
    const parsed = parseTableStatsRows([
      { qualified_name: "public.orders", mode: "cdc", status: "running" },
      { qualified_name: "public.items", mode: "cdc", status: "running", load_mode: "streaming_only" },
      { qualified_name: "public.events", mode: "cdc", status: "removed" },
    ])
    expect(parsed.names).toEqual(["public.orders", "public.items"])
    expect(parsed.removed).toEqual(["public.events"])
  })

  it("never reads a CDC row's inserted_rows as a run count", () => {
    const rows = [{ qualified_name: "public.orders", table_name: "orders", mode: "cdc", inserted_rows: 7, read_rows: 9 }]
    expect(parseTableStatsRows(rows).written).toEqual({})
    // Also when the caller knows the pipeline is CDC but the row has no mode.
    expect(parseTableStatsRows([{ qualified_name: "public.orders", inserted_rows: 7 }], { cdc: true }).written).toEqual({})
  })

  it("control: a batch row's run counts are kept, by qualified and bare name", () => {
    const parsed = parseTableStatsRows([
      { qualified_name: "public.orders", table_name: "orders", mode: "batch", inserted_rows: 7, read_rows: "9" },
    ])
    expect(parsed.written).toEqual({ "public.orders": 7, orders: 7 })
  })

  it("builds a name from schema + table when there is no qualified name, and skips nameless rows", () => {
    const parsed = parseTableStatsRows([{ schema_name: "sales", table_name: "orders" }, { mode: "cdc" }])
    expect(parsed.names).toEqual(["sales.orders"])
  })
})

describe("makeTableMatcher", () => {
  it("matches exactly or on the last two segments", () => {
    const m = makeTableMatcher(["public.orders"])
    expect(m("public.orders")).toBe(true)
    expect(m("PUBLIC.Orders")).toBe(true)
    expect(m("shop.public.orders")).toBe(true)
    expect(m("sales.orders")).toBe(false)
  })

  it("a bare name matches only when one table carries it", () => {
    expect(makeTableMatcher(["public.users"])("users")).toBe(true)
    expect(makeTableMatcher(["public.users", "audit.users"])("users")).toBe(false)
  })
})
