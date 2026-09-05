import { describe, expect, it } from "vitest"
import { sameCounts } from "@/lib/pipeline/rowCounts"

describe("sameCounts", () => {
  it("treats identical maps as unchanged so the refresh costs no source query", () => {
    expect(sameCounts({}, {})).toBe(true)
    expect(sameCounts({ "public.orders": 6 }, { "public.orders": 6 })).toBe(true)
    expect(
      sameCounts({ "public.orders": 6, orders: 6 }, { orders: 6, "public.orders": 6 })
    ).toBe(true)
  })

  it("detects a moved count — the case the refresh exists for", () => {
    expect(sameCounts({ "public.orders": 3 }, { "public.orders": 6 })).toBe(false)
  })

  it("detects added and removed tables, not just changed numbers", () => {
    expect(sameCounts({ a: 1 }, { a: 1, b: 2 })).toBe(false)
    expect(sameCounts({ a: 1, b: 2 }, { a: 1 })).toBe(false)
    // Same size, different keys: a length check alone would call this unchanged.
    expect(sameCounts({ a: 1 }, { b: 1 })).toBe(false)
  })

  it("does not treat 0 and a missing table as the same thing", () => {
    // `b[k]` is undefined for a table that dropped out of the stats response;
    // a loose comparison would read that as 0 and hide the difference.
    expect(sameCounts({ a: 0 }, {})).toBe(false)
  })

  it("compares NaN to itself as equal so a bad reading cannot loop forever", () => {
    expect(sameCounts({ a: NaN }, { a: NaN })).toBe(true)
  })
})
