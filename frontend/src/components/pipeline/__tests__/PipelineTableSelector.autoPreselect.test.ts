import { describe, it, expect, vi } from "vitest"

// Importing the component module pulls in UI deps and the auth-fetch client;
// stub the latter so no effect touches the network at import time.
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(async () => ({ ok: true, status: 200, json: async () => ({}) })),
}))

import {
  computeAutoPreselectKeys,
  AUTO_SELECT_MIN_CONFIDENCE,
  type SuggestedTable,
} from "../PipelineTableSelector"

// Build a tableKey -> suggestion map like the component's suggestionMatch memo.
function matchOf(entries: Array<[string, number | undefined]>): Map<string, SuggestedTable> {
  const m = new Map<string, SuggestedTable>()
  for (const [key, confidence] of entries) {
    const [schema, name] = key.includes(".") ? key.split(".") : ["", key]
    m.set(key, { name, schema: schema || undefined, confidence })
  }
  return m
}

describe("computeAutoPreselectKeys (AI pre-selection confidence gate)", () => {
  it("selects a single clear high-confidence winner", () => {
    const match = matchOf([
      ["public.demo_products", 0.95],
      ["public.orders", 0.4],
    ])
    expect(computeAutoPreselectKeys(match, 2)).toEqual(["public.demo_products"])
  })

  it("selects a confident cluster that still narrows the set", () => {
    const match = matchOf([
      ["public.a", 0.95],
      ["public.b", 0.9],
      ["public.c", 0.3],
      ["public.d", 0.2],
    ])
    expect(computeAutoPreselectKeys(match, 4).sort()).toEqual(["public.a", "public.b"])
  })

  it("selects NOTHING when the ranker is confident about EVERY available table (the reported bug)", () => {
    // All 6 tables returned at high confidence — the 'top N of N' failure mode.
    const match = matchOf([
      ["pipeline_test.demo_products", 0.95],
      ["pipeline_test.demo_customers", 0.92],
      ["pipeline_test.demo_orders", 0.9],
      ["pipeline_test.demo_items", 0.88],
      ["pipeline_test.a", 0.87],
      ["pipeline_test.b", 0.86],
    ])
    expect(computeAutoPreselectKeys(match, 6)).toEqual([])
  })

  it("selects NOTHING when no table clears the confidence bar", () => {
    const match = matchOf([
      ["public.a", 0.6],
      ["public.b", 0.5],
    ])
    expect(computeAutoPreselectKeys(match, 4)).toEqual([])
  })

  it("treats missing confidence as not-a-winner", () => {
    const match = matchOf([
      ["public.a", undefined],
      ["public.b", 0.9],
    ])
    expect(computeAutoPreselectKeys(match, 3)).toEqual(["public.b"])
  })

  it("empty match → empty", () => {
    expect(computeAutoPreselectKeys(new Map(), 5)).toEqual([])
  })

  it("threshold is 0.85 (inclusive)", () => {
    expect(AUTO_SELECT_MIN_CONFIDENCE).toBe(0.85)
    const atBar = matchOf([
      ["public.a", 0.85],
      ["public.b", 0.2],
    ])
    expect(computeAutoPreselectKeys(atBar, 2)).toEqual(["public.a"])
    const belowBar = matchOf([
      ["public.a", 0.849],
      ["public.b", 0.2],
    ])
    expect(computeAutoPreselectKeys(belowBar, 2)).toEqual([])
  })
})
