import { describe, it, expect } from "vitest"
import { getExamplePrompts } from "../examplePrompts"

describe("getExamplePrompts", () => {
  it("returns generic, non-empty examples when no tables are known", () => {
    const examples = getExamplePrompts([])
    expect(examples.length).toBeGreaterThanOrEqual(3)
    for (const ex of examples) {
      expect(ex.label.length).toBeGreaterThan(0)
      expect(ex.prompt.length).toBeGreaterThan(0)
    }
  })

  it("tailors examples to a real table name when one is available", () => {
    const examples = getExamplePrompts([{ name: "orders" }])
    expect(examples.some((ex) => ex.prompt.includes("orders"))).toBe(true)
  })

  it("adds a join example only when at least two tables exist", () => {
    const single = getExamplePrompts([{ name: "orders" }])
    expect(single.some((ex) => /join/i.test(ex.prompt))).toBe(false)

    const pair = getExamplePrompts([{ name: "orders" }, { name: "users" }])
    const join = pair.find((ex) => /join/i.test(ex.prompt))
    expect(join).toBeDefined()
    expect(join!.prompt).toContain("orders")
    expect(join!.prompt).toContain("users")
  })

  it("is deterministic for the same input", () => {
    const a = getExamplePrompts([{ name: "orders" }])
    const b = getExamplePrompts([{ name: "orders" }])
    expect(a).toEqual(b)
  })
})

describe("getExamplePrompts — internal-table filtering + polish", () => {
  it("excludes rsync internal tables and tailors to a real business table (BUG-B)", () => {
    const ex = getExamplePrompts([
      { name: "_rsync_cdc_offsets" },
      { name: "_rsync_pipelines" },
      { name: "orders" },
    ])
    expect(ex.some((e) => e.prompt.includes("orders"))).toBe(true)
    expect(ex.some((e) => /_rsync_/.test(e.prompt) || /_rsync_/.test(e.label))).toBe(false)
  })

  it("falls back to generic examples when every table is internal", () => {
    const ex = getExamplePrompts([{ name: "_rsync_cdc_offsets" }, { name: "_rsync_pipelines" }])
    expect(ex.length).toBeGreaterThanOrEqual(3)
    expect(ex.some((e) => /_rsync_/.test(e.prompt))).toBe(false)
  })

  it("never emits a self-join chip when the two candidate tables share a bare name", () => {
    const ex = getExamplePrompts([
      { name: "orders", schema: "public" },
      { name: "orders", schema: "reporting" },
    ])
    expect(ex.some((e) => /Join orders \+ orders/.test(e.label))).toBe(false)
  })

  it("uses grammar-safe phrasing that reads for singular table names", () => {
    const ex = getExamplePrompts([{ name: "customer" }])
    expect(ex.some((e) => /most recent customer\b/.test(e.prompt))).toBe(false)
    expect(ex.some((e) => /rows from customer\b/.test(e.prompt))).toBe(true)
  })
})
