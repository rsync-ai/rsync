import { describe, it, expect } from "vitest"
import { looksMasked, maskedColumns } from "../redactedColumns"

describe("looksMasked", () => {
  it("accepts exactly the two shapes redactForPreview produces", () => {
    // len > 4 keeps two characters; len <= 4 keeps none.
    expect(looksMasked("ab***")).toBe(true)
    expect(looksMasked("***")).toBe(true)
  })

  it("rejects values that merely end in asterisks", () => {
    expect(looksMasked("a***")).toBe(false)
    expect(looksMasked("abc***")).toBe(false)
    expect(looksMasked("****")).toBe(false)
    expect(looksMasked("ab**")).toBe(false)
    expect(looksMasked("")).toBe(false)
  })

  it("rejects non-strings", () => {
    expect(looksMasked(null)).toBe(false)
    expect(looksMasked(undefined)).toBe(false)
    expect(looksMasked(42)).toBe(false)
    expect(looksMasked({ v: "ab***" })).toBe(false)
  })
})

describe("maskedColumns", () => {
  it("flags a column whose every non-empty value is masked", () => {
    const rows = [
      { id: 1, email: "ra***", city: "Delhi" },
      { id: 2, email: "jo***", city: "Pune" },
      { id: 3, email: "***", city: "Goa" },
    ]
    expect(maskedColumns(["id", "email", "city"], rows)).toEqual(["email"])
  })

  it("ignores NULL and empty values rather than treating them as unmasked", () => {
    const rows = [
      { token: "ab***" },
      { token: "" },
      { token: null },
    ]
    expect(maskedColumns(["token"], rows)).toEqual(["token"])
  })

  it("does not flag a column with no evidence either way", () => {
    // All NULL: redaction and a genuinely empty column look identical.
    expect(maskedColumns(["token"], [{ token: null }, { token: "" }])).toEqual([])
  })

  it("does not flag a column where one real value slipped through", () => {
    const rows = [{ note: "ab***" }, { note: "ab***" }, { note: "shipped" }]
    expect(maskedColumns(["note"], rows)).toEqual([])
  })

  it("returns nothing for an empty or missing result set", () => {
    expect(maskedColumns([], [{ a: "***" }])).toEqual([])
    expect(maskedColumns(["a"], [])).toEqual([])
    expect(maskedColumns(undefined, undefined)).toEqual([])
  })
})
