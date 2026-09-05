import { describe, it, expect } from "vitest"

// Task 0 — proves the Vitest harness is wired correctly. The real
// Data Explorer suites live alongside their modules in this directory.
describe("vitest harness", () => {
  it("runs", () => {
    expect(1 + 1).toBe(2)
  })
})
