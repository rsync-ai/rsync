import { describe, expect, it } from "vitest"
import { dependencyStatusLabel } from "../dependencyStatus"

describe("dependencyStatusLabel", () => {
  it("reads 'Checking…' for a dependency the prober has not checked yet", () => {
    expect(dependencyStatusLabel({ status: "unknown" })).toBe("Checking…")
    expect(dependencyStatusLabel({ status: "unknown", last_checked_at: null })).toBe("Checking…")
  })

  it("keeps 'unknown' once a probe ran and could not decide", () => {
    expect(dependencyStatusLabel({ status: "unknown", last_checked_at: "2026-09-16T10:00:00Z" })).toBe("unknown")
  })

  it("passes checked statuses through", () => {
    expect(dependencyStatusLabel({ status: "healthy" })).toBe("healthy")
    expect(dependencyStatusLabel({ status: "unhealthy", last_checked_at: "2026-09-16T10:00:00Z" })).toBe("unhealthy")
  })
})
