import { describe, expect, it } from "vitest"

import {
  describePolicy,
  validateRetention,
  MIN_VERSIONS_CEIL,
  MIN_VERSIONS_FLOOR,
  RETENTION_DAYS_MAX,
  RETENTION_DAYS_MIN,
} from "@/components/workspace/SavedQueryRetentionCard"

// The card is the only way to reach PUT /api/v1/explorer/version-retention, and
// the server rejects an out-of-range policy with a 400 that never reaches the
// user as anything but a toast. These tests pin the client-side bounds to the
// server's (saved_query_retention.go), so widening one silently is a failure.

describe("validateRetention", () => {
  it("accepts a policy inside both ranges", () => {
    expect(validateRetention({ retention_days: 30, min_versions: 10 })).toBeNull()
  })

  it("accepts every boundary value the server accepts", () => {
    expect(validateRetention({ retention_days: RETENTION_DAYS_MIN, min_versions: MIN_VERSIONS_FLOOR })).toBeNull()
    expect(validateRetention({ retention_days: RETENTION_DAYS_MAX, min_versions: MIN_VERSIONS_CEIL })).toBeNull()
  })

  it("rejects one step outside each boundary", () => {
    // If a bound is ever widened without widening the server's, these four
    // start returning null and the test fails.
    expect(validateRetention({ retention_days: 30, min_versions: MIN_VERSIONS_FLOOR - 1 })).toMatch(/between 5 and 1000/)
    expect(validateRetention({ retention_days: 30, min_versions: MIN_VERSIONS_CEIL + 1 })).toMatch(/between 5 and 1000/)
    expect(validateRetention({ retention_days: RETENTION_DAYS_MIN - 1, min_versions: 10 })).toMatch(/between 1 and 3650/)
    expect(validateRetention({ retention_days: RETENTION_DAYS_MAX + 1, min_versions: 10 })).toMatch(/between 1 and 3650/)
  })

  it("rejects fractional values before they reach a Go int", () => {
    expect(validateRetention({ retention_days: 30, min_versions: 10.5 })).toMatch(/whole number/)
    expect(validateRetention({ retention_days: 30.5, min_versions: 10 })).toMatch(/whole number/)
  })

  it("treats null retention_days as keep-forever, not as a bad number", () => {
    expect(validateRetention({ retention_days: null, min_versions: 10 })).toBeNull()
  })

  it("still enforces min_versions when history is kept forever", () => {
    // min_versions is required on every PUT, including the forever policy.
    expect(validateRetention({ retention_days: null, min_versions: 1 })).toMatch(/between 5 and 1000/)
  })
})

describe("describePolicy", () => {
  it("says nothing is deleted when history is kept forever", () => {
    expect(describePolicy({ retention_days: null, min_versions: 10 })).toBe(
      "Every version is kept forever. Nothing is deleted."
    )
  })

  it("names both axes, because either one alone is misleading", () => {
    const s = describePolicy({ retention_days: 30, min_versions: 10 })
    expect(s).toContain("30 days")
    expect(s).toContain("newest 10 versions")
    expect(s).toContain("however old they are")
  })

  it("uses the singular for a one-day policy", () => {
    expect(describePolicy({ retention_days: 1, min_versions: 5 })).toContain("older than 1 day are")
  })
})
