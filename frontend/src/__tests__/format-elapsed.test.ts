import { describe, expect, it } from "vitest"
import { formatElapsed } from "@/lib/utils"

// Durations truncate at every unit. The executions list used Math.round on the
// leading unit, so 4m45s read "5m 45s", 1h45m read "2h 45m" and 2m59.6s read "2m 60s".
describe("formatElapsed", () => {
  it("truncates minutes instead of rounding them up", () => {
    expect(formatElapsed((4 * 60 + 45) * 1000)).toBe("4m 45s")
  })

  it("truncates hours instead of rounding them up", () => {
    expect(formatElapsed((105 * 60) * 1000)).toBe("1h 45m")
  })

  it("never prints 60 seconds", () => {
    expect(formatElapsed((2 * 60 + 59.6) * 1000)).toBe("2m 59s")
  })

  it("keeps the short forms", () => {
    expect(formatElapsed(850)).toBe("850ms")
    // A tenth of a second below the minute. This read "12s" while the pipeline
    // page's formatter said "12.9s" for the same stage; one of the two had to
    // give, and dropping 0.9s off a 12.9s stage is a 7% misreport.
    expect(formatElapsed(12_900)).toBe("12.9s")
  })

  it("shows a dash for a negative or non-finite span", () => {
    expect(formatElapsed(-5)).toBe("—")
    expect(formatElapsed(Number.NaN)).toBe("—")
  })
})
