/**
 * The Throughput card shows a rate, not just a lifetime total.
 *
 * Its CAPTURED/APPLIED numbers come from `pipeline_run_table_stats` and are
 * cumulative — "72,670 since the stream started". Under a heading that means "per
 * unit time" they read as a rate and are not one, which is why a busy pipeline and
 * an idle one look identical: both show a big number that does not visibly move
 * between glances. The totals now say what they are, and a rate is derived
 * alongside them.
 *
 * The rules worth pinning are the ones where a wrong answer is worse than no
 * answer: too little data must report "measuring…", never 0/min (a claim that
 * nothing is moving), and a counter that went backwards is a reset, not negative
 * throughput.
 */

import { describe, it, expect } from "vitest"

import {
  EMPTY_RATE_WINDOW,
  computeRatePerMin,
  formatRatePerMin,
  lastMovedDetail,
  movedSinceOpen,
  nextRateWindow,
  sinceOpenLine,
} from "@/components/pipeline/MonitorTab"

const t0 = 1_700_000_000_000

describe("computeRatePerMin", () => {
  it("needs two readings far enough apart to divide by", () => {
    expect(computeRatePerMin([])).toBeNull()
    expect(computeRatePerMin([{ t: t0, v: 100 }])).toBeNull()
    // 5s apart: the poll interval. One flush landing or not is quantisation
    // noise, not a rate, so this is deliberately refused.
    expect(computeRatePerMin([{ t: t0, v: 100 }, { t: t0 + 5_000, v: 200 }])).toBeNull()
  })

  it("computes events per minute over the sample span", () => {
    // 600 events in 60s = 600/min.
    expect(
      computeRatePerMin([
        { t: t0, v: 1_000 },
        { t: t0 + 60_000, v: 1_600 },
      ]),
    ).toBe(600)

    // 30 events in 30s = 60/min.
    expect(
      computeRatePerMin([
        { t: t0, v: 0 },
        { t: t0 + 30_000, v: 30 },
      ]),
    ).toBe(60)
  })

  it("spans the whole window, not just the last pair", () => {
    // Intermediate samples must not change the answer: first → last is the span.
    expect(
      computeRatePerMin([
        { t: t0, v: 0 },
        { t: t0 + 20_000, v: 500 },
        { t: t0 + 40_000, v: 500 },
        { t: t0 + 60_000, v: 600 },
      ]),
    ).toBe(600)
  })

  it("reports a genuine standstill as 0, not as unknown", () => {
    // A measured "nothing moved in the last minute" is a finding and must reach
    // the user; it is the unmeasured case that has to stay silent.
    expect(
      computeRatePerMin([
        { t: t0, v: 4_200 },
        { t: t0 + 60_000, v: 4_200 },
      ]),
    ).toBe(0)
  })

  it("treats a counter that went backwards as a reset, not negative throughput", () => {
    // A new run, or a stats row re-seeded from the DB after an orchestrator
    // restart. "-500/min" would be nonsense on a card about volume.
    expect(
      computeRatePerMin([
        { t: t0, v: 5_000 },
        { t: t0 + 60_000, v: 100 },
      ]),
    ).toBeNull()
  })
})

describe("formatRatePerMin", () => {
  it("says it is still measuring rather than claiming zero", () => {
    // The distinction this card exists to make: no reading is not "nothing is
    // moving".
    expect(formatRatePerMin(null)).toBe("measuring…")
  })

  it("distinguishes a real zero from a trickle", () => {
    expect(formatRatePerMin(0)).toBe("0 / min")
    // Rounding 0.4 to "0 / min" would report an active pipeline as stopped.
    expect(formatRatePerMin(0.4)).toBe("<1 / min")
  })

  it("rounds and groups a working rate", () => {
    expect(formatRatePerMin(1)).toBe("1 / min")
    expect(formatRatePerMin(1234.6)).toBe("1,235 / min")
  })
})

// "Since this page opened": the server serves lifetime totals and no history, so
// the page's own first reading is the only honest origin for a longer window.
describe("nextRateWindow / movedSinceOpen", () => {
  const t0 = 1_700_000_000_000

  it("measures from the page's first reading, past the 60 s rate window", () => {
    let w = EMPTY_RATE_WINDOW
    for (let i = 0; i <= 36; i++) w = nextRateWindow(w, 1000 + i * 10, t0 + i * 5000)
    // The rate window dropped the early samples; the origin did not.
    expect(w.samples[0].t).toBeGreaterThan(t0)
    expect(movedSinceOpen(w)).toEqual({ delta: 360, spanMs: 180_000 })
  })

  it("has nothing to say before the first reading", () => {
    expect(movedSinceOpen(EMPTY_RATE_WINDOW)).toBeNull()
  })

  it("starts again when the counter goes backwards, never reporting a negative move", () => {
    let w = nextRateWindow(EMPTY_RATE_WINDOW, 500, t0)
    w = nextRateWindow(w, 600, t0 + 5000)
    w = nextRateWindow(w, 20, t0 + 10_000)
    expect(movedSinceOpen(w)).toEqual({ delta: 0, spanMs: 0 })
    w = nextRateWindow(w, 50, t0 + 15_000)
    expect(movedSinceOpen(w)).toEqual({ delta: 30, spanMs: 5000 })
  })

  it("forgets the origin when the counter stops being measured", () => {
    let w = nextRateWindow(EMPTY_RATE_WINDOW, 500, t0)
    w = nextRateWindow(w, undefined, t0 + 5000)
    expect(movedSinceOpen(w)).toBeNull()
    w = nextRateWindow(w, 900, t0 + 10_000)
    expect(movedSinceOpen(w)).toEqual({ delta: 0, spanMs: 0 })
  })
})

describe("sinceOpenLine", () => {
  it("waits for a whole minute on both sides, so it never reads '(0 min)'", () => {
    expect(sinceOpenLine(["captured", "written"], [{ delta: 5, spanMs: 59_000 }, { delta: 5, spanMs: 120_000 }])).toBeNull()
    expect(sinceOpenLine(["captured", "written"], [null, { delta: 5, spanMs: 120_000 }])).toBeNull()
  })

  it("prints both moves under one span when both sides count from the same reading", () => {
    expect(
      sinceOpenLine(["captured", "written"], [{ delta: 12_000, spanMs: 185_000 }, { delta: 11_500, spanMs: 185_000 }]),
    ).toBe("Since this page opened (3 min): +12,000 captured · +11,500 written")
  })

  // The written counter was unmeasured for the first 5 minutes: one shared
  // "(1 min)" would put 6 minutes of captured rows under 1 minute.
  it("gives each side its own span when they started counting at different readings", () => {
    expect(
      sinceOpenLine(["captured", "written"], [{ delta: 6_000, spanMs: 360_000 }, { delta: 80, spanMs: 60_000 }]),
    ).toBe("Since this page opened: +6,000 captured in 6 min · +80 written in 1 min")
  })
})

describe("lastMovedDetail", () => {
  const age = (iso: string) => `<${iso}>`

  it("takes the newest time on each side across the tables", () => {
    const tables = [
      { last_event_ts: "2026-09-25T10:00:00Z", last_applied_ts: "2026-09-25T09:00:00Z" },
      { last_event_ts: "2026-09-25T11:00:00Z", last_applied_ts: "2026-09-25T10:30:00Z" },
    ]
    expect(lastMovedDetail(tables, 2, age)).toBe(
      "last captured <2026-09-25T11:00:00Z>, last written <2026-09-25T10:30:00Z>",
    )
  })

  it("says which side has not moved yet", () => {
    expect(lastMovedDetail([{ last_event_ts: "2026-09-25T10:00:00Z", last_applied_ts: null }], 1, age)).toBe(
      "last captured <2026-09-25T10:00:00Z>, nothing written yet",
    )
  })

  it("says nothing on one page of several: page one's newest is not the pipeline's", () => {
    const tables = [{ last_event_ts: "2026-09-25T10:00:00Z", last_applied_ts: "2026-09-25T10:00:00Z" }]
    expect(lastMovedDetail(tables, 120, age)).toBeNull()
  })

  it("says nothing with no tables or no timestamps", () => {
    expect(lastMovedDetail(undefined, undefined, age)).toBeNull()
    expect(lastMovedDetail([], 0, age)).toBeNull()
    expect(lastMovedDetail([{ last_event_ts: null, last_applied_ts: null }], 1, age)).toBeNull()
  })
})
