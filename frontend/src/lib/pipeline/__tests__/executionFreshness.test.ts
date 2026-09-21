import { describe, expect, it } from "vitest"

import { pickExecutionFreshness } from "../executionFreshness"

const NOW = Date.parse("2026-09-18T10:00:00Z")
const HOURS_18 = 18 * 3600

describe("pickExecutionFreshness", () => {
  it("measures a live stream from its last event, matching the pipeline page (#56 retest)", () => {
    // Prod: no end time, no transform logs -> the card read "—" beside "18h ago".
    const f = pickExecutionFreshness({
      status: "running",
      liveStream: true,
      finishedAt: null,
      transformTimes: [],
      liveness: { stale_seconds: HOURS_18, last_event_at: "2026-09-17T16:00:00Z" },
      now: NOW,
    })
    expect(f).toEqual({ at: NOW - HOURS_18 * 1000, label: "Last event" })
  })

  it("falls back to last_event_at when the gateway sent no stale_seconds", () => {
    const f = pickExecutionFreshness({
      status: "running",
      liveStream: true,
      finishedAt: null,
      transformTimes: [],
      liveness: { last_event_at: "2026-09-17T16:00:00Z" },
      now: NOW,
    })
    expect(f).toEqual({ at: Date.parse("2026-09-17T16:00:00Z"), label: "Last event" })
  })

  it("keeps the landed time for a live stream whose runtime could not be read", () => {
    const f = pickExecutionFreshness({
      status: "running",
      liveStream: true,
      finishedAt: null,
      transformTimes: ["2026-09-18T09:00:00Z"],
      liveness: null,
      now: NOW,
    })
    expect(f).toEqual({ at: Date.parse("2026-09-18T09:00:00Z"), label: "Last activity" })
  })

  it("ignores liveness for a batch run: its freshness is when it landed", () => {
    const f = pickExecutionFreshness({
      status: "completed",
      liveStream: false,
      finishedAt: new Date("2026-09-18T08:00:00Z"),
      transformTimes: ["2026-09-18T07:59:00Z", "not a date", null],
      liveness: { stale_seconds: 5 },
      now: NOW,
    })
    expect(f).toEqual({ at: Date.parse("2026-09-18T08:00:00Z"), label: "Landed" })
  })

  it("never says Landed for a failed run", () => {
    const f = pickExecutionFreshness({
      status: "failed",
      liveStream: false,
      finishedAt: new Date("2026-09-18T08:00:00Z"),
      transformTimes: [],
      now: NOW,
    })
    expect(f.label).toBe("Last activity")
  })

  it("reports nothing when nothing is known", () => {
    const f = pickExecutionFreshness({ status: "running", liveStream: true, finishedAt: null, transformTimes: [], liveness: {}, now: NOW })
    expect(f.at).toBeNull()
  })
})
