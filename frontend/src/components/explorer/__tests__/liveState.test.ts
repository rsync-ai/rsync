import { describe, expect, it } from "vitest"

import {
  describeFreshnessCause,
  describeRefreshFacts,
  describeUpstream,
  formatSpan,
  liveCellFor,
  openBreachFor,
  overdueSeconds,
  parseOpenBreaches,
  parseRunningModels,
  type Fetched,
  type ModelFreshnessBreach,
  type ModelRefreshState,
  type RunningModelWork,
  type RunningModelsResponse,
} from "@/components/explorer/liveState"

// The Now column's rules. The one that matters most: nothing produces "idle" unless the
// server asked a refresh loop and was told there was none. Each way of NOT knowing is
// pinned here to a cell that is not idle.

const SQ = "sq-1"

function detail(over: Partial<ModelRefreshState> = {}): ModelRefreshState {
  return {
    saved_query_id: SQ,
    phase: "rebuilding",
    queued_completions: 2,
    current: {
      schedule_id: "sch-1",
      upstream_kind: "pipeline",
      upstream_id: "p-1",
      depth: 0,
      coalesced: 1,
      started_at: "2026-09-15T14:02:00Z",
    },
    refreshes_this_run: 4,
    refreshes_per_run: 20,
    ...over,
  }
}

function work(over: Partial<RunningModelWork> = {}): RunningModelWork {
  return {
    saved_query_id: SQ,
    name: "orders_daily",
    workflow_id: `model-refresh-${SQ}`,
    state: "idle",
    detail_available: false,
    ...over,
  }
}

function ok(models: RunningModelWork[], over: Partial<RunningModelsResponse> = {}): Fetched<RunningModelsResponse> {
  return {
    status: "ok",
    data: { models, count: models.length, limit: 50, temporal_available: true, ...over },
  }
}

function breach(over: Partial<ModelFreshnessBreach> = {}): ModelFreshnessBreach {
  return {
    breach_id: "b-1",
    saved_query_id: SQ,
    name: "orders_daily",
    target_table: "analytics.orders_daily",
    deadline_seconds: 3600,
    cause: "overdue",
    reference_at: "2026-09-15T10:00:00Z",
    never_succeeded: false,
    stale_seconds: 600,
    detected_at: "2026-09-15T11:10:00Z",
    ...over,
  }
}

describe("liveCellFor", () => {
  it("never asks about a clock schedule, even if the answer names it", () => {
    // A clock row reading "Idle" would be an answer to a question nobody asked.
    for (const type of ["cron", "interval", ""]) {
      expect(liveCellFor(type, SQ, ok([work({ state: "idle" })]))).toEqual({ kind: "not_applicable" })
    }
  })

  it("reads loading while the first check is out", () => {
    expect(liveCellFor("after_upstream", SQ, { status: "loading" })).toEqual({ kind: "loading" })
  })

  it("says why it is checking again when an old answer was set aside", () => {
    const note = "The last answer is 10m old, so it is not shown while the page asks again."
    expect(liveCellFor("after_upstream", SQ, { status: "loading", note })).toEqual({ kind: "loading", reason: note })
  })

  it("reads unavailable, not idle, when the check failed", () => {
    expect(liveCellFor("after_upstream", SQ, { status: "error", message: "HTTP 503" })).toEqual({
      kind: "unavailable",
      reason: "HTTP 503",
    })
  })

  it("reads unavailable when orchestration is down, whatever the row itself says", () => {
    // The ordering is the point: the row says idle, and it must not be believed.
    const cell = liveCellFor("after_upstream", SQ, ok([work({ state: "idle" })], { temporal_available: false }))
    expect(cell.kind).toBe("unavailable")
  })

  it("reads not checked for a model the answer left out, naming the cap when it was hit", () => {
    const capped = liveCellFor("after_upstream", SQ, ok([work({ saved_query_id: "other" })], { count: 50, limit: 50 }))
    expect(capped).toMatchObject({ kind: "not_checked" })
    expect((capped as { reason: string }).reason).toMatch(/first 50/)

    const uncapped = liveCellFor("after_upstream", SQ, ok([work({ saved_query_id: "other" })]))
    expect(uncapped).toMatchObject({ kind: "not_checked" })
    expect((uncapped as { reason: string }).reason).not.toMatch(/first/)
  })

  it("reads idle only when the server said idle", () => {
    const cell = liveCellFor("after_upstream", SQ, ok([work({ state: "idle", message: "no refresh loop has run for this model" })]))
    expect(cell).toEqual({ kind: "idle", reason: "no refresh loop has run for this model" })
  })

  it("splits a running loop by phase", () => {
    const d = detail()
    expect(liveCellFor("after_upstream", SQ, ok([work({ state: "running", detail_available: true, detail: d })]))).toEqual({
      kind: "rebuilding",
      detail: d,
    })
    const w = detail({ phase: "waiting", current: null })
    expect(liveCellFor("after_upstream", SQ, ok([work({ state: "running", detail_available: true, detail: w })]))).toEqual({
      kind: "waiting",
      detail: w,
    })
  })

  it("keeps a running loop that gave no detail apart from both idle and rebuilding", () => {
    const noDetail = liveCellFor(
      "after_upstream",
      SQ,
      ok([work({ state: "running", detail_available: false, message: "the refresh loop answered with no state" })]),
    )
    expect(noDetail).toEqual({ kind: "running_no_detail", reason: "the refresh loop answered with no state" })

    // detail_available true but no body is still no detail.
    expect(liveCellFor("after_upstream", SQ, ok([work({ state: "running", detail_available: true })])).kind).toBe(
      "running_no_detail",
    )

    // A flag of false beside a body: the flag wins, because it is what the server vouches for.
    expect(
      liveCellFor("after_upstream", SQ, ok([work({ state: "running", detail_available: false, detail: detail() })])).kind,
    ).toBe("running_no_detail")

    const oddPhase = liveCellFor(
      "after_upstream",
      SQ,
      ok([work({ state: "running", detail_available: true, detail: detail({ phase: "draining" }) })]),
    )
    expect(oddPhase.kind).toBe("running_no_detail")
    expect((oddPhase as { reason: string }).reason).toMatch(/draining/)
  })

  it("reports unreachable and unknown as themselves", () => {
    // The server's message is a bare query error; the reason says what was being asked.
    expect(liveCellFor("after_upstream", SQ, ok([work({ state: "unreachable", message: "deadline exceeded" })]))).toEqual({
      kind: "unreachable",
      reason: "The refresh loop could not be asked what it is doing (deadline exceeded).",
    })
    expect(liveCellFor("after_upstream", SQ, ok([work({ state: "unreachable" })]))).toEqual({
      kind: "unreachable",
      reason: "The refresh loop could not be asked what it is doing.",
    })
    expect(liveCellFor("after_upstream", SQ, ok([work({ state: "unknown" })])).kind).toBe("unknown")
  })

  it("reads a state it has never heard of as unknown, not idle", () => {
    const cell = liveCellFor("after_upstream", SQ, ok([work({ state: "paused" })]))
    expect(cell.kind).toBe("unknown")
    expect((cell as { reason: string }).reason).toMatch(/paused/)
  })
})

describe("parseRunningModels", () => {
  it("rejects a body whose temporal_available is missing or not a boolean", () => {
    // Defaulting it would let a truncated body produce idle rows.
    expect(parseRunningModels({ models: [], count: 0, limit: 50 })).toBeNull()
    expect(parseRunningModels({ models: [], count: 0, limit: 50, temporal_available: "false" })).toBeNull()
  })

  it("rejects a body without a models array", () => {
    expect(parseRunningModels({ temporal_available: true })).toBeNull()
    expect(parseRunningModels({ temporal_available: true, models: null })).toBeNull()
    expect(parseRunningModels(null)).toBeNull()
    expect(parseRunningModels("nope")).toBeNull()
  })

  it("keeps a well-formed body, including temporal_available false", () => {
    const body = { models: [work()], count: 1, limit: 50, temporal_available: false }
    expect(parseRunningModels(body)).toEqual(body)
  })

  it("never treats a body with no limit as having hit it", () => {
    const parsed = parseRunningModels({ models: [], temporal_available: true })
    expect(parsed).not.toBeNull()
    expect(parsed!.count >= parsed!.limit).toBe(false)
  })
})

describe("parseOpenBreaches / openBreachFor", () => {
  it("drops resolved breaches from the answer", () => {
    const open = breach({ breach_id: "open" })
    const resolved = breach({ breach_id: "closed", saved_query_id: "sq-2", resolved_at: "2026-09-15T12:00:00Z" })
    expect(parseOpenBreaches({ breaches: [open, resolved], count: 2 })).toEqual([open])
  })

  it("rejects a body without a breaches array", () => {
    expect(parseOpenBreaches({ count: 0 })).toBeNull()
    expect(parseOpenBreaches(undefined)).toBeNull()
  })

  it("finds a model's open breach, and ignores a resolved one handed to it directly", () => {
    const resolved = breach({ resolved_at: "2026-09-15T12:00:00Z" })
    expect(openBreachFor(SQ, [resolved])).toBeUndefined()
    expect(openBreachFor(SQ, [breach({ saved_query_id: "other" })])).toBeUndefined()
    const open = breach()
    expect(openBreachFor(SQ, [resolved, open])).toBe(open)
  })
})

describe("overdueSeconds", () => {
  it("prefers the live figure, including a live figure of zero", () => {
    expect(overdueSeconds(breach({ stale_seconds: 600, stale_seconds_now: 7200 }))).toBe(7200)
    expect(overdueSeconds(breach({ stale_seconds: 600, stale_seconds_now: 0 }))).toBe(0)
    expect(overdueSeconds(breach({ stale_seconds: 600 }))).toBe(600)
  })
})

describe("formatSpan", () => {
  it.each([
    [-5, "0s"],
    [0, "0s"],
    [59.9, "59s"],
    [60, "1m"],
    [754, "12m"],
    [3600, "1h"],
    [3660, "1h 1m"],
    [7500, "2h 5m"],
    [86400, "1d"],
    [86400 * 3 + 3600 * 4 + 59, "3d 4h"],
  ])("%s seconds reads %s", (seconds, expected) => {
    expect(formatSpan(seconds)).toBe(expected)
  })
})

describe("describeFreshnessCause", () => {
  it("names every cause the classifier produces, and does not blank an unfamiliar one", () => {
    expect(describeFreshnessCause("overdue")).toMatch(/active but has not rebuilt/)
    expect(describeFreshnessCause("no_schedule")).toMatch(/nothing is scheduled/)
    expect(describeFreshnessCause("schedule_paused")).toBe("its schedule is paused")
    expect(describeFreshnessCause("schedule_auto_paused")).toMatch(/paused automatically/)
    expect(describeFreshnessCause("no_upstreams")).toMatch(/none are set/)
    expect(describeFreshnessCause("cosmic_rays")).toMatch(/cosmic_rays/)
    expect(describeFreshnessCause("")).toMatch(/no cause/)
  })
})

describe("describeUpstream", () => {
  const upstreams = [
    { kind: "pipeline", id: "p-1", name: "orders_sync" },
    { kind: "model", id: "p-1", name: "same id, other kind" },
    { kind: "model", id: "m-2", name: "   " },
  ]

  it("uses the joined name for a matching kind and id", () => {
    expect(describeUpstream("pipeline", "p-1", upstreams)).toBe("pipeline orders_sync")
    expect(describeUpstream("model", "p-1", upstreams)).toBe("model same id, other kind")
  })

  it("falls back to the id when there is no match or no usable name", () => {
    expect(describeUpstream("pipeline", "p-9", upstreams)).toBe("pipeline p-9")
    expect(describeUpstream("model", "m-2", upstreams)).toBe("model m-2")
    expect(describeUpstream("pipeline", "p-1", undefined)).toBe("pipeline p-1")
  })
})

describe("describeRefreshFacts", () => {
  it("shows only the facts that say something", () => {
    expect(describeRefreshFacts(detail({ queued_completions: 1 }))).toEqual([
      "1 completion queued",
      "refresh 4 of 20 this run",
    ])
  })

  it("adds the batch size and chain depth when they are not the trivial values", () => {
    const d = detail({ queued_completions: 0 })
    d.current = { ...d.current!, coalesced: 3, depth: 2 }
    expect(describeRefreshFacts(d)).toEqual([
      "0 completions queued",
      "this rebuild stands in for 3 completions",
      "2 hops down a model chain",
      "refresh 4 of 20 this run",
    ])
    d.current = { ...d.current!, depth: 1 }
    expect(describeRefreshFacts(d)).toContain("1 hop down a model chain")
  })

  it("omits a per-run counter the loop did not report", () => {
    expect(describeRefreshFacts(detail({ refreshes_per_run: 0, current: null }))).toEqual(["2 completions queued"])
  })
})
