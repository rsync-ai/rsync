import { describe, expect, it } from "vitest"
import {
  EventNormalizer,
  dedupeStageLifecycleEvents,
  type PipelineRunEvent,
} from "../eventNormalizer"

const EXEC = "11111111-1111-1111-1111-111111111111"

function ev(p: Partial<PipelineRunEvent> & { event_type: string; occurred_at: string }): PipelineRunEvent {
  return {
    pipeline_id: "p1",
    execution_id: EXEC,
    event_id: `${p.event_type}-${p.stage_id}-${p.trace_id}-${p.occurred_at}`,
    received_at: p.occurred_at,
    payload: {},
    ...p,
  }
}

// Shape of a live MongoDB -> GCS batch run: the workflow and the worker both emit
// each lifecycle transition (different stage ids, different trace ids), and the
// API returns rows newest-first.
const workflowTrace = EXEC
const workerTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
const rows: PipelineRunEvent[] = [
  ev({ event_type: "STAGE_COMPLETED", stage_id: "executor", stage_group: "executing", trace_id: workerTrace, occurred_at: "2026-09-16T10:05:00.500Z", payload: { summary: "Executing pipeline" } }),
  ev({ event_type: "STAGE_COMPLETED", stage_id: "executor", stage_group: "executing", trace_id: workflowTrace, occurred_at: "2026-09-16T10:05:01Z" }),
  ev({ event_type: "PIPELINE_WAITING", stage_id: "executor", trace_id: workflowTrace, occurred_at: "2026-09-16T10:01:00Z", payload: { message: "Select table(s) to sync (3 available)" } }),
  ev({ event_type: "STAGE_STARTED", stage_id: "executor", stage_group: "executing", trace_id: workflowTrace, occurred_at: "2026-09-16T10:00:30Z" }),
  ev({ event_type: "STAGE_STARTED", stage_id: "executor", stage_group: "executing", trace_id: workerTrace, occurred_at: "2026-09-16T10:00:30.200Z", payload: { summary: "Executing pipeline" } }),
  ev({ event_type: "STAGE_COMPLETED", stage_id: "connection_validator", stage_group: "connecting", trace_id: workerTrace, occurred_at: "2026-09-16T10:00:20Z", payload: { summary: "Connections validated" } }),
  ev({ event_type: "STAGE_COMPLETED", stage_id: "connection_validation", stage_group: "connecting", trace_id: workflowTrace, occurred_at: "2026-09-16T10:00:20Z" }),
  ev({ event_type: "STAGE_STARTED", stage_id: "connection_validation", stage_group: "connecting", trace_id: workflowTrace, occurred_at: "2026-09-16T10:00:10Z" }),
  ev({ event_type: "STAGE_STARTED", stage_id: "connection_validator", stage_group: "connecting", trace_id: workerTrace, occurred_at: "2026-09-16T10:00:10.100Z", payload: { summary: "Validating connections" } }),
]

describe("Trace tab stage events", () => {
  it("collapses the workflow and worker copies of each transition", () => {
    const out = dedupeStageLifecycleEvents(rows)
    const lifecycle = out.filter((e) => e.event_type.startsWith("STAGE_"))
    expect(lifecycle.map((e) => `${e.event_type}:${e.stage_group}`)).toEqual([
      "STAGE_STARTED:connecting",
      "STAGE_COMPLETED:connecting",
      "STAGE_STARTED:executing",
      "STAGE_COMPLETED:executing",
    ])
    // The copy with a human summary wins.
    expect(lifecycle.every((e) => Boolean(e.payload.summary))).toBe(true)
  })

  it("returns events in chronological order even though the API is newest-first", () => {
    const times = dedupeStageLifecycleEvents(rows).map((e) => new Date(e.occurred_at!).getTime())
    expect([...times].sort((a, b) => a - b)).toEqual(times)
  })

  it("keeps a genuine retry (STARTED, FAILED, STARTED)", () => {
    const retry = [
      ev({ event_type: "STAGE_STARTED", stage_id: "planner", trace_id: "a", occurred_at: "2026-09-16T10:00:00Z" }),
      ev({ event_type: "STAGE_FAILED", stage_id: "planner", trace_id: "a", occurred_at: "2026-09-16T10:00:05Z" }),
      ev({ event_type: "STAGE_STARTED", stage_id: "planner", trace_id: "a", occurred_at: "2026-09-16T10:00:10Z" }),
    ]
    expect(dedupeStageLifecycleEvents(retry)).toHaveLength(3)
  })

  it("files the table-selection wait under its stage and does not leave it Pending", () => {
    const groups = EventNormalizer.groupByStage(rows)
    expect(groups.map((g) => g.id)).toEqual(["connecting", "executing"])
    const executing = groups.find((g) => g.id === "executing")!
    expect(executing.status).toBe("completed")
    expect(executing.events.some((e) => e.title.startsWith("Select table(s)"))).toBe(true)
  })

  it("an unanswered wait with nothing after it is still open", () => {
    const groups = EventNormalizer.groupByStage([
      ev({ event_type: "PIPELINE_WAITING", stage_id: "executor", trace_id: "a", occurred_at: "2026-09-16T10:01:00Z" }),
    ])
    expect(groups[0].status).toBe("running")
  })
})

// The Activity badge used to be read off whatever rows were loaded, so the same
// stage read "Pending" on the newest page and "Completed" after Load More. It now
// comes from the stage's latest transition, fetched apart from the paged feed.
describe("a stage's badge and duration do not depend on the pages loaded", () => {
  const at = (event_type: string, occurred_at: string) =>
    ev({ event_type, stage_id: "executor", stage_group: "executing", trace_id: "a", occurred_at })
  const started = at("STAGE_STARTED", "2026-09-15T13:00:00Z")
  const completed = at("STAGE_COMPLETED", "2026-09-16T10:02:06Z")
  // A worker row filed under the stage after it completed: the newest page holds
  // this and nothing else of the stage.
  const newest = [at("STAGE_PROGRESS", "2026-09-16T10:03:00Z")]
  const older = [completed, started]
  const executing = (events: PipelineRunEvent[], status?: PipelineRunEvent[]) =>
    EventNormalizer.groupByStage(events, status).find((g) => g.id === "executing")!

  it("reads the same on the newest page alone and after an older page loads", () => {
    const first = executing(newest, [completed, started])
    const both = executing([...newest, ...older], [completed, started])
    expect(first.status).toBe("completed")
    expect(both.status).toBe("completed")
    // 21h 2m 6s, measured start to finish, not across the rows shown.
    expect(first.duration).toBe(75_726_000)
    expect(both.duration).toBe(first.duration)
  })

  it("control: without the transitions read the newest page cannot say, and does not guess", () => {
    expect(executing(newest).status).toBe("unknown")
    expect(executing([...newest, ...older]).status).toBe("completed")
  })

  // Both durations below used to count the LAST attempt only — the feed walked
  // back to the newest STAGE_STARTED — while the Overview counted the first
  // start to the last end, gaps included. A retried stage therefore read two
  // different numbers on two panels of the same page. Both now sum the
  // attempts (`stageTiming` in @/lib/duration), which is the only one of the
  // three that is a fact about the work rather than about the clock.
  it("a retried stage is running, not failed, and counts both attempts", () => {
    const retry = [
      at("STAGE_STARTED", "2026-09-16T10:00:00Z"),
      at("STAGE_FAILED", "2026-09-16T10:05:00Z"),
      at("STAGE_STARTED", "2026-09-16T10:10:00Z"),
    ]
    const g = executing([at("STAGE_PROGRESS", "2026-09-16T10:12:00Z"), retry[1]], retry)
    expect(g.status).toBe("running")
    // Five minutes before it failed, two minutes into the retry — and the five
    // idle minutes between them belong to neither.
    expect(g.duration).toBe(7 * 60_000)
    expect(g.attempts).toBe(2)
  })

  it("a stage whose latest transition failed is failed, even after an earlier success", () => {
    const g = executing(newest, [
      started,
      completed,
      at("STAGE_STARTED", "2026-09-16T10:04:00Z"),
      at("STAGE_FAILED", "2026-09-16T10:04:30Z"),
    ])
    expect(g.status).toBe("failed")
    // The 21h 2m 6s first attempt plus the 30s retry.
    expect(g.duration).toBe(75_726_000 + 30_000)
    expect(g.attempts).toBe(2)
  })

  it("transitions for a stage with no loaded row add no group", () => {
    const groups = EventNormalizer.groupByStage(newest, [
      completed,
      started,
      ev({ event_type: "STAGE_COMPLETED", stage_id: "connection_validation", stage_group: "connecting", trace_id: "a", occurred_at: "2026-09-15T12:59:00Z" }),
    ])
    expect(groups.map((g) => g.id)).toEqual(["executing"])
  })
})

// Live CDC run 402b1e5e (2026-09-19): each producer reported the executor's
// start and end, seconds apart. The Overview timed the stage start → last end
// (36.5s); Activity kept the earlier end and said 33.3s.
describe("a stage reported twice is timed from its first start to its last end", () => {
  const twice = [
    ev({ event_type: "STAGE_STARTED", stage_id: "executor", stage_group: "executing", trace_id: "orch", occurred_at: "2026-09-19T12:26:25.749Z", payload: { summary: "Executing pipeline" } }),
    ev({ event_type: "STAGE_STARTED", stage_id: "executor", stage_group: "executing", trace_id: "wf", occurred_at: "2026-09-19T12:26:45Z" }),
    ev({ event_type: "STAGE_COMPLETED", stage_id: "executor", stage_group: "executing", trace_id: "wf", occurred_at: "2026-09-19T12:26:59Z", payload: { summary: "Pipeline executed" } }),
    ev({ event_type: "STAGE_COMPLETED", stage_id: "executor", stage_group: "executing", trace_id: "orch", occurred_at: "2026-09-19T12:27:02.262Z" }),
    ev({ event_type: "PIPELINE_WAITING", stage_id: "executor", trace_id: "wf", occurred_at: "2026-09-19T12:27:00Z" }),
  ]

  it("keeps the earliest start and the latest end", () => {
    const lifecycle = dedupeStageLifecycleEvents(twice).filter((e) => e.event_type.startsWith("STAGE_"))
    expect(lifecycle.map((e) => `${e.event_type}@${e.occurred_at}`)).toEqual([
      "STAGE_STARTED@2026-09-19T12:26:25.749Z",
      "STAGE_COMPLETED@2026-09-19T12:27:02.262Z",
    ])
    // The copy with the summary still wins; only its time moves.
    expect(lifecycle[1].payload.summary).toBe("Pipeline executed")
  })

  it("stays chronological after an end moves later", () => {
    const times = dedupeStageLifecycleEvents(twice).map((e) => new Date(e.occurred_at!).getTime())
    expect([...times].sort((a, b) => a - b)).toEqual(times)
  })

  it("the Activity duration matches the Overview's 36.5s", () => {
    const executing = EventNormalizer.groupByStage(twice).find((g) => g.id === "executing")!
    expect(executing.duration).toBe(36_513)
  })
})

describe("infra preflight is its own group", () => {
  it("even on rows stored with the old default group \"planning\"", () => {
    const groups = EventNormalizer.groupByStage([
      ev({ event_type: "STAGE_STARTED", stage_id: "planner", stage_group: "planning", trace_id: "a", occurred_at: "2026-09-19T12:26:10Z" }),
      ev({ event_type: "STAGE_COMPLETED", stage_id: "planner", stage_group: "planning", trace_id: "a", occurred_at: "2026-09-19T12:26:12Z" }),
      ev({ event_type: "STAGE_STARTED", stage_id: "infra_preflight", stage_group: "planning", trace_id: "a", occurred_at: "2026-09-19T12:26:15Z" }),
      ev({ event_type: "STAGE_COMPLETED", stage_id: "infra_preflight", stage_group: "planning", trace_id: "a", occurred_at: "2026-09-19T12:26:20Z" }),
    ])
    expect(groups.map((g) => g.name)).toEqual(["Planning", "Infra Preflight"])
  })
})

// Live run 67b8ac8b (prod, 2026-09-20): Activity listed Executing, then Other
// events, then Infra Preflight -- though the preflight ran first, and the
// Overview showed it first. A stage's badge and duration already come from its
// transitions (fetched apart from the paged feed); its *start*, which orders the
// lanes, was still read off the loaded rows, and the newest page of a streaming
// run holds rows from long after each stage began.
describe("lane order does not depend on which pages are loaded", () => {
  const at = (event_type: string, stage_id: string, stage_group: string, occurred_at: string) =>
    ev({ event_type, stage_id, stage_group, trace_id: "a", occurred_at })
  const transitions = [
    at("STAGE_STARTED", "infra_preflight", "planning", "2026-09-19T12:26:08Z"),
    at("STAGE_COMPLETED", "infra_preflight", "planning", "2026-09-19T12:26:25Z"),
    at("STAGE_STARTED", "executor", "executing", "2026-09-19T12:26:25.749Z"),
  ]
  // The newest page of a live CDC stream: the executor's ticks, plus a preflight
  // row the sentinel emitted hours after the preflight itself finished.
  const newest = [
    at("STAGE_PROGRESS", "executor", "executing", "2026-09-19T14:00:00Z"),
    at("STAGE_PROGRESS", "infra_preflight", "planning", "2026-09-19T14:05:00Z"),
  ]
  const lanes = (events: PipelineRunEvent[]) =>
    EventNormalizer.groupByStage(events, transitions).map((g) => g.id)

  it("puts Infra Preflight before Executing on the newest page alone", () => {
    expect(lanes(newest)).toEqual(["infra_preflight", "executing"])
  })

  it("reads the same after an older page loads", () => {
    expect(lanes([...newest, ...transitions])).toEqual(["infra_preflight", "executing"])
  })

  it("a lane starts when the stage started, not at its first loaded row", () => {
    const infra = EventNormalizer.groupByStage(newest, transitions).find((g) => g.id === "infra_preflight")!
    expect(infra.startedAt).toBe("2026-09-19T12:26:08Z")
  })

  it("control: with no transitions the loaded rows are all there is to order by", () => {
    expect(EventNormalizer.groupByStage(newest).map((g) => g.id)).toEqual(["executing", "infra_preflight"])
  })
})

describe("the stage-less bucket has a name a user can read", () => {
  it('calls it "Other events", not "Ungrouped"', () => {
    // PIPELINE_CREATED names no stage, so it lands in the "ungrouped" bucket;
    // prod 2026-09-19 showed that key to the user verbatim.
    const groups = EventNormalizer.groupByStage([
      ev({ event_type: "PIPELINE_CREATED", stage_id: "", trace_id: "a", occurred_at: "2026-09-19T12:26:05Z" }),
    ])
    expect(groups.map((g) => g.name)).toEqual(["Other events"])
  })
})
