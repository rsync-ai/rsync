import { describe, expect, it } from "vitest"
import { EventNormalizer, type PipelineRunEvent } from "@/lib/pipeline/eventNormalizer"
import { buildAgenticStagesFromEvents } from "@/components/pipeline/PipelineLiveStatePanel"
import { formatDuration, stageTiming } from "@/lib/duration"

/**
 * The Overview's stage list and the Activity feed's lanes report the same stage
 * of the same run, and used to disagree because each derived its own duration:
 *
 *   - the Overview took the FIRST start and the LAST end, so a retried stage was
 *     billed for the idle gap between attempts;
 *   - the Activity feed walked back to the LAST start, so it reported the final
 *     attempt and silently dropped the earlier ones.
 *
 * On the live demo pipeline that is "15m 26s" against "15.0s" for one stage.
 * Both now reduce through `stageTiming`, and this fixture is the two-attempt
 * case that separated them — it fails if either panel grows its own rule again.
 */

const STAGE = "infra_preflight"

// 17s of work, a quarter of an hour idle, then 15s more: 32s active, 15m 32s elapsed.
const T = {
  firstStart: "2026-09-20T15:58:29.000Z",
  firstEnd: "2026-09-20T15:58:46.000Z",
  secondStart: "2026-09-20T16:13:46.000Z",
  secondEnd: "2026-09-20T16:14:01.000Z",
}
const ACTIVE_MS = 17_000 + 15_000
const ELAPSED_MS = 15 * 60_000 + 32_000

function event(eventType: string, at: string, seq: number): PipelineRunEvent {
  return {
    pipeline_id: "p1",
    execution_id: "e1",
    event_id: `${eventType}-${seq}`,
    seq,
    event_type: eventType,
    stage_id: STAGE,
    stage_group: STAGE,
    occurred_at: at,
    received_at: at,
    payload: {},
  }
}

const RETRIED: PipelineRunEvent[] = [
  event("STAGE_STARTED", T.firstStart, 1),
  event("STAGE_FAILED", T.firstEnd, 2),
  event("STAGE_STARTED", T.secondStart, 3),
  event("STAGE_COMPLETED", T.secondEnd, 4),
]

const RAN_ONCE: PipelineRunEvent[] = [
  event("STAGE_STARTED", T.firstStart, 1),
  event("STAGE_COMPLETED", T.firstEnd, 2),
]

/**
 * The shape the live feed actually has, which the fixtures above did not: two
 * producers describe ONE transition, so every lifecycle event arrives twice.
 * The workflow envelope emits `evt-<pipeline>-<seq>` with the human message and
 * a whole-second `occurred_at`; the gateway projector emits a content-hash id
 * with no message, moments later (domain_event_envelope.go,
 * KI-EVENTS-DUAL-ID-NAMESPACE-DUPES).
 *
 * Undeduped, the second START reads as a second attempt and closes the first
 * one where it lands — 9.4s over "2 attempts" for a stage that ran once for
 * 11.9s. That is what the Overview showed on the live run while the Activity
 * feed, which collapses the copies, showed 11.9s. Both panels now dedupe, and
 * this fixture fails if either stops.
 */
const DUP = {
  startEnvelope: "2026-09-20T15:58:29.000Z",
  startProjector: "2026-09-20T15:58:31.500Z",
  endEnvelope: "2026-09-20T15:58:38.400Z",
  endProjector: "2026-09-20T15:58:40.900Z",
}
const DUP_ACTIVE_MS = 11_900
// What the raw stream reduces to when the copies are counted as attempts.
const DUP_RAW_ACTIVE_MS = 9_400

function copy(e: PipelineRunEvent, over: Partial<PipelineRunEvent>): PipelineRunEvent {
  return { ...e, ...over }
}

const DOUBLE_REPORTED: PipelineRunEvent[] = [
  copy(event("STAGE_STARTED", DUP.startEnvelope, 1), {
    event_id: "evt-8413ee82-1",
    payload: { message: "Planning the pipeline" },
  }),
  copy(event("STAGE_STARTED", DUP.startProjector, 2), { event_id: "sha256:aa01" }),
  copy(event("STAGE_COMPLETED", DUP.endEnvelope, 3), {
    event_id: "evt-8413ee82-3",
    payload: { message: "Plan ready" },
  }),
  copy(event("STAGE_COMPLETED", DUP.endProjector, 4), { event_id: "sha256:aa02" }),
]

function lane(events: PipelineRunEvent[]) {
  const group = EventNormalizer.groupByStage(events, events).find((g) => g.id === STAGE)
  expect(group, "the fixture must produce a lane for the stage").toBeTruthy()
  return group!
}

function overview(events: PipelineRunEvent[]) {
  const stage = buildAgenticStagesFromEvents(events, {
    execution_id: "e1",
    created_at: T.firstStart,
  } as never).find((s) => s.stage === STAGE)
  expect(stage, "the fixture must produce an Overview row for the stage").toBeTruthy()
  return stage!
}

describe("stage durations agree between the Overview and the Activity feed", () => {
  it("reports working time, not the gap between attempts", () => {
    expect(lane(RETRIED).duration).toBe(ACTIVE_MS)
    expect(overview(RETRIED).durationMs).toBe(ACTIVE_MS)
  })

  it("renders the same words in both panels", () => {
    expect(formatDuration(lane(RETRIED).duration)).toBe(formatDuration(overview(RETRIED).durationMs))
    expect(formatDuration(lane(RAN_ONCE).duration)).toBe(formatDuration(overview(RAN_ONCE).durationMs))
  })

  it("counts the attempts, so the omitted gap is visible rather than missing", () => {
    expect(lane(RETRIED).attempts).toBe(2)
    expect(overview(RETRIED).currentAttempt).toBe(2)
    expect(lane(RAN_ONCE).attempts).toBe(1)
  })

  it("still agrees for a stage that ran exactly once", () => {
    expect(lane(RAN_ONCE).duration).toBe(17_000)
    expect(overview(RAN_ONCE).durationMs).toBe(17_000)
  })

  it("counts one transition once, however many producers reported it", () => {
    expect(lane(DOUBLE_REPORTED).duration).toBe(DUP_ACTIVE_MS)
    expect(overview(DOUBLE_REPORTED).durationMs).toBe(DUP_ACTIVE_MS)
    expect(lane(DOUBLE_REPORTED).attempts).toBe(1)
    // The retry badge is the same miscount wearing different words.
    expect(overview(DOUBLE_REPORTED).currentAttempt).toBe(1)
    expect(overview(DOUBLE_REPORTED).maxAttempts).toBe(1)
  })

  it("the duplicate fixture really is one a raw reduction gets wrong — the probe is armed", () => {
    const raw = stageTiming(
      DOUBLE_REPORTED.map((e) => ({ type: e.event_type, at: new Date(e.occurred_at!).getTime() })),
    )
    expect(raw.attempts).toBe(2)
    expect(raw.activeMs).toBe(DUP_RAW_ACTIVE_MS)
    expect(raw.activeMs).not.toBe(DUP_ACTIVE_MS)
  })

  it("keeps elapsed available and distinct from active — the probe is armed", () => {
    // If active and elapsed were the same number, the assertions above would pass
    // even with the old first-start-to-last-end rule restored.
    const timing = stageTiming(
      RETRIED.map((e) => ({ type: e.event_type, at: new Date(e.occurred_at!).getTime() })),
    )
    expect(timing.activeMs).toBe(ACTIVE_MS)
    expect(timing.elapsedMs).toBe(ELAPSED_MS)
    expect(timing.elapsedMs).not.toBe(timing.activeMs)
  })
})
