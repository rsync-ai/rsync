// The Data Freshness card on the execution page: which instant its age is measured
// from, and what the line under the age calls it.
//
// #56 prod retest: a live CDC stream has no end time (#35 stopped showing one), so a
// stream with no transform logs read "—" here while its pipeline page said "last
// event 18h ago". A live stream's freshness is its last change event — the same
// runtime liveness the pipeline header reads — so both pages now show one number.

export type FreshnessLabel = "Landed" | "Last activity" | "Last event"

export interface ExecutionFreshness {
  /** Epoch ms the age is measured from; null when nothing is known ("—"). */
  at: number | null
  label: FreshnessLabel
}

export interface RuntimeLivenessLike {
  last_event_at?: string
  stale_seconds?: number
}

function validTime(ts: string | null | undefined): number | null {
  if (!ts) return null
  const ms = new Date(ts).getTime()
  return Number.isNaN(ms) ? null : ms
}

export function pickExecutionFreshness(input: {
  status: string
  liveStream: boolean
  finishedAt: Date | null
  transformTimes: Array<string | null | undefined>
  liveness?: RuntimeLivenessLike | null
  now: number
}): ExecutionFreshness {
  const { status, liveStream, finishedAt, transformTimes, liveness, now } = input

  if (liveStream && liveness) {
    // stale_seconds first, as PipelineHealthHeader does: it is measured by the
    // gateway at request time, so it cannot drift with the browser's clock.
    if (typeof liveness.stale_seconds === "number" && Number.isFinite(liveness.stale_seconds)) {
      return { at: now - Math.max(0, liveness.stale_seconds) * 1000, label: "Last event" }
    }
    const lastEvent = validTime(liveness.last_event_at)
    if (lastEvent !== null) return { at: lastEvent, label: "Last event" }
  }

  const times: number[] = []
  if (finishedAt && !Number.isNaN(finishedAt.getTime())) times.push(finishedAt.getTime())
  for (const ts of transformTimes) {
    const ms = validTime(ts)
    if (ms !== null) times.push(ms)
  }
  // Only a clean success may claim "Landed"; a failed / silent-drop / still-running
  // execution shows "Last activity", so the card never contradicts a red banner.
  const landed = status === "completed" || status === "success"
  return { at: times.length ? Math.max(...times) : null, label: landed ? "Landed" : "Last activity" }
}
