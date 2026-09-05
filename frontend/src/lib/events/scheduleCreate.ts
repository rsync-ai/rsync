"use client"

export const PIPELINE_SCHEDULE_CREATE_EVENT = "rsync:pipeline_schedule_create"

export function emitPipelineScheduleCreate(pipelineId: string) {
  if (typeof window === "undefined") return
  window.dispatchEvent(
    new CustomEvent(PIPELINE_SCHEDULE_CREATE_EVENT, {
      detail: { pipelineId: String(pipelineId || "").trim() },
    })
  )
}

export function onPipelineScheduleCreate(handler: (pipelineId: string) => void) {
  if (typeof window === "undefined") return () => {}

  const wrapped = (evt: Event) => {
    const ce = evt as CustomEvent
    const pid = String(ce.detail?.pipelineId || "").trim()
    if (!pid) return
    handler(pid)
  }

  window.addEventListener(PIPELINE_SCHEDULE_CREATE_EVENT, wrapped as EventListener)
  return () => window.removeEventListener(PIPELINE_SCHEDULE_CREATE_EVENT, wrapped as EventListener)
}

// A pipeline's schedules changed (created / paused / resumed / deleted).
//
// Distinct from PIPELINE_SCHEDULE_CREATE_EVENT above, which is a *request to open the
// create dialog*, not a notification that anything changed. Schedules are mutated from
// two places — the header action (PipelineScheduleCreateDialogLauncher) and the panel's
// own controls — and the panel used to refetch only after its own mutations. So creating
// from the header left the panel reading "No schedules configured" until a hard reload,
// while deleting from the panel refreshed immediately: the same state, two answers,
// depending on which button you happened to use. Every mutation emits this now.
export const PIPELINE_SCHEDULES_CHANGED_EVENT = "rsync:pipeline_schedules_changed"

export function emitPipelineSchedulesChanged(pipelineId: string) {
  if (typeof window === "undefined") return
  window.dispatchEvent(
    new CustomEvent(PIPELINE_SCHEDULES_CHANGED_EVENT, {
      detail: { pipelineId: String(pipelineId || "").trim() },
    })
  )
}

export function onPipelineSchedulesChanged(handler: (pipelineId: string) => void) {
  if (typeof window === "undefined") return () => {}

  const wrapped = (evt: Event) => {
    const ce = evt as CustomEvent
    const pid = String(ce.detail?.pipelineId || "").trim()
    if (!pid) return
    handler(pid)
  }

  window.addEventListener(PIPELINE_SCHEDULES_CHANGED_EVENT, wrapped as EventListener)
  return () => window.removeEventListener(PIPELINE_SCHEDULES_CHANGED_EVENT, wrapped as EventListener)
}

