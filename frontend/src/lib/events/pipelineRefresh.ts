export const PIPELINE_REFRESH_EVENT = "rsync:pipeline_refresh"

export function emitPipelineRefresh(pipelineId: string) {
  if (typeof window === "undefined") return
  window.dispatchEvent(
    new CustomEvent(PIPELINE_REFRESH_EVENT, {
      detail: { pipelineId: String(pipelineId || "").trim() },
    }),
  )
}

export function onPipelineRefresh(handler: (pipelineId: string) => void) {
  if (typeof window === "undefined") return () => {}

  const wrapped = (evt: Event) => {
    const ce = evt as CustomEvent
    const pid = String(ce.detail?.pipelineId || "").trim()
    if (!pid) return
    handler(pid)
  }

  window.addEventListener(PIPELINE_REFRESH_EVENT, wrapped as EventListener)
  return () => window.removeEventListener(PIPELINE_REFRESH_EVENT, wrapped as EventListener)
}

