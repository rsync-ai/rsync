import { toast } from "sonner"

import { authFetch } from "@/lib/api/auth-fetch"
import type { ApiErrorBody } from "@/lib/api/types"
import { API_ENDPOINTS } from "@/lib/config/api"
import { classifyError } from "@/lib/utils/error-handling"

// Actions shared by the Pipelines list row menu and the detail page's ⋯ menu,
// which carry the same items (#52 prod retest: View Logs and Export Config were
// row-only, Schema change alerts detail-only).

/**
 * "View Logs": the latest execution's run log (`#logs` on its page — the page
 * opens on the run's header cards, and the log sits below them). A pipeline
 * that has never run has no execution, so it opens the Monitor tab (live
 * events + throughput) — not Overview, which shows configuration.
 */
export function pipelineLogsHref(pipelineId: string, lastExecutionId?: string | null): string {
  return lastExecutionId ? `/executions/${lastExecutionId}#logs` : `/pipelines/${pipelineId}?tab=monitor`
}

/** "Export Config": downloads the pipeline's JSON as pipeline-<id>.json. */
export async function exportPipelineConfig(id: string): Promise<void> {
  try {
    const res = await authFetch(API_ENDPOINTS.PIPELINES.GET(id))
    if (!res.ok) {
      const body = await res.json().catch(() => ({}))
      const e = classifyError(
        Object.assign(new Error(), {
          statusCode: res.status,
          message: (body as ApiErrorBody)?.error || (body as ApiErrorBody)?.message || res.statusText,
        }),
        "pipeline.export",
      )
      toast.error(e.title, { description: e.hint ?? e.message })
      return
    }
    const data = await res.json()
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `pipeline-${id}.json`
    a.click()
    URL.revokeObjectURL(url)
  } catch (err) {
    const e = classifyError(err, "pipeline.export")
    toast.error(e.title, { description: e.hint ?? e.message })
  }
}
