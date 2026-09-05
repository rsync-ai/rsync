import { PageHeader } from "@/components/layout/PageHeader"
import { ExecutionsListClient } from "@/components/executions/ExecutionsListClient"
import { API_ENDPOINTS } from "@/lib/config/api"
import { cookies } from "next/headers"
import { activeWorkspaceCookieHeader } from "@/lib/workspace/server-workspace"
import { normalizeExecutionStatus } from "@/lib/execution-status"

export const dynamic = "force-dynamic"

export default async function ExecutionsPage() {
  // Server-side fetch must use the internal Docker URL (localhost inside the container is NOT the API gateway).
  let data: any = { executions: [], total: 0, stats: { running: 0, success: 0, failed: 0 } }
  try {
    const cookieStore = await cookies()
    const authToken = cookieStore.get("auth_token")?.value
    // Forward the active-workspace selection (cookie mirror) so SSR scopes to the
    // chosen workspace instead of defaulting to the caller's personal one.
    const authHeaders: Record<string, string> = {
      ...(authToken ? { Authorization: authToken } : {}),
      ...activeWorkspaceCookieHeader(cookieStore),
    }
    const res = await fetch(API_ENDPOINTS.EXECUTIONS.LIST_INTERNAL, {
      cache: "no-store",
      signal: AbortSignal.timeout(5000),
      headers: authHeaders,
    })
    if (res.ok) {
      data = await res.json()
    } else {
      console.error("Failed to fetch executions (server):", res.status)
    }
  } catch (e) {
    console.error("Error fetching executions (server):", e)
  }

  const rawExecutions = Array.isArray(data?.executions) ? data.executions : []

  // Transform to match client component interface (camelCase)
  const executions = rawExecutions.map((e: any) => ({
    id: e.id,
    pipelineId: e.pipeline_id ?? "",
    status: normalizeExecutionStatus(e.status, e.error_message),
    triggerSource: (e.trigger_source ?? "manual") as string,
    scheduleId: e.schedule_id ?? null,
    scheduledTime: e.scheduled_time ? new Date(e.scheduled_time) : null,
    startedAt: e.start_time ? new Date(e.start_time) : new Date(),
    finishedAt: e.end_time ? new Date(e.end_time) : null,
    error: e.error_message ?? null,
    nodeResults: e.node_results ?? null,
    inputData: e.input_data ?? null,
    outputData: e.output_data ?? null,
    userId: e.user_id ?? "",
    pipeline: {
      id: e.pipeline_id ?? "",
      name: e.pipeline_name ?? "Unknown Pipeline",
      status: "active",
    },
  }))

  // Get unique pipelines for filter dropdown
  const pipelineMap = new Map<string, { id: string; name: string }>()
  executions.forEach((ex: any) => {
    if (ex.pipeline?.id && !pipelineMap.has(ex.pipeline.id)) {
      pipelineMap.set(ex.pipeline.id, { id: ex.pipeline.id, name: ex.pipeline.name })
    }
  })
  const pipelines = Array.from(pipelineMap.values())

  const stats = {
    total: typeof data?.total === "number" ? data.total : executions.length,
    running: data?.stats?.running ?? executions.filter((e: any) => e.status === "running" || e.status === "pending").length,
    success: data?.stats?.success ?? executions.filter((e: any) => e.status === "success").length,
    failed: data?.stats?.failed ?? executions.filter((e: any) => e.status === "failed").length,
  }

  return (
    <div className="space-y-6">
      <PageHeader
        heading="Executions"
        description="Monitor and manage your pipeline executions"
      />

      <ExecutionsListClient 
        initialExecutions={executions} 
        pipelines={pipelines}
        stats={stats}
      />
    </div>
  )
}
