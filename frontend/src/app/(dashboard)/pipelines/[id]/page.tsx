import { notFound } from "next/navigation"
import { Suspense } from "react"
import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent } from "@/components/ui/card"
import { ArrowLeft, MessageSquare } from "lucide-react"
import { formatDateTime, formatRelativeTime } from "@/lib/utils"
import { PipelineActions } from "@/components/pipeline/PipelineActions"
import { CDCPipelineActions } from "@/components/pipeline/CDCPipelineActions"
import { PipelineExecutionStatusBadge } from "@/components/pipeline/PipelineExecutionStatusBadge"
import { SchemaDriftBadge } from "@/components/pipeline/SchemaDriftBadge"
import { PipelineScheduleCreateDialogLauncher } from "@/components/pipeline/PipelineSchedulePanel"
import { PipelineHeaderOverflowMenu } from "@/components/pipeline/PipelineHeaderOverflowMenu"
import { PipelineCreateScheduleButton } from "@/components/pipeline/PipelineCreateScheduleButton"
import { PipelineDetailTabsClient } from "@/components/pipeline/PipelineDetailTabsClient"
import { SigNozButton } from "@/components/pipeline/SigNozButton"
import { API_ENDPOINTS, API_GATEWAY_URL_INTERNAL } from "@/lib/config/api"
import { cookies } from "next/headers"
import { activeWorkspaceCookieHeader } from "@/lib/workspace/server-workspace"

export const dynamic = "force-dynamic"

interface Props {
  params: Promise<{ id: string }>
  searchParams: Promise<{ from?: string; tab?: string; editTables?: string; createSchedule?: string }>
}

// Fetch regular pipeline from API
async function getRegularPipeline(id: string) {
  try {
    const cookieStore = await cookies()
    const authToken = cookieStore.get("auth_token")?.value
    // API Gateway expects the raw token value (no Bearer prefix) in local dev.
    // Forward the active-workspace selection (cookie mirror) so the pipeline and
    // its connection fetch scope to the chosen workspace, not the personal one.
    const authHeaders: Record<string, string> = {
      ...(authToken ? { Authorization: authToken } : {}),
      ...activeWorkspaceCookieHeader(cookieStore),
    }

    const res = await fetch(API_ENDPOINTS.PIPELINES.GET_INTERNAL(id), {
      cache: 'no-store',
      signal: AbortSignal.timeout(5000),
      headers: authHeaders,
    })
    
    if (!res.ok) return null
    
    const rawPipeline = await res.json()
    if (!rawPipeline) return null

    let inferredType =
      rawPipeline?.sync_mode === "cdc" || rawPipeline?.cdc_mode ? ("cdc" as const) : ("etl" as const)

    // If the pipeline didn't persist sync_mode yet, fall back to the source connection default.
    // This prevents CDC pipelines from ever showing scheduler UI.
    if (inferredType !== "cdc" && rawPipeline?.source_connection_id) {
      try {
        const connRes = await fetch(`${API_GATEWAY_URL_INTERNAL}/api/v1/connections/${rawPipeline.source_connection_id}`, {
          cache: "no-store",
          signal: AbortSignal.timeout(4000),
          headers: authHeaders,
        })
        if (connRes.ok) {
          const conn = await connRes.json()
          if (conn?.sync_mode === "cdc" || conn?.cdc_mode) {
            inferredType = "cdc"
          }
        }
      } catch {
        // ignore; we'll fall back to pipeline fields only
      }
    }

    // CDC pipelines are continuous; treat pending/draft as running for UI consistency.
    let status = rawPipeline.status ?? "draft"
    if (inferredType === "cdc") {
      const s = String(status || "").toLowerCase()
      if (["draft", "pending", "active", "idle", "scheduled"].includes(s)) {
        status = "running"
      }
    }

    return {
      id: rawPipeline.id,
      name: rawPipeline.name,
      description: rawPipeline.description || "",
      type: inferredType,
      status,
      createdAt: rawPipeline.created_at ? new Date(rawPipeline.created_at) : null,
      updatedAt: rawPipeline.updated_at ? new Date(rawPipeline.updated_at) : null,
      sourceConnection: rawPipeline.source_connection as { id: string; name: string; connector_type: string; status: string } | null ?? null,
      destinationConnection: rawPipeline.destination_connection as { id: string; name: string; connector_type: string; status: string } | null ?? null,
    }
  } catch (error) {
    return null
  }
}

export default async function PipelineDetailPage({ params, searchParams }: Props) {
  const { id } = await params
  const { from, tab } = await searchParams
  const isFromChat = from === "chat"
  const tabValue = ["overview", "history", "steps", "monitor", "table-stats", "transforms"].includes(String(tab || ""))
    ? (String(tab) as "overview" | "history" | "steps" | "monitor" | "table-stats" | "transforms")
    : "overview"
  
  // Always use API Gateway as the source of truth (auth + consistent shape).
  const pipeline = await getRegularPipeline(id)

  if (!pipeline) {
    notFound()
  }

  return (
    <div className="space-y-6">
      {/* Mounted outside tabs so "Create Schedule" works from any tab */}
      {pipeline.type !== "cdc" && <PipelineScheduleCreateDialogLauncher pipelineId={pipeline.id} />}
      {/* Header */}
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="flex min-w-0 items-center gap-4">
          <Link href="/pipelines">
            <Button variant="ghost" size="sm">
              <ArrowLeft className="h-4 w-4 mr-2" />
              Back
            </Button>
          </Link>
          <div>
            <div className="flex items-center gap-3">
              <h1 className="text-2xl font-bold tracking-tight text-zinc-900 dark:text-white break-words">
                {pipeline.name}
              </h1>
              <PipelineExecutionStatusBadge pipelineId={pipeline.id} />
              <SchemaDriftBadge pipelineId={pipeline.id} />
              <Badge
                variant="outline"
                className={
                  pipeline.type === "cdc"
                    ? "bg-violet-50 text-violet-700 dark:bg-violet-900/30 dark:text-violet-300"
                    : "bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300"
                }
              >
                {pipeline.type.toUpperCase()}
              </Badge>
            </div>
            {/* Connection identity strip — shows exactly which connections this pipeline uses */}
            {(pipeline.sourceConnection || pipeline.destinationConnection) ? (
              <div className="flex items-center gap-1.5 mt-1.5 text-sm text-zinc-600 dark:text-zinc-400">
                {pipeline.sourceConnection ? (
                  <span className="font-medium text-zinc-800 dark:text-zinc-200 truncate max-w-[200px]" title={pipeline.sourceConnection.name}>
                    {pipeline.sourceConnection.name}
                  </span>
                ) : (
                  <span className="italic text-zinc-400">No source</span>
                )}
                <span className="text-zinc-400 shrink-0">→</span>
                {pipeline.destinationConnection ? (
                  <span className="font-medium text-zinc-800 dark:text-zinc-200 truncate max-w-[200px]" title={pipeline.destinationConnection.name}>
                    {pipeline.destinationConnection.name}
                  </span>
                ) : (
                  <span className="italic text-zinc-400">No destination</span>
                )}
              </div>
            ) : pipeline.description ? (
              <p className="text-sm text-zinc-500 dark:text-zinc-400 mt-1">
                {pipeline.description}
              </p>
            ) : null}
          </div>
        </div>

        {/* Actions */}
        <div className="flex flex-wrap items-center justify-end gap-2">
          {pipeline.type === "cdc" ? (
            <CDCPipelineActions
              pipelineId={pipeline.id}
              pipelineName={pipeline.name}
              status={pipeline.status}
              destinationName={pipeline.destinationConnection?.name}
            />
          ) : (
            <PipelineActions pipelineId={pipeline.id} status={pipeline.status} />
          )}
          {pipeline.type !== "cdc" && (
            <PipelineCreateScheduleButton pipelineId={pipeline.id} />
          )}
          <SigNozButton pipelineId={pipeline.id} />
          <PipelineHeaderOverflowMenu
            pipelineId={pipeline.id}
            pipelineName={pipeline.name}
            pipelineType={pipeline.type}
            status={pipeline.status}
          />
        </div>
      </div>

      {/* From Chat Banner */}
      {isFromChat && (
        <Card className="border-violet-200 bg-violet-50 dark:bg-violet-950/30 dark:border-violet-800">
          <CardContent className="py-3 px-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2 text-violet-700 dark:text-violet-300">
                <MessageSquare className="h-4 w-4" />
                <span className="text-sm font-medium">Pipeline started from chat</span>
              </div>
              <Link href="/chat">
                <Button variant="ghost" size="sm" className="text-violet-600 hover:text-violet-700 hover:bg-violet-100 dark:text-violet-300 dark:hover:bg-violet-900/50">
                  <ArrowLeft className="h-4 w-4 mr-1" />
                  Back to chat
                </Button>
              </Link>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Tabbed Content — wrap in Suspense so useSearchParams() in PipelineDetailTabsClient
           works correctly in Next.js 15+ (without this, the hook bails out of SSR and
           Radix Tabs context is broken, causing all triggers to render with tabIndex=-1). */}
      <Suspense fallback={
        <div className="h-10 w-full rounded-lg bg-zinc-100 dark:bg-zinc-800 animate-pulse" />
      }>
        <PipelineDetailTabsClient pipelineId={pipeline.id} pipelineType={pipeline.type} initialTab={tabValue} />
      </Suspense>

          {/* Metadata */}
          <Card>
            <CardContent className="pt-6">
              <div className="grid md:grid-cols-3 gap-4 text-sm">
                <div>
                  <p className="text-zinc-500 mb-1">Pipeline ID</p>
              <code className="bg-zinc-100 dark:bg-zinc-800 px-2 py-1 rounded text-xs">{pipeline.id}</code>
                </div>
                <div>
                  <p className="text-zinc-500 mb-1">Created</p>
                  <p>{pipeline.createdAt ? formatDateTime(pipeline.createdAt) : "Unknown"}</p>
                </div>
                <div>
                  <p className="text-zinc-500 mb-1">Last Updated</p>
                  <p>{pipeline.updatedAt ? formatRelativeTime(pipeline.updatedAt) : "Unknown"}</p>
                </div>
              </div>
            </CardContent>
          </Card>
    </div>
  )
}
