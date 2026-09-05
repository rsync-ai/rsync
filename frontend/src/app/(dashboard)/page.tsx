import { Suspense } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import Link from "next/link"
import { 
  GitBranch, 
  Play, 
  CheckCircle2, 
  XCircle, 
  Clock,
  Zap,
  Database,
  ArrowRightLeft,
  Plus,
  History,
  ArrowRight,
  Sparkles,
  AlertCircle,
  Loader2,
  RefreshCw,
  AlertTriangle
} from "lucide-react"
import { HomeAgentWidget } from "@/components/home/HomeAgentWidget"
import { FirstRunOnboarding } from "@/components/onboarding/FirstRunOnboarding"
import { DashboardStatsRefresher } from "@/components/dashboard/DashboardStatsRefresher"
import { API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL } from "@/lib/config/api"
import { formatRelativeTime } from "@/lib/utils"
import { cookies } from "next/headers"
import { ORCHESTRATOR_URL, ORCHESTRATOR_URL_INTERNAL } from "@/lib/config/api"

export const dynamic = "force-dynamic"

async function buildCookieHeader(): Promise<string> {
  try {
    const store = await cookies()
    const all = store.getAll()
    if (!all || all.length === 0) return ""
    return all.map((c) => `${c.name}=${c.value}`).join("; ")
  } catch {
    return ""
  }
}

async function fetchGatewayJSON(path: string) {
  const cookieHeader = await buildCookieHeader()
  const headers: Record<string, string> = cookieHeader ? { cookie: cookieHeader } : {}

  // Prefer internal Docker URL on the server, but fall back to localhost for local dev.
  const baseUrls = [API_GATEWAY_URL_INTERNAL, API_GATEWAY_URL].filter(Boolean)

  for (const baseUrl of baseUrls) {
    try {
      const res = await fetch(`${baseUrl}${path}`, {
        cache: "no-store",
        headers,
      }).catch(() => null)

      if (!res || !res.ok) continue
      return await res.json()
    } catch {
      // try next baseUrl
    }
  }

  return null
}

async function fetchOrchestratorJSON(path: string) {
  const baseUrls = [ORCHESTRATOR_URL_INTERNAL, ORCHESTRATOR_URL].filter(Boolean)
  // This runs server-side (RSC). The orchestrator's CDC endpoints now require a
  // principal (they are no longer anonymously reachable over /orchestrator), so
  // present the shared internal-service secret. INTERNAL_SERVICE_SECRET is a
  // server-only env var — it must NOT be exposed as NEXT_PUBLIC_*.
  const internalSecret = process.env.INTERNAL_SERVICE_SECRET
  const headers: Record<string, string> = {}
  if (internalSecret) headers["X-Internal-Secret"] = internalSecret
  for (const baseUrl of baseUrls) {
    try {
      const res = await fetch(`${baseUrl}${path}`, {
        cache: "no-store",
        headers,
      }).catch(() => null)
      if (!res || !res.ok) continue
      return await res.json()
    } catch {
      // try next baseUrl
    }
  }
  return null
}

// Fetch stats from backend orchestrator API
async function getStats() {
  try {
    // Fetch generic endpoints instead of CDC-specific ones.
    // Important: these endpoints are user-scoped, so we must forward cookies from the incoming request.
    const [pipelinesDataRaw, sourcesDataRaw, destinationsDataRaw, executionsDataRaw, cdcPipelinesRaw, usageDataRaw] = await Promise.all([
      fetchGatewayJSON("/api/v1/pipelines"),
      fetchGatewayJSON("/api/v1/connections?type=source"),
      fetchGatewayJSON("/api/v1/connections?type=destination"),
      fetchGatewayJSON("/api/v1/executions"),
      fetchOrchestratorJSON("/api/v1/cdc/data-pipelines"),
      fetchGatewayJSON("/api/v1/usage"),
    ])

    const pipelinesData = pipelinesDataRaw || { pipelines: [], total: 0 }
    const sourcesData = sourcesDataRaw || { connections: [], total: 0 }
    const destinationsData = destinationsDataRaw || { connections: [], total: 0 }
    const executionsData =
      executionsDataRaw || { executions: [], total: 0, stats: { running: 0, success: 0, failed: 0 } }

    const pipelines = pipelinesData.pipelines || []
    const sources = sourcesData.connections || []
    const destinations = destinationsData.connections || []
    const executions = executionsData.executions || []

    const cdcPipelines = Array.isArray(cdcPipelinesRaw) ? cdcPipelinesRaw : []

    // Build connection lookup by id to enrich pipelines list.
    const connectionById = new Map<string, any>()
    for (const c of [...sources, ...destinations]) {
      if (c?.id) connectionById.set(String(c.id), c)
    }

    const enrichedRegularPipelines = pipelines.map((p: any) => {
      const src = p?.source_connection_id ? connectionById.get(String(p.source_connection_id)) : null
      const dst = p?.destination_connection_id ? connectionById.get(String(p.destination_connection_id)) : null
      return {
        ...p,
        source_connection: src
          ? { id: src.id, name: src.name, connector_type: src.connector_type }
          : p.source_connection,
        destination_connection: dst
          ? { id: dst.id, name: dst.name, connector_type: dst.connector_type }
          : p.destination_connection,
        source_type: src?.connector_type || p.source_type,
        destination_type: dst?.connector_type || p.destination_type,
        __kind: "regular",
      }
    })

    const mappedCdcPipelines = cdcPipelines.map((p: any) => ({
      id: p.id,
      name: p.name || `CDC Pipeline ${String(p.id || "").slice(0, 8)}`,
      status: p.status || "running",
      source_type: p.source_type || "source",
      destination_type: p.destination_type || "destination",
      created_at: p.started_at || p.created_at || null,
      updated_at: p.last_record_at || p.updated_at || p.started_at || null,
      __kind: "cdc",
    }))

    // Count pipeline statuses (if not provided by API)
    const pipelineStats = {
      total: (pipelinesData.total || pipelines.length) + mappedCdcPipelines.length,
      running:
        pipelines.filter((p: any) => p.status === "running").length +
        mappedCdcPipelines.filter((p: any) => p.status === "running").length,
      completed: pipelines.filter((p: any) => p.status === "active" || p.status === "completed").length,
      failed:
        pipelines.filter((p: any) => p.status === "failed").length +
        mappedCdcPipelines.filter((p: any) => p.status === "failed").length,
    }

    // Use execution stats from API if available, otherwise calculate
    const executionStats = executionsData.stats || {
      total: executionsData.total || executions.length,
      running: executions.filter((e: any) => e.status === 'running').length,
      success: executions.filter((e: any) => e.status === 'success' || e.status === 'completed').length,
      failed: executions.filter((e: any) => e.status === 'failed').length,
    }
    // Ensure total is set if missing in stats
    if (!executionStats.total) executionStats.total = executionsData.total || executions.length

    const recentPipelines = [...mappedCdcPipelines, ...enrichedRegularPipelines]
      .sort((a: any, b: any) => {
        const at = a.updated_at || a.created_at || 0
        const bt = b.updated_at || b.created_at || 0
        return String(bt).localeCompare(String(at))
      })
      .slice(0, 5)

    return {
      pipelineStats,
      executionStats,
      sourceCount: sourcesData.total || sources.length,
      destinationCount: destinationsData.total || destinations.length,
      // NL→SQL queries run this month (onboarding step 4 = "ask a question").
      queriesUsed: Number(usageDataRaw?.queries_used ?? 0),
      recentPipelines,
      recentExecutions: executions.slice(0, 5),
      error: null
    }
  } catch (error) {
    console.error("Failed to fetch dashboard stats:", error)
    return {
      pipelineStats: { total: 0, running: 0, completed: 0, failed: 0 },
      executionStats: { total: 0, running: 0, success: 0, failed: 0 },
      sourceCount: 0,
      destinationCount: 0,
      queriesUsed: 0,
      recentPipelines: [],
      recentExecutions: [],
      error: "Failed to connect to backend service. Please check if the API Gateway is running."
    }
  }
}

function StatCard({ title, value, icon: Icon, color, bgColor, subtitle, href }: {
  title: string
  value: number
  icon: React.ElementType
  color: string
  bgColor: string
  subtitle?: string
  href?: string
}) {
  const content = (
    <div className="flex items-center justify-between">
      <div className="space-y-1">
        <p className="text-sm font-medium text-zinc-500 dark:text-zinc-400">
          {title}
        </p>
        <p className="text-3xl font-bold tracking-tight text-zinc-900 dark:text-white">
          {value}
        </p>
        {subtitle && (
          <p className="text-xs text-zinc-400 dark:text-zinc-500">{subtitle}</p>
        )}
      </div>
      <div className={`rounded-full p-3 ${bgColor}`}>
        <Icon className={`h-5 w-5 ${color}`} />
      </div>
    </div>
  )

  if (href) {
    return (
      <Link href={href}>
        <Card className="overflow-hidden hover:border-zinc-300 dark:hover:border-zinc-700 transition-all cursor-pointer h-full">
          <CardContent className="p-6">
            {content}
          </CardContent>
        </Card>
      </Link>
    )
  }

  return (
    <Card className="overflow-hidden h-full">
      <CardContent className="p-6">
        {content}
      </CardContent>
    </Card>
  )
}

function RecentPipelines({ pipelines }: { pipelines: any[] }) {
  if (pipelines.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center">
        <div className="h-12 w-12 rounded-full bg-zinc-100 dark:bg-zinc-800 flex items-center justify-center mb-4">
          <GitBranch className="h-6 w-6 text-zinc-400" />
        </div>
        <p className="text-sm font-medium text-zinc-900 dark:text-zinc-100">
          No pipelines yet
        </p>
        <p className="text-xs text-zinc-500 dark:text-zinc-400 mt-1 max-w-[200px]">
          Create your first pipeline to start syncing data
        </p>
        <Link href="/chat" className="mt-4">
          <Button variant="outline" size="sm">Create Pipeline</Button>
        </Link>
      </div>
    )
  }

  // The pipelines list API returns `derived_status` (running|passed|failed|stopped|
  // paused|scheduled|idle); CDC pipelines carry `status`. Reading `pipeline.status`
  // alone left every card on the default Clock icon (ISSUE-003).
  const pipeStatus = (p: any): string =>
    String(p?.derived_status || p?.pipeline_status || p?.status || '')

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'running':
        return <RefreshCw className="h-4 w-4 text-emerald-600 animate-spin" />
      case 'passed':
      case 'completed':
      case 'active':
        return <CheckCircle2 className="h-4 w-4 text-emerald-600" />
      case 'failed':
        return <XCircle className="h-4 w-4 text-red-600" />
      case 'waiting_for_user':
        return <AlertTriangle className="h-4 w-4 text-amber-600" />
      default:
        return <Clock className="h-4 w-4 text-blue-600" />
    }
  }

  const getStatusBg = (status: string) => {
    switch (status) {
      case 'running':
      case 'passed':
      case 'completed':
      case 'active':
        return "bg-emerald-100 dark:bg-emerald-900/30"
      case 'failed':
        return "bg-red-100 dark:bg-red-900/30"
      case 'waiting_for_user':
        return "bg-amber-100 dark:bg-amber-900/30"
      default:
        return "bg-blue-100 dark:bg-blue-900/30"
    }
  }

  return (
    <div className="space-y-3">
      {pipelines.map((pipeline: any) => (
        <Link
          key={pipeline.id}
          href={`/pipelines/${pipeline.id}`}
          className="flex items-center justify-between p-4 rounded-lg border border-zinc-200 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50 transition-colors group"
        >
          <div className="flex items-center gap-4">
            <div className={`rounded-full p-2 ${getStatusBg(pipeStatus(pipeline))}`}>
              {getStatusIcon(pipeStatus(pipeline))}
            </div>
            <div>
              <p className="font-medium text-zinc-900 dark:text-zinc-100 group-hover:text-violet-600 transition-colors">
                {pipeline.name || `Pipeline ${pipeline.id?.slice(0, 8)}`}
              </p>
              {/* Fallback to generic text if connection details aren't populated yet */}
              <p className="text-sm text-zinc-500">
                {(pipeline.source_connection?.name || pipeline.source_connection?.connector_type || pipeline.source_type || "Source")}{" "}
                →{" "}
                {(pipeline.destination_connection?.name || pipeline.destination_connection?.connector_type || pipeline.destination_type || "Destination")}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <Badge variant={
              ['completed', 'active', 'running', 'passed'].includes(pipeStatus(pipeline)) ? "default" :
              pipeStatus(pipeline) === 'failed' ? "destructive" : "secondary"
            }>
              {pipeStatus(pipeline)}
            </Badge>
            <ArrowRight className="h-4 w-4 text-zinc-400 opacity-0 group-hover:opacity-100 transition-opacity" />
          </div>
        </Link>
      ))}
    </div>
  )
}

function RecentExecutions({ executions }: { executions: any[] }) {
  if (executions.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center">
        <div className="h-12 w-12 rounded-full bg-zinc-100 dark:bg-zinc-800 flex items-center justify-center mb-4">
          <History className="h-6 w-6 text-zinc-400" />
        </div>
        <p className="text-sm font-medium text-zinc-900 dark:text-zinc-100">
          No executions yet
        </p>
        <p className="text-xs text-zinc-500 dark:text-zinc-400 mt-1 max-w-[200px]">
          Run a pipeline to see execution history
        </p>
      </div>
    )
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'running':
        return <Loader2 className="h-4 w-4 text-blue-600 animate-spin" />
      case 'success':
      case 'completed':
        return <CheckCircle2 className="h-4 w-4 text-emerald-600" />
      case 'failed':
        return <XCircle className="h-4 w-4 text-red-600" />
      case 'cancelled':
        return <XCircle className="h-4 w-4 text-zinc-500" />
      default:
        return <Clock className="h-4 w-4 text-zinc-400" />
    }
  }

  return (
    <div className="space-y-2">
      {executions.map((execution: any) => (
        <Link
          key={execution.id}
          href={`/executions/${execution.id}`}
          className="flex items-center gap-3 p-3 rounded-lg hover:bg-zinc-50 dark:hover:bg-zinc-800/50 transition-colors group"
        >
          {getStatusIcon(execution.status)}
          <div className="flex-1 min-w-0">
            <p className="text-sm font-medium text-zinc-900 dark:text-zinc-100 truncate group-hover:text-violet-600 transition-colors">
              {execution.pipeline_name || execution.pipeline?.name || `Execution ${execution.id?.slice(0, 8)}`}
            </p>
            <p className="text-xs text-zinc-500">
              {execution.start_time || execution.startedAt ? formatRelativeTime(execution.start_time || execution.startedAt) : 'Pending'}
            </p>
          </div>
          <Badge 
            variant={
              execution.status === 'success' || execution.status === 'completed' ? "success" : 
              execution.status === 'failed' ? "destructive" : 
              execution.status === 'running' ? "info" : "secondary"
            }
            className="text-[10px] px-1.5 py-0"
          >
            {execution.status}
          </Badge>
        </Link>
      ))}
    </div>
  )
}

export default async function HomePage() {
  const { 
    pipelineStats,
    executionStats,
    sourceCount,
    destinationCount,
    queriesUsed,
    recentPipelines,
    recentExecutions,
    error
  } = await getStats()

  const statCards = [
    {
      title: "Pipelines",
      value: pipelineStats.total,
      iconName: "GitBranch",
      color: "text-violet-600",
      bgColor: "bg-violet-100 dark:bg-violet-900/30",
      subtitle: `${pipelineStats.running} running`,
      href: "/pipelines"
    },
    {
      title: "Executions",
      value: executionStats.total,
      iconName: "History",
      color: "text-blue-600",
      bgColor: "bg-blue-100 dark:bg-blue-900/30",
      subtitle: `${executionStats.success} successful`,
      href: "/executions"
    },
    {
      title: "Sources",
      value: sourceCount,
      iconName: "Database",
      color: "text-emerald-600",
      bgColor: "bg-emerald-100 dark:bg-emerald-900/30",
      href: "/connections?type=source"
    },
    {
      title: "Destinations",
      value: destinationCount,
      iconName: "ArrowRightLeft",
      color: "text-amber-600",
      bgColor: "bg-amber-100 dark:bg-amber-900/30",
      href: "/connections?type=destination"
    },
  ]

  return (
    <div className="space-y-8">
      {/* Welcome Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-zinc-900 dark:text-white">Welcome to Rsync</h1>
          <p className="text-zinc-500 dark:text-zinc-400 mt-1">
            Build and manage your data pipelines with AI assistance
          </p>
        </div>
        <Link href="/chat">
          <Button className="gap-2 bg-violet-600 hover:bg-violet-700 text-white shadow-lg shadow-violet-200 dark:shadow-violet-900/20">
            <Plus className="h-4 w-4" />
            Create Pipeline
          </Button>
        </Link>
      </div>

      {/* Error Banner */}
      {error && (
        <Card className="border-amber-200 bg-amber-50 dark:border-amber-900/50 dark:bg-amber-950/30">
          <CardContent className="p-4 flex items-center gap-3">
            <AlertCircle className="h-5 w-5 text-amber-600 dark:text-amber-500" />
            <p className="text-sm text-amber-700 dark:text-amber-400 font-medium">{error}</p>
          </CardContent>
        </Card>
      )}

      {/* First-run onboarding — activation checklist. Gated on "not yet fully
          onboarded" (some step still incomplete). The component re-verifies
          counts client-side and self-hides once every step is done, so it never
          lingers for established users hit by SSR false-positive zeros. Uses the
          same signals as the checklist (source/dest/pipeline present + at least
          one NL query) so the SSR gate and the client self-hide agree. */}
      {!(sourceCount > 0 && destinationCount > 0 && pipelineStats.total > 0 && (queriesUsed ?? 0) > 0) && (
        <FirstRunOnboarding
          initial={{
            sourceCount,
            destinationCount,
            pipelineCount: pipelineStats.total,
            queryCount: queriesUsed ?? 0,
          }}
        />
      )}

      {/* Stats Grid — uses client-side hydration fallback when SSR returns all zeros */}
      <DashboardStatsRefresher
        initialCards={statCards}
        looksEmpty={pipelineStats.total === 0 && executionStats.total === 0 && sourceCount === 0 && destinationCount === 0}
      />

      {/* AI Agent Widget */}
      <Card className="overflow-hidden border-2 border-violet-200 dark:border-violet-900 shadow-sm">
        <CardHeader className="bg-gradient-to-r from-violet-50 to-indigo-50 dark:from-violet-950/30 dark:to-indigo-950/30 border-b border-violet-100 dark:border-violet-900/50">
          <CardTitle className="flex items-center gap-2 text-violet-900 dark:text-violet-100">
            <Sparkles className="h-5 w-5 text-violet-600 dark:text-violet-400" />
            Create with AI
          </CardTitle>
          <CardDescription className="text-violet-700/80 dark:text-violet-300/70">
            Describe what you want to build and let AI create your pipeline
          </CardDescription>
        </CardHeader>
        <CardContent className="p-0 bg-white dark:bg-zinc-950">
          <Suspense fallback={<div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-violet-500" /></div>}>
            <HomeAgentWidget />
          </Suspense>
        </CardContent>
      </Card>

      {/* Main Content Grid */}
      <div className="grid gap-6 lg:grid-cols-3">
        {/* Recent Pipelines - Takes 2 columns */}
        <Card className="lg:col-span-2 shadow-sm hover:shadow-md transition-shadow">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <div>
              <CardTitle className="flex items-center gap-2 text-base">
                <GitBranch className="h-5 w-5 text-zinc-500" />
                Your Pipelines
              </CardTitle>
              <CardDescription>
                Recent data pipelines and their status
              </CardDescription>
            </div>
            <Link href="/pipelines">
              <Button variant="ghost" size="sm" className="gap-1 text-zinc-500 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100">
                See All <ArrowRight className="h-3 w-3" />
              </Button>
            </Link>
          </CardHeader>
          <CardContent>
            <Suspense fallback={<div className="flex items-center justify-center py-8"><Loader2 className="h-6 w-6 animate-spin text-zinc-400" /></div>}>
              <RecentPipelines pipelines={recentPipelines} />
            </Suspense>
          </CardContent>
        </Card>

        {/* Recent Executions */}
        <Card className="shadow-sm hover:shadow-md transition-shadow">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <div>
              <CardTitle className="flex items-center gap-2 text-base">
                <History className="h-5 w-5 text-zinc-500" />
                Recent Runs
              </CardTitle>
              <CardDescription>
                Latest execution activity
              </CardDescription>
            </div>
            <Link href="/executions">
              <Button variant="ghost" size="sm" className="gap-1 text-zinc-500 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100">
                See All <ArrowRight className="h-3 w-3" />
              </Button>
            </Link>
          </CardHeader>
          <CardContent>
            <Suspense fallback={<div className="flex items-center justify-center py-8"><Loader2 className="h-6 w-6 animate-spin text-zinc-400" /></div>}>
              <RecentExecutions executions={recentExecutions} />
            </Suspense>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
