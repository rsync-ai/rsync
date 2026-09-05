import { Suspense } from "react"
import { PageHeader } from "@/components/layout/PageHeader"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { 
  GitBranch, 
  Play, 
  CheckCircle2, 
  XCircle, 
  Clock,
  Zap,
  Database,
  ArrowRightLeft
} from "lucide-react"
import { QuickActions } from "@/components/dashboard/QuickActions"
import Link from "next/link"
import { API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL } from "@/lib/config/api"
import { cookies } from "next/headers"

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
  for (const baseUrl of [API_GATEWAY_URL_INTERNAL, API_GATEWAY_URL].filter(Boolean)) {
    try {
      const res = await fetch(`${baseUrl}${path}`, { cache: "no-store", headers }).catch(() => null)
      if (!res || !res.ok) continue
      return await res.json()
    } catch { /* try next */ }
  }
  return null
}

async function getStats() {
  try {
    const [pipelinesData, sourcesData, destinationsData] = await Promise.all([
      fetchGatewayJSON("/api/v1/pipelines"),
      fetchGatewayJSON("/api/v1/connections?type=source"),
      fetchGatewayJSON("/api/v1/connections?type=destination"),
    ])

    const pipelines: any[] = Array.isArray(pipelinesData)
      ? pipelinesData
      : Array.isArray(pipelinesData?.pipelines)
      ? pipelinesData.pipelines
      : []
    const sources: any[] = Array.isArray(sourcesData)
      ? sourcesData
      : Array.isArray(sourcesData?.connections)
      ? sourcesData.connections
      : []
    const destinations: any[] = Array.isArray(destinationsData)
      ? destinationsData
      : Array.isArray(destinationsData?.connections)
      ? destinationsData.connections
      : []

    const pipelineStats = {
      total: pipelines.length,
      running: pipelines.filter((p: any) => p.status === 'running').length,
      completed: pipelines.filter((p: any) => p.status === 'completed').length,
      failed: pipelines.filter((p: any) => p.status === 'failed').length,
    }

    // Build connection lookup by id to enrich pipelines list. The /api/v1/pipelines
    // payload carries only *_connection_id — source_type/destination_type are not
    // columns on `pipelines` and are never returned by the list handler
    // (api-gateway/internal/handlers/pipelines.go selects the two ids and nothing
    // else), so the endpoint the pipeline actually points at has to be resolved
    // here or there is nothing to render.
    //
    // Kept byte-identical to the map in app/(dashboard)/page.tsx, and the render
    // below uses that page's field precedence unchanged. Both pages show a
    // "recent pipelines" list off the same three payloads, and this page is the
    // OAuth-callback landing page while the sidebar's one nav entry leads to the
    // other — so a user sees both. Two renderers disagreeing about the same row
    // is the defect class that produced the hardcoded 'S3' this replaces.
    const connectionById = new Map<string, any>()
    for (const c of [...sources, ...destinations]) {
      if (c?.id) connectionById.set(String(c.id), c)
    }

    const enrichedPipelines = pipelines.map((p: any) => {
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
      }
    })

    return {
      pipelineStats,
      sourceCount: sources.length,
      destinationCount: destinations.length,
      recentPipelines: enrichedPipelines.slice(0, 5),
      error: null,
    }
  } catch (error) {
    console.error("Failed to fetch dashboard stats:", error)
    return {
      pipelineStats: { total: 0, running: 0, completed: 0, failed: 0 },
      sourceCount: 0,
      destinationCount: 0,
      recentPipelines: [],
      error: "Failed to connect to backend",
    }
  }
}

function RecentPipelines({ pipelines }: { pipelines: any[] }) {
  if (pipelines.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-8 text-center">
        <Database className="h-12 w-12 text-zinc-400 mb-4" />
        <p className="text-sm text-zinc-500 dark:text-zinc-400">
          No CDC pipelines yet
        </p>
        <p className="text-xs text-zinc-400 dark:text-zinc-500 mt-2">
          Create a pipeline to start syncing data
        </p>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {pipelines.map((pipeline: any) => (
        <div
          key={pipeline.id}
          className="flex items-center justify-between p-4 rounded-lg border border-zinc-200 dark:border-zinc-800"
        >
          <div className="flex items-center gap-4">
            <div className={`rounded-full p-2 ${
              pipeline.status === 'completed' || pipeline.status === 'running'
                ? "bg-emerald-100 dark:bg-emerald-900/30" 
                : pipeline.status === 'failed'
                ? "bg-red-100 dark:bg-red-900/30"
                : "bg-blue-100 dark:bg-blue-900/30"
            }`}>
              {pipeline.status === 'completed' ? (
                <CheckCircle2 className="h-4 w-4 text-emerald-600" />
              ) : pipeline.status === 'failed' ? (
                <XCircle className="h-4 w-4 text-red-600" />
              ) : pipeline.status === 'running' ? (
                <Play className="h-4 w-4 text-emerald-600" />
              ) : (
                <Clock className="h-4 w-4 text-blue-600" />
              )}
            </div>
            <div>
              <p className="font-medium">{pipeline.name || `Pipeline ${pipeline.id?.slice(0, 8)}`}</p>
              {/* Same precedence as app/(dashboard)/page.tsx:336-338 — name, then
                  connector type, then the flat field, then a neutral placeholder.
                  The final fallback used to be the literal 'S3', which was not a
                  default but a wrong answer: it rendered for every row regardless
                  of where the pipeline wrote, and the two fields it fell back from
                  are never present in this payload, so it always fired. */}
              <p className="text-sm text-zinc-500">
                {(pipeline.source_connection?.name || pipeline.source_connection?.connector_type || pipeline.source_type || 'Source')}{" "}
                →{" "}
                {(pipeline.destination_connection?.name || pipeline.destination_connection?.connector_type || pipeline.destination_type || 'Destination')}
              </p>
            </div>
          </div>
          <Badge variant={
            pipeline.status === 'completed' || pipeline.status === 'running' ? "default" : 
            pipeline.status === 'failed' ? "destructive" : "secondary"
          }>
            {pipeline.status}
          </Badge>
        </div>
      ))}
    </div>
  )
}

export default async function DashboardPage() {
  const { pipelineStats, sourceCount, destinationCount, recentPipelines, error } = await getStats()

  const statCards = [
    {
      // Counts ALL pipelines (batch + CDC), matching the sibling total cards and
      // the "Recent Pipelines" list below. Previously labeled "CDC Pipelines",
      // which was misleading — the count and the recent list were never CDC-only.
      title: "Pipelines",
      value: pipelineStats.total,
      icon: GitBranch,
      color: "text-violet-600",
      bgColor: "bg-violet-100 dark:bg-violet-900/30",
    },
    {
      title: "Running",
      value: pipelineStats.running,
      icon: Play,
      color: "text-emerald-600",
      bgColor: "bg-emerald-100 dark:bg-emerald-900/30",
    },
    {
      title: "Sources",
      value: sourceCount,
      icon: Database,
      color: "text-blue-600",
      bgColor: "bg-blue-100 dark:bg-blue-900/30",
    },
    {
      title: "Destinations",
      value: destinationCount,
      icon: ArrowRightLeft,
      color: "text-amber-600",
      bgColor: "bg-amber-100 dark:bg-amber-900/30",
    },
  ]

  return (
    <div className="space-y-8">
      <PageHeader
        heading="Dashboard"
        description="Overview of your pipelines and data connections"
      />

      {/* Stats Grid */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {statCards.map((stat) => (
          <Card key={stat.title}>
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div className="space-y-1">
                  <p className="text-sm font-medium text-zinc-500 dark:text-zinc-400">
                    {stat.title}
                  </p>
                  <p className="text-3xl font-bold tracking-tight">
                    {stat.value}
                  </p>
                </div>
                <div className={`rounded-full p-3 ${stat.bgColor}`}>
                  <stat.icon className={`h-5 w-5 ${stat.color}`} />
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Main Content Grid */}
      <div className="grid gap-6 lg:grid-cols-3">
        {/* Recent Pipelines */}
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Clock className="h-5 w-5 text-zinc-500" />
              Recent Pipelines
            </CardTitle>
            <CardDescription>
              Latest data sync pipelines and their status
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Suspense fallback={<div>Loading...</div>}>
              <RecentPipelines pipelines={recentPipelines} />
            </Suspense>
          </CardContent>
        </Card>

        {/* Quick Actions */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Zap className="h-5 w-5 text-zinc-500" />
              Quick Actions
            </CardTitle>
            <CardDescription>
              Common tasks and shortcuts
            </CardDescription>
          </CardHeader>
          <CardContent>
            <QuickActions />
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
