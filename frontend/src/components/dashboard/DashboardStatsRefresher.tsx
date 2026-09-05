"use client"

// Client-side hydration guard for dashboard stat cards.
// SSR fetches can silently return 0 when the auth cookie isn't forwarded
// in certain environments (e.g. preview proxy). This component re-fetches
// on mount whenever the SSR values all look like an empty state (all zeros).

import { useCallback, useEffect, useState } from "react"
import Link from "next/link"
import { Card, CardContent } from "@/components/ui/card"
import { GitBranch, History, Database, ArrowRightLeft, RefreshCw, LucideIcon } from "lucide-react"
import { onPipelineRefresh } from "@/lib/events/pipelineRefresh"

const ICON_MAP: Record<string, LucideIcon> = {
  GitBranch,
  History,
  Database,
  ArrowRightLeft,
}

interface StatCardData {
  title: string
  value: number
  iconName: string
  color: string
  bgColor: string
  subtitle?: string
  href?: string
}

interface RefresherStats {
  pipelinesTotal: number
  executionsTotal: number
  sourceCount: number
  destinationCount: number
}

function StatCard({ title, value, iconName, color, bgColor, subtitle, href }: StatCardData) {
  const Icon = ICON_MAP[iconName] ?? GitBranch
  const content = (
    <div className="flex items-center justify-between">
      <div className="space-y-1">
        <p className="text-sm font-medium text-zinc-500 dark:text-zinc-400">{title}</p>
        <p className="text-3xl font-bold tracking-tight text-zinc-900 dark:text-white">{value}</p>
        {/* Always render the subtitle line (placeholder when absent) so cards
            without a subtitle — Sources/Destinations — stay the same height as
            Pipelines/Executions and all four align uniformly. */}
        <p className="text-xs text-zinc-400 dark:text-zinc-500">{subtitle ?? " "}</p>
      </div>
      <div className={`rounded-full p-3 ${bgColor}`}>
        <Icon className={`h-5 w-5 ${color}`} />
      </div>
    </div>
  )

  return (
    <Card className="h-full hover:shadow-md transition-shadow">
      <CardContent className="p-6">
        {href ? (
          <Link href={href} className="block">
            {content}
          </Link>
        ) : (
          content
        )}
      </CardContent>
    </Card>
  )
}

export function DashboardStatsRefresher({
  initialCards,
  looksEmpty,
}: {
  initialCards: StatCardData[]
  looksEmpty: boolean
}) {
  const [cards, setCards] = useState<StatCardData[]>(initialCards)
  const [refreshing, setRefreshing] = useState(false)

  // Re-fetch all four stats from the browser (where the auth cookie is available).
  // Returns a cancel fn so callers can ignore in-flight results on unmount/re-trigger.
  const refetch = useCallback(() => {
    let cancelled = false
    setRefreshing(true)

    ;(async () => {
      try {
        const [pipelinesRes, srcRes, dstRes, execRes] = await Promise.allSettled([
          fetch("/api/v1/pipelines", { credentials: "include" }).then((r) => r.ok ? r.json() : null),
          fetch("/api/v1/connections?type=source", { credentials: "include" }).then((r) => r.ok ? r.json() : null),
          fetch("/api/v1/connections?type=destination", { credentials: "include" }).then((r) => r.ok ? r.json() : null),
          fetch("/api/v1/executions", { credentials: "include" }).then((r) => r.ok ? r.json() : null),
        ])

        if (cancelled) return

        const pipelines = pipelinesRes.status === "fulfilled" ? pipelinesRes.value : null
        const src = srcRes.status === "fulfilled" ? srcRes.value : null
        const dst = dstRes.status === "fulfilled" ? dstRes.value : null
        const exec = execRes.status === "fulfilled" ? execRes.value : null

        const pTotal = pipelines?.total ?? (pipelines?.pipelines?.length ?? 0)
        const pRunning = (pipelines?.pipelines ?? []).filter((p: any) => p.status === "running").length
        const srcTotal = src?.total ?? (src?.connections?.length ?? 0)
        const dstTotal = dst?.total ?? (dst?.connections?.length ?? 0)
        const eTotal = exec?.total ?? (exec?.executions?.length ?? 0)
        const eSuccess = exec?.stats?.success ?? (exec?.executions ?? []).filter((e: any) => e.status === "success" || e.status === "completed").length

        setCards((prev) =>
          prev.map((card) => {
            switch (card.title) {
              case "Pipelines":
                return { ...card, value: pTotal, subtitle: `${pRunning} running` }
              case "Executions":
                return { ...card, value: eTotal, subtitle: `${eSuccess} successful` }
              case "Sources":
                return { ...card, value: srcTotal }
              case "Destinations":
                return { ...card, value: dstTotal }
              default:
                return card
            }
          })
        )
      } catch {
        // Silently ignore — keep SSR values
      } finally {
        if (!cancelled) setRefreshing(false)
      }
    })()

    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (!looksEmpty) return
    // SSR returned all zeros — re-fetch from browser where auth cookie is available.
    return refetch()
  }, [looksEmpty, refetch])

  // Keep stat cards live after a pipeline execution: PipelineActions/CDCPipelineActions
  // emit a pipeline-refresh event on run/start. Re-fetch so e.g. the Executions count
  // doesn't stay stuck at N-1 until a full page reload.
  useEffect(() => {
    return onPipelineRefresh(() => {
      refetch()
    })
  }, [refetch])

  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
      {cards.map((card) => (
        <div key={card.title} className="relative h-full">
          <StatCard {...card} />
          {refreshing && (
            <div className="absolute top-2 right-2">
              <RefreshCw className="h-3 w-3 text-zinc-400 animate-spin" />
            </div>
          )}
        </div>
      ))}
    </div>
  )
}
