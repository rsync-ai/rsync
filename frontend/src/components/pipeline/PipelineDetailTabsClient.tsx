"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { usePathname, useRouter, useSearchParams } from "next/navigation"

import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Card, CardContent } from "@/components/ui/card"

import { DataLoadingStrategyCard } from "@/components/pipeline/DataLoadingStrategyCard"
import { PipelineSchedulePanel } from "@/components/pipeline/PipelineSchedulePanel"
import { PipelineLiveStatePanel } from "@/components/pipeline/PipelineLiveStatePanel"
import { SelfHealingPanel } from "@/components/pipeline/SelfHealingPanel"
import { PipelineMonitoringPanelNoSSR } from "@/components/pipeline/PipelineMonitoringPanelNoSSR"
import { ExecutionHistoryTab } from "@/components/pipeline/ExecutionHistoryTab"
import { StepsDAGTab } from "@/components/pipeline/StepsDAGTab"
import { PipelineTransformsTab } from "@/components/pipeline/PipelineTransformsTab"
import { CDCLagAlertsPanel } from "@/components/pipeline/CDCLagAlertsPanel"
import { PipelineHealthHeader } from "@/components/pipeline/PipelineHealthHeader"
import { MonitorTab } from "@/components/pipeline/MonitorTab"

type TabValue = "overview" | "history" | "steps" | "table-stats" | "transforms" | "monitor"

function normalizeTab(v: unknown): TabValue {
  const s = String(v || "").trim()
  if (
    s === "overview" ||
    s === "history" ||
    s === "steps" ||
    s === "table-stats" ||
    s === "transforms" ||
    s === "monitor"
  )
    return s
  return "overview"
}

export function PipelineDetailTabsClient(props: {
  pipelineId: string
  pipelineType: "etl" | "cdc"
  initialTab?: string
}) {
  const { pipelineId, pipelineType } = props
  const router = useRouter()
  const pathname = usePathname()
  const searchParams = useSearchParams()

  const tabFromUrl = useMemo(() => normalizeTab(searchParams?.get("tab")), [searchParams])
  const [value, setValue] = useState<TabValue>(() => normalizeTab(props.initialTab))

  // Keep the tab UI in sync with the URL (required for deep-links and header actions).
  useEffect(() => {
    setValue(tabFromUrl)
  }, [tabFromUrl])

  const onValueChange = useCallback(
    (next: string) => {
      const v = normalizeTab(next)
      setValue(v)

      const sp = new URLSearchParams(searchParams?.toString() || "")
      if (v === "overview") sp.delete("tab")
      else sp.set("tab", v)

      // Tab-specific tokens should not leak between tabs.
      if (v !== "table-stats") sp.delete("editTables")

      const qs = sp.toString()
      router.push(qs ? `${pathname}?${qs}` : pathname)
    },
    [router, pathname, searchParams]
  )

  return (
    <div className="space-y-6">
      <PipelineHealthHeader pipelineId={pipelineId} />
      <Tabs value={value} onValueChange={onValueChange} className="space-y-6">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="history">Execution History</TabsTrigger>
          <TabsTrigger value="steps">Steps/DAG</TabsTrigger>
          <TabsTrigger value="table-stats">Table statistics</TabsTrigger>
          <TabsTrigger value="transforms">Transforms</TabsTrigger>
          {/* Labeled "Data flow" (not "Monitor") so it doesn't collide with the
              transform-level monitoring that lives inside the Transforms tab. The
              tab value stays "monitor" to keep existing deep links working. */}
          <TabsTrigger value="monitor">Data flow</TabsTrigger>
        </TabsList>

      {/* Overview Tab */}
      <TabsContent value="overview" className="space-y-6">
        {/* Data loading strategy (always visible) */}
        <DataLoadingStrategyCard pipelineId={pipelineId} />

        {/* Schedule (batch/ETL only). CDC pipelines are continuous and must not expose scheduler UI. */}
        {pipelineType !== "cdc" && (
          <Card>
            <CardContent className="pt-6">
              <PipelineSchedulePanel pipelineId={pipelineId} />
            </CardContent>
          </Card>
        )}

        {/* Live State */}
        <PipelineLiveStatePanel pipelineId={pipelineId} />

        {/* What the heal agent did about this pipeline, and why. Renders nothing
            when the healer has never had reason to look at it, so a healthy
            pipeline's Overview is unchanged. Below the live state on purpose:
            healing is always a reaction to what that panel is showing. */}
        <SelfHealingPanel pipelineId={pipelineId} />
      </TabsContent>

      {/* Execution History Tab */}
      <TabsContent value="history">
        <Card>
          <CardContent className="pt-6">
            <ExecutionHistoryTab pipelineId={pipelineId} />
          </CardContent>
        </Card>
      </TabsContent>

      {/* Steps/DAG Tab */}
      <TabsContent value="steps">
        <Card>
          <CardContent className="pt-6">
            <StepsDAGTab pipelineId={pipelineId} />
          </CardContent>
        </Card>
      </TabsContent>

      {/* Table statistics Tab */}
      <TabsContent value="table-stats">
        <Card>
          <CardContent className="pt-6">
            <PipelineMonitoringPanelNoSSR pipelineId={pipelineId} variant="table_stats" />
          </CardContent>
        </Card>
      </TabsContent>

      {/* Transforms Tab */}
      <TabsContent value="transforms">
        <Card>
          <CardContent className="pt-6">
            <PipelineTransformsTab pipelineId={pipelineId} />
          </CardContent>
        </Card>
      </TabsContent>

      {/* Monitor Tab */}
      <TabsContent value="monitor" className="space-y-6">
        {/* CDC Source Lag Alerts (CDC pipelines only) — moved here from Overview. */}
        {pipelineType === "cdc" && (
          <CDCLagAlertsPanel pipelineId={pipelineId} />
        )}
        <MonitorTab pipelineId={pipelineId} />
        {/* Rich monitoring (Agent Reasoning / Data Plane / SigNoz) — moved here
            from the Overview tab to remove the duplicate monitoring surface. */}
        <PipelineMonitoringPanelNoSSR pipelineId={pipelineId} variant="monitoring" />
      </TabsContent>
      </Tabs>
    </div>
  )
}

