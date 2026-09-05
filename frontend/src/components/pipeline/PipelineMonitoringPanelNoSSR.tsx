"use client"

import dynamic from "next/dynamic"

export const PipelineMonitoringPanelNoSSR = dynamic(
  () => import("@/components/pipeline/PipelineMonitoringPanel").then((m) => m.PipelineMonitoringPanel),
  { ssr: false }
)


