"use client"

import { useCallback } from "react"
import { Button } from "@/components/ui/button"
import { emitPipelineScheduleCreate } from "@/lib/events/scheduleCreate"

export function PipelineCreateScheduleButton(props: { pipelineId: string; className?: string }) {
  const { pipelineId, className } = props

  const onClick = useCallback(() => {
    emitPipelineScheduleCreate(pipelineId)
  }, [pipelineId])

  return (
    <Button variant="outline" size="sm" onClick={onClick} className={className}>
      Create Schedule
    </Button>
  )
}

