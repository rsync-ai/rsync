"use client"

import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Layers,
  ExternalLink,
  CheckCircle2,
  XCircle,
  Loader2,
  Clock,
  Trash2,
} from "lucide-react"
import { type ActivePipeline } from "@/lib/hooks/useActivePipelines"
import { formatRelativeTime } from "@/lib/utils"

interface ActivePipelinesListProps {
  pipelines: ActivePipeline[]
  currentPipelineId?: string
  onSelect: (pipelineId: string) => void
  onRemove: (pipelineId: string) => void
  onClearCompleted: () => void
}

export function ActivePipelinesList({
  pipelines,
  currentPipelineId,
  onSelect,
  onRemove,
  onClearCompleted,
}: ActivePipelinesListProps) {
  const router = useRouter()

  const getStatusIcon = (status?: string) => {
    switch (status) {
      case "completed":
        return <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />
      case "failed":
        return <XCircle className="h-3.5 w-3.5 text-red-500" />
      case "processing":
      case "running":
        return <Loader2 className="h-3.5 w-3.5 text-blue-500 animate-spin" />
      default:
        return <Clock className="h-3.5 w-3.5 text-zinc-400" />
    }
  }

  const activePipelines = pipelines.filter(
    (p) => p.status !== "completed" && p.status !== "failed"
  )
  const completedPipelines = pipelines.filter(
    (p) => p.status === "completed" || p.status === "failed"
  )

  if (pipelines.length === 0) {
    return null
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="gap-1.5">
          <Layers className="h-4 w-4" />
          <span className="hidden sm:inline">Pipelines</span>
          {activePipelines.length > 0 && (
            <Badge variant="secondary" className="h-5 px-1.5 text-xs">
              {activePipelines.length}
            </Badge>
          )}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-72">
        {activePipelines.length > 0 && (
          <>
            <DropdownMenuLabel className="text-xs text-zinc-500">
              Active Pipelines
            </DropdownMenuLabel>
            {activePipelines.map((pipeline) => (
              <DropdownMenuItem
                key={pipeline.id}
                className="flex items-center justify-between cursor-pointer"
                onClick={() => onSelect(pipeline.id)}
              >
                <div className="flex items-center gap-2 flex-1 min-w-0">
                  {getStatusIcon(pipeline.status)}
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-medium truncate">
                      {pipeline.name || `Pipeline ${pipeline.id.slice(0, 8)}`}
                    </div>
                    <div className="text-xs text-zinc-500">
                      {formatRelativeTime(new Date(pipeline.startedAt))}
                    </div>
                  </div>
                </div>
                {pipeline.id === currentPipelineId && (
                  <Badge variant="outline" className="text-xs">
                    Current
                  </Badge>
                )}
              </DropdownMenuItem>
            ))}
          </>
        )}

        {completedPipelines.length > 0 && (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuLabel className="text-xs text-zinc-500 flex items-center justify-between">
              <span>Recent</span>
              <Button
                variant="ghost"
                size="sm"
                className="h-5 px-1.5 text-xs"
                onClick={(e) => {
                  e.stopPropagation()
                  onClearCompleted()
                }}
              >
                <Trash2 className="h-3 w-3 mr-1" />
                Clear
              </Button>
            </DropdownMenuLabel>
            {completedPipelines.slice(0, 5).map((pipeline) => (
              <DropdownMenuItem
                key={pipeline.id}
                className="flex items-center justify-between cursor-pointer"
                onClick={() => router.push(`/pipelines/${pipeline.id}?from=chat`)}
              >
                <div className="flex items-center gap-2 flex-1 min-w-0">
                  {getStatusIcon(pipeline.status)}
                  <div className="flex-1 min-w-0">
                    <div className="text-sm truncate">
                      {pipeline.name || `Pipeline ${pipeline.id.slice(0, 8)}`}
                    </div>
                    <div className="text-xs text-zinc-500">
                      {formatRelativeTime(new Date(pipeline.lastUpdated))}
                    </div>
                  </div>
                </div>
                <ExternalLink className="h-3.5 w-3.5 text-zinc-400" />
              </DropdownMenuItem>
            ))}
          </>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
