"use client"

import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
  X,
  ExternalLink,
  Clock,
  CheckCircle2,
  XCircle,
  Loader2,
  AlertTriangle,
  RotateCcw,
} from "lucide-react"
import { formatDateTime } from "@/lib/utils"
import { type ExecutionPlanStage, getStageStatusConfig, formatDuration } from "./DAGVisualization"
import { stageDurationMs } from "./dagHelpers"

interface NodeInspectorProps {
  node: ExecutionPlanStage
  pipelineId: string
  onClose: () => void
  onRetry?: (nodeId: string) => void
}

export function NodeInspector({ node, pipelineId, onClose, onRetry }: NodeInspectorProps) {
  const router = useRouter()
  const config = getStageStatusConfig(node.status)
  const StatusIcon = config.icon
  const durationMs = stageDurationMs(node)

  const handleViewFullPage = () => {
    router.push(`/pipelines/${pipelineId}?tab=steps&node=${node.id}`)
  }

  return (
    <div className="h-full flex flex-col bg-white dark:bg-zinc-900 border-l border-zinc-200 dark:border-zinc-800">
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b border-zinc-200 dark:border-zinc-800">
        <div className="flex items-center gap-2">
          <div className={`rounded-full p-1.5 ${config.bgColor}`}>
            <StatusIcon
              className={`h-4 w-4 ${config.color} ${node.status === "running" ? "animate-spin" : ""}`}
            />
          </div>
          <h3 className="font-semibold text-sm text-zinc-900 dark:text-white truncate max-w-[180px]">
            {node.display_name}
          </h3>
        </div>
        <Button variant="ghost" size="sm" onClick={onClose} className="h-8 w-8 p-0">
          <X className="h-4 w-4" />
        </Button>
      </div>

      {/* Content */}
      <ScrollArea className="flex-1">
        <div className="p-4 space-y-4">
          {/* Status */}
          <section>
            <h4 className="text-xs font-medium text-zinc-500 dark:text-zinc-400 uppercase tracking-wide mb-2">
              Status
            </h4>
            <Badge variant={node.status === "complete" ? "default" : node.status === "failed" ? "destructive" : "secondary"}>
              {node.status.charAt(0).toUpperCase() + node.status.slice(1)}
            </Badge>
          </section>

          {/* Timing */}
          <section>
            <h4 className="text-xs font-medium text-zinc-500 dark:text-zinc-400 uppercase tracking-wide mb-2">
              Timing
            </h4>
            <div className="space-y-1 text-sm">
              {node.started_at && (
                <div className="flex items-center gap-2 text-zinc-600 dark:text-zinc-400">
                  <Clock className="h-3 w-3" />
                  <span>Started: {formatDateTime(new Date(node.started_at))}</span>
                </div>
              )}
              {node.completed_at && (
                <div className="flex items-center gap-2 text-zinc-600 dark:text-zinc-400">
                  <CheckCircle2 className="h-3 w-3" />
                  <span>Completed: {formatDateTime(new Date(node.completed_at))}</span>
                </div>
              )}
              {durationMs !== null && durationMs > 0 && (
                <div className="flex items-center gap-2 text-zinc-600 dark:text-zinc-400">
                  <Clock className="h-3 w-3" />
                  <span>Duration: {formatDuration(durationMs)}</span>
                </div>
              )}
              {typeof node.progress === "number" && node.progress > 0 && node.progress < 100 && (
                <div className="flex items-center gap-2 text-zinc-600 dark:text-zinc-400">
                  <Loader2 className="h-3 w-3 animate-spin" />
                  <span>Progress: {node.progress}%</span>
                </div>
              )}
            </div>
          </section>

          {/* Description */}
          {node.description && (
            <section>
              <h4 className="text-xs font-medium text-zinc-500 dark:text-zinc-400 uppercase tracking-wide mb-2">
                Description
              </h4>
              <p className="text-sm text-zinc-600 dark:text-zinc-400">{node.description}</p>
            </section>
          )}

          {/* Metadata Preview */}
          {node.metadata && Object.keys(node.metadata).length > 0 && (
            <section>
              <h4 className="text-xs font-medium text-zinc-500 dark:text-zinc-400 uppercase tracking-wide mb-2">
                Metadata
              </h4>
              <div className="bg-zinc-50 dark:bg-zinc-800 rounded-md p-2">
                <pre className="text-xs text-zinc-600 dark:text-zinc-400 overflow-auto max-h-32">
                  {JSON.stringify(
                    {
                      node_kind: node.node_kind,
                      connector_type: node.metadata?.resolved_connector_type || node.metadata?.node_config?.connector_type,
                      dependencies: node.dependencies,
                    },
                    null,
                    2
                  )}
                </pre>
              </div>
            </section>
          )}

          {/* Error Details */}
          {node.error_message && (
            <section>
              <h4 className="text-xs font-medium text-red-500 uppercase tracking-wide mb-2 flex items-center gap-1">
                <AlertTriangle className="h-3 w-3" />
                Error
              </h4>
              <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-2">
                <p className="text-sm text-red-600 dark:text-red-400">{node.error_message}</p>
              </div>
            </section>
          )}
        </div>
      </ScrollArea>

      {/* Actions */}
      <div className="p-4 border-t border-zinc-200 dark:border-zinc-800 space-y-2">
        <Button
          onClick={handleViewFullPage}
          className="w-full"
          variant="default"
          size="sm"
        >
          <ExternalLink className="h-4 w-4 mr-2" />
          View in Full Page
        </Button>
        {node.status === "failed" && onRetry && (
          <Button
            onClick={() => onRetry(node.id)}
            className="w-full"
            variant="outline"
            size="sm"
          >
            <RotateCcw className="h-4 w-4 mr-2" />
            Retry Node
          </Button>
        )}
      </div>
    </div>
  )
}
