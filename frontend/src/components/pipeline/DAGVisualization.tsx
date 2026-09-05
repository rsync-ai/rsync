"use client"

import { useMemo } from "react"
import {
  CheckCircle2,
  XCircle,
  Loader2,
  Clock,
  ArrowDown,
  GitBranch,
  Circle,
  Database,
  Cloud,
  Zap,
  Bell,
  Bot,
  Filter,
} from "lucide-react"
import { Card, CardContent } from "@/components/ui/card"
import { Progress } from "@/components/ui/progress"
import { getConnectorLogoUrl } from "@/lib/api/mcp-connectors"
import { ConnectorLogo } from "@/components/connectors/ConnectorLogo"
import { formatDateTime } from "@/lib/utils"
// `stageDurationMs` lives in dagHelpers, which imports only the TYPE from this
// file — an `import type` is erased at compile time, so this is not a runtime cycle.
import { stageDurationMs } from "./dagHelpers"

// =============================================================================
// Types
// =============================================================================

export interface ExecutionPlanStage {
  id: string
  display_name: string
  description?: string
  icon?: string
  status: string // pending|running|complete|failed|waiting
  progress?: number
  started_at?: string
  completed_at?: string
  /**
   * Milliseconds. The canonical duration — read it through `stageDurationMs()`
   * in `dagHelpers`, never directly, so legacy rows are handled in one place.
   */
  actual_duration_ms?: number
  /**
   * SECONDS, legacy. The temporal-adapter wrote this field in seconds while six
   * of the seven readers here formatted it as milliseconds, so a 42 s stage
   * rendered "42ms". Still present because plan JSON already persisted in
   * `execution_plans` carries the old unit. Do NOT add readers.
   */
  actual_duration?: number
  /**
   * Free prose the backend writes for a human to read — today always one of
   * "Plan created" / "Plan validated" / "Pipeline executed", documented on the
   * writing struct as `"2-step plan created"`
   * (`types/execution_plan.go:55`). Render it; never parse a number out of it.
   * This comment used to read `e.g. "12,450 rows transferred"`, which described
   * no value the backend has ever sent, and a helper was written against it —
   * see the note at the top of `dagHelpers.ts`.
   */
  result_summary?: string
  error_message?: string
  node_kind?: string // For DAG: source, destination, transform, etc.
  dependencies?: string[] // For DAG: parent node IDs
  metadata?: Record<string, any> // Additional backend metadata (node_config, etc.)
}

export interface ExecutionPlan {
  pipeline_id: string
  workflow_id?: string
  stages?: ExecutionPlanStage[]
  mode?: string
  created_at?: string
  estimated_time?: number
  metadata?: {
    is_dag?: boolean
    node_count?: number
    edge_count?: number
    graph_id?: string
    [key: string]: any
  }
}

export interface DAGNode {
  id: string
  displayName: string
  description?: string
  kind: string
  status: string
  connectorType?: string
  x: number
  y: number
  dependencies: string[]
  progress?: number
  errorMessage?: string
  resultSummary?: string
  actualDuration?: number
}

export interface DAGLayoutResult {
  nodes: DAGNode[]
  width: number
  height: number
}

// =============================================================================
// Status Configuration
// =============================================================================

export function getStageStatusConfig(status: string) {
  const configs: Record<string, { icon: React.ElementType; color: string; bgColor: string; borderColor: string }> = {
    complete: {
      icon: CheckCircle2,
      color: "text-green-600",
      bgColor: "bg-green-100 dark:bg-green-900/30",
      borderColor: "border-green-500",
    },
    running: {
      icon: Loader2,
      color: "text-blue-600",
      bgColor: "bg-blue-100 dark:bg-blue-900/30",
      borderColor: "border-blue-500",
    },
    failed: {
      icon: XCircle,
      color: "text-red-600",
      bgColor: "bg-red-100 dark:bg-red-900/30",
      borderColor: "border-red-500",
    },
    waiting: {
      icon: Clock,
      color: "text-amber-600",
      bgColor: "bg-amber-100 dark:bg-amber-900/30",
      borderColor: "border-amber-500",
    },
    pending: {
      icon: Clock,
      color: "text-zinc-400",
      bgColor: "bg-zinc-100 dark:bg-zinc-800",
      borderColor: "border-zinc-300",
    },
  }
  return configs[status] || configs.pending
}

export function getNodeKindIcon(kind: string) {
  const icons: Record<string, React.ElementType> = {
    source: Database,
    destination: Cloud,
    transform: Filter,
    notification: Bell,
    api_call: Zap,
    llm: Bot,
    condition: GitBranch,
    default: Circle,
  }
  return icons[kind] || icons.default
}

// Tailwind fill class for the kind accent bar (left side of node in SVG, border-left in timeline)
export function getNodeKindAccentFill(kind: string): string {
  const map: Record<string, string> = {
    source: "fill-blue-400",
    destination: "fill-emerald-400",
    transform: "fill-amber-400",
    llm: "fill-violet-400",
    condition: "fill-orange-400",
    api_call: "fill-indigo-400",
  }
  return map[kind] || "fill-zinc-300 dark:fill-zinc-600"
}

// Tailwind border-l class for timeline cards
export function getNodeKindBorderColor(kind: string): string {
  const map: Record<string, string> = {
    source: "border-l-blue-400",
    destination: "border-l-emerald-400",
    transform: "border-l-amber-400",
    llm: "border-l-violet-400",
    condition: "border-l-orange-400",
    api_call: "border-l-indigo-400",
  }
  return map[kind] || ""
}

// Tailwind text color class for kind label in timeline
export function getNodeKindTextColor(kind: string): string {
  const map: Record<string, string> = {
    source: "text-blue-600 dark:text-blue-400",
    destination: "text-emerald-600 dark:text-emerald-400",
    transform: "text-amber-600 dark:text-amber-400",
    llm: "text-violet-600 dark:text-violet-400",
    condition: "text-orange-600 dark:text-orange-400",
    api_call: "text-indigo-600 dark:text-indigo-400",
  }
  return map[kind] || "text-zinc-500"
}

export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  const m = Math.floor(ms / 60000)
  const s = Math.round((ms % 60000) / 1000)
  return s > 0 ? `${m}m ${s}s` : `${m}m`
}

// =============================================================================
// Layout Functions
// =============================================================================

function synthesizeSequentialDependencies(stages: ExecutionPlanStage[]): ExecutionPlanStage[] {
  if (!Array.isArray(stages) || stages.length === 0) return []

  const hasAnyDeps = stages.some((s) => Array.isArray(s.dependencies) && s.dependencies.length > 0)
  if (hasAnyDeps) return stages

  // If the backend doesn't provide dependencies, infer a simple sequential chain
  // based on the stage order. This enables a graph view for "linear" plans.
  return stages.map((s, idx) => {
    if (idx === 0) return { ...s, dependencies: [] }
    return { ...s, dependencies: [stages[idx - 1].id] }
  })
}

export function layoutDAG(stages: ExecutionPlanStage[]): DAGLayoutResult {
  if (!stages || stages.length === 0) {
    return { nodes: [], width: 0, height: 0 }
  }

  const preparedStages = synthesizeSequentialDependencies(stages)

  // Build adjacency for dependency analysis
  const nodeMap = new Map<string, ExecutionPlanStage>()
  preparedStages.forEach((s) => nodeMap.set(s.id, s))

  // Compute levels (topological layers)
  const levels: string[][] = []
  const assigned = new Set<string>()

  // Find root nodes (no dependencies)
  const rootNodes = preparedStages.filter((s) => !s.dependencies || s.dependencies.length === 0)
  if (rootNodes.length > 0) {
    levels.push(rootNodes.map((n) => n.id))
    rootNodes.forEach((n) => assigned.add(n.id))
  }

  // Assign remaining nodes to levels based on dependencies
  let maxIterations = preparedStages.length
  while (assigned.size < preparedStages.length && maxIterations > 0) {
    maxIterations--
    const nextLevel: string[] = []

    for (const stage of preparedStages) {
      if (assigned.has(stage.id)) continue

      const deps = stage.dependencies || []
      const allDepsAssigned = deps.every((d) => assigned.has(d))

      if (allDepsAssigned) {
        nextLevel.push(stage.id)
      }
    }

    if (nextLevel.length === 0) break

    levels.push(nextLevel)
    nextLevel.forEach((id) => assigned.add(id))
  }

  // Add any remaining unassigned nodes to last level
  for (const stage of preparedStages) {
    if (!assigned.has(stage.id)) {
      if (levels.length === 0) {
        levels.push([])
      }
      levels[levels.length - 1].push(stage.id)
    }
  }

  // Layout constants
  const nodeWidth = 200
  const nodeHeight = 96
  const horizontalGap = 80
  const verticalGap = 110

  // Position nodes
  const nodes: DAGNode[] = []
  let maxWidth = 0

  for (let levelIdx = 0; levelIdx < levels.length; levelIdx++) {
    const level = levels[levelIdx]
    const levelWidth = level.length * nodeWidth + (level.length - 1) * horizontalGap
    const startX = (levelWidth > maxWidth ? 0 : (maxWidth - levelWidth) / 2)

    for (let nodeIdx = 0; nodeIdx < level.length; nodeIdx++) {
      const nodeId = level[nodeIdx]
      const stage = nodeMap.get(nodeId)
      if (!stage) continue

      nodes.push({
        id: stage.id,
        displayName: stage.display_name,
        description: stage.description,
        kind: stage.node_kind || "default",
        status: stage.status,
        connectorType:
          (stage.metadata?.resolved_connector_type as string) ||
          (stage.metadata?.node_config?.connector_type as string) ||
          undefined,
        x: startX + nodeIdx * (nodeWidth + horizontalGap),
        y: levelIdx * (nodeHeight + verticalGap),
        dependencies: stage.dependencies || [],
        progress: stage.progress,
        errorMessage: stage.error_message,
        resultSummary: stage.result_summary || undefined,
        actualDuration: stageDurationMs(stage) || undefined,
      })
    }

    maxWidth = Math.max(maxWidth, levelWidth)
  }

  const height = levels.length * nodeHeight + (levels.length - 1) * verticalGap
  return { nodes, width: maxWidth, height }
}

// =============================================================================
// DAG Visualization Component
// =============================================================================

export interface DAGVisualizationProps {
  stages: ExecutionPlanStage[]
  compact?: boolean // For chat panel - uses smaller sizing
  onNodeClick?: (nodeId: string) => void
  selectedNodeId?: string | null
}

export function DAGVisualization({ stages, compact = false, onNodeClick, selectedNodeId }: DAGVisualizationProps) {
  const layout = useMemo(() => layoutDAG(stages), [stages])

  if (layout.nodes.length === 0) {
    return (
      <div className="flex items-center justify-center py-12 text-zinc-500">
        No DAG nodes to display
      </div>
    )
  }

  // Sizing adjustments for compact mode
  const nodeWidth = compact ? 160 : 200
  const nodeHeight = compact ? 76 : 96
  const padding = compact ? 24 : 40
  const scale = compact ? 0.8 : 1

  const svgWidth = (layout.width + nodeWidth + padding * 2) * scale
  const svgHeight = (layout.height + nodeHeight + padding * 2) * scale

  // Create node lookup by id
  const nodeById = new Map<string, DAGNode>()
  layout.nodes.forEach((n) => nodeById.set(n.id, n))

  return (
    <div className="overflow-auto">
      <svg
        width={svgWidth}
        height={svgHeight}
        className="min-w-full"
        viewBox={`0 0 ${svgWidth} ${svgHeight}`}
      >
        <g transform={`scale(${scale})`}>
          {/* Edges */}
          <g className="edges">
            {layout.nodes.map((node) =>
              node.dependencies.map((depId) => {
                const parent = nodeById.get(depId)
                if (!parent) return null

                const x1 = padding + parent.x + nodeWidth / 2
                const y1 = padding + parent.y + nodeHeight
                const x2 = padding + node.x + nodeWidth / 2
                const y2 = padding + node.y

                // Calculate curved path
                const midY = (y1 + y2) / 2
                const path = `M ${x1} ${y1} C ${x1} ${midY}, ${x2} ${midY}, ${x2} ${y2}`

                return (
                  <g key={`edge-${depId}-${node.id}`}>
                    <path
                      d={path}
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                      className="text-zinc-300 dark:text-zinc-600"
                      markerEnd="url(#arrowhead)"
                    />
                  </g>
                )
              })
            )}
          </g>

          {/* Arrow marker definition */}
          <defs>
            <marker
              id="arrowhead"
              markerWidth="10"
              markerHeight="7"
              refX="9"
              refY="3.5"
              orient="auto"
            >
              <polygon
                points="0 0, 10 3.5, 0 7"
                className="fill-zinc-400 dark:fill-zinc-500"
              />
            </marker>
          </defs>

          {/* Nodes */}
          <g className="nodes">
            {layout.nodes.map((node) => {
              const config = getStageStatusConfig(node.status)
              const KindIcon = getNodeKindIcon(node.kind)
              const isSelected = selectedNodeId === node.id
              const isClickable = !!onNodeClick

              const kindAccentFill = getNodeKindAccentFill(node.kind)
              const hasDuration = typeof node.actualDuration === "number" && node.actualDuration > 0
              const hasResult = !!(node.resultSummary && (node.status === "complete" || node.status === "failed"))

              return (
                <g
                  key={node.id}
                  transform={`translate(${padding + node.x}, ${padding + node.y})`}
                  onClick={() => onNodeClick?.(node.id)}
                  style={{ cursor: isClickable ? "pointer" : "default" }}
                  className={isClickable ? "hover:opacity-90 transition-opacity" : ""}
                >
                  {/* Selection highlight */}
                  {isSelected && (
                    <rect
                      x="-4"
                      y="-4"
                      width={nodeWidth + 8}
                      height={nodeHeight + 8}
                      rx="12"
                      className="fill-violet-100 dark:fill-violet-900/50"
                    />
                  )}

                  {/* Node background */}
                  <rect
                    width={nodeWidth}
                    height={nodeHeight}
                    rx="8"
                    className={`fill-white dark:fill-zinc-800 stroke-2 ${isSelected ? "stroke-violet-500" : config.borderColor}`}
                    stroke="currentColor"
                  />

                  {/* Node kind accent bar (left edge, 4px) */}
                  <rect
                    width={4}
                    height={nodeHeight - 4}
                    rx="2"
                    x={0}
                    y={0}
                    className={kindAccentFill}
                  />

                  {/* Status indicator bar (bottom) */}
                  <rect
                    width={nodeWidth}
                    height="4"
                    rx="2"
                    y={nodeHeight - 4}
                    className={config.bgColor.replace("bg-", "fill-")}
                  />

                  {/* Icon — fixed at top-left area */}
                  <g transform="translate(12, 16)">
                    <circle r="16" cx="12" cy="12" className={`${config.bgColor}`} />
                    {/* Prefer connector logo for source/destination nodes */}
                    {node.connectorType && (node.kind === "source" || node.kind === "destination") ? (
                      <>
                        <defs>
                          <clipPath id={`clip-${node.id}`}>
                            <circle cx="12" cy="12" r="12" />
                          </clipPath>
                        </defs>
                        <image
                          href={getConnectorLogoUrl(node.connectorType)}
                          x="0"
                          y="0"
                          width="24"
                          height="24"
                          preserveAspectRatio="xMidYMid meet"
                          clipPath={`url(#clip-${node.id})`}
                        />
                      </>
                    ) : (
                      <foreignObject width="24" height="24">
                        <div className="flex items-center justify-center w-6 h-6">
                          <KindIcon className={`w-4 h-4 ${config.color}`} />
                        </div>
                      </foreignObject>
                    )}
                  </g>

                  {/* Node label */}
                  <text
                    x="48"
                    y={compact ? 24 : 28}
                    className="fill-zinc-900 dark:fill-white font-medium"
                    style={{ fontSize: compact ? "11px" : "12px" }}
                  >
                    {node.displayName.length > (compact ? 16 : 20)
                      ? node.displayName.substring(0, compact ? 14 : 18) + "..."
                      : node.displayName}
                  </text>

                  {/* Description / status line */}
                  <text
                    x="48"
                    y={compact ? 38 : 44}
                    className="fill-zinc-500 dark:fill-zinc-400"
                    style={{ fontSize: compact ? "9px" : "10px" }}
                  >
                    {(() => {
                      const src = node.description || (node.status.charAt(0).toUpperCase() + node.status.slice(1))
                      const maxChars = compact ? 18 : 24
                      return src.length > maxChars ? src.substring(0, maxChars - 1) + "…" : src
                    })()}
                  </text>

                  {/* Result summary (complete/failed only) */}
                  {!compact && hasResult && (
                    <text
                      x="48"
                      y={58}
                      className="fill-zinc-400 dark:fill-zinc-500"
                      style={{ fontSize: "9px" }}
                    >
                      {node.resultSummary!.length > 26
                        ? node.resultSummary!.substring(0, 24) + "…"
                        : node.resultSummary}
                    </text>
                  )}

                  {/* Progress bar for running nodes */}
                  {node.status === "running" && typeof node.progress === "number" && (
                    <g transform={`translate(48, ${compact ? 46 : 58})`}>
                      <rect
                        width={nodeWidth - 60}
                        height="4"
                        rx="2"
                        className="fill-zinc-200 dark:fill-zinc-700"
                      />
                      <rect
                        width={(nodeWidth - 60) * (node.progress / 100)}
                        height="4"
                        rx="2"
                        className="fill-blue-500"
                      />
                    </g>
                  )}

                  {/* Duration badge — top right corner */}
                  {hasDuration && (node.status === "complete" || node.status === "failed") && (
                    <text
                      x={nodeWidth - 8}
                      y={14}
                      textAnchor="end"
                      className="fill-zinc-400 dark:fill-zinc-500"
                      style={{ fontSize: "9px" }}
                    >
                      {formatDuration(node.actualDuration!)}
                    </text>
                  )}

                  {/* Running spinner indicator */}
                  {node.status === "running" && (
                    <g transform={`translate(${nodeWidth - 24}, 12)`}>
                      <circle
                        r="8"
                        cx="8"
                        cy="8"
                        className="fill-blue-100 dark:fill-blue-900"
                      />
                      <foreignObject width="16" height="16">
                        <div className="flex items-center justify-center w-4 h-4">
                          <Loader2 className="w-3 h-3 text-blue-600 animate-spin" />
                        </div>
                      </foreignObject>
                    </g>
                  )}

                  {/* Error indicator */}
                  {node.errorMessage && (
                    <g transform={`translate(${nodeWidth - 24}, 12)`}>
                      <circle r="8" cx="8" cy="8" className="fill-red-100" />
                      <foreignObject width="16" height="16">
                        <div className="flex items-center justify-center w-4 h-4">
                          <XCircle className="w-3 h-3 text-red-600" />
                        </div>
                      </foreignObject>
                    </g>
                  )}
                </g>
              )
            })}
          </g>
        </g>
      </svg>
    </div>
  )
}

// =============================================================================
// Linear Timeline Component
// =============================================================================

export interface LinearTimelineProps {
  stages: ExecutionPlanStage[]
  compact?: boolean
  onStageClick?: (stageId: string) => void
  selectedStageId?: string | null
}

export function LinearTimeline({ stages, compact = false, onStageClick, selectedStageId }: LinearTimelineProps) {
  return (
    <div className={compact ? "space-y-2" : "space-y-4"}>
      {stages.map((stage, index) => {
        const config = getStageStatusConfig(stage.status)
        const StatusIcon = config.icon
        const isLast = index === stages.length - 1
        const nodeKind = String(stage.node_kind || stage.metadata?.node_kind || "").toLowerCase()
        const connectorType =
          (stage.metadata?.resolved_connector_type as string) ||
          (stage.metadata?.node_config?.connector_type as string) ||
          undefined
        const showConnectorLogo =
          (nodeKind === "source" || nodeKind === "destination") && connectorType && connectorType !== "storage"
        const isSelected = selectedStageId === stage.id
        const isClickable = !!onStageClick
        // Kind takes priority for left border when available; status otherwise
        const kindBorder = getNodeKindBorderColor(nodeKind)
        const borderClass = kindBorder || config.bgColor.replace("bg-", "border-l-")
        const kindTextColor = getNodeKindTextColor(nodeKind)
        const hasResult = !!(stage.result_summary && (stage.status === "complete" || stage.status === "failed"))
        const durationMs = stageDurationMs(stage)

        return (
          <div key={stage.id}>
            <Card
              className={`border-l-4 ${borderClass} ${isSelected ? "ring-2 ring-violet-500" : ""} ${isClickable ? "cursor-pointer hover:shadow-md transition-shadow" : ""}`}
              onClick={() => onStageClick?.(stage.id)}
            >
              <CardContent className={compact ? "p-3" : "p-4"}>
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-4 min-w-0">
                    <div className={`rounded-full ${compact ? "p-2" : "p-3"} ${config.bgColor} shrink-0`}>
                      <StatusIcon
                        className={`${compact ? "h-4 w-4" : "h-5 w-5"} ${config.color} ${stage.status === "running" ? "animate-spin" : ""}`}
                      />
                    </div>
                    <div className="min-w-0">
                      <div className={`font-semibold text-zinc-900 dark:text-white flex items-center gap-2 ${compact ? "text-sm" : ""}`}>
                        {showConnectorLogo ? (
                          <ConnectorLogo
                            connectorType={connectorType!}
                            logoUrl={getConnectorLogoUrl(connectorType!)}
                            size="sm"
                            className="bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800"
                          />
                        ) : null}
                        <span>
                          {stage.icon} {stage.display_name}
                        </span>
                        {nodeKind && nodeKind !== "default" && !compact && (
                          <span className={`text-xs font-normal ${kindTextColor}`}>
                            {nodeKind}
                          </span>
                        )}
                      </div>
                      {!compact && stage.description && (
                        <div className="text-sm text-zinc-600 dark:text-zinc-400 mt-1">
                          {stage.description}
                        </div>
                      )}
                      {/* Result summary — most useful data from Temporal */}
                      {hasResult && (
                        <div className={`${compact ? "text-xs mt-1" : "text-sm mt-1"} text-zinc-500 dark:text-zinc-400`}>
                          {stage.result_summary}
                        </div>
                      )}
                      {stage.error_message && (
                        <div className={`text-red-600 dark:text-red-400 ${compact ? "text-xs mt-1" : "text-sm mt-1"}`}>
                          {stage.error_message}
                        </div>
                      )}
                    </div>
                  </div>

                  <div className={`text-right shrink-0 ml-4 ${compact ? "space-y-1" : "space-y-2"}`}>
                    {typeof stage.progress === "number" && stage.progress > 0 && (
                      <div className="flex items-center gap-2 justify-end">
                        <Progress value={stage.progress} className="w-24" />
                        <span className="text-sm text-zinc-600">{stage.progress}%</span>
                      </div>
                    )}
                    {/* Duration pill */}
                    {durationMs !== null && durationMs > 0 && (
                      <div className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 ${compact ? "text-xs" : "text-xs"} font-medium bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400`}>
                        <Clock className="h-3 w-3" />
                        {formatDuration(durationMs)}
                      </div>
                    )}
                    {!compact && stage.started_at && (
                      <div className="text-xs text-zinc-400">
                        {formatDateTime(new Date(stage.started_at))}
                      </div>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Connector arrow */}
            {!isLast && (
              <div className={`flex justify-center ${compact ? "py-1" : "py-2"}`}>
                <ArrowDown className={`${compact ? "h-4 w-4" : "h-5 w-5"} text-zinc-400`} />
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}
