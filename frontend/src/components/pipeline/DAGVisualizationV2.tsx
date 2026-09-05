"use client"

import { useEffect } from "react"
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  Handle,
  Position,
  type Node,
  type Edge,
  type EdgeTypes,
  type NodeProps,
  type BuiltInNode,
  BackgroundVariant,
  MarkerType,
  Panel,
} from "@xyflow/react"
import { AnimatedEdge, type AnimatedEdgeData } from "./AnimatedEdge"
import {
  isThinkingKind,
  detectDurationAnomaly,
  stageDurationMs,
} from "./dagHelpers"
import { AlertTriangle } from "lucide-react"
// @ts-ignore — dagre has no perfect types for this import style
import dagre from "dagre"
import {
  CheckCircle2,
  XCircle,
  Loader2,
  Clock,
  MinusCircle,
  Database,
  Cloud,
  Zap,
  Bell,
  Bot,
  GitBranch,
  Filter,
  Circle,
  ServerCog,
} from "lucide-react"
import { cn } from "@/lib/utils"
import { getConnectorLogoUrl } from "@/lib/api/mcp-connectors"
import type { ExecutionPlanStage } from "./DAGVisualization"
import { formatDuration } from "./DAGVisualization"

// ─── Types ───────────────────────────────────────────────────────────────────

export type StageNodeData = {
  stage: ExecutionPlanStage
  connectorType?: string
  anyRunning: boolean
  isSelected: boolean
  anomalyRatio: number | null
  [key: string]: unknown
}

export type StageNode = Node<StageNodeData, "stageNode">

// ─── Helpers ─────────────────────────────────────────────────────────────────

function kindAccentClass(kind: string): string {
  const map: Record<string, string> = {
    source: "bg-blue-400",
    destination: "bg-emerald-400",
    transform: "bg-amber-400",
    llm: "bg-violet-500",
    condition: "bg-orange-400",
    api_call: "bg-indigo-400",
    notification: "bg-pink-400",
    infra_preflight: "bg-cyan-400",
  }
  return map[kind] ?? "bg-zinc-300 dark:bg-zinc-600"
}

function statusBorderClass(status: string): string {
  const map: Record<string, string> = {
    complete: "border-green-400 dark:border-green-500",
    running: "border-blue-400 dark:border-blue-500",
    failed: "border-red-400 dark:border-red-500",
    waiting: "border-amber-400 dark:border-amber-500",
    skipped: "border-zinc-300 dark:border-zinc-600",
    pending: "border-zinc-200 dark:border-zinc-700",
  }
  return map[status] ?? "border-zinc-200 dark:border-zinc-700"
}

function statusBottomClass(status: string): string {
  const map: Record<string, string> = {
    complete: "bg-green-400",
    running: "bg-blue-400",
    failed: "bg-red-400",
    waiting: "bg-amber-400",
    skipped: "bg-zinc-300 dark:bg-zinc-600",
    pending: "bg-zinc-200 dark:bg-zinc-700",
  }
  return map[status] ?? "bg-zinc-200"
}

function statusIconClass(status: string): string {
  const map: Record<string, string> = {
    complete: "text-green-500",
    running: "text-blue-500",
    failed: "text-red-500",
    waiting: "text-amber-500",
    skipped: "text-zinc-400",
    pending: "text-zinc-400",
  }
  return map[status] ?? "text-zinc-400"
}

function KindIcon({ kind, className }: { kind: string; className?: string }) {
  const icons: Record<string, React.ElementType> = {
    source: Database,
    destination: Cloud,
    transform: Filter,
    notification: Bell,
    api_call: Zap,
    llm: Bot,
    condition: GitBranch,
    infra_preflight: ServerCog,
  }
  const Icon = icons[kind] ?? Circle
  return <Icon className={className} />
}

function StatusIcon({ status, className }: { status: string; className?: string }) {
  if (status === "complete") return <CheckCircle2 className={className} />
  if (status === "failed") return <XCircle className={className} />
  if (status === "running") return <Loader2 className={cn(className, "animate-spin")} />
  // A skipped step (e.g. "Generating Connectors" when connectors already exist)
  // is neither pending nor running — a plain clock reads as "stuck". Show a
  // distinct muted MinusCircle so it's clearly intentionally-skipped.
  if (status === "skipped") return <MinusCircle className={cn(className, "text-zinc-400")} />
  return <Clock className={className} />
}

// ─── Node dimensions (must match dagre layout) ───────────────────────────────

const NODE_WIDTH = 230
const NODE_HEIGHT = 108

// ─── Custom Stage Node ────────────────────────────────────────────────────────

function StageNodeComponent({ data }: NodeProps<StageNode>) {
  const { stage, connectorType, isSelected, anomalyRatio } = data as StageNodeData
  const kind = (stage.node_kind ?? "default").toLowerCase()
  const hasConnectorLogo =
    connectorType && (kind === "source" || kind === "destination")
  const durationMs = stageDurationMs(stage)
  const hasResult =
    !!stage.result_summary &&
    (stage.status === "complete" || stage.status === "failed")
  const isRunning = stage.status === "running"
  const showThinking = isRunning && isThinkingKind(kind)

  return (
    <div
      className={cn(
        "relative rounded-lg border-2 bg-white dark:bg-zinc-900 shadow-md overflow-hidden select-none transition-all cursor-pointer",
        "hover:shadow-lg hover:-translate-y-0.5",
        statusBorderClass(stage.status),
        stage.status === "running" && "shadow-blue-100 dark:shadow-blue-900/30 dag-running-pulse",
        isSelected && "ring-2 ring-violet-500 ring-offset-2 dark:ring-offset-zinc-950",
      )}
      style={{ width: NODE_WIDTH, minHeight: NODE_HEIGHT }}
    >
      {/* Thinking shimmer overlay (LLM/planner running) */}
      {showThinking && (
        <div className="absolute inset-0 pointer-events-none dag-thinking-shimmer" />
      )}
      {/* Anomaly badge (top-right corner) */}
      {anomalyRatio != null && (
        <div
          className="absolute -top-2 -right-2 z-10 flex items-center gap-0.5 text-[10px] font-semibold px-1.5 py-0.5 rounded-full bg-amber-100 dark:bg-amber-950/60 border border-amber-400 text-amber-800 dark:text-amber-300 shadow"
          title={`Duration ${anomalyRatio.toFixed(1)}× peer median`}
        >
          <AlertTriangle className="h-3 w-3" />
          {anomalyRatio.toFixed(1)}×
        </div>
      )}
      {/* Invisible handles — required by ReactFlow to anchor edge endpoints */}
      <Handle
        type="target"
        position={Position.Top}
        style={{ opacity: 0, pointerEvents: "none", width: 1, height: 1 }}
      />
      <Handle
        type="source"
        position={Position.Bottom}
        style={{ opacity: 0, pointerEvents: "none", width: 1, height: 1 }}
      />
      {/* Left kind accent bar */}
      <div
        className={cn(
          "absolute left-0 top-0 bottom-0 w-[3px] rounded-l",
          kindAccentClass(kind),
        )}
      />

      {/* Content */}
      <div className="pl-4 pr-3 pt-3 pb-2">
        {/* Header row */}
        <div className="flex items-start justify-between gap-2">
          <div className="flex items-center gap-2 min-w-0">
            {/* Icon / logo */}
            <div className="shrink-0 w-7 h-7 rounded-md flex items-center justify-center bg-zinc-100 dark:bg-zinc-800 overflow-hidden">
              {hasConnectorLogo ? (
                <img
                  src={getConnectorLogoUrl(connectorType!)}
                  alt={connectorType}
                  className="w-5 h-5 object-contain"
                />
              ) : (
                <KindIcon
                  kind={kind}
                  className={cn("w-4 h-4", statusIconClass(stage.status))}
                />
              )}
            </div>
            {/* Display name */}
            <span className="text-[13px] font-semibold text-zinc-900 dark:text-zinc-100 leading-tight truncate max-w-[130px]">
              {stage.display_name}
            </span>
          </div>

          {/* Duration or running spinner */}
          <div className="shrink-0">
            {stage.status === "running" ? (
              <Loader2 className="h-4 w-4 text-blue-500 animate-spin" />
            ) : durationMs !== null && durationMs > 0 && (stage.status === "complete" || stage.status === "failed") ? (
              <span className="text-[10px] font-mono text-zinc-400 bg-zinc-100 dark:bg-zinc-800 px-1.5 py-0.5 rounded">
                {formatDuration(durationMs)}
              </span>
            ) : null}
          </div>
        </div>

        {/* Description */}
        {stage.description && (
          <p className="text-[11px] text-zinc-500 dark:text-zinc-400 mt-1.5 leading-tight line-clamp-1">
            {stage.description}
          </p>
        )}

        {/* Result summary. A big animated row count used to sit above this and
            suppress it, on a number parsed out of these same words; the parse
            never succeeded, so this branch is what every completed stage has
            always rendered. See the note atop `dagHelpers.ts`. */}
        {hasResult && (
          <p className="text-[11px] text-zinc-700 dark:text-zinc-300 mt-1 font-medium leading-tight line-clamp-1">
            {stage.result_summary}
          </p>
        )}

        {/* Error */}
        {stage.error_message && (
          <p className="text-[11px] text-red-600 dark:text-red-400 mt-1 leading-tight line-clamp-1">
            {stage.error_message}
          </p>
        )}

        {/* Progress bar (running only) */}
        {stage.status === "running" && typeof stage.progress === "number" && stage.progress > 0 && (
          <div className="mt-2 h-1 bg-zinc-200 dark:bg-zinc-700 rounded-full overflow-hidden">
            <div
              className="h-full bg-blue-500 rounded-full transition-all duration-500"
              style={{ width: `${stage.progress}%` }}
            />
          </div>
        )}
      </div>

      {/* Status bottom bar */}
      <div className={cn("h-[3px] w-full", statusBottomClass(stage.status))} />
    </div>
  )
}

const nodeTypes: Record<string, React.ComponentType<NodeProps<BuiltInNode | StageNode>>> = {
  stageNode: StageNodeComponent as React.ComponentType<NodeProps<BuiltInNode | StageNode>>,
}

// ─── Layout with dagre ────────────────────────────────────────────────────────

function synthesizeSequential(stages: ExecutionPlanStage[]): ExecutionPlanStage[] {
  const hasAnyDeps = stages.some(
    (s) => Array.isArray(s.dependencies) && s.dependencies.length > 0,
  )
  if (hasAnyDeps) return stages
  return stages.map((s, i) => ({
    ...s,
    dependencies: i === 0 ? [] : [stages[i - 1].id],
  }))
}

function buildElements(
  stages: ExecutionPlanStage[],
  selectedStageId?: string | null,
): { nodes: StageNode[]; edges: Edge[] } {
  if (!stages.length) return { nodes: [], edges: [] }

  const prepared = synthesizeSequential(stages)
  const anyRunning = prepared.some((s) => s.status === "running")

  // dagre layout
  const g = new dagre.graphlib.Graph()
  g.setDefaultEdgeLabel(() => ({}))
  g.setGraph({ rankdir: "TB", nodesep: 60, ranksep: 70, marginx: 50, marginy: 50 })

  prepared.forEach((s) => g.setNode(s.id, { width: NODE_WIDTH, height: NODE_HEIGHT }))
  prepared.forEach((s) => {
    ;(s.dependencies ?? []).forEach((dep) => g.setEdge(dep, s.id))
  })

  dagre.layout(g)

  const nodes: StageNode[] = prepared.map((s) => {
    const pos = g.node(s.id)
    const connectorType =
      (s.metadata?.resolved_connector_type as string | undefined) ||
      (s.metadata?.node_config?.connector_type as string | undefined) ||
      undefined
    const anomalyRatio = detectDurationAnomaly(s, prepared)

    return {
      id: s.id,
      type: "stageNode" as const,
      position: { x: pos.x - NODE_WIDTH / 2, y: pos.y - NODE_HEIGHT / 2 },
      data: {
        stage: s,
        connectorType,
        anyRunning,
        isSelected: s.id === selectedStageId,
        anomalyRatio,
      } as StageNodeData,
      draggable: false,
    } satisfies StageNode
  })

  const edges: Edge[] = []
  prepared.forEach((s) => {
    ;(s.dependencies ?? []).forEach((dep) => {
      const sourceStage = prepared.find((p) => p.id === dep)
      const sourceKind = (sourceStage?.node_kind ?? "default").toLowerCase()
      const color =
        sourceKind === "source"
          ? "#60a5fa"
          : sourceKind === "destination"
            ? "#34d399"
            : sourceKind === "llm"
              ? "#a78bfa"
              : sourceKind === "transform"
                ? "#fbbf24"
                : "#94a3b8"

      const sourceRunning = sourceStage?.status === "running"
      const targetRunning = s.status === "running" || s.status === "waiting"
      const edgeActive = sourceRunning || targetRunning || (anyRunning && s.status === "pending")

      // Pull output_schema from source stage metadata (populated by executor on STAGE_COMPLETED)
      const rawSchema = sourceStage?.metadata?.output_schema
      const outputSchema = Array.isArray(rawSchema)
        ? (rawSchema as Array<{ name: string; type: string; nullable?: boolean }>)
        : null

      edges.push({
        id: `${dep}->${s.id}`,
        source: dep,
        target: s.id,
        type: "animated",
        data: {
          active: edgeActive,
          color,
          sourceLabel: sourceStage?.display_name,
          targetLabel: s.display_name,
          sourceStatus: sourceStage?.status,
          sourceDuration: sourceStage ? stageDurationMs(sourceStage) : null,
          outputSchema,
        } as AnimatedEdgeData,
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color,
        },
      })
    })
  })

  return { nodes, edges }
}

const edgeTypes: EdgeTypes = {
  animated: AnimatedEdge,
}

// ─── Main component (inner — needs ReactFlowProvider above) ──────────────────

function DAGFlowInner({
  stages,
  selectedStageId,
  onStageClick,
}: {
  stages: ExecutionPlanStage[]
  selectedStageId?: string | null
  onStageClick?: (id: string) => void
}) {
  const [nodes, setNodes, onNodesChange] = useNodesState<StageNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])

  // Sync nodes+edges whenever stage statuses or selection change
  const stagesKey = stages.map((s) => `${s.id}:${s.status}:${s.progress ?? ""}:${s.result_summary ?? ""}`).join("|")
  useEffect(() => {
    const { nodes: n, edges: e } = buildElements(stages, selectedStageId)
    setNodes(n)
    setEdges(e)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stagesKey, selectedStageId])

  const anyRunning = stages.some((s) => s.status === "running" || s.status === "waiting")

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onNodeClick={(_, node) => onStageClick?.(node.id)}
      nodeTypes={nodeTypes}
      edgeTypes={edgeTypes}
      fitView
      fitViewOptions={{ padding: 0.25, maxZoom: 1.2 }}
      minZoom={0.3}
      maxZoom={1.5}
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable={false}
      proOptions={{ hideAttribution: true }}
    >
      <Background
        variant={BackgroundVariant.Dots}
        gap={20}
        size={1}
        className="!bg-zinc-50 dark:!bg-zinc-950"
        color="currentColor"
      />
      <Controls
        showInteractive={false}
        className="!shadow-none !border !border-zinc-200 dark:!border-zinc-700 !bg-white dark:!bg-zinc-900 !rounded-lg overflow-hidden"
      />
      <MiniMap
        nodeColor={(node) => {
          const stage = (node.data as StageNodeData).stage
          if (stage.status === "complete") return "#4ade80"
          if (stage.status === "running") return "#60a5fa"
          if (stage.status === "failed") return "#f87171"
          if (stage.status === "waiting") return "#fbbf24"
          return "#d4d4d8"
        }}
        maskColor="rgba(0,0,0,0.04)"
        className="!bg-white dark:!bg-zinc-900 !border !border-zinc-200 dark:!border-zinc-700 !rounded-lg overflow-hidden"
        pannable
        zoomable
      />
      {anyRunning && (
        <Panel position="top-left">
          <div className="flex items-center gap-1.5 text-[11px] text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-950/40 border border-blue-200 dark:border-blue-800 px-2.5 py-1 rounded-full">
            <Loader2 className="h-3 w-3 animate-spin" />
            Live
          </div>
        </Panel>
      )}
    </ReactFlow>
  )
}

// ─── Public export ────────────────────────────────────────────────────────────

export interface DAGVisualizationV2Props {
  stages: ExecutionPlanStage[]
  selectedStageId?: string | null
  onStageClick?: (id: string) => void
}

export function DAGVisualizationV2({ stages, selectedStageId, onStageClick }: DAGVisualizationV2Props) {
  if (!stages.length) {
    return (
      <div className="flex items-center justify-center h-64 text-zinc-400 text-sm">
        No execution stages to display
      </div>
    )
  }

  return (
    <div className="w-full h-[540px] rounded-lg overflow-hidden border border-zinc-200 dark:border-zinc-800">
      <ReactFlowProvider>
        <DAGFlowInner stages={stages} selectedStageId={selectedStageId} onStageClick={onStageClick} />
      </ReactFlowProvider>
    </div>
  )
}
