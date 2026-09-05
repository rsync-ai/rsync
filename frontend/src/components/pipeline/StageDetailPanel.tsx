"use client"

import { useRouter } from "next/navigation"
import { X, Clock, AlertCircle, AlertTriangle, CheckCircle2, Loader2, XCircle, GitBranch, Database, Cloud, Bot, Zap, Bell, Filter, Circle, ServerCog, MessageSquare } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Progress } from "@/components/ui/progress"
import { ConnectorLogo } from "@/components/connectors/ConnectorLogo"
import { getConnectorLogoUrl } from "@/lib/api/mcp-connectors"
import { formatDateTime, cn } from "@/lib/utils"
import { formatDuration, type ExecutionPlanStage } from "./DAGVisualization"
import { detectDurationAnomaly, stageDurationMs } from "./dagHelpers"

const KIND_ICON: Record<string, React.ElementType> = {
  source: Database,
  destination: Cloud,
  transform: Filter,
  notification: Bell,
  api_call: Zap,
  llm: Bot,
  condition: GitBranch,
  infra_preflight: ServerCog,
}

const KIND_TEXT: Record<string, string> = {
  source: "text-blue-600 dark:text-blue-400",
  destination: "text-emerald-600 dark:text-emerald-400",
  transform: "text-amber-600 dark:text-amber-400",
  llm: "text-violet-600 dark:text-violet-400",
  condition: "text-orange-600 dark:text-orange-400",
  api_call: "text-indigo-600 dark:text-indigo-400",
  notification: "text-pink-600 dark:text-pink-400",
  infra_preflight: "text-cyan-600 dark:text-cyan-400",
}

const STATUS_BADGE: Record<string, { className: string; label: string; Icon: React.ElementType }> = {
  complete: { className: "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400 border-green-300", label: "Complete", Icon: CheckCircle2 },
  running: { className: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400 border-blue-300", label: "Running", Icon: Loader2 },
  failed: { className: "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400 border-red-300", label: "Failed", Icon: XCircle },
  waiting: { className: "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400 border-amber-300", label: "Waiting", Icon: Clock },
  pending: { className: "bg-zinc-100 text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400 border-zinc-300", label: "Pending", Icon: Clock },
}

function prettyKey(k: string): string {
  return k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
}

function isPrimitive(v: unknown): v is string | number | boolean {
  return typeof v === "string" || typeof v === "number" || typeof v === "boolean"
}

function MetadataRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-start justify-between gap-3 py-1.5 border-b border-zinc-100 dark:border-zinc-800 last:border-0">
      <span className="text-xs text-zinc-500 dark:text-zinc-400 shrink-0">{label}</span>
      <span className="text-xs text-zinc-900 dark:text-zinc-100 text-right break-all font-medium">{value}</span>
    </div>
  )
}

export interface StageDetailPanelProps {
  stage: ExecutionPlanStage | null
  allStages: ExecutionPlanStage[]
  onClose: () => void
  onSelectStage?: (id: string) => void
}

export function StageDetailPanel({ stage, allStages, onClose, onSelectStage }: StageDetailPanelProps) {
  const router = useRouter()

  if (!stage) return null

  const kind = (stage.node_kind ?? "default").toLowerCase()
  const anomalyRatio = detectDurationAnomaly(stage, allStages)

  const handleAskAboutNode = () => {
    const status = stage.status
    const ctx: string[] = [`stage "${stage.display_name}"`]
    if (stage.node_kind) ctx.push(`(kind: ${stage.node_kind})`)
    ctx.push(`is currently "${status}"`)
    if (stage.result_summary) ctx.push(`with result: "${stage.result_summary}"`)
    if (stage.error_message) ctx.push(`error: "${stage.error_message}"`)
    const seed = `In my pipeline, the ${ctx.join(" ")}. What does this stage do, what data is flowing through it, and is anything unusual?`
    const params = new URLSearchParams({ prompt: seed, autosend: "1" })
    router.push(`/chat?${params.toString()}`)
  }
  const KindIconCmp = KIND_ICON[kind] ?? Circle
  const status = (stage.status ?? "pending").toLowerCase()
  const statusInfo = STATUS_BADGE[status] ?? STATUS_BADGE.pending
  const StatusIcon = statusInfo.Icon

  const connectorType =
    (stage.metadata?.resolved_connector_type as string | undefined) ||
    (stage.metadata?.node_config?.connector_type as string | undefined)
  const showLogo = (kind === "source" || kind === "destination") && connectorType && connectorType !== "storage"

  const startedAt = stage.started_at ? new Date(stage.started_at) : null
  const completedAt = stage.completed_at ? new Date(stage.completed_at) : null
  const durationMs = stageDurationMs(stage)

  const dependencies = stage.dependencies ?? []
  const downstream = allStages.filter((s) => (s.dependencies ?? []).includes(stage.id))

  // Extract config-like fields from metadata
  const nodeConfig = (stage.metadata?.node_config ?? {}) as Record<string, unknown>
  const configEntries = Object.entries(nodeConfig).filter(([, v]) => isPrimitive(v))
  const otherMetadata = Object.entries(stage.metadata ?? {}).filter(
    ([k, v]) => isPrimitive(v) && !["node_kind", "resolved_connector_type"].includes(k),
  )

  return (
    <div className="w-full lg:w-[380px] shrink-0 lg:sticky lg:top-4 lg:self-start rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 flex flex-col max-h-[calc(100vh-6rem)] lg:max-h-[calc(100vh-2rem)] overflow-hidden">
      {/* Header */}
      <div className="flex items-start justify-between gap-3 p-4 border-b border-zinc-200 dark:border-zinc-800">
        <div className="flex items-start gap-3 min-w-0">
          <div className="shrink-0 w-9 h-9 rounded-md flex items-center justify-center bg-zinc-100 dark:bg-zinc-800 overflow-hidden">
            {showLogo ? (
              <ConnectorLogo
                connectorType={connectorType!}
                logoUrl={getConnectorLogoUrl(connectorType!)}
                size="sm"
                className="bg-white dark:bg-zinc-900"
              />
            ) : (
              <KindIconCmp className={cn("h-5 w-5", KIND_TEXT[kind] ?? "text-zinc-500")} />
            )}
          </div>
          <div className="min-w-0">
            <h4 className="text-sm font-semibold text-zinc-900 dark:text-white truncate">
              {stage.display_name}
            </h4>
            <div className="flex items-center gap-2 mt-1">
              <Badge variant="outline" className={cn("text-[10px] px-1.5 py-0 h-5 gap-1", statusInfo.className)}>
                <StatusIcon className={cn("h-3 w-3", status === "running" && "animate-spin")} />
                {statusInfo.label}
              </Badge>
              {kind && kind !== "default" && (
                <span className={cn("text-[10px] uppercase tracking-wide font-medium", KIND_TEXT[kind] ?? "text-zinc-500")}>
                  {kind}
                </span>
              )}
            </div>
          </div>
        </div>
        <button
          type="button"
          onClick={onClose}
          className="shrink-0 p-1 rounded hover:bg-zinc-100 dark:hover:bg-zinc-800 text-zinc-500"
          aria-label="Close"
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      {/* Body */}
      <div className="overflow-y-auto p-4 space-y-4 text-sm">
        {/* Description */}
        {stage.description && (
          <p className="text-xs text-zinc-600 dark:text-zinc-300 leading-relaxed">
            {stage.description}
          </p>
        )}

        {/* Anomaly callout */}
        {anomalyRatio != null && (
          <div className="rounded-md border border-amber-300 dark:border-amber-700 bg-amber-50 dark:bg-amber-950/30 p-3">
            <div className="text-[10px] uppercase tracking-wide text-amber-700 dark:text-amber-400 mb-1 flex items-center gap-1">
              <AlertTriangle className="h-3 w-3" />
              Duration anomaly
            </div>
            <p className="text-xs text-amber-800 dark:text-amber-200">
              This stage took <strong>{anomalyRatio.toFixed(1)}×</strong> the median for similar
              stages. Consider checking upstream load or downstream backpressure.
            </p>
          </div>
        )}

        {/* Progress */}
        {status === "running" && typeof stage.progress === "number" && stage.progress > 0 && (
          <div>
            <div className="flex items-center justify-between text-xs mb-1.5">
              <span className="text-zinc-500">Progress</span>
              <span className="font-medium text-blue-600">{stage.progress}%</span>
            </div>
            <Progress value={stage.progress} />
          </div>
        )}

        {/* Result */}
        {stage.result_summary && (status === "complete" || status === "failed") && (
          <div className="rounded-md border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-800/40 p-3">
            <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1 flex items-center gap-1">
              <CheckCircle2 className="h-3 w-3" />
              Result
            </div>
            <p className="text-xs text-zinc-800 dark:text-zinc-100 whitespace-pre-wrap break-words">
              {stage.result_summary}
            </p>
          </div>
        )}

        {/* Error */}
        {stage.error_message && (
          <div className="rounded-md border border-red-200 dark:border-red-900 bg-red-50 dark:bg-red-950/30 p-3">
            <div className="text-[10px] uppercase tracking-wide text-red-600 dark:text-red-400 mb-1 flex items-center gap-1">
              <AlertCircle className="h-3 w-3" />
              Error
            </div>
            <p className="text-xs text-red-700 dark:text-red-300 whitespace-pre-wrap break-words">
              {stage.error_message}
            </p>
          </div>
        )}

        {/* Timing */}
        {(startedAt || completedAt || durationMs !== null) && (
          <div>
            <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1.5">Timing</div>
            <div className="rounded-md border border-zinc-200 dark:border-zinc-800 px-3">
              {startedAt && <MetadataRow label="Started" value={formatDateTime(startedAt)} />}
              {completedAt && <MetadataRow label="Completed" value={formatDateTime(completedAt)} />}
              {durationMs !== null && durationMs > 0 && (
                <MetadataRow
                  label="Duration"
                  value={
                    <span className="inline-flex items-center gap-1">
                      <Clock className="h-3 w-3" />
                      {formatDuration(durationMs)}
                    </span>
                  }
                />
              )}
            </div>
          </div>
        )}

        {/* Configuration */}
        {(configEntries.length > 0 || connectorType) && (
          <div>
            <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1.5">Configuration</div>
            <div className="rounded-md border border-zinc-200 dark:border-zinc-800 px-3">
              {connectorType && <MetadataRow label="Connector" value={connectorType} />}
              {configEntries.map(([k, v]) => (
                <MetadataRow key={k} label={prettyKey(k)} value={String(v)} />
              ))}
            </div>
          </div>
        )}

        {/* Other metadata */}
        {otherMetadata.length > 0 && (
          <div>
            <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1.5">Metadata</div>
            <div className="rounded-md border border-zinc-200 dark:border-zinc-800 px-3">
              {otherMetadata.map(([k, v]) => (
                <MetadataRow key={k} label={prettyKey(k)} value={String(v)} />
              ))}
            </div>
          </div>
        )}

        {/* Dependencies */}
        {(dependencies.length > 0 || downstream.length > 0) && (
          <div>
            <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1.5">Connections</div>
            <div className="space-y-2">
              {dependencies.length > 0 && (
                <div>
                  <div className="text-[11px] text-zinc-500 mb-1">Depends on</div>
                  <div className="flex flex-wrap gap-1.5">
                    {dependencies.map((depId) => {
                      const dep = allStages.find((s) => s.id === depId)
                      return (
                        <button
                          key={depId}
                          type="button"
                          onClick={() => onSelectStage?.(depId)}
                          className="text-[11px] px-2 py-0.5 rounded-md border border-zinc-200 dark:border-zinc-700 hover:bg-zinc-100 dark:hover:bg-zinc-800 text-zinc-700 dark:text-zinc-300 transition-colors"
                        >
                          ← {dep?.display_name ?? depId}
                        </button>
                      )
                    })}
                  </div>
                </div>
              )}
              {downstream.length > 0 && (
                <div>
                  <div className="text-[11px] text-zinc-500 mb-1">Triggers</div>
                  <div className="flex flex-wrap gap-1.5">
                    {downstream.map((d) => (
                      <button
                        key={d.id}
                        type="button"
                        onClick={() => onSelectStage?.(d.id)}
                        className="text-[11px] px-2 py-0.5 rounded-md border border-zinc-200 dark:border-zinc-700 hover:bg-zinc-100 dark:hover:bg-zinc-800 text-zinc-700 dark:text-zinc-300 transition-colors"
                      >
                        {d.display_name} →
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Ask about this node */}
        <Button
          variant="outline"
          size="sm"
          className="w-full gap-2 mt-2"
          onClick={handleAskAboutNode}
        >
          <MessageSquare className="h-3.5 w-3.5" />
          Ask about this node
        </Button>

        {/* Stage ID */}
        <div className="pt-2 border-t border-zinc-100 dark:border-zinc-800">
          <div className="text-[10px] text-zinc-400 font-mono break-all">{stage.id}</div>
        </div>
      </div>
    </div>
  )
}
