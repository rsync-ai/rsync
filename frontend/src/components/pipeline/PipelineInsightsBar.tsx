"use client"

import { useRouter } from "next/navigation"
import { Sparkles, AlertTriangle, Clock } from "lucide-react"
import type { ExecutionPlanStage } from "./DAGVisualization"
import { detectDurationAnomaly, stageDurationMs } from "./dagHelpers"

interface PipelineInsightsBarProps {
  pipelineName?: string
  stages: ExecutionPlanStage[]
}

export function PipelineInsightsBar({ pipelineName, stages }: PipelineInsightsBarProps) {
  const router = useRouter()

  // Aggregate insights across all stages
  const anomalies = stages.filter((s) => detectDurationAnomaly(s, stages) != null)
  const failed = stages.filter((s) => s.status === "failed")
  const longest = stages
    .map((s) => ({ stage: s, ms: stageDurationMs(s) ?? 0 }))
    .filter((x) => x.ms > 0)
    .sort((a, b) => b.ms - a.ms)[0]
  const longestStage = longest?.stage

  const askChat = (prompt: string) => {
    router.push(`/chat?${new URLSearchParams({ prompt, autosend: "1" }).toString()}`)
  }

  const explainPrompt = `Explain my pipeline${pipelineName ? ` "${pipelineName}"` : ""} in plain English. It has ${stages.length} stages: ${stages.map((s) => s.display_name).join(" → ")}. What does it do, what's flowing through it, and is anything unusual?`

  const anomalyPrompt = anomalies.length > 0
    ? `In my pipeline, these stages are taking unusually long: ${anomalies.map((s) => s.display_name).join(", ")}. Why might that be, and what should I check?`
    : `Are there any performance anomalies in my pipeline${pipelineName ? ` "${pipelineName}"` : ""}? Walk me through the slowest stage.`

  const slowestPrompt = longest
    ? `Why did the "${longest.stage.display_name}" stage take ${Math.round(longest.ms / 1000)}s? What does that stage do and what would speed it up?`
    : `Which stage in my pipeline is the bottleneck and why?`

  return (
    <div className="rounded-lg border border-violet-200 dark:border-violet-900/50 bg-gradient-to-r from-violet-50 to-indigo-50 dark:from-violet-950/30 dark:to-indigo-950/30 p-3">
      <div className="flex items-start gap-3">
        <div className="shrink-0 mt-0.5">
          <Sparkles className="h-4 w-4 text-violet-600 dark:text-violet-400" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-xs font-semibold text-violet-900 dark:text-violet-200">
              Ask about this pipeline
            </span>
            {anomalies.length > 0 && (
              <span className="text-[11px] text-amber-600 dark:text-amber-400 inline-flex items-center gap-0.5">
                · <AlertTriangle className="h-3 w-3" /> {anomalies.length} anomal{anomalies.length === 1 ? "y" : "ies"}
              </span>
            )}
          </div>
          <div className="flex flex-wrap gap-1.5 mt-2">
            <Chip onClick={() => askChat(explainPrompt)} icon={<Sparkles className="h-3 w-3" />}>
              Explain this pipeline
            </Chip>
            {(anomalies.length > 0 || failed.length > 0) && (
              <Chip
                onClick={() => askChat(anomalyPrompt)}
                icon={<AlertTriangle className="h-3 w-3" />}
                tone="amber"
              >
                {anomalies.length > 0 ? `Why are ${anomalies.length} stage${anomalies.length > 1 ? "s" : ""} slow?` : "Investigate failures"}
              </Chip>
            )}
            {longestStage && anomalies.length === 0 && (
              <Chip onClick={() => askChat(slowestPrompt)} icon={<Clock className="h-3 w-3" />}>
                Why is "{longestStage.display_name}" the slowest?
              </Chip>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

function Chip({
  children,
  icon,
  onClick,
  tone = "violet",
}: {
  children: React.ReactNode
  icon?: React.ReactNode
  onClick: () => void
  tone?: "violet" | "amber" | "cyan"
}) {
  const tones = {
    violet: "border-violet-200 dark:border-violet-800 hover:bg-violet-100 dark:hover:bg-violet-900/40 text-violet-800 dark:text-violet-200",
    amber: "border-amber-300 dark:border-amber-800 hover:bg-amber-100 dark:hover:bg-amber-900/40 text-amber-800 dark:text-amber-300",
    cyan: "border-cyan-300 dark:border-cyan-800 hover:bg-cyan-100 dark:hover:bg-cyan-900/40 text-cyan-800 dark:text-cyan-300",
  }
  return (
    <button
      type="button"
      onClick={onClick}
      className={`inline-flex items-center gap-1.5 text-[11px] font-medium px-2.5 py-1 rounded-full bg-white dark:bg-zinc-900 border transition-colors ${tones[tone]}`}
    >
      {icon}
      {children}
    </button>
  )
}
