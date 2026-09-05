"use client"

import { useState, useEffect, useRef } from "react"
import { useRouter } from "next/navigation"
import {
  Sparkles,
  ChevronRight,
  ChevronLeft,
  Send,
  AlertTriangle,
  Clock,
  HelpCircle,
} from "lucide-react"
import { cn } from "@/lib/utils"
import type { ExecutionPlanStage } from "./DAGVisualization"
import { detectDurationAnomaly } from "./dagHelpers"

interface PipelineCopilotDockProps {
  pipelineId: string
  pipelineName?: string
  stages: ExecutionPlanStage[]
  selectedStage?: ExecutionPlanStage | null
}

const STORAGE_KEY = "pipeline-copilot-dock-open"

export function PipelineCopilotDock({
  pipelineId,
  pipelineName,
  stages,
  selectedStage,
}: PipelineCopilotDockProps) {
  const router = useRouter()
  const [open, setOpen] = useState(false)
  const [input, setInput] = useState("")
  const inputRef = useRef<HTMLTextAreaElement>(null)

  // Persist open state across re-renders
  useEffect(() => {
    try {
      const v = localStorage.getItem(STORAGE_KEY)
      if (v === "1") setOpen(true)
    } catch {}
  }, [])
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, open ? "1" : "0")
    } catch {}
  }, [open])

  // Live aggregate stats
  const anomalies = stages.filter((s) => detectDurationAnomaly(s, stages) != null)
  const failed = stages.filter((s) => s.status === "failed")
  const running = stages.filter((s) => s.status === "running" || s.status === "waiting")
  const completed = stages.filter((s) => s.status === "complete").length

  const sendToFullChat = (prompt: string) => {
    router.push(`/chat?${new URLSearchParams({ prompt, autosend: "1" }).toString()}`)
  }

  const handleSubmit = () => {
    const text = input.trim()
    if (!text) return
    const ctxLines: string[] = []
    ctxLines.push(`Context: I'm looking at pipeline${pipelineName ? ` "${pipelineName}"` : ""} (id: ${pipelineId}).`)
    ctxLines.push(
      `It has ${stages.length} stages: ${stages.map((s) => `"${s.display_name}" [${s.status}]`).join(", ")}.`,
    )
    if (selectedStage) {
      ctxLines.push(
        `I'm currently focused on the "${selectedStage.display_name}" stage (status: ${selectedStage.status}${selectedStage.result_summary ? `, result: "${selectedStage.result_summary}"` : ""}).`,
      )
    }
    ctxLines.push("")
    ctxLines.push(`My question: ${text}`)
    sendToFullChat(ctxLines.join("\n"))
  }

  const presetQuestions: { icon: React.ElementType; label: string; prompt: string; tone?: "amber" | "cyan" }[] = []

  if (running.length > 0) {
    presetQuestions.push({
      icon: Clock,
      label: `What is "${running[0].display_name}" doing right now?`,
      prompt: `In my pipeline, the stage "${running[0].display_name}" is currently running. What does it do, and what's it processing?`,
    })
  }
  if (anomalies.length > 0) {
    presetQuestions.push({
      icon: AlertTriangle,
      label: `Why is "${anomalies[0].display_name}" slow?`,
      prompt: `The "${anomalies[0].display_name}" stage took ${detectDurationAnomaly(anomalies[0], stages)?.toFixed(1)}× the median for similar stages. Why might that be?`,
      tone: "amber",
    })
  }
  if (failed.length > 0) {
    presetQuestions.push({
      icon: AlertTriangle,
      label: `Diagnose "${failed[0].display_name}" failure`,
      prompt: `The "${failed[0].display_name}" stage failed with: "${failed[0].error_message ?? "unknown error"}". What went wrong and how do I fix it?`,
      tone: "amber",
    })
  }
  presetQuestions.push({
    icon: Sparkles,
    label: "Explain this pipeline",
    prompt: `Explain my pipeline${pipelineName ? ` "${pipelineName}"` : ""} in plain English. It has ${stages.length} stages: ${stages.map((s) => s.display_name).join(" → ")}. What does it do?`,
  })
  presetQuestions.push({
    icon: HelpCircle,
    label: "What should I check next?",
    prompt: `Walk me through what to monitor in my pipeline${pipelineName ? ` "${pipelineName}"` : ""} given its current state. What might break, and what's the most useful thing to check next?`,
  })

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="fixed right-0 top-1/2 -translate-y-1/2 z-30 rounded-l-lg border border-r-0 border-violet-200 dark:border-violet-800 bg-gradient-to-r from-violet-600 to-indigo-600 text-white px-2 py-3 shadow-lg hover:from-violet-700 hover:to-indigo-700 transition-all flex flex-col items-center gap-1"
        aria-label="Open Pipeline Copilot"
      >
        <Sparkles className="h-4 w-4" />
        <ChevronLeft className="h-3 w-3" />
        <span className="text-[9px] font-semibold tracking-wider [writing-mode:vertical-rl]">
          COPILOT
        </span>
      </button>
    )
  }

  return (
    <div className="fixed right-0 top-16 bottom-4 z-30 w-[360px] flex flex-col rounded-l-xl border border-r-0 border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 shadow-2xl">
      {/* Header */}
      <div className="flex items-center justify-between gap-2 px-4 py-3 border-b border-zinc-200 dark:border-zinc-800 bg-gradient-to-r from-violet-50 to-indigo-50 dark:from-violet-950/30 dark:to-indigo-950/30 rounded-tl-xl">
        <div className="flex items-center gap-2 min-w-0">
          <div className="h-7 w-7 rounded-md bg-gradient-to-br from-violet-500 to-indigo-600 flex items-center justify-center shrink-0">
            <Sparkles className="h-4 w-4 text-white" />
          </div>
          <div className="min-w-0">
            <div className="text-sm font-semibold text-zinc-900 dark:text-white">
              Pipeline Copilot
            </div>
            <div className="text-[10px] text-zinc-500 truncate">
              {pipelineName ?? "Live observability"}
            </div>
          </div>
        </div>
        <button
          type="button"
          onClick={() => setOpen(false)}
          className="shrink-0 p-1 rounded hover:bg-white/60 dark:hover:bg-zinc-800 text-zinc-500"
          aria-label="Close"
        >
          <ChevronRight className="h-4 w-4" />
        </button>
      </div>

      {/* Live stats */}
      <div className="grid grid-cols-2 gap-2 p-3 border-b border-zinc-200 dark:border-zinc-800">
        <Stat
          label="Stages"
          value={`${completed}/${stages.length}`}
          accent={running.length > 0 ? "blue" : "violet"}
          pulse={running.length > 0}
        />
        {anomalies.length > 0 && (
          <Stat label="Anomalies" value={String(anomalies.length)} accent="amber" />
        )}
        {failed.length > 0 && (
          <Stat label="Failures" value={String(failed.length)} accent="red" />
        )}
      </div>

      {/* Selected-stage hint */}
      {selectedStage && (
        <div className="mx-3 mt-3 px-2.5 py-1.5 rounded-md bg-violet-50 dark:bg-violet-950/30 border border-violet-200 dark:border-violet-800">
          <div className="text-[10px] uppercase tracking-wide text-violet-600 dark:text-violet-400">
            Focused on
          </div>
          <div className="text-xs font-medium text-violet-900 dark:text-violet-100 truncate">
            {selectedStage.display_name}
          </div>
        </div>
      )}

      {/* Preset questions */}
      <div className="flex-1 overflow-y-auto p-3 space-y-1.5">
        <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">
          Suggested questions
        </div>
        {presetQuestions.map((q, i) => {
          const Icon = q.icon
          const tone =
            q.tone === "amber"
              ? "border-amber-200 dark:border-amber-900/60 hover:bg-amber-50 dark:hover:bg-amber-950/30 text-amber-900 dark:text-amber-200"
              : q.tone === "cyan"
                ? "border-cyan-200 dark:border-cyan-900/60 hover:bg-cyan-50 dark:hover:bg-cyan-950/30 text-cyan-900 dark:text-cyan-200"
                : "border-zinc-200 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50 text-zinc-800 dark:text-zinc-200"
          return (
            <button
              key={i}
              type="button"
              onClick={() => sendToFullChat(q.prompt)}
              className={cn(
                "w-full text-left text-xs px-2.5 py-2 rounded-md border transition-colors flex items-start gap-2",
                tone,
              )}
            >
              <Icon className="h-3.5 w-3.5 shrink-0 mt-0.5 opacity-70" />
              <span className="leading-snug">{q.label}</span>
            </button>
          )
        })}
      </div>

      {/* Input */}
      <div className="border-t border-zinc-200 dark:border-zinc-800 p-3 space-y-2">
        <textarea
          ref={inputRef}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
              e.preventDefault()
              handleSubmit()
            }
          }}
          placeholder="Ask about this pipeline…"
          rows={2}
          className="w-full resize-none text-xs px-2.5 py-2 rounded-md border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-800/50 placeholder-zinc-400 focus:outline-none focus:ring-2 focus:ring-violet-500"
        />
        <div className="flex items-center justify-between gap-2">
          <span className="text-[10px] text-zinc-400">⌘+Enter to send</span>
          <button
            type="button"
            onClick={handleSubmit}
            disabled={!input.trim()}
            className="inline-flex items-center gap-1.5 text-xs font-medium px-3 py-1.5 rounded-md bg-violet-600 hover:bg-violet-700 disabled:bg-zinc-300 dark:disabled:bg-zinc-700 text-white disabled:cursor-not-allowed transition-colors"
          >
            <Send className="h-3 w-3" />
            Ask
          </button>
        </div>
      </div>
    </div>
  )
}

function Stat({
  label,
  value,
  accent,
  pulse,
}: {
  label: string
  value: string
  accent: "violet" | "blue" | "amber" | "red" | "cyan"
  pulse?: boolean
}) {
  const accents = {
    violet: "text-violet-700 dark:text-violet-300 bg-violet-50 dark:bg-violet-950/40 border-violet-200 dark:border-violet-900",
    blue: "text-blue-700 dark:text-blue-300 bg-blue-50 dark:bg-blue-950/40 border-blue-200 dark:border-blue-900",
    amber: "text-amber-700 dark:text-amber-300 bg-amber-50 dark:bg-amber-950/40 border-amber-200 dark:border-amber-900",
    red: "text-red-700 dark:text-red-300 bg-red-50 dark:bg-red-950/40 border-red-200 dark:border-red-900",
    cyan: "text-cyan-700 dark:text-cyan-300 bg-cyan-50 dark:bg-cyan-950/40 border-cyan-200 dark:border-cyan-900",
  }
  return (
    <div className={cn("rounded-md border px-2.5 py-2", accents[accent], pulse && "dag-running-pulse")}>
      <div className="text-[10px] uppercase tracking-wide opacity-70">{label}</div>
      <div className="text-base font-bold tabular-nums leading-tight">{value}</div>
    </div>
  )
}
