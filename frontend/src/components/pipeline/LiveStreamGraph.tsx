"use client"

/**
 * The three nodes a running CDC pipeline actually has: Source → Kafka → Destination.
 *
 * WHY THIS EXISTS
 *
 * The Steps/DAG tab renders `pipeline_progress.metadata.execution_plan`, which the
 * Temporal workflow builds ONCE, before any data moves: eight agent setup stages
 * from "Understanding Request" to "Executing Pipeline"
 * (`execution_plan_builder.go:50-155`). For a stream the workflow then completes
 * `executor`, marks the execution `streaming_active` and RETURNS
 * (`nl_pipeline_v2_workflow.go:2063-2096`) — no further stage is ever written.
 *
 * So for a CDC pipeline that tab is a frozen picture of a setup that finished
 * hours ago. Every node is green, the "Live" badge never appears (it needs a stage
 * with status `running`), and the 8 s poll never arms. Nothing on it describes the
 * thing the user came to look at.
 *
 * This is that missing view, built from what P0 and P1 now measure rather than
 * from the plan: the producer's state, the broker's backlog, and the sink's. Its
 * nodes keep the `ExecutionPlanStage` shape (status, result_summary) so the claims
 * stay in the vocabulary the rest of the tab uses, but they are drawn as a compact
 * three-step strip rather than through `DAGVisualizationV2`: three nodes in a fixed
 * line need no pan/zoom canvas, and React Flow gave them a tall, mostly empty box
 * with a minimap above the part of the tab that matters.
 *
 * It renders only for CDC. A batch pipeline's rows go through the executor, not a
 * consumer group, and its existing plan already describes its run.
 */

import { useEffect, useMemo, useState } from "react"
import { ChevronDown, ChevronRight } from "lucide-react"

import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { usePipelineRuntime, type PipelineRuntime } from "@/lib/hooks/usePipelineRuntime"
import { formatSpanCoarse } from "@/lib/duration"
import { cn } from "@/lib/utils"
import type { ExecutionPlanStage } from "./dagTypes"

export interface LiveStreamConsumerTopic {
  topic: string
  table?: string
  lag: number
}

export interface LiveStreamConsumer {
  group: string
  role: string
  state?: string
  members?: number
  total_lag: number
  topics: LiveStreamConsumerTopic[]
}

export interface LiveStreamConsumers {
  consumers: LiveStreamConsumer[]
  measured: boolean
}

/** A node's status in the vocabulary the plan's stages already use. */
type NodeStatus = "running" | "complete" | "failed" | "waiting" | "pending"

// Said in words as well as by the dot, so the state survives without colour.
const STATUS_WORD: Record<NodeStatus, string> = {
  running: "Active",
  complete: "OK",
  failed: "Failed",
  waiting: "Waiting",
  pending: "Waiting",
}

const STATUS_DOT: Record<NodeStatus, string> = {
  running: "bg-sky-500 motion-safe:animate-pulse",
  complete: "bg-emerald-500",
  failed: "bg-red-500",
  waiting: "bg-zinc-400",
  pending: "bg-zinc-400",
}

/**
 * Build the three nodes.
 *
 * Exported and pure so the mapping from "what the backend reports" to "what the
 * picture claims" is testable without React Flow — the claims are the part that
 * can be wrong in a way that matters.
 *
 * The rules that carry weight:
 *
 *  - The SOURCE node is `failed` when the `debezium_task` dependency is
 *    unhealthy. That is the capture hole: with the only producer dead every topic
 *    drains to lag 0 and every other signal reads healthy
 *    (KI-DEBEZIUM-WORKER-DEATH-NOT-SURFACED), so this node is the one place the
 *    picture can be honest about it.
 *  - The KAFKA node is `running` while there is a backlog and `complete` when
 *    drained — but it says "nothing waiting" rather than anything about health,
 *    because an empty queue means nothing on its own.
 *  - The DESTINATION node is `failed` when the sink is stalled (the Sentinel's own
 *    two-signal verdict: a backlog AND no committed progress), NOT merely when it
 *    is behind. A first load is a large backlog on a perfectly healthy sink.
 *  - Anything not measured is `waiting`, never `complete`. A node that has not
 *    been looked at must not render as a finished one.
 */
export function buildLiveStreamStages(
  runtime: PipelineRuntime | null,
  consumers: LiveStreamConsumers | null,
  labels: { source?: string; destination?: string },
): ExecutionPlanStage[] {
  const liveness = runtime?.liveness
  const deps = runtime?.dependencies ?? []
  const debezium = deps.find((d) => d.kind === "debezium_task")
  const sink = deps.find((d) => d.kind === "kafka_sink_worker")
  const dest = deps.find((d) => d.kind === "mcp_dest")

  // ---- Source ----------------------------------------------------------
  let sourceStatus: NodeStatus = "waiting"
  let sourceSummary = "waiting for the first check"
  if (debezium?.status === "healthy") {
    sourceStatus = "running"
    sourceSummary = liveness?.last_event_at
      ? `last change ${formatSpanCoarse(Math.max(0, (Date.now() - Date.parse(liveness.last_event_at)) / 1000))} ago`
      : "capturing"
  } else if (debezium?.status === "degraded") {
    sourceStatus = "running"
    sourceSummary = debezium.last_error || "the source stream is not fully running"
  } else if (debezium?.status === "unhealthy") {
    sourceStatus = "failed"
    // Deliberately explicit: a lag of 0 downstream is the SYMPTOM of this, and
    // reads as health everywhere else.
    sourceSummary = debezium.last_error || "capture stopped — no changes are being read from the source"
  }

  // ---- Kafka -----------------------------------------------------------
  const lag = liveness?.sink_lag_messages
  const topicCount = consumers?.consumers.reduce((n, c) => Math.max(n, c.topics.length), 0) ?? 0
  let kafkaStatus: NodeStatus = "waiting"
  let kafkaSummary = "no lag reading yet"
  if (typeof lag === "number") {
    kafkaStatus = lag > 0 ? "running" : "complete"
    kafkaSummary = lag > 0 ? `${lag.toLocaleString()} waiting` : "nothing waiting"
  }

  // ---- Destination -----------------------------------------------------
  let destStatus: NodeStatus = "waiting"
  let destSummary = "waiting for the first check"
  if (liveness?.sink_stalled) {
    destStatus = "failed"
    const forHow = liveness.sink_stalled_seconds ? ` for ${formatSpanCoarse(liveness.sink_stalled_seconds)}` : ""
    destSummary = `stalled — no progress committed${forHow}`
  } else if (dest?.status === "unhealthy" || sink?.status === "unhealthy") {
    destStatus = "failed"
    destSummary = dest?.last_error || sink?.last_error || "the destination is not reachable"
  } else if (liveness?.sink_committed_moving) {
    destStatus = "running"
    destSummary = "writing changes"
  } else if (typeof lag === "number" && lag === 0 && debezium?.status === "healthy") {
    destStatus = "complete"
    destSummary = "up to date"
  } else if (sink || dest) {
    destStatus = "running"
    destSummary = "connected"
  }

  const topicLabel = topicCount > 0 ? `${topicCount} topic${topicCount === 1 ? "" : "s"}` : "change stream"

  return [
    {
      id: "live_source",
      display_name: labels.source ? `Capture from ${labels.source}` : "Capture from source",
      description: "Debezium reads the source's change log",
      status: sourceStatus,
      result_summary: sourceSummary,
      node_kind: "source",
      dependencies: [],
    },
    {
      id: "live_kafka",
      display_name: "Kafka",
      description: topicLabel,
      status: kafkaStatus,
      result_summary: kafkaSummary,
      node_kind: "transform",
      dependencies: ["live_source"],
    },
    {
      id: "live_destination",
      display_name: labels.destination ? `Write to ${labels.destination}` : "Write to destination",
      description: "the sink applies changes to the destination",
      status: destStatus,
      result_summary: destSummary,
      node_kind: "destination",
      dependencies: ["live_kafka"],
    },
  ]
}

/**
 * Poll the consumer census alongside the runtime.
 *
 * Deliberately without an error surface of its own: this read supplies only the
 * topic count in the Kafka node's label, which degrades to "change stream" on its
 * own. Every claim the picture makes about health comes from the runtime, which
 * has its own error handling. A failed consumer read must not blank the graph.
 */
function useConsumers(pipelineId: string): LiveStreamConsumers | null {
  const [data, setData] = useState<LiveStreamConsumers | null>(null)
  // The pipeline this answer is about. Without the reset, navigating between two
  // pipeline pages shows the one you left until the new read lands — the flap
  // usePipelineRuntime and usePolledJson each carry the same guard for.
  // State, not a ref: this is React's sanctioned adjust-during-render pattern and
  // the one usePolledJson already uses for the same purpose.
  const [subject, setSubject] = useState(pipelineId)
  if (pipelineId !== subject) {
    setSubject(pipelineId)
    setData(null)
  }

  const url = API_ENDPOINTS.PIPELINES.CONSUMERS(pipelineId)
  useEffect(() => {
    let cancelled = false
    const ac = new AbortController()

    const once = async () => {
      try {
        const res = await authFetch(url, { cache: "no-store", signal: ac.signal })
        if (!res.ok || cancelled) return
        const json = (await res.json()) as LiveStreamConsumers
        if (!cancelled) setData(json)
      } catch {
        // Best-effort; see the note above.
      }
    }

    void once()
    const t = window.setInterval(() => void once(), 10_000)
    return () => {
      cancelled = true
      window.clearInterval(t)
      ac.abort()
    }
  }, [url])

  return data
}

export function LiveStreamGraph({
  pipelineId,
  sourceLabel,
  destinationLabel,
}: {
  pipelineId: string
  sourceLabel?: string
  destinationLabel?: string
}) {
  const { runtime } = usePipelineRuntime(pipelineId)
  const consumers = useConsumers(pipelineId)

  const stages = useMemo(
    () => buildLiveStreamStages(runtime, consumers, { source: sourceLabel, destination: destinationLabel }),
    [runtime, consumers, sourceLabel, destinationLabel],
  )

  return (
    <div data-testid="live-stream-graph">
      <div className="mb-2">
        <h4 className="text-sm font-semibold">Live stream</h4>
        <p className="text-xs text-zinc-500 dark:text-zinc-400">
          What this pipeline is doing right now. The steps below ran once, when it was set up.
        </p>
      </div>
      <ol aria-label="Live stream" className="flex flex-col gap-4 sm:flex-row sm:gap-6">
        {stages.map((stage, i) => {
          const status = (stage.status as NodeStatus) in STATUS_WORD ? (stage.status as NodeStatus) : "waiting"
          const failed = status === "failed"
          const last = i === stages.length - 1
          return (
            <li
              key={stage.id}
              data-testid={`live-node-${stage.id}`}
              data-status={status}
              className="relative min-w-0 sm:flex-1"
            >
              <div
                className={cn(
                  "h-full rounded-md border px-3 py-2",
                  failed ? "border-red-300 bg-red-50/60 dark:border-red-900/60 dark:bg-red-950/20" : "border-border/60",
                )}
              >
                <div className="flex items-center gap-2">
                  <span aria-hidden="true" className={cn("h-2 w-2 shrink-0 rounded-full", STATUS_DOT[status])} />
                  <span className="min-w-0 break-words text-sm font-medium">{stage.display_name}</span>
                  <span
                    className={cn(
                      "ml-auto shrink-0 text-[11px] font-medium",
                      failed ? "text-red-600 dark:text-red-400" : "text-muted-foreground",
                    )}
                  >
                    {STATUS_WORD[status]}
                  </span>
                </div>
                {stage.description && <p className="mt-0.5 text-xs text-muted-foreground">{stage.description}</p>}
                {stage.result_summary && (
                  <p className={cn("mt-1 break-words text-xs", failed ? "text-red-700 dark:text-red-300" : "text-foreground")}>
                    {stage.result_summary}
                  </p>
                )}
              </div>
              {!last && (
                <>
                  <ChevronDown
                    aria-hidden="true"
                    className="absolute -bottom-4 left-1/2 h-4 w-4 -translate-x-1/2 text-muted-foreground sm:hidden"
                  />
                  <ChevronRight
                    aria-hidden="true"
                    className="absolute -right-5 top-1/2 hidden h-4 w-4 -translate-y-1/2 text-muted-foreground sm:block"
                  />
                </>
              )}
            </li>
          )
        })}
      </ol>
    </div>
  )
}
