"use client"

import { useState, useRef, useEffect, useCallback } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { cn } from "@/lib/utils"
import {
  Send,
  Bot,
  User,
  Loader2,
  Database,
  CheckCircle2,
  XCircle,
  AlertCircle,
  Settings,
  Rocket,
  ArrowRight,
  Sparkles,
  Zap,
  Eye,
  ChevronDown,
  ChevronRight,
  Plus,
} from "lucide-react"
import Link from "next/link"
import { motion, AnimatePresence } from "framer-motion"
import { DiscoverySummaryCard } from "./DiscoverySummaryCard"
import { SmartRecommendationsCard } from "./SmartRecommendationsCard"
import { ConfigPreview } from "./ConfigPreview"
import { TransformationConfigurator, type Transform } from "./TransformationConfigurator"
import type { SyncMode } from "./SyncModeSelector"
import type { RowFilter } from "./RowFilterBuilder"
import { LLM_SERVICE_URL } from "@/lib/config/api"

// =============================================================================
// TYPES
// =============================================================================

interface CDCConnection {
  id: string
  name: string
  // Some endpoints return snake_case; others are normalized.
  connector_type?: string
  connectorType?: string
  database_type?: string
  hostname?: string
  status?: string
}

interface AgentStep {
  id: string
  name: string
  status: "pending" | "running" | "complete" | "error"
  duration_ms?: number
}

interface AgentActivity {
  agent: string
  status: "running" | "complete" | "error"
  steps: AgentStep[]
  currentStep?: string
  startTime: number
}

interface StageData {
  discovery?: {
    schema: any
    cdc_validation: any
    summary: any
  }
  recommendation?: {
    recommended_tables: any[]
    skipped_tables: any[]
    sync_mode: string
    total_rows: number
  }
  transformation?: {
    sensitivity_analysis: any
    suggested_transforms: any[]
  }
  configuration?: {
    pipeline_config: any
  }
}

interface PipelineState {
  sessionId: string | null
  pipelineId: string | null
  stage: string
  stageData: StageData
  agentActivity: AgentActivity | null
  isLoading: boolean
  error: string | null
}

interface CDCAgentChatProps {
  connections: CDCConnection[]
  onPipelineCreated?: (pipelineId: string) => void
}

// LLM_SERVICE_URL is imported from config

// =============================================================================
// AGENT ACTIVITY PANEL
// =============================================================================

function AgentActivityPanelV2({ activity }: { activity: AgentActivity }) {
  const elapsedTime = (Date.now() - activity.startTime) / 1000
  const completedSteps = activity.steps.filter(s => s.status === "complete").length
  const progress = (completedSteps / Math.max(activity.steps.length, 1)) * 100

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className="rounded-xl border border-violet-200 bg-gradient-to-br from-violet-50 to-indigo-50 p-4 dark:border-violet-800 dark:from-violet-950/30 dark:to-indigo-950/30"
    >
      <div className="mb-3 flex items-center justify-between">
        <h3 className="flex items-center gap-2 text-base font-semibold text-zinc-900 dark:text-white">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-violet-600 to-indigo-600">
            <Sparkles className="h-4 w-4 text-white" />
          </div>
          {activity.agent}
        </h3>
        <div className="flex items-center gap-2">
          <span className="text-sm text-zinc-500">{elapsedTime.toFixed(1)}s</span>
          <Badge className="bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300">
            <Loader2 className="mr-1 h-3 w-3 animate-spin" />
            Working
          </Badge>
        </div>
      </div>

      <div className="space-y-2">
        {activity.steps.map((step, idx) => (
          <motion.div
            key={step.id}
            initial={{ opacity: 0, x: -10 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: idx * 0.1 }}
            className="flex items-center gap-2"
            >
            {step.status === "complete" && (
              <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-500" />
            )}
            {step.status === "running" && (
              <Loader2 className="h-4 w-4 shrink-0 animate-spin text-violet-600" />
            )}
            {step.status === "pending" && (
              <div className="h-4 w-4 shrink-0 rounded-full border-2 border-zinc-300 dark:border-zinc-600" />
            )}
            {step.status === "error" && (
              <XCircle className="h-4 w-4 shrink-0 text-red-500" />
              )}
            <span className={cn(
              "text-sm",
              step.status === "complete" && "text-zinc-600 dark:text-zinc-400",
              step.status === "running" && "font-medium text-violet-700 dark:text-violet-300",
              step.status === "pending" && "text-zinc-400 dark:text-zinc-500",
              step.status === "error" && "text-red-600 dark:text-red-400"
            )}>
              {formatStepName(step.name)}
            </span>
            {step.duration_ms && (
              <span className="text-xs text-zinc-400">{step.duration_ms}ms</span>
            )}
          </motion.div>
        ))}
      </div>

      <div className="mt-4 pt-3 border-t border-violet-200 dark:border-violet-800">
        <div className="flex justify-between text-sm text-zinc-600 dark:text-zinc-400 mb-2">
          <span>Step {completedSteps + 1} of {activity.steps.length}</span>
          <span>{Math.round(progress)}%</span>
          </div>
        <Progress value={progress} className="h-2" />
    </div>
    </motion.div>
  )
}

function formatStepName(name: string): string {
  return name
    .replace(/_/g, " ")
    .replace(/\b\w/g, c => c.toUpperCase())
}

// =============================================================================
// SOURCE SELECTOR
// =============================================================================

function SourceSelector({
  connections,
  onSelect,
  disabled,
}: {
  connections: CDCConnection[]
  onSelect: (connection: CDCConnection, destinationType: string) => void
  disabled: boolean
}) {
  const [selectedSource, setSelectedSource] = useState<CDCConnection | null>(null)
  const [destinationType] = useState("s3") // Fixed to S3 for now

    return (
    <Card className="p-6 border-zinc-200 dark:border-zinc-800">
      <div className="mb-4">
        <h2 className="text-lg font-semibold text-zinc-900 dark:text-white">
          Select Source Database
        </h2>
        <p className="text-sm text-zinc-500 dark:text-zinc-400">
          Choose a database connection to set up CDC replication to AWS S3
        </p>
      </div>

      <div className="grid gap-3">
        {connections.map((conn) => (
          <button
            key={conn.id}
            onClick={() => {
              setSelectedSource(conn)
              onSelect(conn, destinationType)
            }}
            disabled={disabled}
            className={cn(
              "flex items-center gap-4 p-4 rounded-lg border transition-all text-left",
              "hover:border-violet-500 hover:bg-violet-50 dark:hover:bg-violet-950/30",
              selectedSource?.id === conn.id
                ? "border-violet-500 bg-violet-50 dark:bg-violet-950/30"
                : "border-zinc-200 dark:border-zinc-800",
              disabled && "opacity-50 cursor-not-allowed"
            )}
          >
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-gradient-to-br from-blue-500 to-cyan-500">
              <Database className="h-5 w-5 text-white" />
            </div>
            <div className="flex-1">
              <p className="font-medium text-zinc-900 dark:text-white">{conn.name}</p>
              <p className="text-sm text-zinc-500 dark:text-zinc-400">
                {conn.database_type?.toUpperCase()} • {conn.hostname || "localhost"}
              </p>
            </div>
            <Badge variant="outline" className="shrink-0">
              {conn.status || "Ready"}
              </Badge>
          </button>
        ))}
        
        {/* Add New Connection Option */}
        <Link
          href="/connections/new?returnTo=/cdc/new&type=source"
          className={cn(
            "flex items-center gap-4 p-4 rounded-lg border transition-all text-left",
            "border-dashed border-zinc-300 dark:border-zinc-700",
            "hover:border-violet-500 hover:bg-violet-50 dark:hover:bg-violet-950/30",
            disabled && "opacity-50 pointer-events-none"
          )}
        >
          <div className="flex h-10 w-10 items-center justify-center rounded-lg border-2 border-dashed border-zinc-300 dark:border-zinc-600 bg-zinc-50 dark:bg-zinc-800/50">
            <Plus className="h-5 w-5 text-zinc-400" />
          </div>
          <div className="flex-1">
            <p className="font-medium text-zinc-700 dark:text-zinc-300">Add New Connection</p>
            <p className="text-sm text-zinc-500 dark:text-zinc-400">
              Connect a new PostgreSQL, MySQL, or other database
            </p>
          </div>
          <ArrowRight className="h-4 w-4 text-zinc-400" />
        </Link>
      </div>
    </Card>
  )
}

// =============================================================================
// MAIN COMPONENT
// =============================================================================

export function CDCAgentChat({ connections, onPipelineCreated }: CDCAgentChatProps) {
  const [state, setState] = useState<PipelineState>({
    sessionId: null,
    pipelineId: null,
    stage: "source_selection",
    stageData: {},
    agentActivity: null,
    isLoading: false,
    error: null,
  })
  
  // Sync mode state - visible in recommendations, passed to deployment
  const [selectedSyncMode, setSelectedSyncMode] = useState<SyncMode>("full_cdc")

  const scrollRef = useRef<HTMLDivElement>(null)
  const eventSourceRef = useRef<EventSource | null>(null)

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close()
      }
    }
  }, [])

  // Auto-scroll
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [state])

  const handleSourceSelect = useCallback(async (connection: CDCConnection, destinationType: string) => {
    setState(prev => ({
      ...prev,
      isLoading: true,
      stage: "discovery_working",
      agentActivity: {
        agent: "Discovery Agent",
        status: "running",
        steps: [
          { id: "1", name: "connecting", status: "pending" },
          { id: "2", name: "querying_schema", status: "pending" },
          { id: "3", name: "analyzing_tables", status: "pending" },
          { id: "4", name: "validating_cdc", status: "pending" },
        ],
        startTime: Date.now(),
      },
    }))

    try {
      // Use fetch with ReadableStream for SSE
      const response = await fetch(`${LLM_SERVICE_URL}/api/v1/cdc/v2/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          source_connection_id: connection.id,
          destination_type: destinationType,
          source_type: connection.connector_type || connection.connectorType || connection.database_type || "postgres",
        }),
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`)
      }

      const reader = response.body?.getReader()
      if (!reader) throw new Error("No response body")

      const decoder = new TextDecoder()
      let buffer = ""

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split("\n")
        buffer = lines.pop() || ""

        for (const line of lines) {
          if (line.startsWith("event:")) {
            const eventType = line.replace("event:", "").trim()
            continue
          }
          if (line.startsWith("data:")) {
            const data = JSON.parse(line.replace("data:", "").trim())
            handleSSEEvent(data)
          }
        }
      }
    } catch (error) {
      console.error("Pipeline start error:", error)
      setState(prev => ({
        ...prev,
        isLoading: false,
        error: error instanceof Error ? error.message : "Failed to start pipeline",
        agentActivity: null,
      }))
    }
  }, [])

  const handleSSEEvent = useCallback((data: any) => {
    // Handle different event types
    if (data.session_id && !state.sessionId) {
      setState(prev => ({ ...prev, sessionId: data.session_id }))
    }

    if (data.agent && data.step) {
      // Agent step event
      setState(prev => {
        if (!prev.agentActivity) return prev
        
        const steps = prev.agentActivity.steps.map(s => {
          if (s.name === data.step) {
            return { ...s, status: data.status, duration_ms: data.duration_ms }
          }
          // Mark previous steps as complete if this one is running
          if (data.status === "running") {
            const idx = prev.agentActivity!.steps.findIndex(x => x.name === data.step)
            const thisIdx = prev.agentActivity!.steps.findIndex(x => x.name === s.name)
            if (thisIdx < idx && s.status === "pending") {
              return { ...s, status: "complete" as const }
            }
          }
          return s
        })
        
        return {
          ...prev,
          agentActivity: {
            ...prev.agentActivity,
            steps,
            currentStep: data.step,
          },
        }
      })
    }

    if (data.stage === "discovery" && data.data) {
      // Discovery stage complete
      setState(prev => ({
        ...prev,
        stage: "discovery_complete",
        stageData: {
          ...prev.stageData,
          discovery: data.data,
        },
        agentActivity: prev.agentActivity ? {
          ...prev.agentActivity,
          status: "complete",
          steps: prev.agentActivity.steps.map(s => ({ ...s, status: "complete" as const })),
        } : null,
      }))
    }

    if (data.stage === "recommendation" && data.data) {
      // Recommendation stage complete
      setState(prev => ({
        ...prev,
        stage: "recommendation_complete",
        stageData: {
          ...prev.stageData,
          recommendation: data.data,
        },
        isLoading: false,
        agentActivity: null, // Clear agent activity after recommendations
      }))
    }

    if (data.stage === "transformation" && data.data) {
      setState(prev => ({
        ...prev,
        stage: "transformation_complete",
        stageData: {
          ...prev.stageData,
          transformation: data.data,
        },
        isLoading: false,
        agentActivity: null,
      }))
    }

    if (data.stage === "configuration" && data.data) {
      setState(prev => ({
        ...prev,
        stage: "configuration_complete",
        stageData: {
          ...prev.stageData,
          configuration: data.data,
        },
        isLoading: false,
        agentActivity: null,
      }))
    }

    if (data.pipeline_id) {
      // Pipeline deployed
      setState(prev => ({
        ...prev,
        pipelineId: data.pipeline_id,
        stage: "monitoring",
        isLoading: false,
        agentActivity: null,
      }))
      if (onPipelineCreated) {
        onPipelineCreated(data.pipeline_id)
      }
    }

    if (data.error) {
      setState(prev => ({
        ...prev,
        error: data.error,
        isLoading: false,
        agentActivity: null,
      }))
    }

    // Handle agent transitions (discovery → recommendation)
    if (data.agent === "Recommendation Agent" && data.message) {
      setState(prev => ({
        ...prev,
        agentActivity: {
          agent: "Recommendation Agent",
          status: "running",
          steps: [
            { id: "1", name: "analyzing_relationships", status: "pending" },
            { id: "2", name: "categorizing_tables", status: "pending" },
          ],
          startTime: Date.now(),
        },
      }))
    }
  }, [state.sessionId, onPipelineCreated])

  const handleAction = useCallback(async (action: string, data?: any) => {
    if (!state.sessionId) return

    setState(prev => ({
      ...prev,
      isLoading: true,
      agentActivity: action === "approve" || action === "continue" ? {
        agent: "Configuration Agent",
        status: "running",
        steps: [
          { id: "1", name: "generating_cdc_config", status: "pending" },
          { id: "2", name: "generating_sink_config", status: "pending" },
        ],
        startTime: Date.now(),
      } : action === "deploy" ? {
        agent: "Deployment Agent",
        status: "running",
        steps: [
          { id: "1", name: "creating_topics", status: "pending" },
          { id: "2", name: "deploying_cdc_connector", status: "pending" },
          { id: "3", name: "deploying_sink_connector", status: "pending" },
          { id: "4", name: "initial_snapshot", status: "pending" },
        ],
        startTime: Date.now(),
      } : null,
    }))

    try {
      const response = await fetch(`${LLM_SERVICE_URL}/api/v1/cdc/v2/continue`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          session_id: state.sessionId,
          action,
          data,
        }),
      })

      if (!response.ok) throw new Error(`HTTP ${response.status}`)

      const reader = response.body?.getReader()
      if (!reader) throw new Error("No response body")

      const decoder = new TextDecoder()
      let buffer = ""

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split("\n")
        buffer = lines.pop() || ""

        for (const line of lines) {
          if (line.startsWith("data:")) {
            try {
              const eventData = JSON.parse(line.replace("data:", "").trim())
              handleSSEEvent(eventData)
            } catch (e) {
              // Skip invalid JSON
            }
          }
        }
      }
    } catch (error) {
      console.error("Action error:", error)
      setState(prev => ({
        ...prev,
        isLoading: false,
        error: error instanceof Error ? error.message : "Action failed",
        agentActivity: null,
      }))
    }
  }, [state.sessionId, handleSSEEvent])

  // Render based on stage
  return (
    <div className="flex flex-col h-full max-h-[800px] bg-white dark:bg-zinc-950 rounded-xl border border-zinc-200 dark:border-zinc-800 overflow-hidden">
      <ScrollArea className="flex-1 p-4" ref={scrollRef}>
        <div className="space-y-6">
          {/* Stage 1: Source Selection */}
          {state.stage === "source_selection" && (
            <SourceSelector
              connections={connections}
              onSelect={handleSourceSelect}
              disabled={state.isLoading}
            />
          )}

          {/* Agent Working - Show ONLY agent activity, no data */}
          {state.agentActivity && state.agentActivity.status === "running" && (
            <AgentActivityPanelV2 activity={state.agentActivity} />
          )}

          {/* Stage 2: Discovery Complete - Show summary */}
          {state.stage === "discovery_complete" && state.stageData.discovery && !state.agentActivity && (
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
            >
              <DiscoverySummaryCard
                schema={state.stageData.discovery.schema}
                cdcReady={state.stageData.discovery.cdc_validation?.is_ready !== false}
                onContinue={() => handleAction("continue")}
              />
            </motion.div>
          )}

          {/* Stage 3: Recommendations Complete - Show smart recommendations */}
          {state.stage === "recommendation_complete" && state.stageData.recommendation && !state.agentActivity && (
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              className="space-y-4"
            >
              {/* Keep showing discovery summary */}
              {state.stageData.discovery && (
                <DiscoverySummaryCard
                  schema={state.stageData.discovery.schema}
                  cdcReady={true}
                />
              )}

              {/* Show recommendations */}
              <SmartRecommendationsCard
                recommendations={{
                  recommended_tables: state.stageData.recommendation.recommended_tables,
                  recommended_sync_mode: state.stageData.recommendation.sync_mode,
                  skipped_count: state.stageData.recommendation.skipped_tables?.length || 0,
                }}
                syncMode={selectedSyncMode}
                onSyncModeChange={setSelectedSyncMode}
                onApprove={() => handleAction("approve", { sync_mode: selectedSyncMode })}
                onModify={() => handleAction("add_transforms")}
              />
            </motion.div>
          )}

          {/* Stage 3.5: Transformation Complete - Show transformation configurator */}
          {state.stage === "transformation_complete" && state.stageData.transformation && !state.agentActivity && (
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
            >
              <TransformationConfigurator
                sensitivityAnalysis={state.stageData.transformation.sensitivity_analysis}
                suggestedTransforms={state.stageData.transformation.suggested_transforms}
                tables={(state.stageData.recommendation?.recommended_tables || []).map((t: any) => ({
                  name: t.name,
                  columns: t.columns?.map((c: any) => c.name || c) || []
                }))}
                onApply={(config: { transforms: Transform[], syncMode: SyncMode, rowFilters: RowFilter[] }) => 
                  handleAction("apply_transforms", { 
                    transforms: config.transforms,
                    sync_mode: config.syncMode,
                    row_filters: config.rowFilters
                  })
                }
                onSkip={() => handleAction("skip_transforms")}
              />
            </motion.div>
          )}

          {/* Stage 4: Configuration Complete */}
          {state.stage === "configuration_complete" && state.stageData.configuration && !state.agentActivity && (
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
            >
              <ConfigPreview
                config={state.stageData.configuration.pipeline_config}
                onDeploy={() => handleAction("deploy")}
                onAdjust={() => {}}
              />
            </motion.div>
          )}

          {/* Stage 5: Pipeline Running */}
          {state.stage === "monitoring" && (
            <motion.div
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              className="text-center py-8"
            >
              <div className="flex h-16 w-16 mx-auto items-center justify-center rounded-2xl bg-gradient-to-br from-emerald-500 to-teal-500 mb-4">
                <CheckCircle2 className="h-8 w-8 text-white" />
            </div>
              <h2 className="text-xl font-semibold text-zinc-900 dark:text-white mb-2">
                Pipeline Deployed Successfully!
              </h2>
              <p className="text-zinc-500 dark:text-zinc-400 mb-4">
                Your CDC pipeline is now running. Data is being replicated to S3.
              </p>
              <div className="flex gap-3">
                <Button asChild className="bg-gradient-to-r from-violet-600 to-indigo-600">
                  <a href={`/pipelines/${state.pipelineId || state.sessionId || 'latest'}`}>View Pipeline Status</a>
                </Button>
                <Button variant="outline" asChild>
                  <a href="/pipelines">All Pipelines</a>
          </Button>
              </div>
            </motion.div>
          )}

          {/* Error State */}
          {state.error && (
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              className="p-4 rounded-lg bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-800"
            >
              <div className="flex items-center gap-2 text-red-600 dark:text-red-400">
                <XCircle className="h-5 w-5" />
                <span className="font-medium">Error</span>
            </div>
              <p className="mt-1 text-sm text-red-500">{state.error}</p>
              <Button
                variant="outline"
                size="sm"
                className="mt-3"
                onClick={() => setState(prev => ({ ...prev, error: null, stage: "source_selection" }))}
              >
                Try Again
              </Button>
            </motion.div>
          )}
        </div>
      </ScrollArea>
      </div>
  )
}

