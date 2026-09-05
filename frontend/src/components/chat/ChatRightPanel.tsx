"use client"

import { useState, useCallback, useEffect } from "react"
import { useRouter } from "next/navigation"
import { motion, AnimatePresence } from "framer-motion"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent } from "@/components/ui/card"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
  Activity,
  GitBranch,
  Clock,
  ExternalLink,
  RefreshCw,
  CheckCircle2,
  XCircle,
  Loader2,
  X,
  AlertTriangle,
  ChevronRight,
} from "lucide-react"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import {
  DAGVisualization,
  LinearTimeline,
  type ExecutionPlan,
  type ExecutionPlanStage,
} from "@/components/pipeline/DAGVisualization"
import { NodeInspector } from "@/components/pipeline/NodeInspector"
import { formatRelativeTime } from "@/lib/utils"

interface ChatRightPanelProps {
  pipelineId: string
  isOpen: boolean
  onClose: () => void
}

export function ChatRightPanel({ pipelineId, isOpen, onClose }: ChatRightPanelProps) {
  const router = useRouter()
  const [activeTab, setActiveTab] = useState<string>("status")
  const [executionPlan, setExecutionPlan] = useState<ExecutionPlan | null>(null)
  const [pipelineState, setPipelineState] = useState<Record<string, any> | null>(null)
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  const fetchPipelineData = useCallback(async () => {
    if (!pipelineId) return

    try {
      const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/state`, {
        cache: "no-store",
      })
      if (!res.ok) return

      const data = await res.json()
      setPipelineState(data)

      if (data.execution_plan) {
        const plan =
          typeof data.execution_plan === "string"
            ? JSON.parse(data.execution_plan)
            : data.execution_plan
        setExecutionPlan(plan)
      }
    } catch {
      // Ignore errors for now
    } finally {
      setLoading(false)
    }
  }, [pipelineId])

  useEffect(() => {
    if (isOpen && pipelineId) {
      fetchPipelineData()
    }
  }, [isOpen, pipelineId, fetchPipelineData])

  // Poll for updates while panel is open and pipeline is running
  useEffect(() => {
    if (!isOpen || !pipelineId) return

    const status = pipelineState?.status
    if (status === "completed" || status === "failed") return

    const interval = setInterval(fetchPipelineData, 3000)
    return () => clearInterval(interval)
  }, [isOpen, pipelineId, pipelineState?.status, fetchPipelineData])

  const handleNodeClick = useCallback((nodeId: string) => {
    setSelectedNodeId(nodeId)
    setActiveTab("inspector")
  }, [])

  const handleCloseInspector = useCallback(() => {
    setSelectedNodeId(null)
    setActiveTab("dag")
  }, [])

  const handleViewFullPage = useCallback(() => {
    router.push(`/pipelines/${pipelineId}?from=chat`)
  }, [router, pipelineId])

  const selectedNode = executionPlan?.stages?.find((s) => s.id === selectedNodeId)

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "completed":
        return <CheckCircle2 className="h-4 w-4 text-green-500" />
      case "failed":
        return <XCircle className="h-4 w-4 text-red-500" />
      case "running":
      case "processing":
        return <Loader2 className="h-4 w-4 text-blue-500 animate-spin" />
      default:
        return <Clock className="h-4 w-4 text-zinc-400" />
    }
  }

  const getStatusBadge = (status: string) => {
    const variants: Record<string, "default" | "secondary" | "destructive"> = {
      completed: "default",
      failed: "destructive",
      running: "secondary",
      processing: "secondary",
    }
    return (
      <Badge variant={variants[status] || "secondary"}>
        {status?.charAt(0).toUpperCase() + status?.slice(1) || "Unknown"}
      </Badge>
    )
  }

  if (!isOpen) return null

  return (
    <AnimatePresence>
      <motion.div
        initial={{ width: 0, opacity: 0 }}
        animate={{ width: selectedNodeId ? 600 : 360, opacity: 1 }}
        exit={{ width: 0, opacity: 0 }}
        transition={{ duration: 0.2 }}
        className="h-full border-l border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 flex"
      >
        {/* Main Panel Content */}
        <div className={`${selectedNodeId ? "w-[300px]" : "flex-1"} flex flex-col`}>
          {/* Header */}
          <div className="flex items-center justify-between p-3 border-b border-zinc-200 dark:border-zinc-800">
            <div className="flex items-center gap-2">
              {getStatusIcon(pipelineState?.status || "")}
              <span className="text-sm font-medium text-zinc-900 dark:text-white">
                Pipeline Status
              </span>
            </div>
            <div className="flex items-center gap-1">
              <Button
                variant="ghost"
                size="sm"
                onClick={fetchPipelineData}
                className="h-7 w-7 p-0"
              >
                <RefreshCw className="h-3.5 w-3.5" />
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={onClose}
                className="h-7 w-7 p-0"
              >
                <X className="h-3.5 w-3.5" />
              </Button>
            </div>
          </div>

          {/* Tabs */}
          <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col">
            <TabsList className="w-full justify-start px-3 pt-2 bg-transparent">
              <TabsTrigger value="status" className="text-xs">
                <Activity className="h-3.5 w-3.5 mr-1" />
                Status
              </TabsTrigger>
              <TabsTrigger value="dag" className="text-xs">
                <GitBranch className="h-3.5 w-3.5 mr-1" />
                DAG
              </TabsTrigger>
              <TabsTrigger value="timeline" className="text-xs">
                <Clock className="h-3.5 w-3.5 mr-1" />
                Timeline
              </TabsTrigger>
            </TabsList>

            {/* Status Tab */}
            <TabsContent value="status" className="flex-1 m-0">
              <ScrollArea className="h-full">
                <div className="p-3 space-y-4">
                  {loading ? (
                    <div className="flex items-center justify-center py-8">
                      <Loader2 className="h-6 w-6 animate-spin text-violet-600" />
                    </div>
                  ) : (
                    <>
                      {/* Status Card */}
                      <Card>
                        <CardContent className="p-3 space-y-3">
                          <div className="flex items-center justify-between">
                            <span className="text-xs text-zinc-500">Status</span>
                            {getStatusBadge(pipelineState?.status || "")}
                          </div>
                          {pipelineState?.current_stage && (
                            <div className="flex items-center justify-between">
                              <span className="text-xs text-zinc-500">Current Stage</span>
                              <span className="text-xs font-medium text-zinc-900 dark:text-white">
                                {pipelineState.current_stage}
                              </span>
                            </div>
                          )}
                          {pipelineState?.started_at && (
                            <div className="flex items-center justify-between">
                              <span className="text-xs text-zinc-500">Started</span>
                              <span className="text-xs text-zinc-600 dark:text-zinc-400">
                                {formatRelativeTime(new Date(pipelineState.started_at))}
                              </span>
                            </div>
                          )}
                        </CardContent>
                      </Card>

                      {/* Quick Stages Overview */}
                      {executionPlan?.stages && executionPlan.stages.length > 0 && (
                        <div>
                          <h4 className="text-xs font-medium text-zinc-500 mb-2">Stages</h4>
                          <div className="space-y-1">
                            {executionPlan.stages.slice(0, 5).map((stage) => (
                              <button
                                key={stage.id}
                                onClick={() => handleNodeClick(stage.id)}
                                className="w-full flex items-center justify-between p-2 rounded-md bg-zinc-50 dark:bg-zinc-800 hover:bg-zinc-100 dark:hover:bg-zinc-700 transition-colors"
                              >
                                <div className="flex items-center gap-2">
                                  {getStatusIcon(stage.status)}
                                  <span className="text-xs font-medium text-zinc-900 dark:text-white truncate max-w-[160px]">
                                    {stage.display_name}
                                  </span>
                                </div>
                                <ChevronRight className="h-3.5 w-3.5 text-zinc-400" />
                              </button>
                            ))}
                            {executionPlan.stages.length > 5 && (
                              <div className="text-xs text-zinc-500 text-center py-1">
                                +{executionPlan.stages.length - 5} more stages
                              </div>
                            )}
                          </div>
                        </div>
                      )}

                      {/* Error Display */}
                      {pipelineState?.error && (
                        <Card className="border-red-200 dark:border-red-800">
                          <CardContent className="p-3">
                            <div className="flex items-start gap-2">
                              <AlertTriangle className="h-4 w-4 text-red-500 flex-shrink-0 mt-0.5" />
                              <div>
                                <p className="text-xs font-medium text-red-600 dark:text-red-400">
                                  Error
                                </p>
                                <p className="text-xs text-red-500 mt-1">
                                  {pipelineState.error}
                                </p>
                              </div>
                            </div>
                          </CardContent>
                        </Card>
                      )}
                    </>
                  )}
                </div>
              </ScrollArea>
            </TabsContent>

            {/* DAG Tab */}
            <TabsContent value="dag" className="flex-1 m-0 overflow-hidden">
              <ScrollArea className="h-full">
                <div className="p-3">
                  {loading ? (
                    <div className="flex items-center justify-center py-8">
                      <Loader2 className="h-6 w-6 animate-spin text-violet-600" />
                    </div>
                  ) : executionPlan?.stages && executionPlan.stages.length > 0 ? (
                    <DAGVisualization
                      stages={executionPlan.stages}
                      compact
                      onNodeClick={handleNodeClick}
                      selectedNodeId={selectedNodeId}
                    />
                  ) : (
                    <div className="flex flex-col items-center justify-center py-8 text-center">
                      <GitBranch className="h-8 w-8 text-zinc-300 mb-2" />
                      <p className="text-xs text-zinc-500">No DAG available yet</p>
                    </div>
                  )}
                </div>
              </ScrollArea>
            </TabsContent>

            {/* Timeline Tab */}
            <TabsContent value="timeline" className="flex-1 m-0 overflow-hidden">
              <ScrollArea className="h-full">
                <div className="p-3">
                  {loading ? (
                    <div className="flex items-center justify-center py-8">
                      <Loader2 className="h-6 w-6 animate-spin text-violet-600" />
                    </div>
                  ) : executionPlan?.stages && executionPlan.stages.length > 0 ? (
                    <LinearTimeline
                      stages={executionPlan.stages}
                      compact
                      onStageClick={handleNodeClick}
                      selectedStageId={selectedNodeId}
                    />
                  ) : (
                    <div className="flex flex-col items-center justify-center py-8 text-center">
                      <Clock className="h-8 w-8 text-zinc-300 mb-2" />
                      <p className="text-xs text-zinc-500">No timeline available yet</p>
                    </div>
                  )}
                </div>
              </ScrollArea>
            </TabsContent>
          </Tabs>

          {/* Footer */}
          <div className="p-3 border-t border-zinc-200 dark:border-zinc-800">
            <Button
              onClick={handleViewFullPage}
              variant="outline"
              size="sm"
              className="w-full text-xs"
            >
              <ExternalLink className="h-3.5 w-3.5 mr-1.5" />
              View Full Details
            </Button>
          </div>
        </div>

        {/* Node Inspector (slides in when node is selected) */}
        <AnimatePresence>
          {selectedNodeId && selectedNode && (
            <motion.div
              initial={{ width: 0, opacity: 0 }}
              animate={{ width: 300, opacity: 1 }}
              exit={{ width: 0, opacity: 0 }}
              transition={{ duration: 0.15 }}
              className="overflow-hidden"
            >
              <NodeInspector
                node={selectedNode}
                pipelineId={pipelineId}
                onClose={handleCloseInspector}
              />
            </motion.div>
          )}
        </AnimatePresence>
      </motion.div>
    </AnimatePresence>
  )
}
