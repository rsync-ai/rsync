"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { usePathname, useRouter, useSearchParams } from "next/navigation"
import { PanelRightOpen, PanelRightClose } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"

import { AgenticPipelineHome } from "@/components/chat/AgenticPipelineHome"
import {
  PipelineAccordionView,
  type PipelineState as AuthoritativePipelineState,
} from "@/components/chat/PipelineAccordionView"
import { PipelineMonitoringPanel } from "@/components/pipeline/PipelineMonitoringPanel"
import { PipelineConnectionSelector } from "@/components/pipeline/PipelineConnectionSelector"
import { PipelineTableSelector } from "@/components/pipeline/PipelineTableSelector"
import { SuggestionsReviewDialog } from "@/components/chat/SuggestionsReviewDialog"
import { ChatRightPanel } from "@/components/chat/ChatRightPanel"
import { ActivePipelinesList } from "@/components/chat/ActivePipelinesList"
import { ChatMessageItem, type ChatMessage } from "@/components/chat/ChatMessageItem"
import { ChatInputBar } from "@/components/chat/ChatInputBar"
import { useActivePipelines } from "@/lib/hooks/useActivePipelines"
import { useChatPersistence } from "@/lib/hooks/useChatPersistence"
import { useHITLState } from "@/lib/hooks/useHITLState"

import { resetSessionId, sendChatMessage, type PipelinePlan } from "@/lib/api/chat"
import { getPipeline, type DestinationConfig } from "@/lib/api/pipelines"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import type { BlockingReason } from "@/lib/pipeline/stageDefinitions"
import type { DataLoadingStrategy } from "@/lib/pipeline/dataLoadingStrategy"
import { useOptimisticAction, usePipelineState } from "@/lib/pipeline/usePipelineState"

// Re-export so consumers can import the type from this file.
export type { ChatMessage as Message }

// Concrete alias used throughout this file.
type Message = ChatMessage

function msgId(prefix: string) {
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`
}

function asRecord(v: unknown): Record<string, unknown> {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : {}
}

// AgenticChatInterfaceV2 is the sole chat interface. The "V2" suffix is
// vestigial — there is no AgenticChatInterface (V1). Left named as-is to avoid a
// churn-only rename across imports; it is not evidence of a competing version.
export function AgenticChatInterfaceV2() {
  const router = useRouter()
  const pathname = usePathname()
  const searchParams = useSearchParams()
  const consumedPromptRef = useRef<string>("")

  const { state: pipelineState, dispatch, resolveHITL, resetPipeline } = usePipelineState()
  const optimisticAction = useOptimisticAction(dispatch)
  const { pipelines: activePipelines, addPipeline, updatePipeline, removePipeline, clearCompleted } =
    useActivePipelines()

  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState("")
  const [isSendingMessage, setIsSendingMessage] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const scrollAreaRootRef = useRef<React.ComponentRef<typeof ScrollArea>>(null)
  const stickToBottomRef = useRef<boolean>(true)
  const prevPipelineIdRef = useRef<string | null>(null)
  const autoOpenedHitlKeyRef = useRef<string>("")

  const [activeIntent, setActiveIntent] = useState<string>("")
  const [authoritativePipelineState, setAuthoritativePipelineState] =
    useState<AuthoritativePipelineState | null>(null)

  // Chat slot-fill state from the backend conversation machine.
  // Backend emits response.metadata.conversation_state =
  //   awaiting_source | awaiting_destination | awaiting_confirmation | idle
  // and response.data.pending_intent {source_type, destination_type}.
  // The wizard used to ignore both. Surface as a slot chip in the
  // composer so the user can see "Tell me the destination" instead of
  // staring at a vague prompt.
  const [chatSlot, setChatSlot] = useState<{
    state: string
    sourceType?: string
    destType?: string
  } | null>(null)

  // Right panel (DAG/Status/Timeline)
  const [rightPanelOpen, setRightPanelOpen] = useState(false)
  const [activeTab, setActiveTab] = useState<"pipeline" | "monitoring">("pipeline")

  // Resuming indicator shown after HITL resolution (Temporal processing lag)
  const [resumingFromHITL, setResumingFromHITL] = useState(false)
  const resumingTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const flashResumingState = useCallback(() => {
    setResumingFromHITL(true)
    if (resumingTimerRef.current) clearTimeout(resumingTimerRef.current)
    resumingTimerRef.current = setTimeout(() => setResumingFromHITL(false), 12000)
  }, [])

  const pipelineId = pipelineState.metadata.pipelineId ?? undefined
  const isPipelineDone =
    pipelineState.executionState === "completed" ||
    pipelineState.executionState === "failed" ||
    pipelineState.executionState === "cancelled"

  // ── Persistence ──────────────────────────────────────────────────────────
  const { persistUiState, loadPersistedUiState, appendPersistedMessage, clearPersistedState } =
    useChatPersistence<Message>()

  // ── HITL state + handlers ─────────────────────────────────────────────────
  const {
    suppressHitlUntilRef,
    lastBlockingKeyRef,
    unifiedSelectorOpen,
    setUnifiedSelectorOpen,
    connectionRequirements,
    setConnectionRequirements,
    tableSelectorOpen,
    setTableSelectorOpen,
    availableTables,
    setAvailableTables,
    suggestedTables,
    tableSourceType,
    setTableSourceType,
    tableHitlNodeId,
    setTableHitlNodeId,
    tableDiscoveryStatus,
    tableSourceDatabase,
    tableDiscoveryReason,
    suggestionsReviewOpen,
    selectedTablesForSuggestions,
    sourceConnectionIdForSuggestions,
    prefetchedSchemaForSuggestions,
    resetHITL,
    handleConnectorGeneration,
    handleOpenConnectionModal,
    handleOpenTableModal,
    handleTablesSelected,
    handleApplySuggestions,
    handleSkipSuggestions,
  } = useHITLState({
    pipelineId,
    pipelineState,
    authoritativePipelineMetadata: asRecord(authoritativePipelineState?.metadata),
    optimisticAction,
    resolveHITL,
    flashResumingState,
  })

  // ── Destination schema/database name for the table modal ──────────────────
  // The first-run table modal lets the user confirm/edit the destination
  // schema/database name (pre-filled with the seeded default). Source it from
  // the pipeline when the modal opens so it shows during chat creation — the
  // same way the pipeline detail page does.
  const [destinationConfig, setDestinationConfig] = useState<DestinationConfig | null>(null)
  const [destinationType, setDestinationType] = useState<string | undefined>(undefined)
  useEffect(() => {
    if (!pipelineId || !tableSelectorOpen) return
    let cancelled = false
    void (async () => {
      try {
        const p = await getPipeline(pipelineId)
        if (cancelled) return
        setDestinationConfig(
          p.destination_config ??
            (p.destination_namespace ? { namespace: p.destination_namespace } : null)
        )
        const destNode = Array.isArray(p.config?.nodes)
          ? p.config.nodes.find((n) => n.type === "destination")
          : undefined
        // Chat-created pipelines don't have config.nodes populated yet, so prefer
        // the embedded destination connection's connector_type for the label.
        setDestinationType(p.destination_connection?.connector_type || destNode?.connector || undefined)
      } catch {
        // Best-effort: if we can't load it, the modal still works without the field.
      }
    })()
    return () => {
      cancelled = true
    }
  }, [pipelineId, tableSelectorOpen])

  // ── Restore persisted chat on mount ───────────────────────────────────────
  useEffect(() => {
    if (typeof window === "undefined") return
    try {
      const { messages: persisted, activeIntent: persistedIntent } = loadPersistedUiState()
      if (persisted.length === 0 && !persistedIntent) return
      setMessages((prev) => (prev.length > 0 ? prev : persisted))
      if (persistedIntent) setActiveIntent((prev) => (prev ? prev : persistedIntent))
    } catch {
      // ignore
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loadPersistedUiState])

  // ── Autoscroll ────────────────────────────────────────────────────────────
  const scrollToBottom = useCallback((behavior: ScrollBehavior = "smooth") => {
    if (!stickToBottomRef.current) return
    messagesEndRef.current?.scrollIntoView({ behavior, block: "end" })
  }, [])

  useEffect(() => {
    const root = scrollAreaRootRef.current
    const viewport = root?.querySelector?.("[data-radix-scroll-area-viewport]") as HTMLElement | null
    if (!viewport) return
    const thresholdPx = 160
    const onScroll = () => {
      stickToBottomRef.current =
        viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight <= thresholdPx
    }
    viewport.addEventListener("scroll", onScroll, { passive: true })
    onScroll()
    return () => viewport.removeEventListener("scroll", onScroll)
  }, [])

  useEffect(() => {
    const prev = prevPipelineIdRef.current
    prevPipelineIdRef.current = pipelineId || null
    if (!prev && pipelineId) {
      stickToBottomRef.current = true
      requestAnimationFrame(() =>
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" })
      )
      return
    }
    scrollToBottom("smooth")
  }, [messages, pipelineId, authoritativePipelineState?.status, authoritativePipelineState?.current_stage, scrollToBottom])

  // ── Persist state whenever messages / intent change ───────────────────────
  useEffect(() => {
    persistUiState({ messages, activeIntent })
  }, [activeIntent, messages, persistUiState])

  // ── addMessage ────────────────────────────────────────────────────────────
  const addMessage = useCallback(
    (
      role: Message["role"],
      content: string,
      opts?: {
        suggestions?: string[]
        responseType?: string
        plan?: PipelinePlan
        strategy?: DataLoadingStrategy
        confirmationContext?: Message["confirmationContext"]
        diagnosis?: Message["diagnosis"]
      }
    ) => {
      const msg: Message = {
        id: msgId(role),
        role,
        content,
        suggestions: opts?.suggestions,
        responseType: opts?.responseType,
        plan: opts?.plan,
        strategy: opts?.strategy,
        confirmationContext: opts?.confirmationContext,
        diagnosis: opts?.diagnosis,
      }
      const next = appendPersistedMessage(msg, activeIntent)
      setMessages(next)
    },
    [activeIntent, appendPersistedMessage]
  )

  // ── startNew ──────────────────────────────────────────────────────────────
  const startNew = useCallback(() => {
    if (resumingTimerRef.current) clearTimeout(resumingTimerRef.current)
    setResumingFromHITL(false)
    resetPipeline()
    resetSessionId()
    resetHITL()
    setMessages([])
    setInput("")
    setActiveIntent("")
    setAuthoritativePipelineState(null)
    setRightPanelOpen(false)
    setChatSlot(null)
    clearPersistedState()
  }, [resetPipeline, resetHITL, clearPersistedState])

  // Defer runPrompt resolution to break the temporal-dead-zone cycle
  // (URL handler effect runs before runPrompt is declared below).
  const runPromptRef = useRef<((prompt: string) => Promise<void>) | null>(null)

  // ── URL ?prompt= deep-link (with optional ?autosend=1 to auto-submit) ─────
  useEffect(() => {
    const promptFromUrl = searchParams.get("prompt") || ""
    const autoSend = searchParams.get("autosend") === "1"
    if (!promptFromUrl || consumedPromptRef.current === promptFromUrl || pipelineId) return
    consumedPromptRef.current = promptFromUrl
    setInput(promptFromUrl)
    try {
      const params = new URLSearchParams(searchParams)
      params.delete("prompt")
      params.delete("autosend")
      const next = params.toString()
      router.replace(next ? `${pathname}?${next}` : pathname)
    } catch {
      // ignore
    }
    if (autoSend) {
      // Defer to next tick so router.replace + setInput settle, and runPromptRef
      // is populated by the assignment further down in this component.
      setTimeout(() => {
        setInput("")
        const fn = runPromptRef.current
        if (fn) void fn(promptFromUrl)
      }, 0)
    }
  }, [pipelineId, pathname, router, searchParams])

  // ── Authoritative state polling (2 s) ─────────────────────────────────────
  useEffect(() => {
    if (!pipelineId) {
      setAuthoritativePipelineState(null)
      return
    }

    let cancelled = false

    const poll = async () => {
      try {
        const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/state`, {
          cache: "no-store",
        })
        if (!res.ok) return
        const raw: unknown = await res.json()
        if (cancelled) return

        setAuthoritativePipelineState(raw as AuthoritativePipelineState)

        const data = asRecord(raw)
        const status = String(data["status"] || "")
        const stageFromState = String(data["current_stage"] || "unknown")
        const summary = String(data["summary"] || data["error"] || "")
        const pipelineName = String(data["name"] || "")

        updatePipeline(pipelineId, { status, name: pipelineName || undefined })

        if (
          typeof data["execution_id"] === "string" &&
          data["execution_id"] &&
          pipelineState.metadata.executionId !== data["execution_id"]
        ) {
          dispatch({
            type: "METADATA_UPDATE",
            payload: { metadata: { executionId: String(data["execution_id"]) }, timestamp: Date.now() },
          })
        }

        if (status === "failed") {
          dispatch({
            type: "PIPELINE_FAILED",
            payload: { stage: stageFromState, error: summary || "Pipeline failed", timestamp: Date.now() },
          })
          return
        }
        if (status === "completed") {
          dispatch({ type: "PIPELINE_COMPLETED", payload: { summary: data, timestamp: Date.now() } })
          return
        }
        if (status === "processing" && pipelineState.executionState !== "running") {
          dispatch({
            type: "EXECUTION_STATE_CHANGE",
            payload: { executionState: "running", timestamp: Date.now() },
          })
        }

        // HITL hydration from authoritative state
        const blocking = asRecord(data["blocking_reason"])
        const blockingType = String(blocking["type"] || "")
        const blockingDesc = String(blocking["description"] || "")

        if (status === "waiting_for_user" && blockingType) {
          if (Date.now() < suppressHitlUntilRef.current) return

          const stage = String(data["current_stage"] || pipelineState.hitl?.stage || "unknown")
          const details = asRecord(blocking["details"])
          const blockingReason: BlockingReason = {
            type: blockingType,
            description: blockingDesc || "Waiting for user input",
            details,
          }

          const key = `${stage}:${blockingType}:${blockingDesc}`
          if (!pipelineState.hitl || lastBlockingKeyRef.current !== key) {
            lastBlockingKeyRef.current = key
            dispatch({ type: "HITL_REQUIRED", payload: { stage, blockingReason, timestamp: Date.now() } })
          }
        } else {
          if (pipelineState.hitl) {
            lastBlockingKeyRef.current = ""
            dispatch({ type: "HITL_RESOLVED", payload: { timestamp: Date.now() } })
          }
        }
      } catch {
        // Best-effort polling; ignore errors
      }
    }

    const interval = setInterval(poll, 2000)
    poll()
    return () => {
      cancelled = true
      clearInterval(interval)
    }
  }, [dispatch, pipelineId, pipelineState.executionState, pipelineState.hitl, pipelineState.metadata.executionId, suppressHitlUntilRef, lastBlockingKeyRef, updatePipeline])

  // ── Auto-open HITL modals when blocking reason changes ───────────────────
  useEffect(() => {
    if (!pipelineState.hitl) {
      autoOpenedHitlKeyRef.current = ""
      return
    }
    const { blockingReason, stage } = pipelineState.hitl
    const type = blockingReason.type.toLowerCase()
    const key = `${stage}:${type}`
    if (autoOpenedHitlKeyRef.current === key) return
    autoOpenedHitlKeyRef.current = key
    if (type === "connection_config" || type === "user_input_required") {
      handleOpenConnectionModal()
    } else if (type === "table_selection") {
      handleOpenTableModal()
    } else if (type === "connector_generation") {
      handleConnectorGeneration()
    }
  }, [pipelineState.hitl, handleOpenConnectionModal, handleOpenTableModal, handleConnectorGeneration])

  // ── handleRefreshPipelineState ────────────────────────────────────────────
  const handleRefreshPipelineState = useCallback(async () => {
    if (!pipelineId) return
    try {
      const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/state`, {
        cache: "no-store",
      })
      if (!res.ok) return
      const raw: unknown = await res.json()
      setAuthoritativePipelineState(raw as AuthoritativePipelineState)
    } catch {
      // ignore
    }
  }, [pipelineId])

  const handleCancelPipeline = useCallback(async () => {
    if (!pipelineId) return
    try {
      await authFetch(API_ENDPOINTS.PIPELINES.STOP(pipelineId), { method: "POST" })
    } catch {
      // ignore
    }
  }, [pipelineId])

  // ── runPrompt ─────────────────────────────────────────────────────────────
  const runPrompt = useCallback(
    async (promptRaw: string, displayText?: string) => {
      const prompt = String(promptRaw || "").trim()
      if (!prompt) return

      // The backend receives `prompt` (which may be a machine command like
      // "Yes sync_mode=batch"); the user-facing chat + intent show `display`.
      const display = String(displayText || "").trim() || prompt

      setActiveIntent(display)
      try {
        const cur = loadPersistedUiState()
        persistUiState({ messages: cur.messages, activeIntent: display })
      } catch {
        // ignore
      }
      addMessage("user", display)
      setIsSendingMessage(true)

      try {
        const resp = await sendChatMessage({ message: prompt })
        const rawSuggestions = Array.isArray(resp.suggestions) ? resp.suggestions : []

        const data = asRecord(resp.data)
        const plan = (data["plan"] || undefined) as PipelinePlan | undefined
        const pendingIntent = asRecord(data["pending_intent"])
        const sourceType = String(data["source_type"] || pendingIntent["source_type"] || "").trim()
        const destType = String(data["destination_type"] || pendingIntent["destination_type"] || "").trim()
        const supportedSyncModesRaw = data["supported_sync_modes"]
        const supportedSyncModes = Array.isArray(supportedSyncModesRaw)
          ? supportedSyncModesRaw.map((v: unknown) => String(v || "").trim().toLowerCase()).filter(Boolean)
          : []
        const sourceSupportsCDC = Boolean(data["source_supports_cdc"])
        const sourceSupportsIncrementalBatch = Boolean(data["source_supports_incremental_batch"])
        const effectiveSupportedModes =
          supportedSyncModes.length > 0
            ? supportedSyncModes
            : sourceSupportsCDC
              ? ["batch", "cdc"]
              : ["batch"]

        const suggestions =
          resp.type === "confirmation"
            ? effectiveSupportedModes.includes("cdc")
              ? []
              : rawSuggestions.length === 0
                ? ["Yes", "No"]
                : rawSuggestions
            : rawSuggestions

        const syncModeRaw = String(
          pendingIntent["sync_mode"] ||
            pendingIntent["SyncMode"] ||
            data["sync_mode"] ||
            data["mode"] ||
            ""
        )
          .trim()
          .toLowerCase()
        const mode: "cdc" | "batch" = syncModeRaw === "cdc" ? "cdc" : "batch"
        const strategy: DataLoadingStrategy | undefined =
          resp.type === "confirmation"
            ? {
                mode,
                effective_sync_mode: mode,
                effective_cdc_mode: mode === "cdc" ? "initial" : "",
                rerun_default: mode === "cdc" ? "" : "resume",
              }
            : undefined

        // For diagnosis responses, surface the typed action payload so
        // ChatMessageItem can render the DiagnosisCard with a real button
        // instead of the markdown link in the message body.
        const diagnosis =
          resp.type === "diagnosis"
            ? ({
                execution_id: String(data["execution_id"] || ""),
                pipeline_id: String(data["pipeline_id"] || ""),
                pipeline_name: String(data["pipeline_name"] || ""),
                status: String(data["status"] || ""),
                category: String(data["category"] || ""),
                action: String(data["action"] || ""),
                action_label: String(data["action_label"] || ""),
                deep_link: String(data["deep_link"] || ""),
                confidence: typeof data["confidence"] === "number" ? (data["confidence"] as number) : 0,
                rationale: String(data["rationale"] || ""),
                source_rows: typeof data["source_rows"] === "number" ? (data["source_rows"] as number) : undefined,
                landed_rows: typeof data["landed_rows"] === "number" ? (data["landed_rows"] as number) : undefined,
              })
            : undefined

        addMessage("assistant", resp.message || "Working on it…", {
          suggestions,
          responseType: resp.type,
          plan,
          strategy,
          confirmationContext:
            resp.type === "confirmation" && sourceType && destType
              ? {
                  sourceType,
                  destType,
                  supportedSyncModes: effectiveSupportedModes,
                  sourceSupportsCDC,
                  sourceSupportsIncrementalBatch,
                }
              : undefined,
          diagnosis,
        })

        const metadata = asRecord(resp.metadata)

        // Slot-fill state — surface "what is chat waiting for?" in composer.
        const convState = String(metadata["conversation_state"] || "").trim()
        if (convState && convState !== "idle") {
          const pi = asRecord(data["pending_intent"])
          setChatSlot({
            state: convState,
            sourceType: String(pi["source_type"] || pi["SourceType"] || "") || undefined,
            destType: String(pi["destination_type"] || pi["DestinationType"] || "") || undefined,
          })
        } else {
          setChatSlot(null)
        }

        const pid =
          String(data["pipeline_id"] || data["pipelineId"] || "") ||
          String(metadata["pipeline_id"] || metadata["pipelineId"] || "")
        const execId = String(data["execution_id"] || data["executionId"] || "")

        if (pid) {
          dispatch({
            type: "PIPELINE_STARTED",
            payload: { pipelineId: pid, executionId: execId || pid, timestamp: Date.now() },
          })
          addPipeline(pid, undefined, "processing")
        }
      } catch {
        addMessage("assistant", "Failed to start pipeline. Please try again.")
      } finally {
        setIsSendingMessage(false)
      }
    },
    [addMessage, addPipeline, dispatch, loadPersistedUiState, persistUiState]
  )

  // Expose runPrompt to the URL deep-link effect declared above.
  useEffect(() => {
    runPromptRef.current = runPrompt
  }, [runPrompt])

  const handleSubmit = useCallback(
    async (e: React.FormEvent) => {
      e.preventDefault()
      const prompt = input
      setInput("")
      await runPrompt(prompt)
    },
    [input, runPrompt]
  )

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div className="flex h-full min-h-0 w-full">
      {/* Main chat area */}
      <div className="flex h-full min-h-0 flex-1 flex-col overflow-hidden bg-white dark:bg-zinc-950">

        {/* Connection HITL modal */}
        {connectionRequirements && pipelineId && (
          <PipelineConnectionSelector
            isOpen={unifiedSelectorOpen}
            onSuccess={() => {
              suppressHitlUntilRef.current = Date.now() + 20000
              lastBlockingKeyRef.current = ""
              resolveHITL()
              flashResumingState()
            }}
            onClose={() => {
              setUnifiedSelectorOpen(false)
              setConnectionRequirements(null)
            }}
            pipelineId={pipelineId}
            executionId={pipelineState.metadata.executionId || undefined}
            requirements={connectionRequirements}
            userRequest={
              typeof pipelineState.hitl?.blockingReason?.details?.user_request === "string"
                ? pipelineState.hitl.blockingReason.details.user_request
                : undefined
            }
            suggestions={
              Array.isArray(pipelineState.hitl?.blockingReason?.details?.suggestions)
                ? pipelineState.hitl?.blockingReason?.details?.suggestions
                : undefined
            }
            title="Configure Pipeline Connections"
          />
        )}

        {/* Table HITL modal */}
        {pipelineId && (
          <PipelineTableSelector
            isOpen={tableSelectorOpen}
            autoDismissOnResolve
            onClose={() => {
              setTableSelectorOpen(false)
              setAvailableTables([])
              setTableSourceType("")
              setTableHitlNodeId("")
            }}
            onResolved={() => {
              suppressHitlUntilRef.current = Date.now() + 15000
              lastBlockingKeyRef.current = ""
              resolveHITL()
              flashResumingState()
            }}
            onTablesSelected={handleTablesSelected}
            pipelineId={pipelineId}
            executionId={pipelineState.metadata.executionId || undefined}
            sourceType={tableSourceType}
            sourceConnectionId={(() => {
              const hitlDetails = asRecord(pipelineState.hitl?.blockingReason?.details)
              const meta = asRecord(authoritativePipelineState?.metadata)
              return String(hitlDetails["source_connection_id"] || meta["source_connection_id"] || "")
            })()}
            availableTables={availableTables}
            suggestedTables={suggestedTables}
            discoveryStatus={tableDiscoveryStatus}
            sourceDatabase={tableSourceDatabase}
            discoveryReason={tableDiscoveryReason}
            destinationConfig={destinationConfig}
            destinationType={destinationType}
          />
        )}

        {/* Suggestions review modal */}
        {pipelineId && sourceConnectionIdForSuggestions && suggestionsReviewOpen && (
          <SuggestionsReviewDialog
            isOpen={suggestionsReviewOpen}
            // Pass the promise-returning handler directly (not `void`-wrapped) so the
            // dialog can await the skip-resume, show a spinner, and keep itself open
            // with the user's selection intact if the resume fails.
            onClose={handleSkipSuggestions}
            pipelineId={pipelineId}
            executionId={pipelineState.metadata.executionId || undefined}
            sourceConnectionId={sourceConnectionIdForSuggestions}
            selectedTables={selectedTablesForSuggestions}
            onApplyContinue={handleApplySuggestions}
            prefetchedSchema={
              prefetchedSchemaForSuggestions.length > 0
                ? prefetchedSchemaForSuggestions
                : undefined
            }
          />
        )}

        <ScrollArea ref={scrollAreaRootRef} className="flex-1 p-4">
          {!pipelineId && messages.length === 0 && !activeIntent && !isSendingMessage ? (
            <div>
              <AgenticPipelineHome onSubmit={(p) => void runPrompt(p)} onRunPipeline={() => {}} />
            </div>
          ) : (
            <div className="space-y-4">
              {!pipelineId && (messages.length > 0 || activeIntent || isSendingMessage) && (
                <div className="flex justify-end">
                  <Button variant="outline" size="sm" onClick={startNew}>
                    Start new
                  </Button>
                </div>
              )}

              {messages.length > 0 && (
                <div className="space-y-3">
                  {messages.map((m) => (
                    <ChatMessageItem
                      key={m.id}
                      message={m}
                      onRunPrompt={(p, d) => void runPrompt(p, d)}
                      onSetInput={setInput}
                      onSyncModeChosen={(label) =>
                        setMessages((prev) => {
                          const next = prev.map((msg) =>
                            msg.id === m.id ? { ...msg, syncModeChosen: label } : msg
                          )
                          persistUiState({ messages: next, activeIntent })
                          return next
                        })
                      }
                    />
                  ))}
                </div>
              )}

              {pipelineId && (
                <div className="space-y-3">
                  <div className="flex items-center justify-end gap-2">
                    <ActivePipelinesList
                      pipelines={activePipelines}
                      currentPipelineId={pipelineId}
                      onSelect={(id) => router.push(`/pipelines/${id}?from=chat`)}
                      onRemove={removePipeline}
                      onClearCompleted={clearCompleted}
                    />
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setRightPanelOpen(!rightPanelOpen)}
                      className="gap-1"
                    >
                      {rightPanelOpen ? (
                        <>
                          <PanelRightClose className="h-4 w-4" />
                          <span className="hidden sm:inline">Hide Panel</span>
                        </>
                      ) : (
                        <>
                          <PanelRightOpen className="h-4 w-4" />
                          <span className="hidden sm:inline">Show DAG</span>
                        </>
                      )}
                    </Button>
                    <Button variant="outline" size="sm" onClick={startNew}>
                      Start new
                    </Button>
                  </div>

                  <Tabs
                    value={activeTab}
                    onValueChange={(v) => setActiveTab(v as "pipeline" | "monitoring")}
                    className="w-full"
                  >
                    <TabsList>
                      <TabsTrigger value="pipeline">Pipeline</TabsTrigger>
                      <TabsTrigger value="monitoring">Monitoring</TabsTrigger>
                    </TabsList>

                    <TabsContent value="pipeline" className="space-y-4">
                      {authoritativePipelineState ? (
                        <PipelineAccordionView
                          key={`${authoritativePipelineState.pipeline_id}:${authoritativePipelineState.execution_id || ""}:${authoritativePipelineState.status}:${authoritativePipelineState.current_stage || ""}`}
                          state={authoritativePipelineState}
                          userIntent={activeIntent || "Start a pipeline"}
                          onRefresh={handleRefreshPipelineState}
                          onCancel={handleCancelPipeline}
                          onSelectTables={(details) => handleOpenTableModal(details)}
                          onConfigureConnections={() => handleOpenConnectionModal()}
                          onGenerateConnectors={(details) => void handleConnectorGeneration(details)}
                          onViewMonitoring={() => setActiveTab("monitoring")}
                          resumingFromHITL={resumingFromHITL}
                        />
                      ) : (
                        <Card className="p-5">
                          <div className="text-sm text-zinc-600 dark:text-zinc-400">
                            Loading pipeline status…
                          </div>
                        </Card>
                      )}
                    </TabsContent>

                    <TabsContent value="monitoring" className="space-y-4">
                      <PipelineMonitoringPanel pipelineId={pipelineId} />
                    </TabsContent>
                  </Tabs>
                </div>
              )}
            </div>
          )}
          <div ref={messagesEndRef} />
        </ScrollArea>

        {/* Slot indicator — surfaces backend conversation_state so the
            user can see what we're waiting on (e.g. "Tell me the source")
            without having to re-read the assistant's last message. */}
        {chatSlot && !pipelineId && (
          <div className="border-t px-4 pt-2 -mb-2">
            <div className="inline-flex items-center gap-2 rounded-full bg-violet-50 dark:bg-violet-950/30 px-2.5 py-1 text-[11px] text-violet-800 dark:text-violet-200">
              <span className="h-1.5 w-1.5 rounded-full bg-violet-500 animate-pulse" />
              {chatSlot.state === "awaiting_source" && "Waiting for source connector"}
              {chatSlot.state === "awaiting_destination" && (
                <>
                  Waiting for destination
                  {chatSlot.sourceType ? (
                    <>
                      {" "}— source set: <span className="font-mono">{chatSlot.sourceType}</span>
                    </>
                  ) : null}
                </>
              )}
              {chatSlot.state === "awaiting_confirmation" && (
                <>
                  Waiting for your confirmation
                  {chatSlot.sourceType && chatSlot.destType ? (
                    <>
                      {" "}—{" "}
                      <span className="font-mono">{chatSlot.sourceType}</span>
                      {" → "}
                      <span className="font-mono">{chatSlot.destType}</span>
                    </>
                  ) : null}
                </>
              )}
              {!["awaiting_source", "awaiting_destination", "awaiting_confirmation"].includes(
                chatSlot.state,
              ) && <>State: <span className="font-mono">{chatSlot.state}</span></>}
            </div>
          </div>
        )}

        <form onSubmit={handleSubmit} className="border-t p-4">
          <ChatInputBar
            input={input}
            onInputChange={setInput}
            onSubmit={handleSubmit}
            isSendingMessage={isSendingMessage}
            pipelineId={pipelineId}
            isPipelineDone={isPipelineDone}
            executionFailed={pipelineState.executionState === "failed"}
            onStartNew={startNew}
            onViewPipeline={() => router.push(`/pipelines/${pipelineId}`)}
            onRetry={() => {
              startNew()
              setTimeout(() => setInput(activeIntent), 50)
            }}
          />
        </form>
      </div>

      {/* Right Panel — DAG/Status/Timeline */}
      {pipelineId && (
        <ChatRightPanel
          pipelineId={pipelineId}
          isOpen={rightPanelOpen}
          onClose={() => setRightPanelOpen(false)}
        />
      )}
    </div>
  )
}
