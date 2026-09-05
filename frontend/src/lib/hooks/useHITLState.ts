"use client"

import { useCallback, useRef, useState } from "react"
import { resumePipelineConnectors, resumePipelineNodeInput, resumePipelineTables, type DestinationConfig } from "@/lib/api/pipelines"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { useOptimisticAction } from "@/lib/pipeline/usePipelineState"
import type { TransformSuggestion } from "@/lib/api/suggestions"
import type { PipelineUIState } from "@/lib/pipeline/pipelineStateReducer"
import { normalizeSuggestedTables, type AvailableTable, type SuggestedTable } from "@/components/pipeline/PipelineTableSelector"
import type { TableMetadata } from "@/components/chat/SuggestionsReviewDialog"

function asRecord(v: unknown): Record<string, unknown> {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : {}
}

interface UseHITLStateParams {
  pipelineId: string | undefined
  pipelineState: PipelineUIState
  authoritativePipelineMetadata: Record<string, unknown> | null
  optimisticAction: ReturnType<typeof useOptimisticAction>
  resolveHITL: () => void
  flashResumingState: () => void
}

export function useHITLState({
  pipelineId,
  pipelineState,
  authoritativePipelineMetadata,
  optimisticAction,
  resolveHITL,
  flashResumingState,
}: UseHITLStateParams) {
  // Refs — exported so the polling loop can mutate them
  const suppressHitlUntilRef = useRef<number>(0)
  const lastBlockingKeyRef = useRef<string>("")
  // Destination schema/database name chosen in the table modal. Captured when the
  // user confirms table selection and forwarded on the actual resume — including
  // when table selection detours through the suggestions-review step first.
  const destinationConfigRef = useRef<DestinationConfig | null>(null)

  // Connection HITL
  const [unifiedSelectorOpen, setUnifiedSelectorOpen] = useState(false)
  const [connectionRequirements, setConnectionRequirements] = useState<{
    source: { connector_type: string }
    destination: { connector_type: string }
    // Directions the user must still pick. Derived from the backend's
    // `missing_connections`; absent → both required (backward compatible).
    missing?: ("source" | "destination")[]
  } | null>(null)

  // Table HITL
  const [tableSelectorOpen, setTableSelectorOpen] = useState(false)
  const [availableTables, setAvailableTables] = useState<AvailableTable[]>([])
  // AI-suggested tables from blocking_reason.details.suggested_tables — the
  // selector pre-checks these (user override always wins). undefined = the
  // details payload carried no suggested_tables key at all (the backend may
  // still be computing them) → the selector self-fetches from /state as a
  // fallback. An array (even empty) is authoritative and suppresses the
  // selector's redundant self-fetch polling.
  const [suggestedTables, setSuggestedTables] = useState<SuggestedTable[] | undefined>(undefined)
  const [tableSourceType, setTableSourceType] = useState<string>("")
  const [tableHitlNodeId, setTableHitlNodeId] = useState<string>("")
  const [tableDiscoveryStatus, setTableDiscoveryStatus] = useState<
    "ok" | "empty" | "failed" | undefined
  >(undefined)
  const [tableSourceDatabase, setTableSourceDatabase] = useState<string>("")
  const [tableDiscoveryReason, setTableDiscoveryReason] = useState<string>("")

  // Suggestions HITL
  const [suggestionsReviewOpen, setSuggestionsReviewOpen] = useState(false)
  const [selectedTablesForSuggestions, setSelectedTablesForSuggestions] = useState<string[]>([])
  const [sourceConnectionIdForSuggestions, setSourceConnectionIdForSuggestions] = useState<string>("")
  const [prefetchedSchemaForSuggestions, setPrefetchedSchemaForSuggestions] = useState<TableMetadata[]>([])

  const resetHITL = useCallback(() => {
    setConnectionRequirements(null)
    setUnifiedSelectorOpen(false)
    setAvailableTables([])
    setSuggestedTables(undefined)
    setTableSourceType("")
    setTableSelectorOpen(false)
    setTableHitlNodeId("")
    setSuggestionsReviewOpen(false)
    setSelectedTablesForSuggestions([])
    setSourceConnectionIdForSuggestions("")
    setPrefetchedSchemaForSuggestions([])
    lastBlockingKeyRef.current = ""
    suppressHitlUntilRef.current = 0
  }, [])

  const handleConnectorGeneration = useCallback(
    async (detailsOverride?: Record<string, unknown>) => {
      if (!pipelineId || !pipelineState.hitl) return
      await optimisticAction(
        () =>
          resumePipelineConnectors(pipelineId, {
            execution_id: pipelineState.metadata.executionId || undefined,
            connectors: (() => {
              const override = (detailsOverride || {})["missing_connectors"]
              if (Array.isArray(override)) return override
              const fromHitl = pipelineState.hitl?.blockingReason.details?.missing_connectors
              return Array.isArray(fromHitl) ? fromHitl : []
            })(),
            metadata: detailsOverride || pipelineState.hitl?.blockingReason.details || undefined,
          }),
        { type: "HITL_RESOLVED", payload: { timestamp: Date.now() } },
        {
          type: "HITL_REQUIRED",
          payload: {
            stage: pipelineState.hitl.stage,
            blockingReason: pipelineState.hitl.blockingReason,
            timestamp: Date.now(),
          },
        }
      )
    },
    [optimisticAction, pipelineId, pipelineState.hitl, pipelineState.metadata.executionId]
  )

  const handleOpenConnectionModal = useCallback(() => {
    const details = asRecord(pipelineState.hitl?.blockingReason.details)
    let sourceType = ""
    let destType = ""

    const required = details["required_connections"]
    if (required && typeof required === "object" && !Array.isArray(required)) {
      const req = required as Record<string, unknown>
      const src = asRecord(req["source"])
      const dst = asRecord(req["destination"])
      sourceType = String(src["connector_type"] || "")
      destType = String(dst["connector_type"] || "")
    }

    if (!sourceType) sourceType = String(details["source_connector"] || details["source_type"] || "")
    if (!destType) destType = String(details["destination_connector"] || details["destination_type"] || "")

    // The backend lists ONLY the directions that still need a user-selected
    // connection in `missing_connections`. A direction it already resolved and
    // auto-assigned is intentionally absent. Treat that list as authoritative so
    // we don't render a bogus picker (e.g. "Source: Unknown") for an already-wired
    // side — which previously left users unable to complete the modal.
    const missingRaw = details["missing_connections"]
    let missing: ("source" | "destination")[] | undefined
    if (Array.isArray(missingRaw)) {
      const dirs: ("source" | "destination")[] = []
      for (const m of (missingRaw as Array<Record<string, unknown>>)) {
        const dir = String(m?.["direction"] || "").toLowerCase()
        const typ = String(m?.["type"] || m?.["connector_type"] || m?.["connector"] || "").trim()
        if (dir === "source") {
          if (!dirs.includes("source")) dirs.push("source")
          if (typ && !sourceType) sourceType = typ
        } else if (dir === "destination") {
          if (!dirs.includes("destination")) dirs.push("destination")
          if (typ && !destType) destType = typ
        }
      }
      if (dirs.length > 0) missing = dirs
    }

    // Only fall back to the "unknown" placeholder for directions the user must
    // actually choose. Leaving a resolved direction as "unknown" was the root
    // cause of the unsatisfiable "No Unknown sources found" picker.
    const needsSource = !missing || missing.includes("source")
    const needsDest = !missing || missing.includes("destination")
    if (needsSource && !sourceType) sourceType = "unknown"
    if (needsDest && !destType) destType = "unknown"

    setConnectionRequirements({
      source: { connector_type: sourceType },
      destination: { connector_type: destType },
      missing,
    })
    setUnifiedSelectorOpen(true)
  }, [pipelineState.hitl])

  const handleOpenTableModal = useCallback(
    (overrideDetails?: Record<string, unknown>) => {
      const details = asRecord(overrideDetails || pipelineState.hitl?.blockingReason?.details)
      const tables = details["available_tables"]
      setAvailableTables(Array.isArray(tables) ? (tables as AvailableTable[]) : [])
      // Key absent → undefined (selector self-fetches as fallback); key present
      // (even a malformed/empty value) → normalized array, which is
      // authoritative and suppresses the selector's self-fetch.
      const rawSuggested = details["suggested_tables"]
      setSuggestedTables(rawSuggested === undefined ? undefined : normalizeSuggestedTables(rawSuggested))
      setTableSourceType(String(details["source_type"] || details["source_connector"] || ""))
      setTableHitlNodeId(String(details["node_id"] || "").trim())
      const ds = String(details["discovery_status"] || "")
      setTableDiscoveryStatus(ds === "empty" || ds === "failed" || ds === "ok" ? ds : undefined)
      setTableSourceDatabase(String(details["source_database"] || ""))
      setTableDiscoveryReason(String(details["reason"] || ""))
      setTableSelectorOpen(true)
    },
    [pipelineState.hitl]
  )

  const handleTablesSelected = useCallback(
    async (tables: string[], destCfg?: DestinationConfig) => {
      if (!pipelineId) return
      // Remember the destination namespace so it rides along on the eventual
      // resume, even if we first detour through the suggestions-review step.
      destinationConfigRef.current = destCfg ?? null
      const hitlDetails = asRecord(pipelineState.hitl?.blockingReason?.details)
      const meta = asRecord(authoritativePipelineMetadata)
      const sourceConnId = String(hitlDetails["source_connection_id"] || meta["source_connection_id"] || "")

      if (!sourceConnId) {
        const nodeId = String(tableHitlNodeId || "").trim() || String(hitlDetails["node_id"] || "").trim()
        // HITL-Race fix: AWAIT the resume signal and arm HITL suppression
        // SYNCHRONOUSLY after it lands but BEFORE returning. PipelineTableSelector.
        // onConfirm awaits this and only then closes the modal, so the parent's
        // /state poll can no longer re-detect the still-pending table_selection
        // gate in the window between modal-close and the signal POST landing —
        // the bug that made the first "Sync N tables" click silently no-op.
        // (Previously suppression was set inside the POST's .then(), so the modal
        // closed first.) On POST failure the await throws → onConfirm keeps the
        // modal open and suppression is never wrongly armed.
        if (nodeId) {
          await resumePipelineNodeInput(pipelineId, {
            execution_id: pipelineState.metadata.executionId ?? undefined,
            node_id: nodeId,
            config_patch: { selected_tables: tables, ...(destCfg ? { destination_config: destCfg } : {}) },
          })
        } else {
          await resumePipelineTables(pipelineId, {
            execution_id: pipelineState.metadata.executionId ?? undefined,
            selected_tables: tables,
            ...(destCfg ? { destination_config: destCfg } : {}),
          })
        }
        suppressHitlUntilRef.current = Date.now() + 15000
        lastBlockingKeyRef.current = ""
        resolveHITL()
        flashResumingState()
        setTableSelectorOpen(false)
        setTableHitlNodeId("")
        return
      }

      setSelectedTablesForSuggestions(tables)
      setSourceConnectionIdForSuggestions(sourceConnId)
      setPrefetchedSchemaForSuggestions([])
      setSuggestionsReviewOpen(true)
      setTableSelectorOpen(false)

      authFetch(
        `${API_ENDPOINTS.CONNECTIONS.GET(sourceConnId)}/metadata?tables=${encodeURIComponent(tables.join(","))}&include_columns=true`,
        { cache: "no-store" }
      )
        .then((r) => (r.ok ? r.json() : Promise.resolve({})))
        .then((data: Record<string, unknown>) =>
          setPrefetchedSchemaForSuggestions(
            Array.isArray(data?.["tables"]) ? (data["tables"] as TableMetadata[]) : []
          )
        )
        .catch(() => {/* dialog falls back gracefully */})
    },
    [
      authoritativePipelineMetadata,
      pipelineId,
      pipelineState.hitl,
      pipelineState.metadata.executionId,
      resolveHITL,
      tableHitlNodeId,
      flashResumingState,
    ]
  )

  const handleApplySuggestions = useCallback(
    async (transforms: TransformSuggestion[]) => {
      if (!pipelineId) return
      const hitlDetails = asRecord(pipelineState.hitl?.blockingReason?.details)
      const nodeId =
        String(tableHitlNodeId || "").trim() || String(hitlDetails["node_id"] || "").trim()
      const destCfg = destinationConfigRef.current
      // AWAIT the resume BEFORE clearing state / closing the modal. On failure the
      // throw propagates to the caller (SuggestionsReviewDialog.handleApply's catch),
      // which keeps the modal open with the user's transform edits intact so they can
      // retry — an optimistic close-first would discard those edits on a failed resume.
      // The modal shows an "Applying…" spinner during the await, so the click still
      // reads as responsive.
      if (nodeId) {
        await resumePipelineNodeInput(pipelineId, {
          execution_id: pipelineState.metadata.executionId ?? undefined,
          node_id: nodeId,
          config_patch: {
            selected_tables: selectedTablesForSuggestions,
            metadata: { transforms, applied_at: new Date().toISOString() },
            ...(destCfg ? { destination_config: destCfg } : {}),
          },
        })
      } else {
        await resumePipelineTables(pipelineId, {
          execution_id: pipelineState.metadata.executionId ?? undefined,
          selected_tables: selectedTablesForSuggestions,
          metadata: { transforms, applied_at: new Date().toISOString() },
          ...(destCfg ? { destination_config: destCfg } : {}),
        })
      }

      suppressHitlUntilRef.current = Date.now() + 15000
      lastBlockingKeyRef.current = ""
      resolveHITL()
      flashResumingState()
      setSuggestionsReviewOpen(false)
      setSelectedTablesForSuggestions([])
      setSourceConnectionIdForSuggestions("")
      setTableHitlNodeId("")
    },
    [
      pipelineId,
      pipelineState.hitl?.blockingReason?.details,
      pipelineState.metadata.executionId,
      selectedTablesForSuggestions,
      resolveHITL,
      tableHitlNodeId,
      flashResumingState,
    ]
  )

  const handleSkipSuggestions = useCallback(async () => {
    if (
      !pipelineId ||
      !pipelineState?.metadata?.executionId ||
      selectedTablesForSuggestions.length === 0
    ) {
      setSuggestionsReviewOpen(false)
      setSelectedTablesForSuggestions([])
      setSourceConnectionIdForSuggestions("")
      return
    }

    const hitlDetails = asRecord(pipelineState.hitl?.blockingReason?.details)
    const nodeId =
      String(tableHitlNodeId || "").trim() || String(hitlDetails["node_id"] || "").trim()
    const destCfg = destinationConfigRef.current
    // AWAIT the resume BEFORE clearing state / closing the modal. On failure the throw
    // propagates to the caller (SuggestionsReviewDialog.requestClose's catch), which
    // keeps the modal open so the user can retry instead of losing their table
    // selection. The modal shows a "Skipping…" spinner during the await, so the click
    // still reads as responsive.
    if (nodeId) {
      await resumePipelineNodeInput(pipelineId, {
        execution_id: pipelineState.metadata.executionId,
        node_id: nodeId,
        config_patch: {
          selected_tables: selectedTablesForSuggestions,
          metadata: { transforms: [], skipped_suggestions: true, skipped_at: new Date().toISOString() },
          ...(destCfg ? { destination_config: destCfg } : {}),
        },
      })
    } else {
      await resumePipelineTables(pipelineId, {
        execution_id: pipelineState.metadata.executionId,
        selected_tables: selectedTablesForSuggestions,
        metadata: { transforms: [], skipped_suggestions: true, skipped_at: new Date().toISOString() },
        ...(destCfg ? { destination_config: destCfg } : {}),
      })
    }

    suppressHitlUntilRef.current = Date.now() + 15000
    lastBlockingKeyRef.current = ""
    resolveHITL()
    flashResumingState()
    setSuggestionsReviewOpen(false)
    setSelectedTablesForSuggestions([])
    setSourceConnectionIdForSuggestions("")
    setTableHitlNodeId("")
  }, [
    pipelineId,
    pipelineState.hitl?.blockingReason?.details,
    pipelineState.metadata.executionId,
    selectedTablesForSuggestions,
    resolveHITL,
    tableHitlNodeId,
    flashResumingState,
  ])

  return {
    // refs
    suppressHitlUntilRef,
    lastBlockingKeyRef,
    // connection HITL
    unifiedSelectorOpen,
    setUnifiedSelectorOpen,
    connectionRequirements,
    setConnectionRequirements,
    // table HITL
    tableSelectorOpen,
    setTableSelectorOpen,
    availableTables,
    setAvailableTables,
    suggestedTables,
    setSuggestedTables,
    tableSourceType,
    setTableSourceType,
    tableHitlNodeId,
    setTableHitlNodeId,
    tableDiscoveryStatus,
    tableSourceDatabase,
    tableDiscoveryReason,
    // suggestions HITL
    suggestionsReviewOpen,
    setSuggestionsReviewOpen,
    selectedTablesForSuggestions,
    sourceConnectionIdForSuggestions,
    prefetchedSchemaForSuggestions,
    // handlers
    resetHITL,
    handleConnectorGeneration,
    handleOpenConnectionModal,
    handleOpenTableModal,
    handleTablesSelected,
    handleApplySuggestions,
    handleSkipSuggestions,
  }
}
