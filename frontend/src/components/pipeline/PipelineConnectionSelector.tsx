"use client"

import { useEffect, useMemo, useRef, useState } from "react"

import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group"
import { Label } from "@/components/ui/label"
import { Loader2, Database, ArrowDown, Plus, Check, AlertCircle } from "lucide-react"

import { cn } from "@/lib/utils"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import { resumePipelineConnections } from "@/lib/api/pipelines"
import { extractErrorMessage } from "@/lib/utils/error-handling"
import { captureWorkspace, onActiveWorkspaceChange } from "@/lib/workspace/active-workspace"

type ConnectionStatus = "active" | "inactive" | "not_tested" | string

interface Connection {
  id: string
  name: string
  connector_type: string
  type: "source" | "destination" | string
  status?: ConnectionStatus
  created_at?: string
}

interface PipelineConnectionSelectorProps {
  isOpen: boolean
  onClose: () => void
  onSuccess?: () => void
  pipelineId: string
  executionId?: string
  requirements: {
    source: { connector_type: string }
    destination: { connector_type: string }
    // Which directions still need a user-selected connection. The backend lists
    // ONLY unresolved directions; a direction it already resolved + auto-assigned
    // is intentionally absent. When omitted (or empty), both are treated as
    // required (backward compatible). Used to avoid rendering a bogus picker
    // (e.g. "Source: Unknown") for an already-wired side.
    missing?: ("source" | "destination")[]
  }
  // Optional UX helpers (additive; backend may omit)
  userRequest?: string
  suggestions?: Array<{
    source_connector_type: string
    destination_connector_type: string
    confidence?: number
    reasoning?: string
  }>
  title?: string
  // Where to send the user after creating a new connection.
  // Kept as a string to match existing query param semantics used by `/connections/new`.
  // Default remains "chat" for backward compatibility.
  returnTo?: string
}

function normalizeConnectorType(s: string): string {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/_/g, "-")
}

function connectorCandidates(connectorType: string): string[] {
  const n = normalizeConnectorType(connectorType)
  const candidates = new Set<string>([n])

  // Common aliases / normalization variants
  candidates.add(n.replace(/-/g, "_"))
  candidates.add(n.replace(/_/g, "-"))

  // Semantic aliases
  if (n === "aws-s3" || n === "aws_s3" || n === "s3") {
    candidates.add("s3")
    candidates.add("aws-s3")
    candidates.add("aws_s3")
    candidates.add("amazon-s3")
    candidates.add("amazon_s3")
  }

  if (n === "minio") {
    candidates.add("s3")
    candidates.add("aws-s3")
    candidates.add("aws_s3")
  }

  if (n === "postgresql" || n === "postgres" || n === "pg") {
    candidates.add("postgresql")
    candidates.add("postgres")
    candidates.add("pg")
  }

  if (n === "mysql" || n === "mariadb") {
    candidates.add("mysql")
    candidates.add("mariadb")
  }

  if (n === "mongodb" || n === "mongo") {
    candidates.add("mongodb")
    candidates.add("mongo")
  }

  if (n === "gcs" || n === "google-cloud-storage") {
    candidates.add("gcs")
    candidates.add("google-cloud-storage")
  }

  return Array.from(candidates).filter(Boolean)
}

function formatConnectorName(type: string): string {
  const t = normalizeConnectorType(type)
  const names: Record<string, string> = {
    mysql: "MySQL",
    mariadb: "MariaDB",
    postgresql: "PostgreSQL",
    postgres: "PostgreSQL",
    pg: "PostgreSQL",
    "aws-s3": "AWS S3",
    s3: "S3",
    gcs: "Google Cloud Storage",
    "google-cloud-storage": "Google Cloud Storage",
    "azure-blob": "Azure Blob Storage",
    mongodb: "MongoDB",
    mongo: "MongoDB",
    redis: "Redis",
    snowflake: "Snowflake",
    bigquery: "BigQuery",
    minio: "MinIO",
    "google-sheets": "Google Sheets",
  }
  return names[t] || type.charAt(0).toUpperCase() + type.slice(1)
}

function getStatusColor(status?: string): string {
  switch (status) {
    case "active":
      return "text-green-600 bg-green-100 dark:text-green-400 dark:bg-green-900"
    case "inactive":
      return "text-gray-600 bg-gray-100 dark:text-gray-400 dark:bg-gray-800"
    case "error":
      return "text-red-600 bg-red-100 dark:text-red-400 dark:bg-red-900"
    default:
      return "text-amber-600 bg-amber-100 dark:text-amber-400 dark:bg-amber-900"
  }
}

// Loading skeleton component
function ConnectionListSkeleton() {
  return (
    <div className="space-y-2">
      {[1, 2, 3].map((i) => (
        <div
          key={i}
          className="flex items-center space-x-3 p-3 rounded-lg border border-gray-200 dark:border-gray-700 animate-pulse"
        >
          <div className="w-4 h-4 rounded-full bg-gray-200 dark:bg-gray-700" />
          <div className="flex-1 space-y-2">
            <div className="h-4 bg-gray-200 dark:bg-gray-700 rounded w-3/4" />
            <div className="h-3 bg-gray-200 dark:bg-gray-700 rounded w-1/2" />
          </div>
          <div className="h-6 w-16 bg-gray-200 dark:bg-gray-700 rounded-full" />
        </div>
      ))}
    </div>
  )
}

export function PipelineConnectionSelector({
  isOpen,
  onClose,
  onSuccess,
  pipelineId,
  executionId,
  requirements,
  userRequest,
  suggestions = [],
  title = "Configure Pipeline Connections",
  returnTo = "chat",
}: PipelineConnectionSelectorProps) {
  const [loading, setLoading] = useState(true)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const submitAbortRef = useRef<AbortController | null>(null)
  const mountedRef = useRef(true)

  const [sourceConnections, setSourceConnections] = useState<Connection[]>([])
  const [destConnections, setDestConnections] = useState<Connection[]>([])
  const [sourceWrongRoleCount, setSourceWrongRoleCount] = useState(0)
  const [destWrongRoleCount, setDestWrongRoleCount] = useState(0)

  const [selectedSourceId, setSelectedSourceId] = useState<string>("")
  const [selectedDestId, setSelectedDestId] = useState<string>("")

  const [sourceAutoSelected, setSourceAutoSelected] = useState(false)
  const [destAutoSelected, setDestAutoSelected] = useState(false)

  // Allow suggestion chips to override the effective connector types (without changing parent state)
  const [overrideSourceType, setOverrideSourceType] = useState<string>("")
  const [overrideDestType, setOverrideDestType] = useState<string>("")
  const effectiveSourceType = overrideSourceType || requirements.source.connector_type
  const effectiveDestType = overrideDestType || requirements.destination.connector_type

  // Which directions the user must actually choose. The backend only lists
  // unresolved directions in `requirements.missing`; an already-resolved +
  // auto-assigned direction is absent (and must NOT render a picker — that's the
  // "Source: Unknown / No Unknown sources found" bug). Absent/empty → both
  // required (backward compatible).
  const missingDirs =
    requirements.missing && requirements.missing.length > 0 ? requirements.missing : null
  const sourceRequired = !missingDirs || missingDirs.includes("source")
  const destRequired = !missingDirs || missingDirs.includes("destination")
  const sourceSatisfied = !sourceRequired || !!selectedSourceId
  const destSatisfied = !destRequired || !!selectedDestId

  const sourceCandidates = useMemo(() => connectorCandidates(effectiveSourceType), [effectiveSourceType])
  const destCandidates = useMemo(() => connectorCandidates(effectiveDestType), [effectiveDestType])

  // Avoid refetch loops when parent recreates `requirements` object on polling.
  // Only treat requirements as changed when connector types actually change.
  const requirementsKey = useMemo(() => {
    return `${normalizeConnectorType(requirements?.source?.connector_type)}|${normalizeConnectorType(
      requirements?.destination?.connector_type
    )}`
  }, [requirements?.source?.connector_type, requirements?.destination?.connector_type])

  // Telemetry for suggestion usage (sent as optional metadata to backend)
  const [suggestionUsed, setSuggestionUsed] = useState(false)
  const openedAtRef = useState(() => Date.now())[0]

  useEffect(() => {
    mountedRef.current = true
    return () => {
      mountedRef.current = false
      submitAbortRef.current?.abort()
      submitAbortRef.current = null
    }
  }, [])

  useEffect(() => {
    if (isOpen) {
      // Reset overrides/telemetry on open
      setOverrideSourceType("")
      setOverrideDestType("")
      setSuggestionUsed(false)
      fetchAllConnections()
    } else {
      // If modal is closed while a submit is in-flight, abort it and reset UI.
      submitAbortRef.current?.abort()
      submitAbortRef.current = null
      setSubmitting(false)
      setError(null)
    }
  }, [isOpen, requirementsKey])

  // A switch invalidates every connection currently listed. Clear the selection
  // too: the ids belong to the workspace we just left.
  useEffect(() => {
    if (!isOpen) return
    return onActiveWorkspaceChange(() => {
      setSourceConnections([])
      setDestConnections([])
      setSelectedSourceId("")
      setSelectedDestId("")
      fetchAllConnections()
    })
  }, [isOpen])

  async function fetchAllConnections() {
    // The modal can stay open across a workspace switch (same tab or another
    // tab). Drop a response whose workspace is no longer active rather than
    // offering the previous tenant's connections as pipeline endpoints.
    const isStale = captureWorkspace()
    setLoading(true)
    setError(null)
    setSourceWrongRoleCount(0)
    setDestWrongRoleCount(0)

    try {
      // Fetch source and destination connections in parallel
      const [sourceList, destList] = await Promise.all([
        fetchConnectionsForCandidates(sourceCandidates, "source"),
        fetchConnectionsForCandidates(destCandidates, "destination"),
      ])
      if (isStale()) return

      setSourceConnections(sourceList)
      setDestConnections(destList)

      // Helpful hint: if user created a matching connector but with the wrong role
      if (sourceList.length === 0 && sourceCandidates.length > 0) {
        const wrong = await fetchConnectionsForCandidates(sourceCandidates, "destination")
        setSourceWrongRoleCount(wrong.length)
      }
      if (destList.length === 0 && destCandidates.length > 0) {
        const wrong = await fetchConnectionsForCandidates(destCandidates, "source")
        setDestWrongRoleCount(wrong.length)
      }

      // Preserve user selection across refetches; only auto-select when empty OR selection is no longer available.
      const nextSourceIds = new Set(sourceList.map((c) => c.id))
      const nextDestIds = new Set(destList.map((c) => c.id))

      const shouldPickSource = !selectedSourceId || !nextSourceIds.has(selectedSourceId)
      if (shouldPickSource) {
        if (sourceList.length === 1) {
          setSelectedSourceId(sourceList[0].id)
          setSourceAutoSelected(true)
        } else if (sourceList.length > 0) {
          const activeSource = sourceList.find((c) => c.status === "active") || sourceList[0]
          setSelectedSourceId(activeSource.id)
          setSourceAutoSelected(false)
        }
      }

      const shouldPickDest = !selectedDestId || !nextDestIds.has(selectedDestId)
      if (shouldPickDest) {
        if (destList.length === 1) {
          setSelectedDestId(destList[0].id)
          setDestAutoSelected(true)
        } else if (destList.length > 0) {
          const activeDest = destList.find((c) => c.status === "active") || destList[0]
          setSelectedDestId(activeDest.id)
          setDestAutoSelected(false)
        }
      }
    } catch (err) {
      console.error("Failed to fetch connections:", err)
      if (isStale()) return
      setError("Failed to load connections. Please try again.")
    } finally {
      setLoading(false)
    }
  }

  const handleSelectSource = (id: string) => {
    setSelectedSourceId(id)
    setSourceAutoSelected(false)
  }

  const handleSelectDest = (id: string) => {
    setSelectedDestId(id)
    setDestAutoSelected(false)
  }

  async function fetchConnectionsForCandidates(candidates: string[], type: "source" | "destination"): Promise<Connection[]> {
    let found: Connection[] = []

    // Try each candidate
    for (const candidate of candidates) {
      const params = new URLSearchParams({
        type,
        connector_type: candidate,
      })

      try {
        const response = await authFetch(`${API_ENDPOINTS.CONNECTIONS.LIST}?${params.toString()}`, {
          cache: "no-store",
        })

        if (response.ok) {
          const data = await response.json()
          const list: Connection[] = Array.isArray(data) ? data : data.connections || []
          if (list.length > 0) {
            found = list
            break
          }
        }
      } catch (err) {
        console.error(`Failed to fetch connections for candidate ${candidate}:`, err)
      }
    }

    // Fallback: fetch all of this type and filter locally
    if (found.length === 0) {
      try {
        const response = await authFetch(`${API_ENDPOINTS.CONNECTIONS.LIST}?type=${encodeURIComponent(type)}`, {
          cache: "no-store",
        })

        if (response.ok) {
          const data = await response.json()
          const list: Connection[] = Array.isArray(data) ? data : data.connections || []
          // If candidates is empty/unknown, show all connections of this direction.
          if (candidates.length === 0) {
            found = list
          } else {
            // Filter by candidates on client side
            const candidatesSet = new Set(candidates.map((c) => c.toLowerCase()))
            found = list.filter((conn) => candidatesSet.has(normalizeConnectorType(conn.connector_type)))
          }
        }
      } catch (err) {
        console.error(`Failed to fetch all connections for type ${type}:`, err)
      }
    }

    return found
  }

  async function handleConfirm() {
    if ((sourceRequired && !selectedSourceId) || (destRequired && !selectedDestId)) {
      setError(
        sourceRequired && destRequired
          ? "Please select both source and destination connections"
          : sourceRequired
            ? "Please select a source connection"
            : "Please select a destination connection"
      )
      return
    }

    if (submitting) return
    setSubmitting(true)
    setError(null)

    // Abort any previous in-flight submit (defensive)
    submitAbortRef.current?.abort()
    const controller = new AbortController()
    submitAbortRef.current = controller
    const timeoutMs = 20_000
    const timeout = window.setTimeout(() => controller.abort(), timeoutMs)

    try {
      await resumePipelineConnections(
        pipelineId,
        {
        execution_id: executionId,
        // Send only the direction(s) the user actually picked. The backend resume
        // handler ignores empty/omitted ids (omitempty), so an already-resolved
        // direction keeps its existing assignment instead of being wiped.
        source_connection_id: selectedSourceId || undefined,
        destination_connection_id: selectedDestId || undefined,
        // Optional analytics payload; backend will ignore if unsupported.
        metadata: {
          intent_method: suggestions.length > 0 ? "hitl" : "deterministic",
          suggestions_shown: suggestions.length,
          suggestion_clicked: suggestionUsed,
          time_to_selection_seconds: Math.max(0, (Date.now() - openedAtRef) / 1000),
          selected_source_type: effectiveSourceType,
          selected_destination_type: effectiveDestType,
        },
        },
        { signal: controller.signal }
      )

      // Close modal on success — notify caller before closing so it can clear HITL optimistically.
      onSuccess?.()
      onClose()
    } catch (err) {
      console.error("Failed to configure connections:", err)
      if (controller.signal.aborted) {
        setError("Cancelled. If this keeps happening, try again or refresh the page.")
      } else {
        setError(extractErrorMessage(err) || "Failed to configure connections. Please try again.")
      }
    } finally {
      window.clearTimeout(timeout)
      if (submitAbortRef.current === controller) submitAbortRef.current = null
      if (mountedRef.current) setSubmitting(false)
    }
  }

  function handleCancel() {
    // Always allow cancel: abort any in-flight submit and close.
    submitAbortRef.current?.abort()
    submitAbortRef.current = null
    setSubmitting(false)
    onClose()
  }

  function renderConnectionList(
    connections: Connection[],
    selectedId: string,
    onSelect: (id: string) => void,
    connectorType: string,
    role: "source" | "destination",
    autoSelected: boolean
  ) {
    if (connections.length === 0) {
      const wrongRoleCount = role === "source" ? sourceWrongRoleCount : destWrongRoleCount
      return (
        <div className="p-4 text-center border-2 border-dashed rounded-lg border-gray-300 dark:border-gray-700">
          <Database className="h-8 w-8 mx-auto text-gray-400 mb-2" />
          <p className="text-sm text-gray-500 dark:text-gray-400 mb-2">No {formatConnectorName(connectorType)} {role}s found</p>
          {wrongRoleCount > 0 && (
            <p className="text-xs text-amber-700 dark:text-amber-300 mb-2">
              Found {wrongRoleCount} {formatConnectorName(connectorType)} connection{wrongRoleCount === 1 ? "" : "s"}, but they were created as{" "}
              <span className="font-medium">{role === "source" ? "destinations" : "sources"}</span>. Create a {role} connection to continue.
            </p>
          )}
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              // `connections/new` historically used `type=source|destination`. Some callers used
              // `connection_type` instead. Send both for compatibility and smoother HITL UX.
              window.location.href = `/connections/new?connector_type=${encodeURIComponent(connectorType)}&type=${encodeURIComponent(role)}&connection_type=${encodeURIComponent(role)}&returnTo=${encodeURIComponent(returnTo)}`
            }}
          >
            <Plus className="h-4 w-4 mr-1" />
            Create {formatConnectorName(connectorType)} Connection
          </Button>
        </div>
      )
    }

    return (
      <RadioGroup value={selectedId} onValueChange={onSelect} className="space-y-2">
        {connections.map((conn) => (
          <div
            key={conn.id}
            className={cn(
              "flex items-center space-x-3 p-3 rounded-lg border cursor-pointer transition-all",
              selectedId === conn.id
                ? "border-violet-500 bg-violet-50 dark:bg-violet-900/20"
                : "border-gray-200 dark:border-gray-700 hover:border-gray-300 dark:hover:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-800"
            )}
            onClick={() => onSelect(conn.id)}
          >
            <RadioGroupItem value={conn.id} id={conn.id} />
            <Label htmlFor={conn.id} className="flex-1 cursor-pointer">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="font-medium">{conn.name}</span>
                  {autoSelected && selectedId === conn.id && (
                    <span className="text-xs bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300 px-2 py-0.5 rounded-full">
                      Auto-selected
                    </span>
                  )}
                </div>
                <span className={cn("text-xs px-2 py-0.5 rounded-full", getStatusColor(conn.status))}>{conn.status || "unknown"}</span>
              </div>
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">
                {conn.created_at ? `Created: ${new Date(conn.created_at).toLocaleDateString()}` : "No date"}
              </p>
            </Label>
            {selectedId === conn.id && <Check className="h-5 w-5 text-violet-600 dark:text-violet-400" />}
          </div>
        ))}

        {/* Create new option */}
        <div
          className="flex items-center space-x-3 p-3 rounded-lg border border-dashed border-gray-300 dark:border-gray-700 cursor-pointer hover:border-gray-400 dark:hover:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-800 transition-all"
          onClick={() => {
            window.location.href = `/connections/new?connector_type=${encodeURIComponent(connectorType)}&type=${encodeURIComponent(role)}&connection_type=${encodeURIComponent(role)}&returnTo=${encodeURIComponent(returnTo)}`
          }}
        >
          <div className="w-4 h-4 rounded-full border-2 border-gray-300 dark:border-gray-600 flex items-center justify-center">
            <Plus className="h-3 w-3 text-gray-400 dark:text-gray-500" />
          </div>
          <span className="text-sm text-gray-600 dark:text-gray-400">Create new {formatConnectorName(connectorType)} connection</span>
        </div>
      </RadioGroup>
    )
  }

  const canConfirm = sourceSatisfied && destSatisfied && !loading && !submitting

  return (
    <Dialog open={isOpen} onOpenChange={(open) => (!open ? onClose() : null)}>
      <DialogContent className="sm:max-w-[550px] max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Database className="h-5 w-5 text-violet-600 dark:text-violet-400" />
            {title}
          </DialogTitle>
        </DialogHeader>

        {loading ? (
          <div className="space-y-6 py-4">
            <div>
              <h3 className="font-semibold text-gray-900 dark:text-gray-100 mb-3">Source</h3>
              <ConnectionListSkeleton />
            </div>
            <div className="flex justify-center">
              <ArrowDown className="h-5 w-5 text-gray-400" />
            </div>
            <div>
              <h3 className="font-semibold text-gray-900 dark:text-gray-100 mb-3">Destination</h3>
              <ConnectionListSkeleton />
            </div>
          </div>
        ) : (
          <div className="space-y-6 py-4">
            {/* Optional: echo user request + quick picks */}
            {(userRequest || suggestions.length > 0) && (
              <div className="space-y-3">
                {userRequest && (
                  <div className="p-3 rounded-lg bg-zinc-50 dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800 text-sm text-zinc-700 dark:text-zinc-300">
                    <span className="font-medium">You asked:</span> {userRequest}
                  </div>
                )}
                {suggestions.length > 0 && (
                  <div className="space-y-2">
                    <p className="text-sm font-medium text-zinc-700 dark:text-zinc-300">Quick picks:</p>
                    <div className="flex flex-wrap gap-2">
                      {suggestions.map((sug, idx) => (
                        <button
                          key={`${sug.source_connector_type}-${sug.destination_connector_type}-${idx}`}
                          type="button"
                          className="px-3 py-1.5 rounded-lg border border-blue-200 dark:border-blue-800 bg-blue-50 dark:bg-blue-900/20 text-blue-700 dark:text-blue-300 text-sm hover:bg-blue-100 dark:hover:bg-blue-900/30 transition-colors"
                          onClick={async () => {
                            setSuggestionUsed(true)
                            setOverrideSourceType(sug.source_connector_type || "")
                            setOverrideDestType(sug.destination_connector_type || "")
                            // Refresh lists based on the suggestion
                            await fetchAllConnections()
                          }}
                          title={sug.reasoning || ""}
                        >
                          {formatConnectorName(sug.source_connector_type)} → {formatConnectorName(sug.destination_connector_type)}
                          {typeof sug.confidence === "number" && (
                            <span className="ml-2 text-xs opacity-75">{Math.round(sug.confidence * 100)}%</span>
                          )}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Source Section — only when the user must pick a source. A source the
                backend already resolved + auto-assigned shows as a compact
                "already configured" note instead of an unsatisfiable picker. */}
            {sourceRequired ? (
              <div>
                <div className="flex items-center gap-2 mb-3">
                  <div className="w-8 h-8 rounded-full bg-blue-100 dark:bg-blue-900 flex items-center justify-center">
                    <Database className="h-4 w-4 text-blue-600 dark:text-blue-400" />
                  </div>
                  <div>
                    <h3 className="font-semibold text-gray-900 dark:text-gray-100">
                      Source: {formatConnectorName(effectiveSourceType || "source")}
                    </h3>
                    <p className="text-xs text-gray-500 dark:text-gray-400">Where to read data from</p>
                  </div>
                </div>
                {renderConnectionList(
                  sourceConnections,
                  selectedSourceId,
                  handleSelectSource,
                  effectiveSourceType,
                  "source",
                  sourceAutoSelected
                )}
              </div>
            ) : (
              <div className="flex items-center gap-2 rounded-md border border-zinc-200 bg-zinc-50 px-3 py-2 text-sm dark:border-zinc-800 dark:bg-zinc-900/40">
                <span className="text-green-600 dark:text-green-400" aria-hidden>✓</span>
                <span className="text-gray-600 dark:text-gray-400">Source connection already configured.</span>
              </div>
            )}

            {/* Arrow — only meaningful when both pickers are visible */}
            {sourceRequired && destRequired && (
              <div className="flex justify-center">
                <div className="flex items-center gap-2 text-gray-400 dark:text-gray-600">
                  <div className="h-px w-16 bg-gray-200 dark:bg-gray-700" />
                  <ArrowDown className="h-5 w-5" />
                  <div className="h-px w-16 bg-gray-200 dark:bg-gray-700" />
                </div>
              </div>
            )}

            {/* Destination Section — see source note above for the resolved case */}
            {destRequired ? (
              <div>
                <div className="flex items-center gap-2 mb-3">
                  <div className="w-8 h-8 rounded-full bg-green-100 dark:bg-green-900 flex items-center justify-center">
                    <Database className="h-4 w-4 text-green-600 dark:text-green-400" />
                  </div>
                  <div>
                    <h3 className="font-semibold text-gray-900 dark:text-gray-100">
                      Destination: {formatConnectorName(effectiveDestType || "destination")}
                    </h3>
                    <p className="text-xs text-gray-500 dark:text-gray-400">Where to write data to</p>
                  </div>
                </div>
                {renderConnectionList(
                  destConnections,
                  selectedDestId,
                  handleSelectDest,
                  effectiveDestType,
                  "destination",
                  destAutoSelected
                )}
              </div>
            ) : (
              <div className="flex items-center gap-2 rounded-md border border-zinc-200 bg-zinc-50 px-3 py-2 text-sm dark:border-zinc-800 dark:bg-zinc-900/40">
                <span className="text-green-600 dark:text-green-400" aria-hidden>✓</span>
                <span className="text-gray-600 dark:text-gray-400">Destination connection already configured.</span>
              </div>
            )}

            {/* Error */}
            {error && (
              <div className="flex items-center gap-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg text-red-700 dark:text-red-400">
                <AlertCircle className="h-5 w-5" />
                <span className="text-sm">{error}</span>
              </div>
            )}

            {/* Summary */}
            {selectedSourceId && selectedDestId && (
              <div className="p-3 bg-violet-50 dark:bg-violet-900/20 rounded-lg border border-violet-200 dark:border-violet-800">
                <p className="text-sm text-violet-800 dark:text-violet-300">
                  <span className="font-medium">Ready to sync:</span>{" "}
                  {sourceConnections.find((c) => c.id === selectedSourceId)?.name || "Source"} →{" "}
                  {destConnections.find((c) => c.id === selectedDestId)?.name || "Destination"}
                </p>
              </div>
            )}

            {/* Tell the user why "Start Pipeline" is disabled instead of leaving
                the click a silent no-op. */}
            {!error && !canConfirm && !loading && !submitting && (
              <p className="text-xs text-center text-gray-500 dark:text-gray-400">
                {!sourceSatisfied && !destSatisfied
                  ? "Select a source and a destination connection to continue."
                  : !sourceSatisfied
                    ? "Select a source connection to continue."
                    : "Select a destination connection to continue."}
              </p>
            )}
          </div>
        )}

        <DialogFooter className="gap-2">
          <Button variant="outline" onClick={handleCancel}>
            Cancel
          </Button>
          <Button onClick={handleConfirm} disabled={!canConfirm} className="bg-violet-600 hover:bg-violet-700 dark:bg-violet-600 dark:hover:bg-violet-700 disabled:opacity-50 disabled:cursor-not-allowed">
            {submitting ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin mr-2" />
                Configuring...
              </>
            ) : (
              <>
                Start Pipeline
                <ArrowDown className="h-4 w-4 ml-2 rotate-[-90deg]" />
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

