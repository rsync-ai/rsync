"use client"

import { Suspense, useState, useEffect } from "react"
import { useRouter, useSearchParams } from "next/navigation"
import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  ArrowLeft,
  ArrowRight,
  Database,
  Cloud,
  CheckCircle2,
  Loader2,
} from "lucide-react"
import { toast } from "sonner"
import { motion, AnimatePresence } from "framer-motion"
import { cn } from "@/lib/utils"
import { MCPConnector } from "@/lib/types/mcp-connector"
import { fetchMCPConnectors, fetchMCPConnector, saveConnection } from "@/lib/api/mcp-connectors"
import { GenericConnectorForm } from "@/components/connectors/GenericConnectorForm"
import { classifyError } from "@/lib/utils/error-handling"
import { ConnectionLogo } from "@/components/connectors/ConnectionLogo"

// The wizard owns direction + connector selection; the shared GenericConnectorForm
// owns configure + test + save (name, type, sync/CDC mode, the auth-method picker,
// config fields, and the Test/Save buttons). This keeps ONE credential form across
// every "Configure connection" surface (see also UnifiedConnectorModal,
// ConnectorConfigModal, connections/[id]) — so wizard-created connections also get
// the auth-method selector and persist config.auth_method for the runtime dispatcher.
type Step = "direction" | "type" | "configure"

function NewConnectionPageContent() {
  const router = useRouter()
  const searchParams = useSearchParams()

  // Where to return after creation (pipeline/CDC deep-links pass this).
  const returnTo = searchParams.get("returnTo") || "/connections"
  // Support both `type` (preferred) and legacy `connection_type`.
  const preselectedType = (searchParams.get("type") || searchParams.get("connection_type")) as
    | "source"
    | "destination"
    | null
  // Optional preselected connector (used by HITL / pipeline flows).
  const preselectedConnectorType = searchParams.get("connector_type")
  // Deep-linked sync mode (CDC flows pass sync_mode=cdc). Previously dropped on the
  // floor by the old wizard; now threaded into the form's initial sync mode.
  const preselectedSyncMode = (searchParams.get("sync_mode") as "batch" | "cdc" | null) || undefined

  const [step, setStep] = useState<Step>(preselectedType ? "type" : "direction")
  const [connectionType, setConnectionType] = useState<"source" | "destination">(preselectedType || "source")
  const [selectedConnector, setSelectedConnector] = useState<MCPConnector | null>(null)

  // MCP Connectors from API
  const [connectors, setConnectors] = useState<MCPConnector[]>([])
  const [loadingConnectors, setLoadingConnectors] = useState(true)

  // Fetch connectors on mount
  useEffect(() => {
    const loadData = async () => {
      try {
        setLoadingConnectors(true)
        const connectorsData = await fetchMCPConnectors()
        setConnectors(connectorsData.connectors || [])
      } catch (err) {
        const e = classifyError(err, "connectors.load")
        toast.error(e.title, { description: e.hint ?? e.message })
      } finally {
        setLoadingConnectors(false)
      }
    }
    loadData()
  }, [])

  // If we were deep-linked with a preselected direction, sync it after hydration.
  // (Client components can be pre-rendered; relying only on useState(initializer)
  // can miss query params on the first render.)
  useEffect(() => {
    if (!preselectedType) return
    setConnectionType(preselectedType)
    setStep((prev) => (prev === "direction" ? "type" : prev))
  }, [preselectedType])

  // If we arrive from HITL (or deep-link), preselect the connector and jump to configure.
  useEffect(() => {
    if (loadingConnectors) return
    if (!preselectedConnectorType) return
    if (selectedConnector) return
    if (!connectors || connectors.length === 0) return

    const normalize = (s: string) =>
      String(s || "")
        .trim()
        .toLowerCase()
        .replace(/\s+/g, "-")
        .replace(/_/g, "-")

    const target = normalize(preselectedConnectorType)
    const match =
      connectors.find((c) => normalize(c.name) === target) ||
      connectors.find((c) => normalize(c.name).includes(target) || target.includes(normalize(c.name))) ||
      null

    if (!match) return

    setSelectedConnector(match)
    setStep("configure")
    // Enrich with full metadata (supported_auth_methods + complete schema) so the
    // shared form can render the auth-method picker; fall back to the list object.
    fetchMCPConnector(match.name)
      .then((full) =>
        setSelectedConnector((prev) => (prev && prev.name === match.name ? (full as MCPConnector) : prev)),
      )
      .catch(() => {})
  }, [loadingConnectors, connectors, preselectedConnectorType, selectedConnector])

  // Filter connectors by direction
  const sourceConnectors = connectors.filter((c) => c.supports_source)
  const destinationConnectors = connectors.filter((c) => c.supports_destination)
  const availableConnectors = connectionType === "source" ? sourceConnectors : destinationConnectors

  // Group connectors by category
  const groupedConnectors = availableConnectors.reduce<Record<string, MCPConnector[]>>((acc, connector) => {
    const category = connector.category || "other"
    if (!acc[category]) acc[category] = []
    acc[category].push(connector)
    return acc
  }, {})

  const handleDirectionSelect = (direction: "source" | "destination") => {
    setConnectionType(direction)
    setStep("type")
  }

  const handleConnectorSelect = (connector: MCPConnector) => {
    setSelectedConnector(connector)
    setStep("configure")
    // Optimistic render from the list object, then enrich with full metadata
    // (supported_auth_methods + complete configuration_schema).
    fetchMCPConnector(connector.name)
      .then((full) =>
        setSelectedConnector((prev) => (prev && prev.name === connector.name ? (full as MCPConnector) : prev)),
      )
      .catch(() => {})
  }

  const backToConnectorList = () => {
    setSelectedConnector(null)
    setStep("type")
  }

  // GenericConnectorForm emits a CreateConnectionRequest-like payload:
  // { name, connection_type, connector_type, sync_mode?, cdc_mode?, config, description?, trace_id }.
  // We do the create POST + return-to navigation; errors thrown here propagate back
  // and the form renders them inline (do NOT swallow them).
  const handleSaveConnection = async (payload: Record<string, unknown>) => {
    if (!payload.connection_type) payload.connection_type = connectionType
    if (!payload.connector_type) payload.connector_type = selectedConnector?.name
    await saveConnection(payload)
    toast.success("Connection created successfully!")
    router.push(returnTo)
  }

  return (
    <div className="space-y-6 max-w-4xl mx-auto">
      {/* Header */}
      <div className="flex items-center gap-4">
        <Link href={returnTo}>
          <Button variant="ghost" size="icon">
            <ArrowLeft className="h-5 w-5" />
          </Button>
        </Link>
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-zinc-900 dark:text-white">
            Add New Connection
          </h1>
          <p className="text-sm text-zinc-500 mt-1">
            Connect a new data source or destination
          </p>
        </div>
      </div>

      {/* Progress Steps */}
      <div className="flex items-center gap-4 py-4 flex-wrap">
        <StepIndicator step={step} currentStep="direction" label="Direction" />
        <ArrowRight className="h-4 w-4 text-zinc-400" />
        <StepIndicator step={step} currentStep="type" label="Connector" />
        <ArrowRight className="h-4 w-4 text-zinc-400" />
        <StepIndicator step={step} currentStep="configure" label="Configure & Test" />
      </div>

      <AnimatePresence mode="wait">
        {/* Step 1: Select Direction */}
        {step === "direction" && (
          <motion.div
            key="direction"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -20 }}
          >
            <Card>
              <CardHeader>
                <CardTitle>Connection Direction</CardTitle>
                <CardDescription>Are you adding a data source or destination?</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 gap-4">
                  <button
                    onClick={() => handleDirectionSelect("source")}
                    className={cn(
                      "flex flex-col items-center gap-3 p-6 rounded-xl border-2 transition-all",
                      "border-zinc-200 dark:border-zinc-700 hover:border-violet-300",
                    )}
                  >
                    <div className="flex h-14 w-14 items-center justify-center rounded-xl bg-blue-100 dark:bg-blue-900/30">
                      <Database className="h-7 w-7 text-blue-600 dark:text-blue-400" />
                    </div>
                    <div className="text-center">
                      <p className="font-semibold text-zinc-900 dark:text-white">Source</p>
                      <p className="text-sm text-zinc-500 mt-1">Where data comes from</p>
                    </div>
                    <Badge variant="secondary">{sourceConnectors.length} connectors</Badge>
                  </button>
                  <button
                    onClick={() => handleDirectionSelect("destination")}
                    className={cn(
                      "flex flex-col items-center gap-3 p-6 rounded-xl border-2 transition-all",
                      "border-zinc-200 dark:border-zinc-700 hover:border-violet-300",
                    )}
                  >
                    <div className="flex h-14 w-14 items-center justify-center rounded-xl bg-purple-100 dark:bg-purple-900/30">
                      <Cloud className="h-7 w-7 text-purple-600 dark:text-purple-400" />
                    </div>
                    <div className="text-center">
                      <p className="font-semibold text-zinc-900 dark:text-white">Destination</p>
                      <p className="text-sm text-zinc-500 mt-1">Where data goes to</p>
                    </div>
                    <Badge variant="secondary">{destinationConnectors.length} connectors</Badge>
                  </button>
                </div>
              </CardContent>
            </Card>
          </motion.div>
        )}

        {/* Step 2: Select Connector Type */}
        {step === "type" && (
          <motion.div
            key="type"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -20 }}
            className="space-y-6"
          >
            <Card>
              <CardHeader>
                <CardTitle>
                  Select {connectionType === "source" ? "Source" : "Destination"} Connector
                </CardTitle>
                <CardDescription>
                  Choose a connector type ({availableConnectors.length} available)
                </CardDescription>
              </CardHeader>
              <CardContent>
                {loadingConnectors ? (
                  <div className="flex items-center justify-center py-12">
                    <Loader2 className="h-8 w-8 animate-spin text-zinc-400" />
                  </div>
                ) : availableConnectors.length === 0 ? (
                  <div className="text-center py-12">
                    <Database className="h-12 w-12 mx-auto text-zinc-300 mb-4" />
                    <p className="text-zinc-500">No connectors available</p>
                  </div>
                ) : (
                  <div className="space-y-6">
                    {Object.entries(groupedConnectors).map(([category, items]) => (
                      <div key={category}>
                        <h4 className="text-sm font-medium text-zinc-500 mb-3 capitalize">{category}</h4>
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                          {items.map((connector) => (
                            <button
                              key={connector.name}
                              onClick={() => handleConnectorSelect(connector)}
                              className="flex items-center gap-3 p-4 rounded-xl border border-zinc-200 dark:border-zinc-700 hover:border-violet-500 hover:bg-violet-50 dark:hover:bg-violet-950/30 transition-all group text-left"
                            >
                              <ConnectionLogo connector={connector} size="md" />
                              <div className="flex-1 min-w-0">
                                <p className="font-semibold text-zinc-900 dark:text-white group-hover:text-violet-700 dark:group-hover:text-violet-300 truncate">
                                  {connector.display_name}
                                </p>
                                <p className="text-xs text-zinc-500 truncate">{connector.description}</p>
                              </div>
                              {connector.supports_cdc && (
                                <Badge variant="outline" className="text-xs shrink-0">CDC</Badge>
                              )}
                            </button>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>

            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setStep("direction")}>
                <ArrowLeft className="h-4 w-4 mr-2" />
                Back
              </Button>
            </div>
          </motion.div>
        )}

        {/* Step 3: Configure + Test + Save (shared form) */}
        {step === "configure" && selectedConnector && (
          <motion.div
            key="configure"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -20 }}
            className="space-y-4"
          >
            {/* Selected-connector header with a "Change" affordance */}
            <Card>
              <CardContent className="p-4">
                <div className="flex items-center gap-3">
                  <ConnectionLogo connector={selectedConnector} size="md" />
                  <div className="flex-1 min-w-0">
                    <p className="font-semibold text-zinc-900 dark:text-white truncate">
                      {selectedConnector.display_name}
                    </p>
                    <p className="text-xs text-zinc-500 truncate">{selectedConnector.description}</p>
                  </div>
                  <Button variant="outline" size="sm" onClick={backToConnectorList}>
                    <ArrowLeft className="h-4 w-4 mr-2" />
                    Change
                  </Button>
                </div>
              </CardContent>
            </Card>

            <GenericConnectorForm
              connector={selectedConnector}
              initialData={{ connectionType, syncMode: preselectedSyncMode }}
              onSave={handleSaveConnection}
              onCancel={backToConnectorList}
            />
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

// Step indicator component
function StepIndicator({ step, currentStep, label }: { step: Step; currentStep: Step; label: string }) {
  const steps: Step[] = ["direction", "type", "configure"]
  const currentIndex = steps.indexOf(step)
  const targetIndex = steps.indexOf(currentStep)
  const isComplete = currentIndex > targetIndex
  const isCurrent = step === currentStep

  return (
    <div
      className={cn(
        "flex items-center gap-2 px-4 py-2 rounded-full text-sm font-medium transition-all",
        isComplete && "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300",
        isCurrent && "bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-300",
        !isComplete && !isCurrent && "bg-zinc-100 text-zinc-500 dark:bg-zinc-800",
      )}
    >
      {isComplete ? (
        <CheckCircle2 className="h-4 w-4" />
      ) : (
        <span
          className={cn(
            "w-5 h-5 rounded-full text-xs flex items-center justify-center",
            isCurrent ? "bg-violet-600 text-white" : "bg-zinc-300 text-zinc-600",
          )}
        >
          {targetIndex + 1}
        </span>
      )}
      {label}
    </div>
  )
}

export default function NewConnectionPage() {
  return (
    <Suspense fallback={<div>Loading...</div>}>
      <NewConnectionPageContent />
    </Suspense>
  )
}
