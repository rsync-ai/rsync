"use client"

import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { getConnectorStatusBadge, getConnectorConfidenceBadge, getConnectorConfidenceReasons, MCPConnector } from "@/lib/types/mcp-connector"
import { GenericConnectorForm } from "./GenericConnectorForm"
import { ConnectionLogo } from "./ConnectionLogo"
import { Badge } from "@/components/ui/badge"
import { Circle, Container, Info } from "lucide-react"

interface UnifiedConnectorModalProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  connector: MCPConnector | null
  // GenericConnectorForm emits a CreateConnectionRequest-like payload when creating
  // { name, connection_type, connector_type, sync_mode?, cdc_mode?, config, description?, trace_id? }
  onSave: (payload: Record<string, unknown>) => Promise<void>
  initialData?: {
    connectionName?: string
    connectionType?: "source" | "destination"
    syncMode?: "batch" | "cdc"
    cdcMode?: "initial" | "streaming_only"
    description?: string
    config?: Record<string, unknown>
  }
  isEditing?: boolean
  // Required for edit mode: lets GenericConnectorForm test the existing
  // connection by id (stored creds, never masked) and auto-reflect an issued
  // OAuth token. The #289 guard keys on `isEditing && connectionId`, so without
  // this the edit path silently no-ops. Omitted in create mode.
  connectionId?: string
}

export function UnifiedConnectorModal({
  open,
  onOpenChange,
  connector,
  onSave,
  initialData,
  isEditing = false,
  connectionId,
}: UnifiedConnectorModalProps) {
  if (!connector) return null

  // Get Docker status badge
  const getDockerStatusBadge = () => {
    // Live running/stopped state requires a Docker-socket read, which api-gateway
    // can't do (the socket is intentionally not mounted). For Dockerfile connectors
    // it instead reports docker_status="auto_deploys_on_use" with docker_deployed=false.
    // Surface that as a neutral "Auto-deploys on use" rather than an alarming
    // "Not Deployed" — matching the connectors-list card, which already shows "Ready".
    if (
      !connector.docker_deployed &&
      (connector.docker_status === "auto_deploys_on_use" || connector.has_dockerfile)
    ) {
      return (
        <Badge variant="outline" className="text-xs flex items-center gap-1 border-amber-500 text-amber-700 dark:text-amber-400">
          <Container className="h-2.5 w-2.5" />
          Auto-deploys on use
        </Badge>
      )
    }
    if (!connector.docker_deployed) {
      return (
        <Badge variant="outline" className="text-xs flex items-center gap-1">
          <Circle className="h-2 w-2 fill-zinc-400 text-zinc-400" />
          Not Deployed
        </Badge>
      )
    }

    switch (connector.docker_status) {
      case "running":
        return (
          <Badge variant="outline" className="text-xs flex items-center gap-1 border-green-500 text-green-700 dark:text-green-400">
            <Circle className="h-2 w-2 fill-green-500 text-green-500 animate-pulse" />
            Running
          </Badge>
        )
      case "stopped":
        return (
          <Badge variant="outline" className="text-xs flex items-center gap-1 border-amber-500 text-amber-700 dark:text-amber-400">
            <Circle className="h-2 w-2 fill-amber-500 text-amber-500" />
            Stopped
          </Badge>
        )
      case "restarting":
        return (
          <Badge variant="outline" className="text-xs flex items-center gap-1 border-blue-500 text-blue-700 dark:text-blue-400">
            <Circle className="h-2 w-2 fill-blue-500 text-blue-500 animate-pulse" />
            Restarting
          </Badge>
        )
      default:
        return (
          <Badge variant="outline" className="text-xs flex items-center gap-1">
            <Circle className="h-2 w-2 fill-zinc-400 text-zinc-400" />
            Unknown
          </Badge>
        )
    }
  }


  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <div className="flex items-start gap-4">
            {/* Connector Logo */}
            <div className="flex-shrink-0">
              <ConnectionLogo
                connector={connector}
                size="lg"
              />
            </div>

            {/* Title and Metadata */}
            <div className="flex-1 min-w-0">
              {/* No close button here — `DialogContent` renders one for every dialog. */}
              <div className="flex items-start justify-between gap-2">
                <DialogTitle className="text-2xl font-bold mb-2">
                  {isEditing ? "Edit" : "Configure"} {connector.display_name}
                </DialogTitle>
              </div>
              
              <p className="text-sm text-zinc-600 dark:text-zinc-400 mb-3">
                {connector.description}
              </p>

              {/* Status Badges */}
              <div className="flex flex-wrap items-center gap-2">
                {/* Category Badge */}
                <Badge variant="secondary" className="text-xs">
                  {connector.category.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())}
                </Badge>

                {/* Docker Status */}
                {connector.has_dockerfile && (
                  <>
                    <Container className="h-3 w-3 text-zinc-400" />
                    {getDockerStatusBadge()}
                  </>
                )}

                {/* Status (runtime-tested?) + QA confidence — two orthogonal
                    chips; confidence is hidden when unrated so it never masks
                    the Status chip. */}
                {(() => {
                  const statusBadge = getConnectorStatusBadge({
                    status: connector.status,
                    quality_tier: connector.quality_tier,
                    lifecycle: connector.lifecycle,
                  })
                  const confidenceBadge = getConnectorConfidenceBadge({
                    confidence_level: connector.confidence_level,
                    quality_tier: connector.quality_tier,
                    status: connector.status,
                    lifecycle: connector.lifecycle,
                  })
                  const reasons = getConnectorConfidenceReasons(connector)
                  const tip = reasons.length ? `\n${reasons.join("\n")}` : ""
                  return (
                    <>
                      <Badge
                        variant="secondary"
                        title={`Status: ${statusBadge.label}${tip}`}
                        className={`text-xs ${statusBadge.className}`}
                      >
                        {statusBadge.label}
                      </Badge>
                      {confidenceBadge && (
                        <Badge
                          variant="secondary"
                          title={`QA confidence: ${confidenceBadge.label}${tip}`}
                          className={`text-xs ${confidenceBadge.className}`}
                        >
                          {confidenceBadge.label}
                        </Badge>
                      )}
                    </>
                  )
                })()}

                {/* CDC Support */}
                {connector.supports_cdc && (
                  <Badge variant="outline" className="text-xs">
                    CDC Supported
                  </Badge>
                )}
              </div>

              {/* Supported DB engine versions (read-only, sourced from metadata.json) */}
              {connector.supported_versions &&
                (connector.supported_versions.batch || connector.supported_versions.cdc) && (
                  <div className="mt-3 rounded-md border border-zinc-200 bg-zinc-50 p-3 text-sm text-zinc-700 dark:border-zinc-800 dark:bg-zinc-900/40 dark:text-zinc-300">
                    <div className="flex items-start gap-2">
                      <Info className="h-4 w-4 mt-0.5 flex-shrink-0 text-zinc-400" />
                      <div>
                        <div className="font-medium text-zinc-900 dark:text-zinc-100">Supported versions</div>
                        <ul className="mt-1 space-y-1">
                          {connector.supported_versions.batch && (
                            <li>
                              <span className="font-medium">Batch:</span> {connector.supported_versions.batch}
                            </li>
                          )}
                          {connector.supported_versions.cdc && (
                            <li>
                              <span className="font-medium">CDC:</span> {connector.supported_versions.cdc}
                            </li>
                          )}
                        </ul>
                      </div>
                    </div>
                  </div>
                )}

              {/* Generator QA warnings (e.g. "does not declare supports_incremental;
                  skipped") are a connector-developer signal, not actionable by an
                  end user configuring a connection — deliberately not surfaced in
                  this configure-connection modal. */}
            </div>
          </div>
        </DialogHeader>

        {/* Form Content */}
        <div className="mt-6">
          <GenericConnectorForm
            connector={connector}
            onSave={onSave}
            onCancel={() => onOpenChange(false)}
            initialData={initialData}
            isEditing={isEditing}
            connectionId={connectionId}
          />
        </div>
      </DialogContent>
    </Dialog>
  )
}

