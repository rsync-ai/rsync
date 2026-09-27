"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { GenericConnectorForm } from "@/components/connectors/GenericConnectorForm"
import { fetchMCPConnector } from "@/lib/api/mcp-connectors"
import { Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { toast } from "sonner"
import { saveConnection } from "@/lib/api/mcp-connectors"

interface ConnectorConfigModalProps {
  open: boolean
  onClose: () => void
  connectorType: string
  direction: "source" | "destination"
  onSave: (connectionId: string, config: Record<string, unknown>) => void
  pipelineId?: string
}

/**
 * The id the server gave this connection, or an error.
 *
 * The call site read `result.id || crypto.randomUUID()`: when the server
 * returned no id, the client MINTED one, toasted "configured successfully", and
 * handed that id to onSave, which wires it into the pipeline. The pipeline then
 * pointed at a connection that exists in no database, and the failure surfaced
 * much later, far from its cause, as an unresolvable connection id. A create
 * response with no id is a failed create, whatever status code carried it.
 */
export function requireConnectionId(result: { id?: unknown } | null | undefined): string {
  const id = typeof result?.id === "string" ? result.id.trim() : ""
  if (!id) {
    throw new Error("The server accepted the request but returned no connection id")
  }
  return id
}

export function ConnectorConfigModal({
  open,
  onClose,
  connectorType,
  direction,
  onSave,
  pipelineId,
}: ConnectorConfigModalProps) {
  const [loading, setLoading] = useState(false)
  const [connector, setConnector] = useState<any>(null)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  
  // Load connector metadata when modal opens
  useEffect(() => {
    if (open && connectorType) {
      loadConnector()
    }
  }, [open, connectorType])
  
  const loadConnector = async () => {
    setLoading(true)
    setLoadError(null)
    try {
      const data = await fetchMCPConnector(connectorType)
      setConnector(data)
    } catch (error) {
      console.error("Failed to load connector:", error)
      setLoadError(error instanceof Error ? error.message : `Failed to load connector: ${connectorType}`)
      toast.error(`Failed to load connector: ${connectorType}`)
    } finally {
      setLoading(false)
    }
  }
  
  // GenericConnectorForm emits a CreateConnectionRequest-like payload when creating
  // { name, connection_type, connector_type, sync_mode?, cdc_mode?, config, description?, trace_id? }
  const handleSave = async (payload: Record<string, unknown>) => {
    setSaving(true)
    try {
      // Ensure connection_type aligns with modal direction (defensive)
      if (!payload.connection_type) {
        payload.connection_type = direction
      }
      if (!payload.connector_type) {
        payload.connector_type = connectorType
      }

      const result = await saveConnection(payload)

      const connectionId = requireConnectionId(result)

      toast.success(`${direction === "source" ? "Source" : "Destination"} connection configured successfully`)
      onSave(connectionId, (payload.config as Record<string, unknown>) || {})
      onClose()
    } catch (error) {
      console.error("Failed to save connection:", error)
      // The server says WHY (duplicate name, unreachable host, bad credentials).
      // A flat "Failed to save connection" threw all of that away and left the
      // user to guess which field to change.
      const detail = error instanceof Error ? error.message.trim() : ""
      toast.error(detail ? `Failed to save connection: ${detail}` : "Failed to save connection")
    } finally {
      setSaving(false)
    }
  }
  
  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>
            Configure {direction === "source" ? "Source" : "Destination"} Connection
          </DialogTitle>
          <DialogDescription>
            The pipeline needs a {connectorType} connection. Please provide the configuration details.
          </DialogDescription>
        </DialogHeader>
        
        {loading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin text-violet-600" />
          </div>
        ) : connector ? (
          <GenericConnectorForm
            connector={connector}
            onSave={handleSave}
            onCancel={onClose}
            initialData={{
              connectionName: `${direction === "source" ? "Source" : "Destination"} - ${connector.display_name || connector.name}`,
              connectionType: direction,
            }}
          />
        ) : (
          // fetchMCPConnector THROWS on every failure (mcp-connectors.ts:56) and
          // only returns on success, so a null connector here always means the
          // lookup failed -- never that the catalog lacks this connector. The old
          // copy, "Connector not found: postgresql", told the user their setup was
          // wrong when the truth was that the request never landed.
          <div className="py-8 text-center space-y-3">
            <p className="text-sm text-zinc-700 dark:text-zinc-300">
              Could not load the {connectorType} connector
            </p>
            <p className="text-xs text-zinc-500 dark:text-zinc-400">
              {loadError || "The request to the connector catalog did not complete."} This does not
              mean the connector is unavailable.
            </p>
            <Button variant="outline" size="sm" onClick={loadConnector} disabled={loading}>
              Retry
            </Button>
          </div>
        )}
      </DialogContent>
    </Dialog>
  )
}

