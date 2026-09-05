"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { GenericConnectorForm } from "@/components/connectors/GenericConnectorForm"
import { fetchMCPConnector } from "@/lib/api/mcp-connectors"
import { Loader2 } from "lucide-react"
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
  const [saving, setSaving] = useState(false)
  
  // Load connector metadata when modal opens
  useEffect(() => {
    if (open && connectorType) {
      loadConnector()
    }
  }, [open, connectorType])
  
  const loadConnector = async () => {
    setLoading(true)
    try {
      const data = await fetchMCPConnector(connectorType)
      setConnector(data)
    } catch (error) {
      console.error("Failed to load connector:", error)
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
      const connectionId = result.id || crypto.randomUUID()

      toast.success(`${direction === "source" ? "Source" : "Destination"} connection configured successfully`)
      onSave(connectionId, (payload.config as Record<string, unknown>) || {})
      onClose()
    } catch (error) {
      console.error("Failed to save connection:", error)
      toast.error("Failed to save connection")
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
          <div className="py-8 text-center text-zinc-500">
            Connector not found: {connectorType}
          </div>
        )}
      </DialogContent>
    </Dialog>
  )
}

