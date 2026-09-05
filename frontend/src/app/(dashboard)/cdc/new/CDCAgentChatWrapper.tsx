"use client"

import { useState, useEffect } from "react"
import { CDCAgentChat } from "@/components/cdc/CDCAgentChat"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Loader2, AlertCircle, Database, Plus, RefreshCw } from "lucide-react"
import type { CDCConnection } from "@/lib/api/types"
import { API_GATEWAY_URL } from "@/lib/config/api"

export function CDCAgentChatWrapper() {
  const [connections, setConnections] = useState<CDCConnection[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchConnections = async () => {
    setLoading(true)
    setError(null)
    
    try {
      // CDC connections live in the unified Connections API (API Gateway).
      // The old /cdc-connections endpoint is not guaranteed to exist and caused 404s.
      const response = await fetch(`${API_GATEWAY_URL}/api/v1/connections?type=source`, { cache: "no-store" })
      
      if (!response.ok) {
        throw new Error(`Server returned ${response.status}`)
      }

      const data = await response.json()
      
      // Filter to only CDC-capable source connections (databases).
      // We rely on `sync_mode === 'cdc'` for explicit CDC connections,
      // and fallback to detecting DB connectors in dev (mysql/postgresql/sqlserver)
      // so the UI remains usable. Keep in sync with the orchestrator's registered
      // CDC source providers (backend-orchestrator internal/cdc/provider.go).
      const raw = Array.isArray(data?.connections) ? data.connections : Array.isArray(data) ? data : []
      const isDbConnector = (t: string) => ["mysql", "postgresql", "postgres", "sqlserver", "mssql", "mongodb", "mongo"].includes(String(t || "").toLowerCase())

      const sources = raw
        .filter((c: any) => {
          if (String(c?.type || "").toLowerCase() !== "source") return false
          const syncMode = String(c?.sync_mode || "").toLowerCase()
          if (syncMode === "cdc") return true
          const connectorType = String(c?.connector_type || "")
          return isDbConnector(connectorType)
        })
        .map((c: any) => ({
          ...c,
          database_type: c.database_type || c.connector_type,
        }))
      
      setConnections(sources)
    } catch (err) {
      console.error("Failed to fetch connections:", err)
      setError(err instanceof Error ? err.message : "Failed to load connections")
      setConnections([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchConnections()
  }, [])

  if (loading) {
    return (
      <Card className="flex items-center justify-center h-full min-h-[400px] border-zinc-200 dark:border-zinc-800">
        <div className="flex flex-col items-center gap-4">
          <Loader2 className="h-8 w-8 animate-spin text-violet-600" />
          <p className="text-sm text-zinc-500 dark:text-zinc-400">
            Loading connections...
          </p>
        </div>
      </Card>
    )
  }

  if (error) {
    return (
      <Card className="flex items-center justify-center h-full min-h-[400px] border-zinc-200 dark:border-zinc-800">
        <div className="flex flex-col items-center gap-4 text-center">
          <div className="flex h-12 w-12 items-center justify-center rounded-full bg-red-100 dark:bg-red-900/30">
            <AlertCircle className="h-6 w-6 text-red-600 dark:text-red-400" />
          </div>
          <div>
            <h3 className="font-medium text-zinc-900 dark:text-white mb-1">
              Connection Error
            </h3>
            <p className="text-sm text-zinc-500 dark:text-zinc-400 mb-4">
              {error}
            </p>
            <Button onClick={fetchConnections} variant="outline" className="gap-2">
              <RefreshCw className="h-4 w-4" />
              Retry
            </Button>
          </div>
        </div>
      </Card>
    )
  }

  if (connections.length === 0) {
    return (
      <Card className="flex items-center justify-center h-full min-h-[400px] border-zinc-200 dark:border-zinc-800">
        <div className="flex flex-col items-center gap-4 text-center max-w-md">
          <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-gradient-to-br from-violet-500 to-indigo-500">
            <Database className="h-8 w-8 text-white" />
          </div>
          <div>
            <h3 className="text-xl font-semibold text-zinc-900 dark:text-white mb-2">
              No Database Connections
            </h3>
            <p className="text-sm text-zinc-500 dark:text-zinc-400 mb-6">
              Create a database connection first to set up CDC replication.
              You can connect PostgreSQL, MySQL, or other supported databases.
            </p>
            <Button asChild className="gap-2 bg-gradient-to-r from-violet-600 to-indigo-600">
              <a href="/connections/new?returnTo=/cdc/new&type=source&sync_mode=cdc">
                <Plus className="h-4 w-4" />
                Add Database Connection
              </a>
            </Button>
          </div>
        </div>
      </Card>
    )
  }

  return (
    <CDCAgentChat
      connections={connections}
      onPipelineCreated={(pipelineId) => {
        // Log pipeline creation - user will see success state with navigation buttons
        console.log("Pipeline created:", pipelineId)
      }}
    />
  )
}
