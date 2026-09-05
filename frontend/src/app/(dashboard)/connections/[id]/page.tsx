"use client"

import { useState, useEffect, use } from "react"
import { notFound, useRouter, useSearchParams } from "next/navigation"
import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  ArrowLeft,
  RefreshCw,
  Cloud,
  Server,
  Trash2,
  PlayCircle,
  AlertTriangle,
  KeyRound,
} from "lucide-react"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { toast } from "sonner"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import { MCPConnector } from "@/lib/types/mcp-connector"
import { OAuthConnectButton } from "@/components/oauth/OAuthConnectButton"
import { GenericConnectorForm } from "@/components/connectors/GenericConnectorForm"
import { ConnectionLogo } from "@/components/connectors/ConnectionLogo"
import { httpErrorFromResponse, parseApiError, classifyError } from "@/lib/utils/error-handling"
import type { ApiErrorBody } from "@/lib/api/types"

interface Connection {
  id: string
  name: string
  description?: string
  type: "source" | "destination"
  connector_type: string
  sync_mode?: "batch" | "cdc"
  cdc_mode?: "initial" | "streaming_only"
  config: Record<string, unknown>
  status: string
  is_connected?: boolean
  // OAuth token expiry surfaced by api-gateway (BUG-2/10/NEW-2).
  is_expired?: boolean
  created_at: string
  updated_at: string
}

type BlockingPipeline = {
  id: string
  name: string
  status: string
  role?: "source" | "destination" | "unknown"
}

interface Props {
  params: Promise<{ id: string }>
}

export default function ConnectionDetailPage({ params }: Props) {
  const { id } = use(params)
  const router = useRouter()
  const searchParams = useSearchParams()
  const type = searchParams.get("type") || "source"
  
  const [loading, setLoading] = useState(true)
  // notFound() must be thrown during RENDER, not from the fetch callback — this
  // carries the decision out of the effect and into the render body below.
  const [missing, setMissing] = useState(false)
  const [connection, setConnection] = useState<Connection | null>(null)
  const [connector, setConnector] = useState<MCPConnector | null>(null)
  const [editModalOpen, setEditModalOpen] = useState(false)
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [deleting, setDeleting] = useState(false)
  const [deleteBlockedBy, setDeleteBlockedBy] = useState<BlockingPipeline[] | null>(null)
  const [testing, setTesting] = useState(false)
  const [reconnecting, setReconnecting] = useState(false)

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true)
        
        // Fetch connection details
        const connResponse = await authFetch(API_ENDPOINTS.CONNECTIONS.GET(id), { cache: "no-store" })

        // 404 = deleted, or owned by a different workspace (the gateway returns
        // 404 rather than 403 so it never confirms a foreign resource exists).
        // That is not a load FAILURE, so it doesn't belong in the catch below —
        // bouncing to /connections behind "Could not load connections" told the
        // user the list was broken when only this id was out of scope. Render the
        // not-found boundary instead, which names the likely cause.
        if (connResponse.status === 404) {
          setMissing(true)
          return
        }
        if (!connResponse.ok) {
          throw new Error("Connection not found")
        }

        // API Gateway returns the connection object directly
        const connData = await connResponse.json()
        console.log('[Connection Detail] Loaded connection:', {
          id: connData.id,
          type: connData.type,
          connector_type: connData.connector_type,
          name: connData.name,
        })
        setConnection(connData)
        
        // Fetch connector metadata for the form schema (only if connector_type exists)
        if (connData.connector_type) {
          const connectorResponse = await authFetch(API_ENDPOINTS.CONNECTORS.GET(connData.connector_type), {
            cache: "no-store",
          })
          
          if (connectorResponse.ok) {
            const connectorData = await connectorResponse.json()
            setConnector(connectorData)
          }
        } else {
          console.warn("Connection missing connector_type:", connData)
        }
      } catch (err) {
        const e = classifyError(err, "connections.load")
        toast.error(e.title, { description: e.hint ?? e.message })
        router.push("/connections")
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [id, router])

  const handleSaveConnection = async (formData: Record<string, unknown>) => {
    if (!connection) {
      toast.error("Connection data not loaded")
      return
    }

    try {
      // API Gateway's UpdateConnectionRequest accepts: name, description, config, status.
      // Type and connector_type cannot be changed after creation.
      const payload: Record<string, unknown> = {
        name: (formData.name as string) || connection.name,
        config: formData.config as Record<string, unknown>,
        status: connection.status, // Keep existing status
      }
      // Send description only when the form provided one — pointer field on
      // the backend means omission keeps the existing value.
      if (typeof formData.description === "string") {
        payload.description = formData.description
      }

      // Validate required fields
      if (!payload.name || !payload.config) {
        console.error('[Update Connection] Missing required fields:', {
          name: !!payload.name,
          config: !!payload.config,
        })
        toast.error("Missing required connection information")
        return
      }

      console.log('[Update Connection] Sending payload:', {
        name: payload.name,
        has_config: !!payload.config,
        status: payload.status,
        has_description: typeof payload.description === "string",
      })
      
      const response = await authFetch(API_ENDPOINTS.CONNECTIONS.UPDATE(id), {
        method: "PUT",
        body: JSON.stringify(payload),
      })
      
      if (!response.ok) {
        const body = await response.json().catch(() => ({}))
        throw Object.assign(new Error(), { statusCode: response.status, message: (body as ApiErrorBody)?.error || (body as ApiErrorBody)?.message || `HTTP ${response.status}` })
      }

      toast.success("Connection updated successfully")
      setEditModalOpen(false)

      // Refresh connection data
      const updatedResponse = await authFetch(API_ENDPOINTS.CONNECTIONS.GET(id), { cache: "no-store" })
      if (updatedResponse.ok) {
        const connData = await updatedResponse.json()
        setConnection(connData)
      }
    } catch (err) {
      const e = classifyError(err, "connections.update")
      toast.error(e.title, { description: e.hint ?? e.message })
      throw err
    }
  }

  const handleDelete = async () => {
    let blocked = false
    try {
      setDeleting(true)
      setDeleteBlockedBy(null)
      const response = await authFetch(API_ENDPOINTS.CONNECTIONS.DELETE(id), {
        method: "DELETE",
      })
      
      if (!response.ok) {
        throw await httpErrorFromResponse(response, "Failed to delete connection")
      }
      
      toast.success("Connection deleted successfully")
      router.push("/connections")
    } catch (err) {
      const parsed = parseApiError(err)
      const raw = parsed.raw as Record<string, unknown> | undefined
      if (parsed.statusCode === 409 && raw && (raw["pipeline_count"] || raw["pipelines"])) {
        const pipelines = Array.isArray(raw["pipelines"]) ? (raw["pipelines"] as BlockingPipeline[]) : []
        setDeleteBlockedBy(pipelines)
        toast.error("Can’t delete this connection", {
          description: `This connection is still used by ${raw.pipeline_count || pipelines.length} pipeline(s).`,
        })
        // Keep dialog open so user can see what to fix.
        setDeleteDialogOpen(true)
        blocked = true
        return
      }
      const e = classifyError(err, "connections.delete")
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      setDeleting(false)
      // Only close when not blocked (avoid relying on async state updates)
      if (!blocked) setDeleteDialogOpen(false)
    }
  }

  const handleTestConnection = async () => {
    try {
      setTesting(true)
      const response = await authFetch(
        `${API_ENDPOINTS.CONNECTIONS.TEST_BY_ID(id)}?allow_draft=true`,
        { method: "POST" }
      )
      const result = await response.json()
      if (result.success) {
        toast.success("Connection test successful", {
          description:
            result.message && result.message !== "Connection test successful"
              ? result.message
              : "Connection is working properly",
        })
        // Optimistic update: show Connected immediately (backend doesn't persist is_connected on test)
        setConnection((prev) => (prev ? { ...prev, is_connected: true, is_expired: false } : prev))
        // Persist so the list page also shows Connected after navigating back
        sessionStorage.setItem(`connection_tested_${id}`, "true")
        // Refresh from backend; keep is_connected=true even if backend didn't persist it
        const updatedResponse = await authFetch(API_ENDPOINTS.CONNECTIONS.GET(id), { cache: "no-store" })
        if (updatedResponse.ok) {
          const fresh = await updatedResponse.json()
          setConnection({ ...fresh, is_connected: true, is_expired: false })
        }
      } else {
        toast.error("Connection test failed", {
          description: result.error || result.message || "Unable to connect",
        })
      }
    } catch (err) {
      const e = classifyError(err, "connections.test")
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      setTesting(false)
    }
  }

  const handleReconnectSuccess = async (newTokenId: string) => {
    if (!connection) return
    try {
      setReconnecting(true)
      const response = await authFetch(API_ENDPOINTS.CONNECTIONS.UPDATE(id), {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: connection.name,
          config: { ...connection.config, oauth_token_id: newTokenId },
        }),
      })
      if (response.ok) {
        toast.success("Re-authenticated successfully", {
          description: "OAuth credentials refreshed.",
        })
        const updatedResponse = await authFetch(API_ENDPOINTS.CONNECTIONS.GET(id), { cache: "no-store" })
        if (updatedResponse.ok) setConnection(await updatedResponse.json())
      } else {
        toast.error("Reconnect failed", {
          description: "Could not update connection with the new token.",
        })
      }
    } catch (err) {
      const e = classifyError(err, "connections.update")
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      setReconnecting(false)
    }
  }

  // Before the spinner check: once we know the id is out of scope there is
  // nothing left to load. Hands off to connections/[id]/not-found.tsx.
  if (missing) {
    notFound()
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <RefreshCw className="h-8 w-8 animate-spin text-zinc-400" />
      </div>
    )
  }

  if (!connection) {
    return (
      <div className="text-center py-12">
        <p className="text-zinc-500">Connection not found</p>
        <Button variant="outline" className="mt-4" asChild>
          <Link href="/connections">Back to Connections</Link>
        </Button>
      </div>
    )
  }

  return (
    <div className="space-y-6 max-w-4xl">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Link href="/connections">
            <Button variant="ghost" size="icon">
              <ArrowLeft className="h-5 w-5" />
            </Button>
          </Link>
          <ConnectionLogo connectorType={connection.connector_type} size="lg" />
          <div>
            <div className="flex items-center gap-3">
              <h1 className="text-2xl font-bold tracking-tight text-zinc-900 dark:text-white">
                {connection.name}
              </h1>
              <Badge
                variant="outline"
                className={connection.type === "source"
                  ? "bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-900/30 dark:text-blue-300"
                  : "bg-purple-50 text-purple-700 border-purple-200 dark:bg-purple-900/30 dark:text-purple-300"
                }
              >
                {connection.type === "source" ? <Server className="h-3 w-3 mr-1" /> : <Cloud className="h-3 w-3 mr-1" />}
                {connection.type}
              </Badge>
              {connection.sync_mode && (
                <Badge variant="secondary">
                  {connection.sync_mode === "cdc" ? "Real-time CDC" : "Batch"}
                </Badge>
              )}
              {connection.is_expired && (
                <Badge className="bg-red-100 text-red-700 border-red-200 dark:bg-red-900/30 dark:text-red-300 dark:border-red-800">
                  <AlertTriangle className="h-3 w-3 mr-1" />
                  Token expired — re-authenticate
                </Badge>
              )}
            </div>
            <p className="text-sm text-zinc-500 mt-1 capitalize">
              {connection.connector_type} connection
            </p>
          </div>
        </div>
        
        <div className="flex items-center gap-3">
          {/* Re-authenticate button — only shown when oauth token is expired */}
          {connection.is_expired && connector?.oauth_provider && (
            <OAuthConnectButton
              provider={connector.oauth_provider}
              displayName={connection.name}
              label="Re-authenticate"
              variant="outline"
              size="default"
              disabled={reconnecting}
              onSuccess={handleReconnectSuccess}
              className="border-amber-400 text-amber-700 hover:bg-amber-50 dark:border-amber-600 dark:text-amber-400 dark:hover:bg-amber-950/30"
            />
          )}
          <Button
            variant="outline"
            onClick={handleTestConnection}
            disabled={testing}
          >
            {testing ? (
              <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
            ) : (
              <PlayCircle className="h-4 w-4 mr-2" />
            )}
            {testing ? "Testing..." : "Test Connection"}
          </Button>
          <Button variant="outline" onClick={() => setEditModalOpen(true)}>
            Edit Connection
          </Button>
          <Button
            variant="outline"
            className="text-red-600 hover:text-red-700 hover:bg-red-50"
            onClick={() => {
              setDeleteBlockedBy(null)
              setDeleteDialogOpen(true)
            }}
          >
            <Trash2 className="h-4 w-4 mr-2" />
            Delete
          </Button>
        </div>
      </div>

      {/* Connection Details Card */}
      <Card>
        <CardContent className="p-6">
          <div className="grid gap-6 md:grid-cols-2">
            <div>
              <h3 className="text-sm font-medium text-zinc-500 mb-1">Connection ID</h3>
              <p className="font-mono text-sm">{connection.id}</p>
            </div>
            <div>
              <h3 className="text-sm font-medium text-zinc-500 mb-1">Connector Type</h3>
              <p className="capitalize">{connection.connector_type}</p>
            </div>
            <div>
              <h3 className="text-sm font-medium text-zinc-500 mb-1">Status</h3>
              {connection.is_expired ? (
                <Badge className="bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300">
                  <AlertTriangle className="h-3 w-3 mr-1" />
                  Token Expired
                </Badge>
              ) : (
                <Badge variant={connection.status === "active" ? "default" : "secondary"}>
                  {connection.status}
                </Badge>
              )}
            </div>
            <div>
              <h3 className="text-sm font-medium text-zinc-500 mb-1">Sync Mode</h3>
              <p>
                {connection.sync_mode === "cdc" 
                  ? connection.cdc_mode === "streaming_only"
                    ? "Real-time CDC (Streaming Only)"
                    : "Real-time CDC (Historical + Streaming)"
                  : "Batch"}
              </p>
            </div>
            <div>
              <h3 className="text-sm font-medium text-zinc-500 mb-1">Created</h3>
              <p>{new Date(connection.created_at).toLocaleString()}</p>
            </div>
            <div>
              <h3 className="text-sm font-medium text-zinc-500 mb-1">Last Updated</h3>
              <p>{new Date(connection.updated_at).toLocaleString()}</p>
            </div>
          </div>
          
          {connection.description && (
            <div className="mt-6 pt-6 border-t">
              <h3 className="text-sm font-medium text-zinc-500 mb-1">Description</h3>
              <p className="text-zinc-700 dark:text-zinc-300">{connection.description}</p>
            </div>
          )}

          {/* Configuration Summary */}
          <div className="mt-6 pt-6 border-t">
            <h3 className="text-sm font-medium text-zinc-500 mb-3">Configuration</h3>
            <div className="grid gap-3 md:grid-cols-2">
              {connection.config && Object.entries(connection.config).map(([key, value]) => {
                // Hide sensitive fields
                const isSensitive = ['password', 'secret', 'token', 'api_key', 'secret_key', 'access_key'].some(
                  s => key.toLowerCase().includes(s)
                )
                return (
                  <div key={key} className="flex items-center justify-between p-3 bg-zinc-50 dark:bg-zinc-800/50 rounded-lg">
                    <span className="text-sm text-zinc-600 dark:text-zinc-400 capitalize">
                      {key.replace(/_/g, ' ')}
                    </span>
                    <span className="text-sm font-medium text-zinc-900 dark:text-white">
                      {isSensitive ? '••••••••' : String(value)}
                    </span>
                  </div>
                )
              })}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Edit Modal */}
      <Dialog open={editModalOpen} onOpenChange={setEditModalOpen}>
        <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            {/* No close button here — `DialogContent` renders one for every dialog. */}
            <DialogTitle className="flex items-center gap-3">
              <ConnectionLogo connectorType={connection.connector_type} size="sm" />
              <span className="flex-1">Edit {connection.name}</span>
            </DialogTitle>
          </DialogHeader>
          
          {connector ? (
            <GenericConnectorForm
              connector={{
                ...connector,
                // Pre-set the connection type based on existing connection
                supports_source: connection.type === "source",
                supports_destination: connection.type === "destination",
              }}
              initialData={{
                connectionName: connection.name,
                connectionType: connection.type,
                syncMode: connection.sync_mode || "batch",
                cdcMode: connection.cdc_mode || "initial",
                description: connection.description || "",
                config: connection.config,
              }}
              onSave={handleSaveConnection}
              onCancel={() => setEditModalOpen(false)}
              isEditing={true}
              connectionId={connection.id}
            />
          ) : (
            <div className="py-8 text-center text-zinc-500">
              <RefreshCw className="h-6 w-6 animate-spin mx-auto mb-2" />
              Loading connector schema...
            </div>
          )}
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Connection</AlertDialogTitle>
            <AlertDialogDescription className="space-y-3">
              <div>
                Are you sure you want to delete &quot;{connection.name}&quot;? This action cannot be undone.
              </div>

              {deleteBlockedBy && deleteBlockedBy.length > 0 && (
                <div className="rounded-md border border-red-200 bg-red-50 p-3 text-red-900 dark:border-red-900/40 dark:bg-red-900/20 dark:text-red-100">
                  <div className="font-medium">Can’t delete: used by these pipelines</div>
                  <ul className="mt-2 list-disc space-y-1 pl-5">
                    {deleteBlockedBy.map((p) => (
                      <li key={p.id}>
                        <Link href={`/pipelines/${p.id}`} className="underline underline-offset-2">
                          {p.name || p.id}
                        </Link>{" "}
                        <span className="text-xs opacity-80">({p.role || "pipeline"}, {p.status})</span>
                      </li>
                    ))}
                  </ul>
                  <div className="mt-2 text-xs opacity-90">
                    Detach this connection from those pipelines (or delete/archive them), then try again.
                  </div>
                </div>
              )}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel
              disabled={deleting}
              onClick={() => {
                setDeleteBlockedBy(null)
                setDeleteDialogOpen(false)
              }}
            >
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDelete}
              disabled={deleting}
              className="bg-red-600 hover:bg-red-700"
            >
              {deleting ? (
                <>
                  <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                  Deleting...
                </>
              ) : (
                "Delete"
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
