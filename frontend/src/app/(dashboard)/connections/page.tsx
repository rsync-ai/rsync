"use client"

import { useState, useEffect } from "react"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"
import Link from "next/link"
import { PageHeader } from "@/components/layout/PageHeader"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import {
  Database,
  Plus,
  Search,
  Edit,
  Trash2,
  CheckCircle2,
  RefreshCw,
  Cloud,
  Server,
  MoreHorizontal,
  ArrowRight,
  Zap,
  Clock,
  PlayCircle,
  AlertTriangle,
} from "lucide-react"
import { OAuthConnectButton } from "@/components/oauth/OAuthConnectButton"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
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
import { ConnectionLogo } from "@/components/connectors/ConnectionLogo"
import { API_ENDPOINTS } from "@/lib/config/api"
import { httpErrorFromResponse, parseApiError, classifyError, type AppError } from "@/lib/utils/error-handling"
import type { ApiErrorBody } from "@/lib/api/types"
import { authFetch } from "@/lib/api/auth-fetch"
import { captureWorkspace, onActiveWorkspaceChange } from "@/lib/workspace/active-workspace"
import { ErrorBanner } from "@/components/ui/error-banner"

interface Connection {
  id: string
  name: string
  description: string
  type: "source" | "destination"
  connector_type: string
  sync_mode?: "batch" | "cdc"
  cdc_mode?: "initial" | "streaming_only"
  is_connected: boolean
  last_tested_at?: string
  connection_error?: string
  config?: Record<string, unknown>
  created_at: string
  updated_at: string
  // OAuth token expiry surfaced by api-gateway (BUG-2/10). is_expired is true
  // only when the linked oauth_tokens row has a real past expiry; offline
  // tokens (Shopify) report false.
  token_expires_at?: string | null
  is_expired?: boolean
}

interface OAuthToken {
  id: string
  provider: string
  expired: boolean
  expires_at: string
  account_name?: string
}

type BlockingPipeline = {
  id: string
  name: string
  status: string
  role?: "source" | "destination" | "unknown"
}

function formatDate(dateStr: string): string {
  const date = new Date(dateStr)
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  })
}

export default function ConnectionsPage() {
  // Role gate: creating/testing connections is member+ (mirrors the backend
  // requireWorkspaceRole on CreateConnection/TestConnection). Viewers see the
  // list read-only — the create CTAs are hidden rather than failing on click.
  const { can } = useWorkspaceRole()
  const canCreate = can("create_pipeline")
  const [connections, setConnections] = useState<Connection[]>([])
  const [oauthTokens, setOAuthTokens] = useState<OAuthToken[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<AppError | null>(null)
  const [searchQuery, setSearchQuery] = useState("")
  const [activeTab, setActiveTab] = useState<"all" | "source" | "destination">("all")
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [connectionToDelete, setConnectionToDelete] = useState<Connection | null>(null)
  const [deleting, setDeleting] = useState(false)
  const [deleteBlockedBy, setDeleteBlockedBy] = useState<BlockingPipeline[] | null>(null)
  const [testingId, setTestingId] = useState<string | null>(null)
  const [reconnectingId, setReconnectingId] = useState<string | null>(null)
  const [mounted, setMounted] = useState(false)

  // Fix hydration mismatch by only rendering interactive elements after mount
  useEffect(() => {
    setMounted(true)
  }, [])

  // Build a lookup map: token_id → OAuthToken (for expired checks)
  const tokenById = new Map<string, OAuthToken>(oauthTokens.map((t) => [t.id, t]))

  // Returns the OAuth token for a connection if it has one, or null
  function getConnectionToken(connection: Connection): OAuthToken | null {
    const tokenId = connection.config?.oauth_token_id as string | undefined
    if (!tokenId) return null
    return tokenById.get(tokenId) ?? null
  }

  const fetchOAuthTokens = async () => {
    const isStale = captureWorkspace()
    try {
      const response = await authFetch(API_ENDPOINTS.OAUTH.TOKENS, { cache: "no-store" })
      if (!response.ok) return
      const data = await response.json()
      if (isStale()) return
      setOAuthTokens(data.tokens || [])
    } catch {
      // Non-critical: silently ignore token fetch failures
    }
  }

  const fetchConnections = async () => {
    // A switch mid-flight makes this response the previous workspace's list.
    const isStale = captureWorkspace()
    try {
      setLoading(true)
      const response = await authFetch(API_ENDPOINTS.CONNECTIONS.LIST, { cache: "no-store" })

      if (!response.ok) {
        const body = await response.json().catch(() => ({}))
        throw Object.assign(new Error(), { statusCode: response.status, message: (body as ApiErrorBody)?.error || (body as ApiErrorBody)?.message || `HTTP ${response.status}` })
      }

      const data = await response.json()
      if (isStale()) return
      const fresh: Connection[] = data.connections || []
      // Overlay optimistic test-success results so the list reflects what was tested
      // in the detail page (workaround: backend doesn't persist is_connected on test).
      setConnections(
        fresh.map((c) =>
          sessionStorage.getItem(`connection_tested_${c.id}`) === "true"
            ? { ...c, is_connected: true }
            : c
        )
      )
      setError(null)
    } catch (err) {
      if (isStale()) return
      setError(classifyError(err, "connections.load"))
      setConnections([])
    } finally {
      setLoading(false)
    }
  }

  const handleReconnectSuccess = async (connectionId: string, newTokenId: string) => {
    try {
      setReconnectingId(connectionId)
      const connection = connections.find((c) => c.id === connectionId)
      if (!connection) return

      // Update the connection with the new token id
      const response = await authFetch(API_ENDPOINTS.CONNECTIONS.UPDATE(connectionId), {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: connection.name,
          config: { ...connection.config, oauth_token_id: newTokenId },
        }),
      })

      if (response.ok) {
        toast.success("OAuth reconnected", {
          description: "Your credentials have been refreshed successfully.",
        })
        // Refresh both connections and tokens
        await Promise.all([fetchConnections(), fetchOAuthTokens()])
      } else {
        toast.error("Reconnect failed", {
          description: "Could not update the connection with the new token.",
        })
      }
    } catch (err) {
      const e = classifyError(err, "connections.update")
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      setReconnectingId(null)
    }
  }

  useEffect(() => {
    fetchConnections()
    fetchOAuthTokens()
  }, [])

  // Re-fetch when the active workspace changes (header switcher / other tab) so
  // the list never shows the previous workspace's connections. authFetch already
  // scopes each request to the active workspace; this just re-triggers it. (R4)
  useEffect(
    () =>
      onActiveWorkspaceChange(() => {
        fetchConnections()
        fetchOAuthTokens()
      }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  )

  const handleDelete = async () => {
    if (!connectionToDelete) return
    
    try {
      setDeleting(true)
      setDeleteBlockedBy(null)
      const response = await authFetch(
        API_ENDPOINTS.CONNECTIONS.DELETE(connectionToDelete.id),
        {
          method: "DELETE",
        }
      )
      
      if (!response.ok) {
        throw await httpErrorFromResponse(response, "Failed to delete connection")
      }
      
      toast.success("Connection deleted successfully")
      await fetchConnections()
      setDeleteDialogOpen(false)
      setConnectionToDelete(null)
    } catch (err) {
      const parsed = parseApiError(err)
      const raw = parsed.raw as Record<string, unknown> | undefined
      if (parsed.statusCode === 409 && raw && (raw["pipeline_count"] || raw["pipelines"])) {
        const pipelines = Array.isArray(raw["pipelines"]) ? (raw["pipelines"] as BlockingPipeline[]) : []
        setDeleteBlockedBy(pipelines)
        toast.error("Can’t delete this connection", {
          description: `This connection is still used by ${raw["pipeline_count"] || pipelines.length} pipeline(s).`,
        })
        // Keep dialog open so the user can see what to fix.
        setDeleteDialogOpen(true)
        return
      }
      const e = classifyError(err, "connections.delete")
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      setDeleting(false)
    }
  }

  const handleTestConnection = async (connection: Connection) => {
    try {
      setTestingId(connection.id)
      const response = await authFetch(
        `${API_ENDPOINTS.CONNECTIONS.TEST_BY_ID(connection.id)}?allow_draft=true`,
        {
          method: "POST",
        }
      )
      
      const result = await response.json()
      
      if (result.success) {
        toast.success("Connection test successful", {
          description: result.message || "Connection is working properly",
        })
        // Persist test result so list page retains "Connected" badge after re-fetch
        sessionStorage.setItem(`connection_tested_${connection.id}`, "true")
        await fetchConnections() // Refresh to update status (overlay applied inside)
      } else {
        toast.error("Connection test failed", {
          description: result.error || result.message || "Unable to connect",
        })
      }
    } catch (err) {
      const e = classifyError(err, "connections.test")
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      setTestingId(null)
    }
  }

  const filteredConnections = connections.filter((conn) => {
    const searchLower = searchQuery.toLowerCase()
    const matchesSearch = (conn.name?.toLowerCase() || "").includes(searchLower) ||
      (conn.connector_type?.toLowerCase() || "").includes(searchLower)
    const matchesTab = activeTab === "all" || conn.type === activeTab
    return matchesSearch && matchesTab
  })

  const sourceCount = connections.filter(c => c.type === "source").length
  const destinationCount = connections.filter(c => c.type === "destination").length

  return (
    <div className="space-y-6">
      <PageHeader
        heading="Connections"
        description="Manage your data source and destination connections"
      >
        {canCreate && (
          <Link href="/connectors">
            <Button>
              <Plus className="h-4 w-4 mr-2" />
              Add connection
            </Button>
          </Link>
        )}
      </PageHeader>

      {/* Filters */}
      <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between">
        {/* Tabs */}
        <div className="flex gap-2 p-1 bg-zinc-100 dark:bg-zinc-800 rounded-xl">
          <button
            onClick={() => setActiveTab("all")}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
              activeTab === "all"
                ? "bg-white dark:bg-zinc-700 shadow-sm text-zinc-900 dark:text-white"
                : "text-zinc-600 dark:text-zinc-400 hover:text-zinc-900 dark:hover:text-white"
            }`}
          >
            All
            <Badge variant="secondary" className="ml-2">
              {connections.length}
            </Badge>
          </button>
          <button
            onClick={() => setActiveTab("source")}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
              activeTab === "source"
                ? "bg-white dark:bg-zinc-700 shadow-sm text-zinc-900 dark:text-white"
                : "text-zinc-600 dark:text-zinc-400 hover:text-zinc-900 dark:hover:text-white"
            }`}
          >
            <Server className="h-4 w-4 inline mr-1" />
            Sources
            <Badge variant="secondary" className="ml-2">
              {sourceCount}
            </Badge>
          </button>
          <button
            onClick={() => setActiveTab("destination")}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
              activeTab === "destination"
                ? "bg-white dark:bg-zinc-700 shadow-sm text-zinc-900 dark:text-white"
                : "text-zinc-600 dark:text-zinc-400 hover:text-zinc-900 dark:hover:text-white"
            }`}
          >
            <Cloud className="h-4 w-4 inline mr-1" />
            Destinations
            <Badge variant="secondary" className="ml-2">
              {destinationCount}
            </Badge>
          </button>
        </div>

        {/* Search */}
        <div className="relative w-full sm:w-80">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-zinc-400" />
          <Input
            placeholder="Search connections..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10"
          />
        </div>
      </div>

      {/* Loading state */}
      {loading && (
        <div className="flex items-center justify-center py-12">
          <RefreshCw className="h-8 w-8 animate-spin text-zinc-400" />
        </div>
      )}

      {/* Error state */}
      {error && !loading && (
        <Card className="p-6">
          <ErrorBanner error={error} onRetry={fetchConnections} />
        </Card>
      )}

      {/* Empty state */}
      {!loading && !error && connections.length === 0 && (
        <Card className="flex flex-col items-center justify-center py-16 border-zinc-200 dark:border-zinc-800">
          <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-gradient-to-br from-violet-500 to-indigo-500 mb-6">
            <Database className="h-8 w-8 text-white" />
          </div>
          <h3 className="text-xl font-semibold text-zinc-900 dark:text-white mb-2">
            No Connections Yet
          </h3>
          <p className="text-sm text-zinc-500 dark:text-zinc-400 mb-6 text-center max-w-md">
            Create your first connection to start building data pipelines.
            Browse our connectors catalog to add a source or destination.
          </p>
          {canCreate ? (
            <Button className="gap-2 bg-gradient-to-r from-violet-600 to-indigo-600" asChild>
              <Link href="/connectors">
                <Plus className="h-4 w-4" />
                Browse Connectors
              </Link>
            </Button>
          ) : (
            <p className="text-sm text-zinc-500 dark:text-zinc-400">
              You have view-only access to this workspace.
            </p>
          )}
        </Card>
      )}

      {/* Connections list */}
      {!loading && !error && filteredConnections.length > 0 && (
        <div className="grid gap-4">
          {filteredConnections.map((connection) => (
            <Card
              key={connection.id}
              className="p-6 border-zinc-200 dark:border-zinc-800 hover:border-violet-300 dark:hover:border-violet-800 transition-colors"
            >
              <div className="flex items-start justify-between">
                <div className="flex items-start gap-4">
                  <ConnectionLogo 
                    connectorType={connection.connector_type} 
                    size="md"
                    category="database"
                  />
                  <div>
                    <div className="flex items-center gap-3 mb-1">
                      <h3 className="font-semibold text-zinc-900 dark:text-white">
                        {connection.name}
                      </h3>
                      <Badge
                        variant="outline"
                        className={connection.type === "source" 
                          ? "bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-900/30 dark:text-blue-300 dark:border-blue-800" 
                          : "bg-purple-50 text-purple-700 border-purple-200 dark:bg-purple-900/30 dark:text-purple-300 dark:border-purple-800"
                        }
                      >
                        {connection.type === "source" ? (
                          <Server className="h-3 w-3 mr-1" />
                        ) : (
                          <Cloud className="h-3 w-3 mr-1" />
                        )}
                        {connection.type}
                      </Badge>
                      {connection.is_connected ? (
                        <Badge className="bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300">
                          <CheckCircle2 className="h-3 w-3 mr-1" />
                          Connected
                        </Badge>
                      ) : connection.is_expired ? (
                        <Badge className="bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300">
                          <AlertTriangle className="h-3 w-3 mr-1" />
                          Token expired
                        </Badge>
                      ) : (
                        <Badge variant="secondary">
                          Not tested
                        </Badge>
                      )}
                      {/* Sync Mode Badge - Only for sources */}
                      {connection.type === "source" && connection.sync_mode && (
                        <Badge
                          variant="outline"
                          className={connection.sync_mode === "cdc"
                            ? connection.cdc_mode === "streaming_only"
                              ? "bg-amber-50 text-amber-700 border-amber-200 dark:bg-amber-900/30 dark:text-amber-300 dark:border-amber-800"
                              : "bg-emerald-50 text-emerald-700 border-emerald-200 dark:bg-emerald-900/30 dark:text-emerald-300 dark:border-emerald-800"
                            : "bg-slate-50 text-slate-700 border-slate-200 dark:bg-slate-900/30 dark:text-slate-300 dark:border-slate-800"
                          }
                        >
                          {connection.sync_mode === "cdc" ? (
                            <Zap className="h-3 w-3 mr-1" />
                          ) : (
                            <Clock className="h-3 w-3 mr-1" />
                          )}
                          {connection.sync_mode === "cdc"
                            ? connection.cdc_mode === "streaming_only"
                              ? "CDC (Streaming)"
                              : "CDC (Full Sync)"
                            : "Batch"}
                        </Badge>
                      )}
                      {/* Expired OAuth token badge */}
                      {(() => {
                        const token = getConnectionToken(connection)
                        // Suppress "Reconnect required" when is_connected=true — test just passed
                        return token?.expired && !connection.is_connected ? (
                          <Badge
                            variant="outline"
                            className="bg-orange-50 text-orange-700 border-orange-200 dark:bg-orange-900/30 dark:text-orange-300 dark:border-orange-800"
                          >
                            <AlertTriangle className="h-3 w-3 mr-1" />
                            Reconnect required
                          </Badge>
                        ) : null
                      })()}
                    </div>
                    <p className="text-sm text-zinc-500 dark:text-zinc-400 mb-2">
                      {connection.description || `${connection.connector_type} connection`}
                    </p>
                    <div className="flex items-center gap-4 text-xs text-zinc-400">
                      <span>Type: {connection.connector_type}</span>
                      <span>Created: {formatDate(connection.created_at)}</span>
                    </div>
                  </div>
                </div>

                <div className="flex items-center gap-2">
                  {/* Reconnect button for expired OAuth tokens */}
                  {mounted && (() => {
                    const token = getConnectionToken(connection)
                    // Hide Reconnect button when connection is already verified working
                    if (!token?.expired || connection.is_connected) return null
                    return (
                      <OAuthConnectButton
                        provider={token.provider}
                        displayName={token.provider}
                        label="Reconnect"
                        variant="outline"
                        size="sm"
                        disabled={reconnectingId === connection.id}
                        onSuccess={(newTokenId) => handleReconnectSuccess(connection.id, newTokenId)}
                        className="border-orange-300 text-orange-700 hover:bg-orange-50 dark:border-orange-700 dark:text-orange-300 dark:hover:bg-orange-950/30"
                      />
                    )
                  })()}
                  <Button variant="ghost" size="sm" asChild>
                    <Link href={`/connections/${connection.id}?type=${connection.type}`}>
                      View Details
                      <ArrowRight className="h-4 w-4 ml-1" />
                    </Link>
                  </Button>
                  {mounted && (
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      {/* Name it after the connection, not a constant: there is one of
                          these per row, and N identical "Actions" buttons are unusable
                          in a screen-reader element list. */}
                      <Button variant="ghost" size="icon" aria-label={`Actions for ${connection.name}`}>
                        <MoreHorizontal className="h-4 w-4" />
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end">
                      <DropdownMenuItem
                        onClick={() => handleTestConnection(connection)}
                        disabled={testingId === connection.id}
                      >
                        {testingId === connection.id ? (
                          <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                        ) : (
                          <PlayCircle className="h-4 w-4 mr-2" />
                        )}
                        Test Connection
                      </DropdownMenuItem>
                      <DropdownMenuItem asChild>
                        <Link href={`/connections/${connection.id}?type=${connection.type}`}>
                          <Edit className="h-4 w-4 mr-2" />
                          Edit
                        </Link>
                      </DropdownMenuItem>
                      <DropdownMenuSeparator />
                      <DropdownMenuItem
                        className="text-red-600"
                        onClick={() => {
                          setDeleteBlockedBy(null)
                          setConnectionToDelete(connection)
                          setDeleteDialogOpen(true)
                        }}
                      >
                        <Trash2 className="h-4 w-4 mr-2" />
                        Delete
                      </DropdownMenuItem>
                    </DropdownMenuContent>
                  </DropdownMenu>
                  )}
                </div>
              </div>
            </Card>
          ))}
        </div>
      )}

      {/* No results */}
      {!loading && !error && connections.length > 0 && filteredConnections.length === 0 && (
        <div className="text-center py-12">
          <Search className="h-12 w-12 mx-auto text-zinc-300 dark:text-zinc-700 mb-4" />
          <p className="text-zinc-500 dark:text-zinc-400">
            No connections found matching &quot;{searchQuery}&quot;
          </p>
        </div>
      )}

      {/* Delete confirmation dialog */}
      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Connection</AlertDialogTitle>
            <AlertDialogDescription className="space-y-3">
              <div>
                Are you sure you want to delete &quot;{connectionToDelete?.name}&quot;? This action cannot
                be undone and will also clean up any associated replication slots and publications.
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
                setDeleteDialogOpen(false)
                setConnectionToDelete(null)
                setDeleteBlockedBy(null)
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
