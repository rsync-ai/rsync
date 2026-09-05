"use client"

import { useState, useEffect, useRef } from "react"
import { isLeakedLocalhostUrl } from "@/lib/config/api"
import { motion, AnimatePresence } from "framer-motion"
import { PageHeader } from "@/components/layout"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import {
  Database,
  Cloud,
  Puzzle,
  Search,
  Plus,
  ArrowRight,
  RefreshCw,
  Zap,
  ArrowUpRight,
  ArrowDownRight,
  FileJson,
  Activity,
  Sparkles,
  User,
  Container,
  Circle,
  ExternalLink,
  AlertTriangle,
} from "lucide-react"
import { MCPConnector, getConnectorEmoji, getCategoryColor, getConnectorStatusBadge, getConnectorConfidenceBadge, getConnectorConfidenceReasons } from "@/lib/types/mcp-connector"

// Category type matching the connector type definition
type ConnectorCategory = "database" | "storage" | "api" | "analytics" | "messaging" | "streaming" | "file" | "other"
import { fetchMCPConnectors, saveConnection } from "@/lib/api/mcp-connectors"
import { UnifiedConnectorModal } from "@/components/connectors/UnifiedConnectorModal"
import { ConnectionLogo } from "@/components/connectors/ConnectionLogo"
import { toast } from "sonner"
import { useRouter } from "next/navigation"
import { classifyError, type AppError } from "@/lib/utils/error-handling"
import { ErrorBanner } from "@/components/ui/error-banner"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { canGenerateConnectors } from "@/contexts/CurrentUserContext"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"

// Category icons mapping
const categoryIcons: Record<string, React.ElementType> = {
  database: Database,
  storage: Cloud,
  api: Zap,
  streaming: Activity,
  file: FileJson,
  other: Puzzle,
}

// Docker filter options
type DockerFilter = "all" | "running" | "deployed" | "local"

export default function ConnectorsPage() {
  const router = useRouter()
  // Workspace role (owner/admin) gates LLM connector generation
  // (api-gateway WorkspaceGeneratorMiddleware). The active-workspace role is the
  // source of truth, not the global user role.
  const { role: workspaceRole } = useWorkspaceRole()
  const canGenerate = canGenerateConnectors(workspaceRole)
  const [connectors, setConnectors] = useState<MCPConnector[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<AppError | null>(null)
  const [searchQuery, setSearchQuery] = useState("")
  const [selectedCategory, setSelectedCategory] = useState<string | null>(null)
  const [dockerFilter, setDockerFilter] = useState<DockerFilter>("all")
  const [selectedConnector, setSelectedConnector] = useState<MCPConnector | null>(null)
  const [configModalOpen, setConfigModalOpen] = useState(false)

  // Fetch connectors from MCP API
    const loadConnectors = async () => {
      try {
        setLoading(true)
      setError(null)
        const data = await fetchMCPConnectors()
      
      // Filter out internal connectors (like MinIO) that are system-only.
      //
      // sample-data was excluded here too, as "the dormant bundled demo source".
      // Dormant described the DEPLOYMENT, not the connector: it is the only
      // auth_type "none" source in the catalog and the seed image has always
      // baked it, but no compose file started a container for it, so choosing it
      // failed at connection test with a DNS miss. The quickstart now runs it
      // (docker-compose.quickstart.yml, service `sample-data-mcp`), which makes
      // it the one source a fresh install can connect to without first producing
      // a credential -- so hiding it from the browse catalog would now hide the
      // only thing a brand-new user can actually try.
      const visibleConnectors = (data.connectors || []).filter(
        (connector) => !connector.internal
      )
      
      setConnectors(visibleConnectors)
      
      if (process.env.NODE_ENV === 'development' && visibleConnectors.length > 0) {
        console.log(`✅ Loaded ${visibleConnectors.length} connectors`)
      }
      } catch (err) {
        const e = classifyError(err, "connectors.load")
        setError(e)
        setConnectors([])
        toast.error(e.title, { description: e.hint ?? e.message })
      } finally {
        setLoading(false)
      }
    }

  useEffect(() => {
    loadConnectors()
  }, [])

  // Refetch connectors when the config modal transitions from open -> closed,
  // so a failed/cancelled save doesn't leave a stale or empty list.
  const prevConfigModalOpen = useRef(configModalOpen)
  useEffect(() => {
    if (prevConfigModalOpen.current && !configModalOpen) {
      loadConnectors()
    }
    prevConfigModalOpen.current = configModalOpen
  }, [configModalOpen])

  // Define category display order: Database → Streaming → Storage → File → API → Other
  const categoryOrder: ConnectorCategory[] = ["database", "streaming", "storage", "file", "api", "other"]

  // Map API categories to UI categories
  const mapCategory = (apiCategory: string): ConnectorCategory => {
    const categoryMap: Record<string, ConnectorCategory> = {
      // Database types - all DB variants map to "database"
      relational_db: "database",
      nosql_db: "database",
      database: "database",
      document_db: "database",
      wide_column_db: "database",  // Cassandra, ScyllaDB, HBase
      time_series_db: "database",  // InfluxDB, TimescaleDB
      graph_db: "database",        // Neo4j, ArangoDB
      key_value_db: "database",    // Redis, DynamoDB
      search_db: "database",       // Elasticsearch, Solr
      vector_db: "database",       // Pinecone, Milvus
      data_warehouse: "database",  // Snowflake, BigQuery, Redshift (treat as DB in UI)
      // API types
      api_saas: "api",
      api: "api",
      saas: "api",
      crm: "api",
      erp: "api",
      // Streaming types
      streaming: "streaming",
      messaging: "streaming",
      event_streaming: "streaming",
      cdc: "streaming",            // Debezium, etc.
      // Storage types
      storage: "storage",
      cloud_storage: "storage",
      object_storage: "storage",
      data_lake: "storage",
      // File types
      file: "file",
      file_system: "file",
      // Utility/Other
      utility: "other",
      other: "other",
    }
    return categoryMap[apiCategory?.toLowerCase()] || "other"
  }

  // Get unique mapped categories in the defined order
  const allMappedCategories = [...new Set(connectors.map((c) => mapCategory(c.category)))]
  const categories = categoryOrder.filter(cat => allMappedCategories.includes(cat))

  // Docker status counts
  const runningCount = connectors.filter((c) => c.docker_status === "running").length
  const deployedCount = connectors.filter((c) => c.docker_deployed || c.has_dockerfile).length
  const localCount = connectors.filter((c) => !c.has_dockerfile).length

  // Filter connectors
  const filteredConnectors = connectors.filter((c) => {
    // Text search
    const matchesSearch =
      c.display_name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      c.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      c.description?.toLowerCase().includes(searchQuery.toLowerCase())

    // Category filter (use mapped category)
    const matchesCategory = !selectedCategory || mapCategory(c.category) === selectedCategory

    // Docker status filter
    let matchesDocker = true
    if (dockerFilter === "running") {
      matchesDocker = c.docker_status === "running"
    } else if (dockerFilter === "deployed") {
      matchesDocker = c.docker_deployed || c.has_dockerfile
    } else if (dockerFilter === "local") {
      matchesDocker = !c.has_dockerfile
    }

    return matchesSearch && matchesCategory && matchesDocker
  })

  // Group by mapped category
  const groupedConnectors = filteredConnectors.reduce<Record<string, MCPConnector[]>>(
    (acc, connector) => {
      const category = mapCategory(connector.category || "other")
      if (!acc[category]) {
        acc[category] = []
      }
      acc[category].push(connector)
      return acc
    },
    {}
  )

  // Sort grouped connectors by category order, and sort connectors within each
  // category alphabetically by display name. The backend returns connectors in
  // Go map-iteration order (randomized per request), so without this the grid
  // shuffles on every refresh.
  const sortedGroupedConnectors = Object.fromEntries(
    categoryOrder
      .filter(cat => groupedConnectors[cat])
      .map(cat => [
        cat,
        [...groupedConnectors[cat]].sort((a, b) =>
          (a.display_name || a.name).localeCompare(
            b.display_name || b.name,
            undefined,
            { sensitivity: "base" }
          )
        ),
      ])
  )

  const handleConfigure = (connector: MCPConnector) => {
    setSelectedConnector(connector)
    setConfigModalOpen(true)
  }

  const handleSaveConnection = async (config: Record<string, unknown>) => {
    try {
      // Save connection to backend
      const result = await saveConnection(config)
      
      toast.success("Connection saved successfully", {
        description: `Connection "${(config.name as string) || "Unnamed"}" has been created`,
      })
      
    setConfigModalOpen(false)
    setSelectedConnector(null)
      
      // Redirect to connections page
      router.push("/connections")
    } catch (err) {
      const e = classifyError(err, "connector.save")
      toast.error(e.title, { description: e.hint ?? e.message })
      throw err // Re-throw to let the form handle it
    }
  }

  return (
    <div className="flex flex-col h-full">
      <PageHeader
        heading="Data Connectors"
        description="Manage your active MCP connectors and generate new ones"
      >
        <div className="flex gap-2">
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                {/* span wrapper so the tooltip still fires when the button is disabled */}
                <span className={!canGenerate ? "cursor-not-allowed" : undefined}>
                  <Button
                    onClick={() => router.push("/connectors/generate")}
                    disabled={!canGenerate}
                    className="bg-gradient-to-r from-violet-600 to-indigo-600"
                  >
                    <Sparkles className="h-4 w-4 mr-2" />
                    Generate New
                  </Button>
                </span>
              </TooltipTrigger>
              {!canGenerate && (
                <TooltipContent>
                  Connector generation requires the Owner or Admin role in this workspace. Ask your workspace owner to generate it or to raise your role.
                </TooltipContent>
              )}
            </Tooltip>
          </TooltipProvider>
        </div>
      </PageHeader>

      <div className="flex-1 space-y-6 overflow-auto">
            {/* Docker Status Filter */}
            <div className="flex flex-wrap items-center gap-2 p-3 bg-zinc-50 dark:bg-zinc-900/50 rounded-lg border border-zinc-200 dark:border-zinc-800">
              <span className="text-sm font-medium text-zinc-700 dark:text-zinc-300 mr-2">
                <Container className="h-4 w-4 inline mr-1" />
                Status:
              </span>
              <Button
                variant={dockerFilter === "all" ? "default" : "outline"}
                size="sm"
                onClick={() => setDockerFilter("all")}
                className="h-7 text-xs"
              >
                All ({connectors.length})
              </Button>
              <Button
                variant={dockerFilter === "running" ? "default" : "outline"}
                size="sm"
                onClick={() => setDockerFilter("running")}
                className="h-7 text-xs gap-1"
              >
                <Circle className="h-2 w-2 fill-emerald-500 text-emerald-500" />
                Running ({runningCount})
              </Button>
              <Button
                variant={dockerFilter === "deployed" ? "default" : "outline"}
                size="sm"
                onClick={() => setDockerFilter("deployed")}
                className="h-7 text-xs"
              >
                Available ({deployedCount})
              </Button>
              <Button
                variant={dockerFilter === "local" ? "default" : "outline"}
                size="sm"
                onClick={() => setDockerFilter("local")}
                className="h-7 text-xs"
              >
                Unavailable ({localCount})
              </Button>
            </div>

            {/* Search and Category Filter */}
            <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between">
              {/* Category filters */}
              <div className="flex flex-wrap gap-2">
                <Button
                  variant={selectedCategory === null ? "default" : "outline"}
                  size="sm"
                  onClick={() => setSelectedCategory(null)}
                >
                  All Categories
                </Button>
                {categories.map((category) => {
                  const Icon = categoryIcons[category] || Puzzle
                  const count = filteredConnectors.filter((c) => mapCategory(c.category) === category).length
                  return (
                    <Button
                      key={category}
                      variant={selectedCategory === category ? "default" : "outline"}
                      size="sm"
                      onClick={() => setSelectedCategory(category)}
                      className="capitalize"
                    >
                      <Icon className="h-4 w-4 mr-1" />
                      {category} ({count})
                    </Button>
                  )
                })}
              </div>

              {/* Search */}
              <div className="relative w-full sm:w-80">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-zinc-400" />
                <Input
                  placeholder="Search connectors..."
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
          <ErrorBanner error={error} onRetry={loadConnectors} />
        )}

        {/* Connectors grid */}
        {!loading && !error && (
          <AnimatePresence mode="popLayout">
            {Object.keys(groupedConnectors).length === 0 ? (
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                className="text-center py-12"
              >
                <Database className="h-12 w-12 mx-auto text-zinc-300 dark:text-zinc-700 mb-4" />
                <p className="text-zinc-500 dark:text-zinc-400">
                  No connectors found matching your criteria
                </p>
              </motion.div>
            ) : (
              <div className="space-y-8">
                {Object.entries(sortedGroupedConnectors).map(([category, items]) => {
                  const Icon = categoryIcons[category] || Puzzle
                  return (
                    <motion.div
                      key={category}
                      initial={{ opacity: 0, y: 20 }}
                      animate={{ opacity: 1, y: 0 }}
                      exit={{ opacity: 0, y: -20 }}
                    >
                      <div className="flex items-center gap-2 mb-4">
                        <Icon className="h-5 w-5 text-zinc-500" />
                        <h3 className="text-lg font-semibold capitalize text-zinc-900 dark:text-white">
                          {category}
                        </h3>
                        <Badge variant="secondary">{items.length}</Badge>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                        {items.map((connector, index) => (
                          <motion.div
                            key={connector.name}
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: index * 0.05 }}
                            className="group relative p-4 rounded-xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 hover:border-violet-300 dark:hover:border-violet-700 hover:shadow-lg hover:shadow-violet-500/10 transition-all cursor-pointer"
                            onClick={() => handleConfigure(connector)}
                          >
                            {/* Connector icon, name, and status chip */}
                            <div className="flex items-start gap-3 mb-3">
                              <ConnectionLogo connector={connector} size="md" />
                              <div className="flex-1 min-w-0">
                                <h4 className="font-semibold text-zinc-900 dark:text-white truncate">
                                  {connector.display_name}
                                </h4>
                                {/* Canonical connector id/slug — disambiguates same-titled
                                    connectors (e.g. stripe vs stripe-demo) and is the
                                    connector_type users need for config. */}
                                <p
                                  title={connector.name}
                                  className="text-[11px] font-mono text-zinc-500 dark:text-zinc-400 truncate leading-tight"
                                >
                                  {connector.name}
                                </p>
                                <div className="flex items-center gap-2 mt-1">
                                  <Badge variant="secondary" className="text-xs">
                                    v{connector.version}
                                  </Badge>
                                  {(() => {
                                    // Two orthogonal chips: Status (runtime-tested?)
                                    // + Confidence (static QA quality). Confidence is
                                    // hidden when unrated so it never masks Status.
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
                                  {/* Host-loopback health link: only meaningful when the browser is itself
                                      on localhost (self-host / dev). On a real deployment
                                      `http://localhost:<port>` is the USER's machine, so hide the dead link.
                                      These rows are client-fetched (never SSR'd), so no mount gate is needed. */}
                                  {connector.docker_port && connector.docker_status === "running" &&
                                    !isLeakedLocalhostUrl(`http://localhost:${connector.docker_port}/health`) && (
                                    <a
                                      href={`http://localhost:${connector.docker_port}/health`}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      onClick={(e) => e.stopPropagation()}
                                      className="inline-flex items-center gap-1 text-[10px] text-violet-600 hover:text-violet-700 dark:text-violet-400"
                                    >
                                      :{connector.docker_port}
                                      <ExternalLink className="h-2.5 w-2.5" />
                                    </a>
                                  )}
                                </div>
                              </div>

                              {/* Docker status chip — flex sibling so long names truncate instead of hiding under it */}
                              <div className="shrink-0 flex items-center gap-1">
                                {connector.docker_deployed && connector.docker_status === "running" ? (
                                  <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-emerald-100 dark:bg-emerald-900/40 text-[10px] font-medium text-emerald-700 dark:text-emerald-400">
                                    <Circle className="h-2 w-2 fill-emerald-500" />
                                    Running
                                  </span>
                                ) : connector.docker_deployed && connector.docker_status === "stopped" ? (
                                  <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-red-100 dark:bg-red-900/40 text-[10px] font-medium text-red-700 dark:text-red-400">
                                    <Circle className="h-2 w-2 fill-red-500" />
                                    Stopped
                                  </span>
                                ) : connector.has_dockerfile ? (
                                  <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-amber-100 dark:bg-amber-900/40 text-[10px] font-medium text-amber-700 dark:text-amber-400">
                                    <Container className="h-2.5 w-2.5" />
                                    Ready
                                  </span>
                                ) : (
                                  <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-zinc-100 dark:bg-zinc-800 text-[10px] font-medium text-zinc-500 dark:text-zinc-400">
                                    <Circle className="h-2 w-2 fill-zinc-400" />
                                    Local
                                  </span>
                                )}
                              </div>
                            </div>

                            {/* Description */}
                            <p className="text-sm text-zinc-500 dark:text-zinc-400 line-clamp-2 mb-3">
                              {connector.description}
                            </p>

                            {/* QA warnings (transparent, non-blocking) */}
                            {/* Intentionally hide QA/export validation messages on cards (too noisy).
                                Detailed QA metadata remains visible inside Configure modal. */}

                            {/* Capabilities */}
                            <div className="flex flex-wrap gap-1.5 mb-3">
                              {connector.supports_source && (
                                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-emerald-50 dark:bg-emerald-900/30 text-xs text-emerald-600 dark:text-emerald-400">
                                  <ArrowUpRight className="h-3 w-3" />
                                  Source
                                </span>
                              )}
                              {connector.supports_destination && (
                                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-blue-50 dark:bg-blue-900/30 text-xs text-blue-600 dark:text-blue-400">
                                  <ArrowDownRight className="h-3 w-3" />
                                  Destination
                                </span>
                              )}
                              {connector.supports_cdc && (
                                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-purple-50 dark:bg-purple-900/30 text-xs text-purple-600 dark:text-purple-400">
                                  <Zap className="h-3 w-3" />
                                  CDC
                                </span>
                              )}
                            </div>

                            {/* Actions */}
                            <div className="flex items-center justify-between pt-3 border-t border-zinc-100 dark:border-zinc-800">
                              <Button
                                variant="ghost"
                                size="sm"
                                className="text-xs"
                                onClick={(e) => {
                                  e.stopPropagation()
                                  handleConfigure(connector)
                                }}
                              >
                                <Plus className="h-3 w-3 mr-1" />
                                Configure
                              </Button>
                            </div>
                          </motion.div>
                        ))}
                      </div>
                    </motion.div>
                  )
                })}
              </div>
            )}
          </AnimatePresence>
        )}
      </div>

      {/* Configuration Modal */}
      <UnifiedConnectorModal
        open={configModalOpen}
        onOpenChange={setConfigModalOpen}
              connector={selectedConnector}
              onSave={handleSaveConnection}
            />
    </div>
  )
}
